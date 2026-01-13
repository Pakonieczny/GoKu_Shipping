// netlify/functions/openaiImageProxy.js (NEW + RESILIENT)
//
// Purpose:
// - Proxy OpenAI Images API safely from the browser (keeps OPENAI_API_KEY server-side)
// - Robust error handling (never crashes on empty/non-JSON bodies)
// - Supports:
//    1) kind/mode: "generations"  -> POST https://api.openai.com/v1/images/generations (JSON)
//    2) kind/mode: "edits"        -> POST https://api.openai.com/v1/images/edits (multipart)
//
// Expected browser payload (your UI can send either "kind" or "mode"):
// {
//   kind: "edits" | "generations",
//   model: "gpt-image-1.5" | "gpt-image-1" | ...,
//   prompt: "...",
//   n: 1..8,
//   size: "1024x1024" | ...,
//   input_image: "data:image/jpeg;base64,...",   // required for edits
//   mask_image:  "data:image/png;base64,...",    // optional for edits
//   output_format: "png" | "jpeg" | "webp",      // optional (if supported by model)
//   quality: "high" | "medium" | "low",          // optional (if supported by model)
//   background: "transparent" | "white" | ...    // optional (if supported by model)
// }

function respond(statusCode, obj) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      // prevent stale caching while iterating
      "Cache-Control": "no-store",
    },
    body: JSON.stringify(obj),
  };
}

function clampInt(n, min, max, fallback) {
  const v = Number(n);
  if (!Number.isFinite(v)) return fallback;
  return Math.max(min, Math.min(max, Math.trunc(v)));
}

function readJsonBody(event) {
  try {
    return JSON.parse(event.body || "{}");
  } catch {
    return null;
  }
}

function dataUrlToBuffer(dataUrl) {
  // data:image/png;base64,AAAA...
  const m = /^data:(.+?);base64,(.+)$/.exec(String(dataUrl || ""));
  if (!m) throw new Error("Invalid data URL (expected data:<mime>;base64,...)");
  const mime = m[1];
  const b64 = m[2];
  return { mime, buffer: Buffer.from(b64, "base64") };
}

// IMPORTANT: Never call resp.json() directly; Netlify/OpenAI may return empty or non-JSON.
async function readJsonSafe(resp) {
  const text = await resp.text();
  if (!text) return { text: "", json: null };
  try {
    return { text, json: JSON.parse(text) };
  } catch {
    return { text, json: null };
  }
}

function pickMode(body) {
  const v = (body?.kind || body?.mode || "").toLowerCase().trim();
  if (v === "generations" || v === "generation") return "generations";
  if (v === "edits" || v === "edit") return "edits";
  // default (your app primarily duplicates/variants from an uploaded reference)
  return "edits";
}

exports.handler = async (event) => {
  try {
    if (event.httpMethod === "OPTIONS") return respond(200, { ok: true });

    if (event.httpMethod !== "POST") {
      return respond(405, { error: { message: "Method not allowed" } });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      return respond(500, { error: { message: "Missing OPENAI_API_KEY environment variable" } });
    }

    const body = readJsonBody(event);
    if (!body) {
      return respond(400, { error: { message: "Invalid JSON body" } });
    }

    const mode = pickMode(body);
    const model = String(body.model || process.env.OPENAI_IMAGE_MODEL || "gpt-image-1.5");
    const prompt = String(body.prompt || "");
    const size = String(body.size || "1024x1024");
    const n = clampInt(body.n, 1, 8, 1);

    const output_format = body.output_format != null ? String(body.output_format) : undefined;
    const quality = body.quality != null ? String(body.quality) : undefined;
    const background = body.background != null ? String(body.background) : undefined;

    const baseUrl = "https://api.openai.com/v1/images";

    // -------------------------
    // MODE: generations (JSON)
    // -------------------------
    if (mode === "generations") {
      const payload = { model, prompt, size, n };
      if (output_format) payload.output_format = output_format;
      if (quality) payload.quality = quality;
      if (background) payload.background = background;

      const upstream = await fetch(`${baseUrl}/generations`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify(payload),
      });

      const { text, json } = await readJsonSafe(upstream);
      if (!upstream.ok) {
        return respond(upstream.status, {
          error: {
            message:
              json?.error?.message ||
              json?.message ||
              text ||
              `OpenAI images/generations failed with HTTP ${upstream.status} (empty body)`,
            upstream_status: upstream.status,
          },
        });
      }

      if (!json) {
        return respond(502, {
          error: { message: "OpenAI returned 200 but body was empty or not JSON." },
        });
      }

      return respond(200, json);
    }

    // -------------------------
    // MODE: edits (multipart)
    // -------------------------
    if (mode === "edits") {
      const input_image = body.input_image;
      if (!input_image) {
        return respond(400, { error: { message: 'kind/mode "edits" requires input_image (data URL)' } });
      }

      const { mime, buffer } = dataUrlToBuffer(input_image);

      // Node 18+ (Netlify) provides FormData + Blob globals
      const form = new FormData();
      form.append("model", model);
      form.append("prompt", prompt);
      form.append("size", size);
      form.append("n", String(n));

      // image file
      form.append("image", new Blob([buffer], { type: mime }), "input.png");

      // optional mask
      if (body.mask_image) {
        const m2 = dataUrlToBuffer(body.mask_image);
        form.append("mask", new Blob([m2.buffer], { type: m2.mime }), "mask.png");
      }

      // optional passthroughs (only if supported by model)
      if (output_format) form.append("output_format", output_format);
      if (quality) form.append("quality", quality);
      if (background) form.append("background", background);

      const upstream = await fetch(`${baseUrl}/edits`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
        },
        body: form,
      });

      const { text, json } = await readJsonSafe(upstream);
      if (!upstream.ok) {
        return respond(upstream.status, {
          error: {
            message:
              json?.error?.message ||
              json?.message ||
              text ||
              `OpenAI images/edits failed with HTTP ${upstream.status} (empty body)`,
            upstream_status: upstream.status,
          },
        });
      }

      if (!json) {
        return respond(502, {
          error: { message: "OpenAI returned 200 but body was empty or not JSON." },
        });
      }

      return respond(200, json);
    }

    // Should never hit
    return respond(400, { error: { message: `Unknown kind/mode: ${mode}` } });
  } catch (err) {
    return respond(500, {
      error: {
        message: err?.message || String(err),
      },
    });
  }
};