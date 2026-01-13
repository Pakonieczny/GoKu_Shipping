// netlify/functions/openaiImageProxy.js
// Supports:
// - mode: "generations"  -> POST https://api.openai.com/v1/images/generations (JSON)
// - mode: "edits"        -> POST https://api.openai.com/v1/images/edits (multipart)
// Returns OpenAI response JSON (typically includes data[].b64_json)

function json(statusCode, obj) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      // Same-origin on Netlify usually means no CORS required, but this helps during dev.
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
    },
    body: JSON.stringify(obj),
  };
}

function dataUrlToBuffer(dataUrl) {
  // data:image/png;base64,AAAA...
  const m = /^data:(.+?);base64,(.+)$/.exec(String(dataUrl || ""));
  if (!m) throw new Error("Invalid data URL (expected data:<mime>;base64,...)");
  const mime = m[1];
  const b64 = m[2];
  return { mime, buffer: Buffer.from(b64, "base64") };
}

exports.handler = async (event) => {
  try {
    if (event.httpMethod === "OPTIONS") return json(200, { ok: true });

    if (event.httpMethod !== "POST") {
      return json(405, { error: "Method not allowed" });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) return json(500, { error: "Missing OPENAI_API_KEY environment variable" });

    const body = JSON.parse(event.body || "{}");

    const mode = body.mode || "edits"; // default to edits (your use-case: duplicate/modify uploaded images)
    const model = body.model || process.env.OPENAI_IMAGE_MODEL || "gpt-image-1"; // you can send "gpt-image-1.5" from UI
    const prompt = body.prompt || "";
    const size = body.size || "1024x1024";
    const n = Math.max(1, Math.min(8, Number(body.n || 1)));

    const common = { model, prompt, size, n };

    const baseUrl = "https://api.openai.com/v1/images";

    if (mode === "generations") {
      const resp = await fetch(`${baseUrl}/generations`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({
          ...common,
          // Optional passthroughs:
          background: body.background,
          output_format: body.output_format, // "png" | "jpeg" | "webp" (if supported by the model)
          quality: body.quality,
        }),
      });

      const data = await resp.json();
      if (!resp.ok) return json(resp.status, { error: data });
      return json(200, data);
    }

    if (mode === "edits") {
      if (!body.input_image) {
        return json(400, { error: "mode=edits requires input_image (data URL base64)" });
      }

      const { mime, buffer } = dataUrlToBuffer(body.input_image);

      // Node 18+ has global FormData + Blob in Netlify runtimes.
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

      // optional passthroughs
      if (body.background != null) form.append("background", String(body.background));
      if (body.output_format) form.append("output_format", String(body.output_format));
      if (body.quality) form.append("quality", String(body.quality));

      const resp = await fetch(`${baseUrl}/edits`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
        },
        body: form,
      });

      const data = await resp.json();
      if (!resp.ok) return json(resp.status, { error: data });
      return json(200, data);
    }

    return json(400, { error: `Unknown mode: ${mode}` });
  } catch (err) {
    return json(500, { error: err.message || String(err) });
  }
};