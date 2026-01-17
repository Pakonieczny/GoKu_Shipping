/* netlify/functions/openaiImageProxy-background.js
   Background Function: runs long OpenAI image edits without browser/edge inactivity 504s.
   Writes realtime status to Firestore + uploads final PNG to Firebase Storage.
*/

const admin = require("./firebaseAdmin");

// Node 18 on Netlify provides fetch/FormData/Blob globally.
// If your build ever lacks fetch, uncomment:
// const fetch = require("node-fetch");

const JOBS_COLL = "ListingGenerator1Jobs";
const IMAGES_COLL = "ListingGenerator1Images";

// ---- helpers ----
function json(statusCode, obj) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
    },
    body: JSON.stringify(obj),
  };
}

function parseJsonBody(event) {
  try {
    return event?.body ? JSON.parse(event.body) : null;
  } catch {
    return null;
  }
}

function dataUrlToBuffer(dataUrl) {
  // data:image/png;base64,xxxx
  const m = /^data:([^;]+);base64,(.+)$/.exec(dataUrl || "");
  if (!m) throw new Error("input_image must be a data URL: data:<mime>;base64,<...>");
  const mime = m[1];
  const b64 = m[2];
  return { mime, buffer: Buffer.from(b64, "base64") };
}

/**
 * Read an already-uploaded image from Firebase Storage.
 * Avoids sending large base64 payloads from the browser.
 */
async function storagePathToBuffer(storagePath) {
  const p = String(storagePath || "").trim();
  if (!p) throw new Error("input_storage_path must be a non-empty string");

  // Allowlist to prevent arbitrary bucket reads.
  // Must match the prefixes your pipeline uses (reference, charm macro, pass-A outputs).
  const ALLOWED_INPUT_PREFIXES = [
    "listing-generator-1/reference/",
    "listing-generator-1/charm-macro/",
    "listing-generator-1/generated/",
  ];

  if (!ALLOWED_INPUT_PREFIXES.some((prefix) => p.startsWith(prefix))) {
    throw new Error("input_storage_path not allowed");
  }

  const bucket = admin.storage().bucket();
  const file = bucket.file(p);

  const [exists] = await file.exists();
  if (!exists) throw new Error(`input_storage_path not found: ${p}`);

  let mime = "application/octet-stream";
  try {
    const [meta] = await file.getMetadata();
    if (meta?.contentType) mime = meta.contentType;
  } catch {
    // ignore metadata failure; still download bytes
  }

  const [buffer] = await file.download();
  return { mime, buffer };
}

function safeErr(err) {
  return {
    message: err?.message || String(err),
    name: err?.name,
    stack: err?.stack,
  };
}

function clampNumber(n, min, max, fallback) {
  const x = Number(n);
  if (!Number.isFinite(x)) return fallback;
  return Math.max(min, Math.min(max, x));
}

async function callOpenAIImagesEdits({
  apiKey,
  model,
  prompt,
  size,
  quality,
  output_format,
  images, // [{ buffer, mime, filename }]
}) {
  const form = new FormData();
  form.append("model", model);
  form.append("prompt", prompt);
  form.append("size", size);
  form.append("quality", quality);
  form.append("output_format", output_format);

  // images/edits supports multiple input images, but they must be sent as an array:
  // use "image[]" for each file. Order matters.
  for (const img of images) {
    const blob = new Blob([img.buffer], { type: img.mime });
    form.append("image[]", blob, img.filename || "image.png");
  }

  const resp = await fetch("https://api.openai.com/v1/images/edits", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
    },
    body: form,
  });

  const raw = await resp.text();
  let data = null;
  try {
    data = raw ? JSON.parse(raw) : null;
  } catch {
    data = null;
  }

  if (!resp.ok) {
    const msg =
      data?.error?.message ||
      raw ||
      `OpenAI Images API failed with HTTP ${resp.status} (empty body)`;
    throw new Error(msg);
  }

  const b64 = data?.data?.[0]?.b64_json;
  if (!b64) throw new Error("OpenAI response missing data[0].b64_json");

  return Buffer.from(b64, "base64");
}

async function callOpenAIImagesGenerations({ apiKey, model, prompt, size, quality, output_format }) {
  const resp = await fetch("https://api.openai.com/v1/images/generations", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model,
      prompt,
      size,
      quality,
      output_format,
    }),
  });

  const raw = await resp.text();
  let data = null;
  try {
    data = raw ? JSON.parse(raw) : null;
  } catch {
    data = null;
  }

  if (!resp.ok) {
    const msg =
      data?.error?.message ||
      raw ||
      `OpenAI Images API failed with HTTP ${resp.status} (empty body)`;
    throw new Error(msg);
  }

  const b64 = data?.data?.[0]?.b64_json;
  if (!b64) throw new Error("OpenAI response missing data[0].b64_json");

  return Buffer.from(b64, "base64");
}

async function uploadPngBufferToStorage({ outBuf, jobId, runId, slotIndex }) {
  const bucket = admin.storage().bucket(); // uses storageBucket from firebaseAdmin.js init
  const token = globalThis.crypto?.randomUUID
    ? globalThis.crypto.randomUUID()
    : require("crypto").randomUUID();

  const effectiveRunId = runId || `lg1_${Date.now()}`;
  const effectiveSlot = typeof slotIndex === "number" ? slotIndex : null;

  const storagePath = `listing-generator-1/generated/${effectiveRunId}/${jobId}.png`;
  const file = bucket.file(storagePath);

  await file.save(outBuf, {
    resumable: false,
    contentType: "image/png",
    metadata: {
      metadata: {
        firebaseStorageDownloadTokens: token,
      },
    },
  });

  const bucketName = bucket.name;
  const encoded = encodeURIComponent(storagePath).replace(/%2F/g, "%2F");
  const downloadURL = `https://firebasestorage.googleapis.com/v0/b/${bucketName}/o/${encoded}?alt=media&token=${token}`;

  return { storagePath, downloadURL, effectiveRunId, effectiveSlot };
}

/**
 * Postprocess pipeline:
 * - Given passA (with oversized charm),
 * - inpaint-remove charm to recover base pixels,
 * - compute diff mask to isolate charm pixels from passA,
 * - scale charm crop down with Lanczos,
 * - add subtle contact shadow,
 * - composite onto recovered base.
 */
async function postScaleCharmComposite({
  passABuf,
  baseNoCharmBuf,
  scale,
  shadowOpacity,
  shadowBlur,
  diffThreshold,
}) {
  let sharp;
  try {
    sharp = require("sharp");
  } catch (e) {
    throw new Error(
      "Missing dependency: sharp. Add it to your Netlify functions bundle (npm i sharp) to use kind=charm_postscale."
    );
  }

  const aMeta = await sharp(passABuf).metadata();
  const bMeta = await sharp(baseNoCharmBuf).metadata();
  if (!aMeta?.width || !aMeta?.height || !bMeta?.width || !bMeta?.height) {
    throw new Error("Could not read image metadata for postprocess.");
  }
  if (aMeta.width !== bMeta.width || aMeta.height !== bMeta.height) {
    throw new Error(
      `postprocess requires same dimensions. passA=${aMeta.width}x${aMeta.height}, base=${bMeta.width}x${bMeta.height}`
    );
  }

  const width = aMeta.width;
  const height = aMeta.height;

  // Decode raw RGBA for both images
  const aRaw = await sharp(passABuf).ensureAlpha().raw().toBuffer();
  const bRaw = await sharp(baseNoCharmBuf).ensureAlpha().raw().toBuffer();

  // Build a 1-channel alpha mask where pixels differ (likely charm region)
  const thr = clampNumber(diffThreshold, 8, 120, 26);
  const mask = Buffer.alloc(width * height);
  for (let i = 0, p = 0; i < mask.length; i++, p += 4) {
    const dr = Math.abs(aRaw[p] - bRaw[p]);
    const dg = Math.abs(aRaw[p + 1] - bRaw[p + 1]);
    const db = Math.abs(aRaw[p + 2] - bRaw[p + 2]);
    const d = dr + dg + db;
    mask[i] = d > thr ? 255 : 0;
  }

  // Soften edges + reduce speckle
  const maskImg = require("sharp")(mask, { raw: { width, height, channels: 1 } })
    .blur(1)
    .threshold(18);

  const maskRaw = await maskImg.raw().toBuffer();

  // Compute bounding box of mask
  let minX = width,
    minY = height,
    maxX = -1,
    maxY = -1;
  for (let y = 0; y < height; y++) {
    const row = y * width;
    for (let x = 0; x < width; x++) {
      if (maskRaw[row + x] > 0) {
        if (x < minX) minX = x;
        if (y < minY) minY = y;
        if (x > maxX) maxX = x;
        if (y > maxY) maxY = y;
      }
    }
  }

  // If we failed to isolate anything, return passA (safe fallback)
  if (maxX < 0 || maxY < 0) return passABuf;

  // Expand bbox slightly (jump ring + edge pixels)
  const pad = 6;
  const left = Math.max(0, minX - pad);
  const top = Math.max(0, minY - pad);
  const bboxW = Math.min(width - left, maxX - minX + 1 + pad * 2);
  const bboxH = Math.min(height - top, maxY - minY + 1 + pad * 2);

  // Make a soft alpha crop for cleaner edges
  const maskPng = await sharp(maskRaw, { raw: { width, height, channels: 1 } })
    .extract({ left, top, width: bboxW, height: bboxH })
    .blur(0.8)
    .png()
    .toBuffer();

  // Extract charm crop from passA; set alpha from diff-mask crop
  const charmCrop = await sharp(passABuf)
    .extract({ left, top, width: bboxW, height: bboxH })
    .removeAlpha()
    .joinChannel(maskPng)
    .png()
    .toBuffer();

  const s = clampNumber(scale, 0.50, 0.70, 0.65);
  const outW = Math.max(1, Math.round(bboxW * s));
  const outH = Math.max(1, Math.round(bboxH * s));

  // Downscale charm crop (high-quality resampling)
  const scaledCharm = await sharp(charmCrop)
    .resize(outW, outH, { kernel: "lanczos3" })
    .png()
    .toBuffer();

  // Contact shadow from the scaled alpha channel
  const shBlur = clampNumber(shadowBlur, 0, 12, 2);
  const shOp = clampNumber(shadowOpacity, 0, 0.6, 0.28);

  const shadowAlpha = await sharp(scaledCharm)
    .extractChannel(3)
    .blur(shBlur)
    .linear(shOp, 0)
    .png()
    .toBuffer();

  const shadowLayer = await sharp({
    create: {
      width: outW,
      height: outH,
      channels: 4,
      background: { r: 0, g: 0, b: 0, alpha: 1 },
    },
  })
    .composite([{ input: shadowAlpha, blend: "dest-in" }])
    .png()
    .toBuffer();

  // Anchor: keep top-center-ish of original bbox fixed so jump ring stays on chain.
  // (Center anchor is robust; if you want tighter ring-lock later, we can move anchor up.)
  const anchorX = left + Math.round(bboxW / 2);
  const newLeft = Math.max(0, Math.min(width - outW, Math.round(anchorX - outW / 2)));
  const newTop = Math.max(0, Math.min(height - outH, top));

  // Composite: recovered base -> shadow -> charm
  const finalBuf = await sharp(baseNoCharmBuf)
    .composite([
      { input: shadowLayer, left: newLeft, top: Math.min(height - outH, newTop + 1), blend: "multiply" },
      { input: scaledCharm, left: newLeft, top: newTop, blend: "over" },
    ])
    .png()
    .toBuffer();

  return finalBuf;
}

// ---- handler ----
exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return json(200, { ok: true });
  if (event.httpMethod !== "POST") return json(405, { error: { message: "Method not allowed" } });

  const body = parseJsonBody(event);
  if (!body) return json(400, { error: { message: "Invalid JSON body" } });

  const {
    jobId,
    runId,
    slotIndex,
    kind = "edits", // "edits" | "generations" | "charm_postscale"
    model = "gpt-image-1",
    prompt,
    size = "1024x1536",
    quality = "high",
    output_format = "png",

    // Reference image input (either already in storage or inline base64)
    input_storage_path,
    input_image,

    // Charm macro optional second image (for normal edits flow)
    input_charm_storage_path,
    input_charm_image,

    // For charm_postscale:
    remove_prompt,
    postprocess,
  } = body || {};

  if (!jobId) return json(400, { error: { message: "jobId is required" } });

  const db = admin.firestore();
  const jobRef = db.collection(JOBS_COLL).doc(jobId);

  try {
    await jobRef.set(
      {
        status: "running",
        stage: "starting",
        runId: runId || null,
        slotIndex: typeof slotIndex === "number" ? slotIndex : null,
        kind,
        model,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY env var");

    // -------------------------
    // SPECIAL: charm_postscale
    // -------------------------
    if (kind === "charm_postscale") {
      // input_storage_path must point to Pass A output (oversized charm)
      if (!input_storage_path && !input_image) {
        throw new Error("charm_postscale requires input_storage_path or input_image (Pass A output)");
      }

      await jobRef.set(
        { stage: "downloading_inputs", updatedAt: admin.firestore.FieldValue.serverTimestamp() },
        { merge: true }
      );

      const passA = input_storage_path
        ? await storagePathToBuffer(input_storage_path)
        : dataUrlToBuffer(input_image);

      // Step 1: inpaint-remove charm to recover base pixels behind it (same framing)
      const rp =
        String(remove_prompt || "").trim() ||
        "Remove the pendant charm + jump ring completely and reconstruct the satellite chain and skin behind it. Keep EVERYTHING else identical. Do not change framing, color grade, wardrobe, lighting, face, pose. Only remove the pendant and restore the pixels behind it.";

      await jobRef.set(
        { stage: "removing_charm", updatedAt: admin.firestore.FieldValue.serverTimestamp() },
        { merge: true }
      );

      const baseNoCharmBuf = await callOpenAIImagesEdits({
        apiKey,
        model,
        prompt: rp,
        size,
        quality,
        output_format,
        images: [{ buffer: passA.buffer, mime: passA.mime, filename: "passA.png" }],
      });

      // Step 2: postprocess scale (no re-generation of engraving)
      await jobRef.set(
        { stage: "postprocessing", updatedAt: admin.firestore.FieldValue.serverTimestamp() },
        { merge: true }
      );

      const finalBuf = await postScaleCharmComposite({
        passABuf: passA.buffer,
        baseNoCharmBuf,
        scale: postprocess?.scale,
        shadowOpacity: postprocess?.shadowOpacity,
        shadowBlur: postprocess?.shadowBlur,
        diffThreshold: postprocess?.diffThreshold,
      });

      await jobRef.set(
        { stage: "uploading", updatedAt: admin.firestore.FieldValue.serverTimestamp() },
        { merge: true }
      );

      const { storagePath, downloadURL, effectiveRunId, effectiveSlot } =
        await uploadPngBufferToStorage({ outBuf: finalBuf, jobId, runId, slotIndex });

      await db.collection(IMAGES_COLL).add({
        runId: effectiveRunId,
        slotIndex: effectiveSlot,
        createdAt: new Date(),
        storagePath,
        downloadURL,
        model,
        prompt: rp,
        traits: body.traits || null,
        jobId,
        kind,
        postprocess: postprocess || null,
      });

      await jobRef.set(
        {
          status: "done",
          stage: "done",
          storagePath,
          downloadURL,
          finishedAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        },
        { merge: true }
      );

      return json(202, { ok: true, jobId });
    }

    // -------------------------
    // DEFAULT: edits / generations behavior (preserved)
    // -------------------------
    if (!prompt) return json(400, { error: { message: "prompt is required" } });

    if (kind !== "edits" && kind !== "generations") {
      return json(400, { error: { message: "kind must be 'edits', 'generations', or 'charm_postscale'" } });
    }

    await jobRef.set(
      {
        stage: "calling_openai",
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    let outBuf;

    if (kind === "generations") {
      // Correct JSON path for generations
      outBuf = await callOpenAIImagesGenerations({
        apiKey,
        model,
        prompt,
        size,
        quality,
        output_format,
      });
    } else {
      // kind === "edits"
      if (!input_image && !input_storage_path) {
        return json(400, { error: { message: "Missing input_image or input_storage_path" } });
      }

      // Reference image (Image[0])
      const ref = input_storage_path
        ? await storagePathToBuffer(input_storage_path)
        : dataUrlToBuffer(input_image);

      // Charm macro (Image[1]) — optional second image
      let charm = null;
      if (input_charm_storage_path || input_charm_image) {
        charm = input_charm_storage_path
          ? await storagePathToBuffer(input_charm_storage_path)
          : dataUrlToBuffer(input_charm_image);
      }

      const images = [
        { buffer: ref.buffer, mime: ref.mime, filename: "reference.png" },
      ];
      if (charm) {
        images.push({ buffer: charm.buffer, mime: charm.mime, filename: "charm_macro.png" });
      }

      outBuf = await callOpenAIImagesEdits({
        apiKey,
        model,
        prompt,
        size,
        quality,
        output_format,
        images,
      });
    }

    await jobRef.set(
      {
        stage: "uploading",
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    const { storagePath, downloadURL, effectiveRunId, effectiveSlot } =
      await uploadPngBufferToStorage({ outBuf, jobId, runId, slotIndex });

    await db.collection(IMAGES_COLL).add({
      runId: effectiveRunId,
      slotIndex: effectiveSlot,
      createdAt: new Date(),
      storagePath,
      downloadURL,
      model,
      prompt,
      traits: body.traits || null,
      jobId,
      kind,
    });

    await jobRef.set(
      {
        status: "done",
        stage: "done",
        storagePath,
        downloadURL,
        finishedAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    return json(202, { ok: true, jobId });
  } catch (err) {
    await jobRef.set(
      {
        status: "error",
        stage: "error",
        error: safeErr(err),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    // Still 202 so the browser doesn’t treat the request as “failed to enqueue”
    return json(202, { ok: false, jobId, error: safeErr(err) });
  }
};