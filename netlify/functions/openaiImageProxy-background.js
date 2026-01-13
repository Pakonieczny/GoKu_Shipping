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

function safeErr(err) {
  return {
    message: err?.message || String(err),
    name: err?.name,
    stack: err?.stack,
  };
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
    kind = "edits",
    model,
    prompt,
    input_image,
    size = "512x512",
    quality = "low",
    output_format = "png",
    // optional: pass-through extra fields later if needed
  } = body;

  if (!jobId) return json(400, { error: { message: "Missing jobId" } });
  if (!model) return json(400, { error: { message: "Missing model" } });
  if (!prompt) return json(400, { error: { message: "Missing prompt" } });
  if (!input_image) return json(400, { error: { message: "Missing input_image" } });

  const db = admin.firestore();

  // Initialize job doc ASAP so the UI progress bar can go live.
  const jobRef = db.collection(JOBS_COLL).doc(jobId);

  try {
    await jobRef.set(
      {
        jobId,
        runId: runId || null,
        slotIndex: typeof slotIndex === "number" ? slotIndex : null,
        status: "running",
        stage: "calling_openai",
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        model,
        size,
        quality,
        output_format,
      },
      { merge: true }
    );

    // ---- OpenAI call (Images Edits) ----
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error("Missing OPENAI_API_KEY in Netlify environment variables");

    // Convert input dataURL -> Blob for multipart form
    const { mime, buffer } = dataUrlToBuffer(input_image);
    const imgBlob = new Blob([buffer], { type: mime });

    const form = new FormData();
    form.append("model", model);
    form.append("prompt", prompt);

    // These params are documented for Images API. 
    form.append("size", size);
    form.append("quality", quality);
    form.append("output_format", output_format);

    // images/edits uses "image" (some SDKs call it input_image; API wants multipart field)
    form.append("image", imgBlob, "input.png");

    const url =
      kind === "edits"
        ? "https://api.openai.com/v1/images/edits"
        : "https://api.openai.com/v1/images/generations";

    const resp = await fetch(url, {
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

    await jobRef.set(
      {
        stage: "uploading",
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    // ---- Upload to Firebase Storage ----
    const bucket = admin.storage().bucket(); // uses storageBucket from firebaseAdmin.js init
    const token = globalThis.crypto?.randomUUID
      ? globalThis.crypto.randomUUID()
      : require("crypto").randomUUID();

    const effectiveRunId = runId || `lg1_${Date.now()}`;
    const effectiveSlot = typeof slotIndex === "number" ? slotIndex : null;

    const storagePath = `listing-generator-1/generated/${effectiveRunId}/${jobId}.png`;
    const file = bucket.file(storagePath);

    const outBuf = Buffer.from(b64, "base64");
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

    // Mirror your previous “other app” feed record (you were adding these in the browser).  [oai_citation:1‡Listing_Generator_1.html](sediment://file_00000000d92471f5bb0775c77206a36c)
    await db.collection(IMAGES_COLL).add({
      runId: effectiveRunId,
      slotIndex: effectiveSlot,
      createdAt: new Date(),
      storagePath,
      downloadURL,
      model,
      prompt,
      // traits can be passed in if you want — keep it optional
      traits: body.traits || null,
      jobId,
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

    // Background functions always return quickly; still return something useful for debugging.
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