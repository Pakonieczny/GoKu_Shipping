/* netlify/functions/geminiImageProxy-background.js
   Background Function: runs long Gemini image generation/edits without browser/edge inactivity 504s.
   Writes realtime status to Firestore + uploads final PNG to Firebase Storage.
*/

const admin = require("./firebaseAdmin");
// const sharp = require("sharp"); // ensure sharp is installed in package.json
const { initializeFirestore, getFirestore } = require("firebase-admin/firestore");

// Node 18 on Netlify provides fetch/FormData/Blob globally.
// If your build ever lacks fetch, uncomment:
// const fetch = require("node-fetch");

const JOBS_COLL = "ListingGenerator1Jobs";
const IMAGES_COLL = "ListingGenerator1Images";

// -------------------------
// Storage bucket selection
// -------------------------
// If the Admin SDK defaults to a different bucket than the browser (Firebase JS SDK),
// async generation will upload Slot_*.png to the wrong place and the UI will poll forever.
// Force a single bucket handle everywhere (can be overridden by env).
function getBucket() {
  const name =
    process.env.FIREBASE_STORAGE_BUCKET ||
    process.env.GCLOUD_STORAGE_BUCKET ||
    admin.app()?.options?.storageBucket ||
    // Fallback for this project (prevents silent mismatch when options.storageBucket is unset)
    "gokudatabase.firebasestorage.app";
  return admin.storage().bucket(name);
}

// Hard-lock the Gemini image model (ignore any client-provided model)
const GEMINI_IMAGE_MODEL = "gemini-3-pro-image-preview";

const GENERATABLE_CATEGORIES = new Set([
  "Beady_Necklace",
  "Regular_Necklace",
  "Stud_Earrings",
  "Hoop_Earrings",
  "Charms",
  "Bracelets",
]);

// ---- helpers ----

function normalizeCategory(s) {
  return String(s || "").trim();
}

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
  const ALLOWED_INPUT_PREFIXES = [
    "listing-generator-1/Beady_Necklace/",
    "listing-generator-1/Regular_Necklace/",
    "listing-generator-1/Stud_Earrings/",
    "listing-generator-1/Hoop_Earrings/",
    "listing-generator-1/Charms/",
    "listing-generator-1/Bracelets/",
    "listing-generator-1/New_Charms/",
    "listing-generator-1/Completed_Charm/",
    "listing-generator-1/generated/",
  ];

  if (!ALLOWED_INPUT_PREFIXES.some((prefix) => p.startsWith(prefix))) {
    throw new Error("input_storage_path not allowed");
  }

  const bucket = getBucket();
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

async function allocNextSet(activeCategory) {
  const cat = normalizeCategory(activeCategory);
  if (!GENERATABLE_CATEGORIES.has(cat)) throw new Error("activeCategory not generatable");

  const bucket = admin.storage().bucket();
  const prefix = `listing-generator-1/${cat}/Ready_To_List/`;

  // OPTIMIZED: Use delimiter to only fetch "subfolders" (prefixes)
  // This prevents downloading metadata for thousands of files, which causes OOM crashes.
  const [files, nextQuery, apiResponse] = await bucket.getFiles({
    prefix,
    delimiter: "/",
    autoPaginate: false, // We only need the top-level "folders"
  });

  const prefixes = apiResponse?.prefixes || [];
  
  let maxN = 0;
  
  // Prefixes look like: "listing-generator-1/Beady_Necklace/Ready_To_List/Set_1/"
  for (const p of prefixes) {
    const m = p.match(/\/Set_(\d+)\/$/);
    if (m) {
      maxN = Math.max(maxN, Number(m[1]) || 0);
    }
  }

  const setN = maxN + 1;
  const outputBasePath = `listing-generator-1/${cat}/Ready_To_List/Set_${setN}`;
  return { setN, outputBasePath };
}

function assertAllowedOutputBase(base) {
  const b = String(base || "").trim();
  // Must be: listing-generator-1/{Category}/Ready_To_List/Set_N
  if (!/^listing-generator-1\/[^/]+\/Ready_To_List\/Set_\d+$/i.test(b)) {
    throw new Error("output_base_path not allowed");
  }
  return b;
}

async function signedUrlFor(bucketFile) {
  const [url] = await bucketFile.getSignedUrl({
    action: "read",
    expires: Date.now() + 1000 * 60 * 60 * 24 * 7, // 7 days
  });
  return url;
}

// -------------------------
// Firestore (Admin) hardening for serverless
let _db;
function getDb() {
  if (_db) return _db;
  try {
    _db = initializeFirestore(admin.app(), { preferRest: true });
  } catch (e) {
    _db = getFirestore(admin.app());
  }
  return _db;
}
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
function isRetryableFirestoreError(err) {
  const code = err?.code || err?.details;
  const msg = String(err?.message || "").toLowerCase();
  return (
    code === "deadline-exceeded" ||
    code === "resource-exhausted" ||
    code === "unavailable" ||
    code === "aborted" ||
    code === "internal" ||
    msg.includes("deadline") ||
    msg.includes("resource") ||
    msg.includes("unavailable")
  );
}
async function firestoreRetry(fn, label = "firestore") {
  let lastErr;
  for (let attempt = 1; attempt <= 8; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (attempt === 8 || !isRetryableFirestoreError(err)) throw err;
      const backoff = Math.min(6000, 250 * (2 ** (attempt - 1))) + Math.floor(Math.random() * 250);
      console.log(`[${label}] retry ${attempt} after ${backoff}ms`, safeErr(err));
      await sleep(backoff);
    }
  }
  throw lastErr;
}

// Deterministic final framing: crop -> resize back to original size.
async function applyFinalFrameZoomIfNeeded(buf, postprocess = {}) {
  const z = Number(postprocess?.finalFrameZoom);
  if (!Number.isFinite(z) || z <= 1.0001) return buf;

  let sharp;
  try { sharp = require("sharp"); }
  catch (_) {
    throw new Error("finalFrameZoom requires the 'sharp' dependency in your top-level package.json.");
  }

  const ax = clampNumber(postprocess?.anchorX, 0, 1, 0.5);
  const ay = clampNumber(postprocess?.anchorY, 0, 1, 0.45);

  const meta = await sharp(buf).metadata();
  const w = meta?.width || 0;
  const h = meta?.height || 0;
  if (!w || !h) return buf;

  const cropW = Math.max(1, Math.round(w / z));
  const cropH = Math.max(1, Math.round(h / z));
  let left = Math.round(w * ax - cropW / 2);
  let top  = Math.round(h * ay - cropH / 2);
  left = Math.max(0, Math.min(w - cropW, left));
  top  = Math.max(0, Math.min(h - cropH, top));

  return await sharp(buf)
    .extract({ left, top, width: cropW, height: cropH })
    .resize(w, h, { kernel: "lanczos3" })
    .png()
    .toBuffer();
}

function filenameForMime(base, mime) {
  const m = String(mime || "").toLowerCase();
  const ext =
    m.includes("jpeg") || m.includes("jpg") ? "jpg" :
    m.includes("png") ? "png" :
    (m.split("/")[1] || "bin");
  return `${base}.${ext}`;
}

async function callGeminiImagesEdits({
  apiKey,
  model,
  prompt,
  size,
  quality,
  output_format,
  images, 
}) {
  return callGeminiGenerateContentImage({
    apiKey,
    model,
    prompt,
    size,
    images,
  });
}

async function callGeminiImagesGenerations({
  apiKey,
  model,
  prompt,
  size,
  quality,
  output_format,
}) {
  return callGeminiGenerateContentImage({
    apiKey,
    model,
    prompt,
    size,
    images: [],
  });
}

function sizeToAspectRatio(size = "2048x2048") {
  const m = /^(\d+)\s*x\s*(\d+)$/.exec(String(size || "").trim());
  if (!m) return "1:1";
  const w = Number(m[1]), h = Number(m[2]);
  if (!Number.isFinite(w) || !Number.isFinite(h) || w <= 0 || h <= 0) return "1:1";
  if (Math.abs(w - h) < 2) return "1:1";
  const gcd = (a,b)=> b ? gcd(b, a%b) : a;
  const g = gcd(w, h);
  return `${Math.round(w/g)}:${Math.round(h/g)}`;
}

function stripUndefined(obj) {
  if (!obj || typeof obj !== "object") return obj;
  const out = Array.isArray(obj) ? [] : {};
  for (const [k, v] of Object.entries(obj)) {
    if (v === undefined) continue;
    out[k] = stripUndefined(v);
  }
  return out;
}

async function callGeminiGenerateContentImage({
  apiKey,
  model,
  prompt,
  size,
  images,
}) {
  const geminiModel =
    String(model || "gemini-3-pro-image-preview").trim() ||
    "gemini-3-pro-image-preview";
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${geminiModel}:generateContent`;

  const m = /^(\d+)\s*x\s*(\d+)$/.exec(String(size || "").trim());
  const wantW = m ? Number(m[1]) : null;
  const wantH = m ? Number(m[2]) : null;
  const wantAR = sizeToAspectRatio(size);

  const promptText =
    `${String(prompt || "").trim()}\\n\\n` +
    `OUTPUT (NON-NEGOTIABLE): Return a photorealistic ${wantAR} image. ` +
    (wantW && wantH ? `Exact size ${wantW}x${wantH}. ` : "") +
    `Return an image suitable for a product photo.`;

  const parts = [{ text: promptText }];
  for (const img of images || []) {
    parts.push({
      inline_data: {
        mime_type: img?.mime || "image/png",
        data: Buffer.from(img?.buffer || Buffer.alloc(0)).toString("base64"),
      },
    });
  }

  const body = stripUndefined({
    contents: [{ role: "user", parts }],
    generationConfig: {
      responseModalities: ["TEXT", "IMAGE"],
    },
  });

  const resp = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-goog-api-key": apiKey,
    },
    body: JSON.stringify(body),
  });

  const raw = await resp.text().catch(() => "");
  let data = null;
  try { data = raw ? JSON.parse(raw) : null; } catch { data = null; }

  if (!resp.ok) {
    const msg =
      data?.error?.message ||
      raw ||
      `Gemini generateContent failed with HTTP ${resp.status} (empty body)`;
    throw new Error(msg);
  }
  const partsOut = data?.candidates?.[0]?.content?.parts || [];

  const imgPart =
    partsOut.find((p) => p?.inline_data?.data) ||
    partsOut.find((p) => p?.inlineData?.data) ||
    null;

  const b64 = imgPart?.inline_data?.data || imgPart?.inlineData?.data;
  if (!b64) {
    const textOnly = partsOut
      .map((p) => p?.text)
      .filter(Boolean)
      .join("\\n")
      .slice(0, 600);
    throw new Error(
      `Gemini response missing inline_data image payload. Text: ${
        textOnly || "(none)"
      }`
    );
  }

  let outBuf = Buffer.from(b64, "base64");

  // Normalize to PNG + requested size
  try {
    let sharp;
    try { sharp = require("sharp"); } catch(_) {}
    if (sharp) {
      const img = sharp(outBuf);
      const meta = await img.metadata();
      const needResize =
        wantW && wantH && (meta?.width !== wantW || meta?.height !== wantH);
      if (needResize) {
        outBuf = await img.resize(wantW, wantH, { fit: "cover" }).png().toBuffer();
      } else {
        outBuf = await img.png().toBuffer();
      }
    }
  } catch (_) {
    // If sharp fails or not present, still return raw bytes.
  }

  return outBuf;
}

 function newDownloadToken() {
   return globalThis.crypto?.randomUUID
     ? globalThis.crypto.randomUUID()
     : require("crypto").randomUUID();
 }

 function tokenDownloadURLFor(bucketName, storagePath, token) {
   const encoded = encodeURIComponent(storagePath).replace(/%2F/g, "%2F");
   return `https://firebasestorage.googleapis.com/v0/b/${bucketName}/o/${encoded}?alt=media&token=${token}`;
 }

async function uploadPngBufferToStorage({ outBuf, jobId, runId, slotIndex, outputBasePath }) {
  const bucket = admin.storage().bucket();
  const token = newDownloadToken();

  const effectiveRunId = runId || `lg1_${Date.now()}`;
  const effectiveSlot = typeof slotIndex === "number" ? slotIndex : null;

  let storagePath;
  if (outputBasePath) {
    const base = String(outputBasePath).trim();
    if (!/^listing-generator-1\/[^/]+\/Ready_To_List\/Set_\d+$/i.test(base)) {
      throw new Error("output_base_path not allowed");
    }
    storagePath = `${base}/Slot_${effectiveSlot + 1}.png`;
  } else {
    // fallback legacy
    storagePath = `listing-generator-1/generated/${effectiveRunId}/slot_${effectiveSlot + 1}.png`;
  }
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
  const downloadURL = tokenDownloadURLFor(bucketName, storagePath, token);

  return { storagePath, downloadURL, effectiveRunId, effectiveSlot };
}

async function uploadPngBufferToSetPath(outBuf, basePath, sIndex, fallbackJobId, fallbackRunId) {
  const bucket = admin.storage().bucket();
  const effectiveSlot = Number.isFinite(Number(sIndex)) && Number(sIndex) >= 0 ? Number(sIndex) : 0;

  if (basePath) {
    const base = assertAllowedOutputBase(basePath);
    const storagePath = `${base}/Slot_${effectiveSlot + 1}.png`;
    const file = bucket.file(storagePath);
     // IMPORTANT: Always attach firebaseStorageDownloadTokens so browser previews can load reliably.
     const token = newDownloadToken();

     await file.save(outBuf, {
       resumable: false,
       contentType: "image/png",
       metadata: {
         metadata: {
           firebaseStorageDownloadTokens: token,
         },
       },
     });

     const downloadURL = tokenDownloadURLFor(bucket.name, storagePath, token);
     return { storagePath, downloadURL, effectiveRunId: fallbackRunId || fallbackJobId || null, effectiveSlot };
  }

  return await uploadPngBufferToStorage({ outBuf, jobId: fallbackJobId, runId: fallbackRunId, slotIndex: effectiveSlot });
}

/**
 * Postprocess pipeline:
 * charm_postscale logic
 */
async function postScaleCharmComposite({
  passABuf,
  baseNoCharmBuf,
  scale,
  targetPx,
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

  // Decode raw RGBA
  const aRaw = await sharp(passABuf).ensureAlpha().raw().toBuffer();
  const bRaw = await sharp(baseNoCharmBuf).ensureAlpha().raw().toBuffer();

  const diffVals = new Uint8Array(width * height);
  for (let i = 0, p = 0; i < aRaw.length; i += 4, p++) {
    const dr = Math.abs(aRaw[i] - bRaw[i]);
    const dg = Math.abs(aRaw[i + 1] - bRaw[i + 1]);
    const db = Math.abs(aRaw[i + 2] - bRaw[i + 2]);
    diffVals[p] = Math.max(dr, dg, db);
  }

  async function buildMaskAndBBox(thr, feather) {
    const mask = Buffer.alloc(width * height);
    for (let p = 0; p < diffVals.length; p++) mask[p] = diffVals[p] > thr ? 255 : 0;

    const maskRaw = await sharp(mask, { raw: { width, height, channels: 1 } })
      .blur(feather)
      .threshold(18)
      .raw()
      .toBuffer();

    let minX = width, minY = height, maxX = -1, maxY = -1;
    let count = 0;

    for (let y = 0; y < height; y++) {
      const row = y * width;
      for (let x = 0; x < width; x++) {
        const v = maskRaw[row + x];
        if (v > 0) {
          count++;
          if (x < minX) minX = x;
          if (y < minY) minY = y;
          if (x > maxX) maxX = x;
          if (y > maxY) maxY = y;
        }
      }
    }

    const found = maxX >= 0;
    const bboxArea = found ? (maxX - minX + 1) * (maxY - minY + 1) : 0;
    const density = found && bboxArea > 0 ? count / bboxArea : 0; 
    return { found, maskRaw, minX, minY, maxX, maxY, count, bboxArea, density };
  }

  const baseThr = clampNumber(diffThreshold, 8, 120, 40);
  const feather = 1;

  const totalPx = width * height;
  const MAX_MASK_PX_RATIO = 0.035; 
  const MAX_BBOX_AREA_RATIO = 0.075; 
  const MAX_BBOX_W_RATIO = 0.35; 
  const MAX_BBOX_H_RATIO = 0.35; 
  const MIN_DENSITY = 0.035; 
  const CENTER_X_MIN = 0.12, CENTER_X_MAX = 0.88; 
  const CENTER_Y_MIN = 0.12, CENTER_Y_MAX = 0.88;

  let chosen = null;
  let best = null; 

  for (let thr = baseThr; thr <= 90; thr += 8) {
    const m = await buildMaskAndBBox(thr, feather);
    if (!m.found) continue;

    if (!best || m.bboxArea < best.bboxArea) best = { ...m, thr };

    const bboxW = (m.maxX - m.minX + 1);
    const bboxH = (m.maxY - m.minY + 1);
    const bboxWR = bboxW / width;
    const bboxHR = bboxH / height;
    const cx = (m.minX + m.maxX) / 2;
    const cy = (m.minY + m.maxY) / 2;
    const okCenter =
      (cx >= width * CENTER_X_MIN && cx <= width * CENTER_X_MAX) &&
      (cy >= height * CENTER_Y_MIN && cy <= height * CENTER_Y_MAX);

    const okMask = m.count <= totalPx * MAX_MASK_PX_RATIO;
    const okBox = m.bboxArea <= totalPx * MAX_BBOX_AREA_RATIO;
    const okW = bboxWR <= MAX_BBOX_W_RATIO;
    const okH = bboxHR <= MAX_BBOX_H_RATIO;
    const okDense = (m.density || 0) >= MIN_DENSITY;

    if (okMask && okBox && okW && okH && okDense && okCenter) {
      chosen = { ...m, thr };
      break;
    }
  }

  if (!chosen) chosen = best;
  if (!chosen || !chosen.found) return passABuf;

  if (chosen.bboxArea > totalPx * 0.25) {
    console.log("[postscale] bbox too large; skipping charm_postscale", {
      bboxArea: chosen.bboxArea,
      totalPx,
      thr: chosen.thr,
    });
    return passABuf;
  }

  let { maskRaw, minX, minY, maxX, maxY } = chosen;

  try {
    const DS = 4;
    const smallW = Math.max(1, Math.round(width / DS));
    const smallH = Math.max(1, Math.round(height / DS));

    const small = await sharp(maskRaw, { raw: { width, height, channels: 1 } })
      .resize(smallW, smallH, { kernel: "nearest" })
      .threshold(1)
      .raw()
      .toBuffer();

    const visited = new Uint8Array(smallW * smallH);
    let bestArea = 0;
    let best = null;

    const qx = new Int32Array(smallW * smallH);
    const qy = new Int32Array(smallW * smallH);

    for (let y = 0; y < smallH; y++) {
      for (let x = 0; x < smallW; x++) {
        const idx = y * smallW + x;
        if (visited[idx]) continue;
        if (small[idx] === 0) { visited[idx] = 1; continue; }

        visited[idx] = 1;
        let head = 0, tail = 0;
        qx[tail] = x; qy[tail] = y; tail++;

        let area = 0;
        let mnx = x, mny = y, mxx = x, mxy = y;

        while (head < tail) {
          const cx = qx[head];
          const cy = qy[head];
          head++;
          area++;
          if (cx < mnx) mnx = cx;
          if (cy < mny) mny = cy;
          if (cx > mxx) mxx = cx;
          if (cy > mxy) mxy = cy;

          const n1 = cx > 0 ? (cy * smallW + (cx - 1)) : -1;
          const n2 = cx + 1 < smallW ? (cy * smallW + (cx + 1)) : -1;
          const n3 = cy > 0 ? ((cy - 1) * smallW + cx) : -1;
          const n4 = cy + 1 < smallH ? ((cy + 1) * smallW + cx) : -1;

          if (n1 >= 0 && !visited[n1] && small[n1]) { visited[n1] = 1; qx[tail] = cx - 1; qy[tail] = cy; tail++; }
          if (n2 >= 0 && !visited[n2] && small[n2]) { visited[n2] = 1; qx[tail] = cx + 1; qy[tail] = cy; tail++; }
          if (n3 >= 0 && !visited[n3] && small[n3]) { visited[n3] = 1; qx[tail] = cx; qy[tail] = cy - 1; tail++; }
          if (n4 >= 0 && !visited[n4] && small[n4]) { visited[n4] = 1; qx[tail] = cx; qy[tail] = cy + 1; tail++; }
        }

        if (area < 12) continue;

        if (area > bestArea) {
          bestArea = area;
          best = { mnx, mny, mxx, mxy };
        }
      }
    }

    if (best) {
      const padSmall = 2;
      const sx1 = Math.max(0, best.mnx - padSmall);
      const sy1 = Math.max(0, best.mny - padSmall);
      const sx2 = Math.min(smallW - 1, best.mxx + padSmall);
      const sy2 = Math.min(smallH - 1, best.mxy + padSmall);

      minX = Math.max(0, Math.floor(sx1 * DS));
      minY = Math.max(0, Math.floor(sy1 * DS));
      maxX = Math.min(width - 1, Math.ceil((sx2 + 1) * DS) - 1);
      maxY = Math.min(height - 1, Math.ceil((sy2 + 1) * DS) - 1);
    }
  } catch (_) {
    // If refinement fails for any reason, keep the original bbox.
  }

  const pad = 6;
  const left = Math.max(0, minX - pad);
  const top = Math.max(0, minY - pad);
  const bboxW = Math.min(width - left, maxX - minX + 1 + pad * 2);
  const bboxH = Math.min(height - top, maxY - minY + 1 + pad * 2);

  const maskPng = await sharp(maskRaw, { raw: { width, height, channels: 1 } })
    .extract({ left, top, width: bboxW, height: bboxH })
    .blur(0.8)
    .png()
    .toBuffer();

  const charmCrop = await sharp(passABuf)
    .extract({ left, top, width: bboxW, height: bboxH })
    .removeAlpha()
    .joinChannel(maskPng)
    .png()
    .toBuffer();

  const tp = Number(targetPx);
  let outW, outH;
  if (Number.isFinite(tp)) {
    const targetH = Math.round(clampNumber(tp, 4, 96, 14));
    const aspect = bboxH > 0 ? (bboxW / bboxH) : 1;
    outH = Math.max(1, targetH);
    outW = Math.max(1, Math.round(outH * aspect));

    if (outW > width) {
      const k = width / outW;
      outW = Math.max(1, Math.floor(outW * k));
      outH = Math.max(1, Math.floor(outH * k));
    }
    if (outH > height) {
      const k = height / outH;
      outW = Math.max(1, Math.floor(outW * k));
      outH = Math.max(1, Math.floor(outH * k));
    }
  } else {
    const s = clampNumber(scale, 0.50, 0.70, 0.65);
    outW = Math.max(1, Math.round(bboxW * s));
    outH = Math.max(1, Math.round(bboxH * s));
  }

  const scaledCharm = await sharp(charmCrop)
    .resize(outW, outH, { kernel: "lanczos3" })
    .sharpen(0.6)
    .png()
    .toBuffer();

  try {
    const aStats = await sharp(scaledCharm).extractChannel(3).stats();
    if (!aStats?.channels?.[0] || aStats.channels[0].max === 0) return passABuf;
  } catch (_) {
    return passABuf;
  }

  const shBlur = clampNumber(shadowBlur, 0, 12, 2);
  const shOp = clampNumber(shadowOpacity, 0, 0.6, 0.28);

  const shadowAlphaRaw = await sharp(scaledCharm)
    .extractChannel(3)
    .blur(shBlur)
    .linear(shOp, 0)
    .raw()
    .toBuffer();

  const shadowLayer = await sharp({
    create: {
      width: outW,
      height: outH,
      channels: 3,
      background: { r: 0, g: 0, b: 0 },
    },
  })
    .joinChannel(shadowAlphaRaw, { raw: { width: outW, height: outH, channels: 1 } })
    .png()
    .toBuffer();

  const anchorX = left + Math.round(bboxW / 2);
  const newLeft = Math.max(0, Math.min(width - outW, Math.round(anchorX - outW / 2)));
  const newTop = Math.max(0, Math.min(height - outH, top));

  const finalBuf = await sharp(baseNoCharmBuf)
    .composite([
      { input: shadowLayer, left: newLeft, top: Math.min(height - outH, newTop + 1), blend: "multiply" },
      { input: scaledCharm, left: newLeft, top: newTop, blend: "over" },
    ])
    .png()
    .toBuffer();

  return finalBuf;
}

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
    model: _clientModel, // ignored
    prompt,
    size = "2048x2048",
    quality = "high",
    output_format = "png",
    input_storage_path,
    input_image,
    input_charm_storage_path,
    input_charm_image,
    remove_prompt,
    postprocess,
    base_storage_path,
    base_image,
    activeCategory,
    output_base_path,
    source_storage_path,
    manifest,
  } = body || {};

  const model = GEMINI_IMAGE_MODEL;

  // ---------- NEW: non-job operations (no jobId required) ----------
  try {
    if (kind === "alloc_set") {
      const { setN, outputBasePath } = await allocNextSet(activeCategory);
      return json(200, { ok: true, setN, outputBasePath });
    }

    // ------------------------------------------------------------
    // NEW: run_set_async
    // - One request kicks off the whole set
    // - Server processes tasks sequentially
    // - Enforces delayMs BETWEEN Gemini calls (prevents burst overload)
    // - Returns immediately (Netlify BG responds 202 anyway)
    // ------------------------------------------------------------
    if (kind === "run_set_async") {
      const cat = normalizeCategory(activeCategory);
      if (!GENERATABLE_CATEGORIES.has(cat)) {
        return json(400, { error: { message: "activeCategory not generatable" } });
      }

      const base = assertAllowedOutputBase(output_base_path);
      const delayMs = clampNumber(body?.delayMs ?? body?.delay_ms ?? 1000, 0, 10000, 1000);
      const tasks = Array.isArray(body?.tasks) ? body.tasks : null;
      if (!tasks || !tasks.length) {
        return json(400, { error: { message: "tasks must be a non-empty array" } });
      }
      if (tasks.length > 8) {
        return json(400, { error: { message: "tasks max length is 8" } });
      }

      const apiKey = process.env.GEMINI_API_KEY;
      if (!apiKey) return json(400, { error: { message: "Missing GEMINI_API_KEY env var" } });

      const runToken =
        globalThis.crypto?.randomUUID ? globalThis.crypto.randomUUID() : require("crypto").randomUUID();

      // ✅ FIX: Await the async work so the function does not exit early.
      await (async () => {
        const bucket = admin.storage().bucket();

        // Only delay between Gemini submissions (edits tasks).
        let firstGemini = true;

        for (const t of tasks) {
          const slot = Number(t?.slotIndex);
          if (!Number.isFinite(slot) || slot < 0) continue;

          try {
            if (String(t?.type) === "copy") {
              const src = String(t?.source_storage_path || "").trim();
              if (!src) throw new Error("copy task missing source_storage_path");
              const dst = `${base}/Slot_${slot + 1}.png`;
              const dstFile = bucket.file(dst);
              await bucket.file(src).copy(dstFile);

              // Ensure browser previews can load: getDownloadURL() relies on firebaseStorageDownloadTokens.
              const token = newDownloadToken();
              await dstFile.setMetadata({ metadata: { firebaseStorageDownloadTokens: token } });
              continue;
            }

            // edits task (Gemini)
            const basePath0 = String(t?.input_storage_path || "").trim();
            const basePath1 = String(t?.input_charm_storage_path || "").trim();
            const promptT = String(t?.prompt || "").trim();
            if (!basePath0) throw new Error("edits task missing input_storage_path");
            if (!basePath1) throw new Error("edits task missing input_charm_storage_path");
            if (!promptT) throw new Error("edits task missing prompt");

            // ✅ throttle: delay BETWEEN Gemini slot submissions
            if (!firstGemini && delayMs > 0) await sleep(delayMs);
            firstGemini = false;

            const img0 = await storagePathToBuffer(basePath0);
            const img1 = await storagePathToBuffer(basePath1);

            let outBuf = await callGeminiImagesEdits({
              apiKey,
              model: GEMINI_IMAGE_MODEL,
              prompt: promptT,
              size: String(t?.size || body?.size || "2048x2048"),
              quality: "high",
              output_format: "png",
              images: [
                { buffer: img0.buffer, mime: img0.mime, filename: filenameForMime("image0", img0.mime) },
                { buffer: img1.buffer, mime: img1.mime, filename: filenameForMime("image1", img1.mime) },
              ],
            });

            outBuf = await applyFinalFrameZoomIfNeeded(outBuf, t?.postprocess || body?.postprocess);
            await uploadPngBufferToSetPath(outBuf, base, slot, null, runToken);
          } catch (err) {
            console.log("[run_set_async] task failed", { runToken, slot, err: safeErr(err) });
            // Keep going so other slots can finish.
          }
        }
      })();

      // Return successful completion (client received 202 long ago)
      return json(200, { ok: true, finished: true, runId: runToken });
    }

    if (kind === "copy_to_slot") {
      const cat = normalizeCategory(activeCategory);
      if (!GENERATABLE_CATEGORIES.has(cat)) return json(400, { error: { message: "activeCategory not generatable" } });

      const src = String(source_storage_path || "").trim();
      if (!src) return json(400, { error: { message: "source_storage_path is required" } });

      const base = assertAllowedOutputBase(output_base_path);
      const effectiveSlot = Number.isFinite(Number(slotIndex)) && Number(slotIndex) >= 0 ? Number(slotIndex) : 0;
      const dst = `${base}/Slot_${effectiveSlot + 1}.png`;

      const bucket = admin.storage().bucket();
       const dstFile = bucket.file(dst);
       await bucket.file(src).copy(dstFile);

       // Ensure a Firebase download token exists on the copied object.
       const token = newDownloadToken();
       await dstFile.setMetadata({ metadata: { firebaseStorageDownloadTokens: token } });
       const downloadURL = tokenDownloadURLFor(bucket.name, dst, token);
      return json(200, { ok: true, storagePath: dst, downloadURL });
    }

   if (kind === "edits") {
      const cat = normalizeCategory(activeCategory);
      if (!GENERATABLE_CATEGORIES.has(cat)) return json(400, { error: { message: "activeCategory not generatable" } });

      const basePath0 = String(input_storage_path || "").trim();
      const basePath1 = String(input_charm_storage_path || "").trim();
      if (!basePath0) return json(400, { error: { message: "input_storage_path is required" } });
      if (!basePath1) return json(400, { error: { message: "input_charm_storage_path is required" } });

      const apiKey = process.env.GEMINI_API_KEY;
      if (!apiKey) return json(400, { error: { message: "Missing GEMINI_API_KEY env var" } });

      const outputBasePath = assertAllowedOutputBase(output_base_path);
      const effectiveSlot = Number.isFinite(Number(slotIndex)) && Number(slotIndex) >= 0 ? Number(slotIndex) : 0;

      const img0 = await storagePathToBuffer(basePath0);
      const img1 = await storagePathToBuffer(basePath1);

      let outBuf = await callGeminiImagesEdits({
        apiKey,
        model,
        prompt,
        size,
        quality,
        output_format,
        images: [
          { buffer: img0.buffer, mime: img0.mime, filename: filenameForMime("image0", img0.mime) },
          { buffer: img1.buffer, mime: img1.mime, filename: filenameForMime("image1", img1.mime) },
        ],
      });

      outBuf = await applyFinalFrameZoomIfNeeded(outBuf, postprocess);

      const saved = await uploadPngBufferToSetPath(outBuf, outputBasePath, effectiveSlot, null, null);
      return json(200, { ok: true, storagePath: saved.storagePath, downloadURL: saved.downloadURL });
    }

    if (kind === "write_manifest") {
      const cat = normalizeCategory(activeCategory);
      if (!GENERATABLE_CATEGORIES.has(cat)) return json(400, { error: { message: "activeCategory not generatable" } });

      const base = assertAllowedOutputBase(output_base_path);
      const bucket = admin.storage().bucket();
      const p = `${base}/manifest.json`;
      const buf = Buffer.from(JSON.stringify(manifest || {}, null, 2), "utf8");
      await bucket.file(p).save(buf, { contentType: "application/json", resumable: false });
      return json(200, { ok: true, storagePath: p });
    }
  } catch (e) {
    return json(400, { ok: false, error: safeErr(e) });
  }

  // ---------- existing job-based operations (jobId required) ----------
  if (!jobId) return json(400, { error: { message: "jobId is required" } });

  const db = getDb();
  const jobRef = db.collection(JOBS_COLL).doc(jobId);

  try {
    await firestoreRetry(
      () =>
        jobRef.set(
          {
            status: "running",
            stage: "starting",
            runId: runId || null,
            slotIndex: typeof slotIndex === "number" ? slotIndex : null,
            kind,
            model,
            clientModel: _clientModel || null,
            activeCategory: activeCategory || null,
            outputBasePath: output_base_path || null,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        ),
      "jobRef.set"
    );

    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) throw new Error("Missing GEMINI_API_KEY env var");

    // -------------------------
    // SPECIAL: charm_postscale
    // -------------------------
    if (kind === "charm_postscale") {
      if (!input_storage_path && !input_image) {
        throw new Error("charm_postscale requires input_storage_path or input_image (Pass A output)");
      }

      await firestoreRetry(
        () =>
          jobRef.set(
            {
              stage: "downloading_inputs",
              updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            },
            { merge: true }
          ),
        "jobRef.set"
      );

      const passA = input_storage_path
        ? await storagePathToBuffer(input_storage_path)
        : dataUrlToBuffer(input_image);

      await firestoreRetry(() => jobRef.set(
        { stage: "removing_charm", updatedAt: admin.firestore.FieldValue.serverTimestamp() },
        { merge: true }
      ), "jobRef.set");

      let rp = String(remove_prompt || "").trim();
      let baseNoCharmBuf;

      if (base_storage_path || base_image) {
        const base = base_storage_path
          ? await storagePathToBuffer(base_storage_path)
          : dataUrlToBuffer(base_image);

        const [mA, mB] = await Promise.all([
          sharp(passA.buffer).metadata(),
          sharp(base.buffer).metadata(),
        ]);

        if (mA?.width && mA?.height && (mA.width !== mB.width || mA.height !== mB.height)) {
          baseNoCharmBuf = await sharp(base.buffer)
            .resize(mA.width, mA.height, { kernel: "lanczos3" })
            .png()
            .toBuffer();
        } else {
          baseNoCharmBuf = base.buffer;
        }
      } else {
        rp = rp || "Remove the pendant charm + jump ring completely...";
        baseNoCharmBuf = await callGeminiImagesEdits({
          apiKey,
          model,
          prompt: rp,
          size,
          quality,
          output_format,
          images: [{ buffer: passA.buffer, mime: passA.mime, filename: "passA.png" }],
        });
      }

      await firestoreRetry(() => jobRef.set(
        { stage: "postprocessing", updatedAt: admin.firestore.FieldValue.serverTimestamp() },
        { merge: true }
      ), "jobRef.set");

      let finalBuf = await postScaleCharmComposite({
        passABuf: passA.buffer,
        baseNoCharmBuf,
        targetPx: postprocess?.targetPx,
        scale: postprocess?.scale,
        shadowOpacity: postprocess?.shadowOpacity,
        shadowBlur: postprocess?.shadowBlur,
        diffThreshold: postprocess?.diffThreshold,
      });

      finalBuf = await applyFinalFrameZoomIfNeeded(finalBuf, postprocess);

      await firestoreRetry(() => jobRef.set(
        { stage: "uploading", updatedAt: admin.firestore.FieldValue.serverTimestamp() },
        { merge: true }
      ), "jobRef.set");

      const { storagePath, downloadURL, effectiveRunId, effectiveSlot } =
      await uploadPngBufferToSetPath(finalBuf, output_base_path, slotIndex, jobId, runId);

      await firestoreRetry(() => db.collection(IMAGES_COLL).add({
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
      }), "images.add");

      await firestoreRetry(
        () =>
          jobRef.set(
            {
              status: "done",
              stage: "done",
              storagePath,
              downloadURL,
              finishedAt: admin.firestore.FieldValue.serverTimestamp(),
              updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            },
            { merge: true }
          ),
        "jobRef.set"
      );

      return json(202, { ok: true, jobId });
    }

    // -------------------------
    // DEFAULT: edits / generations behavior
    // -------------------------
    if (!prompt) return json(400, { error: { message: "prompt is required" } });

    if (kind !== "edits" && kind !== "generations") {
      return json(400, { error: { message: "kind must be 'edits', 'generations', or 'charm_postscale' (or use alloc_set/copy_to_slot/write_manifest)" } });
    }

    await firestoreRetry(
      () =>
        jobRef.set(
          {
            stage: "uploading",
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        ),
      "jobRef.set"
    );

    let outBuf;

    if (kind === "generations") {
      outBuf = await callGeminiImagesGenerations({
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

      const ref = input_storage_path
        ? await storagePathToBuffer(input_storage_path)
        : dataUrlToBuffer(input_image);

      let charm = null;
      if (input_charm_storage_path || input_charm_image) {
        charm = input_charm_storage_path
          ? await storagePathToBuffer(input_charm_storage_path)
          : dataUrlToBuffer(input_charm_image);
      }

      const images = [{ buffer: ref.buffer, mime: ref.mime, filename: filenameForMime("reference", ref.mime) }];

      if (charm) {
        images.push({ buffer: charm.buffer, mime: charm.mime, filename: filenameForMime("charm_macro", charm.mime) });
      }

      outBuf = await callGeminiImagesEdits({
        apiKey,
        model,
        prompt,
        size,
        quality,
        output_format,
        images,
      });
    }

    outBuf = await applyFinalFrameZoomIfNeeded(outBuf, postprocess);

    await firestoreRetry(
      () =>
        jobRef.set(
          {
            stage: "uploading",
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        ),
      "jobRef.set"
    );

    const { storagePath, downloadURL, effectiveRunId, effectiveSlot } =
      await uploadPngBufferToSetPath(outBuf, output_base_path, slotIndex, jobId, runId);

    await firestoreRetry(() => db.collection(IMAGES_COLL).add({
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
      activeCategory: activeCategory || null,
      outputBasePath: output_base_path || null,
    }), "images.add");

    await firestoreRetry(() => jobRef.set(
      {
        status: "done",
        stage: "done",
        storagePath,
        downloadURL,
        finishedAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    ), "jobRef.set");

    return json(202, { ok: true, jobId });
  } catch (err) {
    await firestoreRetry(
      () =>
        jobRef.set(
          {
            status: "error",
            stage: "error",
            error: safeErr(err),
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        ),
      "jobRef.set"
    );

    return json(202, { ok: false, jobId, error: safeErr(err) });
  }
};