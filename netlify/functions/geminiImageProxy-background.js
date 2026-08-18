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
function getBucket() {
  const name =
    process.env.FIREBASE_STORAGE_BUCKET ||
    process.env.GCLOUD_STORAGE_BUCKET ||
    admin.app()?.options?.storageBucket ||
    "gokudatabase.firebasestorage.app";
  return admin.storage().bucket(name);
}

// Image model routing is intentionally allowlisted. The browser may choose one
// of these models, but it cannot turn this function into an arbitrary upstream
// proxy. Old preview IDs are normalized so in-flight manifests and saved local
// preferences continue to work after the stable model migration.
const DEFAULT_IMAGE_MODEL = "gemini-3.1-flash-image";
const DEFAULT_RENDER_IMAGE_MODEL = "gemini-3-pro-image";
const IMAGE_MODEL_ALIASES = Object.freeze({
  "gemini-3.1-flash-image-preview": "gemini-3.1-flash-image",
  "gemini-3-pro-image-preview": "gemini-3-pro-image",
  "gpt-5.5": "gpt-image-2",
});
const IMAGE_MODEL_CONFIG = Object.freeze({
  "gemini-3.1-flash-image": Object.freeze({
    id: "gemini-3.1-flash-image",
    provider: "gemini",
    supportsBatch: true,
  }),
  "gemini-3-pro-image": Object.freeze({
    id: "gemini-3-pro-image",
    provider: "gemini",
    supportsBatch: true,
  }),
  "gpt-image-2": Object.freeze({
    id: "gpt-image-2",
    provider: "openai",
    supportsBatch: false,
  }),
});

function resolveImageModel(value) {
  const requested = String(value || DEFAULT_IMAGE_MODEL).trim();
  const normalized = IMAGE_MODEL_ALIASES[requested] || requested;
  const config = IMAGE_MODEL_CONFIG[normalized];
  if (!config) {
    throw new Error(
      `Unsupported image model "${requested}". Allowed: ${Object.keys(IMAGE_MODEL_CONFIG).join(", ")}`
    );
  }
  return config;
}

function preferredCharmRenderModelId() {
  return String(
    process.env.GEMINI_RENDER_IMAGE_MODEL ||
    process.env.GEMINI_CHARM_RENDER_MODEL ||
    process.env.GEMINI_STUDIO_IMAGE_MODEL ||
    DEFAULT_RENDER_IMAGE_MODEL
  ).trim();
}

const CHARM_RENDER_KINDS = new Set([
  "edits",
  "generations",
  "run_set_async",
  "batch_submit",
  "charm_batch_orchestrate",
]);

function effectiveRequestImageModel(kind, requestedModel) {
  const raw = String(requestedModel || "").trim();
  if (raw) return raw;
  return CHARM_RENDER_KINDS.has(String(kind || "").trim())
    ? preferredCharmRenderModelId()
    : DEFAULT_IMAGE_MODEL;
}

function apiKeyForImageModel(config) {
  const key =
    config?.provider === "openai"
      ? process.env.OPENAI_API_KEY
      : process.env.GEMINI_API_KEY;
  if (!key) {
    throw new Error(
      config?.provider === "openai"
        ? "Missing OPENAI_API_KEY env var"
        : "Missing GEMINI_API_KEY env var"
    );
  }
  return key;
}

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
    "listing-generator-1/Charm_Maker/", // ✅ Updated to cover the new structure broadly
    "listing-generator-1/generated/",
    // Custom Charm Studio: customer reference uploads and prior versions.
    // Every studio kind additionally checks the path starts with
    // custom-studio/{their own uid}/, so one customer can never read another's.
    "custom-studio/",
  ];

  if (!ALLOWED_INPUT_PREFIXES.some((prefix) => p.startsWith(prefix))) {
    throw new Error("input_storage_path not allowed: " + p);
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

// ============================================================
// Set-number allocation
// ------------------------------------------------------------
// Atomic Set_N allocator backed by Firestore counter docs.
//
// PROBLEM IT SOLVES: The previous implementation scanned Firebase
// Storage for the highest Set_N folder and returned maxN+1. That
// works only when set folders are physically created before the next
// alloc runs. In batch mode, a submission can reserve 4 set numbers
// minutes before any files are written (Gemini takes hours), and
// Storage doesn't track empty folders. So a second submission run
// before the first collected would see no Set folders, hand out the
// same numbers, and overwrite the first batch on collection.
//
// FIX: Each category has a Firestore document at
//   listingGenerator1/setCounters/{category}
// with field `nextSetN`. allocNextSet runs a transaction that reads
// the current value, increments by 1, and writes back. Strictly
// monotonic across any number of concurrent callers.
//
// BOOTSTRAP: On first ever call for a category, the counter doc
// doesn't exist. We seed it from Storage by scanning for the highest
// existing Set_N and starting from there + 1. This way, deployments
// that already have Set_42 in Storage start the counter at 43.
// ============================================================
const SET_COUNTERS_COLL = "listingGenerator1_setCounters";

async function _seedSetCounter(cat) {
  const bucket = admin.storage().bucket();
  const prefix = `listing-generator-1/${cat}/Ready_To_List/`;
  const [_files, _next, apiResponse] = await bucket.getFiles({
    prefix,
    delimiter: "/",
    autoPaginate: false,
  });
  const prefixes = apiResponse?.prefixes || [];
  let maxN = 0;
  for (const p of prefixes) {
    const m = p.match(/\/Set_(\d+)\/$/);
    if (m) maxN = Math.max(maxN, Number(m[1]) || 0);
  }
  return maxN;
}

async function allocNextSet(activeCategory) {
  const cat = normalizeCategory(activeCategory);
  if (!GENERATABLE_CATEGORIES.has(cat)) throw new Error("activeCategory not generatable");

  const db = getDb();
  const ref = db.collection(SET_COUNTERS_COLL).doc(cat);

  const setN = await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    let current;
    if (snap.exists) {
      current = Number(snap.data()?.nextSetN || 0);
    } else {
      // First call ever for this category. Seed from Storage to avoid
      // colliding with any existing Set folders. We do this OUTSIDE
      // the transaction conceptually, but reading Storage from inside
      // a Firestore transaction is allowed (it's not a Firestore op,
      // just a network call) — the transaction will retry if the
      // create races another caller.
      const seed = await _seedSetCounter(cat);
      current = seed; // counter holds "highest used"; next is +1
    }
    const next = current + 1;
    tx.set(ref, {
      nextSetN: next,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    return next;
  });

  const outputBasePath = `listing-generator-1/${cat}/Ready_To_List/Set_${setN}`;
  return { setN, outputBasePath };
}

function assertAllowedOutputBase(base) {
  const b = String(base || "").trim();
  // Must be: listing-generator-1/{Category}/Ready_To_List/Set_N
  // OR: listing-generator-1/Charm_Maker/Generated_Charm_Sets/Deriv_N
  const isSet = /^listing-generator-1\/[^/]+\/Ready_To_List\/Set_\d+$/i.test(b);
  const isDeriv = /^listing-generator-1\/Charm_Maker\/Generated_Charm_Sets\/Deriv_\d+$/i.test(b); // ✅ Updated Regex

  if (!isSet && !isDeriv) {
    throw new Error("output_base_path not allowed: " + b);
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

// -------------------------
// IMAGE PROVIDER RETRY HELPER (aggressive 6s+ backoff & 10 retries)
// -------------------------
async function callImageWithRetry(fn, label = "image-provider", maxRetries = 10) {
  let lastErr;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const msg = String(err?.message || "").toLowerCase();
      
      // Check specifically for overload/rate-limit signals
      const isOverloaded = 
        Number(err?.status) === 429 ||
        Number(err?.status) === 500 ||
        Number(err?.status) === 502 ||
        Number(err?.status) === 503 ||
        msg.includes("overloaded") || 
        msg.includes("rate limit") ||
        msg.includes("429") || 
        msg.includes("503") ||
        msg.includes("502") || 
        msg.includes("500") || 
        msg.includes("internal error") || 
        msg.includes("resource exhausted");

      if (!isOverloaded || attempt === maxRetries) {
        throw err; // Fatal error or out of retries
      }

      // Aggressive Backoff: 6s, 12s, 24s, 48s... + random jitter
      const backoff = (6000 * Math.pow(2, attempt)) + Math.floor(Math.random() * 2000);
      console.log(`[${label}] Model overloaded/failed (attempt ${attempt + 1}/${maxRetries}), retrying in ${backoff}ms...`, safeErr(err));
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

// ============================================================
// PNG TEXT METADATA — embed per-image design descriptions.
// ------------------------------------------------------------
// The Charm Maker's concept planner produces a tailored merchandising
// description for every generated charm (and a second one for its earring
// twin). The client sends it as body.embed_metadata and this helper writes
// it INTO the PNG itself as standard iTXt chunks (UTF-8, keyword/value),
// inserted right after IHDR. The chunks survive server-side copies
// (approve/move flows), downloads and re-uploads, so any later tool —
// including the Index listing generator — can read the design intent
// straight out of the image file. Pure JS, no dependencies; any failure
// returns the original buffer untouched.
// ============================================================
const _PNG_CRC_TABLE = (() => {
  const t = new Int32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
    t[n] = c;
  }
  return t;
})();
function _pngCrc32(buf) {
  let c = 0xffffffff;
  for (let i = 0; i < buf.length; i++) c = _PNG_CRC_TABLE[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}
function sanitizeEmbedMetadata(raw) {
  if (!raw || typeof raw !== "object") return null;
  const out = {};
  let n = 0;
  for (const [k, v] of Object.entries(raw)) {
    if (n >= 8) break;
    const key = String(k || "").replace(/[^\x20-\x7e]/g, "").slice(0, 79).trim();
    const val = String(v == null ? "" : v).slice(0, 8000).trim();
    if (!key || !val) continue;
    out[key] = val;
    n++;
  }
  return n ? out : null;
}
function embedPngTextMetadata(buf, entries) {
  try {
    const meta = sanitizeEmbedMetadata(entries);
    if (!meta || !Buffer.isBuffer(buf) || buf.length < 33) return buf;
    // PNG signature check
    if (buf.readUInt32BE(0) !== 0x89504e47 || buf.readUInt32BE(4) !== 0x0d0a1a0a) return buf;
    // Insertion point: immediately after the IHDR chunk
    const ihdrLen = buf.readUInt32BE(8);
    const insertAt = 8 + 4 + 4 + ihdrLen + 4;
    if (insertAt >= buf.length) return buf;
    const chunks = [];
    for (const [keyword, value] of Object.entries(meta)) {
      const kw = Buffer.from(keyword, "latin1");
      const txt = Buffer.from(value, "utf8");
      // iTXt: keyword \0 compressionFlag(0) compressionMethod(0)
      //       languageTag \0 translatedKeyword \0 text
      const data = Buffer.concat([kw, Buffer.from([0, 0, 0, 0, 0]), txt]);
      const type = Buffer.from("iTXt", "latin1");
      const len = Buffer.alloc(4); len.writeUInt32BE(data.length, 0);
      const crc = Buffer.alloc(4);
      crc.writeUInt32BE(_pngCrc32(Buffer.concat([type, data])), 0);
      chunks.push(Buffer.concat([len, type, data, crc]));
    }
    if (!chunks.length) return buf;
    return Buffer.concat([buf.slice(0, insertAt), ...chunks, buf.slice(insertAt)]);
  } catch (e) {
    console.warn("[embedPngTextMetadata] skipped:", e?.message || e);
    return buf;
  }
}

// ============================================================
// BACKGROUND & GEOMETRY POLICY — prompt-level, never pixel-level.
// ------------------------------------------------------------
// This function used to run a flood-fill that rewrote every
// frame-connected "background-looking" pixel to #000000. That
// pass could eat bright engravings, halos and thin outlines near
// the frame, so it has been RETIRED per the Charm Maker contract
// (capabilities: destructiveBackgroundFloodFill:false,
// usesBackgroundMasking:false, backgroundEnforcement:
// "native_generation_prompt_preflight_only"). The image buffer is
// now returned untouched; background policy is enforced natively:
//   1. charmPolicyFinalText() appends the policy as the FINAL
//      instruction of the model request (after all role labels);
//   2. auditCharmPromptPreflight() runs a text-only audit of the
//      prompt BEFORE the single image generation.
// ============================================================
async function enforceCharmBackgroundPolicy(buf, policy) {
  return buf;
}

// The backend's final word on charm physique. Appended AFTER the caller's
// prompt AND after the image-role labels, so no earlier instruction (and no
// reference image trait) can override it.
// IMPORTANT — THIS TEXT IS A DELIBERATE ECHO, NOT A NEW DOCTRINE.
// The attachment rules below are restated from the Listing Generator's own
// Charm Maker prompts (the "ONE INTEGRATED PROTRUDING CHARM HOOP" section
// and the earring twin's hoop-removal exception) so that the backend's
// final word can never contradict the operator-authored prompt. If you
// change the attachment doctrine in the HTML, change it here to match —
// otherwise the two will fight and the model will pick one at random.
const CHARM_GEOMETRY_OVERRIDES = Object.freeze({
  flat_integrated_eyelet:
    "FINAL GEOMETRY OVERRIDE (BACKEND-ENFORCED, OVERRIDES EVERYTHING ABOVE): " +
    "The charm is ONE perfectly flat, thin sheet of laser-cut polished metal — a single physically " +
    "connected silhouette rendered 100% FRONT-FACING as a flat front elevation: 0% perspective, zero " +
    "3D volume, zero visible thickness or side surfaces, no vanishing lines, no three-quarter or " +
    "angled 'product shot' view — even for real-world boxy subjects like a suitcase, book or house. " +
    "It MUST have EXACTLY ONE small ring-shaped hanging hoop PROTRUDING above its outer silhouette at " +
    "the top — a flat annulus of the same sheet with one clean round front-facing hole through it, " +
    "cut in one continuous outline with the body, placed directly above the design's center of mass " +
    "so the charm hangs level under real-world gravity. A charm with no protruding hoop is a failed " +
    "image. NEVER solve the attachment with a hole punched inside the charm's body or artwork. The " +
    "hoop is part of the charm's own sheet — never a separate ring. NO jump rings, NO split rings, " +
    "NO bails, NO chains, NO second attachment point of any kind, and nothing threaded through the hoop.",
  flat_no_attachment:
    "FINAL GEOMETRY OVERRIDE (BACKEND-ENFORCED, OVERRIDES EVERYTHING ABOVE): " +
    "The charm is ONE perfectly flat, thin sheet of laser-cut polished metal — a single physically " +
    "connected silhouette rendered 100% FRONT-FACING as a flat front elevation: 0% perspective, zero " +
    "3D volume, zero visible thickness or side surfaces, no vanishing lines, no angled view. It has " +
    "NO hanging attachment whatsoever: the protruding hoop is REMOVED ENTIRELY — ring shape, hole and " +
    "all — and the top edge of the silhouette is re-closed cleanly and smoothly as if the sheet had " +
    "been cut without a hoop. No stump, nub, dome, bump, tab or filled-in hoop remnant may remain. " +
    "No eyelet, no hoop, no hanging hole, no jump ring, no split ring, no bail, no chain.",
});

// Combined final policy text for a request. Returns "" when the request
// declares no charm policies (i.e. every non-Charm-Maker call), so default
// listing-compositor behaviour is byte-for-byte unchanged.
function charmPolicyFinalText({ geometryPolicy, backgroundPolicy } = {}) {
  const parts = [];
  const geo = CHARM_GEOMETRY_OVERRIDES[String(geometryPolicy || "").trim()];
  if (geo) parts.push(geo);
  if (String(backgroundPolicy || "").trim() === "solid_black") {
    parts.push(
      "FINAL BACKGROUND OVERRIDE (BACKEND-ENFORCED, OVERRIDES EVERYTHING ABOVE): " +
      "The ENTIRE background must be pure solid black (#000000), edge to edge — no white, no grey, " +
      "no gradients, no vignette, no reflections on a surface, no transparency. Physical holes cut " +
      "through the charm show pure black through them. Any background shown in any reference image " +
      "carries NO authority. If the background is not pure solid black, the image is a failure."
    );
  }
  return parts.join("\n\n");
}

// ============================================================
// TEXT-ONLY PROMPT PREFLIGHT — the independent audit agent.
// ------------------------------------------------------------
// Runs BEFORE the single image generation on Charm Maker requests
// (any request declaring background_policy, charm_geometry_policy
// or charm_edit_intent). It reviews the operator/UI-built prompt
// against the declared policies and returns a corrected final
// prompt. It produces TEXT ONLY — it can never generate an image —
// and it is strictly best-effort: any failure (missing key, model
// error, malformed reply) falls back to the original prompt so a
// generation is never blocked by its own audit.
// ============================================================
async function auditCharmPromptPreflight({
  prompt,
  backgroundPolicy,
  geometryPolicy,
  editIntent,
  imageCount,
  imageRoles,
}) {
  try {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) return null;
    const basePrompt = String(prompt || "").trim();
    if (!basePrompt) return null;

    const model = String(
      process.env.GEMINI_CHARM_PREFLIGHT_MODEL ||
      process.env.GEMINI_CHARM_CONCEPT_MODEL ||
      DEFAULT_IMAGE_MODEL
    ).trim();

    const contractLines = [];
    if (String(backgroundPolicy || "").trim() === "solid_black") {
      contractLines.push("- The output background must be 100% pure solid black (#000000), edge to edge. The prompt must demand this unambiguously and must not contain anything that invites a white, grey, gradient or transparent background.");
    }
    // Echoes the operator prompt's own attachment doctrine — see the note on
    // CHARM_GEOMETRY_OVERRIDES. Keep these two in step with the HTML.
    if (String(geometryPolicy || "").trim() === "flat_integrated_eyelet") {
      contractLines.push("- The charm must be one flat laser-cut sheet, rendered 100% front-facing with zero perspective and zero 3D volume, whose hanging point is EXACTLY ONE small ring-shaped hoop PROTRUDING above the outer silhouette at the top (a flat annulus of the same continuous sheet, with a round hole), placed directly above the design's center of mass so it hangs level. The prompt must make the hoop MANDATORY, must forbid punching the hanging hole inside the charm's body, and must forbid separate jump rings, split rings, bails, chains and anything threaded through the hoop.");
    }
    if (String(geometryPolicy || "").trim() === "flat_no_attachment") {
      contractLines.push("- The charm must be one flat laser-cut sheet, rendered 100% front-facing with zero perspective and zero 3D volume, with its protruding hanging hoop REMOVED ENTIRELY and the top silhouette re-closed cleanly — no stump, bump, dome or filled-in hoop remnant — and NO attachment hardware at all: no eyelet, no hoop, no hanging hole, no jump ring, no bail, no chain.");
    }
    if (String(editIntent || "").trim() === "remove_jump_ring_only") {
      contractLines.push("- This is a surgical repair: ONLY the SEPARATE jump ring / split ring / extra attachment hardware may be removed. The charm's OWN integrated protruding hoop (part of its continuous silhouette) is design, not hardware — the prompt must require preserving it intact with its open hole. Every other aspect of the charm — subject, silhouette, engravings, cutouts, proportions, gold tone, position, scale — must be preserved 100% identical. The prompt must forbid any other change.");
    }
    if (String(imageRoles || "") === "style_reference") {
      contractLines.push("- The supplied reference image is a material/craft style sample ONLY. The prompt must not instruct the model to copy the reference's subject, silhouette or background.");
    }

    const auditInstruction =
      `You are an independent prompt-audit agent for a jewelry image-generation pipeline. ` +
      `You work in TEXT ONLY and you never produce images.\n\n` +
      `Below is the final prompt about to be sent, once, to an image model along with ` +
      `${Math.max(0, Number(imageCount) || 0)} input image(s).\n\n` +
      `NON-NEGOTIABLE CONTRACT THE PROMPT MUST ENFORCE:\n` +
      (contractLines.length ? contractLines.join("\n") : "- (no additional policies declared)") +
      `\n\nYOUR TASK: return a corrected final version of the prompt that preserves the original ` +
      `design intent and wording wherever possible, strengthens or inserts whatever is needed so the ` +
      `contract above cannot be missed, and removes any internal contradiction that works against the ` +
      `contract. Do not shorten aggressively; do not add commentary; do not change the creative brief.\n\n` +
      `Return ONLY valid JSON in exactly this shape: {"approvedPrompt": "the full corrected prompt"}\n\n` +
      `PROMPT TO AUDIT:\n${basePrompt}`;

    const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent`;
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-goog-api-key": apiKey },
      body: JSON.stringify({
        contents: [{ role: "user", parts: [{ text: auditInstruction }] }],
        generationConfig: {
          responseModalities: ["TEXT"],
          temperature: 0.2,
        },
      }),
    });

    const raw = await resp.text().catch(() => "");
    if (!resp.ok) {
      console.warn("[preflight] audit call failed:", raw.slice(0, 200));
      return null;
    }
    let data = null;
    try { data = raw ? JSON.parse(raw) : null; } catch { data = null; }
    const text = (data?.candidates?.[0]?.content?.parts || [])
      .map((p) => p?.text)
      .filter(Boolean)
      .join("");
    if (!text) return null;

    let parsed = null;
    try {
      parsed = JSON.parse(text);
    } catch {
      const m = /\{[\s\S]*\}/.exec(text);
      if (m) { try { parsed = JSON.parse(m[0]); } catch { parsed = null; } }
    }
    const approved = String(parsed?.approvedPrompt || "").trim();
    // Sanity floor: an audit that returns a stub must never replace the
    // operator's full brief.
    if (approved.length < Math.min(120, Math.floor(basePrompt.length / 3))) return null;
    return approved;
  } catch (err) {
    console.warn("[preflight] audit skipped:", err?.message || err);
    return null;
  }
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
  imageRoles,
  charmGeometryPolicy,
  backgroundPolicy,
}) {
  return callGeminiGenerateContentImage({
    apiKey,
    model,
    prompt,
    size,
    images,
    imageRoles,
    charmGeometryPolicy,
    backgroundPolicy,
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

async function readUpstreamJson(resp, providerLabel) {
  const raw = await resp.text().catch(() => "");
  let data = null;
  try { data = raw ? JSON.parse(raw) : null; } catch { data = null; }
  if (!resp.ok) {
    const requestId = resp.headers?.get?.("x-request-id");
    const message =
      data?.error?.message ||
      data?.message ||
      raw ||
      `${providerLabel} failed with HTTP ${resp.status} (empty body)`;
    const err = new Error(requestId ? `${message} [request ${requestId}]` : message);
    err.status = resp.status;
    throw err;
  }
  if (!data) throw new Error(`${providerLabel} returned an empty or non-JSON response`);
  return data;
}

function openAIImageBufferFromResponse(data) {
  const b64 = data?.data?.[0]?.b64_json;
  if (!b64) throw new Error("OpenAI image response did not contain data[0].b64_json");
  return Buffer.from(b64, "base64");
}

function normalizeOpenAIQuality(value) {
  const v = String(value || "medium").toLowerCase();
  return ["low", "medium", "high", "auto"].includes(v) ? v : "medium";
}

function normalizeOpenAIOutputFormat(value) {
  const v = String(value || "png").toLowerCase();
  return ["png", "jpeg", "webp"].includes(v) ? v : "png";
}

async function callOpenAIImagesEdits({
  apiKey,
  model,
  prompt,
  size,
  quality,
  output_format,
  images,
  imageRoles,
  charmGeometryPolicy,
  backgroundPolicy,
}) {
  const form = new FormData();
  form.append("model", model);
  // Gemini receives explicit role labels as separate multimodal parts. OpenAI
  // receives a multipart edit request, so mirror the Charm Maker's special
  // role vocabularies inside the prompt. Default listing-edit behaviour is
  // unchanged.
  let promptText = String(prompt || "");
  if (imageRoles === "style_reference") {
    promptText = `${promptText.trim()}\n\n${IMAGE_ROLE_LABELS.style_reference.single}\n\n${IMAGE_ROLE_LABELS.style_reference.lock}`;
  } else if (imageRoles === "line_art_style" || imageRoles === "customer_lineart" || imageRoles === "lineart_to_charm") {
    const roles = IMAGE_ROLE_LABELS[imageRoles];
    promptText = (images || []).length >= 2
      ? `${promptText.trim()}\n\n${roles.first}\n\n${roles.second}\n\n${roles.lock}`
      : `${promptText.trim()}\n\n${roles.single}\n\n${roles.lock}`;
  }
  // Backend-enforced final word: geometry + background policy text goes last
  // so it outranks everything, mirroring the Gemini part ordering.
  const policyText = charmPolicyFinalText({ geometryPolicy: charmGeometryPolicy, backgroundPolicy });
  if (policyText) promptText = `${promptText}\n\n${policyText}`;
  form.append("prompt", promptText);
  form.append("size", String(size || "2048x2048"));
  form.append("quality", normalizeOpenAIQuality(quality));
  form.append("output_format", normalizeOpenAIOutputFormat(output_format));
  form.append("n", "1");

  for (const [index, img] of (images || []).entries()) {
    const mime = String(img?.mime || "image/png");
    const filename = img?.filename || filenameForMime(`image${index}`, mime);
    form.append(
      "image[]",
      new Blob([Buffer.from(img?.buffer || Buffer.alloc(0))], { type: mime }),
      filename
    );
  }

  const resp = await fetch("https://api.openai.com/v1/images/edits", {
    method: "POST",
    headers: { Authorization: `Bearer ${apiKey}` },
    body: form,
  });
  return openAIImageBufferFromResponse(
    await readUpstreamJson(resp, "OpenAI images/edits")
  );
}

async function callOpenAIImagesGenerations({
  apiKey,
  model,
  prompt,
  size,
  quality,
  output_format,
}) {
  const resp = await fetch("https://api.openai.com/v1/images/generations", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model,
      prompt: String(prompt || ""),
      size: String(size || "2048x2048"),
      quality: normalizeOpenAIQuality(quality),
      output_format: normalizeOpenAIOutputFormat(output_format),
      n: 1,
    }),
  });
  return openAIImageBufferFromResponse(
    await readUpstreamJson(resp, "OpenAI images/generations")
  );
}

async function callImageModelEdits(options) {
  const config = resolveImageModel(options?.model);
  const apiKey = options?.apiKey || apiKeyForImageModel(config);
  // NOTE: only the Gemini path injects role labels — the OpenAI images/edits
  // endpoint receives the prompt and the images with no added framing — so
  // imageRoles is meaningful for Gemini only and harmless to pass either way.
  return config.provider === "openai"
    ? callOpenAIImagesEdits({ ...options, apiKey, model: config.id })
    : callGeminiImagesEdits({ ...options, apiKey, model: config.id });
}

async function callImageModelGenerations(options) {
  const config = resolveImageModel(options?.model);
  const apiKey = options?.apiKey || apiKeyForImageModel(config);
  return config.provider === "openai"
    ? callOpenAIImagesGenerations({ ...options, apiKey, model: config.id })
    : callGeminiImagesGenerations({ ...options, apiKey, model: config.id });
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

// Role-label vocabularies. IMAGE_ROLE_LABELS.edit is the original wording,
// preserved verbatim so default behaviour cannot drift.
const IMAGE_ROLE_LABELS = {
  edit: {
    first:
      "IMAGE 1 — LOCKED SOURCE TEMPLATE / DESTINATION CANVAS. Preserve its composition and all non-charm content.",
    second:
      "IMAGE 2 — MASTER CHARM / DESIGN TRUTH. Use its exact silhouette, integrated eyelet, cutouts, and engraving topology for the replacement charm only.",
    extra: (n) => `IMAGE ${n} — ADDITIONAL REFERENCE.`,
    single:
      "IMAGE 1 — EDIT TARGET. Apply only the requested edit and preserve all unrelated pixels and composition.",
    lock:
      "FINAL IMAGE-ROLE LOCK: edit IMAGE 1 in place. IMAGE 2 supplies only the replacement charm design. Do not blend the images, redraw the whole template, or copy IMAGE 1's old charm details into the new charm.",
  },
  // Used by the Charm Maker's design step. The reference is a material and
  // craft sample, NOT a canvas and NOT a subject to reproduce.
  style_reference: {
    first:
      "IMAGE 1 — MATERIAL AND CRAFT REFERENCE ONLY. Read from it ONLY: the metal alloy and its exact colour and tone, the surface finish and polish, the way light falls on that metal, the thickness and cleanliness of the laser-cut edges, the depth and weight of the surface engraving, the density of detail per unit area, the way the integrated bail is fused into the body, and the background treatment. It is a physical sample of how this workshop makes jewellery. It is NOT a canvas to edit, NOT a composition to preserve, and NOT the object you are being asked to draw.",
    second:
      "IMAGE 2 — ADDITIONAL MATERIAL AND CRAFT REFERENCE. Same role and same limits as IMAGE 1: material, finish, edge quality, engraving depth, detail density, bail construction and background only. Its subject and silhouette are equally off limits.",
    extra: (n) => `IMAGE ${n} — ADDITIONAL MATERIAL AND CRAFT REFERENCE. Material and finish only.`,
    single:
      "IMAGE 1 — MATERIAL AND CRAFT REFERENCE ONLY. Read from it ONLY: the metal alloy and its exact colour and tone, the surface finish and polish, the way light falls on that metal, the thickness and cleanliness of the laser-cut edges, the depth and weight of the surface engraving, the density of detail per unit area, the way the integrated bail is fused into the body, and the background treatment. It is a physical sample of how this workshop makes jewellery. It is NOT a canvas to edit, NOT a composition to preserve, and NOT the object you are being asked to draw.",
    lock:
      "FINAL IMAGE-ROLE LOCK: you are designing a NEW product, not editing a supplied one. The reference image(s) define material, finish, lighting, background and engraving language ONLY. The SUBJECT, the SILHOUETTE and the OUTLINE come from the written brief above and from nowhere else. Do not trace, mirror, re-pose, re-scale or lightly restyle the object shown in the reference. HARD FAIL: an output a shopper would identify as the same object as the reference. HARD FAIL: an output whose outline could be laid over the reference's outline and broadly match. If your draft resembles the reference's shape, discard it and design the brief's subject from scratch.",
  },
  // Used by the Charm Maker's B&W line-art conversion (Slot 3). IMAGE 1 is
  // the charm whose subject must be redrawn — its silhouette IS the truth.
  // IMAGE 2 is a rendering-style sample only (line weight, fill rules,
  // white background). The default "edit" labels were exactly wrong here:
  // they crowned IMAGE 2 "MASTER CHARM / DESIGN TRUTH — use its exact
  // silhouette", which is how the static style sample's subject (e.g. a
  // turtle) periodically REPLACED the charm being converted.
  // Used by the Custom Charm Studio. The customer's reference is the SUBJECT
  // TRUTH — they chose it and they want it honoured — which is the exact
  // opposite of style_reference (subject forbidden) and of edit (preserve the
  // canvas). On a refinement pass IMAGE 2 is the customer's own current
  // version of the same charm, not a style sample.
  customer_reference: {
    first:
      "IMAGE 1 — THE CUSTOMER'S CHOSEN REFERENCE (SUBJECT TRUTH). This is the object the customer asked to have made into a charm. Its subject, recognizable proportions and defining features are authoritative and must survive into your output. Your job is to TRANSLATE it into this workshop's flat laser-cut charm language — one continuous flat sheet, integrated protruding hoop, sparse engraving, pure black background — not to reproduce the photograph and not to invent a different object.",
    second:
      "IMAGE 2 — THE CUSTOMER'S CURRENT VERSION OF THIS CHARM. This is your own previous output, which the customer has been refining. Keep its silhouette, proportions, hoop placement, engraving and cutouts EXACTLY as they are and change ONLY what the latest customer instruction asks for. It is not a style sample and it is not a second subject.",
    extra: (n) => `IMAGE ${n} — ADDITIONAL CUSTOMER REFERENCE. Same subject, additional angle or detail only.`,
    single:
      "IMAGE 1 — THE CUSTOMER'S CHOSEN REFERENCE (SUBJECT TRUTH). This is the object the customer asked to have made into a charm. Its subject, recognizable proportions and defining features are authoritative and must survive into your output. Your job is to TRANSLATE it into this workshop's flat laser-cut charm language — one continuous flat sheet, integrated protruding hoop, sparse engraving, pure black background — not to reproduce the photograph and not to invent a different object.",
    lock:
      "FINAL IMAGE-ROLE LOCK: the SUBJECT comes from IMAGE 1 and the customer's written instructions, and from nowhere else. Translate that subject into a flat laser-cut charm; do not substitute a different object, and do not simply redraw the photograph with its background, setting, hands, props or depth. When a second image is present it is your own previous version of this same charm: preserve it and apply only the newest instruction. HARD FAIL: an output whose subject a customer would not recognise as the thing they uploaded or chose. HARD FAIL: a refinement that redesigns parts of the charm the customer did not ask to change.",
  },
  // Used by the Custom Charm Studio's DESIGN step. The studio designs in
  // B/W production line art FIRST — the drawing is unambiguous about what is
  // engraved and what is polished — and only renders the metal charm after
  // the customer approves the drawing. IMAGE 1 is the customer's reference,
  // IMAGE 2 (refine) is the previous drawing, IMAGE 3 (when present) is the
  // customer's own hand-drawn markup of that drawing.
  customer_lineart: {
    first:
      "IMAGE 1 — THE CUSTOMER'S CHOSEN REFERENCE (SUBJECT TRUTH). This is the object the customer asked to have made into a charm. Its subject, recognizable proportions and defining features are authoritative and must survive into your output. Your job is to DESIGN that charm and draw it as flat black-and-white production line art on pure white — not to reproduce the photograph, not to invent a different object, and not to render metal.",
    second:
      "IMAGE 2 — THE CUSTOMER'S CURRENT DRAWING OF THIS CHARM. This is your own previous line-art output, which the customer has been refining. Keep its silhouette, proportions, hoop placement, engraving and cutouts EXACTLY as they are and change ONLY what the newest customer instruction — and their markup, if supplied — asks for. It is not a style sample and it is not a second subject.",
    extra: (n) => `IMAGE ${n} — THE CUSTOMER'S OWN MARKUP OF THE CURRENT DRAWING. The customer drew on top of your previous output to point at what they want changed: circles and highlight strokes mark the areas to change, and any text bubbles are their written instructions for those areas. Read it as DIRECTION ONLY — the coloured strokes, highlights and bubbles themselves must NEVER appear in your output.`,
    single:
      "IMAGE 1 — THE CUSTOMER'S CHOSEN REFERENCE (SUBJECT TRUTH). This is the object the customer asked to have made into a charm. Its subject, recognizable proportions and defining features are authoritative and must survive into your output. Your job is to DESIGN that charm and draw it as flat black-and-white production line art on pure white — not to reproduce the photograph and not to invent a different object.",
    lock:
      "FINAL IMAGE-ROLE LOCK: the SUBJECT comes from IMAGE 1 and the customer's written instructions, and from nowhere else. The OUTPUT is always flat B/W production line art on pure white. When IMAGE 2 is present it is the customer's current drawing: preserve it and apply only the newest direction. When a markup image is present it steers WHERE to change — its ink never appears in the output. HARD FAIL: photorealism, metal rendering, colour, shading or a black background. HARD FAIL: a refinement that redesigns parts of the drawing the customer did not ask to change. HARD FAIL: markup strokes, highlights or text bubbles reproduced in the output.",
  },
  // The studio's RENDER step: the approved drawing becomes the charm. This is
  // the Charm Maker's gold→line-art conversion run in REVERSE, so the drawing
  // is the structural truth and the output is the photograph.
  lineart_to_charm: {
    first:
      "IMAGE 1 — THE APPROVED PRODUCTION DRAWING (STRUCTURAL TRUTH). Flat COLOUR-CODED line art of a charm on white: its fills are instructions, and black and blue are different instructions. Manufacture EXACTLY this charm: its outer perimeter, proportions, hanging hoop, every engraving stroke and every cutout are authoritative, 1:1.",
    second:
      "IMAGE 2 — UNUSED. Ignore any additional image.",
    extra: (n) => `IMAGE ${n} — UNUSED. Ignore.`,
    single:
      "IMAGE 1 — THE APPROVED PRODUCTION DRAWING (STRUCTURAL TRUTH). Flat COLOUR-CODED line art of a charm on white: its fills are instructions, and black and blue are different instructions. Manufacture EXACTLY this charm: its outer perimeter, proportions, hanging hoop, every engraving stroke and every cutout are authoritative, 1:1.",
    lock:
      "FINAL IMAGE-ROLE LOCK: this is a 1:1 structural replication, drawing → finished charm. The output's silhouette laid over the drawing's silhouette must match. Nothing is added, nothing is removed, nothing is redesigned, nothing is 'improved'. HARD FAIL: an output whose outline, engraving layout or cutouts differ from the drawing. HARD FAIL: an output that is still line art, a sketch or a flat graphic rather than a photograph of real metal. The GROUND the charm is photographed on is not this block's business — it is set by the presentation instructions, and a plain white ground is correct.",
  },
  line_art_style: {
    first:
      "IMAGE 1 — SUBJECT SOURCE (THE TRUTH). This is the exact object you must redraw. Its silhouette, outline, proportions, pose, features, engravings, cutouts and hanging hoop are authoritative and must all appear in your output, converted to the requested style.",
    second:
      "IMAGE 2 — RENDERING-STYLE SAMPLE ONLY. Read from it ONLY how to draw: the line weight, outline treatment, black-fill rules for thick engravings, cutout handling and the pure white background. Its SUBJECT is strictly OFF LIMITS: whatever object it depicts must NOT appear in your output, in whole, in part, or blended.",
    extra: (n) => `IMAGE ${n} — ADDITIONAL RENDERING-STYLE SAMPLE. Drawing style only; its subject is off limits.`,
    single:
      "IMAGE 1 — SUBJECT SOURCE (THE TRUTH). This is the exact object you must redraw in the style described by the written instructions. Its silhouette, outline, proportions, features, engravings and cutouts are authoritative.",
    lock:
      "FINAL IMAGE-ROLE LOCK: redraw the object from IMAGE 1 in the rendering style demonstrated by IMAGE 2. The output's subject and silhouette come from IMAGE 1 and from nowhere else. HARD FAIL: the output shows IMAGE 2's subject instead of, mixed with, or in addition to IMAGE 1's object. If your draft resembles IMAGE 2's subject in any way, discard it and draw IMAGE 1's object.",
  },
};

async function callGeminiGenerateContentImage({
  apiKey,
  model,
  prompt,
  size,
  images,
  imageRoles,
  charmGeometryPolicy,
  backgroundPolicy,
}) {
  const geminiModel =
    String(model || DEFAULT_IMAGE_MODEL).trim() ||
    DEFAULT_IMAGE_MODEL;
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${geminiModel}:generateContent`;

  // ============================================================
  // 2K HARD-LOCK — every synchronous image generated by this
  // function (Redo button, Standard mode generate, Charm Maker,
  // run_set_async edits tasks, etc.) outputs at 2K. The batch path
  // has its own JSONL builder that's also locked to 2K, so every
  // image-producing surface in the app is consistent. Any `size`
  // string passed by callers (e.g., "2048x2048") is honored only
  // for the post-process resize and the prompt's size hint; the
  // tier sent to Gemini via imageConfig is always 2K.
  // ============================================================
  const sizeKey = "2K";

  // Existing WxH derivation, used for the post-process resize and
  // the in-prompt size hint. Default 2048×2048 when no size string
  // was passed, so the hint matches the locked tier.
  const m = /^(\d+)\s*x\s*(\d+)$/.exec(String(size || "").trim());
  const wantW = m ? Number(m[1]) : 2048;
  const wantH = m ? Number(m[2]) : 2048;
  const wantAR = sizeToAspectRatio(size) || "1:1";

  // The generic closing line used to always end with "suitable for a product
  // photo" — a nudge toward studio-white ecommerce backgrounds that directly
  // fought the Charm Maker's solid-black policy. When a background policy is
  // declared, the closing line enforces it instead of contradicting it.
  const closingLine =
    String(backgroundPolicy || "").trim() === "solid_black"
      ? `The ENTIRE background must be pure solid black (#000000), edge to edge.`
      : `Return an image suitable for a product photo.`;

  const promptText =
    `${String(prompt || "").trim()}\n\n` +
    `OUTPUT (NON-NEGOTIABLE): Return a photorealistic ${wantAR} image. ` +
    `Exact size ${wantW}x${wantH}. ` +
    closingLine;

  const imageInputs = Array.isArray(images) ? images : [];
  // Unknown values fall back to "edit" so a typo can never silently unlock the
  // compositor's guarantees.
  const roleSet = IMAGE_ROLE_LABELS[imageRoles] || IMAGE_ROLE_LABELS.edit;

  const parts = [{ text: promptText }];
  for (const [index, img] of imageInputs.entries()) {
    // Explicit role labels prevent multi-image edit models from blending
    // the source template with the master charm or reversing their jobs.
    // Every two-image Listing Generator request uses this same ordering:
    // image 1 = destination/template, image 2 = master charm.
    if (imageInputs.length >= 2) {
      parts.push({
        text:
          index === 0
            ? roleSet.first
            : index === 1
              ? roleSet.second
              : roleSet.extra(index + 1),
      });
    } else {
      parts.push({ text: roleSet.single });
    }

    // Gemini will hard-crash if passed application/octet-stream.
    // Force a valid image MIME type so the API attempts to process the buffer.
    let safeMime = img?.mime || "image/png";
    if (safeMime.includes("octet-stream") || !safeMime.startsWith("image/")) {
      safeMime = "image/png"; 
    }

    parts.push({
      inline_data: {
        mime_type: safeMime,
        data: Buffer.from(img?.buffer || Buffer.alloc(0)).toString("base64"),
      },
    });
  }
  // The edit vocabulary only emits its lock for multi-image requests (a
  // single-image edit needs no disambiguation). The style_reference vocabulary
  // emits it always: with one reference image the "do not reproduce this
  // object" instruction is the entire point, and it must be last so it carries
  // the most weight.
  if (imageInputs.length >= 2 || roleSet === IMAGE_ROLE_LABELS.style_reference) {
    parts.push({ text: roleSet.lock });
  }

  // Backend-enforced FINAL word: charm geometry + background policy. Pushed
  // after every role label and lock so nothing above it — including reference
  // image traits — can override it. Empty (and absent) for all non-Charm-Maker
  // requests.
  const policyText = charmPolicyFinalText({ geometryPolicy: charmGeometryPolicy, backgroundPolicy });
  if (policyText) parts.push({ text: policyText });

  const body = stripUndefined({
    contents: [{ role: "user", parts }],
    generationConfig: {
      responseModalities: ["TEXT", "IMAGE"],
      // imageConfig.imageSize: "2K" tells Gemini to natively produce
      // a 2048×2048 image. Without this, the model uses its default
      // (~1024 area) and post-process upscaling has to compensate,
      // which loses fidelity. Same field/structure the batch JSONL
      // builder uses (see buildBatchJsonlLine).
      imageConfig: { imageSize: sizeKey },
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
      .join("\n")
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

function extractJsonObject(text) {
  const raw = String(text || "").trim();
  if (!raw) throw new Error("Charm concept planner returned no text");
  try { return JSON.parse(raw); } catch (_) {}
  const fenced = raw.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenced) {
    try { return JSON.parse(fenced[1]); } catch (_) {}
  }
  const start = raw.indexOf("{");
  const end = raw.lastIndexOf("}");
  if (start >= 0 && end > start) {
    try { return JSON.parse(raw.slice(start, end + 1)); } catch (_) {}
  }
  throw new Error("Charm concept planner returned invalid JSON");
}

function shortPlannerText(value, max = 500) {
  return String(value == null ? "" : value)
    .replace(/[\u0000-\u001f\u007f]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, max);
}

function normalizeCharmConceptPlan(raw, requestedCount) {
  const referenceSubject = shortPlannerText(raw?.referenceSubject, 180);
  const buyerProfile = shortPlannerText(raw?.buyerProfile, 500);
  const purchaseSignals = Array.isArray(raw?.purchaseSignals)
    ? raw.purchaseSignals.map((v) => shortPlannerText(v, 180)).filter(Boolean).slice(0, 6)
    : [];
  const sourceConcepts = Array.isArray(raw?.concepts) ? raw.concepts : [];
  const concepts = [];
  const seen = new Set();

  for (const source of sourceConcepts) {
    const title = shortPlannerText(source?.title, 100);
    const subject = shortPlannerText(source?.subject, 240);
    const key = `${title}|${subject}`.toLowerCase();
    if (!title || !subject || seen.has(key)) continue;
    seen.add(key);
    concepts.push({
      rank: concepts.length + 1,
      title,
      subject,
      buyerConnection: shortPlannerText(source?.buyerConnection, 420),
      conversionRationale: shortPlannerText(source?.conversionRationale, 420),
      silhouetteBrief: shortPlannerText(source?.silhouetteBrief, 500),
      engravingBrief: shortPlannerText(source?.engravingBrief, 500),
      confidence: Math.round(clampNumber(source?.confidence, 0, 100, 70)),
    });
    if (concepts.length >= requestedCount) break;
  }

  if (!referenceSubject || concepts.length < requestedCount) {
    throw new Error(
      `Charm concept planner returned ${concepts.length}/${requestedCount} usable concepts`
    );
  }
  return {
    version: 1,
    referenceSubject,
    buyerProfile,
    purchaseSignals,
    concepts,
  };
}

async function planComplementaryCharmConcepts({ sourceStoragePath, count }) {
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey) throw new Error("Missing GEMINI_API_KEY env var");

  const requestedCount = Math.round(clampNumber(count, 1, 8, 1));
  const model = String(
    process.env.GEMINI_CHARM_CONCEPT_MODEL || DEFAULT_IMAGE_MODEL
  ).trim();
  const source = await storagePathToBuffer(sourceStoragePath);
  let safeMime = source.mime || "image/png";
  if (!safeMime.startsWith("image/") || safeMime.includes("octet-stream")) {
    safeMime = "image/png";
  }

  const planningPrompt = `CHARM PRODUCT CONCEPT PLANNER

The supplied image is a proven bestselling jewelry charm. Analyze it as market evidence, then choose ${requestedCount} NEW complementary charm concept${requestedCount === 1 ? "" : "s"} most likely to appeal to substantially the same buyer.

This is concept selection, not image editing. The new products must extend the bestseller's audience without replicating the existing SKU.

SELECTION LOGIC — rank concepts using all five factors:
1. Same-buyer affinity (40%): shared identity, hobby, emotion, gifting occasion, collecting behavior, or symbolic meaning.
2. Commercial/gift appeal (20%): evergreen, emotionally legible, easy to search for and easy to give.
3. Tiny-charm recognition (15%): instantly recognizable as an approximately 8–15 mm flat charm.
4. Manufacturability (15%): one-piece laser-cut sheet metal, robust outline, limited strategic engraving, no fragile floating parts.
5. Novelty versus the reference (10%): a genuinely different named subject and materially different silhouette.

ADJACENCY RULE:
- Stay one strong step from the reference, not merely in the same broad category.
- Example: for a cardinal bird, consider another highly collectible garden bird or a closely linked bird-lover symbol; do not make another cardinal, a re-posed cardinal, or a generic copy of the same bird.
- Prefer concrete, visually distinctive subjects over vague decorative variations.
- Each returned concept must be different from the reference and from every other returned concept.

ABSOLUTE EXCLUSIONS:
- Never select the exact subject/species/object shown in the reference.
- Never propose a mirrored, re-posed, re-scaled, simplified, embellished, baby, pair, family, or near-duplicate version of the same subject.
- No words, initials, dates, numbers, logos, copyrighted characters, or trademarked brand shapes.
- Do not claim access to outside sales data. Your confidence is an informed product-fit estimate based on the proven reference and the rubric above.

Return ONLY valid JSON in exactly this shape:
{
  "referenceSubject": "specific identification of the source charm",
  "buyerProfile": "concise description of who buys it and why",
  "purchaseSignals": ["signal 1", "signal 2", "signal 3"],
  "concepts": [
    {
      "title": "short operator-facing concept name",
      "subject": "precise new subject and pose for the image model",
      "buyerConnection": "why the same buyer would want this alongside the bestseller",
      "conversionRationale": "why this should work commercially",
      "silhouetteBrief": "distinctive one-piece outer contour, materially unlike the reference",
      "engravingBrief": "only the minimum internal engraving needed for recognition",
      "confidence": 0
    }
  ]
}

The concepts array must contain exactly ${requestedCount} items in descending commercial-confidence order.`;

  const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent`;
  const resp = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-goog-api-key": apiKey,
    },
    body: JSON.stringify({
      contents: [{
        role: "user",
        parts: [
          { text: planningPrompt },
          { inline_data: { mime_type: safeMime, data: source.buffer.toString("base64") } },
        ],
      }],
      generationConfig: {
        responseModalities: ["TEXT"],
        temperature: 0.35,
      },
    }),
  });

  const raw = await resp.text().catch(() => "");
  let data = null;
  try { data = raw ? JSON.parse(raw) : null; } catch (_) {}
  if (!resp.ok) {
    throw new Error(
      data?.error?.message || raw || `Gemini charm concept planning failed with HTTP ${resp.status}`
    );
  }
  const text = (data?.candidates?.[0]?.content?.parts || [])
    .map((part) => part?.text)
    .filter(Boolean)
    .join("\n");
  return normalizeCharmConceptPlan(extractJsonObject(text), requestedCount);
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
    const isSet = /^listing-generator-1\/[^/]+\/Ready_To_List\/Set_\d+$/i.test(base);
    const isDeriv = /^listing-generator-1\/Charm_Maker\/Generated_Charm_Sets\/Deriv_\d+$/i.test(base); // ✅ Updated Regex
    if (!isSet && !isDeriv) {
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

// ============================================================
// Gemini Batch API helpers (Listing Generator)
// Reference: https://ai.google.dev/gemini-api/docs/batch-api
//
// These helpers are strictly additive. The synchronous (non-batch)
// pipeline elsewhere in this file is untouched. Batch-related kinds
// (batch_submit, batch_status, batch_collect, batch_list, batch_cancel)
// live in their own block in exports.handler below.
// ============================================================

const BATCHES_COLL = "ListingGenerator1Batches";
// Server-side Charm Maker batch orchestrations (tab-independent pipeline).
const ORCH_COLL = "ListingGenerator1CharmOrchestrations";
const GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta";
const GEMINI_UPLOAD_BASE = "https://generativelanguage.googleapis.com/upload/v1beta";

// Build one JSONL request line for the Batch API. Encodes both reference
// images as inline_data (base64). responseModalities: ["IMAGE"] saves a
// few output tokens by skipping the model's default text preamble.
// imageConfig.imageSize: "2K" is the supported way to lock 2048x2048
// output per Google's image-generation docs.
function buildBatchJsonlLine(key, prompt, refMime, refBase64, charmMime, charmBase64, imageSize, opts) {
  const sizeKey = String(imageSize || "2K").toUpperCase();
  const safeRefMime = (refMime && refMime.startsWith("image/")) ? refMime : "image/png";
  const safeCharmMime = (charmMime && charmMime.startsWith("image/")) ? charmMime : "image/png";
  const o = opts || {};
  const imageRoles = String(o.imageRoles || "").trim();
  const backgroundPolicy = String(o.backgroundPolicy || "").trim();
  const geometryPolicy = String(o.geometryPolicy || "").trim();

  // Mirror the prompt augmentation Standard mode uses in
  // callGeminiGenerateContentImage(). Without this suffix, Gemini
  // may reframe or change the aspect ratio of the output, which in
  // turn causes the charm to appear at the wrong scale relative to
  // the reference image. Standard and Batch modes must produce the
  // same output for the same inputs.
  // Pixel-size hint: at 2K we tell the model 2048x2048; at 1K, 1024x1024.
  const sizeHint = sizeKey === "1K" ? "1024x1024"
                 : sizeKey === "2K" ? "2048x2048"
                 : sizeKey === "4K" ? "4096x4096"
                 : "2048x2048";
  // Same closing-line swap Standard mode performs: when the caller declares
  // a solid-black background, the generic "product photo" line (a white-
  // studio nudge) is replaced by the black-background mandate.
  const closingLine = backgroundPolicy === "solid_black"
    ? `The ENTIRE background must be pure solid black (#000000), edge to edge.`
    : `Return an image suitable for a product photo.`;
  const augmentedPrompt =
    `${String(prompt || "").trim()}\n\n` +
    `OUTPUT (NON-NEGOTIABLE): Return a photorealistic 1:1 image. ` +
    `Exact size ${sizeHint}. ` +
    closingLine;

  // Part assembly mirrors callGeminiGenerateContentImage's ordering for the
  // requested role vocabulary, so Standard and Batch produce the same output
  // for the same inputs.
  //   • default (listing compositor, two images): byte-identical to the
  //     original hardcoded "edit" labels.
  //   • "style_reference" (Charm Maker base charm, one image): reference is
  //     a material/craft sample, subject comes from the brief; the backend
  //     geometry/background override text goes last as the final word.
  let parts;
  if (imageRoles === "style_reference") {
    parts = [
      { text: augmentedPrompt },
      { text: IMAGE_ROLE_LABELS.style_reference.single },
      { inline_data: { mime_type: safeRefMime, data: refBase64 } },
      { text: IMAGE_ROLE_LABELS.style_reference.lock },
    ];
  } else {
    parts = [
      { text: augmentedPrompt },
      {
        text:
          "IMAGE 1 — LOCKED SOURCE TEMPLATE / DESTINATION CANVAS. Preserve its composition and all non-charm content.",
      },
      { inline_data: { mime_type: safeRefMime, data: refBase64 } },
      {
        text:
          "IMAGE 2 — MASTER CHARM / DESIGN TRUTH. Use its exact silhouette, integrated eyelet, cutouts, and engraving topology for the replacement charm only.",
      },
      { inline_data: { mime_type: safeCharmMime, data: charmBase64 } },
      {
        text:
          "FINAL IMAGE-ROLE LOCK: edit IMAGE 1 in place. IMAGE 2 supplies only the replacement charm design. Do not blend the images, redraw the whole template, or copy IMAGE 1's old charm details into the new charm.",
      },
    ];
  }

  const policyText = charmPolicyFinalText({ geometryPolicy, backgroundPolicy });
  if (policyText) parts.push({ text: policyText });

  return {
    key,
    request: {
      contents: [{
        role: "user",
        parts,
      }],
      generation_config: {
        // Match Standard mode's modalities exactly. The model may
        // adjust framing/scaling decisions when text output is
        // disallowed, which contributed to charm-size mismatches.
        responseModalities: ["TEXT", "IMAGE"],
        imageConfig: { imageSize: sizeKey },
      },
    },
  };
}

// Upload a JSONL file via the Files API using the resumable protocol
// (the only protocol Google documents for the Batch flow). Returns the
// `files/abc123` resource name.
async function uploadJsonlToGeminiFiles(apiKey, jsonlData, displayName) {
  // jsonlData may be a Buffer (preferred — avoids string→bytes
  // re-encoding inside fetch) or a string. We compute byte length
  // accordingly so the resumable headers are accurate either way.
  const isBuffer = Buffer.isBuffer(jsonlData);
  const bytes = isBuffer ? jsonlData.length : Buffer.byteLength(jsonlData, "utf8");

  const startResp = await fetch(`${GEMINI_UPLOAD_BASE}/files`, {
    method: "POST",
    headers: {
      "x-goog-api-key": apiKey,
      "X-Goog-Upload-Protocol": "resumable",
      "X-Goog-Upload-Command": "start",
      "X-Goog-Upload-Header-Content-Length": String(bytes),
      "X-Goog-Upload-Header-Content-Type": "application/jsonl",
      "Content-Type": "application/jsonl",
    },
    body: JSON.stringify({ file: { display_name: displayName || "lg1-batch" } }),
  });
  if (!startResp.ok) {
    const t = await startResp.text().catch(() => "");
    throw new Error(`Files API resumable-start failed: HTTP ${startResp.status} ${t.slice(0, 400)}`);
  }
  const uploadUrl = startResp.headers.get("x-goog-upload-url");
  if (!uploadUrl) throw new Error("Files API did not return x-goog-upload-url header");

  const uploadResp = await fetch(uploadUrl, {
    method: "POST",
    headers: {
      "Content-Length": String(bytes),
      "X-Goog-Upload-Offset": "0",
      "X-Goog-Upload-Command": "upload, finalize",
    },
    body: jsonlData,
  });
  if (!uploadResp.ok) {
    const t = await uploadResp.text().catch(() => "");
    throw new Error(`Files API upload-finalize failed: HTTP ${uploadResp.status} ${t.slice(0, 400)}`);
  }
  const data = await uploadResp.json().catch(() => ({}));
  const name = data?.file?.name;
  if (!name) throw new Error("Files API upload returned no file.name");
  return name;
}

async function createGeminiBatchJob(apiKey, model, fileName, displayName) {
  const url = `${GEMINI_BASE}/models/${model}:batchGenerateContent`;
  const body = {
    batch: {
      display_name: displayName || "lg1-batch",
      input_config: { file_name: fileName },
    },
  };
  const resp = await fetch(url, {
    method: "POST",
    headers: { "x-goog-api-key": apiKey, "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`Batch create failed: HTTP ${resp.status} ${text.slice(0, 600)}`);
  }
  let parsed;
  try { parsed = JSON.parse(text); }
  catch { throw new Error(`Batch create returned non-JSON: ${text.slice(0, 200)}`); }
  // Returns a long-running operation; the actual batch name is at
  // .name (e.g., "batches/abc123") OR within .metadata. Per the docs,
  // the top-level .name is the operation/batch name we use for polling.
  const batchName = parsed?.name;
  if (!batchName) throw new Error(`Batch create returned no name: ${text.slice(0, 400)}`);
  return { batchName, raw: parsed };
}

async function getGeminiBatchJob(apiKey, batchName) {
  // batchName is "batches/abc123" — keep it as-is in the URL.
  const resp = await fetch(`${GEMINI_BASE}/${batchName}`, {
    method: "GET",
    headers: { "x-goog-api-key": apiKey, "Content-Type": "application/json" },
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`Batch get failed: HTTP ${resp.status} ${text.slice(0, 400)}`);
  }
  const parsed = JSON.parse(text);

  // Normalize state names. Google's batch API returns BATCH_STATE_*
  // strings; our code and Firestore documents use JOB_STATE_*. Map at
  // the source so all downstream logic compares against JOB_STATE_*.
  // Done in-place so state appears in both .metadata.state and .state.
  const norm = (s) => {
    if (!s || typeof s !== "string") return s;
    if (s.startsWith("BATCH_STATE_")) return "JOB_STATE_" + s.slice("BATCH_STATE_".length);
    return s;
  };
  if (parsed?.metadata?.state) parsed.metadata.state = norm(parsed.metadata.state);
  if (parsed?.state) parsed.state = norm(parsed.state);
  return parsed;
}

async function cancelGeminiBatchJob(apiKey, batchName) {
  const resp = await fetch(`${GEMINI_BASE}/${batchName}:cancel`, {
    method: "POST",
    headers: { "x-goog-api-key": apiKey, "Content-Type": "application/json" },
    body: "{}",
  });
  if (!resp.ok) {
    const t = await resp.text().catch(() => "");
    throw new Error(`Batch cancel failed: HTTP ${resp.status} ${t.slice(0, 200)}`);
  }
  return true;
}

// Stream the result JSONL line-by-line instead of loading the entire
// (potentially 2GB+) file into memory. Returns an async generator that
// yields each JSON-parsed line. Throws on non-OK HTTP. Empty/blank
// lines and parse errors are silently skipped (mirrors the older code).
async function* streamGeminiResultLines(apiKey, fileName) {
  const url = `https://generativelanguage.googleapis.com/download/v1beta/${fileName}:download?alt=media`;
  const resp = await fetch(url, {
    method: "GET",
    headers: { "x-goog-api-key": apiKey },
  });
  if (!resp.ok) {
    const t = await resp.text().catch(() => "");
    throw new Error(`Result file download failed: HTTP ${resp.status} ${t.slice(0, 400)}`);
  }
  if (!resp.body) {
    // Fallback for environments without ReadableStream support.
    const text = await resp.text();
    for (const raw of text.split("\n")) {
      const line = raw.trim();
      if (!line) continue;
      try { yield JSON.parse(line); } catch { /* skip */ }
    }
    return;
  }

  // The stream gives us Uint8Array chunks. We accumulate a small text
  // buffer and emit one parsed JSON object per newline. Crucially we
  // never hold more than the in-flight chunk + the trailing partial
  // line, so memory stays bounded regardless of file size.
  const decoder = new TextDecoder("utf-8");
  const reader = resp.body.getReader();
  let buf = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    let nl;
    while ((nl = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, nl).trim();
      buf = buf.slice(nl + 1);
      if (!line) continue;
      try { yield JSON.parse(line); } catch { /* skip malformed */ }
    }
  }
  buf += decoder.decode(); // flush any trailing bytes
  const tail = buf.trim();
  if (tail) {
    try { yield JSON.parse(tail); } catch { /* skip */ }
  }
}

// Run async tasks with bounded concurrency. Each task receives no
// arguments and returns a promise. Max in-flight = `limit`. Returns
// the array of results in input order. Used by batch_collect to
// upload result PNGs without holding all 600 image buffers in RAM
// at once.
async function runBoundedConcurrent(items, limit, worker) {
  const results = new Array(items.length);
  let nextIdx = 0;
  async function loop() {
    while (true) {
      const i = nextIdx++;
      if (i >= items.length) return;
      try {
        results[i] = await worker(items[i], i);
      } catch (e) {
        results[i] = { __error: e };
      }
    }
  }
  const runners = Array.from({ length: Math.min(limit, items.length) }, loop);
  await Promise.all(runners);
  return results;
}

// Sanitize a Gemini batch name "batches/abc123" → "abc123" for use as
// a Firestore doc id. Firestore disallows "/" in doc ids.
function batchDocIdFromName(batchName) {
  return String(batchName || "").replace(/^batches\//, "").replace(/[^\w-]/g, "_");
}


// ============================================================
// SERVER-SIDE CHARM MAKER ORCHESTRATION SUPPORT
// ------------------------------------------------------------
// The functions below are byte-for-byte MIRRORS of the Listing
// Generator's client-side builders (buildCharmDesignPrompt /
// buildCharmImageDescription / cleanCharmPlanText). They exist so
// the charm_batch_orchestrate flow can prepare sets entirely
// server-side, with no browser tab involved. IF YOU CHANGE THE
// PROMPT OR DESCRIPTION DOCTRINE IN THE HTML, CHANGE IT HERE TOO
// — otherwise Standard mode and server-orchestrated Batch mode
// will produce different designs.
// ============================================================
function cmCleanText(value, fallback = "") {
  return String(value == null ? fallback : value)
    .replace(/[\u0000-\u001f\u007f]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function buildCharmDesignPromptServer(concept, plan) {
        const chosen = concept || {};
        const context = plan || {};
        const signals = Array.isArray(context.purchaseSignals) && context.purchaseSignals.length
            ? context.purchaseSignals.join("; ")
            : "same-buyer affinity, collection appeal and giftability";
        return `MARKET-LED NEW CHARM DESIGN — NOT AN EDIT AND NOT A REPLICA

The supplied image is a proven bestselling charm. It is provided ONLY to communicate this workshop's metal colour, polish, engraving weight, flat laser-cut construction, integrated-bail craftsmanship, lighting and black-background treatment. Do not reproduce its object.

REFERENCE BESTSELLER SUBJECT: ${cmCleanText(context.referenceSubject, "shown in the supplied reference")}
INFERRED BUYER: ${cmCleanText(context.buyerProfile, "the same buyer who values the reference bestseller")}
PURCHASE SIGNALS: ${signals}

CHOSEN COMPLEMENTARY PRODUCT:
• CONCEPT: ${cmCleanText(chosen.title, "Buyer-aligned complementary charm")}
• NEW SUBJECT: ${cmCleanText(chosen.subject, "Choose a distinct adjacent subject with the strongest same-buyer appeal")}
• BUYER CONNECTION: ${cmCleanText(chosen.buyerConnection, "It belongs naturally in the same buyer's collection")}
• COMMERCIAL RATIONALE: ${cmCleanText(chosen.conversionRationale, "Evergreen, recognizable and giftable")}
• OUTER CONTOUR BRIEF: ${cmCleanText(chosen.silhouetteBrief, "Use a bold, recognizable one-piece silhouette unlike the reference")}
• ENGRAVING BRIEF: ${cmCleanText(chosen.engravingBrief, "Use only sparse recognition-critical surface engraving")}
• INTENTIONAL CUTOUT BRIEF: ${cmCleanText(chosen.cutoutBrief, "Use negative-space cutouts when they improve recognition and visual appeal; otherwise use none")}

NOVELTY LOCK — HIGHEST PRIORITY:
• Draw the NEW SUBJECT above. The reference subject itself is forbidden.
• The new design must have a materially different silhouette, pose and internal feature map from the reference.
• NEVER trace, mirror, re-pose, re-scale, simplify, embellish or lightly restyle the reference charm.
• NEVER create a baby, pair, family, alternate pose or cosmetic variation of the exact reference subject.
• HARD FAIL: a shopper would identify the result as the same SKU or same object as the reference.
• HARD FAIL: the new outline could be overlaid on the reference and broadly match.

HARD PHYSICAL CONSTRAINTS:
• ONE CONTINUOUS PIECE: the charm body and eyelet are cut together from one uninterrupted flat sheet. There are no separately attached metal components anywhere.
• INTENTIONAL CUTOUT FREEDOM: negative-space cutouts are an important design tool and are fully allowed. Use the INTENTIONAL CUTOUT BRIEF above. Cutouts may be small or large, numerous, openwork, filigree, outline-style, fully enclosed, or intentionally open to the outer contour.
• CUTOUT QUALITY: every intended cutout must have a deliberate recognizable shape and a clean, crisp laser-cut boundary. The remaining metal must form one structurally connected, manufacturable piece with no unsupported floating islands.
• ARTIFACT DISTINCTION: never create irregular black blotches, eroded/faded gaps, shadow-like voids, ragged missing patches or ambiguous areas without a crisp designed cut edge. These are rendering defects—not cutouts.
• UNIFORMLY FLAT SHEET METAL: every millimetre of the charm, including the complete eyelet area, lies in exactly one plane and has the same thin approximately 22-gauge sheet thickness.
• SHARP SQUARE-CUT 2D EDGES: crisp laser-cut perimeter and hole edges. No rounded, rolled, beveled, bulbous, tubular, domed, cast or inflated edges anywhere.
• FLAT SURFACE LIGHTING: high polish may create reflections across the flat face, but the reflections must never make the face or edges look curved, raised or sculpted.
• FRONT VIEW: perfectly front-facing orthographic view with 0% perspective tilt.
• ENGRAVING: sparse, strategic, shallow surface laser etching only. Preserve generous untouched metal areas.
• ENGRAVING TONE — GENTLE, CLOSE TO THE GOLD: render every engraved line in the same warm gold family as the polished face, only approximately 12–18% darker/warmer than the metal beside it — legible at preview size but soft, never a stark dark line against the gold.
• ENGRAVING COLOR LIMITS: never use white, cream or face-matching highlight colour for an engraving, but also never use black, charcoal, dark brown, heavy copper patina, heavy oxidation or deep shadow. Aim for a light warm tone only a few shades deeper than the surrounding gold — subtle, refined, still readable at preview size, never a dark outline.
• ENGRAVING/CUTOUT DISTINCTION: engravings are shallow surface marks; cutouts are true negative space with crisp laser-cut boundaries. Both are allowed and must remain visually unambiguous.
• NO TEXT: no letters, words, dates, numbers, logos or branded shapes.
• NO ATTACHED HARDWARE: no chains, clasps, jump rings, O-rings, split rings, wire loops, folded bails, soldered bails, hinges or connectors of any kind.

ONE INTEGRATED PROTRUDING CHARM HOOP — MANDATORY ON EVERY CHARM, ZERO SEPARATE RINGS:
• ABSOLUTE REQUIREMENT — NO EXCEPTIONS: every generated charm has exactly ONE small ring-shaped hanging hoop that PROTRUDES OUTWARD beyond the charm's outer silhouette at the top — the classic charm hoop. A charm rendered without this hoop is a total failure regardless of how good the rest looks.
• ONE PIECE: the hoop is laser-cut from the SAME flat sheet as the body in one continuous silhouette — the outline flows smoothly from the body, up and around the hoop, and back down. No joint, seam, hinge, fold, overlap, solder point or separate part anywhere.
• HOOP FORM: a small flat annulus — a rounded tab of sheet metal with one clean, round, front-facing hole through its center. It stays perfectly flat and sheet-thin like the rest of the charm: never a doughnut, torus, tube, wire ring, raised rim or edge-on ring, and never a narrow upright oval or slot.
• INTELLIGENT PLACEMENT — THINK BEFORE RENDERING: this charm will hang from this hoop under real-world gravity. Before rendering, locate the design's center of mass (where the visual weight of the metal actually is, accounting for asymmetry, cutouts and heavy/light regions). Place the hoop on the outer contour DIRECTLY ABOVE that center of mass so the charm hangs level and balanced — never tipping nose-up, nose-down or lopsided. For a side-profile animal that means above the back/shoulders, not the nose or tail; for an asymmetric object, shift the hoop toward the heavy side until the hang is level. Choose the placement that is BOTH physically balanced and most aesthetically pleasing for the subject.
• PROPORTION: small and elegant relative to the charm — roughly 10–18% of the charm's height, matching this workshop's reference charms.
• FORBIDDEN — PUNCHED-BODY ATTACHMENT: never use a hole punched inside the charm's body or artwork as the hanging point. The artwork stays intact; the hoop is additional silhouette rising above it.
• FORBIDDEN — SEPARATE HARDWARE: never add a second, separate ring threaded through the hoop; no jump rings, O-rings, split rings, folded/teardrop bails, connectors, clasps or chains. The integrated hoop is the only attachment.
• ZERO STACKED HARDWARE: exactly one hoop — never two hoops, never a hoop plus a punched hole, never a ring passing through the hoop.
• HARD FAIL: a charm with no visible protruding hoop at its top.
• HARD FAIL: a charm whose only attachment is a hole cut inside the main subject's body.
• HARD FAIL: a separate ring, bail or connector distinct from the charm's own sheet, anywhere in the image.
• HARD FAIL: the hoop rendered thick, tubular, domed, three-dimensional or shown edge-on.
• HARD FAIL: a hoop placed off the balance point so the charm would visibly hang tilted.

ABSOLUTE FLATNESS & FRONT-FACING LOCK (EQUAL PRIORITY TO THE HOOP RULE):
• The ENTIRE charm is rendered 100% FRONT-FACING in a perfectly flat orthographic view — as if the flat sheet was scanned face-on. 0% perspective, 0% rotation out of the picture plane.
• ZERO 3D: no depth, no visible thickness, no side walls, no top face, no vanishing lines, no foreshortening, no three-quarter or angled "product shot" viewpoint — even for subjects that are boxy in real life (a suitcase, a book, a house: draw its flat FRONT ELEVATION only).
• HARD FAIL: any part of the charm shown at an angle, in perspective, or with visible 3D volume or side surfaces.

BACKGROUND — ABSOLUTE:
• 100% PURE SOLID BLACK (#000000), edge to edge.
• No white, off-white, grey, studio sweep, gradient, texture or transparency anywhere in the background.
• Generate the image NATIVELY against pure black from the first render. Do not begin with white and remove it. Do not use masking, background removal, chroma keying, cutout compositing or transparent pixels.
• Before returning the image, inspect all four corners and every pixel outside the charm: they must already be uniform #000000.
• The isolated charm is centered with comfortable empty black margin and no cast shadow outside its silhouette.

AESTHETIC CONTINUITY:
Match only the reference's metal alloy/tone, polish, restrained engraving language, lighting quality and production realism. The product concept, subject, silhouette and feature placement come exclusively from the chosen complementary brief above.

FINAL PRE-RENDER CHECK — PERFORM THIS AFTER THE IMAGE IS COMPOSED:
1. Is there exactly ONE small ring-shaped hoop protruding above the charm's body, cut from the same continuous flat silhouette, positioned directly above the design's center of mass? If not, correct the interpretation before creating the single final image.
2. Confirm the hoop extends beyond the body's outline with one clean round hole through it, and that the artwork body itself has NO punched attachment hole anywhere.
3. Confirm there is ZERO separate metal hardware above, behind, in front of or threaded through the hoop. Touching or overlapping the body does not make a separate ring acceptable.
4. Confirm the entire charm is perfectly flat and 100% front-facing: no perspective, no 3D thickness, no angled view anywhere.
5. Repeat the rules before returning the image: EVERY charm has its one integrated protruding hoop. NO separate jump ring or bail. NO hole punched through the body. NO 3D or perspective. The charm with its single integrated hoop must be shown alone.`;
    }

function buildCharmImageDescriptionServer(concept, plan, variant) {
      const c = concept || {};
      const p = plan || {};
      const bits = [];
      const title = cmCleanText(c.title);
      const subject = cmCleanText(c.subject);
      if (variant === "earring") {
        bits.push(`Earring version${title ? ` of "${title}"` : ""}: ${subject || "flat laser-cut gold charm"} — hanging hoop removed for stud/earring assembly, design otherwise identical to the necklace charm.`);
      } else {
        bits.push(`${title ? `"${title}" — ` : ""}${subject || "flat laser-cut gold charm"} with integrated hanging hoop, for necklaces and charm jewelry.`);
      }
      const bc = cmCleanText(c.buyerConnection);
      if (bc) bits.push(bc);
      const cr = cmCleanText(c.conversionRationale);
      if (cr) bits.push(cr);
      const sil = cmCleanText(c.silhouetteBrief);
      if (sil) bits.push(`Silhouette: ${sil}`);
      const eng = cmCleanText(c.engravingBrief);
      if (eng) bits.push(`Engraving: ${eng}`);
      const refS = cmCleanText(p.referenceSubject);
      if (refS) bits.push(`Reference: ${refS}.`);
      const buyer = cmCleanText(p.buyerProfile);
      if (buyer) bits.push(`Target buyer: ${buyer}`);
      if (Array.isArray(p.purchaseSignals) && p.purchaseSignals.length) {
        bits.push(`Purchase drivers: ${p.purchaseSignals.map(cmCleanText).filter(Boolean).join("; ")}.`);
      }
      return bits.join("\n");
    }

function buildCharmEmbedMetadataServer(concept, plan, variant) {
  const c = concept || {};
  const p = plan || {};
  const description = buildCharmImageDescriptionServer(c, p, variant);
  if (!description) return null;
  let conceptJson = null;
  try {
    conceptJson = JSON.stringify({
      variant: variant || "necklace",
      title: cmCleanText(c.title) || null,
      subject: cmCleanText(c.subject) || null,
      buyerConnection: cmCleanText(c.buyerConnection) || null,
      conversionRationale: cmCleanText(c.conversionRationale) || null,
      silhouetteBrief: cmCleanText(c.silhouetteBrief) || null,
      engravingBrief: cmCleanText(c.engravingBrief) || null,
      referenceSubject: cmCleanText(p.referenceSubject) || null,
      buyerProfile: cmCleanText(p.buyerProfile) || null,
      purchaseSignals: Array.isArray(p.purchaseSignals)
        ? p.purchaseSignals.map((s) => cmCleanText(s)).filter(Boolean).slice(0, 6)
        : [],
    });
  } catch (_) {}
  const meta = { Description: description };
  const t = cmCleanText(c.title);
  if (t) meta.CharmTitle = t;
  if (conceptJson) meta.CharmConcept = conceptJson;
  return meta;
}


/* ============================================================================
   CUSTOM CHARM STUDIO — the customer-facing kinds (design guide §10.6)
   ----------------------------------------------------------------------------
     custom_charm_precheck   text-only vision pass on an uploaded image; costs
                             no credit, meters the upload allowance only on an
                             ACCEPTED image so a rejection is free
     custom_charm_generate   verify ID token -> atomic wallet debit -> compose
                             the prompt -> ONE image generation -> write the PNG
                             and the version doc. Credit auto-refunded on failure.
     custom_charm_refine     same, with the prior version as a second reference
     custom_session_status   thin read of a version doc, for browsers where
                             Firestore onSnapshot is blocked

   WHY THIS IS SAFE TO ADD HERE
     This file has no caller authentication of its own and answers
     Access-Control-Allow-Origin "*" — correct for an internal tool on a
     private subdomain, unacceptable for a customer endpoint. So every studio
     kind verifies a Firebase ID token itself, scopes every storage path to the
     caller's own uid, answers a storefront-locked origin instead of "*", and
     applies per-uid and per-IP hourly ceilings on top of the wallet. No
     existing kind's behaviour changes in any way.

   WHAT IT REUSES, VERBATIM
     buildCharmDesignPromptServer's HARD PHYSICAL CONSTRAINTS / HOOP /
     FLATNESS / BACKGROUND / FINAL PRE-RENDER CHECK blocks, the text-only
     auditCharmPromptPreflight, callImageModelEdits with the backend's final
     geometry + background override, embedPngTextMetadata, storagePathToBuffer,
     newDownloadToken and tokenDownloadURLFor. Only the INTENT HEADER differs:
     the Charm Maker's NOVELTY LOCK ("the reference subject is forbidden") is
     replaced by a customer FIDELITY block, because here the customer chose the
     reference and wants it honoured.

   ENV — nothing new. GEMINI_API_KEY, FIREBASE_* and FIREBASE_STORAGE_BUCKET
   already exist on this deployment.
   ========================================================================= */

const STUDIO_KINDS = new Set([
  "custom_charm_precheck",
  "custom_charm_generate",
  "custom_charm_refine",
  /* the studio composed the drawing itself — this only files it */
  "custom_charm_compose",
  "custom_charm_render",
  "custom_session_status",
]);

const STUDIO_SESSIONS_COLL = "customSessions";
const STUDIO_RATE_COLL     = "Brites_Studio_RateLimits";
const STUDIO_CONFIG_DOC    = "config/customStudio";
const STUDIO_MAX_GENS_HOUR_UID = 25;
const STUDIO_MAX_GENS_HOUR_IP  = 60;
const STUDIO_MAX_INSTRUCTION   = 400;

/* Storefront-locked CORS. Deliberately NOT the "*" that json() returns. */
const STUDIO_ALLOWED_ORIGINS = [
  "https://britesjewelry.com",
  "https://www.britesjewelry.com",
  "https://brites-jewelry.myshopify.com",
];
function studioJson(statusCode, obj, origin) {
  const allow = STUDIO_ALLOWED_ORIGINS.includes(origin) ? origin : STUDIO_ALLOWED_ORIGINS[0];
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": allow,
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Vary": "Origin",
      "Cache-Control": "no-store",
    },
    body: JSON.stringify(obj),
  };
}

/** Anonymous uids are first-class here: a guest's free allowance is a real
 *  wallet, so their token must be accepted exactly like a customer's. */
async function requireStudioUser(event) {
  const h = event?.headers || {};
  const raw = h.authorization || h.Authorization || "";
  const token = raw.startsWith("Bearer ") ? raw.slice(7).trim() : "";
  if (!token) { const e = new Error("sign_in_required"); e.status = 401; throw e; }
  try {
    const decoded = await admin.auth().verifyIdToken(token);
    return decoded.uid;
  } catch (_err) {
    const e = new Error("invalid_token"); e.status = 401; throw e;
  }
}

function studioClientIp(event) {
  const h = event?.headers || {};
  return String(
    h["x-nf-client-connection-ip"] ||
    String(h["x-forwarded-for"] || "").split(",")[0] ||
    "unknown"
  ).trim();
}

const STUDIO_DEFAULT_CONFIG = {
  guestFreeCredits: 3,
  signupBonusCredits: 7,
  guestFreeUploads: 3,
  signupBonusUploads: 7,
  generateCost: 1,
  /* ── TWO RENDERERS, TWO PRICES ────────────────────────────────────────
     The metal render is the one step where a second model earns its keep, so
     it is the one step that offers a choice. "Low" is the Gemini renderer the
     studio has always used and stays at generateCost. "High" routes to
     OpenAI's image model, which is slower and dearer, so it costs double.
     Both values live in config/customStudio alongside everything else, which
     means the price and the model can be retuned from Firestore with no
     deploy — and, deliberately, NO new environment variable: the OpenAI key
     the Listing Generator already uses is the same key. */
  renderHighCost: 2,
  renderHighModel: "gpt-image-2",
  /* "off" | "observe" | "enforce" — see the cut check in handleStudioRender.
     Ships off: the scan's fill identification is still being corrected, and a
     check that is sometimes wrong edits a charm somebody paid for. */
  renderCutCheck: "off",
};
let _studioCfg = null, _studioCfgAt = 0;
async function studioConfig() {
  if (_studioCfg && Date.now() - _studioCfgAt < 60000) return _studioCfg;
  try {
    const snap = await getDb().doc(STUDIO_CONFIG_DOC).get();
    _studioCfg = Object.assign({}, STUDIO_DEFAULT_CONFIG, snap.exists ? snap.data() : {});
  } catch (_e) {
    _studioCfg = Object.assign({}, STUDIO_DEFAULT_CONFIG);
  }
  _studioCfgAt = Date.now();
  return _studioCfg;
}

/** Hourly ceiling on top of the wallet, so a stolen ID token cannot burn the
 *  Gemini budget in one go. */
async function studioBudgetOk(key, id, max) {
  const db = getDb();
  const hash = require("crypto").createHash("sha256").update(String(id)).digest("hex").slice(0, 32);
  const ref = db.doc(`${STUDIO_RATE_COLL}/${key}_${hash}`);
  const now = Date.now();
  try {
    return await db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      const d = snap.exists ? snap.data() : {};
      const windowStart = d.windowStart && now - d.windowStart < 3600e3 ? d.windowStart : now;
      const count = windowStart === d.windowStart ? (d.count || 0) : 0;
      if (count >= max) return false;
      tx.set(ref, { windowStart, count: count + 1, updatedAt: now }, { merge: true });
      return true;
    });
  } catch (_e) {
    return true;   // never let the limiter itself take the studio down
  }
}

/* ── wallet ──────────────────────────────────────────────────────────────
   The client's credit meter is cosmetic. THIS is the quota: an atomic
   transaction that runs BEFORE Gemini is called, and a refund if the
   generation fails. */
/* ── UNLIMITED ACCOUNTS ────────────────────────────────────────────────────
 * `users/{uid}.wallet.unlimited === true` exempts an account from the credit
 * meter: generations and renders still run, still log to the ledger, and
 * still obey the hourly rate ceilings — only the balance is left alone.
 *
 * It lives on the USER document rather than in config/customStudio because
 * that config doc is readable by any signed-in visitor (see firestore.rules),
 * and a public list of staff uids is a needless thing to publish. The user doc
 * is readable only by its owner, unwritable from any browser, and is ALREADY
 * loaded inside this transaction — so the check costs nothing.
 *
 * Set it in the Firebase console; no deploy, no code change, and it survives
 * every future top-up because nothing else writes that field.
 * ===================================================================== */
function walletIsUnlimited(w) {
  return !!(w && w.unlimited === true);
}

async function studioDebit(uid, cost, sessionId) {
  const db = getDb();
  const ref = db.doc(`users/${uid}`);
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const w = (snap.exists && snap.data().wallet) || {};

    /* Unlimited: no balance check, no decrement — but still ledgered, so the
       real cost of testing stays visible instead of becoming invisible. */
    if (walletIsUnlimited(w)) {
      tx.create(ref.collection("walletLedger").doc(), {
        delta: 0, reason: "generate_unlimited", ref: sessionId || null,
        at: admin.firestore.FieldValue.serverTimestamp(),
      });
      return;
    }

    const credits = Number(w.credits) || 0;
    if (credits < cost) { const e = new Error("out_of_credits"); e.status = 402; throw e; }
    tx.set(ref, {
      wallet: Object.assign({}, w, {
        credits: credits - cost,
        lifetimeSpent: (Number(w.lifetimeSpent) || 0) + cost,
      }),
      updatedAt: Date.now(),
    }, { merge: true });
    tx.create(ref.collection("walletLedger").doc(), {
      delta: -cost, reason: "generate", ref: sessionId || null,
      at: admin.firestore.FieldValue.serverTimestamp(),
    });
  });
}
async function studioRefund(uid, cost, sessionId) {
  try {
    const db = getDb();
    const ref = db.doc(`users/${uid}`);
    await db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      const w = (snap.exists && snap.data().wallet) || {};
      /* Nothing was taken, so nothing is given back — refunding an unlimited
         account would quietly inflate a balance that is never spent. */
      if (walletIsUnlimited(w)) return;
      tx.set(ref, {
        wallet: Object.assign({}, w, {
          credits: (Number(w.credits) || 0) + cost,
          lifetimeSpent: Math.max(0, (Number(w.lifetimeSpent) || 0) - cost),
        }),
        updatedAt: Date.now(),
      }, { merge: true });
      tx.create(ref.collection("walletLedger").doc(), {
        delta: cost, reason: "failure_refund", ref: sessionId || null,
        at: admin.firestore.FieldValue.serverTimestamp(),
      });
    });
  } catch (e) {
    console.error("[studio] refund failed:", e?.message || e);
  }
}
/** Uploads are metered separately from generation, and ONLY on an accepted
 *  image, so a rejected photo costs the customer nothing. */
async function studioMeterUpload(uid, cfg) {
  const db = getDb();
  const ref = db.doc(`users/${uid}`);
  const user = await admin.auth().getUser(uid).catch(() => null);
  const signedIn = !!(user && user.email);
  let allowed = true;
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const w = (snap.exists && snap.data().wallet) || {};
    const used = Number(w.uploadsUsed) || 0;
    const cap = cfg.guestFreeUploads
              + (signedIn ? cfg.signupBonusUploads : 0)
              + (Number(w.purchasedAllowance) || 0);
    if (used >= cap) { allowed = false; return; }
    tx.set(ref, { wallet: Object.assign({}, w, { uploadsUsed: used + 1 }), updatedAt: Date.now() }, { merge: true });
  });
  return allowed;
}

/* ── the prompt ──────────────────────────────────────────────────────────
   buildCharmDesignPromptServer is the workshop's own doctrine and the single
   source of truth for what a Brites charm physically is. We slice its
   constraint blocks out VERBATIM and wrap them in a customer intent header,
   so the two can never drift: edit the doctrine there and the studio inherits
   it on the next deploy. */
function studioCleanText(text) {
  return String(text == null ? "" : text)
    .replace(/[\x00-\x1f\x7f]+/g, " ")
    .replace(/```+/g, " ")
    .replace(/\b(ignore|disregard|forget)\b[^.]{0,40}\b(previous|above|prior|system)\b[^.]*/gi, " ")
    .replace(/\s+/g, " ")
    .replace(/^[\s.,;:!?)\]}>-]+/, "")
    .trim()
    .slice(0, STUDIO_MAX_INSTRUCTION);
}

/** Everything from HARD PHYSICAL CONSTRAINTS onward, with the Charm Maker's
 *  AESTHETIC CONTINUITY paragraph swapped for the studio's — that one block
 *  says the subject comes from the complementary brief and NOT from the
 *  reference, which is precisely backwards here. */
function studioConstraintBlocks() {
  let full = "";
  try {
    full = String(buildCharmDesignPromptServer({}, {}) || "");
  } catch (_e) {
    full = "";
  }
  const start = full.indexOf("HARD PHYSICAL CONSTRAINTS:");
  const aesthetic = full.indexOf("AESTHETIC CONTINUITY:");
  const finalCheck = full.indexOf("FINAL PRE-RENDER CHECK");
  if (start < 0 || finalCheck < 0) return "";       // doctrine moved — see below

  const physical = aesthetic > start && aesthetic < finalCheck
    ? full.slice(start, aesthetic)
    : full.slice(start, finalCheck);

  const studioAesthetic =
`AESTHETIC CONTINUITY:
Match this workshop's metal alloy and tone, polish, restrained engraving language, lighting quality, production realism and black-background treatment. The SUBJECT comes from the customer's reference image and their written instructions — honour it. Everything about HOW it is made comes from the rules above.

`;
  return physical + studioAesthetic + full.slice(finalCheck);
}

/* Failsafe: if buildCharmDesignPromptServer is ever restructured so the slice
   markers move, the studio must not silently generate unconstrained charms.
   This is the minimum doctrine, used only in that case. */
/* ═══════════ THE FILL LAW ════════════════════════════════════════════════
 * The customer's fill is not decoration and never was — it is the one
 * instruction in the studio that changes what the workshop DOES to the
 * piece. The drawing tool now offers exactly three answers and no others,
 * so this block can be absolute rather than advisory. It is inserted
 * verbatim into every prompt that consumes a customer drawing: the B/W
 * generation and the metal render alike. Both must obey it identically, or
 * the customer's own preview would disagree with the charm they receive.
 * ===================================================================== */
/* The same three instructions, said again as a LIST. A colour inside a
 * compressed JPEG is a thing to be interpreted; a line of text reading
 * "CUT OUT — top centre" is not. Sent alongside the picture, never instead
 * of it, so the two corroborate each other. */
function zoneBlock(zones, which) {
  /* a zone with no geometry is not a zone: it printed as "at the centre
     (about 0% × 0%)" and still incremented a count the model was told to
     obey. Nothing client-side can send one, but nothing server-side checked. */
  const z = (Array.isArray(zones) ? zones : []).filter((q) =>
    q && (q.intent === "cutout" || q.intent === "engrave" || q.intent === "none") &&
    Number(q.w) > 0.004 && Number(q.h) > 0.004);
  if (!z.length) return "";
  const cut = z.filter((q) => q.intent === "cutout");
  const eng = z.filter((q) => q.intent === "engrave");
  const out = z.filter((q) => q.intent === "none");
  const src = which === "reference" ? "THE REFERENCE IMAGE"
            : which === "drawing" ? "THE PRODUCTION DRAWING" : "the mark-up";
  /* ── NOT ONE DIGIT IN THIS LIST. EVER. ────────────────────────────────
     This block has now injected content onto a finished charm twice, and
     both times the vector was the same: a token that only exists to
     structure the list. First it was layer names — "logo 4", "Engraved ·
     Cherry3D Logo.png" — which came back set in type around the rim. The
     names were removed and replaced with index numbers, and the model drew
     the NUMBERS instead, each one placed at the very position its line
     described: a charm with 3 at the top, 4 on the left, 14 at the bottom.
     An image model treats every distinctive token in an instruction block
     as a candidate for the canvas, so the only safe list is one made of
     nothing but ordinary words. No indices, no percentages, no counts in
     figures. Sizes are said in words; counts are said in words; areas are
     told apart by where they are, which is all they ever needed. */
  const sizeWord = (q) => {
    if (q.ring) return "a ring-shaped band around the drawing";
    const span = Math.max(Number(q.w) || 0, Number(q.h) || 0);
    if (span > 0.75) return "spanning nearly the whole drawing";
    if (span > 0.45) return "large";
    if (span > 0.2)  return "medium-sized";
    if (span > 0.08) return "small";
    return "tiny";
  };
  const W = ["no","one","two","three","four","five","six","seven","eight",
             "nine","ten","eleven","twelve"];
  const cw = (n) => n < W.length ? W[n] : "more than twelve";
  const say = (q) => `  – at ${q.where || "the centre"}: ${sizeWord(q)}` +
    (q.ring ? " (its central hole is NOT part of it)" : "");
  return `
THE CUSTOMER'S OWN COUNT OF WHAT EACH AREA IS, taken from the controls they
used in the studio rather than read off the pixels. This list and ${src}
agree; if you ever think they do not, THIS LIST WINS:
${cut.length ? `• CUT CLEAN THROUGH — ${cw(cut.length)} area${cut.length === 1 ? "" : "s"}:\n${cut.map(say).join("\n")}` : "• CUT CLEAN THROUGH: none"}
${out.length ? `• OUTLINE ONLY (boundary worked, interior left as polished metal) — ${cw(out.length)} area${out.length === 1 ? "" : "s"}:\n${out.map(say).join("\n")}` : "• OUTLINE ONLY: none"}
• ENGRAVED SOLID: every remaining BLACK area of ${src} — ${cw(eng.length)} of
  them. They are not itemised because the picture already shows each one;
  engrave every one of them exactly where and as the picture shows it.
  BLACK, not merely dark: a blue area is dark too, and it is a hole.
AN OUTLINE-ONLY INSTRUCTION IS ABOUT ONE SHAPE'S OWN BOUNDARY AND INTERIOR —
NOTHING ELSE. It never blanks, erases, thins or un-engraves anything outside
that shape: every other black area the picture shows stays exactly as dark and
as engraved as the picture shows it. Turning a solidly engraved band into a
thin outline because an outline instruction exists nearby is a hard fail.
EVERY area listed as CUT CLEAN THROUGH must be an actual opening in your
output, and every black area must come through as engraved. Check this before
you finish.
NOTHING IN THIS LIST IS ARTWORK. It describes where areas sit; it is not
lettering, not a caption, not a label, not a legend and not part of the
design. Never draw, engrave, set or letter ANY word, letter, digit or number
anywhere on the charm unless it is given under LETTERING — and this list is
not LETTERING.
This list says what these particular areas ARE; it is not a census of the
whole charm. The hoop's own hole, and any opening the design itself plainly
requires — a ring, a bail, the middle of a letter O — are separate from it
and are still required. What the list forbids is the reverse: turning a
listed cut-out into engraving or solid metal, or a listed engraved area into
plain polished metal.
`;
}

const FILL_LAW =
`THE GOLDEN RULE OF THIS WORKSHOP — ABOVE EVERY OTHER INSTRUCTION IN THIS
PROMPT: the customer's three engraving colours (BLACK, BLUE, RED) are obeyed
EXACTLY, on every generation, every refinement and every render, with no
exception, ever. EVERY solid blue area IS an opening. Every black area IS
engraved. Every red boundary IS an outline with a polished interior. If your
composed image loses even ONE of them — one blue area that is not an opening,
one black area that is not engraved — the image is WRONG and must be redone
before you return it. There is no artistic judgement to apply here; a lost
instruction is a physically different object than the one that was bought.

AREA FILL — THREE MEANINGS, NON-NEGOTIABLE. This is the customer's
manufacturing instruction, not a stylistic choice, and it overrides every
other consideration including your own sense of what would look better:

  1. SOLID BLACK FILLED AREA  =  ENGRAVE IT.
     The whole area is engraved — cut into the surface, hatched or recessed
     solid. The metal is STILL THERE; its surface is worked. In the B/W
     production drawing it is solid black. In the metal render it is a
     recessed, darker, textured area with the metal plainly present.

  2. SOLID BLUE FILLED AREA  =  CUT IT CLEAN THROUGH.
     The metal is REMOVED from that area entirely: an actual opening through
     the charm, with whatever is behind the charm visible through it. Blue is
     reserved in this studio for exactly this and appears for no other
     reason. In the B/W production drawing a cut-out is drawn as an OPEN
     HOLE — paper white inside, with a single clean cut edge around it, the
     same convention as the hoop's hole. NEVER fill a blue area with black.
     NEVER engrave it. NEVER leave it as solid metal. It is a hole.

  3. RED OUTLINE, or an outline with a white/empty interior  =  OUTLINE ONLY.
     Engrave the boundary LINE and nothing else. The interior is flat,
     untouched, polished metal — exactly like the surrounding surface. NEVER
     promote an unfilled outline into a filled or engraved area, never hatch
     it, never shade it, never darken it. If the customer had wanted it
     engraved they would have said so, and they had a control to say it with.
     RED IS RESERVED for this and appears in a Brites drawing for no other
     reason. A red boundary is engraved as an ordinary black production line;
     the red itself is never reproduced.

THESE THREE COLOURS ARE INSTRUCTIONS, NOT PIGMENT. Blue and red are reserved
words in this studio's language: blue says "there is no metal here", red says
"only this line is worked". Neither is EVER a colour of the finished metal —
no blue enamel, no red inlay, nothing on the physical charm is blue or red.
On a production DRAWING they appear as themselves, because a drawing is where
instructions are written down; on a rendered CHARM they appear only as what
they mean: an opening, and a worked line in plain metal.

Applying the wrong one of these three is the single worst error you can make
on this job: it produces a charm that is physically different from the one
the customer designed. Count them before you draw and count them again after.`;

/* ── THE SAME LAW, FOR THE RENDER STEP ONLY ───────────────────────────────
   FILL_LAW above is written from the DRAWING's point of view — half of it
   instructs how the three colours are drawn ("paper white inside", "in the
   B/W production drawing…"), which is noise once the drawing already exists,
   and worse, it hands the model a second description of a hole to reconcile
   with the render's own. The render prompt then restated the whole mapping a
   third time in its own header.

   Three descriptions of one rule are not three times the emphasis; they are a
   reconciliation problem, and a model resolves those by picking. So the render
   says it ONCE, in the only vocabulary that applies to a photograph of metal,
   and leads with the distinction that actually fails: a hole and an engraving
   must not look alike. This block is SHORTER than the three it replaces. */
const FILL_LAW_RENDER =
`THE GOLDEN RULE OF THIS WORKSHOP — ABOVE EVERY OTHER INSTRUCTION HERE.
The drawing's fill colours are manufacturing orders, not pigment.
The finished charm obeys them exactly, and shows none of these colours.

  BLUE area in the drawing → A HOLE. The metal is gone. You see the ground the
  charm rests on straight through it, exactly as through the hoop's hole.
  Never metal, never enamel, never engraving, never a dark patch.

  BLACK area in the drawing → ENGRAVED metal. The metal is still there and
  only its surface is worked. Never a hole.

  RED boundary in the drawing → ONLY THAT LINE is engraved. Its interior is
  flat polished metal, identical to the surface around it. Never fill it.

  GREEN area in the drawing → SOLID METAL, untouched and polished. Nothing is
  done to it. White is never metal: white is where the charm is not.

A HOLE AND AN ENGRAVING MUST NEVER LOOK ALIKE. This is the distinction the
whole charm depends on. A hole shows the ground through it and is bounded by a
cut edge. An engraving shows worked gold, barely different from the polish
beside it. If a viewer could not tell instantly which of the two they were
looking at, the render has failed — and that failure is almost always an
engraving rendered dark enough to read as an opening.

Applying the wrong one of the three produces a charm physically different from
the one the customer bought. Count the holes before you finish, then count them
again.`;

const STUDIO_FALLBACK_CONSTRAINTS =
`HARD PHYSICAL CONSTRAINTS:
• ONE CONTINUOUS PIECE cut from a single uniformly flat, thin (~22-gauge) sheet. No separate parts, no floating islands.
• SHARP SQUARE-CUT 2D EDGES. No rounded, domed, tubular, cast or inflated edges.
• EXACTLY ONE INTEGRATED PROTRUDING HOOP at the top, cut from the same sheet as a flat annulus with one clean round hole, placed directly above the design's center of mass so the charm hangs level. Never a hole punched inside the body. No jump rings, split rings, bails, chains or any separate hardware.
• ENGRAVING: sparse, very shallow, superficial surface etching — a delicate skim of the surface, never a deep carve. Its tone sits only a few shades from the surrounding gold: quiet, low-contrast, never black or white.
• NO TEXT of any kind. NO ATTACHED HARDWARE. NO 3D, no perspective, no visible thickness — a perfectly front-facing flat orthographic view.
• BACKGROUND: 100% pure solid black (#000000), edge to edge, generated natively, no gradient, no transparency, no cast shadow.

AESTHETIC CONTINUITY:
Match this workshop's metal tone, polish, engraving language and lighting. The SUBJECT comes from the customer's reference image and their written instructions — honour it.

FINAL PRE-RENDER CHECK — perform after the image is composed:
1. Exactly ONE protruding hoop above the body, from the same continuous silhouette, above the center of mass?
2. No attachment hole punched through the artwork, and no separate ring anywhere?
3. Entirely flat and 100% front-facing — no perspective, no 3D thickness?
4. Pure #000000 in every pixel outside the charm?
If any answer is no, correct the design before rendering.`;

/* ── the studio's OUTPUT CONTRACT for the design step ─────────────────────
   Adapted from the Charm Maker's promptStrBW, with one inversion: there the
   drawing DESCRIBES an existing gold charm, here the drawing IS the design.
   The ink rules are identical either way — black maps 1:1 to engraved metal —
   because this drawing later becomes the render step's structural truth, and
   the whole point of designing in B/W is that nothing about the engraving is
   ambiguous. */
const STUDIO_LINEART_CONTRACT =
`THE THREE INSTRUCTION COLOURS (THEY MEAN THE SAME IN YOUR INPUT AND YOUR OUTPUT):
This studio's production drawings are colour-coded. Exactly three colours may
carry meaning, and they are manufacturing instructions, never decoration:
• SOLID BLACK area → ENGRAVED metal.
• SOLID BLUE (a bright cyan-blue, close to #00b4ff) area → CUT CLEAN
  THROUGH: a real opening in the sheet. In your drawing that opening is
  filled SOLID BLUE so the instruction is visible at a glance.
• RED (close to #e00000) boundary line → OUTLINE ONLY: that boundary is a
  worked line and the area it encloses is flat polished metal, left WHITE.
Whatever colour an instruction arrives in — on the reference, the sketch or
the mark-up — it leaves in the SAME colour on your drawing. Losing an
instruction colour, moving it, or inventing one nobody gave is a hard fail.

COLOUR-CODED PRODUCTION LINE ART — THE OUTPUT CONTRACT (NON-NEGOTIABLE):
• The output is a flat 2D VECTOR-STYLE production drawing on a solid pure WHITE (#FFFFFF) background, drawn in BLACK ink plus the two instruction colours above and nothing else. Absolutely no transparency, no other colours, no greys, no shading, no gradients, no 3D extrusion, no perspective, no photorealism, no metal rendering.
• It depicts ONE charm: a single continuous silhouette that could be laser-cut from one flat sheet. No separate parts, no floating islands, no props, no scene.
• EXACTLY ONE small ring-shaped hanging hoop protrudes above the silhouette at the top, drawn as part of the same continuous outline — a flat annulus with one clean round hole — directly above the design's center of mass, so the finished charm hangs level. Never a separate jump ring, bail or chain.
• The charm's outer perimeter is ONE clean solid BLUE outline approximately 2px thick — the same instruction blue as the holes, because the silhouette IS a cut: it is where the charm is cut out of the sheet. A blue perimeter LINE is the cut edge; only a blue FILLED AREA is an opening.
• ENGRAVING INK RULE (CRITICAL — BLACK MAPS 1:1 TO ENGRAVED METAL, WHITE TO POLISHED METAL):
    1. STROKE ENGRAVING (the common case): an engraved line — straight, curved, or forming the OUTLINE of a shape such as a cloud, heart, wing, leaf or star — is ONE solid black stroke of appropriate weight. Never two thin parallel outlines standing in for one wide stroke.
    2. INTERIOR STAYS WHITE: when a stroke forms a CLOSED outline, the area it encloses is polished metal and stays pure WHITE. An outlined cloud is an outlined cloud, never a solid black cloud.
    3. SOLID-REGION ENGRAVING (rare): fill an area solid black ONLY when that ENTIRE area is meant to be engraved metal.
    4. THE FINGERTIP TEST: for every inked area — would a fingertip feel it as recessed engraving? If the interior would feel like the same polished surface as the rest of the charm, it stays white with a black outline stroke.
    5. CUTOUTS/HOLES — TWO KINDS, DRAWN DIFFERENTLY, HARD RULE:
       (a) the CUSTOMER'S DECLARED CUT-OUT AREAS in the design are filled SOLID BLUE inside their cut outline — blue area = no metal here.
       (b) STRUCTURAL CUTS — the hanging hoop's hole and the charm's outer silhouette — are drawn as BLUE LINES ONLY: the hoop's hole is pure WHITE inside ONE clean BLUE circular cut line, never filled solid blue and never black; the outer perimeter is one clean BLUE line. A blue LINE is a cut edge; only a blue FILLED AREA is an opening in the design.
    6. OUTLINE-ONLY AREAS: an area the customer marked outline-only has its boundary drawn as ONE clean RED line of ordinary production weight, interior pure white. Red is this drawing's word for "work the line, leave the inside alone".
• RECOGNITION AT CHARM SCALE: sparse, recognition-critical engraving only — the few lines that make the subject read at 12 mm.
• WORDS AND DIGITS: the only text of any kind in your drawing is text visibly present in the customer's own reference/mark-up or given as LETTERING. Never invent, copy or letter anything from these instructions — no numbers, no counts, no percentages, no labels, no filenames. Instructions are read, never drawn.
HARD FAIL: any shading, grey tone, black background, or any colour other than black, the instruction blue and the instruction red.
HARD FAIL: an outlined shape rendered as a filled solid when its interior is polished metal.
HARD FAIL: a cut-through opening left white or drawn black instead of filled blue, or a blue/red instruction dropped, recoloured or moved.
FINAL CHECK BEFORE RENDERING THE ONE IMAGE: one continuous silhouette with its perimeter drawn as one blue line; one integrated protruding hoop above the center of mass, its hole WHITE inside one blue cut line — never solid blue; every black mark is genuinely engraved metal; every declared opening a solid blue AREA — count the blue areas the input declares and count them again in your drawing, they must match; every outline-only boundary red with a white interior; pure white background edge to edge.`;

/* Where on the drawing a normalized point sits, in words the model can bind
   to what it sees — a note's meaning depends on WHERE it is pinned. */
function markupRegion(x, y) {
  const col = x < 0.34 ? "left" : x > 0.66 ? "right" : "centre";
  const row = y < 0.34 ? "top" : y > 0.66 ? "bottom" : "middle";
  return row === "middle" && col === "centre" ? "the centre" : `the ${row} ${col}`;
}

function buildCustomerLineArtPrompt({ instructions, thread, refine, markupNotes, markupZones, markupMode, refMode, refZones }) {
  const clean = studioCleanText(instructions);
  const log = (Array.isArray(thread) ? thread : [])
    .filter((m) => m && m.text)
    .slice(-24)
    .map((m, i) => `${i + 1}. ${m.role === "studio" ? "STUDIO" : "CUSTOMER"}: ${studioCleanText(m.text)}`)
    .filter((line) => line.split(": ").slice(1).join(": ").length > 0)
    .join("\n");

  const header =
`CUSTOMER-DIRECTED CHARM DESIGN — B/W PRODUCTION DRAWING. TRANSLATE, DON'T INVENT.

The supplied image is the customer's chosen reference. Design their charm from
it and draw the design as flat B/W production line art, then apply ONLY the
customer's requested changes:

CUSTOMER INSTRUCTIONS: ${clean || "(no change requested — translate the reference faithfully)"}
REFERENCE HANDLING: keep the recognizable subject and proportions; changes
beyond the instructions are forbidden.

• OUTER CONTOUR BRIEF: follow the reference subject's own recognizable silhouette, simplified into one bold, clean, manufacturable outline.
• ENGRAVING BRIEF: use only sparse, recognition-critical engraving strokes — the few lines that make the subject read at charm size.
• INTENTIONAL CUTOUT BRIEF: use negative-space cutouts where they improve recognition or visual appeal; otherwise use none.`;

  const parts = [header];
  if (log) {
    parts.push(
`CUSTOMER DIRECTION LOG — the full conversation so far, oldest first.
Honour all of it; the last line is the newest request:
${log}`);
  }
  if (refine) {
    parts.push(
`REFINEMENT PASS: the second image is the customer's current drawing of this
charm. Keep it and change ONLY what the newest instruction asks for.`);
  }
  /* ── THE SKETCH CONTRACT ─────────────────────────────────────────────────
     Set only when the reference image is the customer's OWN hand drawing from
     the studio's sketchpad, with the mode they chose there. Exact means the
     sketch IS the design, wanted as drawn; interpreted means it is a brief
     for the real thing. */
  if (!refine && (refMode === "exact" || refMode === "interpret")) {
    parts.push(refMode === "exact"
? `THE REFERENCE IMAGE IS THE CUSTOMER'S OWN HAND-DRAWN DESIGN — EXACT MODE:
The customer chose "exactly as drawn". Your job is faithful VECTORIZATION,
not redesign: reproduce the sketch's geometry, composition, positions and
proportions exactly, converted to clean production line weight. Straighten
only the hand's jitter; change NOTHING else.
• LINE COUNT IS SACRED: one drawn line becomes one production line — never
  doubled, never given an inner or outer parallel, a rim, a border or a halo.
• LINE colour carries no meaning — the sketchpad offers coloured lines only so
  the customer can tell their own objects apart, and every line is an engraved
  line whatever its colour.
• AREA FILL CARRIES THE WHOLE MEANING, and there are exactly three:
  SOLID BLACK area = ENGRAVE that area. SOLID BLUE area = CUT IT CLEAN
  THROUGH — a real opening, drawn in your output as that area filled SOLID
  BLUE, never black, never engraved, never plain white. RED BOUNDARY, or no
  fill at all, = outline only: draw that boundary as one clean RED line and
  leave its interior white as flat polished metal. The instruction colours
  pass straight through: blue in, blue out; red in, red out. Applying the
  wrong one produces a physically different charm; it is the worst error
  available on this job.
• TEXT in the sketch is reproduced at exactly the drawn position and size, in
  one clean sans-serif face — never moved, rescaled, reflowed or restyled.
• Add nothing the sketch does not show; remove nothing it does.
BEFORE RENDERING, CHECK: laid over the sketch, does every element of your
output sit on top of its counterpart at the same size? If not, redo it.`
: `THE REFERENCE IMAGE IS THE CUSTOMER'S OWN HAND-DRAWN DESIGN — INTERPRETED:
A freehand PEN line's colour carries no meaning — it only tells the
customer's own objects apart, and every such line is an engraved line. An
AREA is the opposite: its colour is a manufacturing instruction with exactly
three values. SOLID BLACK area = engrave it. SOLID BLUE area = CUT IT CLEAN
THROUGH — a real opening, shown in your output as that area filled SOLID
BLUE, never as black and never as engraving. A RED BOUNDARY, or no fill at
all = outline only, its interior left as flat polished metal and its boundary
drawn as one clean RED line. The instruction colours pass straight through:
blue in, blue out; red in, red out.
Read the sketch for what it DEPICTS and draw that subject properly in this
studio's production line language — clean, manufacturable, symmetric where
the real subject is symmetric — while keeping the sketch's composition: each
element at the position and relative scale the customer gave it. Honour any
text: the same words, at the same place and relative size, in one clean
sans-serif face.`);
  }
  /* ── AND THE LAW ITSELF, IN FULL, ON EVERY PATH THAT HAS A PLAN ─────────
     This used to be two mutually exclusive branches — "the reference is a
     sketch" and "this is a refinement" — which between them missed the two
     commonest journeys in the studio: a FIRST generation from an uploaded
     picture, and a first generation from a catalogue charm. Those produced a
     prompt with no fill law in it at all, so the one instruction that
     changes the physical object was decided by whatever the model felt like.
     The plan now arrives for all four kinds of reference (the studio reads
     it off the picture when nobody has drawn one), so the law travels
     wherever the plan does.

     Not when a MARK-UP is in the request: the mark-up block below carries
     the same law with the mark-up's own — more recent, more specific — list
     beside it, and stating the law twice with two different censuses of the
     same charm is an invitation to split the difference. */
  const hasRefPlan = Array.isArray(refZones) && refZones.length > 0;
  const markupInPlay = markupMode !== undefined && markupMode !== null;
  if (!markupInPlay && (refMode === "exact" || refMode === "interpret" || refine || hasRefPlan)) {
    parts.push(FILL_LAW);
    const rz = zoneBlock(refZones, "reference");
    if (rz) parts.push(rz);
  }
  /* ── THE MARKUP CONTRACT ─────────────────────────────────────────────────
     The customer drew on the previous drawing with a mouse or a fingertip.
     Two modes, chosen by the customer in the markup studio:

       interpret (default) — every mark is a GESTURE to be read for intent.
         A hand can only approximate; the studio's job is to understand what
         the shape MEANS and draw the real thing properly.
       exact — the customer means their geometry literally; reproduce it,
         cleaned only to production line weight.

     Notes travel with their anchor points so the model can bind each
     instruction to the region it is pinned on — "make this bigger" says
     nothing without the where. Note numbers match the numbered labels baked
     into the composite image. */
  const allNotes = (Array.isArray(markupNotes) ? markupNotes : [])
    .map((n) => {
      if (n && typeof n === "object") {
        const t = studioCleanText(n.text);
        if (!t) return null;
        const x = Number(n.x), y = Number(n.y);
        const where = Number.isFinite(x) && Number.isFinite(y) ? markupRegion(x, y) : null;
        const h = Number(n.h);
        return { text: t, where, kind: n.kind === "lettering" ? "lettering" : "note",
                 h: Number.isFinite(h) && h > 0 && h < 1 ? h : null };
      }
      const t = studioCleanText(n);                     // legacy: bare strings
      return t ? { text: t, where: null, kind: "note" } : null;
    })
    .filter(Boolean).slice(0, 16);
  /* Two different things arrive as text, and confusing them ruins a charm:
     a NOTE is an instruction ABOUT the drawing and must never be drawn; a
     LETTERING item is a word the customer placed ON the charm and must be
     engraved. The drawing studio tags them apart at source. */
  const notes    = allNotes.filter((n) => n.kind !== "lettering").slice(0, 12);
  const lettering = allNotes.filter((n) => n.kind === "lettering").slice(0, 6);
  if (markupMode !== undefined && markupMode !== null) {
    const exact = String(markupMode) === "exact";
    const noteLines = notes.map((n, i) =>
      `${i + 1}. ${n.where ? `(pinned at ${n.where} of the drawing) ` : ""}"${n.text}"`).join("\n");
    const letterLines = lettering.map((n) =>
      `• "${n.text}"${n.where ? ` — placed at ${n.where} of the charm` : ""}${n.h ? `, cap height ≈ ${Math.round(n.h * 100)}% of the drawing's height — reproduce at THIS size, at THIS position, exactly as shown in the markup image` : ""}`).join("\n");
    /* the vocabulary the drawing studio can now produce, in both modes */
    const GRAMMAR =
`${FILL_LAW}
${zoneBlock(markupZones)}
WHAT THE CUSTOMER'S TOOLS MEAN:
• A FREEHAND PEN LINE's colour carries no meaning. The studio lets the
  customer draw in more than one colour purely so they can tell their own
  marks apart, and every such line is the same instruction as a black one:
  engrave it. This says nothing about AREAS — a closed shape's black, blue or
  red is governed by the law above, and a RED BOUNDARY around an empty area
  is that law's third value, not a coloured pen stroke.
• A RING (a shape with a hole in it) = a genuine cut-out — metal removed, so
  the background shows through.
• An ARROW = "this, here": read what it points AT, and treat the note nearest
  its tail as the instruction for the thing at its head.
• A rectangle or circle with a small loop on top = the charm plus its BAIL,
  the loop it hangs from.
• A dashed leader line from a label to a spot = the same as a pinned note.
${lettering.length ? `
LETTERING THE CUSTOMER PLACED ON THE CHARM — these words are part of the
charm and MUST be engraved, in the studio's ONE lettering face (a clean
sans-serif — never a serif, script or decorative face), at exactly the
position and size shown in the markup image, cut deep enough to read at
charm scale. The customer placed and sized these words deliberately; moving,
rescaling or restyling them is a defect:
${letterLines}` : ""}`;
    parts.push(exact
? `CUSTOMER MARKUP — EXACT MODE (the customer chose literal):
The final image is the customer's own hand-drawn markup ON TOP of the current
drawing. The coloured ink and the note bubbles themselves must never appear in
your output — but in THIS mode the drawn geometry is meant literally:
• A drawn SHAPE is reproduced faithfully — same geometry, position and
  proportions — converted to this drawing's own black production ink at proper
  line weight.
• A ring or highlight around existing linework still means "change this area";
  a scribble or strike THROUGH existing linework still means "remove this".

THE PRIME RULE OF EXACT MODE — CHANGE ONLY WHAT WAS MARKED:
Everything the customer did NOT mark is reproduced IDENTICALLY from the
current drawing — same lines, same weights, same positions, same proportions,
same fills and non-fills. An unrequested "improvement" is a DEFECT, not a
courtesy. In particular:
• NEVER double a line. A single outline stays ONE line — no inner or outer
  parallel lines, no rims, no borders, no halos added to any shape, marked
  or unmarked.
• NEVER restyle, thicken, thin, move or resize anything that was not marked.
• An outlined shape with NO fill stays an outline with NO fill — do not fill,
  hatch or blacken it, and do not outline a filled shape.
• LETTERING added by the customer is engraved at EXACTLY the position and
  size shown in the markup image — same spot, same cap height, one single
  clean sans-serif face. Do not move it, rescale it, reflow it or restyle it.
BEFORE RENDERING, CHECK: does every unmarked region of your output match the
current drawing stroke for stroke? If anything unmarked changed, that is
wrong — redo it.

${GRAMMAR}
${notes.length ? `PINNED NOTES — each anchored to the spot its words are about; honour every one in its own region:
${noteLines}` : ""}`
: `CUSTOMER MARKUP — HOW TO READ IT (INTERPRETED, the default):
The final image is the customer's own hand-drawn markup ON TOP of the current
drawing. It is DIRECTION, not artwork: the customer's gesture ink — pen
strokes, highlights, rings, arrows — and the note bubbles themselves must
never appear in your output. The one exception is the three INSTRUCTION
colours: a solid blue AREA, a red BOUNDARY and a solid black AREA are the
fill law speaking, not gestures, and they pass through to your drawing as
exactly what they are.

READ EVERY MARK FOR INTENT, NEVER FOR ITS LITERAL GEOMETRY — it was drawn by
hand with a mouse or fingertip, so its shape is an APPROXIMATION:
1. CLASSIFY each mark first:
   • a ring or highlight AROUND existing linework = "change this area";
   • a scribble or strike THROUGH existing linework = "remove this";
   • a drawn SHAPE in open space, or attached to the silhouette = "ADD
     something like this, here".
2. For every added shape, NAME the real object the sketch approximates. Use
   the pinned notes — especially the note nearest the sketch — and the
   conversation to decide: a rough two-lobed arc at the shoulder with a note
   saying "wings" is a WING, not a wobbly arc.
3. Then draw THAT object properly: clean, manufacturable, symmetric where its
   real counterpart is symmetric, in this drawing's own line language,
   integrated into the charm's continuous silhouette — at the sketch's
   position and roughly its scale.
4. HARD FAIL: output linework that traces, echoes or resembles the customer's
   hand-drawn path. Their wobble is input error, not design.

${GRAMMAR}
${notes.length ? `
PINNED NOTES — each anchored to the spot its words are about, numbered to
match the labels in the markup image. A note applies to the marks and the
region it is pinned on:
${noteLines}
` : ""}
BEFORE RENDERING, ANSWER INTERNALLY: (a) which areas did the customer mark?
(b) what is each mark asking — change, remove, or add? (c) what real object
does each sketched shape stand for, given the note pinned nearest to it?
(d) what does this charm look like with those intents executed cleanly, as if
the workshop's own designer had made the changes? Render THAT.`);
  }
  parts.push(STUDIO_LINEART_CONTRACT);
  return parts.join("\n\n");
}

/* ── the render step's prompt: the Charm Maker's conversion, reversed ───── */
/* ── THE CONSTRAINTS, MINUS THE BACKGROUND ────────────────────────────────
   studioConstraintBlocks() is the Charm Maker's doctrine, and the Charm Maker
   shoots on black — it carries a whole "BACKGROUND — ABSOLUTE:" section
   ("100% PURE SOLID BLACK edge to edge", "no cast shadow outside its
   silhouette", "do not begin with white and remove it"). Correct there; the
   exact opposite of what the studio render needs, which is a white ground and
   a real contact shadow.

   The filter is SECTION-aware, not line-aware: a line-by-line pass caught the
   bullets naming #000000 and left the neighbours in the same section — "no
   white, off-white, grey, studio sweep… anywhere in the background" and "no
   cast shadow" — which are the two that would have hurt most. Whole sections
   whose heading names the background go; the remaining sweep catches strays
   elsewhere. Every physical constraint is untouched, and this is a filter
   rather than a fork so the doctrine still lives in one place. */
function renderConstraintBlocks() {
  const src = String(studioConstraintBlocks() || STUDIO_FALLBACK_CONSTRAINTS);
  const isHeading = (line) => /^[A-Z][A-Z0-9 ,&/'’()—–-]{4,}:\s*$/.test(line.trim());
  const out = [];
  let skipping = false;
  for (const line of src.split("\n")) {
    if (isHeading(line)) skipping = /BACKGROUND/i.test(line);
    if (skipping) continue;
    if (/#000000/.test(line)) continue;
    if (/^\s*[•\-]\s*BACKGROUND\b/i.test(line)) continue;
    if (/\bno cast shadow\b|\bempty black margin\b|\bagainst pure black\b/i.test(line)) continue;
    out.push(line);
  }
  return out
    .join("\n")
    .replace(/,?\s*(?:and\s+)?black-background treatment/gi, "")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function buildLineArtToCharmPrompt({ metal, zones }) {
  const m = studioCleanText(metal) || "gold";
  const label = ({
    silver: "polished sterling silver",
    gold: "polished 14k gold",
    rose: "polished 14k rose gold",
    solid10: "polished 10k solid gold",
    solid14: "polished 14k solid gold",
  })[m] || ("polished " + m);

  /* Presentation — ground, shadow, engraving tone — is stated ONCE, in
     STUDIO_RENDER_FINISH at the end. The header used to preview it here in
     different words, and every one of those words had to be reconciled with
     the block that claimed to supersede them. */
  const header =
`APPROVED PRODUCTION DRAWING → FINISHED CHARM

Render ONE finished charm as a photorealistic flat ${label} charm. IMAGE 1 is the approved production drawing and is the only structural truth.

1:1 STRUCTURAL LOCK — HIGHEST PRIORITY
• Reproduce the drawing exactly. Do not redesign, beautify, simplify, add, remove, move or reinterpret anything.
• The outer silhouette, integrated top hoop, every hole, every engraved area, every outline-only region, every letterform and every proportion must match the drawing 1:1.
• The finished charm contains NO instruction colours: not one blue pixel, not one red pixel.

MATERIAL
• ${label}. The metal changes tone only, never form.
• One thin flat sheet, front-facing: no perspective, no visible thickness.
• The hoop's hole and every hole the drawing declares are real openings.

${FILL_LAW_RENDER}
${zoneBlock(zones, "drawing")}`;

  return header + "\n\n" + renderConstraintBlocks() +
         "\n\n" + STUDIO_RENDER_FINISH;
}

/* ═══════════ HOW THE CUSTOMER'S CHARM IS PRESENTED ═══════════════════════
   Two things were wrong with it, and both are the kind of wrong that makes
   a real object look like a picture of one.

   THE ENGRAVING WAS TOO DARK. Engraving does not change what metal IS; it
   changes how the surface catches light. A recess in polished gold reads as
   a slightly deeper, warmer, more coppery gold — not as brown, not as
   bronze, and never as a separate material inlaid into the piece. It was
   coming back nearly black in places and heavily grained, which reads as
   fake at a glance because no jeweller's laser leaves that.

   AND IT SAT ON BLACK. A charm floating on pure black with no shadow is a
   cut-out; it tells the customer nothing about the object's weight, its
   thickness, or — crucially — where the holes are. A real shadow does, and
   the holes in the shadow are the proof that the cut-outs are cut. */
const STUDIO_RENDER_FINISH =
`PRESENTATION — THIS SUPERSEDES EVERY EARLIER INSTRUCTION ABOUT THE
BACKGROUND, THE SHADOW AND THE ENGRAVING TONE. Where anything above
disagrees with this block, THIS BLOCK WINS.

BACKGROUND AND SHADOW:
• The charm rests on a clean, seamless, pure WHITE studio ground plane — not
  black, not grey, not a gradient, not a vignette, not a reflection. Plain
  white, edge to edge.
• It casts ONE real, physically plausible contact shadow onto that plane:
  soft-edged, short, close under the piece, slightly darker and tighter
  where the metal meets the ground and diffusing outward. A single soft key
  light from the upper left, so the shadow falls gently to the lower right.
  THE SHADOW IS REQUIRED. A charm sitting on white with no shadow reads as a
  cut-out pasted onto a blank page, not as a photographed object, and is an
  incomplete image.
• THE SHADOW IS A TRUE SILHOUETTE OF THE CHARM AND OF NOTHING ELSE. Every
  opening cut through the metal — the hoop's hole and every cut-out in the
  design — appears in the shadow as a corresponding gap of clean white.
  Light passes through a hole; a shadow with the hole missing is the single
  clearest sign of a render that has not understood the object. Count the
  openings in the charm and count them again in the shadow.
• No drop shadow effect, no glow, no outline, no floor line, no horizon, no
  props, no hand, no chain, no packaging.

ENGRAVED SURFACES — THE ONE THING THIS RENDER GETS WRONG MOST OFTEN IS
ENGRAVING RENDERED TOO DARK. Every line below is a CEILING, not a target:
• An engraved area is the SAME GOLD as the polish beside it. What changes is
  the FINISH, not the colour — satin where the rest is mirror, like brushed
  and polished bands on one ring. The polished metal stays bright and
  specular; the contrast between the two comes from finish and the gentlest
  tonal shift, never from a change of colour.
• Converted to greyscale, an engraved area and the polished metal touching it
  would differ only slightly. Brown, bronze, copper, grey, charcoal or black
  is wrong, and so is any recess that reads as a dark shape rather than as
  worked gold.
• If the engraving is the first thing the eye lands on, or if it could be
  mistaken for a hole, it is far too dark. Err lighter every time.
• DEPTH IS SUPERFICIAL. A shallow, delicate skim of the surface — never a
  deep carve, a trench, a chisel cut, or anything with visible walls or
  strong internal shadow.
• EVEN EVERYWHERE. Every engraved area the same tone and the same depth as
  every other. Blotches, patches, or gradients within one area are wrong.
• TEXTURE BARELY THERE. A faint, fine, even micro-texture — the trace of a
  laser, visible only on inspection. Coarse grain, heavy hatching, stippling,
  brushed streaks or any woven look is wrong, and is the main thing that
  makes a render look artificial. When in doubt, less.

FINAL CHECK ON THIS BLOCK: white ground; one soft contact shadow; every
opening present as white inside that shadow; every blue area of the drawing a
real hole rather than metal; and every engraved area so close in tone to the
polish that no one could mistake it for a hole — shallow, even everywhere,
texture barely there.`;

function buildCustomerCharmPrompt({ instructions, thread, metal, refine }) {
  const clean = studioCleanText(instructions);
  const log = (Array.isArray(thread) ? thread : [])
    .filter((m) => m && m.text)
    .slice(-24)
    .map((m, i) => `${i + 1}. ${m.role === "studio" ? "STUDIO" : "CUSTOMER"}: ${studioCleanText(m.text)}`)
    .filter((line) => line.split(": ").slice(1).join(": ").length > 0)
    .join("\n");

  const header =
`CUSTOMER-DIRECTED CHARM DESIGN — TRANSLATE, DON'T INVENT

The supplied image is the customer's chosen reference. Translate its subject
faithfully into this workshop's flat laser-cut charm language, then apply ONLY
the customer's requested changes:

CUSTOMER INSTRUCTIONS: ${clean || "(no change requested — translate the reference faithfully)"}
REFERENCE HANDLING: keep the recognizable subject and proportions; changes
beyond the instructions are forbidden.

• OUTER CONTOUR BRIEF: follow the reference subject's own recognizable silhouette, simplified into one bold, clean, manufacturable outline.
• ENGRAVING BRIEF: use only sparse, recognition-critical surface engraving — the few lines that make the subject read at charm size.
• INTENTIONAL CUTOUT BRIEF: use negative-space cutouts where they improve recognition or visual appeal; otherwise use none.`;

  const parts = [header];
  if (log) {
    parts.push(
`CUSTOMER DIRECTION LOG — the full conversation so far, oldest first.
Honour all of it; the last line is the newest request:
${log}`);
  }
  if (refine) {
    parts.push(
`REFINEMENT PASS: the second image is the customer's current version of this
charm. Keep it and change ONLY what the newest instruction asks for.`);
  }
  if (metal) {
    parts.push(`METAL: render in ${studioCleanText(metal)}. Metal choice affects tone only — never form.`);
  }
  parts.push(studioConstraintBlocks() || STUDIO_FALLBACK_CONSTRAINTS);
  return parts.join("\n\n");
}

/* ── the intake check ────────────────────────────────────────────────────
   Text only, no image output, no credit. The rejection reason is customer
   copy, so it is written in the studio's voice — and the word "AI" appears
   nowhere in it (design guide §9.7). */
const STUDIO_PRECHECK_PROMPT =
`You are the intake check for a handmade jewellery workshop that cuts flat metal charms.
Judge the attached photograph against these questions and answer with JSON only.

1. Is there ONE clear, identifiable main subject?
2. Can that subject be reduced to a flat silhouette with an integrated hanging hoop and still be recognisable?
3. Is it free of identifiable people and faces?
4. Is it free of minors, nudity and sexual content?
5. Is it free of obvious third-party trademarks, logos and licensed characters?
6. Is it sharp and well-lit enough to read the subject's outline?

Answer with exactly this shape and nothing else:
{"usable": true|false, "reason": "<one warm sentence, max 22 words, addressed to the customer, explaining what to try instead>", "subject": "<2-4 words>"}

Rules for "reason": only fill it when usable is false. Never mention models,
generation, prompts or automated checking. Never use the word "AI". Suggest a
concrete alternative, e.g. "A single pet in clear daylight works beautifully —
try a photo where the whole outline is visible."`;

async function studioVisionVerdict(img) {
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey) throw new Error("Missing GEMINI_API_KEY env var");
  const model = String(
    process.env.GEMINI_STUDIO_PRECHECK_MODEL ||
    process.env.GEMINI_CHARM_PREFLIGHT_MODEL ||
    DEFAULT_IMAGE_MODEL
  ).trim();
  let mime = img?.mime || "image/jpeg";
  if (!String(mime).startsWith("image/")) mime = "image/jpeg";

  const resp = await fetch(`${GEMINI_BASE}/models/${model}:generateContent`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-goog-api-key": apiKey },
    body: JSON.stringify({
      contents: [{
        role: "user",
        parts: [
          { text: STUDIO_PRECHECK_PROMPT },
          { inline_data: { mime_type: mime, data: Buffer.from(img.buffer).toString("base64") } },
        ],
      }],
      generationConfig: { temperature: 0, responseMimeType: "application/json" },
    }),
  });
  const data = await readUpstreamJson(resp, "gemini");
  const text = (data?.candidates?.[0]?.content?.parts || [])
    .map((p) => p?.text || "").join("").trim();
  const parsed = extractJsonObject(text);
  if (!parsed) return { usable: false, reason: "" };
  return parsed;
}

/* ── version doc: what drives the client's staged progress bar ───────────
   The storefront subscribes to customSessions/{sid}/versions/{n} and narrates
   `stage` back to the customer as it moves. Every write is best-effort — a
   Firestore hiccup must never fail a generation the customer already paid for. */
async function studioStage(vRef, patch) {
  try { await vRef.set(patch, { merge: true }); } catch (_e) {}
}

async function handleStudioPrecheck({ body, event, origin }) {
  const uid = await requireStudioUser(event);
  const cfg = await studioConfig();
  const p = String(body?.input_storage_path || "").trim();
  if (!p.startsWith(`custom-studio/${uid}/`)) {
    return studioJson(403, { ok: false, error: "forbidden" }, origin);
  }
  if (!(await studioBudgetOk("pc", uid, STUDIO_MAX_GENS_HOUR_UID))) {
    return studioJson(429, { ok: false, error: "rate_limited" }, origin);
  }

  const img = await storagePathToBuffer(p);
  let verdict;
  try {
    verdict = await studioVisionVerdict(img);
  } catch (err) {
    // Never block a customer because the checker was unavailable — the
    // generation applies the same doctrine again, server-side. The upload is
    // still metered, so a checker outage can't turn the allowance off.
    console.error("[studio] precheck unavailable:", err?.message || err);
    if (!(await studioMeterUpload(uid, cfg))) {
      return studioJson(402, { ok: false, error: "no_uploads_left" }, origin);
    }
    return studioJson(200, { ok: true, usable: true, degraded: true, reason: "" }, origin);
  }

  if (verdict.usable) {
    if (!(await studioMeterUpload(uid, cfg))) {
      return studioJson(402, { ok: false, error: "no_uploads_left" }, origin);
    }
  }
  return studioJson(200, {
    ok: true,
    usable: !!verdict.usable,
    verdict: verdict.usable ? "ok" : "rejected",
    subject: verdict.subject || null,
    reason: verdict.usable
      ? ""
      : (verdict.reason || "That image won't translate into a charm — try one with a single, clearly outlined subject."),
  }, origin);
}

/* A refusal decided before any work starts still has to reach the shopper, and
   from a background invocation the HTTP response cannot. This puts it on the
   version doc, which the studio is already watching. Best-effort by design:
   failing to report a failure must not become a second failure. */
/* Shopify's img_url filter returns a PROTOCOL-RELATIVE url — "//host/path",
   no scheme. A browser resolves that against the page it is on and it works
   everywhere on the storefront, which is why it rides all the way through the
   feed templates and into the saved session without anyone noticing. There is
   no page here. Node's fetch() has nothing to resolve against and rejects it
   outright with "Failed to parse URL from //…", killing the generation before
   a single pixel is drawn. One line, and it also repairs every session already
   saved with a bare "//" reference. */
function studioAbsoluteUrl(u) {
  const s = String(u || "").trim();
  if (s.startsWith("//")) return "https:" + s;
  if (s.startsWith("/"))  return "https://britesjewelry.com" + s;
  return s;
}

async function studioFailVersion(vRef, n, error) {
  try {
    await vRef.set({
      n: Number(n) || null,
      status: "failed", stage: "failed", error: String(error).slice(0, 300),
      failedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
  } catch (e) {
    console.error("[studio] could not record failure:", e?.message || e);
  }
}

async function handleStudioGenerate({ kind, body, event, origin }) {
  const uid = await requireStudioUser(event);
  const cfg = await studioConfig();
  const cost = Number(cfg.generateCost) || 1;
  const db = getDb();

  const sessionId = String(body?.sessionId || "").replace(/[^A-Za-z0-9_-]/g, "").slice(0, 60);
  if (!sessionId) return studioJson(400, { ok: false, error: "missing_session" }, origin);

  /* Ownership is checked FIRST, ahead of the rate limits, and the order is
     deliberate. Everything after this point is allowed to write a failure into
     the caller's own version doc — and it has to, because this function is
     invoked in the background: Netlify answers the browser with its own 202
     and the JSON we return here is read by nobody. A refusal that only exists
     as an HTTP status is a refusal the shopper never sees; they just watch a
     progress bar until the client's timeout gives up, with no reason given.
     Writing it to the version doc puts it on the same channel as every other
     outcome — the onSnapshot listener the studio is already holding open. */
  const sRef = db.collection(STUDIO_SESSIONS_COLL).doc(sessionId);
  const sSnap = await sRef.get();
  if (!sSnap.exists || sSnap.data().uid !== uid) {
    /* NOT reported to the version doc: this caller does not own this session,
       so writing into it would be handing them a channel into someone else's
       design. The HTTP 403 is the whole answer. */
    return studioJson(403, { ok: false, error: "forbidden" }, origin);
  }
  const session = sSnap.data();

  const n = Number(body?.versionNumber) || (Number(session.currentVersion) || 0) + 1;
  const vRef = sRef.collection("versions").doc(String(n));

  if (!(await studioBudgetOk("gen", uid, STUDIO_MAX_GENS_HOUR_UID)) ||
      !(await studioBudgetOk("genip", studioClientIp(event), STUDIO_MAX_GENS_HOUR_IP))) {
    await studioFailVersion(vRef, n, "rate_limited");
    return studioJson(429, { ok: false, error: "rate_limited" }, origin);
  }

  // 1) atomic debit, BEFORE any upstream call
  try {
    await studioDebit(uid, cost, sessionId);
  } catch (err) {
    if (err?.status === 402) {
      await studioFailVersion(vRef, n, "out_of_credits");
      return studioJson(402, { ok: false, error: "out_of_credits" }, origin);
    }
    throw err;
  }
  const instructions = studioCleanText(body?.instructions);
  const metal = studioCleanText(body?.metal || session.metal || "gold");
  const isRefine = kind === "custom_charm_refine" && n > 1;

  await studioStage(vRef, {
    n, status: "queued", stage: "queued",
    instructions, refineText: studioCleanText(body?.refineText), metal,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  });

  try {
    await studioStage(vRef, { status: "generating", stage: "planning" });

    // The reference: an uploaded object, or the chosen product's image fetched
    // SERVER-side rather than trusting a client-supplied URL.
    const ref = session.reference || {};
    let refImg = null;
    if (ref.type === "upload" && ref.path) {
      if (!String(ref.path).startsWith(`custom-studio/${uid}/`)) {
        throw new Error("reference outside your own storage");
      }
      refImg = await storagePathToBuffer(ref.path);
    } else if (ref.type === "catalog" && ref.item && ref.item.image) {
      const r = await fetch(studioAbsoluteUrl(ref.item.image));
      if (!r.ok) throw new Error(`reference image fetch ${r.status}`);
      refImg = {
        mime: r.headers.get("content-type") || "image/jpeg",
        buffer: Buffer.from(await r.arrayBuffer()),
      };
    }
    if (!refImg) throw new Error("no reference image on this session");

    const images = [
      { buffer: refImg.buffer, mime: refImg.mime, filename: filenameForMime("reference", refImg.mime) },
    ];
    let hasPrev = false;
    if (isRefine) {
      try {
        const prev = await storagePathToBuffer(
          `custom-studio/${uid}/uploads/designs/${sessionId}/v${n - 1}.png`
        );
        images.push({ buffer: prev.buffer, mime: prev.mime, filename: filenameForMime("previous", prev.mime) });
        hasPrev = true;
      } catch (_e) {
        // first refine after a restore, or the prior version is gone — the
        // direction log alone carries the context
      }
    }

    /* The customer's own markup of the previous drawing — circles, highlights
       and text bubbles composited client-side onto the drawing they refer to.
       It rides the request body (a background invocation caps the payload at
       256 KB, so the client ships a tightly compressed JPEG) and enters the
       generation as DIRECTION, never as subject: the role label and the lock
       both forbid its ink from appearing in the output. Only meaningful on a
       refine, and only when the previous drawing made it into the request —
       markup with no drawing under it would point at nothing. */
    let markupNotes = [];
    let markupZones = [];                      // the engraving instruction, as data
    let markupMode = null;                     // set ONLY when a markup image is accepted
    if (isRefine && hasPrev && typeof body?.markupImage === "string") {
      const mm = /^data:image\/(png|jpeg);base64,([A-Za-z0-9+/=]+)$/.exec(body.markupImage.trim());
      if (mm) {
        const buf = Buffer.from(mm[2], "base64");
        if (buf.length > 0 && buf.length <= 1.5 * 1024 * 1024) {
          images.push({ buffer: buf, mime: "image/" + mm[1], filename: "markup." + (mm[1] === "png" ? "png" : "jpg") });
          markupNotes = Array.isArray(body.markupNotes) ? body.markupNotes : [];
          markupZones = Array.isArray(body.markupZones) ? body.markupZones : [];
          markupMode = String(body.markupMode) === "exact" ? "exact" : "interpret";
        }
      }
    }

    // Same pipeline the Charm Maker runs: a text-only prompt audit, then
    // exactly ONE image generation with the backend's final geometry and
    // background override appended after every role label.
    /* The design step draws in B/W. No geometry or background policy is
       passed DELIBERATELY: both append photoreal-metal / black-background
       overrides that would outrank the line-art contract — the exact same
       reason the Charm Maker's own B/W call passes neither. The contract
       carries the geometry rules in drawing terms instead. */
    const refMode = body?.refMode === "exact" ? "exact"
                  : body?.refMode === "interpret" ? "interpret" : null;
    /* the fill law, as data, for a reference the customer drew themselves */
    const refZones = Array.isArray(body?.refZones) ? body.refZones.slice(0, 40) : [];
    const basePrompt = buildCustomerLineArtPrompt({
      instructions, thread: body?.thread, refine: isRefine, markupNotes, markupZones, markupMode,
      refMode, refZones,
    });
    let effectivePrompt = basePrompt;
    try {
      const audited = await auditCharmPromptPreflight({
        prompt: basePrompt,
        backgroundPolicy: null,
        geometryPolicy: null,
        editIntent: null,
        imageCount: images.length,
        imageRoles: "customer_lineart",
      });
      if (audited) effectivePrompt = audited;
    } catch (_e) { /* best effort — never blocks a paid generation */ }
    /* THE LAW IS NOT NEGOTIABLE, INCLUDING BY OUR OWN AUDITOR. The prompt
       goes through a preflight model asked to smooth out contradictions,
       which may return as little as a third of what it was handed.
       Everything else here can survive being rephrased; this cannot, because
       what it governs is whether there is a hole in somebody's jewellery.
       So it is re-stated verbatim afterwards if the audit dropped it. */
    if ((refMode || isRefine || markupMode || refZones.length) &&
        effectivePrompt.indexOf("AREA FILL — THREE MEANINGS") < 0) {
      /* THE MARK-UP'S LIST WINS. It is the more specific account of the two
         and it is the one the customer is looking at; preferring the
         reference's list here re-stated an older census of the same charm
         underneath the newer one. */
      effectivePrompt += "\n\n" + FILL_LAW +
        (zoneBlock(markupZones) || zoneBlock(refZones, "reference") || "");
    }

    await studioStage(vRef, { stage: "generating" });

    const studioModelConfig = resolveImageModel(
      process.env.GEMINI_STUDIO_IMAGE_MODEL || preferredCharmRenderModelId()
    );
    let outBuf = await callImageModelEdits({
      apiKey: apiKeyForImageModel(studioModelConfig),
      model: studioModelConfig.id,
      prompt: effectivePrompt,
      size: "2048x2048",
      quality: "high",
      output_format: "png",
      images,
      imageRoles: "customer_lineart",
      charmGeometryPolicy: null,
      backgroundPolicy: null,
    });

    await studioStage(vRef, { stage: "polishing" });

    // The design description travels inside the PNG, so it survives download,
    // the production queue and the customer's own copy.
    outBuf = embedPngTextMetadata(outBuf, {
      Description: `Custom Charm Studio production drawing — ${instructions || "customer reference"}. Version ${n}.`,
      CharmTitle: `Custom charm drawing v${n}`,
    });

    // NOTE the "uploads" segment: templates/cart.liquid and
    // snippets/ajax-cart-template.liquid render a clickable thumbnail for any
    // cart line property whose value contains "uploads" plus an image
    // extension, so the customer sees their design as a picture in the cart.
    const storagePath = `custom-studio/${uid}/uploads/designs/${sessionId}/v${n}.png`;
    const bucket = getBucket();
    const token = newDownloadToken();
    await bucket.file(storagePath).save(outBuf, {
      resumable: false,
      contentType: "image/png",
      metadata: { metadata: { firebaseStorageDownloadTokens: token, uid, sessionId, version: String(n) } },
    });
    const downloadURL = tokenDownloadURLFor(bucket.name, storagePath, token);

    await studioStage(vRef, {
      status: "done", stage: "done", storagePath, downloadURL,
      prompt: String(effectivePrompt).slice(0, 4000),
      model: studioModelConfig.id,
      doneAt: admin.firestore.FieldValue.serverTimestamp(),
    });
    try {
      await sRef.set({
        currentVersion: n,
        updatedAtServer: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
    } catch (_e) {}

    return studioJson(200, { ok: true, n, storagePath, downloadURL }, origin);
  } catch (err) {
    // A failed generation must never cost a credit.
    await studioRefund(uid, cost, sessionId);
    await studioStage(vRef, {
      status: "failed", stage: "failed",
      error: String(err?.message || err).slice(0, 300),
    });
    console.error("[studio] generate failed:", err?.message || err);
    return studioJson(500, { ok: false, error: "generation_failed" }, origin);
  }
}


/* ═══════════ DECLARED CUT-OUTS, PUNCHED DETERMINISTICALLY ════════════════
   The studio KNOWS every hole before a render is ever requested: the customer
   declared it and the production drawing carries it in blue. Asking a
   generative model to re-derive that from a coloured region is the one
   probabilistic step in an otherwise deterministic pipeline, and it fails in
   both directions — a declared hole rendered as engraving, and metal cut away
   that nobody declared. So the model is no longer asked. It renders metal;
   the openings are cut here, in arithmetic.

   THE ONLY HARD PART is telling a blue AREA from a blue LINE: the perimeter
   and the hoop are drawn in the same instruction blue, and punching those
   would delete the face of the charm. They separate by LOCAL THICKNESS
   against the drawing's own pen width, which is scale-free. Measured on a
   real studio drawing: three genuine fills at 1.9–3.2x the pen, four lines
   (silhouette, hoop, two strokes) at 0.6–1.0x. A gap that wide is a rule,
   not a tuned threshold.
   ===================================================================== */
let _sharpMod = null, _sharpTried = false;
function studioSharp() {
  if (_sharpTried) return _sharpMod;
  _sharpTried = true;
  /* NEVER LOAD-BEARING. Sharp is a native module and this repo has watched it
     fail to package before. If it is not there the render still ships — it is
     simply recorded as unverified, which is strictly better than a charm the
     customer cannot buy. */
  try { _sharpMod = require("sharp"); }
  catch (err) {
    console.error("[studio] sharp unavailable — cut-out punch disabled:", err?.message || err);
    _sharpMod = null;
  }
  return _sharpMod;
}

function studioBlueMask(px, n, stride) {
  const m = new Uint8Array(n);
  for (let i = 0, p = 0; i < n; i++, p += stride) {
    const r = px[p], g = px[p + 1], b = px[p + 2];
    /* the cut-out blue is now a LIGHT cyan-blue, chosen so it cannot be read
       as black — which means its green channel is high and the old margin of
       40 would have missed it wherever it feathered into the paper */
    if (b - (r > g ? r : g) > 28 && b > 120) m[i] = 1;
  }
  return m;
}

/* 4-connected components, iterative — recursion blows the stack on a 2K image
   long before it finds anything interesting. */
function studioLabel(mask, w, h) {
  const labels = new Int32Array(w * h);
  const stack = new Int32Array(w * h);
  let count = 0;
  for (let s = 0; s < w * h; s++) {
    if (!mask[s] || labels[s]) continue;
    count++;
    let top = 0;
    stack[top++] = s; labels[s] = count;
    while (top) {
      const i = stack[--top], x = i % w, y = (i / w) | 0;
      if (x > 0     && mask[i - 1] && !labels[i - 1]) { labels[i - 1] = count; stack[top++] = i - 1; }
      if (x < w - 1 && mask[i + 1] && !labels[i + 1]) { labels[i + 1] = count; stack[top++] = i + 1; }
      if (y > 0     && mask[i - w] && !labels[i - w]) { labels[i - w] = count; stack[top++] = i - w; }
      if (y < h - 1 && mask[i + w] && !labels[i + w]) { labels[i + w] = count; stack[top++] = i + w; }
    }
  }
  return { labels, count };
}

function studioLargestComponent(mask, w, h) {
  const { labels, count } = studioLabel(mask, w, h);
  if (!count) return mask;
  const size = new Float64Array(count + 1);
  for (let i = 0; i < w * h; i++) if (labels[i]) size[labels[i]]++;
  let best = 1;
  for (let L = 2; L <= count; L++) if (size[L] > size[best]) best = L;
  const out = new Uint8Array(w * h);
  for (let i = 0; i < w * h; i++) if (labels[i] === best) out[i] = 1;
  return out;
}

function studioDilate(mask, w, h, r) {
  let a = mask;
  for (let k = 0; k < r; k++) {
    const b = new Uint8Array(w * h);
    for (let y = 0; y < h; y++) {
      for (let x = 0; x < w; x++) {
        const i = y * w + x;
        if (a[i] || (x && a[i - 1]) || (x < w - 1 && a[i + 1]) ||
            (y && a[i - w]) || (y < h - 1 && a[i + w])) b[i] = 1;
      }
    }
    a = b;
  }
  return a;
}

/* 3-4 chamfer distance, two passes. Divided by three it is within a few
   percent of Euclidean — far more precision than a RATIO needs. */
function studioChamfer(mask, w, h) {
  const d = new Float32Array(w * h);
  for (let i = 0; i < w * h; i++) d[i] = mask[i] ? 1e9 : 0;
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) {
    const i = y * w + x; if (!mask[i]) continue;
    let v = d[i];
    if (y > 0)              v = Math.min(v, d[i - w] + 3);
    if (y > 0 && x > 0)     v = Math.min(v, d[i - w - 1] + 4);
    if (y > 0 && x < w - 1) v = Math.min(v, d[i - w + 1] + 4);
    if (x > 0)              v = Math.min(v, d[i - 1] + 3);
    d[i] = v;
  }
  for (let y = h - 1; y >= 0; y--) for (let x = w - 1; x >= 0; x--) {
    const i = y * w + x; if (!mask[i]) continue;
    let v = d[i];
    if (y < h - 1)              v = Math.min(v, d[i + w] + 3);
    if (y < h - 1 && x < w - 1) v = Math.min(v, d[i + w + 1] + 4);
    if (y < h - 1 && x > 0)     v = Math.min(v, d[i + w - 1] + 4);
    if (x < w - 1)              v = Math.min(v, d[i + 1] + 3);
    d[i] = v;
  }
  for (let i = 0; i < w * h; i++) d[i] /= 3;
  return d;
}

/* ── THE PEN, MEASURED ON EVERY MARK ──────────────────────────────────────
   This used to take the pen from the LONGEST component of the colour being
   tested, on the reasoning that the longest blue thing is the silhouette and a
   silhouette is one pen wide. That held only while the silhouette was drawn in
   blue. On a drawing whose perimeter is BLACK there is no blue line to
   calibrate against, so the longest blue component is a FILL — the estimate
   came back five times too thick and every genuine cut-out was dismissed as a
   line. Measured on a real drawing: three fills at 7.7-11.0px against a "pen"
   of 11.0, so the detector found nothing at all.

   The pen is now the MEDIAN thickness of every inked pixel in the drawing,
   whatever its colour. Line work always dominates a production drawing by
   pixel count, so the median lands on the pen and is unmoved by how many fills
   there are or what colour anything is: it measured 2.0px on both a
   blue-silhouette drawing and a black-silhouette one. */
function studioPenWidth(px, w, h) {
  const n = w * h;
  const ink = new Uint8Array(n);
  for (let i = 0; i < n; i++) if (!studioIsBg(px, i * 3)) ink[i] = 1;
  const d = studioChamfer(ink, w, h);
  const v = [];
  for (let i = 0; i < n; i++) if (ink[i]) v.push(d[i]);
  if (!v.length) return 1;
  v.sort((a, b) => a - b);
  return Math.max(1, v[v.length >> 1]);
}

function studioCutRegions(mask, w, h, minRatio, penIn) {
  /* 2.5x the pen: a stroke reaches one pen, a fill reaches many. Measured
     spread on real drawings is fills 3.9-5.5x against lines at 1.0-1.6x. */
  const ratio0 = minRatio || 2.5;
  const { labels, count } = studioLabel(mask, w, h);
  const cut = new Uint8Array(w * h);
  if (!count) return { cut, areas: 0, lines: 0, pen: 0, cutPx: 0 };
  const dist = studioChamfer(mask, w, h);
  const size = new Float64Array(count + 1), maxd = new Float64Array(count + 1);
  for (let i = 0; i < w * h; i++) {
    const L = labels[i]; if (!L) continue;
    size[L]++; if (dist[i] > maxd[L]) maxd[L] = dist[i];
  }
  const pen = penIn || 1;
  const minPx = Math.max(24, Math.round(w * h * 0.00002));
  const keep = new Uint8Array(count + 1);
  let areas = 0, lines = 0, cutPx = 0;
  for (let L = 1; L <= count; L++) {
    if (maxd[L] / pen >= ratio0 && size[L] >= minPx) { keep[L] = 1; areas++; }
    else lines++;
  }
  for (let i = 0; i < w * h; i++) if (labels[i] && keep[labels[i]]) { cut[i] = 1; cutPx++; }
  return { cut, areas, lines, pen, cutPx };
}

/* Everything the frame cannot reach across background. Enclosed background is
   NOT reached, so this is the charm's outer boundary with its holes filled —
   which is exactly what makes a drawing and a photograph comparable. */
function studioOuterFace(isWall, w, h) {
  const n = w * h;
  const outside = new Uint8Array(n), stack = new Int32Array(n);
  let top = 0;
  const push = (i) => { if (!outside[i] && !isWall[i]) { outside[i] = 1; stack[top++] = i; } };
  for (let x = 0; x < w; x++) { push(x); push((h - 1) * w + x); }
  for (let y = 0; y < h; y++) { push(y * w); push(y * w + w - 1); }
  while (top) {
    const i = stack[--top], x = i % w, y = (i / w) | 0;
    if (x > 0) push(i - 1);
    if (x < w - 1) push(i + 1);
    if (y > 0) push(i - w);
    if (y < h - 1) push(i + w);
  }
  const face = new Uint8Array(n);
  for (let i = 0; i < n; i++) face[i] = outside[i] ? 0 : 1;
  /* one continuous piece, by doctrine — so stray marks outside the charm
     (a label, a speck) cannot distort the bounding box the whole registration
     depends on */
  return studioLargestComponent(face, w, h);
}

function studioBBox(mask, w, h) {
  let x0 = w, y0 = h, x1 = -1, y1 = -1;
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) {
    if (!mask[y * w + x]) continue;
    if (x < x0) x0 = x; if (x > x1) x1 = x;
    if (y < y0) y0 = y; if (y > y1) y1 = y;
  }
  if (x1 < 0) return null;
  return { x0, y0, x1, y1, w: x1 - x0 + 1, h: y1 - y0 + 1 };
}

/* Sample into an N x N grid normalised to the mask's own bounding box, so two
   pictures of the same charm at different scales become comparable. */
function studioNormalise(mask, w, h, box, N) {
  const g = new Uint8Array(N * N);
  for (let j = 0; j < N; j++) {
    const sy = box.y0 + Math.min(box.h - 1, Math.floor((j + 0.5) * box.h / N));
    for (let i = 0; i < N; i++) {
      const sx = box.x0 + Math.min(box.w - 1, Math.floor((i + 0.5) * box.w / N));
      g[j * N + i] = mask[sy * w + sx];
    }
  }
  return g;
}

function studioIoU(a, b, n) {
  let inter = 0, uni = 0;
  for (let i = 0; i < n; i++) { const x = a[i], y = b[i]; if (x && y) inter++; if (x || y) uni++; }
  return uni ? inter / uni : 0;
}

const STUDIO_BG_MIN = 230;
const studioIsBg = (px, p) => px[p] > STUDIO_BG_MIN && px[p + 1] > STUDIO_BG_MIN && px[p + 2] > STUDIO_BG_MIN;

async function studioRawAt(sharp, buf, side) {
  const { data, info } = await sharp(buf)
    .resize(side, side, { fit: "inside" })
    .removeAlpha()
    .raw()
    .toBuffer({ resolveWithObject: true });
  return { px: data, w: info.width, h: info.height };
}


/* ── THE FOURTH INSTRUCTION: SOLID METAL, SAID OUT LOUD ────────────────────
   White meant two different things. Inside the charm it meant "untouched
   polished metal"; outside it meant "paper". And since the render moved to a
   white ground, a HOLE renders white too — so an enclosed white region ringed
   by a thin outline and a real opening became the same picture, and the model
   was left inferring which from context. It inferred wrong on two legs.

   Absence is the weakest signal you can hand a vision encoder, so metal stops
   being an absence. Every interior pixel that is not an instruction and not a
   declared hole is flooded with this tint before the drawing goes to the
   model. White then means exactly one thing: not metal.

   Green because it is the one hue the vocabulary does not use — measured
   against the other four classes its nearest neighbour is dE 103, where the
   closest existing pair (cut blue vs paper) is 58. The customer never sees
   it: this is applied to the RENDER'S INPUT only, never to the drawing filed
   in Storage or shown in the studio. */
const STUDIO_METAL_TINT = [0x4d, 0xff, 0x4d];

const studioIsBlack = (px, p) => px[p] < 90 && px[p + 1] < 90 && px[p + 2] < 90;

/* every region the drawing declares, at the drawing's own resolution, with
   sample points kept so the same regions can be interrogated in the render */
async function studioDrawingPlan(sharp, drawingBuf) {
  const { data: px, info } = await sharp(drawingBuf).removeAlpha().raw()
    .toBuffer({ resolveWithObject: true });
  const w = info.width, h = info.height, n = w * h;

  const ink = new Uint8Array(n);
  for (let i = 0; i < n; i++) if (!studioIsBg(px, i * 3)) ink[i] = 1;
  const face = studioOuterFace(studioDilate(ink, w, h, 2), w, h);
  const faceBox = studioBBox(face, w, h);

  const pen = studioPenWidth(px, w, h);
  const blue = studioBlueMask(px, n, 3);
  const { cut } = studioCutRegions(blue, w, h, 2.5, pen);

  /* black FILLS are engraving and must stay closed; black LINES are outlines
     and are not regions at all — the same thickness test that separates a
     cut-out from a cut edge separates them. */
  const black = new Uint8Array(n);
  for (let i = 0; i < n; i++) if (studioIsBlack(px, i * 3)) black[i] = 1;
  const { cut: blackAreas } = studioCutRegions(black, w, h, 2.5, pen);

  /* interior metal: inside the charm, not ink, not a declared hole. Ink is
     dilated so a region's own outline cannot leak it into its neighbour. */
  const inkWall = studioDilate(ink, w, h, 2);
  const metal = new Uint8Array(n);
  for (let i = 0; i < n; i++) if (face[i] && !inkWall[i] && !cut[i]) metal[i] = 1;

  const minPx = Math.max(200, Math.round(n * 0.0004));
  const regions = [];
  const collect = (mask, kind) => {
    const { labels, count } = studioLabel(mask, w, h);
    const size = new Float64Array(count + 1);
    for (let i = 0; i < n; i++) if (labels[i]) size[labels[i]]++;
    const pts = new Map();
    const step = 3;
    for (let y = 0; y < h; y += step) for (let x = 0; x < w; x += step) {
      const i = y * w + x, L = labels[i];
      if (!L || size[L] < minPx) continue;
      if (!pts.has(L)) pts.set(L, []);
      pts.get(L).push(i);
    }
    for (const [L, list] of pts) if (list.length >= 12) regions.push({ kind, px: size[L], pts: list });
  };
  collect(cut, "open");            // blue: MUST be a hole in the render
  collect(blackAreas, "closed");   // black fill: engraving, must stay metal
  collect(metal, "closed");        // untouched metal, must stay metal
  return { px, w, h, n, ink, face, faceBox, cut, pen, regions };
}

/* the tinted copy that goes to the model — the drawing in Storage is untouched */
async function studioTintMetal(sharp, plan) {
  const { px, w, h, n, face, ink, cut } = plan;
  const out = Buffer.from(px);
  const inkWall = studioDilate(ink, w, h, 1);
  const [tr, tg, tb] = STUDIO_METAL_TINT;
  let filled = 0;
  for (let i = 0; i < n; i++) {
    if (!face[i] || inkWall[i] || cut[i]) continue;
    const p = i * 3;
    if (!studioIsBg(px, p)) continue;
    out[p] = tr; out[p + 1] = tg; out[p + 2] = tb;
    filled++;
  }
  const buf = await sharp(out, { raw: { width: w, height: h, channels: 3 } }).png().toBuffer();
  return { buf, filled };
}

/* ── THE PUNCH ────────────────────────────────────────────────────────────
   Returns the render unchanged plus a verdict whenever anything is uncertain.
   The three outcomes, in the order they are decided:

     undeclared_cut  the render removed metal nobody asked to remove. This is
                     the Image-1 failure, and it is NOT patchable — the metal
                     is gone. Reject so the render is retried.
     unaligned       the render's silhouette does not match the drawing's, so
                     no coordinate mapped from one to the other can be
                     trusted. Punching blind would be worse than not punching.
     punched         the declared cut-outs were missing and have been cut.
   ========================================================================= */
async function studioPunchCutouts(renderBuf, drawingBuf, plan) {
  const sharp = studioSharp();
  const report = { verified: false, reason: "", declaredCuts: 0, punchedPx: 0,
                   alignment: 0, openFrac: 0, regionsOk: 0, regionsBad: 0, worst: "" };
  if (!sharp) { report.reason = "sharp_unavailable"; return { buf: renderBuf, report }; }
  try {
    const d = plan;
    const r0 = await sharp(renderBuf).removeAlpha().raw().toBuffer({ resolveWithObject: true });
    const rw = r0.info.width, rh = r0.info.height, rpx = r0.data;
    const rInk = new Uint8Array(rw * rh);
    for (let i = 0; i < rw * rh; i++) if (!studioIsBg(rpx, i * 3)) rInk[i] = 1;
    const rFace = studioOuterFace(rInk, rw, rh);
    const rBox = studioBBox(rFace, rw, rh);
    if (!d.faceBox || !rBox) { report.reason = "no_subject"; return { buf: renderBuf, report }; }

    /* registration first: every region test below maps a drawing coordinate
       into the render through these two boxes, so if the silhouettes disagree
       there is nothing worth measuring and nothing safe to punch. */
    const N = 192;
    report.alignment = studioIoU(studioNormalise(d.face, d.w, d.h, d.faceBox, N),
                                 studioNormalise(rFace, rw, rh, rBox, N), N * N);
    if (report.alignment < 0.90) { report.reason = "unaligned"; return { buf: renderBuf, report }; }

    const toRender = (i) => {
      const x = i % d.w, y = (i / d.w) | 0;
      const u = (x - d.faceBox.x0) / d.faceBox.w, v = (y - d.faceBox.y0) / d.faceBox.h;
      const rx = Math.round(rBox.x0 + u * rBox.w), ry = Math.round(rBox.y0 + v * rBox.h);
      if (rx < 0 || ry < 0 || rx >= rw || ry >= rh) return -1;
      return ry * rw + rx;
    };

    /* ── EVERY REGION, NOT THE TOTAL ──────────────────────────────────────
       The old test compared total open AREA against total declared area with
       an 8-point allowance. Measured on a real failure — seven openings, 4.6%
       of the face, against three declared at 1% — that test passes: the two
       wrongly cut legs hide inside the allowance. Area cannot see WHERE a
       hole is. So each declared region is now interrogated on its own: a blue
       region must read open, and a black fill or a metal region must read
       closed. One region in the wrong state fails the render. */
    let bad = 0, ok = 0, worst = "";
    for (const reg of d.regions) {
      let seen = 0, open = 0;
      for (const i of reg.pts) {
        const j = toRender(i);
        if (j < 0) continue;
        seen++;
        if (studioIsBg(rpx, j * 3)) open++;
      }
      if (seen < 8) continue;
      const frac = open / seen;
      const good = reg.kind === "open" ? frac > 0.5 : frac < 0.35;
      if (good) ok++;
      else {
        bad++;
        if (!worst) worst = reg.kind === "open"
          ? `declared cut-out came back solid (${Math.round(frac * 100)}% open)`
          : `metal came back cut (${Math.round(frac * 100)}% open)`;
      }
    }
    report.regionsOk = ok; report.regionsBad = bad; report.worst = worst;
    report.declaredCuts = d.regions.filter((x) => x.kind === "open").length;

    let faceN = 0, openN = 0;
    for (let i = 0; i < rw * rh; i++) { if (!rFace[i]) continue; faceN++; if (studioIsBg(rpx, i * 3)) openN++; }
    report.openFrac = faceN ? openN / faceN : 0;

    /* A region the drawing declared as metal that came back open is the one
       failure compositing cannot undo — the metal is gone. Reject; never
       repair. A declared cut-out that came back solid IS repairable, and is
       repaired below, so it does not reject. */
    const cutIntoMetal = d.regions.some((reg, k) => {
      if (reg.kind !== "closed") return false;
      let seen = 0, open = 0;
      for (const i of reg.pts) { const j = toRender(i); if (j < 0) continue; seen++; if (studioIsBg(rpx, j * 3)) open++; }
      return seen >= 8 && open / seen >= 0.35;
    });
    if (cutIntoMetal) { report.reason = "undeclared_cut"; return { buf: renderBuf, report }; }

    if (!report.declaredCuts) { report.verified = true; report.reason = "no_cuts_declared"; return { buf: renderBuf, report }; }

    /* punch at FULL render resolution: the customer's charm is the artefact */
    const full = await sharp(renderBuf).removeAlpha().raw().toBuffer({ resolveWithObject: true });
    const fw = full.info.width, fh = full.info.height, fpx = full.data;
    const sx = fw / rw, sy = fh / rh;
    const fx0 = rBox.x0 * sx, fy0 = rBox.y0 * sy, fbw = rBox.w * sx, fbh = rBox.h * sy;
    const corners = [0, (fw - 1) * 3, (fh - 1) * fw * 3, ((fh - 1) * fw + fw - 1) * 3];
    const g0 = Math.round(corners.reduce((a, p) => a + fpx[p], 0) / 4);
    const g1 = Math.round(corners.reduce((a, p) => a + fpx[p + 1], 0) / 4);
    const g2 = Math.round(corners.reduce((a, p) => a + fpx[p + 2], 0) / 4);
    let punched = 0;
    for (let y = Math.max(0, Math.floor(fy0)); y < Math.min(fh, Math.ceil(fy0 + fbh)); y++) {
      const v = (y - fy0) / fbh;
      const dy = d.faceBox.y0 + Math.min(d.faceBox.h - 1, Math.floor(v * d.faceBox.h));
      if (dy < 0 || dy >= d.h) continue;
      for (let x = Math.max(0, Math.floor(fx0)); x < Math.min(fw, Math.ceil(fx0 + fbw)); x++) {
        const u = (x - fx0) / fbw;
        const dx = d.faceBox.x0 + Math.min(d.faceBox.w - 1, Math.floor(u * d.faceBox.w));
        if (!d.cut[dy * d.w + dx]) continue;
        const p = (y * fw + x) * 3;
        fpx[p] = g0; fpx[p + 1] = g1; fpx[p + 2] = g2;
        punched++;
      }
    }
    report.punchedPx = punched;
    report.verified = bad === 0;
    report.reason = punched ? "punched" : "nothing_to_punch";
    const out = await sharp(fpx, { raw: { width: fw, height: fh, channels: 3 } }).png().toBuffer();
    return { buf: out, report };
  } catch (err) {
    console.error("[studio] cut-out punch failed:", err?.message || err);
    report.reason = "punch_error";
    return { buf: renderBuf, report };
  }
}

/* ── kind: custom_charm_render ────────────────────────────────────────────
 * The approved drawing becomes the charm: the Charm Maker's gold→line-art
 * conversion run in reverse, with the drawing as structural truth. Costs one
 * credit like any generation, debited atomically before the upstream call and
 * refunded on failure. Progress and outcome are written to the SAME version
 * doc the drawing lives on — renderStatus/renderStage/renderURL — so the
 * onSnapshot listener the studio already holds open streams the render too,
 * and the drawing's own status:"done" is never disturbed. Invoked in the
 * background, so refusals decided before any work starts are written to the
 * doc as well: an HTTP status from a background invocation reaches nobody.
 * ===================================================================== */
async function studioFailRender(vRef, error, renderRunId) {
  try {
    await vRef.set({
      renderStatus: "failed", renderStage: "failed",
      renderRunId: String(renderRunId || ""),
      renderError: String(error).slice(0, 300),
    }, { merge: true });
  } catch (e) {
    console.error("[studio] could not record render failure:", e?.message || e);
  }
}

async function handleStudioRender({ body, event, origin }) {
  const uid = await requireStudioUser(event);
  const cfg = await studioConfig();
  const cost = Number(cfg.generateCost) || 1;
  const db = getDb();

  /* ── QUALITY: THE CUSTOMER'S CHOICE OF RENDERER ───────────────────────────
     "low" is the Gemini renderer the studio has always used, at the standard
     one-credit price. "high" routes the same drawing, the same prompt and the
     same zone census to OpenAI's image model for two credits. Anything the
     browser sends that is not exactly "high" is treated as low: a garbled
     value must never silently cost a customer double. */
  const quality = String(body?.quality || "low").trim().toLowerCase() === "high" ? "high" : "low";
  const highCost = Math.max(cost, Number(cfg.renderHighCost) || cost * 2);
  const renderCost = quality === "high" ? highCost : cost;

  const sessionId = String(body?.sessionId || "").replace(/[^A-Za-z0-9_-]/g, "").slice(0, 60);
  if (!sessionId) return studioJson(400, { ok: false, error: "missing_session" }, origin);
  const renderRunId = String(body?.renderRunId || ("rr_" + Date.now() + "_" + Math.random().toString(36).slice(2, 10)))
    .replace(/[^A-Za-z0-9_-]/g, "").slice(0, 80);

  /* Ownership first — a stranger gets the 403 and NOTHING written into the
     doc, exactly as in generate: a failure channel into someone else's
     design would be a way in. */
  const sRef = db.collection(STUDIO_SESSIONS_COLL).doc(sessionId);
  const sSnap = await sRef.get();
  if (!sSnap.exists || sSnap.data().uid !== uid) {
    return studioJson(403, { ok: false, error: "forbidden" }, origin);
  }
  const session = sSnap.data();

  const n = Number(body?.versionNumber) || Number(session.currentVersion) || 0;
  if (!n || n < 1) return studioJson(400, { ok: false, error: "missing_version" }, origin);
  const vRef = sRef.collection("versions").doc(String(n));
  const vSnap = await vRef.get();
  if (!vSnap.exists || vSnap.data().status !== "done") {
    return studioJson(409, { ok: false, error: "version_not_ready" }, origin);
  }

  /* ── PROVE THE RENDERER EXISTS BEFORE TAKING THE MONEY ───────────────────
     resolveImageModel throws on an unknown id and apiKeyForImageModel throws
     when the provider's key is not set. Discovering either AFTER the debit
     means the customer watches their balance drop, waits for a render that
     was never possible, and gets it back only via the refund path. Both are
     settled here, before a credit moves, and the refusal is written to the
     doc in words the studio can show. */
  let studioModelConfig = null;
  try {
    studioModelConfig = resolveImageModel(
      quality === "high"
        ? (cfg.renderHighModel || "gpt-image-2")
        : (process.env.GEMINI_STUDIO_IMAGE_MODEL || preferredCharmRenderModelId())
    );
    apiKeyForImageModel(studioModelConfig);
  } catch (err) {
    console.error("[studio] render model unavailable:", err?.message || err);
    await studioFailRender(vRef, quality === "high" ? "high_unavailable" : "render_unavailable", renderRunId);
    return studioJson(503, { ok: false, error: "render_unavailable" }, origin);
  }

  /* Renders share the generation budgets — a render IS a generation as far
     as the upstream model is concerned. */
  if (!(await studioBudgetOk("gen", uid, STUDIO_MAX_GENS_HOUR_UID)) ||
      !(await studioBudgetOk("genip", studioClientIp(event), STUDIO_MAX_GENS_HOUR_IP))) {
    await studioFailRender(vRef, "rate_limited", renderRunId);
    return studioJson(429, { ok: false, error: "rate_limited" }, origin);
  }

  try {
    await studioDebit(uid, renderCost, sessionId);
  } catch (err) {
    if (err?.status === 402) {
      await studioFailRender(vRef, "out_of_credits", renderRunId);
      return studioJson(402, { ok: false, error: "out_of_credits" }, origin);
    }
    throw err;
  }

  const metal = studioCleanText(body?.metal || session.metal || "gold");

  const cutMode = String(cfg.renderCutCheck || "off").trim().toLowerCase();
  try {
    await vRef.set({ renderStatus: "rendering", renderStage: "planning", renderMetal: metal,
                     renderQuality: quality, renderModel: studioModelConfig.id,
                     renderCost, renderRunId, renderError: null }, { merge: true });

    const bwPath = `custom-studio/${uid}/uploads/designs/${sessionId}/v${n}.png`;
    const bw = await storagePathToBuffer(bwPath);

    /* ── THE MODEL IS A SHADER, NOT A DRAUGHTSMAN ─────────────────────────
       Topology — what is metal and what is a hole — is known exactly from the
       drawing before this request is made, so it is not left to the model to
       infer. Interior metal is flooded with an explicit tint (nothing is meant
       by absence any more), the declared holes are punched afterwards, and a
       render that cut into metal is rejected rather than repaired. What the
       model is asked for is what gold looks like. */
    const sharpMod = studioSharp();
    let studioPlan = null, drawingForModel = bw.buffer;
    if (sharpMod && cutMode !== "off") {
      try {
        studioPlan = await studioDrawingPlan(sharpMod, bw.buffer);
        const tinted = await studioTintMetal(sharpMod, studioPlan);
        if (tinted.filled > 0) drawingForModel = tinted.buf;
        await vRef.set({ renderMetalFilled: tinted.filled, renderRunId }, { merge: true });
      } catch (e) {
        console.error("[studio] metal tint skipped:", e?.message || e);
        studioPlan = null; drawingForModel = bw.buffer;
      }
    }
    const images = [
      { buffer: drawingForModel, mime: "image/png", filename: "drawing.png" },
    ];

    const renderZones = Array.isArray(body?.zones) ? body.zones.slice(0, 40) : [];
    const effectivePrompt = buildLineArtToCharmPrompt({ metal, zones: renderZones });

    await vRef.set({ renderStage: "rendering", renderRunId }, { merge: true });

    /* studioModelConfig was resolved and its key proved above, before the
       debit. Nothing about the request changes with the renderer: the same
       drawing, the same prompt and the same zone census go to both, because
       what "high" buys is a different renderer, not different instructions —
       and callImageModelEdits already mirrors the lineart_to_charm role
       labels and the geometry/background policy into the OpenAI prompt. */
    let outBuf = await callImageModelEdits({
      apiKey: apiKeyForImageModel(studioModelConfig),
      model: studioModelConfig.id,
      prompt: effectivePrompt,
      size: "2048x2048",
      quality: "high",
      output_format: "png",
      images,
      imageRoles: "lineart_to_charm",
      charmGeometryPolicy: "flat_integrated_eyelet",
      /* ── ONE VOICE ABOUT THE BACKGROUND ──────────────────────────────────
         backgroundPolicy: "solid_black" appends a block headed "BACKEND-
         ENFORCED, OVERRIDES EVERYTHING ABOVE" as the LITERAL LAST WORDS of
         the prompt — after STUDIO_RENDER_FINISH, which had already declared
         itself the final authority and asked for a white ground. Two blocks
         each claiming the last word, in opposition.

         It cost more than a background. That block also says holes "show
         pure black through them", and a black hole on a dark ground is
         indistinguishable from a deeply engraved area — so the one
         distinction the fill law exists to protect, hole versus engraving,
         collapsed, and every recess was pulled darker to match a scene the
         model believed was black. The studio render's presentation is
         defined ONCE, in STUDIO_RENDER_FINISH. Nothing overrides it.

         The geometry policy stays: it contradicts nothing. */
      backgroundPolicy: null,
    });

    await vRef.set({ renderStage: "polishing", renderRunId }, { merge: true });

    /* ── THE CUT CHECK IS PARKED ──────────────────────────────────────────
       The punch is correct on the cases it was proven against, but the fill
       identification behind it is still being corrected — and a check that is
       sometimes wrong is worse than no check, because it edits a charm the
       customer is paying for. So it ships OFF, and the customer sees exactly
       what they saw before it existed: no verdict, no rejected renders, no
       pixels touched.

       One value in config/customStudio, no deploy, three settings:
         "off"      what ships. The scan does not run.
         "observe"  the scan runs and files its numbers on the version doc,
                    but the image is returned untouched and nothing is ever
                    rejected. This is how the identification gets tuned
                    against real traffic without a customer ever paying for
                    it — turn it on when we start correcting the fills.
         "enforce"  punch declared cut-outs, reject undeclared ones. */
    if (cutMode === "observe" || cutMode === "enforce") {
      const punch = await studioPunchCutouts(outBuf, bw.buffer, studioPlan);
      if (cutMode === "enforce") {
        outBuf = punch.buf;
        if (punch.report.reason === "undeclared_cut") {
          const e = new Error("cut_check_undeclared");
          e.studioReport = punch.report;
          throw e;
        }
      }
      await vRef.set({
        renderCutMode: cutMode,
        renderCutsDeclared: punch.report.declaredCuts,
        renderCutsPunched: cutMode === "enforce" ? punch.report.punchedPx : 0,
        renderCutsWouldPunch: punch.report.punchedPx,
        renderAlignment: Math.round(punch.report.alignment * 1000) / 1000,
        renderOpenFraction: Math.round(punch.report.openFrac * 1000) / 1000,
        renderVerified: cutMode === "enforce" && !!punch.report.verified,
        renderRegionsOk: punch.report.regionsOk,
        renderRegionsBad: punch.report.regionsBad,
        renderWorstRegion: punch.report.worst || "",
        renderCheck: punch.report.reason,
        renderRunId,
      }, { merge: true });
    }

    outBuf = embedPngTextMetadata(outBuf, {
      Description: `Custom Charm Studio charm — rendered from approved drawing v${n} (${metal}).`,
      CharmTitle: `Custom charm v${n}`,
    });

    /* Same "uploads" segment as the drawing, for the same reason: both cart
       templates render any property URL containing uploads+extension as a
       clickable thumbnail, and BOTH halves of the pair go into the cart. */
    const renderPath = `custom-studio/${uid}/uploads/designs/${sessionId}/v${n}-charm.png`;
    const bucket = getBucket();
    const token = newDownloadToken();
    await bucket.file(renderPath).save(outBuf, {
      resumable: false,
      contentType: "image/png",
      metadata: { metadata: { firebaseStorageDownloadTokens: token, uid, sessionId, version: String(n), render: "1" } },
    });
    const renderURL = tokenDownloadURLFor(bucket.name, renderPath, token);

    await vRef.set({
      renderStatus: "done", renderStage: "done", renderRunId,
      renderURL, renderPath, renderMetal: metal,
      renderQuality: quality, renderModel: studioModelConfig.id, renderCost,
      renderedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });

    return studioJson(200, { ok: true, n, renderURL, renderPath, renderMetal: metal,
                             renderQuality: quality, renderModel: studioModelConfig.id,
                             renderCost, renderRunId }, origin);
  } catch (err) {
    // A failed render must never cost a credit — including a double-priced one.
    await studioRefund(uid, renderCost, sessionId);
    await studioFailRender(vRef, String(err?.message || err), renderRunId);
    console.error("[studio] render failed:", err?.message || err);
    return studioJson(500, { ok: false, error: "render_failed" }, origin);
  }
}

async function handleStudioSessionStatus({ body, event, origin }) {
  const uid = await requireStudioUser(event);
  const db = getDb();
  const sessionId = String(body?.sessionId || "").replace(/[^A-Za-z0-9_-]/g, "").slice(0, 60);
  const n = String(Number(body?.versionNumber) || 1);
  const sRef = db.collection(STUDIO_SESSIONS_COLL).doc(sessionId);
  const sSnap = await sRef.get();
  if (!sSnap.exists || sSnap.data().uid !== uid) {
    return studioJson(403, { ok: false, error: "forbidden" }, origin);
  }
  const v = await sRef.collection("versions").doc(n).get();
  return studioJson(200, {
    ok: true,
    currentVersion: sSnap.data().currentVersion || 0,
    version: v.exists ? Object.assign({ n: Number(n) }, v.data()) : null,
  }, origin);
}

/* ═══════════ FILING A DRAWING THE STUDIO MADE ITSELF ═════════════════════
   In "exactly as drawn" the browser composes the production drawing from the
   customer's own vector items — no model is called and nothing is
   interpreted. But the render step does not accept a picture: it reads a
   FIXED Storage path, `.../designs/<session>/v<n>.png`, and refuses any
   version whose Firestore doc is not marked done. A composed drawing that
   lived anywhere else was therefore invisible to it — the render answered
   409 version_not_ready and the customer watched a progress bar to its
   timeout with no reason given.

   So the composed PNG is filed exactly where a generated one is filed, and
   its version doc is written in the same shape. Everything downstream — the
   render, the cart thumbnail, My designs, the order webhook — then treats it
   as what it is: version n of this design.

   IT COSTS NO GENERATION CREDIT. No model ran, so there is nothing to
   charge for; the upload allowance the browser already spent is the whole
   price. The rate limits still apply, because writing megabytes into
   Storage is worth bounding whoever asks for it. */
async function handleStudioCompose({ body, event, origin }) {
  const uid = await requireStudioUser(event);
  const db = getDb();

  const sessionId = String(body?.sessionId || "").replace(/[^A-Za-z0-9_-]/g, "").slice(0, 60);
  if (!sessionId) return studioJson(400, { ok: false, error: "missing_session" }, origin);

  const sRef = db.collection(STUDIO_SESSIONS_COLL).doc(sessionId);
  const sSnap = await sRef.get();
  if (!sSnap.exists || sSnap.data().uid !== uid) {
    return studioJson(403, { ok: false, error: "forbidden" }, origin);
  }
  const session = sSnap.data();

  const n = Number(body?.versionNumber) || (Number(session.currentVersion) || 0) + 1;
  if (!n || n < 1 || n > 400) return studioJson(400, { ok: false, error: "bad_version" }, origin);
  const vRef = sRef.collection("versions").doc(String(n));

  if (!(await studioBudgetOk("gen", uid, STUDIO_MAX_GENS_HOUR_UID)) ||
      !(await studioBudgetOk("genip", studioClientIp(event), STUDIO_MAX_GENS_HOUR_IP))) {
    return studioJson(429, { ok: false, error: "rate_limited" }, origin);
  }

  let img;
  try {
    img = dataUrlToBuffer(String(body?.dataUrl || ""));
  } catch (_e) {
    return studioJson(400, { ok: false, error: "bad_image" }, origin);
  }
  if (!/^image\/png$/i.test(img.mime)) return studioJson(400, { ok: false, error: "png_only" }, origin);
  if (!img.buffer.length || img.buffer.length > 12 * 1024 * 1024) {
    return studioJson(413, { ok: false, error: "image_too_large" }, origin);
  }

  try {
    /* the same path the generator writes, "uploads" segment included: the
       cart template keys its thumbnail off that word */
    const storagePath = `custom-studio/${uid}/uploads/designs/${sessionId}/v${n}.png`;
    const bucket = getBucket();
    const token = newDownloadToken();
    await bucket.file(storagePath).save(img.buffer, {
      resumable: false,
      contentType: "image/png",
      metadata: { metadata: { firebaseStorageDownloadTokens: token, uid, sessionId,
                              version: String(n), composed: "1" } },
    });
    const downloadURL = tokenDownloadURLFor(bucket.name, storagePath, token);

    await studioStage(vRef, {
      status: "done", stage: "done", storagePath, downloadURL,
      composed: true,
      model: "studio-composer",
      prompt: "Composed locally from the customer's own vector items — exactly as drawn.",
      doneAt: admin.firestore.FieldValue.serverTimestamp(),
    });
    try {
      await sRef.set({
        currentVersion: n,
        updatedAtServer: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
    } catch (_e) {}

    return studioJson(200, { ok: true, n, storagePath, downloadURL }, origin);
  } catch (err) {
    console.error("[studio] compose file:", err);
    return studioJson(500, { ok: false, error: "server_error" }, origin);
  }
}

async function handleStudioKind({ kind, body, event }) {
  const origin = (event?.headers && (event.headers.origin || event.headers.Origin)) || "";
  try {
    if (kind === "custom_charm_precheck") return await handleStudioPrecheck({ body, event, origin });
    if (kind === "custom_charm_generate" || kind === "custom_charm_refine") {
      return await handleStudioGenerate({ kind, body, event, origin });
    }
    if (kind === "custom_charm_compose") return await handleStudioCompose({ body, event, origin });
    if (kind === "custom_charm_render") return await handleStudioRender({ body, event, origin });
    if (kind === "custom_session_status") return await handleStudioSessionStatus({ body, event, origin });
    return studioJson(400, { ok: false, error: "unknown_kind" }, origin);
  } catch (err) {
    if (err?.status) return studioJson(err.status, { ok: false, error: err.message }, origin);
    console.error("[studio]", kind, err);
    return studioJson(500, { ok: false, error: "server_error" }, origin);
  }
}

exports.handler = async (event) => {
  // Top-level safety net. Any error inside the handler that isn't
  // caught by the inner try/catch blocks would otherwise bubble up to
  // Netlify and surface as an opaque "Internal Error. ID: xxx" 500
  // with no useful message. This wrapper guarantees we always return
  // a JSON response with the actual error text so the browser can
  // display it. Crucial for debugging large batch submissions where
  // Netlify's function logs aren't always visible.
  try {
    return await _handlerImpl(event);
  } catch (err) {
    console.error("[handler] unhandled error:", err);
    return {
      statusCode: 500,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify({
        ok: false,
        error: {
          message: String(err?.message || err),
          stack: String(err?.stack || "").split("\n").slice(0, 5).join(" | "),
          kind: (() => { try { return JSON.parse(event?.body || "{}")?.kind; } catch { return null; } })(),
        },
      }),
    };
  }
};

async function _handlerImpl(event) {
  if (event.httpMethod === "OPTIONS") return json(200, { ok: true });
  if (event.httpMethod !== "POST") return json(405, { error: { message: "Method not allowed" } });

  const body = parseJsonBody(event);
  if (!body) return json(400, { error: { message: "Invalid JSON body" } });

  // ---------------------------------------------------------------------
  // Custom Charm Studio (storefront, customer-facing). Dispatched before
  // every existing kind and before the destructure below, because these
  // requests are authenticated with a Firebase ID token, answer a
  // storefront-locked CORS origin rather than "*", and share none of the
  // Listing Generator's body contract. No existing kind is affected.
  // ---------------------------------------------------------------------
  if (STUDIO_KINDS.has(body.kind)) {
    return await handleStudioKind({ kind: body.kind, body, event });
  }

  const {
    jobId,
    runId,
    slotIndex,
    kind = "edits",
    model: _clientModel,
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
    concept_count,
    background_policy,
    // Charm geometry contract, enforced as the backend's FINAL word after all
    // role labels: "flat_integrated_eyelet" (base charm — flat sheet with one
    // fused eyelet, no jump rings) or "flat_no_attachment" (earring twin —
    // flat sheet with no attachment hardware at all).
    charm_geometry_policy,
    // Optional edit-intent declaration (e.g. "remove_jump_ring_only") used by
    // the text-only prompt preflight to scope a repair to the attachment
    // hardware while preserving the charm design 100%.
    charm_edit_intent,
    // Optional {keyword: text} map written into the output PNG as iTXt
    // chunks (e.g. the Charm Maker's tailored per-image design description).
    embed_metadata,
    // "edit" (default) | "style_reference"
    //
    // The role labels this function injects around the inline image parts are
    // written for the listing compositor: IMAGE 1 is a locked template canvas,
    // IMAGE 2 is design truth whose exact silhouette must be reused, and the
    // final lock says "edit IMAGE 1 in place". They are appended AFTER the
    // caller's prompt, so they win any disagreement with it.
    //
    // That is correct for compositing a known charm onto a known template. It
    // is exactly wrong for the Charm Maker, which passes a reference charm and
    // asks for a NEW design: the labels instruct the model to reproduce the
    // silhouette it was supposed to depart from, which is why derivatives came
    // back as near-copies no matter how the prompt was worded.
    //
    // "style_reference" swaps in labels that scope the reference to material,
    // finish, lighting, background and engraving language, and make reusing its
    // subject or silhouette an explicit failure. Default is unchanged, so every
    // existing caller behaves byte-for-byte as before.
    image_roles,
  } = body || {};

  // ---------------------------------------------------------------------
  // kind: "capabilities" — a cheap, side-effect-free probe.
  //
  // The Charm Maker's design mode depends on this file supporting
  // image_roles: "style_reference". An older deployment simply IGNORES an
  // unknown image_roles and appends "IMAGE 1 — EDIT TARGET. Apply only the
  // requested edit and preserve all unrelated pixels", which silently turns
  // every derivative back into a near-copy of its reference with no error
  // anywhere. That silent failure is unacceptable, so the client probes for
  // this endpoint and refuses to use the image-conditioned path unless it
  // answers. An old deployment falls through to the edits branch and 400s,
  // which is itself a clear answer.
  // ---------------------------------------------------------------------
  if (kind === "capabilities") {
    // Every flag below is a live description of this file's behaviour — the
    // Listing Generator refuses to run the Charm Maker against a backend
    // whose answers don't match the contract it was built for.
    return json(200, {
      ok: true,
      proxy: "geminiImageProxy-background",
      // 13: adds the Custom Charm Studio kinds and the customer_reference
      // role vocabulary. Every key below 13 is unchanged — the Listing
      // Generator hard-gates on them.
      capabilitiesVersion: 13,
      roleModes: Object.keys(IMAGE_ROLE_LABELS),
      supportsStyleReference: Object.prototype.hasOwnProperty.call(IMAGE_ROLE_LABELS, "style_reference"),
      supportsCharmConceptPlanning: true,
      supportsCharmBatchOrchestration: true,
      supportsCharmBatchRecovery: true,
      supportsBatchSweep: true,
      // charm_geometry_policy is appended by the backend AFTER all role
      // labels, as the request's final word (charmPolicyFinalText).
      supportsBackendFinalGeometryOverride: true,
      charmGeometryPolicies: Object.keys(CHARM_GEOMETRY_OVERRIDES),
      // Text-only prompt audit by an independent agent, run once BEFORE the
      // single image generation. It can never produce an image.
      supportsCharmPromptPreflight: true,
      promptPreflightMode: "text_only_before_image_generation",
      usesIndependentPromptAuditAgent: true,
      promptAuditProducesImages: false,
      // The manual jump-ring repair is an ordinary single edits request: it
      // runs on the client-selected image model, generates exactly one image,
      // and its preflight contract preserves the charm design 100%.
      supportsManualJumpRingRepair: true,
      manualRepairUsesSelectedModel: true,
      manualRepairMaxImageGenerations: 1,
      manualRepairPreservesCharmDesign: true,
      // One generation per request. What the model returns is what the
      // operator reviews — no automatic retake, no post-generation image QA.
      maxImageGenerationsPerRequest: 1,
      automaticImageRegeneration: false,
      postGenerationImageQA: false,
      // Background policy is enforced natively (generation prompt + text
      // preflight). The old frame-connected flood fill is retired: it could
      // eat bright engraving detail near the frame.
      backgroundEnforcement: "native_generation_prompt_preflight_only",
      destructiveBackgroundFloodFill: false,
      usesBackgroundMasking: false,
      backgroundPolicies: ["solid_black"],
      supportsGenerations: true,
      imageModels: Object.keys(IMAGE_MODEL_CONFIG),
      defaultImageModel: DEFAULT_IMAGE_MODEL,
      defaultCharmRenderModel: preferredCharmRenderModelId(),
      // Custom Charm Studio (storefront). Firebase-ID-token authenticated,
      // storefront-locked CORS, server-side wallet with refund on failure.
      supportsCustomCharmStudio: true,
      customStudioKinds: Array.from(STUDIO_KINDS),
      customStudioAuth: "firebase_id_token",
      customStudioReusesCharmDoctrine: true,
      supportsCustomerReferenceRole: Object.prototype.hasOwnProperty.call(IMAGE_ROLE_LABELS, "customer_reference"),
    });
  }

  if (kind === "charm_concept_plan") {
    try {
      const sourcePath = String(input_storage_path || source_storage_path || "").trim();
      if (!sourcePath) {
        return json(400, { ok: false, error: { message: "input_storage_path is required" } });
      }
      const plan = await planComplementaryCharmConcepts({
        sourceStoragePath: sourcePath,
        count: concept_count,
      });
      return json(200, { ok: true, plan });
    } catch (err) {
      console.error("[charm_concept_plan] failed", safeErr(err));
      return json(502, { ok: false, error: safeErr(err) });
    }
  }

  let modelConfig;
  try {
    modelConfig = resolveImageModel(effectiveRequestImageModel(kind, _clientModel));
  } catch (err) {
    return json(400, { ok: false, error: safeErr(err) });
  }
  const model = modelConfig.id;

  // ============================================================
  // CHARM BATCH ORCHESTRATION — fully server-side, tab-independent.
  // ------------------------------------------------------------
  // One request from the browser hands over the ENTIRE job: the charm
  // list, derivatives count and model. This background function then does
  // everything the browser used to do — per-charm concept planning,
  // per-set folder/snapshot/manifest creation, prompt + embedded-metadata
  // building (via the *Server mirror builders), and dense batch_submit
  // chunks — persisting progress to Firestore as it goes. If the work
  // exceeds one background invocation's budget it SELF-CHAINS: it POSTs
  // itself a resume request and returns. Collection is handled by
  // batch_sweep (fired by charmBatchSweepCron), so neither submission nor
  // collection ever depends on a browser being open.
  // ============================================================
  if (kind === "charm_batch_orchestrate") {
    const db = getDb();
    const bucket = admin.storage().bucket();
    const ORCH_BUDGET_MS = 10.5 * 60 * 1000;
    const ORCH_CHUNK_SETS = 25;
    const startedAtMs = Date.now();

    const inProcess = async (payload) => {
      const res = await module.exports.handler({
        httpMethod: "POST",
        headers: {},
        body: JSON.stringify(payload),
      });
      let parsed = null;
      try { parsed = res && res.body ? JSON.parse(res.body) : null; } catch (_) {}
      return { statusCode: res?.statusCode || 0, body: parsed };
    };

    const selfChain = async (orchestrationId) => {
      const origin = (process.env.URL || process.env.DEPLOY_PRIME_URL || process.env.DEPLOY_URL || "").replace(/\/+$/, "");
      if (!origin) { console.warn("[orchestrate] no site origin env — sweep cron will resume instead"); return; }
      try {
        await fetch(`${origin}/.netlify/functions/geminiImageProxy-background`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ kind: "charm_batch_orchestrate", orchestrationId, resume: true }),
        });
      } catch (e) {
        console.warn("[orchestrate] self-chain failed — sweep cron will resume:", e?.message || e);
      }
    };

    const latestUnderPrefix = async (prefix) => {
      try {
        const [files] = await bucket.getFiles({ prefix, maxResults: 500 });
        let best = null, bestMs = -1;
        for (const f of files) {
          if (f.name.endsWith("/")) continue;
          const ms = Date.parse(f.metadata?.updated || f.metadata?.timeCreated || "") || 0;
          if (ms > bestMs) { bestMs = ms; best = f.name; }
        }
        return best;
      } catch (_) { return null; }
    };

    try {
      let orchRef, orch;
      if (body?.resume && body?.orchestrationId) {
        orchRef = db.collection(ORCH_COLL).doc(String(body.orchestrationId));
        const snap = await orchRef.get();
        if (!snap.exists) return json(404, { ok: false, error: { message: "orchestration not found" } });
        orch = snap.data();
        if (orch.status !== "running") return json(200, { ok: true, orchestrationId: orchRef.id, status: orch.status, note: "already final" });
      } else {
        const charms = Array.isArray(body?.charms)
          ? body.charms
              .map((c) => ({ fullPath: String(c?.fullPath || "").trim(), name: String(c?.name || "").trim() }))
              .filter((c) => c.fullPath)
          : [];
        if (!charms.length) return json(400, { ok: false, error: { message: "charms must be a non-empty array" } });
        const derivCount = Math.max(1, Math.min(8, Number(body?.derivCount) || 1));
        if (charms.length * derivCount > 1000) {
          return json(400, { ok: false, error: { message: "Per-orchestration limit is 1000 sets." } });
        }
        // Resolve the shared refs the manifests record, once per orchestration.
        const earringRef = await latestUnderPrefix("listing-generator-1/Charm_Maker/New_Charms_Earrings/");
        const lineArtRef = await latestUnderPrefix("listing-generator-1/Charm_Maker/Reference_Line_Art_Image/");
        orchRef = db.collection(ORCH_COLL).doc(`orch_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`);
        orch = {
          orchestrationId: orchRef.id,
          sessionId: String(body?.sessionId || orchRef.id),
          model,
          derivCount,
          charms,
          earringRef: earringRef || null,
          lineArtRef: lineArtRef || null,
          cursor: 0,
          setsPlanned: charms.length * derivCount,
          setsPrepared: 0,
          chunksSubmitted: 0,
          batchNames: [],
          prepSkipped: [],
          chunkErrors: [],
          status: "running",
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        };
        await firestoreRetry(() => orchRef.set(orch), "orch.create");
      }

      const persist = async (patch) => {
        Object.assign(orch, patch);
        await firestoreRetry(
          () => orchRef.set({ ...patch, updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }),
          "orch.update"
        );
      };

      let pendingSets = [];
      let lastStamp = 0;

      const submitPending = async () => {
        if (!pendingSets.length) return;
        const chunk = pendingSets;
        pendingSets = [];
        const partN = (orch.chunksSubmitted || 0) + 1;
        const dn = `cm-charms-${String(orch.sessionId).slice(-8)}-part${partN}`;
        let resp = await inProcess({
          kind: "batch_submit", model: orch.model, sets: chunk,
          imageSize: "2K", displayName: dn, sessionId: orch.sessionId,
        });
        if (!(resp.body && resp.body.ok)) {
          console.warn(`[orchestrate] chunk ${dn} submit failed — retrying once`, resp.body?.error);
          await sleep(8000);
          resp = await inProcess({
            kind: "batch_submit", model: orch.model, sets: chunk,
            imageSize: "2K", displayName: dn, sessionId: orch.sessionId,
          });
        }
        if (resp.body && resp.body.ok) {
          await persist({
            chunksSubmitted: partN,
            batchNames: [...(orch.batchNames || []), resp.body.batchName].filter(Boolean),
          });
        } else {
          await persist({
            chunksSubmitted: partN,
            chunkErrors: [...(orch.chunkErrors || []), { part: partN, sets: chunk.length, error: String(resp.body?.error?.message || `HTTP ${resp.statusCode}`).slice(0, 300) }].slice(0, 40),
          });
        }
        await sleep(2000); // Files API stagger
      };

      while (orch.cursor < orch.charms.length && (Date.now() - startedAtMs) < ORCH_BUDGET_MS) {
        const charm = orch.charms[orch.cursor];
        let plan = null;
        try {
          plan = await planComplementaryCharmConcepts({ sourceStoragePath: charm.fullPath, count: orch.derivCount });
        } catch (planErr) {
          console.warn(`[orchestrate] planning failed for ${charm.name} — skipping`, planErr?.message || planErr);
          await persist({
            cursor: orch.cursor + 1,
            prepSkipped: [...(orch.prepSkipped || []), { charm: charm.name, error: String(planErr?.message || planErr).slice(0, 200) }].slice(0, 60),
          });
          continue;
        }
        const planMeta = {
          version: Number(plan?.version) || 1,
          referenceSubject: cmCleanText(plan?.referenceSubject),
          buyerProfile: cmCleanText(plan?.buyerProfile),
          purchaseSignals: Array.isArray(plan?.purchaseSignals) ? plan.purchaseSignals.slice(0, 6) : [],
        };

        for (let d = 0; d < orch.derivCount; d++) {
          const concept = plan.concepts[d] || plan.concepts[plan.concepts.length - 1];
          let stamp = Date.now() + Math.floor(Math.random() * 10000);
          if (stamp <= lastStamp) stamp = lastStamp + 1;
          lastStamp = stamp;
          const folderId = `Deriv_${stamp}`;
          const outputBase = `listing-generator-1/Charm_Maker/Generated_Charm_Sets/${folderId}`;

          // Reference snapshot (mirror of snapshot_charm_reference, inline).
          let sourceSnapshot = null;
          try {
            const srcFile = bucket.file(charm.fullPath);
            const [exists] = await srcFile.exists();
            if (exists) {
              let mime = "image/png";
              try { const [m] = await srcFile.getMetadata(); mime = String(m?.contentType || mime).toLowerCase(); } catch (_) {}
              const ext = mime.includes("jpeg") || mime.includes("jpg") ? "jpg" : mime.includes("webp") ? "webp" : "png";
              const dst = `${outputBase}/Source_Ref.${ext}`;
              await srcFile.copy(bucket.file(dst));
              try {
                await bucket.file(dst).setMetadata({ metadata: { firebaseStorageDownloadTokens: newDownloadToken() } });
              } catch (_) {}
              sourceSnapshot = dst;
            }
          } catch (snapErr) {
            console.warn(`[orchestrate] snapshot failed for ${folderId}`, snapErr?.message || snapErr);
          }

          const manifest = {
            sourceCharm: charm.fullPath,
            sourceSnapshot,
            sourceEarring: orch.earringRef || null,
            lineArtRef: orch.lineArtRef || null,
            charmConceptPlan: planMeta,
            charmConcept: concept,
            imageDescriptions: {
              necklace: buildCharmImageDescriptionServer(concept, planMeta, "necklace"),
              earring: buildCharmImageDescriptionServer(concept, planMeta, "earring"),
            },
            model: orch.model,
            batchQueued: true,
            orchestrationId: orchRef.id,
            timestamp: new Date().toISOString(),
          };
          try {
            await bucket.file(`${outputBase}/manifest.json`).save(
              Buffer.from(JSON.stringify(manifest, null, 2), "utf8"),
              { contentType: "application/json", resumable: false }
            );
          } catch (mErr) {
            console.warn(`[orchestrate] manifest write failed for ${folderId} — skipping set`, mErr?.message || mErr);
            continue;
          }

          pendingSets.push({
            category: "Charms",
            outputBasePath: outputBase,
            setN: stamp,
            setKind: "charm_maker",
            manifest,
            tasks: [{
              type: "edits",
              slotIndex: 0,
              input_storage_path: charm.fullPath,
              prompt: buildCharmDesignPromptServer(concept, planMeta),
              image_roles: "style_reference",
              background_policy: "solid_black",
              charm_geometry_policy: "flat_integrated_eyelet",
              embed_metadata: buildCharmEmbedMetadataServer(concept, planMeta, "necklace"),
            }],
          });
          orch.setsPrepared = (orch.setsPrepared || 0) + 1;
        }

        await persist({ cursor: orch.cursor + 1, setsPrepared: orch.setsPrepared });
        if (pendingSets.length >= ORCH_CHUNK_SETS) await submitPending();
      }

      // Never carry unsubmitted sets across invocations — flush now.
      await submitPending();

      if (orch.cursor < orch.charms.length) {
        await persist({ status: "running" });
        await selfChain(orchRef.id);
        return json(200, {
          ok: true, orchestrationId: orchRef.id, status: "running",
          progress: { cursor: orch.cursor, of: orch.charms.length, setsPrepared: orch.setsPrepared, chunksSubmitted: orch.chunksSubmitted },
          chained: true,
        });
      }

      await persist({ status: "submitted", finishedAt: admin.firestore.FieldValue.serverTimestamp() });
      return json(200, {
        ok: true, orchestrationId: orchRef.id, status: "submitted",
        setsPrepared: orch.setsPrepared, chunksSubmitted: orch.chunksSubmitted,
        prepSkipped: (orch.prepSkipped || []).length, chunkErrors: orch.chunkErrors || [],
      });
    } catch (err) {
      console.error("[charm_batch_orchestrate] failed", safeErr(err));
      return json(502, { ok: false, error: safeErr(err) });
    }
  }

  // ============================================================
  // CHARM BATCH RECOVERY — resubmit sets that never got images.
  // ------------------------------------------------------------
  // When a submission dies mid-run (browser sleep, deploy, crash), the
  // affected Deriv_ folders are left with a manifest.json + Source_Ref
  // snapshot but NO Slot_1.png. Every expensive artifact — the planned
  // concept, buyer analysis, tailored descriptions — is already in that
  // manifest, so recovery rebuilds each set's slot-0 task FROM the
  // manifest (zero re-planning cost) and resubmits in dense chunks.
  //
  // Safety rails:
  //   • a set whose Slot_1.png exists is finished — skipped;
  //   • a set referenced by an UNCOLLECTED, non-failed batch is already
  //     in flight at Gemini — skipped (no double spend);
  //   • a set with no manifest.charmConcept (pre-concept legacy) is
  //     counted and skipped rather than guessed at.
  // Runs on the background function, self-chains across invocations via
  // a lexicographic folder-name cursor, and reports progress through the
  // same orchestration records the panel already displays.
  // ============================================================
  if (kind === "charm_batch_recover") {
    const db = getDb();
    const bucket = admin.storage().bucket();
    const REC_BUDGET_MS = 10.5 * 60 * 1000;
    const REC_CHUNK_SETS = 25;
    const SETS_PREFIX = "listing-generator-1/Charm_Maker/Generated_Charm_Sets/";
    const startedAtMs = Date.now();

    const inProcess = async (payload) => {
      const res = await module.exports.handler({ httpMethod: "POST", headers: {}, body: JSON.stringify(payload) });
      let parsed = null;
      try { parsed = res && res.body ? JSON.parse(res.body) : null; } catch (_) {}
      return { statusCode: res?.statusCode || 0, body: parsed };
    };
    const selfChain = async (orchestrationId) => {
      const origin = (process.env.URL || process.env.DEPLOY_PRIME_URL || process.env.DEPLOY_URL || "").replace(/\/+$/, "");
      if (!origin) return;
      try {
        await fetch(`${origin}/.netlify/functions/geminiImageProxy-background`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ kind: "charm_batch_recover", orchestrationId, resume: true }),
        });
      } catch (e) { console.warn("[recover] self-chain failed — sweep will resume:", e?.message || e); }
    };

    try {
      let orchRef, orch;
      if (body?.resume && body?.orchestrationId) {
        orchRef = db.collection(ORCH_COLL).doc(String(body.orchestrationId));
        const snap = await orchRef.get();
        if (!snap.exists) return json(404, { ok: false, error: { message: "recovery run not found" } });
        orch = snap.data();
        if (orch.status !== "running") return json(200, { ok: true, orchestrationId: orchRef.id, status: orch.status });
      } else {
        orchRef = db.collection(ORCH_COLL).doc(`rec_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`);
        orch = {
          orchestrationId: orchRef.id,
          sessionId: String(body?.sessionId || orchRef.id),
          type: "recovery",
          model,
          cursorName: "",
          scanned: 0,
          resubmitted: 0,
          alreadyDone: 0,
          inFlightSkipped: 0,
          missingManifest: 0,
          setsPlanned: 0,
          setsPrepared: 0,
          chunksSubmitted: 0,
          batchNames: [],
          chunkErrors: [],
          status: "running",
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        };
        await firestoreRetry(() => orchRef.set(orch), "recover.create");
      }

      const persist = async (patch) => {
        Object.assign(orch, patch);
        await firestoreRetry(
          () => orchRef.set({ ...patch, updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }),
          "recover.update"
        );
      };

      // Exclusion set: outputBasePaths already sitting in an uncollected,
      // non-failed Gemini batch (submitted, pending, running, or succeeded
      // and awaiting the sweep's collection).
      const normSt = (x) => String(x || "").startsWith("BATCH_STATE_") ? "JOB_STATE_" + String(x).slice(12) : String(x || "");
      const finalFailed = (st) => ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED"].includes(normSt(st));
      const inFlight = new Set();
      const openSnap = await db.collection(BATCHES_COLL).where("collected", "==", false).limit(500).get();
      openSnap.forEach((d) => {
        const b = d.data();
        if (finalFailed(b.state)) return; // that batch is dead — its sets ARE recoverable
        for (const s of (b.sets || [])) if (s?.outputBasePath) inFlight.add(s.outputBasePath);
      });

      let pendingSets = [];
      const submitPending = async () => {
        if (!pendingSets.length) return;
        const chunk = pendingSets;
        pendingSets = [];
        const partN = (orch.chunksSubmitted || 0) + 1;
        const dn = `cm-recover-${String(orch.sessionId).slice(-8)}-part${partN}`;
        let resp = await inProcess({ kind: "batch_submit", model: orch.model, sets: chunk, imageSize: "2K", displayName: dn, sessionId: orch.sessionId });
        if (!(resp.body && resp.body.ok)) {
          await sleep(8000);
          resp = await inProcess({ kind: "batch_submit", model: orch.model, sets: chunk, imageSize: "2K", displayName: dn, sessionId: orch.sessionId });
        }
        if (resp.body && resp.body.ok) {
          await persist({ chunksSubmitted: partN, resubmitted: (orch.resubmitted || 0), batchNames: [...(orch.batchNames || []), resp.body.batchName].filter(Boolean) });
        } else {
          await persist({ chunksSubmitted: partN, chunkErrors: [...(orch.chunkErrors || []), { part: partN, sets: chunk.length, error: String(resp.body?.error?.message || `HTTP ${resp.statusCode}`).slice(0, 300) }].slice(0, 40) });
        }
        await sleep(2000);
      };

      // Page through set folders lexicographically, resuming past cursorName.
      let query = {
        prefix: SETS_PREFIX,
        delimiter: "/",
        autoPaginate: false,
        maxResults: 500,
      };
      if (orch.cursorName) query.startOffset = SETS_PREFIX + orch.cursorName;
      let exhausted = false;

      while (!exhausted && (Date.now() - startedAtMs) < REC_BUDGET_MS) {
        const [files, nextQuery, apiResp] = await bucket.getFiles(query);
        const prefixes = (apiResp && apiResp.prefixes) || [];
        for (const p of prefixes) {
          if ((Date.now() - startedAtMs) >= REC_BUDGET_MS) break;
          const folderName = p.slice(SETS_PREFIX.length).replace(/\/$/, "");
          if (!folderName.startsWith("Deriv_")) continue;
          if (orch.cursorName && folderName <= orch.cursorName) continue; // startOffset is inclusive
          orch.scanned = (orch.scanned || 0) + 1;

          const base = SETS_PREFIX + folderName;
          try {
            const [slotExists] = await bucket.file(`${base}/Slot_1.png`).exists();
            if (slotExists) { orch.alreadyDone = (orch.alreadyDone || 0) + 1; orch.cursorName = folderName; continue; }
            if (inFlight.has(base)) { orch.inFlightSkipped = (orch.inFlightSkipped || 0) + 1; orch.cursorName = folderName; continue; }

            let manifest = null;
            try {
              const [buf] = await bucket.file(`${base}/manifest.json`).download();
              manifest = JSON.parse(buf.toString("utf8"));
            } catch (_) {}
            if (!manifest || !manifest.charmConcept || !manifest.sourceCharm) {
              orch.missingManifest = (orch.missingManifest || 0) + 1;
              orch.cursorName = folderName;
              continue;
            }

            const concept = manifest.charmConcept;
            const planMeta = manifest.charmConceptPlan || null;
            const inputPath = manifest.sourceSnapshot || manifest.sourceCharm;
            pendingSets.push({
              category: "Charms",
              outputBasePath: base,
              setN: Number((folderName.match(/Deriv_(\d+)/) || [])[1] || 0),
              setKind: "charm_maker",
              manifest,
              tasks: [{
                type: "edits",
                slotIndex: 0,
                input_storage_path: inputPath,
                prompt: buildCharmDesignPromptServer(concept, planMeta),
                image_roles: "style_reference",
                background_policy: "solid_black",
                charm_geometry_policy: "flat_integrated_eyelet",
                embed_metadata: buildCharmEmbedMetadataServer(concept, planMeta, "necklace"),
              }],
            });
            orch.resubmitted = (orch.resubmitted || 0) + 1;
            orch.setsPrepared = orch.resubmitted;
            orch.setsPlanned = orch.resubmitted;
            orch.cursorName = folderName;
            if (pendingSets.length >= REC_CHUNK_SETS) {
              await persist({
                cursorName: orch.cursorName, scanned: orch.scanned, resubmitted: orch.resubmitted,
                alreadyDone: orch.alreadyDone, inFlightSkipped: orch.inFlightSkipped,
                missingManifest: orch.missingManifest, setsPrepared: orch.setsPrepared, setsPlanned: orch.setsPlanned,
              });
              await submitPending();
            }
          } catch (folderErr) {
            console.warn(`[recover] folder ${folderName} failed — skipping`, folderErr?.message || folderErr);
            orch.cursorName = folderName;
          }
        }
        await persist({
          cursorName: orch.cursorName, scanned: orch.scanned, resubmitted: orch.resubmitted,
          alreadyDone: orch.alreadyDone, inFlightSkipped: orch.inFlightSkipped,
          missingManifest: orch.missingManifest, setsPrepared: orch.setsPrepared, setsPlanned: orch.setsPlanned,
        });
        if (nextQuery) { query = nextQuery; } else { exhausted = true; }
      }

      await submitPending();

      if (!exhausted) {
        await persist({ status: "running" });
        await selfChain(orchRef.id);
        return json(200, {
          ok: true, orchestrationId: orchRef.id, status: "running", chained: true,
          progress: { scanned: orch.scanned, resubmitted: orch.resubmitted, alreadyDone: orch.alreadyDone },
        });
      }

      await persist({ status: "submitted", finishedAt: admin.firestore.FieldValue.serverTimestamp() });
      return json(200, {
        ok: true, orchestrationId: orchRef.id, status: "submitted",
        scanned: orch.scanned, resubmitted: orch.resubmitted, alreadyDone: orch.alreadyDone,
        inFlightSkipped: orch.inFlightSkipped, missingManifest: orch.missingManifest,
        chunksSubmitted: orch.chunksSubmitted, chunkErrors: orch.chunkErrors || [],
      });
    } catch (err) {
      console.error("[charm_batch_recover] failed", safeErr(err));
      return json(502, { ok: false, error: safeErr(err) });
    }
  }

  // Lightweight progress read for the Charm Maker panel. Inline-safe.
  if (kind === "charm_batch_status") {
    try {
      const db = getDb();
      const snap = await db.collection(ORCH_COLL).orderBy("createdAt", "desc").limit(20).get();
      const orchestrations = [];
      snap.forEach((doc) => {
        const o = doc.data();
        if (body?.sessionId && o.sessionId !== body.sessionId) return;
        orchestrations.push({
          orchestrationId: o.orchestrationId || doc.id,
          sessionId: o.sessionId || null,
          type: o.type || "orchestration",
          status: o.status || "unknown",
          model: o.model || null,
          scanned: o.scanned || 0,
          resubmitted: o.resubmitted || 0,
          alreadyDone: o.alreadyDone || 0,
          inFlightSkipped: o.inFlightSkipped || 0,
          missingManifest: o.missingManifest || 0,
          charmsTotal: Array.isArray(o.charms) ? o.charms.length : 0,
          cursor: o.cursor || 0,
          setsPlanned: o.setsPlanned || 0,
          setsPrepared: o.setsPrepared || 0,
          chunksSubmitted: o.chunksSubmitted || 0,
          batchNames: (o.batchNames || []).length,
          prepSkipped: (o.prepSkipped || []).length,
          chunkErrors: (o.chunkErrors || []).length,
          createdAt: o.createdAt?.toMillis?.() || null,
          updatedAt: o.updatedAt?.toMillis?.() || null,
        });
      });
      return json(200, { ok: true, orchestrations });
    } catch (err) {
      return json(502, { ok: false, error: safeErr(err) });
    }
  }

  // ============================================================
  // BATCH SWEEP — server-side status refresh + auto-collect + stall
  // recovery. Fired by charmBatchSweepCron every few minutes, so batch
  // results are downloaded into Firebase even when no browser is open.
  // Also resumes orchestrations whose self-chain was dropped.
  // ============================================================
  if (kind === "batch_sweep") {
    const db = getDb();
    const SWEEP_BUDGET_MS = 11 * 60 * 1000;
    const sweepStart = Date.now();
    const guardRef = db.collection("LG1_Config").doc("batchSweep");

    const inProcess = async (payload) => {
      const res = await module.exports.handler({ httpMethod: "POST", headers: {}, body: JSON.stringify(payload) });
      let parsed = null;
      try { parsed = res && res.body ? JSON.parse(res.body) : null; } catch (_) {}
      return parsed;
    };

    try {
      // Overlap guard: skip if another sweep started < 12 minutes ago.
      const guard = await guardRef.get();
      const runningSince = guard.exists ? (guard.data().runningSince?.toMillis?.() || 0) : 0;
      if (runningSince && Date.now() - runningSince < 12 * 60 * 1000) {
        return json(200, { ok: true, skipped: "sweep already running" });
      }
      await guardRef.set({ runningSince: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });

      const normState = (st) => {
        const x = String(st || "");
        return x.startsWith("BATCH_STATE_") ? "JOB_STATE_" + x.slice(12) : x;
      };
      const isFinal = (st) => ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_EXPIRED"].includes(normState(st));
      const isSucceeded = (st) => normState(st) === "JOB_STATE_SUCCEEDED";

      let statusChecked = 0, collected = 0, collectErrors = 0, resumed = 0;

      const openSnap = await db.collection(BATCHES_COLL).where("collected", "==", false).limit(250).get();
      const open = [];
      openSnap.forEach((d) => open.push(d.data()));
      // Oldest first so long-waiting batches are served before fresh ones.
      open.sort((a, b) => (a.createdAt?.toMillis?.() || 0) - (b.createdAt?.toMillis?.() || 0));

      for (const b of open) {
        if (Date.now() - sweepStart > SWEEP_BUDGET_MS) break;
        if (!b.batchName || isFinal(b.state)) continue;
        let state = b.state;
        if (!isSucceeded(state)) {
          const st = await inProcess({ kind: "batch_status", batchName: b.batchName });
          statusChecked++;
          state = st?.state || state;
        }
        if (isSucceeded(state)) {
          const col = await inProcess({ kind: "batch_collect", batchName: b.batchName });
          if (col?.ok) collected++; else collectErrors++;
        }
      }

      // Resume orchestrations whose self-chain got dropped (no update in 20m).
      const orchSnap = await db.collection(ORCH_COLL).where("status", "==", "running").limit(20).get();
      const origin = (process.env.URL || process.env.DEPLOY_PRIME_URL || process.env.DEPLOY_URL || "").replace(/\/+$/, "");
      const stale = [];
      orchSnap.forEach((d) => {
        const o = d.data();
        const upd = o.updatedAt?.toMillis?.() || 0;
        if (Date.now() - upd > 20 * 60 * 1000) stale.push(o.orchestrationId || d.id);
      });
      for (const id of stale) {
        if (!origin) break;
        try {
          await fetch(`${origin}/.netlify/functions/geminiImageProxy-background`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ kind: "charm_batch_orchestrate", orchestrationId: id, resume: true }),
          });
          resumed++;
        } catch (e) { console.warn("[batch_sweep] orchestration resume failed:", id, e?.message || e); }
      }

      await guardRef.set({
        runningSince: null,
        lastSweepAt: admin.firestore.FieldValue.serverTimestamp(),
        lastResult: { statusChecked, collected, collectErrors, resumed },
      }, { merge: true });
      return json(200, { ok: true, statusChecked, collected, collectErrors, resumed, openBatches: open.length });
    } catch (err) {
      try { await guardRef.set({ runningSince: null }, { merge: true }); } catch (_) {}
      console.error("[batch_sweep] failed", safeErr(err));
      return json(502, { ok: false, error: safeErr(err) });
    }
  }

  // ---------- NEW: non-job operations (no jobId required) ----------
  try {
    if (kind === "alloc_set") {
      const { setN, outputBasePath } = await allocNextSet(activeCategory);
      return json(200, { ok: true, setN, outputBasePath });
    }

    // ============================================================
    // SCAN EMPTY SETS — for partial-failure recovery
    // ------------------------------------------------------------
    // After a batch submission, some Set_N folders may end up empty
    // (allocated by alloc_set but never populated with slot images
    // because the underlying batch failed or never collected). This
    // kind enumerates them so the frontend can offer a "Resume" flow
    // that re-submits batches for just the empty ones, leaving the
    // populated ones untouched.
    //
    // Algorithm:
    //   1. Read the Firestore set counter for the category — gives us
    //      the highest setN that's ever been issued.
    //   2. List every file under Ready_To_List/ in one bucket call.
    //   3. Bucket the files by Set_N. A set with at least one
    //      Slot_*.png file counts as "has content"; everything else
    //      (no files at all, or only a stale manifest) counts as
    //      "empty".
    //   4. Return a sorted list of {setN, outputBasePath} for empty
    //      slots in [1, highestN]. Populated slots are NOT returned.
    // ============================================================
    if (kind === "scan_empty_sets") {
      const cat = normalizeCategory(activeCategory);
      if (!GENERATABLE_CATEGORIES.has(cat)) {
        return json(400, { error: { message: "activeCategory not generatable" } });
      }

      const db = getDb();
      const counterSnap = await db.collection(SET_COUNTERS_COLL).doc(cat).get();
      const highestN = counterSnap.exists ? Number(counterSnap.data()?.nextSetN || 0) : 0;
      if (highestN <= 0) {
        return json(200, { ok: true, highestN, empty: [], populated: [], message: "No sets allocated yet for this category." });
      }

      const bucket = admin.storage().bucket();
      const prefix = `listing-generator-1/${cat}/Ready_To_List/`;
      const [files] = await bucket.getFiles({ prefix });

      // Bucket file names by setN. Track whether each setN has at least
      // one Slot_*.png file (the gating signal for "has content"). A
      // manifest.json alone does not count — alloc_set never writes
      // files, so a folder with only a stale manifest is still "empty"
      // by our definition (the manifest typically arrives only after
      // batch_collect, which also writes slot images).
      const hasSlots = new Set();
      const re = /\/Set_(\d+)\/(Slot_\d+\.png|.+)$/;
      for (const f of files) {
        const m = f.name.match(re);
        if (!m) continue;
        const setN = Number(m[1]);
        const filename = m[2];
        if (/^Slot_\d+\.png$/i.test(filename)) {
          hasSlots.add(setN);
        }
      }

      // Identify already-approved sets so we don't classify them as
      // "empty" and accidentally regenerate them when the user clicks
      // Resume Empty Sets. The approval flow moves files from
      // Ready_To_List/Set_N/ to Completed_Listing_Sets/{cat}_Set_N/,
      // leaving Ready_To_List/Set_N/ empty (no slot images). Without
      // this filter, every approved set would look like a gap and get
      // re-filled with brand-new charms — destroying the user's
      // approved work.
      const approvedSetNs = new Set();
      const completedPrefix = `listing-generator-1/Generated_Listing_Sets/Completed_Listing_Sets/${cat}_Set_`;
      try {
        const [completedFiles] = await bucket.getFiles({ prefix: completedPrefix });
        for (const f of completedFiles) {
          // Path looks like ".../Completed_Listing_Sets/{cat}_Set_42/..."
          const idx = f.name.indexOf(`/${cat}_Set_`);
          if (idx === -1) continue;
          const tail = f.name.slice(idx + `/${cat}_Set_`.length);
          const numStr = tail.split("/")[0];
          const num = Number(numStr);
          if (Number.isFinite(num) && num > 0) approvedSetNs.add(num);
        }
      } catch (e) {
        console.warn("[scan_empty_sets] could not enumerate Completed_Listing_Sets:", e?.message || e);
      }

      const empty = [];
      const populated = [];
      const skippedApproved = [];
      for (let n = 1; n <= highestN; n++) {
        if (approvedSetNs.has(n)) {
          skippedApproved.push(n);
          continue;
        }
        const outputBasePath = `listing-generator-1/${cat}/Ready_To_List/Set_${n}`;
        if (hasSlots.has(n)) {
          populated.push({ setN: n, outputBasePath });
        } else {
          empty.push({ setN: n, outputBasePath });
        }
      }

      return json(200, {
        ok: true,
        highestN,
        emptyCount: empty.length,
        populatedCount: populated.length,
        approvedCount: skippedApproved.length,
        empty,
        // populated and skippedApproved returned only as counts to keep
        // the response payload small for large counters.
      });
    }

    // ============================================================
    // CHARM POOL — pick & release for batch mode
    // ------------------------------------------------------------
    // Each generated set must use a UNIQUE charm from the shared pool,
    // and we must avoid duplicate picks across concurrent submissions.
    //
    // Pick algorithm:
    //   1. List all charms in the active pool (necklace or earring).
    //   2. Enumerate every uncollected batch's `sets[].tasks[].input_charm_storage_path`
    //      to find currently-reserved charms.
    //   3. Subtract: available = pool - reserved.
    //   4. Return the first `count` available paths.
    //
    // Atomicity: the pick step doesn't write anything — the
    // reservation happens implicitly when the caller subsequently
    // submits a batch with these paths in its tasks. There's a small
    // race window (two clients pick same charms simultaneously then
    // both submit), which we accept because:
    //   - The window is sub-second in practice
    //   - The user-visible result is just one charm getting reused
    //     once across two concurrent submissions
    //   - The full atomicity solution (Firestore-tracked reservations
    //     with ttl) adds significant complexity for a problem that
    //     basically only manifests if you submit two batches within
    //     the same second
    // ============================================================
    if (kind === "charm_pool_pick") {
      const isEarring = !!body?.isEarring;
      const want = Math.max(1, Number(body?.count || 1));

      const bucket = admin.storage().bucket();
      const poolPrefix = isEarring
        ? "listing-generator-1/Charm_Maker/New_Charms_Earrings/"
        : "listing-generator-1/Charm_Maker/New_Charms/";

      // Step 1: list pool. Skip pseudo-folders (paths ending with "/").
      const [poolFiles] = await bucket.getFiles({ prefix: poolPrefix });
      const poolPaths = poolFiles
        .map((f) => f.name)
        .filter((n) => !n.endsWith("/") && /\.(png|jpg|jpeg|webp)$/i.test(n));

      if (poolPaths.length === 0) {
        return json(200, { ok: true, charms: [], poolSize: 0, reservedCount: 0 });
      }

      // Step 2: enumerate reservations from uncollected batches.
      const db = getDb();
      const reservedSet = new Set();
      try {
        const snap = await db.collection(BATCHES_COLL)
          .where("collected", "==", false).get();
        snap.forEach((doc) => {
          const sets = Array.isArray(doc.data()?.sets) ? doc.data().sets : [];
          for (const s of sets) {
            for (const t of (s.tasks || [])) {
              const p = t?.input_charm_storage_path;
              if (p) reservedSet.add(p);
            }
          }
        });
      } catch (e) {
        console.warn("charm_pool_pick: failed to enumerate reservations", e?.message || e);
      }

      // Step 3: filter and return.
      const available = poolPaths.filter((p) => !reservedSet.has(p));

      // Shuffle so concurrent submissions don't all start from the
      // same position. Fisher-Yates.
      for (let i = available.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [available[i], available[j]] = [available[j], available[i]];
      }

      const picked = available.slice(0, want);
      return json(200, {
        ok: true,
        charms: picked,
        poolSize: poolPaths.length,
        reservedCount: reservedSet.size,
        availableCount: available.length,
      });
    }

    // ------------------------------------------------------------
    // charm_restore — put a charm back into the active pool.
    //
    // The regenerate flow relocates its charm into the Used pool
    // (New_Charms_Earrings → Used_Earring_Charm_Pool) as soon as the
    // charm is claimed, which is correct when the generation lands but
    // burns the charm when the generation fails: it can never be picked
    // again by charm_pool_pick, even though nothing was produced from
    // it. This is the rollback for that case — call it from the
    // failure branch of a regenerate/generate attempt.
    //
    // Mirror image of the move in batch_collect: copy + delete (Firebase
    // Storage has no native rename), a fresh download token on the
    // destination so any UI expecting one can render it, and per-charm
    // error isolation so one bad path doesn't sink the rest.
    //
    // Idempotent by design. Restoring a charm that is already back in
    // the pool, or whose source object no longer exists, is reported
    // under `skipped` rather than as an error, so a retried failure
    // handler never double-moves or throws.
    //
    // Body: { kind: "charm_restore",
    //         charm_storage_path: "…/Used_Earring_Charm_Pool/x.png" }
    //   or   { kind: "charm_restore", charm_storage_paths: [ … ] }
    // ------------------------------------------------------------
    if (kind === "charm_restore") {
      const requested = Array.isArray(body?.charm_storage_paths)
        ? body.charm_storage_paths
        : (body?.charm_storage_path ? [body.charm_storage_path] : []);

      const paths = requested.map((p) => String(p || "").trim()).filter(Boolean);
      if (!paths.length) {
        return json(400, {
          error: { message: "charm_storage_path (or charm_storage_paths[]) is required" },
        });
      }
      if (paths.length > 500) {
        return json(400, { error: { message: "charm_storage_paths max length is 500" } });
      }

      const CHARM_RESTORE_MAP = [
        ["/Charm_Maker/Used_Earring_Charm_Pool/", "listing-generator-1/Charm_Maker/New_Charms_Earrings/"],
        ["/Charm_Maker/Used_Necklace_Charm_Pool/", "listing-generator-1/Charm_Maker/New_Charms/"],
      ];

      const bucket = getBucket();
      const restored = [];
      const skipped = [];
      const errors = [];

      // De-dupe so a caller passing the same charm twice does one move.
      for (const srcPath of Array.from(new Set(paths))) {
        try {
          const rule = CHARM_RESTORE_MAP.find(([usedPrefix]) => srcPath.includes(usedPrefix));

          if (!rule) {
            const alreadyActive =
              srcPath.includes("/Charm_Maker/New_Charms_Earrings/") ||
              srcPath.includes("/Charm_Maker/New_Charms/");
            skipped.push({
              charm: srcPath,
              reason: alreadyActive
                ? "already in the active pool"
                : "not a Charm_Maker used-pool path",
            });
            continue;
          }

          const filename = srcPath.split("/").pop();
          if (!filename) {
            skipped.push({ charm: srcPath, reason: "path has no filename" });
            continue;
          }
          const destPath = rule[1] + filename;

          const srcFile = bucket.file(srcPath);
          const [exists] = await srcFile.exists();
          if (!exists) {
            // Either an earlier restore already ran (dest exists) or the
            // path was wrong to begin with. Neither is worth failing on.
            const [destExists] = await bucket.file(destPath).exists();
            skipped.push({
              charm: srcPath,
              reason: destExists ? "already restored" : "source object not found",
            });
            continue;
          }

          await srcFile.copy(bucket.file(destPath));
          try {
            await bucket.file(destPath).setMetadata({
              metadata: { firebaseStorageDownloadTokens: newDownloadToken() },
            });
          } catch (_) {}
          await srcFile.delete();

          restored.push({ from: srcPath, to: destPath });
        } catch (e) {
          errors.push({ charm: srcPath, error: String(e?.message || e) });
        }
      }

      return json(200, {
        ok: errors.length === 0,
        restoredCount: restored.length,
        restored,
        skipped,
        errors: errors.slice(0, 50),
      });
    }

    // ============================================================
    // BATCH MODE — Listing Generator only (Charm Maker out of scope)
    //
    // Flow:
    //   1. Client calls batch_submit with an array of `sets`. Each
    //      `set` is { category, outputBasePath, setN, tasks: [...] }
    //      where tasks come from SLOT_MAP. Tasks of type "copy" run
    //      synchronously here (no Gemini call). Tasks of type "edits"
    //      become JSONL lines uploaded to Gemini Files API.
    //   2. Server builds the JSONL with explicit imageConfig.imageSize="2K"
    //      and submits to Gemini batchGenerateContent. The batch name
    //      ("batches/abc123") is persisted in Firestore alongside per-
    //      task routing metadata so we can later land each output in
    //      the correct Slot_N.png path.
    //   3. Client polls batch_status. When state=JOB_STATE_SUCCEEDED,
    //      client calls batch_collect. Server downloads result JSONL,
    //      decodes each base64 image, uploads to its destination, and
    //      writes per-set manifests.
    //
    // Constraint discovery: see `keyForTask()` for how we route results.
    // The Gemini Batch API echoes our per-line `key` in each response,
    // so we encode the routing tuple (setSeq, slotIndex) in the key.
    // ============================================================
    if (kind === "batch_submit") {
      // Memory profiling — log at every stage so we can see where the
      // OOM happens. process.memoryUsage() reports:
      //   rss        = total resident set (everything Node uses)
      //   heapUsed   = JS heap actively used
      //   heapTotal  = JS heap allocated
      //   external   = C++ buffers (file I/O, fetch bodies, etc.)
      //   arrayBuffers = TypedArray-backed buffers (subset of external)
      // The OOM in Netlify is "rss exceeds container limit". Watch rss.
      const memLog = (stage) => {
        try {
          const m = process.memoryUsage();
          const fmt = (n) => (n / 1024 / 1024).toFixed(1) + "MB";
          console.log(`[batch_submit:mem] ${stage} rss=${fmt(m.rss)} heap=${fmt(m.heapUsed)}/${fmt(m.heapTotal)} ext=${fmt(m.external)}`);
        } catch (_) {}
      };
      memLog("entry");

      if (modelConfig.provider !== "gemini" || !modelConfig.supportsBatch) {
        return json(400, {
          error: {
            message:
              "Batch mode is available for Gemini 3.1 Flash Image and Gemini 3 Pro Image. Use Standard mode for OpenAI GPT Image 2.",
          },
        });
      }
      let apiKey;
      try { apiKey = apiKeyForImageModel(modelConfig); }
      catch (err) { return json(400, { error: safeErr(err) }); }
      const batchModel = modelConfig.id;

      const sets = Array.isArray(body?.sets) ? body.sets : null;
      if (!sets || sets.length === 0) {
        return json(400, { error: { message: "sets must be a non-empty array" } });
      }
      // Sanity cap. Even at 200 sets * 8 tasks * ~1.2MB inline data ≈ 1.9GB,
      // we approach the Files API 2GB limit. 100 keeps headroom.
      if (sets.length > 100) {
        return json(400, { error: { message: "Per-batch limit is 100 sets. Split into multiple batches." } });
      }

      const displayName = String(body?.displayName || `lg1-batch-${Date.now()}`).slice(0, 100);
      const imageSize = String(body?.imageSize || "2K").toUpperCase();
      const bucket = admin.storage().bucket();

      // Validate every output_base_path up front so we fail fast.
      for (const s of sets) {
        const cat = normalizeCategory(s?.category);
        if (!GENERATABLE_CATEGORIES.has(cat)) {
          return json(400, { error: { message: `category not generatable: ${cat}` } });
        }
        try { assertAllowedOutputBase(s?.outputBasePath); }
        catch (e) {
          return json(400, { error: { message: `Invalid outputBasePath: ${e.message}` } });
        }
        if (!Array.isArray(s?.tasks) || s.tasks.length === 0) {
          return json(400, { error: { message: `set ${s?.outputBasePath} has no tasks` } });
        }
      }

      // Step A: Run all "copy" tasks synchronously. They don't go through
      // Gemini and shouldn't wait for batch turnaround. This mirrors what
      // copy_to_slot does, but inline so we don't double-network-trip.
      // We also do per-task storagePath validation here.
      let copiedCount = 0;
      let copyErrors = [];
      const copyPromises = [];
      for (const s of sets) {
        const base = s.outputBasePath;
        for (const t of s.tasks) {
          if (String(t?.type) !== "copy") continue;
          const slot = Number(t?.slotIndex);
          if (!Number.isFinite(slot) || slot < 0) continue;
          const src = String(t?.source_storage_path || "").trim();
          if (!src) {
            copyErrors.push({ outputBasePath: base, slotIndex: slot, error: "missing source_storage_path" });
            continue;
          }
          copyPromises.push((async () => {
            try {
              const dst = `${base}/Slot_${slot + 1}.png`;
              const dstFile = bucket.file(dst);
              await bucket.file(src).copy(dstFile);
              const token = newDownloadToken();
              await dstFile.setMetadata({ metadata: { firebaseStorageDownloadTokens: token } });
              copiedCount++;
            } catch (e) {
              copyErrors.push({ outputBasePath: base, slotIndex: slot, error: String(e?.message || e) });
            }
          })());
        }
      }
      await Promise.all(copyPromises);
      memLog("after copy step");

      // Step B: Build the JSONL for "edits" tasks. Each line carries a
      // routing key encoding setIndex and slotIndex so we can place
      // the result back in the right folder.
      //
      // MEMORY-CRITICAL DESIGN NOTE
      // ----------------------------
      // We never accumulate full line OBJECTS in memory. Instead:
      //   1. Worker fetches both reference images.
      //   2. Worker base64-encodes them into a single line object.
      //   3. Worker JSON.stringify's the line, encodes UTF-8 → Buffer.
      //   4. Worker pushes that Buffer into jsonlChunks[] and drops
      //      every other reference (line object, base64 strings, raw
      //      buffers) so GC can reclaim them immediately.
      //
      // This prevents the OOM that happened previously when we held
      // all 96 line objects (each ~1.6MB) alive simultaneously, then
      // doubled memory by serializing them into one giant string.
      // Per worker peak ≈ 5 MB; total peak ≈ jsonlChunks size + (10
      // workers × 5 MB).
      const jsonlChunks = []; // Buffer[] — each chunk is one JSONL line + "\n"
      const routes = []; // parallel array: routes[i] = { setIndex, slotIndex, outputBasePath }
      const fetchJobs = []; // { setIdx, slot, promptT, refPath, charmPath, outputBasePath }
      for (let setIdx = 0; setIdx < sets.length; setIdx++) {
        const s = sets[setIdx];
        for (const t of s.tasks) {
          if (String(t?.type) !== "edits") continue;
          const slot = Number(t?.slotIndex);
          if (!Number.isFinite(slot) || slot < 0) continue;
          const refPath = String(t?.input_storage_path || "").trim();
          const charmPath = String(t?.input_charm_storage_path || "").trim();
          const promptT = String(t?.prompt || "").trim();
          // charmPath is OPTIONAL for single-image tasks (Charm Maker base
          // charms use one style-reference image); the listing compositor
          // still sends both.
          if (!refPath || !promptT) continue;
          fetchJobs.push({
            setIdx, slot, promptT, refPath, charmPath,
            outputBasePath: s.outputBasePath,
            imageRoles: String(t?.image_roles || "").trim() || null,
            backgroundPolicy: String(t?.background_policy || "").trim() || null,
            geometryPolicy: String(t?.charm_geometry_policy || "").trim() || null,
          });
        }
      }

      // Routes need a fixed slot per fetchJob. Pre-populate so workers
      // can write into routes[index] without needing a mutex.
      for (let i = 0; i < fetchJobs.length; i++) {
        const j = fetchJobs[i];
        routes.push({ setIndex: j.setIdx, slotIndex: j.slot, outputBasePath: j.outputBasePath });
      }

      // Fetch + build with low concurrency. See memory note above.
      // Concurrency 10 means at most 10 base64 working sets exist at
      // once. Each working set ≈ 5 MB peak (raw + base64 + stringified
      // line + Buffer chunk). After the worker returns, only the
      // pushed Buffer chunk in jsonlChunks survives.
      const FETCH_CONCURRENCY = 10;
      memLog(`before fetch loop (${fetchJobs.length} jobs)`);
      let fetchCounter = 0;
      await runBoundedConcurrent(fetchJobs, FETCH_CONCURRENCY, async (j, idx) => {
        const [ref, charm] = await Promise.all([
          storagePathToBuffer(j.refPath),
          j.charmPath ? storagePathToBuffer(j.charmPath) : Promise.resolve(null),
        ]);
        const key = `s${j.setIdx}_slot${j.slot}`;

        // Build the line, stringify, encode to Buffer, then explicitly
        // null out big locals so V8 has a strong hint to free them
        // before the next worker iteration starts.
        let line = buildBatchJsonlLine(
          key, j.promptT,
          ref.mime, ref.buffer.toString("base64"),
          charm ? charm.mime : null, charm ? charm.buffer.toString("base64") : null,
          imageSize,
          { imageRoles: j.imageRoles, backgroundPolicy: j.backgroundPolicy, geometryPolicy: j.geometryPolicy }
        );
        let jsonStr = JSON.stringify(line) + "\n";
        line = null;
        const chunkBuf = Buffer.from(jsonStr, "utf8");
        jsonStr = null;

        // jsonlChunks order doesn't matter for correctness — the JSONL
        // routing key on each line tells Gemini which response is which.
        jsonlChunks.push(chunkBuf);
        // Periodic memory log so we see in-loop growth.
        const c = ++fetchCounter;
        if (c === 1 || c % 5 === 0 || c === fetchJobs.length) {
          memLog(`fetch ${c}/${fetchJobs.length}`);
        }
      });
      memLog(`after fetch loop`);

      if (jsonlChunks.length === 0) {
        // Edge case: all-copy batch (no Gemini calls needed). Write
        // manifests immediately and short-circuit. We emit slots in the
        // same shape as the gen-collect path so the Review tab renders
        // consistently regardless of which path produced the set.
        for (const s of sets) {
          const manifestPath = `${s.outputBasePath}/manifest.json`;
          const slotsOut = (s.tasks || [])
            .filter((t) => Number.isFinite(Number(t?.slotIndex)))
            .sort((a, b) => Number(a.slotIndex) - Number(b.slotIndex))
            .map((t) => {
              const slotIdx = Number(t.slotIndex);
              const slot = slotIdx + 1;
              const isCopy = String(t?.type) === "copy";
              return isCopy
                ? {
                    slot,
                    type: "copy",
                    source: t?.source_storage_path || null,
                    originalSource: t?.source_storage_path || null,
                    newCharm: null,
                    output: `${s.outputBasePath}/Slot_${slot}.png`,
                  }
                : {
                    slot,
                    type: "gen",
                    source: t?.input_storage_path || null,
                    originalSource: t?.input_storage_path || null,
                    newCharm: t?.input_charm_storage_path || null,
                    output: null, // not generated (no edits tasks)
                  };
            });
          const m = {
            category: s.category,
            setN: s.setN,
            outputBasePath: s.outputBasePath,
            // For consistency with the gen-collect path. Copy-only sets
            // usually have no gen tasks and therefore no charm, but if
            // there happens to be one task of type !=="copy" with a charm
            // path, we record it.
            sourceCharm: (() => {
              for (const t of (s.tasks || [])) {
                if (String(t?.type) !== "copy" && t?.input_charm_storage_path) {
                  return t.input_charm_storage_path;
                }
              }
              return null;
            })(),
            sourceCharmName: (() => {
              for (const t of (s.tasks || [])) {
                if (String(t?.type) !== "copy" && t?.input_charm_storage_path) {
                  return String(t.input_charm_storage_path).split("/").pop() || null;
                }
              }
              return null;
            })(),
            timestamp: new Date().toISOString(),
            model: batchModel,
            batchMode: false,
            copyOnly: true,
            slots: slotsOut,
          };
          await bucket.file(manifestPath).save(
            Buffer.from(JSON.stringify(m, null, 2), "utf8"),
            { contentType: "application/json", resumable: false }
          );
        }
        return json(200, {
          ok: true, batchName: null, copyOnly: true,
          copied: copiedCount, copyErrors, message: "All tasks were copy-only; no batch job needed."
        });
      }

      // Concat once into a single Buffer for upload. This is the only
      // moment when we hold the full JSONL in memory — and it's a
      // single contiguous Buffer, not a doubled UTF-16 string.
      memLog(`before Buffer.concat (${jsonlChunks.length} chunks)`);
      const jsonlBuffer = Buffer.concat(jsonlChunks);
      const jsonlBytes = jsonlBuffer.length;
      // Free chunk array — Buffer.concat copied them, originals can go.
      jsonlChunks.length = 0;
      memLog(`after Buffer.concat (${(jsonlBytes/1e6).toFixed(1)}MB JSONL)`);

      // Hard cap: Files API limit is 2GB. We reject early to avoid uploading.
      if (jsonlBytes > 1.9 * 1024 * 1024 * 1024) {
        return json(400, {
          error: { message: `JSONL too large (${(jsonlBytes / 1e9).toFixed(2)} GB). Reduce sets per batch.` }
        });
      }

      // Step C: Upload JSONL to Gemini Files API, then create the batch.
      const fileName = await uploadJsonlToGeminiFiles(apiKey, jsonlBuffer, displayName);
      memLog(`after Files API upload`);
      const { batchName, raw } = await createGeminiBatchJob(apiKey, batchModel, fileName, displayName);
      memLog(`after batch create`);

      // Step D: Persist routing data in Firestore. The doc id is a
      // sanitized batch name so the client can fetch it directly.
      const db = getDb();
      const docId = batchDocIdFromName(batchName);
      const persistDoc = {
        batchName,
        docId,
        displayName,
        sessionId: String(body?.sessionId || ""),
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        state: "JOB_STATE_PENDING",
        collected: false,
        model: batchModel,
        imageSize,
        inputFileName: fileName,
        inputJsonlBytes: jsonlBytes,
        // sets metadata (so we can route + write manifests later)
        sets: sets.map((s) => ({
          category: s.category,
          outputBasePath: s.outputBasePath,
          setN: s.setN,
          // "charm_maker" sets carry their own manifest (concept, plan,
          // sourceCharm, snapshot) written by the client at submit time;
          // batch_collect preserves it instead of composing the listing-
          // shaped manifest.
          setKind: String(s.setKind || "") || null,
          manifest: (s.manifest && typeof s.manifest === "object") ? s.manifest : null,
          tasks: s.tasks, // store the raw task list so we can write a faithful manifest later
        })),
        routes,
        copyStats: { copied: copiedCount, errors: copyErrors },
        rawCreate: raw || null,
      };
      await firestoreRetry(
        () => db.collection(BATCHES_COLL).doc(docId).set(persistDoc, { merge: true }),
        "batch.create"
      );

      return json(200, {
        ok: true,
        batchName,
        docId,
        requestCount: routes.length,
        setsCount: sets.length,
        copyStats: { copied: copiedCount, errors: copyErrors },
        inputJsonlBytes: jsonlBytes,
      });
    }

    if (kind === "batch_status") {
      let apiKey;
      try { apiKey = apiKeyForImageModel(modelConfig); }
      catch (err) { return json(400, { error: safeErr(err) }); }

      const batchName = String(body?.batchName || "").trim();
      if (!batchName.startsWith("batches/")) {
        return json(400, { error: { message: "batchName must start with batches/" } });
      }

      const data = await getGeminiBatchJob(apiKey, batchName);
      // Gemini wraps the batch info under .metadata for long-running ops.
      // Per the docs, .metadata.state and .response.responsesFile are
      // where progress and the result file land.
      const state = data?.metadata?.state || data?.state || "UNKNOWN";
      const stats = data?.metadata?.batchStats || data?.batchStats || null;
      const respFile = data?.response?.responsesFile || data?.dest?.fileName || null;

      // Mirror state into Firestore so the dashboard can show it without
      // the user having a tab open during the polling phase.
      try {
        const db = getDb();
        const docId = batchDocIdFromName(batchName);
        await firestoreRetry(
          () => db.collection(BATCHES_COLL).doc(docId).set({
            state, batchStats: stats || null, responsesFile: respFile || null,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          }, { merge: true }),
          "batch.statusMirror"
        );
      } catch (_) { /* non-fatal */ }

      return json(200, {
        ok: true, batchName, state,
        batchStats: stats, responsesFile: respFile,
        done: state === "JOB_STATE_SUCCEEDED" || state === "JOB_STATE_FAILED" ||
              state === "JOB_STATE_CANCELLED" || state === "JOB_STATE_EXPIRED",
        succeeded: state === "JOB_STATE_SUCCEEDED",
      });
    }

    if (kind === "batch_collect") {
      const apiKey = process.env.GEMINI_API_KEY;
      if (!apiKey) return json(400, { error: { message: "Missing GEMINI_API_KEY env var" } });

      const batchName = String(body?.batchName || "").trim();
      if (!batchName.startsWith("batches/")) {
        return json(400, { error: { message: "batchName must start with batches/" } });
      }

      const db = getDb();
      const docId = batchDocIdFromName(batchName);
      const docSnap = await db.collection(BATCHES_COLL).doc(docId).get();
      if (!docSnap.exists) {
        return json(400, { error: { message: `No Firestore record for ${batchName}. Submit went through here?` } });
      }
      const docData = docSnap.data();

      if (docData.collected) {
        return json(200, { ok: true, alreadyCollected: true, batchName, results: docData.results || null });
      }

      const live = await getGeminiBatchJob(apiKey, batchName);
      const state = live?.metadata?.state || live?.state || "UNKNOWN";
      const forceMode = !!body?.force;

      // Standard download requires SUCCEEDED. Force download accepts any
      // state and pulls whatever responsesFile Google has produced —
      // useful when a job hangs, fails partway, or expires. Only failed
      // lines stay empty; successful ones still upload.
      if (!forceMode && state !== "JOB_STATE_SUCCEEDED") {
        return json(400, { error: { message: `Batch not succeeded; state=${state}. Use Force download to recover partial results.` } });
      }

      const respFileName = live?.response?.responsesFile || live?.dest?.fileName;
      if (!respFileName) {
        if (forceMode) {
          return json(400, { error: { message: `Force download: no result file available yet for state=${state}. Try again later or cancel.` } });
        }
        return json(500, { error: { message: "Succeeded batch has no responsesFile/fileName" } });
      }

      // STREAMING COLLECT
      // ------------------
      // We can't load the result JSONL into memory (potentially 2GB+) and
      // we can't queue every decoded image buffer either. Instead we
      // stream the JSONL line-by-line, decode each image inline, and
      // dispatch uploads through a bounded-concurrency worker pool.
      //
      // Concurrency limit (UPLOAD_CONCURRENCY) caps in-flight buffers,
      // keeping peak RAM at roughly UPLOAD_CONCURRENCY × ~3MB ≈ 60MB
      // even when collecting 100-set batches. Well under Netlify's
      // 1.5GB free-tier / 3GB Pro-tier function memory cap.
      const UPLOAD_CONCURRENCY = 20;

      const bucket = admin.storage().bucket();
      const routes = Array.isArray(docData.routes) ? docData.routes : [];
      const setsMeta = Array.isArray(docData.sets) ? docData.sets : [];

      const routeByKey = new Map();
      for (let i = 0; i < routes.length; i++) {
        const r = routes[i];
        routeByKey.set(`s${r.setIndex}_slot${r.slotIndex}`, r);
      }

      // Per-task PNG metadata (tailored design descriptions) recorded at
      // submit time — embedded into each result image before upload, exactly
      // like the synchronous edits path does.
      const embedByKey = new Map();
      for (let setIdx = 0; setIdx < setsMeta.length; setIdx++) {
        for (const t of (setsMeta[setIdx]?.tasks || [])) {
          const slot = Number(t?.slotIndex);
          if (!Number.isFinite(slot)) continue;
          if (t?.embed_metadata && typeof t.embed_metadata === "object") {
            embedByKey.set(`s${setIdx}_slot${slot}`, t.embed_metadata);
          }
        }
      }

      let succeededCount = 0;
      let failedCount = 0;
      const perSetSlotResults = new Map();
      const failures = [];

      // Producer: streams parsed lines from Google's result JSONL and
      // pushes upload tasks into a bounded queue. Consumers drain the
      // queue concurrently. We use a simple semaphore (slots[]) to
      // throttle in-flight uploads without buffering the whole list.
      const inFlight = new Set();
      const recordResult = (route, ok, storagePath, error) => {
        const arr = perSetSlotResults.get(route.setIndex) || [];
        if (ok) {
          succeededCount++;
          arr.push({ slotIndex: route.slotIndex, ok: true, storagePath });
        } else {
          failedCount++;
          failures.push({ key: `s${route.setIndex}_slot${route.slotIndex}`, error });
          arr.push({ slotIndex: route.slotIndex, ok: false, error });
        }
        perSetSlotResults.set(route.setIndex, arr);
      };

      const uploadOne = async (route, buffer, key) => {
        try {
          const embed = embedByKey.get(key);
          if (embed) buffer = embedPngTextMetadata(buffer, embed);
          const storagePath = `${route.outputBasePath}/Slot_${route.slotIndex + 1}.png`;
          const file = bucket.file(storagePath);
          const token = newDownloadToken();
          await file.save(buffer, {
            resumable: false,
            contentType: "image/png",
            metadata: { metadata: { firebaseStorageDownloadTokens: token } },
          });
          recordResult(route, true, storagePath);
        } catch (e) {
          recordResult(route, false, null, String(e?.message || e));
        }
        // Buffer drops out of scope once this fn returns; GC reclaims it.
      };

      // Drive the stream. For each line, parse → find route → decode b64
      // → spawn upload promise. If we hit the concurrency cap, wait for
      // the fastest one to finish before spawning the next. This keeps
      // memory bounded by UPLOAD_CONCURRENCY × per-image size.
      try {
        for await (const parsed of streamGeminiResultLines(apiKey, respFileName)) {
          const key = parsed?.key;
          const route = routeByKey.get(key);
          if (!route) {
            failures.push({ key: key || "(none)", error: "no route for key" });
            continue;
          }
          if (parsed?.error) {
            recordResult(route, false, null, parsed.error?.message || "batch error");
            continue;
          }
          const partsOut = parsed?.response?.candidates?.[0]?.content?.parts || [];
          const imgPart = partsOut.find((p) => p?.inline_data?.data) ||
                          partsOut.find((p) => p?.inlineData?.data) || null;
          const b64 = imgPart?.inline_data?.data || imgPart?.inlineData?.data;
          if (!b64) {
            recordResult(route, false, null, "no inline_data in response");
            continue;
          }

          // Decode + queue. Throttle once we hit the concurrency cap.
          const buffer = Buffer.from(b64, "base64");
          const p = uploadOne(route, buffer, key).finally(() => inFlight.delete(p));
          inFlight.add(p);
          if (inFlight.size >= UPLOAD_CONCURRENCY) {
            await Promise.race(inFlight);
          }
        }
        // Drain any remaining in-flight uploads.
        await Promise.all(inFlight);
      } catch (streamErr) {
        // If the stream itself fails, surface the partial state so the
        // user can see what was already uploaded before the break.
        await Promise.all(inFlight);
        failures.push({ key: "(stream)", error: String(streamErr?.message || streamErr) });
      }

      // Step: write a manifest per set, mirroring what Standard mode writes.
      // Manifest format intentionally matches the existing structure so the
      // Approved/Review tabs render batch results identically.
      //
      // The Review tab (loadSetImages → manifest.slots[].source) needs each
      // slot entry to carry its original `source` storage path so the UI
      // can display "Source: …". Standard mode also writes `newCharm` for
      // gen slots; we preserve that field here for cross-mode parity.
      for (let setIdx = 0; setIdx < setsMeta.length; setIdx++) {
        const s = setsMeta[setIdx];
        const slotResults = (perSetSlotResults.get(setIdx) || []).sort((a, b) => a.slotIndex - b.slotIndex);

        // Build a slotIndex → original-task lookup so we can pull source/newCharm.
        const taskBySlot = new Map();
        for (const t of (s.tasks || [])) {
          if (Number.isFinite(Number(t?.slotIndex))) taskBySlot.set(Number(t.slotIndex), t);
        }

        // Compose the slots array. We emit one entry per planned slot
        // (not just the ones that came back from Gemini), so the Review
        // tab can render every slot with its source label even if a
        // particular gen failed.
        const planSlotsByIndex = new Map();
        for (const t of (s.tasks || [])) {
          if (Number.isFinite(Number(t?.slotIndex))) planSlotsByIndex.set(Number(t.slotIndex), t);
        }
        const allSlotIndices = Array.from(planSlotsByIndex.keys()).sort((a, b) => a - b);

        const slotsOut = allSlotIndices.map((slotIdx) => {
          const t = planSlotsByIndex.get(slotIdx);
          // Look up generation result, if any (copies have no result entry — they
          // succeeded synchronously at submit time).
          const r = slotResults.find((x) => x.slotIndex === slotIdx);
          const isCopy = String(t?.type) === "copy";
          // For copy tasks, source == the synced source; output path is canonical.
          const slot = slotIdx + 1;
          if (isCopy) {
            return {
              slot,
              type: "copy",
              source: t?.source_storage_path || null,
              originalSource: t?.source_storage_path || null,
              newCharm: null,
              output: `${s.outputBasePath}/Slot_${slot}.png`,
            };
          }
          // Gen task. r may be undefined if Gemini didn't return this key
          // (e.g., upstream filtering); we still emit the slot for UI consistency.
          return {
            slot,
            type: r?.ok === false ? "error" : "gen",
            source: t?.input_storage_path || null,
            originalSource: t?.input_storage_path || null,
            newCharm: t?.input_charm_storage_path || null,
            output: (r?.ok && r?.storagePath) ? r.storagePath : null,
            error: r?.error || null,
          };
        });

        // Charm Maker sets: the client wrote a full charm-maker manifest
        // (sourceCharm, sourceSnapshot, charmConcept, charmConceptPlan, …)
        // at submit time and batch_submit persisted a copy. Preserve that
        // shape — the Charm Maker card UI, ref preview, concept summary and
        // Redo flow all read those fields — and just stamp the batch
        // metadata + slot results onto it.
        if (String(s.setKind || "") === "charm_maker") {
          const cmManifest = {
            ...((s.manifest && typeof s.manifest === "object") ? s.manifest : {}),
            batchMode: true,
            batchName,
            batchState: state,
            partial: forceMode && state !== "JOB_STATE_SUCCEEDED",
            imageSize: docData.imageSize || "2K",
            model: (s.manifest && s.manifest.model) || docData.model || preferredCharmRenderModelId(),
            batchSlots: slotsOut,
            collectedAt: new Date().toISOString(),
          };
          const cmManifestPath = `${s.outputBasePath}/manifest.json`;
          try {
            await bucket.file(cmManifestPath).save(
              Buffer.from(JSON.stringify(cmManifest, null, 2), "utf8"),
              { contentType: "application/json", resumable: false }
            );
          } catch (e) {
            failures.push({ key: `manifest:${s.outputBasePath}`, error: String(e?.message || e) });
          }
          continue;
        }

        const m = {
          category: s.category,
          setN: s.setN,
          outputBasePath: s.outputBasePath,
          // Lock the listing's charm into the manifest so the Review tab's
          // Redo flow can recover it even after batch_collect moves the
          // charm out of New_Charms/ into Used_*_Charm_Pool/. We pull
          // from the first gen task with an input_charm_storage_path; in
          // batch mode every gen task in a set shares the same charm
          // (one charm per listing).
          sourceCharm: (() => {
            for (const t of (s.tasks || [])) {
              if (String(t?.type) !== "copy" && t?.input_charm_storage_path) {
                return t.input_charm_storage_path;
              }
            }
            return null;
          })(),
          sourceCharmName: (() => {
            for (const t of (s.tasks || [])) {
              if (String(t?.type) !== "copy" && t?.input_charm_storage_path) {
                return String(t.input_charm_storage_path).split("/").pop() || null;
              }
            }
            return null;
          })(),
          timestamp: new Date().toISOString(),
          model: docData.model || preferredCharmRenderModelId(),
          batchMode: true,
          batchName,
          batchState: state,
          partial: forceMode && state !== "JOB_STATE_SUCCEEDED",
          imageSize: docData.imageSize || "2K",
          slots: slotsOut,
        };
        const manifestPath = `${s.outputBasePath}/manifest.json`;
        try {
          await bucket.file(manifestPath).save(
            Buffer.from(JSON.stringify(m, null, 2), "utf8"),
            { contentType: "application/json", resumable: false }
          );
        } catch (e) {
          failures.push({ key: `manifest:${s.outputBasePath}`, error: String(e?.message || e) });
        }
      }

      // Move used charms out of the active pool.
      // ---------------------------------------
      // After successful collection, every unique charm path referenced
      // in this batch's tasks is moved to the matching Used pool:
      //   New_Charms          → Used_Necklace_Charm_Pool
      //   New_Charms_Earrings → Used_Earring_Charm_Pool
      //
      // This ensures the same charm never gets picked again by a
      // future submission. Move happens AFTER images write so a
      // mid-collection failure doesn't strand the charm in the wrong
      // place. We use copy+delete (not rename, which Firebase Storage
      // doesn't support natively) and ignore per-charm errors so a
      // single flaky charm doesn't block the batch from being marked
      // collected.
      const charmsUsed = new Set();
      for (const s of setsMeta) {
        for (const t of (s.tasks || [])) {
          const p = t?.input_charm_storage_path;
          if (p) charmsUsed.add(p);
        }
      }
      const charmMoveErrors = [];
      let charmsMoved = 0;
      for (const srcPath of charmsUsed) {
        try {
          let destPrefix = null;
          if (srcPath.includes("/Charm_Maker/New_Charms_Earrings/")) {
            destPrefix = "listing-generator-1/Charm_Maker/Used_Earring_Charm_Pool/";
          } else if (srcPath.includes("/Charm_Maker/New_Charms/")) {
            destPrefix = "listing-generator-1/Charm_Maker/Used_Necklace_Charm_Pool/";
          } else {
            // Charm came from somewhere else — don't move it.
            continue;
          }
          const filename = srcPath.split("/").pop();
          if (!filename) continue;
          const destPath = destPrefix + filename;

          const srcFile = bucket.file(srcPath);
          const [exists] = await srcFile.exists();
          if (!exists) {
            // Already moved by an earlier collection of an overlapping
            // batch — not an error, just skip.
            continue;
          }
          await srcFile.copy(bucket.file(destPath));
          // Preserve a download token on the destination so it renders
          // in any UI that expects one.
          try {
            await bucket.file(destPath).setMetadata({
              metadata: { firebaseStorageDownloadTokens: newDownloadToken() },
            });
          } catch (_) {}
          await srcFile.delete();
          charmsMoved++;
        } catch (e) {
          charmMoveErrors.push({ charm: srcPath, error: String(e?.message || e) });
        }
      }

      // Mark Firestore record as collected.
      await firestoreRetry(
        () => db.collection(BATCHES_COLL).doc(docId).set({
          collected: true,
          collectedAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          results: {
            succeededCount, failedCount, failures: failures.slice(0, 200),
            charmsMoved, charmMoveErrors: charmMoveErrors.slice(0, 50),
          },
        }, { merge: true }),
        "batch.markCollected"
      );

      return json(200, {
        ok: true, batchName, collected: true,
        succeededCount, failedCount,
        charmsMoved, charmMoveErrors: charmMoveErrors.slice(0, 20),
        failures: failures.slice(0, 50),
      });
    }

    if (kind === "batch_list") {
      const includeCollected = !!body?.includeCollected;
      // Ceiling raised from 200 → 1000 so the panel and the per-batch
      // collect-poll loop can find batches in submissions of up to 1000
      // sets. Default stays at 50 (cheap query for the common small case).
      const limit = clampNumber(body?.limit, 1, 1000, 50);
      const db = getDb();
      let q = db.collection(BATCHES_COLL).orderBy("createdAt", "desc").limit(limit);
      const snap = await q.get();
      const out = [];
      snap.forEach((doc) => {
        const d = doc.data();
        if (!includeCollected && d.collected) return;
        out.push({
          docId: doc.id,
          batchName: d.batchName,
          displayName: d.displayName,
          sessionId: d.sessionId || null,
          model: d.model || preferredCharmRenderModelId(),
          state: d.state,
          collected: !!d.collected,
          batchStats: d.batchStats || null,
          createdAt: d.createdAt?.toMillis ? d.createdAt.toMillis() : null,
          updatedAt: d.updatedAt?.toMillis ? d.updatedAt.toMillis() : null,
          collectedAt: d.collectedAt?.toMillis ? d.collectedAt.toMillis() : null,
          setsCount: Array.isArray(d.sets) ? d.sets.length : 0,
          requestCount: Array.isArray(d.routes) ? d.routes.length : 0,
          results: d.results || null,
        });
      });
      return json(200, { ok: true, batches: out });
    }

    if (kind === "batch_cancel") {
      const apiKey = process.env.GEMINI_API_KEY;
      if (!apiKey) return json(400, { error: { message: "Missing GEMINI_API_KEY env var" } });
      const batchName = String(body?.batchName || "").trim();
      if (!batchName.startsWith("batches/")) {
        return json(400, { error: { message: "batchName must start with batches/" } });
      }
      await cancelGeminiBatchJob(apiKey, batchName);
      try {
        const db = getDb();
        await firestoreRetry(
          () => db.collection(BATCHES_COLL).doc(batchDocIdFromName(batchName)).set({
            state: "JOB_STATE_CANCELLED",
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          }, { merge: true }),
          "batch.cancelMirror"
        );
      } catch (_) { /* non-fatal */ }
      return json(200, { ok: true, cancelled: true, batchName });
    }

    // ------------------------------------------------------------
    // files_cleanup
    //   Lists ALL files uploaded to the Gemini Files API for this API key
    //   and deletes them. Used to free the 20 GB cumulative
    //   file_storage_bytes quota that accumulates across batch_submit
    //   JSONL/reference uploads.
    //
    //   Optional body params:
    //     maxDelete  number  — cap deletion at N files this call (default:
    //                          no cap; use for chunked cleanup if you
    //                          have thousands of files and the function
    //                          might time out)
    //     prefix     string  — only delete files whose displayName starts
    //                          with this prefix (e.g., "lg1-Beady_Necklace-").
    //                          Useful if the same API key is shared with
    //                          other projects.
    //
    //   Returns:
    //     { ok, deleted, totalListed, candidateCount, bytesFreed,
    //       truncated, errors }
    //
    //   CAUTION: deleting Files API uploads breaks any in-flight batch
    //   jobs that still reference those input files. Caller is responsible
    //   for confirmation before invoking.
    // ------------------------------------------------------------
    if (kind === "files_cleanup") {
      const apiKey = process.env.GEMINI_API_KEY;
      if (!apiKey) return json(400, { error: { message: "Missing GEMINI_API_KEY env var" } });

      const maxDeleteRaw = Number(body?.maxDelete);
      const maxDelete = Number.isFinite(maxDeleteRaw) && maxDeleteRaw > 0 ? maxDeleteRaw : Infinity;
      const prefix = typeof body?.prefix === "string" && body.prefix.length > 0 ? body.prefix : null;

      const baseUrl = "https://generativelanguage.googleapis.com/v1beta";

      // 1) Paginate through all files. Hard guard against runaway
      //    pagination at 200 pages × 100/page = 20,000 files.
      const listed = [];
      let pageToken = null;
      let pages = 0;
      const MAX_PAGES = 200;
      do {
        const url = `${baseUrl}/files?key=${encodeURIComponent(apiKey)}&pageSize=100` +
                    (pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : "");
        const r = await fetch(url);
        if (!r.ok) {
          const errBody = await r.text().catch(() => "");
          return json(r.status, {
            error: { message: `Files API list failed: HTTP ${r.status}: ${errBody.slice(0, 300)}` },
          });
        }
        const data = await r.json();
        for (const f of (data.files || [])) listed.push(f);
        pageToken = data.nextPageToken || null;
        pages++;
      } while (pageToken && pages < MAX_PAGES);

      // 2) Optional displayName prefix filter
      const candidates = prefix
        ? listed.filter((f) => typeof f.displayName === "string" && f.displayName.startsWith(prefix))
        : listed.slice();

      // 3) Cap deletion count (for chunked cleanup if maxDelete supplied)
      const toDelete = maxDelete === Infinity
        ? candidates
        : candidates.slice(0, maxDelete);

      // 4) Delete with concurrency 10 — fast enough that even 1000s of
      //    files complete inside the 15-minute background-function budget,
      //    while not so parallel that we trigger our own rate-limit on
      //    the Files API delete endpoint.
      let deleted = 0;
      let bytesFreed = 0;
      const errors = [];
      let idx = 0;
      const CONC = 10;

      async function deleteWorker() {
        while (idx < toDelete.length) {
          const myIdx = idx++;
          const f = toDelete[myIdx];
          try {
            const delUrl = `${baseUrl}/${f.name}?key=${encodeURIComponent(apiKey)}`;
            const r = await fetch(delUrl, { method: "DELETE" });
            if (!r.ok) {
              const errBody = await r.text().catch(() => "");
              errors.push(`${f.name}: HTTP ${r.status} ${errBody.slice(0, 80)}`);
            } else {
              deleted++;
              bytesFreed += Number(f.sizeBytes || 0);
            }
          } catch (e) {
            errors.push(`${f.name}: ${e?.message || e}`);
          }
        }
      }

      const workers = [];
      for (let i = 0; i < CONC; i++) workers.push(deleteWorker());
      await Promise.all(workers);

      return json(200, {
        ok: true,
        deleted,
        totalListed: listed.length,
        candidateCount: candidates.length,
        bytesFreed,
        truncated: candidates.length > toDelete.length,
        errors: errors.slice(0, 20),
      });
    }

// ------------------------------------------------------------
    // NEW: run_set_async
    // - One request kicks off the whole set
    // - Server processes tasks ASYNCHRONOUSLY (Parallel)
    // - Enforces delayMs only on START times (staggered launch)
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

      let apiKey;
      try { apiKey = apiKeyForImageModel(modelConfig); }
      catch (err) { return json(400, { error: safeErr(err) }); }

      const runToken =
        globalThis.crypto?.randomUUID ? globalThis.crypto.randomUUID() : require("crypto").randomUUID();

      // ✅ FIX: Use Promise.all() to run tasks in parallel
      // We map the tasks to an array of promises, allowing them to execute concurrently.
      await Promise.all(tasks.map(async (t, i) => {
        const slot = Number(t?.slotIndex);
        if (!Number.isFinite(slot) || slot < 0) return;

        try {
          // ✅ STAGGERED START:
          // Instead of waiting for the previous task to *finish*, we only delay the *start*.
          // Task 0 starts at 0ms. Task 1 starts at 1500ms (if delayMs=1500).
          // They will all be processing simultaneously after the initial delay.
          const startDelay = i * delayMs;
          if (startDelay > 0) await sleep(startDelay);

          const bucket = admin.storage().bucket();

          // 1. Handle "Copy" tasks (instant)
          if (String(t?.type) === "copy") {
            const src = String(t?.source_storage_path || "").trim();
            if (!src) throw new Error("copy task missing source_storage_path");
            const dst = `${base}/Slot_${slot + 1}.png`;
            const dstFile = bucket.file(dst);
            
            await bucket.file(src).copy(dstFile);

            // Ensure browser previews can load: getDownloadURL() relies on firebaseStorageDownloadTokens.
            const token = newDownloadToken();
            await dstFile.setMetadata({ metadata: { firebaseStorageDownloadTokens: token } });
            return; // Done with this task
          }

          // 2. Handle image edit tasks with the selected provider.
          const basePath0 = String(t?.input_storage_path || "").trim();
          const basePath1 = String(t?.input_charm_storage_path || "").trim();
          const promptT = String(t?.prompt || "").trim();
          
          if (!basePath0 || !basePath1 || !promptT) {
               console.warn(`Skipping invalid task slot ${slot}`);
               return;
          }

          const img0 = await storagePathToBuffer(basePath0);
          const img1 = await storagePathToBuffer(basePath1);

          // Retry transient overload/rate-limit failures for either provider.
          let outBuf = await callImageWithRetry(async () => {
             return await callImageModelEdits({
              apiKey,
              model,
              prompt: promptT,
              size: String(t?.size || body?.size || "2048x2048"),
              quality: String(
                t?.quality ||
                body?.quality ||
                (modelConfig.provider === "openai" ? "medium" : "high")
              ),
              output_format: "png",
              images: [
                { buffer: img0.buffer, mime: img0.mime, filename: filenameForMime("image0", img0.mime) },
                { buffer: img1.buffer, mime: img1.mime, filename: filenameForMime("image1", img1.mime) },
              ],
            });
          }, `${modelConfig.provider}_slot_${slot}`);

          outBuf = await applyFinalFrameZoomIfNeeded(outBuf, t?.postprocess || body?.postprocess);
          
          // Upload result (Client polling will detect this file appearing)
          await uploadPngBufferToSetPath(outBuf, base, slot, null, runToken);

        } catch (err) {
          console.error(`[run_set_async] task failed at slot ${slot}`, safeErr(err));
          // We catch errors here so Promise.all() doesn't fail the entire set if one slot fails.
        }
      }));

      // Return successful completion (client received 202 long ago)
      return json(200, { ok: true, finished: true, runId: runToken });
    }

    if (kind === "snapshot_charm_reference") {
      const cat = normalizeCategory(activeCategory);
      if (cat !== "Charms") {
        return json(400, { error: { message: "snapshot_charm_reference is restricted to Charms" } });
      }
      const src = String(source_storage_path || "").trim();
      if (!src) return json(400, { error: { message: "source_storage_path is required" } });

      const allowedSource = [
        "listing-generator-1/Charm_Maker/",
        "listing-generator-1/Charms/",
      ].some((prefix) => src.startsWith(prefix));
      if (!allowedSource) {
        return json(400, { error: { message: "source_storage_path not allowed" } });
      }
      const base = assertAllowedOutputBase(output_base_path);
      const bucket = admin.storage().bucket();
      const sourceFile = bucket.file(src);
      const [exists] = await sourceFile.exists();
      if (!exists) return json(404, { error: { message: "source_storage_path not found" } });
      let mime = "image/png";
      try {
        const [meta] = await sourceFile.getMetadata();
        mime = String(meta?.contentType || mime).toLowerCase();
      } catch (_) {}
      const ext = mime.includes("jpeg") || mime.includes("jpg")
        ? "jpg"
        : mime.includes("webp")
          ? "webp"
          : "png";
      const dst = `${base}/Source_Ref.${ext}`;
      const dstFile = bucket.file(dst);
      await sourceFile.copy(dstFile);
      const token = newDownloadToken();
      await dstFile.setMetadata({ metadata: { firebaseStorageDownloadTokens: token } });
      return json(200, {
        ok: true,
        storagePath: dst,
        downloadURL: tokenDownloadURLFor(bucket.name, dst, token),
      });
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
      // input_charm_storage_path is OPTIONAL.
      //   • Two-image flow (regeneration / fresh composite): caller supplies
      //     both — image0 is the model/reference, image1 is the charm — and
      //     the selected image model composites them per the prompt.
      //   • One-image flow (in-place edit / Listing-Generator adjustment-mode
      //     redo): caller supplies only basePath0 and a prompt that describes
      //     the requested edit. We send a single image so the model does
      //     not search for differences between two identical inputs or try
      //     to composite them.

      let apiKey;
      try { apiKey = apiKeyForImageModel(modelConfig); }
      catch (err) { return json(400, { error: safeErr(err) }); }

      const outputBasePath = assertAllowedOutputBase(output_base_path);
      const effectiveSlot = Number.isFinite(Number(slotIndex)) && Number(slotIndex) >= 0 ? Number(slotIndex) : 0;

      const img0 = await storagePathToBuffer(basePath0);
      const img1 = basePath1 ? await storagePathToBuffer(basePath1) : null;

      const images = [
        { buffer: img0.buffer, mime: img0.mime, filename: filenameForMime("image0", img0.mime) },
      ];
      if (img1) {
        images.push({ buffer: img1.buffer, mime: img1.mime, filename: filenameForMime("image1", img1.mime) });
      }

      // ------------------------------------------------------------
      // Charm Maker pipeline (any request declaring a charm policy):
      //   1. TEXT-ONLY prompt preflight by an independent audit agent
      //      (best-effort — a failed audit never blocks generation);
      //   2. exactly ONE image generation, with the geometry/background
      //      policies appended by the backend as the request's final word;
      //   3. no post-generation image QA, no masking, no flood fill, no
      //      automatic second image — what the model returns is what the
      //      operator reviews.
      // ------------------------------------------------------------
      const isCharmPipeline = !!(background_policy || charm_geometry_policy || charm_edit_intent);
      let effectivePrompt = prompt;
      if (isCharmPipeline) {
        const audited = await auditCharmPromptPreflight({
          prompt,
          backgroundPolicy: background_policy,
          geometryPolicy: charm_geometry_policy,
          editIntent: charm_edit_intent,
          imageCount: images.length,
          imageRoles: image_roles,
        });
        if (audited) effectivePrompt = audited;
      }

      let outBuf = await callImageModelEdits({
        apiKey,
        model,
        prompt: effectivePrompt,
        size,
        quality,
        output_format,
        images,
        imageRoles: image_roles,
        charmGeometryPolicy: charm_geometry_policy,
        backgroundPolicy: background_policy,
      });

      outBuf = await applyFinalFrameZoomIfNeeded(outBuf, postprocess);

      // Write the caller-supplied design description into the PNG itself so
      // the description travels with the image through approvals, pool moves
      // and downloads.
      outBuf = embedPngTextMetadata(outBuf, embed_metadata);

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

    // ============================================================
    // move_set_to_completed (v5.31)
    // ------------------------------------------------------------
    // Server-side replacement for the browser-driven
    // runBackgroundApproval flow in Listing_Generator_1.html.
    //
    // The old flow downloaded every PNG from Ready_To_List/Set_N
    // into the browser, re-uploaded it to Completed_Listing_Sets/
    // {cat}_Set_N, then deleted the source. That round-tripped 100%
    // of the bytes through the operator's bandwidth and Firebase
    // egress on every approval. For our typical 4–8 MB per slot ×
    // 4–6 slots per set × dozens of approvals/day, this was a
    // significant chunk of the monthly bandwidth bill — and worse,
    // it could leave files orphaned in Ready_To_List if any step
    // failed mid-loop because the UI hides the set immediately
    // (addHiddenSets) without retrying.
    //
    // This handler does the same work entirely server-side:
    //   • bucket.file(src).copy(dst) is a Google-internal byte
    //     transfer with no egress charge.
    //   • Manifest tidying for skipped slots happens here too, so
    //     the client doesn't need to know about manifest structure.
    //   • Per-file errors are collected and returned so the client
    //     can decide whether to surface them; the loop continues
    //     past failures (best-effort) so a single flaky file
    //     doesn't strand a 95%-complete approval.
    //
    // Body:
    //   {
    //     kind        : "move_set_to_completed",
    //     activeCategory: "Beady_Necklace" | ... (one of GENERATABLE_CATEGORIES),
    //     setName     : "Set_42",           // matches /^Set_\d+$/
    //     skippedSlots: ["Slot_3.png", ...] // optional; these get
    //                                       // deleted from src instead of moved
    //   }
    //
    // Response:
    //   { ok: true, moved: N, skippedDeleted: M, errors: [...] }
    //
    // The client should treat ANY non-2xx response or `ok:false` as
    // a signal to fall back to the legacy browser-mediated flow so
    // approvals never get stuck on a deploy lag.
    if (kind === "move_set_to_completed") {
      const cat = normalizeCategory(activeCategory);
      if (!GENERATABLE_CATEGORIES.has(cat)) {
        return json(400, { error: { message: "activeCategory not generatable" } });
      }
      const setName = String(body?.setName || "").trim();
      if (!/^Set_\d+$/.test(setName)) {
        return json(400, { error: { message: "setName must look like 'Set_<n>'" } });
      }
      const skippedSlots = Array.isArray(body?.skippedSlots)
        ? body.skippedSlots.filter((s) => typeof s === "string" && /^Slot_\d+\.png$/.test(s))
        : [];
      const skipSet = new Set(skippedSlots);

      const bucket = admin.storage().bucket();
      const srcPrefix = `listing-generator-1/${cat}/Ready_To_List/${setName}/`;
      const dstPrefix = `listing-generator-1/Generated_Listing_Sets/Completed_Listing_Sets/${cat}_${setName}/`;

      // Step 1: Enumerate source files. getFiles with a prefix is a
      // single-shot list — no pagination needed at this scale (a set
      // is at most ~10 files).
      let files;
      try {
        const [f] = await bucket.getFiles({ prefix: srcPrefix });
        files = f;
      } catch (e) {
        return json(500, { ok: false, error: { message: `getFiles failed: ${e?.message || e}` } });
      }
      if (!files.length) {
        // Nothing to do — set folder is already empty. Treat as success
        // so callers (and the optional fallback path on the client) can
        // proceed without surfacing a confusing "no files" error.
        return json(200, { ok: true, moved: 0, skippedDeleted: 0, errors: [], note: "source folder empty" });
      }

      // Step 2: If there are skipped slots and a manifest is present,
      // tidy the manifest before moving. We mirror the exact transform
      // the old browser code performed:
      //   slots[i] = { ...slots[i], skipped: true, output: null }
      //     for any slot whose Slot_<n>.png is in skipSet.
      // This keeps the manifest semantically truthful in its new home.
      const manifestFile = files.find((f) => f.name === srcPrefix + "manifest.json");
      let tidiedManifestBuf = null;
      if (manifestFile && skipSet.size > 0) {
        try {
          const [raw] = await manifestFile.download();
          const data = JSON.parse(raw.toString("utf8"));
          const tidied = {
            ...data,
            slots: (data.slots || []).map((s) =>
              s && typeof s.slot === "number" && skipSet.has(`Slot_${s.slot}.png`)
                ? { ...s, skipped: true, output: null }
                : s
            ),
          };
          tidiedManifestBuf = Buffer.from(JSON.stringify(tidied, null, 2), "utf8");
        } catch (e) {
          // Manifest unreadable — proceed without tidying. The original
          // manifest will be copied verbatim below, which mirrors the
          // browser code's fallback when its own manifest read fails.
          console.warn(`[move_set_to_completed] manifest read failed for ${srcPrefix}: ${e?.message || e}`);
        }
      }

      // Step 3: Walk files, mirroring the browser logic.
      let moved = 0;
      let skippedDeleted = 0;
      const errors = [];
      for (const f of files) {
        const filename = f.name.slice(srcPrefix.length);
        if (!filename) continue; // pseudo-folder entry

        try {
          if (skipSet.has(filename)) {
            // Skipped slot → delete from src, do not copy.
            await f.delete();
            skippedDeleted++;
          } else if (filename === "manifest.json" && tidiedManifestBuf) {
            // Manifest with tidying applied → write tidied version to
            // dst, then delete src.
            const dstFile = bucket.file(dstPrefix + filename);
            await dstFile.save(tidiedManifestBuf, {
              contentType: "application/json",
              resumable: false,
            });
            await f.delete();
            moved++;
          } else {
            // Default: server-side copy + delete.
            const dstFile = bucket.file(dstPrefix + filename);
            await f.copy(dstFile);
            await f.delete();
            moved++;
          }
        } catch (e) {
          // Per-file failure: log + continue. Returning the error list
          // lets the client decide whether to retry or fall back.
          errors.push({ file: filename, error: String(e?.message || e) });
        }
      }

      return json(200, {
        ok: errors.length === 0,
        moved,
        skippedDeleted,
        errors: errors.slice(0, 20),
        srcPrefix,
        dstPrefix,
      });
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

    const apiKey = apiKeyForImageModel(modelConfig);

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
        baseNoCharmBuf = await callImageModelEdits({
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
      outBuf = await callImageModelGenerations({
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

      // Same Charm Maker pipeline as the set-based edits branch: text-only
      // preflight audit, then exactly one generation with the policies as
      // the backend's final word. No pixel post-processing.
      let effectivePrompt = prompt;
      if (background_policy || charm_geometry_policy || charm_edit_intent) {
        const audited = await auditCharmPromptPreflight({
          prompt,
          backgroundPolicy: background_policy,
          geometryPolicy: charm_geometry_policy,
          editIntent: charm_edit_intent,
          imageCount: images.length,
          imageRoles: image_roles,
        });
        if (audited) effectivePrompt = audited;
      }

      outBuf = await callImageModelEdits({
        apiKey,
        model,
        prompt: effectivePrompt,
        size,
        quality,
        output_format,
        images,
        imageRoles: image_roles,
        charmGeometryPolicy: charm_geometry_policy,
        backgroundPolicy: background_policy,
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
