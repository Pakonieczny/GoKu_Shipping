/*  netlify/functions/etsyMailCollateral.js
 *
 *  v2.0 Step 2.5 — Curated collateral retrieval.
 *
 *  This is the smallest, lowest-risk piece in the v2.0 plan: pure
 *  retrieval of operator-curated URLs (line sheets, product cards,
 *  lookbooks, image sets, terms PDFs). The AI never uploads anything;
 *  the owner uploads files manually (e.g., to Firebase Storage or
 *  any external host) and registers the URL here.
 *
 *  ═══ FOUR OPS ═══════════════════════════════════════════════════════════
 *
 *  POST { op: "search", category?, kind?, keywords?, limit? }
 *      AI tool path. Returns matches by category + kind + optional
 *      keyword overlap. Both roles + the agent can call this.
 *
 *  POST { op: "list", includeInactive? }
 *      UI catalog browser. Both roles can read.
 *
 *  POST { op: "create", actor, item }
 *      OWNER-ONLY. Registers a new collateral entry pointing at an
 *      already-uploaded URL.
 *
 *  POST { op: "update", actor, id, patch }
 *      OWNER-ONLY. Edits metadata or marks active:false.
 *
 *  ═══ COLLATERAL SHAPE ═════════════════════════════════════════════════
 *
 *    EtsyMail_Collateral/{id} = {
 *      id,
 *      category    : "necklace" | "ring" | "wedding" | ...,
 *      kind        : "line_sheet" | "product_card" | "lookbook" |
 *                    "image_set" | "terms" |
 *                    "fit_reference" | "metal_comparison" |
 *                    "care_instructions" | "bracelet_sizing",
 *      name        : "<short display title>",
 *      url         : "<https://...>",
 *      description : "<one-paragraph blurb shown to the AI>",
 *      keywords    : ["<extra match terms>"],
 *      active      : true,
 *      lastUsedAt  : Timestamp | null,    // updated by search() when AI uses it
 *      approvedBy  : "<employeeName>",
 *      approvedAt  : Timestamp,
 *      createdBy, createdAt, updatedAt, lastUpdatedBy
 *    }
 *
 *  ═══ EXPORTED HELPER ══════════════════════════════════════════════════
 *
 *    module.exports.searchCollateral({ category, kind, keywords, limit })
 *
 *  Direct-import path for etsyMailSalesAgent (matches Step 1's
 *  searchListings pattern; no HTTP round-trip from agent's tool loop).
 */

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { requireOwner, logUnauthorized } = require("./_etsyMailRoles");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// v2.5 — Storage bucket for direct file uploads. The bucket is initialized
// in firebaseAdmin.js via FIREBASE_STORAGE_BUCKET; admin.storage().bucket()
// with no args returns the default. The Admin SDK bypasses Storage rules,
// so the user's fallback `allow read, write: if false` does not block our
// writes from this server-side path.
const bucket = admin.storage().bucket();

const COLLATERAL_COLL = "EtsyMail_Collateral";
const AUDIT_COLL      = "EtsyMail_Audit";

const VALID_KINDS = new Set([
  "line_sheet", "product_card", "lookbook", "image_set", "terms",
  // v5.24 — Sales agent auto-attachment kinds. The prompt/agent already
  // emits these for care, metals, fit, and bracelet sizing cards; the
  // collateral admin API must accept them too.
  "fit_reference", "metal_comparison", "care_instructions", "bracelet_sizing"
]);

const SAFE_FIELDS = new Set([
  "category", "kind", "name", "url", "description", "keywords", "active",
  // v2.5: storage metadata for files uploaded through op:"upload". Saved
  // alongside the doc so deleteFile can clean up the underlying GCS object
  // when an entry is deactivated/removed. Storing both fields together
  // avoids ambiguity — `url` is what the AI/customer sees, `storagePath`
  // is what we use to reach the file via the Admin SDK.
  "storagePath", "storageBucket", "uploadedFilename", "uploadedContentType",
  "uploadedSizeBytes"
]);

const SEARCH_DEFAULT_LIMIT = 5;
const SEARCH_MAX_LIMIT     = 20;

// v2.5 — Upload limits + allowlist
//
// Netlify functions have a 6 MB request body cap. Base64 inflates payloads
// by ~33%, so the largest raw file we can accept is ~4.5 MB. We cap at
// 4_500_000 to leave headroom for JSON envelope overhead.
//
// Allowlist is intentionally narrow: collateral is line sheets, product
// cards, lookbooks, image sets, terms PDFs. Anything else is suspicious
// (executables, archives, office docs that customers can't open inline).
const UPLOAD_MAX_BYTES = 4_500_000;
const UPLOAD_ALLOWED_CONTENT_TYPES = new Set([
  "image/jpeg", "image/png", "image/webp", "image/gif",
  "application/pdf"
]);
const UPLOAD_PATH_PREFIX = "etsymail-collateral";

// ─── Helpers ────────────────────────────────────────────────────────────

function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(body)             { return json(200, { ...body }); }

async function writeAudit({ eventType, actor = "system:collateral", payload = {},
                            outcome = "success", ruleViolations = [] }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId: null, draftId: null,
      eventType, actor, payload,
      createdAt: FV.serverTimestamp(),
      outcome, ruleViolations
    });
  } catch (e) {
    console.warn("collateral audit write failed:", e.message);
  }
}

/** Trim a stored collateral doc to the shape returned to the AI / UI.
 *  The AI gets `description` (so it can decide whether to reference it)
 *  but not internal fields like `approvedBy` (no value to the AI). */
function trimForCaller(doc) {
  // v4.3.12 — Expose the storage-mirror fields that exist when the
  // collateral was uploaded through the system (op:"upload"). The
  // agent uses these to construct an `image` attachment on its draft
  // for line-sheet sends — without `storagePath` + `uploadedContentType`,
  // normalizeAttachments in etsyMailDraftSend rejects the entry.
  // Entries created via op:"create" (external URL, no upload) will
  // not have these fields; the agent treats those as link-only.
  //
  // v5.0.2 BUGFIX — field-name mismatch.
  // The upload pipeline (uploadCollateralFile, line ~400) writes the
  // MIME type as `uploadedContentType`. The previous version of this
  // function read `doc.contentType` and exposed it as `out.contentType`
  // — neither field name matches what's on disk OR what the consumer
  // (agent's findAttachableForKind) checks for. Result: every
  // operator-uploaded line sheet was failing the agent's attachable-
  // entry check because uploadedContentType was undefined on the
  // trimmed result. Heidi-thread bug confirmed via audit row showing
  // lineSheetAttach.reason = "no_active_collateral_for_kind" while
  // a perfectly valid line sheet existed in Firestore.
  // Fix: read `uploadedContentType` (the actual persisted field) AND
  // also tolerate `contentType` for legacy/future flexibility, and
  // expose under both names so any downstream consumer works.
  const out = {
    id          : doc.id,
    category    : doc.category,
    kind        : doc.kind,
    name        : doc.name,
    url         : doc.url,
    description : doc.description || "",
    keywords    : doc.keywords || [],
    active      : doc.active !== false
  };
  if (doc.storagePath)        out.storagePath        = doc.storagePath;

  // Resolve content type from either field name. uploadedContentType is
  // what the upload pipeline writes; contentType is the older name kept
  // for compatibility.
  const resolvedContentType = doc.uploadedContentType || doc.contentType || null;
  if (resolvedContentType) {
    out.uploadedContentType = resolvedContentType;
    out.contentType         = resolvedContentType;   // legacy alias
  }

  // Filename: same dual-name treatment.
  const resolvedFilename = doc.uploadedFilename || doc.fileName || null;
  if (resolvedFilename) {
    out.uploadedFilename = resolvedFilename;
    out.fileName         = resolvedFilename;   // legacy alias
  }

  if (typeof doc.uploadedSizeBytes === "number") out.uploadedSizeBytes = doc.uploadedSizeBytes;
  if (typeof doc.bytes === "number")             out.bytes             = doc.bytes;
  return out;
}

function sanitizePatch(rawPatch) {
  if (!rawPatch || typeof rawPatch !== "object") {
    return { ok: false, reason: "PATCH_NOT_OBJECT" };
  }
  const patch = {};
  for (const k of Object.keys(rawPatch)) {
    if (!SAFE_FIELDS.has(k)) continue;
    patch[k] = rawPatch[k];
  }
  if ("category" in patch) {
    if (typeof patch.category !== "string" || !patch.category.trim()) {
      return { ok: false, reason: "category_REQUIRED" };
    }
    patch.category = patch.category.trim();
  }
  if ("kind" in patch) {
    if (!VALID_KINDS.has(patch.kind)) {
      return { ok: false, reason: "kind_INVALID", allowed: Array.from(VALID_KINDS) };
    }
  }
  if ("name" in patch) {
    if (typeof patch.name !== "string" || !patch.name.trim()) {
      return { ok: false, reason: "name_REQUIRED" };
    }
    patch.name = patch.name.trim().slice(0, 200);
  }
  if ("url" in patch) {
    if (typeof patch.url !== "string" || !/^https?:\/\//.test(patch.url)) {
      return { ok: false, reason: "url_MUST_BE_HTTP_OR_HTTPS" };
    }
    if (patch.url.length > 2000) {
      return { ok: false, reason: "url_TOO_LONG" };
    }
  }
  if ("description" in patch) {
    if (typeof patch.description !== "string") {
      return { ok: false, reason: "description_MUST_BE_STRING" };
    }
    patch.description = patch.description.slice(0, 1500);
  }
  if ("keywords" in patch) {
    if (!Array.isArray(patch.keywords)) {
      return { ok: false, reason: "keywords_MUST_BE_ARRAY" };
    }
    patch.keywords = patch.keywords
      .map(k => String(k).trim().toLowerCase())
      .filter(k => k.length >= 2 && k.length <= 60)
      .slice(0, 30);
  }
  if ("active" in patch) patch.active = patch.active === true;

  return { ok: true, patch };
}

// ─── Search — exported for direct-import by sales agent ────────────────

/** Return collateral matching category + optional kind + optional
 *  keyword overlap. Active items only. Sorted by:
 *    1. exact category match score (always positive — non-matches return 0)
 *    2. kind match (if kind specified)
 *    3. keyword overlap (count of keywords matching)
 *    4. lastUsedAt desc (recency tie-breaker)
 *
 *  Returns: { matches: [trimForCaller(doc)], count, totalScored }
 */
const CATEGORY_ALIASES = {
  necklace        : ["necklace", "pendant"],
  huggie          : ["huggie", "hoop"],
  stud            : ["stud"],
  earring         : ["stud", "huggie", "earring"],
  metals_education: ["metal_comparison", "metal", "gold filled"],
  metal_comparison: ["metal_comparison", "metal"],
  aftercare       : ["care_instructions", "care"],
  care            : ["care_instructions", "care"],
  care_instructions: ["care_instructions", "care"],
  bracelet        : ["bracelet"],
  bracelet_sizing : ["bracelet"],
  fit_reference   : ["fit_reference", "fit"],
  fit             : ["fit_reference", "fit"]
};
function categoryAliases(category) {
  const c = String(category || "").trim().toLowerCase();
  const key = Object.keys(CATEGORY_ALIASES).find(k => c === k || c === k + "s" || c.replace(/[\s-]+/g, "_") === k);
  return key ? CATEGORY_ALIASES[key] : [c];
}

async function searchCollateral({ category, kind, keywords, limit } = {}) {
  const cap = Math.max(1, Math.min(parseInt(limit, 10) || SEARCH_DEFAULT_LIMIT, SEARCH_MAX_LIMIT));

  // We always filter on active==true. Either: also filter on category
  // (cheaper) OR scan all active and score in memory (more flexible).
  // Use a category prefilter when supplied; otherwise scan up to 200
  // active items.
  let q = db.collection(COLLATERAL_COLL).where("active", "==", true);
  if (category && typeof category === "string" && category.trim()) {
    q = q.where("category", "==", category.trim());
  }
  let snap = await q.limit(200).get();

  // The AI asks by family ("necklace", "huggie", "stud", "metals_education",
  // "aftercare") but staff store display names ("Custom Necklace Charm",
  // "metal_comparison"). An exact category miss used to return nothing, so
  // get_collateral("necklace") never found the necklace line sheet. On a
  // miss, keep the active items whose category, name or keywords carry the
  // family word or one of its aliases.
  let looseCategory = null;
  if (snap.empty && category && String(category).trim()) {
    looseCategory = categoryAliases(category);
    snap = await db.collection(COLLATERAL_COLL).where("active", "==", true).limit(200).get();
  }

  if (snap.empty) return { matches: [], count: 0, totalScored: 0 };

  const wantKeywords = Array.isArray(keywords)
    ? keywords.map(k => String(k).trim().toLowerCase()).filter(k => k.length >= 2)
    : [];

  const scored = [];
  snap.forEach(d => {
    const data = d.data() || {};
    let score = 1;   // base score for being active + (optionally) category-filtered

    if (looseCategory) {
      const hay = [data.category, data.name, ...(data.keywords || [])].map(v => String(v || "").toLowerCase()).join(" | ");
      if (!looseCategory.some(a => hay.includes(a))) return;
    }

    // Guides are stored as kind "line_sheet", so the AI asking for a care or
    // metals guide as "terms" or "product_card" used to get nothing. A kind
    // mismatch is dropped only when something of the asked kind exists.
    const kindMatch = !kind || data.kind === kind;
    if (kind && kindMatch) score += 5;

    // Every stored guide also carries kind "line_sheet", so a line-sheet
    // request for "necklace" would tie the real sheet with the necklace
    // fit guide. The item actually named a line sheet wins.
    if (kind === "line_sheet" && /line\s*sheet/i.test(String(data.name || ""))) score += 3;

    if (wantKeywords.length > 0) {
      const itemKeywords = (data.keywords || []).map(k => String(k).toLowerCase());
      const desc = String(data.description || "").toLowerCase();
      const name = String(data.name || "").toLowerCase();
      let kwHits = 0;
      for (const w of wantKeywords) {
        if (itemKeywords.includes(w)) kwHits += 2;
        else if (name.includes(w))    kwHits += 2;
        else if (desc.includes(w))    kwHits += 1;
      }
      score += kwHits;
    }

    scored.push({ score, kindMatch, doc: { id: d.id, ...data } });
  });
  if (kind && scored.some(x => x.kindMatch)) {
    for (let i = scored.length - 1; i >= 0; i--) if (!scored[i].kindMatch) scored.splice(i, 1);
  }

  scored.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    const at = a.doc.lastUsedAt && a.doc.lastUsedAt.toMillis ? a.doc.lastUsedAt.toMillis() : 0;
    const bt = b.doc.lastUsedAt && b.doc.lastUsedAt.toMillis ? b.doc.lastUsedAt.toMillis() : 0;
    return bt - at;
  });

  const matches = scored.slice(0, cap).map(s => trimForCaller(s.doc));

  // Best-effort: stamp lastUsedAt on the matches so the UI shows what's
  // being referenced. Skip on error — don't block search on a write.
  if (matches.length > 0) {
    try {
      const batch = db.batch();
      for (const m of matches) {
        batch.set(db.collection(COLLATERAL_COLL).doc(m.id),
                  { lastUsedAt: FV.serverTimestamp() }, { merge: true });
      }
      await batch.commit();
    } catch (e) {
      console.warn("collateral lastUsedAt update failed:", e.message);
    }
  }

  return { matches, count: matches.length, totalScored: scored.length };
}

// A line sheet or guide pasted into a reply as a raw storage URL shows the
// customer a long link instead of the picture. Find those URLs, match each to
// its uploaded collateral item, and return the text without them plus the
// items to attach as images. Unknown URLs are left in place.
const COLLATERAL_URL_RX = /\(?\bhttps?:\/\/[^\s<>"']*etsymail-collateral\/[^\s<>"'()]+\)?/gi;

function removeUrlFromText(text, raw) {
  let out = text.split(raw).join(" ");
  return out
    .replace(/[ \t]*[:\-\u2013\u2014][ \t]+(?=[,.;!?]|\s*$)/gm, "")
    .replace(/[ \t]*[:\-\u2013\u2014][ \t]*\n/g, "\n")
    .replace(/[ \t]+([,.;!?])/g, "$1")
    .replace(/([,;])(?=[,.;!?])/g, "")
    .replace(/:\s*,/g, ",")
    .replace(/[ \t]{2,}/g, " ")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n");
}

async function pullCollateralUrlsFromText(text) {
  const src = String(text || "");
  const found = src.match(COLLATERAL_URL_RX) || [];
  if (!found.length) return { text: src, hits: [] };
  let pool = [];
  try { pool = (await searchCollateral({ limit: 50 })).matches || []; } catch { pool = []; }
  let out = src;
  const hits = [];
  for (const raw of found) {
    const url = raw.replace(/^\(|\)$/g, "").replace(/[.,;:!?]+$/, "");
    let tail = "";
    try { tail = decodeURIComponent(url.split("/etsymail-collateral/")[1] || "").split(/[?#]/)[0]; } catch { tail = ""; }
    const hit = pool.find(c => c && c.storagePath && c.uploadedContentType &&
      (c.url === url || (tail && String(c.storagePath).endsWith("/" + tail))));
    if (!hit) continue;
    if (!hits.some(h => h.id === hit.id)) hits.push(hit);
    const cut = raw.startsWith("(") && raw.endsWith(")") ? raw : raw.slice(0, raw.indexOf(url) + url.length);
    out = removeUrlFromText(out, cut);
  }
  return { text: hits.length ? out.trim() : src, hits };
}

// ─── v2.5: Direct file upload to Firebase Storage ──────────────────────
//
// Path layout:
//   etsymail-collateral/<random-id>-<sanitized-filename>
//
// The random ID prevents filename collisions and makes the URL non-
// guessable for casual enumeration (it's not a security boundary —
// public ACL is set explicitly below — but it's good hygiene).
//
// Files are made publicly readable via file.makePublic(). That sets
// the underlying GCS object ACL, so the resulting URL
//   https://storage.googleapis.com/<bucket>/<path>
// works without auth — bypassing the Firebase Storage rules layer
// entirely. This is the right call for collateral: line sheets,
// lookbooks, etc. are meant to be linkable from Etsy replies that
// customers open in any browser without any login.
//
// If you want to lock down a specific entry later, use op:"deleteFile"
// to remove the GCS object — the Firestore doc still records its
// metadata for audit but the URL stops resolving.

function sanitizeFilename(name) {
  // Strip path separators, collapse spaces/odd chars to underscore, cap
  // length. Keep the extension. Two reasons we keep the original name in
  // the path: (a) operators recognize what they uploaded when browsing
  // the bucket; (b) the GCS URL preserves a meaningful filename when
  // shared.
  const raw = String(name || "file").trim();
  const lastDot = raw.lastIndexOf(".");
  const stem = lastDot > 0 ? raw.slice(0, lastDot) : raw;
  const ext  = lastDot > 0 ? raw.slice(lastDot) : "";
  const safeStem = stem.replace(/[^a-zA-Z0-9._-]+/g, "_").slice(0, 60) || "file";
  const safeExt  = ext.replace(/[^a-zA-Z0-9.]+/g, "").slice(0, 8);
  return safeStem + safeExt;
}

function randomId(len = 12) {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let s = "";
  for (let i = 0; i < len; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}

/** Upload a file to Firebase Storage. Returns the metadata block that
 *  the caller persists onto the collateral doc. */
async function uploadCollateralFile({ filename, contentType, bytesBase64 }) {
  if (!filename || typeof filename !== "string") {
    return { ok: false, code: 400, error: "filename required" };
  }
  if (!contentType || typeof contentType !== "string") {
    return { ok: false, code: 400, error: "contentType required" };
  }
  if (!UPLOAD_ALLOWED_CONTENT_TYPES.has(contentType)) {
    return {
      ok: false, code: 415,
      error: `contentType '${contentType}' not allowed. Permitted: ${[...UPLOAD_ALLOWED_CONTENT_TYPES].join(", ")}`
    };
  }
  if (!bytesBase64 || typeof bytesBase64 !== "string") {
    return { ok: false, code: 400, error: "bytesBase64 required" };
  }

  // Decode + size-check
  let buffer;
  try {
    buffer = Buffer.from(bytesBase64, "base64");
  } catch (e) {
    return { ok: false, code: 400, error: "bytesBase64 was not valid base64: " + e.message };
  }
  if (buffer.length === 0) {
    return { ok: false, code: 400, error: "decoded file was empty" };
  }
  if (buffer.length > UPLOAD_MAX_BYTES) {
    return {
      ok: false, code: 413,
      error: `file is ${buffer.length} bytes; max ${UPLOAD_MAX_BYTES} bytes (~4.5 MB after base64 decode)`
    };
  }

  // Build the storage path. Random ID prefix + sanitized filename.
  const safe = sanitizeFilename(filename);
  const storagePath = `${UPLOAD_PATH_PREFIX}/${randomId()}-${safe}`;
  const file = bucket.file(storagePath);

  // Write + make public. We set the contentType explicitly so the GCS
  // URL serves with the right MIME (Chrome sniffs but Safari doesn't).
  // cacheControl is generous — collateral rarely changes; if a file is
  // edited, you upload a new one with a new path.
  try {
    await file.save(buffer, {
      contentType,
      resumable: false,    // small files; resumable adds latency
      metadata: {
        cacheControl: "public, max-age=86400",
        contentType
      }
    });
    await file.makePublic();
  } catch (e) {
    return { ok: false, code: 500, error: "Storage write failed: " + e.message };
  }

  const url = `https://storage.googleapis.com/${bucket.name}/${storagePath}`;
  return {
    ok: true,
    url,
    storagePath,
    storageBucket: bucket.name,
    uploadedFilename: safe,
    uploadedContentType: contentType,
    uploadedSizeBytes: buffer.length
  };
}

/** Remove a file from Firebase Storage. Idempotent — if the file is
 *  already gone, returns success. Used when an operator removes a
 *  collateral entry whose file was uploaded through this endpoint. */
async function deleteCollateralFile(storagePath) {
  if (!storagePath || typeof storagePath !== "string") {
    return { ok: false, code: 400, error: "storagePath required" };
  }
  // Defense-in-depth: only delete files under our prefix. Stops a
  // misbehaving caller from passing a path that points into another
  // app's tree (game-generator-1, listing-generator-1, etc.).
  if (!storagePath.startsWith(UPLOAD_PATH_PREFIX + "/")) {
    return {
      ok: false, code: 400,
      error: `storagePath must start with '${UPLOAD_PATH_PREFIX}/'`
    };
  }

  const file = bucket.file(storagePath);
  try {
    await file.delete({ ignoreNotFound: true });
    return { ok: true, deleted: true, storagePath };
  } catch (e) {
    return { ok: false, code: 500, error: "Storage delete failed: " + e.message };
  }
}

// ─── Handler ───────────────────────────────────────────────────────────

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }
  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method Not Allowed" });
  }

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON body"); }

  const op = body.op;
  if (!op) return bad("op required");

  try {
    if (op === "search") {
      const result = await searchCollateral({
        category: body.category,
        kind    : body.kind,
        keywords: body.keywords,
        limit   : body.limit
      });
      return ok({ success: true, ...result });
    }

    if (op === "list") {
      const includeInactive = body.includeInactive === true;
      let q = db.collection(COLLATERAL_COLL);
      if (!includeInactive) q = q.where("active", "==", true);
      const snap = await q.limit(500).get();
      const items = [];
      snap.forEach(d => items.push({ id: d.id, ...d.data() }));
      return ok({ success: true, items, count: items.length });
    }

    if (op === "create") {
      const { actor, item } = body;
      if (!item) return bad("item required");

      const ownerCheck = await requireOwner(actor);
      if (!ownerCheck.ok) {
        await logUnauthorized({
          actor,
          eventType: "collateral_create_unauthorized",
          payload  : { reason: ownerCheck.reason, item }
        });
        return json(403, { error: "Owner role required", reason: ownerCheck.reason });
      }

      const clean = sanitizePatch(item);
      if (!clean.ok) return json(422, { error: "Item rejected: " + clean.reason });

      // Required fields for a new entry
      if (!clean.patch.category) return bad("category required");
      if (!clean.patch.kind)     return bad("kind required");
      if (!clean.patch.name)     return bad("name required");
      if (!clean.patch.url)      return bad("url required");
      if (!("active" in clean.patch)) clean.patch.active = true;

      const doc = {
        ...clean.patch,
        approvedBy   : actor,
        approvedAt   : FV.serverTimestamp(),
        createdBy    : actor,
        createdAt    : FV.serverTimestamp(),
        lastUpdatedBy: actor,
        updatedAt    : FV.serverTimestamp(),
        lastUsedAt   : null
      };
      const ref = await db.collection(COLLATERAL_COLL).add(doc);

      await writeAudit({
        eventType: "collateral_created",
        actor,
        payload  : { id: ref.id, kind: clean.patch.kind, category: clean.patch.category }
      });

      return ok({ success: true, id: ref.id });
    }

    if (op === "update") {
      const { actor, id, patch } = body;
      if (!id || !patch) return bad("id and patch required");

      const ownerCheck = await requireOwner(actor);
      if (!ownerCheck.ok) {
        await logUnauthorized({
          actor,
          eventType: "collateral_update_unauthorized",
          payload  : { id, reason: ownerCheck.reason, attemptedPatch: patch }
        });
        return json(403, { error: "Owner role required", reason: ownerCheck.reason });
      }

      const clean = sanitizePatch(patch);
      if (!clean.ok) return json(422, { error: "Patch rejected: " + clean.reason });

      const cleanPatch = clean.patch;
      cleanPatch.lastUpdatedBy = actor;
      cleanPatch.updatedAt     = FV.serverTimestamp();

      await db.collection(COLLATERAL_COLL).doc(id).set(cleanPatch, { merge: true });

      await writeAudit({
        eventType: "collateral_updated",
        actor,
        payload  : { id, patch: cleanPatch }
      });

      return ok({ success: true, id });
    }

    /* ─── v2.5: Upload a file to Firebase Storage ───────────────
     * Owner-only. Accepts { actor, filename, contentType, bytesBase64 }.
     * Returns { url, storagePath, storageBucket, uploadedFilename,
     * uploadedContentType, uploadedSizeBytes } — the caller (the
     * collateral form in the inbox UI) auto-fills its URL field with
     * `url` and persists the rest as part of the `create` op below.
     *
     * This op DOES NOT create a Firestore doc — it only stages the
     * file. The form then calls `create` (or `update`) with the URL
     * + storagePath alongside the user-entered metadata. Splitting
     * the concerns lets the operator change their mind: if they
     * upload a file then click Cancel, an orphaned object remains in
     * Storage but no Firestore doc was created. A follow-up GC pass
     * could sweep `etsymail-collateral/*` objects with no matching
     * Firestore doc, but that's a future cleanup; orphans are
     * harmless and tiny. */
    if (op === "upload") {
      const { actor, filename, contentType, bytesBase64 } = body;

      const ownerCheck = await requireOwner(actor);
      if (!ownerCheck.ok) {
        await logUnauthorized({
          actor,
          eventType: "collateral_upload_unauthorized",
          payload  : { reason: ownerCheck.reason, filename, contentType }
        });
        return json(403, { error: "Owner role required", reason: ownerCheck.reason });
      }

      const result = await uploadCollateralFile({ filename, contentType, bytesBase64 });
      if (!result.ok) {
        return json(result.code || 500, { error: result.error });
      }

      await writeAudit({
        eventType: "collateral_file_uploaded",
        actor,
        payload  : {
          storagePath: result.storagePath,
          storageBucket: result.storageBucket,
          filename: result.uploadedFilename,
          contentType: result.uploadedContentType,
          sizeBytes: result.uploadedSizeBytes
        }
      });

      return ok({
        success            : true,
        url                : result.url,
        storagePath        : result.storagePath,
        storageBucket      : result.storageBucket,
        uploadedFilename   : result.uploadedFilename,
        uploadedContentType: result.uploadedContentType,
        uploadedSizeBytes  : result.uploadedSizeBytes
      });
    }

    /* ─── v2.5: Delete a previously-uploaded file from Storage ──
     * Owner-only. Accepts { actor, storagePath }. Idempotent — if
     * the object is already gone, returns success. Path is required
     * to start with the collateral prefix so a malformed caller
     * can't reach files owned by other apps in the same bucket.
     *
     * Typical caller: the inbox UI's collateral row "delete" button
     * (a future enhancement) OR an audit cleanup task that wants
     * to free Storage when a collateral entry is permanently
     * removed. The Firestore doc itself is NOT deleted by this op
     * — use the `update` op with `active: false` for soft-delete,
     * or delete the doc through firestoreProxy. */
    if (op === "deleteFile") {
      const { actor, storagePath } = body;

      const ownerCheck = await requireOwner(actor);
      if (!ownerCheck.ok) {
        await logUnauthorized({
          actor,
          eventType: "collateral_deletefile_unauthorized",
          payload  : { reason: ownerCheck.reason, storagePath }
        });
        return json(403, { error: "Owner role required", reason: ownerCheck.reason });
      }

      const result = await deleteCollateralFile(storagePath);
      if (!result.ok) {
        return json(result.code || 500, { error: result.error });
      }

      await writeAudit({
        eventType: "collateral_file_deleted",
        actor,
        payload  : { storagePath: result.storagePath }
      });

      return ok({ success: true, storagePath: result.storagePath });
    }

    return bad(`Unknown op '${op}'`);

  } catch (err) {
    console.error("collateral error:", err);
    return json(500, { error: err.message || String(err), op });
  }
};

// Exposed for direct import by etsyMailSalesAgent (Step 2 + 3 use this).
// Every attached image should be named in the reply (owner's rule); replays
// showed guides attached with no word about them. For an English reply, add
// one short sentence per unnamed guide before the sign-off. Other languages
// are left as written (the model is told to name them itself).
const GUIDE_MENTIONS = {
  metal_comparison : { rx: /\b(compar(?:ison|es|ing)|metals?\s+(card|guide|chart)|gold\s+(card|guide|chart))\b/i,
                       name: "our gold comparison card (gold filled, gold plated and solid gold side by side)" },
  care_instructions: { rx: /\bcare\s+(guide|card|instructions|sheet)\b/i,
                       name: "our care guide" },
  fit_reference    : { rx: /\b(fit|length)\s+(guide|reference|chart|card)\b|\bhow\s+(each|the)\s+lengths?\s+sits?\b/i,
                       name: "our necklace length guide, showing how each length sits" },
  bracelet_sizing  : { rx: /\b(siz(e|ing)\s+(guide|chart|card)|wrist\s+(chart|guide))\b/i,
                       name: "our bracelet sizing chart" }
};
function looksEnglish(text) {
  const words = new Set((String(text).toLowerCase().match(/\b[a-z']+\b/g) || []));
  return ["the", "and", "you", "your", "our", "with", "for", "this", "that", "have"].filter(w => words.has(w)).length >= 4;
}
function nameAttachedGuides(text, kinds) {
  const t = String(text || "");
  if (!t.trim() || !Array.isArray(kinds) || !kinds.length || !looksEnglish(t)) return t;
  const unique = [...new Set(kinds)];
  // One guide and the reply already says something is attached: it is named.
  if (unique.length === 1 && /\battach(ed|ing)?\b/i.test(t)) return t;
  const names = unique.map(k => GUIDE_MENTIONS[k]).filter(g => g && !g.rx.test(t)).map(g => g.name);
  if (!names.length) return t;
  const add = ["We've attached " + (names.length > 1 ? names.slice(0, -1).join(", ") + " and " + names[names.length - 1] : names[0]) + "."];
  const m = t.match(/\n\s*\n(?=\s*(?:many\s+thanks|kind\s+regards|best\s+wishes|thanks|thank\s+you|warmly|cheers)[^\n]{0,20}\n[^\n]*\s*$)/i);
  if (m) return t.slice(0, m.index) + " " + add.join(" ") + t.slice(m.index);
  return t.trimEnd() + " " + add.join(" ");
}

// ─── A reply that says something is attached must carry it ─────────────
// Owner, 2026-09-28: a draft said "I've attached the necklace line sheet"
// with no file on it. A reply that promises a file must carry exactly that
// file (the right kind and the right product), never a stand-in.
//
// attachmentClaims reads a reply and lists what it says is attached to this
// message: a line sheet (and for which product), one of the guides, the
// tracking image, a photo, or an unnamed file ("see the attached").
// missingAttachmentClaims keeps the ones the attachments don't cover.
// attachClaimedCollateral finds the promised sheet or guide and returns it
// to add. A link pasted in the text is not a file, nor is a file sent in an
// earlier message.
const CLAIM_VERB_RX = new RegExp([
  "\\b(?:i|we)(?:'ve|’ve|\\s+have)\\s+(?:also\\s+|just\\s+|now\\s+|again\\s+|gone\\s+ahead\\s+and\\s+)?(?:attached|included|enclosed|added|pulled(?:\\s+up)?)\\b",
  "\\b(?:i|we)(?:'m|’m|\\s+am|'re|’re|\\s+are)\\s+(?:also\\s+|just\\s+)?(?:attaching|including|enclosing|sending|sharing|adding)\\b",
  "\\b(?:i|we)\\s+(?:attach|enclose|include)\\b",
  "\\battached\\s+(?:is|are|you'?ll|you\\s+will|here|below)\\b",
  "\\b(?:see|find|check(?:\\s+out)?|look\\s+at|view|open|in|on)\\s+(?:the\\s+|our\\s+)?attached\\b",
  "\\b(?:is|are)\\s+attached\\b",
  "\\battached\\s+(?:here|below|to\\s+this\\s+(?:message|reply))\\b",
  "\\bhere(?:'s|’s|\\s+is|\\s+are)\\s+(?:the\\s+|our\\s+|a\\s+)",
  "\\b(?:sheet|guide|chart|card|photos?|pictures?|images?|screenshots?|files?|pdf|tracking|details|info(?:rmation)?|timeline|snapshot|history)\\s+(?:is\\s+|are\\s+)?(?:attached|below)\\b",
  "\\bbelow\\s+(?:you'?ll|you\\s+can|please)\\s+(?:find|see)\\b"
].join("|"), "i");
const CLAIM_PAST_RX = /\b(?:earlier|previous(?:ly)?|last\s+(?:message|time|week|reply|email)|already\s+sent|sent\s+(?:you\s+)?before|above)\b/i;
const CLAIM_GUIDES = [
  { kind: "care_instructions", rx: /\b(?:care|aftercare)\s+(?:guide|card|instructions?|sheet)\b/gi },
  { kind: "metal_comparison",  rx: /\b(?:metals?|gold)\s+(?:comparison|card|guide|chart)\b|\bcomparison\s+(?:card|chart|guide|sheet)\b/gi },
  { kind: "fit_reference",     rx: /\b(?:fit|length)\s+(?:guide|reference|chart|card)\b/gi },
  { kind: "size_chart",        rx: /\b(?:siz(?:e|ing))\s+(?:guide|chart|card)\b|\bwrist\s+(?:chart|guide)\b/gi }
];
const CLAIM_SHEET_RX    = /\bsheets?\b/i;
const CLAIM_TRACKING_RX = /\btracking\b|\bscan\s+(?:history|activity)\b/i;
const CLAIM_PHOTO_RX    = /\b(?:photos?|pictures?|pics|images?|screenshots?|mock-?ups?|proofs?|drawings?|sketch(?:es)?|renders?|previews?)\b/i;
const CLAIM_FILE_RX     = /\b(?:files?|pdfs?|documents?)\b/i;
// Other languages: which kind of file, when the words say so.
const FOREIGN_ATTACH_RX = /\b(?:adjunt[oaé]\w*|te\s+adjunto|anex[oa]s?|anexei|anexad[oa]s?|ci-joint\w*|pi[eè]ces?\s+jointes?|je\s+joins|anbei|angeh[äa]ngt|im\s+anhang|allegat[oaie]|in\s+allegato|bijlage|bijgevoegd|w\s+za[łl][ąa]czniku|za[łl][ąa]czam)\b|(?:додаю|додав(?:ла)?|додан[оі]|прикріпи?л\S*|прикрепи?л\S*|у\s+вкладенні|во\s+вложении|прилагаю|вкладення)/i;
const FOREIGN_SHEET_RX    = /\b(?:lijnblad|hoja\s+de\s+(?:opciones|l[ií]nea|tallas|medidas)|cat[aá]logo|feuille|fiche|blatt|[üu]bersicht|foglio|scheda|folha|arkusz)\b|(?:аркуш|лист\s+(?:опцій|варіантів|розмірів)|таблиц)/i;
const FOREIGN_TRACKING_RX = /\b(?:seguimiento|rastreo|suivi|sendungsverfolgung|tracciamento|rastreamento|track\s*&\s*trace)\b|(?:відстеженн|отслеживани|трек)/i;
const FOREIGN_PHOTO_RX    = /\b(?:fotos?|foto's|bild(?:er)?|immagin[ei]|imagen(?:es)?|zdj[eę]ci[ea])\b|(?:фото|зображенн|картинк)/i;
const FAMILY_WORDS = [
  { family: "necklace", rx: /\b(?:necklaces?|pendants?|ketting|collar|collier|halskette|collana|colar|naszyjnik)\b|(?:намист|кулон|ожерель)/i },
  { family: "huggie",   rx: /\b(?:huggies?|hoops?)\b/i },
  { family: "stud",     rx: /\bstuds?\b|\bstud\s+earrings?\b/i }
];
const COLLATERAL_CLAIM_KINDS = new Set(["line_sheet", "care_instructions", "metal_comparison", "fit_reference", "bracelet_sizing"]);
const KIND_NAME_WORDS = {
  care_instructions: ["care", "aftercare", "cleaning", "polish"],
  metal_comparison : ["metal comparison", "metals comparison", "gold filled vs", "filled vs", "plated vs", "comparison"],
  fit_reference    : ["fit reference", "on body", "necklace fit", "chain length"],
  bracelet_sizing  : ["bracelet sizing", "bracelet size", "wrist"]
};
const CLAIM_NAMES = { line_sheet: "line sheet", care_instructions: "care guide", metal_comparison: "gold comparison card",
                      fit_reference: "length guide", bracelet_sizing: "bracelet sizing chart", tracking: "tracking image",
                      photo: "photo", file: "file" };

function familiesIn(text) {
  return FAMILY_WORDS.filter(f => f.rx.test(String(text || ""))).map(f => f.family);
}

function attachmentClaims(text) {
  const t = String(text || "");
  if (!t.trim()) return [];
  const out = [];
  const add = (c) => { if (!out.some(o => o.kind === c.kind && o.family === c.family)) out.push(c); };
  const replyFamilies = familiesIn(t);
  const oneFamily = (s) => { const f = familiesIn(s); return f.length === 1 ? f[0] : (!f.length && replyFamilies.length === 1 ? replyFamilies[0] : null); };
  // Another language: its own words for "attached". A short English reply
  // can look foreign by word count, so the English reading runs as well.
  if (!looksEnglish(t)) {
    for (const s of t.split(/(?<=[.!?])\s+|\n+/)) {
      if (!FOREIGN_ATTACH_RX.test(s)) continue;
      if (FOREIGN_SHEET_RX.test(s)) add({ kind: "line_sheet", family: oneFamily(s) });
      else if (FOREIGN_TRACKING_RX.test(s)) add({ kind: "tracking", family: null });
      else if (FOREIGN_PHOTO_RX.test(s)) add({ kind: "photo", family: null });
      else add({ kind: "file", family: null });
    }
  }
  const bracelet = /\b(?:bracelets?|wrists?|anklets?)\b/i.test(t);
  // "I've attached the tracking here" with the carrier's tracking link in
  // the reply: the link is what was promised.
  const trackingLink = /tools\.usps\.com|TrackConfirmAction|ups\.com\/track|fedex\.com\/\S*track|dhl\.\S*track|parcelsapp|17track|aftership/i.test(t);
  for (const s of t.split(/(?<=[.!?])\s+|\n+/)) {
    if (!CLAIM_VERB_RX.test(s) || CLAIM_PAST_RX.test(s)) continue;
    let rest = s;
    for (const g of CLAIM_GUIDES) {
      g.rx.lastIndex = 0;
      if (!g.rx.test(rest)) { g.rx.lastIndex = 0; continue; }
      g.rx.lastIndex = 0;
      rest = rest.replace(g.rx, " ");
      // A size chart is the bracelet card; for a necklace it is the sheet.
      if (g.kind === "size_chart") add(bracelet ? { kind: "bracelet_sizing", family: null } : { kind: "line_sheet", family: oneFamily(s) });
      else add({ kind: g.kind, family: null });
    }
    if (CLAIM_SHEET_RX.test(rest)) {
      add({ kind: "line_sheet", family: oneFamily(s) });
      rest = rest.replace(/\bsheets?\b/gi, " ");
    }
    // A link or web address is text, not a file; so is "here is the
    // tracking info" followed by the scans in the text.
    const link = /\blinks?\b|https?:\/\/|\bwww\.|\.com\//i.test(s);
    const attachWord = /\battach|\bbelow\b|\benclos|\bpulled\b/i.test(s);
    if (link) continue;
    if (attachWord && CLAIM_TRACKING_RX.test(rest)) { if (!trackingLink) add({ kind: "tracking", family: null }); }
    else if (CLAIM_PHOTO_RX.test(rest)) add({ kind: "photo", family: null });
    else if (CLAIM_FILE_RX.test(rest) || /\bthe\s+attached\s*[.!]?\s*$/i.test(s.trim())) add({ kind: "file", family: null });
  }
  return out;
}

// What an attachment on a draft or a send is, from its name and kind.
function attachmentKindOf(a) {
  if (!a || typeof a !== "object") return null;
  if (a.type === "tracking_image") return "tracking";
  if (a.type !== "image") return a.type || null;
  const name = String(a.collateralName || a.name || a.filename || "").replace(/[_-]+/g, " ").toLowerCase();
  for (const k of Object.keys(KIND_NAME_WORDS)) {
    if (KIND_NAME_WORDS[k].some(w => name.includes(w))) return k;
  }
  if (/line\s*sheet/.test(name)) return "line_sheet";
  const ck = a.collateralKind || a.category || a.kind;
  if (ck && COLLATERAL_CLAIM_KINDS.has(ck)) return ck;
  if (/^tracking[\s-]/.test(name)) return "tracking";
  return "photo";
}

function familyOf(c) {
  const hay = [c && (c.collateralName || c.name), c && c.filename, c && c.category, c && c.description,
               ...((c && Array.isArray(c.keywords)) ? c.keywords : [])].map(v => String(v || "").replace(/[_-]+/g, " ")).join(" | ");
  return familiesIn(hay);
}

// Exactly the promised kind: a photo is a picture of our own (not a sheet,
// guide or tracking image); only "see the attached" with no kind named is
// covered by any file.
function claimCovered(claim, attachments) {
  const all = (Array.isArray(attachments) ? attachments : []).filter(Boolean);
  if (claim.kind === "file") return all.some(a => a.type !== "listing") || all.length > 0;
  const same = all.filter(a => attachmentKindOf(a) === claim.kind);
  if (!same.length) return false;
  if (claim.kind !== "line_sheet" || !claim.family) return true;
  // A family sheet is covered by that family's sheet, or by a sheet that
  // names no family (a staff upload called "line sheet").
  return same.some(a => { const f = familyOf(a); return !f.length || f.includes(claim.family); });
}

function missingAttachmentClaims(text, attachments) {
  return attachmentClaims(text).filter(c => !claimCovered(c, attachments));
}

function collateralAttachmentRecord(hit, kind) {
  const ct = hit.uploadedContentType || hit.contentType || "image/png";
  return {
    attachmentId  : "att_collateral_" + (hit.id || Math.random().toString(36).slice(2, 10)),
    type          : "image",
    storagePath   : hit.storagePath,
    proxyUrl      : "/.netlify/functions/etsyMailImage?path=" + encodeURIComponent(hit.storagePath),
    contentType   : ct,
    bytes         : typeof hit.uploadedSizeBytes === "number" ? hit.uploadedSizeBytes : null,
    filename      : hit.uploadedFilename || ((hit.name || kind) + "." + (ct.split("/")[1] || "png")),
    source        : "collateral",
    collateralId  : hit.id || null,
    collateralName: hit.name || null,
    collateralKind: kind,
    queuedForSend : true,
    addedAt       : new Date().toISOString(),
    addedForClaim : true
  };
}

// The uploaded, active sheets and guides (a handful of documents). Cached
// for a minute per function instance; no lastUsedAt writes.
let _poolCache = null;
async function activeCollateralPool() {
  if (_poolCache && Date.now() - _poolCache.at < 60000) return _poolCache.items;
  const snap = await db.collection(COLLATERAL_COLL).where("active", "==", true).limit(100).get();
  const items = [];
  snap.forEach(d => {
    const c = trimForCaller({ id: d.id, ...(d.data() || {}) });
    if (c.storagePath && c.uploadedContentType) items.push(c);
  });
  _poolCache = { at: Date.now(), items };
  return items;
}

const FAMILIES = ["necklace", "huggie", "stud"];
function asFamily(v) {
  const f = familiesIn(String(v || ""));
  if (f.length === 1) return f[0];
  const w = String(v || "").toLowerCase().trim();
  return FAMILIES.includes(w) ? w : (w === "hoop" ? "huggie" : null);
}

// Which product a sheet is for when the reply doesn't say: the product the
// AI was working on, then the sale's saved spec, then what the customer
// wrote (newest first), then a one-word answer from a small model reading
// the reply and the customer's last messages. Null only when all fail.
async function resolveSheetFamily({ replyText, family, threadId, askModel = true } = {}) {
  const fromReply = familiesIn(replyText);
  if (fromReply.length === 1) return { family: fromReply[0], how: "reply" };
  const hinted = asFamily(family);
  if (hinted) return { family: hinted, how: "hint" };
  if (!threadId) return { family: null, how: null };
  let inbound = [];
  try {
    const sc = await db.collection("EtsyMail_SalesContext").doc(String(threadId)).get();
    const spec = sc.exists ? ((sc.data() || {}).accumulatedSpec || {}) : {};
    const f = asFamily(spec.family);
    if (f) return { family: f, how: "sales_spec" };
  } catch {}
  try {
    const ms = await db.collection("EtsyMail_Threads").doc(String(threadId)).collection("messages")
      .orderBy("timestamp", "desc").limit(12).get();
    ms.forEach(d => { const m = d.data() || {}; if (m.direction === "inbound" && m.text) inbound.push(String(m.text)); });
  } catch {}
  for (const m of inbound) {
    const f = familiesIn(m);
    if (f.length === 1) return { family: f[0], how: "customer" };
  }
  if (!askModel || !process.env.ANTHROPIC_API_KEY) return { family: null, how: null };
  try {
    const { callClaudeRaw } = require("./_etsyMailAnthropic");
    const res = await callClaudeRaw({
      model: "claude-haiku-4-5-20251001", maxTokens: 10, useThinking: false,
      system: "A jewellery shop's reply promises its line sheet. The shop has three line sheets: necklace (charms on chains), huggie (charm hoop earrings) and stud (stud earrings). Answer with one word: necklace, huggie or stud.",
      messages: [{ role: "user", content: [{ type: "text", text:
        "Customer's recent messages (newest first):\n" + inbound.slice(0, 4).map(m => "- " + m.slice(0, 600)).join("\n") +
        "\n\nShop's reply:\n" + String(replyText || "").slice(0, 1500) }] }]
    });
    const word = (res.content || []).filter(b => b.type === "text").map(b => b.text).join(" ").toLowerCase();
    const f = FAMILIES.find(x => word.includes(x));
    if (f) return { family: f, how: "model" };
  } catch (e) {
    console.warn("resolveSheetFamily: model pick failed:", e.message);
  }
  return { family: null, how: null };
}

// Finds the promised sheet or guide for each claim the attachments miss.
// Returns { add: [attachment records], missing: [claims still not covered] }.
//   family   : the product the AI was working on, when the reply doesn't say
//   threadId : lets the product come from the conversation when needed
//   prefer   : the files the AI chose for this draft, used first
// Tracking images and photos are not found here; they stay missing.
async function attachClaimedCollateral(text, attachments, { pool, family = null, prefer = null, threadId = null, askModel = true } = {}) {
  const missing = missingAttachmentClaims(text, attachments);
  if (!missing.length) return { add: [], missing: [] };
  const add = [];
  const still = [];
  let items = pool;
  for (const c of missing) {
    if (!COLLATERAL_CLAIM_KINDS.has(c.kind)) { still.push(c); continue; }
    const claim = { ...c };
    if (claim.kind === "line_sheet" && !claim.family) {
      // The AI's own sheet for this draft names the product.
      const own = (Array.isArray(prefer) ? prefer : []).find(a => a && a.type === "image" && attachmentKindOf(a) === "line_sheet" && familyOf(a).length === 1);
      const r = own ? { family: familyOf(own)[0] } : await resolveSheetFamily({ replyText: text, family, threadId, askModel });
      claim.family = r.family;
    }
    const covered = (attachments || []).concat(add);
    const own = (Array.isArray(prefer) ? prefer : []).find(a => a && a.type === "image" && a.storagePath && a.proxyUrl &&
      claimCovered(claim, [a]) && (claim.kind !== "line_sheet" || familyOf(a).includes(claim.family)) &&
      !covered.some(x => x && x.attachmentId === a.attachmentId));
    if (own) { add.push(own); continue; }
    if (!Array.isArray(items)) { try { items = await activeCollateralPool(); } catch { items = []; } }
    const same = items.filter(x => x && x.storagePath && x.uploadedContentType && attachmentKindOf({ type: "image", ...x }) === claim.kind);
    const hit = claim.kind === "line_sheet"
      ? (claim.family ? same.find(x => familyOf(x).includes(claim.family)) : (same.length === 1 ? same[0] : null))
      : same[0];
    if (hit && !covered.some(x => x && x.collateralId === hit.id)) add.push(collateralAttachmentRecord(hit, claim.kind));
    else if (!hit) still.push(claim);
  }
  return { add, missing: still };
}

// A sheet or guide on a draft points at the file that was current when the
// draft was written. If the owner has since replaced or retired it, point
// the attachment at the current file of the same kind and product.
async function refreshCollateralAttachments(attachments, { pool } = {}) {
  const list = Array.isArray(attachments) ? attachments : [];
  if (!list.some(a => a && a.type === "image" && /^att_collateral_/.test(String(a.attachmentId || "")))) return { attachments: list, changed: 0 };
  let items = pool;
  if (!Array.isArray(items)) { try { items = await activeCollateralPool(); } catch { return { attachments: list, changed: 0 }; } }
  let changed = 0;
  const out = list.map(a => {
    if (!a || a.type !== "image" || !/^att_collateral_/.test(String(a.attachmentId || ""))) return a;
    const id = String(a.attachmentId).slice("att_collateral_".length);
    let cur = items.find(x => x.id === id);
    if (!cur) {
      const kind = attachmentKindOf(a);
      const fam = familyOf(a);
      cur = COLLATERAL_CLAIM_KINDS.has(kind)
        ? items.find(x => attachmentKindOf({ type: "image", ...x }) === kind && (kind !== "line_sheet" || !fam.length || familyOf(x).some(f => fam.includes(f))))
        : null;
    }
    if (!cur || cur.storagePath === a.storagePath) return a;
    changed++;
    const rec = collateralAttachmentRecord(cur, attachmentKindOf({ type: "image", ...cur }));
    return { ...a, ...rec, attachmentId: a.attachmentId === "att_collateral_" + cur.id ? a.attachmentId : rec.attachmentId };
  });
  return { attachments: out, changed };
}

function describeClaims(claims) {
  return (claims || []).map(c => (c.family && c.kind === "line_sheet" ? c.family + " " : "") + (CLAIM_NAMES[c.kind] || c.kind)).join(", ");
}

module.exports.attachmentClaims = attachmentClaims;
module.exports.missingAttachmentClaims = missingAttachmentClaims;
module.exports.attachClaimedCollateral = attachClaimedCollateral;
module.exports.attachmentKindOf = attachmentKindOf;
module.exports.refreshCollateralAttachments = refreshCollateralAttachments;
module.exports.resolveSheetFamily = resolveSheetFamily;
module.exports.describeClaims = describeClaims;
module.exports.nameAttachedGuides = nameAttachedGuides;
module.exports.searchCollateral = searchCollateral;
module.exports.pullCollateralUrlsFromText = pullCollateralUrlsFromText;
