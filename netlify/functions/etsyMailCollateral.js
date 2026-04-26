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
 *  ═══ FOUR CORE OPS ═══════════════════════════════════════════════════════
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
 *                    "image_set" | "terms",
 *      name        : "<short display title>",
 *      url         : "<https://...>",
 *      description : "<one-paragraph blurb shown to the AI>",
 *      keywords    : ["<extra match terms>"],
 *      active      : true,
 *      lastUsedAt  : Timestamp | null,    // updated by search() when AI uses it
 *      approvedBy  : "<employeeName>",
 *      approvedAt  : Timestamp,
 *      storagePath, storageBucket, fileName, contentType, bytes,
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

const COLLATERAL_COLL = "EtsyMail_Collateral";
const AUDIT_COLL      = "EtsyMail_Audit";

const VALID_KINDS = new Set([
  "line_sheet", "product_card", "lookbook", "image_set", "terms"
]);

const SAFE_FIELDS = new Set([
  "category", "kind", "name", "url", "description", "keywords", "active"
]);

const SEARCH_DEFAULT_LIMIT = 5;
const SEARCH_MAX_LIMIT     = 20;

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
  return {
    id          : doc.id,
    category    : doc.category,
    kind        : doc.kind,
    name        : doc.name,
    url         : doc.url,
    description : doc.description || "",
    keywords    : doc.keywords || [],
    active      : doc.active !== false,
    storagePath : doc.storagePath || null,
    fileName    : doc.fileName || null,
    contentType : doc.contentType || null,
    bytes       : doc.bytes || null
  };
}

function sanitizeDocId(raw) {
  const id = String(raw || "").trim()
    .toLowerCase()
    .replace(/[^a-z0-9_\-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120);
  return id || null;
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
  const snap = await q.limit(200).get();

  if (snap.empty) return { matches: [], count: 0, totalScored: 0 };

  const wantKeywords = Array.isArray(keywords)
    ? keywords.map(k => String(k).trim().toLowerCase()).filter(k => k.length >= 2)
    : [];

  const scored = [];
  snap.forEach(d => {
    const data = d.data() || {};
    let score = 1;   // base score for being active + (optionally) category-filtered

    if (kind && data.kind === kind) score += 5;
    else if (kind && data.kind !== kind) return;   // kind requested but mismatch → skip

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

    scored.push({ score, doc: { id: d.id, ...data } });
  });

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

      // Prefer stable doc IDs when supplied by seed files / upload UI
      // (e.g. huggie_line_sheet). This keeps customer-facing collateral
      // entries replaceable instead of creating duplicate random docs on
      // every import. If no ID is supplied, preserve the original add()
      // behavior for ad-hoc collateral entries.
      const requestedId = sanitizeDocId(body.id || item.id || "");
      let ref;
      if (requestedId) {
        ref = db.collection(COLLATERAL_COLL).doc(requestedId);
        const existing = await ref.get();
        if (existing.exists) {
          return json(409, { error: "Collateral ID already exists", id: requestedId });
        }
        await ref.set(doc, { merge: false });
      } else {
        ref = await db.collection(COLLATERAL_COLL).add(doc);
      }

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

    return bad(`Unknown op '${op}'`);

  } catch (err) {
    console.error("collateral error:", err);
    return json(500, { error: err.message || String(err), op });
  }
};

// Exposed for direct import by etsyMailSalesAgent (Step 2 + 3 use this).
module.exports.searchCollateral = searchCollateral;
