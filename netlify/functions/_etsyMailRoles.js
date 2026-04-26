/*  netlify/functions/_etsyMailRoles.js
 *
 *  v2.0 Step 2 — Role-check primitive used by every endpoint that mutates
 *  pricing, approves high-value orders, or otherwise needs owner-level
 *  access. NOT a substitute for the X-EtsyMail-Secret auth gate; that gate
 *  ensures the request came from a legitimate client. This helper enforces
 *  what the legitimate client is allowed to do.
 *
 *  ROLE MODEL
 *  ─────────────────────────────────────────────────────────────────────
 *    "owner"    : full access. Can edit EtsyMail_PricingRules, approve
 *                 high-value custom orders (Step 3), edit any sales-mode
 *                 config flag, manage EtsyMail_Collateral (Step 2.5),
 *                 manage other operators.
 *    "operator" : day-to-day operator. Can approve sub-threshold orders,
 *                 advance/rewind sales stages, edit individual quotes
 *                 within rule-defined min/max, send/edit non-sales drafts.
 *                 Can READ pricing rules but not edit. Read-only on
 *                 collateral.
 *
 *  Both roles can read everything; the role gate is purely on mutations.
 *
 *  SOURCE OF TRUTH
 *  ─────────────────────────────────────────────────────────────────────
 *    EtsyMail_Operators/{employeeName} = {
 *      role        : "owner" | "operator",
 *      grantedBy   : "<employeeName>",
 *      grantedAt   : Timestamp,
 *      revokedAt   : Timestamp | null,    // soft-delete; null when active
 *      displayName : "<friendly name>" | null
 *    }
 *
 *  Doc ID is the employeeName the inbox UI uses (whatever value
 *  localStorage's `employee_name` holds for that operator). Keep IDs
 *  consistent across the system — the UI sends `actor: <employeeName>`
 *  on every owner-gated POST.
 *
 *  USAGE PATTERN
 *  ─────────────────────────────────────────────────────────────────────
 *    const { requireOwner, logUnauthorized } = require("./_etsyMailRoles");
 *
 *    const owner = await requireOwner(actor);
 *    if (!owner.ok) {
 *      await logUnauthorized({
 *        actor, eventType: "pricing_rule_edit_unauthorized",
 *        payload: { ruleId, attemptedPatch }
 *      });
 *      return { statusCode: 403, headers: CORS,
 *               body: JSON.stringify({ error: "Owner role required",
 *                                       reason: owner.reason }) };
 *    }
 *
 *  AUDIT SHAPE
 *  ─────────────────────────────────────────────────────────────────────
 *  Audit writes match the canonical v2.0 shape (matches v1.10 timestamps,
 *  adds Step 2's outcome + ruleViolations as top-level fields):
 *    { threadId, draftId, eventType, actor, payload,
 *      createdAt: serverTimestamp(),
 *      outcome  : "blocked",
 *      ruleViolations: ["INSUFFICIENT_ROLE"] }
 *  v1.10 readers see the familiar top-level fields; Step 2 readers can
 *  filter on outcome/ruleViolations.
 */

const admin = require("./firebaseAdmin");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const OPERATORS_COLL = "EtsyMail_Operators";
const AUDIT_COLL     = "EtsyMail_Audit";

// In-memory cache for role lookups.
// Same pattern as auto-pipeline + listings catalog: 15s warm-container
// cache. Operator role is rarely-changing; 15s staleness is acceptable
// (e.g., a freshly-promoted operator may need to wait ~15s for their
// owner-only buttons to start working).
const ROLE_CACHE_MS = 15 * 1000;
const _roleCache = new Map();   // actor → { role, fetchedAt }

/** Read the role for the given actor, returning null if the operator
 *  doc doesn't exist OR the operator was revoked. */
async function getOperatorRole(actor) {
  if (!actor || typeof actor !== "string") return null;

  const cached = _roleCache.get(actor);
  if (cached && (Date.now() - cached.fetchedAt < ROLE_CACHE_MS)) {
    return cached.role;
  }

  try {
    const doc = await db.collection(OPERATORS_COLL).doc(actor).get();
    if (!doc.exists) {
      _roleCache.set(actor, { role: null, fetchedAt: Date.now() });
      return null;
    }
    const data = doc.data() || {};
    // Soft-delete: revokedAt set → treat as no role.
    if (data.revokedAt) {
      _roleCache.set(actor, { role: null, fetchedAt: Date.now() });
      return null;
    }
    const role = data.role || null;
    _roleCache.set(actor, { role, fetchedAt: Date.now() });
    return role;
  } catch (e) {
    console.warn("getOperatorRole failed:", e.message);
    return null;
  }
}

/** Require that the actor has one of the listed roles. Returns
 *    { ok: true, role }
 *  or
 *    { ok: false, reason: "...", actualRole?: "..." }
 *
 *  Reasons:
 *    ACTOR_REQUIRED       — caller didn't pass actor
 *    INVALID_ROLE_LIST    — bug in caller (empty allowedRoles)
 *    OPERATOR_NOT_FOUND   — actor not in EtsyMail_Operators (or revoked)
 *    INSUFFICIENT_ROLE    — actor exists but role is wrong
 */
async function requireRole(actor, allowedRoles) {
  if (!actor || typeof actor !== "string") {
    return { ok: false, reason: "ACTOR_REQUIRED" };
  }
  if (!Array.isArray(allowedRoles) || allowedRoles.length === 0) {
    return { ok: false, reason: "INVALID_ROLE_LIST" };
  }
  const role = await getOperatorRole(actor);
  if (!role) {
    return { ok: false, reason: "OPERATOR_NOT_FOUND" };
  }
  if (!allowedRoles.includes(role)) {
    return {
      ok: false,
      reason: "INSUFFICIENT_ROLE",
      actualRole: role,
      requiredRoles: allowedRoles
    };
  }
  return { ok: true, role };
}

/** Convenience: require owner role. */
async function requireOwner(actor) {
  return requireRole(actor, ["owner"]);
}

/** Convenience: require either role (i.e., must be a registered
 *  operator at all). Useful for endpoints that need an audit trail of
 *  WHO did the thing without restricting to owner-only. */
async function requireAnyRole(actor) {
  return requireRole(actor, ["owner", "operator"]);
}

/** Write an audit row for an unauthorized attempt. Designed to be
 *  fire-and-forget (callers don't await refusal-audit failures). */
async function logUnauthorized({ actor, eventType, payload, threadId = null, draftId = null }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId,
      draftId,
      eventType,
      actor: actor || "unknown",
      payload: payload || {},
      createdAt: FV.serverTimestamp(),
      outcome: "blocked",
      ruleViolations: ["INSUFFICIENT_ROLE"]
    });
  } catch (e) {
    console.warn("logUnauthorized audit write failed:", e.message);
  }
}

/** Drop the cache entry for one actor. Called after the operator's
 *  role doc is mutated (e.g., admin promotes an operator to owner) so
 *  the new role takes effect immediately within the same Lambda. */
function invalidateRoleCache(actor) {
  if (actor) _roleCache.delete(actor);
}

/** List active operators (for UI display). Both roles see this list. */
async function listActiveOperators() {
  const snap = await db.collection(OPERATORS_COLL)
    .where("revokedAt", "==", null)
    .limit(100)
    .get();
  const out = [];
  snap.forEach(doc => out.push({ id: doc.id, ...doc.data() }));
  return out;
}

module.exports = {
  getOperatorRole,
  requireRole,
  requireOwner,
  requireAnyRole,
  logUnauthorized,
  invalidateRoleCache,
  listActiveOperators,
  // Constant exposed for tests / docs
  OPERATORS_COLL
};
