/*  netlify/functions/firestoreProxy.js
 *
 *  Generic Firestore read/write proxy, so browser pages can use Firestore
 *  without loading the Firebase client SDK from the gstatic CDN.
 *
 *  This is NOT a universal Firestore client — it's a small, explicit surface
 *  designed for the EtsyMail operator inbox. It exposes:
 *
 *    GET  ?op=list&coll=<collection>&where=<field>,==,<value>&orderBy=<field>,desc&limit=N
 *    GET  ?op=get&coll=<collection>&id=<docId>
 *    GET  ?op=listSub&coll=<collection>&id=<docId>&sub=<subcollection>&orderBy=...&limit=N
 *    GET  ?op=counts&coll=<collection>&groupBy=<field>
 *    POST body:{ op:'set',  coll, id, data, merge? }
 *    POST body:{ op:'add',  coll, data }
 *    POST body:{ op:'addSub', coll, id, sub, data }
 *    POST body:{ op:'update', coll, id, data }
 *
 *  Collection allowlist prevents random Firestore access. Extend CALLABLE_COLLS
 *  when new collections come online.
 *
 *  Timestamps: the browser can pass { __serverTimestamp: true } and the proxy
 *  substitutes admin.firestore.FieldValue.serverTimestamp().
 */

const admin = require("./firebaseAdmin");
const { requireExtensionAuth } = require("./_etsyMailAuth");
const { requireOwner, requireAnyRole, logUnauthorized } = require("./_etsyMailRoles");
const db    = admin.firestore();
const FV    = admin.firestore.FieldValue;

const CORS = {
  "Access-Control-Allow-Origin" : "*",
  // v1.5: include X-EtsyMail-Secret in allowed headers since the proxy
  // now requires it on every op. The inbox UI forwards it from
  // localStorage on every api() call.
  "Access-Control-Allow-Headers": "Content-Type,Authorization,X-EtsyMail-Secret",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
};

// Only these collections + their subcollections can be read/written through this proxy.
// Extend as new mail-system collections come online.
const CALLABLE_COLLS = new Set([
  "EtsyMail_Threads",
  "EtsyMail_Customers",
  "EtsyMail_Orders",
  "EtsyMail_Drafts",
  "EtsyMail_Audit",
  "EtsyMail_Jobs",
  "EtsyMail_Config",
  "EtsyMail_TrackingCache",          // M4 tracking-image cache (keyed by tracking code)
  "EtsyMail_TrackingJobs",            // M4 tracking-image async job status (keyed by jobId)
  // ─── v2.0 Step 1 ─────────────────────────────────────────────────────
  "EtsyMail_Listings",                // Etsy listings catalog mirror
  "EtsyMail_ListingsSync",            // catalog sync state (id: "global")
  "EtsyMail_IntentClassifications",   // per-thread intent cache
  // ─── v2.0 Step 2 ────────────────────────────────────────────────────
  "EtsyMail_SalesContext",            // per-thread sales-funnel state
  "EtsyMail_SalesPrompts",            // operator-tunable per-stage prompts
  "EtsyMail_Operators",               // role assignments (owner | operator)
  // ─── v2.0 Step 2.5 ──────────────────────────────────────────────────
  "EtsyMail_Collateral",              // owner-curated reference URLs
  // ─── v2.1 Option-sheet pricing (replaces v2.0 band model) ───────────
  "EtsyMail_OptionSheets",            // line-sheet docs per product family
  // ─── v2.2 Etsy shipping upgrades cache ──────────────────────────────
  "EtsyMail_ShippingUpgradesCache"    // synced from Etsy every 6h
  // ─── v2.0 Step 3 will add ───────────────────────────────────────────
  // "EtsyMail_CustomOrders",
  // "EtsyMail_CustomOrderTemplates"
]);
const CALLABLE_SUBS = new Set([
  "messages"
]);

const OWNER_WRITE_COLLS = new Set([
  "EtsyMail_Operators",
  "EtsyMail_Config",
  "EtsyMail_OptionSheets",
  "EtsyMail_Collateral",
  "EtsyMail_SalesPrompts",
  "EtsyMail_ShippingUpgradesCache"
]);

// Operator-write collections: any registered role (owner or operator) may
// write through the proxy, but anonymous extension-secret-only callers
// cannot. This closes the gap where a leaked secret could freely mutate
// thread state or draft payloads without any identity on the audit trail.
//
// EtsyMail_Threads — operator may mirror a sales-stage advance; all other
//   thread-state writes come through etsyMailThreads (role-checked there).
// EtsyMail_Drafts  — operator may persist composer attachment lists; the
//   full draft lifecycle is managed by etsyMailDraftReply / etsyMailDraftSend.
const OPERATOR_WRITE_COLLS = new Set([
  "EtsyMail_SalesContext",
  "EtsyMail_Threads",
  "EtsyMail_Drafts"
]);

function json(statusCode, body) {
  return { statusCode, headers: CORS, body: JSON.stringify(body) };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }
function ok(body) { return json(200, { success: true, ...body }); }

function assertColl(coll) {
  if (!CALLABLE_COLLS.has(coll)) throw new Error(`Collection '${coll}' not in allowlist`);
}
function assertSub(sub) {
  if (!CALLABLE_SUBS.has(sub)) throw new Error(`Subcollection '${sub}' not in allowlist`);
}

async function requireProxyWriteRole(coll, op, actor, payload = {}) {
  if (OWNER_WRITE_COLLS.has(coll)) {
    const owner = await requireOwner(actor);
    if (!owner.ok) {
      await logUnauthorized({
        actor,
        eventType: "firestore_proxy_write_unauthorized",
        payload: { coll, op, reason: owner.reason, ...payload }
      });
      return { ok: false, statusCode: 403, error: "Owner role required", reason: owner.reason };
    }
  } else if (OPERATOR_WRITE_COLLS.has(coll)) {
    const operator = await requireAnyRole(actor);
    if (!operator.ok) {
      await logUnauthorized({
        actor,
        eventType: "firestore_proxy_write_unauthorized",
        payload: { coll, op, reason: operator.reason, ...payload }
      });
      return { ok: false, statusCode: 403, error: "Registered operator role required", reason: operator.reason };
    }
  }
  return { ok: true };
}

/* Recursively substitute { __serverTimestamp:true } with server timestamps
 * and { __arrayUnion:[...] } / { __arrayRemove:[...] } with array ops.
 * Keeps raw dates and primitives alone. */
function hydrate(value) {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(hydrate);
  if (value.__serverTimestamp === true) return FV.serverTimestamp();
  if (Array.isArray(value.__arrayUnion))  return FV.arrayUnion(...value.__arrayUnion);
  if (Array.isArray(value.__arrayRemove)) return FV.arrayRemove(...value.__arrayRemove);
  if (typeof value.__increment === "number") return FV.increment(value.__increment);
  const out = {};
  for (const k of Object.keys(value)) out[k] = hydrate(value[k]);
  return out;
}

/* Convert Firestore doc data to JSON-safe form, turning Timestamps into ISO strings
 * with a {_ts: true, ms: <millis>} marker so the client can reformat. */
function serialize(value) {
  if (value === null || typeof value !== "object") return value;
  if (value && typeof value.toDate === "function" && typeof value.toMillis === "function") {
    return { _ts: true, ms: value.toMillis() };
  }
  if (Array.isArray(value)) return value.map(serialize);
  const out = {};
  for (const k of Object.keys(value)) out[k] = serialize(value[k]);
  return out;
}

function parseWhere(raw) {
  // Accept repeated ?where=field,op,value  — coerce numbers/booleans
  if (!raw) return [];
  const arr = Array.isArray(raw) ? raw : [raw];
  return arr.map(s => {
    const [field, op, ...rest] = String(s).split(",");
    let value = rest.join(",");
    if (value === "true") value = true;
    else if (value === "false") value = false;
    else if (value === "null") value = null;
    else if (/^-?\d+(\.\d+)?$/.test(value)) value = Number(value);
    return { field, op, value };
  });
}
function parseOrderBy(raw) {
  if (!raw) return [];
  const arr = Array.isArray(raw) ? raw : [raw];
  return arr.map(s => {
    const [field, dir = "asc"] = String(s).split(",");
    return { field, dir: dir === "desc" ? "desc" : "asc" };
  });
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };

  // ── v1.5: gate every op behind the shared secret ────────────────
  // Pre-v1.5, the proxy was unauthenticated. A public Netlify URL with
  // unauthenticated read AND write access to thread/draft/customer/audit
  // collections is a security incident waiting to happen — anyone could
  // wipe EtsyMail_Threads, rewrite searchableText fields with garbage,
  // or read every customer's order history.
  //
  // Now: every GET and POST requires X-EtsyMail-Secret. The inbox UI
  // forwards it from localStorage on every api() call (existing
  // behavior, no UI change needed). Functions calling the proxy
  // server-side forward it from process.env.ETSYMAIL_EXTENSION_SECRET.
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  try {
    const method = event.httpMethod;
    const qs     = event.queryStringParameters || {};
    // Support repeated params (Netlify folds them into multiValueQueryStringParameters)
    const mvqs   = event.multiValueQueryStringParameters || {};

    if (method === "GET") {
      const op = qs.op;
      if (!op) return bad("Missing op");

      /* ─── list ─────────────────────────────── */
      if (op === "list") {
        const coll = qs.coll;
        if (!coll) return bad("Missing coll");
        assertColl(coll);

        let q = db.collection(coll);

        const wheres = parseWhere(mvqs.where || qs.where);
        for (const w of wheres) q = q.where(w.field, w.op, w.value);

        const orders = parseOrderBy(mvqs.orderBy || qs.orderBy);
        for (const o of orders) q = q.orderBy(o.field, o.dir);

        const limit = Math.min(parseInt(qs.limit || "100", 10), 500);
        q = q.limit(limit);

        const snap = await q.get();
        return ok({ docs: snap.docs.map(d => ({ id: d.id, ...serialize(d.data()) })) });
      }

      /* ─── get ─────────────────────────────── */
      if (op === "get") {
        const { coll, id } = qs;
        if (!coll || !id) return bad("Missing coll or id");
        assertColl(coll);
        const snap = await db.collection(coll).doc(String(id)).get();
        if (!snap.exists) return ok({ exists: false, doc: null });
        return ok({ exists: true, doc: { id: snap.id, ...serialize(snap.data()) } });
      }

      /* ─── listSub ──────────────────────────── */
      if (op === "listSub") {
        const { coll, id, sub } = qs;
        if (!coll || !id || !sub) return bad("Missing coll, id, or sub");
        assertColl(coll);
        assertSub(sub);

        let q = db.collection(coll).doc(String(id)).collection(sub);
        const orders = parseOrderBy(mvqs.orderBy || qs.orderBy);
        for (const o of orders) q = q.orderBy(o.field, o.dir);
        const limit = Math.min(parseInt(qs.limit || "500", 10), 2000);
        q = q.limit(limit);

        const snap = await q.get();
        return ok({ docs: snap.docs.map(d => ({ id: d.id, ...serialize(d.data()) })) });
      }

      /* ─── counts ────────────────────────────
       * Minimal group-count: scans up to 2000 docs, groups by one field.
       * Good enough for sidebar badges; swap for count() aggregation later. */
      if (op === "counts") {
        const { coll, groupBy } = qs;
        if (!coll || !groupBy) return bad("Missing coll or groupBy");
        assertColl(coll);
        const snap = await db.collection(coll).select(groupBy).limit(2000).get();
        const counts = {};
        snap.forEach(d => {
          const v = (d.data() || {})[groupBy] || "unknown";
          counts[v] = (counts[v] || 0) + 1;
        });
        return ok({ counts });
      }

      return bad(`Unknown op '${op}'`);
    }

    if (method === "POST") {
      let body = {};
      try { body = JSON.parse(event.body || "{}"); }
      catch { return bad("Invalid JSON body"); }

      const { op } = body;
      if (!op) return bad("Missing op");

      /* ─── set ───────────────────────────────
       * Explicit doc id. With merge:true behaves like patch. */
      if (op === "set") {
        const { coll, id, data, merge = false, actor = null } = body;
        if (!coll || !id || !data) return bad("Missing coll, id, or data");
        assertColl(coll);
        const role = await requireProxyWriteRole(coll, op, actor, { id: String(id) });
        if (!role.ok) return json(role.statusCode, { error: role.error, reason: role.reason });
        await db.collection(coll).doc(String(id)).set(hydrate(data), { merge: !!merge });
        return ok({ id: String(id) });
      }

      /* ─── add ─────────────────────────────── */
      if (op === "add") {
        const { coll, data, actor = null } = body;
        if (!coll || !data) return bad("Missing coll or data");
        assertColl(coll);
        const role = await requireProxyWriteRole(coll, op, actor);
        if (!role.ok) return json(role.statusCode, { error: role.error, reason: role.reason });
        const ref = await db.collection(coll).add(hydrate(data));
        return ok({ id: ref.id });
      }

      /* ─── addSub ──────────────────────────── */
      if (op === "addSub") {
        const { coll, id, sub, data, actor = null } = body;
        if (!coll || !id || !sub || !data) return bad("Missing coll, id, sub, or data");
        assertColl(coll);
        assertSub(sub);
        const role = await requireProxyWriteRole(coll, op, actor, { id: String(id), sub });
        if (!role.ok) return json(role.statusCode, { error: role.error, reason: role.reason });
        const ref = await db.collection(coll).doc(String(id)).collection(sub).add(hydrate(data));
        return ok({ id: ref.id });
      }

      /* ─── update ──────────────────────────── */
      if (op === "update") {
        const { coll, id, data, actor = null } = body;
        if (!coll || !id || !data) return bad("Missing coll, id, or data");
        assertColl(coll);
        const role = await requireProxyWriteRole(coll, op, actor, { id: String(id) });
        if (!role.ok) return json(role.statusCode, { error: role.error, reason: role.reason });
        await db.collection(coll).doc(String(id)).update(hydrate(data));
        return ok({ id: String(id) });
      }

      /* ─── deleteSub ─────────────────────────
       * Nuke an ENTIRE subcollection (all docs under
       * coll/id/sub). Used for "clean rescrape" to wipe stale messages
       * from earlier scraper versions. Paginates in chunks of 400 to
       * stay under Firestore's batch limits. */
      if (op === "deleteSub") {
        const { coll, id, sub, confirm, actor = null } = body;
        if (!coll || !id || !sub) return bad("Missing coll, id, or sub");
        if (confirm !== true) return bad("Refusing to deleteSub without { confirm: true } flag");
        assertColl(coll);
        assertSub(sub);
        const role = await requireProxyWriteRole(coll, op, actor, { id: String(id), sub });
        if (!role.ok) return json(role.statusCode, { error: role.error, reason: role.reason });

        const subRef = db.collection(coll).doc(String(id)).collection(sub);
        let totalDeleted = 0;
        // Loop: grab 400 docs, batch-delete them, repeat until empty
        while (true) {
          const snap = await subRef.limit(400).get();
          if (snap.empty) break;
          const batch = db.batch();
          snap.docs.forEach(d => batch.delete(d.ref));
          await batch.commit();
          totalDeleted += snap.docs.length;
          if (snap.docs.length < 400) break;  // last page
        }

        // Reset the parent doc's messageCount so the thread shows accurate state
        try {
          await db.collection(coll).doc(String(id)).set({
            messageCount: 0,
            lastInboundAt: null,
            lastOutboundAt: null,
            updatedAt: admin.firestore.FieldValue.serverTimestamp()
          }, { merge: true });
        } catch (e) {
          console.warn("deleteSub: parent doc reset failed:", e.message);
        }

        return ok({ deleted: totalDeleted });
      }

      /* ─── deleteDoc ─────────────────────────
       * Delete a single top-level doc. */
      if (op === "deleteDoc") {
        const { coll, id, confirm, actor = null } = body;
        if (!coll || !id) return bad("Missing coll or id");
        if (confirm !== true) return bad("Refusing to deleteDoc without { confirm: true } flag");
        assertColl(coll);
        const role = await requireProxyWriteRole(coll, op, actor, { id: String(id) });
        if (!role.ok) return json(role.statusCode, { error: role.error, reason: role.reason });
        await db.collection(coll).doc(String(id)).delete();
        return ok({ id: String(id), deleted: true });
      }

      return bad(`Unknown op '${op}'`);
    }

    return json(405, { error: "Method Not Allowed" });

  } catch (err) {
    console.error("firestoreProxy error:", err);
    return json(500, { error: err.message || String(err) });
  }
};
