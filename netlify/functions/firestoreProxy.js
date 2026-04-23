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
const db    = admin.firestore();
const FV    = admin.firestore.FieldValue;

const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
};

// Only these collections + their subcollections can be read/written through this proxy.
// Extend as new mail-system collections come online.
const CALLABLE_COLLS = new Set([
  "EtsyMail_Threads",
  "EtsyMail_Customers",
  "EtsyMail_Orders",
  "EtsyMail_Receipts",   // M3: cached Etsy shop receipts
  "EtsyMail_Drafts",
  "EtsyMail_Audit",
  "EtsyMail_Jobs",
  "EtsyMail_Config"
]);
const CALLABLE_SUBS = new Set([
  "messages"
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
        const { coll, id, data, merge = false } = body;
        if (!coll || !id || !data) return bad("Missing coll, id, or data");
        assertColl(coll);
        await db.collection(coll).doc(String(id)).set(hydrate(data), { merge: !!merge });
        return ok({ id: String(id) });
      }

      /* ─── add ─────────────────────────────── */
      if (op === "add") {
        const { coll, data } = body;
        if (!coll || !data) return bad("Missing coll or data");
        assertColl(coll);
        const ref = await db.collection(coll).add(hydrate(data));
        return ok({ id: ref.id });
      }

      /* ─── addSub ──────────────────────────── */
      if (op === "addSub") {
        const { coll, id, sub, data } = body;
        if (!coll || !id || !sub || !data) return bad("Missing coll, id, sub, or data");
        assertColl(coll);
        assertSub(sub);
        const ref = await db.collection(coll).doc(String(id)).collection(sub).add(hydrate(data));
        return ok({ id: ref.id });
      }

      /* ─── update ──────────────────────────── */
      if (op === "update") {
        const { coll, id, data } = body;
        if (!coll || !id || !data) return bad("Missing coll, id, or data");
        assertColl(coll);
        await db.collection(coll).doc(String(id)).update(hydrate(data));
        return ok({ id: String(id) });
      }

      /* ─── deleteSub ─────────────────────────
       * Nuke an ENTIRE subcollection (all docs under
       * coll/id/sub). Used for "clean rescrape" to wipe stale messages
       * from earlier scraper versions. Paginates in chunks of 400 to
       * stay under Firestore's batch limits. */
      if (op === "deleteSub") {
        const { coll, id, sub, confirm } = body;
        if (!coll || !id || !sub) return bad("Missing coll, id, or sub");
        if (confirm !== true) return bad("Refusing to deleteSub without { confirm: true } flag");
        assertColl(coll);
        assertSub(sub);

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
        const { coll, id, confirm } = body;
        if (!coll || !id) return bad("Missing coll or id");
        if (confirm !== true) return bad("Refusing to deleteDoc without { confirm: true } flag");
        assertColl(coll);
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
