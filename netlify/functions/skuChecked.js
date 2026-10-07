/*  netlify/functions/skuChecked.js
 *
 *  The SKU console's "already checked" marks.
 *
 *  The console originally wrote these from the browser with the Firebase
 *  client SDK, which the project's Firestore rules refuse ("Missing or
 *  insufficient permissions") — the rules live in the Firebase console, not in
 *  this repo, so the console cannot be made to pass them from here. Writing
 *  through a function instead uses the service account that every other
 *  server-side path here already uses, which is also the shape the rest of
 *  this repo takes for privileged Firestore work.
 *
 *    GET                                  -> { ids: ["1234", ...] }
 *    POST { listing_id, checked }         -> { ok: true }
 *
 *  One document per listing in `sku_checked`, keyed by listing id. The surface
 *  is deliberately this narrow: no arbitrary collection, no arbitrary field,
 *  no document id that is not a listing number.
 */

const admin = require("./firebaseAdmin");

const db = admin.firestore();
const COLLECTION = "sku_checked";
const META_COLLECTION = "sku_checked_meta", REV_DOC = "rev";       // one document, { n: counter }, bumped by every tick written here
const COPY_MS = 5 * 60 * 1000;
let copy = null;                                                   // { rev, until, body } of the last full read, per warm instance

// The consoles that may call this. Anything else gets no CORS grant, so a page
// on another origin cannot drive these writes from a visitor's browser.
const ALLOWED_ORIGINS = new Set([
  "https://sku.goldenspike.app",
  "https://goldenspike.app"
]);

function corsHeaders(origin){
  const allow = ALLOWED_ORIGINS.has(origin) ? origin : "https://sku.goldenspike.app";
  return {
    "Access-Control-Allow-Origin": allow,
    "Access-Control-Allow-Headers": "Content-Type,Access-Token",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Content-Type": "application/json"
  };
}

exports.handler = async (event) => {
  const origin  = (event.headers && (event.headers.origin || event.headers.Origin)) || "";
  const headers = corsHeaders(origin);

  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers, body: "" };

  try {
    if (event.httpMethod === "GET"){
      // The whole set in one read. A shop of this size marks thousands of
      // listings at most, and the console asks for it once per load plus on
      // refocus, so paging would cost more than it saved.
      // Cost (Firebase emergency, FC18): this door is open to anyone who finds it, and every call read every ticked listing. The set now
      // changes only when a tick is POSTed, and each POST bumps one tiny revision document (below), so a call reads that one document and
      // gives the copy it already holds when the revision has not moved (one read instead of thousands; a call after any tick, from any
      // instance, reads the whole set again). The copy also expires after 5 minutes in case a tick was written some other way.
      // Only the document ids are read (select() with no field): the answer is ids and the fields were never used.
      let rev = null;                                              // (null: the revision could not be read, so no copy is used or kept)
      try { const revSnap = await db.collection(META_COLLECTION).doc(REV_DOC).get(); rev = revSnap.exists ? String((revSnap.data() || {}).n) : "none"; } catch (_) { rev = null; }
      if (rev !== null && copy && copy.rev === rev && Date.now() < copy.until) return { statusCode: 200, headers, body: copy.body };
      const snap = await db.collection(COLLECTION).where("checked", "==", true).select().get();
      const ids = [];
      snap.forEach(doc => ids.push(String(doc.id)));
      const out = JSON.stringify({ ids });
      if (rev !== null) copy = { rev, until: Date.now() + COPY_MS, body: out };
      return { statusCode: 200, headers, body: out };
    }

    if (event.httpMethod === "POST"){
      let body = {};
      try { body = JSON.parse(event.body || "{}"); }
      catch { return { statusCode: 400, headers, body: JSON.stringify({ error: "Body is not JSON" }) }; }

      const id = String(body.listing_id == null ? "" : body.listing_id).trim();
      if (!/^\d{1,20}$/.test(id)){
        return { statusCode: 400, headers, body: JSON.stringify({ error: "listing_id must be a listing number" }) };
      }
      if (typeof body.checked !== "boolean"){
        return { statusCode: 400, headers, body: JSON.stringify({ error: "checked must be true or false" }) };
      }

      await db.collection(COLLECTION).doc(id).set({
        checked: body.checked,
        listing_id: Number(id),
        updated_at: admin.firestore.FieldValue.serverTimestamp()
      }, { merge: true });

      // After the tick, so a reader that sees the new revision also sees the tick.
      copy = null;
      try { await db.collection(META_COLLECTION).doc(REV_DOC).set({ n: admin.firestore.FieldValue.increment(1), updated_at: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }); }
      catch (e) { console.error("skuChecked: revision not bumped (the copies expire in 5 minutes):", e && e.message); }

      return { statusCode: 200, headers, body: JSON.stringify({ ok: true }) };
    }

    return { statusCode: 405, headers, body: JSON.stringify({ error: "Method not allowed" }) };
  } catch (e){
    console.error("skuChecked failed:", e);
    return { statusCode: 500, headers, body: JSON.stringify({ error: String((e && e.message) || e) }) };
  }
};
