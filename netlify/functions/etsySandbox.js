/*  netlify/functions/etsySandbox.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  An emulated Etsy for the Charm Sorter ⇄ Design Station sandbox. It answers
 *  the same four calls the Design Station makes of the real Etsy functions,
 *  in the same response shapes, from a snapshot of the shop's open orders
 *  that was read from Etsy once and stored in this project's own Storage:
 *
 *    ?fn=listOpenOrders&offset=N   → { results: [receipts…] }  one page of 100, like listOpenOrders
 *    ?fn=etsyOrderProxy&orderId=R  → { receipt, transactions }  like etsyOrderProxy
 *    ?fn=etsyImages&listingId=L    → [images…]                  the listing's real pictures
 *    ?fn=refreshEtsyToken          → { access_token, expires_in }
 *    ?fn=status                    → { ok, count, at, path }    what the snapshot holds
 *
 *  The one thing that does reach Etsy is a listing's pictures: /listings/{id}/images is a public read that needs only
 *  the application key, no shop session and no order data, and a rehearsal with grey squares where the charms should be
 *  is not a rehearsal of anything. Nothing else here reaches Etsy, and nothing here writes. The snapshot is the
 *  document Charm_Sandbox/current (count, at, path) and the JSON file it
 *  names under charmnest/sandbox/. A missing snapshot answers an empty shop.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const admin = require("./firebaseAdmin");
const { CORS } = require("./_charmNestAuth");
const db = admin.firestore();
const SANDBOX = "Charm_Sandbox";
const PAGE = 100;

const json = (statusCode, body) => ({ statusCode, headers: CORS, body: JSON.stringify(body) });
let cache = { path: null, at: 0, receipts: [] };
const imgCache = new Map();

/** A listing's pictures, straight from Etsy's public listings endpoint — the application key only, never a shop session.
 *  A failure is an empty list, never an error: a card with no picture is a small loss, a broken rehearsal is not. */
async function listingImages(listingId) {
  if (!/^\d{3,20}$/.test(listingId)) return [];
  if (imgCache.has(listingId)) return imgCache.get(listingId);
  const CLIENT_ID = process.env.CLIENT_ID, CLIENT_SECRET = process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET;
  if (!CLIENT_ID) return [];
  try {
    const fetch = require("node-fetch");
    const r = await fetch(`https://api.etsy.com/v3/application/listings/${listingId}/images`, { headers: { "x-api-key": CLIENT_SECRET ? `${CLIENT_ID}:${CLIENT_SECRET}` : CLIENT_ID } });
    if (!r.ok) { imgCache.set(listingId, []); return []; }
    const d = await r.json();
    const out = Array.isArray(d) ? d : (d && d.results) || [];
    if (imgCache.size > 2000) imgCache.clear();
    imgCache.set(listingId, out);
    return out;
  } catch (_) { return []; }
}

async function loadSnapshot() {
  const doc = await db.collection(SANDBOX).doc("current").get();
  if (!doc.exists) return { meta: null, receipts: [] };
  const meta = doc.data();
  if (cache.path === meta.path && cache.receipts.length) return { meta, receipts: cache.receipts };
  const [buf] = await admin.storage().bucket().file(meta.path).download();
  let receipts = [];
  try { const parsed = JSON.parse(buf.toString("utf8")); receipts = Array.isArray(parsed) ? parsed : (parsed.receipts || []); } catch (e) { throw new Error("sandbox snapshot is not valid JSON: " + e.message); }
  cache = { path: meta.path, at: Date.now(), receipts };
  return { meta, receipts };
}
const stripTx = r => { const o = Object.assign({}, r); delete o.transactions; return o; };

exports.handler = async function (event) {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  const q = event.queryStringParameters || {};
  const fn = String(q.fn || "");
  try {
    if (fn === "refreshEtsyToken") return json(200, { access_token: "sandbox-token", refresh_token: "sandbox-refresh", expires_in: 7200, sandbox: true });
    if (fn === "etsyImages") return json(200, await listingImages(String(q.listingId || "")));
    const { meta, receipts } = await loadSnapshot();
    if (fn === "status") return json(200, { ok: true, sandbox: true, count: receipts.length, at: meta ? meta.at : null, path: meta ? meta.path : null, takenBy: meta ? meta.takenBy || null : null });
    if (fn === "listOpenOrders") {
      const offset = Math.max(0, Number(q.offset) || 0);
      // Etsy lists receipts with their transactions; the page shape matches listOpenOrders (results only)
      return json(200, { results: receipts.slice(offset, offset + PAGE), count: receipts.length, sandbox: true });
    }
    if (fn === "etsyOrderProxy") {
      const id = String(q.orderId || "");
      const r = receipts.find(x => String(x.receipt_id) === id);
      if (!r) return json(404, { error: "receipt not found in the sandbox snapshot", sandbox: true });
      return json(200, { receipt: stripTx(r), transactions: r.transactions || [], sandbox: true });
    }
    return json(400, { error: "unknown fn", fns: ["listOpenOrders", "etsyOrderProxy", "etsyImages", "refreshEtsyToken", "status"] });
  } catch (e) {
    console.error("[etsySandbox]", fn, e);
    return json(500, { error: e.message || String(e), sandbox: true });
  }
};
