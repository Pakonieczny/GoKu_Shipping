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
 *  All reads are offline from Etsy, including listing photographs. Images reuse
 *  the saved listing catalog or durable image cache; a missing photo stays missing.
 *  The order snapshot is Charm_Sandbox/current plus its Storage JSON file.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const admin = require("./firebaseAdmin");
const { CORS } = require("./_charmNestAuth");
const db = admin.firestore();
const SANDBOX = "Charm_Sandbox";
const PAGE = 100;

const json = (statusCode, body) => ({ statusCode, headers: CORS, body: JSON.stringify(body) });
let cache = { path: null, at: 0, receipts: [] };
async function listingImages(listingId) {
  const r=await require('./_etsyImageCache').read(listingId,{cacheOnly:true});
  return r.images;
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
      const open=receipts.filter(r=>r.is_paid!==false && r.was_paid!==false && !r.is_shipped && !r.was_shipped && !r.is_canceled && !r.was_canceled && !/cancel/i.test(r.status || ""));
      return json(200, { results: open.slice(offset, offset + PAGE), count: open.length, sandbox: true });
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
