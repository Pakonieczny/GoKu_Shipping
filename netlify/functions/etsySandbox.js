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
 *    ?fn=status                    → { ok, count, at, path, stream }  what the snapshot holds
 *
 *  All reads are offline from Etsy, including listing photographs. Images reuse
 *  the saved listing catalog or durable image cache; a missing photo stays missing.
 *  The order snapshot is Charm_Sandbox/current plus its Storage JSON file.
 *  With the order stream on (Charm_Sandbox/stream, moved by the sorter) the open
 *  list is not the snapshot but the orders that have arrived by the stream's clock.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const admin = require("./firebaseAdmin");
const { CORS } = require("./_charmNestAuth");
const db = admin.firestore();
const SANDBOX = "Charm_Sandbox";
const PAGE = 100;

const json = (statusCode, body) => ({ statusCode, headers: CORS, body: JSON.stringify(body) });
let cache = { path: null, at: 0, receipts: [], meta: null };
let snapshotFlight=null;
async function listingImages(listingId) {
  const r=await require('./_etsyImageCache').read(listingId,{cacheOnly:true});
  return r.images;
}

async function loadSnapshot() {
  if(cache.at && Date.now()-cache.at<60000)return {meta:cache.meta,receipts:cache.receipts};
  if(snapshotFlight)return snapshotFlight;
  snapshotFlight=readSnapshot();try{return await snapshotFlight;}finally{snapshotFlight=null;}
}
async function readSnapshot() {
  const doc = await db.collection(SANDBOX).doc("current").get();
  if (!doc.exists) { cache={path:null,at:Date.now(),meta:null,receipts:[]};return {meta:null,receipts:[]}; }
  const meta = doc.data();
  if (cache.path === meta.path && cache.meta) {cache.at=Date.now();cache.meta=meta;return { meta, receipts: cache.receipts };}
  const [buf] = await admin.storage().bucket().file(meta.path).download();
  let receipts = [];
  try { const parsed = JSON.parse(buf.toString("utf8")); receipts = Array.isArray(parsed) ? parsed : (parsed.receipts || []); } catch (e) { throw new Error("sandbox snapshot is not valid JSON: " + e.message); }
  cache = { path: meta.path, at: Date.now(), receipts, meta };
  return { meta, receipts };
}
const stripTx = r => { const o = Object.assign({}, r); delete o.transactions; return o; };

/* ── the order stream: each simulated ten-minute step brings 2 to 5 new orders, each a copy of a random open snapshot
   receipt under fresh numbers, stamped with its simulated arrival time and keeping the original's lead time to its ship
   date. Pure in (seed, step), so the same seed replays the same orders; built once per step and kept. ── */
let streamFlight = null, built = null;
async function loadStream() {   // read on every list sweep, not cached: at 50x the clock moves every 12 s
  if (streamFlight) return streamFlight;
  streamFlight = db.collection(SANDBOX).doc("stream").get().then(d => (d.exists ? d.data() : null));
  try { return await streamFlight; } finally { streamFlight = null; }
}
const hash = s => { let h = 2166136261; for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 16777619); return h >>> 0; };
const rand = a => () => { a = (a + 0x6D2B79F5) | 0; let t = Math.imul(a ^ (a >>> 15), 1 | a); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
const isOpen = r => r.is_paid !== false && r.was_paid !== false && !r.is_shipped && !r.was_shipped && !r.is_canceled && !r.was_canceled && !/cancel/i.test(r.status || "");
function streamPool(receipts, meta) {
  let maxR = 0, maxT = 0;
  for (const r of receipts) { maxR = Math.max(maxR, +r.receipt_id || 0); for (const x of r.transactions || []) maxT = Math.max(maxT, +x.transaction_id || 0); }
  return { pool: receipts.filter(isOpen), maxR, maxT, at: Math.floor(((meta && meta.at) || Date.now()) / 1000) };
}
/** A copy of a snapshot receipt as a new order that arrived at `t` (epoch seconds, as Etsy writes them). */
function arrival(src, rid, tx0, t, snapAt) {
  const r = JSON.parse(JSON.stringify(src)), was = +(src.create_timestamp || src.created_timestamp || src.creation_tsz) || snapAt, lead = v => (+v ? +v + t - was : v);
  const same = (v, n) => (typeof v === "string" ? String(n) : n);
  r.receipt_id = same(src.receipt_id, rid); if (r.order_number != null) r.order_number = same(src.order_number, rid);
  r.create_timestamp = r.created_timestamp = r.update_timestamp = r.updated_timestamp = t;
  for (const f of ["creation_tsz", "last_modified_tsz"]) if (r[f] != null) r[f] = t;
  for (const f of ["expected_ship_date", "dispatch_date", "ship_by_date"]) if (r[f] != null) r[f] = lead(r[f]);
  (r.transactions || []).forEach((x, i) => {
    x.transaction_id = same(x.transaction_id, tx0 + i); x.receipt_id = r.receipt_id;
    for (const f of ["create_timestamp", "created_timestamp", "paid_timestamp", "update_timestamp", "updated_timestamp"]) if (x[f] != null) x[f] = t;
    if (x.expected_ship_date != null) x.expected_ship_date = lead(x.expected_ship_date); if (x.shipped_timestamp != null) x.shipped_timestamp = null;
  });
  return r;
}
/** Step k's orders (k from 1): arrival times inside the step, and receipt and transaction numbers above every one in
    the snapshot, rising with time as Etsy's do, in a band of their own per seed. */
function batch(s, ctx, k) {
  if (!ctx.pool.length) return [];
  const R = rand(hash(`${s.seed}:${k}`)), n = s.min + Math.floor(R() * (s.max - s.min + 1)), from = s.simStart + (k - 1) * s.stepMs, gap = Math.floor(1000 / n);
  const baseR = (Math.floor(ctx.maxR / 1e7) + 1 + hash(`${s.seed}:r`) % 97) * 1e7, baseT = (Math.floor(ctx.maxT / 1e7) + 1 + hash(`${s.seed}:t`) % 97) * 1e7;
  const times = Array.from({ length: n }, () => from + 1000 + Math.floor(R() * (s.stepMs - 1000))).sort((a, b) => a - b);
  return times.map((ms, j) => { const src = ctx.pool[Math.floor(R() * ctx.pool.length)]; return arrival(src, baseR + k * 1000 + j * gap + Math.floor(R() * gap), baseT + k * 1000 + j * gap, Math.floor(ms / 1000), ctx.at); });
}
/** Every order that has arrived by the stream's clock, newest first as Etsy lists open receipts. */
function streamed(s, receipts, meta) {
  const key = [s.seed, s.simStart, s.stepMs, s.min, s.max, meta ? meta.path : ""].join("|");
  if (!built || built.key !== key) built = { key, ctx: streamPool(receipts, meta), steps: [], byId: new Map() };
  for (let k = built.steps.length + 1; k <= s.tick; k++) { const b = batch(s, built.ctx, k); built.steps.push(b); for (const r of b) built.byId.set(String(r.receipt_id), r); }
  return built.steps.slice(0, s.tick).flat().reverse();
}
exports.batch = (s, receipts, k, meta) => batch(s, streamPool(receipts, meta), k);   // for the tests: one step, from scratch

exports.handler = async function (event) {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  const q = event.queryStringParameters || {};
  const fn = String(q.fn || "");
  try {
    if (fn === "refreshEtsyToken") return json(200, { access_token: "sandbox-token", refresh_token: "sandbox-refresh", expires_in: 7200, sandbox: true });
    if (fn === "etsyImages") {
      const images=await listingImages(String(q.listingId || ""));
      return {...json(200,images),headers:{...CORS,'Cache-Control':images.length?'public, max-age=86400':'public, max-age=300','X-Etsy-Calls':'0'}};
    }
    let { meta, receipts } = await loadSnapshot();
    const stream = fn === "listOpenOrders" || fn === "status" ? await loadStream() : null;
    if (fn === "status") return json(200, { ok: true, sandbox: true, count: receipts.length, at: meta ? meta.at : null, path: meta ? meta.path : null, takenBy: meta ? meta.takenBy || null : null, stream: stream && stream.on ? { seed: stream.seed, tick: stream.tick, simNow: stream.simNow, arrived: streamed(stream, receipts, meta).length } : null });
    if (stream && stream.on) receipts = streamed(stream, receipts, meta);
    if (fn === "listOpenOrders") {
      const offset = Math.max(0, Number(q.offset) || 0);
      // Etsy lists receipts with their transactions; the page shape matches listOpenOrders (results only)
      const open=receipts.filter(r=>r.is_paid!==false && r.was_paid!==false && !r.is_shipped && !r.was_shipped && !r.is_canceled && !r.was_canceled && !/cancel/i.test(r.status || ""));
      return json(200, { results: open.slice(offset, offset + PAGE), count: open.length, sandbox: true });
    }
    if (fn === "etsyOrderProxy") {
      const id = String(q.orderId || "");
      let r = receipts.find(x => String(x.receipt_id) === id);
      // a streamed order read on its own, from the stream playing now (another instance may have seen a reset)
      if (!r && /^\d+$/.test(id)) { const s = await loadStream(); if (s && s.on) { streamed(s, receipts, meta); r = built.byId.get(id); } }
      if (!r) return json(404, { error: "receipt not found in the sandbox snapshot", sandbox: true });
      return json(200, { receipt: stripTx(r), transactions: r.transactions || [], sandbox: true });
    }
    return json(400, { error: "unknown fn", fns: ["listOpenOrders", "etsyOrderProxy", "etsyImages", "refreshEtsyToken", "status"] });
  } catch (e) {
    console.error("[etsySandbox]", fn, e);
    return json(500, { error: e.message || String(e), sandbox: true });
  }
};
