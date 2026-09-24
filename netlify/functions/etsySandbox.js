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
 *  list is not the snapshot but the orders that have arrived by the stream's clock
 *  and not shipped yet: an order finished at the station ships a little later, and
 *  one nobody finishes ships by hand two days past its ship date. So the list, and
 *  what an instance keeps in memory, stay the same size however long it plays.
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
   date. Pure in (seed, step), so the same seed replays the same orders, and any one order is built again alone from its
   receipt number, which names its step (plan). ── */
let streamFlight = null;
async function loadStream() {   // read on every list sweep, not cached: at 50x the clock moves every 12 s
  if (streamFlight) return streamFlight;
  streamFlight = db.collection(SANDBOX).doc("stream").get().then(d => (d.exists ? d.data() : null));
  try { return await streamFlight; } finally { streamFlight = null; }
}
const hash = s => { let h = 2166136261; for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 16777619); return h >>> 0; };
const rand = a => () => { a = (a + 0x6D2B79F5) | 0; let t = Math.imul(a ^ (a >>> 15), 1 | a); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
const isOpen = r => r.is_paid !== false && r.was_paid !== false && !r.is_shipped && !r.was_shipped && !r.is_canceled && !r.was_canceled && !/cancel/i.test(r.status || "");
const LEAD_S = 14 * 86400;   // a snapshot order with no ship date: its copies are due two weeks after they arrive
function streamPool(receipts, meta) {
  let maxR = 0, maxT = 0;
  for (const r of receipts) { maxR = Math.max(maxR, +r.receipt_id || 0); for (const x of r.transactions || []) maxT = Math.max(maxT, +x.transaction_id || 0); }
  const pool = receipts.filter(isOpen), at = Math.floor(((meta && meta.at) || Date.now()) / 1000);
  // how long after it arrives a copy of each open receipt is due to ship: the original's lead to its latest ship date,
  // which arrival() keeps
  const lead = pool.map(r => {
    const was = +(r.create_timestamp || r.created_timestamp || r.creation_tsz) || at; let due = 0;
    for (const f of ["expected_ship_date", "dispatch_date", "ship_by_date"]) due = Math.max(due, +r[f] || 0);
    for (const x of r.transactions || []) due = Math.max(due, +x.expected_ship_date || 0);
    return due ? Math.max(0, due - was) : LEAD_S;
  });
  return { pool, maxR, maxT, at, lead };
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
/** The seed's own band of numbers, above every receipt and transaction number in the snapshot. */
const bases = (s, ctx) => ({ r: (Math.floor(ctx.maxR / 1e7) + 1 + hash(`${s.seed}:r`) % 97) * 1e7, t: (Math.floor(ctx.maxT / 1e7) + 1 + hash(`${s.seed}:t`) % 97) * 1e7 });
/** Step k's orders (k from 1) as numbers: arrival times inside the step, and receipt and transaction numbers above every
    one in the snapshot, rising with time as Etsy's do, in a band of their own per seed; and which open snapshot receipt
    each one copies. A step's receipt numbers lie in base + 1000k … base + 1000k + 999, so a number names its step.
    Nothing is copied here, so a cold instance plans every step it looks back at and builds only the orders it keeps. */
function plan(s, ctx, k) {
  if (!ctx.pool.length) return [];
  const R = rand(hash(`${s.seed}:${k}`)), n = s.min + Math.floor(R() * (s.max - s.min + 1)), from = s.simStart + (k - 1) * s.stepMs, gap = Math.floor(1000 / n), base = bases(s, ctx);
  const times = Array.from({ length: n }, () => from + 1000 + Math.floor(R() * (s.stepMs - 1000))).sort((a, b) => a - b);
  // the draws in the order they have always been made (the source, then the receipt number), so a seed replays as before
  return times.map((ms, j) => { const src = Math.floor(R() * ctx.pool.length); return { src, rid: base.r + k * 1000 + j * gap + Math.floor(R() * gap), tx0: base.t + k * 1000 + j * gap, t: Math.floor(ms / 1000) }; });
}
const build = (ctx, p) => arrival(ctx.pool[p.src], p.rid, p.tx0, p.t, ctx.at);
/** Step k's orders, built. */
const batch = (s, ctx, k) => plan(s, ctx, k).map(p => build(ctx, p));
exports.batch = (s, receipts, k, meta) => batch(s, streamPool(receipts, meta), k);   // for the tests: one step, from scratch

/* ── what an instance keeps of the stream is the orders not yet shipped, so it stays the same size however long the
   stream plays. An order the station finished (Sandbox_Design_Completed Orders) ships SHIP_MS after it was finished:
   until then it is listed as Etsy lists a finished order until it ships, and an undo at the station keeps it open. An
   order nobody finishes ships by hand GRACE_S past its ship date, and no step older than MAX_BACK is looked at. A warm
   instance saw each step come and learns of each finishing from one read of the new ones every ASK_MS; a cold one plans
   the steps it looks back at (no copies) and reads which of those orders are finished, one document each: it never
   builds every step from the first. ── */
const COMPLETED = "Sandbox_Design_Completed Orders";
const SHIP_MS = 10 * 60000, GRACE_S = 2 * 86400, MAX_BACK = 2016, ASK_MS = 10000, SLACK_MS = 15000;   // MAX_BACK: 14 simulated days of steps
let held = null, syncing = Promise.resolve();
/** This instance's view of the stream playing: its orders not yet shipped by step, and when the station finished those
    it has finished. A new stream (a reset, a new seed or snapshot) starts a new view. */
function view(s, receipts, meta) {
  const key = [s.seed, s.simStart, s.stepMs, s.min, s.max, meta ? meta.path : "", s.startedAt || ""].join("|");
  if (!held || held.key !== key || held.tick > (+s.tick || 0)) { const ctx = streamPool(receipts, meta); held = { key, ctx, base: bases(s, ctx), tick: 0, steps: new Map(), done: new Map(), cold: true, doneAt: 0, askedAt: 0 }; }
  return held;
}
const stepOf = (h, rid) => Math.floor((Number(rid) - h.base.r) / 1000);
const heldEntry = (h, rid) => (h.steps.get(stepOf(h, rid)) || []).find(e => e.rid === rid);
const finishedAt = (d, now) => { const v = typeof d.get === "function" ? d.get("completedAt") : (d.data() || {}).completedAt; return v && v.toMillis ? v.toMillis() : (+v || now); };
/** Which of these receipts the station has finished, and when (ms): one document read each, four reads at a time. */
async function finishedOf(ids, now) {
  const out = new Map(), parts = [];
  for (let i = 0; i < ids.length; i += 300) parts.push(ids.slice(i, i + 300));
  await Promise.all(Array.from({ length: Math.min(4, parts.length) }, async () => {
    while (parts.length) { const part = parts.shift(); (await db.getAll(...part.map(id => db.collection(COMPLETED).doc(id)))).forEach(d => { if (d.exists) out.set(String(d.id), finishedAt(d, now)); }); }
  }));
  return out;
}
async function refresh(s, receipts, meta) {
  const h = view(s, receipts, meta), tick = Math.max(0, Math.floor(+s.tick || 0)), now = Date.now();
  const simS = Math.floor((s.simStart + tick * s.stepMs) / 1000), lo = Math.max(1, tick - MAX_BACK + 1), open = e => e.due + GRACE_S >= simS;
  const cold = h.cold; if (cold) { h.cold = false; h.doneAt = now - SLACK_MS; h.askedAt = now; }
  if (tick > h.tick) {
    const come = [];
    for (let k = Math.max(lo, h.tick + 1); k <= tick; k++) { const l = plan(s, h.ctx, k).map(p => ({ p, rid: String(p.rid), due: p.t + h.ctx.lead[p.src] })).filter(open); if (l.length) come.push([k, l]); }
    // a cold instance asks which of the orders that came before it are finished; a warm one saw these steps come after
    // its last read of the finishings, so it learns of theirs below
    let fin = new Map();
    if (cold && come.length) fin = await finishedOf(come.flatMap(([, l]) => l.map(e => e.rid)), now).catch(e => { console.warn("[etsySandbox] finished orders not read:", e.message); return new Map(); });
    for (const [k, l] of come) {
      const keep = [];
      for (const e of l) { const at = fin.get(e.rid); if (at != null && at <= now - SHIP_MS) continue; if (at != null) h.done.set(e.rid, at); keep.push({ r: build(h.ctx, e.p), rid: e.rid, due: e.due }); }
      if (keep.length) h.steps.set(k, keep);
    }
    h.tick = tick;
  }
  // past the look-back, or two days past its ship date with nobody finishing it: shipped by hand
  for (const [k, l] of h.steps) { const keep = k >= lo ? l.filter(open) : []; if (keep.length === l.length) continue; l.forEach(e => { if (!keep.includes(e)) h.done.delete(e.rid); }); if (keep.length) h.steps.set(k, keep); else h.steps.delete(k); }
  // the station's finishings since the last read (all of them go into it, however many pages a sweep reads)
  if (now - h.askedAt >= ASK_MS) {
    h.askedAt = now;
    if (!h.steps.size) h.doneAt = now - SLACK_MS;
    else try {
      const snap = await db.collection(COMPLETED).where("completedAt", ">=", admin.firestore.Timestamp.fromMillis(h.doneAt)).select("completedAt").get();
      snap.docs.forEach(d => { if (heldEntry(h, String(d.id))) h.done.set(String(d.id), finishedAt(d, now)); });
      h.doneAt = now - SLACK_MS;
    } catch (e) { console.warn("[etsySandbox] finished orders not read:", e.message); }
  }
  // finished long enough ago: shipped, once the station still has it finished (an undo there keeps it open, and one
  // finished again since ships that much later)
  const ship = [...h.done].filter(([, at]) => at <= now - SHIP_MS).map(([rid]) => rid);
  if (ship.length) {
    const still = await finishedOf(ship, now).catch(e => { console.warn("[etsySandbox] finished orders not read:", e.message); return null; });
    if (still) for (const rid of ship) {
      const at = still.get(rid);
      if (at != null && at > now - SHIP_MS) { h.done.set(rid, at); continue; }
      h.done.delete(rid); if (at == null) continue;
      const k = stepOf(h, rid), l = (h.steps.get(k) || []).filter(e => e.rid !== rid);
      if (l.length) h.steps.set(k, l); else h.steps.delete(k);
    }
  }
  // newest first, as Etsy lists open receipts
  const out = [];
  for (const k of [...h.steps.keys()].sort((a, b) => b - a)) { const l = h.steps.get(k); for (let j = l.length - 1; j >= 0; j--) out.push(l[j].r); }
  return out;
}
/** Every order that has arrived by the stream's clock and not shipped, newest first. One refresh at a time: the pages of
    a sweep, and the sweeps of two stations, share this instance's view. */
function streamed(s, receipts, meta) { const run = syncing.then(() => refresh(s, receipts, meta)); syncing = run.catch(() => {}); return run; }
/** One streamed order built again from its number alone, however old, and read back as shipped once it has shipped, as
    Etsy reads a shipped receipt: the sorter then settles it as gone instead of reading it again at every check. This
    instance knows the orders it shipped; for the others it reads the order's one finished record, if any. */
async function orderAlone(s, receipts, meta, id) {
  const h = view(s, receipts, meta), k = stepOf(h, id), tick = Math.max(0, Math.floor(+s.tick || 0));
  if (!(k >= 1 && k <= tick)) return null;
  const e = heldEntry(h, id); if (e) return e.r;
  const p = plan(s, h.ctx, k).find(x => String(x.rid) === id); if (!p) return null;
  const r = build(h.ctx, p), now = Date.now(), simS = Math.floor((s.simStart + tick * s.stepMs) / 1000);
  let shipped = k < tick - MAX_BACK + 1 || p.t + h.ctx.lead[p.src] + GRACE_S < simS || (!h.cold && k <= h.tick);
  if (!shipped) { const at = (await finishedOf([id], now)).get(id); shipped = at != null && at <= now - SHIP_MS; }
  if (shipped) { r.is_shipped = true; r.status = "Completed"; }
  return r;
}
// for the tests: the limits, and how many orders this instance holds
exports.upkeep = { SHIP_MS, GRACE_S, MAX_BACK, ASK_MS, size: () => (held ? [...held.steps.values()].reduce((n, l) => n + l.length, 0) : 0) };

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
    // arrived: the streamed orders listed now (arrived and not shipped)
    if (fn === "status") return json(200, { ok: true, sandbox: true, count: receipts.length, at: meta ? meta.at : null, path: meta ? meta.path : null, takenBy: meta ? meta.takenBy || null : null, stream: stream && stream.on ? { seed: stream.seed, tick: stream.tick, simNow: stream.simNow, arrived: (await streamed(stream, receipts, meta)).length } : null });
    if (stream && stream.on) receipts = await streamed(stream, receipts, meta);
    if (fn === "listOpenOrders") {
      const offset = Math.max(0, Number(q.offset) || 0);
      // Etsy lists receipts with their transactions; the page shape matches listOpenOrders (results only)
      const open=receipts.filter(r=>r.is_paid!==false && r.was_paid!==false && !r.is_shipped && !r.was_shipped && !r.is_canceled && !r.was_canceled && !/cancel/i.test(r.status || ""));
      return json(200, { results: open.slice(offset, offset + PAGE), count: open.length, sandbox: true });
    }
    if (fn === "etsyOrderProxy") {
      const id = String(q.orderId || "");
      let r = receipts.find(x => String(x.receipt_id) === id);
      // a streamed order read on its own, from the stream playing now (another instance may have seen a reset): built
      // again from its number alone, however old
      if (!r && /^\d+$/.test(id)) { const s = await loadStream(); if (s && s.on) r = await orderAlone(s, receipts, meta, id); }
      if (!r) return json(404, { error: "receipt not found in the sandbox snapshot", sandbox: true });
      return json(200, { receipt: stripTx(r), transactions: r.transactions || [], sandbox: true });
    }
    return json(400, { error: "unknown fn", fns: ["listOpenOrders", "etsyOrderProxy", "etsyImages", "refreshEtsyToken", "status"] });
  } catch (e) {
    console.error("[etsySandbox]", fn, e);
    return json(500, { error: e.message || String(e), sandbox: true });
  }
};
