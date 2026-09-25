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
 *  list is not the snapshot but the snapshot's orders that have arrived by the
 *  stream's clock, under their real Etsy numbers, and not shipped yet: an order
 *  finished at the station ships a little later, and one nobody finishes ships by
 *  hand two days past its ship date. So the list, and what an instance keeps in
 *  memory, stay small however long it plays.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const admin = require("./firebaseAdmin");
const { CORS, gate } = require("./_charmNestAuth");
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

/* ── the order stream: each simulated ten-minute step brings 2 to 5 of the snapshot's own open orders, oldest first, each
   under its real Etsy receipt and transaction numbers (Paul, 25 Sep: the actual Etsy orders, never made-up numbers),
   stamped with its simulated arrival time and keeping its lead time to its ship date. Each order comes once: when every
   open order of the snapshot has come there is nothing more to bring (charmNestLibrary stops the stream's clock there; a
   new snapshot brings more). Pure in (seed, step), so the same seed replays the same arrivals, and any one order is built
   again alone from its number, whose place in the list names its step (stepOf). ── */
let streamFlight = null;
async function loadStream() {   // read on every list sweep, not cached: at 50x the clock moves every 12 s
  if (streamFlight) return streamFlight;
  streamFlight = db.collection(SANDBOX).doc("stream").get().then(d => (d.exists ? d.data() : null));
  try { return await streamFlight; } finally { streamFlight = null; }
}
const hash = s => { let h = 2166136261; for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 16777619); return h >>> 0; };
const rand = a => () => { a = (a + 0x6D2B79F5) | 0; let t = Math.imul(a ^ (a >>> 15), 1 | a); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
const isOpen = r => r.is_paid !== false && r.was_paid !== false && !r.is_shipped && !r.was_shipped && !r.is_canceled && !r.was_canceled && !/cancel/i.test(r.status || "");
const LEAD_S = 14 * 86400;   // a snapshot order with no ship date: it is due two weeks after it arrives
// a stream of the snapshot's own orders; one started before (copies under made-up numbers) lists nothing, and the sorter's
// next check starts one of these in its place (charmNestLibrary sandboxStream)
const STREAM_V = 2;
const playing = s => !!(s && s.on && (+s.v || 1) >= STREAM_V);
const madeAt = (r, at) => +(r.create_timestamp || r.created_timestamp || r.creation_tsz) || at;
function streamPool(receipts, meta) {
  const at = Math.floor(((meta && meta.at) || Date.now()) / 1000);
  // the open orders in the order the shop took them, oldest first
  const pool = receipts.filter(isOpen).sort((a, b) => madeAt(a, at) - madeAt(b, at) || Number(a.receipt_id) - Number(b.receipt_id));
  // how long after it arrives each is due to ship: its lead to its latest ship date, which arrival() keeps
  const lead = pool.map(r => {
    const was = madeAt(r, at); let due = 0;
    for (const f of ["expected_ship_date", "dispatch_date", "ship_by_date"]) due = Math.max(due, +r[f] || 0);
    for (const x of r.transactions || []) due = Math.max(due, +x.expected_ship_date || 0);
    return due ? Math.max(0, due - was) : LEAD_S;
  });
  // place: where each receipt number stands in the list; starts[k]: how many of its orders came before step k (before())
  return { pool, at, lead, place: new Map(pool.map((r, i) => [String(r.receipt_id), i])), starts: [0, 0] };
}
/** A snapshot receipt as the order that arrived at `t` (epoch seconds, as Etsy writes them): its own numbers and contents,
    the simulated arrival time, and its ship dates as far after it as the original's were. */
function arrival(src, t, snapAt) {
  const r = JSON.parse(JSON.stringify(src)), was = madeAt(src, snapAt), lead = v => (+v ? +v + t - was : v);
  r.create_timestamp = r.created_timestamp = r.update_timestamp = r.updated_timestamp = t;
  for (const f of ["creation_tsz", "last_modified_tsz"]) if (r[f] != null) r[f] = t;
  for (const f of ["expected_ship_date", "dispatch_date", "ship_by_date"]) if (r[f] != null) r[f] = lead(r[f]);
  for (const x of r.transactions || []) {
    for (const f of ["create_timestamp", "created_timestamp", "paid_timestamp", "update_timestamp", "updated_timestamp"]) if (x[f] != null) x[f] = t;
    if (x.expected_ship_date != null) x.expected_ship_date = lead(x.expected_ship_date); if (x.shipped_timestamp != null) x.shipped_timestamp = null;
  }
  return r;
}
/** How many orders step k brings: the first draw of the step's own generator, as plan() draws it. */
const count = (s, k) => s.min + Math.floor(rand(hash(`${s.seed}:${k}`))() * (s.max - s.min + 1));
/** How many of the list's orders came before step k (k from 1): past the step the last one came in, every one of them. */
function before(s, ctx, k) {
  const a = ctx.starts;
  while (a.length <= k && a[a.length - 1] < ctx.pool.length) a.push(a[a.length - 1] + count(s, a.length - 1));
  return a.length > k ? a[k] : a[a.length - 1];
}
/** Step k's orders (k from 1) as numbers: their arrival times inside the step, and which orders of the list they are, the
    next ones after those the steps before it brought. Nothing is copied here, so a cold instance plans every step it looks
    back at and builds only the orders it keeps. */
function plan(s, ctx, k) {
  const first = before(s, ctx, k); if (first >= ctx.pool.length) return [];
  const R = rand(hash(`${s.seed}:${k}`)), n = s.min + Math.floor(R() * (s.max - s.min + 1)), from = s.simStart + (k - 1) * s.stepMs;
  const times = Array.from({ length: Math.min(n, ctx.pool.length - first) }, () => from + 1000 + Math.floor(R() * (s.stepMs - 1000))).sort((a, b) => a - b);
  return times.map((ms, j) => ({ src: first + j, rid: String(ctx.pool[first + j].receipt_id), t: Math.floor(ms / 1000) }));
}
const build = (ctx, p) => arrival(ctx.pool[p.src], p.t, ctx.at);
/** Step k's orders, built. */
const batch = (s, ctx, k) => plan(s, ctx, k).map(p => build(ctx, p));
exports.batch = (s, receipts, k, meta) => batch(s, streamPool(receipts, meta), k);   // for the tests: one step, from scratch

/* ── what an instance keeps of the stream is the orders not yet shipped, so it stays small however long the stream
   plays. An order the station finished (Sandbox_Design_Completed Orders) ships SHIP_MS after it was finished: until then
   it is listed as Etsy lists a finished order until it ships, and an undo at the station keeps it open. An order nobody
   finishes ships by hand GRACE_S past its ship date. A warm instance saw each step come and learns of each finishing from
   one read of the new ones every ASK_MS; a cold one plans the steps that brought orders (no copies; none after the step
   that brought the last) and reads which of those orders are finished, one document each. ── */
const COMPLETED = "Sandbox_Design_Completed Orders";
const SHIP_MS = 10 * 60000, GRACE_S = 2 * 86400, ASK_MS = 10000, SLACK_MS = 15000;
/** The step that brings the list's last order (0 for an empty list): no step after it brings any. */
function lastStep(s, ctx) { before(s, ctx, Infinity); return Math.max(0, ctx.starts.length - 2); }
let held = null, syncing = Promise.resolve();
/** This instance's view of the stream playing: its orders not yet shipped by step, and when the station finished those
    it has finished. A new stream (a reset, a new seed or snapshot) starts a new view. */
function view(s, receipts, meta) {
  const key = [s.seed, s.simStart, s.stepMs, s.min, s.max, meta ? meta.path : "", s.startedAt || ""].join("|");
  if (!held || held.key !== key || held.tick > (+s.tick || 0)) held = { key, s: { seed: s.seed, min: s.min, max: s.max }, ctx: streamPool(receipts, meta), tick: 0, steps: new Map(), done: new Map(), cold: true, doneAt: 0, askedAt: 0 };
  return held;
}
/** The step an order came in, from its place in the list (0: a number the snapshot does not hold). */
function stepOf(h, rid) {
  const i = h.ctx.place.get(String(rid)); if (i == null) return 0;
  const a = h.ctx.starts;
  while (a[a.length - 1] <= i) before(h.s, h.ctx, a.length);   // the steps so far, up to the one that brings it
  let lo = 1, hi = a.length - 1;   // a[lo] <= i < a[hi]
  while (hi - lo > 1) { const m = (lo + hi) >> 1; if (a[m] <= i) lo = m; else hi = m; }
  return lo;
}
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
  const simS = Math.floor((s.simStart + tick * s.stepMs) / 1000), open = e => e.due + GRACE_S >= simS;
  const cold = h.cold; if (cold) { h.cold = false; h.doneAt = now - SLACK_MS; h.askedAt = now; }
  if (tick > h.tick) {
    const come = [];
    for (let k = h.tick + 1, to = Math.min(tick, lastStep(h.s, h.ctx)); k <= to; k++) { const l = plan(s, h.ctx, k).map(p => ({ p, rid: p.rid, due: p.t + h.ctx.lead[p.src] })).filter(open); if (l.length) come.push([k, l]); }
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
  // two days past its ship date with nobody finishing it: shipped by hand
  for (const [k, l] of h.steps) { const keep = l.filter(open); if (keep.length === l.length) continue; l.forEach(e => { if (!keep.includes(e)) h.done.delete(e.rid); }); if (keep.length) h.steps.set(k, keep); else h.steps.delete(k); }
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
  const p = plan(s, h.ctx, k).find(x => x.rid === id); if (!p) return null;
  const r = build(h.ctx, p), now = Date.now(), simS = Math.floor((s.simStart + tick * s.stepMs) / 1000);
  let shipped = p.t + h.ctx.lead[p.src] + GRACE_S < simS || (!h.cold && k <= h.tick);
  if (!shipped) { const at = (await finishedOf([id], now)).get(id); shipped = at != null && at <= now - SHIP_MS; }
  if (shipped) { r.is_shipped = true; r.status = "Completed"; }
  return r;
}
// for the tests: the limits, and how many orders this instance holds
exports.upkeep = { SHIP_MS, GRACE_S, ASK_MS, size: () => (held ? [...held.steps.values()].reduce((n, l) => n + l.length, 0) : 0) };

// The snapshot holds real buyers' names, shipping addresses and orders: a call from outside shows the operator passcode
// first (EDIT_PASSCODE, the sorter's; unset, open as before). This site's own functions call serve() in process.
exports.handler = async function (event) {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  const denied = gate(event); if (denied) return denied;
  return serve(event);
};
async function serve(event) {
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
    // arrived: the streamed orders listed now (arrived and not shipped); total: the open orders the stream brings in all,
    // left: those it has not brought yet
    if (fn === "status") {
      const h = playing(stream) ? view(stream, receipts, meta) : null, tick = h ? Math.max(0, Math.floor(+stream.tick || 0)) : 0;
      return json(200, { ok: true, sandbox: true, count: receipts.length, at: meta ? meta.at : null, path: meta ? meta.path : null, takenBy: meta ? meta.takenBy || null : null, stream: h ? { seed: stream.seed, tick: stream.tick, simNow: stream.simNow, arrived: (await streamed(stream, receipts, meta)).length, total: h.ctx.pool.length, left: Math.max(0, h.ctx.pool.length - before(h.s, h.ctx, tick + 1)) } : null });
    }
    // (a stream of the old kind lists nothing: the sorter's next check starts a new one)
    if (stream && stream.on) receipts = playing(stream) ? await streamed(stream, receipts, meta) : [];
    if (fn === "listOpenOrders") {
      const offset = Math.max(0, Number(q.offset) || 0);
      // Etsy lists receipts with their transactions; the page shape matches listOpenOrders (results only)
      const open=receipts.filter(r=>r.is_paid!==false && r.was_paid!==false && !r.is_shipped && !r.was_shipped && !r.is_canceled && !r.was_canceled && !/cancel/i.test(r.status || ""));
      return json(200, { results: open.slice(offset, offset + PAGE), count: open.length, sandbox: true });
    }
    if (fn === "etsyOrderProxy") {
      const id = String(q.orderId || ""), s = await loadStream();
      // with the stream playing, an order read on its own is the one the stream brought (none before it comes), from the
      // stream playing now (another instance may have seen a reset): built again from its number alone, however old.
      // Without it, the snapshot's own; a stream of the old kind has nothing.
      const r = playing(s) ? (/^\d+$/.test(id) ? await orderAlone(s, receipts, meta, id) : null) : s && s.on ? null : receipts.find(x => String(x.receipt_id) === id);
      if (!r) return json(404, { error: "receipt not found in the sandbox snapshot", sandbox: true });
      return json(200, { receipt: stripTx(r), transactions: r.transactions || [], sandbox: true });
    }
    return json(400, { error: "unknown fn", fns: ["listOpenOrders", "etsyOrderProxy", "etsyImages", "refreshEtsyToken", "status"] });
  } catch (e) {
    console.error("[etsySandbox]", fn, e);
    return json(500, { error: e.message || String(e), sandbox: true });
  }
}
exports.serve = serve;
