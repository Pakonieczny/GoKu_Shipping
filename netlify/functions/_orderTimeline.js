/*  netlify/functions/_orderTimeline.js
 *  One timeline per Etsy order (Paul, 28 Sep 19:21): every step an order goes through, from the moment it comes in to
 *  shipping it and completing it on Etsy, as one event per thing that happened. The sorter writes through
 *  charmNestLibrary (op timelineAdd / timelineGet / cancelCheck), the production stations through firebaseOrders (open,
 *  station events only), the Etsy mirror through record(). Nothing here calls Etsy.
 *
 *  Order_Timeline/{orderId}~{key}   (Sandbox_Order_Timeline in the sandbox)
 *    orderId   Etsy receipt id (digits)            at        ms when it happened (the writer's clock)
 *    type      one of TYPES (order-timeline.js has the same list with its stamps)
 *    by        who: a person's name, "Etsy" or "System"
 *    source    sorter | station | etsy | system
 *    station   sorting | welding | assembly | shipping | design | laser | sorter (where it happened)
 *    device    the station page's own name (assembly-2, shipping-1 …)
 *    lineKey · transactionId · sheetId · sheet (its label, "GF Sheet 2") · setId
 *    text      one short line for the stamp's caption (≤ 200)
 *    data      small details for the event's panel (≤ 2 KB as JSON)
 *    milestone true for a step the order has passed (a big stamp on the timeline)
 *  The key makes a write idempotent: a writer that may repeat itself (a retry, the mirror seeing the same receipt twice)
 *  passes its own `id`; others get `${at}-${random}`.
 *
 *  No composite index: one equality query per order, sorted in memory (an order has tens of events, not thousands). */
"use strict";
const COL = "Order_Timeline";
const CANCELLED = "Charm_Nest_Cancelled";
const TYPES = new Set([
  "arrived", "pulled", "interpreted", "needsDecision", "decided", "skipped", "customRead", "customDecided", "designSent", "designDropped",
  "engraveNeeded", "engraveApproved", "engraveChanged",
  "pooled", "placed", "moved", "removed", "renested", "held", "released", "restored", "cancelled", "etsyCancelled", "cancelRestored",
  "sizeChanged", "included", "excluded", "merged", "roseLine", "roseCut",
  "qrLabel", "setCommitted", "laserDone", "recalled",
  "sealPrinted", "sealCompleted",
  "scan", "sorted", "welded", "assembled", "packed", "labelPrinted", "shipped", "etsyCompleted", "cancelAlert",
  "note", "teamMessage", "customerMessage", "other"
]);
const MILESTONES = new Set(["arrived", "decided", "engraveApproved", "placed", "qrLabel", "setCommitted", "laserDone", "sorted", "welded", "assembled", "packed", "labelPrinted", "shipped", "etsyCompleted", "sealCompleted"]);
// what a production station may write through the open door (firebaseOrders): its own scans and what it did with them
const STATION_TYPES = new Set(["scan", "sorted", "welded", "assembled", "packed", "labelPrinted", "shipped", "etsyCompleted", "cancelAlert", "note"]);
const STATIONS = new Set(["sorting", "welding", "assembly", "shipping", "design", "laser", "sorter", "qr", "inbox"]);

const orderIdOf = v => String(v == null ? "" : v).replace(/\D/g, "").slice(0, 30);
const s = (v, n) => String(v == null ? "" : v).slice(0, n);
const n = v => (Number.isFinite(+v) ? +v : 0);
function small(data) {
  if (!data || typeof data !== "object") return null;
  try { const j = JSON.stringify(data); return j.length <= 2048 ? JSON.parse(j) : { note: "details too large to keep", size: j.length }; } catch (_) { return null; }
}
/** One event as it is stored, or null when it is not one. */
function clean(e, opts = {}) {
  if (!e || typeof e !== "object") return null;
  const orderId = orderIdOf(e.orderId); if (!orderId) return null;
  const type = TYPES.has(e.type) ? e.type : null; if (!type) return null;
  if (opts.stationOnly && !STATION_TYPES.has(type)) return null;
  const at = n(e.at) > 1e12 && n(e.at) < Date.now() + 36e5 ? Math.round(n(e.at)) : Date.now();
  const station = STATIONS.has(e.station) ? e.station : (opts.station || "");
  const out = {
    orderId, type, at, by: s(e.by || opts.by || "", 80), source: opts.source || s(e.source, 20) || "sorter", station, device: s(e.device, 40),
    lineKey: s(e.lineKey, 80), transactionId: s(e.transactionId, 30), sheetId: s(e.sheetId, 100), sheet: s(e.sheet, 80), setId: s(e.setId, 100),
    text: s(e.text, 200), data: small(e.data), milestone: e.milestone === undefined ? MILESTONES.has(type) : !!e.milestone
  };
  const key = s(e.id, 120).replace(/[^\w.:-]/g, "_") || `${at}-${Math.random().toString(36).slice(2, 8)}`;
  return { key: `${orderId}~${type}~${key}`.slice(0, 400), doc: out };
}
const colOf = (db, prefix) => db.collection((prefix || "") + COL);

/** Writes events (at most 100 a call). Returns the keys written. */
async function add(db, FV, events, opts = {}) {
  const list = (Array.isArray(events) ? events : [events]).slice(0, 100).map(e => clean(e, opts)).filter(Boolean);
  if (!list.length) return { ok: true, ids: [] };
  const c = colOf(db, opts.prefix), batch = db.batch();
  for (const { key, doc } of list) batch.set(c.doc(key), Object.assign({}, doc, { createdAt: FV.serverTimestamp() }), { merge: true });
  await batch.commit();
  return { ok: true, ids: list.map(x => x.key) };
}
/** The recorded events of one order, oldest first, plus its cancel record (null when it is not cancelled). */
async function get(db, orderId, opts = {}) {
  const id = orderIdOf(orderId); if (!id) return { error: "orderId required" };
  const [snap, can] = await Promise.all([
    colOf(db, opts.prefix).where("orderId", "==", id).limit(2000).get(),
    db.collection((opts.prefix || "") + CANCELLED).doc(id).get()
  ]);
  const events = snap.docs.map(d => { const x = d.data(); delete x.createdAt; x.id = d.id; return x; }).sort((a, b) => a.at - b.at || String(a.id).localeCompare(String(b.id)));
  const cancelled = can.exists ? (x => { delete x.createdAt; return x; })(can.data()) : null;
  return { orderId: id, events, cancelled, now: Date.now(), truncated: snap.size >= 2000 };
}
/** Which of these orders are cancelled, with who/when/why: what a station asks right after a scan. */
async function cancelCheck(db, ids, opts = {}) {
  const list = [...new Set((Array.isArray(ids) ? ids : String(ids || "").split(",")).map(orderIdOf).filter(Boolean))].slice(0, 60);
  if (!list.length) return { cancelled: {} };
  const refs = list.map(id => db.collection((opts.prefix || "") + CANCELLED).doc(id));
  const docs = await db.getAll(...refs), cancelled = {};
  for (const d of docs) if (d.exists) { const x = d.data(); cancelled[d.id] = { at: n(x.at), by: s(x.by, 80), why: s(x.why, 400), source: s(x.source || (x.by === "Etsy" ? "etsy" : "sorter"), 20), sheets: Array.isArray(x.sheets) ? x.sheets.slice(0, 30) : [] }; }
  return { cancelled, now: Date.now() };
}
module.exports = { COL, TYPES, MILESTONES, STATION_TYPES, STATIONS, orderIdOf, clean, add, get, cancelCheck };
