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
  "pooled", "placed", "moved", "removed", "renested", "held", "released", "restored", "cancelled", "etsyCancelled", "cancelRestored", "cancelStep",
  "sizeChanged", "included", "excluded", "merged", "roseLine", "roseCut",
  "qrLabel", "setCommitted", "laserDone", "recalled",
  "sealPrinted", "sealCompleted",
  "scan", "sorted", "welded", "assembled", "packed", "labelPrinted", "shipped", "etsyCompleted", "cancelAlert",
  "note", "teamMessage", "customerMessage", "other"
]);
// the steps of the rail (RAIL): order in, nested, engraved, laser cut, sorted, welded, assembled, shipped (Etsy's
// completion with it); approving, QR labels, sets committed and packing are steps in between, not milestones
const MILESTONES = new Set(["arrived", "placed", "renested", "engraveApproved", "laserDone", "roseCut", "sorted", "welded", "assembled", "shipped", "etsyCompleted"]);
// what a production station may write through the open door (firebaseOrders): its own scans and what it did with them
const STATION_TYPES = new Set(["scan", "sorted", "welded", "assembled", "packed", "labelPrinted", "shipped", "etsyCompleted", "cancelAlert", "note"]);
const STATIONS = new Set(["sorting", "welding", "assembly", "shipping", "design", "laser", "sorter", "qr", "inbox"]);

const orderIdOf = v => String(v == null ? "" : v).replace(/\D/g, "").slice(0, 30);
const s = (v, n) => String(v == null ? "" : v).slice(0, n);
const n = v => (Number.isFinite(+v) ? +v : 0);
// Firestore keeps no array directly inside an array and refuses the whole batch for one: such a list is kept as its text
const flat = (v, inList) => Array.isArray(v) ? (inList ? JSON.stringify(v) : v.map(x => flat(x, true))) : v && typeof v === "object" ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, flat(x, false)])) : v;
function small(data) {
  if (!data || typeof data !== "object") return null;
  try { const j = JSON.stringify(data); return j.length <= 2048 ? flat(JSON.parse(j), false) : { note: "details too large to keep", size: j.length }; } catch (_) { return null; }
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
    // (a scan says where the order was seen and a label print is a step's detail: neither is ever a milestone)
    text: s(e.text, 200), data: small(e.data), milestone: type === "scan" || type === "labelPrinted" ? false : e.milestone === undefined ? MILESTONES.has(type) : !!e.milestone
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
/** The whole timeline of one order, oldest first: its recorded events, plus (derive, the default) the events read from
    the records the shop already keeps, so an order from before the timeline existed still has its history. Also its
    cancel record (null when it is not cancelled) and `where`: where the order is now.
    opts: { prefix: "Sandbox_" | "", sandboxed: charmNestLibrary's SANDBOXED set, derive: true } */
/* An order's recorded events, a page at a time in document-id order: the one-field index its equality query already
   runs on, so no composite index. Ids are orderId~type~key, so the unordered limit(2000) this replaced answered the
   types in alphabetical order, and a long run of one type (a guard's cancelAlert loop) hid every later one: shipped,
   sorted, welded. A type past PER_TYPE is skipped to its end, so every type is read; at most MAX_READ documents.
   What was left out is said (leftOut): the types cut at PER_TYPE, and `capped` when MAX_READ (or the page limit) ended
   the read, so a later type may be missing altogether. */
const PAGE = 500, PER_TYPE = 500, MAX_READ = 2000;
async function recordedOf(c, id) {
  const docs = [], per = new Map(), skipped = []; let after = null, truncated = false;
  const capped = () => ({ docs, truncated: true, skipped, capped: true });
  for (let pages = 0; pages < 12; pages++) {
    const want = Math.min(PAGE, MAX_READ - docs.length); if (want <= 0) return capped();
    // (a query with no orderBy answers in document-id order already; a later page says so, to start past a document id)
    let q = c.where("orderId", "==", id);
    if (after) q = q.orderBy("__name__").startAfter(after);
    const snap = await q.limit(want).get(); let jump = null;
    for (const d of snap.docs) {
      const t = String(d.id).split("~")[1] || "", k = (per.get(t) || 0) + 1; per.set(t, k);
      if (k > PER_TYPE) { truncated = true; if (!skipped.includes(t)) skipped.push(t); jump = `${id}~${t}~~`; break; }   // ("~" sorts after every key character)
      docs.push(d);
    }
    if (jump) { after = jump; continue; }
    if (snap.docs.length < want) return { docs, truncated, skipped, capped: false };
    after = snap.docs[snap.docs.length - 1].id;
  }
  return capped();
}
// what a cut-short read left out, for the page to say plainly: { types (each cut at `kept`), kept, capped }; null: nothing
const leftOutOf = r => (r.truncated ? { types: r.skipped.slice(0, 20), kept: PER_TYPE, capped: !!r.capped } : null);
async function get(db, orderId, opts = {}) {
  const id = orderIdOf(orderId); if (!id) return { error: "orderId required" };
  const [snap, can, derived] = await Promise.all([
    recordedOf(colOf(db, opts.prefix), id),
    db.collection((opts.prefix || "") + CANCELLED).doc(id).get(),
    opts.derive === false ? null : withTimeout(deriveEvents(db, id, opts), DERIVE_MS)
  ]);
  const recorded = snap.docs.map(d => { const x = d.data(); delete x.createdAt; x.id = d.id; return x; });
  const cancelled = can.exists ? (x => { delete x.createdAt; return x; })(can.data()) : null;
  const sandbox = !!opts.prefix;
  if (!derived) { const events = chronology(recorded, { sandbox }).sort(byTime); return { orderId: id, events, cancelled, where: whereOf(events, cancelled, { record: true }), now: Date.now(), truncated: snap.truncated, leftOut: leftOutOf(snap) }; }
  const { events: raw, sheets, errors, timedOut } = derived.value || { events: [], sheets: null, errors: [], timedOut: true };
  const cancelEvents = cancelled ? finalize(id, cancelEventsOf(id, cancelled)) : [];
  const kept = dedupe(recorded, cancelEvents.concat(raw)); kept.forEach(e => { delete e.series; });
  const events = chronology(recorded.concat(kept), { sandbox }).sort(byTime);
  const out = { orderId: id, events, cancelled, where: whereOf(events, cancelled, { sheets, record: true }), now: Date.now(), truncated: snap.truncated, leftOut: leftOutOf(snap), derived: { count: kept.length, dropped: cancelEvents.length + raw.length - kept.length } };
  if (errors && errors.length) out.derived.errors = errors.slice(0, 12);
  if (timedOut) out.derived.timedOut = true;
  return out;
}
/** Which of these orders are cancelled, with who/when/why: what a station asks right after a scan. */
async function cancelCheck(db, ids, opts = {}) {
  const list = [...new Set((Array.isArray(ids) ? ids : String(ids || "").split(",")).map(orderIdOf).filter(Boolean))].slice(0, 60);
  if (!list.length) return { cancelled: {} };
  const refs = list.map(id => db.collection((opts.prefix || "") + CANCELLED).doc(id));
  const docs = await db.getAll(...refs), cancelled = {};
  for (const d of docs) if (d.exists) {
    const x = d.data(); cancelled[d.id] = { at: n(x.at), by: s(x.by, 80), why: s(x.why, 400), source: s(x.source || (x.by === "Etsy" ? "etsy" : "sorter"), 20), sheets: Array.isArray(x.sheets) ? x.sheets.slice(0, 30) : [] };
    // full (the sorter's library door only; a station's door never asks): the whole record, its lines, fates and removals
    if (opts.full) { const w = Object.assign({}, x); delete w.createdAt; cancelled[d.id] = Object.assign(w, cancelled[d.id]); }
  }
  return { cancelled, now: Date.now() };
}

/* ── the history read from what the shop already keeps (Paul, C1 · C7 · D2) ──────────────────────────────────────────
   An order from before the timeline has no recorded events, but its story is in the records the shop has always kept:
     Charm_Nest_Arrivals/{rid}        firstSeenAt, createTs              → arrived (first seen by the sorter)
     Charm_Pool (orderId == rid)      createdAt, removed*, moved*, engraveApprovedBy, committedAt
                                                                         → pooled, removed, moved, engraveApproved, setCommitted
     Charm_Pool_Back/{poolId}         approvedAt, approvedBy, text       → engraveApproved
     Charm_Nest_Sheets (orders ∋ rid) label, metal, stock, laserDone*, roseCutAt, rosePlanJson stages
                                                                         → placed, qrLabel, laserDone, roseCut, roseLine
     Charm_Nest_Rose_Stock/{stock}/cuts/{sheetId}  by                    → who cut a Rose Gold sheet
     Charm_Nest_Sets/{setId}          committedAt, committed[], orders[rid] → setCommitted
     Charm_Custom_Orders (receiptId == rid)  stamps, completedAt/By      → sealPrinted, sealCompleted
     Charm_Nest_CustomRead/{lineKey}  reads[latest], decided             → customRead, customDecided
     Charm_Nest_Cancelled/{rid}       (read by get() itself)             → cancelled, etsyCancelled
     Brites_Orders/{rid}/messages     the Team's messages and stamps     → teamMessage, note ("DESIGNED :)", QA1, QA2, PE)
     Design_Order_Archive/{rid}, "Design_Completed Orders"/{rid}         → setCommitted / design complete
     EtsyMail_Receipts/{rid}          the inbox's Etsy mirror            → arrived (placed on Etsy), shipped, etsyCompleted,
                                                                           etsyCancelled   (no Etsy call: the mirror is read)
   Two round trips, every read in each in parallel: one-field queries (no composite index) and reads by id, each capped,
   the big fields left out (field masks). Nothing is cached between requests. A read that fails is left out and named
   in derived.errors; the whole derivation gives up after DERIVE_MS and the recorded events are answered alone.
   Sandbox: the sorter's own records carry the Sandbox_ prefix exactly as charmNestLibrary's SANDBOXED set says (it is
   passed in); the stations' records (Brites_Orders, the design ledgers) are the Sandbox_ copies firebaseOrders and
   designArchive keep; the Etsy mirror is production's and is not read in the sandbox; a person's custom decision is
   kept per workspace (decided / decidedSandbox), as _charmNestCustomRead does. ── */
const DERIVE_MS = 2500, DEDUPE_MS = 3 * 60 * 1000;
// charmNestLibrary's SANDBOXED (with Charm_Custom_Orders, which it adds), for a caller that does not pass its own
const SANDBOXED_DEFAULT = new Set(["Charm_Nest_Rose_Stock", "Charm_Nest_Sheets", "Charm_Pool", "Charm_Pool_Back", "Charm_Nest_Sets", "Charm_Nest_Counters", "Charm_Nest_Runs", "Charm_Nest_Run_Lines", "Charm_Nest_Run_Live", "Charm_Nest_Release", "Charm_Nest_Arrivals", "Charm_Nest_Cancelled", "Design_Bridge", "Charm_Custom_Orders"]);
const STATION_SANDBOXED = new Set(["Brites_Orders", "Design_Completed Orders", "Design_Order_Archive"]);
const CAP = { pools: 200, sheets: 40, custom: 40, reads: 40, messages: 120, backs: 100, sets: 20, rose: 6 };
const SHEET_FIELDS = ["id", "metal", "metalLabel", "sheetIndex", "page", "setId", "setSeq", "fileBase", "stock", "poolIds", "orders", "label", "archived", "draft", "laserDoneAt", "laserDoneBy", "roseCutAt", "roseStockId", "rosePlanHash", "createdAt", "cardStartedAt", "updatedAt", "runId"];
const POOL_FIELDS = ["poolId", "orderId", "transactionId", "lineKey", "runId", "setId", "sheetId", "sheetName", "sku", "material", "copy", "state", "orderDate", "createdAt", "updatedAt", "removedAt", "removedBy", "removedReason", "movedAt", "movedBy", "movedFrom", "movedTo", "engraveApprovedBy", "committedAt"];
const CUSTOM_FIELDS = ["key", "receiptId", "transactionId", "sku", "title", "kind", "completedAt", "completedBy", "how", "printedAt", "printedBy", "lastPrintedAt", "lastPrintedBy", "prints", "stamps"];
const BACK_FIELDS = ["poolId", "sheetId", "setId", "approvedAt", "approvedBy", "text", "invalidated", "transactionId", "copy"];
const SET_FIELDS = ["setId", "seq", "name", "day", "committedAt", "committed", "refused", "status"];
const ARCHIVE_FIELDS = ["completedAtMs", "completedAt", "completedBy", "setId", "runId", "sheetIds", "status", "shipments"];
const RECEIPT_FIELDS = ["created_timestamp", "updated_timestamp", "status", "is_shipped", "is_paid", "raw.shipments"];
const METAL_CODE = { gold: "GF", silver: "SS", rose: "RG", gold10k: "10K", gold14k: "14K" };
// the Team's workflow stamps, each its own event (a note) rather than a message
const TEAM_STAMPS = [[/^designed\s*:?\s*\)?$/i, "DESIGNED :)"], [/^qa\s*-?\s*1$/i, "QA1"], [/^qa\s*-?\s*2$/i, "QA2"], [/^pe$/i, "PE"]];

/** ms from whatever a record keeps: a Firestore timestamp, a Date, {seconds}, Etsy's seconds or ms. */
function msOf(v) {
  if (v == null || v === "") return 0;
  if (typeof v.toMillis === "function") return v.toMillis();
  if (v instanceof Date) return v.getTime();
  if (typeof v === "object") { const x = v.seconds != null ? v.seconds : v._seconds; return Number.isFinite(+x) ? +x * 1000 : 0; }
  const x = +v; return !Number.isFinite(x) || x <= 0 ? 0 : x < 1e11 ? Math.round(x * 1000) : Math.round(x);
}
const sheetLabel = d => `${METAL_CODE[d.metal] || s(d.metalLabel, 20) || ""}${METAL_CODE[d.metal] || d.metalLabel ? " " : ""}Sheet ${n(d.sheetIndex) || n(d.page) || 1}`;
const sizeOf = st => (st && n(st.wPt) > 0 && n(st.hPt) > 0 ? `${Math.round(n(st.wPt) / 72 * 25.4)}×${Math.round(n(st.hPt) / 72 * 25.4)} mm` : "");
const withTimeout = (p, ms) => new Promise(resolve => {
  const t = setTimeout(() => resolve({ value: null, timedOut: true }), ms);
  p.then(value => { clearTimeout(t); resolve({ value }); }, e => { clearTimeout(t); resolve({ value: { events: [], sheets: null, errors: ["derive: " + ((e && e.message) || e)] } }); });
});

/** Derived events as they are answered: cleaned as a recorded one is, marked derived, with a stable id
    (orderId~type~d-…, never one a writer uses). `series` (dropped before answering) keeps one record's own list, such as
    a custom line's seal per press, from being folded together. */
function finalize(id, list) {
  return list.map(e => {
    const c = clean(e, { source: e.source }); if (!c) return null;
    const x = c.doc; x.id = `${id}~${e.type}~${s(e.id, 120).replace(/[^\w.:-]/g, "_")}`.slice(0, 400); x.derived = true;
    if (e.series) x.series = e.series;
    return x;
  }).filter(Boolean);
}
/** The events of a cancel record (get() reads it anyway). */
function cancelEventsOf(id, c) {
  const at = n(c.at) || msOf(c.createdAt); if (!at) return [];
  const etsy = c.by === "Etsy" || c.source === "etsy", seenAt = msOf(c.createdAt);
  const out = [{ orderId: id, type: etsy ? "etsyCancelled" : "cancelled", at, by: s(c.by, 80) || (etsy ? "Etsy" : ""), source: etsy ? "etsy" : "sorter", id: "d-cancel",
    text: `${etsy ? "Cancelled on Etsy" : "Cancelled by " + (c.by || "someone")}${c.why ? ": " + c.why : ""}`, data: Object.assign({ why: s(c.why, 400), sheets: Array.isArray(c.sheets) ? c.sheets.slice(0, 30) : [] }, etsy && seenAt ? { seenAt } : {}) }];
  // what the cancel took off and from where, as the record keeps it (its removals, _orderCancel.noteRemovals; an older
  // record: its fates), each a step (Paul, 29 Sep 00:26). A step recorded on the timeline, or the pieces' own removal
  // from that place, says the same and wins (dedupe, sameEvent); a station's "seen" is its own cancelAlert event.
  const rems = Array.isArray(c.removals) && c.removals.length ? c.removals.filter(r => r && r.outcome !== "seen")
    : (Array.isArray(c.fates) ? c.fates : []).map(f => f && { id: `sheet~${f.sheet}`, where: f.sheet, kind: "sheet", fate: f.fate });
  for (const r of rems.slice(0, 60)) {
    const where = s(r && (r.where || r.sheet), 80); if (!where) continue;
    const st = cancelStepOf(Object.assign({}, r, { sheet: where }));
    out.push({ orderId: id, type: "cancelStep", at: n(r.at) || at, by: s(r.by, 80), source: "sorter", station: STATIONS.has(r.station) ? r.station : "sorter", sheet: where, id: `d-rm-${s(r.id, 100) || where}`, text: st.text,
      data: { outcome: st.outcome, done: st.done, sheet: where, kind: s(r.kind, 20), approx: !n(r.at) } });
  }
  return out;
}
/* A cancel's step on one sheet or place (type cancelStep; Paul, 29 Sep 00:26: "show that it was removed successfully from
   a given sheet or process"). From a fate (removed | cut | open, and setAside | failed) or a record's removal (outcome
   removed | setAside | waiting | failed, kind sheet | pool | queue …) → { outcome, done, text }: removed (taken off),
   setAside (on a cut sheet: set aside; `done` once a person said so), waiting (still on it: not taken off yet), failed.
   Its id is the cancel's own time and the place (stepId), so a retry, the next check or a person's "Set aside" lands on
   the same step and says how it stands now. */
const FATE_TO = { removed: "removed", cut: "cut", open: "waiting", setAside: "setAside", failed: "failed" };
function cancelStepOf(f) {
  const where = s(f && (f.sheet || f.where), 80) || "its sheet", by = s(f && f.by, 80), queue = (f && f.kind) === "queue" || /^the queue$/i.test(where);
  // (a removal on the record says setAside for a cut sheet's instruction too, its fate "cut": it is done only once a
  //  person said so, "set aside by …", the Set aside press)
  const o = f && f.fate ? FATE_TO[f.fate] || "waiting" : f && f.outcome === "setAside" ? (/\bset aside by\b/i.test(String(f.text || "")) ? "setAside" : "cut") : (f && f.outcome) || "waiting";
  if (o === "removed") return { outcome: "removed", done: true, text: queue ? "Taken out of the queue" : `Removed from ${where}` };
  if (o === "setAside") return { outcome: "setAside", done: true, text: `On a cut sheet: set aside${by ? " by " + by : ""} (${where})` };
  if (o === "cut") return { outcome: "setAside", done: false, text: `On a cut sheet: set aside (${where})` };
  if (o === "failed") return { outcome: "failed", done: false, text: `Not taken off ${where}: ${s(f && f.text, 120) || "it failed"}` };
  return { outcome: "waiting", done: false, text: `Still on ${where}: not taken off yet` };
}
const stepId = (cancelAt, sheet) => `cx-${Math.round(n(cancelAt))}-${s(sheet, 80)}`;

/* ── Complete Order and Reopen (Paul, 29 Sep 02:08: "The timeline also doesn't have the seals or the points for when an
   order was manually completed by pressing the completed button in the Review tab") ──
   Each press is its own point: a Complete Order press is a sealCompleted (charmNestLibrary customPut, data.how
   "button"), a Reopen or an Undo a note with data.reopened (customReopen). The custom record keeps the same presses
   (its stamps, its completion and its history), so an order completed before its press was recorded still has them.
   The record and the event recorded as it happened share one clock reading: the same press is the same moment
   exactly, and two presses a minute apart stay two points. data.pressedIn: where it was pressed (older ones: none). */
const pressOf = e => e.type === "sealCompleted" && !(e.data && e.data.how === "print");
const reopenOf = e => e.type === "note" && !!(e.data && e.data.reopened);
/** Whether two events are the same Complete press or the same Reopen; null when neither is one. */
function sameOperatorStep(a, b) {
  const k = x => (pressOf(x) ? "complete" : reopenOf(x) ? "reopen" : "");
  if (!k(a) && !k(b)) return null;
  return k(a) === k(b) && Math.abs(n(a.at) - n(b.at)) <= 1000 && !(a.lineKey && b.lineKey && a.lineKey !== b.lineKey);
}
// (a custom line's history: its reopens and undos)
CUSTOM_FIELDS.push("history");
/** A custom line's reopens and undos, as its record's history keeps them, each a point of its own. */
function customReopensOf(id, c) {
  const key = s(c.key || c._id, 80), tid = s(c.transactionId, 30) || key.split("_").pop(), what = s(c.sku || c.title || "custom line", 80);
  return (Array.isArray(c.history) ? c.history : []).slice(-24).filter(h => h && msOf(h.at) > 1e12).map(h => {
    const how = h.how === "undo" ? "undo" : "reopen", by = s(h.by, 80);
    return { orderId: id, type: "note", at: msOf(h.at), id: `d-reopen-${key}-${msOf(h.at)}`, series: `reopen-${key}`, lineKey: key, transactionId: tid, by, source: "sorter", station: "sorter",
      text: `Custom order ${how === "undo" ? "completion undone" : "reopened"}${by ? " by " + by : ""}: ${what} · back to Open`, data: Object.assign({ reopened: how, sku: s(c.sku, 60) }, h.from ? { pressedIn: s(h.from, 40) } : {}) };
  });
}

/** Every event the order's existing records tell, each marked derived with a stable id. */
async function deriveEvents(db, id, opts) {
  const P = opts.prefix || "", sandbox = !!P, SB = opts.sandboxed instanceof Set ? opts.sandboxed : SANDBOXED_DEFAULT;
  const col = name => db.collection((SB.has(name) || STATION_SANDBOXED.has(name) ? P : "") + name);
  const forms = [id].concat(Number.isSafeInteger(+id) ? [+id] : []), errors = [];
  const safe = (what, fn) => Promise.resolve().then(fn).catch(e => { errors.push(`${what}: ${s((e && e.message) || e, 160)}`); return null; });
  const one = (ref, fieldMask) => db.getAll(ref, { fieldMask }).then(r => r[0]);
  const docs = snap => (snap ? snap.docs.map(d => Object.assign({ _id: d.id }, d.data())) : []);
  const got = d => (d && d.exists ? Object.assign({ _id: d.id }, d.data()) : null);

  // ── round 1: everything keyed by the order itself ──
  const [arrS, poolS, sheetS, customS, readS, msgS, doneS, archS, rcS] = await Promise.all([
    safe("arrivals", () => col("Charm_Nest_Arrivals").doc(id).get()),
    safe("pool", () => col("Charm_Pool").where("orderId", "in", forms).limit(CAP.pools).select(...POOL_FIELDS).get()),
    safe("sheets", () => col("Charm_Nest_Sheets").where("orders", "array-contains-any", forms).limit(CAP.sheets).select(...SHEET_FIELDS).get()),
    safe("custom", () => col("Charm_Custom_Orders").where("receiptId", "in", forms).limit(CAP.custom).select(...CUSTOM_FIELDS).get()),
    safe("customRead", () => db.collection("Charm_Nest_CustomRead").where("order", "==", id).limit(CAP.reads).get()),
    safe("messages", () => col("Brites_Orders").doc(id).collection("messages").orderBy("timestamp", "desc").limit(CAP.messages).get()),
    safe("designCompleted", () => col("Design_Completed Orders").doc(id).get()),
    safe("archive", () => one(col("Design_Order_Archive").doc(id), ARCHIVE_FIELDS)),
    sandbox ? null : safe("receipt", () => one(db.collection("EtsyMail_Receipts").doc(id), RECEIPT_FIELDS))
  ]);
  const arrival = got(arrS), done = got(doneS), arch = got(archS), rc = got(rcS);
  const pools = docs(poolS).map(p => Object.assign(p, { poolId: s(p.poolId || p._id, 120) }));
  const sheets = docs(sheetS).filter(d => !d.archived && (d.orders || []).some(v => String(v) === id)).slice(0, CAP.sheets);
  const customs = docs(customS), reads = docs(readS), msgs = docs(msgS);

  // ── round 2: what those name (the backs of its pieces, its sets, its Rose Gold plans and cuts, the custom readings of
  //    lines the first round found but the reading query did not) ──
  const poolIds = [...new Set(pools.map(p => p.poolId))].filter(Boolean).slice(0, CAP.backs);
  const setIds = [...new Set([...sheets.map(d => d.setId), ...pools.map(p => p.setId), arch && arch.setId].filter(v => v && typeof v === "string"))].slice(0, CAP.sets);
  const rose = sheets.filter(d => d.metal === "rose" && (d.rosePlanHash || d.roseCutAt)).slice(0, CAP.rose);
  const cut = rose.filter(d => d.roseCutAt && d.roseStockId && typeof d.roseStockId === "string");
  const readKeys = new Set(reads.map(r => r._id));
  const lineKeys = [...new Set([...pools.map(p => p.lineKey || (p.transactionId ? `${id}_${p.transactionId}` : "")), ...customs.map(c => c.key || c._id)].filter(k => k && /^[\w-]{3,120}$/.test(k) && !readKeys.has(k)))].slice(0, 20);
  const [backS, setS, planS, cutS, moreReadS] = await Promise.all([
    poolIds.length ? safe("backs", () => db.getAll(...poolIds.map(p => col("Charm_Pool_Back").doc(p)), { fieldMask: BACK_FIELDS })) : null,
    setIds.length ? safe("sets", () => db.getAll(...setIds.map(x => col("Charm_Nest_Sets").doc(x)), { fieldMask: SET_FIELDS.concat(["orders." + id]) })) : null,
    rose.length ? safe("rosePlans", () => db.getAll(...rose.map(d => col("Charm_Nest_Sheets").doc(d._id)), { fieldMask: ["rosePlanJson"] })) : null,
    cut.length ? safe("roseCuts", () => db.getAll(...cut.map(d => col("Charm_Nest_Rose_Stock").doc(d.roseStockId).collection("cuts").doc(d._id)), { fieldMask: ["at", "by"] })) : null,
    lineKeys.length ? safe("customRead", () => db.getAll(...lineKeys.map(k => db.collection("Charm_Nest_CustomRead").doc(k)))) : null
  ]);
  const backs = (backS || []).map(got).filter(b => b && !b.invalidated);
  const sets = new Map((setS || []).map(got).filter(Boolean).map(x => [x._id, x]));
  const plans = new Map((planS || []).map(got).filter(Boolean).map(x => [x._id, x.rosePlanJson]));
  const cuts = new Map((cutS || []).map((d, i) => [cut[i]._id, got(d)]).filter(([, v]) => v));
  for (const r of (moreReadS || []).map(got).filter(Boolean)) reads.push(r);

  // ── the events, most trusted source first (a later one that says the same thing is dropped by dedupe) ──
  const out = [];
  const ev = (type, at, f) => { if (!(at > 1e12)) return; out.push(Object.assign({ orderId: id, type, at: Math.round(at) }, f)); };
  const sheetById = new Map(sheets.map(d => [d._id, d]));
  const labelOf = sid => { const d = sheetById.get(sid); return d ? sheetLabel(d) : ""; };
  const setName = x => (x ? s(x.name, 40) || (x.seq ? `Set ${x.seq}` : "") : "");

  // Etsy: when the order was placed, shipped, completed or cancelled
  const placedAt = rc && msOf(rc.created_timestamp) || arrival && msOf(arrival.createTs) || arch && arch.status && msOf(arch.status.createdTs) || pools.map(p => msOf(p.orderDate)).find(Boolean) || 0;
  ev("arrived", placedAt, { id: "d-etsy-placed", by: "Etsy", source: "etsy", text: "Order placed on Etsy", milestone: true, data: rc ? { etsyStatus: s(rc.status, 40) } : null });
  // (seenAt: the real clock's moment, which the sandbox's ledger keeps beside the stream's simulated firstSeenAt)
  if (arrival) ev("arrived", msOf(arrival.seenAt) || msOf(arrival.firstSeenAt), { id: "d-first-seen", by: "System", source: "sorter", station: "sorter", text: "First seen by the sorter", milestone: false, data: msOf(arrival.seenAt) ? { clock: "real", firstSeenAt: msOf(arrival.firstSeenAt) } : null });
  const ships = rc ? (rc.raw && Array.isArray(rc.raw.shipments) ? rc.raw.shipments : []).map(x => ({ at: msOf(x.shipment_notification_timestamp || x.notification_date), carrier: s(x.carrier_name, 40), tracking: s(x.tracking_code, 60) }))
    : arch && Array.isArray(arch.shipments) ? arch.shipments.map(x => ({ at: msOf(x.notificationTs), carrier: s(x.carrier, 40), tracking: s(x.trackingCode, 60) })) : [];
  ships.filter(x => x.at).slice(0, 5).forEach((x, i) => ev("shipped", x.at, { id: `d-etsy-shipped-${i}`, by: "Etsy", source: "etsy", station: "shipping", text: `Shipped${x.carrier ? " with " + x.carrier : ""}${x.tracking ? " · " + x.tracking : ""}`, data: { carrier: x.carrier, tracking: x.tracking } }));
  if (rc) {
    const status = String(rc.status || "").toLowerCase(), upd = msOf(rc.updated_timestamp), firstShip = ships.map(x => x.at).filter(Boolean).sort((a, b) => a - b)[0] || 0;
    if (rc.is_shipped && !firstShip) ev("shipped", upd, { id: "d-etsy-shipped", by: "Etsy", source: "etsy", station: "shipping", text: "Marked shipped on Etsy", data: { approx: true } });
    if (status === "completed") ev("etsyCompleted", firstShip || upd, { id: "d-etsy-completed", by: "Etsy", source: "etsy", text: "Completed on Etsy", data: firstShip ? null : { approx: true } });
    // (the cancel rule of _orderCancel.isCancelled: refunded after shipping is a return, not a cancel)
    if (/cancel/.test(status) || (/fully\s*refund/.test(status) && !(rc.is_shipped || (rc.raw && rc.raw.is_shipped)))) ev("etsyCancelled", upd, { id: "d-etsy-cancelled", by: "Etsy", source: "etsy", text: `Etsy says: ${s(rc.status, 40)}`, data: { etsyStatus: s(rc.status, 40), approx: true } });
  }

  // custom readings and a person's decision
  for (const r of reads) {
    const key = s(r._id, 80), tid = key.includes("_") ? key.split("_").pop() : "";
    const latest = r.reads && typeof r.reads === "object" ? (r.latest && r.reads[r.latest]) || Object.values(r.reads).sort((a, b) => n(b && b.at) - n(a && a.at))[0] : null;
    if (latest) ev("customRead", msOf(latest.at), { id: `d-read-${key}`, lineKey: key, transactionId: tid, by: "System", source: "sorter", station: "sorter", text: `Read as ${latest.kind || "?"}${latest.confidence != null ? ` (${Math.round(n(latest.confidence) * 100)}%)` : ""}${latest.summary ? ": " + latest.summary : ""}`, data: { kind: s(latest.kind, 20), confidence: n(latest.confidence), relatedOrder: latest.relatedOrder || null } });
    const dec = r[sandbox ? "decidedSandbox" : "decided"];
    if (dec && dec.kind) ev("customDecided", msOf(dec.at), { id: `d-decided-${key}`, lineKey: key, transactionId: tid, by: s(dec.by, 80), source: "sorter", station: "sorter", text: `Decided: ${dec.kind}`, data: { kind: s(dec.kind, 20) } });
  }

  // the pieces: in the pool (one event per line), taken off, moved
  const lineOf = p => s(p.lineKey, 80) || (p.transactionId ? `${id}_${p.transactionId}` : "");
  const lines = new Map();
  for (const p of pools) { const k = lineOf(p) || p.poolId; const l = lines.get(k) || lines.set(k, { rows: [] }).get(k); l.rows.push(p); }
  for (const [k, l] of lines) {
    const at = Math.min(...l.rows.map(p => msOf(p.createdAt)).filter(Boolean)), p0 = l.rows[0];
    if (Number.isFinite(at)) ev("pooled", at, { id: `d-pooled-${k}`, lineKey: lineOf(p0), transactionId: s(p0.transactionId, 30), by: "System", source: "sorter", station: "sorter", text: `Ready to nest: ${p0.sku || "charm"}${p0.material ? " · " + p0.material : ""}${l.rows.length > 1 ? ` · ${l.rows.length} pieces` : ""}`, data: { sku: s(p0.sku, 60), material: s(p0.material, 20), pieces: l.rows.length, runId: s(p0.runId, 80) } });
  }
  const groups = (rows, keyOf) => { const m = new Map(); for (const p of rows) { const k = keyOf(p); if (k) (m.get(k) || m.set(k, []).get(k)).push(p); } return m; };
  for (const [k, rows] of groups(pools.filter(p => msOf(p.removedAt)), p => `${msOf(p.removedAt)}|${p.removedBy || ""}|${p.removedReason || ""}`)) {
    const p0 = rows[0], why = s(p0.removedReason, 160);
    ev("removed", msOf(p0.removedAt), { id: `d-removed-${msOf(p0.removedAt)}`, by: s(p0.removedBy, 80), source: "sorter", station: "sorter", lineKey: rows.every(p => lineOf(p) === lineOf(p0)) ? lineOf(p0) : "", text: `Taken off the sheet${why ? ": " + why : ""}`, data: { reason: why, poolIds: rows.map(p => p.poolId).slice(0, 40), key: k.slice(0, 200) } });
  }
  for (const [, rows] of groups(pools.filter(p => msOf(p.movedAt)), p => `${msOf(p.movedAt)}|${p.movedTo || ""}`)) {
    const p0 = rows[0], to = s(p0.movedTo, 100);
    ev("moved", msOf(p0.movedAt), { id: `d-moved-${msOf(p0.movedAt)}-${to}`, by: s(p0.movedBy, 80), source: "sorter", station: "sorter", sheetId: to, sheet: labelOf(to) || s(p0.sheetName, 80), text: `Moved to ${labelOf(to) || p0.sheetName || "another sheet"}`, data: { from: s(p0.movedFrom, 300), to, poolIds: rows.map(p => p.poolId).slice(0, 40) } });
  }

  // back engraving approved (the back's record; the piece's own mark when there is no record, at its last change)
  const backed = new Set();
  for (const [, rows] of groups(backs, b => `${msOf(b.approvedAt)}|${b.approvedBy || ""}|${b.sheetId || ""}`)) {
    const b0 = rows[0]; rows.forEach(b => backed.add(b.poolId || b._id));
    ev("engraveApproved", msOf(b0.approvedAt), { id: `d-back-${msOf(b0.approvedAt)}-${b0.sheetId || ""}`, by: s(b0.approvedBy, 80), source: "sorter", station: "sorter", sheetId: s(b0.sheetId, 100), sheet: labelOf(b0.sheetId), setId: s(b0.setId, 100), transactionId: s(b0.transactionId, 30), text: `Back engraving approved${b0.text ? ": “" + s(b0.text, 120).replace(/\s*\n\s*/g, " / ") + "”" : ""}`, data: { text: s(b0.text, 300), pieces: rows.length } });
  }
  for (const p of pools) if (p.engraveApprovedBy && !backed.has(p.poolId)) { backed.add(p.poolId); ev("engraveApproved", msOf(p.updatedAt), { id: `d-back-${p.poolId}`, by: s(p.engraveApprovedBy, 80), source: "sorter", station: "sorter", lineKey: lineOf(p), text: "Back engraving approved", data: { approx: true } }); }

  // the sheets that hold it: placed, its QR label, Rose Gold lines and cut, laser cut
  const poolOf = new Map(pools.map(p => [p.poolId, p]));
  for (const d of sheets) {
    const sid = d._id, label = sheetLabel(d), set = sets.get(d.setId), mine = (d.poolIds || []).map(x => poolOf.get(x)).filter(Boolean);
    const onIt = mine.length ? mine : pools.filter(p => p.sheetId === sid);
    // when it went on: not before the sheet, its pieces, their last removal, or their move here (no stamp says more)
    const placedOn = Math.max(msOf(d.createdAt) || msOf(d.cardStartedAt), ...onIt.map(p => msOf(p.createdAt)), ...onIt.map(p => msOf(p.removedAt) + 1), ...onIt.filter(p => p.movedTo === sid).map(p => msOf(p.movedAt)), 0);
    const base = { sheetId: sid, sheet: label, setId: s(d.setId, 100), source: "sorter", station: "sorter" };
    ev("placed", placedOn, Object.assign({ id: `d-placed-${sid}`, by: "System", text: `On ${label}${setName(set) ? " · " + setName(set) : d.setSeq ? " · Set " + d.setSeq : ""}${sizeOf(d.stock) ? " · " + sizeOf(d.stock) : ""}`, data: { metal: s(d.metal, 20), metalLabel: s(d.metalLabel, 40), size: sizeOf(d.stock), pieces: onIt.length, fileBase: s(d.fileBase, 100), approx: true } }, base));
    const files = d.label && Array.isArray(d.label.files) ? d.label.files.filter(f => f && (!Array.isArray(f.orders) || f.orders.some(v => String(v) === id))) : [];
    if (files.length) {
      // no stamp says when a label was made: after it went on, and no later than what followed it
      const later = [set && msOf(set.committedAt), n(d.laserDoneAt), msOf(d.updatedAt)].filter(t => t > placedOn);
      ev("qrLabel", later.length ? Math.min(...later) : placedOn, Object.assign({ id: `d-label-${sid}`, by: "System", text: `QR label for ${label}${files.length > 1 ? ` (${files.length} parts)` : ""}`, data: { parts: files.length, approx: true } }, base));
    }
    const plan = plans.get(sid);
    if (plan) {
      let stages = []; try { const j = JSON.parse(plan); stages = Array.isArray(j && j.stages) ? j.stages : []; } catch (_) { stages = []; }
      const mineIds = new Set(onIt.map(p => p.poolId));
      const hits = stages.filter(st => Array.isArray(st.ids) && st.ids.some(x => mineIds.has(String(x)) || mineIds.has(String(x).split(":").pop())));
      (hits.length ? hits : stages.length === 1 ? stages : []).forEach(st => ev("roseLine", n(st.at), Object.assign({ id: `d-roseline-${sid}-${n(st.n)}`, by: "System", text: `RG green line ${n(st.n) || ""} on ${label}`.replace("  ", " "), data: { n: n(st.n), lines: Array.isArray(st.lines) ? st.lines.slice(0, 2) : null } }, base)));
    }
    if (n(d.roseCutAt)) { const c = cuts.get(sid); ev("roseCut", n(d.roseCutAt), Object.assign({}, base, { id: `d-rosecut-${sid}`, by: c ? s(c.by, 80) : "", station: "laser", text: `Rose Gold ${label} cut` })); }
    if (n(d.laserDoneAt)) ev("laserDone", n(d.laserDoneAt), Object.assign({}, base, { id: `d-laser-${sid}`, by: s(d.laserDoneBy, 80), station: "laser", text: `Cut on the laser: ${label}` }));
  }

  // the set committed (the design is complete); then the design archive's record of it; then the pieces' mark (the
  // design station's plain completion, with no set, is written after the Team's thread below: its "DESIGNED :)" wins)
  for (const [sid, x] of sets) {
    const mine = x["orders." + id] || (x.orders && x.orders[id]) || null, committed = Array.isArray(x.committed) && x.committed.some(v => String(v) === id);
    const refused = Array.isArray(x.refused) && x.refused.some(r => r && String(r.id) === id);
    if (msOf(x.committedAt) && (committed || (!Array.isArray(x.committed) && mine && !mine.held)) && !refused)
      ev("setCommitted", msOf(x.committedAt), { id: `d-set-${sid}`, setId: sid, by: arch && arch.setId === sid ? s(arch.completedBy, 80) : "", source: "sorter", station: "sorter", text: `${setName(x) || "Set"} committed: design complete`, data: { set: setName(x) } });
  }
  const archAt = arch ? msOf(arch.completedAtMs) || msOf(arch.completedAt) : 0;
  if (archAt && arch.setId) ev("setCommitted", archAt, { id: `d-archive-${s(arch.setId, 80)}`, setId: s(arch.setId, 100), by: s(arch.completedBy, 80), source: "station", station: "design", text: `${setName(sets.get(arch.setId)) || "Set"} committed: design complete`, data: { sheets: Array.isArray(arch.sheetIds) ? arch.sheetIds.slice(0, 20) : [] } });
  for (const [, rows] of groups(pools.filter(p => msOf(p.committedAt)), p => `${msOf(p.committedAt)}`)) ev("setCommitted", msOf(rows[0].committedAt), { id: `d-pool-commit-${msOf(rows[0].committedAt)}`, setId: s(rows[0].setId, 100), by: "", source: "sorter", station: "sorter", text: "Set committed: design complete", data: { approx: true } });

  // custom orders: each seal (a printed QR label or a Complete Order press), and the completion
  for (const c of customs) {
    const key = s(c.key || c._id, 80), tid = s(c.transactionId, 30) || key.split("_").pop(), what = c.sku || c.title || "custom line";
    const stamps = Array.isArray(c.stamps) && c.stamps.length ? c.stamps : [c.printedAt && { how: "print", at: c.printedAt, by: c.printedBy }, n(c.prints) > 1 && c.lastPrintedAt && { how: "print", at: c.lastPrintedAt, by: c.lastPrintedBy }].filter(Boolean);
    stamps.slice(-24).forEach(st => ev(st.how === "button" ? "sealCompleted" : "sealPrinted", msOf(st.at), { id: `d-seal-${key}-${msOf(st.at)}`, series: `seal-${key}`, lineKey: key, transactionId: tid, by: s(st.by, 80), source: "sorter", station: "sorter", text: st.how === "button" ? `Custom order completed: ${s(what, 80)}` : `Custom QR label printed: ${s(what, 80)}`, data: { how: st.how === "button" ? "button" : "print" } }));
    if (msOf(c.completedAt)) ev("sealCompleted", msOf(c.completedAt), { id: `d-seal-done-${key}`, lineKey: key, transactionId: tid, by: s(c.completedBy, 80), source: "sorter", station: "sorter", text: `Custom order completed: ${s(what, 80)}`, data: { how: c.how === "button" ? "button" : "print" } });
  }
  // each Reopen and Undo, from the record's history (the Complete presses are its stamps, above)
  for (const c of customs) for (const x of customReopensOf(id, c)) ev(x.type, x.at, x);

  // the Team's thread: its workflow stamps as their own events, the rest as messages
  for (const m of msgs) {
    const text = String(m.text || "").trim(), at = msOf(m.timestamp) || msOf(m.at), by = s(m.senderName || "Staff", 80);
    if (!text && !m.imageUrl) continue;
    const stamp = (TEAM_STAMPS.find(([re]) => re.test(text)) || [])[1];
    if (stamp) ev("note", at, { id: `d-msg-${m._id}`, by, source: "station", station: stamp === "DESIGNED :)" ? "design" : "", milestone: stamp === "DESIGNED :)" || undefined, text: stamp, data: { stamp } });
    else ev("teamMessage", at, { id: `d-msg-${m._id}`, by, source: "station", text: s(text || "(picture)", 200), data: Object.assign({ role: s(m.senderRole, 20) }, m.imageUrl ? { imageUrl: s(m.imageUrl, 600) } : {}, text.length > 200 ? { full: s(text, 1500) } : {}) });
  }
  // the design station's completion with no set (an order designed there by hand): the same moment as a set's commit
  // or the "DESIGNED :)" stamp, when there is one (sameEvent)
  const designed = { id: "d-design-complete", source: "station", station: "design", milestone: true, text: "Design complete", data: { stamp: "designComplete" } };
  if (archAt && !arch.setId) ev("note", archAt, Object.assign({ by: s(arch.completedBy, 80) }, designed));
  if (done && msOf(done.completedAt)) ev("note", msOf(done.completedAt), Object.assign({ by: "" }, designed));

  return { events: finalize(id, out), sheets: sheetS ? sheets.map(d => ({ sheetId: d._id, sheet: sheetLabel(d), setId: s(d.setId, 100), cut: n(d.laserDoneAt) > 0 || n(d.roseCutAt) > 0 })) : null, errors };
}

/** Whether two events say the same thing: the same type, within ±3 minutes (at any time for a derived event whose time
    is only a bound, `approx`), on the same sheet and line when both name one, and the same words for a note or message. */
function sameEvent(a, b) {
  // the design station's plain completion is the moment a set was committed, or the "DESIGNED :)" stamp
  const plain = x => x.type === "note" && !!x.data && x.data.stamp === "designComplete";
  if (plain(a) || plain(b)) {
    const o = plain(a) ? b : a;
    return (o.type === "setCommitted" || plain(o) || (o.type === "note" && !!o.data && o.data.stamp === "DESIGNED :)")) && Math.abs(a.at - b.at) <= DEDUPE_MS;
  }
  // a cancel's steps: one per place, whenever it was said (a set aside hours later is the same step); the pieces' own
  // removal from a place is that place's step taken off (and one that names no place, a cancel's, is the record's)
  const place = x => String(x.sheet || "").trim().toLowerCase(), step = a.type === "cancelStep" ? a : b.type === "cancelStep" ? b : null;
  if (step) {
    const o = step === a ? b : a;
    if (o.type === "cancelStep") return place(a) === place(b);
    if (o.type !== "removed" || !(step.data && step.data.outcome === "removed")) return false;
    return place(o) ? place(o) === place(step) : /^cancel/i.test(String((o.data && o.data.reason) || ""));
  }
  const op = sameOperatorStep(a, b); if (op !== null) return op;
  if (a.type !== b.type) return false;
  if (a.type === "sealPrinted") return samePrint(a, b);
  const approx = (a.data && a.data.approx) || (b.data && b.data.approx);
  if (!approx && Math.abs(a.at - b.at) > DEDUPE_MS) return false;
  if (a.sheetId && b.sheetId && a.sheetId !== b.sheetId) return false;
  if (a.lineKey && b.lineKey && a.lineKey !== b.lineKey) return false;
  if (a.type === "note" || a.type === "teamMessage" || a.type === "customerMessage") {
    const t = x => String((x.data && x.data.stamp) || x.text || "").trim().toLowerCase();
    return t(a) === t(b) || (!!(a.data && a.data.stamp) && a.data.stamp === (b.data && b.data.stamp));
  }
  return true;
}
/* A label printed (Paul, 29 Sep 02:08: every print is a seal on the timeline, a reprint a seal of its own): customPut
   stamps the print on the timeline and in its record's stamps with the same moment, so the record's seal (derived) is
   the recorded one only at that moment (a second's leeway); a second print minutes later is a print of its own. */
function samePrint(a, b) {
  return Math.abs(n(a.at) - n(b.at)) <= 1000 && !(a.lineKey && b.lineKey && a.lineKey !== b.lineKey);
}
/** The derived events that say something no recorded event (nor an earlier derived one) already says. */
function dedupe(recorded, derived) {
  const kept = [];
  for (const e of derived) if (!recorded.some(r => sameEvent(r, e)) && !kept.some(k => !(k.series && k.series === e.series) && sameEvent(k, e))) kept.push(e);
  return kept;
}

/* ── chronology (Paul, 28 Sep 21:18, points 3 and 4): the order's arrival ("Order in") is the anchor, it is there once,
   and nothing is drawn before it. Run on the whole answer (recorded + derived), after dedupe.
   Production: the anchor is the Etsy order's own time (the mirror's created_timestamp, else the one the arrivals ledger
   kept from Etsy's list); the sorter's first sight of it is folded into it (data.firstSeenAt). An event stamped before
   it (a browser's clock a little behind Etsy's) is drawn at it, its own time kept (data.recordedAt, approx).
   Sandbox: the stream replays real Etsy orders on a simulated clock (the replayed order's Etsy time and the ledger's
   firstSeenAt are the stream's), while everything else is stamped on the real clock: the two cannot be compared. Its
   anchor is the real-clock moment the sandbox first saw the order (an arrival marked data.clock "real"). An older
   arrival kept only the simulated time, whose real moment cannot be known: it is drawn no later than the first thing
   that happened to the order (data.simAt keeps what the stream said). A custom reading and its decision live in
   Charm_Nest_CustomRead, which both workspaces share and a sandbox reset keeps, so their times can be a reading made
   before the sandbox replayed the order: they never move the anchor, and like anything else before it, they are drawn
   at it. Nothing is dropped and no time is made up: a time moves only to the anchor, and keeps its own beside it. ── */
const SHARED_CLOCK = new Set(["customRead", "customDecided"]);
const ARRIVED_FIRST = (a, b) => (a.type === "arrived" ? -1 : b.type === "arrived" ? 1 : 0);
const recAt = e => (e.data && n(e.data.recordedAt)) || n(e.at);
/** Oldest first; at the same moment the arrival leads, then what was moved to it, in its own order. */
const byTime = (a, b) => n(a.at) - n(b.at) || ARRIVED_FIRST(a, b) || recAt(a) - recAt(b) || String(a.id).localeCompare(String(b.id));
function chronology(events, opts = {}) {
  const list = (events || []).filter(Boolean), arr = list.filter(e => e.type === "arrived" && n(e.at) > 0);
  if (!arr.length) return list;   // no arrival known: nothing to anchor to, and none is made up
  const sandbox = !!opts.sandbox, early = (a, b) => n(a.at) - n(b.at);
  const real = e => !!(e.data && e.data.clock === "real"), etsy = e => e.source === "etsy" || e.by === "Etsy";
  const pref = sandbox ? arr.filter(real) : arr.filter(etsy);
  const anchor = (pref.length ? pref : arr).slice().sort(early)[0];
  const rest = list.filter(e => e.type !== "arrived");
  const data = Object.assign({}, anchor.data || {});
  // the other arrivals say the same thing (the order came in): folded into the one, their times kept
  for (const e of arr) {
    if (e === anchor) continue;
    const sim = sandbox && !real(e), k = sim ? (etsy(e) ? "simEtsyAt" : "simFirstSeenAt") : etsy(e) ? "etsyAt" : "firstSeenAt";
    if (!n(data[k])) data[k] = n(e.at);
  }
  let at = n(anchor.at);
  if (sandbox && !real(anchor)) {
    const first = rest.reduce((m, e) => (!SHARED_CLOCK.has(e.type) && n(e.at) > 0 ? Math.min(m, n(e.at)) : m), Infinity);
    if (first < at) { data.simAt = at; data.approx = true; at = first; }
  }
  const out = [Object.assign({}, anchor, { at, milestone: true, data })];
  for (const e of rest) out.push(n(e.at) < at ? Object.assign({}, e, { at, data: Object.assign({}, e.data || {}, { recordedAt: n(e.at), approx: true }) }) : e);
  return out;
}

/* ── where the order is now ── */
const RANK = {
  arrived: 0, pulled: 0, interpreted: 0, pooled: 0, decided: 0, skipped: 0, customDecided: 0, designSent: 0, designDropped: 0, released: 0, restored: 0,
  needsDecision: 1, engraveNeeded: 1, held: 1, customRead: 1,
  placed: 2, moved: 2, renested: 2, qrLabel: 2, roseLine: 2, included: 2, merged: 2, sizeChanged: 2, setCommitted: 2, sealPrinted: 2, sealCompleted: 2, recalled: 2,
  laserDone: 3, roseCut: 3, sorted: 4, welded: 5, assembled: 6, packed: 7, labelPrinted: 7, shipped: 8, etsyCompleted: 9
};
const STAGE_OF = ["waiting", "review", "sheet", "cut", "sorted", "welded", "assembled", "packed", "shipped", "completed"];
const STAGE_LABEL = { waiting: "Waiting", review: "In review", held: "On hold", designed: "Design complete", sheet: "On a sheet", cut: "Cut on the laser", sorted: "Sorted", welded: "Welded", assembled: "Assembled", packed: "Packed", shipped: "Shipped", completed: "Completed on Etsy", cancelled: "Cancelled" };
const PEOPLE_OUT = new Set(["", "system", "etsy", "operator", "someone"]);
// the order view's milestone rail (Paul, 28 Sep 21:18; charm-nest-timeline-ui.js STAGES, keep the two alike): the real
// steps a piece goes through. Welded is a stud earring's only: the page leaves it out for an order with no stud (its
// stagesFor), and `step` passes over it. Approving is not a step; Etsy's completion folds into Shipped. Engraved: the
// back engraving approved in Engrave and written into its sheet's back file (backPut's engraveApproved).
const RAIL = ["Order in", "Nested", "Engraved", "Laser cut", "Sorted", "Welded", "Assembled", "Shipped"];
const RAIL_KEYS = ["arrived", "sheet", "engraved", "laser", "sorted", "welded", "assembled", "shipped"];
// the furthest step of the rail an event shows the order has reached
const STEP = { arrived: 0, placed: 1, moved: 1, renested: 1, qrLabel: 1, roseLine: 1, included: 1, merged: 1, sizeChanged: 1, setCommitted: 1, sealCompleted: 1,
  engraveApproved: 2, laserDone: 3, roseCut: 3, sorted: 4, welded: 5, assembled: 6, packed: 6, labelPrinted: 6, shipped: 7, etsyCompleted: 7 };
/* A label printed (Paul, 28 Sep 23:51; charm-nest-timeline-ui.js labelStepOf, keep the two alike): an order's QR label
   printed at the Sorting station or the Design Station (its Review tab) is a detail of the Sorted step, never a step of
   its own (it moved the rail to Assembled); a shipping label belongs to the Shipped step (the order is at Shipping, so
   past Assembled; `shipped` stamps the step). data.label: sheetQR | orderQR | custom | shipping, else by the station. */
const SORT_LABELS = new Set(["sheetQR", "orderQR", "custom"]), SORT_SIDE = new Set(["sorting", "design", "qr", "sorter"]);
const labelStepOf = e => { const k = e && e.data && e.data.label; return k === "shipping" ? "shipped" : SORT_LABELS.has(k) || SORT_SIDE.has(e && e.station) ? "sorted" : "shipped"; };
const sortLabel = e => e.type === "labelPrinted" && labelStepOf(e) === "sorted";
const stepOf = e => (sortLabel(e) ? null : STEP[e.type]);
/** Where the order is now, from its events (oldest first) and its cancel record:
    { stage, label, text, sheet, sheetId, setId, station, device, by, at, since, cut, designed, cancelled, step, rail }
    step: the furthest step of the rail (RAIL, 0-7) the order has reached; a cancelled order stopped there.
    stage: waiting | review | held | designed | sheet | cut | sorted | welded | assembled | packed | shipped | completed | cancelled.
    hint.sheets (from the derivation): the sheets that hold the order now, which outrank a stale removal or placement.
    hint.record: `cancelled` is the cancel record as read (null: there is none). The record says whether the order is
    cancelled, as cancelCheck does: a cancel event with no record left (a restore whose cancelRestored event could not be
    written) stays in the history but does not make it cancelled. */
function whereOf(events, cancelled, hint = {}) {
  const list = (events || []).filter(e => e && TYPES.has(e.type)).slice().sort(byTime);
  let rank = 0, stage = "waiting", since = 0, sheet = "", sheetId = "", setId = "", station = "", device = "", by = "", at = 0, cut = false, designed = false, cancel = null, step = list.length ? 0 : -1, seen = false;
  const enter = (st, e) => { if (st !== stage) since = n(e.at); stage = st; };
  for (const e of list) {
    at = Math.max(at, n(e.at));
    if (stepOf(e) != null) step = Math.max(step, stepOf(e));
    else if (e.type === "note" && e.data && (e.data.stamp === "DESIGNED :)" || e.data.stamp === "designComplete")) step = Math.max(step, 1);
    // (a scan only says where the order was seen: "seen at sorting", never a step)
    if (e.station && e.type !== "arrived") { station = e.station; device = e.device || ""; seen = e.type === "scan"; }
    if (!PEOPLE_OUT.has(String(e.by || "").trim().toLowerCase())) by = e.by;
    if (e.setId) setId = e.setId;
    if (e.type === "cancelled" || e.type === "etsyCancelled") { cancel = e; continue; }
    if (e.type === "cancelRestored") { cancel = null; continue; }
    if (e.type === "note" && e.data && (e.data.stamp === "DESIGNED :)" || e.data.stamp === "designComplete")) {
      designed = true;   // designed at the design station (an order the sorter never had), or its set committed
      if (rank <= 2 && !sheetId) { rank = 2; enter("designed", e); }
      continue;
    }
    if (e.type === "setCommitted" || e.type === "sealCompleted") designed = true;
    if (e.type === "removed") {
      if (rank <= 2 && (!e.sheetId || !sheetId || e.sheetId === sheetId)) { rank = 0; sheet = ""; sheetId = ""; enter(/hold/i.test((e.data && e.data.reason) || e.text || "") ? "held" : "waiting", e); }
      continue;
    }
    const r = sortLabel(e) ? null : RANK[e.type]; if (r == null) continue;
    if (r >= 3) { if (r > rank) { rank = r; enter(STAGE_OF[r], e); } if (r === 3) { cut = true; if (e.sheetId) { sheetId = e.sheetId; sheet = e.sheet || sheet; } } continue; }
    if (rank > 2) continue;   // past the sheet: sorter steps after the cut do not bring it back
    if (r === 2) { rank = 2; if (e.sheetId) { sheetId = e.sheetId; sheet = e.sheet || sheet; } enter(sheetId ? "sheet" : designed ? "designed" : "sheet", e); continue; }
    // on a sheet, a reading or another line's step does not take it off; a hold does
    if (sheetId && e.type !== "held") continue;
    rank = 0; if (e.type === "held") { sheet = ""; sheetId = ""; }
    enter(r === 1 ? (e.type === "held" ? "held" : "review") : "waiting", e);
  }
  // the sheets that hold it now (read from the records) outrank a placement or removal that was not recorded
  const now = Array.isArray(hint.sheets) ? hint.sheets : null;
  if (now && rank <= 2) {
    const cur = now.find(x => x.sheetId === sheetId) || now[now.length - 1];
    if (cur) { step = Math.max(step, cur.cut ? 3 : 1); if (cur.cut) { rank = 3; stage = "cut"; cut = true; } else if (rank < 2 || stage !== "sheet") { rank = 2; stage = "sheet"; } sheetId = cur.sheetId; sheet = cur.sheet; setId = cur.setId || setId; }
    else if (stage === "sheet") { stage = designed ? "designed" : "waiting"; sheet = ""; sheetId = ""; }
  }
  if (stage === "sheet" && !sheetId && designed) stage = "designed";
  const isCancelled = hint.record ? !!cancelled : !!(cancelled || cancel);
  if (isCancelled) {
    const c = cancel || {}; stage = "cancelled"; since = n(c.at) || n(cancelled && cancelled.at) || since;
    // a cancel record with no event of its own: whoever cancelled it is the last to act on it
    if (!cancel && cancelled && n(cancelled.at) >= at) { at = n(cancelled.at); if (!PEOPLE_OUT.has(String(cancelled.by || "").trim().toLowerCase())) by = cancelled.by; }
  }
  const label = stage === "sheet" && sheet ? `On ${sheet}` : STAGE_LABEL[stage] || stage;
  const bits = [label];
  if (isCancelled && sheet) bits.push(`pieces on ${sheet}`);
  if (station && !["sheet", "waiting", "review", "held"].includes(stage)) bits.push(`${seen ? "seen at" : "at"} ${station}${device ? " (" + device + ")" : ""}`);
  if (by) bits.push(`by ${by}`);
  return { stage, label, text: s(bits.join(" · "), 200), sheet, sheetId, setId, station, device, by, at, since, cut, designed, cancelled: isCancelled, step, rail: RAIL };
}
module.exports = { RAIL, RAIL_KEYS, labelStepOf, cancelStepOf, stepId, COL, TYPES, MILESTONES, STATION_TYPES, STATIONS, orderIdOf, clean, add, get, cancelCheck, deriveEvents, dedupe, sameEvent, chronology, byTime, whereOf, msOf, SANDBOXED_DEFAULT, STATION_SANDBOXED };
