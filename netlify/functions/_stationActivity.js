/*  netlify/functions/_stationActivity.js
 *  What each person did at each station (Paul, 2 Oct: "every employee, every scan, every click that matters"), recorded
 *  through the open station door (firebaseOrders {activity}) and read only through a gated function. The client side is
 *  station-activity.js; the contract (fields, counters, limits) is plans/employee-efficiency/contract.md.
 *
 *  Station_Activity/{id}            one document per event, created once (a retry finds it and changes nothing)
 *  Efficiency_Daily/{day}__{person} one rollup per person per New York day, kept with atomic increments in the SAME
 *                                   transaction that creates a new event document: a retry or a double send never counts twice
 *  (Sandbox_ copies of both in the sandbox.) The person is the employee's NAME, never the PIN: a digits-only name refuses
 *  the event, and a PIN-looking id or detail is blanked. Nothing here calls Etsy. */
"use strict";
const ACT = "Station_Activity", DAILY = "Efficiency_Daily";
const { STATIONS } = require("./_orderTimeline");           // one list of stations for the timeline, the sessions and this
const ACTIONS = new Set(["scan", "reject", "complete", "print", "undo", "error", "note"]);
const MAX_BATCH = 50, MAX_EVENT_BYTES = 600, MAX_BODY_CHARS = 40000;
const ACTIVE_GAP_MS = 5 * 60000, GAP_CAP_MS = 3600000, MAX_AGE_MS = 7 * 86400000, MAX_PARTS = 100000, MAX_TOUCHED = 3000;
const TZ = "America/New_York";

let fmt = null;
try { fmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, hourCycle: "h23", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" }); } catch (_) {}
function nyParts(t) {
  if (fmt) { const o = {}; for (const p of fmt.formatToParts(new Date(t))) o[p.type] = p.value; return { y: +o.year, m: +o.month, d: +o.day, h: +o.hour % 24 }; }
  const e = new Date(t - 5 * 3600e3);                          // no time zone data: Eastern Standard Time
  return { y: e.getUTCFullYear(), m: e.getUTCMonth() + 1, d: e.getUTCDate(), h: e.getUTCHours() };
}
const pad = n => String(n).padStart(2, "0");
/** New York { day: "YYYY-MM-DD", hour: "00".."23" } of a moment (ms) */
function nyDayHour(t) { const p = nyParts(t); return { day: `${p.y}-${pad(p.m)}-${pad(p.d)}`, hour: pad(p.h) }; }
/** the rollup document id */
const rollupId = (day, person) => `${day}__${String(person || "").replace(/\//g, "-").slice(0, 80)}`;

const str = (v, n) => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, n);
const int = (v, lo, hi) => { const x = Math.round(Number(v)); return Number.isFinite(x) ? Math.max(lo, Math.min(hi, x)) : lo; };
const pinLike = s => /^\d{4,8}$/.test(s);                       // a PIN is digits only: never kept as an id

/** One event as it is stored, or { refused: true }. `scrubbed` says a PIN-looking value was blanked. */
function clean(e, now, prefix) {
  if (!e || typeof e !== "object" || Array.isArray(e)) return { refused: true };
  let bytes = 0; try { bytes = Buffer.byteLength(JSON.stringify(e)); } catch (_) { return { refused: true }; }
  if (bytes > MAX_EVENT_BYTES) return { refused: true };
  if (typeof e.sandbox === "boolean" && e.sandbox !== !!prefix) return { refused: true };   // an event keeps the store it was recorded for
  // (a document id like "__x__" is reserved by Firestore: it would fail the whole batch with a 5xx the client retries forever)
  const id = typeof e.id === "string" && /^[\w.:-]{8,100}$/.test(e.id) && !/^__.*__$/.test(e.id) && !/^\.+$/.test(e.id) ? e.id : "";
  const station = typeof e.station === "string" && STATIONS.has(e.station) ? e.station : "";
  const action = typeof e.action === "string" && ACTIONS.has(e.action) ? e.action : "";
  const person = str(e.person, 80);
  if (!id || !station || !action || !person || !/\p{L}/u.test(person)) return { refused: true };   // no letter (digits, "123 456", "12-34-56") = a PIN, never a name
  const at0 = Number(e.at);
  let at = Number.isFinite(at0) && at0 > 1e12 ? Math.round(at0) : now;
  if (at > now) at = now;                                       // a clock ahead never puts an event in the future
  if (now - at > MAX_AGE_MS) return { refused: true };
  let scrubbed = false;
  const idish = (v, n) => { const s = str(v, n); if (pinLike(s)) { scrubbed = true; return ""; } return s; };
  const orderId = idish(String(e.orderId == null ? "" : e.orderId).replace(/\D/g, ""), 30);
  let detail = str(e.detail, 120);
  if (/^\d+$/.test(detail)) { detail = ""; scrubbed = true; }
  else if (/(?<!\d)\d{6}(?!\d)/.test(detail)) { detail = detail.replace(/(?<!\d)\d{6}(?!\d)/g, "[#]"); scrubbed = true; }
  const { day, hour } = nyDayHour(at);
  const doc = {
    id, station, device: str(e.device, 40).replace(/[^\w .:-]/g, "") || station,
    computer: typeof e.computer === "string" && /^[\w-]{6,64}$/.test(e.computer) ? e.computer : "",
    session: typeof e.session === "string" && /^[\w.:-]{8,100}$/.test(e.session) ? e.session : "",
    person, action, orderId, line: idish(e.line, 40), sku: idish(e.sku, 60),
    parts: int(e.parts, 0, MAX_PARTS), orders: Number(e.orders) >= 1 ? 1 : 0, detail,
    at, seq: int(e.seq, 0, 1e9), sincePrevMs: int(e.sincePrevMs, 0, GAP_CAP_MS),
    serverAt: now, day, hour, v: 1
  };
  if (prefix) doc.sandbox = true;
  return { doc, scrubbed };
}

/** The counters one action adds to its station and hour (plain numbers; add() turns them into increments). */
function bump(o, k, v) { if (v) o[k] = (o[k] || 0) + v; }
function tally(st, ev) {
  switch (ev.action) {
    case "scan": bump(st, "scans", 1); bump(st, "scanParts", ev.parts); break;
    case "complete": bump(st, "completes", 1); bump(st, "parts", ev.parts); bump(st, "orders", ev.orders); break;
    case "print": bump(st, "prints", 1); break;
    case "reject": bump(st, "rejects", 1); break;
    case "undo": bump(st, "undos", 1); bump(st, "undoParts", ev.parts); bump(st, "undoOrders", ev.orders); break;
    case "error": bump(st, "errors", 1); break;
    default: bump(st, "notes", 1);
  }
  bump(st, ev.sincePrevMs <= ACTIVE_GAP_MS ? "activeMs" : "idleMs", ev.sincePrevMs);
}
const incs = (FV, o) => Object.fromEntries(Object.entries(o).map(([k, v]) => [k, FV.increment(v)]));

/** The merge for one person-day rollup from the new events only; prev is the stored rollup (or null). */
function rollupPatch(FV, prev, day, person, evs, prefix) {
  prev = prev || {};
  const st = {}, hours = {}, touched = {}, span = {};
  let first = Infinity, last = 0, n = evs.length;
  const known = prev.touched && typeof prev.touched === "object" ? Object.keys(prev.touched).length : 0;
  let added = 0;
  for (const ev of evs) {
    first = Math.min(first, ev.at); last = Math.max(last, ev.at);
    tally(st[ev.station] || (st[ev.station] = {}), ev);
    const sp = span[ev.station] || (span[ev.station] = { first: Infinity, last: 0 });
    sp.first = Math.min(sp.first, ev.at); sp.last = Math.max(sp.last, ev.at);
    const sc = ev.action === "scan" ? 1 : 0, pr = ev.action === "complete" ? ev.parts : 0, un = ev.action === "undo" ? ev.parts : 0;
    if (sc || pr || un) {
      const h = hours[ev.hour] || (hours[ev.hour] = { tot: {}, by: {} });
      bump(h.tot, "scans", sc); bump(h.tot, "parts", pr); bump(h.tot, "undoParts", un);
      const b = h.by[ev.station] || (h.by[ev.station] = {}); bump(b, "scans", sc); bump(b, "parts", pr); bump(b, "undoParts", un);
    }
    if (ev.orderId) {
      const had = prev.touched && prev.touched[ev.orderId] != null || touched[ev.orderId] != null;
      if (had || known + added < MAX_TOUCHED) { if (!had) added++; (touched[ev.orderId] || (touched[ev.orderId] = {}))[ev.station] = true; }
    }
  }
  const stations = {};
  for (const s of Object.keys(st)) {
    const p = prev.stations && prev.stations[s] || {};
    stations[s] = Object.assign(incs(FV, st[s]), {
      firstAt: Math.min(span[s].first, Number(p.firstAt) || Infinity), lastAt: Math.max(span[s].last, Number(p.lastAt) || 0) });
  }
  const hh = {};
  for (const k of Object.keys(hours)) {
    hh[k] = Object.assign(incs(FV, hours[k].tot), { by: Object.fromEntries(Object.entries(hours[k].by).map(([s, o]) => [s, incs(FV, o)])) });
  }
  const out = { day, person, v: 1, events: FV.increment(n), firstAt: Math.min(first, Number(prev.firstAt) || Infinity), lastAt: Math.max(last, Number(prev.lastAt) || 0), stations };
  if (Object.keys(hh).length) out.hours = hh;
  if (Object.keys(touched).length) out.touched = touched;
  if (prefix) out.sandbox = true;
  return out;
}

/** Writes a batch. Returns { written, duplicate, refused, scrubbed }. Events already stored change nothing and add to no rollup. */
async function add(db, FV, events, opts) {
  const prefix = (opts && opts.prefix) || "", col = name => db.collection(prefix + name);
  const now = Date.now();
  let refused = 0, scrubbed = 0, duplicate = 0;
  const seen = new Set(), docs = [];
  for (const raw of events) {
    const r = clean(raw, now, prefix);
    if (r.refused) { refused++; continue; }
    if (seen.has(r.doc.id)) { duplicate++; continue; }          // the same id twice in one request counts once
    seen.add(r.doc.id); docs.push(r.doc);
    if (r.scrubbed) scrubbed++;
  }
  if (!docs.length) return { written: 0, duplicate, refused, scrubbed };
  const written = await db.runTransaction(async tx => {
    const groups = new Map();
    for (const d of docs) { const k = rollupId(d.day, d.person); if (!groups.has(k)) groups.set(k, { day: d.day, person: d.person, ref: col(DAILY).doc(k), docs: [] }); }
    const gl = [...groups.values()], evRefs = docs.map(d => col(ACT).doc(d.id));
    const snaps = await tx.getAll(...evRefs, ...gl.map(g => g.ref));
    const fresh = docs.filter((d, i) => !snaps[i].exists);
    for (const d of fresh) groups.get(rollupId(d.day, d.person)).docs.push(d);
    for (const d of fresh) tx.set(col(ACT).doc(d.id), Object.assign({}, d, { ts: FV.serverTimestamp() }));
    gl.forEach((g, i) => {
      if (!g.docs.length) return;
      const s = snaps[docs.length + i];
      tx.set(g.ref, rollupPatch(FV, s && s.exists ? (s.data() || {}) : null, g.day, g.person, g.docs, prefix), { merge: true });
    });
    return fresh.length;
  });
  return { written, duplicate: duplicate + (docs.length - written), refused, scrubbed };
}

module.exports = { ACT, DAILY, STATIONS, ACTIONS, MAX_BATCH, MAX_EVENT_BYTES, MAX_BODY_CHARS, ACTIVE_GAP_MS, GAP_CAP_MS, nyDayHour, rollupId, clean, rollupPatch, add };
