/*  netlify/functions/_designLog.js
 *  Every custom piece a person commissions, and every fix piece, with who did it (Design & Engrave items 35 to 37 and D7; Paul, 28 Oct 2026:
 *  "I need to know when employees are commissioning custom pieces, especially when they don't belong to an order that way we can correctly
 *  track our rates and who creates the errors and who is creating the solutions").
 *
 *  Design_Log/{id}     one record per send of a custom piece (Sandbox_Design_Log in the sandbox), written by the page's design log
 *                      (charm-nest-design-log.js) through charmNestLibrary op designLog; read by the Efficiency page's Custom pieces view
 *                      (employeeEfficiency op custom). Employee efficiency data: a sandbox wipe keeps it (charm-nest-sandbox-families.js).
 *    id          "dl-<sendId | first pool id | random>": a retry, or the two kinds of one send, land on the same document (merged)
 *    kind        customPieceSent | fixPiece (a record that names what it fixes is a fixPiece)
 *    by          who commissioned it (the signed-in name; the one login name, _activityKinds.js loginName)
 *    at          when (ms): the writer's time when it is at most 7 days old and not ahead of the server, else the server's; serverAt the server's
 *    day         its New York day (the view reads a range of days: one single-field range, no composite index)
 *    station     design | laser | sorter (where the person was signed in)
 *    orderId     the Etsy order it belongs to (digits), "" for a piece with no order
 *    orderless   true when it has no order (flagged everywhere: Paul's "especially when they don't belong to an order")
 *    customId    CUSTOM-yyyymmdd-n, the id a piece with no order rides the sheets under (D4)     customName · reason   what the person typed
 *    designId · version · name · sku    the design sent (D6: its id and version)                metal · quantity · poolIds
 *    fix         { orderId | customId, poolId, lineKey, causedBy, suggested, why }: the order or piece it fixes, who the mistake was put down to
 *                (causedBy) and who the order's history suggested (suggested: when the two differ, the person changed it) (D7)
 *  Never a PIN: a name with no letter is refused, digits are dropped from a name (as every station door does). No Etsy call. */
"use strict";
const COL = "Design_Log";
const KINDS = new Set(["customPieceSent", "fixPiece"]);
const METALS = new Set(["gold", "silver", "rose", "gold10k", "gold14k"]);
const STATIONS = new Set(["design", "laser", "sorter"]);
const MAX_BATCH = 50, MAX_AGE_MS = 7 * 86400000, MAX_RANGE = 2000;
let loginName = n => n; try { loginName = require("./_activityKinds").loginName || loginName; } catch (_) {}

const str = (v, n) => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, n);
const idish = (v, n) => str(v, n).replace(/[^\w.:-]/g, "_");
const digits = v => String(v == null ? "" : v).replace(/\D/g, "").slice(0, 30);
const CUSTOM_RE = /^CUSTOM-\d{8}-\d{1,4}$/;
const noPin = s => (s.match(/\p{Nd}/gu) || []).length >= 4 ? s.replace(/\p{Nd}+/gu, " ").replace(/\s+/g, " ").trim() : s;
/** a person's name as it is stored, or "" (no letter = a PIN, never a name) */
const nameOf = v => { const s = loginName(noPin(str(v, 120)).slice(0, 80)); return typeof s === "string" && /\p{L}/u.test(s) ? s : ""; };
const orderOf = v => { const d = digits(v); return d.length >= 9 ? d : ""; };      // (an Etsy receipt; 4 to 8 digits could be a PIN: never kept)

let fmt = null;
try { fmt = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit" }); } catch (_) {}
/** the New York day of a moment (ms) */
function nyDay(t) {
  if (fmt) { const o = {}; for (const p of fmt.formatToParts(new Date(t))) o[p.type] = p.value; return `${o.year}-${o.month}-${o.day}`; }
  return new Date(t - 5 * 3600e3).toISOString().slice(0, 10);
}

/** One record as it is stored, or null when it is not one. */
function clean(r, now, prefix) {
  if (!r || typeof r !== "object" || Array.isArray(r)) return null;
  if (typeof r.sandbox === "boolean" && r.sandbox !== !!prefix) return null;     // (a record keeps the store it was made for)
  const id = typeof r.id === "string" && /^dl-[\w.:-]{4,96}$/.test(r.id) ? r.id : "";
  const by = nameOf(r.by);
  if (!id || !by) return null;
  const c0 = Number(r.clientAt), at = Number.isFinite(c0) && c0 > 1e12 && c0 <= now && now - c0 <= MAX_AGE_MS ? Math.round(c0) : now;
  const fx = r.fix && typeof r.fix === "object" && !Array.isArray(r.fix) ? r.fix : null;
  const doc = { id, kind: fx || r.kind === "fixPiece" ? "fixPiece" : KINDS.has(r.kind) ? r.kind : "customPieceSent", by, at, serverAt: now, day: nyDay(at),
    station: STATIONS.has(r.station) ? r.station : "sorter", orderId: orderOf(r.orderId), v: 1 };
  doc.orderless = !doc.orderId;
  const cid = str(r.customId, 24); if (CUSTOM_RE.test(cid)) doc.customId = cid;
  for (const [k, n] of [["customName", 80], ["reason", 200], ["name", 80], ["sku", 60]]) { const s = str(r[k], n); if (s) doc[k] = s; }
  for (const [k, n] of [["designId", 80], ["poolId", 60], ["sendId", 60]]) { const s = idish(r[k], n); if (s) doc[k] = s; }
  const ver = Math.floor(Number(r.version)); if (ver > 0 && ver < 1e6) doc.version = ver;
  if (METALS.has(r.metal)) doc.metal = r.metal;
  const q = Math.floor(Number(r.quantity)); doc.quantity = q > 0 ? Math.min(q, 500) : 1;
  if (Array.isArray(r.poolIds)) { const p = r.poolIds.map(x => idish(x, 60)).filter(Boolean).slice(0, 50); if (p.length) doc.poolIds = p; }
  if (doc.kind === "fixPiece") {
    const f = {}, fo = str(fx && fx.orderId, 40);
    if (CUSTOM_RE.test(fo)) f.customId = fo; else if (orderOf(fo)) f.orderId = orderOf(fo);
    if (!f.customId && fx && CUSTOM_RE.test(str(fx.customId, 24))) f.customId = str(fx.customId, 24);
    for (const [k, n] of [["poolId", 60], ["lineKey", 80]]) { const s = idish(fx && fx[k], n); if (s) f[k] = s; }
    const cb = nameOf(fx && fx.causedBy), sg = nameOf(fx && fx.suggested), why = str(fx && fx.why, 120);
    if (cb) f.causedBy = cb; if (sg) f.suggested = sg; if (why) f.why = why;
    doc.fix = f;
  }
  if (prefix) doc.sandbox = true;
  return doc;
}

/** Writes a batch (at most 50): one set per record, merged, so a retry or the second kind of one send changes the same document.
 *  Returns { ok, written, refused }. One write per record, no read. */
async function add(db, FV, records, opts) {
  const prefix = (opts && opts.prefix) || "", now = Date.now();
  const list = Array.isArray(records) ? records.slice(0, MAX_BATCH) : [];
  let refused = (Array.isArray(records) ? records.length : 0) - list.length;
  const docs = new Map();
  for (const r of list) { const d = clean(r, now, prefix); if (!d) { refused++; continue; } const had = docs.get(d.id); docs.set(d.id, had ? Object.assign({}, had, d, had.fix || d.fix ? { kind: "fixPiece", fix: Object.assign({}, had.fix, d.fix) } : {}) : d); }
  if (!docs.size) return { ok: true, written: 0, refused };
  const batch = db.batch();
  for (const d of docs.values()) batch.set(db.collection(prefix + COL).doc(d.id), Object.assign({}, d, FV && FV.serverTimestamp ? { ts: FV.serverTimestamp() } : {}), { merge: true });
  await batch.commit();
  return { ok: true, written: docs.size, refused };
}

/** The records of the New York days from..to (YYYY-MM-DD), newest first: one range on `day` (its single-field index). { rows, capped } */
async function list(db, opts) {
  const prefix = (opts && opts.prefix) || "", lim = Math.max(1, Math.min(MAX_RANGE, Math.floor(Number(opts && opts.limit)) || MAX_RANGE));
  const snap = await db.collection(prefix + COL).where("day", ">=", opts.from).where("day", "<=", opts.to).limit(lim + 1).get();
  const rows = [];
  for (const d of snap.docs.slice(0, lim)) { const x = d.data() || {}; if (!!x.sandbox === !!prefix && typeof x.day === "string") rows.push(Object.assign({}, x, { id: d.id, at: Number(x.at) || 0 })); }
  rows.sort((a, b) => b.at - a.at || (a.id < b.id ? 1 : -1));
  return { rows, capped: snap.docs.length > lim };
}

module.exports = { COL, KINDS, MAX_BATCH, clean, add, list, nyDay };
