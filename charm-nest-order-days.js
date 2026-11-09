/* charm-nest-order-days.js — SALES DAYS (SAL1, Paul 9 Nov 2026: "how many orders were placed today thus far in real-time, today means Toronto time
   resetting every 24 hours at 12 AM midnight … beside that … yesterday's sales order # count total … the seven day sales order # count total";
   "Make sure you're not pinging Etsy very often").

   The Orders head's three numbers (Today, Yesterday, Last 7 days) are counted from ONE small Firestore document, never from a new Etsy call:
       Charm_Nest_Order_Days/recent           the real shop
       Sandbox_Charm_Nest_Order_Days/recent   the sandbox (its own orders; a family in charm-nest-sandbox-families.js, its reset wipes it)
   { v: 1, at, days: { "YYYY-MM-DD": { ids: { <receipt id>: <created, unix s> }, cx: { <receipt id>: 1 }, hook: { <receipt id>: <unix s> } } } }
   keyed by the order's CREATION day in America/Toronto (DST-safe: Intl with timeZone "America/Toronto", never a fixed offset).
     ids   a receipt Etsy itself listed, with its create_timestamp: written as a side effect of reads the app already makes
           (etsyMailReceiptsMirrorCron: EVERY receipt of the shop, any status, every 7-10 min; charmNestLibrary arrivalRecord: the order check's open orders)
     cx    the receipts of ids Etsy says are cancelled (counted as placed; the tooltip says how many)
     hook  a receipt Etsy's order.paid webhook named (no created time in it: the moment it was paid stands in) — counted only while no `ids` entry of the
           window names the same receipt, so a receipt is counted once whichever path saw it first
   Only the last KEEP_DAYS Toronto days are kept: the document stays a few tens of KB at most.

   UMD: window.CharmNestOrderDays in the page, module.exports on the server and in tests. Pure: no I/O here except merge(), which is handed a
   Firestore transaction by its caller. */
(function (root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api; else root.CharmNestOrderDays = api;
})(typeof self !== "undefined" ? self : globalThis, function () {
  "use strict";
  const TZ = "America/Toronto", KEEP_DAYS = 10, COLL = "Charm_Nest_Order_Days", DOC = "recent";
  let fmt = null;
  const parts = ms => {
    if (!fmt) fmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23" });
    const o = {}; for (const p of fmt.formatToParts(new Date(ms))) o[p.type] = p.value;
    return o;
  };
  /** The Toronto calendar date of a moment (ms), "YYYY-MM-DD". */
  function dateOf(ms) { const o = parts(ms); return `${o.year}-${o.month}-${o.day}`; }
  /** Seconds since Toronto's midnight at that moment (the wall clock: 01:30 on a DST day is 5400). */
  function secOfDay(ms) { const o = parts(ms); return (+o.hour % 24) * 3600 + +o.minute * 60 + +o.second; }
  /** A calendar date k days from date (calendar arithmetic: no clock, so no DST). */
  function shift(date, k) {
    const [y, m, d] = String(date).split("-").map(Number), t = new Date(Date.UTC(y, m - 1, d + k));
    return `${t.getUTCFullYear()}-${String(t.getUTCMonth() + 1).padStart(2, "0")}-${String(t.getUTCDate()).padStart(2, "0")}`;
  }
  const isDate = s => /^\d{4}-\d{2}-\d{2}$/.test(String(s));
  const isId = s => /^\d{1,30}$/.test(String(s));
  const sec = v => { const n = Number(v); if (!Number.isFinite(n) || n <= 0) return 0; return Math.floor(n > 1e11 ? n / 1000 : n); };   // (ms or s → s)

  /** Receipts as the app holds them → [{ id, created (s), cancelled, hook }]. Takes Etsy's raw receipt ({ receipt_id, create_timestamp |
      created_timestamp, status }), the sorter's order ({ receiptId, createTs }), or { id, createTs, cancelled, hook }. */
  function normalize(list) {
    const out = [];
    for (const r of list || []) {
      if (!r || typeof r !== "object") continue;
      const id = String(r.receipt_id != null ? r.receipt_id : r.receiptId != null ? r.receiptId : r.id);
      const created = sec(r.create_timestamp != null ? r.create_timestamp : r.created_timestamp != null ? r.created_timestamp : r.createTs != null ? r.createTs : r.created);
      if (!isId(id) || !created) continue;
      const statusKnown = r.cancelled != null || r.status != null, cancelled = r.cancelled != null ? !!r.cancelled : /cancel/i.test(String(r.status || ""));
      out.push({ id, created, cancelled, statusKnown, hook: !!r.hook });   // (statusKnown: only a receipt whose status was read can take a cancel back)
    }
    return out;
  }
  /** The document after these receipts (a copy; doc untouched) and whether anything moved. nowMs: today's Toronto date is read from it, and only
      receipts created within the last KEEP_DAYS days are kept. */
  function apply(doc, receipts, nowMs) {
    const today = dateOf(nowMs), oldest = shift(today, -(KEEP_DAYS - 1)), days = {};
    let changed = false;
    for (const [date, d] of Object.entries((doc && doc.days) || {})) {
      if (!isDate(date) || date < oldest) { changed = true; continue; }   // (an old day goes)
      days[date] = { ids: Object.assign({}, d && d.ids), cx: Object.assign({}, d && d.cx), hook: Object.assign({}, d && d.hook) };
    }
    for (const r of normalize(receipts)) {
      const date = dateOf(r.created * 1000);
      if (date < oldest || date > shift(today, 1)) continue;
      const d = days[date] || (days[date] = { ids: {}, cx: {}, hook: {} });
      if (r.hook) { if (!d.hook[r.id] && !d.ids[r.id]) { d.hook[r.id] = r.created; changed = true; } continue; }
      if (d.ids[r.id] !== r.created) { d.ids[r.id] = r.created; changed = true; }
      if (d.hook[r.id]) { delete d.hook[r.id]; changed = true; }
      if (r.cancelled && !d.cx[r.id]) { d.cx[r.id] = 1; changed = true; }
      else if (!r.cancelled && d.cx[r.id] && r.statusKnown) { delete d.cx[r.id]; changed = true; }
    }
    return { doc: { v: 1, days }, changed };
  }
  /** One Firestore transaction on the world's document: read it, add the receipts, write it only when something moved. Never throws (a day count
      is never a reason for the read that carries it to fail): { ok, changed, doc } or { ok: false, error }. prefix "" or "Sandbox_". */
  async function merge(db, prefix, receipts, nowMs, mark) {
    try {
      const list = normalize(receipts); if (!list.length && !mark) return { ok: true, changed: false, doc: null };
      const ref = db.collection((prefix || "") + COLL).doc(DOC), now = nowMs || Date.now();
      return await db.runTransaction(async tx => {
        const snap = await tx.get(ref), was = snap.exists ? snap.data() || {} : {}, out = apply(was, list, now);
        // (seededAt: when the days were first filled from the receipt mirror's own copy, so the first week is whole; kept on every later write)
        const doc = Object.assign({}, out.doc, { at: Date.now(), seededAt: (mark && mark.seededAt) || was.seededAt || null });
        if (out.changed || mark) tx.set(ref, doc);
        return { ok: true, changed: out.changed, doc: out.changed || mark ? doc : was };
      });
    } catch (e) { return { ok: false, error: String((e && e.message) || e).slice(0, 200) }; }
  }
  /** The first Toronto day the document keeps (the oldest a seed needs to read), as unix seconds of a moment safely before its midnight. */
  function windowStartSec(nowMs) { return Math.floor(Date.parse(shift(dateOf(nowMs), -(KEEP_DAYS - 1)) + "T00:00:00Z") / 1000); }   // (UTC midnight: 4-5 h before Toronto's; apply() drops what is older)
  /** One read of the world's document. Never throws: null when it cannot be read. */
  async function read(db, prefix) {
    try { const s = await db.collection((prefix || "") + COLL).doc(DOC).get(); return s.exists ? s.data() || {} : {}; } catch (_) { return null; }
  }

  /** The three numbers from the document and what the page itself holds (extra: receipts it knows, e.g. the orders that just arrived): {
        today:     { date, n, cx, ids: [...] }   orders created since 00:00 Toronto
        yesterday: { date, n, cx, sameTime }     the previous Toronto day; sameTime: those created by this time of day (Today's delta)
        week:      { from, to, n, cx }           the 7 Toronto days ending today, today included
      } Each receipt counts once (distinct receipt ids, not pieces). */
  function summarize(doc, nowMs, extra) {
    const today = dateOf(nowMs), yday = shift(today, -1), from = shift(today, -6), clock = secOfDay(nowMs);
    const merged = apply(doc || {}, [], nowMs).doc;
    const add = apply(merged, extra || [], nowMs).doc;   // (the page's own receipts join without being written)
    const known = new Set(); for (const d of Object.values(add.days)) for (const id of Object.keys(d.ids)) known.add(id);
    const dayOf = date => {
      const d = add.days[date] || { ids: {}, cx: {}, hook: {} }, ids = new Map();
      for (const [id, t] of Object.entries(d.ids)) ids.set(id, t);
      for (const [id, t] of Object.entries(d.hook)) if (!known.has(id)) ids.set(id, t);
      return { ids, cx: Object.keys(d.cx).filter(id => ids.has(id)).length };
    };
    const t = dayOf(today), y = dayOf(yday);
    let sameTime = 0; for (const ts of y.ids.values()) if (secOfDay(ts * 1000) <= clock) sameTime++;
    const week = new Set(); let wcx = 0;
    for (let k = 0; k < 7; k++) { const d = dayOf(shift(today, -k)); for (const id of d.ids.keys()) week.add(id); wcx += d.cx; }
    return { today: { date: today, n: t.ids.size, cx: t.cx, ids: [...t.ids.keys()] }, yesterday: { date: yday, n: y.ids.size, cx: y.cx, sameTime },
      week: { from, to: today, n: week.size, cx: wcx }, at: doc && doc.at || 0 };
  }
  /** The document trimmed for an answer: the days of the window only (today and the six before; yesterday is one of them). */
  function forAnswer(doc, nowMs) {
    if (!doc) return null;
    const today = dateOf(nowMs), from = shift(today, -6), days = {};
    for (const [date, d] of Object.entries(doc.days || {})) if (isDate(date) && date >= from && date <= shift(today, 1)) days[date] = d;
    return { v: 1, at: doc.at || Date.now(), days };
  }
  return { TZ, KEEP_DAYS, COLL, DOC, windowStartSec, dateOf, secOfDay, shift, normalize, apply, merge, read, summarize, forAnswer };
});
