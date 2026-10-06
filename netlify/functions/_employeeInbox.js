/*  netlify/functions/_employeeInbox.js
 *  ─────────────────────────────────────────────────────────────────────────────
 *  Inbox tracking for the Employee portal (Paul, 6 Oct 2026: "track all the responses a given user/employee has sent in a
 *  day/week/month/3 months/1 year, how many orders does that cover, how many messages to by customer").
 *  Plan, shapes and what the inbox records: plans/stations-round2/api.md, section IN1. READ-ONLY: it never writes, never calls
 *  Etsy, never calls an AI. Only what a PERSON sent counts (a reply that went into Etsy's send queue under an operator's name):
 *  an AI draft nobody sent is nothing, and a reply the auto-pipeline sent is `auto`, never a person's.
 *
 *  Where the numbers come from
 *    EtsyMail_ReplyDaily/{New York day}   one small entry per sent reply, appended by the send path from now on
 *                                         (_etsyMailLearning.js replyEntry / recordReply): who, manual or auto, order, customer.
 *                                         One document per day: a year is at most 366 reads, a warm repeat 0.
 *    EtsyMail_DraftOutcomes               the learning store, one document per sent reply since 28 Sep 2026: the HISTORY TAIL, read
 *                                         only for the days before the first ReplyDaily entry (a fixed ~9 days, kept 30 min), with
 *                                         the conversation's order number and customer read from EtsyMail_Threads (kept 24 h).
 *    EtsyMail_Operators                   the inbox usernames' display names (field mask: nothing else of a document is read).
 *    config/employeeAliases               the portal's name merging, through the kit's nameKeyOf (one rule for every name).
 *  Before the first record nothing is known: those days are `null` (a dash), never a zero, and `knownFrom` says where records begin.
 *
 *  Days: the portal's New York days (America/Toronto is the same Eastern clock: same days, same daylight-saving changes) and its
 *  rolling windows (Day 1, Week 7, Month 30, 3 months 90, Year 365 days ending on `day`), through the kit's resolveRange.
 *  Real only: the Sandbox keeps no inbox, so every op answers empty with a note and reads nothing.
 *  Binding: employeeEfficiency.js calls bind(kit) once; every function here uses that kit (names, days, caches, the gate stays there).
 *  ───────────────────────────────────────────────────────────────────────────── */
"use strict";

const L = require("./_etsyMailLearning");           // the writer's own entry builder: a replayed outcome becomes exactly the entry a live send writes

const COLL = { daily: L.REPLY_DAILY_COLL, outcomes: L.OUTCOMES_COLL, threads: "EtsyMail_Threads", operators: "EtsyMail_Operators" };
const LIM = { replyDays: 800, entriesDay: 6000, outcomes: 4000, threads: 800, operators: 80, top: 25, topDefault: 5, topPerson: 10, topPersonMax: 50 };
const TTL = { today: 20000, yesterday: 60000, past: 30 * 60000, track: 3600000, trackNone: 60000, tail: 30 * 60000, tailOpen: 120000, thread: 24 * 3600000, operator: 10 * 60000 };
const WINDOWS = ["day", "week", "month", "quarter", "year"];
const DAY = 86400000;
const SANDBOX_NOTE = "The Sandbox keeps no inbox: there is nothing to show here. Switch to Real for the inbox figures.";

let K = null;
/** employeeEfficiency.js hands over its own helpers once (names, aliases, days, the cache, the cleaners), so there is one rule for each. */
function bind(kit) { K = kit; }
const need = () => { if (!K) throw new Error("inbox figures are not bound"); return K; };

/* ── small things ── */
const r1 = x => Math.round(x * 10) / 10;
const fin = v => (v == null || !Number.isFinite(+v) ? null : r1(+v));
const plain = (v, n) => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, n);
const digitsOf = (v, n) => String(v == null ? "" : v).replace(/\D/g, "").slice(0, n || 30);
const epochDay = d => Date.UTC(+d.slice(0, 4), +d.slice(5, 7) - 1, +d.slice(8, 10)) / DAY;
const spanLen = (a, b) => epochDay(b) - epochDay(a) + 1;
const dowOf = d => new Date(d + "T12:00:00Z").getUTCDay();
function daysOf(a, b) { const { addDays } = need(), out = []; for (let d = a, i = 0; d <= b && i < 2200; d = addDays(d, 1), i++) out.push(d); return out; }
const mondayOf = d => need().addDays(d, -((dowOf(d) + 6) % 7));
const median = a => { if (!a.length) return null; const s = a.slice().sort((x, y) => x - y), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };

const newIb = () => ({ rd: { replyDocs: 0, replyQueries: 0, trackDocs: 0, outcomeDocs: 0, threadDocs: 0, operatorDocs: 0 }, errors: [], capped: [], usedTail: false });
function boxOf(ctx) { const c = ctx.cache; return c.inbox || (c.inbox = { days: new Map(), threads: new Map(), track: null }); }
const readsOf = ib => { const r = ib.rd; return Object.assign({ documents: r.replyDocs + r.trackDocs + r.outcomeDocs + r.threadDocs + r.operatorDocs }, r); };   // (replyQueries counts queries, not documents)

/* ── one stored entry, checked: anything else is skipped, never an error ── */
function normEntry(v) {
  if (!v || typeof v !== "object") return null;
  const t = Number(v.t);
  if (!(t > 0) || !Number.isFinite(t)) return null;
  const th = digitsOf(v.th, 24), c = /^[0-9a-f]{6,24}$/.test(String(v.c || "")) ? String(v.c) : "";
  const r = /^\d{9,14}$/.test(String(v.r == null ? "" : v.r)) ? String(v.r) : "";
  return { t: Math.floor(t), by: plain(v.by, 60), o: v.o === "a" ? "a" : "m", r, c: c || (th ? "t" + th : ""), n: plain(v.n, 40), th, day: "" };
}
const isAuto = e => e.o === "a" || /^system:/i.test(e.by);

/* ── reads ── */
/** The inbox operators: username → display name (cached 10 minutes). The field mask keeps everything else of those documents out of reach. */
function readOperators(ctx) {
  const { cached, cleanName } = need();
  return cached(ctx, "ib|operators", TTL.operator, async () => {
    let q = ctx.db.collection(COLL.operators);
    if (typeof q.select === "function") q = q.select("displayName", "revokedAt");
    const snap = await q.limit(LIM.operators + 1).get();
    ctx.ib.rd.operatorDocs += snap.docs.length;
    const m = new Map();
    for (const d of snap.docs.slice(0, LIM.operators)) { const v = d.data() || {}; m.set(String(d.id), { display: cleanName(v.displayName), revoked: !!v.revokedAt }); }
    return m;
  });
}

/** Where records begin: the first ReplyDaily entry (its time) and the first outcome. Found: kept 1 hour; nothing yet: kept 1 minute. */
function readTracking(ctx) {
  const { nyMidnight } = need(), box = boxOf(ctx), hit = box.track;
  if (hit && (!hit.v ? true : ctx.now - hit.at < (hit.v.aggFirstMs ? TTL.track : TTL.trackNone))) return hit.p;
  const entry = { at: ctx.now, v: null, p: null };
  entry.p = (async () => {
    const [a, o] = await Promise.all([
      ctx.db.collection(COLL.daily).orderBy("day", "asc").limit(1).get(),
      ctx.db.collection(COLL.outcomes).orderBy("atMs", "asc").limit(1).get()]);
    ctx.ib.rd.trackDocs += a.docs.length + o.docs.length;
    let aggFirstMs = 0, aggFirstDay = "";
    if (a.docs[0]) {
      const d = a.docs[0].data() || {}, ts = (Array.isArray(d.replies) ? d.replies : []).map(e => Number(e && e.t)).filter(x => x > 0);
      aggFirstDay = typeof d.day === "string" ? d.day : String(a.docs[0].id);
      aggFirstMs = ts.length ? Math.min(...ts) : nyMidnight(aggFirstDay);
    }
    const first = o.docs[0] ? Number((o.docs[0].data() || {}).atMs) : 0;
    entry.v = { aggFirstMs, aggFirstDay, outFirstMs: first > 0 ? first : 0 };
    return entry.v;
  })();
  box.track = entry;
  entry.p.catch(() => { if (box.track === entry) box.track = null; });
  return entry.p;
}

/** The ReplyDaily documents of the days from..to: a day at a time in the cache (today 20 s, yesterday 1 min, older 30 min), ONE range query for the stale ones. */
async function readReplyDays(ctx, from, to) {
  const { addDays } = need(), box = boxOf(ctx), now = ctx.now, days = daysOf(from, to), want = new Map(), stale = [];
  const yesterday = addDays(ctx.today, -1), ttlOf = d => (d >= ctx.today ? TTL.today : d >= yesterday ? TTL.yesterday : TTL.past);
  for (const d of days) { const e = box.days.get(d); if (e && now - e.at < Math.min(ttlOf(d), e.ttl)) want.set(d, e.p); else stale.push(d); }
  if (stale.length) {
    const a = stale[0], z = stale[stale.length - 1];
    const fetchP = ctx.db.collection(COLL.daily).where("day", ">=", a).where("day", "<=", z).limit(LIM.replyDays + 1).get().then(snap => {
      ctx.ib.rd.replyDocs += snap.docs.length; ctx.ib.rd.replyQueries++;
      const by = new Map();
      for (const d of snap.docs.slice(0, LIM.replyDays)) { const v = d.data() || {}; by.set(typeof v.day === "string" ? v.day : String(d.id), Array.isArray(v.replies) ? v.replies : []); }
      return { by, capped: snap.docs.length > LIM.replyDays };
    });
    for (const d of stale) {
      const entry = { at: now, ttl: ttlOf(d), p: fetchP.then(r => ({ replies: r.by.get(d) || [], capped: r.capped })) };
      box.days.set(d, entry);
      entry.p.catch(() => { if (box.days.get(d) === entry) box.days.delete(d); });
      want.set(d, entry.p);
    }
    if (box.days.size > 3000) for (const [k, e] of box.days) if (now - e.at > TTL.past) box.days.delete(k);
  }
  const out = new Map(); let capped = false;
  for (const d of days) { const r = await want.get(d); if (r.replies.length) out.set(d, r.replies); capped = capped || r.capped; }
  return { days: out, capped };
}

/** The conversations (order number, customer) of the history tail: ONE batch read per 100, kept 24 hours, at most 800 per call. */
async function readThreads(ctx, ids) {
  const box = boxOf(ctx), now = ctx.now, out = new Map(), miss = [];
  for (const id of ids) { const e = box.threads.get(id); if (e && now - e.at < TTL.thread) out.set(id, e.v); else miss.push(id); }
  const want = miss.slice(0, LIM.threads);
  if (miss.length > want.length) ctx.ib.capped.push("conversations");
  for (let i = 0; i < want.length; i += 100) {
    const chunk = want.slice(i, i + 100), refs = chunk.map(id => ctx.db.collection(COLL.threads).doc("etsy_conv_" + id));
    const snaps = typeof ctx.db.getAll === "function"
      ? await ctx.db.getAll(...refs, { fieldMask: ["etsyOrderId", "linkedOrderId", "etsyUsername", "customerName"] })
      : await Promise.all(refs.map(r => r.get()));
    ctx.ib.rd.threadDocs += snaps.length;
    snaps.forEach((sn, j) => {
      const v = sn && sn.exists ? (sn.data() || {}) : null;
      const val = v ? { etsyOrderId: v.etsyOrderId, linkedOrderId: v.linkedOrderId, etsyUsername: v.etsyUsername, customerName: v.customerName } : null;
      box.threads.set(chunk[j], { at: now, v: val }); out.set(chunk[j], val);
    });
  }
  return out;
}

/** The history tail: every learning-store outcome from the first one up to the first ReplyDaily entry, as the same entries a live send writes. */
function readTail(ctx, track) {
  const { cached, nyDay } = need(), open = !track.aggFirstMs;
  return cached(ctx, `ib|tail|${track.outFirstMs}|${open ? "open" : track.aggFirstMs}`, open ? TTL.tailOpen : TTL.tail, async () => {
    const zMs = open ? ctx.now + 1 : track.aggFirstMs;
    let q = ctx.db.collection(COLL.outcomes).where("atMs", ">=", track.outFirstMs).where("atMs", "<", zMs).orderBy("atMs", "asc");
    if (typeof q.select === "function") q = q.select("atMs", "employeeName", "sendOrigin", "threadId", "customerName");
    const snap = await q.limit(LIM.outcomes + 1).get();
    ctx.ib.rd.outcomeDocs += snap.docs.length;
    const rows = [];
    for (const d of snap.docs.slice(0, LIM.outcomes)) {
      const v = d.data() || {}, t = Number(v.atMs), th = digitsOf(v.threadId, 24);
      if (!(t > 0) || !th) continue;
      rows.push({ t, th, by: v.employeeName, o: v.sendOrigin, name: v.customerName });
    }
    const info = await readThreads(ctx, [...new Set(rows.map(r => r.th))]);
    const entries = rows.map(r => {
      const thread = Object.assign({}, info.get(r.th) || {}); if (!thread.customerName && r.name) thread.customerName = r.name;
      const e = normEntry(L.replyEntry({ atMs: r.t, employeeName: r.by, sendOrigin: r.o, thread, threadId: "etsy_conv_" + r.th }));
      if (e) e.day = nyDay(e.t);
      return e;
    }).filter(Boolean);
    return { entries, capped: snap.docs.length > LIM.outcomes };
  });
}

/** Every sent-reply entry of the days from..to, oldest first, each with its New York `day`; plus where records begin. Throws {unavailable} when nothing could be read. */
async function load(ctx, from, to) {
  const { safe, nyMidnight, addDays, nyDay } = need(), ib = ctx.ib;
  const [tr, ag] = await Promise.all([safe(readTracking(ctx), "tracking"), safe(readReplyDays(ctx, from, to), "replies")]);
  if (!tr.ok) ib.errors.push("tracking: " + tr.error);
  if (!ag.ok) ib.errors.push("replies: " + ag.error);
  const entries = [], seen = new Set(); let skipped = 0;
  if (ag.ok) {
    if (ag.value.capped) ib.capped.push("replies");
    for (const [day, raw] of ag.value.days) for (const v of raw.slice(0, LIM.entriesDay)) { const e = normEntry(v); if (!e) { skipped++; continue; } e.day = day; entries.push(e); seen.add(e.th + "|" + e.t); }
  }
  const track = tr.ok ? tr.value : { aggFirstMs: 0, aggFirstDay: "", outFirstMs: 0 };
  let tailRead = tr.ok;
  if (tr.ok && track.outFirstMs) {
    const fromMs = nyMidnight(from), toMs = nyMidnight(addDays(to, 1)), zMs = track.aggFirstMs || Infinity;
    if (fromMs < zMs && toMs > track.outFirstMs) {                         // the window touches the tail
      const tl = await safe(readTail(ctx, track), "history");
      if (!tl.ok) { ib.errors.push("history: " + tl.error); tailRead = false; }
      else {
        if (tl.value.capped) ib.capped.push("history");
        for (const e of tl.value.entries) if (e.t >= fromMs && e.t < toMs && !seen.has(e.th + "|" + e.t)) { entries.push(Object.assign({}, e)); ib.usedTail = true; }   // (a copy: the cached tail is shared by every request)
      }
    }
  }
  if (!ag.ok && !tailRead) { const err = new Error("the inbox records could not be read"); err.unavailable = ib.errors.slice(0, 4); throw err; }
  entries.sort((a, b) => a.t - b.t);
  const firsts = [track.outFirstMs ? nyDay(track.outFirstMs) : "", track.aggFirstDay].filter(Boolean).sort();
  let knownFrom = firsts[0] || "";
  if (!knownFrom && entries.length) knownFrom = entries[0].day;            // (the first look found no record yet but entries exist: they are the record)
  if (!tr.ok) knownFrom = entries.length ? entries[0].day : "";            // (cannot say where records begin: only what was read is claimed as known)
  return { entries, knownFrom, aggFirstDay: track.aggFirstDay, skipped };
}

/* ── who sent it: usernames → the portal's person (display name, then the alias map) ── */
function peopleOf(ctx, entries, ops) {
  const { cleanName, okName, niceName, bestForm, nameKeyOf, canonOf } = need(), byUser = new Map(), people = new Map();
  const keyOfUser = by => {
    let k = byUser.get(by); if (k) return k;
    const op = ops.get(by), disp = op && okName(op.display) ? op.display : "", user = cleanName(by);
    k = nameKeyOf(ctx, disp || user); byUser.set(by, k);
    let P = people.get(k); if (!P) people.set(k, P = { key: k, forms: new Map(), users: new Set() });
    P.users.add(user); const f = disp || user; P.forms.set(f, (P.forms.get(f) || 0) + 1);
    return k;
  };
  const kindOf = e => (isAuto(e) ? "auto" : okName(cleanName(e.by)) ? "person" : "unknown");
  for (const e of entries) { e.k = kindOf(e); if (e.k === "person") e.who = keyOfUser(e.by); }
  const display = key => { const P = people.get(key); return canonOf(ctx, key) || (P ? niceName(bestForm(P.forms)) : ""); };
  const spellings = key => { const P = people.get(key); return P ? [...new Set([...P.forms.keys(), ...P.users])].slice(0, 12) : []; };
  return { keyOfUser, display, spellings, people };
}
/** Operator keys (everybody who has an inbox account, replies or not): `found` for a person with none in the window. */
function operatorKeys(ctx, ops) {
  const { okName, nameKeyOf } = need(), s = new Set();
  for (const [user, o] of ops) { if (o.revoked) continue; const n = okName(o.display) ? o.display : user; if (okName(n)) s.add(nameKeyOf(ctx, n)); }
  return s;
}

/* ── the numbers ── */
/** Customers' messages of a list of entries → the figures of one window or bucket. */
function figures(list, topN) {
  const orders = new Set(), custs = new Map(), threads = new Set(), days = new Set();
  let first = 0, last = 0, withoutOrder = 0;
  for (const e of list) {
    if (e.r) orders.add(e.r); else withoutOrder++;
    const ck = e.c || e.th || "?";
    let c = custs.get(ck); if (!c) custs.set(ck, c = { n: 0, label: "", labelAt: 0, orders: new Map(), last: 0 });
    c.n++; if (e.n && e.t >= c.labelAt) { c.label = e.n; c.labelAt = e.t; } if (e.t > c.last) c.last = e.t;
    if (e.r) c.orders.set(e.r, Math.max(c.orders.get(e.r) || 0, e.t));
    if (e.th) threads.add(e.th); days.add(e.day);
    if (!first || e.t < first) first = e.t; if (e.t > last) last = e.t;
  }
  const counts = [...custs.values()].map(c => c.n), sent = list.length, customers = custs.size;
  const top = [...custs.values()].sort((a, b) => b.n - a.n || b.last - a.last).slice(0, topN || 0).map(c => {
    const ids = [...c.orders].sort((a, b) => b[1] - a[1]).map(x => x[0]);
    return { customer: c.label || "Unknown customer", messages: c.n, replies: c.n, orders: ids.length, orderIds: ids.slice(0, 3), rid: ids[0] || "", lastAt: c.last };
  });
  const dist = [1, 2, 3, 4, 5].map(m => ({ messages: m, customers: counts.filter(n => (m === 5 ? n >= 5 : n === m)).length, ...(m === 5 ? { plus: true } : {}) }));
  return { sent, orders: orders.size, customers, conversations: threads.size, withoutOrder, activeDays: days.size, first: first || null, last: last || null,
    average: customers ? r1(sent / customers) : null, median: median(counts), max: counts.length ? Math.max(...counts) : null, top, dist };
}
const NOTHING = { sent: null, replies: null, messages: null, orders: null, customers: null, conversations: null, withoutOrder: null, activeDays: null, repliesPerActiveDay: null, firstAt: null, lastAt: null };
/** How many days of a window have records (from `knownFrom` on). */
const knownDaysOf = (win, knownFrom) => (knownFrom && knownFrom <= win.to ? spanLen(knownFrom > win.from ? knownFrom : win.from, win.to) : 0);
/** The summary S of one window for one group of entries (the window's entries only are looked at). */
function summary(list, win, knownFrom, topN) {
  const kd = knownDaysOf(win, knownFrom), base = { knownDays: kd, days: win.days };
  if (!kd) return Object.assign({}, NOTHING, base, { perCustomer: { average: null, median: null, max: null }, top: [] });
  const f = figures(list.filter(e => e.day >= win.from && e.day <= win.to), topN);
  return Object.assign({ sent: f.sent, replies: f.sent, messages: f.sent, orders: f.orders, customers: f.customers, conversations: f.conversations, withoutOrder: f.withoutOrder, activeDays: f.activeDays,
    repliesPerActiveDay: f.activeDays ? r1(f.sent / f.activeDays) : null, firstAt: f.first, lastAt: f.last }, base, { perCustomer: { average: f.average, median: f.median, max: f.max }, top: f.top });
}
/** The chart points of a window: a day each up to 92 days, Monday weeks beyond. Distinct orders and customers are counted INSIDE each bucket. */
function seriesOf(list, win, weekly, knownFrom) {
  const byDay = new Map();
  for (const e of list) { if (e.day < win.from || e.day > win.to) continue; let a = byDay.get(e.day); if (!a) byDay.set(e.day, a = []); a.push(e); }
  const buckets = [];
  if (!weekly) for (const d of daysOf(win.from, win.to)) buckets.push({ day: d, to: d, ds: [d] });
  else { let cur = null; for (const d of daysOf(win.from, win.to)) { const m = mondayOf(d); if (!cur || cur.mon !== m) { cur = { mon: m, day: d, to: d, ds: [] }; buckets.push(cur); } cur.to = d; cur.ds.push(d); } }
  return buckets.map(b => {
    const p = { day: b.day, to: b.to, days: b.ds.length };
    if (!knownFrom || b.to < knownFrom) return Object.assign(p, NOTHING);
    const items = []; for (const d of b.ds) { const a = byDay.get(d); if (a) items.push(...a); }
    const f = figures(items, 0);
    return Object.assign(p, { sent: f.sent, replies: f.sent, messages: f.sent, orders: f.orders, customers: f.customers, activeDays: f.activeDays,
      firstAt: b.ds.length === 1 ? f.first : null, lastAt: b.ds.length === 1 ? f.last : null });
  });
}
/** The wall-clock hour (0..23, New York) of a time on a day: plain arithmetic on a 24-hour day, the clock itself on a daylight-saving change. */
let hourFmt = null;
function hourOf(t, day) {
  const { nyMidnight, addDays } = need(), m0 = nyMidnight(day), m1 = nyMidnight(addDays(day, 1));
  if (m1 - m0 === DAY) return Math.max(0, Math.min(23, Math.floor((t - m0) / 3600000)));
  if (!hourFmt) hourFmt = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hourCycle: "h23", hour: "numeric" });
  return Number(hourFmt.format(new Date(t))) % 24;
}

/* ── the window of a request, the previous one, and the empty answer of the Sandbox ── */
const prevOf = rg => ({ from: need().addDays(rg.from, -rg.days), to: need().addDays(rg.from, -1), days: rg.days });
const flag = v => v === true || v === 1 || v === "1" || v === "true";
function winsOf(ctx, body) {
  const K_ = need(), names = Array.isArray(body.windows) ? body.windows.filter(w => WINDOWS.includes(w)) : WINDOWS;
  const rg = K_.resolveRange(ctx, body.range == null && !body.from && !body.to ? Object.assign({}, body, { range: "week" }) : body);
  if (rg.error) return { error: rg.error };
  const day = rg.to;
  const wins = {};
  for (const w of WINDOWS) if (names.includes(w) || w === rg.range) wins[w] = { from: K_.addDays(day, -(K_.RANGE_DAYS[w] - 1)), to: day, days: K_.RANGE_DAYS[w] };
  if (rg.range === "custom") wins.custom = { from: rg.from, to: rg.to, days: rg.days };
  return { rg, wins };
}
function emptyAnswer(ctx, extra) {
  return Object.assign({ ok: true, now: ctx.now, mode: "sandbox", today: ctx.today, found: false, knownFrom: "", notes: [SANDBOX_NOTE], reads: readsOf(ctx.ib) }, extra || {});
}
const noteLines = (data, win, ib) => {
  const notes = ["Counted: replies a person sent from the inbox (accepted into Etsy's send queue). AI drafts nobody sent are not counted, and replies the auto-pipeline sent are never a person's."];
  if (data.knownFrom && win.from < data.knownFrom) notes.push(`Sent-reply records begin on ${data.knownFrom}: the days of this window before it are unknown, not zero.`);
  if (!data.knownFrom) notes.push("No sent-reply record exists yet, so nothing is known: a dash, not a zero.");
  if (ib.usedTail) notes.push(`Replies before ${data.aggFirstDay || "the new daily record"} come from the learning store: their order and customer are what the conversation says today, and a reply with no operator name shows as unknown.`);
  return notes;
};
function finish(ctx, out, ib) {
  const errs = [...new Set(ib.errors)].slice(0, 8), capped = [...new Set(ib.capped)];
  if (errs.length || capped.length) { out.partial = true; if (errs.length) out.errors = errs; if (capped.length) out.notes.push("Some lists were cut at their size limit: " + capped.join(", ") + "."); }
  out.reads = readsOf(ib);
  return need().json(200, out);
}

/* ── op inbox: everybody, all windows ── */
async function opInbox(ctx, body) {
  const K_ = need();
  ctx.ib = newIb();
  const w = winsOf(ctx, body);
  if (w.error) return K_.json(400, { ok: false, error: w.error });
  const { rg, wins } = w, rkey = rg.range === "custom" ? "custom" : rg.range;
  if (ctx.prefix) return K_.json(200, emptyAnswer(ctx, { day: rg.to, range: rg.range, windows: wins, people: [], unknown: { windows: {}, series: [] }, auto: { windows: {}, series: [] }, team: { windows: {}, series: [] } }));
  const top = Math.max(1, Math.min(LIM.top, Math.floor(K_.num(body.top)) || LIM.topDefault));
  const from = Object.values(wins).reduce((m, x) => (x.from < m ? x.from : m), rg.to);
  const data = await load(ctx, from, rg.to), ops = await K_.safe(readOperators(ctx), "operators");
  if (!ops.ok) ctx.ib.errors.push("operators: " + ops.error);
  const P = peopleOf(ctx, data.entries, ops.ok ? ops.value : new Map());
  const weekly = rg.days > K_.DAILY_MAX, win = wins[rkey];
  const group = (list) => ({ windows: Object.fromEntries(Object.entries(wins).map(([k, x]) => [k, summary(list, x, data.knownFrom, top)])), series: seriesOf(list, win, weekly, data.knownFrom) });
  const persons = data.entries.filter(e => e.k === "person"), unknown = data.entries.filter(e => e.k === "unknown"), auto = data.entries.filter(e => e.k === "auto");
  const byKey = new Map(); for (const e of persons) { let a = byKey.get(e.who); if (!a) byKey.set(e.who, a = []); a.push(e); }
  const only = body.name != null && body.name !== "" ? K_.nameKeyOf(ctx, K_.cleanName(body.name)) : "";
  let people = [...byKey].filter(([k]) => !only || k === only).map(([k, list]) => Object.assign({ name: P.display(k), spellings: P.spellings(k) }, group(list)));
  people.sort((a, b) => (b.windows[rkey].sent || 0) - (a.windows[rkey].sent || 0) || (a.name < b.name ? -1 : 1));
  const out = { ok: true, now: ctx.now, mode: "real", day: rg.to, today: ctx.today, range: rg.range, windows: wins, granularity: weekly ? "week" : "day", knownFrom: data.knownFrom,
    people, unknown: group(unknown), auto: group(auto), team: group(persons.concat(unknown)), notes: noteLines(data, { from }, ctx.ib) };
  if (data.skipped) out.notes.push(`${data.skipped} stored entries were unreadable and skipped.`);
  return finish(ctx, out, ctx.ib);
}

/* ── op personInbox: one person, IN2's shape ── */
const DEFS = {
  replies: ["Replies sent", "Replies this person sent from the inbox: each reply that went into Etsy's send queue under their name. AI drafts nobody sent are not counted, and replies the auto-pipeline sent are not counted for a person."],
  orders: ["Orders covered", "Different orders those replies are about (the order number stored on the conversation). A conversation with no order number adds none."],
  customers: ["Customers written to", "Different customers those replies went to, one per Etsy buyer: a customer who got five messages counts once."],
  messages: ["Messages sent", "Messages this person sent to customers. One reply is one Etsy message, so this equals replies sent."],
  messagesPerCustomer: ["Messages per customer", "Messages sent divided by different customers: how many messages the average customer got from this person."],
  maxPerCustomer: ["Most to one customer", "The most messages one customer got from this person in the window."],
  repliesPerDay: ["Replies per active day", "Replies sent divided by the days with at least one reply."],
  daysActive: ["Days with replies", "Days of the window on which this person sent at least one reply."]
};
function metric(key, cur, prev, partialWhy) {
  const x = fin(cur), p = fin(prev), d = DEFS[key];
  const o = { label: d[0], unit: "count", value: x, prev: p, delta: x != null && p != null ? r1(x - p) : null, deltaPct: x != null && p != null && p !== 0 ? r1((x - p) / Math.abs(p) * 100) : null, better: null, def: d[1], estimated: !!partialWhy };
  if (partialWhy) o.why = partialWhy;
  return o;
}
async function opPersonInbox(ctx, body) {
  const K_ = need();
  ctx.ib = newIb();
  const name = K_.cleanName(body.name);
  if (!K_.okName(name)) return K_.json(400, { ok: false, error: "name required" });
  const rg = K_.resolveRange(ctx, body);
  if (rg.error) return K_.json(400, { ok: false, error: rg.error });
  const prev = flag(body.compare) ? prevOf(rg) : null, win = { from: rg.from, to: rg.to, days: rg.days };
  let key = K_.nameKeyOf(ctx, name);
  if (ctx.prefix) return K_.json(200, emptyAnswer(ctx, { name: K_.canonOf(ctx, key) || K_.niceName(name), range: rg.range, from: rg.from, to: rg.to, days: rg.days, live: rg.to === ctx.today, prev, series: [], hours: [], totals: {}, unknown: { replies: null, messages: null } }));
  const topN = Math.max(1, Math.min(LIM.topPersonMax, Math.floor(K_.num(body.top)) || LIM.topPerson));
  const data = await load(ctx, prev ? prev.from : rg.from, rg.to), ops = await K_.safe(readOperators(ctx), "operators");
  if (!ops.ok) ctx.ib.errors.push("operators: " + ops.error);
  const opsMap = ops.ok ? ops.value : new Map();
  const acct = [...opsMap].find(([u, o]) => u.toLowerCase() === name.toLowerCase() && K_.okName(o.display));      // (the inbox account name works as a spelling too)
  if (acct) key = K_.nameKeyOf(ctx, acct[1].display);
  const P = peopleOf(ctx, data.entries, opsMap);
  const mine = data.entries.filter(e => e.k === "person" && e.who === key);
  const found = mine.length > 0 || (ops.ok && operatorKeys(ctx, opsMap).has(key));
  const kf = found ? data.knownFrom : "";                                   // (a person with no inbox account and no reply: dashes, never zeros)
  const sNow = summary(mine, win, kf, topN), sPrev = prev ? summary(mine, prev, kf, 0) : null;
  const why = sNow.knownDays && sNow.knownDays < rg.days ? `Counted from ${data.knownFrom}: the earlier days of this window have no record.` : "";
  const g = (k, pick) => metric(k, sNow.knownDays ? pick(sNow) : null, sPrev && sPrev.knownDays ? pick(sPrev) : null, why);
  const totals = {
    replies: g("replies", s => s.sent), orders: g("orders", s => s.orders), customers: g("customers", s => s.customers), messages: g("messages", s => s.sent),
    messagesPerCustomer: g("messagesPerCustomer", s => s.perCustomer.average), maxPerCustomer: g("maxPerCustomer", s => s.perCustomer.max),
    repliesPerDay: g("repliesPerDay", s => s.repliesPerActiveDay), daysActive: g("daysActive", s => s.activeDays) };
  const weekly = rg.days > K_.DAILY_MAX, inWin = mine.filter(e => e.day >= rg.from && e.day <= rg.to);
  const series = seriesOf(mine, win, weekly, kf);
  const hours = Array.from({ length: 24 }, (_, h) => ({ hour: h, replies: sNow.knownDays ? 0 : null, messages: sNow.knownDays ? 0 : null }));
  if (sNow.knownDays) for (const e of inWin) { const h = hourOf(e.t, e.day); hours[h].replies++; hours[h].messages++; }
  const f = figures(inWin, topN);
  const unk = data.entries.filter(e => e.k === "unknown" && e.day >= rg.from && e.day <= rg.to).length;
  const out = { ok: true, now: ctx.now, mode: "real", name: (P.people.has(key) ? P.display(key) : "") || K_.canonOf(ctx, key) || K_.niceName(name),
    found, spellings: P.spellings(key), range: rg.range, from: rg.from, to: rg.to, days: rg.days, today: ctx.today, live: rg.to === ctx.today, prev,
    granularity: weekly ? "week" : "day", knownFrom: data.knownFrom, totals, series, hours,
    perCustomer: sNow.knownDays ? { average: f.average, median: f.median, max: f.max, total: f.customers, distribution: f.dist, top: f.top } : { average: null, median: null, max: null, total: null, distribution: [], top: [] },
    unknown: { replies: sNow.knownDays ? unk : null, messages: sNow.knownDays ? unk : null }, notes: noteLines(data, win, ctx.ib) };
  if (data.skipped) out.notes.push(`${data.skipped} stored entries were unreadable and skipped.`);
  if (!found) out.notes.push(`No inbox account or reply is recorded for ${out.name}.`);
  return finish(ctx, out, ctx.ib);
}

/* ── the board's block: today, all operators (people + unknown, never auto) ── */
async function today(ctx) {
  const K_ = need();
  if (ctx.prefix) return null;
  ctx.ib = newIb();
  const data = await load(ctx, ctx.today, ctx.today), ops = await K_.safe(readOperators(ctx), "operators");
  const P = peopleOf(ctx, data.entries, ops.ok ? ops.value : new Map());
  const list = data.entries.filter(e => (e.k === "person" || e.k === "unknown") && e.day === ctx.today);
  const f = figures(list, 0), byKey = new Map();
  for (const e of list) if (e.k === "person") { let a = byKey.get(e.who); if (!a) byKey.set(e.who, a = []); a.push(e); }
  const byPerson = [...byKey].map(([k, l]) => { const g = figures(l, 0); return { name: P.display(k), replies: g.sent, orders: g.orders, customers: g.customers, messages: g.sent }; })
    .sort((a, b) => b.replies - a.replies || (a.name < b.name ? -1 : 1));
  const known = !!data.knownFrom && data.knownFrom <= ctx.today;
  return { day: ctx.today, replies: known ? f.sent : null, orders: known ? f.orders : null, customers: known ? f.customers : null, messages: known ? f.sent : null,
    unknown: known ? list.filter(e => e.k === "unknown").length : null, byPerson: known ? byPerson : [] };
}

/* ── personOrders with station "inbox": the orders a person SENT a reply on in the window ── */
async function ordersOf(ctx, o) {
  const K_ = need();
  if (ctx.prefix) return { orders: [], byRid: new Map(), knownFrom: "" };
  ctx.ib = newIb();
  const data = await load(ctx, o.from, o.to), ops = await K_.safe(readOperators(ctx), "operators");
  peopleOf(ctx, data.entries, ops.ok ? ops.value : new Map());
  const by = new Map();
  for (const e of data.entries) {
    if (e.k !== "person" || e.who !== o.key || !e.r || e.day < o.from || e.day > o.to) continue;
    let x = by.get(e.r); if (!x) by.set(e.r, x = { rid: e.r, days: new Set(), latest: "", replies: 0, messages: 0, lastAt: 0 });
    x.days.add(e.day); x.replies++; x.messages++; if (e.day > x.latest) x.latest = e.day; if (e.t > x.lastAt) x.lastAt = e.t;
  }
  return { orders: [...by.values()], byRid: by, knownFrom: data.knownFrom, errors: ctx.ib.errors.slice(0, 4), reads: readsOf(ctx.ib) };
}

module.exports = { bind, opInbox, opPersonInbox, today, ordersOf, COLL, LIM, TTL, WINDOWS, _t: { normEntry, figures, summary, seriesOf, hourOf, peopleOf, load, isAuto } };
