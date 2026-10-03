/*  netlify/functions/employeeEfficiency.js
 *  ─────────────────────────────────────────────────────────────────────────────
 *  The READ side of the sorter's Employee efficiency console (Paul, 2 Oct 2026): who signed in and out, at which
 *  stations, how many parts they scanned and produced, throughput by hour, which orders they worked.
 *  Contract: plans/employee-efficiency/contract.md, sections 2 (data model) and "Console API" (this function's answers).
 *
 *  Reads, never writes:
 *    Efficiency_Daily/{day}__{person}   the per-person daily rollups the stations' activity door keeps (real parts, scans)
 *    Station_Activity/{id}              single events: the live feed, a person's recent orders, one order's history
 *    Station_Sessions/{id}              sign-in sessions: first login, lock-out, signed-in minutes, who is on now
 *    Order_Timeline/{orderId}~…         the order seals: HISTORY from before the activity events began (orders, scans,
 *                                       label prints with a person and a station; no part counts)
 *  (sandbox: true reads the Sandbox_ copies of all four, never mixing them with production.)
 *
 *  THE GATE. Station_Activity and Efficiency_Daily are not readable through the open station door; this function is
 *  their only reader, and it answers only a request whose body carries `key` = the manager passcode: the same one as the
 *  Ads console (_editPasscode.js: Netlify EDIT_PASSCODE, else Firestore config/editPasscode field passcode). Compared in
 *  constant time; the passcode is never read here beyond that helper, and never returned, logged or stored. A wrong or
 *  missing key is a 401 with no data; ten wrong keys a minute from one address is a 429.
 *
 *  COST. A full overview is: the rollups of the range (a day's cache is shared, a past day is kept 10 minutes), one
 *  sessions query, the newest 500 events (kept in the instance and only topped up afterwards), and order seals only for
 *  a day that has people without a rollup (at most 10 days, 2,000 each). Every read is capped and says so
 *  (partial: true) when it hit its cap. Answers are cached ~5 s per instance.
 *  ───────────────────────────────────────────────────────────────────────────── */
"use strict";
const EP = require("./_editPasscode");
const { CORS, parseBody, num } = require("./_charmNestAuth");
const admin = require("./firebaseAdmin");
const db = admin.firestore();

const COL = { activity: "Station_Activity", rollup: "Efficiency_Daily", sessions: "Station_Sessions", seals: "Order_Timeline" };
const STATIONS = ["sorting", "welding", "assembly", "shipping", "design", "laser", "sorter", "qr", "inbox"];
const CORE = ["sorting", "welding", "assembly", "shipping"];
const DAY_MS = 86400000, GONE_MS = 15 * 60000, ACTIVE_GAP_MS = 5 * 60000;
const TTL_LIVE = 5000, TTL_PAST = 10 * 60000, TTL_SEALS_LIVE = 120000;
const LIM = { sessions: 1500, rollups: 2000, sealsDay: 2000, sealDays: 10, window: 500, dayEvents: 1500, orderEvents: 500, orderSeals: 400,
  feed: 40, feedDelta: 200, orders: 30, people: 60, orderEventsOut: 300, body: 8000 };
const FAILS_PER_MIN = 10;

/* ── small helpers ── */
const json = (statusCode, body) => ({ statusCode, headers: CORS, body: JSON.stringify(body) });
const ms = v => v == null ? 0 : typeof v.toMillis === "function" ? v.toMillis() : v instanceof Date ? v.getTime() : Number.isFinite(+v) ? +v : 0;
const r1 = x => Math.round(x * 10) / 10;
const shown = x => Math.max(0, r1(x));                              // an hour that nets below zero (an undo in a later hour) is drawn as 0
const zeros = n => new Array(n).fill(0);
const digits = (v, n = 30) => String(v == null ? "" : v).replace(/\D/g, "").slice(0, n);
const cleanName = v => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, 80);
const okName = n => !!n && /\p{L}/u.test(n);                       // a name with no letter ("123456", "123 456") is a PIN, never data
/* One person, however the logins spelled the name: strip accents, case-fold, drop apostrophes, and treat every other
   punctuation mark (underscore, period, hyphen, comma ...) as a space, so "Michael_V" (the PIN list), "Michael V." (typed at
   the design pages or the sorter), "michael v" and "MICHAEL  V" are ONE person; then the alias map (below). The initial still
   counts: "Michael V" and "Michael T" stay two people, and "Giovanna C." joins "Giovanna" only through the alias map. */
const fold = n => cleanName(n).normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase()
  .replace(/['\u2018\u2019`\u00b4]/g, "").replace(/[^\p{L}\p{N}]+/gu, " ").trim();
/* How a name reads on the screen: underscores become spaces, a lone initial gets its dot ("Michael_V" → "Michael V.",
   "Ana_M" → "Ana M.", "Giovanna C." stays), and a name that is ALL lower or ALL upper case gets capitals ("MICHAEL V" →
   "Michael V."). Display only: what the stations store and send is never changed, and an alias's own spelling is shown as written. */
function niceName(raw) {
  const s = cleanName(raw).replace(/_+/g, " ").replace(/\s+/g, " ").trim();
  if (!s) return "";
  let words = s.split(" ");
  if (/\p{L}{3}/u.test(s) && (s === s.toLowerCase() || s === s.toUpperCase()))      // (a two-letter "JJ" is left alone)
    words = words.map(w => w.toLowerCase().replace(/(^|[-'\u2019.])(\p{L})/gu, (_, a, b) => a + b.toUpperCase()));
  if (words.length > 1) words = words.map(w => /^\p{L}$/u.test(w) ? w + "." : w);
  return words.join(" ");
}
// seeded aliases (display name → other spellings); the Firestore doc config/employeeAliases adds to these, never written here
const BUILTIN_ALIASES = { "Giovanna": ["Giovanna C."] };
function buildAliases(extra) {
  const display = new Map(), map = new Map();           // canonical key → display name · folded alias → canonical key
  const add = (name, list) => {
    const d = cleanName(name); if (!okName(d)) return;
    const ck = fold(d); if (!display.has(ck)) display.set(ck, d);
    if (!map.has(ck)) map.set(ck, ck);
    for (const a of Array.isArray(list) ? list.slice(0, 50) : []) { const an = cleanName(a); if (okName(an)) map.set(fold(an), ck); }
  };
  for (const [k, v] of Object.entries(BUILTIN_ALIASES)) add(k, v);
  if (extra && typeof extra === "object") for (const [k, v] of Object.entries(extra).slice(0, 200)) add(k, v);
  return { display, map };
}
function nameKeyOf(ctx, name) {
  let k = fold(name); const m = ctx.aliases && ctx.aliases.map;
  if (m) for (let i = 0; i < 3 && m.has(k) && m.get(k) !== k; i++) k = m.get(k);
  return k;
}
/** The name the console shows for a person: the alias map's own spelling, else the most used (mixed case preferred) of the spellings seen. */
const canonOf = (ctx, key) => (ctx.aliases && ctx.aliases.display.get(key)) || "";
function bestForm(forms) {
  let best = "", score = -1;
  for (const [form, n] of forms) { const v = n * 2 + (form !== form.toLowerCase() && form !== form.toUpperCase() ? 1 : 0); if (v > score || (v === score && form < best)) { best = form; score = v; } }
  return best;
}
const GENERIC_BY = /^(etsy|system|unknown|n\/a|none)$/i;
// what a detail may show: no digits-only text, no lone 6-digit number (a PIN) (the activity door already does this, the same way; again here)
const scrub = v => { const s = String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, 120); return /^\d+$/.test(s) ? "" : s.replace(/(?<!\d)\d{6}(?!\d)/g, "[#]"); };
const DAY_RE = /^\d{4}-\d{2}-\d{2}$/;

/* ── New York days (the shop's midnight, daylight saving included) ── */
const partsFmt = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hourCycle: "h23", year: "numeric", month: "numeric", day: "numeric", hour: "numeric", minute: "numeric", second: "numeric" });
function nyParts(t) { const o = {}; for (const p of partsFmt.formatToParts(new Date(t))) if (p.type !== "literal") o[p.type] = +p.value; return o; }
const pad = n => String(n).padStart(2, "0");
const nyDay = t => { const p = nyParts(t); return `${p.year}-${pad(p.month)}-${pad(p.day)}`; };
const midnights = new Map();
function nyMidnight(day) {
  let t = midnights.get(day); if (t != null) return t;
  const [y, m, d] = day.split("-").map(Number), want = Date.UTC(y, m - 1, d); t = want;
  for (let i = 0; i < 3; i++) { const p = nyParts(t); t += want - Date.UTC(p.year, p.month - 1, p.day, p.hour % 24, p.minute, p.second); }
  if (midnights.size > 400) midnights.clear();
  midnights.set(day, t); return t;
}
const addDays = (day, n) => { const [y, m, d] = day.split("-").map(Number); return new Date(Date.UTC(y, m - 1, d + n)).toISOString().slice(0, 10); };
const validDay = s => DAY_RE.test(s) && addDays(s, 0) === s;
function dayList(from, to) { const out = []; for (let d = from, i = 0; d <= to && i < 100; d = addDays(d, 1), i++) out.push(d); return out; }

/* ── caches (per warm instance, one set per Firestore handle) ── */
const caches = new WeakMap();
function cacheOf(handle) { let c = caches.get(handle); if (!c) { c = { memo: new Map(), recent: new Map(), fails: new Map() }; caches.set(handle, c); } return c; }
/** A shared, time-limited read. A failed read is forgotten at once. */
function cached(ctx, key, ttl, fn) {
  ttl = Math.min(ttl, ctx.life);
  const memo = ctx.cache.memo, hit = memo.get(key);
  if (hit && ctx.now - hit.at < Math.min(ttl, hit.ttl)) return hit.p;   // (an entry read while its day was live stays short-lived after midnight)
  const entry = { at: ctx.now, ttl, p: null };
  entry.p = Promise.resolve().then(fn);
  memo.set(key, entry);
  entry.p.catch(() => { if (memo.get(key) === entry) memo.delete(key); });
  if (memo.size > 400) for (const [k, e] of memo) { if (ctx.now - e.at > TTL_PAST) memo.delete(k); }
  return entry.p;
}
/** config/employeeAliases {"<Display name>": ["alias", ...]}: read-only, kept a minute, shared with every viewer. */
function loadAliases(ctx) {
  return cached(ctx, "aliases", 60000, async () => {
    const snap = await ctx.db.collection("config").doc("employeeAliases").get();
    return buildAliases(snap.exists ? snap.data() : null);
  });
}
const safe = (p, label) => p.then(value => ({ ok: true, value }), e => ({ ok: false, label, error: String((e && (e.message || e.code)) || e || "failed").slice(0, 160) }));

/* ── the reads ── */
function ctxOf(body, handle) {
  const now = Date.now(), sandbox = body.sandbox === true || body.sandbox === 1 || body.sandbox === "1";
  // The sandbox is a small rehearsal that Reset empties, and a warm instance outlives the reset: what it kept (a past day's rollups
  // and sign-ins for 10 minutes, the seals of today for 2, the first day of activity for an hour, the newest events for good) showed
  // the records of before in the console after the reset (Paul, 3 Oct: "it still didn't purge the system fully"). So the sandbox's
  // reads are kept 5 s at most and its newest events are read afresh each time; production's keep their long lives.
  return { db: handle || db, admin, prefix: sandbox ? "Sandbox_" : "", life: sandbox ? TTL_LIVE : Infinity, now, today: nyDay(now), cache: cacheOf(handle || db) };
}
const col = (ctx, name) => ctx.db.collection(ctx.prefix + name);

/** Rollups of the days from..to, a day at a time in the cache (today 5 s, a past day 10 min), one range query for the stale ones. */
async function readRollups(ctx, from, to) {
  const days = dayList(from, to), stale = [], entries = new Map();
  for (const d of days) {
    const e = ctx.cache.memo.get(`roll|${ctx.prefix}|${d}`);
    if (e && ctx.now - e.at < Math.min(d === ctx.today ? TTL_LIVE : TTL_PAST, e.ttl)) entries.set(d, e.p); else stale.push(d);
  }
  if (stale.length) {
    const a = stale[0], z = stale[stale.length - 1];
    const fetchP = col(ctx, COL.rollup).where("day", ">=", a).where("day", "<=", z).limit(LIM.rollups + 1).get().then(snap => {
      const by = new Map(), docs = snap.docs.slice(0, LIM.rollups);
      for (const x of docs) { const v = x.data() || {}; if (typeof v.day === "string" && !!v.sandbox === !!ctx.prefix) { if (!by.has(v.day)) by.set(v.day, []); by.get(v.day).push(v); } }
      return { by, capped: snap.docs.length > LIM.rollups };
    });
    for (const d of stale) {
      const entry = { at: ctx.now, ttl: Math.min(d === ctx.today ? TTL_LIVE : TTL_PAST, ctx.life), p: fetchP.then(r => ({ docs: r.by.get(d) || [], capped: r.capped })) };
      ctx.cache.memo.set(`roll|${ctx.prefix}|${d}`, entry);
      entry.p.catch(() => { if (ctx.cache.memo.get(`roll|${ctx.prefix}|${d}`) === entry) ctx.cache.memo.delete(`roll|${ctx.prefix}|${d}`); });
      entries.set(d, entry.p);
    }
  }
  const out = { docs: [], capped: false };
  for (const d of days) { const r = await entries.get(d); out.docs.push(...r.docs); out.capped = out.capped || r.capped; }
  return out;
}

/** Sessions that started in [fromMs, toMs): the server's times are ms or Firestore times, and a range matches only one kind, so both are read. */
function readSessions(ctx, fromMs, toMs) {
  return cached(ctx, `sess|${ctx.prefix}|${fromMs}|${toMs}`, toMs > nyMidnight(ctx.today) ? TTL_LIVE : TTL_PAST, async () => {
    const TS = ctx.admin.firestore.Timestamp, ranges = [[fromMs, toMs]];
    if (TS && typeof TS.fromMillis === "function") ranges.push([TS.fromMillis(fromMs), TS.fromMillis(toMs)]);
    const snaps = await Promise.all(ranges.map(([a, z]) => col(ctx, COL.sessions).where("startAt", ">=", a).where("startAt", "<", z).orderBy("startAt", "desc").limit(LIM.sessions + 1).get()));
    const seen = new Set(), rows = []; let truncated = false;
    for (const s of snaps) { if (s.docs.length > LIM.sessions) truncated = true; for (const d of s.docs.slice(0, LIM.sessions)) if (!seen.has(d.id)) { seen.add(d.id); rows.push(Object.assign({ id: d.id }, d.data() || {})); } }
    return { rows, truncated };
  });
}

/** The first day any rollup exists (when the activity events began); kept an hour. "" when there are none. */
function readEventsStart(ctx) {
  return cached(ctx, `start|${ctx.prefix}`, 3600000, async () => {
    const snap = await col(ctx, COL.rollup).orderBy("day", "asc").limit(1).get();
    const d = snap.docs[0] && (snap.docs[0].data() || {}).day;
    return typeof d === "string" && validDay(d) ? d : "";
  });
}
const SEAL_PRINT = new Set(["labelPrinted", "sealPrinted", "qrLabel"]);
const STATION_SET = new Set(STATIONS);
/** One seal as history, or null when it is not a person's work at a station. */
function sealRow(d) {
  const at = ms(d.at), station = String(d.station || ""), person = cleanName(d.by), type = String(d.type || "");
  if (!(at > 0) || !STATION_SET.has(station) || !okName(person) || GENERIC_BY.test(person) || /^(etsy|system)$/.test(String(d.source || ""))) return null;
  const kind = type === "scan" ? "scan" : SEAL_PRINT.has(type) ? "print" : d.milestone === true ? "complete" : "";
  return kind ? { at, person, station, type, kind, orderId: digits(d.orderId) } : null;
}
/** The seals of one New York day (cached; today 2 min, a past day 10 min). */
function readSealDay(ctx, day) {
  return cached(ctx, `seal|${ctx.prefix}|${day}`, day === ctx.today ? TTL_SEALS_LIVE : TTL_PAST, async () => {
    const snap = await col(ctx, COL.seals).where("at", ">=", nyMidnight(day)).where("at", "<", nyMidnight(addDays(day, 1))).orderBy("at", "desc").limit(LIM.sealsDay + 1).get();
    const rows = []; for (const d of snap.docs.slice(0, LIM.sealsDay)) { const r = sealRow(d.data() || {}); if (r) rows.push(r); }
    return { rows, capped: snap.docs.length > LIM.sealsDay };
  });
}

/** An event as the console uses it, or null (not this store's, no person). */
function eventRow(id, d, ctx) {
  const person = cleanName(d.person), action = String(d.action || "");
  if (!okName(person) || !action || !!d.sandbox !== !!ctx.prefix) return null;
  const at = ms(d.at), tsMs = ms(d.ts), serverAt = ms(d.serverAt) || tsMs || at;
  // the feed's order and cursor follow the COMMIT time (ts): serverAt is read before the write, and a transaction that waits
  // for a retry commits later, so a poll could pass an event whose serverAt is older than its cursor and never show it
  return { id: String(d.id || id).slice(0, 100), at, k: tsMs || serverAt, tsMs: tsMs || serverAt, person, station: String(d.station || ""), device: String(d.device || "").slice(0, 40), action,
    orderId: digits(d.orderId), parts: Math.max(0, Math.floor(num(d.parts))), detail: scrub(d.detail), sincePrevMs: Math.max(0, num(d.sincePrevMs)), day: typeof d.day === "string" ? d.day : "" };
}
const byNewest = (a, b) => b.k - a.k || (a.id < b.id ? 1 : a.id > b.id ? -1 : 0);
/** The newest events of everybody, kept in the instance: loaded once (newest 500), then only topped up. */
function readRecent(ctx) {
  let R = ctx.cache.recent.get(ctx.prefix);
  if (!R) { R = { events: [], loaded: false, at: 0, newest: 0, p: null }; ctx.cache.recent.set(ctx.prefix, R); }
  if (R.p) return R.p;
  if (R.loaded && ctx.now - R.at < TTL_LIVE) return Promise.resolve(R.events);
  R.p = (async () => {
    try {
      const TS = ctx.admin.firestore.Timestamp, c = col(ctx, COL.activity);
      let snap = null;
      if (R.loaded && !ctx.prefix && TS && typeof TS.fromMillis === "function") {   // (the sandbox is not topped up: events a reset deleted would stay in the list)
        snap = await c.where("ts", ">=", TS.fromMillis(Math.max(0, R.newest - 5000))).orderBy("ts", "asc").limit(LIM.window).get();
        if (snap.docs.length >= LIM.window) snap = null;                     // a gap too big to top up: load the newest again
      }
      const fresh = !snap;
      if (!snap) snap = await c.orderBy("ts", "desc").limit(LIM.window).get();
      const byId = new Map(fresh ? [] : R.events.map(e => [e.id, e]));
      for (const d of snap.docs) { const e = eventRow(d.id, d.data() || {}, ctx); if (e) byId.set(e.id, e); }
      R.events = [...byId.values()].sort(byNewest).slice(0, LIM.window);
      R.newest = R.events.reduce((m, e) => Math.max(m, e.tsMs), 0);
      R.loaded = true; R.at = Date.now();
      return R.events;
    } finally { R.p = null; }
  })();
  return R.p;
}
/** One past day's events (cached 10 min), for that day's feed and order lists. */
function readDayEvents(ctx, day) {
  return cached(ctx, `dayev|${ctx.prefix}|${day}`, TTL_PAST, async () => {
    const snap = await col(ctx, COL.activity).where("day", "==", day).limit(LIM.dayEvents + 1).get();
    const rows = []; for (const d of snap.docs.slice(0, LIM.dayEvents)) { const e = eventRow(d.id, d.data() || {}, ctx); if (e) rows.push(e); }
    return { rows: rows.sort(byNewest), capped: snap.docs.length > LIM.dayEvents };
  });
}

/* ── time arithmetic: sessions → spans clipped to New York days ── */
function spanOf(s, now) {
  const start = ms(s.startAt); if (!(start > 0)) return null;
  const last = Math.max(start, ms(s.lastSeenAt) || start);
  let end = ms(s.endAt), live = false;
  if (!end) { if (now - last >= GONE_MS) end = last; else { end = now; live = true; } }
  end = Math.min(end, now); if (end < start) end = start;
  return { id: String(s.id || ""), station: String(s.station || ""), start, end, live, last };
}
/** [{day, s, e}] pieces of [s, e] inside from..to; a span that ends exactly at midnight puts nothing on the next day. */
function clip(s, e, from, to) {
  const out = []; let d = nyDay(s);
  for (let i = 0; i < 45; i++) {
    const ds = nyMidnight(d), de = nyMidnight(addDays(d, 1)), a = Math.max(s, ds), z = Math.min(e, de);
    if (d >= from && d <= to && (i === 0 ? z >= a : z > a)) out.push({ day: d, s: a, e: z });
    if (e <= de || d >= to) break;
    d = addDays(d, 1);
  }
  return out;
}
/** Milliseconds covered by [s, e] pairs, an overlap counted once (one person on two computers). */
function covered(spans) {
  let total = 0, a = -Infinity, z = -Infinity;
  for (const [s, e] of spans.slice().sort((x, y) => x[0] - y[0])) { if (s > z) { if (z > a) total += z - a; a = s; z = e; } else z = Math.max(z, e); }
  if (z > a) total += z - a;
  return total;
}

/* ── the people: rollups + sessions + seals, per person per day ── */
const tmpl = () => ({ scans: 0, scanParts: 0, completes: 0, parts: 0, orders: 0, prints: 0, rejects: 0, errors: 0, notes: 0, undos: 0, undoParts: 0, undoOrders: 0, activeMs: 0, idleMs: 0 });
const KEYS = Object.keys(tmpl());
const newPD = day => ({ day, src: "", st: {}, hours: zeros(24), hs: {}, orders: new Map(), spans: [], inFirst: 0, inLast: 0, signedMs: 0, rawMs: 0, stMs: {}, firstIn: 0, lastOut: 0, liveSpan: false });
const stAgg = (pd, st) => pd.st[st] || (pd.st[st] = tmpl());
const addOrder = (pd, orderId, st) => { if (!orderId) return; let s = pd.orders.get(orderId); if (!s) pd.orders.set(orderId, s = new Set()); s.add(st); };
const addHour = (pd, st, h, n) => { if (!(h >= 0 && h < 24) || !n) return; pd.hours[h] += n; (pd.hs[st] || (pd.hs[st] = zeros(24)))[h] += n; };

function personsOf(ctx) {
  const people = new Map();
  const get = raw => {
    const name = cleanName(raw); if (!okName(name)) return null;
    const key = nameKeyOf(ctx, name); let P = people.get(key);
    if (!P) people.set(key, P = { key, canon: canonOf(ctx, key), forms: new Map(), days: new Map(), live: [] });
    P.forms.set(name, (P.forms.get(name) || 0) + 1);
    return P;
  };
  const pd = (P, day) => { let x = P.days.get(day); if (!x) P.days.set(day, x = newPD(day)); return x; };
  return { people, get, pd };
}
const displayName = P => P.canon || niceName(bestForm(P.forms));

/** Reads and joins everything for the days winFrom..toDay. Returns the people (per day) and what could not be read. */
async function assemble(ctx, winFrom, toDay) {
  const info = { errors: [], capped: [], sealsLeftOut: 0 };
  const winStart = nyMidnight(winFrom), winEnd = nyMidnight(addDays(toDay, 1));
  const [rr, sr, st0] = await Promise.all([safe(readRollups(ctx, winFrom, toDay), "rollups"), safe(readSessions(ctx, winStart - DAY_MS, winEnd), "sessions"), safe(readEventsStart(ctx), "start")]);
  if (ctx.aliasError) info.errors.push("aliases: " + ctx.aliasError);
  if (!rr.ok) info.errors.push("rollups: " + rr.error);
  if (!sr.ok) info.errors.push("sessions: " + sr.error);
  if (!rr.ok && !sr.ok) { const e = new Error("both reads failed"); e.unavailable = info.errors; throw e; }
  const P = personsOf(ctx);
  // 1 · rollups (the events' own numbers)
  const eventDays = new Set();
  if (rr.ok) {
    if (rr.value.capped) info.capped.push("rollups");
    for (const x of rr.value.docs) {
      const person = P.get(x.person); if (!person || !validDay(x.day)) continue;
      eventDays.add(x.day);
      const pd = P.pd(person, x.day); pd.src = "events";
      for (const [st, v] of Object.entries(x.stations && typeof x.stations === "object" ? x.stations : {})) {
        if (!/^[a-z][\w-]{0,19}$/.test(st) || !v || typeof v !== "object") continue;
        const a = stAgg(pd, st); for (const k of KEYS) a[k] += Math.max(0, num(v[k]));
        const fa = ms(v.firstAt), la = ms(v.lastAt);
        if (fa > 0 && (!pd.inFirst || fa < pd.inFirst)) pd.inFirst = fa;
        if (la > pd.inLast) pd.inLast = la;
      }
      for (const [hh, h] of Object.entries(x.hours && typeof x.hours === "object" ? x.hours : {})) {
        const hr = parseInt(hh, 10); if (!(hr >= 0 && hr < 24) || !h || typeof h !== "object") continue;
        pd.hours[hr] += Math.max(0, num(h.parts)) - Math.max(0, num(h.undoParts));          // produced minus undone, the same net as the totals
        for (const [st, b] of Object.entries(h.by && typeof h.by === "object" ? h.by : {})) if (b && typeof b === "object") (pd.hs[st] || (pd.hs[st] = zeros(24)))[hr] += Math.max(0, num(b.parts)) - Math.max(0, num(b.undoParts));
      }
      for (const [id, m] of Object.entries(x.touched && typeof x.touched === "object" ? x.touched : {})) {
        const oid = digits(id); if (!oid) continue;
        const stations = m && typeof m === "object" ? Object.keys(m).filter(k => m[k]) : [];
        let set = pd.orders.get(oid); if (!set) pd.orders.set(oid, set = new Set()); stations.forEach(s => set.add(s));
      }
      const fa = ms(x.firstAt), la = ms(x.lastAt);
      if (fa > 0 && (!pd.inFirst || fa < pd.inFirst)) pd.inFirst = fa;
      if (la > pd.inLast) pd.inLast = la;
    }
  }
  // 2 · sessions, clipped to New York days (open ones count to now)
  const sessionDays = new Map();    // day → Set(person key) that signed in that day
  if (sr.ok) {
    if (sr.value.truncated) info.capped.push("sessions");
    for (const s of sr.value.rows) {
      const person = P.get(s.person), sp = spanOf(s, ctx.now); if (!person || !sp) continue;
      if (sp.live) person.live.push(sp);
      for (const c of clip(sp.start, sp.end, winFrom, toDay)) {
        const pd = P.pd(person, c.day);
        pd.spans.push({ s: c.s, e: c.e, station: sp.station, live: sp.live, start0: sp.start });
        if (sp.live) pd.liveSpan = true;
        if (!sessionDays.has(c.day)) sessionDays.set(c.day, new Set()); sessionDays.get(c.day).add(person.key);
      }
    }
  }
  // 3 · seals, for the days that have a person without a rollup (newest 10 such days)
  // History = a day before the events began (every day, when none have): its seals are read. After that, only a day with
  // somebody signed in who has no rollup (a station not logging yet). A later day nobody worked is simply empty.
  const began = st0.ok ? st0.value : "", needed = [];
  for (const d of dayList(winFrom, toDay)) {
    let need = !began || d < began;
    if (!need) for (const k of sessionDays.get(d) || []) { const pd = P.people.get(k).days.get(d); if (!pd || pd.src !== "events") { need = true; break; } }
    if (need) needed.push(d);
  }
  needed.reverse();
  const sealDays = needed.slice(0, LIM.sealDays); info.sealsLeftOut = needed.length - sealDays.length;
  const sealReads = await Promise.all(sealDays.map(d => safe(readSealDay(ctx, d), "seals " + d)));
  sealReads.forEach((r, i) => {
    if (!r.ok) { info.errors.push("seals: " + r.error); return; }
    if (r.value.capped) info.capped.push("seals");
    const day = sealDays[i];
    for (const row of r.value.rows) {
      const person = P.get(row.person); if (!person) continue;
      const existing = person.days.get(day); if (existing && existing.src === "events") continue;
      const pd = P.pd(person, day); pd.src = "seals";
      const a = stAgg(pd, row.station);
      if (row.kind === "complete") { a.completes++; addHour(pd, row.station, nyParts(row.at).hour % 24, 1); }
      else if (row.kind === "print") a.prints++; else a.scans++;
      addOrder(pd, row.orderId, row.station);
      if (!pd.inFirst || row.at < pd.inFirst) pd.inFirst = row.at;
      if (row.at > pd.inLast) pd.inLast = row.at;
    }
  });
  // 4 · per person-day: signed-in time, per station, first in, last out
  for (const person of P.people.values()) {
    for (const pd of person.days.values()) {
      if (pd.spans.length) {
        pd.signedMs = covered(pd.spans.map(x => [x.s, x.e])); pd.rawMs = pd.spans.reduce((n, x) => n + (x.e - x.s), 0);
        const per = {}; for (const x of pd.spans) (per[x.station] || (per[x.station] = [])).push([x.s, x.e]);
        for (const [st, list] of Object.entries(per)) pd.stMs[st] = covered(list);
        pd.firstIn = Math.min(...pd.spans.map(x => x.s));
        pd.lastOut = pd.spans.some(x => x.live) ? 0 : Math.max(...pd.spans.map(x => x.e));
        if (!pd.src) pd.src = "sessions";
      } else if (pd.inFirst) { pd.firstIn = pd.inFirst; pd.lastOut = pd.inLast; }
    }
  }
  return { P, info, eventDays };
}

/** Everything about some person-days at once: stations, totals, per hour, order ids. */
function summarize(pds) {
  const acc = {}, ids = new Map(), hours = zeros(24), hs = {}, stMs = {}; let signed = 0, evScans = 0, hasE = false, hasS = false, hasAny = false;
  for (const pd of pds) {
    if (pd.src === "events") hasE = true; else if (pd.src === "seals") hasS = true;
    if (pd.src) hasAny = true;
    signed += pd.signedMs;
    for (const [st, a] of Object.entries(pd.st)) { const t = acc[st] || (acc[st] = tmpl()); for (const k of KEYS) t[k] += a[k]; if (pd.src === "events") evScans += a.scans; }
    for (const [st, v] of Object.entries(pd.stMs)) stMs[st] = (stMs[st] || 0) + v;
    for (let h = 0; h < 24; h++) hours[h] += pd.hours[h];
    for (const [st, arr] of Object.entries(pd.hs)) { const t = hs[st] || (hs[st] = zeros(24)); for (let h = 0; h < 24; h++) t[h] += arr[h]; }
    for (const [id, set] of pd.orders) { let s = ids.get(id); if (!s) ids.set(id, s = new Set()); for (const st of set) s.add(st); }
  }
  const order = st => { const i = STATIONS.indexOf(st); return i < 0 ? 99 : i; };
  const stations = [...new Set(Object.keys(acc).concat(Object.keys(stMs)))].map(st => {
    const a = acc[st] || tmpl(); let touched = 0; for (const s of ids.values()) if (s.has(st)) touched++;
    return { station: st, minutes: r1((stMs[st] > 0 ? stMs[st] : a.activeMs) / 60000), parts: Math.max(0, a.parts - a.undoParts), scanParts: a.scanParts, scans: a.scans, completes: a.completes, prints: a.prints,
      orders: Math.max(touched, Math.max(0, a.orders - a.undoOrders)) };
  }).sort((x, y) => y.parts - x.parts || y.minutes - x.minutes || order(x.station) - order(y.station));
  const sum = k => stations.reduce((n, s) => n + s[k], 0);
  let active = 0, idle = 0, rejects = 0, errors = 0;
  for (const a of Object.values(acc)) { rejects += a.rejects; errors += a.errors; }
  // working and quiet time: each station keeps its own gaps, so a person signed in at two computers at once (overlapping
  // sessions, counted once in signedInMin) adds the gaps of both. Active plus idle can never exceed the time signed in.
  for (const pd of pds) {
    let a = 0, i = 0; for (const x of Object.values(pd.st)) { a += x.activeMs; i += x.idleMs; }
    if (pd.rawMs > pd.signedMs) { a = Math.min(a, pd.signedMs); i = Math.min(i, Math.max(0, pd.signedMs - a)); }
    active += a; idle += i;
  }
  const parts = sum("parts"), scans = sum("scans");
  const totals = { parts, scanParts: sum("scanParts"), scans, orders: ids.size || sum("orders"), rejects, errors, activeMin: r1(active / 60000), idleMin: r1(idle / 60000), signedInMin: r1(signed / 60000),
    rate: active >= 60000 ? r1(parts / (active / 3600000)) : 0, secPerScan: evScans > 0 && active > 0 ? r1(active / 1000 / evScans) : 0 };   // (seal scans carry no time: only logged scans divide the active time)
  const source = hasE && hasS ? "mixed" : hasE ? "events" : hasS ? "seals" : hasAny || signed > 0 ? "sessions" : "none";
  return { stations, totals, perHour: hours.map(shown), hs, ids, source, hasAny: hasAny || signed > 0 };
}

/* ── overview ── */
function parseCursor(v) {
  if (typeof v !== "string") return null;
  const i = v.indexOf("~"); if (i < 1) return null;
  const k = Number(v.slice(0, i)); return Number.isFinite(k) && k >= 0 ? { k, id: v.slice(i + 1) } : null;
}
const cursorOf = events => events.length ? `${events[0].k}~${events[0].id}` : "0~";
const newer = (e, c) => e.k > c.k || (e.k === c.k && e.id > c.id);

async function buildOverview(ctx, day, days) {
  const from = addDays(day, -(days - 1)), winDays = Math.max(14, days), winFrom = addDays(day, -(winDays - 1));
  const evP = day === ctx.today ? safe(readRecent(ctx), "events") : safe(readDayEvents(ctx, day).then(r => ({ rows: r.rows, capped: r.capped })), "events");
  const asm = await assemble(ctx, winFrom, day);
  const evR = await evP, info = asm.info;
  let events = [];
  if (!evR.ok) info.errors.push("events: " + evR.error);
  else if (day === ctx.today) events = evR.value; else { events = evR.value.rows; if (evR.value.capped) info.capped.push("events"); }
  const inRange = events.filter(e => !e.day || (e.day >= from && e.day <= day));
  // the people of the range
  const rangeDays = dayList(from, day), people = [];
  const ordersOf = new Map();
  for (const e of inRange) {
    if (!e.orderId) continue;
    const k = nameKeyOf(ctx, e.person); let m = ordersOf.get(k); if (!m) ordersOf.set(k, m = new Map());
    let o = m.get(e.orderId); if (!o) m.set(e.orderId, o = { orderId: e.orderId, stations: new Set(), made: 0, undone: 0, lastAt: 0 });
    if (e.station) o.stations.add(e.station);
    if (e.action === "complete") o.made += e.parts; else if (e.action === "undo") o.undone += e.parts;      // (events arrive newest first: an undo is met before its completion)
    o.lastAt = Math.max(o.lastAt, e.at);
  }
  const allIds = new Map(), stIds = {}, stNow = {}, perStationHours = {};
  let src = { events: false, seals: false, sessions: false };
  for (const P of asm.P.people.values()) {
    const pds = rangeDays.map(d => P.days.get(d)).filter(Boolean), S = summarize(pds);
    if (!S.hasAny) continue;
    for (const pd of pds) { if (pd.src === "events") src.events = true; else if (pd.src === "seals") src.seals = true; if (pd.spans.length) src.sessions = true; }
    const live = day === ctx.today ? P.live : [];   // "on" is now: a past day shows nobody on
    const inPd = pds.filter(pd => pd.firstIn).sort((a, b) => b.day < a.day ? -1 : 1)[0] || null;
    const nowAt = [...new Set(live.map(s => s.station))];
    const name = displayName(P), orders = [...(ordersOf.get(P.key) || new Map()).values()].sort((a, b) => b.lastAt - a.lastAt).slice(0, LIM.orders).map(o => ({ orderId: o.orderId, stations: [...o.stations], parts: Math.max(0, o.made - o.undone), lastAt: o.lastAt }));
    for (const [id, set] of S.ids) { let s = allIds.get(id); if (!s) allIds.set(id, s = new Set()); set.forEach(x => s.add(x)); for (const st of set) (stIds[st] || (stIds[st] = new Set())).add(id); }
    for (const [st, arr] of Object.entries(S.hs)) { const t = perStationHours[st] || (perStationHours[st] = zeros(24)); for (let h = 0; h < 24; h++) t[h] += arr[h]; }
    for (const st of nowAt) (stNow[st] || (stNow[st] = [])).push(name);
    people.push({ name, status: live.length ? "on" : "out", firstIn: inPd ? inPd.firstIn : null, lastOut: live.length || !inPd || !inPd.lastOut ? null : inPd.lastOut,
      onSince: live.length ? Math.min(...live.map(s => s.start)) : null, inDay: inPd ? inPd.day : null, nowAt, source: S.source, stations: S.stations, totals: S.totals, perHour: S.perHour, orders });
  }
  people.sort((a, b) => (a.status === "on" ? 0 : 1) - (b.status === "on" ? 0 : 1) || b.totals.parts - a.totals.parts || a.name.localeCompare(b.name));
  if (people.length > LIM.people) { info.capped.push("people"); people.length = LIM.people; }
  // the business
  const totals = { parts: 0, scans: 0, orders: allIds.size, people: people.length };
  const perSt = {};
  for (const p of people) { totals.parts += p.totals.parts; totals.scans += p.totals.scans; for (const s of p.stations) { const t = perSt[s.station] || (perSt[s.station] = { parts: 0, scans: 0 }); t.parts += s.parts; t.scans += s.scans; } }
  const order = st => { const i = STATIONS.indexOf(st); return i < 0 ? 99 : i; };
  const stationList = [...new Set(CORE.concat(Object.keys(perSt), Object.keys(stNow)))].sort((a, b) => order(a) - order(b) || (a < b ? -1 : 1))
    .map(st => ({ station: st, parts: (perSt[st] || {}).parts || 0, scans: (perSt[st] || {}).scans || 0, orders: stIds[st] ? stIds[st].size : 0, peopleNow: (stNow[st] || []).slice().sort() }));
  const perHour = {}; for (const [st, arr] of Object.entries(perStationHours)) if (arr.some(v => v > 0)) perHour[st] = arr.map(shown);
  const trend = dayList(winFrom, day).map(d => {
    let parts = 0, n = 0, rank = 0; const ids = new Set();
    for (const P of asm.P.people.values()) {
      const pd = P.days.get(d); if (!pd || (!pd.src && !pd.spans.length)) continue;
      n++; const S = summarize([pd]); parts += S.totals.parts; for (const id of S.ids.keys()) ids.add(id);
      rank = Math.max(rank, pd.src === "events" ? 3 : pd.src === "seals" ? 2 : 1);
    }
    return { day: d, parts, orders: ids.size, people: n, source: ["none", "sessions", "seals", "events"][rank] };
  });
  // sources, notes
  const notes = [];
  if (!src.events) notes.push("Activity events have not been recorded for these days yet: showing sign-in time and order seals only.");
  if (src.seals) notes.push("Days before activity events began come from order seals: orders, scans and label prints by person and station, with no part counts.");
  if (asm.info.sealsLeftOut > 0) notes.push(`Seal history is read for the newest ${LIM.sealDays} days only; ${asm.info.sealsLeftOut} older day${asm.info.sealsLeftOut === 1 ? "" : "s"} show sign-in time only.`);
  if (info.capped.length) notes.push("Some lists were cut at their size limit: " + [...new Set(info.capped)].join(", ") + ".");
  if (info.errors.length) notes.push("Some data could not be read just now; the screen shows what was.");
  const partial = !src.events || info.errors.length > 0 || info.capped.length > 0;
  return { now: ctx.now, cursor: cursorOf(events), people, business: { totals, perHour, stations: stationList, trend },
    feedAll: inRange.slice(0, LIM.feedDelta).map(e => { const P = asm.P.people.get(nameKeyOf(ctx, e.person)); return Object.assign({}, e, { person: P ? displayName(P) : canonOf(ctx, nameKeyOf(ctx, e.person)) || niceName(e.person) }); }), sources: src, notes, partial, errors: info.errors };
}

async function opOverview(ctx, body) {
  let day = body.day == null || body.day === "" ? ctx.today : String(body.day);
  if (!validDay(day)) return json(400, { ok: false, error: "day must be YYYY-MM-DD" });
  if (day > ctx.today) day = ctx.today;
  const n = Math.floor(num(body.days)), days = n <= 1 ? 1 : n <= 7 ? 7 : 30;
  const base = await cached(ctx, `ov|${ctx.prefix}|${day}|${days}`, TTL_LIVE, () => buildOverview(ctx, day, days));
  const after = parseCursor(body.after);
  const feed = (after ? base.feedAll.filter(e => newer(e, after)) : base.feedAll.slice(0, LIM.feed)).slice(0, after ? LIM.feedDelta : LIM.feed)
    .map(e => ({ id: e.id, at: e.at, person: e.person, station: e.station, action: e.action, orderId: e.orderId, parts: e.parts }));
  const out = { ok: true, now: base.now, day, days, cursor: base.cursor, delta: !!after, people: base.people, business: base.business, feed, sources: base.sources, notes: base.notes };
  if (base.partial) out.partial = true;
  if (base.errors.length) out.errors = base.errors;
  return json(200, out);
}

/* ── one person's days ── */
async function opPerson(ctx, body) {
  const name = cleanName(body.name);
  if (!okName(name)) return json(400, { ok: false, error: "name required" });
  let to = body.day == null || body.day === "" ? ctx.today : String(body.day);
  if (!validDay(to)) return json(400, { ok: false, error: "day must be YYYY-MM-DD" });
  if (to > ctx.today) to = ctx.today;
  const n = Math.floor(num(body.days)), days = n >= 1 ? Math.min(62, n) : 30, from = addDays(to, -(days - 1));
  const asm = await assemble(ctx, from, to), info = asm.info, P = asm.P.people.get(nameKeyOf(ctx, name));
  const list = dayList(from, to), pds = P ? list.map(d => P.days.get(d)).filter(Boolean) : [];
  const rows = list.map(d => {
    const pd = P && P.days.get(d), S = summarize(pd ? [pd] : []), t = S.totals;
    return { day: d, source: S.source, parts: t.parts, scanParts: t.scanParts, scans: t.scans, orders: t.orders, rejects: t.rejects, errors: t.errors, activeMin: t.activeMin, idleMin: t.idleMin, signedInMin: t.signedInMin,
      firstIn: pd && pd.firstIn ? pd.firstIn : null, lastOut: pd && pd.lastOut ? pd.lastOut : null, stations: S.stations, perHour: S.perHour };
  });
  const S = summarize(pds), src = { events: pds.some(p => p.src === "events"), seals: pds.some(p => p.src === "seals"), sessions: pds.some(p => p.spans.length > 0) };
  const notes = [];
  if (!src.events) notes.push("No activity events for this person in these days: sign-in time and order seals only.");
  if (src.seals) notes.push("Days before activity events began come from order seals: orders, scans and label prints, with no part counts.");
  if (info.capped.length) notes.push("Some lists were cut at their size limit: " + [...new Set(info.capped)].join(", ") + ".");
  const partial = info.errors.length > 0 || info.capped.length > 0;
  const out = { ok: true, now: ctx.now, name: P ? displayName(P) : niceName(name), from, to, days: rows, totals: S.totals, sources: src, notes };
  if (partial) { out.partial = true; if (info.errors.length) out.errors = info.errors; }
  return json(200, out);
}

/* ── who touched one order, where, when ── */
async function opOrders(ctx, body) {
  const orderId = digits(body.orderId);
  if (!orderId) return json(400, { ok: false, error: "orderId required" });
  const [ar, sr] = await Promise.all([
    safe(col(ctx, COL.activity).where("orderId", "==", orderId).limit(LIM.orderEvents).get(), "events"),
    safe(col(ctx, COL.seals).where("orderId", "==", orderId).limit(LIM.orderSeals).get(), "seals")]);
  if (!ar.ok && !sr.ok) return json(503, { ok: false, error: "the order could not be read", errors: [ar.error, sr.error] });
  const errors = [ar, sr].filter(r => !r.ok).map(r => r.label + ": " + r.error), notes = [];
  const act = ar.ok ? ar.value.docs.map(d => eventRow(d.id, d.data() || {}, ctx)).filter(e => e && e.orderId === orderId) : [];
  act.sort((a, b) => a.at - b.at || a.k - b.k || (a.id < b.id ? -1 : 1));      // oldest first, so an undo lands after the completion it reverses
  const withEvents = new Set(act.map(e => e.station));
  const seals = sr.ok ? sr.value.docs.map(d => sealRow(d.data() || {})).filter(r => r && r.orderId === orderId && !withEvents.has(r.station)) : [];
  const steps = new Map();
  const step = (station, person, source) => { const k = station + "|" + nameKeyOf(ctx, person); let s = steps.get(k); if (!s) steps.set(k, s = { station, person, forms: new Map(), firstAt: 0, lastAt: 0, workMs: 0, scans: 0, completes: 0, prints: 0, parts: 0, source }); s.forms.set(person, (s.forms.get(person) || 0) + 1); return s; };
  const touch = (s, at) => { if (at > 0 && (!s.firstAt || at < s.firstAt)) s.firstAt = at; if (at > s.lastAt) s.lastAt = at; };
  const evOut = [];
  for (const e of act) {
    const s = step(e.station, e.person, "events"); touch(s, e.at);
    if (e.action === "scan") s.scans++; else if (e.action === "complete") { s.completes++; s.parts += e.parts; } else if (e.action === "print") s.prints++; else if (e.action === "undo") s.parts = Math.max(0, s.parts - e.parts);
    if (e.sincePrevMs > 0 && e.sincePrevMs <= ACTIVE_GAP_MS) s.workMs += e.sincePrevMs;
    evOut.push({ at: e.at, person: canonOf(ctx, nameKeyOf(ctx, e.person)) || niceName(e.person), station: e.station, device: e.device, action: e.action, parts: e.parts, detail: e.detail, source: "events" });
  }
  for (const r of seals) {
    const s = step(r.station, r.person, "seals"); touch(s, r.at);
    if (r.kind === "scan") s.scans++; else if (r.kind === "complete") s.completes++; else s.prints++;
    evOut.push({ at: r.at, person: canonOf(ctx, nameKeyOf(ctx, r.person)) || niceName(r.person), station: r.station, device: "", action: r.kind, parts: 0, detail: r.type, source: "seals" });
  }
  const list = [...steps.values()].sort((a, b) => a.firstAt - b.firstAt || a.lastAt - b.lastAt);
  let farthest = 0;
  const out = list.map((s, i) => {
    const wait = i === 0 ? 0 : Math.max(0, s.firstAt - farthest); farthest = Math.max(farthest, s.lastAt);
    const name = canonOf(ctx, nameKeyOf(ctx, s.person)) || niceName(bestForm(s.forms));
    return { station: s.station, person: name, firstAt: s.firstAt, lastAt: s.lastAt, workMs: s.workMs, waitMs: wait, scans: s.scans, completes: s.completes, prints: s.prints, parts: s.parts, source: s.source };
  });
  evOut.sort((a, b) => a.at - b.at);
  const evCut = evOut.length > LIM.orderEventsOut ? evOut.slice(-LIM.orderEventsOut) : evOut;
  if (evOut.length > evCut.length) notes.push(`Showing the newest ${LIM.orderEventsOut} events of ${evOut.length}.`);
  if (seals.length) notes.push("Steps marked seals come from the order's timeline seals (before activity events), with no part counts or work time.");
  if (errors.length) notes.push("Some of this order's records could not be read just now.");
  const firstAt = out.length ? Math.min(...out.map(s => s.firstAt)) : 0, lastAt = out.length ? Math.max(...out.map(s => s.lastAt)) : 0;
  const res = { ok: true, now: ctx.now, orderId, steps: out, events: evCut,
    totals: { firstAt: firstAt || null, lastAt: lastAt || null, spanMs: lastAt - firstAt, workMs: out.reduce((n, s) => n + s.workMs, 0), people: new Set(out.map(s => nameKeyOf(ctx, s.person))).size, stations: new Set(out.map(s => s.station)).size },
    sources: { events: act.length > 0, seals: seals.length > 0 }, notes };
  if (errors.length) { res.partial = true; res.errors = errors; }
  return json(200, res);
}

/* ── the door ── */
const OPS = { overview: opOverview, person: opPerson, orders: opOrders };
function senderOf(event) {
  const h = (event && event.headers) || {};
  const get = k => { for (const x in h) if (x.toLowerCase() === k) return h[x]; return ""; };
  return String(get("x-nf-client-connection-ip") || get("client-ip") || String(get("x-forwarded-for") || "").split(",")[0] || "?").trim();
}
/** Ten wrong keys a minute per address (per warm instance): a passcode cannot be guessed through this door. */
function tooManyFails(cache, ip, now) { const w = cache.fails.get(ip); return !!w && now - w.t0 < 60000 && w.n >= FAILS_PER_MIN; }
function noteFail(cache, ip, now) {
  const w = cache.fails.get(ip);
  if (!w || now - w.t0 >= 60000) { if (cache.fails.size > 2000) cache.fails.clear(); cache.fails.set(ip, { t0: now, n: 1 }); } else w.n++;
}

async function handle(event, handle_) {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "POST") return json(405, { ok: false, error: "POST only" });
  if (String(event.body || "").length > LIM.body) return json(413, { ok: false, error: "request too large" });
  const body = parseBody(event), ctx = ctxOf(body, handle_), ip = senderOf(event);
  if (tooManyFails(ctx.cache, ip, ctx.now)) return json(429, { ok: false, error: "too many attempts, try again in a minute" });
  // the gate, before any data is touched
  const pass = await EP.resolve({ db: ctx.db });
  if (!pass.value) return json(403, { ok: false, error: "locked", code: "EDIT_PASSCODE_NOT_SET" });
  if (!EP.sameSecret(body.key, pass.value)) { noteFail(ctx.cache, ip, ctx.now); return json(401, { ok: false, error: "unauthorized" }); }
  const op = OPS[typeof body.op === "string" ? body.op : "overview"];
  if (!op) return json(400, { ok: false, error: "unknown op" });
  const al = await safe(loadAliases(ctx), "aliases");
  ctx.aliases = al.ok ? al.value : buildAliases(null); ctx.aliasError = al.ok ? "" : al.error;
  try { return await op(ctx, body); }
  catch (e) {
    if (e && e.unavailable) return json(503, { ok: false, error: "the data could not be read just now", errors: e.unavailable });
    console.warn("[employeeEfficiency] " + String((e && e.message) || e).slice(0, 200));
    return json(500, { ok: false, error: "the read failed, try again" });
  }
}

exports.handler = event => handle(event, null);
exports._t = { handle, buildAliases, fold, niceName, nyDay, nyMidnight, addDays, clip, covered, spanOf, summarize, parseCursor, cacheOf, LIM };
