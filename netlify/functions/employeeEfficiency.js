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
const AutoSignout = require("./_stationAutoSignout");
const { CORS, parseBody, num } = require("./_charmNestAuth");
const admin = require("./firebaseAdmin");
const { displayStation } = require("./_activityKinds");      // ONE Sorting station: the stored keys "sorter" and "qr" are SHOWN as "sorting" (history keeps its keys; only a read folds them)
const db = admin.firestore();
const Rev = require("./_employeeRev");                              // the employee data revision (FC5): a poll that finds it unmoved keeps what it read (see _employeeRev.js)
/* The Welding station is not counted in throughput (Paul, 6 Oct 2026): KIND.readStationCounters / throughput are the one rule (see _activityKinds.js). Without the file nothing is left out. */
let KIND = null; try { KIND = require("./_activityKinds"); } catch (_) {}
if (!KIND) KIND = { throughput: () => true, readStationCounters: (st, v) => v, UNATTRIBUTED: "Unattributed", isMatched: () => false, echoScans: () => new Set() };
/* Assembly 1..4 and Shipping 1..3 are desks of two stations (Paul, 7 Oct 2026): deviceNo("assembly", "assembly-2") is "assembly-2", "" for any other page (the kind alone). See _activityKinds.js. */
const deviceNo = KIND.deviceNo || (() => ""), NUMBERED = KIND.NUMBERED || {}, NUMBERED_KEY = KIND.NUMBERED_RE || /(?!)/;
/* The desks of days recorded before desks were told apart are recovered ONCE per day from that day's events (op deskBackfill, _deskBackfill.js); DESK.pending(rollup) is the test the overview uses to tell the console which days in view still need it */
let DESK = null; try { DESK = require("./_deskBackfill"); } catch (_) {}
if (!DESK) DESK = { pending: () => false, MAX_DAYS: 31, op: (ctx, body) => json(400, { ok: false, error: "unknown op" }) };

const COL = { activity: "Station_Activity", rollup: "Efficiency_Daily", sessions: "Station_Sessions", seals: "Order_Timeline" };
const STATIONS = ["sorting", "welding", "assembly", "shipping", "design", "laser", "sorter", "qr", "inbox"];   // every key a stored row may carry (history keeps "sorter" and "qr")
const SHOWN = STATIONS.filter(k => displayStation(k) === k);       // the stations the portal shows: sorter and qr are part of sorting
const CORE = ["sorting", "welding", "assembly", "shipping"];
const DAY_MS = 86400000, GONE_MS = 15 * 60000, ACTIVE_GAP_MS = 5 * 60000;
const TTL_LIVE = 5000, TTL_PAST = 10 * 60000, TTL_SEALS_LIVE = 120000;
const LIM = { sessions: 1500, rollups: 2000, sealsDay: 2000, sealDays: 10, window: 500, dayEvents: 1500, orderEvents: 500, orderSeals: 400,
  feed: 40, feedDelta: 200, orders: 30, people: 60, orderEventsOut: 300, body: 8000 };
const FAILS_PER_MIN = 10;
/* How long a read is kept while the revision has not moved (ms). The live documents carry keep-alive beats that are not counted as changes, so they are looked at again
   sooner; sessions and rollups only change with a counted write (and a session's own time rules are applied again on every call, see readSessionsRange). */
const REV_MAX = { act: 120000, ses: 60000, live: 45000 };

/* ── small helpers ── */
const json = (statusCode, body) => ({ statusCode, headers: CORS, body: JSON.stringify(body) });
const ms = v => v == null ? 0 : typeof v.toMillis === "function" ? v.toMillis() : v instanceof Date ? v.getTime() : Number.isFinite(+v) ? +v : 0;
const r1 = x => Math.round(x * 10) / 10;
const shown = x => Math.max(0, r1(x));                              // an hour that nets below zero (an undo in a later hour) is drawn as 0
const zeros = n => new Array(n).fill(0);
const digits = (v, n = 30) => String(v == null ? "" : v).replace(/\D/g, "").slice(0, n);
/* A name never carries a PIN: four or more digits in a name (typed after it, glued to it, or spaced "12 34 56") are a login number that slipped in, so
   the digits are dropped ("Paul 482915" is "Paul"): the Employee Number can never be stored, shown or echoed through a name. The station doors do the same on write. */
const noPin = s => (s.match(/\p{Nd}/gu) || []).length >= 4 ? s.replace(/\p{Nd}+/gu, " ").replace(/\s+/g, " ").trim() : s;
const cleanName = v => noPin(String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, 200)).slice(0, 80);
const okName = n => !!n && /\p{L}/u.test(n);                       // a name with no letter ("123456", "123 456") is a PIN, never data
/* A station key is a short lower-case word. Anything else read from a stored document (a station called "constructor", "Bad Station!", a 5,000-character
   string) is not a station: it never becomes a key of a per-station map, a label or a filter. */
const okStation = st => typeof st === "string" && /^[a-z][\w-]{0,19}$/.test(st) && !(st in Object.prototype);
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
  if (m) {
    const seen = [];                                                       // follow the aliases to the end; a loop (A is B and B is A) settles on its smallest key, so every spelling of it is ONE person
    while (m.has(k) && m.get(k) !== k && !seen.includes(k) && seen.length < 8) { seen.push(k); k = m.get(k); }
    if (seen.includes(k)) k = seen.slice(seen.indexOf(k)).sort()[0];
  }
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
/** The revision a read may lean on: { v, max } for the kind of change that can alter it ("act" | "ses" | "live"), or undefined (no revision, the sandbox, a failed look: the short lifetimes alone). */
function revDep(ctx, kind, max) { return ctx.rev && ctx.rev.ok && !ctx.prefix ? { v: String(ctx.rev[kind]), max: max || REV_MAX[kind] } : undefined; }
/** An entry read under the same revision, younger than its maximum age, is still true. */
function stillTrue(ctx, e, dep) { return !!(dep && e && e.dep && e.dep.v === dep.v && ctx.now - e.at < Math.min(dep.max, e.dep.max)); }
/** A shared, time-limited read. A failed read is forgotten at once. `dep`: see revDep. */
function cached(ctx, key, ttl, fn, dep) {
  ttl = Math.min(ttl, ctx.life);
  const memo = ctx.cache.memo, hit = memo.get(key);
  if (hit && ctx.now - hit.at < Math.min(ttl, hit.ttl)) return hit.p;   // (an entry read while its day was live stays short-lived after midnight)
  if (hit && stillTrue(ctx, hit, dep)) return hit.p;                    // (the revision has not moved since it was read: nothing was written, so it is still true)
  const entry = { at: ctx.now, ttl, p: null, dep: dep || null };
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
  const days = dayList(from, to), stale = [], entries = new Map(), dep = revDep(ctx, "act");   // (today's rollups change only with a counted activity write: kept while the revision is the same)
  for (const d of days) {
    const e = ctx.cache.memo.get(`roll|${ctx.prefix}|${d}`);
    if (e && (ctx.now - e.at < Math.min(d === ctx.today ? TTL_LIVE : TTL_PAST, e.ttl) || (d === ctx.today && stillTrue(ctx, e, dep)))) entries.set(d, e.p); else stale.push(d);
  }
  if (stale.length) {
    const a = stale[0], z = stale[stale.length - 1];
    const fetchP = col(ctx, COL.rollup).where("day", ">=", a).where("day", "<=", z).limit(LIM.rollups + 1).get().then(snap => {
      const by = new Map(), docs = snap.docs.slice(0, LIM.rollups);
      for (const x of docs) { const v = x.data() || {}; if (typeof v.day === "string" && !!v.sandbox === !!ctx.prefix) { if (!by.has(v.day)) by.set(v.day, []); by.get(v.day).push(v); } }
      return { by, capped: snap.docs.length > LIM.rollups };
    });
    for (const d of stale) {
      const entry = { at: ctx.now, ttl: Math.min(d === ctx.today ? TTL_LIVE : TTL_PAST, ctx.life), dep: d === ctx.today ? dep || null : null, p: fetchP.then(r => ({ docs: r.by.get(d) || [], capped: r.capped })) };
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
async function readSessionsRange(ctx, fromMs, toMs) {
  const live = toMs > nyMidnight(ctx.today);
  const raw = await cached(ctx, `sess|${ctx.prefix}|${fromMs}|${toMs}`, live ? TTL_LIVE : TTL_PAST, async () => {
    const TS = ctx.admin.firestore.Timestamp, ranges = [[fromMs, toMs]];
    if (TS && typeof TS.fromMillis === "function") ranges.push([TS.fromMillis(fromMs), TS.fromMillis(toMs)]);
    const snaps = await Promise.all(ranges.map(([a, z]) => col(ctx, COL.sessions).where("startAt", ">=", a).where("startAt", "<", z).orderBy("startAt", "desc").limit(LIM.sessions + 1).get()));
    const seen = new Set(), rows = []; let truncated = false;
    for (const s of snaps) { if (s.docs.length > LIM.sessions) truncated = true; for (const d of s.docs.slice(0, LIM.sessions)) if (!seen.has(d.id)) { seen.add(d.id); rows.push(Object.assign({ id: d.id }, d.data() || {})); } }
    return { rows, truncated };
  }, live ? revDep(ctx, "ses") : undefined);
  // The auto sign-out rules: a session that is idle (10 minutes without input), past 5:00 pm Toronto with no recent input, or whose page died ends at the person's last input (_stationAutoSignout.js).
  // They run on the clock, so they are applied on EVERY call to the rows kept (no read; a write only when a session now ends), whatever the age of the read: a session is ended as soon as the rule says.
  await AutoSignout.settle({ db: ctx.db, prefix: ctx.prefix, now: ctx.now }, raw.rows);
  return raw;
}
/** The window's sessions in two parts: what started before yesterday's midnight is final (a session never outlives its New York midnight)
    and is kept 10 minutes; what started yesterday or today can still change and is read afresh every 5 s. A poll used to re-read the
    whole fortnight (hundreds of documents) every 5 s per open console; now it reads the last day and a bit. */
function readSessions(ctx, fromMs, toMs) {
  const split = nyMidnight(addDays(ctx.today, -1));
  if (fromMs >= split || toMs <= split) return readSessionsRange(ctx, fromMs, toMs);
  return Promise.all([readSessionsRange(ctx, fromMs, split), readSessionsRange(ctx, split, toMs)])
    .then(([a, b]) => ({ rows: a.rows.concat(b.rows), truncated: a.truncated || b.truncated }));
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
  return kind ? { at, person, station: displayStation(station), type, kind, orderId: digits(d.orderId) } : null;
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
  return { id: (scrub(d.id) || String(id)).slice(0, 100), at, k: tsMs || serverAt, tsMs: tsMs || serverAt, person, station: okStation(String(d.station || "")) ? displayStation(String(d.station)) : "", stored: okStation(String(d.station || "")) ? String(d.station) : "", device: scrub(d.device).slice(0, 40), action,
    orderId: digits(d.orderId), parts: Math.max(0, Math.floor(num(d.parts))), detail: scrub(d.detail), sincePrevMs: Math.max(0, num(d.sincePrevMs)), day: typeof d.day === "string" ? d.day : "",
    orders: num(d.orders) >= 1 ? 1 : 0, seq: Math.max(0, num(d.seq)), task: d.task === "welding" || d.task === "matching" ? d.task : "", unattributed: d.unattributed === true };   // (orders: the action finished an order; seq: the device's counter: the issue counts replay the writer's order with them; task / unattributed: the Welding station's matched scans)
}
const byNewest = (a, b) => b.k - a.k || (a.id < b.id ? 1 : a.id > b.id ? -1 : 0);
/** The newest events of everybody, kept in the instance: loaded once (newest 500), then only topped up. */
function readRecent(ctx) {
  let R = ctx.cache.recent.get(ctx.prefix);
  if (!R) { R = { events: [], loaded: false, at: 0, newest: 0, p: null }; ctx.cache.recent.set(ctx.prefix, R); }
  if (R.p) return R.p;
  if (R.loaded && ctx.now - R.at < TTL_LIVE) return Promise.resolve(R.events);
  const dep = revDep(ctx, "act");                                          // (no event was written since the last look: nothing to top up)
  if (R.loaded && dep && R.dep && R.dep.v === dep.v && ctx.now - R.at < dep.max) return Promise.resolve(R.events);
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
      R.loaded = true; R.at = Date.now(); R.dep = dep || null;
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
  if (!end) { if (now - last >= GONE_MS && !AutoSignout.keptOpen(s, now)) end = last; else { end = now; live = true; } }       // (a quiet page of Laser inside its limit, or of Welding before 17:00, is still signed in)
  end = Math.min(end, now); if (end < start) end = start;
  const station = String(s.station || "");
  return { id: String(s.id || ""), station: okStation(station) ? displayStation(station) : "", device: okStation(station) ? deviceNo(station, s.device) : "", task: station === "welding" && (s.task === "welding" || s.task === "matching") ? s.task : "", start, end, live, last };       // (a key like "constructor" would break the per-station maps)
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
const tmpl = () => ({ scans: 0, scanParts: 0, completes: 0, parts: 0, orders: 0, prints: 0, rejects: 0, errors: 0, notes: 0, undos: 0, undoParts: 0, undoOrders: 0, activeMs: 0, idleMs: 0, matched: 0 });
const KEYS = Object.keys(tmpl());
const newPD = day => ({ day, src: "", st: {}, hours: zeros(24), hs: {}, orders: new Map(), spans: [], inFirst: 0, inLast: 0, signedMs: 0, rawMs: 0, stMs: {}, taskMs: {}, dv: {}, dvMs: {}, firstIn: 0, lastOut: 0, liveSpan: false });
const dvAgg = (pd, dk) => pd.dv[dk] || (pd.dv[dk] = { parts: 0, scans: 0, fin: 0, ids: new Set(), lastAt: 0 });   // one desk of a numbered station, one person-day
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
      if (x.person === KIND.UNATTRIBUTED) continue;                       // (a matched scan made with nobody in Matching: no person; the Stations board counts it)
      const person = P.get(x.person); if (!person || !validDay(x.day)) continue;
      eventDays.add(x.day);
      if (DESK.pending(x)) (info.deskDays || (info.deskDays = new Set())).add(x.day);                 // (a day whose rollup still has events no desk accounts for: the overview tells the console, once, see deskPending)
      const pd = P.pd(person, x.day); pd.src = "events";
      for (const [st0, v0] of Object.entries(x.stations && typeof x.stations === "object" ? x.stations : {})) {
        if (!okStation(st0) || !v0 || typeof v0 !== "object") continue;    // (a station called "constructor" is not a station)
        const st = displayStation(st0);                                    // (counters stored under "sorter" or "qr" add to Sorting)
        const v = KIND.readStationCounters(st, v0);                          // (the Welding station keeps its scans, time and matched count, never pieces, orders or completions)
        const a = stAgg(pd, st); for (const k of KEYS) a[k] += Math.max(0, num(v[k]));
        const ob = pd.ob || (pd.ob = {}), o = ob[st] || (ob[st] = {}); o[st0] = (o[st0] || 0) + Math.max(0, num(v.orders) - num(v.undoOrders));
        const fa = ms(v.firstAt), la = ms(v.lastAt);
        if (fa > 0 && (!pd.inFirst || fa < pd.inFirst)) pd.inFirst = fa;
        if (la > pd.inLast) pd.inLast = la;
      }
      for (const [hh, h] of Object.entries(x.hours && typeof x.hours === "object" ? x.hours : {})) {
        const hr = parseInt(hh, 10); if (!(hr >= 0 && hr < 24) || !h || typeof h !== "object") continue;
        const by = h.by && typeof h.by === "object" ? h.by : {};
        let net = Math.max(0, num(h.parts)) - Math.max(0, num(h.undoParts));                 // produced minus undone, the same net as the totals
        for (const [st, b] of Object.entries(by)) if (okStation(st) && !KIND.throughput(displayStation(st)) && b && typeof b === "object") net -= Math.max(0, num(b.parts)) - Math.max(0, num(b.undoParts));   // (not the Welding station's)
        pd.hours[hr] += net;
        for (const [st, b] of Object.entries(by)) if (okStation(st) && KIND.throughput(displayStation(st)) && b && typeof b === "object") { const ds = displayStation(st); (pd.hs[ds] || (pd.hs[ds] = zeros(24)))[hr] += Math.max(0, num(b.parts)) - Math.max(0, num(b.undoParts)); }
      }
      for (const [id, m] of Object.entries(x.touched && typeof x.touched === "object" ? x.touched : {})) {
        const oid = digits(id); if (!oid) continue;
        const stations = m && typeof m === "object" ? [...new Set(Object.keys(m).filter(k => m[k] && okStation(k) && KIND.throughput(displayStation(k))).map(displayStation))] : [];   // (an order only the Welding station touched is not an order worked)
        if (!stations.length) continue;
        let set = pd.orders.get(oid); if (!set) pd.orders.set(oid, set = new Set()); stations.forEach(s => set.add(s));
        if (m && typeof m === "object") for (const k of Object.keys(m)) if (typeof m[k] === "string") { const dk = deviceNo(k, m[k]); if (dk) dvAgg(pd, dk).ids.add(oid); }   // (a numbered station's order says which desk touched it; `true` = no desk)
      }
      for (const [dk0, v] of Object.entries(x.devices && typeof x.devices === "object" ? x.devices : {})) {   // the desks' own counters (written since desks were told apart)
        const dk = NUMBERED_KEY.test(dk0) && v && typeof v === "object" ? deviceNo(dk0.split("-")[0], dk0) : ""; if (!dk) continue;
        const a = dvAgg(pd, dk); a.parts += Math.max(0, num(v.parts) - num(v.undoParts)); a.scans += Math.max(0, num(v.scans)); a.fin += Math.max(0, num(v.orders) - num(v.undoOrders)); a.lastAt = Math.max(a.lastAt, ms(v.lastAt));
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
        pd.spans.push({ s: c.s, e: c.e, station: sp.station, device: sp.device, task: sp.task, live: sp.live, start0: sp.start });
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
      const a = stAgg(pd, row.station), counted = KIND.throughput(row.station);   // (the Welding station's `welded` seals are history of a completion that is no longer counted: its scans and prints stay, its completions and orders do not)
      if (row.kind === "complete") { if (counted) { a.completes++; addHour(pd, row.station, nyParts(row.at).hour % 24, 1); } }
      else if (row.kind === "print") a.prints++; else a.scans++;
      if (counted) addOrder(pd, row.orderId, row.station);
      if (!pd.inFirst || row.at < pd.inFirst) pd.inFirst = row.at;
      if (row.at > pd.inLast) pd.inLast = row.at;
    }
  });
  // 4 · per person-day: signed-in time, per station, first in, last out
  for (const person of P.people.values()) {
    for (const pd of person.days.values()) {
      // an order finished at two pages of ONE station (the Sorter app, then a sorting page) is one order there: the larger of the pages' own counts, not their sum (the orders touched, counted by id, stay exact)
      for (const [st, src] of Object.entries(pd.ob || {})) { const a = pd.st[st]; if (a && Object.keys(src).length > 1) { a.orders = Math.max(...Object.values(src)); a.undoOrders = 0; } }
      if (pd.spans.length) {
        pd.signedMs = covered(pd.spans.map(x => [x.s, x.e])); pd.rawMs = pd.spans.reduce((n, x) => n + (x.e - x.s), 0);
        const per = {}; for (const x of pd.spans) (per[x.station] || (per[x.station] = [])).push([x.s, x.e]);
        for (const [st, list] of Object.entries(per)) pd.stMs[st] = covered(list);
        const perDesk = {}; for (const x of pd.spans) if (x.device) (perDesk[x.device] || (perDesk[x.device] = [])).push([x.s, x.e]);
        for (const [dk, list] of Object.entries(perDesk)) pd.dvMs[dk] = covered(list);
        // the Welding station's time per task: a person in both tasks has both (so the tasks can add up to more than the station's own time); an old session without a task is "unknown"
        const pt = {}; for (const x of pd.spans) if (x.station === "welding") (pt[x.task || "unknown"] || (pt[x.task || "unknown"] = [])).push([x.s, x.e]);
        for (const [t, list] of Object.entries(pt)) pd.taskMs[t] = covered(list);
        pd.firstIn = Math.min(...pd.spans.map(x => x.s));
        pd.lastOut = pd.spans.some(x => x.live) ? 0 : Math.max(...pd.spans.map(x => x.e));
        if (!pd.src) pd.src = "sessions";
      } else if (pd.inFirst) { pd.firstIn = pd.inFirst; pd.lastOut = pd.inLast; }
    }
  }
  return { P, info, eventDays };
}

/** An order is WORK when some station other than the inbox touched it: a customer conversation in the inbox is not an order worked (the person page and the calendar count the same way). */
const isWork = set => { for (const x of set) if (x !== "inbox") return true; return false; };
/** Everything about some person-days at once: stations, totals, per hour, order ids. */
function summarize(pds) {
  const acc = {}, ids = new Map(), hours = zeros(24), hs = {}, stMs = {}, taskMs = { welding: 0, matching: 0, unknown: 0 }, dv = {}; let signed = 0, evScans = 0, hasE = false, hasS = false, hasAny = false;
  const desk = dk => dv[dk] || (dv[dk] = { parts: 0, scans: 0, fin: 0, short: 0, ids: new Set(), ms: 0 });   // one desk of a numbered station over these person-days
  for (const pd of pds) {
    for (const [dk, a] of Object.entries(pd.dv)) { const t = desk(dk); t.parts += a.parts; t.scans += a.scans; t.fin += a.fin; t.short += Math.max(0, a.fin - a.ids.size); a.ids.forEach(i => t.ids.add(i)); }
    for (const [dk, v] of Object.entries(pd.dvMs)) desk(dk).ms += v;
    if (pd.src === "events") hasE = true; else if (pd.src === "seals") hasS = true;
    if (pd.src) hasAny = true;
    signed += pd.signedMs;
    for (const t of Object.keys(taskMs)) taskMs[t] += (pd.taskMs && pd.taskMs[t]) || 0;
    for (const [st, a] of Object.entries(pd.st)) { const t = acc[st] || (acc[st] = tmpl()); for (const k of KEYS) t[k] += a[k]; if (pd.src === "events") evScans += a.scans; }
    for (const [st, v] of Object.entries(pd.stMs)) stMs[st] = (stMs[st] || 0) + v;
    for (let h = 0; h < 24; h++) hours[h] += pd.hours[h];
    for (const [st, arr] of Object.entries(pd.hs)) { const t = hs[st] || (hs[st] = zeros(24)); for (let h = 0; h < 24; h++) t[h] += arr[h]; }
    for (const [id, set] of pd.orders) { let s = ids.get(id); if (!s) ids.set(id, s = new Set()); for (const st of set) s.add(st); }
  }
  const order = st => { const i = SHOWN.indexOf(st); return i < 0 ? 99 : i; };
  const stations = [...new Set(Object.keys(acc).concat(Object.keys(stMs)))].map(st => {
    const a = acc[st] || tmpl(); let touched = 0; for (const s of ids.values()) if (s.has(st)) touched++;
    const row = { station: st, minutes: r1((stMs[st] > 0 ? stMs[st] : a.activeMs) / 60000), parts: Math.max(0, a.parts - a.undoParts), scanParts: a.scanParts, scans: a.scans, completes: a.completes, prints: a.prints,
      orders: Math.max(touched, Math.max(0, a.orders - a.undoOrders)) };
    if (!KIND.throughput(st)) { row.matched = a.matched; row.taskMin = { welding: r1(taskMs.welding / 60000), matching: r1(taskMs.matching / 60000), unknown: r1(taskMs.unknown / 60000) }; }   // (the Welding station: time per task and the matched count stand where pieces and orders would)
    return row;
  }).sort((x, y) => y.parts - x.parts || y.minutes - x.minutes || order(x.station) - order(y.station));
  const sum = k => stations.reduce((n, s) => n + s[k], 0);
  let active = 0, activeTp = 0, idle = 0, rejects = 0, errors = 0;      // (activeTp: the working time of the stations that count in throughput, the divisor of the pieces-per-hour rate)
  for (const a of Object.values(acc)) { rejects += a.rejects; errors += a.errors; }
  // worked at the Welding station alone: nothing was logged at a station that counts in throughput (the person page's own rule: a day with none has no pieces or orders to show), yet there was Welding work (a scan, a print, a match or time signed in)
  let tpAct = false, ntAct = false;
  for (const [st, a] of Object.entries(acc)) { const any = a.scans + a.completes + a.prints + a.rejects + a.errors + a.undos + a.notes > 0; if (KIND.throughput(st)) { if (any) tpAct = true; } else if (any || a.matched > 0) ntAct = true; }
  for (const [st, v] of Object.entries(stMs)) if (!KIND.throughput(st) && v > 0) ntAct = true;
  // working and quiet time: each station keeps its own gaps, so a person signed in at two computers at once (overlapping
  // sessions, counted once in signedInMin) adds the gaps of both. Active plus idle can never exceed the time signed in.
  for (const pd of pds) {
    let a = 0, i = 0, at = 0; for (const [st, x] of Object.entries(pd.st)) { a += x.activeMs; i += x.idleMs; if (KIND.throughput(st)) at += x.activeMs; }
    if (pd.rawMs > pd.signedMs) { a = Math.min(a, pd.signedMs); at = Math.min(at, pd.signedMs); i = Math.min(i, Math.max(0, pd.signedMs - a)); }
    active += a; activeTp += at; idle += i;
  }
  const parts = sum("parts"), scans = sum("scans");
  const workIds = [...ids.values()].filter(isWork).length, ordersFin = stations.reduce((n, x) => n + (x.station === "inbox" ? 0 : x.orders), 0);
  const totals = { parts, scanParts: sum("scanParts"), scans, orders: ids.size ? workIds : ordersFin, rejects, errors, activeMin: r1(active / 60000), idleMin: r1(idle / 60000), signedInMin: r1(signed / 60000),
    rate: activeTp >= 60000 ? r1(parts / (activeTp / 3600000)) : 0, secPerScan: evScans > 0 && active > 0 ? r1(active / 1000 / evScans) : 0 };   // (seal scans carry no time: only logged scans divide the active time)
  const source = hasE && hasS ? "mixed" : hasE ? "events" : hasS ? "seals" : hasAny || signed > 0 ? "sessions" : "none";
  // the desks of the numbered stations (Assembly 1..4, Shipping 1..3): time signed in at each (sessions, any day), and what the desk's own counters say (events written since desks were told apart)
  const devices = Object.keys(dv).map(dk => ({ station: dk.split("-")[0], device: dk, minutes: r1(dv[dk].ms / 60000), parts: dv[dk].parts, scans: dv[dk].scans, orders: dv[dk].ids.size + dv[dk].short }))
    .sort((x, y) => y.minutes - x.minutes || y.parts - x.parts || (x.device < y.device ? -1 : 1));
  return { stations, totals, perHour: hours.map(shown), hs, ids, source, hasAny: hasAny || signed > 0, weldOnly: !tpAct && ntAct, devices, dv };
}

/* ── overview ── */
function parseCursor(v) {
  if (typeof v !== "string") return null;
  const i = v.indexOf("~"); if (i < 1) return null;
  const k = Number(v.slice(0, i)); return Number.isFinite(k) && k >= 0 ? { k, id: v.slice(i + 1) } : null;
}
const cursorOf = events => events.length ? `${events[0].k}~${events[0].id}` : "0~";
const newer = (e, c) => e.k > c.k || (e.k === c.k && e.id > c.id);

async function buildOverview(ctx, day, days, withTrend) {
  // the daily trend needs a fortnight; a single-day screen (trend:false) reads only its own day, so it does not pull the fortnight's
  // rollups, sign-ins and (for the days before the events began) up to ten days of seals on every poll
  const from = addDays(day, -(days - 1)), winDays = withTrend ? Math.max(14, days) : days, winFrom = addDays(day, -(winDays - 1));
  const evP = day === ctx.today ? safe(readRecent(ctx), "events") : safe(readDayEvents(ctx, day).then(r => ({ rows: r.rows, capped: r.capped })), "events");
  const asm = await assemble(ctx, winFrom, day);
  const evR = await evP, info = asm.info;
  let events = [];
  if (!evR.ok) info.errors.push("events: " + evR.error);
  else if (day === ctx.today) events = evR.value; else { events = evR.value.rows; if (evR.value.capped) info.capped.push("events"); }
  const echo = KIND.echoScans(events);                                  // (the desk page's own scan of a phone scan the scanner app wrote as `matched`: one scan, not two)
  const inRange = events.filter(e => !echo.has(e) && (!e.day || (e.day >= from && e.day <= day)));
  // the people of the range
  const rangeDays = dayList(from, day), people = [];
  const ordersOf = new Map();
  for (const e of inRange) {
    if (!e.orderId || !KIND.throughput(e.station)) continue;       // (an order only the Welding station touched is not an order worked: not in the count, so not in the list either; its matches have their own list)
    const k = nameKeyOf(ctx, e.person); let m = ordersOf.get(k); if (!m) ordersOf.set(k, m = new Map());
    let o = m.get(e.orderId); if (!o) m.set(e.orderId, o = { orderId: e.orderId, stations: new Set(), made: 0, undone: 0, lastAt: 0 });
    if (e.station) o.stations.add(e.station);
    if (e.action === "complete") { if (KIND.throughput(e.station)) o.made += e.parts; } else if (e.action === "undo") o.undone += e.parts;      // (events arrive newest first: an undo is met before its completion)
    o.lastAt = Math.max(o.lastAt, e.at);
  }
  const allIds = new Map(), stIds = {}, stNow = {}, perStationHours = {}, dvTot = {}, devNow = {};   // (dvTot: per desk over everybody; devNow: who is signed in at each desk now)
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
    const nowDevices = [...new Set(live.map(s => s.device).filter(Boolean))];
    for (const dk of nowDevices) (devNow[dk] || (devNow[dk] = [])).push(name);
    for (const [dk, t] of Object.entries(S.dv)) { const x = dvTot[dk] || (dvTot[dk] = { parts: 0, scans: 0, short: 0, ids: new Set() }); x.parts += t.parts; x.scans += t.scans; x.short += t.short; t.ids.forEach(i => x.ids.add(i)); }
    people.push(Object.assign({ name, status: live.length ? "on" : "out", firstIn: inPd ? inPd.firstIn : null, lastOut: live.length || !inPd || !inPd.lastOut ? null : inPd.lastOut,
      onSince: live.length ? Math.min(...live.map(s => s.start)) : null, inDay: inPd ? inPd.day : null, nowAt, nowDevices, devices: S.devices, source: S.source, stations: S.stations, totals: S.totals, perHour: S.perHour, orders }, S.weldOnly ? { noThroughput: true } : {}));   // (noThroughput: worked at the Welding station only: no pieces or orders to count, shown as a dash with the time on task and the matched count)
  }
  people.sort((a, b) => (a.status === "on" ? 0 : 1) - (b.status === "on" ? 0 : 1) || b.totals.parts - a.totals.parts || a.name.localeCompare(b.name));
  // the business: summed over EVERYBODY, then the list is cut (the totals must not lose the people the list does not show)
  const totals = { parts: 0, scans: 0, orders: [...allIds.values()].filter(isWork).length, people: people.length };
  const perSt = {};
  for (const p of people) { totals.parts += p.totals.parts; totals.scans += p.totals.scans; for (const s of p.stations) { const t = perSt[s.station] || (perSt[s.station] = { parts: 0, scans: 0 }); t.parts += s.parts; t.scans += s.scans; if (s.taskMin) { t.matched = (t.matched || 0) + s.matched; const m = t.taskMin || (t.taskMin = { welding: 0, matching: 0, unknown: 0 }); for (const k of Object.keys(m)) m[k] = r1(m[k] + (s.taskMin[k] || 0)); } } }
  const order = st => { const i = SHOWN.indexOf(st); return i < 0 ? 99 : i; };
  const stationList = [...new Set(CORE.concat(Object.keys(perSt), Object.keys(stNow)))].sort((a, b) => order(a) - order(b) || (a < b ? -1 : 1))
    .map(st => { const row = { station: st, parts: (perSt[st] || {}).parts || 0, scans: (perSt[st] || {}).scans || 0, orders: stIds[st] ? stIds[st].size : 0, peopleNow: (stNow[st] || []).slice().sort() }; if (!KIND.throughput(st)) { row.matched = (perSt[st] || {}).matched || 0; row.taskMin = (perSt[st] || {}).taskMin || { welding: 0, matching: 0, unknown: 0 }; }
      if (NUMBERED[st]) {   // Assembly 1..4 / Shipping 1..3: one entry per desk (all of the shop's desks listed, idle ones too) and what no desk claims (events and sessions from before desks were told apart: shown as the kind alone)
        const keys = []; for (let n = 1; n <= NUMBERED[st]; n++) keys.push(`${st}-${n}`);
        for (const dk of Object.keys(dvTot).concat(Object.keys(devNow))) if (dk.split("-")[0] === st && !keys.includes(dk)) keys.push(dk);
        keys.sort((a, b) => parseInt(a.split("-")[1], 10) - parseInt(b.split("-")[1], 10));
        row.devices = keys.map(dk => { const t = dvTot[dk]; return { device: dk, parts: t ? t.parts : 0, scans: t ? t.scans : 0, orders: t ? t.ids.size + t.short : 0, peopleNow: (devNow[dk] || []).slice().sort() }; });
        const sum = k => row.devices.reduce((n, d) => n + d[k], 0);
        row.unassigned = { parts: Math.max(0, row.parts - sum("parts")), scans: Math.max(0, row.scans - sum("scans")), orders: Math.max(0, row.orders - sum("orders")) };
      }
      return row; });
  const perHour = {}; for (const [st, arr] of Object.entries(perStationHours)) if (arr.some(v => v > 0)) perHour[st] = arr.map(shown);
  if (people.length > LIM.people) { info.capped.push("people"); people.length = LIM.people; }       // (the list only: the totals above counted everybody)
  const trend = dayList(winFrom, day).map(d => {
    let parts = 0, n = 0, rank = 0; const ids = new Map();
    for (const P of asm.P.people.values()) {
      const pd = P.days.get(d); if (!pd || (!pd.src && !pd.spans.length)) continue;
      n++; const S = summarize([pd]); parts += S.totals.parts; for (const [id, set] of S.ids) { let x = ids.get(id); if (!x) ids.set(id, x = new Set()); set.forEach(v => x.add(v)); }
      rank = Math.max(rank, pd.src === "events" ? 3 : pd.src === "seals" ? 2 : 1);
    }
    return { day: d, parts, orders: [...ids.values()].filter(isWork).length, people: n, source: ["none", "sessions", "seals", "events"][rank] };
  });
  // sources, notes
  const notes = [];
  const rollupsDown = info.errors.some(e => String(e).startsWith("rollups:"));
  if (!src.events && rollupsDown) notes.push("The activity numbers could not be read just now: pieces, scans and orders show 0 where they are unknown; sign-in times are real.");
  else if (!src.events) notes.push("Activity events have not been recorded for these days yet: showing sign-in time and order seals only.");
  if (src.seals) notes.push("Days before activity events began come from order seals: orders, scans and label prints by person and station, with no piece counts.");
  if (asm.info.sealsLeftOut > 0) notes.push(`Seal history is read for the newest ${LIM.sealDays} days only; ${asm.info.sealsLeftOut} older day${asm.info.sealsLeftOut === 1 ? "" : "s"} show sign-in time only.`);
  if (info.capped.length) notes.push("Some lists were cut at their size limit: " + [...new Set(info.capped)].join(", ") + ".");
  if (info.errors.length) notes.push("Some data could not be read just now; the screen shows what was.");
  const partial = !src.events || info.errors.length > 0 || info.capped.length > 0;
  const oldest = addDays(ctx.today, -DESK.MAX_DAYS), deskPending = [...(info.deskDays || [])].filter(d => d >= from && d <= day && d >= oldest).sort().reverse();   // (days in view that still lack their desk numbers, newest first; never one the op would refuse)
  return { now: ctx.now, cursor: cursorOf(events), deskPending, people, business: { totals, perHour, stations: stationList, trend },
    feedAll: inRange.slice(0, LIM.feedDelta).map(e => { const P = asm.P.people.get(nameKeyOf(ctx, e.person)); return Object.assign({}, e, { person: P ? displayName(P) : canonOf(ctx, nameKeyOf(ctx, e.person)) || niceName(e.person) }); }), sources: src, notes, partial, errors: info.errors };
}

async function opOverview(ctx, body) {
  let day = body.day == null || body.day === "" ? ctx.today : String(body.day);
  if (!validDay(day)) return json(400, { ok: false, error: "day must be YYYY-MM-DD" });
  if (day > ctx.today) day = ctx.today;
  const n = Math.floor(num(body.days)), days = n <= 1 ? 1 : n <= 7 ? 7 : 30;
  const withTrend = !(body.trend === false || body.trend === 0 || body.trend === "0");
  const ovKey = `ov|${ctx.prefix}|${day}|${days}|${withTrend ? "t" : "n"}`;
  const base = await cached(ctx, ovKey, TTL_LIVE, () => buildOverview(ctx, day, days, withTrend));
  if (base.errors.length) ctx.cache.memo.delete(ovKey);                // (an answer with a failed read in it is not kept: the next call tries the source again)
  const after = parseCursor(body.after);
  const feed = (after ? base.feedAll.filter(e => newer(e, after)) : base.feedAll.slice(0, LIM.feed)).slice(0, after ? LIM.feedDelta : LIM.feed)
    .map(e => { const o = { id: e.id, at: e.at, person: e.person, station: e.station, action: e.action, orderId: e.orderId, parts: e.parts }, dk = deviceNo(e.stored, e.device); if (dk) o.device = dk; return o; });   // (device: the desk of a numbered station, "assembly-2"; absent for every other page and for old events)
  const out = { ok: true, now: base.now, day, days, cursor: base.cursor, delta: !!after, people: base.people, business: base.business, feed, sources: base.sources, notes: base.notes };
  if (base.partial) out.partial = true;
  if (base.errors.length) out.errors = base.errors;
  if (!ctx.prefix && base.deskPending && base.deskPending.length) out.deskPending = base.deskPending;   // (the real store only: the console asks for each of these days once, op deskBackfill)
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
  if (src.seals) notes.push("Days before activity events began come from order seals: orders, scans and label prints, with no piece counts.");
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
  if (/^\d{4,8}$/.test(orderId)) return json(400, { ok: false, error: "that is not an order number" });         // (4 to 8 digits could be a PIN: the stations never record such an id, and it is not echoed back)
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
  const evOut = [], echo = KIND.echoScans(act);
  for (const e of act) {
    if (echo.has(e)) { evOut.push({ at: e.at, person: canonOf(ctx, nameKeyOf(ctx, e.person)) || niceName(e.person), station: e.station, device: e.device, action: e.action, parts: e.parts, detail: e.detail, source: "events", echo: true }); continue; }     // (listed, never counted: a phone scan is the matched event)
    const s = step(e.station, e.person, "events"); touch(s, e.at);
    if (e.action === "scan" || e.action === "matched") s.scans++; else if (e.action === "complete") { s.completes++; s.parts += e.parts; } else if (e.action === "print") s.prints++; else if (e.action === "undo") s.parts = Math.max(0, s.parts - e.parts);
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
  if (seals.length) notes.push("Steps marked seals come from the order's timeline seals (before activity events), with no piece counts or work time.");
  if (errors.length) notes.push("Some of this order's records could not be read just now.");
  const firstAt = out.length ? Math.min(...out.map(s => s.firstAt)) : 0, lastAt = out.length ? Math.max(...out.map(s => s.lastAt)) : 0;
  const res = { ok: true, now: ctx.now, orderId, steps: out, events: evCut,
    totals: { firstAt: firstAt || null, lastAt: lastAt || null, spanMs: lastAt - firstAt, workMs: out.reduce((n, s) => n + s.workMs, 0), people: new Set(out.map(s => nameKeyOf(ctx, s.person))).size, stations: new Set(out.map(s => s.station)).size },
    sources: { events: act.length > 0, seals: seals.length > 0 }, notes };
  if (errors.length) { res.partial = true; res.errors = errors; }
  return json(200, res);
}

/* ── the door ── */
const PROFILE = require("./_employeeProfile")({ KIND, COL, LIM, ms, num, r1, zeros, digits, cleanName, okName, okStation, niceName, bestForm, nameKeyOf, canonOf, scrub, validDay, addDays, nyDay, nyMidnight, clip, covered, spanOf, cached, revDep, readRollups, readEventsStart, eventRow, col, json, safe, tmpl, KEYS });   // the employee page: ops person (with range) and personOrders
const OPS = { overview: opOverview, person: (ctx, body) => (body.range != null || body.from || body.to ? PROFILE.opProfile(ctx, body) : opPerson(ctx, body)), orders: opOrders, personOrders: PROFILE.opOrders };
/* op "deskBackfill" { day } (Paul, 7 Oct 2026: the desk rows must add up for days recorded before desks were told apart): recovers one day's desk counters from that day's events, once, bounded; see _deskBackfill.js */
OPS.deskBackfill = (ctx, body) => DESK.op(ctx, body, { json, validDay, addDays });
/* op "live": the stations board (what each station is working on right now), kept in _stationLive.js */
OPS.live = (ctx, body) => require("./_stationLive").op(ctx, body, { json, nyMidnight, cached, revDep, display: raw => canonOf(ctx, nameKeyOf(ctx, raw)) || niceName(raw) });
/* op "laserSheets" (R7, Paul 6 Oct): one person's cut sheets and how long each took, from the Laser_Sheet_Times records the Library's laserDone wrote (kept in _laserSheetTime.js) */
OPS.laserSheets = (ctx, body) => require("./_laserSheetTime").opSheets(ctx, body, { json, nyMidnight, nameKeyOf, cleanName, okName, display: raw => canonOf(ctx, nameKeyOf(ctx, raw)) || niceName(raw) });
/* the inbox figures (Paul, 6 Oct 2026: replies sent per employee, orders covered, messages per customer): _employeeInbox.js gets this file's own name, day and cache rules
   once; ops `inbox` (everybody, all windows) and `personInbox` (one person, the Employee page's Inbox section); _stationLive.js and _employeeProfile.js call it for the board's block and for personOrders station "inbox" */
const INBOX = require("./_employeeInbox");
INBOX.bind({ cleanName, okName, niceName, bestForm, nameKeyOf, canonOf, num, validDay, addDays, nyDay, nyMidnight, cached, safe, json, resolveRange: PROFILE.resolveRange, RANGE_DAYS: PROFILE.RANGE_DAYS, DAILY_MAX: PROFILE.DAILY_MAX });
OPS.inbox = INBOX.opInbox;
OPS.personInbox = INBOX.opPersonInbox;
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
  const offered = typeof body.key === "string" || typeof body.key === "number" ? body.key : "";      // (an object or array as the key: refused, never an exception)
  if (!EP.sameSecret(offered, pass.value)) { noteFail(ctx.cache, ip, ctx.now); return json(401, { ok: false, error: "unauthorized" }); }
  const opName = typeof body.op === "string" ? body.op : "overview";
  const op = Object.prototype.hasOwnProperty.call(OPS, opName) ? OPS[opName] : null;       // (an own entry only: "constructor" and "toString" are not ops)
  if (!op) return json(400, { ok: false, error: "unknown op" });
  const [al, rev] = await Promise.all([safe(loadAliases(ctx), "aliases"), ctx.prefix ? null : Rev.read(ctx.db, ctx.now)]);   // (the revision: one small document, kept 1 s; the sandbox never uses it)
  ctx.aliases = al.ok ? al.value : buildAliases(null); ctx.aliasError = al.ok ? "" : al.error; ctx.rev = rev;
  try {
    const res = await op(ctx, body);
    if (ctx.aliasError && res && res.statusCode === 200) {           // without the alias list two spellings of one person may show as two: every answer says so, once
      try {
        const j = JSON.parse(res.body);
        if (j && typeof j === "object" && !Array.isArray(j)) {
          const errs = Array.isArray(j.errors) ? j.errors : [];
          if (!errs.some(x => String(x).startsWith("aliases:"))) errs.push("aliases: " + ctx.aliasError);
          j.partial = true; j.errors = errs.slice(0, 12);
          return Object.assign({}, res, { body: JSON.stringify(j) });
        }
      } catch (_) { /* not JSON: leave it */ }
    }
    return res;
  }
  catch (e) {
    if (e && e.unavailable) return json(503, { ok: false, error: "the data could not be read just now", errors: e.unavailable });
    console.warn("[employeeEfficiency] " + String((e && e.message) || e).slice(0, 200));
    return json(500, { ok: false, error: "the read failed, try again" });
  }
}

exports.handler = event => handle(event, null);
exports._t = { handle, buildAliases, fold, niceName, nyDay, nyMidnight, addDays, clip, covered, spanOf, summarize, parseCursor, cacheOf, LIM };
