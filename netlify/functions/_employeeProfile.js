/*  netlify/functions/_employeeProfile.js
 *  ─────────────────────────────────────────────────────────────────────────────
 *  The data behind the full-page employee view (Paul, 5 Oct 2026): ops `person` (with `range`) and `personOrders` of
 *  employeeEfficiency.js. Plan and shapes: plans/employee-hr/api.md (section E4). Reads, never writes. Nothing here calls Etsy.
 *
 *  employeeEfficiency.js hands this module a small "kit" of its own helpers (names, New York days, the cached rollup reader ...)
 *  so there is one set of rules for names, aliases, days and the sandbox, and one cache. See make(K) below.
 *
 *  What this file does:
 *    · LOADER   one cached read of the Efficiency_Daily rollups and the Station_Sessions of the window (and of the window before
 *               it, for "compared with the previous period"), joined per person per New York day (people / PD below);
 *               single Station_Activity events are read lazily, one query per person-day, only for the newest days.
 *    · KPIS     production, speed, time: each metric carries its label, unit, one-line definition and an "estimated" flag.
 *    · SERIES   per day, or per week when the window is longer than 92 days; 24 hour bins; the station mix.
 *    · DELTAS   every metric of the window next to the same metric of the window before it.
 *    · ORDERS   op personOrders: every order a person handled, newest first, with real-time search and paging.
 *    · HELPERS  attendance (days off, late/short days, calendar) and issues (rates, contact) are built by _employeeAttendance.js
 *               and _employeeIssues.js; this file calls them with the prepared context (ctx.prof, documented in api.md) and merges
 *               what they return. A helper that is missing or throws only leaves its fields out.
 *  ───────────────────────────────────────────────────────────────────────────── */
"use strict";

const RANGE_DAYS = { day: 1, week: 7, month: 30, quarter: 90, year: 365 };
const MAX_DAYS = 731, EVENT_DAYS = 14, DAILY_MAX = 92;     // EVENT_DAYS: how many of the newest days of a window have their single events read (speed medians)
const TTL = { resp: 30000, respLive: 2000, ev: 600000, evLive: 60000, info: 1800000, sessLive: 5000, sessPast: 600000 };
const CAP = { eventsPerDay: 2500, items: 200, orderPage: 100, infoSearch: 250 };
const RULES = { teamMinPeople: 2, minSignedMin: 15, shortFraction: 0.5, lateAfterMin: 30, activeGapMin: 5 };
const { displayStation } = require("./_activityKinds");   // ONE Sorting station: the stored keys "sorter" and "qr" are shown as "sorting" (history keeps its keys)
const STATION_LABEL = { sorting: "Sorting", welding: "Welding", assembly: "Assembly", shipping: "Shipping", design: "Design", laser: "Laser", inbox: "Inbox" };

/* (literal requires inside try blocks: the bundler follows them when the file exists and skips them when it does not yet)  */
const notFound = e => e && e.code === "MODULE_NOT_FOUND";
const failNote = e => (notFound(e) ? "" : "could not load: " + String((e && e.message) || e).slice(0, 120));
const ATT = { mod: null, err: "" }, ISS = { mod: null, err: "" };
try { ATT.mod = require("./_employeeAttendance"); } catch (e) { ATT.err = failNote(e); }
try { ISS.mod = require("./_employeeIssues"); } catch (e) { ISS.err = failNote(e); }

const ACTIVE_GAP_MS = 300000;      // contract: a gap of 5 minutes or less between two logged actions is working time
const WHY_LOG = "Working time is measured between logged actions: work a station does not record is missed, and phone scans are credited to the person signed in at the desktop.";
const WHY_SCAN = "Phone scans are credited to the person signed in at the desktop, and the sorter's name is typed, not checked.";
/** Every metric of the `kpis` object: its label, unit, which way is better, and the one-line plain definition shown in the hover card. */
const CATALOG = {
  // production
  parts: { label: "Pieces finished", unit: "pieces", better: "up", def: "Pieces the person logged as finished at a station, minus the pieces they took back with Undo. Days with nothing logged are left out, not counted as zero." },
  orders: { label: "Orders worked", unit: "orders", better: "up", def: "Different orders the person scanned or finished at any station. An order handled at two stations counts once; a customer conversation in the inbox is not an order worked." },
  ordersCompleted: { label: "Orders finished", unit: "orders", better: "up", def: "Orders the person marked finished at a station, minus the ones they undid. An order finished at two stations counts at each." },
  scans: { label: "Scans", unit: "scans", better: "up", est: true, why: WHY_SCAN, def: "Times an order code or number was scanned, or typed in as a scan." },
  piecesScanned: { label: "Pieces scanned", unit: "pieces", better: "up", est: true, why: WHY_SCAN, def: "Pieces on the orders the person scanned: an order of 3 pieces scanned once counts 3." },
  prints: { label: "Prints", unit: "prints", better: null, def: "Labels, seals, slips and sheets the person printed." },
  partsPerDay: { label: "Pieces per day", unit: "pieces/day", better: "up", def: "Pieces finished divided by the days on which something was logged." },
  ordersPerDay: { label: "Orders per day", unit: "orders/day", better: "up", def: "Orders worked divided by the days on which something was logged." },
  partsPerActiveHour: { label: "Pieces per working hour", unit: "pieces/hour", better: "up", est: true, why: WHY_LOG, def: "Pieces finished per hour of working time. Working time is the gaps of 5 minutes or less between one logged action and the next." },
  partsPerSignedHour: { label: "Pieces per signed-in hour", unit: "pieces/hour", better: "up", def: "Pieces finished per hour signed in at a station (days with logged activity only; two computers at once count once)." },
  ordersPerActiveHour: { label: "Orders per working hour", unit: "orders/hour", better: "up", est: true, why: WHY_LOG, def: "Orders worked per hour of working time (gaps of 5 minutes or less between logged actions)." },
  bestDay: { label: "Best day", unit: "pieces", better: "up", def: "The day with the most pieces finished in this window." },
  peakHour: { label: "Busiest hour", unit: "clock", better: null, def: "The hour of the day (New York time) in which the person finished the most pieces in this window." },
  dailyVariation: { label: "Day-to-day swing", unit: "percent", better: "down", def: "How much the daily pieces vary around their average: the standard deviation divided by the average. Lower means steadier. Needs 3 days with logged activity." },
  trend: { label: "Trend", unit: "pieces/day", better: "up", def: "How the daily pieces are changing, per week: the slope of the best-fitting steady trend through the days with logged activity (needs 5 such days). Positive means the days are getting bigger." },
  // speed
  secPerOrderMean: { label: "Working seconds per order", unit: "seconds", better: "down", est: true, why: WHY_LOG, def: "Working time divided by the orders worked: an average, including the time between orders." },
  secPerScanMean: { label: "Working seconds per scan", unit: "seconds", better: "down", est: true, why: WHY_LOG, def: "Working time divided by the scans." },
  secPerOrderMedian: { label: "Typical time on an order", unit: "seconds", better: "down", est: true, window: true, why: "Measured from logged actions only: it includes the lead-in before the first scan and cannot see work the station does not record.", def: "The middle value, in seconds, of the logged work time on each order the person finished at a station (the gaps of 5 minutes or less before each of their actions on it). Half the orders took less." },
  secPerOrderP90: { label: "Slow orders (90th percentile)", unit: "seconds", better: "down", est: true, window: true, why: "Measured from logged actions only: it includes the lead-in before the first scan and cannot see work the station does not record.", def: "Nine out of ten finished orders took less logged work time than this." },
  secBetweenScansMedian: { label: "Typical time between scans", unit: "seconds", better: "down", est: true, window: true, why: WHY_SCAN, def: "The middle time from one scan to the next, counting only scans less than 5 minutes apart on the same day." },
  // time
  signedHours: { label: "Signed-in time", unit: "hours", better: null, def: "Time at least one station had the person signed in. Two computers at once count once." },
  activeHours: { label: "Working time", unit: "hours", better: null, est: true, why: WHY_LOG, def: "The gaps of 5 minutes or less between one logged action and the next." },
  idleHours: { label: "Quiet time", unit: "hours", better: null, est: true, why: WHY_LOG, def: "The gaps longer than 5 minutes between logged actions, each counted up to 1 hour." },
  unloggedHours: { label: "Signed in, nothing logged", unit: "hours", better: null, est: true, why: WHY_LOG, def: "Signed-in time covered by neither working nor quiet time: the time after the last logged action of the day, or beyond the 1-hour cap on a gap." },
  activeShare: { label: "Working share", unit: "percent", better: "up", est: true, why: WHY_LOG, def: "Working time as a share of signed-in time (days with logged activity only)." }
};

/** The Welding station's own figures (the station is not counted in throughput: it shows time on task and the matched count instead). */
const WCAT = {
  weldingHours: { label: "Welding hours", unit: "hours", better: null, def: "Time signed in at the Welding station under the Welding task (welding the studs to the charm). It is signed-in time: welding has no scans, so it is never counted in pieces or orders. Two sign-ins under the same task at once count once." },
  matchingHours: { label: "Matching hours", unit: "hours", better: null, def: "Time signed in at the Welding station under the Matching task (matching the welded earrings to their orders with the scanner app and adding the backings). Two sign-ins under the same task at once count once." },
  matchedOrders: { label: "Orders matched", unit: "orders", better: null, est: true, why: "A phone scan is credited to the person signed in under Matching (the one with the latest input when two are), or to nobody when nobody is.", def: "Order codes scanned as Matching at the Welding station. Every scan counts, so an order scanned twice counts twice. Not a completion: the Welding station is not counted in orders finished." }
};

function make(K) {
  const KIND = K.KIND || { throughput: () => true, readStationCounters: (st, v) => v, UNATTRIBUTED: "Unattributed", echoScans: () => new Set() };      // (the Welding station is not counted in throughput: see _activityKinds.js)
  const { COL, LIM, ms, num, r1, zeros, digits, cleanName, okName, okStation, niceName, bestForm, nameKeyOf, canonOf, scrub, validDay, addDays,
    nyDay, nyMidnight, clip, covered, spanOf, cached, readRollups, readEventsStart, eventRow, col, json, safe, tmpl, KEYS } = K;
  const DAY = 86400000;

  /* ── days ── */
  const epochDay = day => Date.UTC(+day.slice(0, 4), +day.slice(5, 7) - 1, +day.slice(8, 10)) / DAY;
  const daysOf = (a, b) => { const out = []; for (let d = a, i = 0; d <= b && i < 2200; d = addDays(d, 1), i++) out.push(d); return out; };
  const dowOf = day => new Date(day + "T12:00:00Z").getUTCDay();                      // 0 = Sunday
  const mondayOf = day => addDays(day, -((dowOf(day) + 6) % 7));
  const spanLen = (a, b) => epochDay(b) - epochDay(a) + 1;
  const fin = v => (v == null || !Number.isFinite(+v) ? null : r1(+v));

  /** The window of a request: { range, from, to, days } or { error }. Windows end on `day` and are rolling, not calendar. */
  function resolveRange(ctx, body) {
    let to = body.day == null || body.day === "" ? ctx.today : String(body.day);
    if (!validDay(to)) return { error: "day must be YYYY-MM-DD" };
    if (to > ctx.today) to = ctx.today;
    let r = body.range;
    if (r && typeof r === "object" && !Array.isArray(r)) body = Object.assign({}, body, { from: r.from, to: r.to }), r = "custom";
    if (r === "custom" || (r == null && (body.from || body.to))) {
      const f = String(body.from || ""), t = String(body.to || "");
      if (!validDay(f) || !validDay(t)) return { error: "a custom range needs from and to as YYYY-MM-DD" };
      const end = t > ctx.today ? ctx.today : t;
      if (f > end) return { error: "from is after to" };
      if (spanLen(f, end) > MAX_DAYS) return { error: `a custom range is at most ${MAX_DAYS} days` };
      return { range: "custom", from: f, to: end, days: spanLen(f, end) };
    }
    const n = typeof r === "string" && Object.prototype.hasOwnProperty.call(RANGE_DAYS, r) ? RANGE_DAYS[r] : 0;      // (an own entry only: "constructor" is not a range)
    if (!n) return { error: "range must be day, week, month, quarter, year or {from,to}" };
    return { range: r, from: addDays(to, -(n - 1)), to, days: n };
  }

  /* ── the loader: rollups + sessions of everybody, joined per person per day ──────────────────────────────────────────── */
  /** Sessions that started in [aMs, zMs): the server's times are ms or Firestore times, and a range matches only one kind, so both are read. */
  function readSessionSeg(ctx, aMs, zMs, ttl) {
    return cached(ctx, `psess|${ctx.prefix}|${aMs}|${zMs}`, ttl, async () => {
      const TS = ctx.admin.firestore.Timestamp, ranges = [[aMs, zMs]];
      if (TS && typeof TS.fromMillis === "function") ranges.push([TS.fromMillis(aMs), TS.fromMillis(zMs)]);
      const snaps = await Promise.all(ranges.map(([a, z]) => col(ctx, COL.sessions).where("startAt", ">=", a).where("startAt", "<", z).orderBy("startAt", "desc").limit(LIM.sessions + 1).get()));
      const seen = new Set(), rows = []; let truncated = false;
      for (const s of snaps) { if (s.docs.length > LIM.sessions) truncated = true; for (const d of s.docs.slice(0, LIM.sessions)) if (!seen.has(d.id)) { seen.add(d.id); rows.push(Object.assign({ id: d.id }, d.data() || {})); } }
      await require("./_stationAutoSignout").settle({ db: ctx.db, prefix: ctx.prefix, now: ctx.now }, rows);       // the auto sign-out rules (idle, 5:00 pm Toronto, a page that died): the session ends at the person's last input
      return { rows, truncated };
    });
  }
  /** The session reads of the days from..to: aligned blocks (8 days for a short window, 32 for a long one) so the cache is reused by every
      window and every viewer; the block that holds yesterday and today is split so only its live part is read often. */
  function sessionSegments(ctx, from, to) {
    const size = spanLen(from, to) <= 70 ? 8 : 32, segs = [], live = addDays(ctx.today, -1);
    const first = Math.floor(epochDay(from) / size) * size, last = epochDay(to);
    const dayOfEpoch = e => new Date(e * DAY).toISOString().slice(0, 10);
    for (let e = first; e <= last; e += size) {
      const a = dayOfEpoch(e), z = dayOfEpoch(e + size);              // [a, z)
      const cut = (x, y, ttl) => { if (x < y) segs.push({ a: nyMidnight(x) - DAY, z: nyMidnight(y), ttl }); };   // (a day earlier: a session that crossed midnight)
      if (z <= live) cut(a, z, TTL.sessPast);
      else if (a >= live) cut(a, z, TTL.sessLive);
      else { cut(a, live, TTL.sessPast); cut(live, z, TTL.sessLive); }
    }
    return segs;
  }

  /** Rollups and sessions of the days from..to for EVERY person. Throws {unavailable} when both reads fail altogether. */
  async function loadRaw(ctx, from, to) {
    const errors = [], capped = [];
    const chunks = []; for (let d = from; d <= to; d = addDays(d, 31)) chunks.push([d, addDays(d, 30) > to ? to : addDays(d, 30)]);
    const segs = sessionSegments(ctx, from, to);
    const [rr, sr] = await Promise.all([
      Promise.all(chunks.map(c => safe(readRollups(ctx, c[0], c[1]), "rollups"))),
      Promise.all(segs.map(s => safe(readSessionSeg(ctx, s.a, s.z, s.ttl), "sessions")))]);
    const rollups = [], sessions = new Map();
    let rollOk = 0, sessOk = 0;
    for (const r of rr) { if (!r.ok) { errors.push("rollups: " + r.error); continue; } rollOk++; if (r.value.capped) capped.push("rollups"); rollups.push(...r.value.docs); }
    for (const r of sr) { if (!r.ok) { errors.push("sessions: " + r.error); continue; } sessOk++; if (r.value.truncated) capped.push("sessions"); for (const s of r.value.rows) if (!sessions.has(s.id)) sessions.set(s.id, s); }
    if (!rollOk && !sessOk) { const e = new Error("both reads failed"); e.unavailable = errors; throw e; }
    return { rollups, sessions: [...sessions.values()], errors, capped };
  }

  const newPD = day => ({ day, hasEvents: false, events: 0, st: {}, hours: zeros(24), hourScans: zeros(24), orders: new Map(), inFirst: 0, inLast: 0, spans: [], signedMs: 0, signedTpMs: 0, rawMs: 0, stMs: {}, taskMs: {}, firstIn: 0, lastOut: 0, liveSpan: false, forms: new Set() });

  /** Joins the raw rows into people → days (PD). `people` is keyed by the normalized name key (aliases merged). */
  function buildPeople(ctx, raw, from, to) {
    const people = new Map();
    const get = rawName => {
      const name = cleanName(rawName); if (!okName(name)) return null;
      const key = nameKeyOf(ctx, name); let P = people.get(key);
      if (!P) people.set(key, P = { key, canon: canonOf(ctx, key), formCount: new Map(), forms: new Set(), days: new Map(), display: "" });
      P.formCount.set(name, (P.formCount.get(name) || 0) + 1);
      return P;
    };
    const pdOf = (P, day) => { let x = P.days.get(day); if (!x) P.days.set(day, x = newPD(day)); return x; };
    for (const x of raw.rollups) {
      if (x.person === KIND.UNATTRIBUTED) continue;                       // (a matched scan made with nobody in Matching has no person: the Stations board counts it)
      const P = get(x.person); if (!P || !validDay(x.day) || x.day < from || x.day > to) continue;
      const pd = pdOf(P, x.day); pd.hasEvents = true; pd.events += Math.max(0, num(x.events));
      const form = String(x.person); pd.forms.add(form); P.forms.add(form);              // (the exact stored name: what a Station_Activity query must match)
      for (const [st0, v0] of Object.entries(x.stations && typeof x.stations === "object" ? x.stations : {})) {
        if (!okStation(st0) || !v0 || typeof v0 !== "object") continue;
        const st = displayStation(st0);                                // (counters stored under "sorter" or "qr" add to Sorting)
        const v = KIND.readStationCounters(st, v0);                          // (the Welding station keeps its scans, time and matched count, never pieces, orders or completions)
        const a = pd.st[st] || (pd.st[st] = tmpl()); for (const k of KEYS) a[k] += Math.max(0, num(v[k]));
        const fa = ms(v.firstAt), la = ms(v.lastAt); if (fa > 0 && (!pd.inFirst || fa < pd.inFirst)) pd.inFirst = fa; if (la > pd.inLast) pd.inLast = la;
      }
      for (const [hh, h] of Object.entries(x.hours && typeof x.hours === "object" ? x.hours : {})) {
        const hr = parseInt(hh, 10); if (!(hr >= 0 && hr < 24) || !h || typeof h !== "object") continue;
        let net = Math.max(0, num(h.parts)) - Math.max(0, num(h.undoParts));          // produced minus undone, the same net as the totals
        for (const [st, b] of Object.entries(h.by && typeof h.by === "object" ? h.by : {})) if (!KIND.throughput(st) && b && typeof b === "object") net -= Math.max(0, num(b.parts)) - Math.max(0, num(b.undoParts));   // (not the Welding station's)
        pd.hours[hr] += net; pd.hourScans[hr] += Math.max(0, num(h.scans));
      }
      for (const [id, m] of Object.entries(x.touched && typeof x.touched === "object" ? x.touched : {})) {
        const oid = digits(id); if (!oid) continue;
        const sts = m && typeof m === "object" ? [...new Set(Object.keys(m).filter(z => m[z] && okStation(z) && KIND.throughput(displayStation(z))).map(displayStation))] : [];     // (an order only the Welding station touched is not an order worked)
        if (!sts.length) continue;
        let set = pd.orders.get(oid); if (!set) pd.orders.set(oid, set = new Set());
        for (const k of sts) set.add(k);
      }
      const fa = ms(x.firstAt), la = ms(x.lastAt); if (fa > 0 && (!pd.inFirst || fa < pd.inFirst)) pd.inFirst = fa; if (la > pd.inLast) pd.inLast = la;
    }
    for (const s of raw.sessions) {
      const P = get(s.person), sp = spanOf(s, ctx.now); if (!P || !sp) continue;
      for (const c of clip(sp.start, sp.end, from, to)) {
        const pd = pdOf(P, c.day);
        pd.spans.push({ s: c.s, e: c.e, station: sp.station, task: sp.task || "", live: sp.live, start0: sp.start, endReason: String(s.endReason || ""), device: String(s.device || "") });
        if (sp.live) pd.liveSpan = true;
      }
    }
    for (const P of people.values()) {
      P.display = P.canon || niceName(bestForm(P.formCount));
      for (const pd of P.days.values()) {
        if (pd.spans.length) {
          pd.signedMs = covered(pd.spans.map(x => [x.s, x.e])); pd.rawMs = pd.spans.reduce((n, x) => n + (x.e - x.s), 0);
          const per = {}; for (const x of pd.spans) (per[x.station] || (per[x.station] = [])).push([x.s, x.e]);
          for (const [st, list] of Object.entries(per)) pd.stMs[st] = covered(list);
          pd.signedTpMs = covered(pd.spans.filter(x => KIND.throughput(x.station)).map(x => [x.s, x.e]));        // (signed in at a station that counts in throughput: the divisor of pieces per signed hour)
          const pt = {}; for (const x of pd.spans) if (x.station === "welding") (pt[x.task || "unknown"] || (pt[x.task || "unknown"] = [])).push([x.s, x.e]);   // the Welding station's time per task: a person in both tasks has both
          for (const [t, list] of Object.entries(pt)) pd.taskMs[t] = covered(list);
          pd.firstIn = Math.min(...pd.spans.map(x => x.s));
          pd.lastOut = pd.spans.some(x => x.live) ? 0 : Math.max(...pd.spans.map(x => x.e));
        } else if (pd.inFirst) { pd.firstIn = pd.inFirst; pd.lastOut = pd.inLast; pd.noSession = true; }   // (a station that logs but signs nobody in: the first and last action stand in)
      }
    }
    return people;
  }

  /** One person-day of single events (cached: a past day 10 min, today 60 s). The query is two equalities (person, day): no composite index. */
  function readPersonDayEvents(ctx, form, day) {
    return cached(ctx, `pev|${ctx.prefix}|${form}|${day}`, day === ctx.today ? TTL.evLive : TTL.ev, async () => {
      const snap = await col(ctx, COL.activity).where("person", "==", form).where("day", "==", day).limit(CAP.eventsPerDay + 1).get();
      const all = []; for (const d of snap.docs.slice(0, CAP.eventsPerDay)) { const e = eventRow(d.id, d.data() || {}, ctx); if (e) all.push(e); }
      const echo = KIND.echoScans(all), rows = echo.size ? all.filter(e => !echo.has(e)) : all;       // (a phone scan is the matched event: the desk page's own scan of it is not a second one)
      return { rows, capped: snap.docs.length > CAP.eventsPerDay };
    });
  }

  /** The single events of one person (key) for the days fromDay..toDay, oldest first, spellings merged. Only days that have a rollup are queried.
      `sink` ({errors, capped}) collects a failed or cut-short read. */
  async function readEvents(ctx, people, key, fromDay, toDay, sink) {
    const P = people.get(key); if (!P) return [];
    const jobs = [];
    for (const day of daysOf(fromDay, toDay)) { const pd = P.days.get(day); if (pd && pd.hasEvents) for (const f of pd.forms) jobs.push(safe(readPersonDayEvents(ctx, f, day), "events " + day)); }
    const rows = [];
    for (const r of await Promise.all(jobs)) {
      if (!r.ok) { sink.errors.push("events: " + r.error); continue; }
      if (r.value.capped && !sink.capped.includes("events")) sink.capped.push("events");
      rows.push(...r.value.rows);
    }
    return rows.sort((a, b) => a.at - b.at || a.k - b.k || (a.id < b.id ? -1 : 1));
  }

  /** Prepares ctx.prof: everything a helper needs about ONE person and the window (documented in api.md, "ctx.prof"). */
  async function prepare(ctx, name, from, to, loadFrom) {
    const raw = await loadRaw(ctx, loadFrom || from, to);
    const people = buildPeople(ctx, raw, loadFrom || from, to);
    const key = nameKeyOf(ctx, name), me = people.get(key) || null;
    const prof = {
      mode: ctx.prefix ? "sandbox" : "real", name: me ? me.display : (canonOf(ctx, key) || niceName(name)), key, found: !!me, spellings: me ? [...me.formCount.keys()].sort() : [],
      from, to, loadFrom: loadFrom || from, today: ctx.today, now: ctx.now,
      raw, people, me, errors: raw.errors.slice(), capped: raw.capped.slice(),
      daysOf, dowOf, mondayOf, spanLen, nyMidnight, nyDay, addDays, cleanName, okName, scrub, digits, safe, rules: RULES, stationLabel: STATION_LABEL, eventDays: EVENT_DAYS,
      col: name => col(ctx, name),                              // a collection of THIS mode (the Sandbox_ copy in the sandbox): never use db.collection(name) directly
      nameKey: n => nameKeyOf(ctx, n),                          // any spelling → the key of `people` (aliases applied)
      cached: (key, ttl, fn) => cached(ctx, key, ttl, fn),      // the shared in-memory cache (sandbox: 5 s at most)
      /** The single events of ONE person (default: the one asked for; `key` = another person's name key) for the days fromDay..toDay,
          oldest first, spellings merged: [{id, at, k, day, person, station, device, action, orderId, parts, detail, sincePrevMs}].
          Only days that have a rollup are queried. A read that fails or is cut at its cap is noted in prof.errors / prof.capped. */
      async events(fromDay, toDay, opts) { return readEvents(ctx, people, opts && opts.key ? opts.key : key, fromDay < prof.loadFrom ? prof.loadFrom : fromDay, toDay, prof); }
    };
    ctx.prof = prof;
    return prof;
  }

  /* ── person-day sums, windows, metrics ─────────────────────────────────────────────────────────────────────────────────── */
  /** The orders of a person-day that were PRODUCTION work: an order only seen at the inbox (a customer conversation) is not an order worked. */
  const workRids = pd => { const out = []; for (const [rid, set] of pd.orders) for (const k of set) if (k !== "inbox") { out.push(rid); break; } return out; };
  /** One person-day in plain numbers (cached on the PD). Pieces are NET of undo; active/idle never exceed the time signed in when two computers overlapped. */
  function daySum(pd) {
    if (pd._sum) return pd._sum;
    let parts = 0, ordersFin = 0, scans = 0, scanParts = 0, completes = 0, prints = 0, rejects = 0, errors = 0, notes = 0, undos = 0, undoOrders = 0, active = 0, activeTp = 0, idle = 0, matched = 0, tp = false;
    const st = {};
    for (const name of new Set(Object.keys(pd.st).concat(Object.keys(pd.stMs)))) {
      const a = pd.st[name] || tmpl(), p = Math.max(0, a.parts - a.undoParts), o = Math.max(0, a.orders - a.undoOrders), counted = KIND.throughput(name);
      parts += p; ordersFin += o; scans += a.scans; scanParts += a.scanParts; completes += a.completes; prints += a.prints; rejects += a.rejects; errors += a.errors; notes += a.notes; undos += a.undos; undoOrders += a.undoOrders; active += a.activeMs; idle += a.idleMs; matched += a.matched;
      if (counted) { activeTp += a.activeMs; if (a.scans + a.completes + a.prints + a.rejects + a.errors + a.undos + a.notes > 0) tp = true; }     // (tp: something was logged at a station that counts in throughput that day)
      st[name] = { parts: p, ordersFin: o, scans: a.scans, scanParts: a.scanParts, completes: a.completes, prints: a.prints, activeMs: a.activeMs, matched: a.matched, minMs: pd.stMs[name] > 0 ? pd.stMs[name] : a.activeMs };
    }
    if (pd.rawMs > pd.signedMs) { active = Math.min(active, pd.signedMs); activeTp = Math.min(activeTp, pd.signedMs); idle = Math.min(idle, Math.max(0, pd.signedMs - active)); }
    return (pd._sum = { hasEvents: pd.hasEvents, tp: pd.hasEvents && tp, parts, orders: pd.orders.size || ordersFin, ordersFin, scans, scanParts, completes, prints, rejects, errors, notes, undos, undoOrders, matched, events: pd.events, activeMs: active, activeTpMs: activeTp, idleMs: idle, signedMs: pd.signedMs, signedTpMs: pd.signedTpMs, st });
  }

  /** Everything about the days from..to of one person (P may be null): sums over the days that have logged activity, plus the per-day list. */
  function aggregate(P, from, to) {
    const A = { from, to, nDays: spanLen(from, to), evDays: 0, tpDays: 0, sessDays: 0, parts: 0, rids: new Set(), ordersFallback: 0, ordersFin: 0, scans: 0, scanParts: 0, completes: 0, prints: 0, rejects: 0, errors: 0, undos: 0, undoOrders: 0, events: 0, matched: 0,
      activeMs: 0, activeTpMs: 0, idleMs: 0, signedMs: 0, signedMsEv: 0, signedTpMsEv: 0, hours: zeros(24), hourScans: zeros(24), st: new Map(), days: [] };
    for (const day of daysOf(from, to)) {
      const pd = P && P.days.get(day);
      if (!pd) { A.days.push({ day, pd: null, sum: null }); continue; }
      const s = daySum(pd); A.days.push({ day, pd, sum: s });
      if (pd.signedMs > 0) { A.sessDays++; A.signedMs += pd.signedMs; }
      if (!pd.hasEvents) continue;
      A.evDays++; A.signedMsEv += pd.signedMs; A.signedTpMsEv += pd.signedTpMs; if (s.tp) A.tpDays++;
      A.matched += s.matched; A.activeTpMs += s.activeTpMs;
      A.parts += s.parts; A.ordersFin += s.ordersFin; A.scans += s.scans; A.scanParts += s.scanParts; A.completes += s.completes; A.prints += s.prints; A.rejects += s.rejects; A.errors += s.errors; A.undos += s.undos; A.undoOrders += s.undoOrders;
      A.events += s.events; A.activeMs += s.activeMs; A.idleMs += s.idleMs;
      const work = workRids(pd); for (const rid of work) A.rids.add(rid);
      if (!work.length) A.ordersFallback += s.ordersFin;
      for (let h = 0; h < 24; h++) { A.hours[h] += pd.hours[h]; A.hourScans[h] += pd.hourScans[h]; }
      for (const [name, x] of Object.entries(s.st)) {
        let t = A.st.get(name); if (!t) A.st.set(name, t = { parts: 0, rids: new Set(), fallback: 0, scans: 0, completes: 0, prints: 0, activeMs: 0, minMs: 0, matched: 0 });
        t.matched += x.matched; t.parts += x.parts; t.scans += x.scans; t.completes += x.completes; t.prints += x.prints; t.activeMs += x.activeMs; t.minMs += x.minMs;
        let any = false; for (const [rid, set] of pd.orders) if (set.has(name)) { t.rids.add(rid); any = true; }
        if (!any) t.fallback += x.ordersFin;
      }
    }
    A.orders = A.rids.size + A.ordersFallback;
    return A;
  }

  const median = a => { if (!a.length) return null; const s = a.slice().sort((x, y) => x - y), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
  const pctile = (a, p) => { if (!a.length) return null; const s = a.slice().sort((x, y) => x - y); return s[Math.max(0, Math.min(s.length - 1, Math.ceil(p / 100 * s.length) - 1))]; };

  /** One person's single events (oldest first) → what took how long: steps per (order, station) and the gaps between scans. */
  function stepsOf(events) {
    const steps = new Map(), scanGaps = []; let prevAt = 0, prevDay = "";
    for (const e of events) {
      if (e.orderId) {
        const k = e.orderId + "|" + e.station; let s = steps.get(k);
        if (!s) steps.set(k, s = { rid: e.orderId, station: e.station, firstAt: e.at, lastAt: e.at, day: e.day, workMs: 0, scans: 0, completes: 0, prints: 0, parts: 0, undos: 0, rejects: 0, errors: 0, notes: 0, details: [] });
        s.firstAt = Math.min(s.firstAt, e.at); s.lastAt = Math.max(s.lastAt, e.at);
        if (e.sincePrevMs > 0 && e.sincePrevMs <= ACTIVE_GAP_MS) s.workMs += e.sincePrevMs;
        if (e.action === "scan" || e.action === "matched") s.scans++; else if (e.action === "complete") { s.completes++; s.parts += e.parts; } else if (e.action === "print") s.prints++;
        else if (e.action === "undo") { s.undos++; s.parts = Math.max(0, s.parts - e.parts); } else if (e.action === "reject") s.rejects++; else if (e.action === "error") s.errors++; else s.notes++;
        if (e.detail && s.details.length < 12) s.details.push({ at: e.at, action: e.action, detail: e.detail });
      }
      if (e.action === "scan" || e.action === "matched") { if (prevAt && e.day === prevDay && e.at > prevAt && e.at - prevAt <= ACTIVE_GAP_MS) scanGaps.push((e.at - prevAt) / 1000); prevAt = e.at; prevDay = e.day; }
    }
    return { steps, scanGaps };
  }
  /** The event-based speed numbers of a window. null when the window has no logged day at all. */
  async function eventStats(prof, from, to) {
    const evFrom = from > addDays(to, -(EVENT_DAYS - 1)) ? from : addDays(to, -(EVENT_DAYS - 1));
    if (!prof.me || !daysOf(evFrom, to).some(d => { const pd = prof.me.days.get(d); return pd && pd.hasEvents; })) return null;
    const events = await prof.events(evFrom, to), { steps, scanGaps } = stepsOf(events);
    const secs = [...steps.values()].filter(s => s.completes > 0 && s.workMs > 0 && s.station !== "inbox" && KIND.throughput(s.station)).map(s => s.workMs / 1000);   // (an inbox conversation is not an order step)
    return { from: evFrom, to, days: spanLen(evFrom, to), events: events.length, orders: secs.length, secMedian: median(secs), secP90: pctile(secs, 90), gapMedian: median(scanGaps), gaps: scanGaps.length };
  }

  /** The metric numbers of a window (null = not known). */
  function valuesOf(A, E) {
    // (the Welding station is not counted in throughput: pieces, orders and the per-hour and per-day rates look at the days and the working / signed-in time of the OTHER stations only; scans, prints and time still count everywhere)
    const has = A.evDays > 0, hasP = A.tpDays > 0, activeH = A.activeMs / 3600000, activeTpH = A.activeTpMs / 3600000, signedTpH = A.signedTpMsEv / 3600000, per = (n, d) => (d > 0 ? n / d : null);
    const v = { parts: hasP ? A.parts : null, orders: hasP ? A.orders : null, ordersCompleted: hasP ? A.ordersFin : null, scans: has ? A.scans : null, piecesScanned: has ? A.scanParts : null, prints: has ? A.prints : null,
      partsPerDay: hasP ? per(A.parts, A.tpDays) : null, ordersPerDay: hasP ? per(A.orders, A.tpDays) : null,
      partsPerActiveHour: hasP && A.activeTpMs >= 60000 ? A.parts / activeTpH : null, partsPerSignedHour: hasP && A.signedTpMsEv >= 60000 ? A.parts / signedTpH : null, ordersPerActiveHour: hasP && A.activeTpMs >= 60000 ? A.orders / activeTpH : null,
      secPerOrderMean: hasP && A.orders > 0 && A.activeTpMs >= 60000 ? A.activeTpMs / 1000 / A.orders : null, secPerScanMean: has && A.scans > 0 && A.activeMs >= 60000 ? A.activeMs / 1000 / A.scans : null,
      secPerOrderMedian: E ? E.secMedian : null, secPerOrderP90: E ? E.secP90 : null, secBetweenScansMedian: E ? E.gapMedian : null,
      signedHours: A.sessDays > 0 ? A.signedMs / 3600000 : null, activeHours: has ? activeH : null, idleHours: has ? A.idleMs / 3600000 : null,
      unloggedHours: has && A.signedMsEv > 0 ? Math.max(0, A.signedMsEv - A.activeMs - A.idleMs) / 3600000 : null, activeShare: has && A.signedMsEv >= 60000 ? Math.min(100, A.activeMs / A.signedMsEv * 100) : null };
    // best day, busiest hour, steadiness, trend
    let best = null; const ys = [];
    for (const d of A.days) if (d.sum && d.sum.tp) { if (!best || d.sum.parts > best.parts) best = { day: d.day, parts: d.sum.parts }; ys.push([epochDay(d.day) - epochDay(A.from), d.sum.parts]); }
    v.bestDay = best ? best.parts : null; v.bestDayDay = best ? best.day : null;
    const hsum = A.hours.reduce((n, x) => n + Math.max(0, x), 0);
    v.peakHour = hsum > 0 ? A.hours.indexOf(Math.max(...A.hours)) * 60 : null;
    const mean = ys.length ? ys.reduce((n, p) => n + p[1], 0) / ys.length : 0;
    v.dailyVariation = ys.length >= 3 && mean > 0 ? Math.sqrt(ys.reduce((n, p) => n + (p[1] - mean) ** 2, 0) / ys.length) / mean * 100 : null;
    if (ys.length >= 5) { const mx = ys.reduce((n, p) => n + p[0], 0) / ys.length, sxx = ys.reduce((n, p) => n + (p[0] - mx) ** 2, 0); v.trend = sxx > 0 ? ys.reduce((n, p) => n + (p[0] - mx) * (p[1] - mean), 0) / sxx * 7 : null; } else v.trend = null;
    return v;
  }
  const NS = { parts: A => A.tpDays, orders: A => A.tpDays, partsPerDay: A => A.tpDays, ordersPerDay: A => A.tpDays, secPerOrderMean: A => A.orders, secPerScanMean: A => A.scans, dailyVariation: A => A.tpDays, trend: A => A.tpDays };
  function metric(key, cur, prev, extra) {
    const c = CATALOG[key] || WCAT[key], x = fin(cur), p = fin(prev);
    const o = { label: c.label, unit: c.unit, value: x, prev: p, delta: x != null && p != null ? r1(x - p) : null, deltaPct: x != null && p != null && p !== 0 ? r1((x - p) / Math.abs(p) * 100) : null, better: c.better || null, def: c.def, estimated: !!c.est };
    if (c.why) o.why = c.why;
    if (c.window) o.window = true;
    return Object.assign(o, extra || {});
  }

  /** The points of the chart: one per day, or one per week (Monday..Sunday, clipped to the window) when the window is long. */
  function seriesOf(A, weekly) {
    const buckets = [];
    if (!weekly) for (const d of A.days) buckets.push({ day: d.day, to: d.day, items: [d] });
    else { let cur = null; for (const d of A.days) { const m = mondayOf(d.day); if (!cur || cur.mon !== m) { cur = { mon: m, day: d.day, to: d.day, items: [] }; buckets.push(cur); } cur.to = d.day; cur.items.push(d); } }
    return buckets.map(b => {
      const ev = b.items.filter(d => d.sum && d.sum.hasEvents), sum = k => ev.reduce((n, d) => n + d.sum[k], 0), has = ev.length > 0, hasP = ev.some(d => d.sum.tp), rids = new Set(); let fb = 0;
      for (const d of ev) { const work = workRids(d.pd); for (const rid of work) rids.add(rid); if (!work.length) fb += d.sum.ordersFin; }
      const active = sum("activeTpMs"), signedEv = ev.reduce((n, d) => n + d.sum.signedTpMs, 0), parts = sum("parts"), orders = rids.size + fb;
      const first = b.items.length === 1 && b.items[0].pd ? b.items[0].pd : null;
      return { day: b.day, to: b.to, days: b.items.length, workedDays: b.items.filter(d => d.pd && (d.pd.signedMs > 0 || d.pd.hasEvents)).length, hasData: has,
        parts: hasP ? parts : null, orders: hasP ? orders : null, scans: has ? sum("scans") : null, completes: has ? sum("completes") : null, prints: has ? sum("prints") : null, rejects: has ? sum("rejects") : null, errors: has ? sum("errors") : null, undos: has ? sum("undos") : null,
        activeMs: has ? sum("activeMs") : null, idleMs: has ? sum("idleMs") : null, signedMs: b.items.reduce((n, d) => n + (d.pd ? d.pd.signedMs : 0), 0),
        perActiveHour: hasP && active >= 60000 ? r1(parts / (active / 3600000)) : null, perSignedHour: hasP && signedEv >= 60000 ? r1(parts / (signedEv / 3600000)) : null, secPerOrder: hasP && orders > 0 && active >= 60000 ? r1(active / 1000 / orders) : null,
        firstIn: first && first.firstIn ? first.firstIn : null, lastOut: first && first.lastOut ? first.lastOut : null, shiftMs: first && first.firstIn && first.lastOut ? first.lastOut - first.firstIn : null };
    });
  }

  /* ── the helpers of other workers: feature-detected, never fatal (a missing file leaves its fields out; a broken one is named in `errors`) ── */
  const helper = (mod, err, fname) => { const f = mod && (typeof mod[fname] === "function" ? mod[fname] : typeof mod === "function" ? mod : null); return { f, err }; };
  const HELPERS = { attendance: helper(ATT.mod, ATT.err, "attendance"), issues: helper(ISS.mod, ISS.err, "issues") };
  async function callHelper(ctx, name, args) {
    const h = HELPERS[name];
    if (!h.f) { if (h.err) ctx.prof.errors.push(name + ": " + h.err); return null; }
    try { return await h.f(ctx, args); } catch (e) { ctx.prof.errors.push(name + ": " + String((e && e.message) || e).slice(0, 160)); return null; }
  }

  /** The Welding station's time per task and matched scans over the days of one aggregate: ms per task (a person in both tasks has both), the station's own time (both tasks once), the matched count. */
  function weldingTotals(A) {
    const t = { welding: 0, matching: 0, unknown: 0, station: 0, matched: 0 };
    for (const d of A.days) {
      if (!d.pd) continue;
      for (const k of ["welding", "matching", "unknown"]) t[k] += d.pd.taskMs[k] || 0;
      t.station += d.pd.stMs.welding || 0;
      if (d.sum && d.pd.hasEvents && d.sum.st.welding) t.matched += d.sum.st.welding.matched || 0;
    }
    return t;
  }
  /** The same buckets as `series` (a day each, or a week each for a long window): the Welding station's time per task and matched scans in each. */
  function weldingSeries(A, weekly) {
    const buckets = [];
    if (!weekly) for (const d of A.days) buckets.push({ day: d.day, to: d.day, items: [d] });
    else { let cur = null; for (const d of A.days) { const m = mondayOf(d.day); if (!cur || cur.mon !== m) { cur = { mon: m, day: d.day, to: d.day, items: [] }; buckets.push(cur); } cur.to = d.day; cur.items.push(d); } }
    return buckets.map(b => {
      const t = weldingTotals({ days: b.items }), any = b.items.some(d => d.pd && (d.pd.stMs.welding > 0 || (d.sum && d.sum.st.welding)));
      return { day: b.day, to: b.to, days: b.items.length, weldingMs: any ? t.welding : null, matchingMs: any ? t.matching : null, unknownMs: any ? t.unknown : null, matched: any ? t.matched : null };
    });
  }
  const weldingBlock = (A, AP, rg, weekly, compare) => {
    const t = weldingTotals(A), tp = AP ? weldingTotals(AP) : null;
    if (!(t.station > 0 || t.matched > 0 || t.welding + t.matching + t.unknown > 0)) return null;     // nothing at the Welding station in this window
    const H = ms => (ms == null ? null : ms / 3600000);
    return { label: "Welding station", def: "Time on task at the Welding station and the order codes matched there. The station is not counted in pieces, orders finished or per-hour rates: it shows hours per task and the matched count instead.",
      hours: { welding: r1(H(t.welding)), matching: r1(H(t.matching)), unknown: r1(H(t.unknown)), station: r1(H(t.station)) }, matched: t.matched,
      metrics: { weldingHours: metric("weldingHours", H(t.welding), tp ? H(tp.welding) : null), matchingHours: metric("matchingHours", H(t.matching), tp ? H(tp.matching) : null), matchedOrders: metric("matchedOrders", t.matched, tp ? tp.matched : null) },
      series: weldingSeries(A, weekly) };
  };

  /** op person WITH a range: the profile. */
  async function opProfile(ctx, body) {
    const name = cleanName(body.name);
    if (!okName(name)) return json(400, { ok: false, error: "name required" });
    const rg = resolveRange(ctx, body); if (rg.error) return json(400, { ok: false, error: rg.error });
    const compare = body.compare !== false && body.compare !== 0 && body.compare !== "0";
    const prev = compare ? { from: addDays(rg.from, -rg.days), to: addDays(rg.from, -1), days: rg.days } : null;
    // E10's list of issue items is paged: issuesLimit (1..200, default 50) and issuesCursor (the `issues.next` of the page before); crossCheck:false skips its look-ups of other people's later work on the newest orders
    const opt = { limit: Math.max(1, Math.min(200, Math.floor(num(body.issuesLimit)) || 50)), cursor: typeof body.issuesCursor === "string" ? body.issuesCursor.slice(0, 200) : "", crossCheck: body.crossCheck !== false && body.crossCheck !== 0 && body.crossCheck !== "0" };
    const ckey = `prof|${ctx.prefix}|${nameKeyOf(ctx, name)}|${rg.from}|${rg.to}|${compare ? 1 : 0}|${opt.limit}|${opt.cursor}|${opt.crossCheck ? 1 : 0}`;
    const out = await cached(ctx, ckey, rg.to === ctx.today ? TTL.respLive : TTL.resp, () => buildProfile(ctx, name, rg, prev, opt));
    if (out && Array.isArray(out.errors) && out.errors.length) ctx.cache.memo.delete(ckey);      // (an answer with a failed read in it is not kept: once the source is back the next call is whole)
    return json(200, out);
  }

  async function buildProfile(ctx, name, rg, prev, opt) {
    // nothing was logged before the first rollup day (one document, kept an hour): do not query the empty years before it (31 days of slack for sign-ins)
    const start = await safe(readEventsStart(ctx), "start");
    let loadFrom = prev ? prev.from : rg.from;
    if (start.ok && start.value) { const floor = addDays(start.value, -31); if (loadFrom < floor) loadFrom = floor > rg.to ? rg.to : floor; }
    const prof = await prepare(ctx, name, rg.from, rg.to, loadFrom);
    prof.prev = prev;
    const [attR, issR, evCur, evPrev] = await Promise.all([
      callHelper(ctx, "attendance", { name: prof.name, from: rg.from, to: rg.to, prev }), callHelper(ctx, "issues", { name: prof.name, from: rg.from, to: rg.to, prev, limit: opt.limit, cursor: opt.cursor, eventDays: EVENT_DAYS, crossCheck: opt.crossCheck }),
      safe(eventStats(prof, rg.from, rg.to), "events"), prev ? safe(eventStats(prof, prev.from, prev.to), "events") : Promise.resolve({ ok: true, value: null })]);
    for (const r of [evCur, evPrev]) if (!r.ok) prof.errors.push("events: " + r.error);
    const accept = (name, r) => {                                  // a helper's own failure or partial answer is named here, never passed on as data
      if (!r || typeof r !== "object") return null;
      if (r.ok === false) { prof.errors.push(`${name}: ${String(r.error || (r.unavailable ? "the data could not be read" : "failed")).slice(0, 120)}`); return null; }
      if (r.partial && Array.isArray(r.errors)) for (const e of r.errors.slice(0, 4)) prof.errors.push(`${name}: ${String(e).slice(0, 120)}`);
      return r;
    };
    const att = accept("attendance", attR), iss = accept("issues", issR);
    const E = evCur.ok ? evCur.value : null, EP = evPrev.ok ? evPrev.value : null;
    const A = aggregate(prof.me, rg.from, rg.to), AP = prev ? aggregate(prof.me, prev.from, prev.to) : null;
    const v = valuesOf(A, E), pv = AP ? valuesOf(AP, EP) : null;
    const trackingStart = start.ok ? start.value : "";
    const kpis = {};
    for (const key of Object.keys(CATALOG)) {
      const extra = {}; const n = NS[key] ? NS[key](A) : null;
      if (CATALOG[key].window) { const w = E; extra.n = key === "secBetweenScansMedian" ? (w ? w.gaps : 0) : (w ? w.orders : 0); }
      else if (n != null) extra.n = n;
      if (key === "bestDay" && v.bestDayDay) extra.day = v.bestDayDay;
      kpis[key] = metric(key, v[key], pv ? pv[key] : null, extra);
    }
    const hoursTotal = A.hours.map(x => Math.max(0, r1(x))), hours = hoursTotal.map((parts, hour) => ({ hour, parts, scans: Math.round(A.hourScans[hour]), perDay: A.evDays > 0 ? r1(parts / A.evDays) : null }));
    const totalMin = [...A.st.values()].reduce((n, t) => n + t.minMs, 0);
    const stations = [...A.st.entries()].map(([station, t]) => Object.assign({ station, label: STATION_LABEL[station] || station, parts: t.parts, orders: t.rids.size + t.fallback, scans: t.scans, completes: t.completes, prints: t.prints, minutes: r1(t.minMs / 60000),
      shareParts: A.parts > 0 && KIND.throughput(station) ? r1(t.parts / A.parts * 100) : null, shareMinutes: totalMin > 0 ? r1(t.minMs / totalMin * 100) : null, perActiveHour: KIND.throughput(station) && t.activeMs >= 60000 ? r1(t.parts / (t.activeMs / 3600000)) : null }, KIND.throughput(station) ? {} : { matched: t.matched }))
      .sort((a, b) => b.parts - a.parts || b.minutes - a.minutes || (a.station < b.station ? -1 : 1));
    const weekly = rg.days > DAILY_MAX, series = seriesOf(A, weekly), welding = weldingBlock(A, AP, rg, weekly, !!prev);
    const notes = [];
    if (!prof.found) notes.push(`There is no record of ${prof.name} in the days that were read.`);
    else if (!A.evDays) notes.push(A.sessDays ? "Nothing was logged at the stations in this window: only sign-in time is known." : "Nothing was logged and nobody signed in under this name in this window.");
    if (trackingStart && rg.from < trackingStart) notes.push(`Station activity has been recorded since ${trackingStart}; earlier days show sign-in time only.`);
    if (E && E.days < rg.days) notes.push(`Typical time on an order and between scans come from single events of the newest ${E.days} days of the window (${E.from} to ${E.to}); everything else covers the whole window.`);
    if (prof.capped.length) notes.push("Some lists were cut at their size limit: " + [...new Set(prof.capped)].join(", ") + ".");
    const out = { ok: true, now: ctx.now, mode: prof.mode, name: prof.name, found: prof.found, spellings: prof.spellings, range: rg.range, from: rg.from, to: rg.to, days: rg.days, today: ctx.today, live: rg.to === ctx.today, prev,
      granularity: weekly ? "week" : "day", trackingStart, eventWindow: E ? { from: E.from, to: E.to, days: E.days, capped: prof.capped.includes("events") } : null, rules: RULES,
      kpis, series, hours, stations, welding, calendar: [], cannotTell: cannotTell(trackingStart, rg), notes };
    if (att) { const { calendar, ...rest } = att; out.attendance = rest; if (Array.isArray(calendar)) out.calendar = calendar; }     // (the calendar once, at the top level: E5 draws it from there)
    if (iss) {
      if (iss.issues) out.issues = iss.issues; if (iss.rates) out.rates = iss.rates; if (iss.contact) out.contact = iss.contact; if (iss.definitions) out.definitions = iss.definitions;
      if (Array.isArray(iss.notes)) for (const n of iss.notes.slice(0, 8)) if (typeof n === "string" && n) out.notes.push(n.slice(0, 300));
    }
    if (prof.errors.length || prof.capped.length) { out.partial = true; if (prof.errors.length) out.errors = [...new Set(prof.errors)].slice(0, 12); }
    return out;
  }
  const cannotTell = (trackingStart, rg) => {
    const t = [
      { topic: "Effort and quality", text: "These numbers show logged activity, not effort or the quality of the work. Work a station does not record (talking to a customer, fixing a piece by hand) looks like quiet time." },
      { topic: "Phone scans", text: "A phone scanner sends the order number to the desktop at its station, so the scan is credited to the person signed in at that desktop." },
      { topic: "Typed names", text: "The sorter and the design pages take a typed name that is not checked: a misspelling or a shared login mixes people. Spellings of one name are merged only by the name rules and config/employeeAliases." },
      { topic: "Orders that came back", text: "When somebody else reopens or redoes an order, the data shows that person's Undo, not a link back to the person who finished it first." },
      { topic: "Complaints and breakage", text: "No record of customer complaints, returns or breakage is kept per person. The QA1 and QA2 notes in an order's chat are approval stamps, not fails." }];
    if (trackingStart && rg.from < trackingStart) t.push({ topic: "Before tracking", text: `Station activity is recorded from ${trackingStart}. Earlier days have sign-in time only.` });
    return t;
  };

  /* ── op personOrders: every order a person handled, newest first, with real-time search ───────────────────────────────── */
  const MONTHS = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"];
  const WEEKDAYS = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"];
  const WORDS = new Set([...MONTHS, ...MONTHS.map(m => m.slice(0, 3)), ...WEEKDAYS, ...WEEKDAYS.map(m => m.slice(0, 3)), ...Object.keys(STATION_LABEL), ...Object.values(STATION_LABEL).map(x => x.toLowerCase()), "sorter", "qr", "printer", "labels"]);        // (the Sorter app and the QR Printer are pages of Sorting: those words still find its orders)
  const isWordPrefix = t => { for (const w of WORDS) if (w.startsWith(t)) return true; return false; };
  const MONTH_WORDS = new Set([...MONTHS, ...MONTHS.map(m => m.slice(0, 3))]);
  /** The search words: "oct 3" and "3 oct" are ONE word (a date), not "oct" and "3"; so are 2026-10-03 and 10/3 (matched whole, so 10/3 is not 10/30). */
  function searchTokens(q) {
    const raw = q.toLowerCase().split(/[\s,;]+/).filter(Boolean), out = [];
    for (let i = 0; i < raw.length && out.length < 8; i++) {
      const a = raw[i], b = raw[i + 1];
      if (b && MONTH_WORDS.has(a) && /^\d{1,2}$/.test(b)) { out.push({ t: a + " " + +b, date: true }); i++; }
      else if (b && /^\d{1,2}$/.test(a) && MONTH_WORDS.has(b)) { out.push({ t: +a + " " + b, date: true }); i++; }
      else out.push({ t: a, date: /^\d{4}-\d{2}-\d{2}$/.test(a) || /^\d{1,2}\/\d{1,2}$/.test(a) });
    }
    return out;
  }
  /** The ways a date can be typed: 2026-10-03, 10/3, 10/03, oct 3, october 3, 3 oct, a month, a weekday. */
  function dateWords(day) {
    const mo = +day.slice(5, 7), d = +day.slice(8, 10), m = MONTHS[mo - 1], wd = WEEKDAYS[dowOf(day)];
    return [day, day.slice(0, 7), `${mo}/${d}`, `${String(mo).padStart(2, "0")}/${String(d).padStart(2, "0")}`, m, m.slice(0, 3), `${m} ${d}`, `${m.slice(0, 3)} ${d}`, `${d} ${m.slice(0, 3)}`, `${d} ${m}`, wd, wd.slice(0, 3)].join(" ");
  }
  const ISSUE_LABEL = { undone: "Completion undone", refused: "Order refused", cancelAlert: "Cancelled-order alert", heldOrSkipped: "Held or skipped", failed: "Action failed", lookupFailed: "Etsy lookup failed", reprint: "Printed again", rescan: "Scanned again" };
  /** One event → the issue kind it shows, or "". Prints and scans are judged by the caller (the second one of an order is the repeat). */
  function issueKind(e) {
    if (e.action === "undo") return "undone";
    if (e.action === "reject") return /^cancelled order/i.test(e.detail) ? "cancelAlert" : /^(held|skipped):/i.test(e.detail) ? "heldOrSkipped" : "refused";
    if (e.action === "error") return /lookup failed/i.test(e.detail) ? "lookupFailed" : "failed";
    return "";
  }
  function issuesOf(events) {                           // one order's events, oldest first
    const out = [], seen = { print: new Set(), scan: new Set() };
    for (const e of events) {
      let kind = issueKind(e);
      if (!kind && (e.action === "print" || e.action === "scan" || e.action === "matched")) {
        const ak = e.action === "matched" ? "scan" : e.action, k = e.stored || e.station; const again = seen[ak].has(k); seen[ak].add(k);        // (the STORED key: a print at the Sorter app then at a sorting page is the normal path, not a reprint)
        if (again || /\bagain\b|reprint/i.test(e.detail)) kind = ak === "print" ? "reprint" : "rescan";
      }
      if (kind) out.push({ kind, label: ISSUE_LABEL[kind], at: e.at, note: e.detail });
    }
    return out;
  }

  const imgUrl = images => { const l = (Array.isArray(images) ? images : []).filter(x => x && typeof x === "object").sort((a, b) => (num(a.rank) || 99) - (num(b.rank) || 99)); for (const x of l) { const u = x.url_570xN || x.url || x.url_fullxfull || x.url_340x270 || x.url_170x135; if (typeof u === "string" && /^https:\/\/[^\s"<>]{8,600}$/.test(u)) return u; } return ""; };
  /** The listing's first stored picture (the image cache, then the listings catalog): reads only, no Etsy call. Kept 30 min. */
  function listingThumb(ctx, id) {
    return cached(ctx, `limg|${id}`, TTL.info, async () => {
      for (const c of ["Etsy_Listing_Image_Cache", "EtsyMail_Listings"]) { const snap = await ctx.db.collection(c).doc(id).get(); const u = snap.exists ? imgUrl((snap.data() || {}).images) : ""; if (u) return u; }
      return "";
    });
  }
  const plain = v => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim();      // (a SKU or a title keeps its digits: only a NAME has them dropped)
  const NO_INFO = { info: false, customer: "", thumbUrl: "", pieces: [], piecesCount: 0, text: "" };
  /** What the stored receipt says about an order: customer, pieces (one per piece) with their pictures. Production only (the mirror is not in the sandbox). */
  function orderInfo(ctx, rid) {
    if (ctx.prefix) return Promise.resolve(NO_INFO);
    return cached(ctx, `oinfo|${rid}`, TTL.info, async () => {
      const snap = await ctx.db.collection("EtsyMail_Receipts").doc(rid).get();
      if (!snap.exists) return NO_INFO;
      const d = snap.data() || {}, raw = d.raw && typeof d.raw === "object" ? d.raw : {};
      let customer = cleanName(d.buyer_name || raw.name || ""); if (!okName(customer)) customer = "";
      const txs = (Array.isArray(raw.transactions) ? raw.transactions : []).filter(t => t && typeof t === "object").slice(0, 20);
      const thumbs = new Map(); let thumbFailed = false;
      await Promise.all([...new Set(txs.map(t => digits(t.listing_id, 20)).filter(Boolean))].map(async id => { try { thumbs.set(id, await listingThumb(ctx, id)); } catch (_) { thumbs.set(id, ""); thumbFailed = true; } }));
      if (thumbFailed) ctx.cache.memo.delete(`oinfo|${rid}`);      // (an answer without its pictures is not kept: the next look reads them again, and this one says partial)
      const pieces = []; let count = 0; const text = [customer];
      for (const t of txs) {
        const q = Math.max(1, Math.min(5000, Math.floor(num(t.quantity)) || 1)), sku = plain(t.sku).slice(0, 60), title = plain(t.title).slice(0, 120), thumb = thumbs.get(digits(t.listing_id, 20)) || "";
        text.push(sku, title);
        count += q;                                       // (the true number of pieces, a 300-piece line included; only the first 12 are listed)
        for (let i = 1; i <= q && pieces.length < 12; i++) pieces.push({ id: `${digits(t.transaction_id, 20) || pieces.length + 1}_${i}`, label: sku || title, sku, thumbUrl: thumb });
      }
      return { info: true, thumbFailed, customer, thumbUrl: (pieces.find(p => p.thumbUrl) || {}).thumbUrl || "", pieces, piecesCount: count, text: text.join(" ").toLowerCase() };
    });
  }
  /** Runs fn over the items, `n` at a time. */
  async function pool(items, n, fn) { let i = 0; await Promise.all(Array.from({ length: Math.min(n, items.length) }, async () => { while (i < items.length) { const it = items[i++]; await fn(it); } })); }

  async function opOrders(ctx, body) {
    const name = cleanName(body.name);
    if (!okName(name)) return json(400, { ok: false, error: "name required" });
    const limit = Math.max(1, Math.min(CAP.orderPage, Math.floor(num(body.limit)) || 25));
    const q = String(body.q == null ? "" : body.q).replace(/[\u0000-\u001f]/g, " ").slice(0, 120).trim();
    const tokens = searchTokens(q);
    const cm = /^o(\d{1,6})$/.exec(String(body.cursor || "")), offset = cm ? +cm[1] : 0;
    const stationF = typeof body.station === "string" && Object.prototype.hasOwnProperty.call(STATION_LABEL, displayStation(body.station)) ? displayStation(body.station) : "";    // (an old filter "sorter" or "qr" is Sorting)
    let to = body.to && validDay(String(body.to)) ? String(body.to) : ctx.today; if (to > ctx.today) to = ctx.today;
    const floor = addDays(to, -(MAX_DAYS - 1));
    let from = body.from && validDay(String(body.from)) ? String(body.from) : "";
    if (!from) { const st0 = await safe(readEventsStart(ctx), "start"); from = st0.ok && st0.value ? st0.value : addDays(to, -30); }
    if (from < floor) from = floor;
    if (from > to) return json(400, { ok: false, error: "from is after to" });
    const key = nameKeyOf(ctx, name), sink = { errors: [], capped: [] };
    // the person's rollups of the window: one read per 31 days, the shared per-day cache
    const chunks = []; for (let d = from; d <= to; d = addDays(d, 31)) chunks.push([d, addDays(d, 30) > to ? to : addDays(d, 30)]);
    const rr = await Promise.all(chunks.map(c => safe(readRollups(ctx, c[0], c[1]), "rollups")));
    const rollups = []; let okReads = 0;
    for (const r of rr) { if (!r.ok) { sink.errors.push("rollups: " + r.error); continue; } okReads++; if (r.value.capped) sink.capped.push("rollups"); rollups.push(...r.value.docs); }
    if (!okReads) { const e = new Error("rollups failed"); e.unavailable = sink.errors; throw e; }
    const people = buildPeople(ctx, { rollups, sessions: [] }, from, to), P = people.get(key) || null;
    const display = P ? P.display : (canonOf(ctx, key) || niceName(name));
    // every order the person touched: rid → { days, stations }, newest first
    const uni = new Map(), matchedOnly = body.matched === true || body.matched === 1 || body.matched === "1", matchedAt = new Map();
    if (matchedOnly && P) {
      // only the orders the person scanned as Matching at the Welding station: their events of the days whose rollup says there were matched scans
      const mdays = [...P.days.values()].filter(pd => pd.hasEvents && pd.st.welding && pd.st.welding.matched > 0).map(pd => pd.day).sort();
      const evs = await Promise.all(mdays.map(day => readEvents(ctx, people, key, day, day, sink)));
      evs.forEach((list, i) => { for (const e of list) if (e.orderId && KIND.isMatched(e)) { let u = uni.get(e.orderId); if (!u) uni.set(e.orderId, u = { rid: e.orderId, days: new Set(), stations: new Set(["welding"]), latest: "" }); u.days.add(mdays[i]); if (mdays[i] > u.latest) u.latest = mdays[i]; matchedAt.set(e.orderId, Math.max(matchedAt.get(e.orderId) || 0, e.at)); } });
    }
    else if (P) for (const pd of P.days.values()) {
      if (!pd.hasEvents) continue;
      for (const [rid, set] of pd.orders) { if (stationF !== "inbox" && ![...set].some(x => x !== "inbox")) continue; let e = uni.get(rid); if (!e) uni.set(rid, e = { rid, days: new Set(), stations: new Set(), latest: "" }); e.days.add(pd.day); set.forEach(x => e.stations.add(x)); if (pd.day > e.latest) e.latest = pd.day; }
    }
    let list = [...uni.values()]; const scanned = list.length;
    if (stationF) list = list.filter(e => e.stations.has(stationF));
    const byNewest = (a, b) => (a.latest < b.latest ? 1 : a.latest > b.latest ? -1 : a.rid < b.rid ? 1 : a.rid > b.rid ? -1 : 0);
    list.sort(byNewest);
    // search: order number, station and date are matched on every order; customer and SKU text on the newest INFO orders (read once, kept 30 min)
    const needInfo = tokens.some(x => !x.date && /\p{L}/u.test(x.t) && x.t.length >= 3 && !isWordPrefix(x.t));
    let withDetails = 0, infos = new Map();
    if (needInfo && !ctx.prefix) {
      const want = list.slice(0, CAP.infoSearch);
      await pool(want, 12, async e => { try { const i = await orderInfo(ctx, e.rid); infos.set(e.rid, i); if (i.thumbFailed) sink.errors.push("pictures: unreadable"); } catch (err) { sink.errors.push("receipts: " + String((err && err.message) || err).slice(0, 80)); } });
      withDetails = [...infos.values()].filter(i => i.info).length;
    }
    let matches = list;
    if (tokens.length) {
      matches = list.filter(e => {
        const info = infos.get(e.rid), dates = ` ${[...e.days].map(dateWords).join(" ")} `, hay = `${e.rid} ${[...e.stations].map(s => s + " " + (STATION_LABEL[s] || s).toLowerCase() + (s === "sorting" ? " sorter qr printer labels" : "")).join(" ")} ${dates} ${info ? info.text : ""}`.toLowerCase();
        return tokens.every(x => (x.date ? dates.includes(" " + x.t + " ") : hay.includes(x.t)));
      });
    }
    const total = matches.length;
    // within a day the order is the time of the last action; read the events of the days the page touches, then cut the page
    const dayEv = new Map();                                      // day → Map(rid → [events])
    const evOfDay = async day => {
      if (dayEv.has(day)) return dayEv.get(day);
      const load = async () => { const ev = await readEvents(ctx, people, key, day, day, sink), m = new Map(); for (const e of ev) if (e.orderId) { let a = m.get(e.orderId); if (!a) m.set(e.orderId, a = []); a.push(e); } return m; };
      let m = await load();
      if (day === ctx.today) {                                      // a new order the rollup already knows but today's cached events do not: read them afresh (at most every 5 s)
        const Pd = P && P.days.get(day);
        if (Pd && edge.some(e => e.latest === day && !m.has(e.rid))) {
          let dropped = false;
          for (const f of Pd.forms) { const k = `pev|${ctx.prefix}|${f}|${day}`, hit = ctx.cache.memo.get(k); if (hit && ctx.now - hit.at > 5000) { ctx.cache.memo.delete(k); dropped = true; } }
          if (dropped) m = await load();
        }
      }
      dayEv.set(day, m); return m;
    };
    const edge = matches.slice(offset, offset + limit + 1), pageDays = [...new Set(edge.map(e => e.latest))];
    await Promise.all(pageDays.map(evOfDay));
    const lastOn = e => { const a = dayEv.has(e.latest) && dayEv.get(e.latest).get(e.rid); return a ? a[a.length - 1].at : e.latest === ctx.today ? 9e15 : 0; };   // (no events yet for an order of today: it just arrived, so it goes on top)
    if (pageDays.length) matches = matches.slice().sort((a, b) => (a.latest < b.latest ? 1 : a.latest > b.latest ? -1 : (pageDays.includes(a.latest) ? lastOn(b) - lastOn(a) : 0) || (a.rid < b.rid ? 1 : a.rid > b.rid ? -1 : 0)));
    const page = matches.slice(offset, offset + limit), rows = [];
    await pool(page, 8, async e => {
      const evs = []; for (const d of [...e.days].sort()) { const m = await evOfDay(d); const a = m.get(e.rid); if (a) evs.push(...a); }
      evs.sort((a, b) => a.at - b.at || a.k - b.k || (a.id < b.id ? -1 : 1));
      const { steps } = stepsOf(evs), list2 = [...steps.values()].sort((a, b) => a.firstAt - b.firstAt);
      const onLatest = list2.filter(s => s.day === e.latest), lastStep = list2.slice().sort((a, b) => b.lastAt - a.lastAt)[0];
      let info = NO_INFO; try { info = await orderInfo(ctx, e.rid); if (info.thumbFailed) sink.errors.push("pictures: unreadable"); } catch (_) { sink.errors.push("receipts: unreadable"); }
      const sum = k => list2.reduce((n, s) => n + s[k], 0), firstAt = onLatest.length ? Math.min(...onLatest.map(s => s.firstAt)) : null, lastAt = onLatest.length ? Math.max(...onLatest.map(s => s.lastAt)) : null;
      rows.push({ rid: e.rid, number: e.rid, at: firstAt, day: e.latest, station: lastStep ? lastStep.station : [...e.stations][0] || "", stations: [...e.stations], durationMs: list2.length ? sum("workMs") : null, spanMs: firstAt != null ? lastAt - firstAt : null,
        scans: sum("scans"), completes: sum("completes"), prints: sum("prints"), parts: sum("parts"), undone: sum("undos"), rejected: sum("rejects"), errors: sum("errors"),
        steps: list2.map(s => ({ station: s.station, firstAt: s.firstAt, lastAt: s.lastAt, durationMs: s.workMs, scans: s.scans, completes: s.completes, prints: s.prints, parts: s.parts })),
        issues: issuesOf(evs), customer: info.customer, info: info.info, thumbUrl: info.thumbUrl, qr: { text: e.rid }, pieces: info.pieces, piecesCount: info.piecesCount });
      if (matchedOnly) { const row = rows[rows.length - 1]; row.matchedAt = matchedAt.get(e.rid) || row.at; row.matched = evs.filter(x => KIND.isMatched(x)).length; }
    });
    const order = new Map(page.map((e, i) => [e.rid, i])); rows.sort((a, b) => order.get(a.rid) - order.get(b.rid));
    const notes = [];
    if (needInfo && withDetails < Math.min(list.length, CAP.infoSearch) && !ctx.prefix) notes.push(`Customer and SKU text was found for ${withDetails} of the newest ${Math.min(list.length, CAP.infoSearch)} orders.`);
    if (needInfo && list.length > CAP.infoSearch) notes.push(`Customer name and SKU search covers the newest ${CAP.infoSearch} orders; order number, station and date cover all ${list.length}.`);
    if (ctx.prefix) notes.push("The Sandbox keeps no receipts: customer, pictures and pieces are not available here.");
    if (!P) notes.push(`There is no record of ${display} in this window.`);
    if (sink.capped.length) notes.push("Some lists were cut at their size limit: " + [...new Set(sink.capped)].join(", ") + ".");
    const out = { ok: true, now: ctx.now, mode: ctx.prefix ? "sandbox" : "real", name: display, found: !!P, from, to, total, scanned, searched: { orders: list.length, withDetails }, orders: rows, next: offset + limit < total ? "o" + (offset + limit) : null, notes };
    if (sink.errors.length || sink.capped.length) { out.partial = true; if (sink.errors.length) out.errors = [...new Set(sink.errors)].slice(0, 8); }
    return json(200, out);
  }

  return { opProfile, opOrders, resolveRange, prepare, loadRaw, buildPeople, readPersonDayEvents, daysOf, epochDay, dowOf, mondayOf, spanLen, fin, RULES, RANGE_DAYS, STATION_LABEL, CAP, TTL, EVENT_DAYS, DAILY_MAX, MAX_DAYS };
}

module.exports = make;
module.exports.RULES = RULES;
module.exports.CATALOG = CATALOG;
module.exports.EVENT_DAYS = EVENT_DAYS;
module.exports.STATION_LABEL = STATION_LABEL;
