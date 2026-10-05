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
const MAX_DAYS = 731, EVENT_DAYS = 31, DAILY_MAX = 92;
const TTL = { resp: 30000, respLive: 2000, ev: 600000, evLive: 60000, info: 1800000, sessLive: 5000, sessPast: 600000 };
const CAP = { eventsPerDay: 2500, items: 200, orderPage: 100, infoSearch: 250 };
const RULES = { teamMinPeople: 2, minSignedMin: 15, shortFraction: 0.5, lateAfterMin: 30, activeGapMin: 5 };
const STATION_LABEL = { sorting: "Sorting", welding: "Welding", assembly: "Assembly", shipping: "Shipping", design: "Design", laser: "Laser", sorter: "Sorter", qr: "QR labels", inbox: "Inbox" };

/* (literal requires inside try blocks: the bundler follows them when the file exists and skips them when it does not yet)  */
const notFound = e => e && e.code === "MODULE_NOT_FOUND";
const failNote = e => (notFound(e) ? "" : "could not load: " + String((e && e.message) || e).slice(0, 120));
const ATT = { mod: null, err: "" }, ISS = { mod: null, err: "" };
try { ATT.mod = require("./_employeeAttendance"); } catch (e) { ATT.err = failNote(e); }
try { ISS.mod = require("./_employeeIssues"); } catch (e) { ISS.err = failNote(e); }

function make(K) {
  const { COL, LIM, ms, num, r1, zeros, digits, cleanName, okName, niceName, bestForm, nameKeyOf, canonOf, scrub, validDay, addDays,
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
    const n = RANGE_DAYS[r];
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

  const newPD = day => ({ day, hasEvents: false, events: 0, st: {}, hours: zeros(24), hourScans: zeros(24), orders: new Map(), inFirst: 0, inLast: 0, spans: [], signedMs: 0, rawMs: 0, stMs: {}, firstIn: 0, lastOut: 0, liveSpan: false, forms: new Set() });

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
      const P = get(x.person); if (!P || !validDay(x.day) || x.day < from || x.day > to) continue;
      const pd = pdOf(P, x.day); pd.hasEvents = true; pd.events += Math.max(0, num(x.events));
      const form = String(x.person); pd.forms.add(form); P.forms.add(form);              // (the exact stored name: what a Station_Activity query must match)
      for (const [st, v] of Object.entries(x.stations && typeof x.stations === "object" ? x.stations : {})) {
        if (!/^[a-z][\w-]{0,19}$/.test(st) || !v || typeof v !== "object") continue;
        const a = pd.st[st] || (pd.st[st] = tmpl()); for (const k of KEYS) a[k] += Math.max(0, num(v[k]));
        const fa = ms(v.firstAt), la = ms(v.lastAt); if (fa > 0 && (!pd.inFirst || fa < pd.inFirst)) pd.inFirst = fa; if (la > pd.inLast) pd.inLast = la;
      }
      for (const [hh, h] of Object.entries(x.hours && typeof x.hours === "object" ? x.hours : {})) {
        const hr = parseInt(hh, 10); if (!(hr >= 0 && hr < 24) || !h || typeof h !== "object") continue;
        pd.hours[hr] += Math.max(0, num(h.parts)) - Math.max(0, num(h.undoParts)); pd.hourScans[hr] += Math.max(0, num(h.scans));   // produced minus undone, the same net as the totals
      }
      for (const [id, m] of Object.entries(x.touched && typeof x.touched === "object" ? x.touched : {})) {
        const oid = digits(id); if (!oid) continue;
        let set = pd.orders.get(oid); if (!set) pd.orders.set(oid, set = new Set());
        for (const k of m && typeof m === "object" ? Object.keys(m).filter(z => m[z]) : []) set.add(k);
      }
      const fa = ms(x.firstAt), la = ms(x.lastAt); if (fa > 0 && (!pd.inFirst || fa < pd.inFirst)) pd.inFirst = fa; if (la > pd.inLast) pd.inLast = la;
    }
    for (const s of raw.sessions) {
      const P = get(s.person), sp = spanOf(s, ctx.now); if (!P || !sp) continue;
      for (const c of clip(sp.start, sp.end, from, to)) {
        const pd = pdOf(P, c.day);
        pd.spans.push({ s: c.s, e: c.e, station: sp.station, live: sp.live, start0: sp.start, endReason: String(s.endReason || ""), device: String(s.device || "") });
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
      const rows = []; for (const d of snap.docs.slice(0, CAP.eventsPerDay)) { const e = eventRow(d.id, d.data() || {}, ctx); if (e) rows.push(e); }
      return { rows, capped: snap.docs.length > CAP.eventsPerDay };
    });
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
      daysOf, dowOf, mondayOf, spanLen, nyMidnight, addDays, rules: RULES, stationLabel: STATION_LABEL, eventDays: EVENT_DAYS,
      /** The single events of ONE person (default: the one asked for; `key` = another person's name key) for the days fromDay..toDay,
          oldest first, spellings merged: [{id, at, k, day, person, station, device, action, orderId, parts, detail, sincePrevMs}].
          Only days that have a rollup are queried. A read that fails or is cut at its cap is noted in prof.errors / prof.capped. */
      async events(fromDay, toDay, opts) {
        const P = opts && opts.key ? people.get(opts.key) : me; if (!P) return [];
        const jobs = [];
        for (const day of daysOf(fromDay < prof.loadFrom ? prof.loadFrom : fromDay, toDay)) { const pd = P.days.get(day); if (pd && pd.hasEvents) for (const f of pd.forms) jobs.push(safe(readPersonDayEvents(ctx, f, day), "events " + day)); }
        const rows = [];
        for (const r of await Promise.all(jobs)) {
          if (!r.ok) { prof.errors.push("events: " + r.error); continue; }
          if (r.value.capped && !prof.capped.includes("events")) prof.capped.push("events");
          rows.push(...r.value.rows);
        }
        return rows.sort((a, b) => a.at - b.at || a.k - b.k || (a.id < b.id ? -1 : 1));
      }
    };
    ctx.prof = prof;
    return prof;
  }

  /* ── the helpers of other workers: feature-detected, never fatal (a missing file leaves its fields out; a broken one is named in `errors`) ── */
  const helper = (mod, err, fname) => { const f = mod && (typeof mod[fname] === "function" ? mod[fname] : typeof mod === "function" ? mod : null); return { f, err }; };
  const HELPERS = { attendance: helper(ATT.mod, ATT.err, "attendance"), issues: helper(ISS.mod, ISS.err, "issues") };
  async function callHelper(ctx, name, args) {
    const h = HELPERS[name];
    if (!h.f) { if (h.err) ctx.prof.errors.push(name + ": " + h.err); return null; }
    try { return await h.f(ctx, args); } catch (e) { ctx.prof.errors.push(name + ": " + String((e && e.message) || e).slice(0, 160)); return null; }
  }

  /** op person WITH a range: the profile. */
  async function opProfile(ctx, body) {
    const name = cleanName(body.name);
    if (!okName(name)) return json(400, { ok: false, error: "name required" });
    const rg = resolveRange(ctx, body); if (rg.error) return json(400, { ok: false, error: rg.error });
    const compare = body.compare !== false && body.compare !== 0 && body.compare !== "0";
    const prev = compare ? { from: addDays(rg.from, -rg.days), to: addDays(rg.from, -1), days: rg.days } : null;
    const ckey = `prof|${ctx.prefix}|${nameKeyOf(ctx, name)}|${rg.from}|${rg.to}|${compare ? 1 : 0}`;
    const out = await cached(ctx, ckey, rg.to === ctx.today ? TTL.respLive : TTL.resp, () => buildProfile(ctx, name, rg, prev));
    return json(200, out);
  }

  async function buildProfile(ctx, name, rg, prev) {
    const prof = await prepare(ctx, name, rg.from, rg.to, prev ? prev.from : rg.from);
    prof.prev = prev;
    const [att, iss] = await Promise.all([callHelper(ctx, "attendance", { name: prof.name, from: rg.from, to: rg.to, prev }), callHelper(ctx, "issues", { name: prof.name, from: rg.from, to: rg.to, prev })]);
    const out = { ok: true, now: ctx.now, mode: prof.mode, name: prof.name, found: prof.found, spellings: prof.spellings, range: rg.range, from: rg.from, to: rg.to, days: rg.days, today: ctx.today, live: rg.to === ctx.today, prev,
      granularity: rg.days > DAILY_MAX ? "week" : "day", rules: RULES, kpis: {}, series: [], hours: [], stations: [], calendar: [], cannotTell: [], notes: [] };
    if (att) { out.attendance = att; if (Array.isArray(att.calendar)) out.calendar = att.calendar; }
    if (iss) { if (iss.issues) out.issues = iss.issues; if (iss.rates) out.rates = iss.rates; if (iss.contact) out.contact = iss.contact; }
    if (prof.errors.length) { out.partial = true; out.errors = prof.errors.slice(0, 12); }
    return out;
  }

  /** op personOrders (skeleton: the shape of the answer, no orders yet; the search lands in the next commit). */
  async function opOrders(ctx, body) {
    const name = cleanName(body.name);
    if (!okName(name)) return json(400, { ok: false, error: "name required" });
    return json(200, { ok: true, now: ctx.now, mode: ctx.prefix ? "sandbox" : "real", name: niceName(name), found: false, q: String(body.q || "").slice(0, 120), from: "", to: ctx.today, total: 0, scanned: 0,
      searched: { orders: 0, withDetails: 0 }, orders: [], next: null, notes: ["not built yet"] });
  }

  return { opProfile, opOrders, resolveRange, prepare, loadRaw, buildPeople, readPersonDayEvents, daysOf, epochDay, dowOf, mondayOf, spanLen, fin, RULES, RANGE_DAYS, STATION_LABEL, CAP, TTL, EVENT_DAYS, DAILY_MAX, MAX_DAYS };
}

module.exports = make;
module.exports.RULES = RULES;
module.exports.STATION_LABEL = STATION_LABEL;
