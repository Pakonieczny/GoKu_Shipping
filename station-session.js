/*  station-session.js — how long each person is signed in, on which computer, at which station or page, and the
 *  midnight sign-out: at midnight (America/New_York) everybody is signed out, so every morning everybody signs in again
 *  (Paul, 28 Sep 23:53).
 *
 *    StationSession.init({ station, device, person: () => ({ name, id }) | null, signOut: reason => void,
 *                          labelHost?, labelCss?, sandbox? })
 *        station: a key of STATIONS in _orderTimeline.js (sorting, welding, assembly, shipping, design, laser, sorter,
 *        qr, inbox); device: the page ("weld-1", "assembly-2", …). person(): who is signed in on this page now (null:
 *        nobody; { name, id, pending: true }: named but not signed in yet, e.g. the Sorter's "Laser or Design?" is not answered: no
 *        session, but the name gets its sign-in day, so the day turning signs it out). signOut(reason): the page clears its own login keys and shows its own sign-in screen or PIN box,
 *        keeping the work on screen. labelHost (optional, element or selector): where the tiny "Computer: …" line and
 *        its one-time name field go; labelCss: extra inline CSS for that line.
 *    StationSession.signedIn({ name, id })   // call from the page's login success
 *    StationSession.signedOut(reason)        // call from the page's own sign-out ("signOut" unless said otherwise)
 *
 *  A ROLE (the Sorter app, Paul 6 Oct: a non-Admin person is asked "Laser or Design?" once per sign-in): init({ role: () => "laser" |
 *  "design" | "" }). While it answers laser or design, the session's station IS that role (written under station `laser` or
 *  `design`, device unchanged, plus a `role` field) and every event of station-activity.js carries it; "" is the page's own
 *  station (the Admin, and every page without a role). StationSession.roleChanged() (or signedIn again) ends the session under
 *  the old role ("switched") and starts the one under the new, so both are tracked on their own. StationSession.role() says it.
 *
 *  Pages with more than one person at once (the Welding station: one welds, one matches, same page) pass multi: true and
 *  people: () => [{ name, task }] (who is signed in, under which task) instead of person(). Then, and only then:
 *    signedIn({ name, id, task })            // adds ONE person (one session per person and task); it never ends another
 *    signedOut(reason, { name, task })       // ends that one session (no task: every task of that name; no who: everybody)
 *    signOut(reason, { name, task })         // the page's own callback, once per person when the day turns
 *    StationSession.people()                 // [{ name, task, session, since, startAt, lastInputAt, device, station }]
 *    StationSession.who(task?)               // the person an action is credited to (see below), or null
 *    StationSession.touch(ts?, who?)         // an input happened at this page (the scanner relay calls it too)
 *    StationSession.lastInput()              // ms of the latest input at the page (0: none yet)
 *  The same person may be in two tasks (two sessions). Single-person pages (no multi) behave exactly as before: a second
 *  sign-in there ends the first ("switched"). Doc: plans/stations-round2/api.md (C2).
 *
 *  A session runs from sign-in to sign-out for one person on one computer at one page. It is written through the open
 *  station door (firebaseOrders {session}) to Station_Sessions: start, a beat every 5 minutes and once on pagehide,
 *  and an end (signOut · midnight · switched · closed). The server stamps its own times and the minutes.
 *  Never a PIN: an id that looks like one (only digits) is dropped here and again on the server. The computer is
 *  localStorage.station_computer_id (random, per browser profile); localStorage.station_signin_day is the New York date
 *  of the latest sign-in. A page loaded after the day turned signs out ("midnight") before anything else.
 *  Nothing here blocks or throws into the page: every write is fire-and-forget and every entry point is wrapped.
 *
 *  AUTO SIGN-OUT (Paul, 6 Oct 2026; everybody except an Admin):
 *    Rule A, idle: 10 minutes without any input at this page signs the person out through the page's own signOut("idle").
 *    Rule B, closing: at 17:00 America/Toronto everybody who has had no input in the last 10 minutes is signed out
 *      (signOut("closing")); anybody with input inside those 10 minutes stays and Rule A takes over from that input.
 *    Input = the time of the last pointer, touch, key, wheel, click, input or paste event at this page, or StationSession.touch()
 *      (the station's scanner relay). It is ONE number in memory, stamped at most once a second: never what was typed or
 *      pressed, never a log. ONLY events a person made (event.isTrusted true) are input: events a script dispatches, or a remote cursor draws, are not. A reload is input. Input in a frame
 *      inside the page, or in the page that holds this frame, counts too (only the time is passed on, by postMessage). On a
 *      multi page input at the page counts for every person signed in there.
 *    The session's end time is the LAST INPUT, not the moment the sign-out was noticed, so hours stay honest. Heartbeats and the
 *      end carry lastInputAt. What keeps the clock across a reload is the last input time inside the session's own record
 *      (localStorage station_session.<station>.<device>), kept to the latest few seconds.
 *    Admin: the name is asked of the server once per sign-in (POST firebaseOrders { stationAdmin: name }, answer { ok: true, admin })
 *      and kept for that sign-in, in memory (never stored beyond the sign-in's own record). Unknown, offline, 503 or any other
 *      answer is NOT Admin (fail closed). An Admin gets neither rule; the midnight (New York) sign-out stays for everybody.
 *    Every start, beat and end also carries sentAt (this page's clock when sent) so the server can undo a wrong computer clock. A beat
 *      the server answers `ended: true` with idle, closing or closed (it ended the session on what it knew, e.g. beats that did not arrive)
 *      is read against this page's own rules (serverEnded): when they agree the person is signed out the same way; when they do not (the
 *      person has been working) that session is closed on the record and a new one carries on, so nobody working is thrown out.
 *    Two tabs of one computer share one sign-in record and its last input: input in either keeps the person in. A computer clock set
 *      back puts the last input "in the future": it is taken as now, once, so the rule is never held still for hours.
 *    Timing uses Date.now() against stored times on a 10 second tick, on every input that follows a gap, and on
 *      visibilitychange / focus / pageshow / resume (a sleeping computer or a background tab signs the person out at the
 *      right moment and with the right end time); never one long setTimeout.
 *    StationSession.isAdmin(name)          Promise: true | false | null (null: not known, treated as not Admin)
 *    StationSession.notice(reason, tail?)  the one-line wording for the page's own notice ("" for any other reason)
 *    The page's signOut(reason, who) also receives "idle" and "closing" (the work on screen stays, the page shows its own
 *    sign-in, and its notice uses StationSession.notice(reason) for these two). */
(function () {
  "use strict";
  if (window.StationSession) return;
  const TZ = "America/New_York", TZ_CLOSING = "America/Toronto";
  const BEAT_MS = 5 * 60000, CLOSED_MS = 15 * 60000, TICK_MS = 30000;
  const IDLE_MS = 10 * 60000, RULES_TICK_MS = 10000, CLOSING_HOUR = 17;
  const REASONS = new Set(["signOut", "midnight", "switched", "closed", "idle", "closing"]);
  const NOTICES = { idle: "Signed out after 10 minutes without input.", closing: "Signed out at 5:00 pm." };
  const LABEL = { sorting: "Sorting", welding: "Welding", assembly: "Assembly", shipping: "Shipping", design: "Design",
    laser: "Laser", sorter: "Sorter", qr: "QR Printer", inbox: "Inbox" };
  const K = { computer: "station_computer_id", name: "station_computer_name", day: "station_signin_day",
    days: "station_signin_days", unsent: "station_session_unsent", input: "station_person_input" };
  const ROLES = new Set(["laser", "design"]);
  const cfg = { station: "", device: "", person: null, role: null, signOut: null, labelHost: null, labelCss: "", sandbox: false,
    multi: false, people: null, creditTask: "matching" };
  let ready = false, cur = null, seen = "", quiet = "", tickT = 0, midT = 0, memId = "", labelBox = null;
  const many = new Map(), manyQuiet = new Set();        // multi pages: running sessions by person and task; people the page has not dropped yet
  const unseen = new Map();                            // multi pages: people listed with no session and no record yet, by when this page first saw them (monotonic ms)
  const monoNow = () => { try { if (typeof performance !== "undefined" && performance && typeof performance.now === "function") return performance.now(); } catch (_) {} return Date.now(); };
  let snap = new Map();                                // multi pages: the sessions kept in storage, while a reload or a second tab picks them up
  let lastIn = 0, stampAt = 0, rulesT = 0, savedLi = 0;   // the latest input at this page (ms), in memory

  const warn = (...a) => { try { console.warn("[StationSession]", ...a); } catch (_) {} };
  const lsGet = k => { try { return localStorage.getItem(k) || ""; } catch (_) { return ""; } };
  const lsSet = (k, v) => { try { localStorage.setItem(k, v); } catch (_) {} };
  const lsDel = k => { try { localStorage.removeItem(k); } catch (_) {} };
  const lsJson = (k, d) => { try { const v = JSON.parse(localStorage.getItem(k) || "null"); return v == null ? d : v; } catch (_) { return d; } };
  const clean = (v, n) => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, n);
  const safeId = v => { const s = clean(v, 60); return !s || /^\d+$/.test(s) ? "" : s; };   // a PIN (digits) is never kept or sent
  /** a person's name as it is sent and kept: digit runs of four or more are dropped ("Marco 123456" typed by mistake is "Marco": a number
      could be a PIN) and a name with no letter is nobody. Names the pages already tidy (the Design Stations, the sorter) come out unchanged. */
  const cleanName = v => { const s = clean(String(v == null ? "" : v).replace(/\d{4,}/g, " "), 80); return /\p{L}/u.test(s) ? s : ""; };

  /* ── time zones: New York for the day and its midnight, Toronto for the 5 pm closing (one helper, the zone is a parameter) ── */
  const fmts = {};
  function fmtOf(tz) {
    if (!(tz in fmts)) {
      try { fmts[tz] = new Intl.DateTimeFormat("en-US", { timeZone: tz, hourCycle: "h23", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" }); }
      catch (_) { fmts[tz] = null; }
    }
    return fmts[tz];
  }
  function zoneParts(t, tz) {
    const d = new Date(t), fmt = fmtOf(tz);
    if (fmt) {
      try {
        const o = {}; for (const p of fmt.formatToParts(d)) o[p.type] = p.value;
        return { y: +o.year, m: +o.month, d: +o.day, h: +o.hour % 24, mi: +o.minute, s: +o.second };
      } catch (_) {}
    }
    const e = new Date(t - 5 * 3600e3);                 // no time zone data: Eastern Standard Time
    return { y: e.getUTCFullYear(), m: e.getUTCMonth() + 1, d: e.getUTCDate(), h: e.getUTCHours(), mi: e.getUTCMinutes(), s: e.getUTCSeconds() };
  }
  const nyParts = t => zoneParts(t, TZ);
  const pad = n => String(n).padStart(2, "0");
  function nyDay(t) { const p = nyParts(t == null ? Date.now() : t); return `${p.y}-${pad(p.m)}-${pad(p.d)}`; }
  function zoneOffset(t, tz) { const p = zoneParts(t, tz); return Date.UTC(p.y, p.m - 1, p.d, p.h, p.mi, p.s) - Math.floor(t / 1000) * 1000; }
  /** the first midnight after t (ms) in a zone */
  function midnightAfter(t, tz) {
    const p = zoneParts(t, tz), wall = Date.UTC(p.y, p.m - 1, p.d + 1);
    let u = wall - zoneOffset(t, tz); u = wall - zoneOffset(u, tz);
    return u > t ? u : t + 86400e3;
  }
  /** the first New York midnight after t (ms) */
  const nextMidnight = t => midnightAfter(t, TZ);
  /** the instant (ms) of a wall-clock time in a zone; `near` is any instant of that day (it picks the right UTC offset, also on a
      day the clocks change: the offset is found twice, the second time at the answer of the first) */
  function wallAt(tz, y, m, d, h, mi, near) {
    const wall = Date.UTC(y, m - 1, d, h, mi);          // (a day of 0 or a month past 12 rolls over, as Date.UTC does)
    let u = wall - zoneOffset(near, tz); u = wall - zoneOffset(u, tz);
    return u;
  }
  /** the latest 17:00 in Toronto at or before t (ms) */
  function closingAt(t) {
    const p = zoneParts(t, TZ_CLOSING);
    let u = wallAt(TZ_CLOSING, p.y, p.m, p.d, CLOSING_HOUR, 0, t);
    if (u > t) u = wallAt(TZ_CLOSING, p.y, p.m, p.d - 1, CLOSING_HOUR, 0, t - 86400e3);
    return u;
  }

  /* ── the computer ── */
  const ALPHA = "23456789ABCDEFGHJKMNPQRSTUVWXYZ";
  function rand(n) {
    let s = "";
    try { const a = new Uint32Array(n); crypto.getRandomValues(a); for (const x of a) s += ALPHA[x % ALPHA.length]; }
    catch (_) { for (let i = 0; i < n; i++) s += ALPHA[Math.floor(Math.random() * ALPHA.length)]; }
    return s;
  }
  function computerId() {
    let id = lsGet(K.computer);
    if (!/^[\w-]{6,64}$/.test(id)) { id = memId || "pc-" + rand(12); lsSet(K.computer, id); }
    return (memId = id);
  }
  const shortId = () => computerId().replace(/^pc-/, "").slice(0, 4).toUpperCase();
  function label() {
    const base = `${LABEL[cfg.station] || cfg.station} ${cfg.device} · ${shortId()}`.trim();
    const nm = clean(lsGet(K.name), 30);
    return (nm ? `${nm} (${base})` : base).slice(0, 80);
  }

  /* ── who is signed in, and since which New York day ── */
  function person() {
    try {
      const p = cfg.person ? cfg.person() : null;
      const name = p && cleanName(p.name);
      return name ? (p.pending ? { name, id: safeId(p.id), pending: true } : { name, id: safeId(p.id) }) : null;
    } catch (_) { return null; }
  }
  /** the role the page says is in force ("laser" | "design"), else "" (the page's own station) */
  function roleNow() {
    try { const r = cfg.role ? String(cfg.role() || "") : ""; return ROLES.has(r) ? r : ""; } catch (_) { return ""; }
  }
  /** the station a session started now is written under: the role when there is one, else the page's station */
  const stationNow = () => roleNow() || cfg.station;
  /** the station a running session was started under (sessions kept from before roles have none: the page's) */
  const stationOf = s => (s && s.station) || cfg.station;
  function markDay(name, day) {
    lsSet(K.day, day);
    const m = lsJson(K.days, {}); const o = m && typeof m === "object" ? m : {};
    delete o[name]; o[name] = day;
    const keys = Object.keys(o); for (const k of keys.slice(0, Math.max(0, keys.length - 20))) delete o[k];
    lsSet(K.days, JSON.stringify(o));
  }
  function dayOf(name) { const m = lsJson(K.days, {}); return (m && m[name]) || lsGet(K.day); }
  // (a name's old day is kept, not deleted: another app on this computer that signs in first this morning must not make a
  // login left over from yesterday in this one look like today's; markDay keeps the last 20 names)
  function clearStaleDays(today) {
    if (lsGet(K.day) && lsGet(K.day) !== today) lsDel(K.day);
  }

  /* ── this page's session (kept so a reload within 15 minutes goes on with it) ── */
  const recKey = () => "station_session." + cfg.station + "." + cfg.device;
  function loadRec() { const r = lsJson(recKey(), null); return r && typeof r === "object" && typeof r.id === "string" ? r : null; }
  function saveRec(r) { try { lsSet(recKey(), JSON.stringify(r)); } catch (_) {} }
  // a multi page keeps all of its running sessions in one list, so a reload or a second tab goes on with each of them
  const recsKey = () => "station_session." + cfg.station + "." + cfg.device + ".multi";
  function loadRecs() { const l = lsJson(recsKey(), []); return Array.isArray(l) ? l.filter(r => r && typeof r === "object" && typeof r.id === "string" && typeof r.key === "string" && !r.ended) : []; }
  function saveRecs() {
    try {
      const l = [...many.values(), ...[...snap.values()].filter(r => !many.has(r.key) && !r.ended)];     // (the ones not picked up yet are not lost)
      if (l.length) lsSet(recsKey(), JSON.stringify(l)); else lsDel(recsKey());
    } catch (_) {}
  }
  const persist = s => { if (s && s.key !== undefined) saveRecs(); else saveRec(s); };

  /* ── the door ── */
  const URL_ = () => "/.netlify/functions/firebaseOrders" + (cfg.sandbox ? "?sandbox=1" : "");
  function body(s, event, reason, at) {
    const o = { id: s.id, event, person: s.name, employeeId: s.eid || "", station: stationOf(s), device: cfg.device,
      computerId: computerId(), computerLabel: label(), at: at || Date.now(), reason: reason || undefined, task: s.task || undefined, role: s.role || undefined };
    const li = inputOf(s); if (li) o.lastInputAt = li;          // the time of the last input (ms), never what it was
    o.sentAt = Date.now();                                      // this page's clock at the moment of sending: the server undoes a wrong computer clock with it
    return o;
  }
  function post(session) {
    try {
      const text = JSON.stringify({ session });
      if (typeof fetch !== "function") { if (navigator.sendBeacon) navigator.sendBeacon(URL_(), new Blob([text], { type: "application/json" })); return Promise.resolve(null); }
      return fetch(URL_(), { method: "POST", headers: { "Content-Type": "application/json" }, body: text, keepalive: true })
        .then(r => {
          const o = { status: r.status };
          if (!(r.status >= 200 && r.status < 300) || typeof r.json !== "function") return o;
          return r.json().then(j => { o.j = j && typeof j === "object" ? j : null; return o; }, () => o);        // (the answer of a beat says whether the server already ended the session)
        }, () => null).catch(() => null);
    } catch (_) { return Promise.resolve(null); }
  }
  // an end that could not be sent (offline) is sent again later; the server keeps it within the times it stamped
  function sendEnd(b) {
    post(b).then(r => { if (!r || r.status >= 500 || r.status === 429) keep(b); });
  }
  function keep(b) { const q = lsJson(K.unsent, []); const list = Array.isArray(q) ? q : []; list.push(b); lsSet(K.unsent, JSON.stringify(list.slice(-20))); }
  let flushing = false;
  function flush() {
    const q = lsJson(K.unsent, []);
    if (flushing || !Array.isArray(q) || !q.length || navigator.onLine === false) return;
    flushing = true; lsDel(K.unsent);
    Promise.all(q.map(b => post(Object.assign({}, b, { sentAt: Date.now() })).then(r => { if ((!r || r.status >= 500 || r.status === 429) && Date.now() - (b.at || 0) < 7 * 86400e3) keep(b); })))
      .then(() => { flushing = false; }, () => { flushing = false; });
  }

  /* ── input: the time of the last user input at this page (memory only; never what was typed or pressed) ── */
  const INPUT_EVENTS = ["pointerdown", "pointermove", "mousedown", "mousemove", "touchstart", "touchmove", "keydown", "wheel", "click", "input", "paste"];
  const MSG = "station-session";
  const inFrame = (() => { try { return window.self !== window.top; } catch (_) { return true; } })();
  /** a session that runs at this page now (a single page's own, or one of a multi page's) */
  const runsHere = s => !!s && (s === cur || (s.key !== undefined && many.get(s.key) === s));
  /** the last input that counts for a session: this page's own (it counts for everybody signed in here) when the session runs here,
      the stored one for a record left by an earlier load */
  function inputOf(s) {
    if (!s) return 0;
    const now = Date.now(), li = Math.max(runsHere(s) ? Math.max(lastIn, Number(s.li) || 0) : Number(s.li) || 0, personInput(s.name)), st = Number(s.startAt) || 0;
    return li > 0 ? Math.min(now, Math.max(li, st <= now + 5000 ? st : 0)) : 0;        // (a start in the future is a clock set back: not an input)
  }
  /* ONE person's last input at ANY station page of this browser. The sign-in keys (employee_id / employee_name) are shared by every
     station page of a computer (assembly-1..4, shipping-1..3, the Design pages), so a person signed in there has a session at EVERY such
     page that is open, also at one nobody touches. Each page's own last input alone would then sign the person out of the page being
     worked in as soon as a quiet tab's 10 minutes were up (the quiet tab clears the shared keys: the worked page keeps looking signed in
     and records nothing). Input anywhere on the computer is the person's input. A time only, never what was typed (AS2, 6 Oct 2026). */
  const personKey = n => String(n || "").toLowerCase();
  function personInput(name) {
    try {
      const m = lsJson(K.input, {}), t = Number(m && typeof m === "object" && !Array.isArray(m) ? m[personKey(name)] : 0);
      return t > 0 && t <= Date.now() + 5000 ? t : 0;          // (a time in the future is a clock set back: not input)
    } catch (_) { return 0; }
  }
  function savePersonInput(name, t) {
    try {
      const k = personKey(name); if (!k || !(t > 0)) return;
      const m = lsJson(K.input, {}), o = m && typeof m === "object" && !Array.isArray(m) ? m : {};
      if (!(t > (Number(o[k]) || 0))) return;
      delete o[k]; o[k] = t;
      const keys = Object.keys(o); for (const x of keys.slice(0, Math.max(0, keys.length - 20))) delete o[x];
      lsSet(K.input, JSON.stringify(o));
    } catch (_) {}
  }
  function mark(t) { lastIn = Math.max(lastIn, t); stampAt = Math.max(stampAt, t); shareInput(t); }
  /* Two tabs of one computer run the same page and share one sign-in record: input in EITHER keeps the person in. The record holds the
     last input (a time), written at most every 5 seconds, and every rule check reads the other tab's latest first. */
  let sharedAt = 0;
  function shareInput(t) {
    try {
      if (!ready || t - sharedAt < 5000) return;
      sharedAt = t;
      if (cfg.multi) { if (many.size) { for (const s of many.values()) { s.li = Math.max(Number(s.li) || 0, lastIn); savePersonInput(s.name, lastIn); } savedLi = lastIn; saveRecs(); } }
      else if (cur) { cur.li = Math.max(Number(cur.li) || 0, lastIn); savedLi = cur.li; savePersonInput(cur.name, lastIn); saveShared(cur); }
    } catch (_) {}
  }
  /** writes the page's record only while it is still this session's (a second tab that already went on to another one keeps it) */
  function saveShared(s) { const r = loadRec(); if (!r || r.id === s.id) saveRec(s); }
  function syncShared() {
    try {
      const now = Date.now(), fresh = li => li > 0 && li <= now + 5000;        // (a time in the future is a clock that was set back: not input)
      if (cfg.multi) {
        for (const r of loadRecs()) { const s = many.get(r.key); const li = Number(r.li) || 0; if (s && s.id === r.id && fresh(li) && li > (Number(s.li) || 0)) s.li = Math.min(li, now); }
      } else if (cur) {
        const r = loadRec(), li = Number(r && r.li) || 0;
        if (r && r.id === cur.id && !r.ended && fresh(li) && li > (Number(cur.li) || 0)) cur.li = Math.min(li, now);
      }
    } catch (_) {}
  }
  /** records input at time t (ms), at most one stamp a second. When the gap since the last input had already reached 10 minutes, the
      sign-out that gap earned happens first (the person was gone), and only then does this input count. */
  function stamp(t, remote) {
    const now = Date.now(); t = t > 0 ? Math.min(t, now) : now;
    if (lastIn && t - stampAt < 1000) return;
    if (ready && lastIn && now - lastIn >= IDLE_MS) { try { checkRules(now); } catch (e) { warn("rules:", e); } }
    mark(t);
    if (!remote) relay(t);
  }
  function onInput(e) { if (!e || e.isTrusted !== true) return; const now = Date.now(); if (lastIn && now - stampAt < 1000) return; stamp(now, false); }
  /* A frame inside this page, or the page that holds this one, is told the TIME of an input (never more): a person working in the
     Design Station frame of the sorter is at the sorter, and the other way round. */
  function relay(t) {
    try {
      const m = { source: MSG, type: "input", at: t };
      if (inFrame) { try { window.parent.postMessage(m, "*"); } catch (_) {} }
      const fr = document.getElementsByTagName("iframe");
      for (let i = 0; i < fr.length; i++) { try { if (fr[i].contentWindow) fr[i].contentWindow.postMessage(m, "*"); } catch (_) {} }
    } catch (_) {}
  }
  function onMessage(ev) {
    try {
      const d = ev && ev.data;
      if (!d || d.source !== MSG || d.type !== "input") return;
      let ok = inFrame && ev.source === window.parent;
      if (!ok) { const fr = document.getElementsByTagName("iframe"); for (let i = 0; i < fr.length && !ok; i++) ok = fr[i].contentWindow === ev.source; }
      if (ok) stamp(Number(d.at) > 0 ? Number(d.at) : Date.now(), true);
    } catch (_) {}
  }
  /* ── Admin: asked of the server once per sign-in, kept for it; anything but a clear answer is "not Admin" ── */
  const admins = new Map();            // lower-case name → { v: true | false | null, day, p: pending promise, tries, at }
  const adminKey = n => String(n || "").toLowerCase();
  /** AD2's door (read-only, one name, in the body only): POST firebaseOrders { stationAdmin: name } answers { ok: true, admin: boolean }.
      → true | false | null (no answer now: 503, offline, too many, a timeout; try again later) | undefined (a refusal or a bad answer: stop asking) */
  function adminDoor(name) {
    try {
      if (typeof fetch !== "function") return Promise.resolve(undefined);
      let ac = null, to = 0;
      try { if (typeof AbortController === "function") { ac = new AbortController(); to = setTimeout(() => { try { ac.abort(); } catch (_) {} }, 8000); } } catch (_) {}
      return fetch(URL_(), { method: "POST", cache: "no-store", headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({ stationAdmin: name }), signal: ac ? ac.signal : undefined })
        .then(r => r.json().then(j => ({ s: r.status, j }), () => ({ s: r.status, j: null })))
        .then(o => {
          clearTimeout(to);
          if (o.s === 200 && o.j && o.j.ok === true && typeof o.j.admin === "boolean") return o.j.admin;
          return o.s >= 500 || o.s === 429 ? null : undefined;
        })
        .catch(() => { clearTimeout(to); return null; });
    } catch (_) { return Promise.resolve(null); }
  }
  /** Promise → true | false | null. null = not known (offline, no answer): the person is treated as not Admin. A sign-in asks once
      (fresh = true); a page that calls it for its own reasons (before a sign-in) may ask again, a little apart. */
  function isAdmin(name, fresh) {
    const n = cleanName(name); if (!n) return Promise.resolve(null);
    const k = adminKey(n), today = nyDay(), now = Date.now();
    let e = admins.get(k);
    if (e && e.day !== today) { admins.delete(k); e = null; }
    if (e && typeof e.v === "boolean") return Promise.resolve(e.v);
    if (e && e.p) return e.p;
    if (!e) { e = { v: null, day: today, p: null, tries: 0, at: 0 }; admins.set(k, e); }
    if (!fresh && (e.tries >= 6 || now - e.at < 40000)) return Promise.resolve(null);     // (a page asking on its own: a few tries, a little apart)
    e.tries++; e.at = now;
    e.p = adminDoor(n).then(v => { e.p = null; if (typeof v === "boolean") { e.v = v; return v; } if (v === undefined) e.tries = 99; return null; }, () => { e.p = null; return null; });
    return e.p;
  }
  /** the answer already known for a session's person: true | false | null */
  function knownAdmin(s) {
    if (!s) return null;
    if (s.adm === true || s.adm === false) return s.adm;
    const e = admins.get(adminKey(s.name));
    if (e && e.day === nyDay() && typeof e.v === "boolean") { s.adm = e.v; return e.v; }
    return null;
  }
  /** ONE question for a sign-in (and one more when a kept sign-in is picked up again by a reload with no answer stored): no answer, or a
      late one, is "not Admin" until it comes; it is never asked again for that sign-in (R5: fail closed) */
  function askAdmin(s) {
    if (!s || s.asked || knownAdmin(s) !== null) return;
    s.asked = true;
    isAdmin(s.name, true).then(v => { if (typeof v === "boolean" && !s.ended) { s.adm = v; if (runsHere(s)) persist(s); } }, () => {});
  }

  /** the sessions this page runs now: one (a single-person page), or one per person and task (a page in multi mode) */
  function liveSessions() { return cfg.multi ? [...many.values()] : (cur ? [cur] : []); }
  /** which rule, if any, a person whose last input was at L is under at `now`: Rule B (17:00 Toronto, no input in the 10 minutes before
      it) before Rule A (10 minutes of nothing). Either way the session ends AT THE LAST INPUT. */
  function dueRule(L, now) {
    L = Math.min(L, now);
    if (L <= closingAt(now) - IDLE_MS) return { reason: "closing", at: L };
    if (now - L >= IDLE_MS) return { reason: "idle", at: L };
    return null;
  }
  /** the computer's clock was set back: an input that is "in the future" would hold the idle clock still until the clock caught up
      (hours). It is taken as happening now, once, so the person is signed out 10 minutes after the change at the latest. */
  function rebase(now) {
    let moved = false;
    if (lastIn > now + 5000) { lastIn = now; stampAt = Math.min(stampAt, now); savedLi = Math.min(savedLi, now); sharedAt = Math.min(sharedAt, now); moved = true; }
    for (const s of liveSessions()) if (Number(s.li) > now + 5000) { s.li = now; moved = true; }
    if (moved) { try { if (cfg.multi) saveRecs(); else if (cur) saveShared(cur); } catch (_) {} }          // (the shared record loses its future time too)
  }
  /** signs out everybody who is due (not an Admin; a day that turned is the midnight rule's) */
  function checkRules(now) {
    if (!ready) return;
    now = now || Date.now();
    syncShared();
    rebase(now);
    const today = nyDay(now);
    for (const s of liveSessions().slice()) {
      if (!s || s.ended || s.day !== today || knownAdmin(s) === true) continue;
      const d = dueRule(inputOf(s), now);
      if (d) lapse(s, d.reason, d.at);
    }
  }
  /** ends a session by Rule A or B at the time of the last input and lets the page sign the person out (its work stays on screen) */
  function lapse(s, reason, at) {
    if (s.key !== undefined) manyQuiet.add(s.key);                       // (a multi page: this person only; the others carry on untouched)
    const p = cfg.multi ? null : person();
    finish(s, reason, at);
    if (s === cur) cur = null;
    if (!cfg.multi) { quiet = p && p.name === s.name ? p.name : quiet; seen = ""; }     // a page that could not clear its login does not start them again
    try { if (cfg.signOut) cfg.signOut(reason, { name: s.name, task: s.task }); } catch (e) { warn("signOut failed:", e); }
  }
  function rulesTick() {
    try {
      if (!ready) return;
      checkRules(Date.now());
    } catch (e) { warn("rulesTick:", e); }
  }

  /* ── start · beat · end ── */
  /** a login kept while the page was closed: when its last input is 10 minutes old (and it is not an Admin's) it lapsed then: true */
  function lapsedWhileClosed(r, now) {
    const li = Math.max(Number(r && r.li) || 0, personInput(r && r.name));          // (input at another station page of this computer counts)
    if (!li || r.adm === true || now - li < IDLE_MS) return false;
    const d = dueRule(li, now) || { reason: "idle", at: li };
    lapse(r, d.reason, d.at);
    return true;
  }
  function begin(p, resume) {
    const today = nyDay(), now = Date.now(), r = loadRec();
    if (r && !r.ended) {
      const same = stationOf(r) === stationNow();             // (a session of the other role is not gone on with: it ends, "switched")
      if (resume && same && r.name === p.name && r.day === today) {
        // (a reload inside the 10 minutes is input and goes on with the session; one after them finds the login lapsed)
        if (lapsedWhileClosed(r, now)) return;
        if (now - (r.lastBeat || 0) < CLOSED_MS) { cur = r; cur.asked = false; mark(now); cur.li = lastIn; savePersonInput(cur.name, now); beat(); askAdmin(cur); return; }
      }
      finish(r, r.name === p.name && same ? "signOut" : "switched");
    }
    mark(now); savePersonInput(p.name, now);
    const role = roleNow();
    cur = { id: `${cfg.device}-${shortId()}-${now.toString(36)}-${rand(4)}`.replace(/[^\w.:-]/g, "_").slice(0, 100),
      name: p.name, eid: p.id || "", day: today, startAt: now, lastBeat: now, li: now, station: role || cfg.station, role };
    saveRec(cur); savedLi = now;
    post(body(cur, "start"));
    askAdmin(cur);
  }
  function beat(s) {
    s = s || cur;
    if (!s) return;
    syncShared();
    s.lastBeat = Date.now(); s.li = Math.max(Number(s.li) || 0, lastIn); if (s === cur) savedLi = s.li; persist(s);
    post(body(s, "beat")).then(r => { if (r && r.j && r.j.ended === true) serverEnded(s, r.j.endReason); });
  }
  /** a beat that was answered `ended: true` by the server (AD2) with idle, closing or closed: the server ended this session on what it
      knew (a page whose beats did not arrive, e.g. the network was away). The page knows more. When its own rules agree (the last input
      is 10 minutes old, or 17:00 passed) the person is signed out the same way. When they do not (the person has been working), that
      session is over on the server and a new one carries on from now, so the person is never thrown out for a network gap and the
      time after it is recorded. Any other end reason (midnight, signOut, switched: another tab or page ended it) is left alone. */
  function serverEnded(s, reason) {
    try {
      if ((reason !== "idle" && reason !== "closing" && reason !== "closed") || !s || s.ended || !runsHere(s)) return;
      const now = Date.now(), L = inputOf(s), due = knownAdmin(s) === true || s.day !== nyDay(now) ? null : dueRule(L, now);
      if (due) { lapse(s, due.reason, due.at); return; }
      const key = s.key, name = s.name, at = Math.min(now, Math.max(Number(s.startAt) || 0, Number(s.lastBeat) || 0));
      finish(s, "closed", at);                                // (the server already has its end: this one is only kept for the record and is ignored there)
      if (key !== undefined) { const p = pagePeople().find(x => x.key === key); if (p && !manyQuiet.has(p.key)) mBegin(p, false); }
      else { const p = person(); if (p && !p.pending && p.name === name && p.name !== quiet) begin(p, false); }
    } catch (e) { warn("serverEnded:", e); }
  }
  /** ends a session. With an `at` (Rule A or B) it ends at that time, the last input. Otherwise: one that went quiet for 15
      minutes ended ("closed") at the last input it knew of (an Admin's at its last beat); one from an earlier day at its
      midnight; any other now, with the reason given */
  function finish(s, reason, at) {
    if (!s || s.ended) return;
    const now = Date.now(), quietFor = now - (s.lastBeat || s.startAt || 0);
    if (runsHere(s)) s.li = Math.max(Number(s.li) || 0, lastIn);
    if (at != null && Number.isFinite(at)) { const st = Number(s.startAt) || 0; at = Math.max(st <= now + 5000 ? st : 0, Math.min(now, at)); }       // (a start in the future is a clock set back)
    else {
      at = now;
      if (quietFor >= CLOSED_MS) {
        reason = "closed"; at = s.lastBeat || s.startAt || now;
        const li = Number(s.li) || 0;                            // a page that died ended at the last input it knew of (not an Admin's)
        if (knownAdmin(s) !== true && li > (Number(s.startAt) || 0) && li < at) at = li;
      }
      else if (s.day !== nyDay(now)) { reason = "midnight"; at = Math.min(now, nextMidnight(s.startAt || now)); }
      else if ((reason === "idle" || reason === "closing") && inputOf(s)) at = Math.min(now, inputOf(s));
    }
    s.ended = true; s.endReason = reason; s.endAt = at;
    if (s.key !== undefined) { many.delete(s.key); snap.delete(s.key); }       // (a multi page's session: it leaves the running list before it is saved)
    if (s.key !== undefined) persist(s);
    else { const k = loadRec(); if (!(k && k.id !== s.id && !k.ended)) saveRec(s); }       // (a second tab of this computer that already started the next session keeps it as the page's record: this one's end must not wipe it, or both tabs would start one each; ST2)
    if (s === cur) cur = null;
    admins.delete(adminKey(s.name));                           // the next sign-in asks again
    sendEnd(body(s, "end", reason, at));
  }
  function end(reason) { if (cur) finish(cur, REASONS.has(reason) ? reason : "signOut"); }

  /** the day turned: end the session and let the page sign out (its work stays on screen) */
  function midnight() {
    const today = nyDay(), r = cur || loadRec();
    if (r && !r.ended) finish(r, "midnight");
    cur = null;
    clearStaleDays(today);
    const p = person();
    quiet = p ? p.name : ""; seen = "";          // a page that could not clear its login does not start them again
    try { if (cfg.signOut) cfg.signOut("midnight"); } catch (e) { warn("signOut failed:", e); }
  }

  /** compares who the page says is signed in with the session; starts, switches and ends accordingly */
  function reconcile() {
    if (!ready) return;
    try {
      const today = nyDay(), p = person();
      if (cur && cur.day !== today) { midnight(); return; }
      if (!p) { if (cur) finish(cur, "signOut"); seen = ""; quiet = ""; return; }
      if (p.name === quiet) return;
      if (p.pending) {          // named but not signed in yet (the Sorter's "Laser or Design?" is not answered): no session, but the day is kept like any sign-in, so the day turning clears the name
        if (cur) finish(cur, "signOut");
        if (p.name !== seen) { seen = p.name; markDay(p.name, today); }
        const dp = dayOf(p.name);
        if (dp && dp !== today) midnight();
        return;
      }
      if (cur && cur.name === p.name && stationOf(cur) !== stationNow()) {     // the role changed: the one session ends, the other starts
        seen = p.name; markDay(p.name, today);
        finish(cur, "switched"); begin(p, true);          // (resume: another tab that switched first has already started the new one, this tab goes on with it; ST2)
        return;
      }
      if (p.name !== seen) {                     // someone signed in here (or in another tab of this computer)
        seen = p.name; markDay(p.name, today);
        if (cur && cur.name !== p.name) finish(cur, "switched");
        if (!cur) begin(p, true);
        return;
      }
      const d = dayOf(p.name);
      if (d && d !== today) { midnight(); return; }
      if (!cur) begin(p, true);
    } catch (e) { warn("reconcile:", e); }
  }
  function tick() {
    try {
      if (cfg.multi) { mTick(); flush(); if (labelBox && !labelBox.querySelector("input") && labelBox.dataset.t !== label()) paint(labelBox); return; }
      reconcile();
      checkRules(Date.now());                    // (a page that slept: the person whose last input is 10 minutes old signs out here, at that input)
      // the page slept or was frozen for 15 minutes: that session closed at its last beat, a new one goes on from now
      if (cur && Date.now() - (cur.lastBeat || 0) >= CLOSED_MS) {
        const p = person(); finish(cur, "closed");
        if (p && !p.pending && p.name !== quiet) begin(p, false);
      }
      if (cur && Date.now() - (cur.lastBeat || 0) >= BEAT_MS) beat();
      else if (cur && lastIn > savedLi) { cur.li = savedLi = lastIn; saveShared(cur); }     // the clock a reload goes on from
      flush();
      if (labelBox && !labelBox.querySelector("input") && labelBox.dataset.t !== label()) paint(labelBox);
    } catch (e) { warn("tick:", e); }
  }
  /* the next New York midnight: a timer (at most an hour at a time, so sleep and clock changes are caught), re-armed on
     wake, on visibilitychange and focus; the 30 s tick checks the day as well */
  function armMidnight() {
    try {
      clearTimeout(midT);
      const now = Date.now(), ms = nextMidnight(now) - now + 1000;
      midT = setTimeout(() => { tick(); armMidnight(); }, Math.max(1000, Math.min(ms, 3600e3)));
    } catch (_) {}
  }
  function wake() { tick(); rulesTick(); armMidnight(); }

  /* ── pages with more than one person at once (multi: true; the Welding station) ──
     One session per person AND task (Tess welding, Tess matching and Ray matching are three), all running together.
     A sign-in adds one and never ends another. The page says who is signed in (people()); a name it drops is signed out.
     The session id starts `${station}__${device}__${person}__${task}__` and ends with the start time, so the same person
     can sign in again the same day (an ended session stays ended on the server). Input at the page counts for everybody. */
  const cleanTask = v => { const s = clean(v, 20).toLowerCase(); return /^[a-z][a-z-]*$/.test(s) ? s : ""; };
  const pkey = (name, task) => name.toLowerCase() + "|" + task;
  const sameName = (a, b) => String(a).toLowerCase() === String(b).toLowerCase();
  /** who the page says is signed in: [{ name, id, task, key }], tidy, no duplicates (no `people` callback: the sessions themselves) */
  function pagePeople() {
    try {
      const raw = cfg.people ? cfg.people() : [...many.values()], out = [], have = new Set();
      for (const p of Array.isArray(raw) ? raw : []) {
        const name = cleanName(p && p.name); if (!name) continue;
        const task = cleanTask(p.task), key = pkey(name, task);
        if (have.has(key)) continue;
        have.add(key); out.push({ name, id: safeId(p.id), task, key });
      }
      return out;
    } catch (_) { return []; }
  }
  const idPart = (v, n) => clean(v, 60).replace(/[^\w.:-]+/g, "_").replace(/^_+|_+$/g, "").slice(0, n) || "x";
  function mBegin(p, resume) {
    const today = nyDay(), now = Date.now();
    const old = many.get(p.key) || snap.get(p.key) || loadRecs().find(r => r.key === p.key);
    if (old && !old.ended) {
      if (resume && old.day === today) {
        // (a reload inside the 10 minutes is input and goes on with the session; one after them finds the login lapsed, not given a new session)
        if (lapsedWhileClosed(old, now)) return null;
        if (now - (old.lastBeat || 0) < CLOSED_MS) { many.set(p.key, old); snap.delete(p.key); old.li = Math.max(Number(old.li) || 0, lastIn); savePersonInput(old.name, now); old.asked = false; beat(old); askAdmin(old); return old; }
      }
      many.set(p.key, old); finish(old, "signOut");        // (finish says "closed" or "midnight" when that is what happened)
    }
    const s = { id: `${idPart(cfg.station, 20)}__${idPart(cfg.device, 30)}__${idPart(p.name, 24)}__${idPart(p.task || "-", 12)}__${now.toString(36)}${rand(3)}`.slice(0, 100),
      name: p.name, eid: p.id || "", task: p.task || "", key: p.key, day: today, startAt: now, lastBeat: now, touchedAt: now, addedAt: now, li: now };
    mark(now); savePersonInput(p.name, now);
    many.set(p.key, s); saveRecs();
    post(body(s, "start"));
    askAdmin(s);
    return s;
  }
  /** page-level end (the day turned; the idle and closing timers use it too): every session ends with `reason`, and the page signs
      each person out, once per person (its work stays on screen) */
  function mEndAll(reason) {
    const listed = pagePeople();
    for (const r of [...many.values(), ...loadRecs().filter(r => !many.has(r.key))]) { many.set(r.key, r); finish(r, reason); }
    snap = new Map();
    for (const p of listed) manyQuiet.add(p.key);        // a page that could not clear its login does not start them again
    for (const p of listed) { try { if (cfg.signOut) cfg.signOut(reason, { name: p.name, task: p.task }); } catch (e) { warn("signOut failed:", e); } }
  }
  function mMidnight() { clearStaleDays(nyDay()); mEndAll("midnight"); }
  function mReconcile() {
    if (!ready) return;
    try {
      const today = nyDay(), list = pagePeople(), keys = new Set(list.map(p => p.key)), now = Date.now();
      if ([...many.values()].some(s => s.day !== today)) { mMidnight(); return; }
      snap = new Map(loadRecs().filter(r => !many.has(r.key)).map(r => [r.key, r]));
      for (const s of [...many.values()]) if (!keys.has(s.key) && now - (s.addedAt || 0) > 5000) finish(s, "signOut");   // signed out on the page (or in another tab)
      for (const k of [...manyQuiet]) if (!keys.has(k)) manyQuiet.delete(k);
      for (const p of list) {
        if (manyQuiet.has(p.key)) continue;
        const d = dayOf(p.name);
        if (d && d !== today) { mMidnight(); return; }
        if (!many.has(p.key)) {
          /* a person who just appeared on the page with no session and no saved record: a sign-in in another tab of this computer writes the page's
             list a moment BEFORE its session record, and the storage event can win that race, so this tab would start a second session for the same
             person. Look once more (about a second and a half on) before starting one; the record is there by then and is gone on with (ST2). */
          if (!snap.has(p.key)) {
            const first = unseen.get(p.key), mono = monoNow();
            if (first === undefined) { unseen.set(p.key, mono); setTimeout(tick, 1500); continue; }
            if (mono - first < 1400) continue;
          }
          unseen.delete(p.key); markDay(p.name, today); mBegin(p, true);
        }
      }
      for (const k of [...unseen.keys()]) if (!keys.has(k)) unseen.delete(k);
    } catch (e) { warn("reconcile:", e); }
    snap = new Map();
  }
  function mTick() {
    mReconcile();
    const now = Date.now();
    checkRules(now);                                     // (each person whose page's last input is 10 minutes old, or past 17:00, signs out here, at that input; an Admin does not)
    // the page slept or was frozen for 15 minutes: that session closed at its last beat, a new one goes on from now
    for (const s of [...many.values()]) {
      if (now - (s.lastBeat || 0) < CLOSED_MS) continue;
      finish(s, "closed");
      const p = pagePeople().find(x => x.key === s.key);
      if (p && !manyQuiet.has(p.key)) mBegin(p, false);
    }
    for (const s of [...many.values()]) if (now - (s.lastBeat || 0) >= BEAT_MS) beat(s);
    if (lastIn > savedLi) { for (const s of many.values()) s.li = lastIn; savedLi = lastIn; saveRecs(); }       // the clock a reload goes on from
  }
  function mInit() {
    const today = nyDay(), list = pagePeople(), day = lsGet(K.day);
    const legacy = lsJson(recKey(), null);                // a single-person session left by this page before it went multi: it ends now
    if (legacy && typeof legacy === "object" && typeof legacy.id === "string" && !legacy.ended) finish(legacy, "signOut");
    if ((day && day !== today && list.length) || list.some(p => { const d = dayOf(p.name); return d && d !== today; })) { mMidnight(); return; }
    snap = new Map(loadRecs().map(r => [r.key, r]));
    for (const p of list) { if (!dayOf(p.name)) markDay(p.name, today); mBegin(p, true); }
    for (const r of [...snap.values()]) { many.set(r.key, r); finish(r, "signOut"); }                  // signed out while this page was closed
    snap = new Map();
  }
  function mSignedIn(who) {
    const name = cleanName(who && who.name);
    if (!name) return;
    const task = cleanTask(who.task), key = pkey(name, task), today = nyDay();
    markDay(name, today); manyQuiet.delete(key); touch(Date.now(), { name, task });
    const s = many.get(key);
    if (s && s.day === today) { s.addedAt = Date.now(); return; }       // already in: nothing changes, nobody else is touched
    if (s) finish(s, "signOut");
    mBegin({ name, id: safeId(who.id), task, key }, false);
  }
  function mSignedOut(reason, who) {
    const r = REASONS.has(reason) ? reason : "signOut";
    const name = who && cleanName(who.name), task = who && who.task ? cleanTask(who.task) : "";
    for (const s of [...many.values()]) {
      if (who && (!name || !sameName(s.name, name) || (task && s.task !== task))) continue;
      manyQuiet.add(s.key); finish(s, r);
    }
  }
  /** the session an action is credited to: of this task (default: the credit task, "matching"), the one with the latest input
      (input at a shared page cannot be told apart: unless the page says whose, it is the one who signed in last); null: nobody */
  function pick(task) {
    task = task === undefined ? cfg.creditTask : cleanTask(task);
    const listed = cfg.people ? new Set(pagePeople().map(p => p.key)) : null;
    let best = null;
    for (const s of many.values()) {
      if (task && s.task !== task) continue;
      if (listed && !listed.has(s.key)) continue;
      const a = Math.max(s.startAt || 0, s.touchedAt || 0), b = best ? Math.max(best.startAt || 0, best.touchedAt || 0) : -1;
      if (!best || a > b || (a === b && (s.startAt || 0) > (best.startAt || 0))) best = s;
    }
    return best;
  }
  /** an input happened at this page (a tap, a key, a scan): one timestamp a second at most; the content is never kept. With who
      ({ name, task }) the input is also that person's. */
  function touch(ts, who) {
    try {
      let t = Number(ts); const now = Date.now();
      if (!(t > 0) || t > now) t = now;
      stamp(t, false);                                     // (the one stamp a second; a gap of 10 minutes signs the person out first)
      if (who && cfg.multi) {
        const name = cleanName(who.name), s = name && many.get(pkey(name, cleanTask(who.task)));
        if (s) s.touchedAt = Math.max(s.touchedAt || 0, t);
      }
    } catch (_) {}
  }

  /* ── the tiny "Computer: …" line, and its one-time name field (never a pop-up) ── */
  function paint(box) {
    try {
      if (box.querySelector("input")) return;
      box.textContent = ""; box.dataset.t = label();
      const t = document.createElement("span"); t.textContent = "Computer: " + label(); box.appendChild(t);
      if (lsGet(K.name)) return;
      const b = document.createElement("button");
      b.type = "button"; b.textContent = "Name it"; b.title = "Give this computer a name (once)";
      b.style.cssText = "all:unset;cursor:pointer;color:#2563eb;margin-left:6px;text-decoration:underline;";
      b.addEventListener("click", e => {
        e.preventDefault(); e.stopPropagation();
        const inp = document.createElement("input");
        inp.type = "text"; inp.maxLength = 30; inp.placeholder = "e.g. Front bench"; inp.className = "browser-default";
        inp.setAttribute("aria-label", "Name this computer");
        inp.style.cssText = "width:110px;height:20px;margin:0 0 0 6px;padding:0 4px;box-sizing:border-box;border:1px solid #d1d5db;border-radius:4px;font:inherit;color:#111827;background:#fff;";
        const done = save => {
          const v = clean(inp.value, 30);
          if (save && v && !lsGet(K.name)) lsSet(K.name, v);
          inp.remove(); paint(box);
        };
        inp.addEventListener("keydown", ev => {
          ev.stopPropagation();
          if (ev.key === "Enter") { ev.preventDefault(); done(true); } else if (ev.key === "Escape") { ev.preventDefault(); done(false); }
        });
        inp.addEventListener("blur", () => { if (inp.isConnected) done(true); });
        b.replaceWith(inp); inp.focus();
      });
      box.appendChild(b);
    } catch (_) {}
  }
  function mountLabel() {
    try {
      const host = typeof cfg.labelHost === "string" ? document.querySelector(cfg.labelHost) : cfg.labelHost;
      if (!host || host.querySelector(".station-session-pc")) return;
      const box = document.createElement("div");
      box.className = "station-session-pc";
      box.style.cssText = "font:11px/1.35 -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;color:#6b7280;" + (cfg.labelCss || "");
      labelBox = box; paint(box); host.appendChild(box);
    } catch (_) {}
  }

  /* ── the API ── */
  function init(o) {
    try {
      if (ready) return;
      o = o || {};
      cfg.station = clean(o.station, 20); cfg.device = clean(o.device, 40) || cfg.station;
      cfg.person = typeof o.person === "function" ? o.person : null;
      cfg.role = typeof o.role === "function" ? o.role : null;
      cfg.signOut = typeof o.signOut === "function" ? o.signOut : null;
      cfg.labelHost = o.labelHost || null; cfg.labelCss = String(o.labelCss || "");
      cfg.sandbox = o.sandbox != null ? !!o.sandbox : /[?&]sandbox=1\b/.test(location.search);
      cfg.multi = o.multi === true; cfg.people = cfg.multi && typeof o.people === "function" ? o.people : null;
      cfg.creditTask = cleanTask(o.creditTask) || "matching";
      computerId();
      ready = true;
      touch();                                       // the page was opened: an input
      // loaded after the day turned: sign out before anything else. (A login this module has never seen, with no day
      // stored on this computer, e.g. the first load after it was added, counts as today's and ends at the next midnight.)
      const today = nyDay(), p = person(), day = lsGet(K.day), d = p ? dayOf(p.name) : "";
      if (cfg.multi) mInit();
      else if ((day && day !== today) || (d && d !== today)) midnight();
      else if (p) { if (!d) markDay(p.name, today); seen = p.name; if (!p.pending) begin(p, true); }
      tickT = setInterval(tick, TICK_MS);
      rulesT = setInterval(rulesTick, RULES_TICK_MS);
      armMidnight();
      document.addEventListener("visibilitychange", () => { if (document.visibilityState === "visible") wake(); });
      document.addEventListener("resume", wake);                       // a frozen page thawed (Page Lifecycle)
      window.addEventListener("focus", wake);
      window.addEventListener("pageshow", wake);
      window.addEventListener("online", flush);
      for (const t of INPUT_EVENTS) window.addEventListener(t, onInput, { capture: true, passive: true });
      window.addEventListener("message", onMessage);
      window.addEventListener("storage", e => { if (!e.key || !/^station_session\./.test(e.key)) setTimeout(tick, 0); });
      window.addEventListener("pagehide", () => { try { if (cur) beat(); for (const x of [...many.values()]) beat(x); } catch (_) {} });
      if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", mountLabel); else mountLabel();
      setTimeout(flush, 3000);
    } catch (e) { warn("init failed:", e); }
  }
  function signedIn(who) {
    try {
      if (!ready) return;
      if (cfg.multi) return mSignedIn(who);
      const name = cleanName(who && who.name);
      if (!name) return;
      const today = nyDay();
      markDay(name, today); seen = name; quiet = "";
      if (cur && cur.name === name && cur.day === today && stationOf(cur) === stationNow()) return;
      if (cur) finish(cur, "switched");
      begin({ name, id: safeId(who && who.id) }, !!(who && who.resume));      // (resume: a page loaded with its person already there goes on with the session it kept, as init does)
    } catch (e) { warn("signedIn:", e); }
  }
  function signedOut(reason, who) {
    try {
      if (!ready) return;
      if (cfg.multi) return mSignedOut(reason, who);
      const p = person();
      end(REASONS.has(reason) ? reason : "signOut");
      seen = ""; quiet = p ? p.name : "";
    } catch (e) { warn("signedOut:", e); }
  }

  /** the page says its role changed (the Sorter app: "Laser · switch to Design"): the session under the old one ends ("switched"), the
      one under the new starts. Nothing happens when nobody is signed in or the role is the same. */
  function roleChanged() { try { reconcile(); } catch (e) { warn("roleChanged:", e); } }

  window.StationSession = {
    init, signedIn, signedOut, roleChanged,
    // for the pages and the tests: read-only views
    computerId: () => { try { return computerId(); } catch (_) { return ""; } },
    computerLabel: () => { try { return label(); } catch (_) { return ""; } },
    current: () => {
      if (cfg.multi) { const s = pick(); return s ? { id: s.id, person: s.name, task: s.task, startAt: s.startAt, day: s.day } : null; }
      return cur ? { id: cur.id, person: cur.name, startAt: cur.startAt, day: cur.day, station: stationOf(cur), role: cur.role || "" } : null;
    },
    /** the role the running session was started under: "laser" | "design", or "" (no role: the page's own station) */
    role: () => { try { return !cfg.multi && cur && ready ? (cur.role || "") : ""; } catch (_) { return ""; } },
    /** who is signed in on this page now (a multi page: everybody, one entry per person and task; any page: [] when nobody):
        [{ name, task, session, since, startAt, lastInputAt, device, station }]. Names only, never a PIN. */
    people: () => {
      try {
        if (!ready) return [];
        const at = Math.max(lastIn, 0);
        if (!cfg.multi) {
          const p = person();
          return cur && p && p.name === cur.name ? [{ name: cur.name, task: "", session: cur.id, since: cur.startAt, startAt: cur.startAt, lastInputAt: Math.max(at, cur.startAt), device: cfg.device, station: stationOf(cur) }].map(x => (cur.role ? Object.assign(x, { role: cur.role }) : x)) : [];
        }
        const listed = cfg.people ? new Set(pagePeople().map(p => p.key)) : null;
        return [...many.values()].filter(s => !listed || listed.has(s.key)).sort((a, b) => a.startAt - b.startAt || (a.id < b.id ? -1 : 1))
          .map(s => ({ name: s.name, task: s.task, session: s.id, since: s.startAt, startAt: s.startAt, lastInputAt: Math.max(at, s.startAt), device: cfg.device, station: cfg.station }));
      } catch (_) { return []; }
    },
    /** an input at this page (see touch above): ts defaults to now; who ({ name, task }) makes it that person's too */
    touch,
    /** the latest input at this page, ms (0: none yet): input counts for everybody signed in here */
    lastInput: () => lastIn,
    /** who is working now, for station-activity.js: { person, station, device, computer, session, startAt, sandbox }, or null
        when nobody is signed in (or this page's session is not running). The name only, never a PIN. */
    who: task => {
      try {
        if (cfg.multi) {                                // the Matching person (latest input if two; nobody: null), or the one of `task`
          const s = ready ? pick(task) : null;
          return s ? { person: s.name, station: cfg.station, device: cfg.device, computer: computerId(), session: s.id, startAt: s.startAt, sandbox: !!cfg.sandbox, task: s.task } : null;
        }
        if (!ready || !cur) return null;
        const p = person();
        if (!p || p.name !== cur.name) return null;
        const w = { person: p.name, station: stationOf(cur), device: cfg.device, computer: computerId(), session: cur.id, startAt: cur.startAt, sandbox: !!cfg.sandbox };
        if (cur.role) w.role = cur.role;
        return w;
      } catch (_) { return null; }
    },
    /** this page (known even when nobody is signed in): { station, device, computer, sandbox }, or null before init */
    page: () => {
      try { return ready ? { station: stationNow(), device: cfg.device, computer: computerId(), sandbox: !!cfg.sandbox } : null; } catch (_) { return null; }
    },
    nyDay, nextMidnight,
    /** auto sign-out (see the top of this file) */
    isAdmin,                                                            // Promise: true | false | null (not known: treated as not Admin)
    notice: (reason, tail) => { const t = NOTICES[reason]; return t ? (tail ? t + " " + String(tail) : t) : ""; },
    idleMs: IDLE_MS, closingAt                                         // the 10 minutes; the latest 17:00 in Toronto at or before a time
  };
})();
