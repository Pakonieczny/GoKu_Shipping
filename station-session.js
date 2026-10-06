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
 *  Nothing here blocks or throws into the page: every write is fire-and-forget and every entry point is wrapped. */
(function () {
  "use strict";
  if (window.StationSession) return;
  const TZ = "America/New_York";
  const BEAT_MS = 5 * 60000, CLOSED_MS = 15 * 60000, TICK_MS = 30000;
  const REASONS = new Set(["signOut", "midnight", "switched", "closed"]);
  const LABEL = { sorting: "Sorting", welding: "Welding", assembly: "Assembly", shipping: "Shipping", design: "Design",
    laser: "Laser", sorter: "Sorter", qr: "QR Printer", inbox: "Inbox" };
  const K = { computer: "station_computer_id", name: "station_computer_name", day: "station_signin_day",
    days: "station_signin_days", unsent: "station_session_unsent" };
  const ROLES = new Set(["laser", "design"]);
  const cfg = { station: "", device: "", person: null, role: null, signOut: null, labelHost: null, labelCss: "", sandbox: false,
    multi: false, people: null, creditTask: "matching" };
  let ready = false, cur = null, seen = "", quiet = "", tickT = 0, midT = 0, memId = "", labelBox = null;
  const many = new Map(), manyQuiet = new Set();        // multi pages: running sessions by person and task; people the page has not dropped yet
  const unseen = new Map();                            // multi pages: people listed with no session and no record yet, by when this page first saw them (monotonic ms)
  const monoNow = () => { try { if (typeof performance !== "undefined" && performance && typeof performance.now === "function") return performance.now(); } catch (_) {} return Date.now(); };
  let snap = new Map();                                // multi pages: the sessions kept in storage, while a reload or a second tab picks them up
  let lastIn = 0;                                      // the latest input at this page (ms)

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

  /* ── New York time ── */
  let fmt = null;
  try { fmt = new Intl.DateTimeFormat("en-US", { timeZone: TZ, hourCycle: "h23", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" }); } catch (_) {}
  function nyParts(t) {
    const d = new Date(t);
    if (fmt) {
      try {
        const o = {}; for (const p of fmt.formatToParts(d)) o[p.type] = p.value;
        return { y: +o.year, m: +o.month, d: +o.day, h: +o.hour % 24, mi: +o.minute, s: +o.second };
      } catch (_) {}
    }
    const e = new Date(t - 5 * 3600e3);                 // no time zone data: Eastern Standard Time
    return { y: e.getUTCFullYear(), m: e.getUTCMonth() + 1, d: e.getUTCDate(), h: e.getUTCHours(), mi: e.getUTCMinutes(), s: e.getUTCSeconds() };
  }
  const pad = n => String(n).padStart(2, "0");
  function nyDay(t) { const p = nyParts(t == null ? Date.now() : t); return `${p.y}-${pad(p.m)}-${pad(p.d)}`; }
  function offset(t) { const p = nyParts(t); return Date.UTC(p.y, p.m - 1, p.d, p.h, p.mi, p.s) - Math.floor(t / 1000) * 1000; }
  /** the first New York midnight after t (ms) */
  function nextMidnight(t) {
    const p = nyParts(t), wall = Date.UTC(p.y, p.m - 1, p.d + 1);
    let u = wall - offset(t); u = wall - offset(u);
    return u > t ? u : t + 86400e3;
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
    return { id: s.id, event, person: s.name, employeeId: s.eid || "", station: stationOf(s), device: cfg.device,
      computerId: computerId(), computerLabel: label(), at: at || Date.now(), reason: reason || undefined, task: s.task || undefined, role: s.role || undefined };
  }
  function post(session) {
    try {
      const text = JSON.stringify({ session });
      if (typeof fetch !== "function") { if (navigator.sendBeacon) navigator.sendBeacon(URL_(), new Blob([text], { type: "application/json" })); return Promise.resolve(null); }
      return fetch(URL_(), { method: "POST", headers: { "Content-Type": "application/json" }, body: text, keepalive: true })
        .then(r => ({ status: r.status }), () => null).catch(() => null);
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
    Promise.all(q.map(b => post(b).then(r => { if ((!r || r.status >= 500 || r.status === 429) && Date.now() - (b.at || 0) < 7 * 86400e3) keep(b); })))
      .then(() => { flushing = false; }, () => { flushing = false; });
  }

  /* ── start · beat · end ── */
  function begin(p, resume) {
    const today = nyDay(), now = Date.now(), r = loadRec();
    if (r && !r.ended) {
      const same = stationOf(r) === stationNow();             // (a session of the other role is not gone on with: it ends, "switched")
      if (resume && same && r.name === p.name && r.day === today && now - (r.lastBeat || 0) < CLOSED_MS) { cur = r; beat(); return; }
      finish(r, r.name === p.name && same ? "signOut" : "switched");
    }
    const role = roleNow();
    cur = { id: `${cfg.device}-${shortId()}-${now.toString(36)}-${rand(4)}`.replace(/[^\w.:-]/g, "_").slice(0, 100),
      name: p.name, eid: p.id || "", day: today, startAt: now, lastBeat: now, station: role || cfg.station, role };
    saveRec(cur);
    post(body(cur, "start"));
  }
  function beat(s) {
    s = s || cur;
    if (!s) return;
    s.lastBeat = Date.now(); persist(s);
    post(body(s, "beat"));
  }
  /** ends a session: one that went quiet for 15 minutes ended at its last beat ("closed"), one from an earlier day at
      its midnight; otherwise now, with the reason given */
  function finish(s, reason) {
    if (!s || s.ended) return;
    const now = Date.now(), quietFor = now - (s.lastBeat || s.startAt || 0);
    let at = now;
    if (quietFor >= CLOSED_MS) { reason = "closed"; at = s.lastBeat || s.startAt || now; }
    else if (s.day !== nyDay(now)) { reason = "midnight"; at = Math.min(now, nextMidnight(s.startAt || now)); }
    s.ended = true; s.endReason = reason; s.endAt = at;
    if (s.key !== undefined) { many.delete(s.key); snap.delete(s.key); }       // (a multi page's session: it leaves the running list before it is saved)
    if (s.key !== undefined) persist(s);
    else { const k = loadRec(); if (!(k && k.id !== s.id && !k.ended)) saveRec(s); }       // (a second tab of this computer that already started the next session keeps it as the page's record: this one's end must not wipe it, or both tabs would start one each; ST2)
    if (s === cur) cur = null;
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
      // the page slept or was frozen for 15 minutes: that session closed at its last beat, a new one goes on from now
      if (cur && Date.now() - (cur.lastBeat || 0) >= CLOSED_MS) {
        const p = person(); finish(cur, "closed");
        if (p && !p.pending && p.name !== quiet) begin(p, false);
      }
      if (cur && Date.now() - (cur.lastBeat || 0) >= BEAT_MS) beat();
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
  function wake() { tick(); armMidnight(); }

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
      if (resume && old.day === today && now - (old.lastBeat || 0) < CLOSED_MS) { many.set(p.key, old); snap.delete(p.key); beat(old); return old; }
      many.set(p.key, old); finish(old, "signOut");        // (finish says "closed" or "midnight" when that is what happened)
    }
    const s = { id: `${idPart(cfg.station, 20)}__${idPart(cfg.device, 30)}__${idPart(p.name, 24)}__${idPart(p.task || "-", 12)}__${now.toString(36)}${rand(3)}`.slice(0, 100),
      name: p.name, eid: p.id || "", task: p.task || "", key: p.key, day: today, startAt: now, lastBeat: now, touchedAt: now, addedAt: now };
    many.set(p.key, s); saveRecs();
    post(body(s, "start"));
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
    // the page slept or was frozen for 15 minutes: that session closed at its last beat, a new one goes on from now
    for (const s of [...many.values()]) {
      if (now - (s.lastBeat || 0) < CLOSED_MS) continue;
      finish(s, "closed");
      const p = pagePeople().find(x => x.key === s.key);
      if (p && !manyQuiet.has(p.key)) mBegin(p, false);
    }
    for (const s of [...many.values()]) if (now - (s.lastBeat || 0) >= BEAT_MS) beat(s);
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
      if (!(t > 0) || t > now + 60000) t = now;
      if (t - lastIn >= 1000) lastIn = t;
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
      armMidnight();
      document.addEventListener("visibilitychange", () => { if (document.visibilityState === "visible") wake(); });
      window.addEventListener("focus", wake);
      window.addEventListener("pageshow", wake);
      window.addEventListener("online", flush);
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
    nyDay, nextMidnight
  };
})();
