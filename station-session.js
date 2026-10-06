/*  station-session.js — how long each person is signed in, on which computer, at which station or page, and the
 *  midnight sign-out: at midnight (America/New_York) everybody is signed out, so every morning everybody signs in again
 *  (Paul, 28 Sep 23:53).
 *
 *    StationSession.init({ station, device, person: () => ({ name, id }) | null, signOut: reason => void,
 *                          labelHost?, labelCss?, sandbox? })
 *        station: a key of STATIONS in _orderTimeline.js (sorting, welding, assembly, shipping, design, laser, sorter,
 *        qr, inbox); device: the page ("weld-1", "assembly-2", …). person(): who is signed in on this page now (null:
 *        nobody). signOut(reason): the page clears its own login keys and shows its own sign-in screen or PIN box,
 *        keeping the work on screen. labelHost (optional, element or selector): where the tiny "Computer: …" line and
 *        its one-time name field go; labelCss: extra inline CSS for that line.
 *    StationSession.signedIn({ name, id })   // call from the page's login success
 *    StationSession.signedOut(reason)        // call from the page's own sign-out ("signOut" unless said otherwise)
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
 *      pressed, never a log. Events a script made itself (isTrusted false) are not input. A reload is input. Input in a frame
 *      inside the page, or in the page that holds this frame, counts too (only the time is passed on, by postMessage).
 *    The session's end time is the LAST INPUT, not the moment the sign-out was noticed, so hours stay honest. Heartbeats and the
 *      end carry lastInputAt. Only what keeps the clock across a reload is stored: the last input time inside the page's own
 *      session record (localStorage station_session.<station>.<device>), kept to the latest few seconds.
 *    Admin: the name is asked of the server once per sign-in (GET firebaseOrders?isAdmin=<name>, answer { admin: boolean }) and
 *      kept for that sign-in. Unknown, offline or any other answer is NOT Admin (fail closed). An Admin gets neither rule; the
 *      midnight (New York) sign-out stays for everybody.
 *    Timing uses Date.now() against stored times on a 10 second tick, on every input that follows a gap, and on
 *      visibilitychange / focus / pageshow / resume (a sleeping computer or a background tab signs the person out at the
 *      right moment and at the right end time); never one long setTimeout.
 *    StationSession.touch(ts?)         record input (the scanner relay calls it for every scan it hands to the page)
 *    StationSession.lastInput()        ms of the last input at this page
 *    StationSession.isAdmin(name)      Promise: true | false | null (null: not known, treated as not Admin)
 *    StationSession.notice(reason, tail?)  the one-line wording for the page's own notice ("" for any other reason)
 *    The page's signOut(reason, who) now also receives "idle" and "closing" (the work on screen stays, the page shows its own
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
    days: "station_signin_days", unsent: "station_session_unsent" };
  const cfg = { station: "", device: "", person: null, signOut: null, labelHost: null, labelCss: "", sandbox: false };
  let ready = false, cur = null, seen = "", quiet = "", tickT = 0, midT = 0, memId = "", labelBox = null;
  let lastInputAt = 0, stampAt = 0, rulesT = 0, savedLi = 0;     // the last input at this page (ms), in memory

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
      return name ? { name, id: safeId(p.id) } : null;
    } catch (_) { return null; }
  }
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

  /* ── the door ── */
  const URL_ = () => "/.netlify/functions/firebaseOrders" + (cfg.sandbox ? "?sandbox=1" : "");
  function body(s, event, reason, at) {
    const o = { id: s.id, event, person: s.name, employeeId: s.eid || "", station: cfg.station, device: cfg.device,
      computerId: computerId(), computerLabel: label(), at: at || Date.now(), reason: reason || undefined };
    const li = inputOf(s); if (li) o.lastInputAt = li;          // the time of the last input (ms), never what it was
    return o;
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

  /* ── input: the time of the last user input at this page (memory only; never what was typed or pressed) ── */
  const INPUT_EVENTS = ["pointerdown", "pointermove", "mousedown", "mousemove", "touchstart", "touchmove", "keydown", "wheel", "click", "input", "paste"];
  const MSG = "station-session";
  const inFrame = (() => { try { return window.self !== window.top; } catch (_) { return true; } })();
  /** the last input that counts for a session: this page's own when the session runs here, the stored one for a record left by an earlier load */
  function inputOf(s) {
    if (!s) return 0;
    const li = s === cur ? Math.max(lastInputAt, Number(s.li) || 0) : Number(s.li) || 0;
    return li > 0 ? Math.min(Date.now(), Math.max(li, Number(s.startAt) || 0)) : 0;
  }
  function mark(t) { lastInputAt = Math.max(lastInputAt, t); stampAt = Math.max(stampAt, t); }
  /** records input at time t (ms), at most one stamp a second. When the gap since the last input had already reached 10 minutes, the
      sign-out that gap earned happens first (the person was gone), and only then does this input count. */
  function stamp(t, remote) {
    const now = Date.now(); t = t > 0 ? Math.min(t, now) : now;
    if (lastInputAt && t - stampAt < 1000) return;
    if (ready && lastInputAt && now - lastInputAt >= IDLE_MS) { try { checkRules(now); } catch (e) { warn("rules:", e); } }
    mark(t);
    if (!remote) relay(t);
  }
  function onInput(e) { if (e && e.isTrusted === false) return; const now = Date.now(); if (lastInputAt && now - stampAt < 1000) return; stamp(now, false); }
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
  /** input recorded by hand: the station's scanner relay calls it for every scan it hands to this page */
  function touch(ts) { try { if (ready) stamp(Number(ts) > 0 ? Number(ts) : Date.now(), false); } catch (_) {} }

  /* ── Admin: asked of the server once per sign-in, kept for it; anything but a clear answer is "not Admin" ── */
  const admins = new Map();            // lower-case name → { v: true | false | null, day, p: pending promise, tries, at }
  const adminKey = n => String(n || "").toLowerCase();
  function adminDoor(name) {          // → true | false | null (try again later) | undefined (the door does not answer this: stop asking)
    try {
      if (typeof fetch !== "function") return Promise.resolve(undefined);
      let ac = null, to = 0;
      try { if (typeof AbortController === "function") { ac = new AbortController(); to = setTimeout(() => { try { ac.abort(); } catch (_) {} }, 8000); } } catch (_) {}
      const url = URL_() + (cfg.sandbox ? "&" : "?") + "isAdmin=" + encodeURIComponent(name);
      return fetch(url, { method: "GET", cache: "no-store", headers: { Accept: "application/json" }, signal: ac ? ac.signal : undefined })
        .then(r => r.json().then(j => ({ s: r.status, j }), () => ({ s: r.status, j: null })))
        .then(o => { clearTimeout(to); if (o.s === 200 && o.j && typeof o.j.admin === "boolean") return o.j.admin; return o.s >= 500 || o.s === 429 ? null : undefined; })
        .catch(() => { clearTimeout(to); return null; });
    } catch (_) { return Promise.resolve(null); }
  }
  /** Promise → true | false | null. null = not known (offline, no answer): the person is treated as not Admin. */
  function isAdmin(name) {
    const n = cleanName(name); if (!n) return Promise.resolve(null);
    const k = adminKey(n), today = nyDay(), now = Date.now();
    let e = admins.get(k);
    if (e && e.day !== today) { admins.delete(k); e = null; }
    if (e && typeof e.v === "boolean") return Promise.resolve(e.v);
    if (e && e.p) return e.p;
    if (!e) { e = { v: null, day: today, p: null, tries: 0, at: 0 }; admins.set(k, e); }
    if (e.tries >= 6 || now - e.at < 40000) return Promise.resolve(null);     // a few tries a sign-in, a little apart
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
  function askAdmin(s) {
    if (!s || knownAdmin(s) !== null) return;
    isAdmin(s.name).then(v => { if (typeof v === "boolean" && !s.ended) { s.adm = v; if (s === cur) saveRec(s); } }, () => {});
  }

  /** the sessions this page runs now: one (a single-person page), or one per person and task (a page in multi mode) */
  function liveSessions() { return cur ? [cur] : []; }
  /** which rule, if any, a person whose last input was at L is under at `now`: Rule B (17:00 Toronto, no input in the 10 minutes before
      it) before Rule A (10 minutes of nothing). Either way the session ends AT THE LAST INPUT. */
  function dueRule(L, now) {
    L = Math.min(L, now);
    if (L <= closingAt(now) - IDLE_MS) return { reason: "closing", at: L };
    if (now - L >= IDLE_MS) return { reason: "idle", at: L };
    return null;
  }
  /** signs out everybody who is due (not an Admin; a day that turned is the midnight rule's) */
  function checkRules(now) {
    if (!ready) return;
    now = now || Date.now();
    const today = nyDay(now);
    for (const s of liveSessions().slice()) {
      if (!s || s.ended || s.day !== today || knownAdmin(s) === true) continue;
      const d = dueRule(inputOf(s), now);
      if (d) lapse(s, d.reason, d.at);
    }
  }
  /** ends a session by Rule A or B at the time of the last input and lets the page sign the person out (its work stays on screen) */
  function lapse(s, reason, at) {
    const p = person();
    finish(s, reason, at);
    if (s === cur) cur = null;
    quiet = p && p.name === s.name ? p.name : quiet; seen = "";          // a page that could not clear its login does not start them again
    try { if (cfg.signOut) cfg.signOut(reason, { name: s.name, task: s.task }); } catch (e) { warn("signOut failed:", e); }
  }
  function rulesTick() {
    try {
      if (!ready) return;
      checkRules(Date.now());
      for (const s of liveSessions()) if (knownAdmin(s) === null) askAdmin(s);        // an answer that has not come yet is asked again, a little apart
    } catch (e) { warn("rulesTick:", e); }
  }

  /* ── start · beat · end ── */
  function begin(p, resume) {
    const today = nyDay(), now = Date.now(), r = loadRec();
    if (r && !r.ended) {
      if (resume && r.name === p.name && r.day === today) {
        // this login was kept while the page was closed: when its last input is 10 minutes old it lapsed then (a reload inside
        // the 10 minutes is input and goes on with it)
        const li = Number(r.li) || 0;
        if (li && r.adm !== true && now - li >= IDLE_MS) {
          const d = dueRule(li, now) || { reason: "idle", at: li };
          lapse(r, d.reason, d.at);
          return;
        }
        if (now - (r.lastBeat || 0) < CLOSED_MS) { cur = r; mark(now); cur.li = lastInputAt; beat(); askAdmin(cur); return; }
      }
      finish(r, r.name === p.name ? "signOut" : "switched");
    }
    mark(now);
    cur = { id: `${cfg.device}-${shortId()}-${now.toString(36)}-${rand(4)}`.replace(/[^\w.:-]/g, "_").slice(0, 100),
      name: p.name, eid: p.id || "", day: today, startAt: now, lastBeat: now, li: now };
    saveRec(cur); savedLi = now;
    post(body(cur, "start"));
    askAdmin(cur);
  }
  function beat() {
    if (!cur) return;
    cur.lastBeat = Date.now(); cur.li = Math.max(Number(cur.li) || 0, lastInputAt); savedLi = cur.li; saveRec(cur);
    post(body(cur, "beat"));
  }
  /** ends a session. With an `at` (Rule A or B) it ends at that time, the last input. Otherwise: one that went quiet for 15
      minutes ended ("closed") at the last input it knew of (an Admin's at its last beat); one from an earlier day at its
      midnight; any other now, with the reason given */
  function finish(s, reason, at) {
    if (!s || s.ended) return;
    const now = Date.now(), quietFor = now - (s.lastBeat || s.startAt || 0);
    if (s === cur) s.li = Math.max(Number(s.li) || 0, lastInputAt);
    if (at != null && Number.isFinite(at)) at = Math.max(Number(s.startAt) || 0, Math.min(now, at));
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
    saveRec(s);
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
      reconcile();
      checkRules(Date.now());                    // (a page that slept: the person whose last input is 10 minutes old signs out here, at that input)
      // the page slept or was frozen for 15 minutes: that session closed at its last beat, a new one goes on from now
      if (cur && Date.now() - (cur.lastBeat || 0) >= CLOSED_MS) {
        const p = person(); finish(cur, "closed");
        if (p && p.name !== quiet) begin(p, false);
      }
      if (cur && Date.now() - (cur.lastBeat || 0) >= BEAT_MS) beat();
      else if (cur && lastInputAt > savedLi) { cur.li = savedLi = lastInputAt; saveRec(cur); }     // the clock a reload goes on from
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
      cfg.signOut = typeof o.signOut === "function" ? o.signOut : null;
      cfg.labelHost = o.labelHost || null; cfg.labelCss = String(o.labelCss || "");
      cfg.sandbox = o.sandbox != null ? !!o.sandbox : /[?&]sandbox=1\b/.test(location.search);
      computerId();
      ready = true;
      mark(Date.now());                          // a load (a reload too) is input
      // loaded after the day turned: sign out before anything else. (A login this module has never seen, with no day
      // stored on this computer, e.g. the first load after it was added, counts as today's and ends at the next midnight.)
      const today = nyDay(), p = person(), day = lsGet(K.day), d = p ? dayOf(p.name) : "";
      if ((day && day !== today) || (d && d !== today)) midnight();
      else if (p) { if (!d) markDay(p.name, today); seen = p.name; begin(p, true); }
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
      window.addEventListener("pagehide", () => { try { if (cur) beat(); } catch (_) {} });
      if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", mountLabel); else mountLabel();
      setTimeout(flush, 3000);
    } catch (e) { warn("init failed:", e); }
  }
  function signedIn(who) {
    try {
      if (!ready) return;
      const name = cleanName(who && who.name);
      if (!name) return;
      const today = nyDay();
      markDay(name, today); seen = name; quiet = "";
      if (cur && cur.name === name && cur.day === today) return;
      if (cur) finish(cur, "switched");
      begin({ name, id: safeId(who && who.id) }, false);
    } catch (e) { warn("signedIn:", e); }
  }
  function signedOut(reason) {
    try {
      if (!ready) return;
      const p = person();
      end(REASONS.has(reason) ? reason : "signOut");
      seen = ""; quiet = p ? p.name : "";
    } catch (e) { warn("signedOut:", e); }
  }

  window.StationSession = {
    init, signedIn, signedOut,
    // for the pages and the tests: read-only views
    computerId: () => { try { return computerId(); } catch (_) { return ""; } },
    computerLabel: () => { try { return label(); } catch (_) { return ""; } },
    current: () => (cur ? { id: cur.id, person: cur.name, startAt: cur.startAt, day: cur.day } : null),
    /** who is working now, for station-activity.js: { person, station, device, computer, session, startAt, sandbox }, or null
        when nobody is signed in (or this page's session is not running). The name only, never a PIN. */
    who: () => {
      try {
        if (!ready || !cur) return null;
        const p = person();
        if (!p || p.name !== cur.name) return null;
        return { person: p.name, station: cfg.station, device: cfg.device, computer: computerId(), session: cur.id, startAt: cur.startAt, sandbox: !!cfg.sandbox };
      } catch (_) { return null; }
    },
    /** this page (known even when nobody is signed in): { station, device, computer, sandbox }, or null before init */
    page: () => {
      try { return ready ? { station: cfg.station, device: cfg.device, computer: computerId(), sandbox: !!cfg.sandbox } : null; } catch (_) { return null; }
    },
    nyDay, nextMidnight,
    /** auto sign-out (see the top of this file) */
    touch,                                                              // record input (the scanner relay calls it for every scan)
    lastInput: () => lastInputAt,                                       // ms of the last input at this page
    isAdmin,                                                            // Promise: true | false | null (not known: treated as not Admin)
    notice: (reason, tail) => { const t = NOTICES[reason]; return t ? (tail ? t + " " + String(tail) : t) : ""; },
    idleMs: IDLE_MS, closingAt                                         // the 10 minutes; the latest 17:00 in Toronto at or before a time
  };
})();
