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
  const cfg = { station: "", device: "", person: null, signOut: null, labelHost: null, labelCss: "", sandbox: false };
  let ready = false, cur = null, seen = "", quiet = "", tickT = 0, midT = 0, memId = "", labelBox = null;

  const warn = (...a) => { try { console.warn("[StationSession]", ...a); } catch (_) {} };
  const lsGet = k => { try { return localStorage.getItem(k) || ""; } catch (_) { return ""; } };
  const lsSet = (k, v) => { try { localStorage.setItem(k, v); } catch (_) {} };
  const lsDel = k => { try { localStorage.removeItem(k); } catch (_) {} };
  const lsJson = (k, d) => { try { const v = JSON.parse(localStorage.getItem(k) || "null"); return v == null ? d : v; } catch (_) { return d; } };
  const clean = (v, n) => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, n);
  const safeId = v => { const s = clean(v, 60); return !s || /^\d+$/.test(s) ? "" : s; };   // a PIN (digits) is never kept or sent

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
      const name = p && clean(p.name, 80);
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
  function clearStaleDays(today) {
    if (lsGet(K.day) && lsGet(K.day) !== today) lsDel(K.day);
    const m = lsJson(K.days, {}); if (!m || typeof m !== "object") return;
    for (const k of Object.keys(m)) if (m[k] !== today) delete m[k];
    lsSet(K.days, JSON.stringify(m));
  }

  /* ── this page's session (kept so a reload within 15 minutes goes on with it) ── */
  const recKey = () => "station_session." + cfg.station + "." + cfg.device;
  function loadRec() { const r = lsJson(recKey(), null); return r && typeof r === "object" && typeof r.id === "string" ? r : null; }
  function saveRec(r) { try { lsSet(recKey(), JSON.stringify(r)); } catch (_) {} }

  /* ── the door ── */
  const URL_ = () => "/.netlify/functions/firebaseOrders" + (cfg.sandbox ? "?sandbox=1" : "");
  function body(s, event, reason, at) {
    return { id: s.id, event, person: s.name, employeeId: s.eid || "", station: cfg.station, device: cfg.device,
      computerId: computerId(), computerLabel: label(), at: at || Date.now(), reason: reason || undefined };
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
      if (resume && r.name === p.name && r.day === today && now - (r.lastBeat || 0) < CLOSED_MS) { cur = r; beat(); return; }
      finish(r, r.name === p.name ? "signOut" : "switched");
    }
    cur = { id: `${cfg.device}-${shortId()}-${now.toString(36)}-${rand(4)}`.replace(/[^\w.:-]/g, "_").slice(0, 100),
      name: p.name, eid: p.id || "", day: today, startAt: now, lastBeat: now };
    saveRec(cur);
    post(body(cur, "start"));
  }
  function beat() {
    if (!cur) return;
    cur.lastBeat = Date.now(); saveRec(cur);
    post(body(cur, "beat"));
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
    saveRec(s);
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
      // the page slept or was frozen for 15 minutes: that session closed at its last beat, a new one goes on from now
      if (cur && Date.now() - (cur.lastBeat || 0) >= CLOSED_MS) {
        const p = person(); finish(cur, "closed");
        if (p && p.name !== quiet) begin(p, false);
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
      // loaded after the day turned: sign out before anything else. (A login this module has never seen, with no day
      // stored on this computer, e.g. the first load after it was added, counts as today's and ends at the next midnight.)
      const today = nyDay(), p = person(), day = lsGet(K.day), d = p ? dayOf(p.name) : "";
      if ((day && day !== today) || (d && d !== today)) midnight();
      else if (p) { if (!d) markDay(p.name, today); seen = p.name; begin(p, true); }
      tickT = setInterval(tick, TICK_MS);
      armMidnight();
      document.addEventListener("visibilitychange", () => { if (document.visibilityState === "visible") wake(); });
      window.addEventListener("focus", wake);
      window.addEventListener("pageshow", wake);
      window.addEventListener("online", flush);
      window.addEventListener("storage", e => { if (!e.key || !/^station_session\./.test(e.key)) setTimeout(tick, 0); });
      window.addEventListener("pagehide", () => { try { if (cur) beat(); } catch (_) {} });
      if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", mountLabel); else mountLabel();
      setTimeout(flush, 3000);
    } catch (e) { warn("init failed:", e); }
  }
  function signedIn(who) {
    try {
      if (!ready) return;
      const name = clean(who && who.name, 80);
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
    nyDay, nextMidnight
  };
})();
