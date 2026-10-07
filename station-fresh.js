/* station-fresh.js: a station page that runs an older deploy reloads itself ONCE, at a quiet moment, the next time somebody uses it
 * (Paul, 7 Oct 2026: "Some users may not have refreshed their app, please ensure that there's a one time back and refresh the next
 * time a person interacts with a given station to assure the newest version is running").
 *
 *   StationFresh.hold(tag?)  -> release()   call around an action that must not be cut off by a reload (a print, a label bought, a scan
 *                                           being processed); the returned function (or StationFresh.release(tag?)) ends it. A hold
 *                                           that is never released lapses by itself after 5 minutes.
 *   StationFresh.touch()                    a scan arrived (the phone scanner, the relay): counts as somebody using the station
 *   StationFresh.check()  -> Promise        one check now (the tests; the page never needs it)
 *   StationFresh.status()                   read only: { baseline, pending, lastCheckAt, holds, writes, ... } (no text, no names)
 *
 * HOW A STALE PAGE IS TOLD, static files only: the station's own page is asked for again (the same address, no query, the browser's
 * conditional request: a 304 on Netlify's CDN when nothing changed, the new page when it did) and compared with what is running.
 *   1. the ?v= tokens of its <script src> and <link href> tags against the tags this page really loaded (the repository's own
 *      convention: every changed station script gets a new token, so a new deploy changes the page);
 *   2. a hash of the page text against the text this page was loaded with (the first check after load is the baseline).
 * No Firestore, no Etsy, no function call, no timer of its own: a check happens (a) when the page loads, (b) when the tab is shown
 * again (visibilitychange / focus / pageshow, at least 15 s apart), (c) on the first interaction after 3 minutes without one. Never
 * while the tab is hidden or the computer is offline; a failed check changes nothing. It runs async: an interaction never waits.
 *
 * WHEN IT RELOADS (all of it, or nothing): only after a person used the station (pointerdown or keydown they made, or touch()), then
 * 1.5 s with no key or tap (phone scanner pages: 4 s, so the code is out of view), and: the tab is visible and online; no hold(); no
 * request to /.netlify/functions in flight (a print, a label, an order load, a sign-in; each counted for at most 20 s); no dialog open
 * (dialog[open], role=dialog/alertdialog, aria-modal, Materialize .modal, SweetAlert); no busy marker (aria-busy, spinner); no text
 * typed in the focused field and not yet sent (Enter / change / submit); no new frame in the last 25 s (the hidden frames the print
 * helpers use); no print dialog (beforeprint .. afterprint); no OAuth return in progress. The moment is looked for once a second for
 * 2 minutes after the last use, then the page waits for the next one. Then ONE request (cache: 'reload') brings the newest page into
 * the browser's cache, the moment is checked again, the build is written to sessionStorage and location.reload() runs. A build a
 * tab already reloaded for is never reloaded for again (so no loop, even if the CDN is slow), and nothing reloads at all if
 * sessionStorage cannot hold that note. After the reload one small note says "Updated to the newest version" for 3 s.
 * A page inside a frame (the Charm Sorter's Design Station), an automated browser (navigator.webdriver, unless
 * localStorage.stationFreshForce = "1") and anything but http(s) are left alone; the API above then does nothing.
 * The login (employee name and id) is kept by the pages in localStorage, so a reload does not sign anybody out. */
(function (root) {
  "use strict";
  if (root.StationFresh) return;

  var cfg = {
    quietMs: 1500, armMs: 120000, tickMs: 1000, checkGapMs: 180000, returnGapMs: 15000, timeoutMs: 10000,
    holdMaxMs: 300000, writeMaxMs: 20000, frameHoldMs: 25000, noteMs: 3000, note: "Updated to the newest version"
  };
  var KEY = "stationFresh.v1:", NOTE_KEY = "stationFresh.v1.note";
  var DIALOGS = 'dialog[open],[role="dialog"],[role="alertdialog"],[aria-modal="true"],.modal,.modal-overlay,.swal2-container';
  var BUSY = '[aria-busy="true"],.cc-spinner,.spinner,.preloader-wrapper.active';

  /* ───────────── pure decisions (the test calls these through StationFresh._t) ───────────── */

  /** a short fingerprint of a page text: length and FNV-1a */
  function hashText(s) {
    s = String(s == null ? "" : s);
    var h = 0x811c9dc5;
    for (var i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; }
    return s.length.toString(36) + "." + h.toString(16);
  }
  function splitVersion(u) {
    var m = /^([^?#]*)\?(?:[^#]*&)?v=([^&#]*)/.exec(String(u || ""));
    return m ? [m[1], m[2]] : null;
  }
  function addToken(out, u) {
    var pv = splitVersion(u);
    if (pv) (out[pv[0]] = out[pv[0]] || []).push(pv[1]);
  }
  /** the ?v= tokens of the <script src> / <link href> tags in a page text: { path: [token, …] } */
  function pageTokens(text) {
    var out = {}, re = /<(?:script|link)\b[^>]*?\b(?:src|href)\s*=\s*(?:"([^"]*)"|'([^']*)')/gi, m;
    text = String(text == null ? "" : text);
    while ((m = re.exec(text))) addToken(out, m[1] != null ? m[1] : m[2]);
    return out;
  }
  function fingerprint(text) { return { hash: hashText(text), tokens: pageTokens(text) }; }
  /** the same map for the tags the running page really has */
  function runningTokens(doc) {
    var out = {};
    try {
      var els = doc.querySelectorAll("script[src],link[href]");
      for (var i = 0; i < els.length; i++) addToken(out, els[i].getAttribute("src") || els[i].getAttribute("href"));
    } catch (_) {}
    return out;
  }
  /** stale: the page text differs from the one this page was loaded with, or a tag it loaded has a token the new page no longer has */
  function isStale(fresh, baseline, running) {
    if (!fresh || !fresh.hash) return false;
    if (baseline && baseline.hash && fresh.hash !== baseline.hash) return true;
    var ft = fresh.tokens || {}, rt = running || {};
    for (var p in ft) {
      if (!Object.prototype.hasOwnProperty.call(ft, p) || !rt[p] || !rt[p].length) continue;
      var shared = false;
      for (var i = 0; i < ft[p].length; i++) if (rt[p].indexOf(ft[p][i]) >= 0) shared = true;
      if (!shared) return true;
    }
    return false;
  }
  /** "" when this is a safe moment to reload, else the reason it is not. s: { now, lastInputAt, quietMs, armMs, visible, online, oauth,
      holds, writes, frameUntil, dirty, dialog, busy } */
  function quietReason(s) {
    if (!s.visible) return "hidden";
    if (!s.online) return "offline";
    if (!(s.lastInputAt > 0)) return "nobody used it";
    var since = s.now - s.lastInputAt;
    if (since > s.armMs) return "idle";
    if (since < s.quietMs) return "typing";
    if (s.oauth) return "oauth";
    if (s.holds > 0) return "hold";
    if (s.writes > 0) return "request";
    if (s.frameUntil > s.now) return "frame";
    if (s.dirty) return "unsent text";
    if (s.dialog) return "dialog";
    if (s.busy) return "busy";
    return "";
  }
  /** once per build: false when this tab already reloaded for that build (or there is no build to reload for) */
  function worthReload(fresh, remembered) {
    return !!(fresh && fresh.hash && fresh.hash !== remembered);
  }
  /** holds with a time limit, counted by a plain list so a forgotten release cannot block for ever */
  function createHolds(maxMs) {
    var list = [], seq = 0;
    function prune(now) { list = list.filter(function (h) { return now - h.at < maxMs; }); }
    return {
      add: function (tag, now) { var id = ++seq; list.push({ id: id, tag: String(tag || ""), at: now }); return id; },
      drop: function (idOrTag) {
        for (var i = list.length - 1; i >= 0; i--) {
          if (idOrTag == null || list[i].id === idOrTag || list[i].tag === String(idOrTag)) { list.splice(i, 1); return true; }
        }
        return false;
      },
      count: function (now) { prune(now); return list.length; }
    };
  }

  /* ───────────── the page ───────────── */

  var hooks = {
    now: function () { return Date.now(); },
    reload: function () { root.location.reload(); },
    fetch: function (url, init) { return (hooks.rawFetch || root.fetch).call(root, url, init); },
    rawFetch: null
  };
  var holds = createHolds(cfg.holdMaxMs);
  var st = { baseline: null, pending: null, lastCheckAt: 0, checking: false, reloading: false, lastInputAt: 0, timer: 0,
             dirty: null, frames: 0, frameUntil: 0, writes: [], printRelease: null };

  function framed() { try { return root.top !== root.self; } catch (_) { return true; } }
  function inertReason() {
    try {
      if (!root.document || !root.location || !/^https?:$/.test(root.location.protocol)) return "not http";
      if (framed()) return "framed";
      if (root.navigator && root.navigator.webdriver) {
        var force = ""; try { force = root.localStorage.getItem("stationFreshForce") || ""; } catch (_) {}
        if (force !== "1") return "automated browser";
      }
      if (typeof root.fetch !== "function" || typeof root.Promise !== "function" || !Math.imul) return "old browser";
    } catch (_) { return "error"; }
    return "";
  }

  function pageUrl() { return root.location.origin + root.location.pathname; }
  function visible() { try { return root.document.visibilityState !== "hidden"; } catch (_) { return true; } }
  function online() { try { return root.navigator.onLine !== false; } catch (_) { return true; } }
  function oauthBusy() {
    try {
      var q = root.location.search || "";
      return (/[?&]code=/.test(q) && /[?&]state=/.test(q)) || !!root.localStorage.getItem("oauth_in_progress");
    } catch (_) { return false; }
  }
  function shown(el) {
    try {
      if (!el || el.hidden) return false;
      var cs = root.getComputedStyle(el);
      return cs.display !== "none" && cs.visibility !== "hidden" && el.getClientRects().length > 0;
    } catch (_) { return false; }
  }
  function anyShown(sel) {
    try {
      var list = root.document.querySelectorAll(sel);
      for (var i = 0; i < list.length; i++) if (shown(list[i])) return true;
    } catch (_) {}
    return false;
  }
  function frameCount() { try { return root.document.getElementsByTagName("iframe").length; } catch (_) { return 0; } }
  function sampleFrames(now) {
    var n = frameCount();
    if (n > st.frames) st.frameUntil = now + cfg.frameHoldMs;     // a frame appeared: the print helpers are hidden frames
    st.frames = n;
  }
  function textLike(el) {
    if (!el || !el.tagName) return false;
    var t = el.tagName;
    if (t === "TEXTAREA") return true;
    if (t === "INPUT") return /^(|text|search|tel|email|url|number|password)$/i.test(el.getAttribute("type") || "");
    return !!el.isContentEditable;
  }
  function unsentText() {
    try {
      var d = st.dirty, a = root.document.activeElement;
      if (!d || d !== a || d.isConnected === false) return false;
      return String(d.isContentEditable ? d.textContent : d.value || "").trim() !== "";
    } catch (_) { return false; }
  }
  function snapshot() {
    var now = hooks.now();
    st.writes = st.writes.filter(function (t) { return now - t < cfg.writeMaxMs; });
    sampleFrames(now);
    return {
      now: now, lastInputAt: st.lastInputAt, quietMs: cfg.quietMs, armMs: cfg.armMs, visible: visible(), online: online(), oauth: oauthBusy(),
      holds: holds.count(now), writes: st.writes.length, frameUntil: st.frameUntil, dirty: unsentText(),
      dialog: anyShown(DIALOGS), busy: anyShown(BUSY)
    };
  }

  /* what this tab has already reloaded for (sessionStorage, one tab); false when it cannot be kept */
  function recall() { try { return root.sessionStorage.getItem(KEY + root.location.pathname) || ""; } catch (_) { return ""; } }
  function remember(hash) {
    try {
      root.sessionStorage.setItem(KEY + root.location.pathname, hash);
      return root.sessionStorage.getItem(KEY + root.location.pathname) === hash;
    } catch (_) { return false; }
  }

  /** the page's own text, or null (offline, an error, a timeout, not HTML). mode: "no-cache" (a check) | "reload" (before the reload) */
  function fetchPage(mode) {
    return new Promise(function (resolve) {
      var t = 0;
      function end(v) { try { clearTimeout(t); } catch (_) {} resolve(v); }
      try {
        var ctl = typeof root.AbortController === "function" ? new root.AbortController() : null;
        if (ctl) t = setTimeout(function () { try { ctl.abort(); } catch (_) {} }, cfg.timeoutMs);
        Promise.resolve(hooks.fetch(pageUrl(), { cache: mode, credentials: "same-origin", signal: ctl ? ctl.signal : undefined }))
          .then(function (r) {
            var ct = r && r.headers && r.headers.get ? (r.headers.get("content-type") || "") : "text/html";
            if (!r || !r.ok || !/html/i.test(ct)) return null;
            return r.text();
          })
          .then(function (text) { end(typeof text === "string" && text ? text : null); }, function () { end(null); });
      } catch (_) { end(null); }
    });
  }

  function schedule() {
    if (st.timer || st.reloading) return;
    st.timer = setTimeout(tick, cfg.tickMs);
  }
  function tick() {
    st.timer = 0;
    try {
      if (!st.pending || st.reloading) return;
      var s = snapshot(), why = quietReason(s);
      if (why === "idle" || why === "hidden" || why === "offline" || why === "nobody used it") return;   // the next interaction looks again
      if (why) { schedule(); return; }
      attempt();
    } catch (_) { st.reloading = false; }
  }
  function attempt() {
    st.reloading = true;
    fetchPage("reload").then(function (text) {
      if (text == null) { st.reloading = false; return; }                      // offline or an error: nothing happens
      var fresh = fingerprint(text);
      if (!isStale(fresh, st.baseline, runningTokens(root.document))) { st.pending = null; st.reloading = false; return; }
      if (!worthReload(fresh, recall())) { st.pending = null; st.reloading = false; return; }
      if (quietReason(snapshot())) { st.reloading = false; schedule(); return; }  // somebody touched the station meanwhile
      if (!remember(fresh.hash)) { st.pending = null; st.reloading = false; return; }   // no memory, no reload: never a loop
      try { root.sessionStorage.setItem(NOTE_KEY, "1"); } catch (_) {}
      hooks.reload();
    }).then(null, function () { st.reloading = false; });
  }

  function check() {
    if (st.checking || st.reloading || !visible() || !online()) return Promise.resolve(null);
    st.checking = true; st.lastCheckAt = hooks.now();
    return fetchPage("no-cache").then(function (text) {
      st.checking = false;
      if (text == null) return null;
      var fresh = fingerprint(text), stale = isStale(fresh, st.baseline, runningTokens(root.document));
      if (!st.baseline) st.baseline = fresh;
      st.pending = stale && worthReload(fresh, recall()) ? fresh : null;
      if (st.pending && st.lastInputAt > 0) schedule();
      return stale;
    }, function () { st.checking = false; return null; });
  }

  function onUse(ev) {
    try {
      if (ev && ev.isTrusted === false) return;                  // only what a person did (or touch())
      var now = hooks.now();
      st.lastInputAt = now;
      sampleFrames(now);
      if (!st.pending && now - st.lastCheckAt >= cfg.checkGapMs) check();
      if (st.pending) schedule();
    } catch (_) {}
  }
  function onKey(ev) {
    try { if (ev && ev.key === "Enter" && ev.target && ev.target.tagName === "INPUT" && st.dirty === ev.target) st.dirty = null; } catch (_) {}
    onUse(ev);
  }
  function onTyped(ev) { try { if (ev && ev.isTrusted !== false && textLike(ev.target)) st.dirty = ev.target; } catch (_) {} }
  function onSent(ev) { try { if (ev && st.dirty && (ev.target === st.dirty || (ev.target && ev.target.contains && ev.target.contains(st.dirty)))) st.dirty = null; } catch (_) {} }
  function onReturn() {
    try { if (visible() && hooks.now() - st.lastCheckAt >= cfg.returnGapMs) check(); } catch (_) {}
  }

  /** every call to a Netlify function counts as a request in flight, for at most 20 s (a long hold must not block a reload for ever) */
  function trackRequests() {
    var raw = root.fetch;
    hooks.rawFetch = raw;
    root.fetch = function (input) {
      var p = raw.apply(this, arguments);
      try {
        var url = typeof input === "string" ? input : (input && input.url) || String(input || "");
        if (/\/\.netlify\/functions\//.test(url) && p && typeof p.then === "function") {
          var t = hooks.now(), done = function () { var i = st.writes.indexOf(t); if (i >= 0) st.writes.splice(i, 1); };
          st.writes.push(t); p.then(done, done);
        }
      } catch (_) {}
      return p;
    };
  }

  function showNote() {
    try {
      var d = root.document;
      if (!d.body) { d.addEventListener("DOMContentLoaded", showNote, { once: true }); return; }
      var n = d.createElement("div");
      n.id = "stationFreshNote"; n.setAttribute("role", "status"); n.setAttribute("aria-live", "polite"); n.textContent = cfg.note;
      var css = { position: "fixed", left: "50%", bottom: "46px", transform: "translateX(-50%)", zIndex: "10049", maxWidth: "calc(100vw - 24px)",
        padding: "6px 14px", border: "1px solid #cbd5e1", borderRadius: "999px", background: "#f8fafc", color: "#334155",
        font: "13px/1.4 -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif", textAlign: "center", pointerEvents: "none",
        userSelect: "none", webkitUserSelect: "none", boxShadow: "0 1px 4px rgba(0,0,0,.12)", opacity: "1", transition: "opacity .4s" };
      for (var k in css) n.style[k] = css[k];
      d.body.appendChild(n);
      setTimeout(function () { n.style.opacity = "0"; }, Math.max(0, cfg.noteMs - 400));
      setTimeout(function () { try { n.remove(); } catch (_) {} }, cfg.noteMs + 200);
    } catch (_) {}
  }

  /* ───────────── the API ───────────── */

  function hold(tag) {
    var id = holds.add(tag, hooks.now()), done = false;
    return function () { if (!done) { done = true; holds.drop(id); } };
  }
  var api = {
    v: 1,
    hold: hold,
    release: function (tag) { return holds.drop(tag == null ? null : tag); },
    touch: function () { onUse({ type: "touch" }); },
    check: check,
    status: function () {
      return { baseline: st.baseline && st.baseline.hash, pending: st.pending && st.pending.hash, lastCheckAt: st.lastCheckAt,
               holds: holds.count(hooks.now()), writes: st.writes.length, lastInputAt: st.lastInputAt, reloading: st.reloading };
    },
    _t: { cfg: cfg, hooks: hooks, st: st, hashText: hashText, pageTokens: pageTokens, fingerprint: fingerprint, runningTokens: runningTokens,
          isStale: isStale, quietReason: quietReason, worthReload: worthReload, createHolds: createHolds, snapshot: snapshot, onUse: onUse,
          onKey: onKey, onTyped: onTyped, onSent: onSent, remember: remember, recall: recall, tick: tick }
  };

  var why = inertReason();
  if (why) {                                                    // the API stays, doing nothing: pages may call it without asking
    api = { v: 1, inert: why, hold: function () { return function () {}; }, release: function () { return false; }, touch: function () {},
            check: function () { return Promise.resolve(null); }, status: function () { return { inert: why }; }, _t: api._t };
  } else {
    try {
      var tag = root.document.currentScript, q = tag && Number(tag.getAttribute("data-quiet-ms"));
      if (q >= 1000 && q <= 10000) cfg.quietMs = q;             // the phone scanner pages say 4000
    } catch (_) {}
    trackRequests();
    st.frames = frameCount();
    var d = root.document;
    d.addEventListener("pointerdown", onUse, true);
    d.addEventListener("keydown", onKey, true);
    d.addEventListener("input", onTyped, true);
    d.addEventListener("change", onSent, true);
    d.addEventListener("submit", onSent, true);
    d.addEventListener("visibilitychange", onReturn);
    root.addEventListener("focus", onReturn);
    root.addEventListener("pageshow", function (e) { if (e && e.persisted) { st.checking = false; onReturn(); } });
    root.addEventListener("beforeprint", function () { if (st.printRelease) st.printRelease(); st.printRelease = hold("print"); });
    root.addEventListener("afterprint", function () { if (st.printRelease) { st.printRelease(); st.printRelease = null; } });
    try { if (root.sessionStorage.getItem(NOTE_KEY)) { root.sessionStorage.removeItem(NOTE_KEY); showNote(); } } catch (_) {}
    check();                                                    // the baseline
  }
  root.StationFresh = api;
  if (typeof module === "object" && module && module.exports) module.exports = api;
})(typeof window !== "undefined" ? window : this);
