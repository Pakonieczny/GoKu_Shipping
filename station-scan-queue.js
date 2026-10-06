/* station-scan-queue.js: phone scans that reach a station's desktop page while nobody is signed in are KEPT, shown as one small
 * note, and run in the order they arrived right after the next sign-in (under the person who signs in). Used by weld-1 and
 * assembly-1..4 (the pages whose phone scanner relays through Firestore Brites_Orders/<station>-scan-N).
 *
 *   const q = StationScanQueue.create({
 *     device:   "weld-1",                 // names the sessionStorage key; one queue per page
 *     signedIn: () => window.isEmployeeLoggedIn === true,
 *     run:      order => Promise|any,     // load the order exactly as a typed one; resolve when this page has finished with it
 *     hold:     fn => boolean             // optional: true when the red CANCELLED alert is up and fn (the drain) was parked until Understood
 *   });
 *   q.offer(order)  → true when kept (false: not a usable order number, or the queue is full). Starts the drain.
 *   q.drain()       → runs the waiting scans, one at a time, oldest first, while somebody is signed in. Safe to call any time.
 *   q.count() / q.pending()   (read only)
 *
 * Rules: a scan leaves the list the moment it starts (so it can never run twice, not even after a reload mid-load), and it is
 * kept in sessionStorage until then (one tab; the order numbers only, never a person, a PIN or a passcode). At most 100 wait;
 * the oldest are kept and the note says so when more were scanned. No network, no Firestore, no Etsy: that stays with `run`.
 * Every offered scan also counts as input for the auto sign-out (StationSession.touch: the time only, never the order).
 * Never breaks the page: every entry point is wrapped. The person is not stored: the page's own activity and timeline code
 * credits whoever is signed in when `run` loads the order. */
(function () {
  "use strict";
  if (window.StationScanQueue) return;
  var MAX = 100, RUN_MAX_MS = 90000, KEY = "stationScanQueue.v1.";

  function clean(v) {
    try {
      var s = String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, "").trim();
      return s.slice(0, 64);
    } catch (_) { return ""; }
  }
  function limit(p, ms) {          // wait for p, but never longer than ms (a hung load must not hold the rest forever)
    return new Promise(function (done) {
      var t = setTimeout(done, ms), fin = function () { clearTimeout(t); done(); };
      try { Promise.resolve(p).then(fin, fin); } catch (_) { fin(); }
    });
  }

  function create(o) {
    o = o || {};
    var key = KEY + (clean(o.device).replace(/[^\w.-]/g, "") || "page");
    var signedIn = typeof o.signedIn === "function" ? o.signedIn : function () { return false; };
    var run = typeof o.run === "function" ? o.run : function () {};
    var hold = typeof o.hold === "function" ? o.hold : null;
    var max = o.max > 0 ? Math.floor(o.max) : MAX;
    var list = [], extra = 0, running = false, current = Promise.resolve(), box = null;

    function save() {
      try {
        if (list.length) sessionStorage.setItem(key, JSON.stringify({ v: 1, items: list }));
        else sessionStorage.removeItem(key);
      } catch (_) {}
    }
    function restore() {
      try {
        var raw = sessionStorage.getItem(key), d = raw ? JSON.parse(raw) : null;
        if (!d || d.v !== 1 || !Array.isArray(d.items)) return;
        d.items.forEach(function (it) {
          var n = clean(it && it.n);
          if (n && list.length < max) list.push({ n: n, at: Number(it && it.at) > 0 ? Number(it.at) : Date.now() });
        });
      } catch (_) {}
    }

    /* the one small note: calm, in the page's corner, never blocks a click or a key */
    function render() {
      try {
        var n = list.length;
        if (!n && !(box && box.style.display !== "none")) return;
        if (!document.body) { document.addEventListener("DOMContentLoaded", render, { once: true }); return; }
        if (!box) {
          box = document.createElement("div");
          box.id = "stationScanQueueNote";
          box.setAttribute("role", "status"); box.setAttribute("aria-live", "polite");
          box.style.cssText = "position:fixed;left:50%;bottom:14px;transform:translateX(-50%);z-index:10050;max-width:calc(100vw - 24px);" +
            "padding:6px 14px;border:1px solid #fcd34d;border-radius:999px;background:#fffbeb;color:#92400e;" +
            "font:13px/1.4 -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;text-align:center;" +
            "pointer-events:none;user-select:none;-webkit-user-select:none;box-shadow:0 1px 4px rgba(0,0,0,.12);display:none;";
          document.body.appendChild(box);
        }
        if (!n) { extra = 0; box.style.display = "none"; box.textContent = ""; box.removeAttribute("data-count"); return; }
        var s = n === 1 ? "" : "s", full = extra > 0 ? " (limit of " + max + " reached; later scans were not kept)" : "";
        box.textContent = signedIn()
          ? n + " more phone scan" + s + " waiting to load"
          : n + " phone scan" + s + " waiting for a sign-in" + full;
        box.setAttribute("data-count", String(n));
        box.style.display = "block";
      } catch (_) {}
    }

    function offer(order) {
      try {
        var n = clean(order);
        if (!n) return false;
        // a scan from the station's scanner app is input at this page (the auto sign-out's 10 minutes): the time only, never the order
        try { if (window.StationSession && StationSession.touch) StationSession.touch(); } catch (_) {}
        if (list.length >= max) { extra++; render(); return false; }
        list.push({ n: n, at: Date.now() });
        save(); render();
        drain();
        return true;
      } catch (_) { return false; }
    }

    function drain() {
      try {
        if (running) return current;                 // a drain is already running: it takes whatever has arrived
        if (!list.length || !signedIn()) { render(); return current; }
        running = true;
        current = loop();
        return current;
      } catch (_) { running = false; return current; }
    }
    async function loop() {
      try {
        while (list.length && signedIn()) {
          if (hold) { var held = false; try { held = !!hold(drain); } catch (_) {} if (held) break; }
          var it = list.shift(); save(); render();               // leaves the list before it starts: it can never run twice
          try { await limit(run(it.n, it), RUN_MAX_MS); } catch (_) {}
        }
      } catch (_) {}
      running = false; render();                     // same turn as the last check above: a scan cannot slip in between
    }

    restore();
    var api = {
      offer: offer, drain: drain,
      count: function () { return list.length; },
      pending: function () { return list.map(function (it) { return it.n; }); }
    };
    if (list.length) render();
    return api;
  }

  window.StationScanQueue = { create: create, v: 1 };
})();
