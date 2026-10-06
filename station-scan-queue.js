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
 *   q.offer(order, relay?) → true when kept (false: not a usable order number, the queue is full, or at the Welding station a duplicate).
 *                    Starts the drain. `relay` (optional) is the relay document's data as the phone page wrote it, or { at, id }: the real
 *                    scan time and the scan's id (see relayInfo). run(order, item) gets item = { n, at, s?, m? }: the order, the real scan
 *                    time, the phone's scan id, and m = 1 once the scan was recorded as `matched`.
 *   q.drain()       → runs the waiting scans, one at a time, oldest first, while somebody is signed in. Safe to call any time.
 *   q.count() / q.pending()   (read only)
 *
 * The Welding station (stations round 2, plans/stations-round2/api.md): its scanner app is used while a person matches welded stud earrings
 * to their orders, so EVERY scan that reaches the page is one `matched` activity event (StationActivity.matched: the order, the real scan
 * time, the person credited by rule R3, or `unattributed` when nobody was signed in as Matching). The queue does it, once per scan: when
 * the scan arrives and somebody is signed in, otherwise when the drain starts after the next sign-in. Either way the credit is the Matching
 * person who was signed in AT THE SCAN'S REAL TIME (StationSession.whoAt): a scan made while nobody was in Matching is stored unattributed
 * even when somebody signs in before it is recorded, and an offline phone's late scan is never credited to whoever is in when it
 * arrives (ST2, 6 Oct 2026; the order itself still loads under whoever is signed in when it runs). It is on for a page whose
 * StationSession station is "welding" (or create({ matched: true | false }) says so). The same order twice within 10 s, or the same
 * scan id twice, is one scan. A scan arriving is also input at the station (StationSession.touch()): it keeps the signed-in people awake.
 *
 * Rules: a scan leaves the list the moment it starts (so it can never run twice, not even after a reload mid-load), and it is
 * kept in sessionStorage until then (one tab; the order numbers only, never a person, a PIN or a passcode). At most 100 wait;
 * the oldest are kept and the note says so when more were scanned. No network, no Firestore, no Etsy: that stays with `run`.
 * Never breaks the page: every entry point is wrapped. The person is never stored here: the page's own activity and timeline code
 * credits whoever is signed in when `run` loads the order, and the matched event takes its person from StationSession.whoAt(scan time). */
(function () {
  "use strict";
  if (window.StationScanQueue) return;
  var MAX = 100, RUN_MAX_MS = 90000, KEY = "stationScanQueue.v1.", DUP_MS = 10000;

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

  /** What a phone scan says about itself, from the relay document the scanner app wrote (or { at, id }): { at, id }.
      The phone writes the real scan time as an ISO string in "Shipping Label Timestamps" and a small JSON descriptor in "Staff Note":
      { v:1, id: scan id, at: scan time ms, sent: the moment it was pushed }. A phone clock that is minutes off is corrected by the
      difference between `sent` and this computer's clock at arrival, so a scan that waited on the phone (offline) keeps its real time.
      Without a descriptor (an older phone page) the time is the arrival time unless the phone's own time is within 2 minutes of it.
      Never in the future. `now` is for the tests. */
  function relayInfo(meta, now) {
    now = now > 0 ? now : Date.now();
    var out = { at: now, id: "" };
    try {
      if (!meta || typeof meta !== "object") return out;
      var d = null;
      if (meta["Staff Note"] != null) { try { d = JSON.parse(String(meta["Staff Note"])); } catch (_) { d = null; } }
      d = d && typeof d === "object" ? d : {};
      var iso = Date.parse(String(meta["Shipping Label Timestamps"] || ""));
      var at = Number(d.at) > 1e12 ? Number(d.at) : (Number(meta.at) > 1e12 ? Number(meta.at) : (iso > 1e12 ? iso : 0));
      var sent = Number(d.sent) > 1e12 ? Number(d.sent) : 0;
      var id = String(d.id != null ? d.id : (meta.id != null ? meta.id : "")).replace(/[^\w.:-]/g, "").slice(0, 24);
      if (at) {
        if (sent) out.at = Math.abs(now - sent) < 30 * 60000 ? at + (now - sent) : at;
        else out.at = now - at >= 0 && now - at < 120000 ? at : now;
      }
      out.at = Math.round(Math.max(now - 7 * 86400000 + 60000, Math.min(now, out.at)));
      out.id = id;
    } catch (_) {}
    return out;
  }

  function create(o) {
    o = o || {};
    var key = KEY + (clean(o.device).replace(/[^\w.-]/g, "") || "page");
    var signedIn = typeof o.signedIn === "function" ? o.signedIn : function () { return false; };
    var run = typeof o.run === "function" ? o.run : function () {};
    var hold = typeof o.hold === "function" ? o.hold : null;
    var max = o.max > 0 ? Math.floor(o.max) : MAX;
    var list = [], extra = 0, running = false, current = Promise.resolve(), box = null;
    var seenAt = {}, seenIds = {};                  // order -> real time of its newest scan, scan id -> true (this page load; the matched event keeps its own persistent memory)

    /* the Welding station records every scan as a `matched` event: on for the page whose station is "welding" */
    function matchedOn() {
      try {
        if (o.matched === true) return !!(window.StationActivity && StationActivity.matched);
        if (o.matched === false) return false;
        var p = window.StationSession && typeof StationSession.page === "function" ? StationSession.page() : null;
        return !!(p && p.station === "welding" && window.StationActivity && typeof StationActivity.matched === "function");
      } catch (_) { return false; }
    }
    function record(it) {                             // once per scan: the credit is who StationSession.whoAt() says was signed in at the scan's real time (never the person who signs in later)
      if (!it || it.m) return;
      it.m = 1;
      try { StationActivity.matched({ orderId: it.n, at: it.at, scanId: it.s, detail: "phone scan" }); } catch (_) {}
    }
    function touchStation() {                         // a scan from the scanner app is input at the station: it keeps the signed-in people awake
      try {
        var S = window.StationSession;
        if (S && typeof S.touch === "function") S.touch();
      } catch (_) {}
    }
    function isDuplicate(n, at, id) {
      if (id && seenIds[id]) return true;
      var t = seenAt[n];
      if (t != null && Math.abs(at - t) < DUP_MS) return true;
      for (var i = 0; i < list.length; i++) if (list[i].n === n && Math.abs(list[i].at - at) < DUP_MS) return true;
      return false;
    }

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
          if (n && list.length < max) {
            var r = { n: n, at: Number(it && it.at) > 0 ? Number(it.at) : Date.now() };
            var sid = String(it && it.s != null ? it.s : "").replace(/[^\w.:-]/g, "").slice(0, 24);
            if (sid) r.s = sid;
            if (it && it.m === 1) r.m = 1;
            list.push(r);
          }
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

    function offer(order, relay) {
      try {
        var n = clean(order);
        if (!n) return false;
        var on = matchedOn(), now = Date.now(), info = relay ? relayInfo(relay, now) : { at: now, id: "" };
        if (on && isDuplicate(n, info.at, info.id)) return false;        // the same scan told twice, or the same order within seconds: one scan
        touchStation();
        var it = { n: n, at: relay ? info.at : now };
        if (info.id) it.s = info.id;
        if (on) {
          if (Object.keys(seenAt).length > 500) seenAt = {}; if (Object.keys(seenIds).length > 500) seenIds = {};
          seenAt[n] = it.at; if (info.id) seenIds[info.id] = true;
        }
        if (list.length >= max) {                                        // (the scan cannot wait for a load, but its record is never dropped)
          extra++; if (on) record(it); render(); return false;
        }
        if (on && signedIn()) record(it);                                // somebody is signed in: the people here NOW are credited
        list.push(it);
        save(); render();
        drain();
        return true;
      } catch (_) { return false; }
    }

    function drain() {
      try {
        if (running) return current;                 // a drain is already running: it takes whatever has arrived
        if (!list.length || !signedIn()) { render(); return current; }
        if (matchedOn()) { list.forEach(record); save(); }              // the replay after a sign-in: whoever is signed in now is credited, with each scan's real time
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
          if (!it.m && matchedOn()) record(it);
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

  window.StationScanQueue = { create: create, relayInfo: relayInfo, v: 1 };
})();
