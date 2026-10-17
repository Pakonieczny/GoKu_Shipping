/* Background clock: the sorter keeps full speed while its tab is out of view.
   A browser slows the timers of a tab that is not shown to once a second, and after a few minutes to once a minute,
   and stops its animation frames altogether, so a run slowed to a crawl or stopped when Paul clicked away from the
   tab (24 Sep). A dedicated worker's timers are not slowed. Loaded as a page script, this file starts itself as such a
   worker and routes the page's timers through it: each timer is set in the worker, and the worker's message runs the
   callback on the page at once, shown or not. While the tab is hidden, animation-frame callbacks run on a 16 ms
   timer the same way, so a step that waits for a frame goes on. A lock held while the page is open asks the browser
   not to freeze the tab. If the worker cannot start, the page keeps the browser's own timers. The Design Station,
   framed by the sorter, loads it too (data-framed-only), since each order check waits on the station's timers.
   COST (Oct 2026): only while there is work that needs it. A page that says what that is (CharmNestClock.need(fn): the
   sorter's nests, intake, labelled work, runs and Auto mode) has the worker, the 16 ms frames and the lock only while
   fn() is true, asked every 5 s and when the tab is hidden; the rest of the time the browser's own timers run, and slow
   down in a hidden tab as on any page. A timer is moved between the two with what is left of its wait, under the id its
   owner holds, and runs once. A page that never calls need() (the framed Design Station) keeps the clock on, as before. */
(function () {
  "use strict";
  if (typeof window === "undefined") {
    // the worker: each timer posts its id back when due; an interval keeps posting until cancelled
    const timers = new Map();
    self.onmessage = e => {
      const { id, ms, repeat, cancel } = e.data;
      if (cancel) { const t = timers.get(id); if (t != null) { clearTimeout(t); clearInterval(t); timers.delete(id); } return; }
      if (repeat) timers.set(id, setInterval(() => self.postMessage(id), ms));
      else timers.set(id, setTimeout(() => { timers.delete(id); self.postMessage(id); }, ms));
    };
    return;
  }
  if (window.CharmNestClock) return;
  // the Design Station loads this only for the sorter's run: framed by the sorter it keeps pace, on its own it is unchanged
  if (document.currentScript && document.currentScript.hasAttribute("data-framed-only") && window.top === window) return;
  const src = document.currentScript && document.currentScript.src;
  let worker = null;
  try { worker = src ? new Worker(src) : null; } catch (e) { worker = null; }
  const native = {
    setTimeout: window.setTimeout.bind(window), clearTimeout: window.clearTimeout.bind(window),
    setInterval: window.setInterval.bind(window), clearInterval: window.clearInterval.bind(window),
    requestAnimationFrame: window.requestAnimationFrame ? window.requestAnimationFrame.bind(window) : null,
    cancelAnimationFrame: window.cancelAnimationFrame ? window.cancelAnimationFrame.bind(window) : null
  };
  const clock = window.CharmNestClock = { native, active: !!worker, need() {}, on: () => false };
  if (!worker) return;
  let on = true;                 // timers in the worker, hidden frames on the 16 ms timer; off: the browser's own timers and frames
  // ids far above the browser's own, so a timer set before this file ran is still cleared by the browser
  let next = 0x40000000, nextPlace = 0;
  const pending = new Map();     // id -> { fn, args, repeat, ms, due, place, wid, nid, first }
  const byWid = new Map();       // the worker's id of the current placement -> the id handed out
  const fail = e => native.setTimeout(() => { throw e; }, 0);
  /** Runs a timer, when `where` is still its place (a timer cleared, or moved after this call was on its way, does not run here). */
  function fire(id, where) {
    const t = pending.get(id);
    if (!t || t.place !== where) return;
    if (t.repeat) t.due = performance.now() + t.ms; else { pending.delete(id); if (t.wid != null) byWid.delete(t.wid); }
    try { t.fn.apply(window, t.args); } catch (err) { fail(err); }
  }
  /** Puts a timer where the clock is now, `wait` ms from now. An interval moved part way through its period waits what was
      left of it first (`first`), then keeps its period. */
  function place(id, t, wait) {
    t.first = t.repeat && wait < t.ms;
    if (on && worker) {
      const wid = ++nextPlace; t.wid = wid; t.nid = null; t.place = "w" + wid; byWid.set(wid, id);
      worker.postMessage({ id: wid, ms: wait, repeat: t.repeat && !t.first });
    } else {
      const where = "n" + (++nextPlace); t.wid = null; t.place = where;
      if (t.first) t.nid = native.setTimeout(() => { if (pending.get(id) !== t || t.place !== where) return; t.first = false; t.nid = native.setInterval(() => fire(id, where), t.ms); fire(id, where); }, wait);
      else t.nid = t.repeat ? native.setInterval(() => fire(id, where), t.ms) : native.setTimeout(() => fire(id, where), wait);
    }
  }
  /** Takes a timer off its place: a worker message or a browser timer of that place that still comes finds nothing to run. */
  function unplace(t) {
    if (t.wid != null) { byWid.delete(t.wid); if (worker) worker.postMessage({ id: t.wid, cancel: true }); }
    if (t.nid != null) { native.clearTimeout(t.nid); native.clearInterval(t.nid); }
    t.wid = t.nid = null; t.place = "";
  }
  worker.onmessage = e => {
    const wid = e.data, id = byWid.get(wid), t = id == null ? null : pending.get(id);
    if (!t || t.place !== "w" + wid) return;
    if (t.first) { t.first = false; worker.postMessage({ id: wid, ms: t.ms, repeat: true }); }   // (the rest of a moved period is over: the period goes on in the worker)
    fire(id, "w" + wid);
  };
  const set = repeat => function (fn, ms, ...args) {
    if (typeof fn !== "function") return (repeat ? native.setInterval : native.setTimeout)(fn, ms, ...args);
    const id = ++next, wait = Math.max(0, +ms || 0), t = { fn, args, repeat, ms: wait, due: performance.now() + wait, place: "", wid: null, nid: null, first: false };
    pending.set(id, t); place(id, t, wait);
    return id;
  };
  const clear = nativeClear => function (id) {
    const t = pending.get(id);
    if (t) { pending.delete(id); unplace(t); return; }
    nativeClear(id);
  };
  window.setTimeout = set(false);
  window.setInterval = set(true);
  window.clearTimeout = clear(native.clearTimeout);
  window.clearInterval = clear(native.clearInterval);
  /** Every waiting timer to where the clock is now, with what is left of its wait. */
  function moveAll() {
    const now = performance.now(), want = on && worker ? "w" : "n";
    for (const [id, t] of pending) { if (t.place[0] === want) continue; const left = Math.max(0, Math.min(t.ms, t.due - now)); unplace(t); place(id, t, left); }
  }
  let setFrames = () => {};
  if (native.requestAnimationFrame) {
    // shown, frames come from the browser; hidden with the clock on, from a 16 ms timer, since a hidden tab runs no frames at
    // all. A frame asked for while the tab was shown and still waiting when it is hidden moves to the timer, or it would
    // wait until the tab is shown again; with the clock off a frame waits for the browser, as on any page.
    const frames = new Map();   // id -> { fn, nativeId, timerId }
    let nextFrame = 0x50000000;
    const runFrame = id => { const f = frames.get(id); if (!f) return; frames.delete(id); f.fn(performance.now()); };
    const timed = () => document.hidden && on && !!worker;
    const toTimer = (id, f) => { if (f.nativeId != null) native.cancelAnimationFrame(f.nativeId); f.nativeId = null; f.timerId = window.setTimeout(() => runFrame(id), 16); };
    const toNative = (id, f) => { if (f.timerId != null) window.clearTimeout(f.timerId); f.timerId = null; f.nativeId = native.requestAnimationFrame(() => runFrame(id)); };
    window.requestAnimationFrame = function (fn) {
      const id = ++nextFrame, f = { fn, nativeId: null, timerId: null };
      frames.set(id, f);
      if (timed()) toTimer(id, f); else toNative(id, f);
      return id;
    };
    window.cancelAnimationFrame = function (id) {
      const f = frames.get(id);
      if (!f) return native.cancelAnimationFrame(id);
      frames.delete(id);
      if (f.nativeId != null) native.cancelAnimationFrame(f.nativeId);
      if (f.timerId != null) window.clearTimeout(f.timerId);
    };
    setFrames = () => { const tm = timed(); for (const [id, f] of frames) { if (tm && f.nativeId != null) toTimer(id, f); else if (!tm && f.timerId != null) toNative(id, f); } };
  }
  // a page holding a lock is not frozen in the background; it is let go when the clock goes off, and when the page closes
  let unlock = null;
  function lock(want) {
    if (want && !unlock) { try { if (navigator.locks && navigator.locks.request) navigator.locks.request("charm-sorter-open-" + Math.random().toString(36).slice(2), () => new Promise(res => { unlock = res; })).catch(() => {}); } catch (e) {} }
    else if (!want && unlock) { const u = unlock; unlock = null; u(); }
  }
  lock(true);
  worker.onerror = () => { worker = null; clock.active = on = false; moveAll(); setFrames(); };   // (the timers waiting on the stopped worker go to the browser's own, each once)
  function turn(want) {
    want = !!want && !!worker;
    if (want === on) return;
    on = want; clock.active = want;
    moveAll(); setFrames(); lock(want);
  }
  let needFn = null, poll = null;
  const ask = () => { if (!needFn) return; let v = true; try { v = !!needFn(); } catch (e) { v = true; } turn(v); };   // (a question that throws keeps the clock on)
  /** need(fn): the page says when it has work that must keep pace out of view: fn() is asked every 5 s and when the tab is
      hidden. need(null) goes back to always on. */
  clock.need = fn => {
    needFn = typeof fn === "function" ? fn : null;
    if (poll != null) { native.clearInterval(poll); poll = null; }
    if (!needFn) { turn(true); return; }
    poll = native.setInterval(ask, 5000); ask();
  };
  clock.on = () => on;
  document.addEventListener("visibilitychange", () => { if (document.hidden) ask(); setFrames(); });
})();
