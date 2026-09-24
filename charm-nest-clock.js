/* Background clock: the sorter keeps full speed while its tab is out of view.
   A browser slows the timers of a tab that is not shown to once a second, and after a few minutes to once a minute,
   and stops its animation frames altogether, so a run slowed to a crawl or stopped when Paul clicked away from the
   tab (24 Sep). A dedicated worker's timers are not slowed. Loaded as a page script, this file starts itself as such a
   worker and routes the page's timers through it: each timer is set in the worker, and the worker's message runs the
   callback on the page at once, shown or not. While the tab is hidden, animation-frame callbacks run on a 16 ms
   timer the same way, so a step that waits for a frame goes on. A lock held while the page is open asks the browser
   not to freeze the tab. If the worker cannot start, the page keeps the browser's own timers. The Design Station,
   framed by the sorter, loads it too (data-framed-only), since each order check waits on the station's timers.     */
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
  window.CharmNestClock = { native, active: !!worker };
  if (!worker) return;
  // ids far above the browser's own, so a timer set before this file ran is still cleared by the browser
  let next = 0x40000000;
  const pending = new Map();   // id -> { fn, args, repeat, ms, due }
  const fail = e => native.setTimeout(() => { throw e; }, 0);
  worker.onmessage = e => {
    const id = e.data, t = pending.get(id);
    if (!t) return;
    if (!t.repeat) pending.delete(id);
    try { t.fn.apply(window, t.args); } catch (err) { fail(err); }
  };
  worker.onerror = () => { worker = null; window.CharmNestClock.active = false; restore(); };
  const set = repeat => function (fn, ms, ...args) {
    if (typeof fn !== "function" || !worker) return (repeat ? native.setInterval : native.setTimeout)(fn, ms, ...args);
    const id = ++next, wait = Math.max(0, +ms || 0);
    pending.set(id, { fn, args, repeat, ms: wait, due: performance.now() + wait });
    worker.postMessage({ id, ms: wait, repeat });
    return id;
  };
  const clear = nativeClear => function (id) {
    if (pending.has(id)) { pending.delete(id); if (worker) worker.postMessage({ id, cancel: true }); return; }
    nativeClear(id);
  };
  window.setTimeout = set(false);
  window.setInterval = set(true);
  window.clearTimeout = clear(native.clearTimeout);
  window.clearInterval = clear(native.clearInterval);
  if (native.requestAnimationFrame) {
    // shown, frames come from the browser; hidden, from a 16 ms timer, since a hidden tab runs no frames at all. A frame
    // asked for while the tab was shown and still waiting when it is hidden moves to the timer, or it would wait until
    // the tab is shown again.
    const frames = new Map();   // id -> { fn, nativeId, timerId }
    let nextFrame = 0x50000000;
    const runFrame = id => { const f = frames.get(id); if (!f) return; frames.delete(id); f.fn(performance.now()); };
    const onTimer = (id, f) => { f.nativeId = null; f.timerId = window.setTimeout(() => runFrame(id), 16); };
    window.requestAnimationFrame = function (fn) {
      const id = ++nextFrame, f = { fn, nativeId: null, timerId: null };
      frames.set(id, f);
      if (document.hidden && worker) onTimer(id, f); else f.nativeId = native.requestAnimationFrame(() => runFrame(id));
      return id;
    };
    window.cancelAnimationFrame = function (id) {
      const f = frames.get(id);
      if (!f) return native.cancelAnimationFrame(id);
      frames.delete(id);
      if (f.nativeId != null) native.cancelAnimationFrame(f.nativeId);
      if (f.timerId != null) window.clearTimeout(f.timerId);
    };
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden || !worker) return;
      for (const [id, f] of frames) if (f.nativeId != null) { native.cancelAnimationFrame(f.nativeId); onTimer(id, f); }
    });
  }
  function restore() {
    // Timers still waiting on the stopped worker move to the browser's own, each with its own period (a timeout with what
    // was left of it), under the ids their owners hold. They were all set again at 1 s, or at once, under new ids nobody
    // could clear: a heartbeat or a tick restarted after that ran twice, and the 10-minute photo check every second.
    const moved = new Map(), now = performance.now();   // the id handed out -> { native id, repeat }
    for (const [id, t] of pending) { pending.delete(id); moved.set(id, { repeat: t.repeat, native: t.repeat ? native.setInterval(t.fn, t.ms, ...t.args) : native.setTimeout(() => { moved.delete(id); t.fn.apply(window, t.args); }, Math.max(0, t.due - now)) }); }
    const unmove = nativeClear => function (id) { const m = moved.get(id); if (!m) return nativeClear(id); moved.delete(id); (m.repeat ? native.clearInterval : native.clearTimeout)(m.native); };
    Object.assign(window, { setTimeout: native.setTimeout, clearTimeout: unmove(native.clearTimeout), setInterval: native.setInterval, clearInterval: unmove(native.clearInterval) });
    if (native.requestAnimationFrame) Object.assign(window, { requestAnimationFrame: native.requestAnimationFrame, cancelAnimationFrame: native.cancelAnimationFrame });
  }
  // a page holding a lock is not frozen in the background; the lock is released when the page closes
  try { if (navigator.locks && navigator.locks.request) navigator.locks.request("charm-sorter-open-" + Math.random().toString(36).slice(2), () => new Promise(() => {})).catch(() => {}); } catch (e) {}
})();
