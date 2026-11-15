/* Stall recorder (Paul, 14 Nov 2026: "if I leave the sorter running it always crashes ... I get a blank screen, and I have to close the tab and reopen it").
   A tab that goes blank while it runs is not out of memory: its main thread is stuck inside the page's own code (seen live: one core at 100 %, every sample inside the page's JavaScript). A stuck page cannot say where.
   A small worker beside it can: the page answers its ping once a second; when the answers stop for 20 seconds the worker writes down, in this browser's storage (IndexedDB "cn-hang"), which
   piece of code the page was inside (a timer, a frame, an event or a worker message, with its source), the last 30 pieces it ran, how long the longest recent ones took, and the
   page's memory and size over the last hour. Nothing is sent anywhere. Read it with CharmNestHang.report() (or from another tab of this site) after a stall.
   Loaded first, before charm-nest-clock.js. It changes nothing the page does: every timer, frame, event and worker message still runs exactly as it did, inside a wrapper that notes its start and end.
   Needs a page that may share memory with its worker (crossOriginIsolated); elsewhere it does nothing. */
(function () {
  "use strict";
  if (typeof window === "undefined" || window.CharmNestHang) return;
  if (window.top !== window) return;   // (the Design Station framed by the sorter is not the page to watch)
  let iso = false; try { iso = !!window.crossOriginIsolated && typeof SharedArrayBuffer === "function" && typeof Atomics === "object"; } catch (_) { iso = false; }
  const HW = window.CharmNestHang = { active: false, run: (fn, kind, self, args) => fn.apply(self, args), report: () => Promise.resolve([]) };
  if (!iso) return;
  try {
    const RING = 32, STALL_S = 20, SAMPLE_MS = 30000, SLOW_MS = 600;
    // shared words: [0] answers counted, [1] ring head, [2] id of the piece running now (0 = none), [3] 1 while the tab is frozen by the browser, [4] 1 while hidden, [8 .. 8+RING) the last ids started; start time of the running piece at byte 512
    const sab = new SharedArrayBuffer(1024), ia = new Int32Array(sab), ft = new Float64Array(sab, 512, 4);
    const WSRC = `
      let ia = null, ft = null, port = null, marks = new Map(), cnames = new Map(), reg = new Map(), samples = [], slow = [], lastCnt = -1, same = 0, stallAt = 0, lastWrite = 0, db = null, RING = ${RING}, STALL_S = ${STALL_S};
      const open = () => new Promise((res, rej) => { const q = indexedDB.open('cn-hang', 1); q.onupgradeneeded = () => { const d = q.result; if (!d.objectStoreNames.contains('rec')) d.createObjectStore('rec', { keyPath: 'id', autoIncrement: true }); if (!d.objectStoreNames.contains('kv')) d.createObjectStore('kv'); }; q.onsuccess = () => res(q.result); q.onerror = () => rej(q.error); });
      const put = (store, val, key) => open().then(d => new Promise((res) => { const tx = d.transaction(store, 'readwrite'); const s = tx.objectStore(store); if (store === 'rec') { s.add(val); const c = s.count(); c.onsuccess = () => { if (c.result > 120) { const k = s.openCursor(); let n = c.result - 120; k.onsuccess = () => { const cur = k.result; if (cur && n-- > 0) { cur.delete(); cur.continue(); } }; } }; } else s.put(val, key); tx.oncomplete = () => { d.close(); res(); }; tx.onerror = () => { d.close(); res(); }; })).catch(() => {});
      const recent = () => { const head = Atomics.load(ia, 1), out = []; for (let i = 0; i < RING; i++) { const id = ia[8 + (((head - 1 - i) % RING) + RING) % RING]; if (id) out.push([id, (reg.get(id) || '?').slice(0, 400)]); } return out; };
      const now = () => ft[0];
      onmessage = e => { const d = e.data; if (d.init) { ia = new Int32Array(d.sab); ft = new Float64Array(d.sab, 512, 4); port = d.port; return; } if (d.id) { reg.set(d.id, d.k + ': ' + d.src); if (reg.size > 20000) { const k = reg.keys().next().value; reg.delete(k); } return; } if (d.mark) { marks.set(d.mark, d.label); return; } if (d.cnt) { cnames.set(d.cnt, d.label); return; } if (d.sample) { samples.push(d.sample); if (samples.length > 120) samples.shift(); return; } if (d.slow) { slow.push(d.slow); if (slow.length > 60) slow.shift(); } };
      const details = (kind) => { const cur = Atomics.load(ia, 2), mk = Atomics.load(ia, 5); const counts = {}; for (const [i, n] of cnames) counts[n] = Atomics.load(ia, 63 + i); return { kind, at: Date.now(), counts, lastMark: mk ? [marks.get(mk) || '?', Math.round(performance.timeOrigin + performance.now() - ft[1])] : null, page: self.location ? self.location.origin : '', inside: cur ? [cur, (reg.get(cur) || '?').slice(0, 600)] : null, insideForMs: cur ? Math.round(performance.timeOrigin + performance.now() - ft[0]) : null, hidden: Atomics.load(ia, 4) === 1, frozen: Atomics.load(ia, 3) === 1, recent: recent(), slow: slow.slice(-25), samples: samples.slice(-30) }; };
      setInterval(() => {
        if (!ia || !port) return;
        try { port.postMessage(1); } catch (_) {}
        const c = Atomics.load(ia, 0);
        if (c !== lastCnt) { if (stallAt) { put('rec', { kind: 'resume', at: Date.now(), stalledSec: Math.round((Date.now() - stallAt) / 1000) }); stallAt = 0; } lastCnt = c; same = 0; }
        else if (Atomics.load(ia, 3) !== 1) same++;
        if (same >= STALL_S && !stallAt) { stallAt = Date.now() - same * 1000; lastWrite = Date.now(); put('rec', Object.assign(details('stall'), { stalledSec: same })); }
        else if (stallAt && Date.now() - lastWrite > 60000) { lastWrite = Date.now(); put('rec', Object.assign(details('stall-update'), { stalledSec: Math.round((Date.now() - stallAt) / 1000) })); }
        if (!stallAt && Date.now() % 60000 < 1000) put('kv', { at: Date.now(), samples: samples.slice(-120), slow: slow.slice(-40) }, 'latest');
      }, 1000);`;
    const worker = new Worker(URL.createObjectURL(new Blob([WSRC], { type: "text/javascript" })));
    const ch = new MessageChannel();
    ch.port1.onmessage = () => { Atomics.add(ia, 0, 1); };   // (an event, not a timer: a hidden tab's slowed timers do not stop the answers)
    worker.postMessage({ init: 1, sab, port: ch.port2 }, [ch.port2]);
    const ids = new WeakMap(), byKey = new Map(), made = new WeakMap(); let nid = 1, slowSent = 0, madeSecond = 0, madeCount = 0;
    const src = fn => { try { return Function.prototype.toString.call(fn).slice(0, 700).replace(/\s+/g, " ").slice(0, 400); } catch (_) { return "?"; } };
    // where a piece of code was set up, for the ones that carry no source of their own: `await new Promise(r => setTimeout(r, 500))` sets a timer whose function is the promise's native resolver,
    // and what runs after it is the code that waited, which only the place that set the timer can name
    const place = () => { try { return String(new Error().stack || "").split("\n").slice(1).map(l => { const m = /at (?:async )?(?:([^\s(]+) )?\(?(.*?):(\d+):\d+\)?\s*$/.exec(l); if (!m || /charm-nest-(hangwatch|clock)\.js/.test(m[2])) return null; return (m[1] || "anon") + "@" + m[2].replace(/^.*\//, "").replace(/\?.*$/, "") + ":" + m[3]; }).filter(Boolean).slice(0, 5).join(" < "); } catch (_) { return ""; } };
    HW.created = fn => { try { if (typeof fn !== "function" || made.has(fn)) return; const s = Math.floor(Date.now() / 1000); if (s !== madeSecond) { madeSecond = s; madeCount = 0; } if (++madeCount > 200) return; const t = Function.prototype.toString.call(fn); if (t.length < 80 && t.indexOf("[native code]") >= 0) made.set(fn, place()); } catch (_) {} };
    const idOf = (fn, kind) => {
      let id = ids.get(fn); if (id) return id;
      const where = made.get(fn), text = where ? "(no source: set up at) " + where : src(fn), key = kind + "|" + text;
      id = byKey.get(key); if (!id) { id = nid++; if (byKey.size > 20000) byKey.clear(); byKey.set(key, id); try { worker.postMessage({ id, k: kind, src: text }); } catch (_) {} }
      ids.set(fn, id); return id;
    };
    const run = (fn, kind, self, args) => {
      const id = idOf(fn, kind), h = Atomics.add(ia, 1, 1); ia[8 + (h % RING)] = id; Atomics.store(ia, 2, id); const t0 = performance.now(); ft[0] = performance.timeOrigin + t0;
      try { return fn.apply(self, args); }
      finally { Atomics.store(ia, 2, 0); const d = performance.now() - t0; if (d > SLOW_MS && Date.now() - slowSent > 5000) { slowSent = Date.now(); try { worker.postMessage({ slow: [id, Math.round(d), Date.now()] }); } catch (_) {} } }
    };
    HW.run = run; HW.active = true;
    // mark(label): says where the code is now (a step of the run, a stage of a check); the record of a stall shows the last mark and how long ago it was set
    const markIds = new Map(), cntIds = new Map(); let mid = 0;
    // count(label): how many times a place was passed (up to 48 places); the record of a stall shows the counts, so a cycle that never rests shows as one number far above the others
    HW.count = label => { try { let i = cntIds.get(label); if (!i) { if (cntIds.size >= 48) return; i = cntIds.size + 1; cntIds.set(label, i); worker.postMessage({ cnt: i, label }); } Atomics.add(ia, 63 + i, 1); } catch (_) {} };
    HW.mark = label => { try { let id = markIds.get(label); if (!id) { id = ++mid; markIds.set(label, id); worker.postMessage({ mark: id, label }); } Atomics.store(ia, 5, id); ft[1] = performance.timeOrigin + performance.now(); } catch (_) {} };
    const wrap = (fn, kind) => { if (typeof fn !== "function") return fn; HW.created(fn); return function (...a) { return run(fn, kind, this, a); }; };
    const oST = window.setTimeout, oSI = window.setInterval, oRAF = window.requestAnimationFrame;
    window.setTimeout = function (f, ...r) { return oST.call(this, wrap(f, "timeout"), ...r); };
    window.setInterval = function (f, ...r) { return oSI.call(this, wrap(f, "interval"), ...r); };
    if (oRAF) window.requestAnimationFrame = function (f) { return oRAF.call(this, wrap(f, "frame")); };
    const wm = new WeakMap(), oAdd = EventTarget.prototype.addEventListener, oRem = EventTarget.prototype.removeEventListener;
    const keyOf = (t, o) => t + "|" + ((o && typeof o === "object" ? o.capture : o) ? 1 : 0);
    EventTarget.prototype.addEventListener = function (t, l, o) {
      if (typeof l !== "function") return oAdd.call(this, t, l, o);
      let m = wm.get(l); if (!m) { m = new Map(); wm.set(l, m); }
      const k = keyOf(t, o); let w = m.get(k); if (!w) { w = wrap(l, "event " + t); m.set(k, w); }
      return oAdd.call(this, t, w, o);
    };
    EventTarget.prototype.removeEventListener = function (t, l, o) {
      if (typeof l === "function") { const m = wm.get(l), w = m && m.get(keyOf(t, o)); if (w) return oRem.call(this, t, w, o); }
      return oRem.call(this, t, l, o);
    };
    // a message handler set the old way (worker.onmessage = fn; the same on a message port and a broadcast channel)
    for (const [C, kind] of [[typeof Worker === "function" ? Worker : null, "worker message"], [typeof MessagePort === "function" ? MessagePort : null, "port message"], [typeof BroadcastChannel === "function" ? BroadcastChannel : null, "channel message"]]) {
      try {
        const d = C && Object.getOwnPropertyDescriptor(C.prototype, "onmessage");
        if (d && d.set && d.get) Object.defineProperty(C.prototype, "onmessage", { configurable: true, enumerable: d.enumerable, get() { return this.__cnOm || null; }, set(f) { this.__cnOm = f; d.set.call(this, typeof f === "function" ? wrap(f, kind) : f); } });
      } catch (_) { /* left as it is */ }
    }
    const note = () => { Atomics.store(ia, 4, document.hidden ? 1 : 0); };
    document.addEventListener("visibilitychange", note); note();
    document.addEventListener("freeze", () => Atomics.store(ia, 3, 1)); document.addEventListener("resume", () => Atomics.store(ia, 3, 0));
    // memory and size, every 30 s, on the browser's own timer (not the clock's)
    oSI.call(window, () => { try { const m = performance.memory || {}; worker.postMessage({ sample: { t: Date.now(), heapMB: Math.round((m.usedJSHeapSize || 0) / 1048576), totalMB: Math.round((m.totalJSHeapSize || 0) / 1048576), nodes: document.getElementsByTagName("*").length, hidden: document.hidden } }); } catch (_) {} }, SAMPLE_MS);
    HW.report = () => new Promise(res => { try { const q = indexedDB.open("cn-hang", 1); q.onupgradeneeded = () => { try { q.result.createObjectStore("rec", { keyPath: "id", autoIncrement: true }); q.result.createObjectStore("kv"); } catch (_) {} }; q.onsuccess = () => { const d = q.result; try { const out = {}; const tx = d.transaction(["rec", "kv"], "readonly"); const a = tx.objectStore("rec").getAll(), b = tx.objectStore("kv").get("latest"); tx.oncomplete = () => { d.close(); res({ records: a.result || [], latest: b.result || null }); }; tx.onerror = () => { d.close(); res({ records: [], latest: null }); }; } catch (e) { d.close(); res({ records: [], latest: null }); } }; q.onerror = () => res({ records: [], latest: null }); } catch (_) { res({ records: [], latest: null }); } });
  } catch (e) { try { console.warn("[hangwatch] not started:", e && e.message); } catch (_) {} }
})();
