// The background clock (charm-nest-clock.js) is on only while the page says there is work (COST, Oct 2026: CharmNestClock.need).
// Runs the real file in node's vm against a virtual clock: the browser's own timers and frames, a stand-in worker (its timers
// on the same virtual clock, never slowed) and navigator.locks. Checks that
//   1 · with no need() (the framed Design Station) the clock stays on: timers go through the worker, the lock is held
//   2 · need(() => false) turns it off: waiting timers move to the browser's own with what was left of their wait, under the
//       ids their owners hold, each runs ONCE; the lock is let go; a hidden tab's frames wait for the browser
//   3 · need(() => true) again turns it back on: the timers move back to the worker, still once each; an interval keeps its
//       period and never runs twice for one period; clearTimeout / clearInterval work on a moved timer
//   4 · a question that throws keeps the clock on; the worker failing moves everything to the browser's own, once each
//   node tests/charm-nest/clock-gate.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const SRC = fs.readFileSync(path.join(__dirname, '../../charm-nest-clock.js'), 'utf8');

function world() {
  let now = 0, seq = 0, nid = 0;
  const q = [];   // { at, seq, fn, id, every }
  const add = (fn, ms, every) => { const id = ++nid; q.push({ at: now + Math.max(0, ms), seq: ++seq, fn, id, every }); return id; };
  const drop = id => { const i = q.findIndex(t => t.id === id); if (i >= 0) q.splice(i, 1); };
  const log = { browser: 0, worker: 0, locks: [], released: [] };
  // the browser's own timers (counted: "browser")
  const nat = {
    setTimeout: (fn, ms) => add(() => { log.browser++; fn(); }, ms),
    setInterval: (fn, ms) => { const id = add(null, ms, ms); const t = q.find(x => x.id === id); t.fn = () => { log.browser++; fn(); }; return id; },
    clearTimeout: drop, clearInterval: drop
  };
  const frames = new Map(); let fid = 0;
  const doc = { hidden: false, currentScript: { src: 'charm-nest-clock.js', hasAttribute: () => false }, listeners: [], addEventListener(t, f) { if (t === 'visibilitychange') this.listeners.push(f); } };
  let failWorker = null;
  class Worker {
    constructor() { const self = this; this.timers = new Map(); failWorker = () => self.onerror && self.onerror(new Error('gone')); }
    postMessage(d) {
      // (the worker's timers, on the same clock: what it posts back arrives as a task, as a message does)
      const back = () => { log.worker++; this.onmessage && this.onmessage({ data: d.id }); };
      if (d.cancel) { const t = this.timers.get(d.id); if (t != null) { drop(t); this.timers.delete(d.id); } return; }
      if (d.repeat) { const id = add(null, d.ms, d.ms); q.find(x => x.id === id).fn = back; this.timers.set(d.id, id); }
      else this.timers.set(d.id, add(() => { this.timers.delete(d.id); back(); }, d.ms));
    }
  }
  const win = {
    setTimeout: nat.setTimeout, setInterval: nat.setInterval, clearTimeout: nat.clearTimeout, clearInterval: nat.clearInterval,
    requestAnimationFrame: fn => { const id = ++fid; frames.set(id, fn); return id; }, cancelAnimationFrame: id => frames.delete(id),
    document: doc, Worker, performance: { now: () => now },
    navigator: { locks: { request: (name, cb) => { log.locks.push(name); return Promise.resolve(cb()).then(() => log.released.push(name)); } } }
  };
  win.window = win; win.self = win;
  const c = vm.createContext(Object.assign(win, { console, Math, Map, Set, Promise, Error }));
  vm.runInContext(SRC, c);
  const run = ms => { const end = now + ms; for (;;) { q.sort((a, b) => a.at - b.at || a.seq - b.seq); const t = q[0]; if (!t || t.at > end) break; now = t.at; if (t.every) { t.at += t.every; t.seq = ++seq; } else q.shift(); t.fn(); } now = end; };
  const paintFrames = () => { const fs_ = [...frames.values()]; frames.clear(); fs_.forEach(f => f(now)); return fs_.length; };
  const hide = h => { doc.hidden = h; doc.listeners.forEach(f => f()); };
  return { c, win, run, log, hide, paintFrames, frames, fail: () => failWorker(), now: () => now, flush: () => new Promise(r => setImmediate(r)) };
}

(async () => {
  /* 1 · nobody asks: on, as before */
  {
    const w = world(), C = w.win.CharmNestClock;
    assert.equal(C.on(), true, 'on from the start'); assert.equal(w.log.locks.length, 1, 'the lock is asked for');
    let n = 0; w.win.setTimeout(() => n++, 1000); w.run(1500);
    assert.equal(n, 1); assert.equal(w.log.worker, 1, 'through the worker'); assert.equal(w.log.browser, 0);
    console.log('  ✓ without need(): the clock is on (worker, lock), as it always was');
  }
  /* 2 · off: timers move to the browser's own, once each, with what was left */
  {
    const w = world(), C = w.win.CharmNestClock;
    const ran = []; let busy = true;
    const a = w.win.setTimeout(() => ran.push(['a', w.now()]), 10000);
    w.win.setTimeout(() => ran.push(['b', w.now()]), 3000);
    let ticks = []; const iv = w.win.setInterval(() => ticks.push(w.now()), 4000);
    C.need(() => busy);
    assert.equal(C.on(), true, 'work: on');
    w.run(2000);                                      // t = 2000
    busy = false; w.run(5000);                        // the next 5 s question (t = 5000) turns it off
    await w.flush();
    assert.equal(C.on(), false, 'no work: off');
    assert.equal(w.log.released.length, 1, 'the lock was let go');
    w.run(20000);                                     // t = 27000
    const b = ran.filter(r => r[0] === 'b'), aa = ran.filter(r => r[0] === 'a');
    assert.equal(b.length, 1, 'b ran once'); assert.equal(b[0][1], 3000, 'b at its time (still in the worker then)');
    assert.equal(aa.length, 1, 'a ran once, after the move'); assert.equal(aa[0][1], 10000, 'a at its own time, not reset by the move: ' + aa[0][1]);
    const gaps = ticks.slice(1).map((t, i) => t - ticks[i]);
    assert.deepEqual(ticks.slice(0, 2), [4000, 8000], 'the interval kept its period across the move: ' + ticks);
    assert(gaps.every(g => g === 4000), 'never twice in one period: ' + gaps);
    w.win.clearInterval(iv); const k = ticks.length; w.run(10000); assert.equal(ticks.length, k, 'clearInterval stops a moved interval');
    w.win.clearTimeout(a);   // (already run: nothing)
    // a hidden tab's frames, clock off: they wait for the browser, as on any page
    w.hide(true); let f = 0; w.win.requestAnimationFrame(() => f++); w.run(1000);
    assert.equal(f, 0, 'off and hidden: no frame from a timer'); assert.equal(w.paintFrames(), 1, 'the browser has it'); assert.equal(f, 1);
    console.log('  ✓ need() false: off, the lock let go; a waiting timer moved with what was left of its wait and ran once; an interval kept its period; frames wait for the browser');
  }
  /* 3 · back on: to the worker again, still once each; clear works on a moved timer */
  {
    const w = world(), C = w.win.CharmNestClock;
    let busy = false; C.need(() => busy); assert.equal(C.on(), false);
    const ran = []; w.win.setTimeout(() => ran.push(w.now()), 7000); const gone = w.win.setTimeout(() => ran.push('gone'), 8000);
    const ticks = []; w.win.setInterval(() => ticks.push(w.now()), 3000);
    w.run(1000); busy = true; w.run(4000);            // t = 5000: the question at 5000 turns it on
    assert.equal(C.on(), true, 'work again: on');
    assert.equal(w.log.locks.length, 2, 'the lock is asked for again');
    w.win.clearTimeout(gone);                          // cleared after its move
    const before = w.log.worker; w.run(10000);         // t = 15000
    assert.deepEqual(ran, [7000], 'the moved timeout ran once at its time, the cleared one never: ' + JSON.stringify(ran));
    assert(w.log.worker > before, 'through the worker now');
    assert.deepEqual(ticks, [3000, 6000, 9000, 12000, 15000], 'the interval: its period, once per period, across the move: ' + ticks);
    // hidden with the clock on: frames on the 16 ms timer
    w.hide(true); let f = 0; w.win.requestAnimationFrame(() => f++); w.run(20);
    assert.equal(f, 1, 'on and hidden: the frame runs on the timer');
    // a frame waiting on the timer when the clock goes off moves to the browser
    w.win.requestAnimationFrame(() => f++); busy = false; w.hide(true); assert.equal(C.on(), false, 'asked when the tab is hidden: off');   // (a hidden tab asks at once)
    w.run(5000); assert.equal(f, 1, 'the waiting frame left the timer');
    assert.equal(w.paintFrames(), 1, 'the browser has it'); assert.equal(f, 2, 'and it runs once');
    console.log('  ✓ need() true again: on, the lock asked again; timers moved back ran once at their time; a moved timer can still be cleared; hidden frames follow the clock');
  }
  /* 4 · a question that throws keeps it on; the worker failing moves everything to the browser's own */
  {
    const w = world(), C = w.win.CharmNestClock;
    C.need(() => { throw new Error('not ready'); }); assert.equal(C.on(), true, 'a question that throws: on');
    const ran = []; w.win.setTimeout(() => ran.push(w.now()), 6000); const ticks = []; w.win.setInterval(() => ticks.push(w.now()), 2500);
    w.run(1000); w.fail(); assert.equal(C.on(), false, 'the worker failed: off');
    const b0 = w.log.browser; w.run(9000);
    assert.deepEqual(ran, [6000], 'the timeout ran once at its time on the browser: ' + ran); assert(w.log.browser > b0);
    assert.deepEqual(ticks, [2500, 5000, 7500, 10000], 'the interval goes on, once per period: ' + ticks);
    console.log('  ✓ a question that throws keeps the clock on; a failed worker hands every timer to the browser, once each');
  }
  console.log('clock-gate: all checks passed');
})().catch(e => { console.error(e); process.exit(1); });
