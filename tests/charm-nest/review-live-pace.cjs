// COST (Oct 2026): how often the sorter's own pollers ask the cloud, by who is there. Offline, no browser: the files run in a vm with a fake
// clock, a fake document and fake answers; nothing leaves the process.
//   1 · ReviewLive (charm-nest-review-live.js): 2 s on the Review tab while the person works there (an input in the last 2 minutes, the window
//       focused); 10 s on the Orders and Library tabs, with the window unfocused or nobody at the keyboard; 60 s after 10 minutes with no input;
//       the first input after a quiet spell brings the next read forward; a press on this page (nudge) reads at once whatever the pace
//   2 · the Employee efficiency console's pace (Efficiency.api.pace, charm-nest-efficiency.js): the same three steps for every timer of the console
//   node tests/charm-nest/review-live-pace.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');

function world() {
  let now = Date.UTC(2026, 9, 17, 15, 0, 0), seq = 0;
  const timers = new Map(), listeners = {}, wl = {};
  const doc = { hidden: false, focus: true, hasFocus() { return this.focus; }, querySelector: () => null, getElementById: () => null,
    addEventListener(n, f) { (listeners[n] = listeners[n] || []).push(f); }, removeEventListener() {} };
  const win = { document: doc, console: { warn() {}, log() {} }, addEventListener(n, f) { (wl[n] = wl[n] || []).push(f); },
    setTimeout(f, ms) { const id = ++seq; timers.set(id, { f, at: now + Math.max(0, +ms || 0) }); return id; }, clearTimeout(id) { timers.delete(id); },
    setInterval() { return 0; }, clearInterval() {} };
  win.window = win; win.globalThis = win;
  vm.createContext(win);
  vm.runInContext('Date.now = () => __now();', Object.assign(win, { __now: () => now }));
  const advance = async ms => {          // run the timers due within ms, in order, letting the promises they start settle
    const end = now + ms;
    for (;;) {
      let next = null; for (const [id, t] of timers) if (t.at <= end && (!next || t.at < next[1].at)) next = [id, t];
      if (!next) break;
      timers.delete(next[0]); now = Math.max(now, next[1].at); next[1].f();
      for (let i = 0; i < 20; i++) await Promise.resolve();
    }
    now = end;
  };
  const fire = (n, target) => { for (const f of ((target === 'win' ? wl : listeners)[n] || [])) f({}); };
  return { win, doc, advance, fire, get now() { return now; }, set now(v) { now = v; } };
}

(async () => {
  /* 1 · ReviewLive */
  {
    const W = world(), calls = [];
    W.win.S = { mode: 'review', cloud: { ok: true } };
    vm.runInContext('var S = window.S;', W.win);
    W.win.Orders = { customBase: () => ({ at: W.now - 1000, seen: W.now }), takeCustom: () => [] };
    W.win.api = async (fn, body) => { calls.push({ at: W.now, body }); return { records: {}, at: W.now }; };
    vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-review-live.js'), 'utf8'), W.win);
    const RL = W.win.ReviewLive;
    const gaps = async (ms) => { const c0 = calls.length; await W.advance(ms); const at = calls.slice(c0 ? c0 - 1 : 0).map(c => c.at); return at.slice(1).map((x, i) => x - at[i]); };
    RL.shown('review'); await W.advance(10);
    assert.equal(calls.length, 1, 'shown: one read at once');
    W.fire('pointerdown');
    let g = await gaps(20000);
    assert(g.length >= 8 && g.every(x => x >= 2000 && x <= 2700), 'on Review while the person works: about every 2 s: ' + g);
    W.doc.focus = false; await W.advance(3000);
    g = await gaps(40000);
    assert(g.length >= 3 && g.every(x => x >= 9900 && x <= 10700), 'window unfocused: every 10 s: ' + g);
    W.doc.focus = true; W.fire('pointerdown'); const c1 = calls.length; await W.advance(2100);
    assert(calls.length > c1, 'the first input after a quiet spell brings the next read forward');
    W.win.S.mode = 'orders'; RL.shown('orders'); await W.advance(100); W.fire('keydown');
    g = await gaps(40000);
    assert(g.length >= 3 && g.every(x => x >= 9900 && x <= 10700), 'the Orders tab follows every 10 s, even while the person works: ' + g);
    W.win.S.mode = 'review'; RL.shown('review'); W.fire('keydown'); await W.advance(100);
    g = await gaps(3 * 60000);
    assert(g.slice(-3).every(x => x >= 9900 && x <= 10700), 'nobody at the keyboard for 2 minutes: every 10 s: ' + g.slice(-3));
    await W.advance(9 * 60000);
    g = await gaps(4 * 60000);
    assert(g.length >= 3 && g.every(x => x >= 59000 && x <= 61000), 'after 10 minutes with no input: every minute: ' + g);
    assert.equal(RL.state().pace, 60000);
    const c2 = calls.length; RL.nudge(); await W.advance(700);
    assert(calls.length > c2, 'a press on this page reads at once, whatever the pace');
    W.doc.hidden = true; const c3 = calls.length; await W.advance(5 * 60000);
    assert(calls.length <= c3 + 1, 'nothing is read while the tab is hidden');
    console.log('  ok  ReviewLive: 2 s on Review while worked at, 10 s on other tabs / unfocused / nobody there, 60 s after 10 minutes, an input or a press reads sooner, none hidden');
  }

  /* 2 · the efficiency console's pace */
  {
    const W = world();
    vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-efficiency.js'), 'utf8'), W.win);
    const P = W.win.Efficiency.api.pace;
    assert.equal(P(3000), 3000, 'live, the person at work: 3 s'); assert.equal(P(10000), 10000, 'overview: 10 s'); assert.equal(P(5000), 5000);
    W.doc.focus = false; assert.equal(P(3000), 10000, 'window unfocused: live 10 s'); assert.equal(P(10000), 30000, 'overview 30 s'); W.doc.focus = true;
    W.now += 3 * 60000; assert.equal(P(3000), 10000, 'no input for 3 minutes: 10 s'); assert.equal(P(30000), 90000, 'laser sheets: 90 s');
    W.now += 8 * 60000; assert.equal(P(3000), 60000, 'no input for 11 minutes: a minute'); assert.equal(P(30000), 90000);
    W.fire('pointerdown'); assert.equal(P(3000), 3000, 'an input: back to 3 s');
    assert.equal(typeof W.win.Efficiency.api.liveNow, 'function', 'the shared live read is offered to the modules');
    console.log('  ok  Efficiency.api.pace: as set while worked at, 3x (10 s at least) unfocused or idle 2 min, a minute at least after 10 minutes');
  }
  console.log('review-live-pace: all checks passed');
})().catch(e => { console.error(e); process.exit(1); });
