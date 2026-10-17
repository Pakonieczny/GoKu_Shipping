// FC4 (Firebase cost): the order view's timeline feed (charm-nest-timeline-ui.js, feed()). The real file in a vm with a fake clock and a stub of
// OrderTimeline.get; no network. Checks: the poll asks the cheap question (ifRev) and redraws nothing while it answers { unchanged }; a whole
// read at least every 10 minutes; a change redraws at once; a change made on this page reads in full (now and 1.5 s later, as before); events this page
// holds for the server read in full; the pace follows use (COST, Oct 2026: 3.5 s touched in the last 2 minutes with the window focused, 15 s otherwise,
// 60 s after 10 unused minutes, back at once on a touch); an unchanged answer's revision is kept; a poke asks verify; nothing in a hidden tab.
//   node tests/cost/fc4-timeline-feed.cjs
"use strict";
const fs = require("fs"), path = require("path"), vm = require("vm"), assert = require("assert/strict");
const root = path.join(__dirname, "../..");
let t = 0, seq = 0; const timers = new Map();
const listeners = {}, winListeners = {};
const doc = { visibilityState: "visible", addEventListener: (n, f) => { (listeners[n] = listeners[n] || []).push(f); }, removeEventListener: (n, f) => { listeners[n] = (listeners[n] || []).filter(x => x !== f); }, getElementById: () => null, createElement: () => ({}), head: { appendChild() {} } };
const win = {
  document: doc, setTimeout: (fn, ms) => { timers.set(++seq, { at: t + (ms || 0), fn }); return seq; }, clearTimeout: id => timers.delete(id), setInterval: () => 0, clearInterval() {},
  addEventListener: (n, f) => { (winListeners[n] = winListeners[n] || []).push(f); }, removeEventListener() {}, console, Promise,
  Date: class extends Date { static now() { return 1.8e12 + t; } }
};
win.window = win; vm.createContext(win);
vm.runInContext(fs.readFileSync(path.join(root, "charm-nest-timeline-ui.js"), "utf8"), win);
const UI = win.OrderTimelineUI, flush = () => new Promise(r => setImmediate(r));
async function advance(ms) {
  const end = t + ms;
  for (;;) {
    let next = null; for (const [id, x] of timers) if (x.at <= end && (!next || x.at < next[1].at)) next = [id, x];
    if (!next) break;
    timers.delete(next[0]); t = Math.max(t, next[1].at); next[1].fn(); await flush(); await flush();
  }
  t = end; await flush();
}
const touch = () => { for (const f of listeners.pointermove || []) f({}); };

(async () => {
  let rev = "aaaaaaaaaaaa", calls = [], recorded = new Set();
  const events = () => [{ orderId: "4200000001", type: "arrived", at: 1.8e12 - 1e6, id: "4200000001~arrived~x", by: "Etsy" }];
  win.OrderTimeline = {
    get: async (id, o) => { calls.push(Object.assign({ at: t }, o)); if (o && o.ifRev && o.ifRev === rev) return { orderId: id, unchanged: true, rev }; return { orderId: id, events: events(), cancelled: null, where: {}, now: 1, rev }; },
    onRecord: fn => { recorded.add(fn); return () => recorded.delete(fn); }
  };
  const f = UI.feed("4200000001"); const draws = []; f.subscribe(k => { if (k === "data") draws.push(t); });
  const ids = () => calls.map(c => (c.ifRev ? "probe" : c.wantRev ? "full" : "plain"));
  await f.refresh(); await advance(0);
  assert.deepEqual(ids(), ["full"], "the first read is whole and asks for its revision"); assert.equal(f.rev, rev); assert.equal(draws.length, 1);

  // 1. polls: the cheap question every 3.5 s while the page is in use, nothing redrawn
  await advance(14000);
  assert.deepEqual(ids().slice(1), ["probe", "probe", "probe", "probe"], "four polls in 14 s, each the cheap question: " + ids()); assert.equal(draws.length, 1, "an unchanged answer redraws nothing");
  assert(calls.slice(1).every(c => c.ifRev === rev), "each carries the revision held");

  // 2. a whole read at least every 10 minutes (COST, Oct 2026: it was every minute; the server's Order_Rev now sees an event rewritten in place)
  calls = []; for (let i = 0; i < 10; i++) { touch(); await advance(60000); }
  const fulls = calls.filter(c => c.wantRev && !c.ifRev); assert.equal(fulls.length, 1, "one whole read in 10 minutes (the rest cheap): " + fulls.length + " of " + calls.length); assert.equal(draws.length, 2);
  assert(calls.length >= 150 && calls.length <= 180, "in use: about one read every 3.5 s: " + calls.length);

  // 3. the server's digest moved: the poll's answer is whole and redraws at once
  calls = []; rev = "bbbbbbbbbbbb"; const d0 = draws.length; await advance(3600);
  assert.deepEqual(ids().slice(0, 2), ["probe", "full"].slice(0, ids().length >= 2 ? 2 : 1), "a poll that finds it moved: " + ids());
  assert(draws.length > d0, "the change is drawn"); assert.equal(f.rev, "bbbbbbbbbbbb", "the new revision is held");
  // (the stub answers a whole read when the probe's revision is stale, as the server does)
  assert.equal(calls[0].ifRev, "aaaaaaaaaaaa", "the stale revision went in the probe");

  // 4. a change made on this page: a whole read now and another 1.5 s later (never the cheap question)
  calls = []; await advance(5); f.nudge(); await advance(0); await advance(1600);
  assert(calls.length >= 2 && calls.slice(0, 2).every(c => c.wantRev && !c.ifRev), "a nudge reads in full, now and 1.5 s later: " + ids());

  // 5. an event this page recorded and the server has not answered for: whole reads while it waits, the cheap question again once it is answered
  calls = []; await advance(4000); assert(ids().every(x => x === "probe"), "quiet again: " + ids());
  const rec = [...recorded][0]; rec({ orderId: "4200000001", type: "sealCompleted", at: win.Date.now(), id: "pressed-1", by: "Tess" });
  calls = []; await advance(11000);
  assert(calls.length >= 3 && calls.every(c => !c.ifRev), "while an event of this page is unconfirmed every read is whole: " + ids());

  // 6. the pace follows use (COST, Oct 2026): touched or moved in the last 2 minutes with the window focused 3.5 s; otherwise 15 s; 60 s after 10 unused
  //    minutes (asking the server to let its one-read answer stand 5 minutes); a touch brings it back at once
  const f2 = UI.feed("4200000002"); f2.subscribe(() => {}); await f2.refresh(); await advance(0);
  let n2 = 0;   // (the server hands back a fresh revision with every unchanged answer: when its digest was last taken)
  win.OrderTimeline.get = async (id, o) => { calls.push(Object.assign({ at: t, id }, o)); return o && o.ifRev ? { orderId: id, unchanged: true, rev: "cccccccccccc.11111111." + (++n2).toString(36) } : { orderId: id, events: [], cancelled: null, where: {}, rev: "cccccccccccc.11111111.0" }; };
  f.destroy();
  const gaps = async ms => { calls = []; await advance(ms); const at = calls.filter(c => c.id === "4200000002").map(c => c.at); return at.slice(1).map((x, i) => x - at[i]); };
  await f2.refresh({ force: true }); await advance(0);
  let g = await gaps(60000); assert(g.length && g.every(x => x >= 3400 && x <= 3700), "just moved: the beat is 3.5 s: " + g);
  const probes = calls.filter(c => c.ifRev);
  assert(probes.slice(1).every((c, i) => c.ifRev === "cccccccccccc.11111111." + (n2 - probes.length + 1 + i).toString(36)), "each probe sends the revision the last unchanged answer handed back: " + probes.map(c => c.ifRev));
  await advance(2 * 60000);
  g = await gaps(120000); assert(g.length && g.every(x => x >= 14900 && x <= 15300), "untouched for 2 minutes the beat is 15 s: " + g);
  assert(calls.every(c => !c.netMs), "and the server's net stays a minute");
  await advance(8 * 60000);
  g = await gaps(300000); assert(g.length && g.every(x => x >= 59900 && x <= 60300), "after 10 unused minutes 60 s: " + g);
  assert(calls.filter(c => c.ifRev).every(c => c.netMs === 5 * 60000), "an idle window lets the one-read answer stand 5 minutes: " + calls.map(c => c.netMs));
  calls = []; touch(); await advance(3700);
  assert(calls.length >= 1, "a touch of the page reads within the normal beat, not at the slow one (a whole read when one is due, else the cheap question): " + JSON.stringify(calls.map(c => c.at)));
  g = await gaps(20000); assert(g.length && g.every(x => x >= 3400 && x <= 3700), "and the beat is 3.5 s again: " + g);
  assert(calls.every(c => !c.netMs), "with the server's net back at a minute");
  // touched, but the window is not focused (another window is in front): 15 s
  doc.hasFocus = () => false; touch(); await advance(4000);
  g = await gaps(60000); assert(g.length && g.every(x => x >= 14900 && x <= 15300), "an unfocused window is at 15 s: " + g);
  doc.hasFocus = () => true; for (const fn of winListeners.focus || []) fn(); await advance(4000);
  g = await gaps(20000); assert(g.length && g.every(x => x >= 3400 && x <= 3700), "focused again after a recent touch: 3.5 s: " + g);
  // a poke (the page's placement feed saw the order move) asks the full digest
  calls = []; assert.equal(UI.poke("4200000002"), 1); await advance(3600);
  const pk = calls.filter(c => c.ifRev); assert(pk.length && pk[0].verify === true, "the probe after a poke asks verify: " + JSON.stringify(pk[0]));
  assert(pk.slice(1).every(c => !c.verify), "once");

  // 7. a hidden tab reads nothing; seen again, one cheap question
  doc.visibilityState = "hidden"; for (const fn of listeners.visibilitychange || []) fn(); calls = []; await advance(30000);
  assert.equal(calls.length, 0, "nothing in a hidden tab");
  doc.visibilityState = "visible"; for (const fn of listeners.visibilitychange || []) fn(); await advance(10);
  assert.equal(calls.length, 1, "one read when the tab is seen again"); assert(calls[0].ifRev, "and it is the cheap question");
  f2.destroy(); calls = []; await advance(60000); assert.equal(calls.length, 0, "nothing once closed");
  console.log("fc4-timeline-feed: all checks passed");
})().catch(e => { console.error(e); process.exit(1); });
