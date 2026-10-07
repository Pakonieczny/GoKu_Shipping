// LB2 (7 Oct 2026). Paul: every effect of Complete Order / Print QR label / Hold / Release / Cancel shows in about 3 s on EVERY screen, one that somebody only WATCHES
// (a wall monitor, waiting for the other person to press) included. The cost work (FC4) slows an order window nobody touches: 5 s after 10 minutes, 10 s after an hour.
// This keeps that slow-down and makes it safe: the page's CHEAP shared signals (the custom records feed of Review: Complete, QR Print, Reopen; the placement feed's counter:
// Hold, Release, Cancel, take-off) wake the window of that order at once. The real files in a vm with a fake clock and stubs; no browser, no network.
//   node tests/charm-nest/complete-realtime-wake.cjs
//   1  the order window's feed (charm-nest-timeline-ui.js) slowed to 10 s: poke() reads the cheap question within 0.7 s, a whole read follows when the digest moved, the
//      full pace is back; without the poke the slowed window really takes 10 s (the test means something); another order's poke wakes nothing
//   2  the placement of the order moves (PiecePlacement's signature, told by OrderPieces): the slowed feed wakes; a quiet cloud (same signature) wakes nothing
//   3  ReviewLive (charm-nest-review-live.js): a record another computer or tab wrote, applied, pokes that order's window and tells OrderPieces; and the BroadcastChannel
//      tells the other tabs the very record (the sandbox has its own channel), no read of any kind
//   4  the sheet window (charm-nest-sheetwin.js): any change OrderPieces tells asks for its one-record probe at once, whatever the beat has slowed to
"use strict";
const fs = require("fs"), path = require("path"), vm = require("vm"), assert = require("assert/strict");
const root = path.join(__dirname, "../..");
const read = f => fs.readFileSync(path.join(root, f), "utf8");
const flush = () => new Promise(r => setImmediate(r));

/* ───────────── 1 + 2 · the feed ───────────── */
async function feedTests() {
  let t = 0, seq = 0; const timers = new Map(), listeners = {};
  const doc = { visibilityState: "visible", addEventListener: (n, f) => { (listeners[n] = listeners[n] || []).push(f); }, removeEventListener: (n, f) => { listeners[n] = (listeners[n] || []).filter(x => x !== f); }, getElementById: () => null, createElement: () => ({ style: {}, setAttribute() {}, appendChild() {} }), head: { appendChild() {} }, body: { appendChild() {} } };
  const win = { document: doc, setTimeout: (fn, ms) => { timers.set(++seq, { at: t + (ms || 0), fn }); return seq; }, clearTimeout: id => timers.delete(id), setInterval: () => 0, clearInterval() {}, addEventListener() {}, removeEventListener() {}, console, Promise, Date: class extends Date { static now() { return 1.8e12 + t; } } };
  win.window = win; vm.createContext(win);
  vm.runInContext(read("charm-nest-timeline-ui.js"), win);
  const UI = win.OrderTimelineUI;
  async function advance(ms) {
    const end = t + ms;
    for (;;) {
      let next = null; for (const [id, x] of timers) if (x.at <= end && (!next || x.at < next[1].at)) next = [id, x];
      if (!next) break;
      timers.delete(next[0]); t = Math.max(t, next[1].at); next[1].fn(); await flush(); await flush();
    }
    t = end; await flush();
  }
  const rev = { "4200000001": "aaaaaaaaaaaa", "4200000002": "aaaaaaaaaaaa", "4200000003": "aaaaaaaaaaaa" }; let calls = [];
  const events = id => [{ orderId: id, type: "arrived", at: 1.8e12 - 1e6, id: id + "~arrived~x", by: "Etsy" }].concat(rev[id] === "bbbbbbbbbbbb" ? [{ orderId: id, type: "sealCompleted", at: 1.8e12 - 1000, id: id + "~sealCompleted~y", by: "Tess" }] : []);
  win.OrderTimeline = {
    get: async (id, o) => { calls.push(Object.assign({ at: t, id }, o)); if (o && o.ifRev && o.ifRev === rev[id]) return { orderId: id, unchanged: true, rev: rev[id] }; return { orderId: id, events: events(id), cancelled: null, where: {}, now: 1, rev: rev[id] }; },
    onRecord: () => () => {}
  };
  const kinds = c => c.map(x => (x.ifRev ? "probe" : x.wantRev ? "full" : "plain"));

  // ── 1 · a window unused for an hour is at 10 s; a poke wakes it
  const f = UI.feed("4200000001"); const draws = []; f.subscribe(k => { if (k === "data") draws.push(t); });
  await f.refresh(); await advance(0);
  await advance(61 * 60000);
  calls = []; await advance(60000);
  const at = calls.filter(c => c.id === "4200000001").map(c => c.at), gaps = at.slice(1).map((x, i) => x - at[i]);
  assert(gaps.length >= 3 && gaps.every(x => x >= 9900 && x <= 10300), "unused for an hour the beat is 10 s (the slow-down this test is about): " + gaps);
  // the cloud changes (a Complete Order on another computer); nothing has told this page yet: the slowed window takes up to 10 s
  rev["4200000001"] = "bbbbbbbbbbbb"; const d0 = draws.length, tChange = t;
  calls = []; await advance(1500);
  assert.equal(draws.length, d0, "without a signal the slowed window has not drawn it 1.5 s on");
  // the signal (Review's custom records feed read the new record) arrives: the window reads now
  const woken = UI.poke("4200000001"); assert.equal(woken, 1, "poke wakes the feed of that order");
  await advance(800);
  assert(draws.length > d0, "the change is on the window within 0.8 s of the signal (a probe, then the whole read): " + kinds(calls));
  // (the cheap question, answered whole because the digest moved; or a whole read straight away when one was due anyway: never more than those two reads)
  assert(kinds(calls).length <= 2 && kinds(calls).includes("full") && kinds(calls).every((k, i, a) => k === "full" || i === 0), "one probe at most, then the whole answer: " + kinds(calls));
  assert(calls.filter(c => c.ifRev).every(c => c.ifRev === "aaaaaaaaaaaa"), "a probe carries the revision held");
  // full pace again: the next beats are 2.5 s apart
  calls = []; await advance(12000);
  const at2 = calls.filter(c => c.id === "4200000001").map(c => c.at), g2 = at2.slice(1).map((x, i) => x - at2[i]);
  assert(g2.length >= 3 && g2.every(x => x >= 2400 && x <= 2700), "back at full speed after the change (it moved): " + g2);
  assert.equal(UI.poke("4299999999"), 0, "another order's window is not woken");
  assert.equal(UI.poke(""), 0, "nor is any, for no order");
  f.destroy(); assert.equal(UI.poke("4200000001"), 0, "a closed window is not woken");

  // ── 1b · a poke while a read is on its way is followed by another read (the one on its way may be older than the change)
  const g = UI.feed("4200000002"); const drawsG = []; let release = null; g.subscribe(k => { if (k === "data") drawsG.push(t); });
  await g.refresh(); await advance(0); await advance(61 * 60000);
  const realGet = win.OrderTimeline.get;
  win.OrderTimeline.get = (id, o) => new Promise(res => { release = () => res(realGet(id, o)); });
  await advance(10000);   // (a slow poll is now on its way, held)
  assert(release, "a read is on its way"); rev["4200000002"] = "bbbbbbbbbbbb"; const dG = drawsG.length;
  UI.poke("4200000002"); await advance(0);
  win.OrderTimeline.get = realGet; const r1 = release; release = null; r1(); await advance(1500);
  assert(drawsG.length > dG, "the read that was on its way is followed by another that sees the change");
  g.destroy();

  // ── 2 · the placement of the order moves (Hold, Release, Cancel, take-off: the placement feed's counter, told by OrderPieces): the slowed feed wakes
  let sig = "waiting~", subs = [];
  win.PiecePlacement = { of: id => ({ sig: id + ":" + sig }) };
  win.OrderPieces = { subscribe: fn => { subs.push(fn); return () => { subs = subs.filter(x => x !== fn); }; } };
  const h = UI.feed("4200000003"); const drawsH = []; h.subscribe(k => { if (k === "data") drawsH.push(t); });
  assert.equal(subs.length, 1, "the feed listens to where this order's pieces are, once");
  await h.refresh(); await advance(0); await advance(61 * 60000);
  calls = []; await advance(30000); assert(calls.filter(c => c.id === "4200000003").length <= 4, "slowed: few reads in 30 s");
  // a quiet cloud: OrderPieces tells nothing different (the placement feed bumps nothing) -> no wake
  calls = []; for (const fn of subs) fn(); await advance(700); assert.equal(calls.filter(c => c.id === "4200000003").length, 0, "the same placement is no signal: " + kinds(calls));
  // a Hold pressed on another computer: the feed's counter moved, OrderPieces read the pool rows, the placement says "on hold"
  rev["4200000003"] = "bbbbbbbbbbbb"; sig = "hold~held by Maria"; const dH = drawsH.length;
  calls = []; for (const fn of subs) fn(); await advance(800);
  assert(drawsH.length > dH, "a Hold / Release / Cancel read by the placement feed reaches the slowed window within 0.8 s: " + kinds(calls));
  h.destroy(); assert.equal(subs.length, 0, "the feed stops listening when it is destroyed");
  delete win.PiecePlacement; delete win.OrderPieces;
  console.log("  ok  the order window's feed: a slowed window (10 s) is woken by a signal within 0.8 s, back at full speed; the placement wake; nothing woken without a signal");
}

/* ───────────── 3 · ReviewLive: apply pokes + notifies, the BroadcastChannel tells the other tabs ───────────── */
async function reviewTests() {
  const make = sandbox => {
    const made = []; class BC { constructor(n) { this.name = n; this.sent = []; made.push(this); } postMessage(m) { this.sent.push(m); } }
    const log = { settle: 0, notify: 0, pokes: [], takes: [], api: 0 };
    const doc = { hidden: false, addEventListener() {}, removeEventListener() {}, querySelector: () => null };
    const win = { document: doc, addEventListener() {}, setTimeout, clearTimeout, console, Promise, Date, BroadcastChannel: BC, S: { mode: "nest", cloud: { ok: true } } };
    win.window = win;
    if (sandbox != null) win.WORKSPACE_SANDBOX = sandbox;
    win.Seal = { list: r => r.stamps || [] };
    win.Orders = { customBase: () => ({ at: 1000, seen: Date.now() }), takeCustom: recs => { const out = Object.entries(recs).map(([key, rec]) => ({ key, was: null, rec })); log.takes.push(out.map(x => x.key)); return out; }, interpretAll() {}, render() {} };
    win.CustomPrint = { settle() { log.settle++; }, touching: () => false };
    win.OrderPieces = { notify() { log.notify++; } };
    win.OrderTimelineUI = { poke: rid => { log.pokes.push(rid); return 1; } };
    win.api = () => { log.api++; return Promise.reject(new Error("no network in this test")); };
    vm.createContext(win); vm.runInContext(read("charm-nest-review-live.js"), win);
    return { win, made, log };
  };
  const rec = (key, at) => ({ key, state: "completed", updatedAtMs: at, stamps: [{ how: "button", at, by: "Tess" }] });

  // a record another tab wrote (or another computer's feed read), applied
  let { win, made, log } = make(false);
  assert.equal(made.length, 1, "one channel, opened at load"); assert.equal(made[0].name, "cn-custom", "the real shop's channel");
  const n = win.ReviewLive.heard({ v: 1, records: { "4200000001_9": rec("4200000001_9", 5000), "4200000001_10": rec("4200000001_10", 5100), "4200000002_3": rec("4200000002_3", 5200) } });
  assert.equal(n, 3, "the three records are applied"); assert.equal(log.settle, 1, "everything that follows a press follows it (settle)"); assert.equal(log.notify, 1, "OrderPieces is told once (the sheet window, the search, the lists)");
  assert.deepEqual(log.pokes.sort(), ["4200000001", "4200000002"], "the window of each order is woken once, even if it is slowed");
  assert.equal(log.api, 0, "no call to the cloud: the record came with the message");
  // junk and foreign messages do nothing
  for (const bad of [null, {}, { v: 2, records: {} }, { v: 1, records: { a: { key: "b" } } }, { v: 1, records: { "4200000001_9": 5 } }, { v: 1 }]) assert.equal(win.ReviewLive.heard(bad), 0, "ignored: " + JSON.stringify(bad));
  assert.equal(log.settle, 1, "nothing more was applied");
  // what this page writes is told to the other tabs, as the cloud answered it
  assert.equal(win.ReviewLive.announce("customPut", { ok: true, record: rec("4200000001_9", 6000) }), true); assert.equal(made[0].sent.length, 1);
  assert.deepEqual(Object.keys(made[0].sent[0].records), ["4200000001_9"]); assert.equal(made[0].sent[0].v, 1);
  assert.equal(win.ReviewLive.announce("customReopen", { ok: true, record: rec("4200000001_9", 6100) }), true, "a reopen too");
  assert.equal(win.ReviewLive.announce("poolPut", { record: rec("4200000001_9", 6000) }), false, "no other op is told"); assert.equal(win.ReviewLive.announce("customPut", { ok: true, record: null }), false, "nothing to tell");
  assert.equal(win.ReviewLive.announce("customPut", { error: "x" }), false);
  // the sandbox has its own channel: its presses never reach the real shop's tabs
  ({ win, made, log } = make(true)); assert.equal(made[0].name, "cn-custom-sandbox", "the sandbox's channel");
  // (a message arrives on the channel itself)
  ({ win, made, log } = make(false)); made[0].onmessage({ data: { v: 1, records: { "4200000005_1": rec("4200000005_1", 7000) } } });
  assert.equal(log.settle, 1, "a message on the channel is applied at once"); assert.deepEqual(log.pokes, ["4200000005"]);
  console.log("  ok  ReviewLive: a record from another computer or tab wakes that order's window and tells OrderPieces; the channel carries the saved record (sandbox apart); no call to the cloud");
}

/* ───────────── 4 · the sheet window ───────────── */
function sheetTests() {
  const src = read("charm-nest-sheetwin.js");
  // any change OrderPieces tells (the placement feed's read, Review's Complete / QR Print applied by ReviewLive.refresh -> OrderPieces.notify) asks for the sheet's own
  // one-record probe 80 ms later, whatever the unused beat has slowed to (follow(true) skips the gap)
  assert(/OP\.subscribe\(\(\) => \{ if \(W\.dlg && W\.dlg\.open\) \{ clearTimeout\(followHook\.t\); followHook\.t = setTimeout\(\(\) => follow\(true\), 80\); \} \}\)/.test(src), "the sheet window asks for its read at once on every OrderPieces change");
  assert(/if \(now !== true && Date\.now\(\) - FOLLOW\.at < followGap\(\)\) return;/.test(src), "follow(true) skips the slowed gap");
  const rl = read("charm-nest-review-live.js");
  assert(/refresh\(\)[\s\S]{0,1200}OrderPieces\.notify/.test(rl), "ReviewLive.refresh tells OrderPieces, so the sheet window's hook fires for a change read from the cloud or from another tab");
  console.log("  ok  the sheet window: every OrderPieces change (Hold, Release, Cancel from the placement feed; Complete, QR Print from ReviewLive) asks for its one-record probe at once");
}

(async () => {
  await feedTests(); await reviewTests(); sheetTests();
  console.log("complete-realtime-wake: all checks passed");
})().catch(e => { console.error(e); process.exit(1); });
