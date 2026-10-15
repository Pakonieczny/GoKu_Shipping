// ADV-OFFLINE (Paul, 15 Oct 2026: "resilient against lost internet connections ... two benches"): two sorter tabs of one browser share ONE order check
// (one takes the lock, the other takes its record from localStorage). A tab whose own earlier check failed kept its "Order check failed" chip after the
// other tab's check went through, for as long as the other tab kept winning the next ones, and the chip's own "checks again by itself" never happened
// (the tab only ever took the other's times). The real Arrivals code runs in node's vm with stand-ins around it. No network.
//   /tmp/cn-run.sh 120 tests/charm-nest/adv-offline-tabs.cjs
"use strict";
const assert = require("node:assert/strict"), fs = require("node:fs"), path = require("node:path"), vm = require("node:vm");
const bridge = fs.readFileSync(path.join(__dirname, "../../charm-nest-bridge.js"), "utf8");
const part = (src, a, b) => { const i = src.indexOf(a); assert(i >= 0, "not found: " + a); const j = src.indexOf(b, i); assert(j > i, "not found: " + b); return src.slice(i, j); };

(async () => {
  const c = { console, __listeners: {}, __intervals: [], __lockFree: true }; vm.createContext(c);
  vm.runInContext(`
    const window = { addEventListener: (t, f) => (__listeners[t] ||= []).push(f) };
    const document = { hidden: false, addEventListener() {}, getElementById: () => null, querySelector: () => null, querySelectorAll: () => [], body: { appendChild() {} } };
    const el = () => ({ style: {}, classList: { toggle() {} }, setAttribute() {}, removeAttribute() {} });
    const storage = new Map(), localStorage = { getItem: k => (storage.has(k) ? storage.get(k) : null), setItem: (k, v) => storage.set(k, v) };
    const navigator = { locks: { request: async (name, opts, cb) => cb(__lockFree ? {} : null) } };
    const S = { settings: { pollMinutes: 10, runMode: 'manual' }, cloud: { ok: true }, mode: 'nest' }, WORKSPACE_SANDBOX = false;
    const B = { run: null, orders: { rows: [], byKey: new Map() } }, allSheets = () => [];
    const Orders = { view: () => ({}), rows: () => B.orders.rows, render() {}, interpretAll() {}, claim: async () => {}, applyPullRule: x => x, loadMaps: async () => {} };
    const Recall = { state: () => ({}), on: () => false }, Engrave = { render() {}, background() {} }, Review = { render() {} }, Session = { schedule() {} }, ListMedia = { prepare() {} }, Master = { load: async () => {} };
    const RunCtl = { renderBanner() {}, save: async () => {}, poke() {}, stop() {}, start: async () => {}, clearRunState() {} };
    const O = { lineKey: (o, l) => o.receiptId + '/' + l.transactionId }, Sandbox = { on: () => false, streaming: () => false };
    const toast = () => {}, notifyPerson = () => {}, refreshAllCards = () => {}, agent = () => {}, setMode = () => {};
    const TL = { pulled() {}, line() {} };
    let snapshots = 0, failCloud = false;
    const DesignLink = { ensure: async () => {}, etsyBudgetOk: () => true, meter() {},
      call: async () => { snapshots++; return { total: 1, hydrated: 1, openIds: ['1'], orders: [{ receiptId: '1', hydrated: true, updateTs: 1, lines: [{ transactionId: 't' }] }] }; } };
    const api = async () => { if (failCloud) throw new Error('Failed to fetch'); return { firstSeen: { 1: 1 }, count24: 1, count1: 1 }; };
    const setInterval = f => { __intervals.push(f); return __intervals.length; }, clearInterval = () => {};
  `, c);
  vm.runInContext(part(bridge, "const Arrivals = window.Arrivals =", "/* A new batch"), c);
  const run = code => vm.runInContext(code, c), A = run("Arrivals");
  const KEY = "cn.arrivals.production", other = o => JSON.stringify(Object.assign({ seen: {}, lastCheck: 0, nextCheck: Date.now() + 600000, lastAdded: 0, error: null }, o));
  const hear = value => { for (const f of c.__listeners.storage || []) f({ key: KEY, newValue: value }); };

  // this tab's own check fails: the chip's cause is kept, and written for the other tab
  run("failCloud = true"); await A.check();
  assert.equal(A.state().error, "Failed to fetch");
  assert.equal(JSON.parse(run(`localStorage.getItem("${KEY}")`)).error, "Failed to fetch", "the failure is in the shared record");

  // 1 · the other tab's check goes through (its record reaches this tab as a storage event): the chip clears here too
  const t1 = Date.now() + 1000;
  hear(other({ lastCheck: t1, nextCheck: t1 + 600000, error: null }));
  assert.equal(A.state().error, null, "the other tab's good check clears this tab's old failure");

  // 2 · the other tab's check fails (same last good check): this tab says so too
  hear(other({ lastCheck: t1, nextCheck: Date.now() + 15000, error: "Failed to fetch" }));
  assert.equal(A.state().error, "Failed to fetch", "a failure the other tab met is this tab's too");

  // 3 · a tab that was not listening (asleep, background) takes the record at its tick instead: no check of its own, and the failure is gone
  run("failCloud = false");
  const snapshots = run("snapshots"), t2 = Date.now() + 2000;
  run(`localStorage.setItem("${KEY}", ${JSON.stringify(other({ lastCheck: t2, nextCheck: t2 + 600000, error: null }))})`);
  A.start(); const tick = c.__intervals.at(-1);
  A.state().nextCheck = Date.now() - 1; tick(); await new Promise(r => setImmediate(r)); await new Promise(r => setImmediate(r));
  assert.equal(run("snapshots"), snapshots, "the other tab's check stands: this tab does not ask Etsy again");
  assert.equal(A.state().error, null, "and its old failure is gone");
  assert.equal(A.state().lastCheck, t2, "with the other tab's time of check");

  // 4 · an older record never clears a newer failure of this tab
  run("failCloud = true"); A.state().nextCheck = Date.now() - 1; await A.check();
  assert.equal(A.state().error, "Failed to fetch");
  hear(other({ lastCheck: t2 - 5000, nextCheck: t2 + 600000, error: null }));
  assert.equal(A.state().error, "Failed to fetch", "a record older than this tab's last good check says nothing about its failure");
  console.log("adv-offline-tabs OK: a sorter tab takes the other tab's 'Order check failed' / good check along with its times");
})().catch(e => { console.error(e); process.exit(1); });
