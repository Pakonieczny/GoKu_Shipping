// A clock change, or the network coming back, must not stall the Auto run (Paul, 15 Oct 2026: "resilient against ... interruptions"). The computer's clock set
// back by hours (a wrong time corrected, a resumed virtual machine) left the next order check hours ahead of it: no order was looked
// for until the clock caught up, with nothing on screen saying so. Runs the sorter's own Arrivals code in node's vm; nothing live.
//   node tests/charm-nest/adv-autorun-clock.cjs
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const bridge = fs.readFileSync(process.env.BRIDGE_JS || path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
const part = (src, a, b) => { const i = src.indexOf(a); if (i < 0) throw new Error(`not found: ${a.trim()}`); const j = src.indexOf(b, i); if (j < 0) throw new Error(`not found: ${b.trim()}`); return src.slice(i, j); };

function world(stored, { locks = false } = {}) {
  const c = { console, __listeners: {}, __intervals: [], __stored: stored };
  vm.createContext(c);
  vm.runInContext(`
    const __realNow = Date.now; let __clock = null; Date.now = () => __clock ?? __realNow.call(Date);
    const window = { addEventListener: (t, f) => (__listeners[t] ||= []).push(f) };
    const document = { hidden: false, addEventListener() {}, getElementById: () => null, querySelector: () => null, querySelectorAll: () => [], body: { appendChild() {} } };
    const storage = new Map(__stored ? [['cn.arrivals.production', JSON.stringify(__stored)]] : []), localStorage = { getItem: k => storage.get(k), setItem: (k, v) => storage.set(k, v) }, navigator = ${locks ? '{ locks: { request: (n, o, cb) => Promise.resolve(cb({})) } }' : '{}'};
    const S = { settings: { pollMinutes: 10, runMode: 'manual', pollOrders: 'on' }, cloud: { ok: true }, mode: 'nest' }, WORKSPACE_SANDBOX = false;
    const B = { run: null, orders: { rows: [], byKey: new Map() } }, allSheets = () => [];
    const Orders = { view: () => ({}), rows: () => B.orders.rows, render() {}, interpretAll() {}, claim: async () => {}, applyPullRule: x => x, loadMaps: async () => {} };
    const Recall = { state: () => ({}), on: () => false }, Engrave = { render() {}, background() {} }, Review = { render() {} }, Session = { schedule() {} }, ListMedia = { prepare() {} }, Master = { load: async () => {} };
    const RunCtl = { renderBanner() {}, save: async () => {}, poke() {}, stop() {}, start: async () => {}, clearRunState() {} };
    const O = { lineKey: (o, l) => o.receiptId + '/' + l.transactionId }, Sandbox = { on: () => false, streaming: () => false };
    const toast = () => {}, notifyPerson = () => {}, refreshAllCards = () => {}, agent = () => {}, setMode = () => {};
    const TL = { pulled() {}, line() {} };
    let snapshots = 0, stationDown = false;
    const DesignLink = { ensure: async () => { if (stationDown) throw new Error('Failed to fetch'); }, etsyBudgetOk: () => true, meter() {}, call: async () => { snapshots++; return { total: 0, hydrated: 0, openIds: [], orders: [] }; } };
    const api = async () => ({ firstSeen: {}, count24: 0, count1: 0 });
    const setInterval = f => { __intervals.push(f); return __intervals.length; }, clearInterval = () => {};
  `, c);
  vm.runInContext(part(bridge, 'const Arrivals = window.Arrivals =', '/* A new batch'), c);
  return { c, run: code => vm.runInContext(code, c) };
}

(async () => {
  const MIN = 60000;
  // 1 · the clock is set back three hours while the sorter is open: the next check is one interval away, not three hours and ten minutes
  {
    const { c, run } = world(null);
    run('Arrivals.start()'); const tick = c.__intervals.at(-1);
    run('__clock = Date.now()'); tick();
    run('Arrivals.state().nextCheck = __clock + 10 * 60000');                        // a check was due in ten minutes
    run('__clock -= 3 * 3600000'); tick();                                            // the clock is corrected, three hours back
    const left = run('Arrivals.state().nextCheck - Date.now()');
    assert(left <= 10 * MIN + 1000, `the next check is at most one interval away after the clock went back (${Math.round(left / MIN)} min)`);
    // and the check really happens when that interval has passed, with no wait for the old time
    const before = run('snapshots'); for (let i = 0; i < 21; i++) { run('__clock += 30000'); tick(); } await new Promise(r => setTimeout(r, 30));   // (a tick every 30 s for ten minutes and a half, no tick a minute late)
    assert.equal(run('snapshots'), before + 1, 'the order check is made when the shortened interval is over');
  }
  // 2 · a last check that is dated ahead of the clock (set back, then the page reloaded): the first check is one interval away at most
  {
    const ahead = Date.now() + 5 * 3600000;
    const { run } = world({ seen: {}, lastCheck: ahead, nextCheck: ahead + 10 * MIN, lastAdded: 0, error: null });
    run('Arrivals.start()');
    const left = run('Arrivals.state().nextCheck - Date.now()');
    assert(left <= 10 * MIN + 1000, `a reload after the clock was set back still checks within one interval (${Math.round(left / MIN)} min)`);
  }
  // 3 · an ordinary check time is left as it is, and a longer poll setting is not cut short
  {
    const { c, run } = world(null);
    run('Arrivals.start()'); const tick = c.__intervals.at(-1);
    run('S.settings.pollMinutes = 60; __clock = Date.now()'); run('Arrivals.state().nextCheck = __clock + 50 * 60000'); tick();
    assert.equal(run('Arrivals.state().nextCheck - __clock'), 50 * MIN, 'a check 50 minutes away under a 60-minute setting stays');
    run('Arrivals.state().nextCheck = __clock + 2 * 3600000'); tick();
    assert.equal(run('Arrivals.state().nextCheck - __clock'), 60 * MIN, 'but never more than the setting');
  }
  // 4 · the network comes back after failed checks: the check is made 3 s later, not when the failed check's own wait is over
  // (the tick reads the check's time from the browser's copy under the lock open sorters share; the online event kept it only in the page)
  {
    const { c, run } = world(null, { locks: true });
    run('Arrivals.start()'); const tick = c.__intervals.at(-1);
    run('__clock = Date.now(); stationDown = true'); await run('Arrivals.check()');
    assert.equal(run('Arrivals.state().error'), 'Failed to fetch'); assert(run('Arrivals.state().nextCheck - __clock') >= 14000, 'the failed check is put off by 15 s or more');
    run('stationDown = false'); for (const f of c.__listeners.online || []) f();
    const before = run('snapshots');
    for (let i = 0; i < 5; i++) { run('__clock += 1000'); tick(); await new Promise(r => setTimeout(r, 10)); }
    await new Promise(r => setTimeout(r, 30));
    assert.equal(run('snapshots'), before + 1, 'the check is made within 5 s of the network coming back');
  }
  console.log('Clock change and network back OK: 4 checks');
})().catch(e => { console.error('FAIL', e.message); process.exit(1); });
