// A sheet still filling its gaps when the sandbox order stream runs out is released and gets its QR label (Paul, 27 Sep
// 22:21: "The system still does not successfully auto-generate the QR codes when a given sheet is full"). A sandbox run at
// 1000x ended with GF Sheet 1 at 73% filling its gaps (30 of 35 later orders tried, the order that missed on Sheet 2):
// the run came to rest, the pill said "all orders in", and the sheet never joined a set.
//   1. The arrivals tick asks CN.settleTopups at every tick with no check out. At 1000x the next check is due 600 ms
//      after the last one began and a check takes longer, so the tick started a check every time; the question was only
//      asked between checks, which never came, and settleTopups never ran.
//   2. settleTopups itself: once every order of the stream is in and taken in, a Gold or Silver sheet filling its gaps
//      and at rest is released (its gap fill closed, releaseFull saved) and the run's set is assembled, which makes its
//      QR label; while orders still come, wait to be taken in, or a sheet is still at work, nothing is released.
//   3. The release rules: a sheet filling its gaps waits; the same sheet released is a full sheet that joins the set.
// The real code runs in node's vm with stand-ins around it. No network.
//   node tests/charm-nest/gap-fill-stream-end.cjs
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const root = path.join(__dirname, '../..');
const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
const part = (src, a, b) => { const i = src.indexOf(a); assert(i >= 0, 'not found: ' + a); const j = src.indexOf(b, i); assert(j > i, 'not found: ' + b); return src.slice(i, j); };
const turn = () => new Promise(r => setImmediate(r));

(async () => {
  /* ── 1 · the arrivals tick at 1000x, every check slower than its 600 ms interval ── */
  {
    const c = { console, __intervals: [], __settled: [], __calls: [] }; vm.createContext(c);
    vm.runInContext(`
      let __clock = 1790550000000; Date.now = () => __clock;
      const window = { addEventListener() {}, CN: { settleTopups: () => __settled.push({ at: __clock, checking: /Checking/.test(Arrivals.text()) }) } };
      const document = { hidden: false, getElementById: () => null, querySelector: () => null, querySelectorAll: () => [] };
      const storage = new Map(), localStorage = { getItem: k => storage.get(k), setItem: (k, v) => storage.set(k, v) }, navigator = {};
      const S = { settings: { sandboxStream: 'on', sandboxSpeed: 1000, runMode: 'auto', pollMinutes: 10 }, cloud: { ok: false }, mode: 'orders' }, WORKSPACE_SANDBOX = true;
      const B = { run: { runId: 'run-1', status: 'processed', step: 'complete' }, orders: { rows: [], byKey: new Map() } }, allSheets = () => [];
      const Orders = { rows: () => B.orders.rows, render() {}, interpretAll() {}, claim: async () => {}, applyPullRule: x => x, loadMaps: async () => {} };
      const Recall = { on: () => false }, Engrave = { render() {}, background() {} }, Review = { render() {} }, Session = { schedule() {} }, ListMedia = { prepare() {} }, Master = { load: async () => {} };
      const RunCtl = { renderBanner() {}, save: async () => {}, poke() {}, stop() {}, start: async () => {} };
      const O = { lineKey: (o, l) => o.receiptId + '/' + l.transactionId };
      // every order of the stream is in: the steps stop, and the checks go on every simulated ten minutes (600 ms at 1000x)
      const Sandbox = { on: () => true, streaming: () => true, done: () => true, speed: () => 1000, advance: async () => ({ done: true }), render() {}, label: () => 'Sandbox 1000x · all orders in' };
      const SimClock = { now: () => __clock };
      const toast = () => {}, notifyPerson = () => {}, refreshAllCards = () => {}, agent = () => {};
      // the station's sweep answers when the test says: 700 ms after it was asked
      let __answer = null;
      const DesignLink = { ensure: async () => {}, etsyBudgetOk: () => true, meter() {}, call: () => new Promise(res => { __calls.push(__clock); __answer = res; }) };
      const api = async () => ({});
      const setInterval = f => { __intervals.push(f); return __intervals.length; }, clearInterval = () => {};
    `, c);
    vm.runInContext(part(bridge, 'const Arrivals = window.Arrivals =', '/* A new batch'), c);
    const run = code => vm.runInContext(code, c), A = run('Arrivals');
    A.start(); const tick = c.__intervals.at(-1);
    assert.equal(run('Arrivals.state().nextCheck - Date.now()'), 600, 'at 1000x a check every 600 ms, from the start of the last');
    for (let step = 0; step < 48; step++) {   // twelve seconds of ticks, 250 ms apart
      run('__clock += 250');
      if (run('__answer') && run('__clock') - c.__calls.at(-1) >= 700) { run('__answer({ total: 0, hydrated: 0, orders: [], openIds: [] }); __answer = null'); for (let i = 0; i < 20; i++) await turn(); }
      tick(); for (let i = 0; i < 20; i++) await turn();
    }
    assert(c.__calls.length >= 10, `the checks went on back to back: ${c.__calls.length}`);
    // (the two ticks before the first check was due asked it before, too)
    const between = c.__settled.filter(s => s.at > c.__calls[0]).length;
    assert(between >= c.__calls.length - 1, `the gap fills were asked to settle between the checks (${between} times for ${c.__calls.length} checks); they were never asked once the checks began, and the sheet waited for good`);
    assert(c.__settled.every(s => !s.checking), 'never while a check is out: the one that brings the last orders merges them first');
    // a check that is out: the tick asks nothing, as before
    const before = c.__settled.length; run('__clock += 250'); tick(); await turn();
    assert(run('!!__answer') && c.__settled.length === before, 'nothing asked while the check is out');
  }

  /* ── 2 · settleTopups: released once the stream is over and everything is taken in ── */
  {
    const puts = [], assembled = [], logs = [], notes = [];
    const c = { console, Set, Map, Math, JSON, Object, Array, String, Number, Promise, puts, assembled, logs, notes }; vm.createContext(c);
    vm.runInContext(`
      const TOPUP = { orders: 35, target: .75 };
      const fmt = { pct: v => Math.round(v * 100) + '%' }, topupNow = () => 1790550000000;
      const sheetName = sh => sh.metal + ' · sheet ' + sh.page, activeCharms = sh => sh.charms.filter(c => !c.excluded);
      const log = (sh, m) => logs.push(sh.metal + sh.page + ': ' + m), agent = (_, __, text) => notes.push(text), renderCard = () => {};
      const api = async (_, body) => { puts.push(JSON.parse(JSON.stringify(body))); return {}; };
      let done = false, heldWhy = '', pending = false, sheets = [];
      const allSheets = () => sheets;
      const window = { B: { run: { runId: 'run-1', status: 'processed', step: 'complete' } }, Sandbox: { done: () => done }, Arrivals: { held: () => heldWhy, state: () => ({ pending }) },
        Gate: { modern: id => id === 'run-1', assemble: async run => { assembled.push(run.runId); } }, Session: { schedule() {} } };
      const Gate = window.Gate;
    `, c);
    vm.runInContext(part(html, '/* Gap fill also ends when no more orders will come', 'function usableArea('), c);
    const run = code => vm.runInContext(code, c);
    const placedOn = (ids) => ids.map((id, i) => ({ id, cxPt: 10 + 10 * i, cyPt: 10, angle: 0 }));
    const sheet = (over) => Object.assign({ metal: 'gold', page: 1, sheetId: 'gold-1', runId: 'run-1', status: 'complete', dirty: false, persistedDone: true, feedWait: null, verification: { ok: true }, density: .7306, log: [],
      charms: [{ id: 'a' }, { id: 'b' }], placements: placedOn(['a', 'b']), rejects: [], topup: { at: 1, base: .7306, placed: 77, tried: Array.from({ length: 30 }, (_, i) => 'o' + i) } }, over);
    const gold = sheet(), next = sheet({ page: 2, sheetId: 'gold-s2', topup: null, density: .32 });
    const silver = sheet({ metal: 'silver', sheetId: 'silver-1', charms: [{ id: 's1' }, { id: 's2' }], placements: placedOn(['s1']) });   // a piece still waiting on it: not at rest
    const rose = sheet({ metal: 'rose', sheetId: 'rose-1' });
    c.__list = [gold, next, silver, rose]; run('sheets = __list');
    const settle = () => run('settleTopups()');
    await settle(); assert(!gold.releaseFull && !gold.topup.closedAt && !assembled.length, 'orders still to come: the gap fill goes on (production keeps 75% or 35 later orders)');
    run('done = true; heldWhy = "adding the last arrivals"'); await settle(); assert(!gold.releaseFull, 'the last orders still being added: it waits');
    run('heldWhy = ""; pending = true'); await settle(); assert(!gold.releaseFull, 'the last orders waiting for the run: it waits');
    run('pending = false'); gold.status = 'nesting'; await settle(); assert(!gold.releaseFull, 'a sheet at work: it waits');
    gold.status = 'complete'; gold.persistedDone = false; await settle(); assert(!gold.releaseFull, 'a sheet still saving: it waits');
    gold.persistedDone = true; run('window.B.run.status = "complete"'); await settle(); assert(!gold.releaseFull, 'a finished run releases nothing');
    run('window.B.run.status = "processed"');
    await settle();
    assert.equal(gold.releaseFull, true, 'every order in and taken in: the sheet filling its gaps is released as it stands');
    assert.equal(gold.topup.closedAt, 1790550000000); assert.equal(gold.topup.density, .7306);
    assert.deepEqual(puts.map(p => [p.op, p.sheet.id, p.sheet.releaseFull, !!p.sheet.topup.closedAt]), [['putSheet', 'gold-1', true, true]], 'the release is saved on the sheet record');
    assert.deepEqual(assembled, ['run-1'], "the run's set is assembled: the sheet joins it and its QR label is made");
    assert.match(logs.at(-1), /every order of the stream is in: 73% → 73%, sheet released/); assert.match(notes.at(-1), /30 of 35 later orders tried/);
    assert(!next.releaseFull && !silver.releaseFull && !rose.releaseFull, 'a sheet not filling its gaps, one with a piece still waiting on it, and Rose Gold stay as they are');
    await settle(); assert.equal(puts.length, 1, 'released once'); assert.equal(assembled.length, 1);
  }

  /* ── 3 · the release rules the set assembly applies (Gate.policy → CharmNestOrders.sheetRelease) ── */
  {
    const O = require(path.join(root, 'charm-nest-orders.js'));
    // (solid and picked: the 10K/14K helpers policy reads, as the bridge defines them beside it)
    const c = { O_: O, TOPUP: { orders: 35, target: .75 }, selected: () => ({}), solid: m => ['gold10k', 'gold14k'].includes(m), picked: sh => sh.solidPick === true }; vm.createContext(c);
    vm.runInContext(part(bridge, '  function policy(sh, seq, choices = selected()) {', '  async function upgrade(run) {'), c);
    const sh = { metal: 'gold', verification: { ok: true }, placements: [{ id: 'a' }], endedBy: 'no-room', dirty: false, status: 'complete', releaseFull: false, topup: { at: 1, tried: Array.from({ length: 30 }, (_, i) => 'o' + i) } };
    assert.deepEqual(c.policy(sh, 1), { include: false, reason: 'Filling its gaps · 30 of 35 later orders tried' }, 'a sheet filling its gaps waits');
    Object.assign(sh, { releaseFull: true, topup: Object.assign(sh.topup, { closedAt: 2 }) });
    assert.deepEqual(c.policy(sh, 1), { include: true, reason: 'Full sheet' }, 'released, it joins the set');
  }
  console.log('Gap fill at the end of the stream OK: the arrivals tick asks between back-to-back checks at 1000x (never during one); once every order is in and taken in, a sheet filling its gaps is released, saved and its set assembled for its QR label; nothing is released while orders come, wait or a sheet is at work');
})().catch(e => { console.error(e); process.exit(1); });
