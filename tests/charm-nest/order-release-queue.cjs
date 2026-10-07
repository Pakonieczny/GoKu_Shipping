// Release hold: the queue's place for an order released from hold (Paul, 5 Oct 2026: "When an order gets released from hold it
// must go back in queue and be placed on the next available placement on the available sheet ahead of the incoming orders
// from Etsy"). An order released from hold carries `frontAt` (the release time, ms) on its lines, pieces and pool rows; every
// placement path puts what carries it first, the order released first first, and the rest as before (oldest order first).
// Each path is run here as the page's own code (sliced out of the real files), with fakes only for what it reads:
//   the pull order (Pool.addAll) · the legacy release plan (planRelease) · the nest's feed turn and the orders still waiting
//   (feedTurn, feedOn) · keeping orders whole · the Merge order (byDate) · the sheet window's waiting candidates ·
//   the job the solver reads, and the real solver itself (the server's solver reads the same job).
//   node tests/charm-nest/order-release-queue.cjs
const fs = require('fs'), vm = require('vm'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const sheetwin = fs.readFileSync(path.join(root, 'charm-nest-sheetwin.js'), 'utf8');
const slice = (src, from, to) => { const a = src.indexOf(from); assert(a >= 0, 'not found: ' + from); const b = src.indexOf(to, a + from.length); assert(b > a, 'not found: ' + to); return src.slice(a, b); };
const ok = [];
const T0 = 1790000000000;          // "now" in ms; a release a minute later
const SEC = 1790000000;            // an order date, s

// 1 · the order itself
{
  const f = x => O.frontOf(x), q = O.byQueue(x => x.d);
  assert.equal(f({}), 0); assert.equal(f({ frontAt: T0 }), T0); assert.equal(f({ row: { frontAt: T0 } }), T0); assert.equal(f(null), 0); assert.equal(f({ frontAt: 'x' }), 0);
  const list = [{ n: 'etsy-old', d: SEC - 9 }, { n: 'rel-2', d: SEC + 99, frontAt: T0 + 60000 }, { n: 'etsy-new', d: SEC + 5 }, { n: 'rel-1', d: SEC + 50, frontAt: T0 }].sort(q).map(x => x.n);
  assert.deepEqual(list, ['rel-1', 'rel-2', 'etsy-old', 'etsy-new'], 'released first, in release order, then oldest first: ' + list);
  // the piece's date: ahead of every real order date, in release order among its own kind
  const r1 = O.rankDate({ frontAt: T0, orderDate: SEC + 999 }), r2 = O.rankDate({ frontAt: T0 + 1, orderDate: 5 }), plain = O.rankDate({ orderDate: 946684800 });
  assert(r1 > 0 && r1 < r2 && r2 < plain, 'a front piece ranks before the oldest real order and in release order: ' + [r1, r2, plain]);
  assert.equal(O.rankDate({ orderDate: SEC }), SEC); assert.equal(O.rankDate({}), 0); assert.equal(O.rankDate(undefined), 0);
  ok.push('the order: released first (the first release first), then oldest first; a front piece ranks before every real order date');
}

// 2 · the run's pull order: Pool.addAll
(async () => {
  const mk = (rid, createTs, extra) => Object.assign({ key: rid + ':1', order: { receiptId: rid, createTs }, state: 'pulled', hold: null }, extra);
  const rows = [mk('A', SEC - 10), mk('B', SEC - 5), mk('C', SEC), mk('REL', SEC + 40, { frontAt: T0 })], seen = [];
  const code = slice(bridge, '  async function addAll(run) {', '  async function addRows(run, rows) {');
  const ctx = { O, Orders: { interpretAll() {}, rows: () => rows }, recover() {}, window: { Cancelled: { has: () => false } }, placing: new Map(), onSheets: () => false, settle: () => true, due: () => true, lock: () => () => {}, addRows: async (run, list) => { seen.push(...list.map(r => r.order.receiptId)); return list.length; } };
  vm.createContext(ctx); vm.runInContext(code + ';this.addAll = addAll;', ctx);
  await ctx.addAll({});
  assert.deepEqual(seen, ['REL', 'A', 'B', 'C'], 'the run pulls the released order first, then the incoming ones oldest first: ' + seen);
  ok.push('Pool.addAll (the run\'s pull order): the released order is pulled before three incoming orders, even though it is the newest');
})().then(() => {

// 3 · the legacy release plan (a line released from hold never waits for a full sheet)
  const area = 100, line = (key, orderId, createTs, extra) => Object.assign({ key, orderId, material: 'gold', areaPt2: area, createTs, shipBy: 0 }, extra);
  const lines = [line('a', 'A', 1), line('b', 'B', 2), line('c', 'C', 3), line('rel', 'R', 99, { frontAt: T0 })];
  const plan = O.planRelease(lines, { today: '2026-10-05', capacity: { gold: 250 }, lastReleased: {}, released: {}, forceFill: {} });
  assert(plan.take.has('rel') && !plan.wait.has('rel'), 'a line released from hold goes now, not waiting for a full sheet');
  assert(plan.take.has('a'), 'the full sheet worth (250 of 400) is the released line first, then the oldest incoming one');
  assert(plan.wait.has('b') && plan.wait.has('c'), 'the others still wait for a full sheet');
  const plain = O.planRelease(lines.map(l => ({ key: l.key, orderId: l.orderId, material: l.material, areaPt2: l.areaPt2, createTs: l.createTs, shipBy: 0 })), { today: '2026-10-05', capacity: { gold: 250 }, lastReleased: {}, released: {}, forceFill: {} });
  assert(plain.take.has('a') && plain.take.has('b') && plain.wait.has('c') && plain.wait.has('rel'), 'without frontAt nothing changes: the two oldest go, the newest wait');
  // a released line that does not even fit a full sheet's worth is still never made to wait
  const big = O.planRelease([line('a', 'A', 1), line('rel', 'R', 99, { frontAt: T0, areaPt2: 900 })], { today: '2026-10-05', capacity: { gold: 250 }, lastReleased: {}, released: {}, forceFill: {} });
  assert(big.take.has('rel') && !big.wait.has('rel'), 'a released line is never held back by the full-sheet rule');
  ok.push('planRelease (older runs\' rule): a line released from hold goes at once, ahead of lines that wait for a full sheet; the rest as before');

// 4 · the nest's feed turn: the first three orders are placed now, the rest wait (feedTurn), and the next few come in (feedOn)
  const feed = slice(html, 'const FEED_ORDERS = 3;', 'function nestItems(sh)');
  const mkc = (id, order, orderDate, extra) => Object.assign({ id, order, orderDate, poolId: id }, extra);
  const fctx = { S: { settings: {} }, carefulNest: () => true, topupRoom: () => Infinity, CharmNestOrders: O };
  vm.createContext(fctx); vm.runInContext(feed + ';this.feedTurn = feedTurn; this.orderRank = orderRank;', fctx);
  {
    const sh = { runId: 'r', metal: 'gold', placements: [], topup: null };
    const items = [mkc('o1', '1', SEC - 30), mkc('o2', '2', SEC - 20), mkc('o3', '3', SEC - 10), mkc('rel', 'R', SEC + 100, { frontAt: T0 })];
    const turn = fctx.feedTurn(sh, items).map(c => c.order);
    assert.deepEqual(turn.sort(), ['1', '2', 'R'], 'the released order takes a place in the first three, ahead of the oldest incoming one that would have been third: ' + turn);
    assert.deepEqual(sh.feedWait, ['o3'], 'the newest-but-one incoming order is the one that waits');
    const sh2 = { runId: 'r', metal: 'gold', placements: [], topup: null };
    const t2 = fctx.feedTurn(sh2, items.filter(c => c.order !== 'R')).map(c => c.order);
    assert.deepEqual(t2.sort(), ['1', '2', '3'], 'without it the three oldest go: ' + t2);
    // a sheet filling its gaps takes the orders it still owes a try: the released one first
    const sh3 = { runId: 'r', metal: 'gold', placements: [{ id: 'x' }], topup: { tried: [] } };
    fctx.topupRoom = () => 2;
    const t3 = fctx.feedTurn(sh3, items).map(c => c.order);
    assert.deepEqual(t3.sort(), ['1', 'R'], 'a sheet filling its gaps with room for two takes the released order and the oldest incoming one: ' + t3);
    fctx.topupRoom = () => Infinity;
  }
  {
    const fo = slice(html, 'function feedOn(sh) {', '/* A stopped run starts none of its sheets');
    const sh = { feedWait: ['a', 'b', 'rel'], placements: [{ id: 'p' }], charms: [mkc('a', '1', SEC - 3), mkc('b', '2', SEC - 2), mkc('rel', 'R', SEC + 9, { frontAt: T0 }), mkc('p', '0', SEC - 50)], topup: { closedAt: 0 }, verification: { ok: true }, status: 'complete', rejects: [], endedBy: 'done' };
    let passed = null;
    const c = { allSheets: () => [sh], activeCharms: s => s.charms, topupRoom: () => 2, orderKey: x => String(x.order || x.id), orderRank: fctx.orderRank, overflowToNextSheet: s => { passed = s.rejects.slice(); return null; }, startNest() {}, S: { settings: {} } };
    vm.createContext(c); vm.runInContext(fo + ';this.feedOn = feedOn;', c);
    c.feedOn(sh);
    assert.deepEqual(passed, ['b'], 'a sheet filling its gaps keeps the orders it owes a try, the released one first, and passes the rest on: ' + passed);
  }
  ok.push('feedTurn / feedOn: the released order is in the first turn (ahead of the oldest incoming one), also on a sheet filling its gaps; without it nothing changes');

// 5 · keeping orders whole: a released order that missed is the oldest waiting order; younger orders are lifted for it, never the other way round
  {
    const kw = slice(html, 'function keepOrdersWhole(sh, byId', 'function orderSummary(charms)');
    const logs = [];
    const c = { orderRank: fctx.orderRank, agent: (...a) => logs.push(a) };
    vm.createContext(c); vm.runInContext(kw + ';this.keepOrdersWhole = keepOrdersWhole;', c);
    const byId = new Map([['old', mkc('old', 'OLD', SEC - 50)], ['new', mkc('new', 'NEW', SEC + 50)], ['rel', mkc('rel', 'REL', SEC + 90, { frontAt: T0 + 60000 })], ['rel0', mkc('rel0', 'REL0', SEC + 95, { frontAt: T0 })]]);
    // a released order missed the sheet: it is the oldest waiting order, so the incoming orders placed before it make way and it goes first on the next sheet;
    // the order released before it keeps its place
    const sh = { placements: [{ id: 'old' }, { id: 'new' }, { id: 'rel0' }], rejects: ['rel'], topup: null, metal: 'gold' };
    c.keepOrdersWhole(sh, byId);
    assert.deepEqual(sh.placements.map(p => p.id), ['rel0'], 'the order released first keeps its place; the incoming orders make way for the one released after it: ' + JSON.stringify(sh.placements));
    assert(sh.rejects.includes('old') && sh.rejects.includes('new') && sh.rejects.includes('rel'));
    // and a plain miss behaves as before: the order that missed is NEW, nothing younger is placed, the released order is never lifted for it
    const sh2 = { placements: [{ id: 'old' }, { id: 'rel' }], rejects: ['new'], topup: null, metal: 'gold' };
    c.keepOrdersWhole(sh2, byId);
    assert.deepEqual(sh2.placements.map(p => p.id), ['old', 'rel'], 'the released order is never lifted for a younger one: ' + JSON.stringify(sh2.placements));
    // a sheet still filling its gaps never lifts a younger order for an older miss (as before)
    const sh3 = { placements: [{ id: 'old' }, { id: 'new' }], rejects: ['rel'], topup: { closedAt: 0 }, metal: 'gold' };
    c.keepOrdersWhole(sh3, byId);
    assert.deepEqual(sh3.placements.map(p => p.id), ['old', 'new'], 'a topping-up sheet is unchanged');
    ok.push('keepOrdersWhole: a released order is never lifted for an incoming one; when it misses, the incoming orders placed before it make way; a topping-up sheet is unchanged');
  }

// 6 · the job the solver reads (the server's solver reads the same job), and the real solver
  {
    assert(/orderDate: sh\.topup && !sh\.topup\.closedAt && sh\.appendOnly \? 0 : orderRank\(c\)/.test(html), 'the job asks each piece for its rank date');
    const solver = require(path.join(root, 'charm-nest-solver.js'));
    // a sheet 100 wide that holds two of three 38-wide orders: the oldest two when none is released, the released one and the oldest when one is
    const mkp = (id, order, date, extra) => { const w = 38; return { id, order, orderDate: O.rankDate(Object.assign({ orderDate: date }, extra || {})), w, h: 20, scale: 1, bits: new Uint8Array(w * 20).fill(1), areaPt2: w * 20, centerPt: [w / 2, 10] }; };
    const job = pieces => ({ sheet: { wPt: 100, hPt: 20, insetPt: 0 }, pieces, angles: [0], fineRes: 1, coarseRes: 1, clearancePt: 0, timeBudgetMs: 300, maxTrials: 2, maxFill: .80 });
    return Promise.all([
      solver.solve(job([mkp('a', 'A', SEC - 30), mkp('b', 'B', SEC - 20), mkp('rel', 'R', SEC + 70, { frontAt: T0 })])),
      solver.solve(job([mkp('a', 'A', SEC - 30), mkp('b', 'B', SEC - 20), mkp('c', 'C', SEC + 70)]))
    ]).then(([withRel, without]) => {
      const placed = r => r.placements.map(p => p.id).sort();
      assert.deepEqual(placed(without), ['a', 'b'], 'without a released order the two oldest are placed: ' + placed(without));
      assert(placed(withRel).includes('rel'), 'the released order is placed first, though it is the newest: ' + placed(withRel));
      assert.equal(placed(withRel).length, 2, 'the sheet still holds two orders (density as before)');
      ok.push('the solver (browser and server read the same job): the released order is placed first on a sheet with room for two of three');
    });
  }
}).then(() => {

// 7 · the Merge order and the sheet window's waiting candidates
  assert(/const byDate = \(a, b\) => O\.rankDate\(a\) - O\.rankDate\(b\);/.test(bridge), 'Merge sorts by the rank date');
  const cand = slice(sheetwin, '  function candidatesFor(target) {', '  function plateGrid(target) {');
  const mkchar = (rid, id, placedAt, extra) => Object.assign({ id, poolId: rid + '_x_1', order: rid, bits: 'AA', w: 3, orderDate: placedAt, areaPt2: 10 }, extra);
  const sheetA = { sheetId: 'A', metal: 'gold', placements: [], charms: [mkchar('1001', 'c1', SEC - 90), mkchar('1002', 'c2', SEC - 80), mkchar('1003', 'c3', SEC + 500, { frontAt: T0 })] }, target = { sheetId: 'T', metal: 'gold', placements: [{ id: 'z' }], charms: [] };
  const rowsBy = { '1001': [{ order: { receiptId: '1001', createTs: SEC - 90 }, state: 'pooled' }], '1002': [{ order: { receiptId: '1002', createTs: SEC - 80 }, state: 'pooled' }], '1003': [{ order: { receiptId: '1003', createTs: SEC + 500 }, frontAt: T0, state: 'pooled' }] };
  const ctx = { allSheets: () => [sheetA, target], movableFrom: () => true, activeCharms: s => s.charms, ridOf: c => String(c.order), Orders: { rows: () => [].concat(...Object.values(rowsBy)) }, Cancelled: { has: () => false }, window: { CharmNestOrders: O, Orders: null }, CharmNestOrders: O };
  ctx.window.Orders = ctx.Orders; ctx.window.CharmNestOrders = O;
  vm.createContext(ctx); vm.runInContext(cand + ';this.candidatesFor = candidatesFor;', ctx);
  const list = Array.from(ctx.candidatesFor(target), k => k.rid);
  assert.deepEqual(list, ['1003', '1001', '1002'], 'a waiting order released from hold is the first suggestion for a freed spot, then the oldest: ' + list);
  sheetA.charms[2].frontAt = 0; rowsBy['1003'][0].frontAt = 0;
  assert.deepEqual(Array.from(ctx.candidatesFor(target), k => k.rid), ['1001', '1002', '1003'], 'without it, oldest first as before');
  ok.push('the sheet window: a waiting order released from hold is the first suggestion for a freed spot; the Merge order keeps it first');

  console.log('order-release-queue OK\n  ' + ok.join('\n  '));
}).catch(e => { console.error(e); process.exit(1); });
