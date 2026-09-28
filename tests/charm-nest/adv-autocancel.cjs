// Adversarial (task G, AutoCancel on uncut sheets): a cancelled order must never come back onto a sheet.
//   1. Pool.addAll: a cancel that lands while the run is making the order's lines up (its designs loading, its pool being
//      recorded) is taken off Orders by AutoCancel meanwhile; the line it was making up must not then go onto a sheet,
//      and the pool records just written for it are let go. An order already cancelled is not made up at all.
//   2. Arrivals.merge: an order cancelled after the check picked it (the Recall inbox's "N new orders" chip merges a list
//      picked minutes earlier; a check's arrivalRecord call) is not added to Orders again.
// The real module code runs in a vm; the page and the cloud are small stand-ins. No network.
//   node tests/charm-nest/adv-autocancel.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const O = require('../../charm-nest-orders.js');
const source = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
const slice = (from, to) => { const a = source.indexOf(from), b = source.indexOf(to, a); assert(a >= 0 && b > a, 'slice ' + from); return source.slice(a, b); };

/* ── 1 · Pool.addAll ── */
async function poolTest() {
  const calls = { poolPut: [], poolUpdate: [] };
  const entries = new Map([['OK', { sku: 'OK', aiPath: 'ok.ai', masterHash: 'm1' }]]);
  const page = { metal: 'silver', charms: [], placements: [], status: 'idle' };
  let rows = [];
  const cancelled = new Set();
  const mk = (id, sku) => {
    const order = { receiptId: id, createTs: +id, updateTs: 1 }, line = { transactionId: 't' + id, sku, title: sku };
    const r = { key: id + '/t' + id, order, line, spec: null, problems: [], state: 'pulled', reason: null, poolIds: [], engrave: null };
    rows.push(r); return r;
  };
  const small = { charms: [{ areaPt2: 400, widthPt: 20, heightPt: 20, hash: 'h' }] };
  let onPoolPut = null;
  const ctx = {
    window: { CNProgress: { start: () => ({ set() {}, end() {} }) }, LiveNest: { intakePage: () => page, closed: () => false }, Cancelled: { has: rid => cancelled.has(String(rid)) } },
    Date, JSON, Map, Set, Promise, Math, Object, Array, String, Number, Error, console,
    B: { pool: { rows: new Map(), sources: new Map([['ok.ai', small]]) }, master: { entries } }, S: { cloud: { ok: true }, settings: { insetPt: 0, maxFill: 0.8, silhouetteRes: 6, minPt: 6 }, sheets: { silver: { active: 0, pages: [page] } }, poolSources: {} },
    O, MM: 25.4 / 72,
    Master: { entryFor: sku => entries.get(String(sku).toUpperCase()) || null, fetchEntry: async () => null, skuRegex: () => /x/ },
    Orders: { rows: () => rows, interpretAll() { for (const r of rows) { r.spec = { designSku: r.line.sku, material: 'silver', quantity: 1, size: null, problems: [] }; r.problems = []; } }, render() {}, lineRecord: r => [r.key, r.state] },
    Gate: { plan: async list => ({ take: new Set(list.map(r => r.key)), wait: new Map() }), afterPool: async () => {} },
    Review: { syncOrderItems() {}, problemText: p => p.kind },
    RunCtl: { save: async () => {} },
    api: async (fn, body) => {
      if (body.op === 'poolPut') { calls.poolPut.push(body.pools.map(p => p.poolId)); if (onPoolPut) { const f = onPoolPut; onPoolPut = null; f(); } return {}; }
      if (body.op === 'poolUpdate') { calls.poolUpdate.push([body.poolIds, body.patch]); return {}; }
      return {};
    },
    CharmNestAssets: { bytes: async () => { throw new Error('unreachable'); } }, P: {},
    stockFor: () => ({ wPt: 1000, hPt: 1000 }), labelOf: m => m, allSheets: () => [page], pagesOf: () => [page], addPage: () => page,
    sheetDirty() {}, renderCard() {}, renderRail() {}, updateTopSub() {}, refreshAllCards() {}, agent() {},
  };
  ctx.CNProgress = ctx.window.CNProgress; ctx.LiveNest = ctx.window.LiveNest; ctx.Cancelled = ctx.window.Cancelled;
  vm.createContext(ctx);
  vm.runInContext(slice('const Pool = window.Pool = (() => {', '/* Carry-forward'), ctx);
  const Pool = ctx.window.Pool;
  const run = { runId: 'run', lines: {} };
  const ridOn = () => page.charms.map(c => String(c.poolId || '').split('_')[0]);

  // X and Y are being made up by the run's pool step; X is cancelled on Etsy while its pool is being recorded:
  // AutoCancel (it does not wait for the run's pool step, only for an arrival) marks its line gone and drops it
  const X = '4200000001', Y = '4200000002';
  const x = mk(X, 'OK'), y = mk(Y, 'OK');
  onPoolPut = () => { cancelled.add(X); x.state = 'gone'; x.reason = 'cancelled on Etsy'; rows = rows.filter(r => r !== x); };
  await Pool.addAll(run);
  assert.deepEqual(ridOn(), [Y], 'the order cancelled meanwhile does not go onto the sheet; the other one does: ' + JSON.stringify(ridOn()));
  assert.equal(x.state, 'gone', 'its line stays gone');
  assert(!ctx.B.pool.rows.has(`${X}_t${X}_1`), 'its piece is not kept as pooled here');
  await new Promise(r => setImmediate(r));
  assert(calls.poolUpdate.some(([ids, p]) => ids.includes(`${X}_t${X}_1`) && p.state === 'abandoned'), 'the pool record just written for it is let go: ' + JSON.stringify(calls.poolUpdate));
  assert.equal(y.state, 'pooled');

  // an order already cancelled (its record read, AutoCancel not through yet) is not made up at all
  const Z = '4200000003'; const z = mk(Z, 'OK'); cancelled.add(Z);
  const puts = calls.poolPut.length;
  await Pool.addAll(run);
  assert(!ridOn().includes(Z), 'a cancelled order is never placed');
  assert(!calls.poolPut.slice(puts).some(ids => ids.some(id => id.startsWith(Z))), 'nor recorded in the pool');
  assert.notEqual(z.state, 'pooled');
  console.log('  ok · Pool.addAll: a cancel landing mid-way keeps the order off the sheet; a cancelled order is not made up');
}

/* ── 2 · Arrivals.merge ── */
async function mergeTest() {
  const store = new Map(), rows = [], cancelled = new Set(), added = [], toasts = [];
  const ctx = {
    WORKSPACE_SANDBOX: false, JSON, Map, Set, Promise, Math, Object, Array, String, Number, Date, console,
    localStorage: { getItem: k => store.get(k) ?? null, setItem: (k, v) => store.set(k, String(v)) },
    window: { addEventListener() {}, Cancelled: { has: rid => cancelled.has(String(rid)) } },
    document: { querySelectorAll: () => [], getElementById: () => null, querySelector: () => null },
    S: { cloud: { ok: false }, settings: {} }, Sandbox: { streaming: () => false, on: () => false }, Recall: { on: () => false },
    B: { orders: { rows, byKey: new Map() }, run: null },
    Orders: { rows: () => rows, interpretAll() {}, render() {} }, O, TL: { pulled: o => added.push(String(o.receiptId)) },
    Review: { render() {} }, Engrave: { render() {} }, refreshAllCards() {}, Session: { schedule() {} }, ListMedia: { prepare() {} },
    toast: t => { toasts.push(t); return null; }, notifyPerson() {}, RunCtl: { save: async () => {} }, api: async () => { throw new Error('no cloud in this test'); },
  };
  ctx.Cancelled = ctx.window.Cancelled; ctx.B.orders.rows = rows;
  vm.createContext(ctx);
  vm.runInContext(slice('const Arrivals = window.Arrivals = (() => {', '/* A new batch tries the newest open sheet'), ctx);
  const Arrivals = ctx.window.Arrivals;
  const order = id => ({ receiptId: id, createTs: 1, updateTs: 1, lines: [{ transactionId: 't' + id, sku: 'OK' }] });
  // the inbox was picked by a check a few minutes ago; one of its orders has been cancelled since (AutoCancel's poll read it)
  const A = '4300000001', C = '4300000002';
  const inbox = [order(A), order(C)];
  cancelled.add(C);
  await Arrivals.merge(inbox);
  const got = [...new Set(rows.map(r => String(r.order.receiptId)))];
  assert.deepEqual(got, [A], 'the cancelled order is not added to Orders again: ' + JSON.stringify(got));
  // (and the notice counts only what came in: it said "2 new orders arrived", the cancelled one counted, adv area 2)
  assert.deepEqual(toasts, ['1 new order arrived'], 'the arrivals notice counts only the order added: ' + JSON.stringify(toasts));
  console.log('  ok · Arrivals.merge: an order cancelled after it was picked is not added back, nor counted as arrived');
}

(async () => {
  await poolTest();
  await mergeTest();
  console.log('adv-autocancel: all passed');
})().catch(e => { console.error(e); process.exit(1); });
