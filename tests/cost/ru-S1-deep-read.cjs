// RU-S1 (Firestore cost, 18 Oct 2026): the placement feed's "deep" read. OrderPieces.load (charm-nest-order-pieces.js) sends the placement counter it holds (ifGen) with every
// forced read, so a counter that has not moved costs one small read; but every GEN_DEEP (five minutes) it asks WITHOUT it, and the server reads all the documents of the orders
// on screen to learn that nothing changed (61 to 176 documents, ~150 KB for 48 orders): 12 times an hour in every tab with orders on screen, a screen left on over a night
// included. It is the net under "a write nothing raised the counter for". Now a tab that has had no input for ten minutes makes it every thirty minutes instead, and the first
// input brings the five minutes back (a deep read is then due at the next ask). A caller may name its own interval (opts.deepMs).
// The real OrderPieces.load (makePage over a fake page with a document whose input events the test fires) calling the REAL charmNestLibrary handler over FC1's meter, on a fake clock.
//   node tests/cost/ru-S1-deep-read.cjs
'use strict';
const assert = require('assert'), path = require('path');
const meter = require('./meter.cjs');
const say = s => process.stdout.write(s + '\n');
const root = path.join(__dirname, '../..');
const PER = 3, SHEETS = 6, N = 48, rid = i => String(4190000000 + i);

(async () => {
  const m = meter.create();
  const seed = {}, sheets = Array.from({ length: SHEETS }, (_, i) => ({ id: 'sheet-ru1-' + i, poolIds: [], orders: [] }));
  for (let i = 0; i < N; i++) for (let k = 1; k <= PER; k++) {
    const r = rid(i), poolId = `${r}_${r}0${k}_1`, s = sheets[(i * 2 + k) % SHEETS]; s.poolIds.push(poolId); if (!s.orders.includes(r)) s.orders.push(r);
    seed['Charm_Pool/' + poolId] = { poolId, orderId: r, transactionId: r + '0' + k, lineKey: r + '_' + r + '0' + k, sku: 'DUCK-' + k, material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: s.id, sheetName: s.id, setId: 'set-ru1', runId: 'run-ru1', charmHash: 'h'.repeat(40), masterHash: 'm'.repeat(40), aiPath: 'charmnest/masters/duck/size-1.ai', size: 'S', form: 'charm', chain: null, orderDate: 1790000000000, arrivedAt: 1790000000000, engrave: false, updateTs: 1790000000 };
  }
  for (const s of sheets) {
    for (let j = 0; j < 40; j++) { const r = rid(1000 + j + 50 * sheets.indexOf(s)); s.poolIds.push(r + '_x_1'); if (!s.orders.includes(r)) s.orders.push(r); }
    seed['Charm_Nest_Sheets/' + s.id] = { id: s.id, metal: 'gold', sheetIndex: 1, setId: 'set-ru1', setSeq: 1, folder: s.id, fileBase: s.id, day: '2026-10-07', status: 'written', orders: s.orders, poolIds: s.poolIds, page: 1, placements: 'p'.repeat(60000), charms: 'c'.repeat(40000) };
  }
  m.db.seed(seed); m.install();
  const lib = require(path.join(root, 'netlify/functions/charmNestLibrary.js'));
  const Core = require(path.join(root, 'charm-nest-order-pieces.js'));
  let NOW = Date.UTC(2026, 9, 19, 9, 0, 0); const realNow = Date.now; Date.now = () => NOW;
  const warn = console.warn; console.warn = () => {};

  /** A page: window + document whose input listeners the test can fire, the real OrderPieces on it, and the asks it sends (ifGen present or not). */
  function page(withDocument) {
    const on = {}, asks = [];
    const hook = (ev, fn) => { (on[ev] = on[ev] || []).push(fn); };
    const win = { CN: { api: async (fn, body) => { asks.push(body); return JSON.parse((await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) })).body); } } };
    if (withDocument) { win.document = { hidden: false, addEventListener: hook }; win.addEventListener = hook; }
    const OP = Core.makePage(win);
    return { OP, asks, fire: ev => (on[ev] || []).forEach(f => f()), hook: on };
  }
  const ids = n => Array.from({ length: n }, (_, i) => rid(i));
  /** One tab: a forced read every `every` seconds for `secs` seconds; `input(t)` is called each tick (seconds since the load) and may fire an input event. Returns the reads of each hour and the deep asks. */
  async function run(label, { withDocument = true, n = 16, every = 60, secs = 7200, input = null, opts = {} } = {}) {
    const p = page(withDocument), my = ids(n);
    await p.OP.load(my);
    const out = { hours: [0, 0, 0], deep: [], label };
    let hr = 0, mark = m.snapshot();
    for (let t = every; t <= secs; t += every) {
      NOW += every * 1000;
      if (input) input(t, p);
      const before = p.asks.length;
      await m.op('tick', () => p.OP.load(my, Object.assign({ force: true }, opts)));
      for (const a of p.asks.slice(before)) if (a.ifRev && a.ifGen === undefined) out.deep.push(t);
      if (t % 3600 === 0) { const d = m.since(mark); out.hours[hr++] = d.reads; mark = m.snapshot(); }
    }
    return out;
  }
  const show = r => say('  ' + r.label.padEnd(66) + 'reads in hour 1 / hour 2: ' + String(r.hours[0]).padStart(5) + ' / ' + String(r.hours[1]).padStart(5) + '   deep reads at (min): ' + r.deep.map(t => Math.round(t / 60)).join(','));

  say('one tab, 16 rows on screen, a read every 60 s (the idle cadence), nothing changing in the cloud, two hours:');
  const active = await run('input every 2 minutes (a person working)', { input: (t, p) => { if (t % 120 === 0) p.fire('pointerdown'); } });
  const idle = await run('no input after the load (a screen left on)', {});
  const old = await run('a page with no document (the five minutes, as before)', { withDocument: false });
  show(active); show(idle); show(old);
  assert(active.deep.length >= 22 && active.deep.length <= 24, 'a tab somebody works in makes its deep read every five minutes (' + active.deep.length + ' in two hours)');
  assert(old.deep.length >= 22 && old.deep.length <= 24 && old.hours[1] === active.hours[1], 'a page with no document keeps the five minutes, and costs what a worked-in tab costs');
  assert(idle.deep.filter(t => t > 600).every((t, i, a) => i === 0 || t - a[i - 1] >= 1800), 'past ten minutes without input the deep read is made no oftener than every thirty minutes: ' + idle.deep.map(t => Math.round(t / 60)));
  assert(idle.deep.length <= 6, 'two idle hours make at most six deep reads (' + idle.deep.length + ')');
  assert(idle.hours[1] <= active.hours[1] * 0.35, 'the second idle hour costs a third of a worked-in hour or less: ' + idle.hours[1] + ' vs ' + active.hours[1]);

  say('the first input after a long quiet brings the five minutes back at once:');
  let back = null;
  const ret = await run('quiet for 90 minutes, then a click', { secs: 6000, input: (t, p) => { if (t === 5400) { back = p.asks.length; p.fire('pointerdown'); } } });
  show(ret);
  assert(ret.deep.some(t => t === 5400 || t === 5460), 'a deep read is made at the first ask after the click (minute ' + ret.deep.map(t => Math.round(t / 60)) + ')');
  const looked = await run('quiet for 90 minutes, then the tab is looked at (visibilitychange)', { secs: 6000, input: (t, p) => { if (t === 5400) p.fire('visibilitychange'); } });
  show(looked);
  assert(looked.deep.some(t => t === 5400 || t === 5460), 'looking at the tab does the same');
  say('a caller that names its interval is obeyed:');
  const named = await run('opts.deepMs = 600000 (ten minutes), no input', { opts: { deepMs: 600000 } });
  show(named);
  assert(named.deep.length >= 11 && named.deep.length <= 12, 'ten minutes is ten minutes, idle or not (' + named.deep.length + ' deep reads in two hours)');

  Date.now = realNow; console.warn = warn; m.uninstall();
  say('ru-S1-deep-read: passed');
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
