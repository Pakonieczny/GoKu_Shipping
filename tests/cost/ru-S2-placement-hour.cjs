// RU-S2: what one open Sorter tab costs the database per hour through the placement feed, measured end to end on the REAL code: the page's own OrderPieces.load
// (charm-nest-order-pieces.js, makePage over a fake page object) calling the REAL charmNestLibrary handler over FC1's meter and its in-memory Firestore, on a fake clock,
// exactly as charm-nest-placement-feed.js calls it (a forced read of the rows on screen at every tick).
// It shows why the rows watched in a quiet window matter (ru-S2-placement-feed.cjs proves the feed now watches the 16 rows laid out on screen, not 27 with the 700 px band):
//   node tests/cost/ru-S2-placement-hour.cjs
// Numbers it prints are MEASURED reads per tab-hour with nothing changing in the cloud (one ask per 30 rows, plus the 5-minute "deep" read the page makes without ifGen),
// and with the placement counter raised 20 times an hour by writes to other orders (a raise resets the deep-read timer, so a busy hour costs about the same as a quiet one).
'use strict';
const assert = require('assert'), path = require('path');
const meter = require('./meter.cjs');
const say = s => process.stdout.write(s + '\n');
const root = path.join(__dirname, '../..');
const PER = 3, SHEETS = 6, N = 60, rid = i => String(4190000000 + i);

(async () => {
  const m = meter.create();
  const seed = {}, sheets = Array.from({ length: SHEETS }, (_, i) => ({ id: 'sheet-ru2-' + i, poolIds: [], orders: [] }));
  for (let i = 0; i < N; i++) for (let k = 1; k <= PER; k++) {
    const r = rid(i), poolId = `${r}_${r}0${k}_1`, s = sheets[(i * 2 + k) % SHEETS]; s.poolIds.push(poolId); if (!s.orders.includes(r)) s.orders.push(r);
    seed['Charm_Pool/' + poolId] = { poolId, orderId: r, transactionId: r + '0' + k, lineKey: r + '_' + r + '0' + k, sku: 'DUCK-' + k, material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: s.id, sheetName: s.id, setId: 'set-ru2', runId: 'run-ru2', charmHash: 'h'.repeat(40), masterHash: 'm'.repeat(40), aiPath: 'charmnest/masters/duck/size-1.ai', size: 'S', form: 'charm', chain: null, orderDate: 1790000000000, arrivedAt: 1790000000000, engrave: false, updateTs: 1790000000 };
  }
  for (const s of sheets) {
    for (let j = 0; j < 40; j++) { const r = rid(1000 + j + 50 * sheets.indexOf(s)); s.poolIds.push(r + '_x_1'); if (!s.orders.includes(r)) s.orders.push(r); }
    seed['Charm_Nest_Sheets/' + s.id] = { id: s.id, metal: 'gold', sheetIndex: 1, setId: 'set-ru2', setSeq: 1, folder: s.id, fileBase: s.id, day: '2026-10-07', status: 'written', orders: s.orders, poolIds: s.poolIds, page: 1, placements: 'p'.repeat(60000), charms: 'c'.repeat(40000) };
  }
  m.db.seed(seed); m.install();
  const lib = require(path.join(root, 'netlify/functions/charmNestLibrary.js'));
  const Core = require(path.join(root, 'charm-nest-order-pieces.js'));
  let NOW = Date.UTC(2026, 9, 19, 9, 0, 0); const realNow = Date.now; Date.now = () => NOW;
  const warn = console.warn; console.warn = () => {};
  const win = { CN: { api: async (fn, body) => JSON.parse((await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) })).body) } };

  /** One tab-hour: a forced read of the first n orders every `every` seconds; `raises` writes an hour that raise the placement counter (to other orders' pool rows). */
  async function hour(n, every, raises) {
    const OP = Core.makePage(win), ids = Array.from({ length: n }, (_, i) => rid(i));
    await OP.load(ids);
    const s0 = m.snapshot(), gap = raises ? Math.floor(3600 / raises) : 0;
    for (let t = every; t <= 3600; t += every) {
      NOW += every * 1000;
      if (gap && t % gap < every) await m.op('raise', () => win.CN.api('charmNestLibrary', { op: 'poolUpdate', poolIds: [rid(N + 5) + '_x_1'], patch: { note: 'x' + t }, by: 'T' }));
      await m.op('tick', () => OP.load(ids, { force: true }));
    }
    return m.since(s0).reads;
  }
  const rows = [
    ['ACTIVE 4 s, 27 rows (15 on screen + the 700 px band)', 27, 4], ['ACTIVE 4 s, 48 rows', 48, 4],
    ['RECENT 15 s, 27 rows (before: band kept)', 27, 15], ['RECENT 15 s, 16 rows (after: only what is on screen)', 16, 15],
    ['IDLE 60 s, 27 rows (before)', 27, 60], ['IDLE 60 s, 16 rows (after)', 16, 60]
  ];
  say('one tab, nothing changing in the cloud, one hour (OrderPieces only; the cancelled-orders feed adds about 1.2k / 0.5k / 0.4k):');
  const got = {};
  for (const [label, n, every] of rows) { got[label] = await hour(n, every, 0); say('  ' + label.padEnd(58) + String(got[label]).padStart(6) + ' reads/hour'); }
  say('same, with 20 placement-counter raises an hour from writes to other orders:');
  const busy27 = await hour(27, 15, 20), busy16 = await hour(16, 15, 20);
  say('  ' + 'RECENT 15 s, 27 rows (before)'.padEnd(58) + String(busy27).padStart(6) + ' reads/hour'); say('  ' + 'RECENT 15 s, 16 rows (after)'.padEnd(58) + String(busy16).padStart(6) + ' reads/hour');
  Date.now = realNow; console.warn = warn; m.uninstall();

  // the claims, as assertions (fixtures are the fc3 shape: 3 pieces per order over six sheets)
  assert(got[rows[3][0]] < got[rows[2][0]] * 0.8, 'a quiet window reading the 16 rows on screen costs at least 20% less than reading 27 (RECENT)');
  assert(got[rows[5][0]] < got[rows[4][0]] * 0.8, '... and in IDLE');
  assert(busy16 < busy27 * 0.8, '... and when other people\'s writes keep raising the counter');
  assert(got[rows[0][0]] < 3000 && got[rows[1][0]] < 4500, 'an active tab stays within about 2.1k (27 rows) / 3.9k (48 rows) reads an hour');
  say('ru-S2-placement-hour: passed');
})().catch(e => { console.error(e); process.exit(1); });
