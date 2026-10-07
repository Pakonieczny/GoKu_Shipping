// FC3 (Firestore cost): what one open Sorter tab costs the database through the placement feed (charm-nest-placement-feed.js ->
// OrderPieces.load -> charmNestLibrary getOrderPieces, every 2.5 s while the tab is visible, up to 48 orders = two asks of 30 and 18),
// before and after the placement counter, and what the stations' per-order notes check reads. Real handlers, FC1's meter and its in-memory Firestore.
//   node tests/cost/fc3-orders-feed-cost.cjs
'use strict';
const assert = require('assert'), path = require('path');
const meter = require('./meter.cjs');
const say = s => process.stdout.write(s + '\n');
const root = path.join(__dirname, '../..');

const N = 48, PER = 3, SHEETS = 6, rid = i => String(4190000000 + i);
(async () => {
  const m = meter.create();
  /* a shop's worth: 48 orders on screen, three pieces each, over six sheets that each carry about 60 pieces of 30 orders */
  const seed = {}, sheets = Array.from({ length: SHEETS }, (_, i) => ({ id: 'sheet-fc3-' + i, poolIds: [], orders: [] }));
  for (let i = 0; i < N; i++) for (let k = 1; k <= PER; k++) {
    const r = rid(i), poolId = `${r}_${r}0${k}_1`, s = sheets[(i * 2 + k) % SHEETS]; s.poolIds.push(poolId); if (!s.orders.includes(r)) s.orders.push(r);
    seed['Charm_Pool/' + poolId] = { poolId, orderId: r, transactionId: r + '0' + k, lineKey: r + '_' + r + '0' + k, sku: 'DUCK-' + k, material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: s.id, sheetName: s.id, setId: 'set-fc3', runId: 'run-fc3', charmHash: 'h'.repeat(40), masterHash: 'm'.repeat(40), aiPath: 'charmnest/masters/duck/size-1.ai', size: 'S', form: 'charm', chain: null, orderDate: 1790000000000, arrivedAt: 1790000000000, engrave: false, updateTs: 1790000000 };
  }
  for (const s of sheets) {
    for (let j = 0; j < 40; j++) { const r = rid(1000 + j + 50 * sheets.indexOf(s)); s.poolIds.push(r + '_x_1'); if (!s.orders.includes(r)) s.orders.push(r); }
    seed['Charm_Nest_Sheets/' + s.id] = { id: s.id, metal: 'gold', sheetIndex: 1, setId: 'set-fc3', setSeq: 1, folder: s.id, fileBase: s.id, day: '2026-10-07', status: 'written', orders: s.orders, poolIds: s.poolIds, page: 1, placements: 'p'.repeat(60000), charms: 'c'.repeat(40000) };   // (the heavy fields a feed ask never reads)
  }
  // the orders' records of the stations: a Staff Note on a few, and the rest of the record (messages, label times) on all
  for (let i = 0; i < 100; i++) seed['Brites_Orders/' + rid(i)] = Object.assign({ 'Order Number': rid(i), 'Client Name': 'Customer ' + i, 'Brites Messages': 'x'.repeat(2500), 'Shipping Label Timestamps': ['2026-10-06T12:00:00Z'], 'Employee Name': 'Paul' }, i % 10 === 0 ? { 'Staff Note': 'call the buyer' } : {});
  m.db.seed(seed);
  m.install();
  const lib = require(path.join(root, 'netlify/functions/charmNestLibrary.js')), orders = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
  const call = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }); return JSON.parse(r.body); };
  const A = Array.from({ length: 30 }, (_, i) => rid(i)), B = Array.from({ length: 18 }, (_, i) => rid(30 + i));
  const tick = async (asks, label) => { const s = m.snapshot(); const out = []; for (const a of asks) out.push(await m.op(label, () => call(a))); return { d: m.since(s), out }; };

  /* 1 · one tick of the feed */
  const full = await tick([{ op: 'getOrderPieces', orderIds: A }, { op: 'getOrderPieces', orderIds: B }], 'feed.full');
  const [a, b] = full.out; assert(a.rev && b.rev && a.gen === 0, 'gen answered');
  const idle = await tick([{ op: 'getOrderPieces', orderIds: A, ifRev: a.rev, ifGen: a.gen }, { op: 'getOrderPieces', orderIds: B, ifRev: b.rev, ifGen: b.gen }], 'feed.idle');
  assert(idle.out.every(x => x.unchanged), 'unchanged');
  const hr = d => meter.perHour(d, 3600 / 2.5);
  const H = { full: hr(full.d), idle: hr(idle.d) };
  say('placement feed, one open tab, 48 orders on screen, a tick every 2.5 s (1,440 an hour):');
  say('  every tick a full read (before): ' + full.d.reads + ' reads, ' + Math.round(full.d.bytes / 1024) + ' KB a tick = ' + H.full.reads.toLocaleString() + ' reads and ' + Math.round(H.full.bytes / 1048576) + ' MB an hour = $' + H.full.usd.toFixed(3) + ' an hour');
  say('  nothing changed (after):         ' + idle.d.reads + ' reads, ' + idle.d.bytes + ' bytes a tick = ' + H.idle.reads.toLocaleString() + ' reads and ' + (H.idle.bytes / 1048576).toFixed(2) + ' MB an hour = $' + H.idle.usd.toFixed(4) + ' an hour');
  meter.assertMax(idle.d, { reads: 2, bytes: 100 }, 'a feed tick when nothing changed');
  assert(H.full.usd / H.idle.usd > 50, 'at least fifty times cheaper when nothing changed');

  /* 2 · a change costs one full read of the orders asked about, then the page is cheap again */
  await call({ op: 'poolUpdate', poolIds: [`${rid(3)}_${rid(3)}01_1`], patch: { state: 'abandoned', sheetId: null, setId: null, heldBy: 'Paul', heldReason: 'on hold', heldAt: 1790000009999 }, by: 'Paul' });
  const moved = await tick([{ op: 'getOrderPieces', orderIds: A, ifRev: a.rev, ifGen: a.gen }], 'feed.changed');
  assert(moved.out[0].orders && moved.out[0].gen === 1, 'the change is read in full');
  const after = await tick([{ op: 'getOrderPieces', orderIds: A, ifRev: moved.out[0].rev, ifGen: moved.out[0].gen }], 'feed.after');
  assert(after.out[0].unchanged && after.d.reads === 1);
  say('  a hold on another computer:      one full read (' + moved.d.reads + ' reads), then ' + after.d.reads + ' read a tick again');

  /* 3 · the stations' "which of these orders carry a Staff Note" (100 ids a request, ten per query): only the note crosses the wire */
  const ids = Array.from({ length: 100 }, (_, i) => rid(i));
  const s0 = m.snapshot();
  const r = await m.op('staffNotesFor', () => orders.handler({ httpMethod: 'GET', headers: {}, queryStringParameters: { staffNotesFor: ids.join(',') } }));
  const out = JSON.parse(r.body), d = m.since(s0);
  assert.strictEqual(out.orderNumbers.length, 10, 'the ten orders with a note');
  say('staffNotesFor, 100 open orders: ' + d.reads + ' reads, ' + d.bytes.toLocaleString() + ' bytes (the whole records were ' + (100 * meter.sizeOf(seed['Brites_Orders/' + rid(1)])).toLocaleString() + ' bytes)');
  assert(d.bytes < 100 * 120, 'only the note field is read');
  m.uninstall();
  say('fc3-orders-feed-cost: passed');
})().catch(e => { console.error(e); process.exit(1); });
