// FC3b (Firestore cost): what one open Sorter tab costs the database through Cancelled.load(true), which the placement feed forces every 2.5 s
// (1,440 times an hour per visible tab): cancelList { idsOnly, after: cursor } = a query of the records written since plus a count() of the
// collection. Before the cancel counter that was about 1 + ceil(N/1000) reads a tick whatever had changed; now a counter that has not moved
// is answered for ONE read (Charm_Nest_Rev/cancel, raised in the same commit as every write of a cancel record). Real handler (charmNestLibrary
// cancelList / cancelPut / cancelRestore, _orderCancel.fromReceipts), FC1's meter and its in-memory Firestore.
//   node tests/cost/fc3b-cancel-cost.cjs
'use strict';
const assert = require('assert'), path = require('path');
const meter = require('./meter.cjs');
const say = s => process.stdout.write(s + '\n');
const root = path.join(__dirname, '../..');

const N = 5000;   // cancel records kept (Etsy's cancels from the sweep and the mirror are kept for good too: thousands)
(async () => {
  const m = meter.create();
  const seed = {}, t0 = Date.now() - 400 * 86400e3;
  for (let i = 0; i < N; i++) { const id = String(3000000000 + i); seed['Charm_Nest_Cancelled/' + id] = { orderId: id, by: 'Etsy', source: 'etsy', why: 'Cancelled on Etsy', at: t0 + i * 60e3, createdAt: new meter.Timestamp(Math.floor((t0 + i * 60e3) / 1000), 0), buyer: 'Buyer ' + i, lines: [{ transactionId: '9' + i, sku: 'DUCK-1', title: 'Duck charm', quantity: 1, material: 'gold' }], sheets: [] }; }
  m.db.seed(seed);
  m.install();
  const lib = require(path.join(root, 'netlify/functions/charmNestLibrary.js')), OrderCancel = require(path.join(root, 'netlify/functions/_orderCancel.js'));
  const call = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }); return JSON.parse(r.body); };
  const ask = async (body, label) => { const s = m.snapshot(); const out = await m.op(label, () => call(body)); return { out, d: m.since(s) }; };

  const first = await ask({ op: 'cancelList', idsOnly: true, track: true, wantGen: true }, 'cancel.first');       // the page's whole first read
  assert.strictEqual(first.out.ids.length, N); assert.strictEqual(first.out.gen, 0); const cursor = first.out.cursor;
  say('cancel records kept: ' + N.toLocaleString() + '; the first whole read costs ' + first.d.reads.toLocaleString() + ' reads (once per page load)');

  /* one tick of the feed while nothing changed */
  const before = await ask({ op: 'cancelList', idsOnly: true, after: cursor, limit: 500 }, 'cancel.tick.before');                    // an older page: no ifGen
  const after = await ask({ op: 'cancelList', idsOnly: true, after: cursor, limit: 500, wantGen: true, ifGen: 0 }, 'cancel.tick.after');
  assert.strictEqual(after.out.unchanged, true); assert.deepStrictEqual(after.out.ids, []);
  const deep = await ask({ op: 'cancelList', idsOnly: true, after: cursor, limit: 500, wantGen: true }, 'cancel.tick.deep');          // the once-a-minute deep read
  assert.strictEqual(deep.out.gen, 0); assert.strictEqual(deep.out.total, N);
  const reads = d => d.reads + d.aggs;
  const per = (d, n) => meter.perHour(Object.assign({}, d, { reads: reads(d), aggs: 0 }), n);
  const H = { before: per(before.d, 1440), after: per(after.d, 1380), deep: per(deep.d, 60) };
  const hourAfter = H.after.reads + H.deep.reads;
  say('one open tab, a tick every 2.5 s (1,440 an hour), nothing changed:');
  say('  before: ' + reads(before.d) + ' reads a tick = ' + H.before.reads.toLocaleString() + ' reads an hour');
  say('  after:  ' + reads(after.d) + ' read a tick, plus a deep read (' + reads(deep.d) + ' reads) once a minute = ' + hourAfter.toLocaleString() + ' reads an hour (' + (H.before.reads / hourAfter).toFixed(1) + ' times fewer)');
  assert.strictEqual(reads(after.d), 1, 'an idle tick is one read');
  assert(reads(before.d) >= 1 + Math.floor(N / 1000), 'before: query plus count()');
  meter.assertMax(after.d, { reads: 1, bytes: 100 }, 'an idle cancel tick');
  assert(H.before.reads / hourAfter > 3, 'at least three times fewer reads an hour');

  /* every writer raises the counter, in its own commit, and the reader sees the news at its next tick */
  const rev = () => (m.db.dump ? (m.db.dump()['Charm_Nest_Rev/cancel'] || {}).n : undefined);
  await call({ op: 'cancelPut', orderId: '4100000001', by: 'Paul', why: 'buyer asked', record: {} });
  const c1 = await ask({ op: 'cancelList', idsOnly: true, after: cursor, limit: 500, wantGen: true, ifGen: 0 }, 'cancel.tick.changed');
  assert.deepStrictEqual(c1.out.ids, ['4100000001']); assert.strictEqual(c1.out.gen, 1); assert(!c1.out.unchanged);
  const c2 = await ask({ op: 'cancelList', idsOnly: true, after: c1.out.cursor, limit: 500, wantGen: true, ifGen: 1 }, 'cancel.tick.after2');
  assert.strictEqual(c2.out.unchanged, true); assert.strictEqual(reads(c2.d), 1);
  const rc = x => ({ receipt_id: 5100000000 + x, status: 'Canceled', updated_timestamp: 1790000000 + x, created_timestamp: 1789000000, buyer_name: 'B' + x, transactions: [{ transaction_id: 9000 + x, sku: 'S', title: 'T', quantity: 1 }] });
  const FV = meter.FieldValue;
  const mir = await OrderCancel.fromReceipts(m.db, FV, [rc(1), rc(2)]);   // the Etsy mirror's hook, outside the handler
  assert.strictEqual(mir.created, 2);
  const c3 = await ask({ op: 'cancelList', idsOnly: true, after: c1.out.cursor, limit: 500, wantGen: true, ifGen: 1 }, 'cancel.tick.mirror');
  assert.strictEqual(c3.out.gen, 2); assert.deepStrictEqual(c3.out.ids.sort(), ['5100000001', '5100000002']);
  await call({ op: 'cancelRestore', orderId: '5100000001', by: 'Tess' });
  const c4 = await ask({ op: 'cancelList', idsOnly: true, after: c3.out.cursor, limit: 500, wantGen: true, ifGen: 2 }, 'cancel.tick.restore');
  assert.strictEqual(c4.out.gen, 3); assert(!c4.out.unchanged); assert.strictEqual(c4.out.total, N + 2, 'the count shows the restore to the page');
  say('  a cancel by a person, Etsy\'s cancels from the mirror and a restore each raise the counter once; the next tick reads the news (' + reads(c1.d) + ' reads) and is cheap again (1)');
  m.uninstall();
  say('fc3b-cancel-cost: passed');
})().catch(e => { console.error(e); process.exit(1); });
