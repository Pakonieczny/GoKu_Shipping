// FC3b (Firestore cost): `firebaseOrders?designCompleted=1` (the Design Stations' first Refresh of a page session, the backfill) reads every document of the
// completion ledger, "Design_Completed Orders" (37,314 on 7 Oct 2026, growing forever). _designCompleted.js keeps an index of the same ids (32 shard documents
// + meta, written in the same batch as every ledger write) and answers from it: 33 reads. This test runs the REAL firebaseOrders handler on FC1's meter
// (in-memory Firestore) and checks: the answer is the ledger's, before and after the index exists; every writer keeps it in step; an un-complete, a lost
// index write and an edit by hand are all found; a race can never undo a completion or an un-complete; the sandbox keeps none; and what it costs.
//   node tests/cost/fc3b-completed-index.cjs            (N = 37,314 ledger documents, the live count of 7 Oct 2026; N=4000 node ... for a quick run)
'use strict';
const assert = require('assert'), path = require('path');
const meter = require('./meter.cjs');
const say = s => process.stdout.write(s + '\n');
const root = path.join(__dirname, '../..');
const N = Number(process.env.N) || 37314, id = i => String(2900000000 + i * 7);   // receipt ids, ten digits, spread
const ids0 = Array.from({ length: N }, (_, i) => id(i)).sort();

(async () => {
  const m = meter.create(), seed = {};
  for (const x of ids0) seed['Design_Completed Orders/' + x] = { orderId: x, completed: true, completedAt: new meter.Timestamp(1790000000, 0) };
  m.db.seed(seed);
  m.install();
  const DC = require(path.join(root, 'netlify/functions/_designCompleted.js')), orders = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
  const FV = meter.FieldValue;
  const get = async (qs, label) => { const s = m.snapshot(); const r = await m.op(label, () => orders.handler({ httpMethod: 'GET', headers: {}, queryStringParameters: qs })); return { r, body: JSON.parse(r.body), d: m.since(s) }; };
  const post = async (body, qs) => { const r = await orders.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: qs || {}, body: JSON.stringify(body) }); return { status: r.statusCode, body: JSON.parse(r.body) }; };
  const rd = d => d.reads + d.aggs;
  const doc = p => m.db.docs.get(p);
  const keys = pre => [...m.db.docs.keys()].filter(k => k.startsWith(pre));
  const indexDocs = () => keys('Design_Completed_Index/');
  const indexIds = () => [].concat(...indexDocs().filter(k => /\/s\d\d$/.test(k)).map(k => doc(k).ids || [])).sort();
  const ledgerIds = () => keys('Design_Completed Orders/').map(k => k.split('/')[1]).sort();

  /* 1 · no index yet: the first read is the whole ledger, as ever, and builds the index from it */
  const first = await get({ designCompleted: '1' }, 'dc.first');
  assert.deepStrictEqual(first.body.orderNumbers, ids0, 'the answer is every id of the ledger, in document-id order');
  assert(rd(first.d) >= N && rd(first.d) <= N + 4, 'the first read is the whole ledger (one read per document) and two small reads of the empty index: ' + rd(first.d));
  assert.strictEqual(doc('Design_Completed_Index/meta').ready, true, 'the index is ready after the build');
  assert.deepStrictEqual(indexIds(), ids0, 'the index holds exactly the ledger');
  assert(indexDocs().length <= 33, 'at most 32 shards and meta');
  say('completion ledger: ' + N.toLocaleString() + ' documents. The first read after this ships: ' + rd(first.d).toLocaleString() + ' reads (the whole ledger, as before) and the index is built (' + (indexDocs().length - 1) + ' shards, ' + Math.round(Math.max(...indexDocs().filter(k => /\/s\d\d$/.test(k)).map(k => meter.sizeOf(doc(k)))) / 1024) + ' KB the largest)');

  /* 2 · the index answers: the same ids, 33 reads (the first one also counts the ledger against it, about 38 more) */
  const second = await get({ designCompleted: '1' }, 'dc.second');
  assert.deepStrictEqual(second.body.orderNumbers, ids0, 'the same answer from the index');
  const third = await get({ designCompleted: '1' }, 'dc.third');
  assert.deepStrictEqual(third.body.orderNumbers, ids0);
  assert(rd(third.d) <= 33, 'a read from the index: ' + rd(third.d));
  const before = N, after = rd(third.d);
  say('  from the index: ' + rd(third.d) + ' reads a read (first one, with the count check: ' + rd(second.d) + '), the same ' + ids0.length.toLocaleString() + ' ids  =>  ' + (before / after).toFixed(0) + ' times fewer reads per page session; ' + Math.round(third.d.bytes / 1024) + ' KB read inside Google');

  /* 3 · the Complete button, QR Print and Undo go through firebaseOrders: ledger and index change in ONE batch */
  const A = '4100000001', B = '4100000002', C = '4100000003';
  assert.strictEqual((await post({ completedIds: [A, B, C] })).status, 200);
  assert(ledgerIds().includes(A) && indexIds().includes(A) && indexIds().includes(C), 'completed: in the ledger and in the index');
  let r = await get({ designCompleted: '1' }, 'dc.after-complete');
  assert.deepStrictEqual(r.body.orderNumbers, ledgerIds(), 'the answer follows at once'); assert(r.body.orderNumbers.includes(B)); assert(rd(r.d) <= 33);
  assert.strictEqual((await post({ uncompleteIds: [B] })).status, 200);
  assert(!ledgerIds().includes(B) && !indexIds().includes(B), 'un-completed: out of both');
  r = await get({ designCompleted: '1' }, 'dc.after-undo');
  assert(!r.body.orderNumbers.includes(B) && r.body.orderNumbers.includes(A), 'the un-complete shows at once and the others stay'); assert.deepStrictEqual(r.body.orderNumbers, ledgerIds());
  assert.strictEqual((await post({ completedIds: [A] })).status, 200);   // again: once
  assert.strictEqual(indexIds().filter(x => x === A).length, 1, 'once');
  // a big batch (more than one write batch holds) is split, every id lands in both
  const many = Array.from({ length: 1234 }, (_, i) => String(4200000000 + i));
  assert.strictEqual((await post({ completedIds: many })).status, 200);
  assert(many.every(x => doc('Design_Completed Orders/' + x)), 'all in the ledger'); assert.deepStrictEqual(indexIds(), ledgerIds(), 'index equals ledger after a big batch');
  assert.strictEqual((await post({ uncompleteIds: many })).status, 200); assert.deepStrictEqual(indexIds(), ledgerIds(), 'and after taking them all back');
  say('  Complete / Undo / a 1,234-order batch: ledger and index move in the same batch, the next read shows it at once (' + rd(r.d) + ' reads)');

  /* 4 · what the index cannot know: an edit by hand in the console. The count check finds it (every 15 minutes), the daily reconcile finds any other */
  const t0 = Date.now(), X = '4300000001', Y = ids0[5];
  await m.db.doc('Design_Completed Orders/' + X).set({ orderId: X, completed: true });   // added by hand: not in the index
  let res = await DC.list(m.db, FV, { now: t0 + 1000 });
  assert.strictEqual(res.via, 'index', 'within 15 minutes of the last check the index answers (the count was checked a moment ago)');
  res = await DC.list(m.db, FV, { now: t0 + DC.CHECK_MS + 5000 });
  assert.strictEqual(res.via, 'reconciled', 'a count check later: ledger and index differ in size? ' + res.via);
  assert(res.ids.includes(X), 'the answer is the ledger');
  assert(indexIds().includes(X), 'and the index is corrected'); assert.deepStrictEqual(indexIds(), ledgerIds());
  await m.db.doc('Design_Completed Orders/' + Y).delete();                                  // removed by hand: still in the index
  res = await DC.list(m.db, FV, { now: t0 + 2 * DC.CHECK_MS + 6000 });
  assert.strictEqual(res.via, 'reconciled'); assert(!res.ids.includes(Y) && !indexIds().includes(Y), 'a record removed by hand is dropped from the index too (it is gone from the ledger)');
  await m.db.doc('Design_Completed Orders/' + X).delete(); await m.db.doc('Design_Completed Orders/4300000002').set({ orderId: '4300000002' });   // one out, one in: same count
  res = await DC.list(m.db, FV, { now: t0 + 3 * DC.CHECK_MS + 10000 });
  assert.strictEqual(res.via, 'index', 'a swap keeps the count: the count check cannot see it'); assert(!res.ids.includes('4300000002'));
  res = await DC.list(m.db, FV, { now: t0 + 3 * DC.CHECK_MS + DC.RECONCILE_MS + 20000 });
  assert.strictEqual(res.via, 'reconciled', 'the weekly reconcile compares the sets'); assert(res.ids.includes('4300000002') && !res.ids.includes(X)); assert.deepStrictEqual(indexIds(), ledgerIds());
  say('  an edit by hand: found by the count check within 15 minutes, a swap by the weekly reconcile; the answer is always the ledger when they differ');

  /* 5 · a race can never undo a completion or an un-complete: each id is checked against its own ledger document where its shard changes */
  const P = '4400000001', Q = ids0[9];
  await m.db.doc('Design_Completed Orders/' + P).set({ orderId: P, completed: true }); await m.db.doc('Design_Completed Orders/' + P).delete();   // completed, then undone, after the reconcile read it
  const fixed = await DC.fix(m.db, FV, [P], []);
  assert.strictEqual(fixed.added, 0, 'a missing id whose ledger document is gone again is not added'); assert(!indexIds().includes(P));
  assert(ledgerIds().includes(Q) && indexIds().includes(Q));
  const fixed2 = await DC.fix(m.db, FV, [], [Q]);
  assert.strictEqual(fixed2.removed, 0, 'an extra id that is in the ledger again is not removed'); assert(indexIds().includes(Q));
  say('  a completion or an un-complete that lands during a reconcile is never undone (each id verified inside its shard\'s transaction)');

  /* 6 · an index that is half built (the lease, a crash) is not ready: the ledger answers; the sandbox keeps no index; a failing index falls back to the ledger */
  const m2 = meter.create(); m2.db.seed({ 'Design_Completed Orders/1': { c: 1 }, 'Design_Completed Orders/2': { c: 1 }, 'Design_Completed_Index/meta': { v: 1, buildingAt: Date.now() }, 'Design_Completed_Index/s03': { ids: ['2', '9'] } });
  let out = await DC.list(m2.db, FV, {}); assert.strictEqual(out.via, 'ledger'); assert.deepStrictEqual(out.ids, ['1', '2']);
  assert.strictEqual(m2.db.dump()['Design_Completed_Index/meta'].ready, undefined, 'a builder is running (lease): nobody else builds');
  const sb = meter.create(); sb.db.seed({ 'Sandbox_Design_Completed Orders/77': { c: 1 } });
  out = await DC.list(sb.db, FV, { prefix: 'Sandbox_' }); assert.deepStrictEqual(out.ids, ['77']); assert.strictEqual(Object.keys(sb.db.dump()).filter(k => /Index/.test(k)).length, 0, 'the sandbox keeps no index');
  const broken = { collection: () => ({ get: async () => { throw new Error('UNAVAILABLE'); }, select() { return { get: async () => ({ docs: [{ id: '5' }] }) }; }, doc: () => ({}) }), runTransaction: async () => { throw new Error('x'); } };
  out = await DC.list(broken, FV, {}); assert.deepStrictEqual(out.ids, ['5'], 'an index that cannot be read: the ledger answers');
  out = await DC.list(m2.db, {}, {}); assert.deepStrictEqual(out.ids, ['1', '2'], 'no array transforms (an old SDK): the ledger answers');
  m.uninstall();
  say('fc3b-completed-index: passed');
})().catch(e => { console.error(e); process.exit(1); });
