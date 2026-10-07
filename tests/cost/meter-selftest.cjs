// Self-test of tests/cost/meter.cjs: the billing rules (reads, bytes, writes, deletes, count, select, offset, listeners, transactions,
// batches, getAll), per-operation and per-caller attribution, wrapping a foreign fake, and the real firebaseAdmin hook.
//   node tests/cost/meter-selftest.cjs
'use strict';
const assert = require('assert'), path = require('path');
const meter = require('./meter.cjs');
const say = s => process.stdout.write(s + '\n');

(async () => {
  /* 1 · the in-memory Firestore and its counters */
  const m = meter.create();
  const db = m.db;
  const big = 'x'.repeat(1000);
  db.seed({ 'sheets/a': { n: 1, st: 'open', blob: big }, 'sheets/b': { n: 2, st: 'open', blob: big }, 'sheets/c': { n: 3, st: 'done', blob: big }, 'orders/o1': { id: 'o1', total: 5 }, 'orders/o1/lines/l1': { sku: 'S1' } });
  assert.strictEqual(m.total.reads, 0, 'seeding is free');

  await m.op('doc.get', () => db.collection('orders').doc('o1').get());
  assert.deepStrictEqual([m.byOp['doc.get'].reads, m.byOp['doc.get'].bytes], [1, meter.sizeOf({ id: 'o1', total: 5 })]);
  await m.op('doc.get.missing', () => db.collection('orders').doc('nope').get());
  assert.deepStrictEqual([m.byOp['doc.get.missing'].reads, m.byOp['doc.get.missing'].bytes], [1, 0], 'a missing document is still one read');

  const q = await m.op('query', () => db.collection('sheets').where('st', '==', 'open').get());
  assert.strictEqual(q.size, 2); assert.strictEqual(m.byOp.query.reads, 2); assert.ok(m.byOp.query.bytes > 2000);
  await m.op('query.empty', () => db.collection('sheets').where('st', '==', 'zzz').get());
  assert.strictEqual(m.byOp['query.empty'].reads, 1, 'an empty query is one read');

  const sel = await m.op('query.select', () => db.collection('sheets').select('n', 'st').get());
  assert.strictEqual(sel.size, 3); assert.strictEqual(m.byOp['query.select'].reads, 3);
  assert.ok(m.byOp['query.select'].bytes < 200, 'a field mask only counts the selected fields: ' + m.byOp['query.select'].bytes);
  assert.deepStrictEqual(sel.docs[0].data(), { n: 1, st: 'open' });

  await m.op('query.offset', () => db.collection('sheets').orderBy('n').offset(2).limit(5).get());
  assert.strictEqual(m.byOp['query.offset'].reads, 3, 'skipped documents are billed: 2 skipped + 1 returned');

  const c = await m.op('count', () => db.collection('sheets').count().get());
  assert.strictEqual(c.data().count, 3); assert.strictEqual(m.byOp.count.aggs, 1); assert.strictEqual(m.byOp.count.bytes, 0);

  await m.op('getAll', () => db.getAll(db.doc('sheets/a'), db.doc('sheets/b'), db.doc('sheets/zz')));
  assert.strictEqual(m.byOp.getAll.reads, 3);

  const ga = await m.op('getAll.mask', () => db.getAll(db.doc('sheets/a'), db.doc('sheets/b'), { fieldMask: ['n'] }));
  assert.deepStrictEqual(ga[0].data(), { n: 1 }); assert.strictEqual(m.byOp['getAll.mask'].reads, 2); assert.ok(m.byOp['getAll.mask'].bytes < 40, 'getAll fieldMask: only the masked fields are bytes: ' + m.byOp['getAll.mask'].bytes);
  const gb = await m.op('tx.getAll.mask', () => db.runTransaction(tx => tx.getAll(db.doc('sheets/a'), { fieldMask: ['st'] })));
  assert.deepStrictEqual(gb[0].data(), { st: 'open' }); assert.ok(m.byOp['tx.getAll.mask'].bytes < 30);

  await m.op('group', () => db.collectionGroup('lines').get());
  assert.strictEqual(m.byOp.group.reads, 1);

  /* writes and deletes */
  await m.op('writes', async () => {
    await db.collection('orders').doc('o2').set({ total: 1 });
    await db.collection('orders').doc('o2').update({ total: 2 });
    await db.collection('orders').add({ total: 3 });
    const b = db.batch(); b.set(db.doc('orders/o3'), { total: 4 }); b.update(db.doc('orders/o3'), { total: 5 }); b.delete(db.doc('orders/o2')); await b.commit();
    await db.doc('orders/o3').delete();
  });
  assert.deepStrictEqual([m.byOp.writes.writes, m.byOp.writes.deletes], [5, 2]);
  assert.strictEqual(m.byCollection.orders.writes >= 5, true);

  /* transaction: reads and writes inside it are counted once each */
  await m.op('tx', () => db.runTransaction(async tx => {
    const s = await tx.get(db.doc('sheets/a')); const t = await tx.getAll(db.doc('sheets/b'), db.doc('sheets/c'));
    tx.update(db.doc('sheets/a'), { n: s.data().n + 1 }); tx.set(db.doc('sheets/b'), { z: 1 }, { merge: true }); return t.length;
  }));
  assert.deepStrictEqual([m.byOp.tx.reads, m.byOp.tx.writes], [3, 2]);
  assert.strictEqual(db.dump()['sheets/a'].n, 2); assert.strictEqual(db.dump()['sheets/b'].z, 1); assert.strictEqual(db.dump()['sheets/b'].n, 2, 'merge keeps the other fields');

  /* FieldValue, merge, arrays */
  await db.doc('sheets/c').update({ n: meter.FieldValue.increment(5), tags: meter.FieldValue.arrayUnion('x', 'y'), 'deep.k': 1, at: meter.FieldValue.serverTimestamp() });
  const cd = (await db.doc('sheets/c').get()).data(); assert.strictEqual(cd.n, 8); assert.deepStrictEqual(cd.tags, ['x', 'y']); assert.strictEqual(cd.deep.k, 1); assert.ok(cd.at instanceof meter.Timestamp);

  /* listeners: first answer = all docs, then one read per changed doc, nothing while idle */
  const a0 = m.snapshot(); let hits = 0;
  const un = await m.op('listener', async () => {
    const off = db.collection('sheets').where('st', '==', 'open').onSnapshot(() => { hits++; });
    await new Promise(r => setTimeout(r, 20)); return off;
  });
  const d1 = m.since(a0); assert.strictEqual(d1.reads, 2, 'listener opened over 2 documents: 2 reads, got ' + d1.reads);
  await new Promise(r => setTimeout(r, 30)); assert.strictEqual(m.since(a0).reads, 2, 'no reads while idle');
  await m.op('listener.change', () => db.doc('sheets/a').update({ n: 100 })); await new Promise(r => setTimeout(r, 20));
  assert.strictEqual(m.since(a0).reads, 3, 'one changed document = one read'); un();

  /* 2 · per-caller attribution points at the code that issued the call (here: this test file, no product frame in between) */
  const callers = Object.keys(m.byCaller); assert.ok(callers.every(k => /meter-selftest\.cjs:\d+/.test(k)), callers.join(','));

  /* 3 · projections and assertions */
  const ph = meter.perHour({ reads: 5, aggs: 0, bytes: 20000, writes: 0, deletes: 0 }, 1200);
  assert.deepStrictEqual([ph.reads, ph.bytes], [6000, 24000000]);
  assert.throws(() => meter.assertMax({ reads: 11, aggs: 0, bytes: 1 }, { reads: 10 }, 'x'), /cost budget exceeded \(x\): reads 11 > 10/);
  meter.assertMax({ reads: 10, aggs: 0, bytes: 1 }, { reads: 10, bytes: 5 });

  /* 4 · wrap() over a foreign fake with its own shapes (Map + closures, the style the other suites use) */
  const store = new Map([['k/a', { v: 1 }], ['k/b', { v: 2 }]]);
  const col = c => { const q = { doc: id => ({ path: c + '/' + id, get: async () => ({ exists: store.has(c + '/' + id), data: () => store.get(c + '/' + id), ref: { path: c + '/' + id } }), set: async (d) => { store.set(c + '/' + id, d); }, delete: async () => { store.delete(c + '/' + id); } }),
    where: () => q, limit: () => q, get: async () => ({ docs: [...store].filter(([k]) => k.startsWith(c + '/')).map(([k, d]) => ({ id: k.split('/')[1], data: () => d })) }) }; return q; };
  const foreign = { collection: col, getAll: (...refs) => Promise.all(refs.map(r => r.get())), runTransaction: async fn => fn({ get: async r => r.get(), set: (r, d) => { store.set(r.path, d); } }) };
  const m2 = meter.create(); const f = meter.wrap(foreign, m2);
  await m2.op('f.q', () => f.collection('k').where('x', '==', 1).limit(5).get());
  await m2.op('f.doc', () => f.collection('k').doc('a').get());
  await m2.op('f.getAll', () => f.getAll(f.collection('k').doc('a'), f.collection('k').doc('b')));
  await m2.op('f.tx', () => f.runTransaction(async tx => { await tx.get(f.collection('k').doc('a')); tx.set(f.collection('k').doc('z'), { v: 9 }); }));
  await m2.op('f.write', async () => { await f.collection('k').doc('y').set({ v: 3 }); await f.collection('k').doc('y').delete(); });
  assert.deepStrictEqual([m2.byOp['f.q'].reads, m2.byOp['f.doc'].reads, m2.byOp['f.getAll'].reads, m2.byOp['f.tx'].reads, m2.byOp['f.tx'].writes, m2.byOp['f.write'].writes, m2.byOp['f.write'].deletes], [2, 1, 2, 1, 1, 1, 1], JSON.stringify(m2.byOp));
  assert.ok(store.has('k/z'), 'the wrapped fake still does its job');

  /* 4b · wrap() over a real-looking fake keeps per-collection numbers */
  assert.ok(m2.byCollection.k.reads === 6, JSON.stringify(m2.byCollection));

  /* 5 · install(): the real product code, through its own `require("./firebaseAdmin")` */
  const m3 = meter.create(); m3.install();
  m3.db.seed({ 'sorterOrders/x': { a: 1 } });
  const fakeFn = path.join(__dirname, '..', '..', 'netlify', 'functions', '_meterProbe.js');       // never written: we only resolve a module named like a function
  const Module = require('module'); const admin = Module._load('./firebaseAdmin');
  assert.strictEqual(typeof admin.firestore, 'function'); assert.strictEqual(admin.firestore(), m3.db);
  await m3.op('admin.hook', () => admin.firestore().collection('sorterOrders').doc('x').get());
  assert.strictEqual(m3.byOp['admin.hook'].reads, 1);
  const bk = admin.storage().bucket(); await bk.file('a.bin').save(Buffer.alloc(1000)); await bk.file('a.bin').download();
  assert.deepStrictEqual([m3.storage.uploadBytes, m3.storage.downloadBytes], [1000, 1000]);
  m3.uninstall();
  assert.throws(() => Module._load('./firebaseAdmin'), /Cannot find module|FIREBASE_PRIVATE_KEY/, 'uninstall restores the loader');

  m.print(s => { });          // the printer must not throw
  say('meter-selftest: all checks passed (' + Object.keys(m.byOp).length + ' operations, ' + m.total.reads + ' reads, ' + m.total.bytes + ' bytes counted)');
})().catch(e => { console.error(e); process.exit(1); });
