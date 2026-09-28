// Adversarial checks of the order timeline store (_orderTimeline.js add/get, wave 3 area 6). No network.
// A small in-memory Firestore that answers as the real one does where it matters here:
//   · an equality query with no orderBy answers in document-id order (the single-field index it runs on), and limit cuts it;
//   · orderBy("__name__") with startAfter(id) pages it;
//   · a write whose value holds an array directly inside an array is refused, and the whole batch with it
//     ("Cannot convert an array value in an array value").
//   1. A very long history: 2,100 "cancelAlert" events (a guard loop at a station) must not hide the order's
//      "shipped" and "sorted" events, which sort after them by id.
//   2. One event whose details hold a nested array must not take the rest of its batch down (the outbox would send
//      that same batch forever, and nothing queued after it would ever land).
//   node tests/charm-nest/adv-timeline-store.cjs
const path = require('path'), assert = require('assert/strict');
const TL = require(path.join(__dirname, '../../netlify/functions/_orderTimeline.js'));

const store = new Map();   // "coll/id" → data
const nested = v => Array.isArray(v) ? v.some(x => Array.isArray(x) || nested(x)) : v && typeof v === 'object' ? Object.values(v).some(nested) : false;
let reads = 0;
function query(coll, f = null, ordered = false, lim = 0, after = null) {
  return {
    where(field, op, v) { assert.equal(op, '=='); return query(coll, [field, v], ordered, lim, after); },
    orderBy(field) { assert.equal(field, '__name__', 'only document-id order (no composite index)'); return query(coll, f, true, lim, after); },
    limit(n) { return query(coll, f, ordered, n, after); },
    startAfter(x) { assert(ordered, 'startAfter needs an orderBy'); return query(coll, f, ordered, lim, typeof x === 'string' ? x : x.id); },
    doc(id) { return { id, parent: { id: coll }, get: async () => { const d = store.get(coll + '/' + id); reads++; return { exists: !!d, id, data: () => d && Object.assign({}, d) }; } }; },
    async get() {
      let rows = [...store].filter(([k]) => k.startsWith(coll + '/')).map(([k, v]) => ({ id: k.slice(coll.length + 1), v }))
        .filter(r => !f || r.v[f[0]] === f[1]).sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
      if (after != null) rows = rows.filter(r => r.id > after);
      if (lim) rows = rows.slice(0, lim);
      reads += Math.max(1, rows.length);
      const docs = rows.map(r => ({ id: r.id, data: () => Object.assign({}, r.v) }));
      return { docs, size: docs.length, empty: !docs.length };
    }
  };
}
const db = {
  collection: c => query(c),
  batch() {
    const ops = [];
    return { set(ref, d) { ops.push([ref, d]); }, async commit() {
      for (const [, d] of ops) if (nested(d)) throw new Error('3 INVALID_ARGUMENT: Cannot convert an array value in an array value.');
      for (const [ref, d] of ops) store.set(ref.parent.id + '/' + ref.id, Object.assign({}, d));
    } };
  }
};
const FV = { serverTimestamp: () => ({ ts: 1 }) };

(async () => {
  /* ── 1 · a very long history keeps its later types ── */
  const R = '4190000001', t0 = Date.now() - 86400000;
  for (let i = 0; i < 2100; i++) store.set(`Order_Timeline/${R}~cancelAlert~${t0 + i}-a${i}`, { orderId: R, type: 'cancelAlert', at: t0 + i, text: 'alert' });
  store.set(`Order_Timeline/${R}~arrived~x`, { orderId: R, type: 'arrived', at: t0 - 1000 });
  store.set(`Order_Timeline/${R}~sorted~y`, { orderId: R, type: 'sorted', at: t0 + 5000, milestone: true });
  store.set(`Order_Timeline/${R}~shipped~z`, { orderId: R, type: 'shipped', at: t0 + 9000, milestone: true });
  reads = 0;
  const tl = await TL.get(db, R, { derive: false });
  const types = new Set(tl.events.map(e => e.type));
  assert(types.has('arrived'), 'arrived kept');
  assert(types.has('sorted'), 'sorted must not be hidden by 2,100 cancel alerts');
  assert(types.has('shipped'), 'shipped must not be hidden by 2,100 cancel alerts');
  assert.equal(tl.truncated, true, 'said to be cut short');
  assert.deepEqual(tl.leftOut, { types: ['cancelAlert'], kept: 500, capped: false }, 'says what was left out: ' + JSON.stringify(tl.leftOut));
  assert(reads <= 2600, 'reads stay bounded: ' + reads);
  // a short history is whole and not called truncated
  const S = '4190000002';
  for (let i = 0; i < 30; i++) store.set(`Order_Timeline/${S}~moved~${t0 + i}-m`, { orderId: S, type: 'moved', at: t0 + i });
  const short = await TL.get(db, S, { derive: false });
  assert.equal(short.events.length, 30); assert.equal(short.truncated, false); assert.equal(short.leftOut, null);
  // exactly one page long: all there, not truncated
  const P = '4190000003';
  for (let i = 0; i < 500; i++) store.set(`Order_Timeline/${P}~scan~${t0 + i}-s`, { orderId: P, type: 'scan', at: t0 + i });
  store.set(`Order_Timeline/${P}~welded~w`, { orderId: P, type: 'welded', at: t0 + 600 });
  const page = await TL.get(db, P, { derive: false });
  assert.equal(page.events.length, 501); assert(page.events.some(e => e.type === 'welded')); assert.equal(page.truncated, false);
  console.log('  ✓ a long history keeps every type (paged by document id, bounded reads)');

  /* ── 2 · one event with a nested array does not sink its batch ── */
  const O = '4190000004';
  const out = await TL.add(db, FV, [
    { orderId: O, type: 'note', id: 'good-1', text: 'before' },
    { orderId: O, type: 'note', id: 'bad', text: 'odd details', data: { box: [[0, 0], [1, 1]], ok: 1 } },
    { orderId: O, type: 'note', id: 'good-2', text: 'after' }
  ], { source: 'station' }).catch(e => ({ error: e.message }));
  assert(!out.error, 'the batch lands: ' + out.error);
  const keys = [...store.keys()].filter(k => k.startsWith(`Order_Timeline/${O}~`)).sort();
  assert.deepEqual(keys, [`Order_Timeline/${O}~note~bad`, `Order_Timeline/${O}~note~good-1`, `Order_Timeline/${O}~note~good-2`]);
  const bad = store.get(`Order_Timeline/${O}~note~bad`);
  assert(!nested(bad), 'stored without a nested array');
  assert.equal(bad.data.ok, 1, 'the rest of its details kept');
  console.log('  ✓ a nested array in one event\'s details does not refuse its whole batch');
  console.log('adv-timeline-store: all passed');
})().catch(e => { console.error(e); process.exit(1); });
