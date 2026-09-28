// Adversarial checks of the order timeline's integrity on the server (task G, 28 Sep). Runs the real charmNestLibrary
// handler against an in-memory Firestore (the harness of server-stamps.cjs). No network.
//   1. A cancel undone while the timeline could not be written: the cancel record is gone (the order is not cancelled,
//      cancelCheck says so), but its `cancelled` event stays with no `cancelRestored` after it. `where` must follow the
//      record, not the lone event, or the order view says "Cancelled" forever.
//   2. The sandbox reset clears the sandbox's timeline with the rest of its records: a replayed order (the sandbox plays
//      real orders under their real numbers) must not start with the last play's events and stage.
//   node tests/charm-nest/adv-timeline.cjs
//   node tests/charm-nest/server-stamps.cjs
const path = require('path'), crypto = require('crypto'), assert = require('assert');
const fnDir = path.join(__dirname, '../../netlify/functions');
delete process.env.EDIT_PASSCODE;

/* ── in-memory Firestore (as server-history-bounds.cjs), with a switch that makes the timeline unwritable ── */
const store = new Map();
const SENT = { ts: { __ts: 1 }, del: { __del: 1 } };
const ts = ms => ({ toMillis: () => ms });
const FieldValue = { serverTimestamp: () => SENT.ts, delete: () => SENT.del, increment: n => ({ __inc: n }) };
const plain = v => v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Date) && !v.toMillis && !v.__inc && v !== SENT.ts && v !== SENT.del;
const clone = v => Array.isArray(v) ? v.map(clone) : v instanceof Date ? new Date(v) : plain(v) ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, clone(x)])) : v;
const value = (cur, v) => v === SENT.ts ? ts(Date.now()) : v && v.__inc != null ? (cur || 0) + v.__inc : clone(v);
function merge(target, src, deep) {
  for (const [k, v] of Object.entries(src)) {
    if (v === SENT.del) delete target[k];
    else if (deep && plain(v) && plain(target[k])) merge(target[k], v, true);
    else target[k] = plain(v) ? merge({}, v, true) : value(target[k], v);
  }
  return target;
}
const getPath = (o, f) => f.split('.').reduce((x, k) => (x == null ? undefined : x[k]), o);
const norm = v => (v && v.toMillis ? v.toMillis() : v instanceof Date ? v.getTime() : v);
const pick = (d, fields) => Object.fromEntries(fields.filter(f => d[f] !== undefined).map(f => [f, d[f]]));
let timelineDown = false;
const isTimeline = coll => /Order_Timeline$/.test(coll);
function docRef(coll, id) {
  const key = coll + '/' + id;
  return {
    id, path: key, parent: { id: coll },
    async get(mask) { const d0 = store.get(key), d = d0 && mask ? pick(d0, mask) : d0; return { exists: !!d, id, ref: this, data: () => (d ? clone(d) : undefined) }; },
    collection(sub) { return query(key + '/' + sub); },
    async set(data, opts) { if (timelineDown && isTimeline(coll)) throw new Error('DEADLINE_EXCEEDED: the timeline is down'); store.set(key, merge(opts && opts.merge ? clone(store.get(key) || {}) : {}, data, !!(opts && opts.merge))); },
    async update(data) {
      const cur = store.get(key); if (!cur) throw new Error('NOT_FOUND: ' + key);
      const next = clone(cur);
      for (const [k, v] of Object.entries(data)) { const parts = k.split('.'), last = parts.pop(); let o = next; for (const p of parts) o = plain(o[p]) ? o[p] : (o[p] = {}); if (v === SENT.del) delete o[last]; else o[last] = value(o[last], v); }
      store.set(key, next);
    },
    async delete() { store.delete(key); }
  };
}
function query(coll, filters = [], order = null, lim = 0, mask = null) {
  const q = {
    where(f, op, v) { return query(coll, filters.concat([[f, op, v]]), order, lim, mask); },
    orderBy(f, dir) { return query(coll, filters, [f, dir || 'asc'], lim, mask); },
    limit(n) { return query(coll, filters, order, n, mask); },
    select(...fields) { return query(coll, filters, order, lim, fields); },
    startAfter() { return q; },
    doc(id) { return docRef(coll, id || 'auto' + crypto.randomBytes(6).toString('hex')); },
    async add(data) { const r = q.doc(); await r.set(data); return r; },
    count() { return { get: async () => ({ data: () => ({ count: q.rows().length }) }) }; },
    async get() {
      const docs = q.rows().map(({ id, v }) => { const d = mask ? pick(v, mask) : v; return { id, exists: true, ref: docRef(coll, id), data: () => clone(d) }; });
      return { size: docs.length, docs, empty: !docs.length };
    },
    rows() {
      let rows = [...store.entries()].filter(([k]) => k.startsWith(coll + '/') && !k.slice(coll.length + 1).includes('/')).map(([k, v]) => ({ id: k.slice(coll.length + 1), v }));
      for (const [f, op, v0] of filters) rows = rows.filter(({ v: d }) => {
        const x = norm(getPath(d, f)), v = norm(v0);
        if (op === '==') return x === v; if (op === 'in') return v0.includes(x);
        if (x === undefined || x === null || typeof x !== typeof v) return false;
        return op === '<' ? x < v : op === '<=' ? x <= v : op === '>' ? x > v : op === '>=' ? x >= v : false;
      });
      if (order) { const k = r => norm(getPath(r.v, order[0])); rows = rows.filter(r => k(r) !== undefined).sort((a, b) => { const c = k(a) > k(b) ? 1 : k(a) < k(b) ? -1 : 0; return order[1] === 'desc' ? -c : c; }); }
      return lim ? rows.slice(0, lim) : rows;
    }
  };
  return q;
}
const timelineBatches = { n: 0 };   // commits that wrote timeline events
const db = {
  collection: c => query(c),
  batch() { const ops = []; let tl = false; return { set(ref, d, o) { if (isTimeline(ref.parent.id)) tl = true; ops.push(() => ref.set(d, o)); }, update(ref, d) { ops.push(() => ref.update(d)); }, delete(ref) { ops.push(() => ref.delete()); }, async commit() { if (tl) timelineBatches.n++; for (const o of ops) await o(); } }; },
  async getAll(...refs) { const o = refs.length && typeof refs[refs.length - 1].get !== 'function' ? refs.pop() : null; return Promise.all(refs.map(r => r.get(o && o.fieldMask))); },
  async runTransaction(fn) { return fn({ get: r => r.get(), getAll: (...refs) => db.getAll(...refs), set: (r, d, o) => r.set(d, o), update: (r, d) => r.update(d), delete: r => r.delete() }); }
};
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue, FieldPath: { documentId: () => '__name__' }, Timestamp: { fromMillis: ts } }), storage: () => ({ bucket: () => ({ name: 'test', file: p => ({ async move() {}, async copy() {}, async delete() {}, async getMetadata() { return [{}]; } }) }) }) };
const Module = require('module'), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === 'node-fetch') return async () => { throw new Error('no network in tests'); };
  if (req === 'firebase-admin' || req === './firebaseAdmin' || /[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin;
  return realLoad.call(this, req, ...rest);
};
const warnings = [], realWarn = console.warn;
console.warn = (...a) => { warnings.push(a.map(String).join(' ')); };
const lib = require(path.join(fnDir, 'charmNestLibrary.js'));
const create = require(path.join(fnDir, '_charmNestRoseStock.js'));
const post = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
const ok = async body => { const before = timelineBatches.n, r = await post(body); assert.strictEqual(r.status, 200, body.op + ': ' + JSON.stringify(r.body)); assert(timelineBatches.n - before <= 1, body.op + ' writes at most one timeline batch'); return r.body; };
const events = (prefix = '') => [...store].filter(([k]) => k.startsWith(prefix + 'Order_Timeline/')).map(([k, v]) => Object.assign({ key: k.slice(k.indexOf('/') + 1) }, v));
const of = (orderId, type, prefix) => events(prefix).filter(e => e.orderId === orderId && (!type || e.type === type));
const one = (orderId, type) => { const l = of(orderId, type); assert.strictEqual(l.length, 1, `one ${type} for ${orderId}: ${JSON.stringify(l)}`); return l[0]; };

(async () => {
  /* ── 1 · a restore whose cancelRestored stamp could not be written ── */
  const R1 = '4188000001';
  await ok({ op: 'cancelPut', orderId: R1, by: 'Ana', why: 'buyer asked' });
  assert.strictEqual(one(R1, 'cancelled').by, 'Ana');
  timelineDown = true;
  await ok({ op: 'cancelRestore', orderId: R1, by: 'Ben' });
  timelineDown = false;
  assert(!store.has('Charm_Nest_Cancelled/' + R1), 'the record is gone: the order is not cancelled');
  assert.strictEqual(of(R1, 'cancelRestored').length, 0, '(the restore could not be stamped)');
  const check = (await ok({ op: 'cancelCheck', orderIds: [R1] })).cancelled;
  assert(!check[R1], 'cancelCheck: not cancelled');
  for (const derive of [true, false]) {
    const tl = await ok({ op: 'timelineGet', orderId: R1, derive });
    assert.strictEqual(tl.cancelled, null);
    assert.notStrictEqual(tl.where.stage, 'cancelled', `where follows the record (derive ${derive}): ${JSON.stringify(tl.where)}`);
    assert.strictEqual(tl.where.cancelled, false);
    assert(tl.events.some(e => e.type === 'cancelled'), 'the cancel stays in the history');
  }
  // a cancel that stands is still said, with or without its event
  const R2 = '4188000002';
  await ok({ op: 'cancelPut', orderId: R2, by: 'Ana', why: 'x' });
  assert.strictEqual((await ok({ op: 'timelineGet', orderId: R2 })).where.stage, 'cancelled');
  store.delete(`Order_Timeline/${one(R2, 'cancelled').key}`);
  assert.strictEqual((await ok({ op: 'timelineGet', orderId: R2, derive: false })).where.stage, 'cancelled', 'the record alone says cancelled');
  console.log('  ✓ a restore with no cancelRestored event: where follows the cancel record');

  /* ── 2 · the sandbox reset clears the sandbox timeline ── */
  const R3 = '4188000003';
  await ok({ op: 'sandboxCancel', orderId: R3, sandbox: true });
  await ok({ op: 'timelineAdd', sandbox: true, events: [{ orderId: R3, type: 'held', id: 'x1', text: 'on hold' }] });
  assert(of(R3, null, 'Sandbox_').length >= 2, 'the sandbox play wrote its events');
  assert.strictEqual(of(R3).length, 0, 'none in production');
  store.set('Order_Timeline/' + R3 + '~note~prod', { orderId: R3, type: 'note', at: Date.now(), text: 'production' });
  for (let i = 0; i < 5; i++) { const r = await post({ op: 'sandboxReset', sandbox: true }); assert.strictEqual(r.status, 200, JSON.stringify(r.body)); if (!r.body.more) break; }
  assert(!store.has('Sandbox_Charm_Nest_Cancelled/' + R3), 'the sandbox cancel record is cleared');
  assert.deepStrictEqual(of(R3, null, 'Sandbox_').map(e => e.key), [], 'the sandbox timeline is cleared with it');
  const replay = await ok({ op: 'timelineGet', orderId: R3, sandbox: true, derive: false });
  assert.strictEqual(replay.events.length, 0); assert.notStrictEqual(replay.where.stage, 'cancelled', 'a replayed order starts clean');
  assert.strictEqual(of(R3).length, 1, 'production timeline untouched');
  console.log('  ✓ the sandbox reset clears Sandbox_Order_Timeline, production kept');
  console.log('adv-timeline: all passed');
})().catch(e => { console.error(e); process.exit(1); });
