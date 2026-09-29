// Station tracking, part F (Paul, 28 Sep 23:51): "who completed it at every station". Engraving is approved in the
// sorter's Engrave tab (and its sheet and order windows, which call the same approval); the back file saved for the laser
// is backPut, which stamps engraveApproved on the order's timeline. This checks the Engraved seal carries the person the
// sorter's sign-in knows, the station and the page, once per piece (copy) and per order: a retry adds nothing, a refused
// back records nothing, a new approval of the same piece is a new event, and a timeline that cannot be written never
// fails the save. Runs the real handler against an in-memory Firestore (as server-stamps.cjs). No network.
//   node tests/charm-nest/st-f.cjs
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
const post = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
const ok = async body => { const before = timelineBatches.n, r = await post(body); assert.strictEqual(r.status, 200, body.op + ': ' + JSON.stringify(r.body)); assert(timelineBatches.n - before <= 1, body.op + ' writes at most one timeline batch'); return r.body; };
const events = (prefix = '') => [...store].filter(([k]) => k.startsWith(prefix + 'Order_Timeline/')).map(([k, v]) => Object.assign({ key: k.slice(k.indexOf('/') + 1) }, v));
const of = (orderId, type, prefix) => events(prefix).filter(e => e.orderId === orderId && (!type || e.type === type));
const T = Date.now() - 3600e3;

(async () => {
  const SET = 'set-2026-09-28-2', R1 = '4180000001', R2 = '4180000002', R3 = '4180000003';
  const A1 = R1 + '_81001_1', A2 = R1 + '_81001_2', B1 = R1 + '_81002_1', C1 = R2 + '_82001_1';
  store.set('Charm_Nest_Sheets/sh-ss-1', { id: 'sh-ss-1', metal: 'silver', setId: SET, sheetIndex: 1, fileBase: 'SS_Sep.28.26_Set-2_Sheet-1', orders: [R1, R2], poolIds: [A1, A2, B1, C1], placements: [] });
  const at = T + 5000;
  const row = (poolId, o) => Object.assign({ poolId, sheetId: 'sh-ss-1', setId: SET, order: poolId.split('_')[0], transactionId: poolId.split('_')[1], sku: 'NK-7', copy: +poolId.split('_')[2], text: 'Ava\n2019', lines: ['Ava', '2019'], approvedAt: at, approvedBy: 'Marco R.' }, o);
  const sheetSave = () => [row(A1), row(A2), row(B1, { approvedBy: 'Lina P.', approvedAt: at + 60e3 }), row(C1)];

  /* ── one save of a sheet's backs: an event a piece, each on its own order, with who, where and when ── */
  const out = await ok({ op: 'backPut', backs: sheetSave() });
  assert(out.ok && out.written === 4, JSON.stringify(out));
  const r1 = of(R1, 'engraveApproved'), r2 = of(R2, 'engraveApproved');
  assert.strictEqual(r1.length, 3, 'order 1: one Engraved event for each of its three pieces');
  assert.strictEqual(r2.length, 1, 'order 2: its one piece');
  for (const e of r1.concat(r2)) {
    assert(e.station === 'sorter' && e.device === 'charm-nest-1' && e.source === 'sorter' && e.milestone === true, 'where: ' + JSON.stringify(e));
    assert(e.data.signedIn === true && !('employeeId' in e.data), 'signed in, and no id the sorter does not have: ' + JSON.stringify(e.data));
    assert(e.sheet === 'SS Sheet 1' && e.sheetId === 'sh-ss-1' && e.setId === SET && /Ava \/ 2019/.test(e.text), JSON.stringify(e));
  }
  const byPiece = new Map(r1.concat(r2).map(e => [e.data.poolId, e]));
  assert.deepStrictEqual([...byPiece.keys()].sort(), [A1, A2, B1, C1].sort(), 'every piece named once');
  assert(byPiece.get(A1).by === 'Marco R.' && byPiece.get(A1).at === at && byPiece.get(A1).lineKey === R1 + '_81001' && byPiece.get(A1).data.copy === 1, JSON.stringify(byPiece.get(A1)));
  assert(byPiece.get(A2).data.copy === 2 && byPiece.get(A2).lineKey === R1 + '_81001', 'the second copy of the same line is its own piece');
  assert(byPiece.get(B1).by === 'Lina P.' && byPiece.get(B1).at === at + 60e3 && byPiece.get(B1).transactionId === '81002', 'each piece keeps the person who approved it and when: ' + JSON.stringify(byPiece.get(B1)));
  assert(byPiece.get(C1).orderId === R2 && byPiece.get(C1).by === 'Marco R.', 'another order on the same sheet gets its own event');

  /* ── a retry (the sheet saved again, the same approvals) doubles nothing ── */
  await ok({ op: 'backPut', backs: sheetSave() });
  assert.strictEqual(of(R1, 'engraveApproved').length, 3, 'a re-save keeps one event a piece');
  assert.strictEqual(of(R2, 'engraveApproved').length, 1);

  /* ── the piece approved again (the words edited) is a new approval, with its own person ── */
  await ok({ op: 'backPut', back: row(A1, { text: 'Ava 2019', lines: ['Ava 2019'], approvedAt: at + 120e3, approvedBy: 'Lina P.' }) });
  const again = of(R1, 'engraveApproved').filter(e => e.data.poolId === A1).sort((p, q) => p.at - q.at);
  assert(again.length === 2 && again[1].by === 'Lina P.' && again[1].at === at + 120e3 && again[0].by === 'Marco R.', 'the new approval is added, the first stays: ' + JSON.stringify(again));

  /* ── a refused back records nothing: a piece not on the sheet, an approval with no name ── */
  const stray = R3 + '_83001_1';
  const bad = await post({ op: 'backPut', backs: [row(stray)] });
  assert(bad.status === 500 && /does not contain/.test(bad.body.error), JSON.stringify(bad));
  const anon = await post({ op: 'backPut', back: row(stray, { approvedBy: '' }) });
  assert.strictEqual(anon.status, 400, 'an approval must name its person');
  assert.strictEqual(of(R3).length, 0, 'nothing recorded for a refused back');

  /* ── a sandbox save stays in the sandbox's timeline ── */
  store.set('Sandbox_Charm_Nest_Sheets/sh-sb-1', { id: 'sh-sb-1', metal: 'gold', sheetIndex: 4, fileBase: 'GF_Sep.28.26_Set-9_Sheet-4', poolIds: [R3 + '_83002_1'], placements: [] });
  await ok({ op: 'backPut', sandbox: true, back: row(R3 + '_83002_1', { sheetId: 'sh-sb-1', setId: null }) });
  assert.strictEqual(of(R3, 'engraveApproved', 'Sandbox_').length, 1, 'the sandbox has it');
  assert.strictEqual(of(R3, 'engraveApproved').length, 0, 'production does not');

  /* ── the timeline down: the back is still saved, the op answers as before ── */
  timelineDown = true;
  const down = await ok({ op: 'backPut', back: row(A2, { approvedAt: at + 180e3 }) });
  timelineDown = false;
  assert(down.ok && down.written === 1 && store.get('Charm_Pool_Back/' + A2).approvedAt === at + 180e3, 'the save went through: ' + JSON.stringify(down));
  assert(warnings.some(w => /timeline not recorded \(engraving approved\)/.test(w)), 'the failure is logged');

  console.log('st-f: engraving approvals are stamped per piece and order, with who, where and when: ok');
})().catch(e => { console.error(e); process.exit(1); });
