// Station tracking B, Laser cutting (Paul, 28 Sep 23:51: "track who is working on what … who scanned who completed it
// at every station"). The laser station is the sorter's Library: the operator marks a sheet or a whole set completed
// (op laserDone, from the Library's card or the sheet window), and a Rose Gold sheet is cut with "Cut Sheet" (op
// roseRecordCut). Every order on a cut sheet gets its laser milestone with who (as the sorter's sign-in names them;
// nobody is "" with data.signedIn false), the station (laser), the page (device) and the view it was pressed in.
// Runs the real handler against an in-memory Firestore (as server-stamps.cjs). No network.
//   node tests/charm-nest/st-b.cjs
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
const T = Date.now() - 3600e3;

(async () => {
  /* ── Library "Mark completed" (the laser operator's check): every order on each sheet gets laserDone, with who,
        the laser station, the page (device) and the view it was pressed in ── */
  const SET = 'set-2026-09-29-1', R1 = '4180000001', R2 = '4180000002', R3 = '4180000003', R4 = '4180000004';
  store.set(`Charm_Nest_Sets/${SET}`, { setId: SET, seq: 1, sheetIds: ['sh-1', 'sh-2', 'sh-3'] });
  store.set('Charm_Nest_Sheets/sh-1', { id: 'sh-1', metal: 'gold', setId: SET, sheetIndex: 1, fileBase: 'GF_Sep.29.26_Set-1_Sheet-1', orders: [R1, R2], placements: [] });
  store.set('Charm_Nest_Sheets/sh-2', { id: 'sh-2', metal: 'gold', setId: SET, sheetIndex: 2, fileBase: 'GF_Sep.29.26_Set-1_Sheet-2', orders: [R3], placements: [] });
  // a sheet saved without its orders list: its pieces' pool ids still name the order
  store.set('Charm_Nest_Sheets/sh-3', { id: 'sh-3', metal: 'gold', setId: SET, sheetIndex: 3, fileBase: 'GF_Sep.29.26_Set-1_Sheet-3', poolIds: [R4 + '_9001_1', R4 + '_9002_1', 'pool-x'], placements: [] });

  const cut = await ok({ op: 'laserDone', kind: 'sheet', id: 'sh-1', by: 'Marco R.', device: 'charm-nest-1', via: 'Sheet window' });
  for (const r of [R1, R2]) {
    const e = one(r, 'laserDone');
    assert(e.by === 'Marco R.' && e.station === 'laser' && e.device === 'charm-nest-1' && e.milestone === true && e.sheetId === 'sh-1' && e.sheet === 'GF Sheet 1' && e.setId === SET && e.at === cut.at, JSON.stringify(e));
    assert(e.data.signedIn === true && e.data.marked === 'sheet' && e.data.via === 'Sheet window', JSON.stringify(e.data));
  }
  assert.strictEqual(of(R3).length, 0, 'an order on another sheet is not marked');
  await ok({ op: 'laserDone', kind: 'sheet', id: 'sh-1', by: 'Marco R.', device: 'charm-nest-1', via: 'Library' });
  assert.strictEqual(of(R1, 'laserDone').length, 1, 'marked again (a retry, a second click): one seal');

  // the whole set: its other sheets' orders, one event each, the one already marked keeps its own
  const setCut = await ok({ op: 'laserDone', kind: 'set', id: SET, by: 'Lina', device: 'charm-nest-1', via: 'Library' });
  const e3 = one(R3, 'laserDone'), e4 = one(R4, 'laserDone');
  assert(e3.by === 'Lina' && e3.data.marked === 'set' && e3.data.via === 'Library' && e3.at === setCut.at && e3.sheet === 'GF Sheet 2', JSON.stringify(e3));
  assert(e4.by === 'Lina' && e4.sheetId === 'sh-3' && e4.device === 'charm-nest-1', 'the order is found in the pool ids: ' + JSON.stringify(e4));
  assert.strictEqual(one(R1, 'laserDone').by, 'Marco R.', 'the sheet cut earlier keeps who cut it');
  assert.strictEqual(of('9001').length + of('9002').length, 0, 'a transaction id is not taken for an order');

  // a device or view with odd characters is cleaned; no name is still refused (the flow asks the operator first)
  assert.strictEqual((await post({ op: 'laserDone', kind: 'sheet', id: 'sh-2', by: '', device: 'x' })).status, 400, 'a mark says who made it');
  await ok({ op: 'laserDone', kind: 'sheet', id: 'sh-2', done: false, by: 'Lina', device: 'charm<nest>"1', via: 'Library<b>' });
  const undone = one(R3, 'note');
  assert(undone.device === 'charmnest1' && undone.data.via === 'Libraryb' && undone.data.laserDoneBy === 'Lina' && /laser cut undone/.test(undone.text) && undone.milestone === false, JSON.stringify(undone));
  // an older page sends neither device nor via: recorded as before, with no empty via
  await ok({ op: 'laserDone', kind: 'sheet', id: 'sh-2', by: 'Old Page' });
  const again = of(R3, 'laserDone').find(e => e.by === 'Old Page');
  assert(again && again.device === '' && !('via' in again.data) && again.data.signedIn === true, JSON.stringify(again));

  /* ── Rose Gold "Cut Sheet": the person the sorter's sign-in names; nobody signed in is "" and signedIn false ── */
  const Readiness = require('../../charm-nest-readiness');
  const shape = (id, x, y, w, h) => ({ id, paths: [[[x, y], [x + w, y], [x + w, y + h], [x, y + h]]] });
  const roseCase = async (n, by, orders) => {
    const R = '41800001' + n + '0', sid = 'sheet-rose-' + n, run = 'run-rose-' + n, pid = R + '_700' + n + '_1';
    store.set('Charm_Nest_Runs/' + run, { lines: { a: { poolIds: [pid], spec: { engraveCandidate: false } } } });
    const rose = { id: sid, metal: 'rose', sheetIndex: n, fileBase: `RG_Sep.29.26_Set-1_Sheet-${n}`, verification: { ok: true }, status: 'complete', dirty: false, saving: false, draft: false, setId: SET, runId: run, poolIds: [pid], placedCount: 1,
      placements: [{ id: pid, cxPt: 20, cyPt: 20, angle: 0, scale: 1 }], outputs: { ai: { url: 'saved.ai' }, preview: { url: 'saved.png' } }, label: { files: [{ path: 'qr', url: 'qr.png', payload: 'x', orders: [R] }] } };
    if (orders) rose.orders = [R];
    for (const k of [...store.keys()]) if (k.startsWith('Charm_Nest_Rose_Stock/')) store.delete(k);   // (a new sheet of stock each case)
    const claim = await ok({ op: 'roseClaim', sheetId: sid, wPt: 100, hPt: 50 });
    store.set('Charm_Nest_Sheets/' + sid, rose);
    const plan = await ok({ op: 'rosePlan', sheetId: sid, stockId: claim.stock.id, revision: 0, fingerprint: create.fingerprint(rose), shapesJson: JSON.stringify([shape(pid, 2, 2, 10, 30)]), allowanceMm: 0.2 });
    assert(Readiness.sheet({ ...store.get('Charm_Nest_Sheets/' + sid), engraving: Readiness.decisions(Object.values(store.get('Charm_Nest_Runs/' + run).lines)) }).ready, 'the fixture passes the production checks');
    const args = { op: 'roseRecordCut', sheetId: sid, stockId: claim.stock.id, revision: 0, planHash: plan.planHash, by, device: 'charm-nest-1' };
    const rc = await ok(args); await ok(args);
    assert.strictEqual(of(R, 'roseCut').length, 1, 'the cut recorded again is one event');
    return { e: one(R, 'roseCut'), rc };
  };
  const signed = await roseCase(1, 'Kim', true);
  assert(signed.e.by === 'Kim' && signed.e.station === 'laser' && signed.e.device === 'charm-nest-1' && signed.e.data.signedIn === true && signed.e.milestone === true, JSON.stringify(signed.e));
  const nobody = await roseCase(2, '', false);
  assert(nobody.e.by === '' && nobody.e.data.signedIn === false && nobody.e.station === 'laser', 'nobody signed in is said so, never a guess: ' + JSON.stringify(nobody.e));
  assert.strictEqual(nobody.rc.cut.by, 'operator', 'the stock ledger keeps its placeholder as before');
  const legacy = await roseCase(3, 'operator', true);
  assert(legacy.e.by === '' && legacy.e.data.signedIn === false, 'an older page’s "operator" is no person: ' + JSON.stringify(legacy.e));

  /* ── the sorter's pages send who, the page and the view; the Rose cut no longer invents a name ── */
  const fs = require('fs'), root = path.join(__dirname, '../..');
  const lib = fs.readFileSync(path.join(root, 'charm-nest-library.js'), 'utf8'), rose = fs.readFileSync(path.join(root, 'charm-nest-rose-ui.js'), 'utf8');
  const win = fs.readFileSync(path.join(root, 'charm-nest-sheetwin.js'), 'utf8'), html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  assert(/op: 'laserDone', kind, id, done, by: by \|\| undefined, device: 'charm-nest-1', via \}/.test(lib), 'the Library says where it was marked');
  assert(/LibraryDone\.mark\("sheet", id, done, \{ via: "Sheet window" \}\)/.test(win), 'the sheet window says it was there');
  assert(!/\|\|'operator'/.test(rose) && /CNEmployee\?\.name\?\.\(\)/.test(rose) && /device:'charm-nest-1'/.test(rose), 'the Rose cut sends the signed-in name or nothing');
  for (const f of ['charm-nest-library.js', 'charm-nest-rose-ui.js', 'charm-nest-sheetwin.js']) assert(html.includes(`${f}?v=20260928-st-b`), f + ' version bumped');

  console.warn = realWarn;
  assert(!warnings.some(w => /timeline not recorded/.test(w)), 'no stamp failed: ' + warnings.join(' | '));
  console.log(`st-b: ok (${events().length} events)`);
})().catch(e => { console.warn = realWarn; console.error(e); process.exit(1); });
