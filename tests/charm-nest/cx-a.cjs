// Cancelled orders on the timeline, part A (Paul, 29 Sep 00:26): every cancel path records the exact moment and each
// removal with its place and outcome, once (stable ids), and the order view's Now card says it in plain words. Runs the
// real charmNestLibrary handler over an in-memory Firestore (the harness of adv-timeline.cjs). No network.
//   node tests/charm-nest/cx-a.cjs
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
  batch() { const ops = []; let tl = false; return { create(ref, d) { ops.push(() => ref.set(d)); }, set(ref, d, o) { if (isTimeline(ref.parent.id)) tl = true; ops.push(() => ref.set(d, o)); }, update(ref, d) { ops.push(() => ref.update(d)); }, delete(ref) { ops.push(() => ref.delete()); }, async commit() { if (tl) timelineBatches.n++; for (const o of ops) await o(); } }; },
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
const fs = require('fs'), vm = require('vm');
const Timeline = require(path.join(fnDir, '_orderTimeline.js'));
const OrderCancel = require(path.join(fnDir, '_orderCancel.js'));

(async () => {
  /* ── 1 · Etsy's cancel: Etsy's own moment, and when the sorter saw it ── */
  const E = '4199000001', etsyAt = Date.now() - 7 * 60000;
  const out = await OrderCancel.fromReceipts(db, FieldValue, [{ receipt_id: E, status: 'Canceled', updated_timestamp: Math.round(etsyAt / 1000), created_timestamp: Math.round((etsyAt - 864e5) / 1000) }]);
  assert.strictEqual(out.created, 1, JSON.stringify(out));
  const ex = one(E, 'etsyCancelled');
  assert.strictEqual(ex.at, Math.round(etsyAt / 1000) * 1000, 'the cancel is at Etsy\'s time, not the mirror\'s');
  assert(ex.data.seenAt >= ex.at + 6 * 60000, 'the sorter\'s sight of it is kept beside it');
  await OrderCancel.fromReceipts(db, FieldValue, [{ receipt_id: E, status: 'Canceled', updated_timestamp: Math.round(etsyAt / 1000) }]);
  one(E, 'etsyCancelled');
  console.log('  ✓ 1 · Etsy\'s cancel at Etsy\'s moment, seen-by-the-sorter kept, once on a re-detection');

  /* ── 2 · a person's cancel at the press, each sheet its own removal, the steps that stayed ── */
  const P = '4199000002', pressed = Date.now() - 90000;
  for (const [id, sheetId, name, setId] of [[`${P}_11111_1`, 'sh-gf1', 'GF_Sep.29.26_Set-2_Sheet-1', 'day-2'], [`${P}_11111_2`, 'sh-ss3', 'SS_Sep.29.26_Set-2_Sheet-3', 'day-2'], [`${P}_12222_1`, '', '', '']])
    store.set('Charm_Pool/' + id, { poolId: id, orderId: P, sheetId, sheetName: name, setId, material: 'silver', state: 'written' });
  const put = await ok({ op: 'cancelPut', orderId: P, by: 'Ana', why: 'buyer asked', at: pressed });
  assert.strictEqual(put.record.at, pressed, 'the record keeps the moment Cancel was pressed');
  assert.strictEqual(one(P, 'cancelled').at, pressed);
  const removedAt = Date.now();
  const patch = { state: 'abandoned', sheetId: null, setId: null, removedBy: 'Ana', removedReason: 'cancelled: buyer asked', removedAt };
  await ok({ op: 'poolUpdate', poolIds: [`${P}_11111_1`, `${P}_11111_2`, `${P}_12222_1`], patch, by: 'Ana' });
  await ok({ op: 'poolUpdate', poolIds: [`${P}_11111_1`, `${P}_11111_2`, `${P}_12222_1`], patch, by: 'Ana' });   // a retry
  const rm = of(P, 'removed').sort((a, b) => a.text.localeCompare(b.text));
  assert.deepStrictEqual(rm.map(e => e.text), ['Removed from GF Sheet 1 (Set 2)', 'Removed from SS Sheet 3 (Set 2)', 'Removed from the SS pool'], JSON.stringify(rm.map(e => e.text)));
  assert(rm.every(e => e.data.outcome === 'removed' && e.data.cancel && e.by === 'Ana' && e.at === removedAt));
  // the sheet window's fates: one cut sheet stays, one saved sheet still to come off; then it comes off
  await ok({ op: 'cancelFates', orderId: P, cancelAt: pressed, by: 'Ana', fates: [{ sheet: 'GF Sheet 1', fate: 'removed' }, { sheet: 'GF Sheet 4', fate: 'cut' }, { sheet: 'SS Sheet 5', fate: 'open' }] });
  const steps = () => of(P, 'cancelStep').sort((a, b) => a.sheet.localeCompare(b.sheet));
  assert.deepStrictEqual(steps().map(e => [e.text, e.data.outcome, e.data.done]), [['On a cut sheet: set aside (GF Sheet 4)', 'setAside', false], ['Still on SS Sheet 5: not taken off yet', 'waiting', false]], JSON.stringify(steps()));
  await ok({ op: 'cancelFates', orderId: P, fates: [{ sheet: 'SS Sheet 5', fate: 'removed' }] });   // (no cancelAt: read from the record)
  assert.deepStrictEqual(steps().map(e => [e.text, e.data.outcome]), [['On a cut sheet: set aside (GF Sheet 4)', 'setAside'], ['Removed from SS Sheet 5', 'removed']], 'the same step says it came off');
  // a person's "Set aside" (AutoCancel.step through timelineAdd) lands on the same step
  await ok({ op: 'timelineAdd', events: [{ orderId: P, type: 'cancelStep', by: 'Bo', sheet: 'GF Sheet 4', id: `cx-${pressed}-GF Sheet 4`, text: 'On a cut sheet: set aside by Bo (GF Sheet 4)', data: { outcome: 'setAside', done: true } }] });
  assert.strictEqual(steps().length, 2, 'no second step for the same place');
  assert.strictEqual(steps()[0].by, 'Bo');
  // …and on the record's own removals (part B's list), with who; the queue too (AutoCancel.step)
  await ok({ op: 'cancelFates', orderId: P, fates: [], by: 'Bo', removals: [{ id: 'sheet~GF Sheet 4', where: 'GF Sheet 4', kind: 'sheet', outcome: 'setAside', by: 'Bo', text: 'On a cut sheet: set aside by Bo (GF Sheet 4)' }] });
  await ok({ op: 'cancelFates', orderId: P, fates: [], by: 'Ana', removals: [{ id: 'queue~the queue', where: 'the queue', kind: 'queue', outcome: 'removed', by: 'Ana', text: 'Taken out of the queue (2 lines)' }] });
  const recP = store.get('Charm_Nest_Cancelled/' + P);
  assert.strictEqual(recP.removals.find(r => r.id === 'sheet~GF Sheet 1').by, 'Ana', 'the record\'s removals say who: ' + JSON.stringify(recP.removals));
  const live = await ok({ op: 'timelineGet', orderId: P });
  const places = live.events.filter(e => e.type === 'cancelStep' || e.type === 'removed').map(e => e.sheet.toLowerCase());
  assert.strictEqual(new Set(places).size, places.length, 'one step per place, recorded or read from the record: ' + places);
  assert(live.events.some(e => e.type === 'cancelStep' && e.text === 'Taken out of the queue'), 'the queue, read from the record');
  console.log('  ✓ 2 · a person\'s cancel at the press; a removal per sheet and the pool, once on a retry; set aside / still on / came off as one step each');

  /* ── 3 · the whole story from the server, and in the order view's Now card ── */
  await ok({ op: 'cancelRestore', orderId: P, by: 'Cy' });
  const tl = await ok({ op: 'timelineGet', orderId: P });
  const types = tl.events.map(e => e.type);
  for (const t of ['cancelled', 'removed', 'cancelStep', 'cancelRestored']) assert(types.includes(t), t + ' in the history: ' + types.join(','));
  assert.strictEqual(tl.events.filter(e => e.type === 'removed').length, 3, 'the pieces\' own records add no second removal');
  const win = { document: { getElementById: () => null, createElement: () => ({ style: {} }), head: { appendChild() {} }, querySelector: () => null } };
  win.window = win; vm.createContext(win);
  vm.runInContext(fs.readFileSync(path.join(__dirname, '../../charm-nest-timeline-ui.js'), 'utf8'), win);
  const UI = win.OrderTimelineUI, cx = tl.events.find(e => e.type === 'cancelled');
  const now = UI.nowStamps(tl.events, { cancelled: cx });
  assert(/tlNowSeal cx/.test(now.seal), 'the CANCELLED ORDER seal');
  const txt = now.recent.replace(/<[^>]+>/g, '|');
  for (const want of ['Cancelled by Ana', 'Removed from GF Sheet 1 (Set 2)', 'Removed from the SS pool', 'set aside by Bo (GF Sheet 4)', 'Removed from SS Sheet 5', 'Restored by Cy']) assert(txt.includes(want), `Now card says "${want}": ${txt}`);
  assert(UI.sealed({ type: 'cancelStep' }), 'a cancel step draws a seal');
  const etl = await ok({ op: 'timelineGet', orderId: E });
  const en = UI.nowStamps(etl.events, { cancelled: etl.events.find(e => e.type === 'etsyCancelled') }).recent.replace(/<[^>]+>/g, '|');
  assert(/Cancelled on Etsy .*seen by the sorter/.test(en), en);
  // a record's fates with no recorded step (an older cancel) still tell what stayed
  const O = '4199000003';
  store.set('Charm_Nest_Cancelled/' + O, { orderId: O, by: 'Etsy', source: 'etsy', at: Date.now() - 3600e3, fates: [{ sheet: 'GF Sheet 9', fate: 'cut' }, { sheet: 'GF Sheet 8', fate: 'removed' }] });
  const otl = await ok({ op: 'timelineGet', orderId: O });
  assert.deepStrictEqual(otl.events.filter(e => e.type === 'cancelStep').map(e => e.text).sort(), ['On a cut sheet: set aside (GF Sheet 9)', 'Removed from GF Sheet 8']);
  // a record with part B's removals: each a step, a waiting one said plainly; a station's "seen" is its own alert
  const Q = '4199000004';
  store.set('Charm_Nest_Cancelled/' + Q, { orderId: Q, by: 'Ana', source: 'sorter', at: Date.now() - 7200e3, removals: [{ id: 'sheet~GF Sheet 7', where: 'GF Sheet 7', kind: 'sheet', outcome: 'waiting', by: 'Ana', at: Date.now() - 7100e3 },
    { id: 'queue~the queue', where: 'the queue', kind: 'queue', outcome: 'removed', by: 'Ana' }, { id: 'sheet~GF Sheet 9', where: 'GF Sheet 9', kind: 'sheet', outcome: 'setAside', by: 'Ana', text: 'already cut on GF Sheet 9: set aside' },
    { id: 'station~assembly', where: 'Assembly', kind: 'station', outcome: 'seen', by: 'Kim' }] });
  const qtl = await ok({ op: 'timelineGet', orderId: Q });
  assert.deepStrictEqual(qtl.events.filter(e => e.type === 'cancelStep').map(e => e.text).sort(), ['On a cut sheet: set aside (GF Sheet 9)', 'Still on GF Sheet 7: not taken off yet', 'Taken out of the queue']);
  const qn = UI.nowStamps(qtl.events, { cancelled: qtl.events.find(e => e.type === 'cancelled') }).recent.replace(/<[^>]+>/g, '|');
  assert(qn.includes('Still on GF Sheet 7: not taken off yet') && qn.includes('Taken out of the queue') && qn.includes('Cancelled by Ana'), qn);
  assert(!warnings.some(w => /timeline not recorded/.test(w)), warnings.join('\n'));
  console.log('  ✓ 3 · timelineGet tells it all after a restore; the Now card lists the exact moment, each removal, set aside, restored');
  console.log('cx-a: all passed');
})().catch(e => { console.error(e); process.exit(1); });
