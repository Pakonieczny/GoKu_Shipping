// Server stamps (Paul, 28 Sep, D1-D3: every change is recorded, every step passed is a stamped milestone). The
// charmNestLibrary ops that already know who did what write it on the order's timeline (Order_Timeline): customPut,
// laserDone, backPut, poolUpdate, customDecide, roseRecordCut, arrivalRecord and cancelRestore. Only the real milestones
// (MILESTONES in _orderTimeline.js, Paul, 28 Sep) are milestones: a Complete Order press and a label print are sealed
// points on the Office lane and a set committed is a step in between, none of them a milestone. Each event has a stable
// id (an op sent twice writes it once), each op adds at most one timeline batch, and a timeline that cannot be written
// never fails the op. Runs the real handler against an in-memory Firestore. No network.
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
const Timeline = require(path.join(fnDir, '_orderTimeline.js'));
let lastRequestAt=Date.now();
// Separate human actions have separate clock ticks; instant in-memory requests must not collide on a timestamp ID.
const post = async body => {
  const realNow=Date.now,tick=lastRequestAt=Math.max(realNow(),lastRequestAt+1);Date.now=()=>tick;
  try {const r=await lib.handler({httpMethod:'POST',headers:{},body:JSON.stringify(body),queryStringParameters:{}});return {status:r.statusCode,body:JSON.parse(r.body || '{}')};}
  finally {Date.now=realNow;}
};
const ok = async body => { const before = timelineBatches.n, r = await post(body); assert.strictEqual(r.status, 200, body.op + ': ' + JSON.stringify(r.body)); assert(timelineBatches.n - before <= 1, body.op + ' writes at most one timeline batch'); return r.body; };
const events = (prefix = '') => [...store].filter(([k]) => k.startsWith(prefix + 'Order_Timeline/')).map(([k, v]) => Object.assign({ key: k.slice(k.indexOf('/') + 1) }, v));
const of = (orderId, type, prefix) => events(prefix).filter(e => e.orderId === orderId && (!type || e.type === type));
const one = (orderId, type) => { const l = of(orderId, type); assert.strictEqual(l.length, 1, `one ${type} for ${orderId}: ${JSON.stringify(l)}`); return l[0]; };
const T = Date.now() - 3600e3;

(async () => {
  /* ── customPut: each new seal is an event (print → sealPrinted, Complete Order → sealCompleted); both are sealed points
     on the Office lane, not milestones (only MILESTONES are) ── */
  const R1 = '4170000001';
  await ok({ op: 'customPut', key: R1 + '_5001', receiptId: R1, transactionId: '5001', sku: 'CUSTOM-NAME', by: 'Ana' });
  const printed = one(R1, 'sealPrinted');
  assert.strictEqual(printed.by, 'Ana'); assert.strictEqual(printed.lineKey, R1 + '_5001'); assert.strictEqual(printed.transactionId, '5001');
  assert(printed.at > T && printed.milestone === false && printed.source === 'sorter' && /print 1/.test(printed.text), JSON.stringify(printed));
  assert.strictEqual(printed.at, store.get(`Charm_Custom_Orders/${R1}_5001`).stamps[0].at, 'the event is the seal the record keeps');
  await ok({ op: 'customPut', key: R1 + '_5001', receiptId: R1, transactionId: '5001', how: 'button', by: 'Ben' });
  const completed = one(R1, 'sealCompleted');
  assert(completed.by === 'Ben' && completed.milestone === false && completed.data.how === 'button', JSON.stringify(completed));
  assert(!Timeline.MILESTONES.has('sealCompleted') && !Timeline.MILESTONES.has('sealPrinted'), 'a seal is a point, not a milestone');
  assert.strictEqual(of(R1).length, 2);

  /* ── laserDone: every order on the sheet, with its label; marked again adds nothing; undone is a note ── */
  const SET = 'set-2026-09-28-1', R2 = '4170000002', R3 = '4170000003', R4 = '4170000004';
  // Timeline checks use genuinely laser-ready fixture records: every line, file and QR is present.
  const runId='stamp-laser-fixture', lineRows={};
  store.set(`Charm_Nest_Sets/${SET}`, { setId: SET, seq: 1, runId, sheetIds: ['sh-gf-1', 'sh-gf-2'] });
  for(const [sheetId,index,orders] of [['sh-gf-1',2,[R2,R3]],['sh-gf-2',3,[R4]]]) {
    const poolIds=orders.map((order,i)=>order+'_'+(sheetId==='sh-gf-1'?60001+i:60003+i)+'_1');
    for(const pool of poolIds)lineRows[pool.slice(0,pool.lastIndexOf('_'))]={orderId:pool.split('_')[0],state:'written',quantity:1,poolIds:[pool],engraveCandidate:false};
    store.set('Charm_Nest_Sheets/'+sheetId,{id:sheetId,metal:'gold',setId:SET,runId,sheetIndex:index,fileBase:'GF_Sep.28.26_Set-1_Sheet-'+index,orders,poolIds,placements:[],placedCount:poolIds.length,status:'complete',verification:{ok:true},outputs:{ai:{path:sheetId+'.ai',url:'https://example.test/'+sheetId+'.ai'},preview:{path:sheetId+'.png',url:'https://example.test/'+sheetId+'.png'}},label:{files:[{path:sheetId+'-qr.png',url:'https://example.test/'+sheetId+'-qr.png',payload:orders.join(','),orders}]}});
  }
  store.set('Charm_Nest_Runs/'+runId,{runId,lines:lineRows});
  const wrongStage=await post({op:'laserDone',kind:'sheet',id:'sh-gf-1',stage:'progress',by:'Cara'});
  assert.strictEqual(wrongStage.status,409,'a timeline seal never bypasses the laser stage gate');
  const cut = await ok({ op: 'laserDone', stage: 'laser', kind: 'sheet', id: 'sh-gf-1', by: 'Cara' });
  assert(cut.ok && !('marks' in cut), 'the answer is what it was');
  for (const r of [R2, R3]) { const e = one(r, 'laserDone'); assert(e.by === 'Cara' && e.sheet === 'GF Sheet 2' && e.sheetId === 'sh-gf-1' && e.setId === SET && e.station === 'laser' && e.milestone === true && e.at === cut.at, JSON.stringify(e)); }
  assert.strictEqual(of(R4).length, 0, 'an order on another sheet is not marked');
  await ok({ op: 'laserDone', stage: 'laser', kind: 'sheet', id: 'sh-gf-1', by: 'Cara' });
  assert.strictEqual(of(R2, 'laserDone').length, 1, 'a sheet marked again (a retry) keeps its one event');
  const mark = store.get('Charm_Nest_Sheets/sh-gf-1').laserDoneAt;
  await ok({ op: 'laserDone', stage: 'laser', kind: 'sheet', id: 'sh-gf-1', done: false });
  const undone = one(R2, 'note');
  assert(/laser cut undone/.test(undone.text) && undone.data.laserDoneAt === mark &&undone.data.laserDoneBy === 'Cara' && undone.sheet === 'GF Sheet 2', JSON.stringify(undone));
  await ok({ op: 'laserDone', stage: 'laser', kind: 'sheet', id: 'sh-gf-1', done: false });
  assert.strictEqual(of(R2, 'note').length, 1, 'undone twice is one note');
  const setCut = await ok({ op: 'laserDone', stage: 'laser', kind: 'set', id: SET, by: 'Dan' });
  assert.strictEqual(of(R2, 'laserDone').length, 2, 'cut again after the undo: a second laserDone');
  const r4 = one(R4, 'laserDone'); assert(r4.sheet === 'GF Sheet 3' && r4.by === 'Dan' && r4.at === setCut.at, 'a set marks the orders of each of its sheets');

  /* ── backPut: each approved back is an engraveApproved of its order, with the words and the approver ── */
  const P2 = R2 + '_60001_1', approvedAt = T + 1000;
  const back = { poolId: P2, sheetId: 'sh-gf-1', setId: SET, order: R2, transactionId: '60001', sku: 'BR-1', copy: 1, text: 'Love you, Mom', approvedAt, approvedBy: 'Eve' };
  await ok({ op: 'backPut', back });
  const appr = one(R2, 'engraveApproved');
  assert(appr.by === 'Eve' && appr.at === approvedAt && /Love you, Mom/.test(appr.text) && appr.data.text === 'Love you, Mom' && appr.lineKey === R2 + '_60001' && appr.sheet === 'GF Sheet 2' && appr.milestone === true, JSON.stringify(appr));
  await ok({ op: 'backPut', back });
  assert.strictEqual(of(R2, 'engraveApproved').length, 1, 'the same approval sent again is one event');

  /* ── poolUpdate: placed (a milestone), moved, set committed (a step in between, not a milestone), removed; each once ── */
  const R5 = '4170000005', A = R5 + '_70001_1', B = R5 + '_70002_1';
  for (const id of [A, B]) store.set('Charm_Pool/' + id, { poolId: id, orderId: R5, transactionId: id.split('_')[1], sheetId: null, setId: SET, state: 'ready' });
  const placedPatch = { sheetId: 'sh-gf-1', setId: SET, state: 'written', sheetName: 'GF_Sep.28.26_Set-1_Sheet-2' };
  await ok({ op: 'poolUpdate', poolIds: [A, B], patch: placedPatch });
  const placed = one(R5, 'placed');
  assert(placed.milestone === true && placed.sheet === 'GF Sheet 2' && placed.sheetId === 'sh-gf-1' && placed.data.copies === 2 && placed.lineKey === '', JSON.stringify(placed));
  await ok({ op: 'poolUpdate', poolIds: [A, B], patch: placedPatch });
  assert.strictEqual(one(R5, 'placed').at, placed.at, 'the sheet saved again: still one placed, at its first time');
  const movedAt = T + 2000;
  const movePatch = { sheetId: 'sh-gf-2', sheetName: 'GF_Sep.28.26_Set-1_Sheet-3', movedFrom: 'sh-gf-1', movedTo: 'sh-gf-2', movedBy: 'Fay', movedAt };
  await ok({ op: 'poolUpdate', poolIds: [A], patch: movePatch });
  await ok({ op: 'poolUpdate', poolIds: [A], patch: movePatch });
  const moved = one(R5, 'moved');
  assert(moved.by === 'Fay' && moved.at === movedAt && moved.text === 'GF Sheet 2 → GF Sheet 3' && moved.data.from === 'sh-gf-1' && moved.data.to === 'sh-gf-2' && moved.lineKey === R5 + '_70001', JSON.stringify(moved));
  await ok({ op: 'poolUpdate', poolIds: [A, B], patch: { state: 'committed', committedAt: T + 3000 } });
  const committed = one(R5, 'setCommitted');
  assert(committed.setId === SET && committed.text === 'Set 1' && committed.milestone === false && committed.at === T + 3000, JSON.stringify(committed));
  assert(!Timeline.MILESTONES.has('setCommitted'), 'a set committed is a step in between, not a milestone');
  await ok({ op: 'poolUpdate', poolIds: [A], patch: { state: 'abandoned', sheetId: null, setId: null, removedBy: 'Gus', removedReason: 'cancelled: buyer asked', removedAt: T + 4000 } });
  const removed = one(R5, 'removed');
  // (a cancel's removal, Paul 29 Sep 00:26: "Removed from GF Sheet 3 (Set 1)", its reason and outcome in the details)
  assert(removed.by === 'Gus' && removed.sheet === 'GF Sheet 3' && removed.data.reason === 'cancelled: buyer asked' && removed.text === 'Removed from GF Sheet 3 (Set 1)' && removed.data.outcome === 'removed' && removed.at === T + 4000, JSON.stringify(removed));
  const count5 = of(R5).length;
  await ok({ op: 'poolUpdate', poolIds: [A], patch: { removedVerifiedAt: Date.now() } });
  await ok({ op: 'poolUpdate', poolIds: [B], patch: { engrave: true } });
  assert.strictEqual(of(R5).length, count5, 'a patch that says none of these records nothing');

  /* ── customDecide: a changed decision, once per order (and the sandbox apart) ── */
  const R6 = '4170000006', keys = [R6 + '_8001', R6 + '_8002'];
  await ok({ op: 'customDecide', keys, kind: 'custom', by: 'Hal' });
  const dec = one(R6, 'customDecided');
  assert(dec.by === 'Hal' && dec.data.kind === 'custom' && dec.data.lines.length === 2 && /custom order/.test(dec.text), JSON.stringify(dec));
  await ok({ op: 'customDecide', keys, kind: 'custom', by: 'Hal' });
  assert.strictEqual(of(R6, 'customDecided').length, 1, 'the same decision again (a retry) is no new event');
  await ok({ op: 'customDecide', key: keys[0], kind: 'regular', by: 'Hal' });
  const changed = of(R6, 'customDecided').find(e => e.data.kind === 'regular');
  assert(changed && changed.lineKey === keys[0] && changed.data.was[keys[0]] === 'custom', 'a changed decision is recorded with what it was');
  await ok({ op: 'customDecide', key: keys[0], kind: 'rework', by: 'Ivy', sandbox: true });
  assert.strictEqual(of(R6, 'customDecided', 'Sandbox_').length, 1, 'the sandbox writes its own timeline');
  assert.strictEqual(of(R6, 'customDecided').length, 2, 'and not production’s');

  /* ── roseRecordCut: every order on the Rose Gold sheet ── */
  const Readiness = require('../../charm-nest-readiness');
  const R7 = '4170000007', shape = (id, x, y, w, h) => ({ id, paths: [[[x, y], [x + w, y], [x + w, y + h], [x, y + h]]] });
  const shapes = [shape('pool-1', 2, 2, 10, 30)];
  store.set('Charm_Nest_Runs/run-rose', { lines: { a: { orderId: R7, state: 'written', poolIds: ['pool-1'], spec: { quantity: 1, engraveCandidate: false } } } });
  const rose = { id: 'sheet-rose', metal: 'rose', sheetIndex: 1, fileBase: 'RG_Sep.28.26_Set-1_Sheet-1', verification: { ok: true }, status: 'complete', dirty: false, saving: false, draft: false, setId: SET, runId: 'run-rose', poolIds: ['pool-1'], placedCount: 1,
    placements: [{ id: 'pool-1', cxPt: 20, cyPt: 20, angle: 0, scale: 1 }], outputs: { ai: { url: 'saved.ai' }, preview: { url: 'saved.png' } }, label: { files: [{ path: 'qr', url: 'qr.png', payload: 'x', orders: [R7] }] }, orders: [R7] };
  const claim = await ok({ op: 'roseClaim', sheetId: rose.id, wPt: 100, hPt: 50 });
  store.set('Charm_Nest_Sheets/' + rose.id, rose);
  const plan = await ok({ op: 'rosePlan', sheetId: rose.id, stockId: claim.stock.id, revision: 0, fingerprint: create.fingerprint(rose), shapesJson: JSON.stringify(shapes), allowanceMm: 0.2, cut: true });
  const saved = store.get('Charm_Nest_Sheets/' + rose.id);
  const roseLines=Object.values(store.get('Charm_Nest_Runs/run-rose').lines), physicalRose={...saved,engraving:Readiness.decisions(roseLines)};
  assert(Readiness.sheet({...physicalRose,orderReadiness:Readiness.orderReports(roseLines,[physicalRose])}).ready, 'the fixture passes both physical and whole-order production checks');
  const cutArgs = { op: 'roseRecordCut', sheetId: rose.id, stockId: claim.stock.id, revision: 0, planHash: plan.planHash, by: 'Kim' };
  const rc = await ok(cutArgs);
  assert(!('cutSheet' in rc), 'the answer is what it was');
  const rcut = one(R7, 'roseCut');
  assert(rcut.by === 'Kim' && rcut.sheet === 'RG Sheet 1' && rcut.data.revision === 1 && rcut.at === rc.cut.at && rcut.station === 'laser', JSON.stringify(rcut));
  await ok(cutArgs);
  assert.strictEqual(of(R7, 'roseCut').length, 1, 'the cut recorded again is one event');

  /* ── arrivalRecord: an order's first arrival, a milestone, once ── */
  const R8 = '4170000008';
  await ok({ op: 'arrivalRecord', orders: [{ id: R8, createTs: 1790000000 }] });
  const arr = one(R8, 'arrived');
  assert(arr.milestone === true && arr.data.firstSeenAt === store.get('Charm_Nest_Arrivals/' + R8).firstSeenAt && arr.data.createTs === 1790000000 && arr.key.endsWith('~first'), JSON.stringify(arr));
  await ok({ op: 'arrivalRecord', orders: [{ id: R8, createTs: 1790000000 }] });
  assert.strictEqual(of(R8, 'arrived').length, 1, 'seen again, it arrived once');

  /* ── cancelRestore: the cancel record is kept in the event before it is deleted ── */
  const R9 = '4170000009', cancelAt = T + 5000;
  const lines = Array.from({ length: 40 }, (_, i) => ({ transactionId: String(9000 + i), sku: 'SKU-' + i, title: 'A very long listing title that goes on and on about a charm necklace '.repeat(2), quantity: 1, material: 'gold' }));
  store.set('Charm_Nest_Cancelled/' + R9, { orderId: R9, by: 'Lea', why: 'buyer changed mind', at: cancelAt, buyer: 'Pat', sheets: ['sh-gf-1'], lines, createdAt: ts(cancelAt) });
  await ok({ op: 'cancelRestore', orderId: R9, by: 'Max' });
  assert(!store.has('Charm_Nest_Cancelled/' + R9), 'the cancel record is gone');
  const restored = one(R9, 'cancelRestored');
  assert(restored.by === 'Max' && restored.data.cancelled.by === 'Lea' && restored.data.cancelled.why === 'buyer changed mind' && restored.data.cancelled.at === cancelAt && /Was cancelled by Lea: buyer changed mind/.test(restored.text), JSON.stringify(restored).slice(0, 400));
  assert(JSON.stringify(restored.data).length <= 2048 && !restored.data.note, 'the cancel fits in the event (not dropped as too large)');
  await ok({ op: 'cancelRestore', orderId: R9, by: 'Max' });
  assert.strictEqual(of(R9).length, 1, 'restored again: nothing more to record');

  /* ── a timeline that cannot be written never fails an op: each answers as before, and a warning is logged ── */
  timelineDown = true; warnings.length = 0;
  const RX = '4170000010', total = events().length;
  store.set('Charm_Nest_Cancelled/' + RX, { orderId: RX, by: 'Lea', why: 'x', at: cancelAt });
  store.set('Charm_Pool/' + RX + '_10001_1', { poolId: RX + '_10001_1', orderId: RX, sheetId: null, state: 'ready' });
  const down = [
    { op: 'customPut', key: RX + '_1001', receiptId: RX, by: 'Ana' },
    { op: 'laserDone', stage: 'laser', kind: 'sheet', id: 'sh-gf-1', done: false },
    { op: 'backPut', back: Object.assign({}, back, { approvedAt: approvedAt + 1 }) },
    { op: 'poolUpdate', poolIds: [RX + '_10001_1'], patch: placedPatch },
    { op: 'customDecide', key: RX + '_1001', kind: 'custom', by: 'Hal' },
    { op: 'arrivalRecord', orders: [{ id: RX, createTs: 1 }] },
    { op: 'cancelRestore', orderId: RX, by: 'Max' }
  ];
  for (const body of down) { const r = await post(body); assert.strictEqual(r.status, 200, body.op + ' fails with the timeline down: ' + JSON.stringify(r.body)); assert(r.body.ok, body.op); }
  assert.strictEqual(events().length, total, 'nothing was written to the timeline');
  assert(store.get(`Charm_Custom_Orders/${RX}_1001`) && !store.has('Charm_Nest_Cancelled/' + RX) && store.get('Charm_Pool/' + RX + '_10001_1').state === 'written' && store.has('Charm_Nest_Arrivals/' + RX), 'the ops still did their own work');
  assert(warnings.filter(w => /timeline not recorded/.test(w)).length >= down.length, 'each is logged as a warning: ' + warnings.join(' | '));
  timelineDown = false;

  console.warn = realWarn;
  console.log(`server-stamps: ok (${events().length} events, ${events('Sandbox_').length} in the sandbox)`);
})().catch(e => { console.warn = realWarn; console.error(e); process.exit(1); });
