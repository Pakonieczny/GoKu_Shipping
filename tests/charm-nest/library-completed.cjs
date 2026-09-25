// The Library's Current | Completed tabs, on the server: marking a sheet or a set cut on the laser (laserDone), the
// Completed list in pages (laserDoneList), and the order / listing search (findSheets), against an in-memory Firestore
// that hands out only the fields a query selects and orders as Firestore does (a record without the field is in no
// ordered query). No network.
//   node tests/charm-nest/library-completed.cjs
const path = require('path'), crypto = require('crypto'), assert = require('assert');
const fnDir = path.join(__dirname, '../../netlify/functions');

/* ── in-memory Firestore ──────────────────────────────────────────────── */
const store = new Map(), reads = { n: 0 };
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
function docRef(coll, id) {
  const key = coll + '/' + id;
  return {
    id, path: key,
    async get(mask) { const d0 = store.get(key), d = d0 && mask ? pick(d0, mask) : d0; if (d) reads.n++; return { exists: !!d, id, ref: this, data: () => (d ? clone(d) : undefined) }; },
    collection(sub) { return query(key + '/' + sub); },
    async set(data, opts) { store.set(key, merge(opts && opts.merge ? clone(store.get(key) || {}) : {}, data, !!(opts && opts.merge))); },
    async update(data) {
      const cur = store.get(key); if (!cur) throw new Error('NOT_FOUND: ' + key);
      const next = clone(cur);
      for (const [k, v] of Object.entries(data)) { if (v === SENT.del) delete next[k]; else next[k] = value(next[k], v); }
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
    doc(id) { return docRef(coll, id || 'auto' + crypto.randomBytes(6).toString('hex')); },
    count() { return { get: async () => ({ data: () => ({ count: q.rows().length }) }) }; },
    async get() {
      const docs = q.rows().map(({ id, v }) => { reads.n++; const d = mask ? pick(v, mask) : v; return { id, exists: true, ref: docRef(coll, id), data: () => clone(d) }; });
      return { size: docs.length, docs, empty: !docs.length };
    },
    rows() {
      let rows = [...store.entries()].filter(([k]) => k.startsWith(coll + '/') && !k.slice(coll.length + 1).includes('/')).map(([k, v]) => ({ id: k.slice(coll.length + 1), v }));
      for (const [f, op, v0] of filters) rows = rows.filter(({ v: d }) => {
        const x = norm(getPath(d, f)), v = Array.isArray(v0) ? v0.map(norm) : norm(v0);
        if (op === '==') return x === v; if (op === 'in') return v.includes(x);
        if (op === 'array-contains') return Array.isArray(getPath(d, f)) && getPath(d, f).includes(v0);
        if (op === 'array-contains-any') { assert(v0.length <= 30, 'array-contains-any takes at most 30 values'); return Array.isArray(getPath(d, f)) && getPath(d, f).some(e => v0.includes(e)); }
        if (x === undefined || x === null || typeof x !== typeof v) return false;   // a range never matches a missing field
        return op === '<' ? x < v : op === '<=' ? x <= v : op === '>' ? x > v : op === '>=' ? x >= v : false;
      });
      // one field in a query's filters and order (no composite index): this build never asks for more
      const fields = new Set(filters.filter(([, op]) => !['==', 'in', 'array-contains', 'array-contains-any'].includes(op)).map(([f]) => f).concat(order ? [order[0]] : []));
      assert(fields.size <= 1, 'a query ranges and orders on one field only: ' + [...fields]);
      const key = r => getPath(r.v, order[0]);
      // Firestore orders ties by document name, in the order's direction
      if (order) { rows = rows.filter(r => key(r) !== undefined); rows.sort((a, b) => { const x = norm(key(a)), y = norm(key(b)); const c = x > y ? 1 : x < y ? -1 : a.id > b.id ? 1 : a.id < b.id ? -1 : 0; return order[1] === 'desc' ? -c : c; }); }
      return lim ? rows.slice(0, lim) : rows;
    }
  };
  return q;
}
const db = {
  collection: c => query(c),
  batch() { const ops = []; return { set(ref, d, o) { ops.push(() => ref.set(d, o)); }, update(ref, d) { ops.push(() => ref.update(d)); }, delete(ref) { ops.push(() => ref.delete()); }, async commit() { for (const o of ops) await o(); } }; },
  async getAll(...refs) { const o = refs.length && typeof refs[refs.length - 1].get !== 'function' ? refs.pop() : null; return Promise.all(refs.map(r => r.get(o && o.fieldMask))); },
  async runTransaction(fn) {
    // reads before writes, as Firestore requires
    let wrote = false; const w = f => (...a) => { wrote = true; return f(...a); }, r = f => (...a) => { assert(!wrote, 'a transaction reads before it writes'); return f(...a); };
    return fn({ get: r(ref => ref.get()), getAll: r((...refs) => db.getAll(...refs)), set: w((ref, d, o) => ref.set(d, o)), update: w((ref, d) => ref.update(d)), delete: w(ref => ref.delete()) });
  }
};
const bucket = { name: 'test-bucket', file: p => ({ name: p, async exists() { return [false]; }, async getMetadata() { throw Object.assign(new Error('no blob'), { code: 404 }); } }) };
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue, FieldPath: { documentId: () => '__name__' }, Timestamp: { fromMillis: ts } }), storage: () => ({ bucket: () => bucket }) };
const Module = require('module'), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === 'node-fetch') return async () => ({ ok: true, status: 202 });
  if (req === 'firebase-admin' || req === './firebaseAdmin' || /[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin;
  return realLoad.call(this, req, ...rest);
};
const lib = require(path.join(fnDir, 'charmNestLibrary.js'));
const post = body => lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
const ok = async body => { const r = await post(body); assert.strictEqual(r.status, 200, body.op + ': ' + JSON.stringify(r.body)); return r.body; };
const doc = (c, id) => store.get(c + '/' + id);

/* ── a shop: two sets and a sheet on its own, with orders, listings and the run lines that bought them ── */
const DAY = '2026-09-24', T = Date.parse(DAY + 'T15:00:00Z');
function sheet(id, o) {
  store.set('Charm_Nest_Sheets/' + id, Object.assign({ id, metal: 'gold', metalLabel: 'GF 14/20', day: DAY, status: 'complete', placedCount: 12, charmCount: 12, density: 0.7, freePt2: 100, verification: { ok: true },
    outputs: { preview: { path: `charmnest/x/${id}/preview.png`, url: `https://example.test/${id}.png` } }, poolIds: [], orders: [], names: '', archived: false, updatedAt: ts(T), createdAt: ts(T),
    charms: [{ id: 'c1', name: 'heavy outline data' }], placements: [{ id: 'c1' }] }, o));
}
store.set('Charm_Nest_Sets/set-A', { setId: 'set-A', seq: 1, day: DAY, runId: 'run-A', name: 'Set-1', sheetIds: ['shA1', 'shA2', 'shA3'], materials: ['gold', 'silver'], status: 'nesting', orders: {}, updatedAt: ts(T), createdAt: ts(T) });
store.set('Charm_Nest_Sets/set-B', { setId: 'set-B', seq: 2, day: DAY, runId: 'run-B', name: 'Set-2', sheetIds: ['shB1', 'shB2'], materials: ['rose'], status: 'nesting', orders: {}, updatedAt: ts(T), createdAt: ts(T) });
sheet('shA1', { setId: 'set-A', setSeq: 1, sheetIndex: 1, runId: 'run-A', fileBase: 'GF_Sep.24.26_Set-1_Sheet-1', orders: ['3700000001', '3700000002'], listings: ['1718001', '1718002'], names: '3700000001 · BR-TST-01 3700000002 · BR-TST-02' });
sheet('shA2', { setId: 'set-A', setSeq: 1, sheetIndex: 2, runId: 'run-A', fileBase: 'GF_Sep.24.26_Set-1_Sheet-2', orders: ['3700000003'], listings: ['1718001'], names: '3700000003 · BR-TST-01' });
sheet('shA3', { setId: 'set-A', setSeq: 1, sheetIndex: 1, runId: 'run-A', metal: 'silver', metalLabel: 'SS', fileBase: 'SS_Sep.24.26_Set-1_Sheet-1', orders: ['3700000002'], listings: ['1718002'], names: '3700000002 · BR-TST-02' });
// set B is older: its sheets were saved before `listings` was written, and only its run's lines say what was bought
sheet('shB1', { setId: 'set-B', setSeq: 2, sheetIndex: 1, runId: 'run-B', metal: 'rose', metalLabel: 'RG', fileBase: 'RG_Sep.24.26_Set-2_Sheet-1', orders: ['3700000010'], names: '3700000010 · BR-TST-09' });
sheet('shB2', { setId: 'set-B', setSeq: 2, sheetIndex: 2, runId: 'run-B', metal: 'rose', metalLabel: 'RG', fileBase: 'RG_Sep.24.26_Set-2_Sheet-2', orders: ['3700000011', '3700000012'], names: '3700000011 · BR-TST-09 3700000012 · BR-TST-05' });
sheet('shS1', { runId: 'run-C', draft: true, fileBase: 'GF_Sep.24.26_Sheet-1', orders: ['3700000020'], names: '3700000020 · BR-TST-07' });
const line = (orderId, tx, listingId) => ({ orderId, transactionId: tx, sku: 'BR', state: 'written', snap: { listingId: String(listingId), title: 't' } });
// run B keeps some lines in its record, some beside it (live) and the rest in its line archive
const liveJson = JSON.stringify({ '3700000011_2': line('3700000011', '2', 1719999) });
store.set('Charm_Nest_Run_Live/run-B~live1', { json: liveJson, bytes: liveJson.length, lines: 1, seq: 0 });
store.set('Charm_Nest_Runs/run-B', { runId: 'run-B', day: DAY, status: 'complete', lines: undefined, liveLines: { ids: ['run-B~live1'], lines: 1 }, lineArchive: { parts: 1, lines: 2 }, updatedAt: ts(T), createdAt: ts(T) });
const archJson = JSON.stringify({ '3700000010_1': line('3700000010', '1', 1719999), '3700000012_3': line('3700000012', '3', 1718555) });
store.set('Charm_Nest_Run_Lines/run-B~p1', { runId: 'run-B', json: archJson, bytes: archJson.length, lines: 2, keys: ['3700000010_1', '3700000012_3'], orders: ['3700000010', '3700000012'], at: T, seq: 0 });
store.set('Charm_Nest_Runs/run-C', { runId: 'run-C', day: DAY, status: 'complete', lines: { '3700000020_1': line('3700000020', '1', 1718555) }, updatedAt: ts(T), createdAt: ts(T) });
store.set('Charm_Nest_Runs/run-A', { runId: 'run-A', day: DAY, status: 'complete', lines: {}, updatedAt: ts(T), createdAt: ts(T) });

(async () => {
  /* ── marking ── */
  assert.strictEqual((await post({ op: 'laserDone', kind: 'sheet', id: 'shA1', done: true })).status, 400, 'a mark says who made it');
  assert.strictEqual((await post({ op: 'laserDone', kind: 'thing', id: 'shA1', by: 'Paul' })).status, 400);
  assert.strictEqual((await post({ op: 'laserDone', kind: 'sheet', id: 'nope-1', by: 'Paul' })).status, 404);
  assert.strictEqual((await post({ op: 'laserDone', kind: 'set', id: 'set-none', by: 'Paul' })).status, 404);
  let r = await ok({ op: 'laserDone', kind: 'sheet', id: 'shA1', done: true, by: 'Paul' });
  assert(r.at > 0 && r.by === 'Paul' && r.setId === 'set-A' && r.setDone === false && r.setChanged === false, JSON.stringify(r));
  assert.deepStrictEqual(r.counts, { sheets: 1, sets: 0 });
  assert.strictEqual(doc('Charm_Nest_Sheets', 'shA1').laserDoneBy, 'Paul');
  assert.strictEqual(doc('Charm_Nest_Sheets', 'shA1').laserDoneAt, r.at, 'a time in ms');
  assert(!doc('Charm_Nest_Sets', 'set-A').laserDoneAt, 'a set is not completed while a sheet of it is not');
  const markedAt = r.at;

  // an open run saving its copy of the sheet keeps the mark; op_setUpdate cannot write it either
  await ok({ op: 'putSheet', sheet: { id: 'shA1', metal: 'gold', placedCount: 12, laserDoneAt: null, laserDoneBy: null, listings: ['1718001', 1718003, 'x', '1718001'] } });
  assert.strictEqual(doc('Charm_Nest_Sheets', 'shA1').laserDoneAt, markedAt, 'putSheet leaves the laser mark as it is');
  assert.deepStrictEqual(doc('Charm_Nest_Sheets', 'shA1').listings, ['1718001', '1718003'], 'listings are kept as listing numbers, once each');
  await ok({ op: 'putSheet', sheet: { id: 'shA1', listings: ['1718001', '1718002'] } });
  await ok({ op: 'setUpdate', setId: 'set-B', patch: { status: 'nesting', laserDoneAt: 5, laserDoneBy: 'nobody' } });
  assert(!('laserDoneAt' in doc('Charm_Nest_Sets', 'set-B')), 'setUpdate does not write the laser mark');

  // the last sheet of a set completes the set; taking one back takes the set back
  await ok({ op: 'laserDone', kind: 'sheet', id: 'shA2', by: 'Ana' });
  r = await ok({ op: 'laserDone', kind: 'sheet', id: 'shA3', by: 'Ana' });
  assert.strictEqual(r.setDone, true); assert.strictEqual(r.setChanged, true); assert.deepStrictEqual(r.counts, { sheets: 3, sets: 1 });
  assert.strictEqual(doc('Charm_Nest_Sets', 'set-A').laserDoneBy, 'Ana');
  r = await ok({ op: 'laserDone', kind: 'sheet', id: 'shA2', done: false });
  assert.strictEqual(r.setDone, false); assert.strictEqual(r.setChanged, true); assert(!('laserDoneAt' in doc('Charm_Nest_Sets', 'set-A')) && !('laserDoneAt' in doc('Charm_Nest_Sheets', 'shA2')), 'taken back: the fields are gone');
  assert.strictEqual(doc('Charm_Nest_Sheets', 'shA1').laserDoneAt, markedAt, 'the other sheets keep their marks');
  // a set marks every sheet of it in one moment, and keeps an earlier sheet's own mark
  r = await ok({ op: 'laserDone', kind: 'set', id: 'set-A', by: 'Lee' });
  assert.deepStrictEqual(r.sheetIds, ['shA2'], 'only the sheets not marked yet');
  assert.strictEqual(doc('Charm_Nest_Sheets', 'shA1').laserDoneBy, 'Paul'); assert.strictEqual(doc('Charm_Nest_Sheets', 'shA2').laserDoneBy, 'Lee');
  assert.strictEqual(doc('Charm_Nest_Sets', 'set-A').laserDoneBy, 'Lee');
  r = await ok({ op: 'laserDone', kind: 'set', id: 'set-B', by: 'Lee' });
  assert.deepStrictEqual(r.sheetIds.sort(), ['shB1', 'shB2']); assert.strictEqual(doc('Charm_Nest_Sheets', 'shB1').laserDoneAt, doc('Charm_Nest_Sheets', 'shB2').laserDoneAt, 'one moment');
  r = await ok({ op: 'laserDone', kind: 'set', id: 'set-B', done: false });
  assert(!('laserDoneAt' in doc('Charm_Nest_Sheets', 'shB1')) && !('laserDoneAt' in doc('Charm_Nest_Sets', 'set-B')), 'a set taken back takes every sheet back');
  assert.deepStrictEqual(r.counts, { sheets: 3, sets: 1 });
  // a sheet on its own (no set) is marked alone
  r = await ok({ op: 'laserDone', kind: 'sheet', id: 'shS1', by: 'Paul' });
  assert.strictEqual(r.setId, null);

  /* ── the lists read the mark: Current leaves out what is completed ── */
  let l = await ok({ op: 'listSheets', limit: 50 });
  const A1 = l.sheets.find(s => s.id === 'shA1');
  assert(A1 && A1.laserDoneAt === markedAt && A1.laserDoneBy === 'Paul' && A1.listings.includes('1718001'), 'a list entry carries the mark and the listings (SLIM_SHEET reads them): ' + JSON.stringify(A1 && { at: A1.laserDoneAt, by: A1.laserDoneBy, l: A1.listings }));
  l = await ok({ op: 'listSheets', limit: 50, excludeDone: true });
  assert.deepStrictEqual(l.sheets.map(s => s.id).sort(), ['shB1', 'shB2'], 'excludeDone: only what is still to be cut');
  const g = await ok({ op: 'getSheet', id: 'shA2' });
  assert.strictEqual(g.sheet.laserDoneBy, 'Lee');
  const ls = await ok({ op: 'laserStatus', sheetIds: ['shA1', 'shB1'] });
  assert.strictEqual(ls.sheets.find(s => s.id === 'shA1').laserDoneAt, markedAt); assert.strictEqual(ls.sheets.find(s => s.id === 'shB1').laserDoneAt, null);
  const sl = await ok({ op: 'setList', includeSheets: true, excludeDone: true });
  assert.deepStrictEqual(sl.sets.map(s => s.setId), ['set-B'], 'a completed set is left out of Current');
  assert.strictEqual((await ok({ op: 'setList' })).sets.find(s => s.setId === 'set-A').laserDoneBy, 'Lee');
  const h = await ok({ op: 'history', limit: 10 });
  const hA = h.sets.find(x => x.setId === 'set-A');
  assert(hA && hA.laserDoneBy === 'Lee' && hA.sheets.every(s => s.laserDoneAt > 0), 'the run history shows the marks');

  /* ── Completed, in pages: newest first, ties (a set's sheets) never lost or repeated across pages ── */
  let c = await ok({ op: 'laserDoneList', countOnly: true });
  assert.deepStrictEqual(c, { counts: { sheets: 4, sets: 1 } });
  // set B's two sheets are marked in one moment: a page may end between them
  await ok({ op: 'laserDone', kind: 'set', id: 'set-B', by: 'Lee' });
  assert.strictEqual(doc('Charm_Nest_Sheets', 'shB1').laserDoneAt, doc('Charm_Nest_Sheets', 'shB2').laserDoneAt);
  for (const limit of [1, 2, 3]) {
    const seen = []; let cursor = null, pages = 0;
    do { const p = await ok({ op: 'laserDoneList', limit, cursor }); if (!cursor) assert.deepStrictEqual(p.counts, { sheets: 6, sets: 2 }); else assert(!p.counts); seen.push(...p.rows.map(x => x.id)); cursor = p.next; pages++; } while (cursor && pages < 20);
    assert.deepStrictEqual([...new Set(seen)].sort(), ['shA1', 'shA2', 'shA3', 'shB1', 'shB2', 'shS1'], 'every completed sheet once: ' + seen);
    assert.strictEqual(seen.length, 6, 'none twice: ' + seen);
    assert.deepStrictEqual(seen.slice(0, 2).sort(), ['shB1', 'shB2'], 'newest first');
  }
  await ok({ op: 'laserDone', kind: 'set', id: 'set-B', done: false });
  const all = await ok({ op: 'laserDoneList', limit: 60 });
  assert(all.rows.every((x, i, a) => !i || a[i - 1].at >= x.at), 'ordered by completion');
  const rA1 = all.rows.find(x => x.id === 'shA1');
  assert.deepStrictEqual({ kind: rA1.kind, orders: rA1.orders, pieces: rA1.pieces, fill: rA1.fill, preview: rA1.preview, by: rA1.by, setSeq: rA1.setSeq, sheetIndex: rA1.sheetIndex }, { kind: 'sheet', orders: 2, pieces: 12, fill: 0.7, preview: 'https://example.test/shA1.png', by: 'Paul', setSeq: 1, sheetIndex: 1 });
  assert.strictEqual(all.next, null, 'the end is said');
  assert.deepStrictEqual((await ok({ op: 'laserDoneList', metal: 'silver' })).rows.map(x => x.id), ['shA3'], 'a metal');
  assert.deepStrictEqual((await ok({ op: 'laserDoneList', q: 'BR-TST-07' })).rows.map(x => x.id), ['shS1'], 'a SKU (in the charm names)');
  assert.deepStrictEqual((await ok({ op: 'laserDoneList', q: 'sep 24 26 set 1 sheet 2' })).rows.map(x => x.id), ['shA2'], 'a file name, however it is typed');
  assert.deepStrictEqual((await ok({ op: 'laserDoneList', q: '3700000002' })).rows.map(x => x.id).sort(), ['shA1', 'shA3'], 'an order number');
  const sets = await ok({ op: 'laserDoneList', kind: 'sets' });
  assert.strictEqual(sets.rows.length, 1); const sA = sets.rows[0];
  assert.deepStrictEqual({ setId: sA.setId, seq: sA.seq, sheets: sA.sheets.map(s => s.id), orders: sA.orders, pieces: sA.pieces, by: sA.by }, { setId: 'set-A', seq: 1, sheets: ['shA1', 'shA2', 'shA3'], orders: 3, pieces: 36, by: 'Lee' });
  assert.strictEqual(sA.sheets[0].preview, 'https://example.test/shA1.png');
  assert.strictEqual((await ok({ op: 'laserDoneList', kind: 'sets', metal: 'rose' })).rows.length, 0);
  assert.strictEqual((await ok({ op: 'laserDoneList', kind: 'sets', q: 'BR-TST-02' })).rows.length, 1, 'a set is found by what its sheets hold');
  // a search reads at most DONE_SCAN records a call and says where it stopped
  for (let i = 0; i < 900; i++) sheet('fill' + i, { fileBase: 'GF_filler_' + i, laserDoneAt: T - 1000 - i, laserDoneBy: 'x', names: 'filler' });
  sheet('Fold', { fileBase: 'GF_needle', laserDoneAt: T - 5000, laserDoneBy: 'x', names: 'needle-in-the-haystack' });
  let s1 = await ok({ op: 'laserDoneList', q: 'needle-in-the-haystack', limit: 10 });
  assert(s1.scanned <= 800 && s1.next, 'one call is bounded: ' + s1.scanned);
  let found = s1.rows.map(x => x.id), guard = 0;
  while (s1.next && guard++ < 5) { s1 = await ok({ op: 'laserDoneList', q: 'needle-in-the-haystack', limit: 10, cursor: s1.next }); found = found.concat(s1.rows.map(x => x.id)); }
  assert.deepStrictEqual(found, ['Fold'], 'and the next call carries on');
  for (let i = 0; i < 900; i++) store.delete('Charm_Nest_Sheets/fill' + i);
  store.delete('Charm_Nest_Sheets/Fold');

  /* ── find: an order, a listing, and a listing on sheets saved before `listings` ── */
  assert.strictEqual((await post({ op: 'findSheets', q: 'BR-TST' })).status, 400);
  let f = await ok({ op: 'findSheets', q: '3700000002' });
  assert.deepStrictEqual(f.rows.map(x => x.id).sort(), ['shA1', 'shA3'], 'an order: its completed sheets as Completed rows');
  assert(f.rows.every(x => x.match.includes('order'))); assert.strictEqual(f.matches.order, 2);
  assert.strictEqual(f.fallback, null, 'a number found as an order is not looked for in run lines');
  assert.deepStrictEqual(f.setRows.map(x => x.setId), ['set-A']); assert(f.sets.some(s => s.setId === 'set-A'));
  f = await ok({ op: 'findSheets', q: '1718001' });
  assert.deepStrictEqual(f.rows.map(x => x.id).sort(), ['shA1', 'shA2'], 'a listing, from the sheets\' listings');
  assert(f.rows.every(x => x.match.includes('listing'))); assert.strictEqual(f.matches.listing, 2);
  f = await ok({ op: 'findSheets', q: '1719999', today: DAY });
  assert.deepStrictEqual(f.sheets.map(x => x.id).sort(), ['shB1', 'shB2'], 'older sheets through their run\'s live and archived lines: ' + JSON.stringify(f));
  assert(f.sheets.every(x => x.match.includes('listing') && x.laser && 'ready' in x.laser), 'a sheet not completed comes as a Library card, with its readiness');
  assert.deepStrictEqual(f.fallback.window, { from: '2026-08-26', to: DAY }); assert.strictEqual(f.fallback.capped, false); assert.strictEqual(f.fallback.orders, 2); assert.strictEqual(f.fallback.parts, 1);
  f = await ok({ op: 'findSheets', q: '1718555', today: DAY });
  assert.deepStrictEqual(f.sheets.map(x => x.id), ['shB2'], 'an archived line');
  assert.deepStrictEqual(f.rows.map(x => x.id), ['shS1'], 'and a line kept in the run record, whatever the sheet\'s metal or set');
  f = await ok({ op: 'findSheets', q: '9999999', today: DAY });
  assert(!f.sheets.length && !f.rows.length && f.fallback, 'nothing found says so');
  // the fallback is bounded, and says when a cap cut it short
  for (let i = 0; i < 130; i++) { sheet('legacy' + i, { runId: 'run-L' + i, fileBase: 'L' + i }); store.set('Charm_Nest_Runs/run-L' + i, { runId: 'run-L' + i, day: DAY, lines: {}, updatedAt: ts(T) }); }
  f = await ok({ op: 'findSheets', q: '1719999', today: DAY });
  assert.strictEqual(f.fallback.capped, true, 'more runs than one search reads: capped');
  assert(f.fallback.runs <= 120);
  console.log('Library Completed OK · marks (sheet, set, the set with its last sheet, taken back), kept by putSheet and setUpdate · Current leaves out completed · Completed in pages with ties kept, metal and search bounded per call · find by order, by listing and by the run lines of older sheets, capped and said so');
})().catch(e => { console.error(e); process.exit(1); });
