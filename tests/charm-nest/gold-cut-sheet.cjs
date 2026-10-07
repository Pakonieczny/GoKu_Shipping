// GC1: 10K and 14K gold sheets have the same green-line Cut Sheet as RG 14/20: one metal list, the server's claim / plan / record /
// list / release / take-off per metal, the leftover a cut saves (GC3's record, in the cut's own transaction), and readiness.
//   node tests/charm-nest/gold-cut-sheet.cjs
'use strict';
const assert = require('node:assert/strict'), vm = require('node:vm'), fs = require('node:fs');
const Rose = require('../../charm-nest-rose'), Readiness = require('../../charm-nest-readiness');
const Remnants = require('../../netlify/functions/_charmNestRemnants'), RoseStock = require('../../netlify/functions/_charmNestRoseStock');
const shape = (id, x, y, w, h) => ({ id, paths: [[[x, y], [x + w, y], [x + w, y + h], [x, y + h]]] });

(async () => {
  // 1. the one metal list, and the names RG keeps
  assert.deepEqual(Rose.CUT_METALS, ['rose', 'gold10k', 'gold14k']);
  for (const m of ['rose', 'gold10k', 'gold14k']) assert.equal(Rose.cuts(m), true, m + ' has a green line');
  for (const m of ['gold', 'silver', '', undefined, null, 'rose ', 'GOLD10K']) assert.equal(Rose.cuts(m), false, String(m) + ' has none');
  assert.deepEqual(['rose', 'gold10k', 'gold14k', 'gold'].map(Rose.cutCode), ['RG', '10K', '14K', '']);
  assert.deepEqual(['rose', 'gold10k', 'gold14k'].map(Rose.cutWord), ['Rose Gold', '10K Gold', '14K Gold']);
  // the browser global: CharmNestRose (RG's name) and CharmNestCutLine are the same module
  const root = {}; vm.runInNewContext(fs.readFileSync('charm-nest-rose.js', 'utf8'), root);
  assert(root.CharmNestRose && root.CharmNestRose === root.CharmNestCutLine, 'CharmNestCutLine is the same module as CharmNestRose');
  assert.equal(root.CharmNestCutLine.cuts('gold14k'), true);

  // 2. readiness: a 10K / 14K sheet that holds a physical sheet but has no green line yet is held ("Green line needed"), as RG is
  const back = (id, sheetId) => ({ poolId: id, sheetId, approvedAt: 10, approvedBy: 'Paul', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: id + '.ai', url: 'https://example.com/' + id + '.ai' } } });
  const ready = (metal, extra = {}) => { const pool = ['1000_t0_1', '1001_t1_1'], orders = ['1000', '1001'];
    return { id: 'r-' + metal, metal, setId: 'set1', runId: 'run1', sheetIndex: 1, status: 'complete', poolIds: pool, placedCount: 2, verification: { ok: true }, preview: 'https://example.com/p.png', outputs: { ai: 'https://example.com/f.ai' },
      orders, label: { files: [{ path: 'qr.png', url: 'https://example.com/qr.png', payload: 'x', orders }] }, backPool: pool.map(p => back(p, 'r-' + metal)), engraving: {}, orderReadiness: Object.fromEntries(orders.map(o => [o, { ready: true }])), ...extra }; };
  const nesting = e => e.steps.find(s => s.key === 'nesting');
  for (const metal of ['rose', 'gold10k', 'gold14k']) {
    const held = Readiness.explain(ready(metal, { roseStockId: 'rgs-1' }));
    assert.equal(nesting(held).state, 'blocked', metal + ': an unlined sheet is held');
    assert.match(nesting(held).items[0].why, /green dash line/);
    const lined = Readiness.explain(ready(metal, { roseStockId: 'rgs-1', rosePlanHash: 'h' }));
    assert.notEqual(nesting(lined).state, 'blocked', metal + ': a lined sheet is not held for its line');
    assert.equal(Readiness.explain(ready(metal)).ready, true, metal + ': a sheet with no physical sheet claimed (never cut) is not held');
  }
  assert.notEqual(nesting(Readiness.explain(ready('gold', { roseStockId: 'rgs-1' }))).state, 'blocked', 'a metal with no green line is never held for one');

  // 3. the server, in memory (a fake Firestore that refuses a read after a write)
  const store = new Map(), clone = x => structuredClone(x);
  const ref = path => ({ path, id: path.split('/').at(-1), collection: n => query(path + '/' + n), get: async () => snap(path), set: async (v, o) => put({ path }, v, o && o.merge) });
  const snap = path => ({ id: path.split('/').at(-1), ref: ref(path), exists: store.has(path), data: () => clone(store.get(path)) });
  const query = (path, filters = [], order = null, limit = Infinity, after = null) => ({ doc: id => ref(path + '/' + id), where: (...f) => query(path, [...filters, f], order, limit, after), orderBy: (...o) => query(path, filters, o, limit, after), limit: n => query(path, filters, order, n, after), startAfter: n => query(path, filters, order, limit, n),
    get: async () => { let docs = [...store.keys()].filter(k => k.startsWith(path + '/') && !k.slice(path.length + 1).includes('/')).map(snap); docs = docs.filter(d => filters.every(([f, , v]) => d.data()[f] === v)); if (order) docs.sort((a, b) => (a.data()[order[0]] - b.data()[order[0]]) * (order[1] === 'desc' ? -1 : 1)); if (after !== null) docs = docs.filter(d => d.data()[order[0]] < after); docs = docs.slice(0, limit); return { docs, size: docs.length }; } });
  const put = (r, v, merge) => { const old = merge ? store.get(r.path) || {} : {}; const out = { ...old }; for (const [k, x] of Object.entries(v)) out[k] = x && x.__inc ? (+old[k] || 0) + x.__inc : clone(x); store.set(r.path, out); };
  let serial = Promise.resolve(), writesMade = 0;
  const db = { runTransaction: fn => { const p = serial.then(async () => { const writes = []; let wrote = false; const r = await fn({ get: async x => { assert(!wrote, 'Firestore requires all reads before writes'); return x.get(); }, set: (x, v, o) => { wrote = true; writes.push(() => put(x, v, o && o.merge)); }, update: (x, v) => { wrote = true; writes.push(() => put(x, v, true)); }, delete: x => { wrote = true; writes.push(() => store.delete(x.path)); } }); writes.forEach(f => f()); writesMade += writes.length; return r; }); serial = p.catch(() => { }); return p; } };
  const FV = { serverTimestamp: () => 123456, increment: n => ({ __inc: n }) };
  const sheetLabel = d => `${Rose.cutCode(d.metal) || 'GF'} Sheet ${d.sheetIndex}`, setLabel = id => 'Set ' + (/-(\d+)$/.exec(id) || [])[1];
  const rem = Remnants({ db, col: query, FV, sheetLabel, setLabel, revDoc: () => ref('Charm_Nest_Rev/remnants') });
  const api = RoseStock({ db, col: query, FV, Readiness, sheetLabel, recordRemnant: rem.recordRemnant, stamp: async () => { } });
  const sheetDoc = (id, metal, shapes, idx) => ({ id, metal, sheetIndex: idx, setId: 'set-2026-1', fileBase: Rose.cutCode(metal) + '_x_Sheet-' + idx, verification: { ok: true }, status: 'complete', dirty: false, saving: false, draft: false, runId: 'run-test', poolIds: shapes.map(s => s.id), placedCount: shapes.length,
    placements: shapes.map(s => ({ id: s.id, cxPt: 20, cyPt: 20, angle: 0, scale: 1 })), outputs: { ai: { url: 'saved.ai' }, preview: { url: 'saved.png' } }, label: { files: [{ path: 'qr', url: 'qr.png', payload: 'test', orders: [] }] }, orders: [] });
  store.set('Charm_Nest_Runs/run-test', { lines: { a: { poolIds: ['p1', 'p2', 'p3'], spec: { engraveCandidate: false } } } });
  const stocks = () => [...store.keys()].filter(k => /^Charm_Nest_Rose_Stock\/[^/]+$/.test(k));
  const cutOne = async (sheetId, metal, shapes, idx, stockArgs = {}, by = 'Pat Lee') => {
    const claim = await api.roseClaim({ sheetId, metal, wPt: 100, hPt: 50, ...stockArgs }), stockId = claim.stock.id, doc = sheetDoc(sheetId, metal, shapes, idx);
    store.set('Charm_Nest_Sheets/' + sheetId, doc);
    const p = await api.rosePlan({ sheetId, metal, stockId, revision: claim.stock.revision, fingerprint: RoseStock.fingerprint(doc), shapesJson: JSON.stringify(shapes), allowanceMm: .2, cut: true });
    return api.roseRecordCut({ sheetId, metal, stockId, revision: claim.stock.revision, planHash: p.planHash, by, via: 'nest', device: 'charm-nest-1' });
  };

  // 3a. "nest on a leftover when one fits, else claim nothing" (onlyRemnant): with none, nothing is created or written
  const w0 = writesMade;
  assert.deepEqual(await api.roseClaim({ sheetId: 'gsheet-a', metal: 'gold10k', wPt: 100, hPt: 50, onlyRemnant: true }), { stock: null, protectedJson: null });
  assert.equal(stocks().length, 0, 'no leftover: no stock document was made'); assert.equal(writesMade, w0, 'and nothing was written');
  // 3b. a sheet of any other metal cannot use the cut-line operations
  await assert.rejects(() => api.roseClaim({ sheetId: 'gsheet-gf', metal: 'gold', wPt: 100, hPt: 50 }), /Rose Gold, 10K Gold or 14K Gold/);
  await assert.rejects(() => api.roseClaim({ sheetId: 'gsheet-ss', metal: 'silver', wPt: 100, hPt: 50 }), /Rose Gold, 10K Gold or 14K Gold/);
  assert.equal((await api.roseClaim({ sheetId: 'gsheet-old', wPt: 100, hPt: 50, fresh: true })).stock.metal, 'rose', 'a request with no metal is Rose Gold (the pages before this change)');
  await api.roseRelease({ sheetId: 'gsheet-old', stockId: stocks().map(k => k.split('/')[1])[0] });
  for (const k of stocks()) store.delete(k);

  // 3c. a 10K sheet is cut: its own physical sheet, its line, its leftover record
  const one = await cutOne('gsheet-1', 'gold10k', [shape('p1', 2, 2, 10, 30)], 1);
  const stockId = one.stock.id;
  assert.equal(one.stock.metal, 'gold10k', 'the physical sheet belongs to the metal that was cut');
  assert.deepEqual([one.stock.lastCutBy, one.stock.lastCutSheetId, one.stock.lastCutLabel, one.stock.revision], ['Pat Lee', 'gsheet-1', '10K Sheet 1', 1]);
  assert(JSON.parse(one.cut.planJson).lines.length, 'a green dash line was saved');
  const rec = store.get(`Charm_Nest_Remnants/${stockId}-1`);
  assert(rec, 'the cut saved its leftover sheet in the same transaction (GC3 hook)');
  assert.deepEqual([rec.metal, rec.code, rec.sheetName, rec.status, rec.via], ['gold10k', '10K', '10K Sheet 1', 'available', 'nest']);
  // 3d. the leftover is for 10K sheets only
  assert.deepEqual((await api.roseList({ metal: 'gold10k' })).stocks.map(s => s.id), [stockId]);
  assert.deepEqual((await api.roseList({ metal: 'gold14k' })).stocks, [], '14K cannot see 10K leftovers');
  assert.deepEqual((await api.roseList({ metal: 'rose' })).stocks, [], 'nor can Rose Gold');
  assert.equal((await api.roseList({})).stocks.length, 1, 'no metal asked: every leftover, as before');
  await assert.rejects(() => api.roseList({ metal: 'silver' }), /Rose Gold, 10K Gold or 14K Gold/);
  assert.deepEqual(await api.roseClaim({ sheetId: 'gsheet-14', metal: 'gold14k', wPt: 100, hPt: 50, onlyRemnant: true }), { stock: null, protectedJson: null }, 'a 14K sheet of the same size takes no 10K leftover');
  const rg = await api.roseClaim({ sheetId: 'gsheet-rg', metal: 'rose', wPt: 100, hPt: 50 });
  assert.notEqual(rg.stock.id, stockId, 'a Rose Gold sheet of the same size gets its own new physical sheet, not the 10K leftover');
  await assert.rejects(() => api.roseClaim({ sheetId: 'gsheet-rg2', metal: 'rose', stockId, wPt: 100, hPt: 50 }), /10K Gold, not Rose Gold/, 'asking for that leftover by id is refused');
  await assert.rejects(() => api.roseClaim({ sheetId: 'gsheet-14', metal: 'gold14k', stockId, wPt: 100, hPt: 50 }), /10K Gold, not 14K Gold/);
  await api.roseRelease({ sheetId: 'gsheet-rg', stockId: rg.stock.id });
  assert.equal(store.has('Charm_Nest_Rose_Stock/' + rg.stock.id), true, 'Rose Gold lets go as it always did: the stock stays, available');
  // 3e. the next 10K sheet nests on that leftover
  const next = await api.roseClaim({ sheetId: 'gsheet-2', metal: 'gold10k', wPt: 100, hPt: 50, onlyRemnant: true });
  assert.equal(next.stock.id, stockId, 'the next 10K sheet takes the leftover'); assert.equal(next.stock.profileJson, one.stock.profileJson);
  // a plan whose sheet and stock are different metals is refused (a sheet that changed metal under a claim)
  const laterShapes = [shape('p2', 25, 2, 8, 25)], later = sheetDoc('gsheet-2', 'gold14k', laterShapes, 2); store.set('Charm_Nest_Sheets/gsheet-2', later);
  await assert.rejects(() => api.rosePlan({ sheetId: 'gsheet-2', metal: 'gold14k', stockId, revision: 1, fingerprint: RoseStock.fingerprint(later), shapesJson: JSON.stringify(laterShapes), allowanceMm: .2, cut: true }), /10K Gold, not 14K Gold/);
  store.set('Charm_Nest_Sheets/gsheet-2', sheetDoc('gsheet-2', 'gold10k', laterShapes, 2));
  const doc2 = store.get('Charm_Nest_Sheets/gsheet-2');
  const p2 = await api.rosePlan({ sheetId: 'gsheet-2', metal: 'gold10k', stockId, revision: 1, fingerprint: RoseStock.fingerprint(doc2), shapesJson: JSON.stringify(laterShapes), allowanceMm: .2, cut: true });
  const before = JSON.parse(next.stock.profileJson), after = JSON.parse(p2.planJson).profile;
  assert(after.values.every((v, i) => v >= before.values[i]), 'the second line only goes further in');
  const two = await api.roseRecordCut({ sheetId: 'gsheet-2', metal: 'gold10k', stockId, revision: 1, planHash: p2.planHash, by: 'Ana', via: 'library' });
  assert.equal(two.stock.revision, 2);
  assert.equal(store.get(`Charm_Nest_Remnants/${stockId}-1`).status, 'used', 'the leftover the second cut was made on turned used');
  assert.deepEqual([store.get(`Charm_Nest_Remnants/${stockId}-2`).metal, store.get(`Charm_Nest_Remnants/${stockId}-2`).via], ['gold10k', 'library']);
  assert.equal((await api.roseGet({ stockId })).cuts.length, 2, 'both cuts are in the stock history');
  // a cut is recorded once however often it is pressed
  assert.equal((await api.roseRecordCut({ sheetId: 'gsheet-2', metal: 'gold10k', stockId, revision: 1, planHash: p2.planHash })).cut.at, two.cut.at);
  assert.equal([...store.keys()].filter(k => k.startsWith('Charm_Nest_Remnants/')).length, 2);

  // 3f. a 14K sheet: its own stock; a fresh claim that is let go again leaves nothing behind (an uncut sheet is no leftover)
  const f14 = await api.roseClaim({ sheetId: 'gsheet-3', metal: 'gold14k', wPt: 100, hPt: 50, fresh: true });
  assert.equal(f14.stock.metal, 'gold14k');
  await api.roseRelease({ sheetId: 'gsheet-3', stockId: f14.stock.id });
  assert.equal(store.has('Charm_Nest_Rose_Stock/' + f14.stock.id), false, 'an uncut 14K sheet that is let go is deleted, not offered as a leftover');
  assert.deepEqual((await api.roseList({ metal: 'gold14k' })).stocks, []);
  // ...but a cut leftover that is let go stays available for its metal
  const held = await api.roseClaim({ sheetId: 'gsheet-4', metal: 'gold10k', wPt: 100, hPt: 50, onlyRemnant: true });
  assert.equal(held.stock.id, stockId);
  await api.roseRelease({ sheetId: 'gsheet-4', stockId });
  assert.deepEqual((await api.roseList({ metal: 'gold10k' })).stocks.map(s => s.id), [stockId], 'a cut 10K leftover let go is available again');

  // 3g. 14K cut on its own; the leftover is a 14K one
  const c14 = await cutOne('gsheet-5', 'gold14k', [shape('p3', 2, 2, 12, 20)], 5, { fresh: true });
  assert.equal(c14.stock.metal, 'gold14k'); assert.equal(store.get(`Charm_Nest_Remnants/${c14.stock.id}-1`).code, '14K');
  assert.deepEqual((await api.roseList({ metal: 'gold14k' })).stocks.map(s => s.id), [c14.stock.id]);
  assert.deepEqual((await api.roseList({ metal: 'gold10k' })).stocks.map(s => s.id), [stockId], '10K and 14K leftovers stay apart');

  // 3h. a sheet of a metal with no green line: take-off changes nothing, plan is refused
  store.set('Charm_Nest_Sheets/gsheet-gf', { id: 'gsheet-gf', metal: 'gold', placements: [] });
  assert.deepEqual(await api.roseTakeOff({ sheetId: 'gsheet-gf', gone: ['x'] }), { ok: true, changed: false });
  await assert.rejects(() => api.rosePlan({ sheetId: 'gsheet-gf', stockId, revision: 2, fingerprint: 'x', shapesJson: '[]', allowanceMm: .2 }), /layout first/);
  // the protected-layout guard names the metal that holds it, and Rose Gold's wording is unchanged
  const guard = { placements: [{ id: 'a', cxPt: 1, cyPt: 1, angle: 0 }] };
  assert.throws(() => RoseStock.assertProtected(guard, [], 'gold14k'), /protected 14K Gold layout cannot be moved/);
  assert.throws(() => RoseStock.assertProtected(guard, []), /protected Rose Gold layout cannot be moved/);
  console.log('gold-cut-sheet: ok (one metal list, per-metal stock, onlyRemnant claims nothing, leftovers stay in their metal, cut + leftover record for 10K and 14K, readiness)');
})().catch(e => { console.error(e); process.exit(1); });
