// Options Studio round 2, the data side: sheetMake (a person makes a blank sheet of any size), sheetDelete (a soft delete with a reason), and how a made sheet behaves as a partial
// (listed as kind 'new', claimed through the real partial claim, held by one sheet only, a cut on it makes revision 1 and turns its revision-0 record used, history says made / deleted).
// Offline: a fake Firestore (nested arrays refused on every write, a transaction may not read after it wrote, any removal of a document is counted: there must be none).
//   node tests/charm-nest/options-sheets-data.cjs
'use strict';
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const assert = require('node:assert/strict');
const Rose = require('../../charm-nest-rose'), Readiness = require('../../charm-nest-readiness'), P = require('../../charm-nest-partial');
const Remnants = require('../../netlify/functions/_charmNestRemnants'), RoseStock = require('../../netlify/functions/_charmNestRoseStock');
const MM = Rose.MM, pause = ms => new Promise(r => setTimeout(r, ms));
const shape = (id, x, y, w, h) => ({ id, paths: [[[x, y], [x + w, y], [x + w, y + h], [x, y + h]]] });

(async () => {
  const store = new Map(), clone = x => structuredClone(x); let writes = 0, removed = 0;
  const mask = (d, f) => (f ? Object.fromEntries(Object.entries(d).filter(([k]) => f.includes(k))) : d);
  const ref = path => ({ path, id: path.split('/').at(-1), collection: n => query(path + '/' + n), get: async () => snap(path), set: async (v, o) => put({ path }, v, o && o.merge) });
  const snap = (path, f) => ({ id: path.split('/').at(-1), ref: ref(path), exists: store.has(path), data: () => mask(clone(store.get(path)), f) });
  const query = (path, filters = [], order = null, limit = Infinity, f = null, after = null) => ({
    doc: id => ref(path + '/' + id), where: (...x) => query(path, [...filters, x], order, limit, f, after), orderBy: (...o) => query(path, filters, o, limit, f, after), limit: n => query(path, filters, order, n, f, after),
    select: (...x) => query(path, filters, order, limit, x, after), startAfter: v => query(path, filters, order, limit, f, v),
    get: async () => {
      let docs = [...store.keys()].filter(k => k.startsWith(path + '/') && !k.slice(path.length + 1).includes('/')).map(k => snap(k));
      docs = docs.filter(d => filters.every(([k, , v]) => d.data()[k] === v));
      if (order) { docs = docs.filter(d => d.data()[order[0]] !== undefined); docs.sort((a, b) => (a.data()[order[0]] - b.data()[order[0]]) * (order[1] === 'desc' ? -1 : 1)); if (after != null) docs = docs.filter(d => (order[1] === 'desc' ? d.data()[order[0]] < after : d.data()[order[0]] > after)); }
      docs = docs.slice(0, limit).map(d => snap(d.ref.path, f)); return { docs, size: docs.length };
    } });
  const put = (r, v, merge) => { writes++; refuseNestedArrays(v, r.path); const old = merge ? store.get(r.path) || {} : {}; const out = { ...old }; for (const [k, x] of Object.entries(v)) out[k] = x && x.__inc ? (+old[k] || 0) + x.__inc : clone(x); store.set(r.path, out); };
  let serial = Promise.resolve();
  const db = { runTransaction: fn => { const p = serial.then(async () => { const w = []; let wrote = false; const r = await fn({ get: async x => { assert(!wrote, 'Firestore requires all reads before writes'); return x.get(); }, set: (x, v, o) => { wrote = true; w.push(() => put(x, v, o && o.merge)); }, update: (x, v) => { wrote = true; w.push(() => put(x, v, true)); }, delete: x => { wrote = true; w.push(() => { removed++; store.delete(x.path); }); } }); w.forEach(f => f()); return r; }); serial = p.catch(() => {}); return p; } };
  const FV = { serverTimestamp: () => 123456, increment: n => ({ __inc: n }) };
  const sheetLabel = (d, name) => (d ? `RG Sheet ${d.sheetIndex}` : String(name || '')), setLabel = id => 'Set ' + (/-(\d+)$/.exec(id) || [])[1];
  const rem = Remnants({ db, col: query, FV, sheetLabel, setLabel, revDoc: () => ref('Charm_Nest_Rev/remnants'), configRef: () => ref('config/charmNestPartials'), statsRef: () => ref('Charm_Nest_Rev/partialStats'), statsWrite: () => true });
  const api = RoseStock({ db, col: query, FV, Readiness, sheetLabel, recordRemnant: rem.recordRemnant, remnantSync: rem.sync, stamp: async () => {} });
  rem.bind(api);
  const O = rem.ops, counter = () => store.get('Charm_Nest_Rev/remnants').n;
  store.set('Charm_Nest_Rev/remnants', { n: 1, backfilledAt: 1, reconciledAt: 1 });
  store.set('Charm_Nest_Runs/run-test', { lines: { a: { poolIds: ['p1'], spec: { engraveCandidate: false } } } });
  const ids = r => r.items.map(i => i.id);

  // 1. make: a 120 x 60 and an 80 x 40 rose sheet and an 80 x 40 10K sheet; each is 1 stock + 1 record + 1 counter write, in one transaction
  const n0 = counter(), w0 = writes;
  const A = await O.sheetMake({ metal: 'rose', wMm: 120, hMm: 60, by: 'Pat Lee' });
  assert.equal(writes - w0, 3, 'a made sheet is 3 writes: the stock, its record, the counter'); assert.equal(counter(), n0 + 1, 'the counter moved once');
  await pause(4); const B = await O.sheetMake({ metal: 'rose', wMm: 80, hMm: 40, by: 'Pat Lee' }); await pause(4); const T = await O.sheetMake({ metal: 'gold10k', wMm: '80', hMm: 40.04, by: '' });
  const stockA = A.item.stockId, stockB = B.item.stockId;
  assert.deepEqual([A.ok, A.item.id, A.item.kind, A.item.status, A.item.revision, A.item.madeBy, A.item.sourceSheet, A.item.sheetWMm, A.item.sheetHMm, A.item.wMm, A.item.hMm, A.item.areaMm2, A.item.code], [true, `${stockA}-0`, 'new', 'available', 0, 'Pat Lee', 'New sheet 120 x 60 mm', 120, 60, 120, 60, 7200, 'RG']);
  assert.deepEqual(A.item.outline, [[[0, 0], [120, 0], [120, 60], [0, 60]]], 'the outline is the full rectangle'); assert(A.item.madeAt > 0 && A.item.estimate && A.item.estimate.pieces >= 0, 'made when, and the estimate through estimateFit');
  const sA = store.get(`Charm_Nest_Rose_Stock/${stockA}`), rA = store.get(`Charm_Nest_Remnants/${stockA}-0`);
  assert.deepEqual([sA.revision, sA.owner, sA.available, sA.profileJson, sA.metal, sA.made, sA.wPt, typeof rA.ringsJson], [0, null, true, null, 'rose', true, 120 / MM, 'string'], 'a physical sheet as roseClaim makes a fresh one, and the outline is one string');
  assert.equal(T.item.sheetWMm, 80); assert.equal(T.item.sheetHMm, 40); assert.equal(T.item.madeBy, '', 'no one signed in stays none'); assert.equal(T.item.code, '10K');
  for (const bad of [{ wMm: 4.9, hMm: 40 }, { wMm: 40, hMm: 500.5 }, { wMm: 'x', hMm: 40 }, { wMm: '', hMm: 40 }, { wMm: 40 }, { metal: 'gold', wMm: 40, hMm: 40 }]) await assert.rejects(() => O.sheetMake({ metal: 'rose', ...bad }), /5 to 500 mm|Choose Rose Gold/);
  assert.equal([...store.keys()].filter(k => k.startsWith('Charm_Nest_Rose_Stock/')).length, 3, 'a refused make wrote nothing');

  // 2. they list as new cards, newest first; the policy and the plan see them like any partial
  const list = await O.partialList({ metal: 'rose' });
  assert.deepEqual(ids(list), [`${stockB}-0`, `${stockA}-0`]); assert(list.items.every(c => c.kind === 'new' && c.status === 'available' && c.estimate && c.outline.length === 1 && c.madeAt > 0));
  assert.deepEqual(ids(await O.partialList({ metal: 'gold10k' })), [`${T.item.stockId}-0`]);
  const plan = await O.partialPlan({ metal: 'rose', pieces: [{ areaMm2: 300 }] }); assert.equal(plan.available, 2);
  const stocksOf = await O.partialStocks({ ids: [`${stockA}-0`] }); assert.deepEqual([stocksOf.stocks[`${stockA}-0`].revision, stocksOf.stocks[`${stockA}-0`].profileJson, stocksOf.stocks[`${stockA}-0`].held, stocksOf.stocks[`${stockA}-0`].current], [0, null, false, true], 'the nester gets the stock with an empty profile');

  // 3. claim one through the real partial claim: it is held by that sheet and no longer offered to another
  const c = await O.partialClaim({ metal: 'rose', id: `${stockA}-0`, sheetId: 'sheet-1', sheetName: 'RG Sheet 1', by: 'Pat Lee' });
  assert.deepEqual([c.stock.id, c.stock.revision, c.stock.profileJson, c.stock.owner, c.partial.status], [stockA, 0, null, 'sheet-1', 'inUse']);
  assert.deepEqual(ids(await O.partialList({ metal: 'rose' })), [`${stockB}-0`], 'a held one is not on the available list'); assert.equal((await O.partialList({ metal: 'rose', inUse: true })).items.find(i => i.id === `${stockA}-0`).inUseBySheetName, 'RG Sheet 1');
  await assert.rejects(() => O.partialClaim({ metal: 'rose', id: `${stockA}-0`, sheetId: 'sheet-2', sheetName: 'RG Sheet 2' }), /reserved for another layout|in use by RG Sheet 1/);
  await assert.rejects(() => api.roseClaim({ sheetId: 'sheet-2', metal: 'rose', stockId: stockA, wPt: 120 / MM, hPt: 60 / MM }), /reserved for another layout/, 'one source of truth: the stock owner');
  assert.equal((await O.partialClaim({ metal: 'rose', id: `${stockA}-0`, sheetId: 'sheet-1', sheetName: 'RG Sheet 1' })).stock.owner, 'sheet-1', 'the holder asking again keeps it');
  // give it back (the record is available again, the stock stays), take it again
  const rel = await O.partialRelease({ sheetId: 'sheet-1' }); assert.deepEqual([rel.released, rel.partialId], [true, `${stockA}-0`]);
  assert.deepEqual([store.get(`Charm_Nest_Remnants/${stockA}-0`).status, store.get(`Charm_Nest_Rose_Stock/${stockA}`).available, store.get(`Charm_Nest_Rose_Stock/${stockA}`).owner], ['available', true, null]);
  await O.partialClaim({ metal: 'rose', id: `${stockA}-0`, sheetId: 'sheet-1', sheetName: 'RG Sheet 1', by: 'Pat Lee' });
  // a 10K made sheet given back is NOT thrown away like an uncut gold sheet is: it is a repository sheet
  await O.partialClaim({ metal: 'gold10k', id: `${T.item.stockId}-0`, sheetId: 'sheet-7', sheetName: '10K Sheet 7' }); await api.roseRelease({ stockId: T.item.stockId, sheetId: 'sheet-7' });
  assert.deepEqual([store.has(`Charm_Nest_Rose_Stock/${T.item.stockId}`), store.get(`Charm_Nest_Remnants/${T.item.stockId}-0`).status, store.get(`Charm_Nest_Rose_Stock/${T.item.stockId}`).available], [true, 'available', true], 'the 10K made sheet stays and is available again');

  // 4. delete: an available one, with a reason; refused for a held one and for a missing, short or long reason
  await assert.rejects(() => O.sheetDelete({ id: `${stockA}-0`, reason: 'Made it twice', by: 'Pat Lee' }), /holds this sheet.*RG Sheet 1/, 'a held sheet cannot be deleted');
  for (const reason of [undefined, '', '  ', ' ab ', 'x'.repeat(301)]) await assert.rejects(() => O.sheetDelete({ id: `${stockB}-0`, reason, by: 'Pat Lee' }), /Say why you are deleting this sheet \(3 to 300 characters\)/);
  await assert.rejects(() => O.sheetDelete({ id: `${stockB}-9`, reason: 'No such sheet' }), /Sheet not found/); await assert.rejects(() => O.sheetDelete({ reason: 'No id' }), /Choose a sheet/);
  assert.equal(store.get(`Charm_Nest_Remnants/${stockB}-0`).status, 'available', 'every refusal left it alone');
  const nd = counter(), wd = writes; await pause(4);
  const D = await O.sheetDelete({ id: `${stockB}-0`, reason: '  Made by accident  ', by: 'Pat Lee' });
  assert.equal(writes - wd, 3, 'a delete is 3 writes: the record, the stock, the counter'); assert.equal(counter(), nd + 1);
  assert.deepEqual([D.ok, D.item.status, D.item.deletedBy, D.item.deletedReason, D.item.kind, D.item.estimate], [true, 'deleted', 'Pat Lee', 'Made by accident', 'new', null]); assert(D.item.deletedAt >= D.item.madeAt);
  assert.deepEqual(ids(await O.partialList({ metal: 'rose' })), [], 'a deleted sheet is not offered by partialList');
  assert.deepEqual(ids(await O.partialList({ metal: 'rose', inUse: true, used: true })), [`${stockA}-0`], 'nor by the inUse / used lists');
  assert.equal((await O.partialPlan({ metal: 'rose', pieces: [{ areaMm2: 300 }] })).available, 0, 'nor by the plan');
  const sB = store.get(`Charm_Nest_Rose_Stock/${stockB}`); assert.deepEqual([sB.available, sB.deleted, sB.owner], [false, true, null], 'the nester never offers it');
  assert.deepEqual(ids(await api.roseList({ metal: 'rose' }).then(r => ({ items: r.stocks }))), [], 'the stock list of the nester has no free rose sheet');
  await assert.rejects(() => O.partialClaim({ metal: 'rose', id: `${stockB}-0`, sheetId: 'sheet-3' }), /was deleted/); await assert.rejects(() => api.roseClaim({ sheetId: 'sheet-3', metal: 'rose', stockId: stockB, wPt: 80 / MM, hPt: 40 / MM }), /was deleted/);
  await assert.rejects(() => O.remnantMark({ id: `${stockB}-0`, status: 'available' }), /deleted/); await assert.rejects(() => O.partialUse({ id: `${stockB}-0`, sheetId: 'sheet-3' }), /was deleted/);
  await assert.rejects(() => O.sheetDelete({ id: `${stockB}-0`, reason: 'Again' }), /already deleted/);
  // ...but it is in the search list (all statuses) with the three fields, and its history says made and deleted
  const all = await O.partialSearchList({}), dB = all.items.find(i => i.id === `${stockB}-0`);
  assert.deepEqual([all.items.length, dB.status, dB.deletedAt, dB.deletedBy, dB.deletedReason, dB.kind, dB.madeBy, dB.sourceSheet], [3, 'deleted', D.item.deletedAt, 'Pat Lee', 'Made by accident', 'new', 'Pat Lee', 'New sheet 80 x 40 mm']);
  const hB = await O.sheetHistory({ stockId: stockB });
  assert.deepEqual([hB.cuts, hB.made.by, hB.made.at, hB.deleted, hB.stock.kind, hB.stock.revision, hB.stock.wMm, hB.stock.hMm], [[], 'Pat Lee', B.item.madeAt, { at: D.item.deletedAt, by: 'Pat Lee', reason: 'Made by accident' }, 'new', 0, 80, 40]);

  // 5. a cut on the claimed new sheet creates revision 1; the revision-0 record becomes used exactly as a cut turns a leftover to used
  const doc = { id: 'sheet-1', metal: 'rose', sheetIndex: 1, setId: 'set-2026-1', fileBase: 'RG_2026-10-07_Set-1_Sheet-1', verification: { ok: true }, status: 'complete', dirty: false, saving: false, draft: false, runId: 'run-test', poolIds: ['p1'], placedCount: 1,
    placements: [{ id: 'p1', cxPt: 20, cyPt: 20, angle: 0, scale: 1 }], charms: [{ id: 'p1', areaPt2: 600, widthPt: 28, heightPt: 34 }], outputs: { ai: { url: 'saved.ai' }, preview: { url: 'saved.png' } }, label: { files: [{ path: 'qr', url: 'qr.png', payload: 'test', orders: [] }] }, orders: [] };
  store.set('Charm_Nest_Sheets/sheet-1', doc); const shapes = [shape('p1', 2, 2, 10, 30)];
  const plan1 = await api.rosePlan({ sheetId: 'sheet-1', stockId: stockA, revision: 0, fingerprint: RoseStock.fingerprint(doc), shapesJson: JSON.stringify(shapes), allowanceMm: .2, cut: true });
  const cut = await api.roseRecordCut({ sheetId: 'sheet-1', stockId: stockA, revision: 0, planHash: plan1.planHash, by: 'Pat Lee', via: 'nest', device: 'charm-nest-1' });
  assert.equal(cut.stock.revision, 1);
  const r0 = store.get(`Charm_Nest_Remnants/${stockA}-0`), r1 = store.get(`Charm_Nest_Remnants/${stockA}-1`);
  assert.deepEqual([r0.status, r0.usedBySheetId, r0.usedBySheetName, r0.usedBy, r0.inUseBySheetId, r0.kind, r0.madeBy], ['used', 'sheet-1', 'RG Sheet 1', 'Pat Lee', null, 'new', 'Pat Lee'], 'the revision-0 record is used, still a made sheet');
  assert.deepEqual([r1.status, r1.revision, r1.kind, r1.sheetName, r1.by, r1.stockId], ['available', 1, undefined, 'RG Sheet 1', 'Pat Lee', stockA], 'the leftover is revision 1, a cut leftover (no kind)');
  const hA = await O.sheetHistory({ stockId: stockA });
  assert.deepEqual([hA.cuts.length, hA.cuts[0].n, hA.cuts[0].revision, hA.cuts[0].by, hA.made.by, hA.deleted, hA.stock.revision, hA.stock.kind, hA.rev], [1, 1, 1, 'Pat Lee', 'Pat Lee', null, 1, 'new', '1'], 'one cut, and who made the sheet');
  assert(hA.cuts[0].rings.length && hA.cuts[0].exact === true && !hA.cuts[0].derived);
  await assert.rejects(() => O.sheetDelete({ id: `${stockA}-0`, reason: 'Too late' }), /was used/, 'a used sheet cannot be deleted');
  const after = await O.partialSearchList({}), mine = after.items.filter(i => i.stockId === stockA);
  assert.deepEqual(mine.map(i => [i.revision, i.status, i.kind || null]).sort(), [[0, 'used', 'new'], [1, 'available', null]], 'the list holds the made sheet and its leftover; the grouping is by stockId');
  assert.equal(ids(await O.partialList({ metal: 'rose' })).join(), `${stockA}-1`, 'the leftover is offered, the used made sheet is not');
  // the leftover is cut again on revision 1 (the old path is untouched) and the deleted sheet never came back
  assert.equal(store.get(`Charm_Nest_Rose_Stock/${stockB}`).available, false);
  assert.equal(removed, 0, 'no document was ever removed'); for (const k of [`Charm_Nest_Rose_Stock/${stockA}`, `Charm_Nest_Rose_Stock/${stockB}`, `Charm_Nest_Remnants/${stockB}-0`, `Charm_Nest_Remnants/${stockA}-0`]) assert(store.has(k), k + ' still exists');

  // 6. the page layer: make / remove send the signed-in person, mark the lists changed through on(), drop the cached history, no timer, no call for a refused answer
  const sent = [];
  global.setInterval = () => { throw new Error('nothing polls'); };
  global.window = { CharmNestPartial: P, CNEmployee: { name: () => 'Pat Lee' }, CN: { api: async (fn, body) => { sent.push(body); return JSON.parse(JSON.stringify(await O[body.op](body))); } } };
  require('../../charm-nest-partial-data.js');
  const PS = global.window.PartialSheets, heard = []; PS.on(i => heard.push(i.reason));
  const m = await PS.make({ metal: 'gold14k', wMm: 200, hMm: 100 });
  assert.deepEqual([sent[0].op, sent[0].by, sent[0].metal, sent[0].wMm, m.item.kind, m.item.madeBy, m.item.code], ['sheetMake', 'Pat Lee', 'gold14k', 200, 'new', 'Pat Lee', '14K']); assert(heard.includes('changed'), 'the lists are marked changed');
  await assert.rejects(() => PS.make({ metal: 'rose', wMm: 3, hMm: 50 }), /5 to 500 mm/); await assert.rejects(() => PS.make({ metal: 'silver', wMm: 30, hMm: 50 }), /Choose Rose Gold/);
  const hm = await PS.history({ stockId: m.item.stockId, revision: 0 }); assert.deepEqual([hm.made.by, hm.deleted], ['Pat Lee', null]); sent.length = 0;
  await assert.rejects(() => PS.remove(m.item.id, 'ab'), /3 to 300/); assert.equal(sent.length, 0, 'a short reason is refused before any call');
  heard.length = 0; const g = await PS.remove(m.item.id, '  Wrong metal  ');
  assert.deepEqual([sent[0].op, sent[0].by, sent[0].reason, g.item.status, g.item.deletedReason, g.item.deletedBy], ['sheetDelete', 'Pat Lee', 'Wrong metal', 'deleted', 'Wrong metal', 'Pat Lee']); assert(heard.includes('changed'));
  sent.length = 0; const hd = await PS.history({ stockId: m.item.stockId, revision: 0 });
  assert.deepEqual([sent.map(b => b.op), hd.deleted.reason, hd.made.by], [['sheetHistory'], 'Wrong metal', 'Pat Lee'], 'the history cached before the delete is read again: it says deleted');
  await assert.rejects(() => PS.remove(m.item.id, 'Twice over'), /already deleted/);
  assert.equal(removed, 0, 'the page layer removed nothing either');
  console.log('options-sheets-data: ok (sheetMake 1 stock + 1 record + 1 counter write, kind new cards, claim holds one sheet only, soft delete with reason in search and history, cut on a new sheet makes revision 1 and uses revision 0, nothing removed)');
})().catch(e => { console.error(e); process.exit(1); });
