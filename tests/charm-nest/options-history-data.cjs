// Options Studio, the data side: sheetHistory (every cut of one physical sheet, oldest first, nested rings, an older cut with no saved leftover derived from its plan),
// partialSearchList (every partial, every status and metal, an `unchanged` answer on the revision), and the page layer's history() / searchAll() (one call, cached).
// Offline: a fake Firestore (field masks honoured, nested arrays refused on every write, no write at all after setup). No real data, no network.
//   node tests/charm-nest/options-history-data.cjs
'use strict';
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const assert = require('node:assert/strict');
const Rose = require('../../charm-nest-rose'), Readiness = require('../../charm-nest-readiness'), P = require('../../charm-nest-partial');
const Remnants = require('../../netlify/functions/_charmNestRemnants'), RoseStock = require('../../netlify/functions/_charmNestRoseStock');
const MM = Rose.MM, pause = ms => new Promise(r => setTimeout(r, ms));
const shape = (id, x, y, w, h) => ({ id, paths: [[[x, y], [x + w, y], [x + w, y + h], [x, y + h]]] });

(async () => {
  const store = new Map(), clone = x => structuredClone(x), reads = []; let frozen = false, writes = 0;
  const mask = (d, f) => (f ? Object.fromEntries(Object.entries(d).filter(([k]) => f.includes(k))) : d);
  const ref = path => ({ path, id: path.split('/').at(-1), collection: n => query(path + '/' + n), get: async () => { reads.push({ path, fields: null }); return snap(path); }, set: async (v, o) => put({ path }, v, o && o.merge) });
  const snap = (path, f) => ({ id: path.split('/').at(-1), ref: ref(path), exists: store.has(path), data: () => mask(clone(store.get(path)), f) });
  const query = (path, filters = [], order = null, limit = Infinity, f = null, after = null) => ({
    doc: id => ref(path + '/' + id), where: (...x) => query(path, [...filters, x], order, limit, f, after), orderBy: (...o) => query(path, filters, o, limit, f, after), limit: n => query(path, filters, order, n, f, after),
    select: (...x) => query(path, filters, order, limit, x, after), startAfter: v => query(path, filters, order, limit, f, v),
    get: async () => {
      let docs = [...store.keys()].filter(k => k.startsWith(path + '/') && !k.slice(path.length + 1).includes('/')).map(k => snap(k));
      docs = docs.filter(d => filters.every(([k, , v]) => d.data()[k] === v));
      if (order) { docs = docs.filter(d => d.data()[order[0]] !== undefined); docs.sort((a, b) => (a.data()[order[0]] - b.data()[order[0]]) * (order[1] === 'desc' ? -1 : 1)); if (after != null) docs = docs.filter(d => (order[1] === 'desc' ? d.data()[order[0]] < after : d.data()[order[0]] > after)); }
      docs = docs.slice(0, limit).map(d => snap(d.ref.path, f)); docs.forEach(d => reads.push({ path: d.ref.path, fields: f })); return { docs, size: docs.length };
    } });
  const put = (r, v, merge) => { assert(!frozen, 'nothing may be written here: ' + r.path); writes++; refuseNestedArrays(v, r.path); const old = merge ? store.get(r.path) || {} : {}; const out = { ...old }; for (const [k, x] of Object.entries(v)) out[k] = x && x.__inc ? (+old[k] || 0) + x.__inc : clone(x); store.set(r.path, out); };
  let serial = Promise.resolve();
  const db = { runTransaction: fn => { const p = serial.then(async () => { const w = []; let wrote = false; const r = await fn({ get: async x => { assert(!wrote, 'Firestore requires all reads before writes'); return x.get(); }, set: (x, v, o) => { wrote = true; w.push(() => put(x, v, o && o.merge)); }, update: (x, v) => { wrote = true; w.push(() => put(x, v, true)); }, delete: x => { wrote = true; w.push(() => store.delete(x.path)); } }); w.forEach(f => f()); return r; }); serial = p.catch(() => {}); return p; } };
  const FV = { serverTimestamp: () => 123456, increment: n => ({ __inc: n }) };
  const sheetLabel = (d, name) => (d ? `RG Sheet ${d.sheetIndex}` : (/_Sheet-(\d+)/.exec(String(name || '')) ? `RG Sheet ${/_Sheet-(\d+)/.exec(name)[1]}` : String(name || ''))), setLabel = id => 'Set ' + (/-(\d+)$/.exec(id) || [])[1];
  const rem = Remnants({ db, col: query, FV, sheetLabel, setLabel, revDoc: () => ref('Charm_Nest_Rev/remnants'), configRef: () => ref('config/charmNestPartials'), statsRef: () => ref('Charm_Nest_Rev/partialStats'), statsWrite: () => true });
  const api = RoseStock({ db, col: query, FV, Readiness, sheetLabel, recordRemnant: rem.recordRemnant, remnantSync: rem.sync, stamp: async () => {} });
  rem.bind(api);
  const O = rem.ops;
  store.set('Charm_Nest_Rev/remnants', { n: 1, backfilledAt: 1, reconciledAt: 1 });
  store.set('Charm_Nest_Runs/run-test', { lines: { a: { poolIds: ['p1', 'p2', 'p3'], spec: { engraveCandidate: false } } } });
  const sheetDoc = (id, shapes, idx) => ({ id, metal: 'rose', sheetIndex: idx, setId: 'set-2026-1', fileBase: `RG_2026-10-05_Set-1_Sheet-${idx}`, verification: { ok: true }, status: 'complete', dirty: false, saving: false, draft: false, runId: 'run-test', poolIds: shapes.map(s => s.id), placedCount: shapes.length,
    placements: shapes.map(s => ({ id: s.id, cxPt: 20, cyPt: 20, angle: 0, scale: 1 })), charms: shapes.map(s => ({ id: s.id, areaPt2: 600, widthPt: 28, heightPt: 34 })), outputs: { ai: { url: 'saved.ai' }, preview: { url: 'saved.png' } }, label: { files: [{ path: 'qr', url: 'qr.png', payload: 'test', orders: [] }] }, orders: [] });
  const cutOne = async (sheetId, shapes, idx, stockArgs, by, via) => {
    await pause(4);   // (distinct cut times)
    const claim = await api.roseClaim({ sheetId, wPt: 100 / MM, hPt: 50 / MM, ...stockArgs }), stockId = claim.stock.id, doc = sheetDoc(sheetId, shapes, idx);
    store.set('Charm_Nest_Sheets/' + sheetId, doc);
    const p = await api.rosePlan({ sheetId, stockId, revision: claim.stock.revision, fingerprint: RoseStock.fingerprint(doc), shapesJson: JSON.stringify(shapes), allowanceMm: .2, cut: true });
    return api.roseRecordCut({ sheetId, stockId, revision: claim.stock.revision, planHash: p.planHash, by, via, device: 'charm-nest-1' });
  };
  const one = await cutOne('sheet-1', [shape('p1', 2, 2, 10, 30)], 1, {}, 'Pat Lee', 'nest');
  const stockId = one.stock.id;
  await cutOne('sheet-2', [shape('p2', 25, 2, 8, 25)], 2, { stockId, revision: 1 }, '', 'library');
  const three = await cutOne('sheet-3', [shape('p3', 45, 2, 8, 20)], 3, { stockId, revision: 2 }, 'Eve', 'nest');
  assert.equal(three.stock.revision, 3, 'three cuts on one physical sheet');
  // the oldest cut has no saved leftover (it was made before leftovers were saved): its cuts document and plan are all there is
  const saved1 = store.get(`Charm_Nest_Remnants/${stockId}-1`); store.delete(`Charm_Nest_Remnants/${stockId}-1`);
  // a never-cut physical sheet a sheet holds, and two leftovers of the other metals (discarded 10K, available 14K)
  store.set('Charm_Nest_Rose_Stock/rgs-fresh-0', { id: 'rgs-fresh-0', metal: 'gold10k', wPt: 100 / MM, hPt: 50 / MM, revision: 0, owner: 'sheet-9', available: false });
  store.set('Charm_Nest_Sheets/sheet-9', { id: 'sheet-9', metal: 'gold10k', sheetIndex: 9, roseStockId: 'rgs-fresh-0' });
  const unit = (id, metal, code, status, cutAt, extra) => store.set('Charm_Nest_Remnants/' + id, { v: 1, metal, code, sheetId: 's-' + id, sheetName: code + ' Sheet 5', setId: 'set-2026-2', setName: 'Set 2', stockId: id.replace(/-\d+$/, ''), revision: 1, via: 'nest', cutAt, by: 'Ana', sheetWMm: 100, sheetHMm: 50,
    ringsJson: JSON.stringify([[[0, 0], [60, 0], [60, 50], [0, 50]]]), areaMm2: 3000, bboxMm: { x: 0, y: 0, w: 60, h: 50 }, status, statusAt: cutAt, lastUsedAt: cutAt, lastUsedBy: 'Ana', lastUsedSheet: code + ' Sheet 5', ...extra });
  unit('rgs-ten-1', 'gold10k', '10K', 'discarded', 1000, { auto: true, reason: 'Too small to reuse' });
  unit('rgs-fourteen-1', 'gold14k', '14K', 'available', 2000);
  // held by sheet-4 now: the leftover of the third cut
  await O.partialClaim({ metal: 'rose', id: `${stockId}-3`, sheetId: 'sheet-4', sheetName: 'RG Sheet 4', by: 'Ana' });
  frozen = true; const w0 = writes;

  // 1. sheetHistory: three cuts, oldest first, nested rings; the oldest derived from its plan, the others from their saved leftovers
  reads.length = 0;
  const h = await O.sheetHistory({ stockId });
  assert.deepEqual([h.ok, h.rev, h.stock], [true, '3', { id: stockId, metal: 'rose', code: 'RG', wMm: 100, hMm: 50, revision: 3, ownerSheetId: 'sheet-4', ownerSheetName: 'RG Sheet 4' }]);
  assert.deepEqual(h.cuts.map(c => c.n), [1, 2, 3]); assert.deepEqual(h.cuts.map(c => c.revision), [1, 2, 3]);
  assert(h.cuts[0].at < h.cuts[1].at && h.cuts[1].at < h.cuts[2].at, 'oldest first');
  assert.deepEqual(h.cuts.map(c => c.by), ['Pat Lee', '', 'Eve']); assert.deepEqual(h.cuts.map(c => c.exact), [true, false, true], 'a cut nobody signed in for is not exact');
  assert.deepEqual(h.cuts.map(c => c.via), ['', 'library', 'nest']); assert.deepEqual(h.cuts.map(c => c.sheetId), ['sheet-1', 'sheet-2', 'sheet-3']);
  assert.deepEqual(h.cuts.map(c => c.sheetName), ['RG Sheet 1', 'RG Sheet 2', 'RG Sheet 3']); assert.deepEqual(h.cuts.map(c => c.setName), ['Set 1', 'Set 1', 'Set 1']);
  assert(h.cuts.every(c => Array.isArray(c.rings) && c.rings.length && Array.isArray(c.rings[0]) && Array.isArray(c.rings[0][0])), 'rings nest freely in the answer (an array of rings of points)');
  assert.equal(h.cuts[0].derived, true); assert.equal(h.cuts[1].derived, undefined);
  assert.deepEqual(h.cuts[0].rings, JSON.parse(saved1.ringsJson), 'derived from the plan: the very rings the leftover record would have held');
  assert.deepEqual([h.cuts[0].areaMm2, h.cuts[0].bboxMm], [saved1.areaMm2, saved1.bboxMm]);
  assert(h.cuts[0].areaMm2 > h.cuts[1].areaMm2 && h.cuts[1].areaMm2 > h.cuts[2].areaMm2 && h.cuts[2].areaMm2 > 0, 'every cut leaves less');
  const cutReads = reads.filter(r => r.path.includes('/cuts/')), stockReads = reads.filter(r => /^Charm_Nest_Rose_Stock\/[^/]+$/.test(r.path));
  assert.deepEqual([cutReads.length, stockReads.length, reads.filter(r => r.path.startsWith('Charm_Nest_Remnants/')).length], [1, 1, 2], 'one stock, two saved leftovers, ONE cut document (the one with no leftover)');
  assert(cutReads[0].path.endsWith('/cuts/sheet-1') && cutReads[0].fields.includes('planJson'), 'only that cut is read with its plan');
  assert(reads.filter(r => r.path.startsWith('Charm_Nest_Remnants/')).every(r => r.fields && !r.fields.includes('planJson')), 'the saved leftovers are read with a field mask');
  // by sheet: the sheet's own roseStockId (one read), else the leftover its cut made
  assert.equal((await O.sheetHistory({ sheetId: 'sheet-3' })).stock.id, stockId, 'a sheet that was cut: the leftover its cut made');
  store.set('Charm_Nest_Sheets/sheet-1', { ...store.get('Charm_Nest_Sheets/sheet-1'), roseStockId: stockId }); // (frozen test data: a plain Map write)
  assert.equal((await O.sheetHistory({ sheetId: 'sheet-1' })).cuts.length, 3, 'a sheet that holds a physical sheet: that sheet');
  await assert.rejects(() => O.sheetHistory({ sheetId: 'sheet-nobody' }), /not on a physical sheet/);
  await assert.rejects(() => O.sheetHistory({ stockId: 'rgs-nothing-3' }), /Sheet not found/); await assert.rejects(() => O.sheetHistory({ stockId: '!!' }), /Choose a sheet/); await assert.rejects(() => O.sheetHistory({}), /Choose a sheet/);
  const fresh = await O.sheetHistory({ stockId: 'rgs-fresh-0' });
  assert.deepEqual([fresh.cuts, fresh.stock.code, fresh.stock.revision, fresh.stock.ownerSheetId, fresh.stock.ownerSheetName, fresh.rev], [[], '10K', 0, 'sheet-9', 'RG Sheet 9', '0'], 'a never-cut sheet: no cuts, and who holds it');

  // 2. partialSearchList: every status, every metal, newest cut first, the revision probe
  const all = await O.partialSearchList({});
  assert.deepEqual([all.items.length, all.more, all.needsBackfill], [4, false, undefined], 'the two leftovers still saved + the 10K + the 14K (cut 1 has none saved)');
  assert.deepEqual(new Set(all.items.map(i => i.metal)), new Set(['rose', 'gold10k', 'gold14k'])); assert.deepEqual(new Set(all.items.map(i => i.status)), new Set(['inUse', 'used', 'discarded', 'available']));
  assert(all.items.every((c, i) => !i || (all.items[i - 1].cutAt || 0) >= (c.cutAt || 0)), 'newest cut first');
  const byId = Object.fromEntries(all.items.map(i => [i.id, i])), c3 = byId[`${stockId}-3`], c2 = byId[`${stockId}-2`];
  assert.deepEqual([c3.status, c3.inUseBySheetName, c2.status, c2.usedBySheetName, byId['rgs-ten-1'].status, byId['rgs-fourteen-1'].status], ['inUse', 'RG Sheet 4', 'used', 'RG Sheet 3', 'discarded', 'available']);
  assert(c3.estimate && c3.estimate.pieces >= 0 && byId['rgs-fourteen-1'].estimate, 'a held or available one carries the estimate');
  assert.deepEqual([c2.estimate, byId['rgs-ten-1'].estimate], [null, null], 'a used or discarded one does not');
  assert(c3.outline.length && c3.sheetWMm === 100 && c3.bboxMm.w > 0 && c3.stockId === stockId && c3.revision === 3 && c3.sourceSheet === 'RG Sheet 3' && c3.cutBy === 'Eve' && c3.code === 'RG', 'the partialList card');
  assert.deepEqual(await O.partialSearchList({ ifRev: all.rev }), { unchanged: true, rev: all.rev }, 'nothing moved: one tiny read');
  assert.equal((await O.partialSearchList({ ifRev: all.rev, verify: true })).items.length, 4, 'verify reads in full');
  const p1 = await O.partialSearchList({ limit: 2 }), p2 = await O.partialSearchList({ limit: 2, before: p1.items[1].cutAt });
  assert.deepEqual([p1.items.length, p1.more, p2.items.length, p2.more], [2, true, 2, false]); assert.deepEqual([...p1.items, ...p2.items].map(i => i.id), all.items.map(i => i.id), 'older ones with before');
  assert.equal((await O.partialSearchList({ limit: 'x' })).items.length, 4); assert.equal(writes, w0, 'nothing was written by either op');

  // 3. the page layer: one call per sheet (cached by stockId + revision), ONE list for the search (revision probe, no timer)
  const sent = [];
  global.setInterval = () => { throw new Error('nothing polls'); };
  global.window = { CharmNestPartial: P, CNEmployee: { name: () => 'Pat Lee' }, CN: { api: async (fn, body) => { sent.push(body); return JSON.parse(JSON.stringify(await O[body.op](body))); } } };
  require('../../charm-nest-partial-data.js');
  const PS = global.window.PartialSheets, ops = () => sent.splice(0).map(b => b.op + (b.ifRev ? '+ifRev' : '') + (b.verify ? '+verify' : '') + (b.before ? '+before' : '') + (b.sheetId ? '+sheetId' : ''));
  const a = await PS.history({ stockId }); assert.equal(a.cuts.length, 3); assert.deepEqual(ops(), ['sheetHistory']);
  assert.equal(await PS.history({ stockId }), a); assert.equal(await PS.history({ stockId, revision: 3 }), a); assert.deepEqual(ops(), [], 'as new as asked: no call');
  await PS.history({ stockId, revision: 4 }); assert.deepEqual(ops(), ['sheetHistory'], 'a revision newer than the cache: one call');
  PS.changed(); await PS.history({ stockId, revision: 3 }); assert.deepEqual(ops(), [], 'a revision never changes: still no call after a change here'); await PS.history({ stockId }); assert.deepEqual(ops(), ['sheetHistory'], 'asked without a revision after a change: one call');
  const bySheet = await PS.history({ sheetId: 'sheet-9' }); assert.deepEqual([bySheet.stock.id, bySheet.cuts.length], ['rgs-fresh-0', 0]); assert.deepEqual(ops(), ['sheetHistory+sheetId']); await PS.history({ sheetId: 'sheet-9' }); assert.deepEqual(ops(), [], 'the sheet is known now');
  const [x, y] = await Promise.all([PS.history({ stockId: 'rgs-ten-9' }).catch(e => e.message), PS.history({ stockId: 'rgs-ten-9' }).catch(e => e.message)]); assert.deepEqual([x, y], ['Sheet not found', 'Sheet not found']); assert.deepEqual(ops(), ['sheetHistory'], 'two asks at once share one call');
  await assert.rejects(() => PS.history({}), /Choose a sheet/);
  const s1 = await PS.searchAll(); assert.equal(s1.items.length, 4); assert.deepEqual(ops(), ['partialSearchList']);
  assert.equal(await PS.searchAll(), s1); assert.deepEqual(ops(), [], 'asked again within 20 s: no call at all');
  PS.changed(); assert.equal(await PS.searchAll(), s1); assert.deepEqual(ops(), ['partialSearchList+ifRev'], 'after a change: one call that sends the revision, nothing moved');
  await PS.searchAll({ force: true }); assert.deepEqual(ops(), ['partialSearchList+verify'], 'Refresh reads in full');
  const part = await PS.searchAll({ force: true, limit: 2 }); assert.deepEqual([part.items.length, part.more], [2, true]); ops();
  const more = await PS.searchAll({ more: true }); assert.deepEqual(ops(), ['partialSearchList+before']); assert.deepEqual([more.items.map(i => i.id), more.more], [s1.items.map(i => i.id), false], 'the older ones are added, none twice');
  assert.equal(await PS.searchAll({ more: true }), more); assert.deepEqual(ops(), [], 'nothing older: no call'); assert.equal(writes, w0, 'the page layer wrote nothing either');
  console.log('options-history-data: ok (sheetHistory 3 cuts oldest first with nested rings, older cut derived from its plan, partialSearchList all statuses and metals, unchanged on ifRev, page layer one call per sheet, no writes)');
})().catch(e => { console.error(e); process.exit(1); });
