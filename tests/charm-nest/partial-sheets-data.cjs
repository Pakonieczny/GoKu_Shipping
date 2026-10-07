// PS3: partial sheets, the data layer: the estimate of how many regular pieces fit, the partial's status model (available / inUse / used / discarded) kept in step
// with the Rose stock's owner in the stock's own transactions, claim / release / use, the list (one cheap read, revision probe), the per-metal setting, the plan.
// Offline: a fake Firestore that refuses a read after a write inside a transaction. No real data, no network.
//   node tests/charm-nest/partial-sheets-data.cjs
'use strict';
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const assert = require('node:assert/strict');
const Rose = require('../../charm-nest-rose'), Readiness = require('../../charm-nest-readiness'), P = require('../../charm-nest-partial');
const Remnants = require('../../netlify/functions/_charmNestRemnants'), RoseStock = require('../../netlify/functions/_charmNestRoseStock');
const MM = Rose.MM;
const rect = (x0, y0, x1, y1) => [[x0, y0], [x1, y0], [x1, y1], [x0, y1]];
const shape = (id, x, y, w, h) => ({ id, paths: [rect(x, y, x + w, y + h)] });
const T = { areaMm2: 110, minMm: 9, maxMm: 14 }, SHEET = { sheetWMm: 100, sheetHMm: 50 };

(async () => {
  // 1. estimateFit: a rectangle, an L shape, thin strips; always low <= pieces <= high
  const block = P.estimateFit([rect(0, 0, 50, 40)], T, SHEET);   // sheet border on two sides, green cut edge on the other two
  assert.deepEqual([block.pieces, block.low, block.high, block.packedPct], [11, 9, 13, 65], 'the header example');
  assert(Math.abs(block.usableMm2 - (50 - .53 - 1) * (40 - .53 - 1)) < 25, 'usable = outline less the inset band and the edge distance');
  const whole = P.estimateFit([rect(0, 0, 100, 50)], T, SHEET), half = P.estimateFit([rect(0, 0, 50, 50)], T, SHEET);
  assert(whole.pieces > 25 && whole.pieces <= Math.floor(0.68 * 100 * 50 / 110), `a whole 100 x 50 sheet holds about 30 pieces at 68 percent, not more than the area allows (${whole.pieces})`);
  assert(half.pieces < whole.pieces && half.pieces >= 12 && half.pieces <= 14, 'half the sheet, about half the pieces (' + half.pieces + ')');
  // an L shape: the 50 x 40 block plus a 30 x 10 arm going right along the top: more than the block alone, less than its bounding box
  const L = [[[0, 0], [80, 0], [80, 10], [50, 10], [50, 40], [0, 40]]], el = P.estimateFit(L, T, SHEET), box = P.estimateFit([rect(0, 0, 80, 40)], T, SHEET);
  assert(el.pieces > block.pieces && el.pieces < box.pieces, `L shape: ${block.pieces} < ${el.pieces} < ${box.pieces}`);
  // thin strips: narrower than a typical piece (9 mm) holds none of it; a wider strip holds some, packed lower than a clean block
  const thin = P.estimateFit([rect(0, 0, 100, 6)], T, SHEET), strip = P.estimateFit([rect(0, 0, 100, 12)], T, SHEET), wide = P.estimateFit([rect(0, 0, 100, 25)], T, SHEET);
  assert.equal(thin.pieces, 0, 'a 6 mm strip holds no typical piece'); assert(thin.high >= 1 && thin.high <= 4, 'only smaller pieces would fit it (high ' + thin.high + ')');
  assert(strip.pieces >= 3 && strip.pieces < wide.pieces, `a 12 mm strip holds a few (${strip.pieces}), a 25 mm one more (${wide.pieces})`);
  assert(strip.packedPct <= block.packedPct && wide.packedPct >= strip.packedPct, 'strips are never packed better than a clean block');
  for (const e of [block, whole, half, el, thin, strip, wide]) assert(e.low <= e.pieces && e.pieces <= e.high && e.packedPct >= 0 && e.packedPct <= 100, 'low <= pieces <= high');
  assert.deepEqual(P.estimateFit([], T, SHEET), { pieces: 0, low: 0, high: 0, packedPct: 0, usableMm2: 0, packMm2: 0 }, 'nothing left, nothing fits');
  assert(P.estimateFit([rect(0, 0, 50, 40)], { areaMm2: 220, minMm: 12, maxMm: 20 }, SHEET).pieces < block.pieces, 'bigger regular pieces, fewer fit');
  const t0 = Date.now(); for (let i = 0; i < 100; i++) P.estimateFit(L, T, SHEET); assert(Date.now() - t0 < 2500, '100 estimates are quick (' + (Date.now() - t0) + ' ms)');
  // the typical piece from the running sums: the default with no history, the metal's own average blended under a small prior
  assert.deepEqual(P.typicalFromSums(null), { ...P.DEFAULT_TYPICAL, n: 0 });
  const tf = P.typicalFromSums({ n: 50, areaMm2: 50 * 200, minMm: 50 * 12, maxMm: 50 * 20 }); assert(tf.areaMm2 > 190 && tf.areaMm2 < 200 && tf.n === 50, 'fifty pieces of 200 mm2 pull the typical piece to about 200 (' + tf.areaMm2 + ')');
  // planFor: filled one after the other, no limit on how many, whether all fit
  const card = (id, rings, at, extra) => ({ id, status: 'available', outline: rings, sheetWMm: 100, sheetHMm: 50, lastUsedAt: at, ...extra });
  const cards = [card('a-1', [rect(0, 0, 50, 40)], 3000), card('b-1', [rect(0, 0, 100, 25)], 2000), card('c-1', [rect(0, 0, 100, 6)], 5000), card('d-1', [rect(0, 0, 40, 30)], 1000, { status: 'inUse' })];
  const few = P.planFor(cards, { areaMm2: 5 * 110, count: 5 }, { typical: T });
  assert.deepEqual([few.fitsAll, few.needed, few.estimate, few.short.pieces], [true, ['a-1'], true, 0], 'five pieces fit the newest USEFUL one (the thin strip takes none; an in-use one is not offered)');
  const many = P.planFor(cards, { areaMm2: 25 * 110, count: 25 }, { typical: T });
  assert.equal(many.fitsAll, true); assert.deepEqual(many.needed, ['a-1', 'b-1'], 'more than one fits on a: the next one takes the rest'); assert(many.partials[0].usedMm2 === many.partials[0].capacityMm2, 'the first is filled before the next is used');
  const toomany = P.planFor(cards, { areaMm2: 80 * 110, count: 80 }, { typical: T });
  assert.equal(toomany.fitsAll, false); assert(toomany.short.mm2 > 0 && toomany.short.pieces > 0, 'say how many more are needed');
  assert.deepEqual(P.planFor(cards, { areaMm2: 25 * 110, count: 25 }, { typical: T, order: 'largest' }).needed[0], 'b-1', 'largest first when asked');
  const twelve = Array.from({ length: 12 }, (_, i) => card('s' + i + '-1', [rect(0, 0, 30, 30)], i));
  const lots = P.planFor(twelve, Array.from({ length: 40 }, () => ({ areaMm2: 110 })), { typical: T });
  assert(lots.needed.length > 4 && lots.fitsAll, 'there is no limit on how many partials take part (' + lots.needed.length + ')');
  assert.deepEqual(P.planFor(cards, [], {}).needed, [], 'no pieces, nothing needed');

  // 2. the data layer on a fake Firestore (reads before writes, in one transaction)
  const store = new Map(), clone = x => structuredClone(x);
  const ref = path => ({ path, id: path.split('/').at(-1), collection: n => query(path + '/' + n), get: async () => snap(path), set: async (v, o) => put({ path }, v, o && o.merge) });
  const snap = path => ({ id: path.split('/').at(-1), ref: ref(path), exists: store.has(path), data: () => clone(store.get(path)) });
  const query = (path, filters = [], order = null, limit = Infinity) => ({ doc: id => ref(path + '/' + id), where: (...f) => query(path, [...filters, f], order, limit), orderBy: (...o) => query(path, filters, o, limit), limit: n => query(path, filters, order, n),
    get: async () => { let docs = [...store.keys()].filter(k => k.startsWith(path + '/') && !k.slice(path.length + 1).includes('/')).map(snap); docs = docs.filter(d => filters.every(([f, , v]) => d.data()[f] === v)); if (order) docs.sort((a, b) => (a.data()[order[0]] - b.data()[order[0]]) * (order[1] === 'desc' ? -1 : 1)); docs = docs.slice(0, limit); return { docs, size: docs.length }; } });
  const put = (r, v, merge) => { refuseNestedArrays(v, r.path); const old = merge ? store.get(r.path) || {} : {}; const out = { ...old }; for (const [k, x] of Object.entries(v)) out[k] = x && x.__inc ? (+old[k] || 0) + x.__inc : clone(x); store.set(r.path, out); };
  let serial = Promise.resolve();
  const db = { runTransaction: fn => { const p = serial.then(async () => { const writes = []; let wrote = false; const r = await fn({ get: async x => { assert(!wrote, 'Firestore requires all reads before writes'); return x.get(); }, set: (x, v, o) => { wrote = true; writes.push(() => put(x, v, o && o.merge)); }, update: (x, v) => { wrote = true; writes.push(() => put(x, v, true)); }, delete: x => { wrote = true; writes.push(() => store.delete(x.path)); } }); writes.forEach(f => f()); return r; }); serial = p.catch(() => {}); return p; } };
  const FV = { serverTimestamp: () => 123456, increment: n => ({ __inc: n }) };
  const sheetLabel = d => `RG Sheet ${d.sheetIndex}`, setLabel = id => 'Set ' + (/-(\d+)$/.exec(id) || [])[1];
  const rem = Remnants({ db, col: query, FV, sheetLabel, setLabel, revDoc: () => ref('Charm_Nest_Rev/remnants'), configRef: () => ref('config/charmNestPartials'), statsRef: () => ref('Charm_Nest_Rev/partialStats'), statsWrite: () => true });
  const api = RoseStock({ db, col: query, FV, Readiness, sheetLabel, recordRemnant: rem.recordRemnant, remnantSync: rem.sync, stamp: async () => {} });
  rem.bind(api);
  const O = rem.ops;
  store.set('Charm_Nest_Rev/remnants', { n: 1, backfilledAt: 1 });   // (the backfill is the leftover test's: here it is done already)
  store.set('Charm_Nest_Runs/run-test', { lines: { a: { poolIds: ['p1', 'p2', 'p8'], spec: { engraveCandidate: false } } } });
  const sheetDoc = (id, shapes, idx) => ({ id, metal: 'rose', sheetIndex: idx, setId: 'set-2026-1', fileBase: 'RG_x_Sheet-' + idx, verification: { ok: true }, status: 'complete', dirty: false, saving: false, draft: false, runId: 'run-test', poolIds: shapes.map(s => s.id), placedCount: shapes.length,
    placements: shapes.map(s => ({ id: s.id, cxPt: 20, cyPt: 20, angle: 0, scale: 1 })), charms: shapes.map(s => ({ id: s.id, areaPt2: 600, widthPt: 28, heightPt: 34 })), outputs: { ai: { url: 'saved.ai' }, preview: { url: 'saved.png' } }, label: { files: [{ path: 'qr', url: 'qr.png', payload: 'test', orders: [] }] }, orders: [] });
  const cutOne = async (sheetId, shapes, idx, stockArgs, by) => {
    const claim = await api.roseClaim({ sheetId, wPt: 100 / MM, hPt: 50 / MM, ...stockArgs }), stockId = claim.stock.id, doc = sheetDoc(sheetId, shapes, idx);
    store.set('Charm_Nest_Sheets/' + sheetId, doc);
    const p = await api.rosePlan({ sheetId, stockId, revision: claim.stock.revision, fingerprint: RoseStock.fingerprint(doc), shapesJson: JSON.stringify(shapes), allowanceMm: .2, cut: true });
    return api.roseRecordCut({ sheetId, stockId, revision: claim.stock.revision, planHash: p.planHash, by, via: 'nest', device: 'charm-nest-1' });
  };
  const one = await cutOne('sheet-1', [shape('p1', 2, 2, 10, 30)], 1, {}, 'Pat Lee');
  const stockId = one.stock.id, pid = `${stockId}-1`;

  // the record: lastUsed starts as the cut; the metal's piece sums moved with the cut
  const r1 = store.get('Charm_Nest_Remnants/' + pid);
  assert.deepEqual([r1.status, r1.lastUsedAt, r1.lastUsedBy, r1.lastUsedSheet, r1.lastUsedSheetId, r1.inUseBySheetId], ['available', one.cut.at, 'Pat Lee', 'RG Sheet 1', 'sheet-1', null]);
  const stats = store.get('Charm_Nest_Rev/partialStats');
  assert.equal(stats.rose_n, 1); assert(Math.abs(stats.rose_areaMm2 - 600 * MM * MM) < .02 && Math.abs(stats.rose_minMm - 28 * MM) < .02 && Math.abs(stats.rose_maxMm - 34 * MM) < .02, 'the cut taught the metal its regular piece');

  // the list: the card, the estimate, the policies, an unchanged answer from one tiny read
  const list = await O.partialList({ metal: 'rose' });
  assert.equal(list.items.length, 1); const c = list.items[0];
  assert.deepEqual([c.id, c.metal, c.code, c.status, c.sourceSheet, c.sourceSet, c.cutBy, c.cutAt, c.lastUsedAt, c.stockId, c.revision], [pid, 'rose', 'RG', 'available', 'RG Sheet 1', 'Set 1', 'Pat Lee', one.cut.at, one.cut.at, stockId, 1]);
  assert(c.outline.length && c.sheetWMm === 100 && c.sheetHMm === 50 && c.wMm > 0 && c.hMm > 0 && c.areaMm2 > 0 && c.bboxMm.w === c.wMm, 'outline, sheet, W x H, area');
  assert(Math.abs(c.wPt - 100 / MM) < .01 && Math.abs(c.hPt - 50 / MM) < .01, 'the physical sheet in points (the claim tolerates .01)');
  assert(c.estimate.pieces >= 1 && c.estimate.low <= c.estimate.pieces && c.estimate.pieces <= c.estimate.high && c.estimate.packedPct > 0, 'the estimate is on the card');
  assert.deepEqual(list.policies.rose, { mode: 'auto', wMm: 100, hMm: 50, by: '', at: null }, 'no setting saved: automatic, 100 x 50');
  assert.deepEqual([list.typical.n, list.typical.areaMm2 > 100], [1, true]);
  assert.deepEqual(await O.partialList({ metal: 'rose', ifRev: list.rev }), { unchanged: true, rev: list.rev }, 'nothing moved: one tiny read');
  assert.deepEqual((await O.partialList({ metal: 'gold10k' })).items, [], 'each metal has its own repository');
  await assert.rejects(() => O.partialList({ metal: 'silver' }), /Choose Rose Gold, 10K Gold or 14K Gold/);

  // claim: one source of truth (the stock's owner); two sheets can never hold it
  const claimed = await O.partialClaim({ metal: 'rose', id: pid, sheetId: 'sheet-2', by: 'Ana Ruiz', sheetName: 'RG Sheet 2' });
  const s1 = store.get('Charm_Nest_Rose_Stock/' + stockId), rc = store.get('Charm_Nest_Remnants/' + pid);
  assert.deepEqual([s1.owner, s1.available, claimed.stock.owner, claimed.stock.id], ['sheet-2', false, 'sheet-2', stockId], 'the stock is held by the sheet');
  assert.deepEqual([rc.status, rc.inUseBySheetId, rc.inUseBySheetName, rc.inUseBy], ['inUse', 'sheet-2', 'RG Sheet 2', 'Ana Ruiz']);
  assert(rc.lastUsedAt >= one.cut.at && rc.lastUsedBy === 'Ana Ruiz' && rc.lastUsedSheet === 'RG Sheet 2' && rc.lastUsedSheetId === 'sheet-2', 'last use = nested on it now');
  assert.equal(claimed.partial.status, 'inUse');
  assert.equal((await O.partialList({ metal: 'rose' })).items.length, 0, 'a held partial leaves the available list');
  const held = await O.partialList({ metal: 'rose', inUse: true }); assert.deepEqual(held.items.map(i => [i.id, i.status, i.inUseBySheetName]), [[pid, 'inUse', 'RG Sheet 2']]);
  await assert.rejects(() => O.partialClaim({ metal: 'rose', id: pid, sheetId: 'sheet-3', by: 'Ben' }), /in use by RG Sheet 2/, 'another sheet cannot claim it (the list)');
  await assert.rejects(() => api.roseClaim({ sheetId: 'sheet-3', wPt: 100 / MM, hPt: 50 / MM, stockId }), /reserved for another layout/, 'nor through the older path (the stock)');
  await assert.rejects(() => api.roseClaim({ sheetId: 'sheet-3', wPt: 100 / MM, hPt: 50 / MM, stockId, exact: true, partialId: pid }), /in use by RG Sheet 2|reserved/, 'nor with an exact claim');
  await assert.rejects(() => O.partialClaim({ metal: 'gold10k', id: pid, sheetId: 'sheet-3' }), /Rose Gold, not 10K Gold/, 'a partial is its own metal');
  assert.equal((await O.partialClaim({ metal: 'rose', id: pid, sheetId: 'sheet-2', by: 'Ana Ruiz' })).stock.owner, 'sheet-2', 'the same sheet asking again is the same claim');
  await assert.rejects(() => O.partialUse({ id: pid, sheetId: 'sheet-3' }), /in use by RG Sheet 2/, 'only the holder can use it');

  // release gives it back; lastUsedAt stays what the claim set; nothing is deleted
  const usedAtClaim = rc.lastUsedAt;
  assert.deepEqual(await O.partialRelease({ sheetId: 'sheet-2' }), { ok: true, released: true, stockId, partialId: pid });
  const back = store.get('Charm_Nest_Remnants/' + pid), sb = store.get('Charm_Nest_Rose_Stock/' + stockId);
  assert.deepEqual([back.status, back.inUseBySheetId, back.lastUsedAt, sb.owner, sb.available], ['available', null, usedAtClaim, null, true]);
  assert.deepEqual(await O.partialRelease({ sheetId: 'sheet-nothing' }), { ok: true, released: false, partialId: null }, 'a sheet that holds nothing releases nothing');
  assert.equal((await O.partialList({ metal: 'rose' })).items.length, 1, 'and it is available again');

  // use: used + dated, the holder keeps its claim until its cut or release; a release before a cut undoes it
  await O.partialClaim({ metal: 'rose', id: pid, sheetId: 'sheet-2', by: 'Ana Ruiz', sheetName: 'RG Sheet 2' });
  const used = await O.partialUse({ id: pid, sheetId: 'sheet-2', by: 'Ana Ruiz' }), ru = store.get('Charm_Nest_Remnants/' + pid);
  assert.deepEqual([used.partial.status, ru.status, ru.usedBySheetId, ru.inUseBySheetId, ru.lastUsedBy, store.get('Charm_Nest_Rose_Stock/' + stockId).owner, store.get('Charm_Nest_Rose_Stock/' + stockId).available], ['used', 'used', 'sheet-2', null, 'Ana Ruiz', 'sheet-2', false]);
  assert.equal((await O.partialUse({ id: pid, sheetId: 'sheet-2' })).same, true, 'using it again is the same use');
  await assert.rejects(() => O.partialClaim({ metal: 'rose', id: pid, sheetId: 'sheet-3' }), /already used/);
  await O.partialRelease({ sheetId: 'sheet-2' });
  assert.deepEqual([store.get('Charm_Nest_Remnants/' + pid).status, store.get('Charm_Nest_Remnants/' + pid).usedBySheetId, store.get('Charm_Nest_Rose_Stock/' + stockId).available], ['available', null, true], 'given back before a cut: it was not used');

  // a person's mark follows the stock: discarded can be claimed by nobody, put back can again
  await O.remnantMark({ id: pid, status: 'discarded', by: 'Pat Lee' });
  assert.equal(store.get('Charm_Nest_Rose_Stock/' + stockId).available, false, 'the older path stops offering it too');
  await assert.rejects(() => O.partialClaim({ metal: 'rose', id: pid, sheetId: 'sheet-3' }), /discarded/);
  await O.remnantMark({ id: pid, status: 'available', by: 'Pat Lee' });
  assert.equal(store.get('Charm_Nest_Rose_Stock/' + stockId).available, true);

  // the older path (Rose Gold as today: the first available stock of the sheet's size) keeps the record in step too
  const viaOld = await api.roseClaim({ sheetId: 'sheet-5', wPt: 100 / MM, hPt: 50 / MM, nesting: false, by: 'Cy' });
  assert.equal(viaOld.stock.id, stockId, 'the older path finds the same stock');
  assert.deepEqual([store.get('Charm_Nest_Remnants/' + pid).status, store.get('Charm_Nest_Remnants/' + pid).inUseBySheetId], ['inUse', 'sheet-5']);
  await assert.rejects(() => O.partialClaim({ metal: 'rose', id: pid, sheetId: 'sheet-2' }), /in use by sheet-5/, 'whichever way it was taken');
  await assert.rejects(() => O.remnantMark({ id: pid, status: 'discarded' }), /holds this leftover/, 'a held leftover cannot be marked');
  await api.roseRelease({ stockId, sheetId: 'sheet-5' });
  assert.equal(store.get('Charm_Nest_Remnants/' + pid).status, 'available', 'released by the older path: available again');

  // swap: one transaction; a refused claim loses nothing; without swap a sheet holding another physical sheet is refused
  const fresh = await api.roseClaim({ sheetId: 'sheet-6', wPt: 100 / MM, hPt: 50 / MM, fresh: true, nesting: false });
  await assert.rejects(() => O.partialClaim({ metal: 'rose', id: pid, sheetId: 'sheet-6' }), /already holds another physical sheet/);
  await O.partialClaim({ metal: 'rose', id: pid, sheetId: 'sheet-7', sheetName: 'RG Sheet 7' });
  await assert.rejects(() => O.partialClaim({ metal: 'rose', id: pid, sheetId: 'sheet-6', swap: true }), /in use by RG Sheet 7/);
  assert.equal(store.get('Charm_Nest_Rose_Stock/' + fresh.stock.id).owner, 'sheet-6', 'a refused swap changes nothing: the sheet still holds what it held');
  await O.partialRelease({ sheetId: 'sheet-7' });
  const sw = await O.partialClaim({ metal: 'rose', id: pid, sheetId: 'sheet-6', swap: true, by: 'Dee' });
  assert.deepEqual([sw.stock.id, store.get('Charm_Nest_Rose_Stock/' + fresh.stock.id).owner, store.get('Charm_Nest_Rose_Stock/' + fresh.stock.id).available, store.get('Charm_Nest_Rose_Stock/' + stockId).owner], [stockId, null, true, 'sheet-6'], 'given back and taken in one go');
  await O.partialRelease({ sheetId: 'sheet-6' });

  // Paul, 7 Oct: "I'm prevented from using All, new sheets and existing partial sheets. There's no reason why I should be prevented." A sheet in a committed or the current
  // set, with a saved green line that was never cut, or holding protected lines may swap onto a partial: the line of the old seat is dropped with that seat (never carried onto
  // the new outline), the sheet keeps its set, the old physical sheet goes back, a small note says what was dropped. A recorded cut is still refused, and loses nothing.
  {
    const mine = []; const stockDoc = id => store.get('Charm_Nest_Rose_Stock/' + id);
    const seatedSheet = async (id, idx, extra) => {
      const f = await api.roseClaim({ sheetId: id, wPt: 100 / MM, hPt: 50 / MM, fresh: true, nesting: false }); mine.push(f.stock.id);
      const shapes = [shape('q' + idx, 25, 2, 8, 25)], doc = { ...sheetDoc(id, shapes, idx), ...(extra || {}) }; store.set('Charm_Nest_Sheets/' + id, doc);
      return { f, doc, shapes };
    };
    // 1. a sheet of a committed set with a saved green line (a plan, never cut)
    const a = await seatedSheet('sheet-9', 9);
    await api.rosePlan({ sheetId: 'sheet-9', stockId: a.f.stock.id, revision: 0, fingerprint: RoseStock.fingerprint(a.doc), shapesJson: JSON.stringify(a.shapes), allowanceMm: .2, cut: true });
    assert(store.get('Charm_Nest_Sheets/sheet-9').rosePlanJson && store.get('Charm_Nest_Sheets/sheet-9').setId === 'set-2026-1', 'the sheet is in its set and holds a saved line');
    const r9 = await O.partialClaim({ metal: 'rose', id: pid, sheetId: 'sheet-9', swap: true, by: 'Fay' });
    const d9 = store.get('Charm_Nest_Sheets/sheet-9');
    assert.equal(r9.stock.id, stockId); assert.equal(r9.protectedJson, null, 'no line of the old seat is handed to the nest on the new outline');
    assert.deepEqual([d9.rosePlanJson, d9.rosePlanHash, d9.roseFingerprint, d9.roseProtectedJson, d9.dirty, d9.setId, d9.draft, d9.roseStockId, d9.roseCutAt], [null, null, null, null, true, 'set-2026-1', false, stockId, undefined], 'plan and protected line gone, sheet marked changed, its set and its record otherwise untouched');
    assert.deepEqual(d9.roseReseated.map(x => [x.fromStockId, x.toStockId, x.plan, x.kept]), [[a.f.stock.id, stockId, true, false]], 'a small note of what was dropped');
    assert.deepEqual([stockDoc(a.f.stock.id).owner, stockDoc(a.f.stock.id).available, stockDoc(stockId).owner], [null, true, 'sheet-9'], 'the old physical sheet went back, the partial is held');
    assert.deepEqual([store.get('Charm_Nest_Remnants/' + pid).status, store.get('Charm_Nest_Remnants/' + pid).inUseBySheetId], ['inUse', 'sheet-9']);
    assert.deepEqual(d9.placements.map(x => x.id), ['q9'], 'its pieces and placements are not deleted by the claim (the nest places them again)');
    // 2. a sheet in the current set holding the lines an append kept (protected), swapped onto the partial once it is free again
    store.set('Charm_Nest_Sheets/sheet-9', { ...d9, draft: true }); await O.partialRelease({ sheetId: 'sheet-9' });   // (the older release path, a draft sheet: the partial is free again)
    assert.equal(store.get('Charm_Nest_Remnants/' + pid).status, 'available');
    const b = await seatedSheet('sheet-10', 10, { roseProtectedJson: JSON.stringify({ profile: { version: 1 }, lines: [], shapes: [], placements: [], stages: [] }) });
    const r10 = await O.partialClaim({ metal: 'rose', id: pid, sheetId: 'sheet-10', swap: true });
    const d10 = store.get('Charm_Nest_Sheets/sheet-10'); assert.equal(r10.protectedJson, null); assert.deepEqual([d10.roseProtectedJson, d10.rosePlanJson, d10.setId, d10.dirty, d10.roseReseated[0].kept, d10.roseReseated[0].plan], [null, null, 'set-2026-1', true, true, false]);
    assert.equal(stockDoc(b.f.stock.id).owner, null, 'given back');
    // 3. the same claim without swap keeps the saved lines exactly as before (the nest's own claim of the stock it already holds)
    store.set('Charm_Nest_Sheets/sheet-10', { ...d10, draft: true }); await O.partialRelease({ sheetId: 'sheet-10' });
    const c = await seatedSheet('sheet-12', 12, { roseProtectedJson: JSON.stringify({ profile: { version: 1 }, lines: [], shapes: [], placements: [], stages: [] }) });
    const keep = await api.roseClaim({ sheetId: 'sheet-12', wPt: 100 / MM, hPt: 50 / MM, stockId: c.f.stock.id, nesting: true });
    assert(keep.protectedJson && store.get('Charm_Nest_Sheets/sheet-12').roseProtectedJson, 'a plain nest claim keeps the protected lines');
    // 4. a recorded cut is permanent: refused, and the sheet still holds what it held
    const d = await seatedSheet('sheet-11', 11, { roseCutAt: 1760000000000 });
    await assert.rejects(() => O.partialClaim({ metal: 'rose', id: pid, sheetId: 'sheet-11', swap: true }), /already cut/);
    assert.equal(stockDoc(d.f.stock.id).owner, 'sheet-11', 'a refused swap loses nothing'); assert.equal(store.get('Charm_Nest_Remnants/' + pid).status, 'available');
    for (const id of mine) store.delete('Charm_Nest_Rose_Stock/' + id); for (const id of ['sheet-9', 'sheet-10', 'sheet-11', 'sheet-12']) store.delete('Charm_Nest_Sheets/' + id);
  }

  // the cut: the leftover it was made on is used (by that sheet), the new one is available with lastUsed = the cut; the held record clears
  await O.partialClaim({ metal: 'rose', id: pid, sheetId: 'sheet-8', by: 'Eve', sheetName: 'RG Sheet 8' });
  const two = await cutOne('sheet-8', [shape('p8', 25, 2, 8, 25)], 8, { stockId, revision: 1 }, 'Eve');
  const prev = store.get('Charm_Nest_Remnants/' + pid), next = store.get(`Charm_Nest_Remnants/${stockId}-2`);
  assert.deepEqual([prev.status, prev.usedBySheetId, prev.inUseBySheetId, prev.lastUsedAt, prev.lastUsedBy, prev.lastUsedSheet], ['used', 'sheet-8', null, two.cut.at, 'Eve', 'RG Sheet 8'], 'cut from it: used, dated');
  assert.deepEqual([next.status, next.lastUsedAt, next.lastUsedSheet, next.inUseBySheetId], ['available', two.cut.at, 'RG Sheet 8', null]);
  assert.equal(store.get('Charm_Nest_Rev/partialStats').rose_n, 2, 'a second cut, two pieces taught');
  const later = await O.partialList({ metal: 'rose', used: true });
  assert.deepEqual(later.items.map(i => [i.id, i.status]), [[`${stockId}-2`, 'available'], [pid, 'used']], 'newest used first; the used flag adds the used ones; a used one has no estimate');
  assert.equal(later.items[1].estimate, null);

  // the setting: default automatic; set per metal with the signed-in name; sizes kept when it goes back to automatic; bad sizes refused
  assert.deepEqual((await O.partialPolicyGet()).policies.gold14k, { mode: 'auto', wMm: 100, hMm: 50, by: '', at: null });
  const set = await O.partialPolicySet({ metal: 'gold14k', mode: 'new', wMm: 120, hMm: 60.04, by: 'Pat Lee' });
  assert.deepEqual([set.policy.mode, set.policy.wMm, set.policy.hMm, set.policy.by], ['new', 120, 60, 'Pat Lee']);
  assert.deepEqual([(await O.partialPolicyGet()).policies.rose.mode, (await O.partialPolicyGet()).policies.gold14k.mode], ['auto', 'new'], 'one metal does not change another');
  assert.equal((await O.partialPolicySet({ metal: 'gold14k', mode: 'auto', by: 'Pat Lee' })).policy.wMm, 120, 'automatic keeps the size the person typed');
  assert.equal((await O.partialPolicySet({ metal: 'gold14k', mode: 'new', by: 'Pat Lee' })).policy.hMm, 60, 'and "new" without a size takes the saved one');
  await assert.rejects(() => O.partialPolicySet({ metal: 'rose', mode: 'new', wMm: 2, hMm: 50 }), /5 to 500 mm/);
  await assert.rejects(() => O.partialPolicySet({ metal: 'rose', mode: 'new', wMm: 'big', hMm: 50 }), /5 to 500 mm/);
  await assert.rejects(() => O.partialPolicySet({ metal: 'rose', mode: 'maybe' }), /Choose to reuse/);
  await assert.rejects(() => O.partialPolicySet({ metal: 'silver', mode: 'auto' }), /Choose Rose Gold/);
  const afterSet = await O.partialList({ metal: 'rose', ifRev: later.rev }); assert.equal(afterSet.unchanged, undefined, 'a changed setting moves the counter: open pages read it');
  assert.equal(afterSet.policies.gold14k.mode, 'new', 'and the list carries the setting');

  // the plan op reads the same list; stocks reads only the ids asked
  const plan = await O.partialPlan({ metal: 'rose', pieces: { areaMm2: 4 * 110, count: 4 } });
  assert.deepEqual([plan.fitsAll, plan.needed, plan.metal], [true, [`${stockId}-2`], 'rose']);
  const st = await O.partialStocks({ ids: [`${stockId}-2`, pid, 'rgs-nothing-3', '!!'] });
  assert.deepEqual([st.stocks[`${stockId}-2`].current, st.stocks[pid].current, st.stocks['rgs-nothing-3'].missing, Object.keys(st.stocks).length], [true, false, true, 3]);
  assert(JSON.parse(st.stocks[`${stockId}-2`].profileJson).values.length > 10 && st.stocks[`${stockId}-2`].wPt > 0, 'the stock under a partial: its profile and size, for a trial pack');

  // a held stock at the first list ever: the one-time backfill saves it as 'inUse', never as available
  store.set('Charm_Nest_Rev/remnants', { n: 9 });   // (not backfilled yet)
  const old = Rose.plan([shape('z', 2, 2, 12, 40)], 100, 50, null, .2).profile;
  store.set('Charm_Nest_Rose_Stock/rgs-old-1', { id: 'rgs-old-1', wPt: 100, hPt: 50, revision: 2, profileJson: JSON.stringify(old), available: false, owner: 'sheet-held', lastCutAt: 1759660000000, metal: 'rose' });
  store.set('Charm_Nest_Sheets/sheet-old', { id: 'sheet-old', metal: 'rose', sheetIndex: 1, setId: 'set-2026-10-05-1', fileBase: 'RG_2026-10-05_Sheet-1' });
  store.set('Charm_Nest_Rose_Stock/rgs-old-1/cuts/c', { sheetId: 'sheet-old', revision: 2, by: 'Ana', at: 1759660000000, fileBase: 'RG_2026-10-05_Sheet-1' });
  // ... and a record that said 'available' before claims kept it in step, while a sheet already holds that physical sheet, is set to inUse (once)
  store.set('Charm_Nest_Rose_Stock/rgs-held-2', { id: 'rgs-held-2', wPt: 100 / MM, hPt: 50 / MM, revision: 1, profileJson: JSON.stringify(old), available: false, owner: 'sheet-hold2', metal: 'rose' });
  store.set('Charm_Nest_Sheets/sheet-hold2', { id: 'sheet-hold2', metal: 'rose', sheetIndex: 4 });
  store.set('Charm_Nest_Remnants/rgs-held-2-1', { ...store.get('Charm_Nest_Remnants/' + pid), id: 'rgs-held-2-1', stockId: 'rgs-held-2', revision: 1, status: 'available', usedBySheetId: null });
  const first = await O.partialList({ metal: 'rose', inUse: true });
  assert(first.backfilled >= 1, String(first.backfillError) + ' ' + 'the first list ever saved the earlier leftovers (' + first.backfilled + ')');
  const bf = first.items.find(i => i.id === 'rgs-old-1-2');
  assert.deepEqual([bf.status, bf.inUseBySheetId, bf.lastUsedAt], ['inUse', 'sheet-held', 1759660000000], 'a stock a sheet holds is saved as held, with its cut as its last use');
  const rec2 = first.items.find(i => i.id === 'rgs-held-2-1');
  assert.deepEqual([first.reconciled, rec2.status, rec2.inUseBySheetId, rec2.inUseBySheetName], [1, 'inUse', 'sheet-hold2', 'RG Sheet 4'], 'a partial a sheet already holds is no longer offered');
  assert.equal((await O.partialList({ metal: 'rose', inUse: true })).backfilled, undefined, 'once, never again');
  assert.equal((await O.partialList({ metal: 'rose', inUse: true })).reconciled, undefined, 'the reconcile too');
  // 3. the page's data layer (charm-nest-partial-data.js) against these very ops: cached per page, a revision probe, the name added by itself, the setting read sync
  const sent = [];
  global.window = { CharmNestPartial: P, CNEmployee: { name: () => 'Pat Lee' }, CN: { api: async (fn, body) => { sent.push(body); const r = await O[body.op](body); return JSON.parse(JSON.stringify(r)); } } };
  require('../../charm-nest-partial-data.js');
  const PS = global.window.PartialSheets, ops = () => sent.splice(0).map(b => b.op + (b.ifRev ? '+ifRev' : '') + (b.verify ? '+verify' : ''));
  assert.deepEqual(PS.policy('rose'), { mode: 'auto', wMm: 100, hMm: 50 }, 'before anything is loaded: automatic, 100 x 50');
  assert.equal(global.window.partialPolicy('gold10k').mode, 'auto');
  const a = await PS.list('rose'); assert(a.items.length >= 1 && a.rev && a.policies); assert.deepEqual(ops(), ['partialList'], 'the first read');
  assert.equal(PS.policy('gold14k').mode, 'new', 'the list carried the setting: now the page knows it without another call'); assert.deepEqual([PS.policy('gold14k').wMm, PS.policy('gold14k').hMm], [120, 60]);
  await PS.list('rose'); assert.deepEqual(ops(), [], 'asked again within 20 s: no call at all');
  PS.changed(); const b2 = await PS.list('rose'); assert.deepEqual(ops(), ['partialList+ifRev'], 'after a change: one call that sends the revision'); assert.equal(b2, a, 'nothing moved: the same answer, one tiny read');
  await PS.list('rose', { force: true }); assert.deepEqual(ops(), ['partialList+verify'], 'Refresh reads in full');
  assert.deepEqual(await PS.loadPolicy(), { ...(await PS.loadPolicy()) }); assert.deepEqual(ops(), [], 'the setting is already known: no call');
  const cl = await PS.claim('rose', `${stockId}-2`, 'sheet-9', { sheetName: 'RG Sheet 9' }); assert.equal(cl.stock.owner, 'sheet-9');
  assert.equal(store.get(`Charm_Nest_Remnants/${stockId}-2`).inUseBy, 'Pat Lee', 'the signed-in name was added by the page: nobody typed it'); ops();
  assert.equal((await PS.list('rose')).items.length, a.items.length - 1, 'a claim made here is seen at once'); assert.deepEqual(ops(), ['partialList+ifRev']);
  const pl = await PS.plan('rose', [{ areaMm2: 110 }, { areaMm2: 110 }]); assert.deepEqual([pl.fitsAll, pl.estimate], [pl.needed.length > 0, true]); ops();
  assert.equal((await PS.release('sheet-9')).partialId, `${stockId}-2`); ops();
  const stk = await PS.stocks([`${stockId}-2`]); assert(stk[`${stockId}-2`].profileJson); ops(); await PS.stocks([`${stockId}-2`]); assert.deepEqual(ops(), [], 'a stock read once is kept');
  assert.equal((await PS.setPolicy('rose', { mode: 'new', wMm: 90, hMm: 45 })).by, 'Pat Lee'); assert.deepEqual(PS.policy('rose'), { mode: 'new', wMm: 90, hMm: 45 }, 'the setting is read sync right after it is saved');
  console.log('partial-sheets-data: ok (estimateFit rectangle / L / thin strip, planFor, status model, claim / release / use / swap, one source of truth with the stock, list, setting, backfill)');
})().catch(e => { console.error(e); process.exit(1); });
