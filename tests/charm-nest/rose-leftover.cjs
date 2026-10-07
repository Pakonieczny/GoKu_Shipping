// GC3: the leftover sheet a green line makes is saved in the cut's own transaction, in its exact shape and real size, and listed.
//   node tests/charm-nest/rose-leftover.cjs
'use strict';
const assert = require('node:assert/strict');
const Rose = require('../../charm-nest-rose'), Readiness = require('../../charm-nest-readiness');
const Remnants = require('../../netlify/functions/_charmNestRemnants'), RoseStock = require('../../netlify/functions/_charmNestRoseStock');
const { leftover } = Remnants, MM = Rose.MM;
const shape = (id, x, y, w, h) => ({ id, paths: [[[x, y], [x + w, y], [x + w, y + h], [x, y + h]]] });
// even-odd point in rings (mm)
const inside = (rings, x, y) => { let on = false; for (const ring of rings) for (let a = 0, b = ring.length - 1; a < ring.length; b = a++) { const p = ring[a], q = ring[b]; if ((p[1] > y) !== (q[1] > y) && x < (q[0] - p[0]) * (y - p[1]) / (q[1] - p[1]) + p[0]) on = !on; } return on; };
const shoelace = rings => rings.reduce((s, ring) => s + Math.abs(ring.reduce((t, p, i) => { const q = ring[(i + 1) % ring.length]; return t + p[0] * q[1] - q[0] * p[1]; }, 0) / 2), 0);

(async () => {
  // 1. the leftover of a real plan (both axes) is the stock minus the region the profile cut: same answer as the rows, point by point
  const landscape = Rose.plan([shape('a', 2, 2, 10, 15), shape('b', 2, 22, 16, 23)], 100, 50, null, .2).profile;
  const portrait = Rose.plan([shape('d', 2, 2, 40, 8), shape('e', 2, 14, 22, 30)], 50, 100, { version: 1, wPt: 50, hPt: 100, axis: 'y', step: .5, values: Array(100).fill(0) }, .2).profile;
  const later = Rose.plan([shape('c', 22, 3, 12, 30)], 100, 50, landscape, .2).profile;   // a second cut on the same sheet: smaller again
  for (const [name, profile] of [['landscape', landscape], ['portrait', portrait], ['second cut', later]]) {
    const g = leftover(profile), W = profile.wPt * MM, H = profile.hPt * MM;
    assert.equal(g.sheetWMm, +W.toFixed(3)); assert.equal(g.sheetHMm, +H.toFixed(3));
    const want = (profile.wPt * profile.hPt - Rose.area(profile)) * MM * MM;
    assert(Math.abs(g.areaMm2 - want) < .01, `${name}: area ${g.areaMm2} is the stock minus the cut region ${want.toFixed(2)}`);
    assert(Math.abs(shoelace(g.rings) - g.areaMm2) / g.areaMm2 < .002, `${name}: the rings enclose that area`);
    let bad = 0, n = 0;
    for (let x = .17; x < W; x += .6) for (let y = .13; y < H; y += .6) { n++; if (inside(g.rings, x, y) === Rose.intersects(profile, x / MM, y / MM, 0, 0)) bad++; }
    assert.equal(bad, 0, `${name}: ${bad} of ${n} sample points are on the wrong side of the leftover's edge`);
    const xs = g.rings.flat().map(p => p[0]), ys = g.rings.flat().map(p => p[1]);
    assert.deepEqual(g.bboxMm, { x: Math.min(...xs), y: Math.min(...ys), w: +(Math.max(...xs) - Math.min(...xs)).toFixed(3), h: +(Math.max(...ys) - Math.min(...ys)).toFixed(3) });
    assert(g.areaMm2 < W * H * .98 && g.bboxMm.w * g.bboxMm.h >= g.areaMm2 - .01, `${name}: a partial sheet is smaller than the stock and fits its box`);
  }
  assert(leftover(later).areaMm2 < leftover(landscape).areaMm2, 'a later cut leaves less');
  const whole = leftover({ ...landscape, values: landscape.values.map(() => landscape.wPt) });
  assert.deepEqual([whole.rings, whole.areaMm2], [[], 0], 'a sheet cut right through leaves nothing');
  const uncut = leftover({ ...landscape, values: landscape.values.map(() => 0) });
  assert.deepEqual([uncut.rings.length, uncut.rings[0].length, uncut.areaMm2], [1, 4, +(100 * 50 * MM * MM).toFixed(2)], 'an uncut sheet is its whole rectangle');

  // 2. the record: written in the cut's transaction (with a fake Firestore that refuses a read after a write), the previous leftover turns used
  const store = new Map(), clone = x => structuredClone(x);
  const ref = path => ({ path, id: path.split('/').at(-1), collection: n => query(path + '/' + n), get: async () => snap(path), set: async (v, o) => put({ path }, v, o && o.merge) });
  const snap = path => ({ id: path.split('/').at(-1), ref: ref(path), exists: store.has(path), data: () => clone(store.get(path)) });
  const query = (path, filters = [], order = null, limit = Infinity) => ({ doc: id => ref(path + '/' + id), where: (...f) => query(path, [...filters, f], order, limit), orderBy: (...o) => query(path, filters, o, limit), limit: n => query(path, filters, order, n),
    get: async () => { let docs = [...store.keys()].filter(k => k.startsWith(path + '/') && !k.slice(path.length + 1).includes('/')).map(snap); docs = docs.filter(d => filters.every(([f, , v]) => d.data()[f] === v)); if (order) docs.sort((a, b) => (a.data()[order[0]] - b.data()[order[0]]) * (order[1] === 'desc' ? -1 : 1)); docs = docs.slice(0, limit); return { docs, size: docs.length }; } });
  const put = (r, v, merge) => { const old = merge ? store.get(r.path) || {} : {}; const out = { ...old }; for (const [k, x] of Object.entries(v)) out[k] = x && x.__inc ? (+old[k] || 0) + x.__inc : clone(x); store.set(r.path, out); };
  let serial = Promise.resolve();
  const db = { runTransaction: fn => { const p = serial.then(async () => { const writes = []; let wrote = false; const r = await fn({ get: async x => { assert(!wrote, 'Firestore requires all reads before writes'); return x.get(); }, set: (x, v, o) => { wrote = true; writes.push(() => put(x, v, o && o.merge)); }, update: (x, v) => { wrote = true; writes.push(() => put(x, v, true)); }, delete: x => { wrote = true; writes.push(() => store.delete(x.path)); } }); writes.forEach(f => f()); return r; }); serial = p.catch(() => {}); return p; } };
  const FV = { serverTimestamp: () => 123456, increment: n => ({ __inc: n }) };
  const sheetLabel = d => `RG Sheet ${d.sheetIndex}`, setLabel = id => 'Set ' + (/-(\d+)$/.exec(id) || [])[1];
  const rem = Remnants({ db, col: query, FV, sheetLabel, setLabel, revDoc: () => ref('Charm_Nest_Rev/remnants') });
  const api = RoseStock({ db, col: query, FV, Readiness, sheetLabel, recordRemnant: rem.recordRemnant, stamp: async () => {} });
  const sheetDoc = (id, shapes, idx) => ({ id, metal: 'rose', sheetIndex: idx, setId: 'set-2026-1', fileBase: 'RG_x_Sheet-' + idx, verification: { ok: true }, status: 'complete', dirty: false, saving: false, draft: false, runId: 'run-test', poolIds: shapes.map(s => s.id), placedCount: shapes.length,
    placements: shapes.map(s => ({ id: s.id, cxPt: 20, cyPt: 20, angle: 0, scale: 1 })), outputs: { ai: { url: 'saved.ai' }, preview: { url: 'saved.png' } }, label: { files: [{ path: 'qr', url: 'qr.png', payload: 'test', orders: [] }] }, orders: [] });
  store.set('Charm_Nest_Runs/run-test', { lines: { a: { poolIds: ['p1', 'p2'], spec: { engraveCandidate: false } } } });
  const cutOne = async (sheetId, shapes, idx, stockArgs, by, via) => {
    const claim = await api.roseClaim({ sheetId, wPt: 100, hPt: 50, ...stockArgs }), stockId = claim.stock.id, doc = sheetDoc(sheetId, shapes, idx);
    store.set('Charm_Nest_Sheets/' + sheetId, doc);
    const p = await api.rosePlan({ sheetId, stockId, revision: claim.stock.revision, fingerprint: RoseStock.fingerprint(doc), shapesJson: JSON.stringify(shapes), allowanceMm: .2, cut: true });
    return api.roseRecordCut({ sheetId, stockId, revision: claim.stock.revision, planHash: p.planHash, by, via, device: 'charm-nest-1' });
  };
  const one = await cutOne('sheet-1', [shape('p1', 2, 2, 10, 30)], 1, {}, 'Pat Lee', 'nest');
  const stockId = one.stock.id, r1 = store.get(`Charm_Nest_Remnants/${stockId}-1`);
  assert(r1, 'the cut saved its leftover sheet in the same transaction');
  assert.deepEqual([r1.metal, r1.code, r1.sheetId, r1.sheetName, r1.setId, r1.setName, r1.via, r1.by, r1.cutAt, r1.status, r1.revision], ['rose', 'RG', 'sheet-1', 'RG Sheet 1', 'set-2026-1', 'Set 1', 'nest', 'Pat Lee', one.cut.at, 'available', 1]);
  const g1 = leftover(JSON.parse(store.get(`Charm_Nest_Rose_Stock/${stockId}`).profileJson));
  assert.deepEqual([r1.rings, r1.areaMm2, r1.bboxMm, r1.sheetWMm, r1.sheetHMm], [g1.rings, g1.areaMm2, g1.bboxMm, g1.sheetWMm, g1.sheetHMm], 'the record holds the outline of the stock the cut left');
  assert.equal(store.get('Charm_Nest_Rev/remnants').n, 1, 'the counter moved with the record');
  const again = await api.roseRecordCut({ sheetId: 'sheet-1', stockId, revision: 0, planHash: one.cut.planHash }); assert(again.ok);
  assert.equal([...store.keys()].filter(k => k.startsWith('Charm_Nest_Remnants/')).length, 1, 'a repeated press saves one leftover, not two');
  const two = await cutOne('sheet-2', [shape('p2', 25, 2, 8, 25)], 2, { stockId, revision: 1 }, '', 'library');
  const r1b = store.get(`Charm_Nest_Remnants/${stockId}-1`), r2 = store.get(`Charm_Nest_Remnants/${two.stock.id}-2`);
  assert.deepEqual([r1b.status, r1b.usedBySheetId, r1b.usedBySheetName, r1b.marked], ['used', 'sheet-2', 'RG Sheet 2', false], 'the leftover the second cut was made on is used');
  assert.deepEqual([r2.status, r2.via, r2.by], ['available', 'library', ''], 'the new one is available; nobody signed in is saved as none');
  assert(r2.areaMm2 < r1.areaMm2 && r2.rings[0].length > r1.rings[0].length, 'the second leftover is smaller, with one more step in its edge');

  // 3. the list: one query, newest first, available only by default; unchanged from the counter alone; a person's mark
  const list = await rem.ops.remnantList({});
  assert.deepEqual(list.items.map(i => i.id), [`${stockId}-2`], 'available only'); assert.equal(list.rev, '2'); assert(list.items[0].rings.length);
  assert.deepEqual((await rem.ops.remnantList({ ifRev: list.rev })), { unchanged: true, rev: '2' }, 'nothing moved: one tiny read');
  assert.deepEqual((await rem.ops.remnantList({ scope: 'all' })).items.map(i => i.id), [`${stockId}-2`, `${stockId}-1`], 'all, newest first');
  const marked = await rem.ops.remnantMark({ id: `${stockId}-2`, status: 'discarded', by: 'Pat Lee' });
  assert.deepEqual([marked.item.status, marked.item.statusBy, marked.item.marked], ['discarded', 'Pat Lee', true]);
  assert.equal((await rem.ops.remnantList({ ifRev: list.rev })).unchanged, undefined, 'a mark moves the counter');
  await assert.rejects(() => rem.ops.remnantMark({ id: `${stockId}-1`, status: 'available' }), /stays used/, 'a leftover a later cut was made on stays used');
  assert.equal((await rem.ops.remnantMark({ id: `${stockId}-2`, status: 'available', by: 'Pat Lee' })).item.status, 'available', "a person's own mark can be taken back");
  await assert.rejects(() => rem.ops.remnantMark({ id: 'nope-nope', status: 'used' }), /not found/);
  // 4. the cuts made before leftovers were saved: the current leftover of each cut stock is saved once, create-only (never one already saved)
  assert.equal((await rem.ops.remnantList({})).needsBackfill, true, 'the first read says the earlier cuts are not saved yet');
  const old = Rose.plan([shape('z', 2, 2, 12, 40)], 100, 50, null, .2).profile, T = 1759660000000;
  store.set('Charm_Nest_Rose_Stock/rgs-old-1', { id: 'rgs-old-1', wPt: 100, hPt: 50, revision: 2, profileJson: JSON.stringify(old), available: true, owner: null, lastCutAt: T });
  store.set('Charm_Nest_Rose_Stock/rgs-old-1/cuts/sheet-old', { sheetId: 'sheet-old', revision: 2, by: 'Ana', at: T, fileBase: 'RG_2026-10-05_Sheet-1', planJson: '{"huge":1}' });
  store.set('Charm_Nest_Rose_Stock/rgs-old-1/cuts/sheet-older', { sheetId: 'sheet-older', revision: 1, by: 'Ben', at: T - 5, fileBase: 'RG_2026-10-04_Sheet-1' });
  store.set('Charm_Nest_Sheets/sheet-old', { id: 'sheet-old', metal: 'rose', sheetIndex: 1, setId: 'set-2026-10-05-1', fileBase: 'RG_2026-10-05_Sheet-1' });
  store.set('Charm_Nest_Rose_Stock/rgs-fresh-1', { id: 'rgs-fresh-1', wPt: 100, hPt: 50, revision: 0, profileJson: null });
  const before = JSON.stringify(store.get(`Charm_Nest_Remnants/${stockId}-2`));
  const bf = await rem.ops.remnantBackfill();
  assert.deepEqual([bf.ok, bf.created], [true, 1], 'one stock needed it: the cut one with no saved leftover');
  const old2 = store.get('Charm_Nest_Remnants/rgs-old-1-2'), og = leftover(old);
  assert.deepEqual([old2.status, old2.backfilled, old2.sheetName, old2.setName, old2.by, old2.cutAt, old2.revision, old2.areaMm2], ['available', true, 'RG Sheet 1', 'Set 1', 'Ana', T, 2, og.areaMm2]);
  assert.deepEqual(old2.rings, og.rings);
  assert.equal(JSON.stringify(store.get(`Charm_Nest_Remnants/${stockId}-2`)), before, 'a leftover already saved is never touched');
  assert.equal(store.get('Charm_Nest_Remnants/rgs-fresh-1-0'), undefined, 'a sheet never cut has no leftover to save');
  assert.deepEqual(await rem.ops.remnantBackfill(), { ok: true, already: true, created: 0 }, 'done once, never again');
  assert.equal((await rem.ops.remnantList({})).needsBackfill, undefined);
  assert((await rem.ops.remnantList({})).items.some(i => i.id === 'rgs-old-1-2'), 'and it is on the list');
  console.log('rose-leftover: ok (exact outline, area, bounding box, both axes, record in the cut transaction, used on the next cut, list, mark)');
})().catch(e => { console.error(e); process.exit(1); });
