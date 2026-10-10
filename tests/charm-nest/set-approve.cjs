/* A set advances as ONE (Paul, 5 Oct 2026, round 7): "you cannot have a green approved button on a single sheet that is part of a set
   where the other sheets are not ready yet and don't have the green Approve button engaged."
   Part 1: CharmNestReadiness.setGate, the one truth the Approve button, its grey reason, the server's refusal and the set-level wait row all
   read. Part 2: LibraryFlow and the server twin (a fake in-memory shop running the real charmNestLibrary handler). Part 3: the buttons on
   the Library's cards (jsdom, the real LaserReview). Pure or fake backends only: no live endpoint, no paid AI, no Etsy call. */
const assert = require('node:assert/strict');
const R = require('../../charm-nest-readiness.js');
require('../../charm-nest-set-rules.js').enforce(false);   // (this suite is about approval mechanics and its fixture sets predate the set principle, a completed GF and a completed SS sheet: that has its own suite, sets-form.cjs)

const sheet = (id, extra = {}) => ({ id, metal: 'gold', sheetIndex: 1, poolIds: [id + '1'], placedCount: 1, verification: { ok: true }, preview: 'p', outputs: { ai: 'f' }, label: { files: [] }, backPool: [], engraving: { [id + '1']: { needed: false, state: 'none', approved: true } }, updatedAt: 1, ...extra });
// n pieces to engrave, `done` of them approved
const engraving = (id, n, done, extra = {}) => sheet(id, { poolIds: Array.from({ length: n }, (_, i) => `${id}${i + 1}`), placedCount: n, engraving: Object.fromEntries(Array.from({ length: n }, (_, i) => [`${id}${i + 1}`, i < done ? { needed: true, state: 'approved', approved: true } : { needed: true, state: 'words', approved: false }])), ...extra });
const labelled = s => ({ ...s, label: { files: [{ path: 'qr', url: 'u', payload: 'o' }] } });
const set = (ids, extra = {}) => ({ setId: 'set-1', seq: 1, sheetIds: ids, ...extra });
const states = g => Object.fromEntries(g.sheets.map(x => [x.sheetId, x.state]));

// Paul's picture (image4/5): GF Sheet 1 only waits on its order check (soft), SS Sheet 1 has 7 of 25 back engravings approved
{
  const GF = sheet('GF', { metal: 'gold', orders: ['1'], orderReadiness: { 1: { ready: false, why: 'x' } } }), SS = engraving('SS', 25, 7, { metal: 'silver' });
  const g = R.setGate(set(['GF', 'SS']), [GF, SS]);
  assert.equal(g.ready, false, 'one sheet of the set is not ready: the set is not');
  assert.equal(g.blockers.length, 1); const b = g.blockers[0];
  assert.deepEqual([b.sheetId, b.sheetLabel, b.step, b.why, b.counter], ['SS', 'SS Sheet 1', 'engraving', 'back engravings 7 of 25', { done: 7, of: 25 }]);
  assert.equal(g.reason, 'SS Sheet 1 · back engravings 7 of 25', 'the plain line names the sheet and what it lacks');
  assert.deepEqual(states(g), { GF: 'open', SS: 'blocked' }, 'GF alone would have had a green button: its own soft items (the order check) never grey it');
  assert.deepEqual(g.toApprove, ['GF', 'SS']);
  assert.equal(g.single, false);
  // every sheet reads the same gate: nothing here depends on which sheet asks
  assert.equal(JSON.stringify(R.setGate(set(['SS', 'GF']), [SS, GF]).blockers), JSON.stringify(g.blockers));
}

// both ready to be approved: the set is, whatever soft items are still to come
{
  const GF = sheet('GF', { orders: ['1'], orderReadiness: { 1: { ready: false, why: 'x' } }, verification: { ok: false } }), SS = sheet('SS', { metal: 'silver' });
  const g = R.setGate(set(['GF', 'SS']), [GF, SS]);
  assert.equal(g.ready, true); assert.deepEqual(g.blockers, []); assert.equal(g.reason, '');
  assert.deepEqual(states(g), { GF: 'open', SS: 'open' }, 'a layout check, a QR label or an order still to come are the press\'s to list, never a grey button');
}

// "and N more": the first blocking sheet by the set's own order, the rest counted
{
  const A = engraving('A', 4, 1), B = sheet('B', { draft: true }), C = engraving('C', 3, 0, { metal: 'rose' });
  const g = R.setGate(set(['A', 'B', 'C']), [C, B, A]);
  assert.equal(g.ready, false); assert.equal(g.blockers.length, 3);
  assert.equal(g.blockers[0].sheetId, 'A', 'the set\'s order, not the order the sheets were handed over');
  assert.equal(g.reason, 'GF Sheet 1 · back engravings 1 of 4, and 2 more');
  assert.equal(R.setGate(set(['A', 'B']), [A, B]).reason, 'GF Sheet 1 · back engravings 1 of 4, and 1 more');
  assert.equal(g.blockers[1].why, 'still a draft');
  assert.equal(R.setGate(set(['B']), [B]).reason, 'GF Draft 1 · still a draft');
}

// the hard test is the lone button's test: laying out, not in the set, back engravings. Nothing else.
{
  for (const [mutate, step, why] of [
    [s => ({ ...s, dirty: true }), 'nesting', 'still being laid out'], [s => ({ ...s, saving: true }), 'nesting', 'still being laid out'],
    [s => ({ ...s, status: 'nesting' }), 'nesting', 'still being laid out'], [s => ({ ...s, status: 'queued' }), 'nesting', 'still being laid out'], [s => ({ ...s, status: 'error' }), 'nesting', 'still being laid out'],
    [s => ({ ...s, draft: true }), 'nesting', 'still a draft'], [s => ({ ...s, solidIncluded: false }), 'nesting', 'not included in a set yet']]) {
    const g = R.setGate(set(['A', 'B']), [mutate(sheet('A')), sheet('B', { metal: 'silver' })]);
    assert.equal(g.ready, false, why); assert.deepEqual([g.blockers[0].step, g.blockers[0].why], [step, why]); assert.equal(g.blockers[0].counter, null);
  }
  for (const mutate of [s => ({ ...s, verification: { ok: false } }), s => ({ ...s, verification: null }), s => ({ ...s, outputs: {} }), s => ({ ...s, preview: null }), s => ({ ...s, label: { files: [] } }), s => ({ ...s, orders: ['9'], orderReadiness: { 9: { ready: false, why: 'x' } } }),
    s => ({ ...s, backPool: [], poolIds: ['A1'], engraving: { A1: { needed: true, state: 'approved', approved: true } } })]) {
    assert.equal(R.setGate(set(['A', 'B']), [mutate(sheet('A')), sheet('B')]).ready, true, 'soft: ' + JSON.stringify(mutate(sheet('A'))).slice(0, 60));
  }
  // an unplaced piece with no identity is an unknown engraving: it waits
  const g = R.setGate(set(['A', 'B']), [sheet('A', { placedCount: 3 }), sheet('B')]);
  assert.equal(g.ready, false); assert.equal(g.blockers[0].why, 'back engravings 0 of 2', 'the plain piece needs none; the two unplaced-identity pieces wait');
}

// sheets already past: completed (cut) and already fully ready ones never block, a set with some past uses the sheets still to approve
{
  const cut = labelled(sheet('A', { laserDoneAt: 5 })), ready = labelled(sheet('B', { metal: 'silver' })), open = sheet('C', { metal: 'rose' });
  let g = R.setGate(set(['A', 'B', 'C']), [cut, ready, open]);
  assert.equal(g.ready, true); assert.deepEqual(states(g), { A: 'past', B: 'ready', C: 'open' }); assert.deepEqual(g.toApprove, ['B', 'C'], 'a completed sheet has no button: the sheets still to approve');
  g = R.setGate(set(['A', 'B', 'C']), [cut, ready, engraving('C', 2, 0, { metal: 'rose' })]);
  assert.equal(g.ready, false); assert.deepEqual(g.blockers.map(x => x.sheetId), ['C']); assert.equal(states(g).B, 'ready', 'a ready sheet is blocked by its mate, it does not block');
  // a cut sheet that was later reopened passed Laser cutting once: its approval stays
  const reopened = engraving('A', 2, 0, { processSeals: [{ how: 'laserDone', at: 7 }] });
  assert.equal(R.setGate(set(['A', 'B']), [reopened, sheet('B')]).ready, true);
  // every sheet cut or ready: the set has nothing left to approve, and is ready
  g = R.setGate(set(['A', 'B']), [cut, ready]); assert.equal(g.ready, true); assert.deepEqual(g.toApprove, ['B']);
  assert.deepEqual(R.setGate(set(['A']), [cut]).toApprove, []);
}

// a person's hold is no membership problem (the press lifts it); a sheet of the set that is gone blocks
{
  const held = sheet('A', { laserHold: { at: 3, by: 'Paul' } });
  assert.equal(R.setGate(set(['A', 'B']), [held, sheet('B')]).ready, true, 'a held sheet is pressable: Approve releases it');
  const g = R.setGate(set(['A', 'B']), [engraving('A', 2, 0, { laserHold: { at: 3, by: 'Paul' } }), sheet('B')]); assert.equal(g.ready, false); assert.equal(g.blockers[0].why, 'back engravings 0 of 2');
  const gone = R.setGate(set(['A', 'B']), [sheet('A')]); assert.equal(gone.ready, false); assert.equal(gone.reason, 'A sheet of this set · cannot be found'); assert.deepEqual(gone.toApprove, ['A']);
  const archived = R.setGate(set(['A', 'B']), [sheet('A'), sheet('B', { archived: true })]); assert.equal(archived.ready, false, 'a removed sheet still listed in its set');
  assert.equal(R.setGate(set([]), []).ready, false, 'no sheets: nothing to approve');
}

// a set of one sheet, and a loose sheet, are their own gate
{
  const g = R.setGate(set(['A']), [engraving('A', 5, 4)]);
  assert.equal(g.single, true); assert.equal(g.ready, false); assert.equal(g.reason, 'GF Sheet 1 · back engravings 4 of 5');
  assert.equal(R.setGate(null, [sheet('A')]).ready, true); assert.equal(R.setGate(null, [sheet('A')]).single, true);
  assert.equal(R.approveBlock(sheet('A')), null); assert.equal(R.approveBlock(labelled(sheet('A'))), null, 'a ready sheet has nothing to approve');
}

// the 10K/14K per-sheet Include and Rose Gold keep their rules: an excluded solid sheet blocks, a Rose Gold sheet is judged by its own hard needs only
{
  const solid = sheet('K', { metal: 'gold14k', solidIncluded: false });
  const g = R.setGate(set(['A', 'K']), [sheet('A'), solid]); assert.equal(g.ready, false); assert.equal(g.reason, '14K Draft 1 · not included in a set yet');
  const rose = sheet('R', { metal: 'rose', roseStockId: 'stock-1' });                              // no green line yet: the Cut Sheet / its own yes, never a grey button
  assert.equal(R.setGate(set(['A', 'R']), [sheet('A'), rose]).ready, true);
  const roseWait = engraving('R', 3, 1, { metal: 'rose', roseStockId: 'stock-1' });
  assert.equal(R.setGate(set(['A', 'R']), [sheet('A'), roseWait]).reason, 'RG Sheet 1 · back engravings 1 of 3');
}

const parts = [];   // (the server and flow part, then the page part, run in order below)

// ── Part 2: LibraryFlow and the server twin, over the in-memory shop running the real charmNestLibrary handler ──
parts.push(async () => {
  const { start } = require('./bridge-server.cjs'), LF = require('../../charm-nest-flow.js');
  const S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUN = 'Charm_Nest_Runs';
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-03';
  const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
  const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
  const lines = {}; let n = 0;
  // one sheet of one order. engrave: 'plain' (nothing to engrave) | 'blocked' (a back engraving waits for approval) | 'done' (approved and saved)
  const mk = (id, o = {}) => {
    const order = String(3900000000 + ++n), pool = order + '_1_1', engrave = o.engrave || 'plain';
    lines[order + '_1'] = { orderId: order, state: 'written', quantity: 1, poolIds: [pool], ...(engrave === 'plain' ? { engraveCandidate: false } : engrave === 'blocked' ? { engrave: { needed: true, state: 'review', approved: false } } : { engrave: { needed: true, state: 'written', approved: true } }) };
    const back = engrave === 'done' ? { backPool: [{ poolId: pool, sheetId: id, approvedAt: 5, approvedBy: 'Maria', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: id + '-back.ai', url: image } } }] } : {};
    const { engrave: _e, noLabel, metal, ...rest } = o;
    st.put(S, id, { id, runId: 'run-s', metal: metal || 'gold', day, status: 'complete', placedCount: 1, charmCount: 1, density: .7, stock: { wIn: 6, hIn: 4.5 }, poolIds: [pool], orders: [order], verification: { ok: true },
      outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } },
      ...(noLabel ? {} : { label: { files: [{ path: id + '-qr.png', url: image, payload: order, orders: [order] }], orders: [order] } }), sheetIndex: 1, updatedAt: ts, createdAt: ts, ...back, ...rest });
    return { order, pool };
  };
  const finish = id => { const d = st.doc(S, id), pool = d.poolIds[0], order = d.orders[0]; lines[order + '_1'].engrave = { needed: true, state: 'written', approved: true }; delete lines[order + '_1'].engraveCandidate;
    st.put(S, id, { backPool: [{ poolId: pool, sheetId: id, approvedAt: 5, approvedBy: 'Maria', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: id + '-back.ai', url: image } } }] }); st.put(RUN, 'run-s', { lines }); };
  const mkSet = (setId, sheetIds, extra = {}) => st.put(SET, setId, { setId, seq: +setId.replace(/\D/g, '') || 1, day, runId: 'run-s', sheetIds, materials: ['gold'], orders: {}, status: 'labelled', updatedAt: ts, createdAt: ts, ...extra });
  const held = id => !!(st.doc(S, id).laserHold && st.doc(S, id).laserHold.at);
  const seals = id => (st.doc(S, id).processSeals || []).map(x => x.how + ':' + x.by);
  const writes = () => st.calls.filter(c => c.op === 'flowApply').length;

  // Set 1 is Paul's picture: GF Sheet 1 is fine, SS Sheet 1 still has a back engraving to approve
  mkSet('set-1', ['gf-1', 'ss-1']); mk('gf-1', { setId: 'set-1', setSeq: 1 }); mk('ss-1', { setId: 'set-1', setSeq: 1, metal: 'silver', engrave: 'blocked', noLabel: true });
  // a set of ONE sheet whose engraving is still to approve, held by a person
  mkSet('set-2', ['solo-1']); mk('solo-1', { setId: 'set-2', setSeq: 2, engrave: 'blocked', laserHold: { at: now - 500, by: 'Paul' } });
  // a Rose Gold sheet that has no green line yet, in a set with a gold sheet
  mkSet('set-3', ['c-gf', 'c-rg']); mk('c-gf', { setId: 'set-3', setSeq: 3 }); mk('c-rg', { setId: 'set-3', setSeq: 3, metal: 'rose', roseStockId: 'stock-1' });
  // the same with the Rose Gold sheet's own back engraving still to approve
  mkSet('set-4', ['e-gf', 'e-rg']); mk('e-gf', { setId: 'set-4', setSeq: 4 }); mk('e-rg', { setId: 'set-4', setSeq: 4, metal: 'rose', roseStockId: 'stock-1', rosePlanHash: 'h', engrave: 'blocked' });
  // a 14K solid sheet left out of its set by its own Include switch
  mkSet('set-5', ['k-gf', 'k-14']); mk('k-gf', { setId: 'set-5', setSeq: 5 }); mk('k-14', { setId: 'set-5', setSeq: 5, metal: 'gold14k', solidIncluded: false });
  // a set that has a cut sheet and two still to approve
  mkSet('set-6', ['p-cut', 'p-gf', 'p-ss']); mk('p-cut', { setId: 'set-6', setSeq: 6, laserDoneAt: now - 9000, processSeals: [{ id: 'l', how: 'laserReady', at: 1, by: 'A' }, { id: 'd', how: 'laserDone', at: now - 9000, by: 'A' }] }); mk('p-gf', { setId: 'set-6', setSeq: 6 }); mk('p-ss', { setId: 'set-6', setSeq: 6, metal: 'silver', noLabel: true });
  st.put(RUN, 'run-s', { runId: 'run-s', status: 'complete', lines });

  const labels = [], roseCalls = [];
  LF.configure({ api: post, employee: () => 'Paul', rows: () => [], remakeLabel: async id => { labels.push(id); const rec = st.doc(S, id); st.put(S, id, { label: { files: [{ path: id + '-qr.png', url: image, payload: rec.orders[0], orders: rec.orders }], orders: rec.orders } }); },
    rose: () => ({ check: async () => ({ needsLine: true, sheets: [{ sheetId: 'c-rg', label: 'RG Sheet 1', needsLine: true, source: 'live', blocked: '' }], confirm: { key: 'roseLine', label: 'Add the green dash line to RG Sheet 1?', detail: 'This calculates the cut contour for these charms' } }), calculate: async item => { roseCalls.push('calculate:' + item.id); return { ok: true, lines: 1, sheets: [] }; } }) });
  try {
    // 1. the server says the same as the page: the set is not ready to be approved, and why
    let r = await post({ op: 'flowState', sheetIds: ['gf-1'], setIds: ['set-1'] });
    assert.equal(r.status, 200); const g = r.gates['set-1'];
    assert.equal(g.ready, false); assert.equal(g.reason, 'SS Sheet 1 · back engravings 0 of 1'); assert.deepEqual(g.blockers.map(b => [b.sheetId, b.step]), [['ss-1', 'engraving']]);
    assert.equal(r.gates['set-2'], undefined, 'a set of one sheet has no set gate: it is its own sheet');

    // 2. a move or an approval of ONE sheet of that set is refused with that reason, and does nothing at all
    const before = JSON.stringify([st.doc(S, 'gf-1'), st.doc(S, 'ss-1'), st.doc(SET, 'set-1')]), calls0 = st.calls.length;
    for (const item of [{ kind: 'sheet', id: 'gf-1' }, { kind: 'sheet', id: 'ss-1' }, { kind: 'set', id: 'set-1' }]) {
      const p = await LF.plan({ ...item, to: { area: 'laser' } });
      assert.equal(p.ok, false, item.id); assert.equal(p.needs[0].key, 'setGate', 'the set\'s reason comes first: ' + JSON.stringify(p.needs.map(x => x.key)));
      assert.equal(p.needs[0].label, 'SS Sheet 1 · back engravings 0 of 1', 'the same words for every sheet of the set and for the set'); assert.deepEqual(p.needs[0].items.map(i => i.id), ['ss-1']);
      assert.equal(p.steps.length, 0, 'no step: nothing of a refused set is half done'); assert.deepEqual(p.auto, []); assert.deepEqual(p.confirm, []); assert(!p.needs.some(x => x.key === 'members'), 'one wait, said once');
      const c = await LF.commit(p, { by: 'Paul' }); assert.equal(c.ok, false); assert(/SS Sheet 1 · back engravings 0 of 1/.test(c.error), c.error);
      const a = await LF.approve({ ...item, by: 'Paul' }); assert.equal(a.approved, false); assert.equal(a.needs[0].key, 'setGate'); assert.deepEqual(a.applied, []);
    }
    assert.equal(JSON.stringify([st.doc(S, 'gf-1'), st.doc(S, 'ss-1'), st.doc(SET, 'set-1')]), before, 'nothing was written');
    assert.equal(writes(), 0); assert.deepEqual(labels, []); assert(!st.calls.slice(calls0).some(c => c.op === 'flowApply' || c.op === 'laserDone'));

    // 3. a stale page: its own release or seal for one sheet of that set is refused by the server, and writes nothing
    r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'hold', sheetIds: ['gf-1'], note: 'x' }] }); assert.equal(r.status, 200, 'a hold goes the other way: never refused'); assert(held('gf-1'));
    const sealsBefore = seals('gf-1').length, stale = [
      [{ type: 'release', sheetIds: ['gf-1'] }], [{ type: 'seal', kind: 'sheet', id: 'gf-1' }], [{ type: 'seal', kind: 'set', id: 'set-1' }], [{ type: 'release', sheetIds: ['gf-1', 'ss-1'] }, { type: 'seal', kind: 'set', id: 'set-1' }]];
    for (const steps of stale) {
      r = await post({ op: 'flowApply', by: 'Paul', steps }); assert.equal(r.status, 409, JSON.stringify(steps));
      assert(/approved together/.test(r.error) && /SS Sheet 1 · back engravings 0 of 1/.test(r.error), r.error); assert.equal(r.setId, 'set-1'); assert.deepEqual(r.blockers.map(b => b.sheetId), ['ss-1']);
      assert(held('gf-1'), 'the hold stays'); assert.equal(seals('gf-1').length, sealsBefore); assert.equal(seals('ss-1').length, 0); assert(!st.doc(SET, 'set-1').processReady);
    }
    // what only restores what a failed move changed is never the approval it refuses
    r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'release', sheetIds: ['gf-1'], restore: true }] }); assert.equal(r.status, 200); assert(!held('gf-1'));
    // the page's own plan to move a held sheet of the set is refused before it writes (the page reads the same gate)
    await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'hold', sheetIds: ['gf-1'] }] });
    const refusedMove = await LF.commit(await LF.plan({ kind: 'sheet', id: 'gf-1', to: { area: 'laser' } }), { by: 'Paul' }); assert.equal(refusedMove.ok, false); assert(held('gf-1'), 'still held');

    // 4. every sheet of the set ready to be approved: ONE action approves the whole set, each sheet with its own seals
    finish('ss-1');
    r = await post({ op: 'flowState', sheetIds: ['gf-1'], setIds: ['set-1'] }); assert.equal(r.gates['set-1'].ready, true); assert.equal(r.gates['set-1'].reason, '');
    let p = await LF.plan({ kind: 'sheet', id: 'gf-1', to: { area: 'laser' } });
    assert.equal(p.ok, true, JSON.stringify(p.needs)); assert(!p.needs.length);
    assert(p.notes.some(x => /GF Sheet 1 is approved with Set 1: its sheets move together/.test(x)), p.notes.join('|'));
    assert.deepEqual(p.steps.filter(x => x.type === 'release').flatMap(x => x.sheetIds), ['gf-1'], 'the held sheet is released'); assert.deepEqual(p.steps.filter(x => x.type === 'qrLabel').map(x => x.sheetId), ['ss-1'], 'and the sheet that lacks its label gets it');
    assert.equal(p.steps.filter(x => x.type === 'seal').length, 1, 'one seal for the set'); assert.deepEqual(p.steps.filter(x => x.type === 'seal').map(x => [x.kind, x.id]), [['set', 'set-1']]);
    const a = await LF.approve({ kind: 'sheet', id: 'gf-1', by: 'Paul' });
    assert.equal(a.approved, true, JSON.stringify(a.needs) + a.error); assert.deepEqual(labels, ['ss-1']); assert(!held('gf-1'));
    assert(seals('gf-1').includes('laserReady:Paul') && seals('ss-1').includes('laserReady:Paul') && (st.doc(SET, 'set-1').processSeals || []).some(x => x.how === 'laserReady' && x.by === 'Paul'), 'each sheet has its own seal, and the set has its own: ' + seals('gf-1') + ' ' + seals('ss-1'));
    assert.equal((await LF.plan({ kind: 'set', id: 'set-1', to: { area: 'nowhere' } })).from.area, 'laser', 'the whole set reached Laser cutting together');
    const again = await LF.approve({ kind: 'set', id: 'set-1', by: 'Paul' }); assert.equal(again.approved, true); assert.deepEqual(again.applied, [], 'a second press finds everything done'); assert.deepEqual(labels, ['ss-1']);

    // 5. a set of one sheet is exactly as before: its own hard needs are listed, no set gate, the server does not refuse
    p = await LF.plan({ kind: 'sheet', id: 'solo-1', to: { area: 'laser' } });
    assert(p.needs.some(x => x.key === 'engraving') && !p.needs.some(x => x.key === 'setGate'), JSON.stringify(p.needs.map(x => x.key))); assert.deepEqual(p.steps.filter(x => x.type === 'release').flatMap(x => x.sheetIds), ['solo-1'], 'the safe steps are as they were');
    r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'release', sheetIds: ['solo-1'] }] }); assert.equal(r.status, 200, 'a set of one is not refused'); assert(!held('solo-1'));

    // 6. Rose Gold: its own hard needs count, its green line and its Cut Sheet are its own yes, never taken by an approval
    r = await post({ op: 'flowState', sheetIds: ['c-gf'], setIds: ['set-3', 'set-4'] }); assert.equal(r.gates['set-3'].ready, true, 'a Rose Gold sheet without its green line is soft: its own yes'); assert.equal(r.gates['set-4'].reason, 'RG Sheet 1 · back engravings 0 of 1');
    p = await LF.plan({ kind: 'sheet', id: 'e-gf', to: { area: 'laser' } }); assert.equal(p.needs[0].label, 'RG Sheet 1 · back engravings 0 of 1'); assert.equal(p.steps.length, 0);
    r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'seal', kind: 'set', id: 'set-4' }] }); assert.equal(r.status, 409); assert(/RG Sheet 1/.test(r.error));
    const ap = await LF.approve({ kind: 'sheet', id: 'c-gf', by: 'Paul' });
    assert.equal(ap.approved, false); assert.deepEqual(ap.confirm.map(c => c.key), ['roseLine'], 'the green line stays a yes the person gives'); assert(!roseCalls.length, 'nothing was calculated'); assert.equal(st.doc(S, 'c-rg').rosePlanHash, undefined); assert(!st.doc(S, 'c-rg').roseCutAt, 'no cut recorded by an approval');

    // 7. 10K / 14K: a sheet left out of its set by its own Include switch holds the set, and the server says so
    r = await post({ op: 'flowState', sheetIds: ['k-gf'], setIds: ['set-5'] }); assert.equal(r.gates['set-5'].reason, '14K Draft 1 · not included in a set yet');
    r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'seal', kind: 'sheet', id: 'k-gf' }] }); assert.equal(r.status, 409); assert(/14K Draft 1 · not included in a set yet/.test(r.error));
    p = await LF.plan({ kind: 'sheet', id: 'k-gf', to: { area: 'laser' } }); assert.equal(p.needs[0].key, 'setGate');

    // 8. a set with a cut sheet: the sheets still to approve are the gate
    r = await post({ op: 'flowState', sheetIds: ['p-gf'], setIds: ['set-6'] }); assert.equal(r.gates['set-6'].ready, true); assert.deepEqual(r.gates['set-6'].toApprove.sort(), ['p-gf', 'p-ss']);
    st.put(S, 'p-gf', { dirty: true, status: 'nesting' });
    r = await post({ op: 'flowState', sheetIds: ['p-gf'], setIds: ['set-6'] }); assert.equal(r.gates['set-6'].reason, 'GF Sheet 1 · still being laid out', 'a sheet not nested yet holds the set');
    r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'seal', kind: 'set', id: 'set-6' }] }); assert.equal(r.status, 409);
    console.log('Set approve OK (part 2): refused as one set on the page plan and on the server, a stale page cannot release or seal half a set, one press approves both sheets with their own seals, one-sheet sets, Rose Gold and 14K as before');
  } finally { srv.close(); LF.configure({ api: null, remakeLabel: null, rose: () => null }); }
});

// ── Part 3: the buttons on the Library's cards (the real LaserReview over a fake LibraryFlow and a fake cloud) ──
parts.push(async () => {
  const fs = require('node:fs'), vm = require('node:vm'), { JSDOM } = require('jsdom');
  const src = fs.readFileSync('charm-nest-bridge.js', 'utf8');
  const dom = new JSDOM('<body><main id="libBody"></main></body>', { runScripts: 'outside-only', pretendToBeVisual: true }), win = dom.window, document = win.document, frames = [], requests = [], calls = [], shows = [], opened = [];
  win.matchMedia = () => ({ matches: true });
  win.eval(fs.readFileSync('charm-nest-motion.js', 'utf8'));
  win.eval(fs.readFileSync('charm-nest-readiness.js', 'utf8'));
  let world = {};   // what the cloud answers to a laserStatus read: every record, as it is now
  const esc = x => String(x == null ? '' : x).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;');
  const c = vm.createContext({ window: win, document, console, esc, cors: x => x, S: { mode: 'library', cloud: { ok: true } }, allSheets: () => [], Orders: { rows: () => [] }, Engrave: { items: () => new Map() }, O: { setLabel: n => 'Set ' + n }, CNListActivity: { compare: () => 0, compareBlocks: () => 0, state: () => ({ direction: 1 }) }, innerHeight: win.innerHeight,
    requestAnimationFrame: fn => (frames.push(fn), frames.length), setTimeout: win.setTimeout.bind(win), clearTimeout: win.clearTimeout.bind(win), setInterval() {}, api: async (name, payload) => { requests.push({ name, payload }); return { sheets: Object.values(world), sets: [] }; } });
  vm.runInContext(src.slice(src.indexOf('const LaserReview ='), src.indexOf('const Sets =')), c);
  const L = win.LaserReview, body = document.querySelector('#libBody');
  const flush = () => { for (let i = 0; i < 8 && frames.length; i++) frames.splice(0).forEach(f => f()); };
  const tick = (ms = 15) => new Promise(r => setTimeout(r, ms));
  const rect = card => { card.getBoundingClientRect = () => ({ left: 10, top: 80, right: 310, bottom: 430, width: 300, height: 350 }); return card; };
  const setCard = (setId, ids, extra = {}) => { const a = document.createElement('div'); a.className = 'setCard'; a.dataset.laserCard = 'set'; a._laserSet = { setId, seq: 1, sheetIds: ids, ...extra }; a._laserSheets = ids;
    a.innerHTML = `<div class="sh"><span class="nm" data-set-title></span></div><div class="sheetsRow">${ids.map(id => `<article class="librarySheet"><div class="libCard" data-id="${id}"><span data-sheet-status="${id}"></span></div></article>`).join('')}</div>`; return rect(a); };
  const area = card => card.closest('[data-laser-area]').dataset.laserArea;
  // a set has ONE button (key set:<setId>), at the bottom of its card; a sheet that is part of no set has its own (key sheet:<id>)
  const boxOf = (root, key) => root.querySelector(`.approveBox[data-approve-for="${key}"]`);
  const btnOf = (root, key) => boxOf(root, key).querySelector('[data-approve-btn]'), whyOf = (root, key) => boxOf(root, key).querySelector('[data-approve-why]');
  const off = b => b.getAttribute('aria-disabled') === 'true';
  const place = (card, ids) => { L.place(card, L.group(card._laserSet, ids.map(id => world[id])).ready, body); };
  // the records
  const sheet2 = (id, extra = {}) => ({ id, metal: 'gold', sheetIndex: 1, poolIds: [id + '1'], placedCount: 1, verification: { ok: true }, preview: 'p', outputs: { ai: 'f' }, label: { files: [] }, backPool: [], engraving: { [id + '1']: { needed: false, state: 'none', approved: true } }, updatedAt: 1, ...extra });
  const labelled2 = s => ({ ...s, label: { files: [{ path: 'qr', url: 'u', payload: 'o' }] }, updatedAt: (s.updatedAt || 0) + 1 });
  const back2 = (id, pid) => ({ poolId: pid, sheetId: id, approvedAt: 10, approvedBy: 'Maria', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: pid + '.ai', url: 'https://example.com/' + pid } } });
  // n pieces to engrave, `done` approved (and saved)
  const eng = (id, n, done, extra = {}) => { const ids = Array.from({ length: n }, (_, i) => `${id}${i + 1}`); return sheet2(id, { poolIds: ids, placedCount: n, engraving: Object.fromEntries(ids.map((p, i) => [p, i < done ? { needed: true, state: 'approved', approved: true } : { needed: true, state: 'words', approved: false }])), backPool: ids.slice(0, done).map(p => back2(id, p)), ...extra }); };
  const put = (...rs) => { for (const r of rs) { world[r.id] = r; L.record(r); } };
  const bump = (r, extra = {}) => ({ ...r, ...extra, updatedAt: (r.updatedAt || 0) + 1 });

  L.sections(body);
  win.LibraryFlow = { approve: o => { calls.push({ ...o }); return new Promise(r => { win._release = () => r({ ok: true, auto: [{ key: 'seal', label: 'Ready seal recorded' }], needs: [], confirm: [], notes: [] }); }); } };
  win.LibraryApprovalUI = { show: (host, plan, opts) => shows.push({ host, plan, opts }), hide() {} };
  win.CNEmployee = { name: () => 'Maria' };
  win.LaserReview.openChecklist = (card, info) => opened.push({ card, ...info });

  // ── A. Paul's picture (image 4 and 5): GF Sheet 1 is ready, SS Sheet 1 has 7 of 25 backs approved: the set's ONE button is grey and says why ──
  put(labelled2(sheet2('GF')), eng('SS', 25, 7, { metal: 'silver' }));
  const card = setCard('set1', ['GF', 'SS']); place(card, ['GF', 'SS']);
  L.changed(); flush();
  assert.equal(area(card), 'pending');
  const REASON = 'SS Sheet 1 · back engravings 7 of 25', K1 = 'set:set1';
  assert.deepEqual([...card.querySelectorAll('.approveBox')].map(b => b.dataset.approveFor), [K1], 'ONE button for the set, and none on GF Sheet 1 or SS Sheet 1');
  assert.equal(card.lastElementChild, boxOf(card, K1), 'at the bottom of the set card, under the sheet columns'); assert.equal(card.querySelectorAll('.librarySheet .approveBox').length, 0);
  assert.equal(off(btnOf(card, K1)), true, 'grey: a single sheet cannot advance without the rest of its set');
  assert.equal(whyOf(card, K1).textContent, REASON, 'it says which sheet is not ready and what it lacks, in one plain line');
  assert.match(btnOf(card, K1).getAttribute('aria-label'), /not yet: SS Sheet 1 · back engravings 7 of 25/); assert.equal(btnOf(card, K1).getAttribute('aria-describedby'), whyOf(card, K1).id, 'the reason is read with the button');
  assert.equal(btnOf(card, K1).disabled, false, 'aria-disabled, not disabled: the keyboard stays on it');
  assert(!/\b(bad|warn|err|red)\b/i.test(whyOf(card, K1).innerHTML), 'no red text: the reason is a quiet line'); assert.equal(whyOf(card, K1).querySelectorAll('[title]').length, 0, 'no tooltip');
  assert.equal(boxOf(card, K1).dataset.mode, 'blocked');
  btnOf(card, K1).click(); assert.equal(calls.length, 0, 'a grey button presses nothing');
  assert.equal(whyOf(card, K1).querySelector('[data-approve-reason]').textContent, REASON, 'the line is a link while the blocking sheet has its \'!\'');
  whyOf(card, K1).querySelector('[data-approve-reason]').click(); assert.deepEqual(opened.map(o => [o.kind, o.id]), [['sheet', 'SS']], 'the reason opens SS Sheet 1\'s panel: the sheet that holds the set back');

  // ── B. the live read (about every 3 s) turns it green and grey in ONE pass, the same button and the same place ──
  const watch = () => { const batches = []; const mo = new win.MutationObserver(l => batches.push([...new Set(l.map(m => m.target.closest('[data-approve-for]').dataset.approveFor))].sort())); mo.observe(card, { attributes: true, attributeFilter: ['aria-disabled'], subtree: true }); return { batches, stop: () => mo.disconnect() }; };
  const cloudSays = async () => { L.nudge(); await tick(650); flush(); await tick(2); };      // (the live read: a person's change asks for a read at once)
  btnOf(card, K1).focus(); assert.equal(document.activeElement, btnOf(card, K1));
  const node = boxOf(card, K1); let w = watch(); const asked = requests.length;
  world.SS = bump(eng('SS', 25, 25, { metal: 'silver' }), { updatedAt: 5 });                    // the cloud now says: every back is approved and saved (its QR label is still the press's own step)
  await cloudSays(); w.stop();
  assert(requests.length > asked, 'the cards were read again');
  assert.deepEqual(w.batches, [[K1]], 'the one button changed, once: ' + JSON.stringify(w.batches));
  assert.equal(boxOf(card, K1), node, 'the same button, not drawn again'); assert.equal(off(btnOf(card, K1)), false, 'green'); assert.equal(whyOf(card, K1).textContent, ''); assert.equal(boxOf(card, K1).dataset.mode, 'ready');
  assert.equal(area(card), 'pending', 'the set is still In progress until it is approved');
  assert.equal(document.activeElement, btnOf(card, K1), 'the keyboard stayed on the button through the change');

  // grey again (a back changed on another computer)
  w = watch();
  world.SS = bump(eng('SS', 25, 24, { metal: 'silver' }), { updatedAt: 9 }); await cloudSays(); w.stop();
  assert.deepEqual(w.batches, [[K1]], 'grey, in one pass: ' + JSON.stringify(w.batches));
  assert.equal(off(btnOf(card, K1)), true); assert.equal(whyOf(card, K1).textContent, 'SS Sheet 1 · back engravings 24 of 25'); assert.equal(boxOf(card, K1), node);
  assert.equal(document.activeElement, btnOf(card, K1), 'the keyboard stayed on the button that turned grey (aria-disabled, not disabled)');
  w = watch(); await cloudSays(); w.stop(); assert.deepEqual(w.batches, [], 'a read that says nothing new redraws nothing: no flicker');

  // ── C. green: ONE press approves the whole set once ──
  world.SS = bump(eng('SS', 25, 25, { metal: 'silver' }), { updatedAt: 12 }); await cloudSays();
  assert.equal(off(btnOf(card, K1)), false);
  btnOf(card, K1).click(); btnOf(card, K1).click(); btnOf(card, K1).click();
  assert.equal(calls.length, 1, 'one action for the set, however many times it was pressed'); assert.deepEqual(calls[0], { kind: 'set', id: 'set1', by: 'Maria' });
  assert.equal(boxOf(card, K1).dataset.mode, 'busy', 'the button shows the one running approval'); assert.match(btnOf(card, K1).textContent, /Approving/); assert(btnOf(card, K1).querySelector('.spin'), 'a small spinner with its word'); assert.equal(off(btnOf(card, K1)), true);
  L.changed(); flush(); btnOf(card, K1).click(); assert.equal(calls.length, 1, 'still one, even through a redraw');
  world.SS = labelled2(world.SS); win._release(); await tick(); flush();                           // (the press made the QR label and sealed the set: both sheets are ready)
  assert.equal(shows.length, 1, 'one Moving bar'); assert.equal(shows[0].host, card, 'on the set\'s card'); assert.equal(shows[0].opts.title, 'Approve for laser cutting'); assert.equal(shows[0].opts.where, 'end', 'under the one button');
  await cloudSays();
  assert.equal(area(card), 'ready', 'the whole set reached Laser cutting together');
  assert.equal(card.querySelectorAll('.approveBox').length === 0 || boxOf(card, K1).dataset.mode !== 'ready', true, 'no green button is left on a set in Laser cutting');

  // ── D. a set with a completed sheet: the set's one button, the cut sheet has none ──
  const card2 = setCard('set2', ['A', 'B', 'C']), K2 = 'set:set2';
  put(labelled2(sheet2('A', { laserDoneAt: 5 })), eng('B', 3, 3, { metal: 'silver' }), eng('C', 4, 1, { metal: 'rose' })); place(card2, ['A', 'B', 'C']); L.changed(); flush();
  assert.equal(area(card2), 'pending'); assert.deepEqual([...card2.querySelectorAll('.approveBox')].map(b => b.dataset.approveFor), [K2], 'a completed sheet has no button, nor has any other sheet of the set');
  assert.equal(off(btnOf(card2, K2)), true); assert.equal(whyOf(card2, K2).textContent, 'RG Sheet 1 · back engravings 1 of 4');
  // the Rose Gold sheet's own approvals count; once they are in, its green line is its own yes and never greys the button
  put(bump(eng('C', 4, 4, { metal: 'rose', roseStockId: 'stock-1' }))); L.changed(); flush();
  assert.equal(off(btnOf(card2, K2)), false, 'green: a Rose Gold sheet without its line is soft');
  // "and N more": several sheets that are not ready (F is a draft: it is no part of the set, it keeps a button of its own)
  const card3 = setCard('set3', ['D', 'E', 'F']), K3 = 'set:set3';
  put(eng('D', 4, 1), eng('E', 2, 0, { metal: 'silver' }), sheet2('F', { metal: 'rose', draft: true })); place(card3, ['D', 'E', 'F']); L.changed(); flush();
  assert.equal(whyOf(card3, K3).textContent, 'GF Sheet 1 · back engravings 1 of 4, and 2 more'); assert.deepEqual([...card3.querySelectorAll('.approveBox')].map(b => b.dataset.approveFor).sort(), ['set:set3', 'sheet:F'], 'only the draft that is no part of the set has its own');
  // a 14K sheet left out of its set by its own Include switch holds the others back
  const card4 = setCard('set4', ['G', 'H']), K4 = 'set:set4';
  put(sheet2('G'), sheet2('H', { metal: 'gold14k', solidIncluded: false })); place(card4, ['G', 'H']); L.changed(); flush();
  assert.equal(off(btnOf(card4, K4)), true); assert.equal(whyOf(card4, K4).textContent, '14K Sheet 1 · not included in a set yet');
  // a sheet still being laid out, and one a person holds (the press lifts the hold: it is no reason to be grey)
  put(bump(world.H, { solidIncluded: true }), bump(world.G, { status: 'nesting' })); L.changed(); flush();
  assert.equal(whyOf(card4, K4).textContent, 'GF Sheet 1 · still being laid out'); assert.deepEqual([...card4.querySelectorAll('.approveBox')].map(b => b.dataset.approveFor), [K4]);
  put(bump(world.G, { status: 'complete', laserHold: { at: 3, by: 'Paul' } })); L.changed(); flush();
  assert.equal(off(btnOf(card4, K4)), false, 'a held sheet is pressable: Approve lifts the hold');

  // ── E. a set of one sheet is one button at set level, its sheet's own test, a quiet reason for back engravings; a loose group is its sheets' own ──
  const one = setCard('set5', ['I']), K5 = 'set:set5'; put(eng('I', 3, 0)); place(one, ['I']); L.changed(); flush();
  assert.deepEqual([...one.querySelectorAll('.approveBox')].map(b => b.dataset.approveFor), [K5], 'one button for the set of one sheet, none on the sheet');
  assert.equal(off(btnOf(one, K5)), true); assert.equal(whyOf(one, K5).textContent, '', 'a lone sheet\'s engravings are the rail\'s to say'); assert.match(btnOf(one, K5).getAttribute('aria-label'), /Waiting on 3 back engravings/);
  const loose = setCard(undefined, ['J', 'K'], { setId: undefined, working: true, sheetIds: undefined }); put(sheet2('J'), eng('K', 2, 0, { metal: 'silver' })); place(loose, ['J', 'K']); L.changed(); flush();
  assert.deepEqual([...loose.querySelectorAll('.approveBox')].map(b => b.dataset.approveFor), ['sheet:J', 'sheet:K'], 'sheets of a loose group are no set: each keeps its own');
  assert.equal(off(btnOf(loose, 'sheet:J')), false, 'J is green whatever K lacks'); assert.equal(off(btnOf(loose, 'sheet:K')), true);
  calls.length = 0; btnOf(loose, 'sheet:J').click(); assert.deepEqual(calls[0], { kind: 'sheet', id: 'J', by: 'Maria' }, 'its press approves that sheet, as before'); win._release();
  await tick();

  console.log('Set approve OK (part 3): GF ready + SS blocked: the set\'s one button is grey with one plain reason; both ready: it is green, one press approves the set once; green and grey flip in one pass without a flicker and keep the keyboard; completed, held, 14K, Rose Gold, one-sheet and loose cases');
  win.close();
});

(async () => { for (const part of parts) await part(); })().then(() => process.exit(process.exitCode || 0), e => { console.error(e); process.exit(1); });   // (a fake page's timers are let go with the process)
