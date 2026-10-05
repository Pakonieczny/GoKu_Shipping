/* A set advances as ONE (Paul, 5 Oct 2026, round 7): "you cannot have a green approved button on a single sheet that is part of a set
   where the other sheets are not ready yet and don't have the green Approve button engaged."
   Part 1 (this file grows): CharmNestReadiness.setGate, the one truth the Approve button, its grey reason, the server's refusal and
   the set-level wait row all read. Pure: no page, no network, no live endpoint. */
const assert = require('node:assert/strict');
const R = require('../../charm-nest-readiness.js');

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
  assert.equal(R.setGate(set(['B']), [B]).reason, 'GF Sheet 1 · still a draft');
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
  const g = R.setGate(set(['A', 'K']), [sheet('A'), solid]); assert.equal(g.ready, false); assert.equal(g.reason, '14K Sheet 1 · not included in a set yet');
  const rose = sheet('R', { metal: 'rose', roseStockId: 'stock-1' });                              // no green line yet: the Cut Sheet / its own yes, never a grey button
  assert.equal(R.setGate(set(['A', 'R']), [sheet('A'), rose]).ready, true);
  const roseWait = engraving('R', 3, 1, { metal: 'rose', roseStockId: 'stock-1' });
  assert.equal(R.setGate(set(['A', 'R']), [sheet('A'), roseWait]).reason, 'RG Sheet 1 · back engravings 1 of 3');
}

console.log('Set approve OK (part 1, the gate): one truth for every sheet of a set, the lone button\'s hard test, soft items never grey, past sheets and holds, missing sheets, one-sheet sets');
