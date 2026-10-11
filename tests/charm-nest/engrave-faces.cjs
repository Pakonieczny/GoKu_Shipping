// ENGMAP (front-back-1011): the FACE of an engraving. Pure model, offline: no browser, no network, no paid call, no Firestore.
//
// What it proves
//   1  identity: a back key is exactly what it always was; a front key adds "~front"; parseKey and jobKey are inverses for a whole line, an ear and a disc, both faces;
//      a key that matches nothing is a whole line; a record key is the poolId for a back and "<poolId>~front" for a front
//   2  the line's setup: no reading = today's setup (back, or none when the line is no candidate); the order 4195966039 reading (front "W", back "1. 27. 24") gives
//      two faces; a person's override always wins and is never lost by a later reading; a face the intake could not fill needs a person, never a guess
//   3  the plan: slot major, Front before Back; reconcile adds what is missing, removes only undecided extras and never a decided one
//   4  pieces: groupPieces / pieceCount count pieces, never jobs; a back-only line groups exactly as it did
//   5  status: a piece is done only when every face it expects is decided; the combined decision keeps the old three fields, `faces` only when a front exists
//   6  approve-all: the same words rule, per face; a front and a back with different words are not "differing" words
//   7  records: dedupe keeps the newest per (piece, face); an old record with no face is a back
//   8  the panel's rows
//   9  CharmNestEngraveSides: keys with a face, the plan ignores front jobs, a front job's side row, the summary (combined decision, faces apart, text), the piece records
//  10  CharmNestEngraveRows: a line with a front job is grouped by piece, Back/Next go piece by piece, one press asks the per-face gate; a back-only line is grouped as before
//  11  CharmNestPieceSwitch: the Front | Back switch under the piece switch; a list with no front draws exactly what it drew
//  12  CNEngravingSeals: a front seal says its face and never merges into the back's; events split by face
//  13  CharmNestBacks: records keyed by recKey; a front record's words are not un-mirrored
//  14  CharmNestGeom: the FRONT view turns nothing over, puts the hoop on top, lies a long charm flat; the back view is what it was
//  15  CharmNestReadiness: a piece is approved and its files saved only when every face it expects is; a piece with no front reads exactly as before
//   node tests/charm-nest/engrave-faces.cjs
'use strict';
const path = require('path'), assert = require('assert/strict');
const F = require(path.join(__dirname, '../../charm-nest-engrave-faces.js'));
let n = 0;
const ok = (name, fn) => { fn(); n++; };

/* 1 ── identity */
ok('keys', () => {
  const lk = '4195966039_5009123456789';
  assert.equal(F.jobKey(lk, null, 'back'), lk);
  assert.equal(F.jobKey(lk, null), lk);
  assert.equal(F.jobKey(lk, 'L', 'back'), lk + '#L');
  assert.equal(F.jobKey(lk, null, 'front'), lk + '~front');
  assert.equal(F.jobKey(lk, 'D2', 'front'), lk + '#D2~front');
  for (const slot of [null, 'L', 'R', 'D1', 'D2', 'D12']) for (const face of ['back', 'front']) {
    const k = F.jobKey(lk, slot, face), p = F.parseKey(k);
    assert.equal(p.lineKey, lk); assert.equal(p.rowKey, lk); assert.equal(p.slot, slot); assert.equal(p.face, face);
    assert.equal(p.pieceKey, lk + (slot ? '#' + slot : '')); assert.equal(p.key, k);
    assert.equal(F.pieceKeyOf(k), p.pieceKey);
  }
  assert.deepEqual(F.parseKey('weird key'), { key: 'weird key', lineKey: 'weird key', rowKey: 'weird key', slot: null, face: 'back', pieceKey: 'weird key' });
  assert.equal(F.parseKey('').lineKey, ''); assert.equal(F.parseKey(null).face, 'back');
  assert.equal(F.recKey('1_2_1', 'back'), '1_2_1'); assert.equal(F.recKey('1_2_1', 'front'), '1_2_1~front'); assert.equal(F.recKey('1_2_1'), '1_2_1');
  assert.deepEqual(F.parseRecKey('1_2_1~front'), { poolId: '1_2_1', face: 'front' }); assert.deepEqual(F.parseRecKey('1_2_1'), { poolId: '1_2_1', face: 'back' });
  assert.equal(F.recKeyOf({ poolId: '1_2_1', face: 'front' }), '1_2_1~front'); assert.equal(F.recKeyOf({ poolId: '1_2_1' }), '1_2_1');
});
ok('faceOf', () => {
  assert.equal(F.faceOf(null), 'back'); assert.equal(F.faceOf({}), 'back'); assert.equal(F.faceOf({ face: 'back' }), 'back'); assert.equal(F.faceOf({ face: 'front' }), 'front');
  assert.equal(F.faceOf({ key: 'a_b~front' }), 'front'); assert.equal(F.faceOf({ key: 'a_b#L' }), 'back'); assert.equal(F.faceOf('a_b~front'), 'front');
  assert.equal(F.faceOf({ data: { face: 'front' } }), 'front'); assert.equal(F.faceOf({ face: 'nonsense' }), 'back');
  assert.equal(F.isFront({ face: 'front' }), true); assert.equal(F.isFront({}), false);
  assert.equal(F.label('front'), 'Front'); assert.equal(F.label('back'), 'Back'); assert.equal(F.tag('front'), 'FRONT');
});

/* 2 ── the line's setup */
const ORDER_4195966039 = { spec: { engraveCandidate: true, engraving: { v: 1, mode: 'both', source: 'labelled', front: { text: 'W' }, back: { text: '1. 27. 24' }, textSource: 'personalization', needsPerson: false, confidence: 0.95, why: 'The buyer labelled front and back' } } };
ok('setup: the real order', () => {
  const s = F.setupOf(ORDER_4195966039);
  assert.equal(s.mode, 'both'); assert.deepEqual(s.faces, ['front', 'back']); assert.equal(s.multi, true);
  assert.deepEqual(s.front.lines, ['W']); assert.deepEqual(s.back.lines, ['1. 27. 24']); assert.equal(s.edited, false); assert.equal(s.needsPerson, false);
  assert.equal(F.describe(s), 'Front: W · Back: 1. 27. 24'); assert.equal(F.modeLabel('both'), 'Front + back');
  assert.deepEqual(F.seedFor(s, 'front').lines, ['W']); assert.deepEqual(F.seedFor(s, 'back').lines, ['1. 27. 24']);
  assert.equal(F.seedFor(s, 'back').how, 'personalization'); assert.equal(F.seedFor(s, 'back').source, 'labelled');
});
ok('setup: no reading is today', () => {
  for (const x of [undefined, null, {}, { spec: {} }, { spec: { engraveCandidate: true } }, { spec: { engraving: 'junk' } }, { spec: { engraving: { mode: 'weird' } } }]) {
    const s = F.setupOf(x); assert.equal(s.mode, 'back'); assert.deepEqual(s.faces, ['back']); assert.equal(s.multi, false); assert.equal(s.source, 'default'); assert.equal(F.seedFor(s, 'back'), null);
    assert.equal(s.front, null); assert.equal(s.needsPerson, false);
  }
  const none = F.setupOf({ spec: { engraveCandidate: false } }); assert.equal(none.mode, 'none'); assert.deepEqual(none.faces, []); assert.equal(F.describe(none), 'No engraving');
  assert.deepEqual(F.planFaces(F.setupOf({}), []), [{ slot: null, face: 'back' }]);
});
ok('setup: back-only reading with its own default is still the reader path', () => {
  const s = F.setupOf({ spec: { engraving: { v: 1, mode: 'back', source: 'default', back: { text: 'Anna' } } } });
  assert.deepEqual(s.faces, ['back']); assert.equal(F.seedFor(s, 'back'), null);       // the paid reader decides, as it always did
  const lab = F.setupOf({ spec: { engraving: { v: 1, mode: 'back', source: 'labelled', back: { text: 'Anna' } } } });
  assert.deepEqual(F.seedFor(lab, 'back').lines, ['Anna']);
});
ok('setup: front only, back text ignored', () => {
  const s = F.setupOf({ spec: { engraving: { mode: 'front', source: 'labelled', front: { text: 'W' }, back: { text: 'ignored' } } } });
  assert.deepEqual(s.faces, ['front']); assert.equal(s.back, null); assert.equal(F.seedFor(s, 'back'), null); assert.deepEqual(F.seedFor(s, 'front').lines, ['W']);
});
ok('setup: needsPerson and unlabelled both', () => {
  const s = F.setupOf({ spec: { engraving: { mode: 'both', source: 'inferred', needsPerson: true, textSource: 'personalization', front: null, back: null } } });
  assert.deepEqual(s.faces, ['front', 'back']); assert.equal(s.needsPerson, true); assert.equal(F.seedFor(s, 'front'), null); assert.equal(F.seedFor(s, 'back'), null);
});
ok('setup: a person wins and is never overwritten by a reading', () => {
  const row = { spec: ORDER_4195966039.spec, engravingOverride: { mode: 'both', front: { text: 'WW' }, by: 'Paul', at: 1760000000000 } };
  const s = F.setupOf(row);
  assert.equal(s.edited, true); assert.equal(s.source, 'person'); assert.deepEqual(s.front.lines, ['WW']); assert.deepEqual(s.back.lines, ['1. 27. 24']);   // the back stays the order's, the front is Paul's
  assert.equal(F.seedFor(s, 'front').source, 'person'); assert.equal(F.seedFor(s, 'front').confidence, 1); assert.equal(s.by, 'Paul'); assert.equal(s.why, 'Set by Paul');
  const swap = F.setupOf({ spec: ORDER_4195966039.spec, engravingOverride: { mode: 'back', by: 'Paul', at: 1 } });
  assert.deepEqual(swap.faces, ['back']); assert.equal(swap.front, null); assert.equal(swap.edited, true); assert.deepEqual(F.seedFor(swap, 'back').lines, ['1. 27. 24']);
  const none = F.setupOf({ spec: ORDER_4195966039.spec, engravingOverride: { mode: 'none', by: 'Paul', at: 1 } });
  assert.deepEqual(none.faces, []);
  const second = F.setupOf(ORDER_4195966039, { mode: 'front', front: { lines: ['A', 'B'] }, by: 'Rita' });                  // an explicit override argument beats the row's
  assert.deepEqual(second.faces, ['front']); assert.deepEqual(second.front.lines, ['A', 'B']);
  assert.equal(F.setupOf({ spec: ORDER_4195966039.spec, engravingOverride: {} }).edited, false);   // an empty override is no override
  assert.equal(F.setupOf({ spec: ORDER_4195966039.spec, engravingOverride: 'x' }).edited, false);
});
ok('setup: words are normalised', () => {
  const s = F.setupOf({ spec: { engraving: { mode: 'both', source: 'labelled', front: { text: ' A  b \n\n c ' }, back: 'Z' } } });
  assert.deepEqual(s.front.lines, ['A b', 'c']); assert.equal(s.front.text, 'A b\nc'); assert.deepEqual(s.back.lines, ['Z']);
  assert.equal(F.normFace(''), null); assert.equal(F.normFace({}), null); assert.equal(F.normFace(5), null); assert.equal(F.normFace({ lines: ['x'], fontWord: ' Ace ' }).fontWord, 'Ace');
});

/* 3 ── the plan */
ok('plan', () => {
  const both = F.setupOf(ORDER_4195966039);
  assert.deepEqual(F.planFaces(both, []), [{ slot: null, face: 'front' }, { slot: null, face: 'back' }]);
  assert.deepEqual(F.planFaces(both, ['L', 'R']), [{ slot: 'L', face: 'front' }, { slot: 'L', face: 'back' }, { slot: 'R', face: 'front' }, { slot: 'R', face: 'back' }]);
  assert.deepEqual(F.planFaces(both, ['D2', 'D1']).map(p => p.slot + p.face[0]), ['D1f', 'D1b', 'D2f', 'D2b']);
  assert.deepEqual(F.planFaces(F.setupOf({}), ['L', 'R']), [{ slot: 'L', face: 'back' }, { slot: 'R', face: 'back' }]);
});
ok('reconcile', () => {
  const lk = '1_2';
  const job = (slot, face, state) => ({ key: F.jobKey(lk, slot, face), slot, face, state });
  const both = F.setupOf(ORDER_4195966039), backOnly = F.setupOf({});
  let r = F.reconcile(both, [job(null, 'back', 'review')], []);
  assert.deepEqual(r.add, [{ slot: null, face: 'front' }]); assert.equal(r.drop.length, 0); assert.equal(r.keep.length, 0);
  r = F.reconcile(backOnly, [job(null, 'front', 'review'), job(null, 'back', 'review')], []);
  assert.equal(r.add.length, 0); assert.deepEqual(r.drop.map(j => j.key), [lk + '~front']);
  r = F.reconcile(backOnly, [job(null, 'front', 'approved'), job(null, 'back', 'review')], []);
  assert.equal(r.drop.length, 0); assert.deepEqual(r.keep.map(j => j.key), [lk + '~front']);      // a decided front is never removed
  r = F.reconcile(F.setupOf({ spec: { engraving: { mode: 'front', front: { text: 'W' } } } }), [job(null, 'back', 'written')], []);
  assert.deepEqual(r.add, [{ slot: null, face: 'front' }]); assert.deepEqual(r.keep.map(j => j.key), [lk]);
  r = F.reconcile(both, [job('L', 'back', 'review'), job('R', 'back', 'review')], ['L', 'R']);
  assert.deepEqual(r.add.map(a => a.slot + a.face[0]), ['Lf', 'Rf']);
  r = F.reconcile(backOnly, [{ key: lk, state: 'review' }], []);                                   // an old job (no face field) is a back: nothing changes
  assert.equal(r.add.length + r.drop.length + r.keep.length, 0);
});

/* 4 ── pieces */
ok('pieces', () => {
  const lk = '1_2', j = (slot, face, state, extra) => Object.assign({ key: F.jobKey(lk, slot, face), state }, extra);
  const discs = [j('D1', 'back', 'review'), j('D2', 'back', 'review'), j('D3', 'back', 'review')];
  assert.equal(F.pieceCount(discs), 3); assert.equal(F.pieceCount([j(null, 'back', 'review')]), 1); assert.equal(F.pieceCount([]), 0);
  const g = F.groupPieces(discs); assert.deepEqual(g.map(x => x.slot), ['D1', 'D2', 'D3']); assert.equal(g[0].label, 'Disc 1'); assert.equal(g[0].front, null); assert.equal(g[0].back, discs[0]); assert.deepEqual(g[0].faces, ['back']);
  const four = [j('R', 'front', 'review'), j('L', 'back', 'review'), j('L', 'front', 'review'), j('R', 'back', 'review')];
  assert.equal(F.pieceCount(four), 2);
  const g2 = F.groupPieces(four); assert.deepEqual(g2.map(x => x.slot), ['L', 'R']); assert.equal(g2[0].front.key, lk + '#L~front'); assert.equal(g2[0].back.key, lk + '#L'); assert.equal(g2[0].pieceKey, lk + '#L');
  assert.deepEqual(g2[1].jobs.map(x => F.faceOf(x)), ['front', 'back']); assert.equal(g2[1].label, 'Right');
  const fj = F.faceJobs(four, 'R'); assert.equal(fj.front.key, lk + '#R~front'); assert.equal(fj.back.key, lk + '#R');
  assert.equal(F.faceJobs(four, null).front, null);
  const whole = F.groupPieces([j(null, 'front', 'review'), j(null, 'back', 'review')]); assert.equal(whole.length, 1); assert.equal(whole[0].slot, null); assert.equal(whole[0].label, '');
});

/* 5 ── status */
ok('status', () => {
  const lk = '1_2', j = (face, state) => ({ key: F.jobKey(lk, null, face), state });
  let st = F.faceStatus([j('back', 'approved')]);
  assert.equal(st.done, true); assert.equal(st.state, 'approved'); assert.deepEqual(st.expected, ['back']);
  st = F.faceStatus([j('front', 'review'), j('back', 'approved')]);
  assert.equal(st.done, false); assert.equal(st.state, 'review'); assert.equal(st.faces.front, 'review');
  assert.equal(F.bothFacesDone([j('front', 'review'), j('back', 'approved')]), false);
  assert.equal(F.bothFacesDone([j('front', 'approved'), j('back', 'written')]), true);
  assert.equal(F.bothFacesDone([j('back', 'approved')], { expected: ['front', 'back'] }), false);   // the front job is not made yet: not done
  assert.equal(F.bothFacesDone([j('back', 'approved')]), true);                                     // an old line: its one job decides
  assert.equal(F.bothFacesDone([]), false);
  assert.equal(F.bothFacesWritten([j('front', 'written'), j('back', 'written')]), true);
  assert.equal(F.bothFacesWritten([j('front', 'approved'), j('back', 'written')]), false);
  assert.equal(F.bothFacesWritten([j('front', 'skipped'), j('back', 'written')]), true);
  assert.equal(F.faceStatus([j('front', 'none'), j('back', 'none')]).state, 'none');
  assert.equal(F.faceStatus([j('front', 'skipped'), j('back', 'none')]).state, 'skipped');
  assert.equal(F.faceStatus([j('front', 'fitting'), j('back', 'words')]).state, 'words');
  assert.equal(F.faceStatus([]).done, false);
});
ok('combine: the old three fields, `faces` only with a front', () => {
  assert.equal(F.combine([]), undefined);
  const back = F.combine([{ needed: true, state: 'approved', approved: true }]);
  assert.deepEqual(back, { needed: true, state: 'approved', approved: true });                      // byte for byte the old shape
  const c = F.combine([{ face: 'front', needed: true, state: 'review', approved: false }, { face: 'back', needed: true, state: 'approved', approved: true }]);
  assert.equal(c.needed, true); assert.equal(c.state, 'review'); assert.equal(c.approved, false); assert.deepEqual(c.faces, { front: { needed: true, state: 'review', approved: false }, back: { needed: true, state: 'approved', approved: true } });
  const done = F.combine([{ face: 'front', needed: true, state: 'written', approved: true }, { face: 'back', needed: true, state: 'written', approved: true }]);
  assert.equal(done.approved, true); assert.equal(done.state, 'written');
  const skipFront = F.combine([{ face: 'front', needed: false, state: 'skipped', approved: false }, { face: 'back', needed: true, state: 'approved', approved: true }]);
  assert.equal(skipFront.state, 'approved'); assert.equal(skipFront.approved, false);                // a skipped face is not approved, so the line is not "approved" for the summary
  assert.equal(F.combine([{ needed: false, state: 'none', approved: false }]).state, 'none');
});

/* 6 ── approve all */
ok('approve-all gate', () => {
  const lk = '1_2', ready = (slot, face, text) => ({ key: F.jobKey(lk, slot, face), slot, face, state: 'review', lines: [text], fit: {}, verify: { geometry: { ok: true } } });
  const ears = [ready('L', 'back', 'J'), ready('R', 'back', 'J')];
  assert.equal(F.approveAllGate(ears).ok, true);
  assert.equal(F.approveAllGate([ears[0]]).ok, false);
  assert.equal(F.approveAllGate([ready('L', 'back', 'J'), ready('R', 'back', 'K')]).ok, false);
  const four = [ready('L', 'front', 'W'), ready('L', 'back', '1. 27. 24'), ready('R', 'front', 'W'), ready('R', 'back', '1. 27. 24')];
  assert.equal(F.approveAllGate(four).ok, true);                                                   // different words on different faces are fine, equal per face
  const bad = four.slice(); bad[2] = ready('R', 'front', 'X'); const r = F.approveAllGate(bad); assert.equal(r.ok, false); assert.match(r.why, /front words differ/);
  const single = [ready(null, 'front', 'W'), ready(null, 'back', '1. 27. 24')];
  assert.equal(F.approveAllGate(single).ok, true);
  assert.equal(F.approveAllGate(single, { shown: j => F.faceOf(j) === 'back' }).ok, false);
  assert.match(F.approveAllGate(single, { shown: j => F.faceOf(j) === 'back' }).why, /front placement/);
  assert.equal(F.approveAllGate([ears[0], Object.assign({}, ears[1], { state: 'fitting' })]).ok, false);
  assert.equal(F.approveAllGate([ears[0], Object.assign({}, ears[1], { backSaving: true })]).ok, false);
  assert.equal(F.approveAllGate(ears, { busy: j => j.slot === 'R' }).ok, false);
});

/* 7 ── records */
ok('records', () => {
  const recs = [
    { poolId: '1_2_1', approvedAt: 10, by: 'a' }, { poolId: '1_2_1', approvedAt: 20, by: 'b' },                            // two backs: the newer
    { poolId: '1_2_1', face: 'front', approvedAt: 15, by: 'c' },
    { poolId: '1_2_2', approvedAt: 5, invalidated: true }, { poolId: '1_2_3', approvedAt: 1 }, null, { approvedAt: 1 }];
  const d = F.dedupe(recs);
  assert.deepEqual([...d.keys()].sort(), ['1_2_1', '1_2_1~front', '1_2_3']); assert.equal(d.get('1_2_1').by, 'b'); assert.equal(d.get('1_2_1~front').by, 'c');
  assert.equal(F.pickRecord(recs, '1_2_1', 'front').by, 'c'); assert.equal(F.pickRecord(recs, '1_2_1', 'back').by, 'b'); assert.equal(F.pickRecord(recs, '1_2_2', 'back'), null);
  const both = F.recordsOf(recs, '1_2_1'); assert.equal(both.front.by, 'c'); assert.equal(both.back.by, 'b'); assert.equal(F.recordsOf(recs, '1_2_3').front, null);
  assert.equal(F.pickRecord(recs, '1_2_3', 'front'), null);                                        // an old record (no face) is never a front
});

/* 8 ── the panel's rows */
ok('panel rows', () => {
  const both = F.setupOf(ORDER_4195966039), lk = '1_2';
  let rows = F.panelRows(both, []);
  assert.deepEqual(rows.map(r => [r.face, r.words, r.from, r.state]), [['front', 'W', 'order', ''], ['back', '1. 27. 24', 'order', '']]);
  const jobs = [{ key: lk + '~front', face: 'front', state: 'review', lines: ['W'], wordsSource: 'spec.engraving' }, { key: lk, state: 'approved', lines: ['1. 27. 24'], decision: { by: 'Paul' } }];
  rows = F.panelRows(both, jobs);
  assert.deepEqual(rows.map(r => [r.face, r.words, r.from, r.state]), [['front', 'W', 'order', 'review'], ['back', '1. 27. 24', 'person', 'approved']]);
  rows = F.panelRows(F.setupOf({}), [{ key: lk, state: 'review', lines: ['Anna'], text: 'Anna', confidence: 0.9 }]);
  assert.deepEqual(rows.map(r => [r.face, r.words, r.from]), [['back', 'Anna', 'reader']]);        // an old back-only line: one row, the reader's
  assert.equal(F.panelRows(F.setupOf({ spec: { engraveCandidate: false } }), []).length, 0);
  rows = F.panelRows(F.setupOf({ spec: { engraving: { mode: 'both', source: 'inferred', needsPerson: true } } }), []);
  assert.deepEqual(rows.map(r => [r.face, r.from]), [['front', 'unread'], ['back', 'unread']]);
});

/* 9 ── sides */
const Sides = require(path.join(__dirname, '../../charm-nest-engrave-sides.js'));
ok('sides: keys', () => {
  assert.deepEqual(Sides.parseKey('x#L'), { rowKey: 'x', slot: 'L' }); assert.deepEqual(Sides.parseKey('x'), { rowKey: 'x', slot: null });
  assert.deepEqual(Sides.parseKey('x#D2~front'), { rowKey: 'x', slot: 'D2', face: 'front' }); assert.deepEqual(Sides.parseKey('x~front'), { rowKey: 'x', slot: null, face: 'front' });
  assert.equal(Sides.jobKey('x', 'L'), 'x#L'); assert.equal(Sides.jobKey('x', 'L', 'front'), 'x#L~front'); assert.equal(Sides.jobKey('x', null, 'front'), 'x~front'); assert.equal(Sides.jobKey('x', null), 'x');
});
const mkJob = (lk, slot, face, state, text, extra) => Object.assign({ key: F.jobKey(lk, slot, face), slot, rowKey: lk, state, copies: [lk + '_1'], lines: text ? [text] : [], engraveRec: { needed: state !== 'none', state, approved: ['approved', 'written'].includes(state), text } }, face === 'front' ? { face: 'front' } : {}, extra || {});
ok('sides: plan ignores front jobs', () => {
  const row = { key: '1_2', poolIds: ['1_2_1'], spec: {} }, ctx = {};
  const withFront = [mkJob('1_2', null, 'front', 'approved', 'W')], none = [];
  assert.deepEqual(Sides.plan(ctx, row, withFront), Sides.plan(ctx, row, none));        // a decided slotless front does not make the line "legacy decided"
  const back = mkJob('1_2', null, 'back', 'approved', 'X'); delete back.engraveRec;
  assert.equal(Sides.plan(ctx, row, [back, ...withFront]).split, false);
});
ok('sides: a front side row', () => {
  const parent = { key: '1_2', poolIds: ['1_2_1', '1_2_2'], order: { receiptId: '1' }, spec: {} }, ctx = {};
  const fj = mkJob('1_2', null, 'front', 'review', 'W'), bj = mkJob('1_2', null, 'back', 'review', 'X');
  const fr = Sides.sideRow(ctx, parent, null, fj);
  assert.equal(fr.key, '1_2~front'); assert.equal(fr.face, 'front'); assert.equal(fr.slot, null); assert.equal(fr.parentRow, parent); assert.equal(fr.engrave, fj.engraveRec);
  assert.deepEqual(fr.poolIds, ['1_2_1', '1_2_2']);
  const br = Sides.sideRow(ctx, parent, 'L', Object.assign({}, bj, { slot: 'L' })); assert.equal(br.key, '1_2#L'); assert.equal('face' in br, false);   // a back row is what it always was
  const flr = Sides.sideRow(ctx, parent, 'L', mkJob('1_2', 'L', 'front', 'review', 'W')); assert.equal(flr.key, '1_2#L~front'); assert.equal(flr.slot, 'L'); assert.equal(flr.side, 'L');
});
ok('sides: the summary', () => {
  const ctx = { mergeSeals: (...r) => [].concat(...r.map(x => (x && x.seals) || [])) };
  // back only: byte for byte what it gave (pair of ears)
  const L = mkJob('1_2', 'L', 'back', 'approved', 'A', { copies: ['1_2_1'] }), R = mkJob('1_2', 'R', 'back', 'review', 'B', { copies: ['1_2_2'] });
  const s0 = Sides.summary(ctx, undefined, [L, R]);
  assert.deepEqual(s0.pieces, { '1_2_1': { needed: true, state: 'approved', approved: true }, '1_2_2': { needed: true, state: 'review', approved: false } });
  assert.equal(s0.state, 'review'); assert.equal(s0.approved, false); assert.equal(s0.text, 'Left: A · Right: B');
  // a single charm, front W + back 1. 27. 24 (the order)
  const f = mkJob('1_2', null, 'front', 'approved', 'W'), b = mkJob('1_2', null, 'back', 'review', '1. 27. 24');
  const s1 = Sides.summary(ctx, undefined, [f, b]);
  assert.equal(s1.text, 'Front: W · Back: 1. 27. 24'); assert.equal(s1.approved, false); assert.equal(s1.state, 'review'); assert.equal(s1.needed, true);
  assert.deepEqual(s1.pieces['1_2_1'], { needed: true, state: 'review', approved: false, faces: { front: { needed: true, state: 'approved', approved: true }, back: { needed: true, state: 'review', approved: false } } });
  b.state = 'approved'; b.engraveRec = Object.assign({}, b.engraveRec, { state: 'approved', approved: true });
  const s2 = Sides.summary(ctx, undefined, [f, b]); assert.equal(s2.approved, true); assert.equal(s2.state, 'approved'); assert.equal(s2.pieces['1_2_1'].approved, true);
  // two ears, both faces, ears differ
  const jobs = [mkJob('1_2', 'L', 'front', 'approved', 'W', { copies: ['1_2_1'] }), mkJob('1_2', 'L', 'back', 'approved', '1', { copies: ['1_2_1'] }), mkJob('1_2', 'R', 'front', 'approved', 'W', { copies: ['1_2_2'] }), mkJob('1_2', 'R', 'back', 'approved', '2', { copies: ['1_2_2'] })];
  assert.equal(Sides.summary(ctx, undefined, jobs).text, 'Left (Front: W · Back: 1) · Right (Front: W · Back: 2)');
  jobs[3].engraveRec.text = '1'; assert.equal(Sides.summary(ctx, undefined, jobs).text, 'Front: W · Back: 1');
  // a skipped (cut plain) front does not hold the line back
  const sk = mkJob('1_2', null, 'front', 'skipped', ''); sk.engraveRec = { needed: false, state: 'skipped', approved: true };
  const s3 = Sides.summary(ctx, undefined, [sk, b]); assert.equal(s3.approved, true); assert.equal(s3.state, 'approved'); assert.equal(s3.pieces['1_2_1'].approved, true);
});
ok('sides: piece records', () => {
  const lk = '1_2', f = mkJob(lk, 'D2', 'front', 'review', 'W'), b = mkJob(lk, 'D2', 'back', 'review', 'Q'), d1 = mkJob(lk, 'D1', 'back', 'review', 'J'), d3 = mkJob(lk, 'D3', 'back', 'review', 'K');
  const recs = Sides.pieceRecords([d3, b, f, d1], {});
  assert.deepEqual(recs.map(r => [r.slot, r.face]), [['D1', 'back'], ['D2', 'front'], ['D2', 'back'], ['D3', 'back']]);
  assert.equal(recs[0].of, 3); assert.equal(recs[1].of, 3);                                               // pieces, not jobs
  assert.equal(recs[1].tag, 'DISC 2 of 3 · FRONT'); assert.equal(recs[2].tag, 'DISC 2 of 3'); assert.equal(recs[0].tag, 'DISC 1 of 3');
  assert.equal(recs[1].faceLabel, 'Front'); assert.equal(recs[2].faceTag, 'BACK'); assert.equal(recs[1].pieceKey, lk + '#D2'); assert.equal(recs[1].lineKey, lk); assert.equal(recs[1].key, lk + '#D2~front');
  const plain = Sides.pieceRecords([mkJob(lk, 'D2', 'back', 'review', 'Q'), d1], {}); assert.deepEqual(plain.map(r => [r.slot, r.of, r.tag]), [['D1', 2, 'DISC 1 of 2'], ['D2', 2, 'DISC 2 of 2']]);
  const one = Sides.pieceRecord(mkJob(lk, null, 'front', 'review', 'W'), { of: 1, faces: 2 }); assert.equal(one.tag, 'FRONT'); assert.equal(Sides.pieceRecord(mkJob(lk, null, 'front', 'review', 'W'), {}).tag, '');
});

/* 10 ── rows */
const Rows = require(path.join(__dirname, '../../charm-nest-engrave-rows.js'));
ok('rows: grouping and steps', () => {
  const lk = '1_2', jb = (slot, face, state) => mkJob(lk, slot, face, state || 'review', face === 'front' ? 'W' : 'X');
  // back only: unchanged
  const L = jb('L', 'back'), R = jb('R', 'back'); let rows = Rows.group([L, R], undefined, { discs: true });
  assert.equal(rows.length, 1); assert.equal(rows[0].pair, true); assert.deepEqual(rows[0].jobs, [L, R]); assert.equal('faces' in rows[0], false); assert.equal(Rows.kindOf(rows[0]), 'ears');
  // a single charm with both faces: ONE row
  const f = jb(null, 'front'), b = jb(null, 'back'); rows = Rows.group([f, b], undefined, { discs: true });
  assert.equal(rows.length, 1); assert.equal(rows[0].faces, true); assert.equal(rows[0].pair, false); assert.deepEqual(rows[0].jobs, [f, b]); assert.equal(Rows.kindOf(rows[0]), 'faces'); assert.equal(Rows.multi(rows[0]), true);
  // two ears, both faces: ONE pair row holding four jobs, front before back, piece by piece
  const four = [jb('R', 'back'), jb('L', 'back'), jb('R', 'front'), jb('L', 'front')]; rows = Rows.group(four, undefined, { discs: true });
  assert.equal(rows.length, 1); assert.equal(rows[0].pair, true); assert.equal(rows[0].faces, true); assert.deepEqual(rows[0].jobs.map(j => j.key), [lk + '#L~front', lk + '#L', lk + '#R~front', lk + '#R']); assert.equal(Rows.kindOf(rows[0]), 'ears');
  // discs: one row; Back / Next = one stop per disc; the stop's job is the disc's Front
  const discs = [jb('D1', 'back'), jb('D1', 'front'), jb('D2', 'back'), jb('D2', 'front'), jb('D3', 'back')]; rows = Rows.group(discs, undefined, { discs: true });
  assert.equal(rows.length, 1); assert.equal(rows[0].discs, true); assert.equal(Rows.kindOf(rows[0]), 'discs');
  const s1 = Rows.step(rows, lk + '#D1', 1); assert.equal(s1.job.key, lk + '#D2~front');
  const s2 = Rows.step(rows, lk + '#D2', 1); assert.equal(s2.job.key, lk + '#D3');                       // from the back of disc 2 to disc 3
  assert.equal(Rows.step(rows, lk + '#D3', 1).job.key, lk + '#D1~front'); assert.equal(Rows.step(rows, lk + '#D1~front', -1).job.key, lk + '#D3');
  assert.equal(Object.keys(s1).sort().join(), 'job,row');
  // other lines keep their rows and the order of the selection
  const other = { key: '9_9', slot: null, state: 'review', lines: ['Z'] }; rows = Rows.group([other, f, b], undefined, { discs: true }); assert.equal(rows.length, 2); assert.equal(rows[0].jobs[0], other); assert.equal(rows[1].faces, true);
  assert.equal(Rows.lineKeyOf(f), lk); assert.equal(Rows.earOf({ key: lk + '#L~front' }), 'L'); assert.equal(Rows.discOf({ key: lk + '#D2~front' }), 2);
});
ok('rows: one press for the row', () => {
  const lk = '1_2', ready = (slot, face, text) => ({ key: F.jobKey(lk, slot, face), slot, face, state: 'review', lines: [text], fit: {}, verify: { geometry: { ok: true } } });
  const row = Rows.group([ready(null, 'front', 'W'), ready(null, 'back', '1. 27. 24')], undefined, {})[0];
  assert.equal(Rows.approveRow(row, {}).ok, true); assert.equal(Rows.approveBoth(row, {}).ok, true);
  assert.equal(Rows.approveRow(row, { shown: j => j.face !== 'front' }).ok, false);
  const ears = Rows.group([ready('L', 'back', 'J'), ready('R', 'back', 'J')], undefined, {})[0]; assert.equal(Rows.approveRow(ears, {}).ok, true);
  const diff = Rows.group([ready('L', 'back', 'J'), ready('R', 'back', 'K')], undefined, {})[0]; assert.equal(Rows.approveRow(diff, {}).ok, false);
  assert.equal(Rows.approveRow({ jobs: [ready(null, 'back', 'x')] }, {}).ok, false);
});

/* 11 ── piece switch */
const PS = require(path.join(__dirname, '../../charm-nest-piece-switch.js'));
ok('piece switch: faces', () => {
  const lk = '1_2', jb = (slot, face, state, t) => mkJob(lk, slot, face, state, t);
  const plain = [jb('L', 'back', 'review', 'J'), jb('R', 'back', 'review', 'J')], pp = PS.list(plain);
  assert.equal(PS.html(pp, plain[0].key), PS.html(pp, plain[0].key, { faces: false }));                  // no front: the old markup, either way
  assert.ok(!/egFaces/.test(PS.html(pp, plain[0].key))); assert.equal(PS.kindOf(pp), 'ears'); assert.deepEqual(pp.map(p => [p.face, p.faces, p.tag]), [['back', 1, 'LEFT EAR'], ['back', 1, 'RIGHT EAR']]);
  const one = [jb(null, 'front', 'review', 'W'), jb(null, 'back', 'review', '1. 27. 24')], op = PS.list(one);
  assert.equal(PS.kindOf(op), 'faces'); const h1 = PS.html(op, one[0].key);
  assert.equal((h1.match(/egEarSwitch/g) || []).length, 1); assert.ok(/egFaces/.test(h1)); assert.equal((h1.match(/data-a="ear"/g) || []).length, 2);
  assert.ok(h1.indexOf('data-ear="1_2~front"') < h1.indexOf('data-ear="1_2"')); assert.ok(/aria-pressed="true"[^>]*><span class="egPiece" data-slot="" data-face="front">FRONT/.test(h1));
  const four = [jb('L', 'front', 'approved', 'W'), jb('L', 'back', 'review', '1'), jb('R', 'front', 'review', 'W'), jb('R', 'back', 'review', '2')], fp = PS.list(four);
  const h2 = PS.html(fp, four[1].key); assert.equal((h2.match(/egEarSwitch/g) || []).length, 2);
  assert.equal((h2.match(/data-ear=/g) || []).length, 4);                                                // 2 pieces + 2 faces of the shown piece
  const groups = PS.groupPieces(fp); assert.deepEqual(groups.map(g => [g.slot, g.front && g.front.face, g.back && g.back.face, g.key]), [['L', 'front', 'back', four[1].key], ['R', 'front', 'back', four[2].key]]);   // the face still to settle
  assert.equal(PS.allSameWords(fp), false); const same = PS.list([jb('L', 'front', 'review', 'W'), jb('L', 'back', 'review', '1'), jb('R', 'front', 'review', 'W'), jb('R', 'back', 'review', '1')]); assert.equal(PS.allSameWords(same), true);
  assert.equal(PS.list(four)[0].record.of, 2); assert.equal(PS.list(four, { of: 2 })[1].tag, 'LEFT EAR');
});

/* 12 ── seals */
const Seals = require(path.join(__dirname, '../../charm-nest-engraving-seals.js'));
ok('seals: faces', () => {
  const a = { key: '1_2', approvedAt: 5, approvedBy: 'Ana', state: 'approved' }, b = { key: '1_2~front', approvedAt: 5, approvedBy: 'Ana', state: 'approved' };
  assert.deepEqual(Seals.merge(a), [{ id: 'engraveApproved:5:Ana', how: 'engraveApproved', at: 5, by: 'Ana' }]);                 // a back seal: what it always was
  assert.deepEqual(Seals.merge(b), [{ id: 'engraveApproved:front:5:Ana', how: 'engraveApproved', at: 5, by: 'Ana', face: 'front' }]);
  assert.equal(Seals.merge(a, b).length, 2);                                                                                      // same instant, same person: two seals
  const job = { key: '1_2~front', face: 'front', row: { engrave: {} } }; const seal = Seals.add(job, 'engraveApproved', 'Ana', 7);
  assert.equal(seal.face, 'front'); assert.equal(seal.id, 'engraveApproved:front:7:Ana'); assert.equal(Seals.list(job).length, 1);
  const bjob = { key: '1_2', row: { engrave: {} } }; assert.equal(Seals.add(bjob, 'engraveApproved', 'Ana', 7).face, undefined);
  const parent = { order: { receiptId: '1' }, line: { transactionId: '2' }, poolIds: ['1_2_1'], key: '1_2' };
  const ev = [{ type: 'engraveApproved', id: '1_2_1~front-9', at: 9, by: 'P', orderId: '1', data: { poolId: '1_2_1', face: 'front' }, lineKey: '1_2' }, { type: 'engraveApproved', id: '1_2_1-8', at: 8, by: 'Q', orderId: '1', data: { poolId: '1_2_1' }, lineKey: '1_2' }];
  assert.deepEqual(Seals.fromEvents(ev, parent).map(x => x.by + (x.face || '')), ['Q', 'Pfront']);
  const sideF = Object.assign(Object.create(parent), { parentRow: parent, slot: null, key: '1_2~front', face: 'front' }), sideB = Object.assign(Object.create(parent), { parentRow: parent, slot: null, key: '1_2' });
  assert.deepEqual(Seals.fromEvents(ev, sideF).map(x => x.by), ['P']); assert.deepEqual(Seals.fromEvents(ev, sideB).map(x => x.by), ['Q']);
});

/* 13 ── backs */
const Backs = require(path.join(__dirname, '../../charm-nest-backs.js'));
ok('backs: faces', () => {
  const sheet = { id: 's1', poolIds: ['1_2_1', '1_2_2'] };
  const recs = [{ poolId: '1_2_1', approvedAt: 10 }, { poolId: '1_2_1', approvedAt: 20, text: 'newer' }, { poolId: '1_2_1', face: 'front', approvedAt: 15 }, { poolId: '1_2_2', approvedAt: 1, invalidated: true }, { poolId: '9_9_1', approvedAt: 1 }];
  const out = Backs.forSheet(sheet, recs); assert.deepEqual(out.map(b => [b.poolId, b.face || 'back', b.approvedAt]), [['1_2_1', 'front', 15], ['1_2_1', 'back', 20]]);
  assert.deepEqual(Backs.forSheet(sheet, [recs[0], recs[1]]).map(b => b.approvedAt), [20]);                                         // back only: as before
  assert.equal(Backs.recKey('1_2_1', 'front'), '1_2_1~front'); assert.equal(Backs.recKey('1_2_1'), '1_2_1');
  const html = Backs.markup(out.map(b => Object.assign({ sheetId: 's1', order: '1', sku: 'S' }, b)), {});
  assert.equal((html.match(/<figure/g) || []).length, 2); assert.equal((html.match(/data-face="front"/g) || []).length, 1); assert.ok(/aria-label="Front: /.test(html) || /Preview pending/.test(html));
  // a front record's words sit where the charm is drawn (turned hoop-up only), a back's are mirrored back
  const charm = { bbox: [0, 0, 20, 20], centerPt: [10, 10] }, mk = f => Backs.engraveOn(Object.assign({ poolId: 'p', lines: ['A'], sizePt: 4, centre: [14, 10], upAngle: 90 }, f ? { face: f } : {}), charm);
  assert.deepEqual(mk('front').centre, [14, 10]); assert.deepEqual(mk().centre, [6, 10]); assert.equal(mk('front').face, 'front'); assert.equal('face' in mk(), false);
});

/* 14 ── geometry */
const G = require(path.join(__dirname, '../../charm-nest-geom.js'));
ok('geom: the front view', () => {
  const rect = (x0, y0, x1, y1) => [['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['l', [x0, y0]]];
  const circ = (cx, cy, r) => { const k = 0.5523 * r; return [['m', [cx + r, cy]], ['c', [cx + r, cy + k], [cx + k, cy + r], [cx, cy + r]], ['c', [cx - k, cy + r], [cx - r, cy + k], [cx - r, cy]], ['c', [cx - r, cy - k], [cx - k, cy - r], [cx, cy - r]], ['c', [cx + k, cy - r], [cx + r, cy - k], [cx + r, cy]]]; };
  const bbOf = sub => { let x0 = 1e9, y0 = 1e9, x1 = -1e9, y1 = -1e9; for (const sg of sub) for (let i = 1; i < sg.length; i++) { const p = sg[i]; x0 = Math.min(x0, p[0]); y0 = Math.min(y0, p[1]); x1 = Math.max(x1, p[0]); y1 = Math.max(y1, p[1]); } return [x0, y0, x1, y1]; };
  const mem = (sub, extra) => Object.assign({ kind: 'path', layer: 'CUT', stroke: true, fill: false, strokeRGB: [0, 0, 0], fillRGB: [0, 0, 0], lwPt: 0.25, paintOp: 'S', closed: true, subpaths: [sub], bbox: bbOf(sub) }, extra || {});
  const charm = (body, holes, art) => { const o = mem(body), hs = holes.map(h => mem(h)), as = (art || []).map(a => mem(a, { layer: 'ENGRAVE', stroke: false, fill: true, fillRGB: [0, 0, 1] })); return { outline: o, members: [o, ...hs, ...as], bbox: o.bbox, widthPt: o.bbox[2] - o.bbox[0], heightPt: o.bbox[3] - o.bbox[1] }; };
  const holeAt = v => { const h = v.members[1].bbox, o = v.outline.bbox; return Math.atan2((h[1] + h[3]) / 2 - (o[1] + o[3]) / 2, (h[0] + h[2]) / 2 - (o[0] + o[2]) / 2) * 180 / Math.PI; };
  const disc = charm(circ(30, 30, 25), [circ(12, 48, 3)]);
  const fv = G.backView(disc, { res: 6, face: 'front' }), bv = G.backView(disc, { res: 6 });
  assert.equal(fv.face, 'front'); assert.equal('face' in bv, false); assert.deepEqual(fv.M, [1, 0, 0, 1, 0, 0]); assert.deepEqual(bv.M, [-1, 0, 0, 1, 2 * bv.cx, 0]);
  assert.ok(Math.abs(holeAt(fv) - 90) < 1e-6, 'the front view puts the hoop on top');
  assert.ok(fv.checks.pixels && fv.checks.holes && fv.checks.area && fv.checks.detailDropped);
  assert.equal(G.viewOptionsFor({ face: 'front' }).face, 'front'); assert.equal('face' in G.viewOptionsFor({}), false);
  assert.equal(G.viewOptionsFor({ face: 'front', entry: { upSource: 'operator', upAngle: 33 } }).upAngle, undefined); assert.equal(G.viewOptionsFor({ entry: { upSource: 'operator', upAngle: 33 } }).upAngle, 33);
  const bar = charm(rect(0, 0, 58, 10), [circ(1.5, 9, 2), circ(56.5, 9, 2)]), fb = G.backView(bar, { res: 6, face: 'front' }), bb = fb.outline.bbox;
  assert.ok((bb[2] - bb[0]) / (bb[3] - bb[1]) > 5, 'a long charm lies flat in the front view'); assert.ok(Math.abs(fb.angleDeg) < 1e-6);
  // the front keeps its words off the design's own engraved art (a filled shape), the back view is not asked to
  const art = charm(rect(0, 0, 60, 60), [], [rect(20, 20, 40, 40)]);
  const fa = G.backView(art, { res: 6, face: 'front' }), mAvoid = G.engraveMask(fa, { marginMm: .8 }), mPlain = G.engraveMask(Object.assign({}, fa, { face: undefined }), { marginMm: .8 });
  assert.ok(G.area(mAvoid) < G.area(mPlain), 'the shape on the front is kept clear (' + G.area(mAvoid) + ' < ' + G.area(mPlain) + ')');
  assert.equal(G.area(G.engraveMask(bv, { marginMm: .8 })), G.area(G.engraveMask(G.backView(disc, { res: 6 }), { marginMm: .8 })));
});

/* 15 ── readiness */
const Readiness = require(path.join(__dirname, '../../charm-nest-readiness.js'));
ok('readiness: faces', () => {
  const mk = o => Object.assign({ id: 's1', poolIds: ['1_2_1'], placedCount: 1, orders: ['1'] }, o);
  const rec = (face, at, ok) => Object.assign({ poolId: '1_2_1', approvedAt: at, approvedBy: 'P' }, ok === false ? {} : { verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: 'x', url: 'https://x/y' } } }, face ? { face } : {});
  const fc = (f, b) => ({ needed: true, state: 'written', approved: true, faces: { front: { needed: true, state: f, approved: ['approved', 'written'].includes(f) }, back: { needed: true, state: b, approved: ['approved', 'written'].includes(b) } } });
  const only = Readiness.sheet(mk({ engraving: { '1_2_1': { needed: true, state: 'written', approved: true } }, backPool: [rec(null, 5)] }));
  assert.deepEqual([only.approved, only.waiting, only.saved, only.required], [1, 0, 1, 1]);
  const pend = Readiness.sheet(mk({ engraving: { '1_2_1': Object.assign(fc('review', 'written'), { state: 'review', approved: false }) }, backPool: [rec(null, 5)] }));
  assert.deepEqual([pend.approved, pend.waiting, pend.saved], [0, 1, 0]);                                  // the back is saved, the front is not approved: the piece waits
  const both = Readiness.sheet(mk({ engraving: { '1_2_1': fc('written', 'written') }, backPool: [rec(null, 5), rec('front', 6)] }));
  assert.deepEqual([both.approved, both.waiting, both.saved, both.stages.approval, both.stages.backs], [1, 0, 1, true, true]);
  const nofile = Readiness.sheet(mk({ engraving: { '1_2_1': fc('written', 'written') }, backPool: [rec(null, 5), rec('front', 6, false)] }));
  assert.deepEqual([nofile.approved, nofile.saved, nofile.stages.approval, nofile.stages.backs], [1, 0, true, false]);   // approved, but the front file is not saved yet
  const plainFront = Readiness.sheet(mk({ engraving: { '1_2_1': { needed: false, state: 'skipped', approved: true } }, backPool: [] })); assert.equal(plainFront.plain, 1);
  const dec = Readiness.decisions([{ key: '1_2', state: 'pooled', poolIds: ['1_2_1'], spec: { engraveCandidate: true }, engrave: { needed: true, state: 'review', approved: false, pieces: { '1_2_1': { needed: true, state: 'review', approved: false, faces: { front: { needed: true, state: 'review', approved: false }, back: { needed: true, state: 'approved', approved: true } } } } } }]);
  assert.deepEqual(dec['1_2_1'].faces.front, { needed: true, state: 'review', approved: false }); assert.equal(dec['1_2_1'].state, 'review');
  const old = Readiness.decisions([{ key: '1_2', state: 'pooled', poolIds: ['1_2_1'], spec: { engraveCandidate: true }, engrave: { needed: true, state: 'review', approved: false } }]); assert.deepEqual(old['1_2_1'], { needed: true, state: 'review', approved: false });
});

/* 16 ── the reading the intake marks for a person, and words for each ear (ENGINTAKE's shapes) */
ok('setup: a reading marked for a person stays so until a person speaks; a mode-only change keeps the flag only for a face still without words', () => {
  const eng = { v: 1, mode: 'both', source: 'option', front: { text: 'W' }, back: { text: 'B' }, needsPerson: true, reviewKind: 'conflict', why: 'two options disagree', raw: 'x' };
  const spec = { engraveCandidate: true, engraving: eng };
  const s = F.setupOf({ spec }); assert.equal(s.needsPerson, true); assert.equal(s.reviewKind, 'conflict'); assert.equal(s.raw, 'x'); assert.equal(s.why, 'two options disagree'); assert.deepEqual(F.seedFor(s, 'front').lines, ['W']);
  assert.equal(F.setupOf({ spec, engravingOverride: { mode: 'back', by: 'P', at: 1 } }).needsPerson, false, 'the person chose the mode and the back has its words');
  assert.equal(F.setupOf({ spec, engravingOverride: { front: { text: 'Z' }, by: 'P', at: 1 } }).needsPerson, false, 'the person gave words');
  assert.equal(F.setupOf({ spec: { engraving: Object.assign({}, eng, { mode: 'back', front: null, back: null }) } }).needsPerson, true);
  assert.equal(F.setupOf({ spec: { engraving: Object.assign({}, eng, { front: null, back: null }) }, engravingOverride: { mode: 'front', by: 'P', at: 1 } }).needsPerson, true, 'the front is still without words');
  const nn = F.setupOf({ spec: { engraveCandidate: true, engraving: { v: 1, mode: 'none', source: 'option', needsPerson: true, reviewKind: 'conflict', why: 'the option says blank but the buyer wrote words' } } });
  assert.deepEqual(nn.faces, []); assert.equal(nn.needsPerson, true); assert.equal(nn.mode, 'none');
});
ok('setup: words for each ear / disc', () => {
  const eng = { v: 1, mode: 'both', source: 'note', needsPerson: true, reviewKind: 'pieces', why: 'each ear has its own words', pieces: { L: { front: { text: 'A' }, back: { text: '1' } }, R: { front: { text: 'B' }, back: { text: '2' } } } };
  const s = F.setupOf({ spec: { engraving: eng } });
  assert.deepEqual(Object.keys(s.pieces), ['L', 'R']); assert.deepEqual(s.pieces.R.front.lines, ['B']);
  assert.deepEqual(F.seedFor(s, 'front', 'R').lines, ['B']); assert.deepEqual(F.seedFor(s, 'back', 'L').lines, ['1']); assert.equal(F.seedFor(s, 'front'), null, 'no words for the line as a whole');
  assert.equal(F.describe(s), 'Left (Front: A · Back: 1) · Right (Front: B · Back: 2)');
  assert.equal(F.setupOf({ spec: { engraving: eng }, engravingOverride: { front: { text: 'Z' }, by: 'P', at: 1 } }).pieces, null, 'a person\'s words for the line replace the ears\' own');
  assert.equal(F.setupOf({ spec: { engraving: Object.assign({}, eng, { mode: 'front' }) } }).pieces.L.back, null);
  assert.equal(F.setupOf({ spec: ORDER_4195966039.spec }).pieces, null);
});

console.log(`Engrave faces OK: ${n} groups (keys, setup, plan, pieces, status, gate, records, panel rows, sides, rows, switch, seals, backs, geometry, readiness)`);
