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

console.log(`Engrave faces OK: ${n} groups (keys, setup, plan, pieces, status, gate, records, panel rows)`);
