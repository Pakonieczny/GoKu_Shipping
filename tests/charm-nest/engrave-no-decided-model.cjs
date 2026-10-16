// NODECIDED (Paul, 16 Oct 2026: "The user should be free to update/modify any already Approved engraving order and simply need to press the 'Approve' button again"): the MODEL side, node only
// (charm-nest-engrave-setup.js with a stand-in for the page's jobs; no jsdom, no browser, no network). The page side (the transition itself, the card, the list) is tests/charm-nest/engrave-no-decided.cjs.
//   1  a decided face (approved, saved to its sheet, cut plain) is no longer LOCKED in the setup editor: its words and font can be edited (locked false), it says it is decided (decided, lockState, lockBy)
//   2  the save path (apply): words a person changed on an approved / saved / cut-plain face call env.reopenForEdit(job) once and then seed the new words; the job a person did not touch, the same words
//      again, a face with no engraving ('none') and a face that is busy are left alone (a busy one is told in res.kept)
//   3  a decided face on a line that is already cut can still be edited; only taking a decided face OUT of the setup stays refused there (the old rule)
//   node tests/charm-nest/engrave-no-decided-model.cjs
'use strict';
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const F = require(path.join(root, 'charm-nest-engrave-faces.js'));
const S = require(path.join(root, 'charm-nest-engrave-setup.js'));
let n = 0;
const ok = (msg, cond) => { assert.ok(cond, msg); n++; };
const eq = (msg, a, b) => { assert.deepEqual(a, b, msg); n++; };
const LK = '4195966039_5009123456789';
const reading = (x = {}) => Object.assign({ v: 1, mode: 'both', source: 'option', front: { text: 'W', lines: ['W'], fontWord: null }, back: { text: '1. 27. 24', lines: ['1. 27. 24'], fontWord: null }, textSource: 'labelled', why: 'test', confidence: .95 }, x);
const mkLine = (o = {}) => Object.assign({ key: LK, state: 'pooled', order: { receiptId: '4195966039' }, line: { transactionId: '5009123456789' }, poolIds: [LK + '_1'],
  spec: Object.assign({ designSku: '5X8.5', engraveCandidate: true, personalization: ['front - W', 'back - 1. 27. 24'], font: { asked: 'Ace', id: 'jost', name: 'Jost', source: 'option' }, engraving: reading() }, o.spec || {}) }, o.row || {});
const job = (face, state, lines, extra = {}) => Object.assign({ key: F.jobKey(LK, null, face), face, slot: null, state, lines: lines.slice(), text: lines.join('\n') }, extra);
const fakeEnv = (jobs, o = {}) => {
  const log = [];
  return { log, who: () => 'Ana_M', now: () => 1760190000000,
    store: (l, ov) => { if (ov) l.engravingOverride = ov; else delete l.engravingOverride; },
    jobsOf: () => jobs.slice(), ensureJobs: () => jobs, slots: () => [null],
    remove: j => { log.push(['remove', j.key]); jobs.splice(jobs.indexOf(j), 1); }, skip: async (j, by) => { log.push(['skip', j.key, by]); j.state = 'skipped'; },
    setNone: j => { log.push(['none', j.key]); j.state = 'none'; }, reopen: j => { log.push(['reopen', j.key]); j.state = 'words'; },
    reopenForEdit: o.noHook ? undefined : j => { log.push(['reopenForEdit', j.key, j.state]); if (o.busy) return false; j.state = 'review'; return true; },
    seed: async (j, seed) => { log.push(['seed', j.key, seed.lines.join('/')]); j.lines = seed.lines.slice(); j.text = seed.text; }, changed: () => log.push(['changed']) };
};
(async () => {
  console.log('NODECIDED model');
  /* 1 ── no lock */
  for (const state of ['approved', 'written', 'skipped']) {
    S.DRAFTS && S.DRAFTS.clear && S.DRAFTS.clear();
    const line = mkLine(), jobs = [job('front', 'review', ['W']), job('back', state, ['1. 27. 24'], { approvedBy: 'Cy' })], vm = S.read(line, { jobs }), back = vm.rows.find(r => r.face === 'back'), front = vm.rows.find(r => r.face === 'front');
    ok(`a ${state} face is not locked: its words and font can be edited`, back.locked === false);
    ok(`a ${state} face says it is decided, by whom and how`, back.decided === true && back.lockState === state && back.lockBy === 'Cy');
    ok('a face still to settle is not marked decided', front.decided === false && front.locked === false);
  }
  /* 2 ── the save path */
  for (const state of ['approved', 'written']) {
    const line = mkLine(), jobs = [job('front', 'review', ['W']), job('back', state, ['1. 27. 24'], { approvedBy: 'Cy' })], env = fakeEnv(jobs), res = await S.apply(line, { back: { text: '2. 3. 25' } }, env);
    ok(`${state}: saved`, res.ok);
    eq(`${state}: the face is reopened for Approve first, then takes the new words`, env.log.filter(x => x[0] === 'reopenForEdit' || x[0] === 'seed').map(x => x.slice(0, 3)), [['reopenForEdit', F.jobKey(LK, null, 'back'), state], ['seed', F.jobKey(LK, null, 'back'), '2. 3. 25']]);
    eq(`${state}: the result says which job was re-seeded`, res.seeded, [F.jobKey(LK, null, 'back')]);
    ok(`${state}: the other face is untouched`, !env.log.some(x => x[1] === F.jobKey(LK, null, 'front') && (x[0] === 'seed' || x[0] === 'reopenForEdit')));
    ok(`${state}: nothing is cut plain or removed`, !env.log.some(x => ['skip', 'remove', 'none', 'reopen'].includes(x[0])));
  }
  { // cut plain: the person puts words in it: reopened (the old seals stay), then seeded
    const line = mkLine(), jobs = [job('front', 'review', ['W']), job('back', 'skipped', ['1. 27. 24'], { decidedBy: 'Cy' })], env = fakeEnv(jobs), res = await S.apply(line, { back: { text: 'New' } }, env);
    eq('skipped: reopened for edit, then seeded', env.log.filter(x => x[0] === 'reopenForEdit' || x[0] === 'seed').map(x => x[0]), ['reopenForEdit', 'seed']); ok('skipped: saved', res.ok);
  }
  { // the front is edited: the approved back is never touched
    const line = mkLine(), jobs = [job('front', 'review', ['W']), job('back', 'approved', ['1. 27. 24'])], env = fakeEnv(jobs); await S.apply(line, { front: { text: 'X' } }, env);
    ok('an edit of the front leaves the approved back alone', !env.log.some(x => x[0] === 'reopenForEdit') && env.log.some(x => x[0] === 'seed' && x[1] === F.jobKey(LK, null, 'front')));
  }
  { // the same words again: nothing changes
    const line = mkLine(), jobs = [job('front', 'review', ['W']), job('back', 'approved', ['1. 27. 24'])], env = fakeEnv(jobs); await S.apply(line, { back: { text: '1. 27. 24' } }, env);
    ok('the same words saved again do not reopen an approved piece', !env.log.some(x => x[0] === 'reopenForEdit' || x[0] === 'seed'));
  }
  { // a 'none' face is never seeded
    const line = mkLine(), jobs = [job('front', 'review', ['W']), job('back', 'none', [])], env = fakeEnv(jobs); await S.apply(line, { back: { text: 'Z' } }, env);
    ok('a face with no engraving is not reopened by a words edit', !env.log.some(x => x[0] === 'reopenForEdit'));
  }
  { // busy (being approved / written): not reopened, said in res.kept, not seeded
    const line = mkLine(), jobs = [job('front', 'review', ['W']), job('back', 'approved', ['1. 27. 24'])], env = fakeEnv(jobs, { busy: true }), res = await S.apply(line, { back: { text: '2. 3. 25' } }, env);
    ok('busy: not seeded', !env.log.some(x => x[0] === 'seed' && x[1] === F.jobKey(LK, null, 'back'))); eq('busy: told in res.kept', res.kept, [F.jobKey(LK, null, 'back')]);
  }
  { // an older page with no hook: an approved face is left as it always was
    const line = mkLine(), jobs = [job('front', 'review', ['W']), job('back', 'approved', ['1. 27. 24'])], env = fakeEnv(jobs, { noHook: true }), res = await S.apply(line, { back: { text: '2. 3. 25' } }, env);
    ok('no hook: nothing is seeded into an approved face', !env.log.some(x => x[0] === 'seed' && x[1] === F.jobKey(LK, null, 'back')) && res.ok);
  }
  /* 3 ── a line that is already cut */
  { const line = mkLine({ row: { state: 'written' } }), jobs = [job('front', 'review', ['W']), job('back', 'approved', ['1. 27. 24'])], env = fakeEnv(jobs), res = await S.apply(line, { back: { text: '2. 3. 25' } }, env);
    ok('a decided face on a cut line can have its words edited (reopened for Approve)', res.ok && env.log.some(x => x[0] === 'reopenForEdit'));
    const env2 = fakeEnv([job('front', 'review', ['W']), job('back', 'approved', ['1. 27. 24'])]), r2 = await S.apply(mkLine({ row: { state: 'written' } }), { mode: 'front' }, env2);
    ok('taking a decided face OUT of the setup of a cut line is still refused', !r2.ok && /already cut/.test(r2.message || '')); }
  console.log(`  ${n} checks passed`);
})().catch(e => { console.error('  FAIL ' + (e && e.stack || e)); process.exit(1); });
