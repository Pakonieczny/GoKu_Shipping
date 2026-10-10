// Offline test of charm-nest-set-rules.js: the definition of a Completed sheet and of a valid Set (Paul, 10 Oct 2026).
'use strict';
const assert = require('assert');
const R = require('../../charm-nest-set-rules.js');

const sh = (id, metal, o = {}) => Object.assign({ id, metal, placedCount: 10, density: 0.5, draft: false, releaseFull: false }, o);
const gfDone = id => sh(id, 'gold', { releaseFull: true, density: 0.77 });
const ssDone = id => sh(id, 'silver', { releaseFull: true, density: 0.76 });

// metalClass: metal, the card's own code, a plain code
assert.strictEqual(R.metalClass({ metal: 'gold' }), 'GF');
assert.strictEqual(R.metalClass({ metal: 'silver' }), 'SS');
assert.strictEqual(R.metalClass({ metal: 'rose' }), 'RG');
assert.strictEqual(R.metalClass({ metal: 'gold10k' }), '10K');
assert.strictEqual(R.metalClass({ metal: 'gold14k' }), '14K');
assert.strictEqual(R.metalClass({ metalLabel: 'GF 14/20 · sheet 2' }), 'GF');
assert.strictEqual(R.metalClass({ metalLabel: 'SS · sheet 2' }), 'SS');
assert.strictEqual(R.metalClass({ metalLabel: '14K Gold · sheet 1' }), '14K');
assert.strictEqual(R.metalClass('GF'), 'GF');
assert.strictEqual(R.metalClass(null), '');

// completed: released (full), or cut; never by nest status, set membership or a draft flag alone
assert.strictEqual(R.isCompleted(gfDone('a')), true);
assert.strictEqual(R.isCompleted(sh('b', 'silver')), false, 'still filling');
assert.strictEqual(R.isCompleted(sh('c', 'silver', { status: 'complete', endedBy: 'complete', setId: 's1' })), false, 'status complete + in a set is not completed (the 57% SS of Set-1)');
assert.strictEqual(R.isCompleted(sh('d', 'gold', { status: 'partial', releaseFull: true, draft: true })), true, 'status partial + released + draft is a full sheet (the 75% GF)');
assert.strictEqual(R.isCompleted(sh('e', 'gold', { laserDoneAt: 1700000000000 })), true, 'cut by the laser');
assert.strictEqual(R.isCompleted(sh('f', 'rose', { roseCutAt: 1700000000000 })), true, 'rose cut recorded');
assert.strictEqual(R.isCompleted(sh('g', 'gold', { releaseFull: true, archived: true })), false);
assert.strictEqual(R.isCompleted(sh('h', 'gold', { releaseFull: true, placedCount: 0 })), false, 'nothing placed');
assert.strictEqual(R.isCompleted(sh('i', 'gold', { releaseFull: true, placedCount: undefined, placements: [{ id: 1 }] })), true);
assert.strictEqual(R.isCompleted(null), false);
assert.strictEqual(R.isCompleted(sh('j', 'gold', { releaseFull: 'false' })), false);

// validSet
let v = R.validSet([gfDone('a'), ssDone('b')]);
assert.deepStrictEqual([v.ok, v.missing, v.reason], [true, [], '']);
v = R.validSet([gfDone('a'), gfDone('a2'), sh('c', 'silver', { status: 'complete', setId: 's1' })]);   // Set-1 of the screenshot
assert.strictEqual(v.ok, false); assert.deepStrictEqual(v.missing, ['SS']);
assert.ok(/it has no completed SS sheet\.$/.test(v.reason), v.reason);
v = R.validSet([sh('x', 'gold'), sh('y', 'silver')]);
assert.deepStrictEqual(v.missing, ['GF', 'SS']);
assert.ok(/no completed GF sheet and no completed SS sheet/.test(v.reason), v.reason);
v = R.validSet([gfDone('a'), ssDone('b'), sh('r', 'rose'), sh('p', 'gold')]);   // partials and other metals may be in a valid set
assert.strictEqual(v.ok, true); assert.deepStrictEqual(v.have, { GF: 1, SS: 1 });
assert.strictEqual(R.validSet([]).ok, false);
assert.strictEqual(R.validSet(null).ok, false);
assert.strictEqual(R.validSet([ssDone('b'), sh('r', 'rose', { releaseFull: true })]).ok, false, 'RG never stands in for GF');

// wouldStayValid
const set = [gfDone('g1'), gfDone('g2'), ssDone('s1')];
assert.strictEqual(R.wouldStayValid(set, ['g1'], []).ok, true, 'one of two GF can leave');
let w = R.wouldStayValid(set, ['s1'], []);
assert.deepStrictEqual([w.ok, w.missing], [false, ['SS']], 'the only completed SS cannot leave');
w = R.wouldStayValid(set, ['s1'], [ssDone('s2')]);
assert.strictEqual(w.ok, true, 'swapped for another completed SS');
w = R.wouldStayValid(set, ['s1'], [sh('s3', 'silver')]);
assert.strictEqual(w.ok, false, 'a partial SS never takes the place of a completed one');
w = R.wouldStayValid([gfDone('g1')], [], [ssDone('s1')]);
assert.strictEqual(w.ok, true, 'a completed SS joining makes it valid'); assert.strictEqual(w.after.length, 2);
w = R.wouldStayValid(set, ['g1', 'g2'], []);
assert.deepStrictEqual(w.missing, ['GF']);
assert.strictEqual(R.wouldStayValid(set, [], []).ok, true);
assert.strictEqual(R.wouldStayValid(set, ['nope'], null).ok, true);
// the same sheet moved in again is counted once
assert.strictEqual(R.wouldStayValid(set, [], [ssDone('s1')]).after.length, 3);

// whyNotCompleted
assert.strictEqual(R.whyNotCompleted(gfDone('a')), '');
assert.ok(/still filling \(57% full\)/.test(R.whyNotCompleted(sh('c', 'silver', { density: 0.573, sheetIndex: 1 }))));
assert.ok(/SS Sheet 1/.test(R.whyNotCompleted(sh('c', 'silver', { sheetIndex: 1 }))));

console.log('sets-rules: ok');
