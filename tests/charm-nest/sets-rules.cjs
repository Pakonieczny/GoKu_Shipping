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

// ── Paul, 12 Oct 2026: a set FORMS as soon as it holds a completed GF and a completed SS sheet, then it is CLOSED to automatic joining; the next completed sheets form the NEXT set ──
// closed(): the one closing rule
assert.strictEqual(R.CLOSING.afterFormation, true);
assert.strictEqual(R.closed([]), false, 'an empty set is still forming');
assert.strictEqual(R.closed([gfDone('a')]), false, 'a completed GF alone has not formed a set');
assert.strictEqual(R.closed([gfDone('a'), sh('p', 'silver')]), false, 'a partial SS does not count');
assert.strictEqual(R.closed([gfDone('a'), ssDone('b')]), true, 'a completed GF and a completed SS: the set has formed, it takes no sheet on its own');
assert.strictEqual(R.closed([gfDone('a'), ssDone('b'), sh('r', 'rose'), sh('p', 'gold')]), true);
assert.strictEqual(R.closed(null), false);
R.CLOSING.afterFormation = false; assert.strictEqual(R.closed([gfDone('a'), ssDone('b')]), false, 'the one switch: a set keeps taking sheets until it is approved (the older reading)'); R.CLOSING.afterFormation = true;
// form(): where the free sheets go now
let f = R.form([], [gfDone('g1'), ssDone('s1'), sh('p', 'gold')], false);
assert.deepStrictEqual([f.where, f.join.length, f.wait.length, f.closed], ['new', 3, 0, false], 'no set yet: the completed GF and SS form Set 1 and the partial sheet that comes with them (the cardinal rule) joins');
f = R.form([], [gfDone('g1'), sh('s0', 'silver')], false);
assert.deepStrictEqual([f.where, f.join.length, f.wait.length], [null, 0, 2], 'a completed GF with only a partial SS: no set, both wait');
assert.ok(/^Waits for a completed SS sheet/.test(f.wait[0].why), f.wait[0].why);
f = R.form([], [sh('p1', 'gold'), sh('p2', 'silver')], false); assert.deepStrictEqual([f.where, f.join.length], [null, 0], 'partial sheets never start a set');
f = R.form([gfDone('g1')], [ssDone('s1')], true); assert.deepStrictEqual([f.where, f.join.length, f.closed], ['last', 1, false], 'the newest set has not formed yet: the completed SS completes it');
f = R.form([gfDone('g1'), ssDone('s1')], [gfDone('g2'), ssDone('s2')], true, 'Set-1');
assert.deepStrictEqual([f.where, f.join.length, f.closed], ['new', 2, true], 'Set-1 has formed: the following completed pair forms the NEXT set');
f = R.form([gfDone('g1'), ssDone('s1')], [gfDone('g2')], true, 'Set-1');
assert.deepStrictEqual([f.where, f.join.length, f.wait.length], [null, 0, 1], 'a lone completed GF after Set-1 formed waits outside every set');
assert.strictEqual(f.wait[0].why, 'Set-1 has formed, so this sheet waits for the next set: it needs a completed SS sheet with it', f.wait[0].why);
assert.strictEqual(R.nextSetWords('', ['GF']), 'The set has formed, so this sheet waits for the next set: it needs a completed GF sheet with it');
f = R.form([gfDone('g1'), ssDone('s1')], [], true); assert.deepStrictEqual([f.where, f.join.length, f.wait.length], [null, 0, 0]);
// partner words and the gate: a set that formed while a partner sheet cannot join yet is not ready for laser cutting, with that one line
assert.strictEqual(R.partnerWords(['RG Draft 1']), 'orders shared with RG Draft 1: add that sheet to this set');
assert.strictEqual(R.partnerWords(['RG Draft 1', 'GF Draft 4']), 'orders shared with RG Draft 1 and GF Draft 4: add those sheets to this set');
const members = [gfDone('g1'), ssDone('s1')];
let g = R.gate({ setId: 's', status: 'awaiting review' }, members);
assert.deepStrictEqual([g.applies, g.ok], [true, true], 'valid and no partner outside: nothing in the way');
g = R.gate({ setId: 's', status: 'awaiting review', cardinalWait: [{ label: 'RG Draft 1', id: 'rose-1' }] }, members);
assert.deepStrictEqual([g.applies, g.ok, g.missing], [true, false, []]);
assert.strictEqual(g.short, 'orders shared with RG Draft 1: add that sheet to this set');
assert.strictEqual(g.line, 'Waiting: orders shared with RG Draft 1: add that sheet to this set, so that orders are not spread over two sets.');
g = R.gate({ setId: 's', status: 'awaiting review', cardinalWait: [{ label: 'RG Draft 1' }] }, [gfDone('g1'), sh('p', 'silver')]);
assert.deepStrictEqual([g.ok, g.missing], [false, ['SS']], 'a set short of a completed SS sheet says that first');
g = R.gate({ setId: 's', status: 'complete', committedAt: 5, cardinalWait: [{ label: 'RG Draft 1' }] }, members); assert.strictEqual(g.applies, false, 'a set committed before is never blocked afterwards');
g = R.gate({ setId: 's', cardinalWait: [] }, members); assert.strictEqual(g.ok, true, 'an empty list is no partner');

console.log('sets-rules: ok');
