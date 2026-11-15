/* FOLLOWASK: asksOf separates the shop's instructions from the words to engrave. node tests/charm-nest/engrave-asks.cjs */
'use strict';
const assert = require('node:assert/strict'), A = require('../../charm-nest-engrave-asks.js');
const T = [
  // [input, words, asks as "type:value", unsure]
  ['Anna (on the left side please)', 'Anna', ['position:left'], false],
  ['left side: Anna', 'Anna', ['position:left'], false],
  ['Anna - small font, all caps', 'Anna', ['case:upper', 'size:small'], false],
  ['please put it on the right\nAnna', 'Anna', ['position:right'], false],
  ['Anna\ncentered', 'Anna', ['position:center'], false],
  ['two lines: Anna / Marie', 'Anna\nMarie', ['layout:two-lines'], false],
  ['Anna on the left side', 'Anna', ['position:left'], false],
  ['Anna\nall lowercase', 'Anna', ['case:lower'], false],
  ['Anna [one line]', 'Anna', ['layout:one-line'], false],
  ['Anna, bigger', 'Anna', ['size:large'], false],
  ['Left my heart in Rome', 'Left my heart in Rome', [], false],
  ['Top Gun', 'Top Gun', [], false],
  ['Big Sur', 'Big Sur', [], false],
  ['small town girl', 'small town girl', [], false],
  ['Mary-Jane', 'Mary-Jane', [], false],
  ['Right', 'Right', [], true],
  ['LEFT', 'LEFT', [], true],
  ['left and right\nAnna (right side)', 'left and right\nAnna (right side)', [], true],
  ['https://etsy.com/x', '', ['reference:https://etsy.com/x'], false],
  ['Anna https://i.imgur.com/a.png', 'Anna', ['reference:https://i.imgur.com/a.png'], false],
];
let n = 0;
for (const [i, w, a, u] of T) {
  const r = A.asksOf(i);
  assert.equal(r.words, w, JSON.stringify(i)); assert.deepEqual(r.asks.map(x => x.type + ':' + x.value).sort(), a.slice().sort(), JSON.stringify(i)); assert.equal(r.unsure, u, JSON.stringify(i)); n++;
}
// arithmetic: plan and the thirds of the safe area
assert.deepEqual(A.plan([{ type: 'position', value: 'left' }, { type: 'position', value: 'right' }, { type: 'reference', value: 'x' }, { type: 'size', value: 'small' }]), { position: 'left', size: 'small' });
const b = { x0: 0, x1: 30, y0: 0, y1: 60 };
assert.deepEqual(A.thirdOf('left', b), { axis: 'x', to: 10 }); assert.deepEqual(A.thirdOf('right', b), { axis: 'x', to: 20 });
assert.deepEqual(A.thirdOf('top', b), { axis: 'y', to: 40 }); assert.deepEqual(A.thirdOf('bottom', b), { axis: 'y', to: 20 });
assert.deepEqual(A.caseLines(['Anna Marie'], 'upper'), ['ANNA MARIE']); assert.equal(A.layoutMode('two-lines', ['Anna Marie']), '2'); assert.equal(A.layoutMode('two-lines', ['a', 'b']), 'preserve'); assert.equal(A.layoutMode('one-line', ['a', 'b']), '1');
console.log('engrave-asks: ' + (n + 8) + ' checks passed');
