// KEYALPHA + KEYNOENG (Paul, 17 Oct 2026, order 4196617547, listing 1099225598 "Key Necklace ... Key to My Heart", line SKU KEY_5598, "Initial: A" of an A to Z drop-down, "AT" in the personalisation box):
//   "These Key orders NEVER get engraved. When a user chooses a letter all it represents is the letter that must be inside the key. I already asked you to correctly match each key SKU to the letter
//    chosen by the buyer from the drop-down menu."
// The master now holds the keys by their letter, HEARTKEY_NECKLACE_A ... HEARTKEY_NECKLACE_Z (a family of the alphabet itself, not numbered). So:
//   · the letter the buyer chose finds the key of that letter, whatever SKU the line carries (the base, another letter's, or one saved for the listing for every letter);
//   · a key that already is the chosen letter is just that: the option is answered, no "no master design for letter A" question (the order was sent to Review for it);
//   · a design that carries its letter is NEVER engraved: spec.engraveNever, no engraving candidate (the "AT" stays what the customer wrote);
//   · numbered letter families (INITIAL_NECKLACE_SCRIPT-0 ... -25) read as before, and are not made never-engraved.
//   node tests/charm-nest/key-letter-families.cjs
const assert = require('assert/strict'), path = require('path');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const A_Z = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';

const lib = new Map();
for (const L of A_Z) lib.set('HEARTKEY_NECKLACE_' + L, { sku: 'HEARTKEY_NECKLACE_' + L });
for (let i = 0; i < 26; i++) lib.set('INITIAL_NECKLACE_SCRIPT-' + i, { sku: 'INITIAL_NECKLACE_SCRIPT-' + i });
for (const s of ['KEY_5598', 'KEY_6641', 'T_2800']) lib.set(s, { sku: s });
const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const entryFor = s => lib.get(String(s || '').toUpperCase()) || null;
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: entryFor, masterLoose: s => loose[O.looseKey(s)] || '' }, extra || {});
const order = rid => ({ receiptId: String(rid), updateTs: 1, staffNote: '', buyerMessage: '' });
let tx = 6100000000;
const line = (listing, sku, title, vars, extra) => Object.assign({ transactionId: String(++tx), listingId: listing, productId: String(++tx), sku, title, quantity: 1, metalKey: 'gold', metalLabel: '', personalization: [], buyerMessage: '', variations: vars.map(([name, value, propertyId, valueId]) => ({ name, value, propertyId: propertyId || null, valueId: valueId || null })) }, extra || {});
const KEYTITLE = 'Key Necklace Personalized Gift for Her Initial Jewelry Key Pendant Key to My Heart Charm Necklace';
const keyLine = (letter, sku, extra) => line('1099225598', sku, KEYTITLE, [['Finish', 'Gold • 14" • heart', '513', '105'], ['Initial', letter, '514', 'v-' + letter]], Object.assign({ personalization: ['AT'] }, extra));
const read = (l, extra) => O.interpretLine(order('4196617547'), l, ctx(extra));
const asks = sp => sp.problems.filter(p => p.kind === 'needsMapping' || p.kind === 'unmatchedSku' || p.kind === 'blockedSku');
const step = name => console.log('  ✓', name);

// 1 · the family
{
  const me = entryFor;
  eq(O.letterFamily('HEARTKEY_NECKLACE_A', me), { base: 'HEARTKEY_NECKLACE_', n: 0, own: true, lettered: true });
  eq(O.letterFamily('heartkey_necklace_t', me), { base: 'HEARTKEY_NECKLACE_', n: 19, own: true, lettered: true });
  eq(O.letterFamily('HEARTKEY_NECKLACE', me), { base: 'HEARTKEY_NECKLACE_', n: -1, own: false, lettered: true }, 'the base of the family');
  eq(O.letterFamily('INITIAL_NECKLACE_SCRIPT-3', me), { base: 'INITIAL_NECKLACE_SCRIPT-', n: 3, own: true }, 'a numbered family is read as before');
  eq(O.letterFamily('KEY_5598', me), null); eq(O.letterFamily('T_2800', me), null, 'one design with a letter-like end is no family');
  step('HEARTKEY_NECKLACE_A ... _Z is a lettered family (a member, or its base); numbered families and other SKUs are as before');
}
// 2 · the order in the screenshots: the line says HEARTKEY_NECKLACE_A and the buyer chose A
{
  const sp = read(keyLine('A', 'HEARTKEY_NECKLACE_A'));
  eq(sp.designSku, 'HEARTKEY_NECKLACE_A'); eq(asks(sp), [], 'no "no master design for letter A" question: ' + JSON.stringify(sp.problems));
  ok(sp.options.every(o => o.mapped) || sp.options.filter(o => o.name === 'Initial').every(o => o.mapped && o.mapped.field === 'design'), 'the letter option is answered by the key itself: ' + JSON.stringify(sp.options.filter(o => o.name === 'Initial')));
  ok(sp.engraveNever && sp.engraveNever.rule === 'letter-design' && /never engraved/.test(sp.engraveNever.why), 'the key is never engraved: ' + JSON.stringify(sp.engraveNever));
  ok(sp.engraveCandidate === false, '"AT" in the box is not an engraving candidate'); eq(sp.engraveNever.note, ['AT'], 'it stays what the customer wrote');
  step('Initial: A on HEARTKEY_NECKLACE_A: read, answered, never engraved, "AT" kept as the customer\'s words');
}
// 3 · the chosen letter finds its key, whatever SKU the line carries
for (const [sku, how] of [['HEARTKEY_NECKLACE_A', 'one letter saved for every buyer'], ['HEARTKEY_NECKLACE_Z', 'another letter\'s key'], ['HEARTKEY_NECKLACE', 'the base']]) {
  const sp = read(keyLine('T', sku));
  eq(sp.designSku, 'HEARTKEY_NECKLACE_T', `Initial: T on ${how}: the T key`); eq(asks(sp), [], 'nothing asked'); ok(sp.engraveNever && sp.engraveCandidate === false, 'never engraved');
}
{
  const aliases = { 1099225598: { sku: 'HEARTKEY_NECKLACE_A', bySku: { KEY_5598: 'HEARTKEY_NECKLACE_A' } } };   // (a person's "Use this charm" for the listing's own SKU, the answer for ONE letter that is read for all)
  for (const L of ['A', 'M', 'T', 'Z']) { const sp = read(keyLine(L, 'KEY_5598'), { aliases }); eq(sp.designSku, 'HEARTKEY_NECKLACE_' + L, `the saved answer for the SKU does not pin the key to A: Initial: ${L}`); eq(asks(sp), []); }
  step('the letter the buyer chose picks the key: from the base, from another letter\'s key, and over one answer saved for the listing');
}
// 4 · a letter whose key the master does not hold waits with one plain line; nothing is guessed
{
  lib.delete('HEARTKEY_NECKLACE_Q');
  const sp = read(keyLine('Q', 'HEARTKEY_NECKLACE_A'));
  const q = asks(sp); eq(q.length, 1, JSON.stringify(sp.problems)); ok(/no master design for letter Q/.test(q[0].letter.why), q[0].letter.why);
  ok(sp.designSku !== 'HEARTKEY_NECKLACE_Q', 'no key is invented');
  lib.set('HEARTKEY_NECKLACE_Q', { sku: 'HEARTKEY_NECKLACE_Q' });
  step('a missing key (Q) is one plain question, never another letter\'s key');
}
// 5 · numbered families and every other line are untouched
{
  const l = line('777', 'INITIAL_NECKLACE_SCRIPT-0', 'Initial Necklace', [['Initial', 'A']], { personalization: ['Engrave me'] });
  const sp = read(l); ok(!sp.engraveNever, 'a numbered letter family is not a "never engraved" design'); ok(sp.engraveCandidate === true, 'its words are still read for the back as ever');
  const plain = line('888', 'T_2800', 'Letter T charm', [['Metal', 'Gold']], { personalization: ['Mom'] });
  const sp2 = read(plain); ok(!sp2.engraveNever && sp2.engraveCandidate === true, 'an ordinary design is engraved as ever');
  step('a numbered letter family and an ordinary charm keep their engraving');
}
console.log('key-letter-families: ' + n + ' checks passed');
