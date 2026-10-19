// NUMBERS (Paul, 19 Oct 2026, orders 4199087297 / 4193596549, listing 1871838409 "Number Charm Gold Number Pendant Add On Charm Personalized Jewelry Team Number Charm …"):
//   the listing's drop-downs "Metal Choice" and "Numbers" 0 … 9 carry the variation SKUs Necklace_Charm-0 … -9 (the NECKLACE row of the design file SKUs_CORRECTED_Numbers.ai); its Huggie option is the huggie version
//   of the number, labelled "Necklace_Charm-N (HUGGIE)" in the same file (the shop's huggie naming). Order 4199087297 (personalisation "6", Charm Type "Necklace CHARM", SKU "Comic Number") was read as COMIC NUMBER
//   (one drawing, a "4") and asked "Words: Check the requested inscription"; order 4193596549 (a Huggie CHARM SET, "7") was read as COMIC NUMBER (HUGGIE), a wrong drawing.
//   → the digit the buyer chose (drop-down, the line's own SKU, the box) picks the member, the form picks the family; the digit is never engraved; a person's answer for the line wins; a listing-wide answer or a link
//   to the generic COMIC NUMBER drawings no longer does; a member the library lacks waits with one plain line; a library without the family reads as before; every other listing reads as before.
//   node tests/charm-nest/number-charm-profile.cjs      Offline: no browser, no network, no Etsy call. The library names are the ones the labelled design file gives; no buyer data beyond the orders' choices.
'use strict';
const assert = require('assert'), path = require('path');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);

/* ── the world: the number family in both forms (one design file each), the old one-drawing COMIC NUMBER designs, and two unrelated designs ── */
const F = f => 'charmnest/master/' + f + '.ai';
function world(o = {}) {
  const lib = new Map(), put = (sku, file, w = 20, h = 33, holes = 1) => lib.set(sku.toUpperCase(), { sku: sku.toUpperCase(), aiPath: F(file), widthPt: w, heightPt: h, members: 2, holes, sizes: null, blocked: null });
  for (let i = 0; i < 10; i++) {
    if (!o.noFamily && !(o.noNeck || []).includes(i)) put('Necklace_Charm-' + i, 'SKUs_CORRECTED_Numbers_Necklace_Charm-' + i);
    if (!o.noFamily && !o.noHuggieFamily && !(o.noHug || []).includes(i)) put('Necklace_Charm-' + i + ' (HUGGIE)', 'SKUs_CORRECTED_Numbers_Necklace_Charm-' + i + '_HUGGIE_', 14, 27);
  }
  put('COMIC NUMBER', 'COMIC_NUMBER', 20.2, 32.9); put('COMIC NUMBER (HUGGIE)', 'COMIC_NUMBER_HUGGIE_', 13.9, 26.9);
  put('MAPLE_8065', 'MAPLE_8065', 30, 33); put('COMET_9427', 'COMET_9427', 30, 33);
  const byFile = new Map(); for (const [k, e] of lib) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
  const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
  return { masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: s => { const e = lib.get(String(s || '').toUpperCase()); return e ? byFile.get(e.aiPath) || [] : []; }, masterSkus: () => [...lib.keys()] };
}
const ctxOf = (w, extra) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {} }, w || world(), extra || {});
const W = world();
let tx = 7400000000;
const read = (line, c, order) => O.interpretLine(Object.assign({ receiptId: '4199000000', updateTs: 1, buyerMessage: '', staffNote: '' }, order || {}), JSON.parse(JSON.stringify(line)), c || ctxOf(W));
const kinds = sp => (sp.problems || []).map(p => p.kind);
const members = sp => (sp.pair && sp.pair.members || []).map(m => m.side + ':' + m.sku);

/* ── the lines as Etsy gives them ── */
const TITLE = 'Number Charm Gold Number Pendant Add On Charm Personalized Jewelry Team Number Charm Sports Team Jewelry Charm Gift for Mom';
// o: { num (the Numbers drop-down), sku, type (Charm Type), box (personalisation), listingId }
const line = (o = {}) => Object.assign({ transactionId: String(++tx), listingId: o.listingId || '1871838409', sku: o.sku != null ? o.sku : '', title: TITLE, quantity: o.q || 1, metalKey: 'gold', metalLabel: 'Gold Filled',
  personalization: o.box ? [o.box] : [], buyerMessage: o.note || '',
  variations: [{ name: 'Metal Choice', value: 'Gold Filled' }].concat(o.type ? [{ name: 'Charm Type', value: o.type }] : [], o.num != null ? [{ name: 'Numbers', value: String(o.num) }] : [], o.box ? [{ name: 'Personalization', value: o.box }] : []) });
const NECK = 'Necklace CHARM', HUG = 'Huggie CHARM SET';

/* ── 1 · the ten numbers: by the Numbers drop-down, by the line's own SKU, by the personalisation box ── */
for (let d = 0; d < 10; d++) {
  const want = 'NECKLACE_CHARM-' + d;
  for (const [how, ln] of [['drop-down + its SKU', line({ num: d, sku: 'Necklace_Charm-' + d })], ['drop-down alone', line({ num: d, sku: 'Comic Number' })], ['its SKU alone', line({ sku: 'Necklace_Charm-' + d })],
    ['the box (old listing, Comic Number)', line({ sku: 'Comic Number', type: NECK, box: String(d) })], ['the box, no SKU', line({ box: String(d) })]]) {
    const sp = read(ln);
    eq([sp.designSku, sp.skuSource, kinds(sp), sp.pieceCount, sp.lineForm === 'huggie'], [want, 'profile', [], 1, false], `${d} by ${how}: ${want}, one piece, no question`);
    eq([sp.listingProfile.profile, sp.listingProfile.form, sp.listingProfile.symbols.map(x => [x.name, x.sku])], ['number-charm', 'necklace', [[String(d), want]]], `${d} by ${how}: the number and its charm are kept as facts`);
    eq([sp.engraveCandidate, !!sp.engraveNever, sp.engraving || null], [false, true, null], `${d} by ${how}: no engraving`);
  }
  eq(read(line({ num: d, sku: 'Necklace_Charm-' + d })).options.find(o => o.name === 'Numbers').mapped, { field: 'design', value: want, source: 'rule:listing-profile' }, `${d}: the Numbers drop-down is answered by its charm (no option question)`);
}
eq(['#7', 'Number 7', 'No. 7', 'Seven', 'number seven please', '7 please'].map(b => read(line({ sku: 'Comic Number', type: NECK, box: b })).designSku), Array(6).fill('NECKLACE_CHARM-7'), 'the box: "#7", "Number 7", "No. 7", "Seven", "number seven please", "7 please"');
eq(read(line({ sku: 'Comic Number', type: NECK, box: '' , note: 'Number 3 please' })).designSku, 'NECKLACE_CHARM-3', 'nothing in the box or the drop-down: the buyer\'s note names the number');
eq(read(line({ num: 5, sku: 'Necklace_Charm-5', note: 'please ship within 2 days' })).designSku, 'NECKLACE_CHARM-5', 'a digit in the note is not read when the drop-down named one');
pass('the ten numbers by the drop-down, the SKU and the box are NECKLACE_CHARM-0 … -9, one piece, never engraved');

/* ── 2 · order 4199087297 as it is: SKU "Comic Number", Charm Type "Necklace CHARM", personalisation "6" ── */
{ const ln = line({ sku: 'Comic Number', type: NECK, box: '6' }), sp = read(ln, null, { receiptId: '4199087297' });
  eq([sp.designSku, sp.skuSource, kinds(sp), sp.pieceCount, sp.lineForm, sp.designForm], ['NECKLACE_CHARM-6', 'profile', [], 1, 'necklace', 'necklace'], 'order 4199087297: NECKLACE_CHARM-6, one necklace piece, no question');
  ok(sp.designSku !== 'COMIC NUMBER', 'never the generic COMIC NUMBER drawing (its "4")');
  eq([sp.engraveCandidate, sp.engraveNever && sp.engraveNever.rule, sp.engraveNever && sp.engraveNever.why, sp.engraving || null, sp.personalization], [false, 'number-choice', O.NUMBER_NEVER_WHY, null, ['6']], 'no "Check the requested inscription": the "6" is the buyer\'s choice, never engraved; the buyer\'s word is kept');
  eq(sp.pair.notes.filter(x => /^Number/.test(x)), ['Number: 6 (NECKLACE_CHARM-6), from the personalisation box'], 'the order\'s reading says the number and its charm once');
  eq(sp.listingProfile.was, { sku: 'COMIC NUMBER', source: 'transaction' }, 'and keeps the reading it replaced'); }
pass('order 4199087297: NECKLACE_CHARM-6, no words decision (was COMIC NUMBER and "Check the requested inscription")');

/* ── 3 · the HUGGIE option: the huggie version of the number, Left + Right ── */
for (let d = 0; d < 10; d++) {
  const want = 'NECKLACE_CHARM-' + d + ' (HUGGIE)';
  for (const [how, ln] of [['Huggie CHARM SET + drop-down + SKU', line({ type: HUG, num: d, sku: 'Necklace_Charm-' + d })], ['Huggie CHARM SET + SKU', line({ type: HUG, sku: 'Necklace_Charm-' + d })], ['Huggie CHARM SET + box (Comic Number)', line({ type: HUG, sku: 'Comic Number', box: String(d) })], ['a Numbers value naming the huggie', line({ num: 'Huggie ' + d, sku: 'Necklace_Charm-' + d })]]) {
    const sp = read(ln);
    eq([sp.designSku, sp.skuSource, kinds(sp), sp.pieceCount, sp.pair.sides, sp.lineForm, sp.designForm], [want, 'profile', [], 2, ['L', 'R'], 'huggie', 'huggie'], `${d} by ${how}: ${want} on both ears`);
    eq([sp.listingProfile.form, !!sp.engraveNever, sp.engraveCandidate], ['huggie', true, false], `${d} by ${how}: the huggie family, never engraved`);
  }
}
{ const sp = read(line({ type: HUG, sku: 'Comic Number', box: '7' }), null, { receiptId: '4193596549' });
  eq([sp.designSku, sp.skuSource, kinds(sp), sp.pieceCount, sp.pair.sides, sp.pair.earring], ['NECKLACE_CHARM-7 (HUGGIE)', 'profile', [], 2, ['L', 'R'], true], 'order 4193596549: the huggie 7 on the Left and on the Right');
  ok(sp.designSku !== 'COMIC NUMBER (HUGGIE)' && sp.designSku !== 'NECKLACE_CHARM-7', 'never COMIC NUMBER (HUGGIE), never the necklace drawing');
  eq(sp.pair.notes.filter(x => /^Number/.test(x)), ['Number: 7 (NECKLACE_CHARM-7 (HUGGIE), both ears), from the personalisation box'], 'the reading says it once');
  eq(sp.listingProfile.was, { sku: 'COMIC NUMBER (HUGGIE)', source: 'transaction' }, 'it replaced the COMIC NUMBER (HUGGIE) match'); }
// two different digits typed for a huggie: a pair, the first named the Left (the rune row's behaviour)
for (const box of ['7 and 3', '7 & 3', 'Left 7 right 3', 'seven and three']) {
  const sp = read(line({ type: HUG, sku: 'Comic Number', box }));
  eq([kinds(sp), members(sp), sp.pieceCount, sp.pair.sides], [[], ['L:NECKLACE_CHARM-7 (HUGGIE)', 'R:NECKLACE_CHARM-3 (HUGGIE)'], 2, ['L', 'R']], `huggie "${box}": Left 7, Right 3`);
}
{ const ln = line({ type: HUG, sku: 'Comic Number', box: '7 and 3' }), said = { 1871838409: { 'Two designs on this line': { ['4199000000/' + ln.transactionId]: { field: 'ignore', value: null } } } };
  eq([read(ln, ctxOf(W, { optionMaps: said })).designSku, members(read(ln, ctxOf(W, { optionMaps: said })))], ['NECKLACE_CHARM-7 (HUGGIE)', []], 'a person\'s "the same on both ears" for the line: the first named on both ears'); }
pass('the Huggie option: NECKLACE_CHARM-N (HUGGIE) on both ears (order 4193596549: the huggie 7); two digits typed are a pair');

/* ── 4 · the form decides the family, never the other form's drawing ── */
eq([O.formOfText('NECKLACE_CHARM-7 (HUGGIE)'), O.formOfText('NECKLACE_CHARM-7'), O.formOfText('COMIC NUMBER (HUGGIE)'), O.formOfText('NECKLACE STUD (HUGGIE)')], ['huggie', 'necklace', 'huggie', ''], '"X (HUGGIE)" is the huggie drawing of X even when X says necklace (a SKU naming two forms otherwise says nothing, as before)');
{ const neck = read(line({ num: 4, sku: 'Necklace_Charm-4', type: NECK })), hug = read(line({ num: 4, sku: 'Necklace_Charm-4', type: HUG }));
  eq([neck.designSku, hug.designSku], ['NECKLACE_CHARM-4', 'NECKLACE_CHARM-4 (HUGGIE)'], 'the same product SKU: the necklace option is the necklace charm, the huggie option its huggie version');
  eq([O.formClash(neck.lineForm, O.masterForm('NECKLACE_CHARM-4 (HUGGIE)', { masterEntry: W.masterEntry, masterShared: W.masterShared })), O.formClash(hug.lineForm, O.masterForm('NECKLACE_CHARM-4', { masterEntry: W.masterEntry, masterShared: W.masterShared }))], [true, true], 'and the form guard would stop either crossing'); }
pass('the form picks the family: necklace → NECKLACE_CHARM-N, huggie → NECKLACE_CHARM-N (HUGGIE)');

/* ── 5 · doubtful lines wait with one plain line ── */
{ const dis = read(line({ num: 6, sku: 'Necklace_Charm-6', box: '9' }));
  eq([dis.designSku, kinds(dis), dis.formHold.reason], [null, ['unmatchedSku'], 'Which number? The “Numbers” drop-down says 6 but the buyer\'s words name 9'], 'the drop-down and the box disagree: a person says');
  const two = read(line({ sku: 'Comic Number', type: NECK, box: '7 and 3' }));
  eq([two.designSku, two.formHold.reason], [null, 'Which number? The buyer names 7 and 3'], 'two digits for one necklace charm: a person says');
  const none = read(line({ sku: 'Comic Number', type: NECK, box: '' }));
  eq([none.designSku, none.formHold.reason, kinds(none)], [null, 'Which number? The buyer named none (the personalisation box is empty)', ['unmatchedSku']], 'no number at all: one plain question, never COMIC NUMBER');
  const team = read(line({ sku: 'Comic Number', type: NECK, box: '23' }));
  eq([team.designSku, team.formHold.reason], [null, 'Which number? The buyer named none (the personalisation box says “23”)'], 'a two-digit number is no single digit: a person says (never the 2 or the 3 alone)'); }
eq([O.digitsInText('23', false), O.digitsInText('14k gold', false), O.digitsInText('7mm', false), O.digitsInText('1/2', false), O.digitsInText('3rd', false), O.digitsInText('10/14/2026', false), O.digitsInText('one for my mom', false)], [[], [], [], [], [], [], []], 'never a digit inside "23", "14k", "7mm", "1/2", "3rd", a date, or "one" in a sentence');
eq([O.digitsInText('#7', false), O.digitsInText('Seven', false), O.digitsInText('One', true), O.digitsInText('Number 0', false)].map(r => r.map(x => x.name)), [['7'], ['7'], ['1'], ['0']], '"#7", "Seven", the drop-down "One", "Number 0"');
pass('a disagreeing, missing or two-digit number waits with one plain line');

/* ── 6 · saved answers: which stand, which the profile replaces ── */
{ // a listing-wide answer (the listing's design), a link under the shared SKU "Comic Number", the listing's huggie answer: all to the generic drawings, all replaced
  const lw = ctxOf(W, { aliases: { 1871838409: { sku: 'COMIC NUMBER', v: 2 } } });
  eq([read(line({ sku: 'Comic Number', type: NECK, box: '6' }), lw).designSku, read(line({ type: NECK, box: '6' }), lw).designSku], ['NECKLACE_CHARM-6', 'NECKLACE_CHARM-6'], 'the listing-wide saved answer COMIC NUMBER no longer wins');
  const by = ctxOf(W, { aliases: { 1871838409: { bySku: { 'COMIC NUMBER': 'COMIC NUMBER', 'COMIC NUMBER (HUGGIE)': 'COMIC NUMBER (HUGGIE)' } } } });
  eq([read(line({ sku: 'Comic Number', type: NECK, box: '6' }), by).designSku, read(line({ sku: 'Comic Number', type: HUG, box: '7' }), by).designSku], ['NECKLACE_CHARM-6', 'NECKLACE_CHARM-7 (HUGGIE)'], 'links saved under the shared SKU "Comic Number" (necklace and huggie set) no longer win');
  const hug = ctxOf(W, { aliases: { 1871838409: { huggie: 'COMIC NUMBER (HUGGIE)' } } });
  eq(read(line({ type: HUG, box: '7' }), hug).designSku, 'NECKLACE_CHARM-7 (HUGGIE)', 'the listing\'s huggie-set answer COMIC NUMBER (HUGGIE) no longer wins');
  const other = ctxOf(W, { aliases: { 1871838409: { bySku: { 'COMIC NUMBER': 'MAPLE_8065' } } } });
  eq(read(line({ sku: 'Comic Number', type: NECK, box: '6' }), other).designSku, 'NECKLACE_CHARM-6', 'a link under the shared SKU to any design is listing-wide: replaced (it would give every buyer one digit)');
  const ownComic = ctxOf(W, { aliases: { 1871838409: { bySku: { 'NECKLACE_CHARM-6': 'COMIC NUMBER' } } } });
  eq(read(line({ num: 6, sku: 'Necklace_Charm-6' }), ownComic).designSku, 'NECKLACE_CHARM-6', 'a link to the generic COMIC NUMBER drawing never wins, even under the member SKU'); }
{ // a person's answer for THIS line stands
  const own = ctxOf(W, { aliases: { 1871838409: { bySku: { 'NECKLACE_CHARM-6': 'MAPLE_8065' } } } });
  const a = read(line({ num: 6, sku: 'Necklace_Charm-6' }), own);
  eq([a.designSku, a.skuSource, !!a.listingProfile], ['MAPLE_8065', 'alias', false], 'a link saved under the line\'s own member SKU (that one product) wins');
  eq(read(line({ num: 7, sku: 'Necklace_Charm-7' }), own).designSku, 'NECKLACE_CHARM-7', 'and reaches no other number');
  const oc = ctxOf(W, { optionMaps: { 1871838409: { numbers: { 6: { field: 'design', value: 'COMET_9427' } } } } });
  const b = read(line({ num: 6, sku: 'Necklace_Charm-6' }), oc);
  eq([b.designSku, b.skuSource], ['COMET_9427', 'option'], 'an option a person mapped to a charm wins');
  const pc = ctxOf(W, { aliases: { 1871838409: { pairBySku: { 'COMIC NUMBER': { L: 'NECKLACE_CHARM-1 (HUGGIE)', R: 'NECKLACE_CHARM-2 (HUGGIE)' } } } } });
  eq(members(read(line({ sku: 'Comic Number', type: HUG, box: '7' }), pc)), ['L:NECKLACE_CHARM-1 (HUGGIE)', 'R:NECKLACE_CHARM-2 (HUGGIE)'], 'a pair a person saved for the listing and SKU wins'); }
pass('a listing-wide answer or any link to COMIC NUMBER no longer wins; a person\'s answer for the line (own SKU link, option map, saved pair) does');

/* ── 7 · a member the library lacks waits with one plain line, never another digit, never COMIC NUMBER ── */
{ const c = ctxOf(world({ noNeck: [7] }));
  const sp = read(line({ num: 7, sku: 'Necklace_Charm-7' }), c);
  eq([sp.designSku, sp.skuSource, kinds(sp), sp.formHold.reason], [null, null, ['unmatchedSku'], 'The number 7 charm is not in the repo yet (NECKLACE_CHARM-7 is not in the library)'], 'NECKLACE_CHARM-7 missing: the line waits with ONE plain line naming the number');
  ok(!sp.problems.some(p => p.kind === 'needsMapping') && !sp.problems.some(p => /not in any master file/.test(p.reason || '')), 'no second question and no unknown-SKU line beside it');
  eq(read(line({ sku: 'Comic Number', type: NECK, box: '7' }), c).formHold.reason, 'The number 7 charm is not in the repo yet (NECKLACE_CHARM-7 is not in the library)', 'the same from the box (never COMIC NUMBER)');
  eq(read(line({ num: 8, sku: 'Necklace_Charm-8' }), c).designSku, 'NECKLACE_CHARM-8', 'the other numbers are made from their own members');
  const h = read(line({ type: HUG, sku: 'Comic Number', box: '7' }), ctxOf(world({ noHug: [7] })));
  eq([h.designSku, h.formHold.reason, members(h)], [null, 'The number 7 charm is not in the repo yet (NECKLACE_CHARM-7 (HUGGIE) is not in the library)', []], 'the huggie 7 missing: waits, never the necklace 7, never COMIC NUMBER (HUGGIE)');
  const nh = read(line({ type: HUG, sku: 'Necklace_Charm-3' }), ctxOf(world({ noHuggieFamily: true })));
  eq([nh.designSku, nh.formHold.reason], [null, 'The number 3 charm is not in the repo yet (NECKLACE_CHARM-3 (HUGGIE) is not in the library)'], 'a library holding only the necklace row: a huggie line waits (never the necklace drawing)'); }
pass('a missing member waits with one plain line naming the number');

/* ── 8 · a library without the family (before the new file is added) reads exactly as before ── */
{ const c = ctxOf(world({ noFamily: true }));
  const sp = read(line({ sku: 'Comic Number', type: NECK, box: '6' }), c);
  eq([sp.designSku, sp.skuSource, !!sp.listingProfile, sp.engraveCandidate], ['COMIC NUMBER', 'transaction', false, true], 'no NECKLACE_CHARM member in the library: COMIC NUMBER as before, the box is read as before');
  const h = read(line({ type: HUG, sku: 'Comic Number', box: '7' }), c);
  eq([h.designSku, !!h.listingProfile], ['COMIC NUMBER (HUGGIE)', false], 'and the huggie set too');
  const u = read(line({ num: 6, sku: 'Necklace_Charm-6' }), c);
  eq([u.designSku, u.skuSource, kinds(u), !!u.listingProfile], ['NECKLACE_CHARM-6', 'transaction', ['needsMapping'], false], 'the new SKU the master lacks waits for a person as any such SKU does (its option asked), no profile'); }
pass('before the family is in the library the listing reads as before');

/* ── 9 · an engraving the buyer chose is still read; a stud line is not this profile's ── */
{ const sp = read(line({ num: 6, sku: 'Necklace_Charm-6', type: 'CHARM + Engraving', box: 'JAKE' }));
  eq([sp.designSku, sp.engraveCandidate, !!sp.engraveNever], ['NECKLACE_CHARM-6', true, false], '"CHARM + Engraving" with the Numbers drop-down: NECKLACE_CHARM-6 and the box is read as an engraving');
  const st = read(Object.assign(line({ num: 6, sku: 'Necklace_Charm-6' }), { variations: [{ name: 'Metal Choice', value: 'Gold Filled' }, { name: 'Style', value: 'Stud Earrings' }, { name: 'Numbers', value: '6' }] }));
  ok(!st.listingProfile, 'a stud line (the file\'s stud row has no SKUs) is not read by the profile: ' + st.lineForm); }
pass('an engraving option the buyer chose is read; stud lines stand aside');

/* ── 10 · every other listing reads as before; the SKU family is read on any listing ── */
{ const other = read(line({ listingId: '555000111', sku: 'Comic Number', box: '6' }));
  eq([other.designSku, other.skuSource, !!other.listingProfile, other.engraveCandidate], ['COMIC NUMBER', 'transaction', false, true], 'another listing carrying "Comic Number" is read as before');
  const maple = read({ transactionId: String(++tx), listingId: '555000222', sku: 'MAPLE_8065', title: 'Maple Leaf Necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: ['7'], variations: [{ name: 'Metal', value: 'Gold' }, { name: 'Length', value: '18"' }, { name: 'Personalization', value: '7' }] });
  eq([maple.designSku, maple.skuSource, !!maple.listingProfile, maple.engraveCandidate, !!maple.engraveNever], ['MAPLE_8065', 'transaction', false, true, false], 'a "7" in the box of any other listing stays words to engrave');
  const fam = read(line({ listingId: '1000000042', sku: 'Necklace_Charm-3' }));
  eq([fam.designSku, fam.skuSource, fam.listingProfile && fam.listingProfile.profile], ['NECKLACE_CHARM-3', 'profile', 'number-charm'], 'a Necklace_Charm-N SKU on another listing is read by the profile');
  eq([O.numberListing({ listingId: '1871838409' }, []).id, O.numberListing({ listingId: '1', sku: 'necklace charm-4' }, []).id, O.numberListing({ listingId: '1', sku: 'NECKLACE_CHARM-4 (HUGGIE)' }, []).id, O.numberListing({ listingId: '1', sku: 'NECKLACE_CHARM-12' }, []), O.numberListing({ listingId: '1', sku: 'COMIC NUMBER' }, [])],
    ['number-charm', 'number-charm', 'number-charm', null, null], 'the row: by listing, or by a member SKU (any spelling, with or without "(HUGGIE)"), never another SKU');
  eq(O.NUMBER_LISTINGS.map(x => [x.id, x.listings, x.base, x.huggieSuffix]), [['number-charm', ['1871838409'], 'NECKLACE_CHARM-', ' (HUGGIE)']], 'one row'); }
pass('every other listing reads as before');

console.log(`number-charm-profile: ${n} checks passed`);
