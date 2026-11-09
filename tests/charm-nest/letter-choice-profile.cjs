// LETTERCHOICE (Paul, 9 Nov 2026, listing 1871836213 "Initial Letter Charm Gold Letter Pendant Add On Charm Personalized Jewelry Initial Disc Charm Bridesmaid Jewelry Charm Gift for Mom"):
//   every product of the listing carries the Etsy SKU "CHAPPARAL LOWERCASE" (no master design), so no letter could be matched. The buyer chooses Necklace or Huggie on the listing and leaves ONE letter in the
//   personalisation or the note. → the letter (a drop-down naming one, else the personalisation, else the note) picks the member, the form bought picks the family: LETTER_NECKLACE-n (necklace ring) or
//   LETTER_NECKLACE-n (HUGGIE) (huggie hoop, both ears), a = 0 … z = 25; "No exact match = no match": two letters, none, a word, a letter outside a–z, two different answers, or no Necklace / Huggie choice
//   wait in Review with one plain line; a letter the library does not hold yet waits saying so; the letter is never engraved; a necklace is one piece, never mirrored; every other listing reads as before.
//   node tests/charm-nest/letter-choice-profile.cjs      Offline: no browser, no network, no Etsy request, no Firestore (a fake library in memory). No buyer data.
'use strict';
const assert = require('assert'), path = require('path');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);
// no request of any kind may leave this process
const realFetch = global.fetch; global.fetch = () => { throw new Error('a network request in an offline test'); };

const AZ = 'abcdefghijklmnopqrstuvwxyz';
const F = f => 'charmnest/master/' + f + '.ai';
/** The library: the 52 letter charms (unless o.noLetters / o.noHuggie / o.missing), the 26 earring letters they came from, the number family, two unrelated designs. */
function world(o = {}) {
  const lib = new Map(), put = (sku, file, w = 19, h = 27, holes = 1) => lib.set(sku.toUpperCase(), { sku: sku.toUpperCase(), aiPath: F(file), widthPt: w, heightPt: h, members: 2, holes, sizes: null, blocked: null });
  for (let i = 0; i < 26; i++) {
    put('LETTER_EARRING-' + i, 'LETTER_EARRING-' + i, 19, 19, 0);
    if (!o.noLetters && !(o.missing || []).includes('LETTER_NECKLACE-' + i)) put('LETTER_NECKLACE-' + i, 'LETTER_NECKLACE-' + i);
    if (!o.noLetters && !o.noHuggie && !(o.missing || []).includes('LETTER_NECKLACE-' + i + ' (HUGGIE)')) put('LETTER_NECKLACE-' + i + ' (HUGGIE)', 'LETTER_NECKLACE-' + i + '__HUGGIE_', 19, 30);
  }
  for (let i = 0; i < 10; i++) { put('Necklace_Charm-' + i, 'NC-' + i, 20, 33); put('Necklace_Charm-' + i + ' (HUGGIE)', 'NC-' + i + '_H', 14, 27); }
  put('COMIC NUMBER', 'COMIC_NUMBER', 20, 33); put('MAPLE_8065', 'MAPLE_8065', 30, 33);
  const byFile = new Map(); for (const [k, e] of lib) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
  const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
  return { masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: s => { const e = lib.get(String(s || '').toUpperCase()); return e ? byFile.get(e.aiPath) || [] : []; }, masterSkus: () => [...lib.keys()] };
}
const ctxOf = (w, extra) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {} }, w || world(), extra || {});
const W = world();
let tx = 8800000000;
const read = (line, c, order) => O.interpretLine(Object.assign({ receiptId: '4300000000', updateTs: 1, buyerMessage: '', staffNote: '' }, order || {}), JSON.parse(JSON.stringify(line)), c || ctxOf(W));
const kinds = sp => (sp.problems || []).map(p => p.kind);

const TITLE = 'Initial Letter Charm Gold Letter Pendant Add On Charm Personalized Jewelry Initial Disc Charm Bridesmaid Jewelry Charm Gift for Mom';
// o: { type (Charm Type), box (personalisation), note, letter (a Letter drop-down), listingId, sku }
const line = (o = {}) => Object.assign({ transactionId: String(++tx), listingId: o.listingId || '1871836213', sku: o.sku != null ? o.sku : 'CHAPPARAL LOWERCASE', title: o.title || TITLE, quantity: 1, metalKey: 'gold', metalLabel: 'Gold Filled',
  personalization: o.box != null && o.box !== '' ? [o.box] : [], buyerMessage: o.note || '',
  variations: [{ name: 'Metal Choice', value: 'Gold Filled' }].concat(o.type ? [{ name: 'Charm Type', value: o.type }] : [], o.letter != null ? [{ name: 'Letter', value: o.letter }] : [], o.box != null && o.box !== '' ? [{ name: 'Personalization', value: o.box }] : []) });
const NECK = 'Necklace CHARM', HUG = 'Huggie CHARM SET';
const held = sp => (sp.problems || []).find(p => p.formHold) || null;

/* ── 0 · the reader of one letter ── */
for (const t of ['b', 'B', 'letter b', ' b. ', 'B please', '(b)', '"b"', '‘b’', 'Initial: B', 'Letter B please', 'b!', 'B, b']) eq(O.lettersInText(t), { letters: ['b'], empty: false, odd: '' }, `"${t}" is the letter b`);
for (const t of ['ab', 'a and b', 'a b', 'bee', 'I want b', '3', 'b3']) ok(!O.lettersInText(t).letters.length || O.lettersInText(t).letters.length > 1, `"${t}" is not ONE letter`);
for (const t of ['é', 'ж', 'ß', 'Ω']) eq(O.lettersInText(t).odd, 'latin', `"${t}" is a letter outside a–z`);
for (const t of ['', '   ', 'none', 'N/A', '-', '.']) eq(O.lettersInText(t).empty, true, `"${t}" says nothing`);
pass('the reader: exactly one letter a–z, tolerant of case, quotes, brackets, punctuation and "letter … please"; nothing else is a letter');

/* ── 1 · the 26 letters, both forms ── */
for (let i = 0; i < 26; i++) {
  const L = AZ[i];
  for (const [how, o] of [['personalisation', { box: L }], ['personalisation, capital', { box: L.toUpperCase() }], ['note', { note: L }], ['Letter drop-down', { letter: L.toUpperCase() }]]) {
    const sn = read(line(Object.assign({ type: NECK }, o)));
    eq([sn.designSku, sn.skuSource, kinds(sn), sn.pieceCount, ((sn.pair && sn.pair.sides) || []).filter(Boolean), sn.lineForm, sn.designForm], ['LETTER_NECKLACE-' + i, 'profile', [], 1, [], 'necklace', 'necklace'], `${L} (necklace, by ${how}): LETTER_NECKLACE-${i}, one piece, no side (never mirrored)`);
    eq([sn.engraveCandidate, sn.engraveNever && sn.engraveNever.rule], [false, 'charm-only-choice'], `${L} (necklace, by ${how}): the letter is not engraved`);
    const sh = read(line(Object.assign({ type: HUG }, o)));
    eq([sh.designSku, sh.skuSource, kinds(sh), sh.pieceCount, sh.pair.sides, sh.lineForm, sh.designForm], ['LETTER_NECKLACE-' + i + ' (HUGGIE)', 'profile', [], 2, ['L', 'R'], 'huggie', 'huggie'], `${L} (huggie, by ${how}): LETTER_NECKLACE-${i} (HUGGIE) on both ears`);
    eq([sh.engraveCandidate, sh.engraveNever && sh.engraveNever.rule], [false, 'earring'], `${L} (huggie, by ${how}): never engraved`);
  }
}
pass('a … z by the personalisation, the note or a Letter drop-down: LETTER_NECKLACE-0 … -25 for a necklace, LETTER_NECKLACE-n (HUGGIE) on both ears for a huggie set');

/* ── 2 · Paul's examples, in the box and in the note ── */
for (const t of ['b', 'B', 'letter b', ' b. ', 'B please', '(b)']) {
  for (const where of ['box', 'note']) {
    const sp = read(line({ type: NECK, [where]: t }));
    eq([sp.designSku, kinds(sp)], ['LETTER_NECKLACE-1', []], `"${t}" in the ${where}: LETTER_NECKLACE-1`);
    eq(sp.pair.notes.filter(x => /^Letter charm/.test(x)), [`Letter charm: b (LETTER_NECKLACE-1), from the ${where === 'box' ? 'personalisation' : 'note'} “${t.trim()}”`], `"${t}" in the ${where}: the reading says where the letter came from`);
    eq(read(line({ type: HUG, [where]: t })).designSku, 'LETTER_NECKLACE-1 (HUGGIE)', `"${t}" in the ${where}, huggie set: LETTER_NECKLACE-1 (HUGGIE)`);
  }
}
{ const sp = read(line({ type: NECK, box: 'b' }));
  eq([sp.listingProfile.profile, sp.listingProfile.form, sp.listingProfile.symbols.map(x => [x.name, x.from, x.sku])], ['letter-charm-only', 'necklace', [['b', 'box', 'LETTER_NECKLACE-1']]], 'the letter and its charm are kept as facts');
  eq(sp.listingProfile.was, { sku: 'CHAPPARAL LOWERCASE', source: 'transaction' }, 'and the reading it replaced ("CHAPPARAL LOWERCASE": no design)');
  eq([sp.engraving || null, sp.personalization], [null, ['b']], 'the "b" in the box is the choice, never an inscription to check'); }
{ const sp = read(line({ type: NECK, letter: 'B' }));
  eq(sp.options.find(o => o.name === 'Letter').mapped, { field: 'design', value: 'LETTER_NECKLACE-1', source: 'rule:listing-profile' }, 'a Letter drop-down is answered by its charm (no option question)'); }
pass('"b", "B", "letter b", " b. ", "B please", "(b)" in the personalisation or the note: the letter b');

/* ── 3 · no exact match = no match: Review with one plain line, never a guess ── */
const hold = (o, re, m, c) => {
  const sp = read(line(o), c);
  const p = held(sp);
  ok(sp.designSku == null && p && p.kind === 'unmatchedSku' && re.test(p.reason), `${m}: waits in Review (${p && p.reason})`);
  eq([sp.pieceCount === 0 || !sp.designSku, sp.formHold && sp.formHold.reason === p.reason], [true, true], `${m}: no piece is made, the hold carries the reason`);
  return sp;
};
hold({ type: NECK, box: 'ab' }, /^Letter charm: the personalisation says “ab”, which letter\?$/, '"ab" in the personalisation');
hold({ type: NECK, note: 'ab' }, /^Letter charm: the note says “ab”, which letter\?$/, '"ab" in the note');
hold({ type: HUG, box: 'a and b' }, /^Letter charm: the personalisation says “a and b”, which letter\?$/, '"a and b"');
hold({ type: NECK, box: '' }, /^Letter charm: the buyer did not say which letter \(the personalisation is empty, no note\)\. Which letter\?$/, 'nothing in the box or the note ("")');
hold({ type: NECK, box: 'none', note: '' }, /did not say which letter/, '"none"');
hold({ type: NECK, box: 'é' }, /^Letter charm: the personalisation says “é”, not one of the letters a–z\. Which letter\?$/, 'a letter outside a–z (é)');
hold({ type: NECK, note: 'ж' }, /not one of the letters a–z/, 'a non-Latin letter (ж)');
hold({ type: NECK, box: 'Bella' }, /says “Bella”, which letter\?/, 'a name, not a letter');
hold({ type: NECK, box: '3' }, /says “3”, which letter\?/, 'a digit');
hold({ type: NECK, box: 'b', note: 'c' }, /^Letter charm: the personalisation says “b” but the note says “c”, which letter\?$/, 'the box and the note name two different letters');
hold({ type: NECK, letter: 'B', box: 'c' }, /the drop-down says “B” but the personalisation says “c”/, 'the drop-down and the box disagree');
hold({ box: 'b' }, /^Letter charm: the letter is b \(the personalisation says “b”\) but the line does not say which \(no Necklace \/ Huggie option\)\. Necklace or Huggie\?$/, 'no Necklace / Huggie choice');
hold({ type: 'Necklace + Huggie', box: 'b' }, /both a necklace charm and a huggie set\. Necklace or Huggie\?$/, 'both forms named in the option');
// the same letter twice, or a note that only chats, is no conflict
eq(read(line({ type: NECK, box: 'b', note: 'B' })).designSku, 'LETTER_NECKLACE-1', 'the same letter in the box and the note: b');
eq(read(line({ type: NECK, box: 'b', note: 'Thank you so much, it is a gift for my sister!' })).designSku, 'LETTER_NECKLACE-1', 'a chatty note does not unsettle a letter the box named');
eq(read(line({ type: NECK, box: 'n/a', note: 'k' })).designSku, 'LETTER_NECKLACE-10', '"n/a" in the box: the note decides');
pass('"ab", "", "é", "ж", a name, a digit, two answers, no form: Review with one plain line');

/* ── 4 · before Paul presses the upload: the letter charms are not in the repo yet → the line waits, saying so ── */
{ const c = ctxOf(world({ noLetters: true }));
  const sp = hold({ type: NECK, box: 'b' }, /^Letter charm b \(necklace charm\): LETTER_NECKLACE-1 is not in the charm repo yet\. The line waits here until the new letter charms are added/, 'the necklace letters not uploaded yet', c);
  ok(!kinds(sp).includes('blockedSku') && !(sp.problems || []).some(p => p.reason === 'not in any master file'), 'it waits as the letter rule, not as an unknown "CHAPPARAL LOWERCASE"');
  hold({ type: HUG, note: 'B' }, /^Letter charm b \(Huggie set\): LETTER_NECKLACE-1 \(HUGGIE\) is not in the charm repo yet/, 'the huggie letters not uploaded yet', c); }
{ const c = ctxOf(world({ noHuggie: true }));
  eq(read(line({ type: NECK, box: 'b' }), c).designSku, 'LETTER_NECKLACE-1', 'only the necklace set in the repo: a necklace line is matched');
  const sp = read(line({ type: HUG, box: 'b' }), c); ok(sp.designSku == null && /LETTER_NECKLACE-1 \(HUGGIE\) is not in the charm repo yet/.test(held(sp).reason), 'a huggie line waits: never the necklace drawing'); }
{ const c = ctxOf(world({ missing: ['LETTER_NECKLACE-1'] }));
  const sp = read(line({ type: NECK, box: 'b' }), c); ok(sp.designSku == null && /LETTER_NECKLACE-1 is not in the charm repo yet/.test(held(sp).reason), 'one letter missing: it waits; never LETTER_NECKLACE-0 or -2, never the earring letter'); }
pass('before the upload: "LETTER_NECKLACE-n is not in the charm repo yet", the line waits in Review (no wrong drawing, no failure)');

/* ── 5 · a person's answer and listing-wide answers ── */
{ const aliases = { '1871836213': { sku: 'MAPLE_8065', bySku: { 'CHAPPARAL LOWERCASE': 'MAPLE_8065' } } };
  eq(read(line({ type: NECK, box: 'c' }), ctxOf(W, { aliases })).designSku, 'LETTER_NECKLACE-2', 'a listing-wide saved answer ("CHAPPARAL LOWERCASE" → one design) is replaced: the buyer\'s letter wins'); }
{ const optionMaps = { '1871836213': { letter: { b: { field: 'design', value: 'MAPLE_8065' } } } };
  eq(read(line({ type: NECK, letter: 'B' }), ctxOf(W, { optionMaps })).designSku, 'MAPLE_8065', 'a person who mapped the drop-down value to a charm: their answer stands'); }
pass('a person\'s answer for the line stands; a listing-wide answer does not');

/* ── 6 · every other listing reads as before ── */
{ const other = { listingId: '1801964100000', title: 'Maple Leaf Charm', sku: 'MAPLE_8065', type: NECK, box: 'b' };
  const sp = read(line(other));
  eq([sp.designSku, sp.skuSource, sp.listingProfile || null], ['MAPLE_8065', 'transaction', null], 'another listing with a "b" in the box: its own design, no letter rule'); }
{ const sp = read(line({ listingId: '1871838409', sku: 'Necklace_Charm-7', type: NECK, box: 'b', title: 'Number Charm Gold Number Pendant' }));
  eq([sp.designSku, sp.listingProfile && sp.listingProfile.profile], ['NECKLACE_CHARM-7', 'number-charm'], 'the number listing keeps its own rule'); }
{ const sp = read(line({ listingId: '1999999999', sku: 'CHAPPARAL LOWERCASE', type: NECK, box: 'b' }));
  ok(sp.designSku !== 'LETTER_NECKLACE-1' && !(sp.listingProfile && sp.listingProfile.profile === 'letter-charm-only'), 'the same SKU on another listing is not read by this rule (the rule is the listing\'s)'); }
pass('another listing is unaffected');

/* ── 7 · the rule is data ── */
{ const row = O.CHARM_ONLY_LISTINGS.find(r => r.id === 'letter-charm-only');
  eq([row.letter, row.listings, row.necklace.base, row.huggie.base + '0' + row.huggie.suffix, row.necklace.names.length], [true, ['1871836213'], 'LETTER_NECKLACE-', 'LETTER_NECKLACE-0 (HUGGIE)', 26], 'one row of the listing table: listing, families, a … z');
  ok(O.charmOnlyListing({ listingId: '1871836213' }) === row && O.charmOnlyListing({ listingId: '1871836214' }) === null, 'found by its listing number only'); }

global.fetch = realFetch;
console.log(`letter-choice-profile: ${n} checks passed`);
