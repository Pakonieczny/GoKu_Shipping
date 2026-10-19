// HUGSYM + LISTDESIGN (Paul, 19 Oct 2026): three Etsy listings read as the library charms the shop owner named.
//   1 · listing 1744307981 "Huggie Hoop Earrings with Astrological Charm …", Etsy SKU "Huggie Hoops- Zodiac" on every sign, drop-downs METAL CHOICE, HOOP SIZE, ZODIAC SIGN (order 4200256896, Capricorn):
//       "ensure that this listing is correctly profiled and that the appropriate Huggie charms get assigned to the buyer's choices from the Drop Down menu" → ZODIAC_HUGGIE-<the sign's place> (Aries 0 … Pisces 11), both ears.
//   2 · listing 1730115500 "Norse Runes Charm Huggie Hoop Earrings …", Etsy SKU "HUGGIE HOOPS- VIKING RUNES", a rune drop-down (order 4198076628, box "Rune Wunjo und Rune Algiz"): "Same … except these are the Runes"
//       → RUNE_HUGGIE_CHARM-<the rune's place> (Fehu 0 … Jera 11); two runes in the buyer's words are a pair (Left, Right); a rune the library does not hold (Algiz) waits with ONE plain line, never another rune's file.
//   3 · listing 1630282472 "Astronaut Charm Necklace …", NO SKU on the transaction (order 4204609963): "Assign this SKU (ASTRONAUT_9809) to this order/listing" → LISTING_DESIGNS; the personalisation stays engraving text.
//   A person's saved answer (alias, option map) wins in every case; every other listing reads as before.
//   node tests/charm-nest/huggie-zodiac-runes-profile.cjs      Offline: no browser, no network, no Etsy call. The library names are the shop's (SKUs_CORRECTED_2024-1014.ai); no buyer data beyond the orders' choices.
'use strict';
const assert = require('assert'), path = require('path');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);

/* ── the world: the library's numbered families (one design file each), the old Libra huggie drawing that HUGGIE HOOPS- ZODIAC is filed under, a named zodiac huggie, and the astronaut drawing ── */
const F = f => 'charmnest/master/' + f + '.ai';
const SIGNS = ['Aries', 'Taurus', 'Gemini', 'Cancer', 'Leo', 'Virgo', 'Libra', 'Scorpio', 'Sagittarius', 'Capricorn', 'Aquarius', 'Pisces'];
const RUNES = ['Fehu', 'Uruz', 'Thurisaz', 'Ansuz', 'Raidho', 'Kenaz', 'Gebo', 'Wunjo', 'Hagalaz', 'Nauthiz', 'Isa', 'Jera'];
const ABSENT = ['Eihwaz', 'Perthro', 'Algiz', 'Sowilo', 'Tiwaz', 'Berkano', 'Ehwaz', 'Mannaz', 'Laguz', 'Ingwaz', 'Dagaz', 'Othala'];
function world(o = {}) {
  const lib = new Map(), put = (sku, file, w = 22, h = 28) => lib.set(sku.toUpperCase(), { sku: sku.toUpperCase(), aiPath: F(file), widthPt: w, heightPt: h, members: 1, sizes: null, blocked: null });
  for (let i = 0; i < 12; i++) {
    if (!(o.noZodiac || []).includes(i) && !o.noZodiacFamily) put('Zodiac_Huggie-' + i, 'SKUs_CORRECTED_2024-1014_Zodiac_Huggie-' + i);
    if (!(o.noRune || []).includes(i) && !o.noRuneFamily) put('Rune_Huggie_Charm-' + i, 'SKUs_CORRECTED_2024-1014_Rune_Huggie_Charm-' + i, 9, 30);
  }
  for (const k of ['HUGGIE HOOPS- ZODIAC', 'HUGGIE HOOPS- ZODIAC AVATAR', 'ZODIAC ICONS LIBRA (HUGGIE)']) put(k, 'HUGGIE_HOOPS-_ZODIAC_AVATAR');
  put('ZODIAC ICONS LEO (HUGGIE)', 'ZODIAC_ICONS_LEO_HUGGIE_'); put('ZODIAC ICONS LEO', 'ZODIAC_ICONS_LEO', 30, 34);
  for (const k of ['ASTRONAUT_9809', 'ASTRONAUT_', 'CO-GOLDCHARMO']) put(k, 'ASTRONAUT_', 25.8, 40.3);
  for (const k of ['BLOOMING_16014', 'BLOOMING_35148', 'BLOOMING_44107']) put(k, 'BLOOMING_16014', 27, 39);
  put('MAPLE_8065', 'MAPLE_8065', 30, 33); put('COMET_9427', 'COMET_9427', 30, 33);
  const byFile = new Map(); for (const [k, e] of lib) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
  const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
  return { masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: s => { const e = lib.get(String(s || '').toUpperCase()); return e ? byFile.get(e.aiPath) || [] : []; }, masterSkus: () => [...lib.keys()] };
}
const ctxOf = (w, extra) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {} }, w || world(), extra || {});
const W = world();
let tx = 7300000000;
const read = (line, c, order) => O.interpretLine(Object.assign({ receiptId: '4200000000', updateTs: 1, buyerMessage: '', staffNote: '' }, order || {}), JSON.parse(JSON.stringify(line)), c || ctxOf(W));
const kinds = sp => (sp.problems || []).map(p => p.kind);
const members = sp => (sp.pair && sp.pair.members || []).map(m => m.side + ':' + m.sku);

/* ── the lines as Etsy gives them ── */
const ZTITLE = 'Huggie Hoop Earrings with Astrological Charm, Handcrafted in Gold Vermeil, Silver, Solid 14k Gold | Handcrafted Horoscope Jewelry';
const zodiac = (sign, box, over) => Object.assign({ transactionId: String(++tx), listingId: '1744307981', sku: 'Huggie Hoops- Zodiac', title: ZTITLE, quantity: 1, metalKey: 'gold', metalLabel: 'Gold Vermeil', personalization: box ? [box] : [], buyerMessage: '',
  variations: [{ name: 'METAL CHOICE', value: 'Gold Vermeil' }, { name: 'HOOP SIZE', value: '8mm' }].concat(sign ? [{ name: 'ZODIAC SIGN', value: sign }] : [], box ? [{ name: 'Personalization', value: box }] : []) }, over || {});
const RTITLE = 'Norse Runes Charm Huggie Hoop Earrings in Gold Vermeil, Sterling Silver, or Solid 14k Gold | Ancient Symbolic Jewelry';
const rune = (value, box, over) => Object.assign({ transactionId: String(++tx), listingId: '1730115500', sku: 'HUGGIE HOOPS- VIKING RUNES', title: RTITLE, quantity: 1, metalKey: 'silver', metalLabel: 'Sterling Silver', personalization: box ? [box] : [], buyerMessage: '',
  variations: [{ name: 'METAL CHOICE', value: 'Sterling Silver' }, { name: 'HOOP SIZE', value: '10mm' }].concat(value ? [{ name: 'RUNE', value }] : [], box ? [{ name: 'Personalization', value: box }] : []) }, over || {});
const ATITLE = 'Astronaut Charm Necklace Space Jewelry Planet Pendant Dainty Whimsical Necklace Quirky Jewelry Birthday Gifts Gift for Her';
const astro = over => Object.assign({ transactionId: String(++tx), listingId: '1630282472', sku: '', title: ATITLE, quantity: 1, metalKey: 'gold', metalLabel: '14k Gold Filled', personalization: ['love you to the moon and back'], buyerMessage: '',
  variations: [{ name: 'Metal', value: '14k Gold Filled' }, { name: 'Length', value: '18"' }, { name: 'Personalization', value: 'love you to the moon and back' }] }, over || {});

/* ── 1 · ZODIAC: all twelve signs from the drop-down, and from the box (the listing before its drop-down) ── */
SIGNS.forEach((s, i) => {
  for (const [how, line] of [['drop-down', zodiac(s)], ['box', zodiac('', s)], ['drop-down + box', zodiac(s, s)], ['lower case', zodiac(s.toLowerCase())]]) {
    const sp = read(line);
    eq([sp.designSku, sp.skuSource, kinds(sp), sp.pieceCount, sp.pair.sides], ['ZODIAC_HUGGIE-' + i, 'profile', [], 2, ['L', 'R']], `${s} by the ${how}: ZODIAC_HUGGIE-${i} on both ears, no question`);
    ok(sp.zodiacSign && sp.zodiacSign.sign === s && sp.zodiacSign.design === 'ZODIAC_HUGGIE-' + i && sp.listingProfile.profile === 'zodiac-huggie', `${s} (${how}): the sign and its charm are kept as facts`);
  }
  const dd = read(zodiac(s)).options.find(o => o.name === 'ZODIAC SIGN');
  eq(dd.mapped, { field: 'design', value: 'ZODIAC_HUGGIE-' + i, source: 'rule:listing-profile' }, `${s}: the ZODIAC SIGN drop-down is answered by its charm (no option question)`);
});
// the same SKU on another listing of huggie form reads the same; the Etsy SKU written as the master writes it, or any case, too
for (const over of [{ listingId: '1000000042', sku: 'HUGGIE HOOPS-ZODIAC' }, { listingId: '1000000042', sku: 'huggie hoops - zodiac' }, { sku: 'HUGGIE HOOPS- ZODIAC' }, { sku: '' }]) {
  const sp = read(zodiac('Pisces', '', over)); eq([sp.designSku, kinds(sp)], ['ZODIAC_HUGGIE-11', []], 'Pisces under ' + JSON.stringify(over) + ': ZODIAC_HUGGIE-11');
}
// a typo in the box, a shop-language word, a sentence
eq(['Capricon', 'Steinbock', 'my sign is capricorn please', 'Verseau'].map(b => read(zodiac('', b)).designSku), ['ZODIAC_HUGGIE-9', 'ZODIAC_HUGGIE-9', 'ZODIAC_HUGGIE-9', 'ZODIAC_HUGGIE-10'], 'the box is read as section 3c reads it (typos, shop languages, sentences)');
pass('zodiac: the 12 signs, by the ZODIAC SIGN drop-down and by the box, are ZODIAC_HUGGIE-0 … -11 on both ears');

/* ── 2 · order 4200256896 as it is: Capricorn in the drop-down and in the box, a pair of huggies ── */
{ const line = zodiac('Capricorn', 'Capricorn'), sp = read(line, null, { receiptId: '4200256896' });
  eq([sp.designSku, sp.skuSource, kinds(sp), sp.pieceCount, sp.pair.earring, sp.pair.sides], ['ZODIAC_HUGGIE-9', 'profile', [], 2, true, ['L', 'R']], 'order 4200256896: ZODIAC_HUGGIE-9 on the Left and on the Right, no question');
  ok(sp.designSku !== 'HUGGIE HOOPS- ZODIAC' && sp.designSku !== 'ZODIAC ICONS LIBRA (HUGGIE)' && !sp.formHold, 'never the Libra drawing, never "not in the repo yet"');
  eq(sp.pair.notes.filter(x => /Zodiac sign/.test(x)), ['Zodiac sign: Capricorn (ZODIAC_HUGGIE-9, both ears), from the “ZODIAC SIGN” option'], "the order's reading says the sign and its charm once");
  eq([sp.zodiacSign.textOnly, !!sp.engraveNever, sp.engraveCandidate, sp.personalization], [true, true, false, ['Capricorn']], 'the box word is the buyer\'s choice of sign, never engraved; the buyer\'s word is kept');
  const old = read(line, ctxOf(world({ noZodiacFamily: true })));
  eq([old.designSku, old.formHold && old.formHold.reason], [null, 'The Capricorn charm is not in the repo yet'], 'a library without the Zodiac_Huggie family reads as before (section 3c)'); }
pass('order 4200256896: ZODIAC_HUGGIE-9 on both ears (was "The Capricorn charm is not in the repo yet" and a Libra vector)');

/* ── 3 · a member the master does not hold waits with one plain line, never another sign's file ── */
{ const c = ctxOf(world({ noZodiac: [9] }));
  const sp = read(zodiac('Capricorn', 'Capricorn'), c);
  eq([sp.designSku, sp.skuSource, kinds(sp), sp.formHold.reason], [null, null, ['unmatchedSku'], 'The Capricorn charm is not in the repo yet (ZODIAC_HUGGIE-9 is not in the library)'], 'ZODIAC_HUGGIE-9 missing: Capricorn waits with ONE plain line, no design on the row');
  ok(!sp.problems.some(p => p.kind === 'needsMapping'), 'and no second question about the drop-down');
  eq([read(zodiac('Leo'), c).designSku, read(zodiac('Libra'), c).designSku], ['ZODIAC_HUGGIE-4', 'ZODIAC_HUGGIE-6'], 'the other signs are made from their own members (never the old named drawings)'); }
pass('a missing family member waits with one plain line');

/* ── 4 · zodiac: what a person said wins; doubtful lines wait ── */
{ const lc = ctxOf(W, { aliases: { 1744307981: { bySku: { 'HUGGIE HOOPS- ZODIAC': 'ZODIAC ICONS LEO (HUGGIE)' } } } });
  eq([read(zodiac('Capricorn'), lc).designSku, read(zodiac('Capricorn'), lc).skuSource], ['ZODIAC ICONS LEO (HUGGIE)', 'alias'], 'a saved link for the listing and SKU wins over the profile');
  const oc = ctxOf(W, { optionMaps: { 1744307981: { 'zodiac sign': { capricorn: { field: 'design', value: 'ZODIAC ICONS LEO (HUGGIE)' } } } } });
  eq([read(zodiac('Capricorn'), oc).designSku, read(zodiac('Capricorn'), oc).skuSource], ['ZODIAC ICONS LEO (HUGGIE)', 'option'], 'an option a person mapped to a charm wins over the profile');
  const two = read(zodiac('Aries', 'Leo'));
  eq([two.designSku, kinds(two), two.formHold.reason], [null, ['unmatchedSku'], 'Which zodiac sign? The “ZODIAC SIGN” drop-down says Aries but the buyer\'s words name Leo'], 'the drop-down and the box disagree: a person says (never guessed)');
  const none = read(zodiac('', ''));
  eq([none.designSku, none.formHold.reason], [null, 'Which zodiac sign? The buyer named none (the personalisation box is empty)'], 'no sign at all: one plain question');
  eq(read(zodiac('', 'Aries and Leo')).formHold.reason, 'Which zodiac sign? The buyer names Aries and Leo', 'two signs typed with no "2 symbols" choice wait, as they did');
  // a "2 symbols" line (the shop's pair reader, pairMembers): each ear is its own sign's ZODIAC_HUGGIE member
  const pair = read(zodiac('', '', { variations: [{ name: 'METAL CHOICE', value: 'Gold Vermeil' }, { name: 'Zodiac Sign', value: '2 symbols-leave note' }], buyerMessage: 'Aries and Leo please' }));
  eq([members(pair), kinds(pair), pair.pieceCount], [['L:ZODIAC_HUGGIE-0', 'R:ZODIAC_HUGGIE-4'], [], 2], '"2 symbols" with Aries and Leo in the note: Left ZODIAC_HUGGIE-0, Right ZODIAC_HUGGIE-4'); }
pass('zodiac: a person\'s link or option answer wins; disagreeing or missing signs wait with one plain line');

/* ── 5 · RUNES: all twelve from the drop-down, spellings, the box ── */
RUNES.forEach((r, i) => {
  for (const [how, line] of [['drop-down', rune(r)], ['box', rune('', r)], ['box sentence', rune('', 'I would like the rune ' + r + ' please')], ['upper case', rune(r.toUpperCase())]]) {
    const sp = read(line);
    eq([sp.designSku, sp.skuSource, kinds(sp), sp.pieceCount, sp.pair.sides], ['RUNE_HUGGIE_CHARM-' + i, 'profile', [], 2, ['L', 'R']], `${r} by the ${how}: RUNE_HUGGIE_CHARM-${i} on both ears`);
  }
  eq(read(rune(r)).options.find(o => o.name === 'RUNE').mapped, { field: 'design', value: 'RUNE_HUGGIE_CHARM-' + i, source: 'rule:listing-profile' }, `${r}: the rune drop-down is answered by its charm`);
});
eq(['Raido', 'Thurs', 'Naudiz', 'Jeran', 'Gifu', 'Wynn', 'Kaunan', 'Hagall', 'Ansur', 'Feoh', 'Rune Ur', 'Rune Is', 'Raidō', 'Þurisaz'].map(v => read(rune(v)).designSku),
  ['RUNE_HUGGIE_CHARM-4', 'RUNE_HUGGIE_CHARM-2', 'RUNE_HUGGIE_CHARM-9', 'RUNE_HUGGIE_CHARM-11', 'RUNE_HUGGIE_CHARM-6', 'RUNE_HUGGIE_CHARM-7', 'RUNE_HUGGIE_CHARM-5', 'RUNE_HUGGIE_CHARM-8', 'RUNE_HUGGIE_CHARM-3', 'RUNE_HUGGIE_CHARM-0', 'RUNE_HUGGIE_CHARM-1', 'RUNE_HUGGIE_CHARM-10', 'RUNE_HUGGIE_CHARM-4', 'RUNE_HUGGIE_CHARM-2'],
  'the usual spellings of the twelve runes are read');
eq([read(rune('Is')).designSku, read(rune('Ur')).designSku], ['RUNE_HUGGIE_CHARM-10', 'RUNE_HUGGIE_CHARM-1'], '"Is" and "Ur" as the whole drop-down value are the runes');
pass('runes: the 12 runes by the drop-down, the box and their spellings are RUNE_HUGGIE_CHARM-0 … -11');

/* ── 6 · order 4198076628: "Rune Wunjo und Rune Algiz", a pair; Algiz is not in the library ── */
for (const [how, line] of [['the box alone', rune('', 'Rune Wunjo und Rune Algiz')], ['Wunjo in the drop-down', rune('Wunjo', 'Rune Wunjo und Rune Algiz')]]) {
  const sp = read(line, null, { receiptId: '4198076628' });
  eq([sp.designSku, sp.skuSource, kinds(sp), sp.formHold && sp.formHold.reason], [null, null, ['unmatchedSku'], 'The Algiz charm is not in the repo yet (the repo has the 12 runes Fehu … Jera)'], `order 4198076628 (${how}): waits with ONE plain line, no design on the row`);
  eq(sp.listingProfile.symbols.map(x => [x.side, x.name, x.sku]), [['L', 'Wunjo', 'RUNE_HUGGIE_CHARM-7'], ['R', 'Algiz', '']], 'the reading: Left Wunjo = RUNE_HUGGIE_CHARM-7, Right Algiz (not in the library)');
  ok(sp.pair.notes.some(x => /^Runes: Left Wunjo \(RUNE_HUGGIE_CHARM-7\), Right Algiz \(not in the repo\)/.test(x)), 'the order\'s reading says it once: ' + sp.pair.notes.join(' | '));
  ok(!sp.problems.some(p => /HUGGIE HOOPS- VIKING RUNES|not in any master file|no SKU/.test(p.reason || '')) && !sp.problems.some(p => p.kind === 'needsMapping'), 'never "Unknown SKU … file", never an option question beside it');
  ok(!members(sp).length && sp.designSku !== 'RUNE_HUGGIE_CHARM-7', 'the Left ear is not made alone, and no other rune stands in for Algiz');
  eq([!!sp.engraveNever, sp.engraveCandidate], [true, false], 'the rune words are the buyer\'s choice, never engraved');
}
// two runes the library holds: a pair, each ear its own member
for (const box of ['Rune Wunjo und Rune Isa', 'wunjo and isa', 'Wunjo/Isa', 'WUNJO & ISA']) {
  const sp = read(rune('', box));
  eq([sp.designSku, kinds(sp), members(sp), sp.pieceCount, sp.pair.mismatched, sp.pair.sides], ['RUNE_HUGGIE_CHARM-7', [], ['L:RUNE_HUGGIE_CHARM-7', 'R:RUNE_HUGGIE_CHARM-10'], 2, true, ['L', 'R']], `"${box}": Left Wunjo, Right Isa, both made`);
}
{ const sp = read(rune('Isa', 'Rune Wunjo und Rune Isa'));
  eq(members(sp), ['L:RUNE_HUGGIE_CHARM-7', 'R:RUNE_HUGGIE_CHARM-10'], 'the drop-down among the two named: the pair, in the buyer\'s order');
  // a person said "the same on both ears" for this line (the pseudo option "Two designs on this line", answered "Nothing — ignore it"): the drop-down's rune on both ears, no pair
  const ln = rune('Isa', 'Rune Wunjo und Rune Isa'), said = { 1730115500: { 'Two designs on this line': { ['4198076628/' + ln.transactionId]: { field: 'ignore', value: null } } } };
  const same = read(ln, ctxOf(W, { optionMaps: said }), { receiptId: '4198076628' });
  eq([same.designSku, members(same), kinds(same)], ['RUNE_HUGGIE_CHARM-10', [], []], 'a person\'s "the same on both ears" sets the pair aside: Isa (the drop-down) on both ears');
  const dis = read(rune('Fehu', 'Wunjo'));
  eq([dis.designSku, dis.formHold.reason], [null, 'Which rune? The “RUNE” drop-down says Fehu but the buyer\'s words name Wunjo'], 'the drop-down and the box name two runes, neither repeated: a person says');
  const many = read(rune('', 'Fehu, Uruz and Gebo'));
  eq([many.designSku, many.formHold.reason], [null, 'Which rune? The buyer names Fehu, Uruz and Gebo'], 'three runes for two ears: a person says'); }
// both ears waiting
eq(read(rune('', 'Algiz and Sowilo')).formHold.reason, 'The Algiz and Sowilo charms are not in the repo yet (the repo has the 12 runes Fehu … Jera)', 'two runes the library does not hold: one plain line');
eq(read(rune('Algiz')).formHold.reason, 'The Algiz charm is not in the repo yet (the repo has the 12 runes Fehu … Jera)', 'a single rune the library does not hold (the drop-down): one plain line');
{ const c = ctxOf(world({ noRune: [7] })), sp = read(rune('Wunjo'), c);
  eq([sp.designSku, sp.formHold.reason], [null, 'The Wunjo charm is not in the repo yet (RUNE_HUGGIE_CHARM-7 is not in the library)'], 'a member the master lacks: one plain line'); }
pass('order 4198076628: Left Wunjo = RUNE_HUGGIE_CHARM-7, Right Algiz waits with one plain line; two runes in the library make both ears');

/* ── 7 · ordinary words are no runes ── */
eq([O.runesInText('this is for ur sister', false), O.runesInText('man, this is the best', false), O.runesInText('thorn and rose', false)], [[], [], []], '"is", "ur", "man", "thorn" in ordinary words are no runes');
eq(O.runesInText('Rune Is and rune Ur', false).map(x => x.name), ['Isa', 'Uruz'], 'after the word "rune" they are');
eq(O.runesInText('Rune Wunjo und Rune Algiz', false).map(x => x.name), ['Wunjo', 'Algiz'], 'German "und" between two runes');
{ const sp = read(rune('Fehu', 'this is for ur sister'));
  eq([sp.designSku, kinds(sp), sp.listingProfile.symbols.length, sp.listingProfile.textOnly], ['RUNE_HUGGIE_CHARM-0', [], 1, false], '"is"/"ur" in the box beside the Fehu drop-down: Fehu on both ears, the box names no rune');
  const sp2 = read(rune('', 'this is for ur sister'));
  eq([sp2.designSku, sp2.formHold.reason], [null, 'Which rune? The buyer named none (the personalisation box says “this is for ur sister”)'], 'and with no drop-down the line waits: no rune is invented'); }
pass('"is" / "ur" in ordinary text are no runes');

/* ── 8 · runes: what a person said wins ── */
{ const lc = ctxOf(W, { aliases: { 1730115500: { bySku: { 'HUGGIE HOOPS- VIKING RUNES': 'MAPLE_8065' } } } });
  eq([read(rune('Wunjo'), lc).designSku, read(rune('Wunjo'), lc).skuSource], ['MAPLE_8065', 'alias'], 'a saved link for the listing and SKU wins');
  const oc = ctxOf(W, { optionMaps: { 1730115500: { rune: { wunjo: { field: 'design', value: 'COMET_9427' } } } } });
  eq([read(rune('Wunjo'), oc).designSku, read(rune('Wunjo'), oc).skuSource], ['COMET_9427', 'option'], 'an option a person mapped to a charm wins');
  const pc = ctxOf(W, { aliases: { 1730115500: { pairBySku: { 'HUGGIE HOOPS- VIKING RUNES': { L: 'MAPLE_8065', R: 'COMET_9427' } } } } });
  eq(members(read(rune('', 'Rune Wunjo und Rune Algiz'), pc)), ['L:MAPLE_8065', 'R:COMET_9427'], 'a pair a person saved for the listing and SKU wins (over the plain wait)'); }
pass('runes: a person\'s link, option answer or saved pair wins');

/* ── 9 · ASTRONAUT: listing 1630282472, no SKU on the transaction ── */
{ const sp = read(astro(), null, { receiptId: '4204609963' });
  eq([sp.designSku, sp.skuSource, kinds(sp), sp.pieceCount, sp.lineForm], ['ASTRONAUT_9809', 'alias', [], 1, 'necklace'], 'order 4204609963: ASTRONAUT_9809, no "Unknown SKU", one necklace piece');
  eq([sp.personalization, sp.engraveCandidate, !!sp.engraveNever], [['love you to the moon and back'], true, false], 'the personalisation stays the words to engrave');
  const same = read(astro({ listingId: '999000111', sku: 'ASTRONAUT_9809' }));
  eq([sp.engraving || null, sp.engraveCandidate], [same.engraving || null, same.engraveCandidate], 'its engraving reads exactly as a line that carried the SKU ASTRONAUT_9809 itself');
  eq(O.resolveSku(astro(), {}, W.masterEntry, { patterns: [], skus: [] }, W.masterLoose), { sku: 'ASTRONAUT_9809', source: 'alias', listed: true }, 'resolveSku (the save-to-repo flow\'s) says the same');
  for (const sku of ['CO-GOLDCHARMO', 'SOMETHING ELSE', 'astronaut_']) eq(read(astro({ sku })).designSku, 'ASTRONAUT_9809', `whatever the transaction carries (${sku})`);
  eq([O.listingDesign('1630282472'), O.listingDesign(1630282472), O.listingDesign(''), O.listingDesign('constructor'), O.listingDesign(null)], ['ASTRONAUT_9809', 'ASTRONAUT_9809', '', '', ''], 'the table answers for the listing number, and nothing for junk');
  eq(JSON.parse(JSON.stringify(O.LISTING_DESIGNS)), { 1630282472: 'ASTRONAUT_9809', 4456808584: 'BLOOMING_16014' }, 'the table holds the astronaut and the blooming listings only'); }
{ const al = ctxOf(W, { aliases: { 1630282472: { sku: 'ASTRONAUT_' } } });
  eq([read(astro(), al).designSku, read(astro(), al).skuSource], ['ASTRONAUT_', 'alias'], 'a person\'s listing-wide answer wins');
  const by = ctxOf(W, { aliases: { 1630282472: { bySku: { 'CO-GOLDCHARMO': 'MAPLE_8065' } } } });
  eq(read(astro({ sku: 'CO-GOLDCHARMO' }), by).designSku, 'MAPLE_8065', 'a person\'s answer for the SKU wins');
  const kind = ctxOf(W, { aliases: { 1630282472: { listingKind: { kind: 'regular', by: 'Paul' } } } });
  eq(read(astro(), kind).designSku, 'ASTRONAUT_9809', 'a listing kind alone is no design: the table still answers');
  const oc = ctxOf(W, { optionMaps: { 1630282472: { metal: { '14k gold filled': { field: 'design', value: 'COMET_9427' } } } } });
  eq(read(astro(), oc).designSku, 'COMET_9427', 'an option a person mapped to a charm still decides');
  const gone = world(); const me = gone.masterEntry; gone.masterEntry = s => (String(s).toUpperCase() === 'ASTRONAUT_9809' ? null : me(s));
  const g = read(astro(), ctxOf(gone));
  ok(g.designSku !== 'ASTRONAUT_' && g.problems.some(p => p.kind === 'unmatchedSku' && p.reason === 'not in any master file'), 'ASTRONAUT_9809 not in the library: waits as an unknown SKU, never another design'); }
pass('astronaut: listing 1630282472 is ASTRONAUT_9809, its words stay engraving text, a person\'s answer wins');

/* ── 9b · BLOOMING: listing 4456808584 (order 4205305534), Etsy SKU BLOOMING_40376 in no master file → BLOOMING_16014 (Paul, 19 Oct 2026) ── */
{ const bloom = over => astro(Object.assign({ listingId: '4456808584', sku: 'BLOOMING_40376', title: 'Blooming Dandelion Charm: Sterling Silver, Gold Filled, 14K Solid Gold Floral Jewelry Gift', personalization: ['Sharney ( can you add a small heart too)'],
    variations: [{ name: 'Metal', value: 'Sterling Silver' }, { name: 'Personalization', value: 'Sharney ( can you add a small heart too)' }] }, over || {}));
  const sp = read(bloom(), null, { receiptId: '4205305534' });
  eq([sp.designSku, sp.skuSource, (sp.problems || []).some(p => p.kind === 'unknownSku')], ['BLOOMING_16014', 'alias', false], 'order 4205305534: BLOOMING_16014, no "Unknown SKU" for BLOOMING_40376');
  eq(O.listingDesign('4456808584'), 'BLOOMING_16014', 'the table answers for the listing');
  const lc = ctxOf(W, { aliases: { 4456808584: { sku: 'MAPLE_8065' } } });
  eq(read(bloom(), lc).designSku, 'MAPLE_8065', 'a person\'s saved design for the listing wins'); }
pass('blooming: listing 4456808584 is BLOOMING_16014');

/* ── 10 · every other listing reads as before ── */
{ const plain = read({ transactionId: String(++tx), listingId: '555000111', sku: 'ASTRONAUT_9809', title: 'Astronaut Charm', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: [{ name: 'Metal', value: 'Gold' }] });
  eq([plain.designSku, plain.skuSource], ['ASTRONAUT_9809', 'transaction'], 'another listing carrying ASTRONAUT_9809 is read by its transaction, as before');
  const nosku = read(astro({ listingId: '1630282473' }));
  eq([nosku.designSku, kinds(nosku)], [null, ['unmatchedSku']], 'a listing one number off with no SKU still waits');
  const neck = read({ transactionId: String(++tx), listingId: '555000222', sku: 'HUGGIE HOOPS-ZODIAC', title: 'Zodiac Necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: ['Leo'], variations: [{ name: 'Metal', value: 'Gold' }, { name: 'Length', value: '18"' }] });
  ok(!neck.listingProfile, 'a necklace line with the zodiac SKU is no profiled huggie');
  const words = read({ transactionId: String(++tx), listingId: '555000333', sku: 'MAPLE_8065', title: 'Maple Leaf Necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: ['Rune Wunjo is my Capricorn'], variations: [{ name: 'Metal', value: 'Gold' }, { name: 'Length', value: '18"' }] });
  eq([words.designSku, words.skuSource, !!words.listingProfile, words.personalization], ['MAPLE_8065', 'transaction', false, ['Rune Wunjo is my Capricorn']], 'rune and sign words on any other listing change nothing');
  eq([O.symbolListing({ listingId: '1744307981' }, { form: 'unknown' }, '').id, O.symbolListing({ listingId: '1730115500' }, null, '').id, O.symbolListing({ listingId: '1', sku: 'HUGGIE HOOPS- VIKING RUNES' }, { form: 'huggie' }, 'HUGGIE HOOPS- VIKING RUNES').id, O.symbolListing({ listingId: '1', sku: 'HUGGIE HOOPS-ZODIAC' }, { form: 'necklace' }, 'HUGGIE HOOPS-ZODIAC')],
    ['zodiac-huggie', 'rune-huggie', 'rune-huggie', null], 'the profile rows: by listing, or by SKU on a huggie line only');
  eq(O.SYMBOL_LISTINGS.map(x => [x.id, x.listings, x.base, x.names.length]), [['zodiac-huggie', ['1744307981'], 'ZODIAC_HUGGIE-', 12], ['rune-huggie', ['1730115500'], 'RUNE_HUGGIE_CHARM-', 12]], 'two rows, twelve names each');
  eq([O.RUNE_NAMES, O.RUNES_ABSENT], [RUNES, ABSENT], 'the runes the library holds, in order, and the twelve it does not'); }
pass('every other listing reads as before');

console.log(`huggie-zodiac-runes-profile: ${n} checks passed`);
