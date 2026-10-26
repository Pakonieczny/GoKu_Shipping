// CHARMONLY (Paul, 19 Oct 2026, orders 4203879769 / 4202871419): two charm-only listings that sell the SAME charm as a necklace charm or as a huggie charm set ("Charm Type: Necklace CHARM / CHARM + Engraving /
// Huggie CHARM SET"): "This charm only listing has to be profiled to match the SKU from image #1 for the Necklace Option and image #3 SKU for the Huggie Options."
//   · listing 1816093385 "Viking Runes Charm Rune Pendant Add On Charm …": order 4203879769 (Etsy SKU "VIKING RUNES_URUZ", Gold Filled, necklace) was "Unknown SKU" / "No vector available"
//     → the rune the buyer chose (the SKU, the drop-down, the box) picks the member; necklace → RUNE_NECKLACE_CHARM-<n> (one piece), huggie → RUNE_HUGGIE_CHARM-<n> (Left + Right)
//   · listing 1801964100 "Playing Cards Charm Add On Charm …": order 4202871419 was matched to "CARDS 2 - (4 CARDS)" → necklace CARDS 1 - (2 CARDS), huggie CARDS 1 - (2 CARDS) (HUGGIE), whatever SKU Etsy carries
//   → the form decides, never the other form's drawing; a missing member waits with one plain line; the rune is the buyer's choice, never engraved (unless "CHARM + Engraving"); a person's answer for the line wins,
//     a listing-wide saved answer does not; every other listing reads as before.
//   node tests/charm-nest/charm-only-by-form.cjs      Offline: no browser, no network. The real interpretLine; the library names as the real master writes them (RUNE_NECKLACE_CHARM-0 … -23, options-link-data.json).
'use strict';
const assert = require('assert'), path = require('path');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);

/* ── the world: both rune families, the playing cards charm under its three names and its huggie, the four-card drawing and its huggie, two unrelated designs ── */
const F = f => 'charmnest/master/' + f + '.ai';
const CARDS1 = 'CARDS 1 - (2 CARDS)', CARDS1H = 'CARDS 1 - (2 CARDS) (HUGGIE)', CARDS2 = 'CARDS 2 - (4 CARDS)', CARDS2H = 'CARDS 2 - (4 CARDS) (HUGGIE)';
function world(o = {}) {
  const lib = new Map(), put = (sku, file, w = 18, h = 35, holes = 1) => lib.set(sku.toUpperCase(), { sku: sku.toUpperCase(), aiPath: F(file), widthPt: w, heightPt: h, members: 1, holes, sizes: null, blocked: null });
  for (let i = 0; i < 24; i++) if (!(o.noNeck || []).includes(i)) put((o.neckBase || 'RUNE_NECKLACE_CHARM-') + i, 'RUNE_NECKLACE_CHARM-' + i);
  for (let i = 0; i < 12; i++) if (!(o.noHug || []).includes(i)) put('Rune_Huggie_Charm-' + i, 'SKUs_CORRECTED_2024-1014_Rune_Huggie_Charm-' + i, 12, 24);
  if (!o.noCards1) { put(CARDS1, 'CARDS_1_-_2_CARDS_', 26, 30); put('CARDS 1 - 2 CARDS', 'CARDS_1_-_2_CARDS_', 26, 30); }
  put('PLAYING_0805', 'CARDS_1_-_2_CARDS_', 26, 30);
  if (!o.noCards1H) put(CARDS1H, 'CARDS_1_-_2_CARDS__HUGGIE_', 20, 26);
  put(CARDS2, 'CARDS_2_-_4_CARDS_', 27.6, 27.4); put(CARDS2H, 'CARDS_2_-_4_CARDS__HUGGIE_', 23, 29.5, 7);
  put('MAPLE_8065', 'MAPLE_8065', 30, 33); put('COMET_9427', 'COMET_9427', 30, 33);
  const byFile = new Map(); for (const [k, e] of lib) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
  const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
  return { masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: s => { const e = lib.get(String(s || '').toUpperCase()); return e ? byFile.get(e.aiPath) || [] : []; }, masterSkus: () => [...lib.keys()] };
}
const ctxOf = (w, extra) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {} }, w || world(), extra || {});
const W = world();
let tx = 7600000000;
const read = (line, c, order) => O.interpretLine(Object.assign({ receiptId: '4203000000', updateTs: 1, buyerMessage: '', staffNote: '' }, order || {}), JSON.parse(JSON.stringify(line)), c || ctxOf(W));
const kinds = sp => (sp.problems || []).map(p => p.kind);
const members = sp => (sp.pair && sp.pair.members || []).map(m => m.side + ':' + m.sku);

/* ── the lines as Etsy gives them ── */
const RUNES = 'Viking Runes Charm Rune Pendant Add On Charm Norse Mythology Charm Viking Jewelry Nordic Necklace Christmas Gift Viking Alphabet';
const CARDS = 'Playing Cards Charm Add On Charm Poker Card Charm Ace of Hearts Jewelry Birthday Gift Card Player Gift';
const NECK = 'Necklace CHARM', ENG = 'CHARM + Engraving', HUG = 'Huggie CHARM SET';
// o: { sku, type (Charm Type), rune (the rune drop-down), box, note, listingId }
const rune = (o = {}) => Object.assign({ transactionId: String(++tx), listingId: o.listingId || '1816093385', sku: o.sku != null ? o.sku : '', title: RUNES, quantity: 1, metalKey: 'gold', metalLabel: 'Gold Filled',
  personalization: o.box ? [o.box] : [], buyerMessage: o.note || '',
  variations: [{ name: 'Metal Choice', value: 'Gold Filled' }].concat(o.type ? [{ name: 'Charm Type', value: o.type }] : [], o.rune ? [{ name: 'Viking Rune', value: o.rune }] : [], o.box ? [{ name: 'Personalization', value: o.box }] : []) });
const card = (o = {}) => Object.assign({ transactionId: String(++tx), listingId: o.listingId || '1801964100', sku: o.sku != null ? o.sku : CARDS2, title: CARDS, quantity: 1, metalKey: 'silver', metalLabel: 'Sterling Silver',
  personalization: o.box ? [o.box] : [], buyerMessage: '',
  variations: [{ name: 'Metal Choice', value: 'Sterling Silver' }].concat(o.type ? [{ name: 'Charm Type', value: o.type }] : [], o.box ? [{ name: 'Personalization', value: o.box }] : []) });
const NAMES = O.RUNE_NAMES, FUTHARK = O.RUNE_NAMES.concat(O.RUNES_ABSENT);
const skuOf = name => 'VIKING RUNES_' + name.toUpperCase();

/* ── 1 · the twelve runes, necklace: by the SKU, by the drop-down, by the box → RUNE_NECKLACE_CHARM-<n>, one piece, never engraved ── */
NAMES.forEach((name, i) => {
  const want = 'RUNE_NECKLACE_CHARM-' + i;
  for (const [how, ln] of [['its SKU', rune({ sku: skuOf(name), type: NECK })], ['its SKU, no Charm Type', rune({ sku: skuOf(name) })], ['the drop-down', rune({ rune: name, type: NECK })],
    ['the drop-down "<name> - meaning"', rune({ rune: name + ' - Meaning', type: NECK })], ['the box', rune({ box: name, type: NECK })], ['the box "Rune <name>"', rune({ box: 'Rune ' + name })]]) {
    const sp = read(ln);
    eq([sp.designSku, sp.skuSource, kinds(sp), sp.pieceCount, sp.lineForm === 'huggie'], [want, 'profile', [], 1, false], `${name} by ${how}: ${want}, one piece, no question`);
    eq([sp.listingProfile.profile, sp.listingProfile.form, sp.listingProfile.symbols.map(x => [x.name, x.sku])], ['rune-charm-only', 'necklace', [[name, want]]], `${name} by ${how}: the rune and its charm are facts`);
    eq([sp.engraveCandidate, sp.engraveNever && sp.engraveNever.rule, sp.engraving || null], [false, 'charm-only-choice', null], `${name} by ${how}: the rune is the buyer's choice, never engraved`);
  }
  eq(read(rune({ rune: name, type: NECK })).options.find(o => o.name === 'Viking Rune').mapped, { field: 'design', value: want, source: 'rule:listing-profile' }, `${name}: the rune drop-down is answered by its charm`);
});
// the necklace family holds all 24 runes (Algiz is RUNE_NECKLACE_CHARM-14 on Etsy's own variation SKUs: options-link-data.json)
O.RUNES_ABSENT.forEach((name, j) => eq([read(rune({ sku: skuOf(name), type: NECK })).designSku], ['RUNE_NECKLACE_CHARM-' + (12 + j)], `${name} necklace: RUNE_NECKLACE_CHARM-${12 + j}`));
pass('the twelve runes by SKU, drop-down and box are RUNE_NECKLACE_CHARM-0 … -11 (and the other twelve -12 … -23), one piece, never engraved');

/* ── 2 · order 4203879769 as it is: SKU "VIKING RUNES_URUZ", Gold Filled, necklace ── */
{ const sp = read(rune({ sku: 'VIKING RUNES_URUZ', type: NECK }), null, { receiptId: '4203879769' });
  eq([sp.designSku, sp.skuSource, kinds(sp), sp.pieceCount, sp.lineForm, sp.materialKey], ['RUNE_NECKLACE_CHARM-1', 'profile', [], 1, 'necklace', 'gold'], 'order 4203879769: RUNE_NECKLACE_CHARM-1, one gold-filled necklace piece');
  ok(!sp.problems.some(p => /not in any master file|no SKU/.test(p.reason || '')), 'never "Unknown SKU" / "No vector available"');
  eq(sp.pair.notes.filter(x => /^Rune/.test(x)), ['Rune: Uruz (RUNE_NECKLACE_CHARM-1), from the piece\'s SKU'], 'the order\'s reading says the rune and its charm once');
  eq(sp.listingProfile.was, { sku: 'VIKING RUNES_URUZ', source: 'transaction' }, 'and keeps the reading it replaced (the unknown SKU)');
  const before = read(rune({ sku: 'VIKING RUNES_URUZ', type: NECK, listingId: '1700000123' }));
  ok(before.designSku === 'VIKING RUNES_URUZ' && before.problems.some(p => /not in any master file/.test(p.reason || '')), 'the same SKU on another listing reads as before (the profile is the listing\'s)'); }
pass('order 4203879769: RUNE_NECKLACE_CHARM-1, no Unknown SKU');

/* ── 3 · the Huggie option: RUNE_HUGGIE_CHARM-<n>, Left + Right ── */
NAMES.forEach((name, i) => {
  const want = 'RUNE_HUGGIE_CHARM-' + i;
  for (const [how, ln] of [['its SKU', rune({ sku: skuOf(name), type: HUG })], ['the drop-down', rune({ rune: name, type: HUG })], ['the box', rune({ box: name, type: HUG })]]) {
    const sp = read(ln);
    eq([sp.designSku, sp.skuSource, kinds(sp), sp.pieceCount, sp.pair.sides, sp.lineForm], [want, 'profile', [], 2, ['L', 'R'], 'huggie'], `${name} huggie by ${how}: ${want} on both ears`);
    eq([sp.listingProfile.form, sp.engraveNever && sp.engraveNever.rule, sp.engraveCandidate], ['huggie', 'earring', false], `${name} huggie by ${how}: never engraved`);
  }
});
{ const sp = read(rune({ sku: 'VIKING RUNES_URUZ', type: HUG }));
  eq([sp.designSku, sp.pieceCount, sp.pair.sides, sp.pair.earring], ['RUNE_HUGGIE_CHARM-1', 2, ['L', 'R'], true], 'the Uruz huggie set: RUNE_HUGGIE_CHARM-1 on the Left and on the Right');
  eq(sp.pair.notes.filter(x => /^Rune/.test(x)), ['Rune: Uruz (RUNE_HUGGIE_CHARM-1, both ears), from the piece\'s SKU'], 'said once'); }
pass('the Huggie option: RUNE_HUGGIE_CHARM-0 … -11 on both ears');

/* ── 4 · the form decides, never the other form's drawing ── */
{ const neck = read(rune({ sku: 'VIKING RUNES_URUZ', type: NECK })), hug = read(rune({ sku: 'VIKING RUNES_URUZ', type: HUG }));
  eq([neck.designSku, hug.designSku], ['RUNE_NECKLACE_CHARM-1', 'RUNE_HUGGIE_CHARM-1'], 'the same SKU: the necklace option is the necklace rune, the huggie option the huggie rune');
  eq(read(rune({ sku: 'RUNE_NECKLACE_CHARM-1', type: HUG })).designSku, 'RUNE_HUGGIE_CHARM-1', 'a necklace member SKU on a huggie line: the huggie member of the same rune');
  eq(read(rune({ sku: 'Rune_Huggie_Charm-7', type: NECK })).designSku, 'RUNE_NECKLACE_CHARM-7', 'a huggie member SKU on a necklace line: the necklace member of the same rune');
  const c1 = read(card({ type: NECK })), c2 = read(card({ type: HUG }));
  eq([c1.designSku, c2.designSku], [CARDS1, CARDS1H], 'cards: necklace and huggie each their own entry'); }
pass('the form picks the entry: necklace and huggie never cross');

/* ── 5 · a rune the library lacks waits with one plain line, never another rune ── */
{ const sp = read(rune({ sku: 'VIKING RUNES_ALGIZ', type: HUG }));
  eq([sp.designSku, sp.skuSource, kinds(sp), sp.formHold.reason], [null, null, ['unmatchedSku'], 'The Algiz huggie charm is not in the repo yet (the repo has the 12 huggie runes Fehu … Jera)'], 'Algiz huggie (not in the library): waits with ONE plain line');
  ok(!sp.problems.some(p => p.kind === 'needsMapping') && !sp.problems.some(p => /not in any master file/.test(p.reason || '')), 'no second question, no unknown-SKU line beside it');
  eq(read(rune({ box: 'Algiz', type: HUG })).formHold.reason, 'The Algiz huggie charm is not in the repo yet (the repo has the 12 huggie runes Fehu … Jera)', 'the same from the box');
  const m = read(rune({ sku: 'VIKING RUNES_URUZ', type: NECK }), ctxOf(world({ noNeck: [1] })));
  eq([m.designSku, m.formHold.reason, kinds(m)], [null, 'The Uruz necklace charm is not in the repo yet (RUNE_NECKLACE_CHARM-1 is not in the library)', ['unmatchedSku']], 'RUNE_NECKLACE_CHARM-1 missing: waits naming it (never another rune, never the huggie)');
  const h = read(rune({ sku: 'VIKING RUNES_URUZ', type: HUG }), ctxOf(world({ noHug: [1] })));
  eq([h.designSku, h.formHold.reason], [null, 'The Uruz huggie charm is not in the repo yet (RUNE_HUGGIE_CHARM-1 is not in the library)'], 'RUNE_HUGGIE_CHARM-1 missing: waits, never the necklace Uruz');
  const none = read(rune({ type: NECK }));
  eq([none.designSku, none.formHold.reason], [null, 'Which rune? The buyer named none (the personalisation box is empty)'], 'no rune at all: one plain question');
  const two = read(rune({ type: NECK, box: 'Uruz and Fehu' }));
  eq([two.designSku, two.formHold.reason], [null, 'Which rune? The buyer names Uruz and Fehu'], 'two runes for one charm: a person says');
  eq(read(rune({ sku: 'VIKING RUNES_URUZ', rune: 'Fehu', type: NECK })).designSku, 'RUNE_NECKLACE_CHARM-1', 'the line\'s SKU comes before the drop-down'); }
pass('a missing rune or member waits with one plain line naming it');

/* ── 6 · "CHARM + Engraving": the necklace charm, the box is engraved as usual ── */
{ const sp = read(rune({ sku: 'VIKING RUNES_URUZ', type: ENG, box: 'Erik 2026' }));
  eq([sp.designSku, sp.pieceCount, kinds(sp).filter(k => k === 'unmatchedSku'), sp.engraveCandidate, !!sp.engraveNever], ['RUNE_NECKLACE_CHARM-1', 1, [], true, false], 'CHARM + Engraving: RUNE_NECKLACE_CHARM-1, the box is words to engrave');
  const r = read(rune({ sku: 'VIKING RUNES_URUZ', type: ENG, box: 'Fehu' }));
  eq([r.designSku, r.engraveCandidate], ['RUNE_NECKLACE_CHARM-1', true], 'with an engraving chosen the box is never read as the rune choice');
  const c = read(card({ type: ENG, box: 'Ace' }));
  eq([c.designSku, c.pieceCount, c.engraveCandidate], [CARDS1, 1, true], 'cards CHARM + Engraving: the necklace charm, engraved'); }
pass('"CHARM + Engraving" is the necklace form with the buyer\'s words engraved');

/* ── 7 · the playing cards: CARDS 1 - (2 CARDS) / its (HUGGIE), whatever SKU the transaction carries ── */
for (const sku of [CARDS2, CARDS2H, '', 'PLAYING CARDS', CARDS1]) {
  const sp = read(card({ sku, type: NECK }), null, { receiptId: '4202871419' });
  eq([sp.designSku, sp.skuSource, kinds(sp), sp.pieceCount, sp.lineForm === 'huggie'], [CARDS1, 'profile', [], 1, false], `cards necklace, transaction SKU "${sku}": ${CARDS1}, one piece`);
  const h = read(card({ sku, type: HUG }));
  eq([h.designSku, h.skuSource, kinds(h), h.pieceCount, h.pair.sides], [CARDS1H, 'profile', [], 2, ['L', 'R']], `cards huggie, transaction SKU "${sku}": ${CARDS1H} on both ears`);
}
eq(read(card({ sku: CARDS2 })).designSku, CARDS1, 'no Charm Type at all: the necklace charm');
{ const sp = read(card({ sku: CARDS2, type: NECK }), null, { receiptId: '4202871419' });
  eq(sp.listingProfile.was, { sku: CARDS2, source: 'transaction' }, 'order 4202871419: it replaced the CARDS 2 - (4 CARDS) match');
  eq(sp.pair.notes.filter(x => /^Charm-only/.test(x)), [`Charm-only listing: ${CARDS1} (the necklace charm of this listing)`], 'said once');
  eq(read(card({ type: NECK, box: 'Ace of hearts please' })).engraveCandidate, true, 'a cards necklace keeps the buyer\'s words as words (no symbol to choose)'); }
{ const a = read(card({ type: NECK }), ctxOf(world({ noCards1: true })));
  eq(a.designSku, 'PLAYING_0805', 'the same drawing under its other name when the library writes it so');
  const m = read(card({ type: HUG }), ctxOf(world({ noCards1H: true })));
  eq([m.designSku, m.formHold.reason, members(m)], [null, `The huggie charm of this listing is not in the repo yet (${CARDS1H} is not in the library)`, []], 'the huggie entry missing: waits, never the necklace charm, never CARDS 2 (HUGGIE)'); }
pass('the playing cards: necklace CARDS 1 - (2 CARDS), huggie CARDS 1 - (2 CARDS) (HUGGIE), whatever the transaction carries');

/* ── 8 · saved answers: a listing-wide one no longer wins, a person's answer for the line does ── */
{ const lw = ctxOf(W, { aliases: { 1801964100: { sku: CARDS2, v: 2 } } });
  eq([read(card({ type: NECK }), lw).designSku, read(card({ sku: '', type: NECK }), lw).designSku], [CARDS1, CARDS1], 'the listing-wide saved answer CARDS 2 - (4 CARDS) no longer wins');
  const by = ctxOf(W, { aliases: { 1801964100: { bySku: { [CARDS2]: CARDS2, [CARDS2H]: CARDS2H } } } });
  eq([read(card({ type: NECK }), by).designSku, read(card({ type: HUG }), by).designSku], [CARDS1, CARDS1H], 'links under the SKU every product of the listing shares no longer win');
  const hug = ctxOf(W, { aliases: { 1801964100: { huggie: CARDS2H } } });
  eq(read(card({ sku: '', type: HUG }), hug).designSku, CARDS1H, 'the listing\'s huggie-set answer no longer wins');
  const rw = ctxOf(W, { aliases: { 1816093385: { sku: 'RUNE_NECKLACE_CHARM-0', v: 2 } } });
  eq(read(rune({ sku: '', type: NECK, box: 'Uruz' }), rw).designSku, 'RUNE_NECKLACE_CHARM-1', 'a listing-wide answer for the runes (every buyer one rune) no longer wins'); }
{ const oc = ctxOf(W, { optionMaps: { 1801964100: { 'charm type': { 'necklace charm': { field: 'design', value: 'COMET_9427' } } } } });
  eq([read(card({ type: NECK }), oc).designSku, read(card({ type: NECK }), oc).skuSource], ['COMET_9427', 'option'], 'an option a person mapped to a charm wins');
  const own = ctxOf(W, { aliases: { 1816093385: { bySku: { 'VIKING RUNES_URUZ': 'MAPLE_8065' } } } });
  const a = read(rune({ sku: 'VIKING RUNES_URUZ', type: NECK }), own);
  eq([a.designSku, a.skuSource, !!a.listingProfile], ['MAPLE_8065', 'alias', false], 'a link saved under the line\'s own rune SKU (that one product) wins');
  eq(read(rune({ sku: 'VIKING RUNES_FEHU', type: NECK }), own).designSku, 'RUNE_NECKLACE_CHARM-0', 'and reaches no other rune');
  const pc = ctxOf(W, { aliases: { 1801964100: { pairBySku: { [CARDS2]: { L: 'MAPLE_8065', R: 'COMET_9427' } } } } });
  eq(members(read(card({ type: HUG }), pc)), ['L:MAPLE_8065', 'R:COMET_9427'], 'a pair a person saved for the line\'s listing and SKU wins'); }
pass('a listing-wide saved answer (what made order 4202871419 CARDS 2) no longer wins; a person\'s answer for the line does');

/* ── 9 · every other listing reads as before ── */
{ const hs = read({ transactionId: String(++tx), listingId: '1730115500', sku: 'HUGGIE HOOPS- VIKING RUNES', title: 'Norse Runes Charm Huggie Hoop Earrings', quantity: 1, metalKey: 'silver', metalLabel: 'Sterling Silver', personalization: [], buyerMessage: '', variations: [{ name: 'Metal Choice', value: 'Sterling Silver' }, { name: 'Rune', value: 'Wunjo' }] });
  eq([hs.designSku, hs.listingProfile.profile], ['RUNE_HUGGIE_CHARM-7', 'rune-huggie'], 'the rune huggie listing 1730115500 keeps its own row (HUGSYM)');
  const studs = read(card({ listingId: '1749318013', sku: CARDS2, type: '' }));
  eq([studs.designSku, !!studs.listingProfile], [CARDS2, false], 'the card stud listing 1749318013 keeps CARDS 2 - (4 CARDS)');
  const other = read(card({ listingId: '1700000999', sku: CARDS2, type: NECK }));
  eq([other.designSku, !!other.listingProfile], [CARDS2, false], 'CARDS 2 - (4 CARDS) on any other listing reads as before');
  eq([O.charmOnlyListing({ listingId: '1816093385' }).id, O.charmOnlyListing({ listingId: '1801964100' }).id, O.charmOnlyListing({ listingId: '1871838409' }), O.charmOnlyListing({ listingId: '' })], ['rune-charm-only', 'cards-charm-only', null, null], 'the table covers exactly its two listings');
  eq([O.vikingRuneOfSku('VIKING RUNES_URUZ'), O.vikingRuneOfSku('VIKING RUNES_ISA'), O.vikingRuneOfSku('Viking Runes-Thurisaz'), O.vikingRuneOfSku('VIKING_15953'), O.vikingRuneOfSku('HUGGIE HOOPS- VIKING RUNES')], ['Uruz', 'Isa', 'Thurisaz', '', ''], 'the SKU reader: "VIKING RUNES_<RUNE>" only');
  eq(O.CHARM_ONLY_RUNE_NECKLACE_BASE, 'RUNE_NECKLACE_CHARM-', 'the necklace base, in one place'); }
{ const lw = read(rune({ sku: 'VIKING RUNES_URUZ', type: NECK }), ctxOf(world({ neckBase: 'RUNE_NECKLACE_CHARM_' })));
  eq(lw.designSku, 'RUNE_NECKLACE_CHARM_1', 'a library that writes RUNE_NECKLACE_CHARM_1 is found by its loose key'); }
pass('every other listing reads as before; the necklace base is found by the master\'s own spelling');

console.log(`charm-only-by-form: ${n} checks passed`);
