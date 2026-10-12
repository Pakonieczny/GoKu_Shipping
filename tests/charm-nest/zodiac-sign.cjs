// ZODSIGN (Paul, 12 Oct 2026 18:15 UTC, order 4200256896, listing "Huggie Hoop Earrings with Astrological Charm, Handcrafted in Gold Vermeil, Silver, Solid 14k Gold", Etsy SKU HUGGIE HOOPS-ZODIAC):
// "the buyer chose the Capricorn zodiac option … however you chose the Libra vector file".
//
//  THE CAUSE (section 1, read with the real code): the listing has two drop-downs (METAL CHOICE, HOOP SIZE) and a personalisation box; the buyer's sign, "Capricorn", is in the box. The SKU
//  HUGGIE HOOPS-ZODIAC is shared by all twelve signs, but the master keeps it on ONE drawing: HUGGIE HOOPS- ZODIAC, HUGGIE HOOPS- ZODIAC AVATAR and ZODIAC ICONS LIBRA (HUGGIE) are one design
//  file, the Libra huggie. resolveSku takes a SKU the master knows (source "spelling") and nothing read the box for a sign, so every sign was Libra, with no hold.
//
//  THE FIX (charm-nest-orders.js section 3c, zodiacRead): a shared zodiac SKU + a sign the buyer named picks THAT sign's charm of the line's form; a sign the repo has no charm of waits with one plain line
//  ("The Capricorn charm is not in the repo yet"), never another sign's file; no sign / two signs wait ("Which zodiac sign?"); the hold clears itself when the library gets the master.
//
//   node tests/charm-nest/zodiac-sign.cjs      Offline: no browser, no network, no Etsy call, nothing live, no AI. No buyer data (the names and files are the master's, read 10 Oct 2026).
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);

/* ── the world: the master's real zodiac names, design files and sizes (the staged index of 10 Oct 2026); one drawing under several names shares one file ── */
const F = f => 'charmnest/master/' + f + '.ai';
const E = [
  // the seven sign designs of the huggie hoops (8.1 x 10.0 mm for Libra …): the repo has these and no others
  ['HUGGIE HOOPS- ZODIAC', F('HUGGIE_HOOPS-_ZODIAC_AVATAR'), 22.84, 28.45], ['HUGGIE HOOPS- ZODIAC AVATAR', F('HUGGIE_HOOPS-_ZODIAC_AVATAR'), 22.84, 28.45], ['ZODIAC ICONS LIBRA (HUGGIE)', F('HUGGIE_HOOPS-_ZODIAC_AVATAR'), 22.84, 28.45],
  ['ZODIAC DISCS PISCES (HUGGIE)', F('ZODIAC_DISCS_PISCES_HUGGIE_'), 20.16, 29.02], ['ZODIAC DISCS SCORPIO (HUGGIE)', F('ZODIAC_DISCS_SCORPIO_HUGGIE_'), 19.67, 29.97], ['ZODIAC DISCS TAURUS (HUGGIE)', F('ZODIAC_DISCS_TAURUS_HUGGIE_'), 21.91, 26.78],
  ['ZODIAC VIRGO (HUGGIE)', F('ZODIAC_VIRGO_HUGGIE_'), 22.84, 30.76], ['ZODIAC ICONS LEO (HUGGIE)', F('ZODIAC_ICONS_LEO_HUGGIE_'), 22.91, 30.32], ['ZODIAC ICONS ARIES (HUGGIE)', F('ZODIAC_ICONS_ARIES_HUGGIE_'), 18.69, 28.52],
  // their necklace-size twins (plain names), some under a second name of the numbered necklace family; Aquarius has a necklace disc and NO huggie
  ['ZODIAC ICONS LIBRA', F('ZODIAC_ICONS_LIBRA'), 31.35, 32.84], ['ZODIAC ICONS LEO', F('ZODIAC_ICONS_LEO'), 30.21, 34.67], ['ZODIAC ICON - BEADY', F('ZODIAC_ICONS_LEO'), 30.21, 34.67], ['ZODIAC ICONS ARIES', F('ZODIAC_ICONS_ARIES'), 27.94, 36.5], ['ARIES', F('ZODIAC_ICONS_ARIES'), 27.94, 36.5],
  ['ZODIAC VIRGO', F('ZODIAC_NECKLACE_CHARM-5'), 33.68, 38.57], ['ZODIAC_NECKLACE_CHARM-5', F('ZODIAC_NECKLACE_CHARM-5'), 33.68, 38.57],
  ['ZODIAC DISCS PISCES', F('ZODIAC_NECKLACE_DISC-11'), 29.65, 35.03], ['ZODIAC DISCS SCORPIO', F('ZODIAC_NECKLACE_DISC-7'), 29.65, 35.03], ['ZODIAC DISC', F('ZODIAC_NECKLACE_DISC-7'), 29.65, 35.03], ['ZODIAC DISCS TAURUS', F('ZODIAC_NECKLACE_DISC-1'), 29.65, 35.03], ['ZODIAC DISCS AQUARIUS', F('ZODIAC_NECKLACE_DISC-10'), 29.65, 35.03],
  // look-alikes that are not zodiac designs, and generic SKUs that merely hold the word
  ['PISCES_68933 (HUGGIE)', F('PISCES_68933_HUGGIE_'), 22.23, 29.36], ['LEAPING_12924 (HUGGIE)', F('PISCES_68933_HUGGIE_'), 22.23, 29.36], ['SCORPION (HUGGIE)', F('SCORPION_HUGGIE_'), 22.12, 30.33], ['HUGGIE HOOPS- HEALTH5-CANCER', F('HUGGIE_HOOPS-_HEALTH5-CANCER'), 16.36, 30.24],
  ['ZODIAC_39104', F('ZODIAC_39104'), 35.99, 41.32], ['ZODIAC_90351 (HUGGIE)', F('LEAPING_61844_HUGGIE_'), 19.03, 26.7], ['ZODIAC', F('ZODIAC_EARRING_DISC-0'), 25.68, 25.68],
  ['MAPLE_8065', F('MAPLE_8065'), 30, 33], ['MAPLE_8065 (HUGGIE)', F('MAPLE_8065_HUGGIE_'), 22, 25]
];
const SIGNS = ['Aries', 'Taurus', 'Gemini', 'Cancer', 'Leo', 'Virgo', 'Libra', 'Scorpio', 'Sagittarius', 'Capricorn', 'Aquarius', 'Pisces'];
const base = () => { const m = new Map(E.map(([sku, aiPath, widthPt, heightPt]) => [sku, { sku, aiPath, widthPt, heightPt, members: 3, sizes: null, blocked: null }]));
  const put = (sku, aiPath, widthPt, heightPt, members) => { if (!m.has(sku)) m.set(sku, { sku, aiPath, widthPt, heightPt, members, sizes: null, blocked: null }); };
  for (let i = 0; i < 12; i++) {                                 // the numbered families (index 0 = Aries ... 11 = Pisces); a member the list above already holds keeps its file
    put('ZODIAC_NECKLACE_DISC-' + i, F('ZODIAC_NECKLACE_DISC-' + i), 29.65, 35.03, 3);
    put('ZODIAC_NECKLACE_CHARM-' + i, F('ZODIAC_NECKLACE_CHARM-' + i), 33, 38, 2);
    put('ZODIAC_EARRINGS-' + i, F('ze' + i), 25, 25, 2);
    put('ZODIAC_EARRING_DISC-' + i, F('ZODIAC_EARRING_DISC-' + i), 25, 25, 2);
  }
  return m; };
function world(extra) {
  const lib = base(); for (const [sku, aiPath, widthPt, heightPt] of extra || []) lib.set(sku, { sku, aiPath, widthPt, heightPt, members: 1, sizes: null, blocked: null });
  const byFile = new Map(); for (const [k, e] of lib) if (e.aiPath) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
  const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
  return { lib, masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: s => { const e = lib.get(String(s || '').toUpperCase()); return e && e.aiPath ? byFile.get(e.aiPath) || [] : []; }, masterSkus: () => [...lib.keys()] };
}
const ctxOf = (w, extra) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {} }, w, extra || {});
const W0 = world();

/* ── the real shape of order 4200256896's line: the listing's own title and SKU, METAL CHOICE and HOOP SIZE, and the buyer's sign in the personalisation box ── */
const TITLE = 'Huggie Hoop Earrings with Astrological Charm, Handcrafted in Gold Vermeil, Silver, Solid 14k Gold | Handcrafted Horoscope Jewelry';
let tx = 7100000000;
const huggie = (pers, over) => Object.assign({ transactionId: String(++tx), listingId: '1000000042', sku: 'HUGGIE HOOPS-ZODIAC', title: TITLE, quantity: 1, metalKey: 'gold', metalLabel: '14k Gold Filled', personalization: pers ? [pers] : [], buyerMessage: '',
  variations: [{ name: 'METAL CHOICE', value: '14k Gold Filled' }, { name: 'HOOP SIZE', value: '8mm' }].concat(pers ? [{ name: 'Personalization', value: pers }] : []) }, over || {});
const read = (line, c, order) => O.interpretLine(Object.assign({ receiptId: '4200256896', updateTs: 1 }, order || {}), JSON.parse(JSON.stringify(line)), c || ctxOf(W0));
const plain = sp => ({ design: sp.designSku || null, src: sp.skuSource || null, problems: (sp.problems || []).map(p => p.kind + (p.optionName ? ':' + p.optionName : '')), pieces: sp.pieceCount, hold: sp.formHold ? sp.formHold.reason : null });
const MASTER = { Libra: 'ZODIAC ICONS LIBRA (HUGGIE)', Pisces: 'ZODIAC DISCS PISCES (HUGGIE)', Scorpio: 'ZODIAC DISCS SCORPIO (HUGGIE)', Taurus: 'ZODIAC DISCS TAURUS (HUGGIE)', Virgo: 'ZODIAC VIRGO (HUGGIE)', Leo: 'ZODIAC ICONS LEO (HUGGIE)', Aries: 'ZODIAC ICONS ARIES (HUGGIE)' };
const MISSING = ['Capricorn', 'Aquarius', 'Gemini', 'Cancer', 'Sagittarius'];

/* ── 1 · the cause, with the real code: without the master list the line reads as it always did, and that was Libra for every sign ── */
{ const old = read(huggie('Capricorn'), ctxOf(W0, { masterSkus: undefined }));
  eq([old.designSku, old.skuSource, old.problems.length], ['HUGGIE HOOPS- ZODIAC', 'spelling', 0], 'old reading: the master spelling of the SKU, no question, the sign in the box never read');
  ok(O.sameDesign('HUGGIE HOOPS- ZODIAC', 'ZODIAC ICONS LIBRA (HUGGIE)', W0.masterEntry), 'and that SKU is the very drawing of the Libra huggie (one design file under three names)');
  eq([O.skuSign('ZODIAC ICONS LIBRA (HUGGIE)'), O.skuSign('HUGGIE HOOPS- ZODIAC'), O.skuSign('SCORPION (HUGGIE)'), O.skuSign('ZODIAC_39104'), O.skuSign('LEOPARD_15337')], ['Libra', '', '', '', ''], 'a SKU names a sign only by its English name as a whole word');
  const fam = O.sharedZodiac('HUGGIE HOOPS- ZODIAC', ctxOf(W0)); eq(fam.named, ['ZODIAC ICONS LIBRA (HUGGIE)'], 'the shared SKU sits on the drawing of the one master SKU that names a sign');
  for (const s of ['ZODIAC ICONS LIBRA (HUGGIE)', 'ZODIAC_39104', 'ZODIAC', 'ZODIAC_NECKLACE_DISC-7', 'ZODIAC_EARRINGS-6', 'MAPLE_8065', 'PISCES_68933 (HUGGIE)']) eq(O.sharedZodiac(s, ctxOf(W0)), null, s + ' is not a shared zodiac SKU');
  eq(['HUGGIE HOOPS- ZODIAC AVATAR', 'ZODIAC DISC', 'ZODIAC ICON - BEADY'].map(s => !!O.sharedZodiac(s, ctxOf(W0))), [true, true, true], 'the other generic names on a sign drawing are shared too (the avatar alias, the Scorpio disc, the Leo icon)'); }
pass('cause: HUGGIE HOOPS-ZODIAC is the Libra huggie drawing under a generic name, the box was never read, so every sign was Libra with no hold');

/* ── 2 · order 4200256896: Capricorn waits, with one plain line, and is never Libra ── */
{ const sp = read(huggie('Capricorn')), hp = sp.problems.find(p => p.formHold);
  eq([sp.designSku, sp.skuSource, sp.formHold.reason, sp.problems.length], [null, null, 'The Capricorn charm is not in the repo yet', 1], 'Capricorn on HUGGIE HOOPS-ZODIAC: no design on the row, one plain line');
  eq([hp.kind, hp.reason, hp.zodiac, hp.sku], ['unmatchedSku', 'The Capricorn charm is not in the repo yet', 'missing:Capricorn', 'HUGGIE HOOPS-ZODIAC'], 'the Review row is an unmatched-SKU card with that line, one decision per sign');
  eq(sp.pair.notes.filter(x => /Zodiac sign/.test(x)), ['Zodiac sign: Capricorn, from the personalisation box'], "and the order's reading says what the buyer chose");
  eq([sp.zodiacSign.sign, sp.zodiacSign.from, sp.zodiacSign.textOnly, sp.zodiacSign.held, sp.zodiacSign.design], ['Capricorn', 'box', true, 'missing', null], 'spec.zodiacSign: the sign word is the buyer\'s choice and the box held nothing else (never words to engrave)');
  ok(!sp.problems.some(p => p.kind === 'needsMapping'), 'no option question: the sign is in the box, METAL CHOICE and HOOP SIZE are read as always');
  for (const s of MISSING) { const q = read(huggie(s)); eq([q.designSku, q.problems.length, /^The \w+ charm is not in the repo yet/.test(q.formHold.reason), q.formHold.reason.includes(s)], [null, 1, true, true], s + ' waits'); }
  eq(read(huggie('Aquarius')).formHold.reason, 'The Aquarius charm is not in the repo yet (the repo has it only as a necklace design: ZODIAC DISCS AQUARIUS)', 'a sign the repo holds only in another size says so: never the necklace disc on huggie hoops');
  eq(read(huggie('Capricorn')).formHold.reason, 'The Capricorn charm is not in the repo yet', 'Capricorn has no design of any kind: just the plain line'); }
pass('Capricorn (and Aquarius, Gemini, Cancer, Sagittarius) on HUGGIE HOOPS-ZODIAC wait with "The <sign> charm is not in the repo yet" and no design: never Libra');

/* ── 3 · the seven signs the repo has each pick their own huggie master ── */
{ for (const [sign, sku] of Object.entries(MASTER)) {
    const sp = read(huggie(sign)); eq([sp.designSku, sp.skuSource, sp.problems.length, sp.zodiacSign.how, sp.zodiacSign.held], [sku, 'zodiac', 0, 'name', ''], sign + ' picks ' + sku);
    eq(sp.pair.notes.filter(x => /Zodiac sign/.test(x)), [`Zodiac sign: ${sign}, from the personalisation box`], sign + ': the reading says so');
    ok(!sp.formHold && sp.pieceCount === 2 && sp.pair.earring, sign + ': a pair of earrings, nothing held'); }
  eq([...new Set(Object.values(MASTER))].length, 7, 'seven different masters');
  ok(O.sameDesign(read(huggie('Libra')).designSku, 'HUGGIE HOOPS- ZODIAC', W0.masterEntry), 'Libra is the drawing the SKU always gave: a pooled Libra order does not change (the same file)');
  for (const sign of ['SCORPIO', 'scorpio', ' Scorpio! ', 'Sign: Scorpio ♏']) eq(read(huggie(sign)).designSku, MASTER.Scorpio, `"${sign}" (case, spaces, punctuation, a symbol)`); }
pass('Scorpio, Pisces, Leo, Virgo, Aries, Taurus and Libra each pick their own master (the huggie one, by the sign named in the master), and say so');

/* ── 4 · typos and the shop languages ZODIACTWO reads ── */
{ const sign = t => { const sp = read(huggie(t)); return sp.zodiacSign ? sp.zodiacSign.sign + (sp.zodiacSign.typo ? '~' : '') + ':' + (sp.designSku || 'wait') : 'none:' + (sp.formHold && sp.formHold.zodiac); };
  eq(sign('Capricon'), 'Capricorn~:wait', 'a typo (Capricon) is Capricorn: it waits like Capricorn'); eq(sign('Sagitarius'), 'Sagittarius~:wait', 'Sagitarius'); eq(sign('Scorpoi'), 'Scorpio~:' + MASTER.Scorpio, 'a swap (Scorpoi) picks Scorpio');
  eq(sign('Pices'), 'Pisces~:' + MASTER.Pisces, 'Pices'); eq(sign('Virgp'), 'Virgo~:' + MASTER.Virgo, 'Virgp'); eq(sign('Aquarus'), 'Aquarius~:wait', 'Aquarus'); eq(sign('Libr4'), 'none:which', 'a 5 letter guess two edits away is not a sign');
  eq(read(huggie('Capricon')).pair.notes.filter(x => /Zodiac/.test(x)), ['Zodiac sign: Capricorn, from the personalisation box (typed “Capricon”)'], 'a typo is said as typed');
  for (const w of ['pieces', 'Pieces please', 'cancel', 'dancer', 'aquarium', 'libre', 'arise']) eq(sign(w), 'none:which', `"${w}" is an ordinary word, never a sign`);
  eq([sign('balance'), sign('lion'), sign('Löwe'), sign('Skorpion'), sign('Jungfrau'), sign('Stier')], ['Libra:' + MASTER.Libra, 'Leo:' + MASTER.Leo, 'Leo:' + MASTER.Leo, 'Scorpio:' + MASTER.Scorpio, 'Virgo:' + MASTER.Virgo, 'Taurus:' + MASTER.Taurus], 'the shop languages (French, German): balance, lion, Löwe, Skorpion, Jungfrau, Stier');
  eq([sign('Steinbock'), sign('verseau'), sign('Sagittaire'), sign('Capricorne')], ['Capricorn:wait', 'Aquarius:wait', 'Sagittarius:wait', 'Capricorn:wait'], 'a shop-language sign the repo has no charm of waits too');
  // a long sentence: typos only for words of 7 letters or more, exact names always
  eq(sign('Hi, could you make them Capricon please, thank you so much'), 'Capricorn~:wait', 'a long note: a typo of a long name is read');
  eq(sign('Hi, could you make them Pices please, thank you so much'), 'none:which', 'a long note: a short misspelt name is not guessed'); }
pass('typos (Capricon, Sagitarius, Scorpoi, Pices) and shop-language names (balance, lion, Löwe, Steinbock) read as the sign; ordinary words never do');

/* ── 5 · where the sign may be: an option, the box, the buyer's note, the staff note; the title never ── */
{ const opt = read(huggie('', { variations: [{ name: 'METAL CHOICE', value: '14k Gold Filled' }, { name: 'HOOP SIZE', value: '8mm' }, { name: 'Zodiac Sign', value: 'Leo' }] }));
  eq([opt.designSku, opt.problems.length, opt.zodiacSign.from], [MASTER.Leo, 0, 'option'], 'a Zodiac Sign option on a shared SKU picks the sign\'s charm');
  eq(opt.options.find(o => o.name === 'Zodiac Sign').mapped, { field: 'design', value: MASTER.Leo, source: 'rule:zodiac-sign' }, 'and answers the option (nothing is asked about it)');
  eq(opt.pair.notes.filter(x => /Zodiac/.test(x)), ['Zodiac sign: Leo, from the “Zodiac Sign” option'], 'the reading names the option');
  const optMiss = read(huggie('', { variations: [{ name: 'METAL CHOICE', value: '14k Gold Filled' }, { name: 'Zodiac Sign', value: 'Capricorn' }] }));
  eq([optMiss.designSku, optMiss.formHold.reason, optMiss.problems.map(p => p.kind)], [null, 'The Capricorn charm is not in the repo yet', ['unmatchedSku', 'needsMapping']], 'a missing sign in an option: the plain line first, and the option stays a question a person can answer for that value');
  const note = read(huggie('', { buyerMessage: 'Hello! Please make them Virgo, thank you' })); eq([note.designSku, note.zodiacSign.from], [MASTER.Virgo, 'note'], 'the buyer\'s note');
  const staff = read(huggie(''), ctxOf(W0), { staffNote: 'customer wrote to us: Taurus' }); eq([staff.designSku, staff.zodiacSign.from], [MASTER.Taurus, 'staff'], 'the staff note');
  const title = read(huggie('', { title: 'Leo Zodiac Huggie Hoop Earrings' })); eq([title.designSku, title.formHold.zodiac], [null, 'which'], 'the title\'s sign is never read: with no sign named by the buyer the line asks');
  const both = read(huggie('Scorpio', { buyerMessage: 'scorpio please' })); eq([both.designSku, both.zodiacSign.from], [MASTER.Scorpio, 'box'], 'the box and the note agree: one sign');
  const clash = read(huggie('Scorpio', { buyerMessage: 'actually Leo' })); eq([clash.designSku, clash.formHold.reason], [null, 'Which zodiac sign? The buyer names Scorpio and Leo (SKU “HUGGIE HOOPS-ZODIAC” is shared by every sign of this listing)'], 'the box and the note disagree: nothing is guessed');
  const only = read(huggie('Capricorn - for Emma')); eq([only.zodiacSign.sign, only.zodiacSign.textOnly], ['Capricorn', false], 'a sign with other words in the box is still the sign, and the box is not "only" the sign'); }
pass('the sign comes from an option, the box, the buyer\'s note or the staff note, in that order of trust; the title never; a disagreement waits');

/* ── 6 · no sign named, or two: a shared SKU never falls back to its one drawing ── */
{ const none = read(huggie('')), two = read(huggie('Leo and Aries')), gift = read(huggie('gift for mum'));
  eq([none.designSku, none.formHold.reason], [null, 'Which zodiac sign? SKU “HUGGIE HOOPS-ZODIAC” is shared by every sign of this listing and the buyer named none (the personalisation box is empty)'], 'no sign: "which sign?", never the Libra drawing');
  eq([two.designSku, two.formHold.reason], [null, 'Which zodiac sign? The buyer names Leo and Aries (SKU “HUGGIE HOOPS-ZODIAC” is shared by every sign of this listing)'], 'two signs: asked, not guessed');
  eq([gift.designSku, /the personalisation box says “gift for mum”/.test(gift.formHold.reason)], [null, true], 'other words in the box: asked, and the words are shown');
  eq([none.problems.length, none.problems[0].zodiac, none.zodiacSign], [1, 'which:', null], 'one problem; no sign is claimed'); }
pass('no sign named (or two) waits with "Which zodiac sign?"');

/* ── 7 · a person's answer wins, the form guard keeps its line, and nothing else moves ── */
{ const alias = ctxOf(W0, { aliases: { '1000000042': { sku: 'ZODIAC ICONS LEO (HUGGIE)', bySku: { 'HUGGIE HOOPS-ZODIAC': 'ZODIAC ICONS LEO (HUGGIE)' }, v: 2 } } });
  const a = read(huggie('Capricorn'), alias); eq([a.designSku, a.skuSource, a.problems.length], [MASTER.Leo, 'alias', 0], 'a charm a person saved for this listing and SKU stands (it is that person\'s word)');
  const om = ctxOf(W0, { optionMaps: { '1000000042': { 'zodiac sign': { capricorn: { field: 'design', value: 'ZODIAC ICONS LEO (HUGGIE)' } } } } });
  const o = read(huggie('', { variations: [{ name: 'METAL CHOICE', value: '14k Gold Filled' }, { name: 'Zodiac Sign', value: 'Capricorn' }] }), om); eq([o.designSku, o.skuSource, o.problems.length], [MASTER.Leo, 'option', 0], 'a charm a person gave for that value of the option stands');
  // a necklace line on the huggie SKU is the form guard's (HUGGIEGUARD), never this rule's
  const neck = read(huggie('Scorpio', { title: 'Zodiac Necklace, Silver or Gold Filled', variations: [{ name: 'Metal Choice', value: 'Silver' }, { name: 'Necklace Length in inches', value: '18"' }, { name: 'Personalization', value: 'Scorpio' }] }));
  eq([neck.designSku, neck.zodiacSign, !!neck.formHold && /huggie design in the library, but this listing is a necklace/.test(neck.formHold.reason)], [null, null, true], 'a necklace line on the huggie SKU stays the form guard\'s hold');
  // a SKU that already names its sign, a numbered family member, a SKU that only holds the word, a REVAMP-style SKU the master does not hold: read as they always were
  const controls = [
    ['ZODIAC ICONS LEO (HUGGIE)', 'Capricorn', TITLE], ['ZODIAC_39104', 'Leo', 'Zodiac Lion Pendant Charm: Sterling Silver, Gold Filled'], ['ZODIAC_EARRINGS-6', 'Capricorn', 'Zodiac Stud Earrings'], ['ZODIAC', 'Leo', 'Zodiac Stud Earrings'],
    ['MAPLE_8065', 'Leo', 'Maple Leaf Charm Necklace'], ['Zodiac REVAMP', 'Leo', 'Pisces Zodiac Stud Earrings, Astrological Symbol Earrings'], ['HUGGIE HOOPS- HEALTH5-CANCER', 'Cancer', 'Cancer Ribbon Hinged Hoops']];
  for (const [sku, pers, title] of controls) { const l = huggie(pers, { sku, title }), on = read(l), off = read(l, ctxOf(W0, { masterSkus: undefined })); eq(plain(on), plain(off), `${sku}: read exactly as before (rule on = rule off)`); ok(!on.zodiacSign, sku + ': no sign claimed'); }
  // ZODIAC REVAMP "2 symbols": the real 2-symbols line, shared SKU not in the master: the pair path is untouched by this rule
  const two = read({ transactionId: '1', listingId: '1706155793', sku: 'Zodiac REVAMP', title: 'Pisces Zodiac Stud Earrings, Astrological Symbol Earrings', quantity: 1, metalKey: 'silver', metalLabel: '', personalization: ['Leo and Aries'], buyerMessage: '',
    variations: [{ name: 'Metal Choice :', value: 'Silver • 2 symbols' }, { name: 'Zodiac Sign', value: 'Libra' }, { name: 'Personalization', value: 'Leo and Aries' }] });
  const twoOff = read({ transactionId: '1', listingId: '1706155793', sku: 'Zodiac REVAMP', title: 'Pisces Zodiac Stud Earrings, Astrological Symbol Earrings', quantity: 1, metalKey: 'silver', metalLabel: '', personalization: ['Leo and Aries'], buyerMessage: '',
    variations: [{ name: 'Metal Choice :', value: 'Silver • 2 symbols' }, { name: 'Zodiac Sign', value: 'Libra' }, { name: 'Personalization', value: 'Leo and Aries' }] }, ctxOf(W0, { masterSkus: undefined }));
  eq(JSON.parse(JSON.stringify(two)), JSON.parse(JSON.stringify(twoOff)), 'ZODIAC REVAMP "2 symbols" with its three signs: the whole reading is identical with and without this rule'); }
pass('a person\'s alias or option answer wins; a necklace on the huggie SKU is the form guard\'s; named, numbered, plain-word, REVAMP and other lines read exactly as before');

/* ── 8 · the day the master exists, the hold clears by itself ── */
{ const held = read(huggie('Capricorn')), W1 = world([['ZODIAC ICONS CAPRICORN (HUGGIE)', F('ZODIAC_ICONS_CAPRICORN_HUGGIE_'), 20, 30]]);
  const now = read(huggie('Capricorn'), ctxOf(W1));
  eq([now.designSku, now.skuSource, now.problems.length, !!now.formHold, now.zodiacSign.held], ['ZODIAC ICONS CAPRICORN (HUGGIE)', 'zodiac', 0, false, ''], 'a Capricorn huggie master in the library: the very same line now takes it, no hold');
  ok(O.questionGone({ state: 'unmatched', spec: now, problems: [] }, held), 'the held row goes back to the intake by itself (questionGone: no stale hold)');
  // other signs of the same master world are untouched; a necklace-size Capricorn alone never counts for huggie hoops
  eq(read(huggie('Leo'), ctxOf(W1)).designSku, MASTER.Leo, 'the other signs are as they were');
  const necklaceOnly = world([['ZODIAC DISCS CAPRICORN', F('ZODIAC_NECKLACE_DISC-9'), 29.65, 35.03]]);
  eq(read(huggie('Capricorn'), ctxOf(necklaceOnly)).formHold.reason, 'The Capricorn charm is not in the repo yet (the repo has it only as a necklace design: ZODIAC DISCS CAPRICORN)', 'a necklace Capricorn does not clear a huggie hold');
  // two Capricorn huggie drawings: not guessed
  const dup = world([['ZODIAC ICONS CAPRICORN (HUGGIE)', F('CAP1'), 20, 30], ['ZODIAC CAPRICORN (HUGGIE)', F('CAP2'), 21, 30]]), d = read(huggie('Capricorn'), ctxOf(dup));
  eq([d.designSku, /more than one design of this kind in the repo \(ZODIAC CAPRICORN \(HUGGIE\), ZODIAC ICONS CAPRICORN \(HUGGIE\)\)/.test(d.formHold.reason)], [null, true], 'two different Capricorn huggie drawings: a person picks');
  // the same drawing under two names is one design
  const same = world([['ZODIAC ICONS CAPRICORN (HUGGIE)', F('CAP1'), 20, 30], ['HUGGIE HOOPS- ZODIAC CAPRICORN', F('CAP1'), 20, 30]]); eq(read(huggie('Capricorn'), ctxOf(same)).designSku, 'ZODIAC ICONS CAPRICORN (HUGGIE)', 'one drawing under two names is one design (the (HUGGIE) one, shortest)');
  // a line is held only while it waits: a library that changes size re-reads it (the bridge's signature), the sign's master being the only thing that ends the hold
  const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  ok(/row\.spec && \(row\.spec\.formHold \|\| /.test(bridge) && /\? B\.master\.entries\.size : 0/.test(bridge), 'the bridge reads a held line again whenever the library grows'); }
pass('a Capricorn master in the library ends the hold by itself (questionGone); a necklace Capricorn does not; two drawings are not guessed');

/* ── 9 · the numbered family: ZODIAC DISC (the Scorpio necklace disc, ZODIAC_NECKLACE_DISC-7) takes the sign's member only when the master's own names agree with the numbering ── */
{ const disc = (pers, c) => read(huggie(pers, { sku: 'ZODIAC DISC', title: 'Zodiac Disc Necklace, Silver or Gold Filled', variations: [{ name: 'Metal Choice', value: 'Silver' }, { name: 'Necklace Length in inches', value: '18"' }, { name: 'Personalization', value: pers }] }), c);
  eq([disc('Capricorn').designSku, disc('Capricorn').zodiacSign.how, disc('Gemini').designSku], ['ZODIAC_NECKLACE_DISC-9', 'numbered', 'ZODIAC_NECKLACE_DISC-2'], 'a necklace on the ZODIAC DISC SKU: Capricorn is the numbered disc at its place (9), Gemini 2');
  eq([disc('Taurus').designSku, disc('Aquarius').designSku, disc('Scorpio').designSku], ['ZODIAC DISCS TAURUS', 'ZODIAC DISCS AQUARIUS', 'ZODIAC DISCS SCORPIO'], 'a sign the master names picks its named disc');
  // names that disagree with the numbering: the family is not trusted, the sign waits
  const bad = world(); bad.lib.get('ZODIAC DISCS TAURUS').aiPath = F('ZODIAC_NECKLACE_DISC-2'); const byFile = new Map(); for (const [k, e] of bad.lib) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); } bad.masterShared = s => { const e = bad.lib.get(String(s).toUpperCase()); return e ? byFile.get(e.aiPath) || [] : []; };
  eq(disc('Capricorn', ctxOf(bad)).formHold.reason, 'The Capricorn charm is not in the repo yet', 'the master names Taurus on place 2: the numbering is not trusted, Capricorn waits'); }
pass('a numbered family is used only when the master\'s own sign names agree with its numbering');

/* ── 10 · corpora: the 569 real lines of the repo read exactly the same with this rule on and off ── */
{ const TD = __dirname, WG = JSON.parse(fs.readFileSync(path.join(TD, 'fixtures/huggie-guard-world.json'), 'utf8')), OL = JSON.parse(fs.readFileSync(path.join(TD, 'options-link-data.json'), 'utf8')), J = f => JSON.parse(fs.readFileSync(path.join(TD, f), 'utf8'));
  const lib = new Map(WG.master.map(e => [e.sku.toUpperCase(), Object.assign({ sizes: null, blocked: null }, e)]));
  for (const [k, e] of W0.lib) lib.set(k, e);                         // the zodiac family is in the library the corpora are read against
  const byFile = new Map(); for (const [k, e] of lib) if (e.aiPath) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
  const mk = (tables, on) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: tables || {}, masterEntry: s => lib.get(String(s).toUpperCase()) || null, masterLoose: s => WG.loose[O.looseKey(s)] || '',
    masterShared: s => { const e = lib.get(String(s).toUpperCase()); return e && e.aiPath ? byFile.get(e.aiPath) || [] : []; } }, on ? { masterSkus: () => [...lib.keys()] } : {});
  const lines = [];
  for (const l of J('fixtures/addon-snapshot-lines.json').lines) lines.push([{ transactionId: l.tid, listingId: l.listing, sku: l.sku, title: l.title, quantity: l.q, metalKey: l.mk, metalLabel: '', personalization: [], buyerMessage: '', variations: l.vars.map(v => ({ name: v[0], value: v[1] })) }, { receiptId: l.rid, updateTs: 1 }, false]);
  for (const l of OL.lines) lines.push([{ transactionId: l.tx, listingId: l.listing, productId: l.product, sku: l.sku, title: l.title, quantity: l.quantity, metalKey: 'gold', metalLabel: '', personalization: [], buyerMessage: '', variations: l.variations }, { receiptId: l.receipt, updateTs: 1 }, true]);
  for (const f of ['fixtures/adv-count-lines.json', 'fixtures/font-option-lines.json']) for (const l of J(f).lines) lines.push([{ transactionId: l.tid, listingId: l.listingId, sku: l.sku, title: l.title, quantity: l.q, metalKey: 'gold', metalLabel: '', personalization: [], buyerMessage: '', variations: l.vars.map(v => ({ name: v[0], value: v[1] })) }, { receiptId: l.rid, updateTs: 1 }, false]);
  for (const g of WG.gg) lines.push([g.line, g.order, false]);
  ok(lines.length >= 560, 'the corpora: ' + lines.length + ' lines');
  const changed = [];
  for (const [line, order, tables] of lines) { const a = O.interpretLine(order, JSON.parse(JSON.stringify(line)), mk(tables ? OL.tables : null, true)), b = O.interpretLine(order, JSON.parse(JSON.stringify(line)), mk(tables ? OL.tables : null, false)); if (JSON.stringify(plain(a)) !== JSON.stringify(plain(b)) || a.zodiacSign) changed.push(line.sku); }
  eq(changed, [], 'no real line changes (none carries a shared zodiac SKU): ' + changed.join(', '));
  pass(lines.length + ' real order lines (Sep 17 snapshot, option-link tables, count / font lines, Greek Goddess earrings): all read exactly the same with the rule on and off'); }

/* ── 11 · wiring ── */
{ const orders = fs.readFileSync(path.join(root, 'charm-nest-orders.js'), 'utf8'), bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  ok(/masterSkus: \(\) => \[\.\.\.B\.master\.entries\.keys\(\)\]/.test(bridge), 'the page hands the line reader its master list (every SKU the library holds)');
  ok(/if \(sp\.formHold\) \{ const hp = \(sp\.problems \|\| \[\]\)\.find\(p => p\.formHold\); if \(hp\) row\.problems\.push\(hp\); row\.state = "unmatched"; row\.reason = sp\.formHold\.reason; return null; \}/.test(bridge), 'a held line is never pooled: the pool attempt re-raises the plain line (the form guard\'s own route)');
  ok(/\$\{p\.zodiac \? SEP \+ p\.zodiac : ""\}/.test(bridge), 'each missing sign is its own Review decision');
  for (const name of ['charm-nest-orders.js', 'charm-nest-bridge.js']) { const m = new RegExp('src="' + name.replace('.', '\\.') + '\\?v=([^"]*)"').exec(html); ok(m && /-zs1(?:-|$)/.test(m[1]) && /^2026100/.test(m[1]), name + ' token carries -zs1'); }
  pass('wiring: master list from the page, the held line route, one decision per sign, cache tokens -zs1'); }

console.log('zodiac-sign: all passed (' + n + ' checks)');
