// MONOGRAM order recognition (MONOGRAMUI, Paul, 15 Oct 2026): "These 2 listings are for custom monograms ... The buyer will always provide 3 individual letters" (orders 4200683017 "APV", listing 997905351,
// and 4202783520 "OSR", listing 606439317). A monogram line is the buyer's own design (special.monogram, own design, no SKU / metal / option question, no engraving); the two listings are named in the code
// (MONOGRAM_LISTINGS), and any other listing that says MONOGRAM (the word) in its title with exactly three letters in the personalisation box or in an option for initials is read the same way.
// Nothing else changes: a bracelet that says monogram with a name, two letters or four stays what it was; a catalogue charm whose title says monogram and whose SKU is a master design is still that design;
// a person's word for the listing ("regular") wins; the 411 lines of the Sep 17 snapshot read exactly as before.
// Offline, node only:  node tests/charm-nest/custom-monogram.cjs
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const FIX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/addon-snapshot-lines.json'), 'utf8'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; };

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 12, 17) / 1000);
const order = rid => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [] });
const line = (o = {}) => Object.assign({ transactionId: '5001', listingId: '555555', sku: 'MONO_X1', title: 'Monogram Necklace Dainty Initials Pendant', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal choice', value: 'Gold' }, { name: 'Chain length', value: '16 inches' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: ['APV'], buyerMessage: '' }, o);
const KNOWN = new Set();
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: s => (KNOWN.has(String(s).toUpperCase()) ? { sku: s } : null), masterLoose: () => '' }, extra || {});
const read = (l, c) => O.interpretLine(order('4200000001'), l, c || ctx());
const isMono = sp => !!(sp.special && sp.special.monogram);

// ── the two listings of Paul's orders ──
{
  const a = read(line({ transactionId: '1', listingId: '997905351', sku: 'MONOGRAM_5351', title: 'Monogram Necklace Personalized Jewelry Initials Pendant', personalization: ['APV'] }));
  ok(isMono(a) && a.special.label === 'Custom monogram' && a.special.words === 'APV' && a.special.own === true && /custom monogram listing/.test(a.special.why), 'listing 997905351 (APV): a custom monogram: ' + JSON.stringify(a.special));
  ok(a.problems.length === 0 && a.ownDesign === true && a.noDesign === false && a.engraveCandidate === false, 'it asks nothing (no SKU, metal or option question), is its own design, has no engraving');
  const b = read(line({ transactionId: '2', listingId: '606439317', sku: 'MONOGRAM_9317', title: 'Monogram Necklace Dainty Monogram Necklace', personalization: ['O S R'] }));
  ok(isMono(b) && b.special.words === 'O S R', 'listing 606439317 (OSR): a custom monogram with its words: ' + JSON.stringify(b.special));
  // the listing table is explicit: whatever the buyer wrote (the card shows it, a person types the letters), the master's design of the same name is never used
  KNOWN.add('MONOGRAM_5351');
  const c = read(line({ transactionId: '3', listingId: '997905351', sku: 'MONOGRAM_5351', personalization: ['Jane Doe'] }));
  ok(isMono(c) && c.special.words === 'Jane Doe', 'a listing of the table is a monogram even when the words are not three letters, and even when the master holds a design of its SKU');
  KNOWN.clear();
  ok(O.specialOf.addOn.monograms['997905351'] && O.specialOf.addOn.monograms['606439317'] && Object.keys(O.specialOf.addOn.monograms).length === 2, 'the listing table holds the two listings');
  // a person's word for the listing stands over the table
  const reg = read(line({ transactionId: '4', listingId: '997905351', sku: 'MONOGRAM_5351', title: 'Monogram Necklace Personalized Jewelry Initials Pendant' }), ctx({ aliases: { 997905351: { listingKind: { kind: 'regular', by: 'Paul' } } } }));
  ok(!isMono(reg), 'a person took the listing out of the special kinds: it is read as a regular listing again');
}
// ── any listing that says MONOGRAM with exactly three letters ──
{
  const yes = [
    ['the box', { personalization: ['KLM'] }, 'KLM'], ['spaces', { personalization: ['K L M'] }, 'KLM'], ['dots', { personalization: ['K.L.M.'] }, 'KLM'], ['lower case', { personalization: ['klm'] }, 'KLM'],
    ['a label', { personalization: ['Initials: K L M'] }, 'KLM'], ['accents', { personalization: ['Á-É-Í'] }, 'AEI'], ['emoji between', { personalization: ['K 😀 L 😀 M'] }, 'KLM'],
    ['an option for initials', { personalization: [], variations: [{ name: 'Initials', value: 'O.S.R' }] }, 'OSR'], ['the Personalization option', { personalization: [], variations: [{ name: 'Personalization', value: 'A P V' }] }, 'APV'],
    ['the box and the same in an option', { personalization: ['APV'], variations: [{ name: 'Personalization', value: 'A P V' }] }, 'APV'], ['an HTML-escaped box', { personalization: ['A&amp;P&amp;V'] }, 'APV']
  ];
  for (const [what, extra, want] of yes) {
    const sp = read(line(Object.assign({ listingId: '777001', sku: 'NEWMONO_1' }, extra)));
    ok(isMono(sp) && sp.special.signals[0] === 'listing' && sp.special.own === true && sp.special.label === 'Custom monogram', `${what}: read as a custom monogram`);
    ok(sp.special.words && O.specialOf.addOn.monogramLetters(line(Object.assign({ listingId: '777001' }, extra))) === want, `${what}: the letters are ${want}`);
    ok(/says monogram and the buyer gave three letters/.test(sp.special.why), `${what}: it says why: ` + sp.special.why);
    ok(sp.problems.length === 0 && sp.ownDesign === true && sp.noDesign === false, `${what}: no questions, its own design`);
  }
  const monoWords = ['Monogram Necklace', 'Dainty MONOGRAM Charm', 'Personalized monograms for her', 'Gold Monogram, Custom Initials', 'monogram-pendant'];
  for (const title of monoWords) ok(isMono(read(line({ listingId: '777002', sku: 'NEWMONO_2', title }))), `the title “${title}” says monogram`);
}
// ── false positives: a different letter count, a name, no monogram in the title, a master design ──
{
  const no = [
    ['a name (one word, capitalised)', { personalization: ['Amy'] }],
    ['two letters', { personalization: ['A B'] }], ['four letters', { personalization: ['ABCD'] }], ['a first and last name', { personalization: ['Amy Lee'] }], ['seven letters', { personalization: ['Jane Doe'] }],
    ['digits', { personalization: ['123'] }], ['nothing', { personalization: [] }], ['an option that is not about letters ("Color: Red")', { personalization: [], variations: [{ name: 'Color', value: 'Red' }] }],
    ['the box and an option that disagree', { personalization: ['APV'], variations: [{ name: 'Personalization', value: 'XYZ' }] }], ['only a buyer message', { personalization: [], buyerMessage: 'APV please' }]
  ];
  for (const [what, extra] of no) {
    const sp = read(line(Object.assign({ listingId: '777003', sku: 'NEWMONO_3', title: 'Monogram Bracelet Personalized Initial Bracelet' }, extra)));
    ok(!isMono(sp), `a monogram bracelet with ${what} is not a custom monogram`);
  }
  ok(isMono(read(line({ listingId: '777003', sku: 'NEWMONO_3', title: 'Monogram Bracelet', personalization: ['AMY'] }))), 'three capital letters are three letters, whatever they might spell');
  const titles = ['Custom Necklace Personalized Initial Charm', 'Monogrammed Gift Box', 'Monogramming service', 'Heart Charm Necklace', 'Initial Necklace, Letter Charm'];
  for (const title of titles) ok(!isMono(read(line({ listingId: '777004', sku: 'NEWMONO_4', title, personalization: ['APV'] }))), `“${title}” does not say monogram (the word), so three letters alone make no monogram`);
  // a catalogue charm (its SKU is a master design) with MONOGRAM in its title and three letters to engrave is still that design: the letters are its back words
  KNOWN.add('HEART_1234');
  const cat = read(line({ listingId: '777005', sku: 'HEART_1234', title: 'Heart Monogram Charm Necklace', personalization: ['A P V'] }));
  ok(!isMono(cat) && cat.ownDesign !== true, 'a line whose SKU is a master design is that design, whatever its title says');
  KNOWN.clear();
  // a person's own word for the listing ("regular") wins over the title rule
  const reg = read(line({ listingId: '777006', sku: 'NEWMONO_6', personalization: ['APV'] }), ctx({ aliases: { 777006: { listingKind: { kind: 'regular', by: 'Paul' } } } }));
  ok(!isMono(reg), 'a person\'s "regular" for the listing wins over the title rule');
}
// ── the Sep 17 snapshot: not one of its 411 lines is read as a monogram ──
{
  const KN = FIX.known, known = s => !!KN[String(s).toUpperCase()];
  const cx = ctx({ masterEntry: s => (known(s) ? { sku: s } : null), masterLoose: s => FIX.loose[String(s)] || '' });
  let monos = 0, withWord = 0;
  for (const f of FIX.lines) {
    const l = { transactionId: f.tid, listingId: f.listing, sku: f.sku, title: f.title, quantity: f.q, expectedShipDate: SHIP, variations: f.vars.map(([name, value]) => ({ name, value })), metalKey: f.mk, metalLabel: f.mk, personalization: [], buyerMessage: '' };
    if (/monogram/i.test(f.title)) withWord++;
    if (isMono(O.interpretLine(Object.assign(order(f.rid)), l, cx))) monos++;
  }
  ok(FIX.lines.length === 411 && monos === 0, `none of the ${FIX.lines.length} snapshot lines is a monogram (${withWord} say the word in their title: with no letters in a box they stay as they were)`);
}
// ── the same reading as the engine's (charm-nest-monogram.js lettersOf, which the Monograms tab uses): this module keeps its own short copy so the server's bundles do not carry the geometry engine ──
{
  const M = require(path.join(root, 'charm-nest-monogram.js')), mine = O.specialOf.addOn.monoLettersIn;
  const STRINGS = ['APV', 'A P V', 'apv', 'a.p.v', 'A.P.V.', 'A-P-V', 'A, P, V', 'A/P/V', '  O  S  R  ', 'osr', 'Á-P-V', 'É.M.Ñ', 'Åsa Öberg', 'ÀÈÌ', 'Ø.Ł.Đ', 'Initials: ASK', 'Initials: A S K', 'initials A.S.K please', 'Monogram for APV please', 'Letters: J, K, L', 'Initials: AND',
    'M & J & S', 'A and B and C', 'A 😀 P 😀 V', '😀APV😀', '★A★P★V★', 'A\u200bP\u200bV', 'ＡＰＶ', 'A_P_V', 'A1P2V3', '\n\tK\nL\tM\n', 'AP', 'A', 'APVX', 'A P V X', '123', '', '   ', '!!! ??? ...', 'Hello', 'Mary Ann Lee', 'Jane Doe', 'Initials: Jane Doe',
    'Initials: A P V (large)', 'Initials', 'Initials: AP', 'АПВ', null, undefined, 42, 'my initials are J K L', 'for the J N S', 'A N D', 'Please make it A.B.C thanks', 'please and thanks', 'with love J M T', 'Our initials: K R B', 'LMNOP', 'N. P. Q.', 'Ǻ.Ŕ.Ç', 'Ł-Ø-Đ'];
  for (const t of STRINGS) { const a = M.lettersOf(t), b = mine(t); ok((a.ok ? a.letters.join('') : null) === (b ? b.join('') : null), `the two readings agree on ${JSON.stringify(t)}: ${a.ok ? a.letters.join('') : 'none'} / ${b ? b.join('') : 'none'}`); }
}
// ── the pure words ──
{
  const W = O.specialOf.addOn.monogramWords;
  assert.deepEqual(W(line({ personalization: ['  A  P  V '], variations: [{ name: 'Metal', value: 'Gold' }, { name: 'Initials', value: 'O.S.R' }] })), ['A P V', 'O.S.R'], 'the box and the options that ask for letters, whitespace squeezed; a metal is not one'); n++;
  assert.deepEqual(W(null), [], 'nothing'); n++;
}
console.log(`custom monogram OK: ${n} checks`);
