// EARWORDS (10 Oct 2026; Paul: "ALL earrings must be mirror versions of each other", a pair is ALWAYS two pieces, quantity q = q Left + q Right).
// The code reads earring-ness from the LISTING'S WORDS only (title, chosen options, option names, the shop's own SKU), never from a note. MIRRORCATALOG found that no earring
// line of the Sep 17 snapshot is missed, but several real wordings would fail SILENTLY (one piece, nothing mirrored). This test closes them, and pins what must NOT become a pair.
//   node tests/charm-nest/pairs-words.cjs
// What it proves (charm-nest-orders.js lineSignals / pairInfo / form rules, the twin CharmNestPair.isEarringPair, the station page's StationLiveOrder), offline, nothing live:
//   A · each latent wording is a pair at once: a title with only "huggie", an option NAME that says earring, an earring-style SKU under a silent title, "ear rings", "Both ears";
//       q = 1 is a Left and a Right (the Right mirrored), q = 2 is L R L R; the raw-line path (stations) and the pair module agree
//   B · "Poodle Earring Single" / "Earring (Right)" are ONE earring (the Right names its ear), not an extra piece; a plural title and "Single Pearl Stud Earrings" stay pairs
//   C · "Charm only (no hoops)" is the loose charm, whatever the option is called and whatever the title says
//   D · a wording that may or may not be earrings is NOT guessed: "huggie" in a necklace title, an earring SKU under a necklace title, "Pair of ... charms": one plain question,
//       one answer per listing title (earrings / huggie / not earrings), nothing pooled while it waits
//   E · what must never become a pair: necklace, pendant, bracelet, key ring, charm only, counted discs, a plain quantity-3 line, a note or a gift text that says earrings,
//       "Add matching earrings", a hoop charm necklace, a numbered design called Huggie, a form the options chose
//   F · the Sep 17 snapshot (411 lines): every distinct line makes the pieces it made before this work (tests/charm-nest/pairs-words-snapshot.json)
const assert = require('assert');
const fs = require('fs'), path = require('path'), vm = require('vm');
const root = path.join(__dirname, '../..');
const O = require('../../charm-nest-orders.js');
const CP = require('../../charm-nest-pair.js');
const noNested = require('./_noNestedArrays.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };

const order = { receiptId: '4170000001', updateTs: 1789100000 };
const V = (name, value) => ({ name, value });
const mk = over => Object.assign({ transactionId: '5200000001', listingId: '1718', sku: 'A', title: 'Dog Charm', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: [V('Metal Choice', 'Gold')] }, over);
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: () => null }, extra);
const asRaw = l => Object.assign({}, l, { receipt_id: order.receiptId, transaction_id: l.transactionId, variations: (l.variations || []).map(v => ({ formatted_name: v.name, formatted_value: v.value })) });   // (an Etsy transaction as a station page holds it)
const answer = (title, field, value) => ({ '1718': { [O.norm("Earrings in this listing's words")]: { [O.norm(title)]: { field, value } } } });
const SLO = (() => {
  const win = { CharmNestOrders: O };
  const c = vm.createContext({ window: win, document: { addEventListener() {} }, Date, JSON, Math, String, Number, Array, Object, Set, RegExp });
  vm.runInContext(fs.readFileSync(path.join(root, 'station-live-order.js'), 'utf8'), c);
  return win.StationLiveOrder;
})();
const sides = x => x.map(p => p.side || '-').join('');
const asked = spec => spec.problems.filter(p => p.kind === 'needsMapping' && p.earWords);
function read(over, c) { const line = mk(over), spec = O.interpretLine(order, line, c || ctx()); noNested(spec, 'spec'); return { line, spec, pair: spec.pair, raw: asRaw(line) }; }

// ── A · the wordings that are a pair at once ──────────────────────────────────────────────────────────────────────
function isPair(name, over, c) {
  for (const q of [1, 2]) {
    const r = read(Object.assign({ quantity: q }, over), c), tag = `${name} q${q}`;
    ok(r.pair.earring === true && r.pair.single === false, tag + ': an earring pair line');
    eq(r.spec.pieceCount, 2 * q, tag + ': two pieces for each one bought');
    eq(r.pair.sides.map(s => s || '-').join(''), 'LR'.repeat(q), tag + ': L R' + (q > 1 ? ' L R' : ''));
    eq(asked(r.spec).length, 0, tag + ': nothing to ask');
    eq(O.pieceCountOf(r.raw), 2 * q, tag + ': the raw line (a station) counts the same');
    eq(sides(O.piecesOf(r.raw)), 'LR'.repeat(q), tag + ': the raw line has the same sides');
    ok(CP.isEarringPair({ form: r.spec.form || '', spec: r.spec, quantity: q }, null) === true, tag + ': CharmNestPair.isEarringPair agrees');
    const ps = CP.piecesFor({ form: r.spec.form || '', spec: r.spec, quantity: q, receiptId: order.receiptId, transactionId: r.line.transactionId }, null);
    eq(sides(ps), 'LR'.repeat(q), tag + ': the pair module makes a Left and a Right per unit');
    eq(ps.map(p => p.mirror), Array.from({ length: q }, () => [false, true]).flat(), tag + ': the Right is the mirror of the Left');
    eq(SLO.kind(r.raw).pieces, 2 * q, tag + ': the station page counts the pieces');
    ok(SLO.kind(r.raw).sided === true, tag + ': the station page tells a Left and a Right');
  }
}
// gap a · "huggie" alone in a title
isPair('title: Poodle Huggie Charm', { title: 'Poodle Huggie Charm' });
isPair('title: Huggie Charms', { title: 'Huggie Charms for Dog Lovers' });
isPair('title: Huggy', { title: 'Dog Huggy Charm' });
isPair('title: huggie + Pair option', { title: 'Huggie Charm + Shipping', sku: 'Huggie_3722', variations: [V('Price', 'Gold Filled - Pair')] });
// gap b · an option NAME that says it
isPair('name: Earring Style: Huggie (a form option)', { title: 'Poodle Charm', variations: [V('Earring Style', 'Huggie')] });
isPair('name: Hoop Size', { title: 'Poodle Charm', variations: [V('Hoop Size', '8.5mm')] });
isPair('name: Earring Backs', { title: 'Poodle Charm', variations: [V('Earring Backs', 'Butterfly')] });
isPair('name: Stud Size', { title: 'Poodle Charm', variations: [V('Stud Size', '6mm')] });
isPair('names: Left Ear Charm / Right Ear Charm', { title: 'Mixed Charms', variations: [V('Left Ear Charm', 'Dog'), V('Right Ear Charm', 'Cat')] });
eq(read({ title: 'Poodle Charm', variations: [V('Earring Style', 'Huggie')] }).spec.form, 'huggie', 'Earring Style: Huggie is a form option: no question, the form is huggie');
// gap c · the shop's own SKU under a silent title
for (const sku of ['Huggie Hoops- Poodle', 'Huggie_Hoops_Poodle', 'POODLE (HUGGIE)', 'CUPCAKE 1 - HUGGIE', 'HOOPS- MIDDLE FINGER', 'POODLE STUD', 'ZODIAC_EARRING_DISC-3', 'LETTER_EARRING-7', 'CUSTOM-H-020-660181', 'CUSTOM-E-004-100200', 'CUSTOM-S-004-100200'])
  isPair('sku: ' + sku, { sku, title: 'Tiny Poodle Charm', variations: [V('Metal Choice', 'Gold')] });
// other real wordings
isPair('title: Ear rings', { title: 'Owl Ear Rings' });
isPair('title: Earings (the common misspelling)', { title: 'Owl Earings Gold' });
isPair('title: Ear-rings', { title: 'Owl Ear-rings' });
isPair('option: Both ears', { title: 'Poodle Earring', variations: [V('Ears', 'Both ears')] });
isPair('title: Huggie Charm Sets', { title: 'Poodle Huggie Charm Sets' });
// the old wordings are still pairs
isPair('title: Poodle Earrings', { title: 'Poodle Earrings Handcrafted in Gold Filled' });
isPair('title: singular Earring used for a pair', { title: 'Mushroom Earring, Dainty Earring, Mix and Match Gold Earring' });
isPair('title: Single Pearl Stud Earrings (one pearl on each ear)', { title: 'Single Pearl Stud Earrings' });
isPair('title: Huggie Hoops', { title: 'Snail Charm Huggie Hoops', variations: [V('HOOP SIZE', '8.5mm')] });
isPair('option: Huggie CHARM SET', { title: 'Dog Charm Pendant Add On Charm', variations: [V('Charm Type', 'Huggie CHARM SET')] });
// a title that is silent and a SKU that is silent are not earrings (the control of A)
for (const over of [{ title: 'Poodle Charm' }, { title: 'Poodle Charm', sku: 'Poodle' }, { title: 'Poodle Charm', sku: 'STUDIO_68750' }, { title: 'Poodle Charm', sku: 'CUSTOM-N-001-665441' }]) {
  const r = read(over); ok(r.pair.earring === false && r.spec.pieceCount === 1, 'a silent title and SKU stay one piece: ' + JSON.stringify(over));
}

// ── B · one earring, not a pair ──────────────────────────────────────────────────────────────────────────────────
function isSingle(name, over, side) {
  for (const q of [1, 2]) {
    const r = read(Object.assign({ quantity: q }, over)), tag = `${name} q${q}`;
    ok(r.pair.earring === false && r.pair.single === true, tag + ': a single earring');
    eq(r.spec.pieceCount, q, tag + ': one piece for each one bought');
    eq(r.pair.sides.map(s => s || '-').join(''), (side || '-').repeat(q), tag + ': ' + (side ? 'the ear it names' : 'no ear named'));
    ok(CP.isEarringPair({ form: r.spec.form || '', spec: r.spec, quantity: q }, null) === false, tag + ': the pair module agrees');
    eq(O.pieceCountOf(r.raw), q, tag + ': the raw line counts one for each');
  }
}
isSingle('Poodle Earring Single', { title: 'Poodle Earring Single' });
isSingle('Poodle Stud Earring (Single)', { title: 'Poodle Stud Earring (Single)' });
isSingle('Poodle Earring (Right)', { title: 'Poodle Earring (Right)' }, 'R');
isSingle('Poodle Earring - Left', { title: 'Poodle Earring - Left' }, 'L');
isSingle('Poodle Huggie Earring Left Ear', { title: 'Poodle Huggie Earring Left Ear' }, 'L');
isSingle('Poodle Huggie Charm, Single (a bare huggie)', { title: 'Poodle Huggie Charm, Single' });
isSingle('a single option under a huggie title', { title: 'Huggie Charm + Shipping', sku: 'Huggie_3722', variations: [V('Price', 'Gold Filled - Single')] });
isSingle('an earring SKU, a Single title', { sku: 'Huggie Hoops- Poodle', title: 'Tiny Poodle Charm, Single' });
isSingle('an earring name, a Single option', { title: 'Poodle Charm', variations: [V('Earring Style', 'Stud - Single')] });
for (const t of ['Poodle Earrings Left Right Set', 'Poodle Earrings, Right', 'Poodle Stud Earrings']) { const r = read({ title: t }); ok(r.pair.earring === true, 'a plural earrings title is a pair: ' + t); }

// ── C · "Charm only (no hoops)" is the loose charm ───────────────────────────────────────────────────────────────────
for (const [name, v] of [['Charm Type', 'Charm only (no hoops)'], ['Choose', 'Charm only (no hoops)'], ['Pick one', 'Charm only - no earrings'], ['Style', 'Pendant without hoops'], ['Earring Type', 'Charm only'], ['Option', 'Charm only no huggie hoops']]) {
  for (const title of ['Poodle Earrings', 'Poodle Huggie Hoops Charm', 'Poodle Charm']) {
    const r = read({ title, variations: [V('Metal Choice', 'Gold'), V(name, v)] }), tag = `${title} / ${name}: ${v}`;
    ok(r.pair.earring === false && r.spec.pieceCount === 1, tag + ': the loose charm, one piece');
    eq(r.spec.form, 'charm', tag + ': the form is charm (no question to answer)');
    eq(r.spec.problems.filter(p => p.kind === 'needsMapping').length, 0, tag + ': nothing to map');
    eq(O.pieceCountOf(r.raw), 1, tag + ': the raw line (a station) counts one');
    ok(SLO.kind(r.raw).sided === false, tag + ': the station page tells no sides');
  }
}
ok(read({ sku: 'Huggie Hoops- Poodle', title: 'Poodle Charm', variations: [V('Charm Type', 'CHARM + Engraving')] }).pair.earring === false, 'an earring SKU and the buyer chose the loose charm: not a pair');
ok(read({ title: 'Poodle Earrings', variations: [V('Charm Type', 'Necklace CHARM')] }).pair.earring === false, 'a form the options chose (Necklace CHARM) beats the title (unchanged)');

// ── D · the wordings that are NOT guessed ───────────────────────────────────────────────────────────────────────────
const ASKED = [
  ['huggie in a title that also says necklace', { title: 'Poodle Huggie Charm Necklace' }, /huggie/i],
  ['huggie in a title that also says pendant', { title: 'Wolf Huggie Charm Pendant, Gold' }, /huggie/i],
  ['an earring SKU under a necklace title', { sku: 'Huggie Hoops- Poodle', title: 'Poodle Charm Necklace' }, /SKU/],
  ['an earring SKU under a bracelet title', { sku: 'LETTER_EARRING-3', title: 'Letter Charm Bracelet' }, /SKU/],
  ['Pair of ... charms', { title: 'Pair of Dog Charms' }, /pair/i],
  ['Set of 2 charms', { title: 'Set of 2 Dog Charms' }, /set of 2/i],
  ['a mismatched pair of charms', { title: 'Mismatched Pair Dog and Cat Charms' }, /pair/i]
];
for (const [name, over, why] of ASKED) {
  const r = read(Object.assign({ quantity: 2 }, over)), a = asked(r.spec);
  eq(a.length, 1, name + ': one question');
  ok(why.test(a[0].earWords.why) && a[0].earWords.why.length < 200, name + ': one plain line: ' + a[0].earWords.why);
  eq(a[0].optionName, "Earrings in this listing's words", name + ': kept under the listing-words option');
  eq(a[0].optionValue, O.norm(over.title).slice(0, 190), name + ': for this listing title');
  ok(r.pair.earring === false && r.spec.pieceCount === 2, name + ': nothing is guessed: made as before (a piece for each one bought) while it waits');
  eq(r.pair.asks.map(x => x.rule), ['ear:words'], name + ': the pair facts carry the question');
  // a person answers once, for the listing title
  const title = over.title;
  for (const [field, value, earring, form] of [['form', 'earrings', true, 'earrings'], ['form', 'huggie', true, 'huggie'], ['ignore', null, false, null]]) {
    const b = read(Object.assign({ quantity: 2 }, over), ctx({ optionMaps: answer(title, field, value) }));
    eq(asked(b.spec).length, 0, `${name} answered ${field} ${value}: no question`);
    eq(b.pair.earring, earring, `${name} answered ${field} ${value}: ${earring ? 'a pair of earrings' : 'not earrings'}`);
    eq(b.spec.pieceCount, earring ? 4 : 2, `${name} answered ${field} ${value}: the pieces`);
    eq(b.pair.sides.map(s => s || '-').join(''), earring ? 'LRLR' : '--', `${name} answered ${field} ${value}: the sides`);
    eq(b.spec.form || null, form, `${name} answered ${field} ${value}: the form the answer gives`);
  }
}
// what decides it already is never asked
for (const [name, over] of [
  ['a form the options chose (Necklace CHARM)', { title: 'Poodle Huggie Charm Necklace', variations: [V('Charm Type', 'Necklace CHARM')] }],
  ['the loose charm', { title: 'Poodle Huggie Charm Necklace', variations: [V('Charm Type', 'CHARM + Engraving')] }],
  ['a Huggie CHARM SET choice', { title: 'Poodle Huggie Charm Necklace', variations: [V('Charm Type', 'Huggie CHARM SET')] }],
  ['an earring word in the title', { title: 'Poodle Huggie Charm Necklace and Earrings' }],
  ['a Single option', { title: 'Pair of Dog Charms', variations: [V('Price', 'Gold Filled - Single')] }],
  ['an earring word in an option', { title: 'Poodle Huggie Charm Necklace', variations: [V('Type', 'Huggie hoops')] }],
  ['a necklace in a Pair of title', { title: 'Pair of Dog Charm Necklaces' }],
  ['Pairs well with', { title: 'Dog Charm, Pairs Well With Any Chain' }]
]) { const r = read(over); eq(asked(r.spec).length, 0, name + ': no question'); }
ok(read({ title: 'Poodle Huggie Charm Necklace', variations: [V('Charm Type', 'Huggie CHARM SET')] }).pair.earring === true, 'a Huggie CHARM SET choice makes the pair (unchanged)');
ok(read({ title: 'Poodle Huggie Charm Necklace', variations: [V('Type', 'Huggie hoops')] }).pair.earring === true, 'a huggie in a chosen option makes the pair even under a necklace title (the hoop rule)');
// a question in the Review list is a needsMapping problem with the words (the bridge card), and the run keeps it as plain data
{ const r = read({ title: 'Pair of Dog Charms' }), p = asked(r.spec)[0]; eq(p.listingId, '1718', 'the question names the listing'); eq(Object.keys(p.earWords), ['why'], 'the question carries its one line'); noNested(p, 'problem'); }

// ── E · what must never become a pair ──────────────────────────────────────────────────────────────────────────────
const NOT = [
  ['necklace', { title: 'Poodle Charm Necklace', variations: [V('Necklace Length', '16"')] }, 1],
  ['pendant', { title: 'Poodle Pendant' }, 1],
  ['bracelet', { title: 'Poodle Charm Bracelet' }, 1],
  ['anklet', { title: 'Poodle Charm Anklet' }, 1],
  ['key ring', { title: 'Poodle Key Ring' }, 1],
  ['keychain', { title: 'Poodle Keychain Charm' }, 1],
  ['charm only', { title: 'Poodle Charm', variations: [V('Charm Type', 'CHARM + Engraving')] }, 1],
  ['necklace charm', { title: 'Poodle Charm', variations: [V('Charm Type', 'Necklace CHARM')] }, 1],
  ['counted discs', { title: 'Initial Disc Necklace', variations: [V('Necklace Options', 'ROSEGOLD - 2 Disc')] }, 2],
  ['a plain quantity 3 line of single charms', { title: 'Poodle Charm', quantity: 3 }, 3],
  ['a gift text that says earrings', { title: 'Poodle Charm Necklace', personalization: ['for my earrings friend'], variations: [V('Necklace Length', '16"'), V('Personalization', 'matching earrings and hoops please')] }, 1],
  ['a buyer note that says earrings, hoops and studs', { title: 'Poodle Charm Necklace', buyerMessage: 'I also love your earrings, hoops and studs!' }, 1],
  ['a note that says huggie hoops (a charm line)', { title: 'Poodle Charm', buyerMessage: 'can this go on my huggie hoops?', personalization: ['huggie hoops'] }, 1],
  ['Add matching earrings: No thanks', { title: 'Poodle Charm Necklace', variations: [V('Add Matching Earrings', 'No thanks')] }, 1],
  ['Add matching earrings: Yes', { title: 'Poodle Charm Necklace', variations: [V('Add Matching Earrings', 'Yes please')] }, 1],
  ['a hoop charm necklace', { title: 'Gold Hoop Charm Necklace Pendant' }, 1],
  ['a hoop charm necklace with a size', { title: 'Gold Hoop Charm Necklace Pendant', variations: [V('Hoop Size', '15mm')] }, 1],
  ['Hoop Size under a pendant title', { title: 'Poodle Pendant', variations: [V('Hoop Size', '8.5mm')] }, 1],
  ['a student gift', { title: 'Nursing Student Gift Necklace' }, 1],
  ['a numbered design called Huggie', { sku: 'Huggie_3722', title: 'Poodle Charm' }, 1],
  ['Studio', { sku: 'STUDIO_68750', title: 'Poodle Charm' }, 1],
  ['an earring SKU and the form Necklace CHARM', { sku: 'LETTER_EARRING-3', title: 'Letter Charm', variations: [V('Charm Type', 'Necklace CHARM')] }, 1],
  ['an earring SKU and the loose charm', { sku: 'Huggie Hoops- Poodle', title: 'Poodle Charm', variations: [V('Charm Type', 'CHARM + Engraving')] }, 1],
  ['an earring SKU and the form bracelet', { sku: 'Huggie Hoops- Poodle', title: 'Poodle Charm', variations: [V('Type', 'Bracelet')] }, 1],
  ['an earring name with no answer', { title: 'Poodle Charm Necklace', variations: [V('Earring Style', 'No thanks')] }, 1],
  ['a name that offers two products, the value is the necklace', { title: 'Poodle Charm', variations: [V('Earrings or Necklace', 'Necklace')] }, 1],
  ['a name that offers two products (a slash), the value is a pendant', { title: 'Poodle Charm', variations: [V('Choose Stud / Pendant', 'Pendant with chain')] }, 1],
  ['an earring name whose value is a necklace chain', { title: 'Poodle Charm', variations: [V('Earring Style', 'Necklace chain')] }, 1],
  ['a huggie title and the buyer chose the loose charm', { title: 'Poodle Huggie Charm', variations: [V('Charm Type', 'CHARM + Engraving')] }, 1],
  ['a huggie title and the buyer chose the necklace charm', { title: 'Poodle Huggie Charm', variations: [V('Charm Type', 'Necklace CHARM')] }, 1],
  ['a custom necklace SKU', { sku: 'CUSTOM-N-001-665441', title: 'Custom Silver Necklace Charm', variations: [] }, 1],
  ['Pairs well with', { title: 'Poodle Charm, Pairs Well With Any Chain' }, 1],
  ['Pair of necklaces', { title: 'Pair of Poodle Charm Necklaces' }, 1]
];
for (const [name, over, per] of NOT) for (const q of [1, 2]) {
  const r = read(Object.assign({}, over, { quantity: (over.quantity || 1) * q })), tag = `${name} q${(over.quantity || 1) * q}`;
  ok(r.pair.earring === false, tag + ': not an earring pair');
  eq(r.spec.pieceCount, per * q, tag + ': the pieces it always made');
  ok(r.pair.sides.every(s => s === null), tag + ': no side');
  ok(CP.isEarringPair({ form: r.spec.form || '', spec: r.spec, quantity: r.spec.quantity }, null) === false, tag + ': the pair module agrees');
  const ps = CP.piecesFor({ form: r.spec.form || '', spec: r.spec, quantity: r.spec.quantity, receiptId: order.receiptId, transactionId: r.line.transactionId }, null);
  ok(ps.every(p => p.side === null && p.mirror === false), tag + ': no piece is mirrored');
  eq(O.pieceCountOf(r.raw), per * q, tag + ': the raw line counts the same');
  ok(SLO.kind(r.raw).sided === false, tag + ': the station page tells no sides');
}

// ── F · the Sep 17 snapshot: nothing it held changes class ────────────────────────────────────────────────────────────
{
  const snap = JSON.parse(fs.readFileSync(path.join(__dirname, 'pairs-words-snapshot.json'), 'utf8'));
  eq(snap.lines, 411, 'the snapshot digest stands for 411 lines');
  let earrings = 0, singles = 0;
  snap.rows.forEach(([title, sku, quantity, vars, pieces, sidesWanted, cls], i) => {
    const r = read({ transactionId: String(5300000000 + i), title, sku, quantity, listingId: String(9000 + i), variations: vars.map(([name, value]) => V(name, value)) }), tag = `line ${i} [${sku}] ${title.slice(0, 40)}`;
    eq(r.spec.pieceCount, pieces, tag + ': the same pieces');
    eq(r.pair.sides.map(s => s || '-').join(''), sidesWanted, tag + ': the same sides');
    eq(r.pair.earring ? 'E' : r.pair.single ? 'S' : '-', cls, tag + ': the same class');
    eq(asked(r.spec).length, 0, tag + ': no new question');
    eq(O.pieceCountOf(r.raw), pieces, tag + ': the raw line makes the same pieces');
    if (cls === 'E') earrings++; if (cls === 'S') singles++;
  });
  eq([earrings, singles], [95, 2], 'the distinct earring lines and the two single earrings of the snapshot (103 earring lines in all, counting repeats)');
}

// ── G · the question has a card in Review (charm-nest-bridge.js): three presses, kept under the question's own option name ───────────────────────────────────────────
{
  const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  ok(src.includes('p.earWords ? `${p.earWords.why}`'), 'the Review list says the question in its one line');
  ok(/it\.kind === "needsMapping" && p\.earWords/.test(src), 'a decision card for the question');
  ok(src.includes('keep("form", "earrings"') && src.includes('keep("form", "huggie"') && src.includes('keep("ignore", null'), 'a pair of earrings / huggie hoops / not earrings: a form or ignore');
  ok(/optionName: p\.optionName, optionValue: p\.optionValue, map: \{ field, value \}/.test(src.slice(src.indexOf('p.earWords) {'), src.indexOf('p.count) {'))), 'the answer is put under the question\'s own option name and value');
  eq(O.optionLookup(answer('Pair of Dog Charms', 'form', 'earrings'), '1718', "Earrings in this listing's words", O.norm('Pair of Dog Charms')).field, 'form', 'an answer kept for a title is found by the intake');
}

console.log(`pairs-words: ${n} checks passed`);
