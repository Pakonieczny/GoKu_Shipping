// Pairs at intake (PAIRINTAKE; Paul, 9 Oct 2026): how an Etsy order line becomes pieces. Offline, no Etsy, no paid call, nothing live.
//   node tests/charm-nest/pairs-intake.cjs
// What it proves (charm-nest-orders.js pieceCountOf / piecesOf / countRead / pairInfo; the pool maker's old-record guard; the server's answer and guard):
//   · a PAIR line (stud, hoop, huggie hoops, "earrings", Huggie CHARM SET) makes TWO pieces per unit, a Left then a Right, matching or not: quantity 2 = 4 (L R L R)
//   · a line whose chosen option or title says Single makes ONE per unit; its side only when the line names left or right, else a plain note
//   · an option that names how many discs / charms / tags a necklace carries ("2 Disc", "Number of Discs: 3") makes that many pieces of ONE group, no side
//   · an option that may name a count but does not say what is counted (letters, a range, "Set of 3", two numbers) or that the buyer's note disagrees with is NOT
//     guessed: a needsMapping problem with `count` holds the line; a person's answer ({ field: "count" }) settles it; "1" says it is no count
//   · "1-5 Character" (how long the engraving is) and sizes, lengths, purities are never counts; "N symbols" on an earring line is a number of designs, not of pieces
//   · a mismatched DESIGN is 2 per unit, L and R (PIECE_RULES.mismatchedMakesTwo); with the switch off, or when the pool cannot cut its two bodies apart (glue()), one glued copy per unit
//   · old records: a line already pooled keeps its pieces (pinPieces, the pool maker's pinPooled, the server's `short` answer); the new count is for lines pooled from now
//   · ONE function: every count the bridge shows reads CharmNestOrders.pieceCountOf; no stored field holds an array inside an array
const assert = require('assert');
const fs = require('fs'), path = require('path'), vm = require('vm');
const root = path.join(__dirname, '../..');
const O = require('../../charm-nest-orders.js');
const CP = require('../../charm-nest-pair.js');
const noNested = require('./_noNestedArrays.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };

const order = { receiptId: '4170000001', updateTs: 1789100000 };
const mk = over => Object.assign({ transactionId: '5200000001', listingId: '1718', sku: 'A', title: 'Moon Charm Necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: [{ name: 'Metal Choice', value: 'Gold' }] }, over);
const MASTER = { 'A': {}, 'A (HUGGIE)': {}, 'B': {}, 'MITTENS 1': {}, 'MITTENS 2': {}, 'MISMATCHED_7134': {}, 'MISMATCHED': {}, 'HUGGIE HOOPS-TENNIS BALL': {}, 'HUGGIE HOOPS-RACKET3': {}, 'DUAL_PAIR': { pair: { v: 1, bodies: 2, mismatched: true } }, 'MISMATCHED_9': { pair: { v: 1, bodies: 1, mismatched: false } }, 'TWIN': { pair: { v: 1, bodies: 2, mismatched: false } } };
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: s => MASTER[s] || null }, extra);
const row = (over, c) => { const line = mk(over); return { order, line, spec: O.interpretLine(order, line, c || ctx()), poolIds: [] }; };
const withRules = (r, fn) => { const keep = Object.assign({}, O.PIECE_RULES); Object.assign(O.PIECE_RULES, r); try { return fn(); } finally { Object.assign(O.PIECE_RULES, keep); } };
const V = (name, value) => ({ name, value });
const counts = x => ({ q: x.spec.quantity, pieces: x.spec.pieceCount, sides: x.spec.pair.sides.map(s => s || '').join(''), kind: x.spec.pair.kind });
const asks = x => x.spec.problems.filter(p => p.kind === 'needsMapping' && p.count);

eq(O.PIECE_RULES, { pairFormsMakeTwo: true, optionCountsMake: true, mismatchedMakesTwo: true }, 'the rule: pairs, option counts and mismatched pairs (a piece per ear) are on');

// ── 1 · the shapes of a line, quantity 1, 2 and 3 ───────────────────────────────────────────────────────────────────────────
const SHAPES = [
  // name, line over, pieces per unit, earring pair?, sides of one unit
  ['necklace', { title: 'Moon Charm Necklace', variations: [V('Style', 'Necklace')] }, 1, false, ['-']],
  ['stud earrings', { title: 'Crab Charm Stud Earrings', variations: [V('Metal Choice', 'Gold')] }, 2, true, ['L', 'R']],
  ['pair of earrings (a Type option)', { title: 'Star Earrings', variations: [V('Type', 'Pair of earrings')] }, 2, true, ['L', 'R']],
  ['huggie hoops', { title: 'Cancer Ribbon Hinged Hoops Small Hoop Earrings', variations: [V('HOOP SIZE', '8.5mm')] }, 2, true, ['L', 'R']],
  ['huggie charm set', { title: 'Star Charm Add On Charm', variations: [V('Charm Type', 'Huggie CHARM SET')] }, 2, true, ['L', 'R']],
  ['single earring (a Type option)', { title: 'Star Single Earring', variations: [V('Type', 'Single earring')] }, 1, false, ['-']],
  ['3 discs', { title: 'Initial Disc Necklace', variations: [V('Number of Discs / Metal', '3 discs • gold')] }, 3, false, ['-', '-', '-']],
  ['2 disc', { title: 'Initial Disc Necklace', variations: [V('Necklace Options', 'ROSEGOLD - 2 Disc')] }, 2, false, ['-', '-']],
  ['charm only', { title: 'Lighthouse Charm', variations: [V('Charm Type', 'Necklace CHARM')] }, 1, false, ['-']],
  // an earring listing whose chosen form is a necklace or a loose charm is that, not an earring pair
  ['earring title, form necklace', { title: 'Lighthouse Charm Earrings or Necklace', variations: [V('Charm Type', 'Necklace CHARM')] }, 1, false, ['-']]
];
for (const [name, over, per, earring, sides] of SHAPES) for (const q of [1, 2, 3]) {
  const r = row(Object.assign({ quantity: q }, over)), tag = `${name} q${q}`;
  eq(r.spec.quantity, q, tag + ': spec.quantity is the Etsy quantity');
  eq(r.spec.pieceCount, q * per, tag + ': pieces = units x pieces per unit');
  eq(r.spec.pair.perUnit, per, tag + ': pieces per unit');
  eq(r.spec.pair.earring, earring, tag + ': earring pair line');
  for (const x of [r, r.spec, r.line]) eq(O.pieceCountOf(x), q * per, tag + ': pieceCountOf(row / spec / raw line) is the one count');
  eq(r.spec.pair.sides, Array.from({ length: q }, () => sides.map(s => s === '-' ? null : s)).flat(), tag + ': sides in order (L R L R for a pair, none for anything else)');
  eq(O.piecesOf(r).map(p => p.side), r.spec.pair.sides, tag + ': piecesOf agrees');
  eq(O.piecesOf(r).map(p => p.n), Array.from({ length: q * per }, (_, i) => i + 1), tag + ': pieces numbered 1..n');
  eq(O.piecesOf(r).map(p => p.unit), Array.from({ length: q * per }, (_, i) => Math.floor(i / per) + 1), tag + ': each piece knows its unit');
  eq(r.spec.problems.length, 0, tag + ': nothing to ask');
  noNested(r.spec, tag);
}
// the kind: one earring pair is "pair"; everything else with 2 or more pieces is "multi" (2 discs, 2 singles, 2 pairs), one piece is "single"
eq(counts(row({ title: 'Crab Charm Stud Earrings', quantity: 1 })), { q: 1, pieces: 2, sides: 'LR', kind: 'pair' }, 'a quantity-1 stud line is a pair: a Left and a Right');
eq(counts(row({ title: 'Crab Charm Stud Earrings', quantity: 2 })), { q: 2, pieces: 4, sides: 'LRLR', kind: 'multi' }, 'quantity 2 of a pair line is 4 pieces: L R L R');
eq(counts(row({ title: 'Initial Disc Necklace', variations: [V('Necklace Options', 'ROSEGOLD - 2 Disc')] })), { q: 1, pieces: 2, sides: '', kind: 'multi' }, 'a 2-disc necklace is 2 pieces and is not an earring pair (no side, kind multi)');
eq(counts(row({ title: 'Moon Charm Necklace' })).kind, 'single', 'a necklace is single');
eq(CP.groupKey({ receiptId: order.receiptId, transactionId: '5200000001' }), '4170000001:5200000001', 'one group per order line');

// ── 2 · single earrings: one piece per unit, the side only when the line names it ────────────────────────────────────────────
{
  const huggie = { title: 'Huggie Charm + Shipping', variations: [V('Price', 'Gold Filled - Single'), V('Personalization', 'One grizzly bear and one winged griffin please')] };
  const s1 = row(Object.assign({ quantity: 1 }, huggie)), s2 = row(Object.assign({ quantity: 2 }, huggie));
  eq(counts(s1), { q: 1, pieces: 1, sides: '', kind: 'single' }, 'a Single option is one piece per unit (4176744752)');
  eq(counts(s2).pieces, 2, 'quantity 2 of singles is 2 pieces');
  ok(s1.spec.pair.single && !s1.spec.pair.earring && s1.spec.pair.soldAs === 'single', 'it says single');
  ok(s1.spec.pair.notes.some(x => /does not say left or right/.test(x)), 'the line does not say which ear: a plain note, nothing guessed');
  eq(s1.spec.pair.sideSaid, null, 'no side');
  const named = (value, note) => row({ title: 'Star Stud Earrings', variations: [V('Type', value)].concat(note ? [V('Personalization', note)] : []) });
  eq(named('Single - Left').spec.pair.sideSaid, 'L', 'an option "Single - Left" names the left ear');
  eq(named('Single - Left').spec.pair.sides, ['L'], 'the one piece is a Left');
  eq(named('Single earring').spec.pair.sideSaid, null, 'Single earring alone names no ear');
  eq(named('Single earring', 'right ear only please').spec.pair.sideSaid, 'R', 'the buyer\'s note names the right ear');
  eq(named('Single earring', 'for my left ear').spec.pair.sideSaid, 'L', 'for my left ear');
  eq(named('Single earring', 'I left the design to you').spec.pair.sideSaid, null, '"left" as a verb is not a side');
  eq(named('Single earring', 'left ear and right ear please').spec.pair.sideSaid, null, 'both ears named is not a side (a person reads it)');
  eq(row({ quantity: 2, title: 'Star Stud Earrings', variations: [V('Type', 'Single - Left')] }).spec.pair.sides, ['L', 'L'], 'two singles whose OPTION names the left ear: both are Left (the buyer chose that option for each unit)');
  { const two = named('Single earring', 'right ear only please'); const q2 = row({ quantity: 2, title: 'Star Stud Earrings', variations: [V('Type', 'Single earring'), V('Personalization', 'right ear only please')] });
    eq(two.spec.pair.sideBy, 'note', 'a note names the ear'); eq(q2.spec.pair.sides, [null, null], 'two singles and a NOTE that names one ear: which is which is not said');
    ok(q2.spec.pair.notes.some(x => /not guessed/.test(x)), 'and a plain note says so'); }
  // a TITLE that says Single (PAIRTESTS, snapshot order 4172023444): the word need not sit next to "earring"
  const T = (title, q, vars) => row({ quantity: q || 1, title, variations: vars || [] });
  eq(counts(T('Custom Single Replacement Silver Cat Huggie Earring Left Ear')), { q: 1, pieces: 1, sides: 'L', kind: 'single' }, 'title: Single ... Earring Left Ear is one Left piece');
  eq(counts(T('Single Cat Huggie Earring Left Ear')), { q: 1, pieces: 1, sides: 'L', kind: 'single' }, 'title: Single Cat Huggie Earring Left Ear');
  eq(counts(T('Huggie Earring, Single')).pieces, 1, 'title: ends with ", Single"'); eq(counts(T('Stud Earrings - Single')).pieces, 1, 'title: ends with "- Single"');
  eq(counts(T('Single Cat Huggie Earring Right Ear', 3)), { q: 3, pieces: 3, sides: 'RRR', kind: 'multi' }, 'title: three singles that name the right ear are three Right pieces');
  eq(T('Single Pearl Stud Earrings').spec.pieceCount, 2, 'Single Pearl Stud Earrings is a PAIR (one pearl each, the plural word after it)'); eq(T('Single Disc Hoop Earrings').spec.pieceCount, 2, 'Single Disc Hoop Earrings is a pair');
  eq(T('Single Initial Disc Necklace').spec.pieceCount, 1, 'a necklace titled Single is one piece, no earring');
  eq(row({ title: 'Star Stud Earrings', variations: [V('Type', 'Pair')] }).spec.pair.soldAs, 'pair', 'an option that says Pair is a pair');
  eq(row({ title: 'Single Stud Earring or Pair', variations: [V('Type', 'Pair')] }).spec.pieceCount, 2, 'the option the buyer chose beats the title');
  eq(row({ title: 'Single Stud Earring', variations: [V('Type', 'Whatever')] }).spec.pieceCount, 1, 'a title that says single earring is one');
}

// ── 3 · option counts (the drop-downs) ──────────────────────────────────────────────────────────────────────────────────────────
{
  const disc = (name, value, extra) => row(Object.assign({ title: 'Initial Disc Necklace', variations: [V(name, value)] }, extra));
  eq(disc('Number of Discs / Metal', '3 discs • gold').spec.pieceCount, 3, '"3 discs • gold" is 3 (count:unit)');
  eq(disc('Necklace Options', 'ROSEGOLD - 2 Disc').spec.pieceCount, 2, '"ROSEGOLD - 2 Disc" is 2');
  eq(disc('Discs', 'three discs').spec.pieceCount, 3, 'a number word');
  eq(disc('Options', '2-disc').spec.pieceCount, 2, '2-disc');
  eq(disc('Options', 'x4 Charms').spec.pieceCount, 4, 'x4 charms');
  eq(disc('Number of Discs', 'Three').spec.pieceCount, 3, 'a name that asks "number of" and a bare number word (count:name)');
  eq(disc('How many charms?', '2').spec.pieceCount, 2, 'how many + a digit');
  eq(disc('Number of Discs', 'Double').spec.pieceCount, 2, 'number of discs: Double');
  eq(disc('Options', '1 disc').spec.pieceCount, 1, '1 disc is one');
  eq(disc('Options', '3 discs').spec.problems.length, 0, 'a settled count asks nothing');
  eq(disc('Options', '3 discs').spec.options.map(o => o.mapped && o.mapped.field), ['count'], 'the option answers itself as a count (no generic "what does this option decide" question)');
  // each unit its own piece of one group, never a multiple of the necklace
  eq(disc('Options', '3 discs', { quantity: 2 }).spec.pieceCount, 6, '2 necklaces of 3 discs: 6 pieces');
  eq(O.piecesOf(disc('Options', '3 discs', { quantity: 2 })).map(p => p.unit), [1, 1, 1, 2, 2, 2], 'which necklace each disc belongs to');
  // never a count
  for (const [name, value] of [['How Many Characters & Metal Color?', '1-5 Character-SILVER'], ['METAL - ENGRAVING', 'GOLD·6-10·Characters'], ['METAL - ENGRAVING', 'SILVER·1-5·Character'], ['HOOP SIZE', '8.5mm'], ['Ring size', '8 US'], ['Necklace Length in inches', '16'], ['Metal Choice', '14K SOLID GOLD'], ['Price', '5'], ['Charm Type', 'Tag1 (front engrave)'], ['Charm Size', '14mm + engraving'], ['Zodiac Sign', 'Pisces'], ['Fonts', '16"/ Typewriter']]) {
    const r = disc(name, value);
    eq(r.spec.pieceCount, 1, `${name}: ${value} is not a count`);
    eq(asks(r).length, 0, `${name}: ${value} asks nothing about a count`);
  }
  // not guessed: a person says
  const q1 = disc('Letters', '3 letters'); eq(q1.spec.pieceCount, 1, '3 letters: one piece until a person says (letters may be on one piece)'); eq(asks(q1).length, 1, 'one question'); eq(asks(q1)[0].count.guess, 3, 'with the number it reads as the suggestion'); ok(/letters/.test(asks(q1)[0].count.why), 'in plain words');
  ok(q1.spec.pair.asks.length === 1 && q1.spec.problems.some(p => p.kind === 'needsMapping' && p.optionName === 'Letters'), 'the line carries the question as a needsMapping problem (it waits like an unmatched SKU)');
  eq(asks(disc('Options', 'Set of 3')).length, 1, 'Set of 3 does not say of what: asked');
  eq(asks(disc('Number of Items', '3')).length, 1, 'a count cue and no unit: asked');
  eq(asks(disc('Options', '1-3 discs')).length, 1, 'a range: asked');
  eq(asks(disc('Number of Charms', '2 or 3')).length, 1, 'two numbers in one value: asked');
  eq(asks(disc('Metal', '4 Silver / 2 Gold', { sku: 'CUSTOM-N-001-413361', title: 'Custom Necklace Charms' })).length, 0, 'a custom special order is a person\'s (Custom Orders), not a count question');
  // a pair line: "N symbols" is a number of designs
  const z1 = row({ title: 'Pisces Zodiac Stud Earrings', variations: [V('Metal Choice :', 'Silver • 1 symbol'), V('Zodiac Sign', 'Pisces')] }), z2 = row({ quantity: 2, title: 'Pisces Zodiac Stud Earrings', variations: [V('Metal Choice :', 'Silver • 2 symbols'), V('Zodiac Sign', 'Libra')] });
  eq(counts(z1), { q: 1, pieces: 2, sides: 'LR', kind: 'pair' }, '"1 symbol" on a stud line: the same symbol on both ears, a pair');
  eq(counts(z2), { q: 2, pieces: 4, sides: 'LRLR', kind: 'multi' }, '"2 symbols" on a stud line (quantity 2) is 4 pieces, not 8: symbols are designs');
  eq(asks(z2).length + asks(z1).length, 0, 'no question for symbols on an earring line');
  eq(z2.spec.pair.says, true, '"2 symbols" says two designs');
  ok(z2.spec.pair.notes.some(x => /two different designs but names one/.test(x)), 'two designs but one SKU: made as a matching pair with a plain note');
  eq(asks(row({ title: 'Zodiac Symbol Necklace', variations: [V('Options', '3 symbols')] })).length, 1, '"3 symbols" on a necklace: does each make a piece? asked');
  // an earring line that names discs / charms: asked (is it per earring?)
  const e1 = row({ title: 'Disc Stud Earrings', variations: [V('Options', '2 discs')] }); eq(asks(e1).length, 1, 'an earring line that names 2 discs asks'); eq(e1.spec.pieceCount, 2, 'and meanwhile counts the pair');
  // the buyer's note vs the option
  const c1 = disc('Options', '3 discs', { variations: [V('Options', '3 discs'), V('Personalization', 'Please make 2 discs, J and Q')] });
  eq(asks(c1).length, 1, 'the option says 3, the note says 2 discs: a person reads the note'); ok(/note/.test(asks(c1)[0].count.why), 'the question says why');
  const c2 = disc('Options', '2 Disc', { variations: [V('Options', 'ROSEGOLD - 2 Disc'), V('Personalization', 'Tag 1: J, Tag 2: Q')] });
  eq(asks(c2).length, 0, 'the note agrees (Tag 1, Tag 2): nothing to ask'); eq(c2.spec.pieceCount, 2, '2 pieces');
  const c3 = disc('Options', 'x', { variations: [V('Metal Choice', 'Gold'), V('Personalization', 'three discs: A, B, C')] });
  eq(c3.spec.pieceCount, 1, 'a note alone never counts by itself'); eq(asks(c3).length, 1, 'but a note that names 3 discs with no option count is ASKED (the line waits, F5)');
  eq([asks(c3)[0].optionName, asks(c3)[0].optionValue, asks(c3)[0].count.rule], ['Buyer note', 'three discs', 'count:note-only'], 'under the pseudo option "Buyer note", the note\'s own words');
  { const answered = n => disc('Options', 'x', { variations: [V('Metal Choice', 'Gold'), V('Personalization', 'three discs: A, B, C')] }); const withAns = v => row({ title: 'Initial Disc Necklace', variations: [V('Metal Choice', 'Gold'), V('Personalization', 'three discs: A, B, C')] }, ctx({ optionMaps: { '1718': { 'buyer note': { 'three discs': { field: 'count', value: v } } } } }));
    eq([withAns('3').spec.pieceCount, asks(withAns('3')).length], [3, 0], 'a person answered 3 for that wording: 3 pieces, nothing left to ask'); eq(withAns('1').spec.pieceCount, 1, 'answered Just 1: one piece'); void answered; }
  eq(asks(row({ title: 'Disc Stud Earrings', variations: [V('Metal Choice', 'Gold'), V('Personalization', 'two discs please')] })).length, 0, 'a note on an EARRING line is no count of pieces: nothing asked');
  // a person's answer
  const maps = value => ({ '1718': { letters: { '3 letters': { field: 'count', value } } } });
  const a3 = row({ title: 'Initial Disc Necklace', variations: [V('Letters', '3 letters')] }, ctx({ optionMaps: maps('3') }));
  eq(a3.spec.pieceCount, 3, 'answered 3: 3 pieces'); eq(a3.spec.problems.length, 0, 'no question left');
  const a1 = row({ title: 'Initial Disc Necklace', variations: [V('Letters', '3 letters')] }, ctx({ optionMaps: maps('1') }));
  eq(a1.spec.pieceCount, 1, 'answered "just 1": one piece'); eq(a1.spec.problems.length, 0, 'no question left');
  eq(row({ title: 'Stud Earrings', quantity: 1, variations: [V('Letters', '3 letters')] }, ctx({ optionMaps: maps('4') })).spec.pieceCount, 4, 'an answer is the pieces ONE unit makes in all, on a pair line too');
  eq(row({ title: 'Initial Disc Necklace', quantity: 2, variations: [V('Letters', '3 letters')] }, ctx({ optionMaps: maps('3') })).spec.pieceCount, 6, 'and times the units');
  eq(row({ title: 'Initial Disc Necklace', variations: [V('Letters', '3 letters')] }, ctx({ optionMaps: { '*': { letters: { '3 letters': { field: 'count', value: '2' } } } } })).spec.pieceCount, 2, 'a shop-wide answer');
  eq(row({ title: 'Initial Disc Necklace', variations: [V('Necklace Options', '2 Disc')] }, ctx({ optionMaps: { '1718': { 'necklace options': { '2 disc': { field: 'ignore', value: null } } } } })).spec.pieceCount, 2, 'an old "this option changes nothing" answer does not hide a settled disc count');
  eq(row({ title: 'Initial Disc Necklace', variations: [V('Necklace Options', '2 Disc')] }, ctx({ optionMaps: { '1718': { 'necklace options': { '2 disc': { field: 'count', value: '1' } } } } })).spec.pieceCount, 1, 'but a person\'s count answer wins');
  // a line with no design is never asked
  eq(asks(row({ sku: 'CHAIN_1', title: 'Chain Replacement', variations: [V('Length', '16 Inches'), V('Letters', '3 letters')] }, ctx({ noDesign: { skus: ['CHAIN_1'], patterns: [] } }))).length, 0, 'a no-design line asks nothing');
}

// ── 4 · what a line says in its own words (pure; raw Etsy transactions too) ───────────────────────────────────────────────────────
{
  const raw = { transaction_id: 1, listing_id: 2, sku: 'Huggie Hoops-Tennis Ball/Racket3', title: 'Mismatched Tennis Ball and Raquet Huggie Hoops', quantity: 1, variations: [{ formatted_name: 'Metal Choice', formatted_value: 'Gold' }], message_from_buyer: '' };
  ok(O.lineSignals(raw).says && O.lineMismatched(raw), 'a raw Etsy transaction that says Mismatched');
  eq(O.pieceCountOf(raw), 2, 'a raw huggie hoops transaction (no master) is 2 pieces');
  eq(O.splitSkus('Huggie Hoops-Tennis Ball/Racket3'), ['Huggie Hoops-Tennis Ball', 'Racket3'], 'two designs named by one SKU');
  eq(O.splitSkus('MITTENS 1 + MITTENS 2'), ['MITTENS 1', 'MITTENS 2'], 'plus');
  eq(O.splitSkus('A'), null, 'one SKU');
  ok(!O.lineMismatched(mk({ title: 'Left-facing wolf necklace', variations: [V('Font', 'Left aligned')] })), '"left" alone is never a mismatched pair');
  ok(O.lineSignals(mk({ variations: [V('Left Earring Design', 'MITTENS 1'), V('Right Earring Design', 'MITTENS 2')] })).says, 'options named for the left and right ear');
  eq(O.countRead(raw).n, 0, 'no count in a raw line');
  eq(O.pieceCountOf({ pieceCount: 5, quantity: 1 }), 5, 'an explicit count wins');
  eq(O.pieceCountOf({ spec: { pieceCount: 3 }, quantity: 9 }), 3, 'an explicit spec.pieceCount wins');
  eq(O.pieceCountOf(null), 1, 'nothing is one');
}

// ── 5 · a mismatched pair ───────────────────────────────────────────────────────────────────────────────────────────────────────
{
  const byName = row({ sku: 'MISMATCHED_7134', title: 'Mittens Mismatched Stud Earrings', quantity: 1 });
  ok(byName.spec.pair.mismatched && byName.spec.pair.source === 'name', 'MISMATCHED_7134 is a mismatched pair (by its name until the catalogue carries pair)');
  eq(counts(byName), { q: 1, pieces: 2, sides: 'LR', kind: 'mismatched' }, 'a mismatched pair is a Left and a Right piece');
  ok(byName.spec.pair.earring && !byName.spec.pair.glued && byName.spec.pair.split === true, 'cut from its two bodies');
  eq(O.piecesOf(byName).map(p => p.bodyIndex), [0, 1], 'body 0 is the left, body 1 the right');
  const byField = row({ sku: 'DUAL_PAIR', quantity: 2 });
  ok(byField.spec.pair.mismatched && byField.spec.pair.source === 'design', 'entry.pair.mismatched makes it a mismatched pair');
  eq(counts(byField), { q: 2, pieces: 4, sides: 'LRLR', kind: 'multi' }, 'two units are 4 pieces, L R L R');
  eq(O.piecesOf(byField).map(p => p.bodyIndex), [0, 1, 0, 1], 'bodies 0 1 0 1');
  ok(!row({ sku: 'MISMATCHED_9' }).spec.pair.mismatched, 'entry.pair says not mismatched: the record wins over the name');
  ok(!row({ sku: 'TWIN' }).spec.pair.mismatched, 'two identical bodies are a matching pair, not mismatched');
  ok(!O.interpretLine(order, mk({ sku: 'MISMATCHED_5555' }), ctx()).pair.mismatched, 'a MISMATCHED name no master holds is an unmatched SKU, not a pair');
  const entry = { pair: { v: 1, bodies: 2, mismatched: true } };
  const ps = CP.piecesFor(Object.assign({ receiptId: order.receiptId, transactionId: byName.line.transactionId }, byName), entry);
  eq(ps.map(p => p.side), ['L', 'R'], 'charm-nest-pair.js piecesFor says Left then Right'); eq(ps.map(p => p.bodyIndex), [0, 1], 'bodies 0 and 1');
  eq(CP.piecesFor(Object.assign({ receiptId: order.receiptId, transactionId: byField.line.transactionId }, byField), entry).map(p => p.side), ['L', 'R', 'L', 'R'], 'quantity 2: L R L R');
  // two SKUs / two options name the pair's designs
  const two = row({ sku: 'MITTENS 1 + MITTENS 2', title: 'Mittens Mismatched Studs' });
  ok(two.spec.pair.mismatched && two.spec.pair.source === 'skus', 'two SKUs in the master name a mismatched pair'); eq(two.spec.pair.members.map(m => m.sku), ['MITTENS 1', 'MITTENS 2'], 'its members'); eq(two.spec.designSku, 'MITTENS 1', 'pooled as its first design'); eq(two.spec.pieceCount, 2, '2 pieces');
  const opt = row({ sku: 'A', title: 'Mittens Stud Earrings', variations: [V('Left Earring Design', 'MITTENS 1'), V('Right Earring Design', 'MITTENS 2')] });
  eq(opt.spec.pair.source, 'options', 'options named for the left and the right'); eq(opt.spec.pieceCount, 2, '2 pieces');
  // the glued fallback: a mismatched pair whose bodies the pool cannot cut apart is the one glued copy per unit it always was
  const g = row({ sku: 'MISMATCHED_7134', title: 'Mittens Mismatched Stud Earrings', quantity: 2 });
  O.glue(g.spec);
  eq(counts(g), { q: 2, pieces: 2, sides: '', kind: 'multi' }, 'glue(): one copy per unit, no side'); ok(g.spec.pair.glued && g.spec.pair.notes.some(x => /glued piece/.test(x)), 'and it says so');
  eq(counts(O.glue(row({ sku: 'MISMATCHED_7134', quantity: 1 }).spec) && row({ sku: 'MISMATCHED_7134', quantity: 1 })).pieces, 2, 'glue() changes only the spec it is given');
  const g1 = row({ sku: 'MISMATCHED_7134', quantity: 1 }); O.glue(g1.spec); eq(counts(g1), { q: 1, pieces: 1, sides: '', kind: 'mismatched' }, 'one glued copy is still the mismatched kind');
  ok(O.glue(row({ sku: 'A' }).spec).pair.glued === false, 'glue() leaves a line that is not mismatched alone');
  // the switch off brings the glued copy back for every mismatched design at once
  withRules({ mismatchedMakesTwo: false }, () => {
    const off = row({ sku: 'MISMATCHED_7134', title: 'Mittens Mismatched Stud Earrings', quantity: 2 });
    ok(off.spec.pair.glued && off.spec.pair.earring && off.spec.pair.split === false, 'switch off: one glued copy per unit'); eq(off.spec.pieceCount, 2, 'quantity 2: two glued copies'); eq(off.spec.pair.sides, [null, null], 'with no side');
    const twoOff = row({ sku: 'MITTENS 1 + MITTENS 2', title: 'Mittens Mismatched Studs' });
    ok(twoOff.spec.pair.mismatched && twoOff.spec.pair.glued, 'a two-SKU line is read as its pair but counted as one glued copy');
  });
  const says = row({ sku: 'A', title: 'Mismatched Studs', quantity: 1 }); ok(says.spec.pair.says && !says.spec.pair.mismatched, 'words alone never make a mismatched pair'); eq(says.spec.pieceCount, 2, 'it is made as a matching pair of 2 meanwhile'); ok(says.spec.pair.notes.length === 1, 'with a plain note');
}

// ── 6 · old records keep their pieces ───────────────────────────────────────────────────────────────────────────────────────────────
{
  const r = row({ title: 'Crab Charm Stud Earrings', quantity: 1 });
  eq(r.spec.pieceCount, 2, 'the rule says 2');
  O.pinPieces(r.spec, 1);
  eq(r.spec.pieceCount, 1, 'a stud line pooled as 1 piece keeps 1'); eq(r.spec.pieceRule, 2, 'the rule\'s count is kept beside it'); ok(/pooled as 1 piece before the pair and count rule; the rule now gives 2/.test(r.spec.pieceNote), r.spec.pieceNote);
  eq(r.spec.pair.sides, [null], 'an old piece carries no side'); eq(r.spec.pair.kind, 'single', 'kind follows what exists'); ok(r.spec.pair.legacy, 'marked legacy');
  eq(O.pieceCountOf(r), 1, 'every reader sees the pieces that exist'); eq(O.piecesOf(r).map(p => p.side), [null], 'piecesOf too');
  O.pinPieces(r.spec, 1); eq(r.spec.pieceCount, 1, 'pinning twice changes nothing');
  O.pinPieces(r.spec, 2); eq(r.spec.pieceCount, 2, 'a line pooled at the full count is whole again'); ok(!r.spec.pieceNote && !r.spec.pieceRule && !r.spec.pair.legacy, 'the note is gone'); eq(r.spec.pair.sides, ['L', 'R'], 'its sides are back'); eq(r.spec.pair.kind, 'pair', 'and its kind');
  const q2 = row({ title: 'Crab Charm Stud Earrings', quantity: 2 }); O.pinPieces(q2.spec, 2); eq(q2.spec.pieceCount, 2, 'a quantity-2 line pooled as 2 keeps 2'); eq(q2.spec.pair.kind, 'multi', 'kind multi, never an earring pair of two'); ok(/the rule now gives 4/.test(q2.spec.pieceNote), q2.spec.pieceNote);
  const disc = row({ title: 'Initial Disc Necklace', variations: [V('Necklace Options', '2 Disc')] }); O.pinPieces(disc.spec, 1); eq(O.pieceCountOf(disc), 1, 'a disc necklace pooled as 1 keeps 1');
  eq(O.pinPieces(null, 3), null, 'nothing to pin'); const keep = row({ title: 'Moon Necklace' }); O.pinPieces(keep.spec, 0); eq(keep.spec.pieceCount, 1, 'no pool rows: untouched');
  // the pool maker's guard (the real function from the bridge, run on fakes)
  const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const from = src.indexOf('  function pinPooled(row) {'), to = src.indexOf('  function cloneCharm(');
  ok(from > 0 && to > from, 'pinPooled is in the bridge');
  const rows = new Map(); const c = { B: { pool: { rows } }, O };
  vm.createContext(c); vm.runInContext(src.slice(from, to) + ';this.pinPooled = pinPooled;', c);
  const mkrow = (over, ids, qty) => { const x = row(over); x.poolIds = ids; for (const id of ids) rows.set(id, { poolId: id, quantity: qty }); return x; };
  const old = mkrow({ title: 'Crab Charm Stud Earrings', quantity: 1 }, ['4170000001_5200000001_1'], 1); c.pinPooled(old);
  eq(old.spec.pieceCount, 1, 'the pool maker: a pooled line whose rows say 1 keeps 1'); ok(old.spec.pieceNote, 'with a note');
  const fresh = mkrow({ title: 'Crab Charm Stud Earrings', quantity: 1, transactionId: '5200000002' }, ['4170000001_5200000002_1', '4170000001_5200000002_2'], 2); c.pinPooled(fresh);
  eq(fresh.spec.pieceCount, 2, 'a line pooled at the new count is whole'); ok(!fresh.spec.pieceNote, 'no note');
  const none = row({ title: 'Crab Charm Stud Earrings', quantity: 1 }); c.pinPooled(none); eq(none.spec.pieceCount, 2, 'a line never pooled gets the rule\'s count');
  const changed = mkrow({ title: 'Crab Charm Stud Earrings', quantity: 1, transactionId: '5200000003' }, ['4170000001_5200000003_1'], 1); changed.repoolChanged = true; c.pinPooled(changed);
  eq(changed.spec.pieceCount, 2, 'a line changed on Etsy is made up new, at the new count');
  const half = mkrow({ title: 'Crab Charm Stud Earrings', quantity: 1, transactionId: '5200000004' }, ['4170000001_5200000004_2'], 2); c.pinPooled(half);
  eq(half.spec.pieceCount, 2, 'a pair with one piece taken off on purpose is still a pair (the rows\' own quantity says 2)'); ok(!half.spec.pieceNote, 'not an old record');
  const off = mkrow({ title: 'Crab Charm Stud Earrings', quantity: 1, transactionId: '5200000005' }, ['4170000001_5200000005_1'], 1); rows.get(off.poolIds[0]).state = 'abandoned'; c.pinPooled(off);
  eq(off.spec.pieceCount, 2, 'an old line taken off its sheet (abandoned) is made up new, at the rule\'s count'); ok(!off.spec.pieceNote, 'with no note');
  const sup = mkrow({ title: 'Crab Charm Stud Earrings', quantity: 1, transactionId: '5200000006' }, ['4170000001_5200000006_1'], 1); rows.get(sup.poolIds[0]).state = 'superseded'; c.pinPooled(sup);
  eq(sup.spec.pieceCount, 2, 'and one superseded by an Etsy change likewise');
}

// ── 7 · the bridge reads ONE count (static: nothing counts a line's pieces any other way) ────────────────────────────────────────
{
  const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const mp = src.slice(src.indexOf('  async function makePool('), src.indexOf('  /** The recorded line joins its sheet.'));
  ok(/pinPooled\(row\)/.test(mp) && /sp\.pieceCount/.test(mp) && !/copy <= sp\.quantity/.test(mp), 'makePool pins an old line, then makes the intake\'s spec.pieceCount copies, not the Etsy quantity'); ok(/O\.glue\(sp\)/.test(mp), 'and falls back to the glued copy when a mismatched pair\'s bodies cannot be cut apart');
  ok(/copy, quantity: count/.test(mp), 'a pool row\'s quantity is the pieces of its line');
  for (const m of ['piecesOfRows = rows =>', 'charms+=O.pieceCountOf(r)', '* O.pieceCountOf(sp)', 'waiting.reduce((n, r) => n + O.pieceCountOf(r), 0)']) ok(src.includes(m), 'bridge count site: ' + m);
  ok(/field: "count", value: String\(n\)/.test(src) && /How many pieces\?/.test(src), 'the Review card answers a count question');
  ok(/spec\?\.pieceCount/.test(fs.readFileSync(path.join(root, 'charm-nest-readiness.js'), 'utf8')), 'readiness reads spec.pieceCount');
}

// ── 9 · the adversarial review's findings (ADVCOUNT F1 to F7): lost pieces, single earrings by any wording, a line that says two designs but names one ─────────────
{
  // F1 · "Just 1" on an earring line says the option is no count: the pair stays a Left and a Right
  const maps1 = (name, value) => ({ '1718': { [name]: { [value]: { field: 'count', value: '1' } } } });
  const e1 = row({ title: 'Star Studs', variations: [V('Qty', '2 studs')] }, ctx({ optionMaps: maps1('qty', '2 studs') }));
  eq(counts(e1), { q: 1, pieces: 2, sides: 'LR', kind: 'pair' }, 'F1: "Qty: 2 studs" answered Just 1 on a stud line is still a pair');
  const e2 = row({ title: 'Cancer Hinged Hoop Earrings', variations: [V('Charms', '2 charms')] }, ctx({ optionMaps: maps1('charms', '2 charms') }));
  eq(counts(e2).pieces, 2, 'F1: "Charms: 2 charms" on a hoop line answered Just 1 is still a pair');
  const e3 = row({ title: 'Mismatched Earrings', sku: 'MISMATCHED_7134', variations: [V('Qty', '2 pairs')] }, ctx({ optionMaps: maps1('qty', '2 pairs') }));
  eq(counts(e3).pieces, 2, 'F1: a mismatched design answered Just 1 is still two pieces');
  eq(counts(row({ title: 'Initial Disc Necklace', variations: [V('Letters', '3 letters')] }, ctx({ optionMaps: maps1('letters', '3 letters') }))).pieces, 1, 'F1: Just 1 on a necklace is one piece, as before');
  eq(counts(row({ title: 'Star Studs', variations: [V('Qty', '2 studs')] }, ctx({ optionMaps: { '1718': { qty: { '2 studs': { field: 'count', value: '4' } } } } }))).pieces, 4, 'F1: an answer above 1 is the pieces the line makes in all (4)');

  // F2 · a name that is a piece word and a bare number under it is a count
  for (const [name, value, want] of [['Discs', '2', 2], ['Disc Count', '2', 2], ['Disc Quantity', '2', 2], ['Charms', '3', 3], ['# of discs', '2', 2], ['Discs (qty)', '2', 2], ['Select Discs', 'Two', 2], ['Tags', 'x3', 3]])
    eq(row({ title: 'Initial Disc Necklace', variations: [V(name, value)] }).spec.pieceCount, want, `F2: "${name}: ${value}" is ${want}`);
  eq(asks(row({ title: 'Initial Disc Necklace', variations: [V('Letters', '2')] })).length, 1, 'F2: "Letters: 2" is asked (marks on a disc, or a disc each?)');
  for (const [name, value] of [['Charm Size', '12'], ['Disc Size', '10'], ['Charm Type', '2'], ['Chain Length', '18']]) eq(row({ title: 'Initial Disc Necklace', variations: [V(name, value)] }).spec.pieceCount, 1, `F2: "${name}: ${value}" is a size or a style, never a count`);

  // F3 · describing words between the number and the unit, double / triple
  for (const [value, want] of [['3 Gold Discs', 3], ['2 Rose Gold Plated Charms', 2], ['Double Disc', 2], ['Triple Disc', 3], ['Duo Charms', 2]]) eq(row({ title: 'Initial Disc Necklace', variations: [V('Necklace Options', value)] }).spec.pieceCount, want, `F3: "${value}" is ${want}`);
  eq(row({ title: 'Initial Disc Necklace', variations: [V('Necklace Options', '2 Tone Charm')] }).spec.pieceCount, 1, 'F3: "2 Tone" is not a count');
  eq(asks(row({ title: 'Initial Disc Necklace', variations: [V('Necklace Options', '2 Initial Discs')] })).length, 1, 'F3: "2 Initial Discs" is still asked');

  // F4 · an option that ADDS a piece, or "2 of 3", is asked
  const ask1 = (name, value, title) => asks(row({ title: title || 'Initial Disc Necklace', variations: [V(name, value)] }));
  eq(ask1('Add a second charm', 'Yes +$12')[0].count.rule, 'count:add', 'F4: "Add a second charm: Yes +$12" is asked (count:add)');
  for (const [name, value] of [['Extra', 'Add Extra Charm'], ['Add another disc', 'Yes +$14'], ['Extras', '2 extra charms +$20'], ['Add Charms', 'Add 2 charms']]) eq(ask1(name, value).length, 1, `F4: "${name}: ${value}" is asked`);
  eq(ask1('Disc 2 of 3', 'Initial B')[0].count.rule, 'count:ordinal', 'F4: "Disc 2 of 3" is asked (count:ordinal)'); eq(ask1('Disc 2 of 3', 'Initial B')[0].count.guess, 3, 'with 3 as the guess');
  for (const [name, value] of [['Add a second charm', 'No'], ['Charm Type', 'Add On Charm'], ['Charm Size', 'Extra Small'], ['Gift', 'Add a gift box']]) eq(ask1(name, value).length, 0, `F4: "${name}: ${value}" asks nothing`);

  // F6 · a single earring by any wording
  const one = (over, sides, tag) => { const r = row(Object.assign({ title: 'Star Stud Earrings' }, over)); eq([r.spec.pieceCount, r.spec.pair.sides.map(x => x || '').join('')], [1, sides], tag); };
  one({ title: 'Single Cat Huggie Earring' }, '', 'F6: "Single Cat Huggie Earring" is one'); one({ title: 'Single Star Earring' }, '', 'F6: "Single Star Earring"'); one({ title: 'Cat Earring (single)' }, '', 'F6: "Earring (single)"');
  one({ title: '1 Stud' }, '', 'F6: "1 Stud"'); one({ title: 'Individual earring' }, '', 'F6: "Individual earring"');
  one({ title: 'Right Earring Replacement' }, 'R', 'F6: "Right Earring Replacement" is one Right'); one({ title: 'Replacement Stud Earring for Left Ear' }, 'L', 'F6: "Replacement Stud Earring for Left Ear" is one Left');
  one({ variations: [V('Side', 'Left ear only')] }, 'L', 'F6: option "Side: Left ear only"'); one({ variations: [V('Ear', 'Left')] }, 'L', 'F6: option "Ear: Left"'); one({ variations: [V('Choose side', 'Right')] }, 'R', 'F6: option "Choose side: Right"');
  eq(row({ title: 'Star Stud Earrings', variations: [V('Side', 'Both ears')] }).spec.pieceCount, 2, 'F6: "Both ears" is a pair');
  eq(row({ title: 'Star Stud Earrings', variations: [V('Left ear', 'Left'), V('Right ear', 'Right')] }).spec.pieceCount, 2, 'F6: a Left option and a Right option together are not a single');
  eq(row({ title: 'Pair of Single Stone Studs' }).spec.pieceCount, 2, 'F6: a title with Pair or plural studs stays a pair');

  // F7 · a line that says two designs but names one waits; a person's answer settles it, for this line only
  const mis = (extra, c) => row(Object.assign({ title: 'Mismatched Tennis Ball and Racket Huggie Hoops', sku: 'A', variations: [V('HOOP SIZE', '8.5mm')] }, extra), c);
  const m0 = mis({}), p0 = m0.spec.problems.find(x => x.pairSecond);
  ok(p0 && p0.kind === 'needsMapping' && p0.optionName === 'Two designs on this line' && p0.optionValue === '4170000001/5200000001', 'F7: the line waits with a pairSecond question, one per line'); ok(/names one/.test(p0.pairSecond.why), 'in one plain line');
  eq(m0.spec.pair.second.answered, '', 'asked, not answered'); eq(counts(m0).pieces, 2, 'meanwhile it counts the pair (2)');
  const same = mis({}, ctx({ optionMaps: { '1718': { 'two designs on this line': { '4170000001/5200000001': { field: 'ignore', value: null } } } } }));
  eq([same.spec.problems.some(x => x.pairSecond), same.spec.pair.second.answered, counts(same).pieces, counts(same).sides], [false, 'same', 2, 'LR'], 'F7: answered "the same on both ears": nothing left to ask, a matching pair');
  const named = mis({}, ctx({ optionMaps: { '1718': { 'two designs on this line': { '4170000001/5200000001': { field: 'design', value: 'B' } } } } }));
  eq([named.spec.problems.some(x => x.pairSecond), named.spec.pair.mismatched, named.spec.pair.source, named.spec.pair.members.map(m => m.side + m.sku).join(','), counts(named).sides], [false, true, 'answer', 'LA,RB', 'LR'], 'F7: a second design named by a person: a mismatched pair, Left A and Right B');
  const otherLine = mis({ transactionId: '5200000002' }, ctx({ optionMaps: { '1718': { 'two designs on this line': { '4170000001/5200000001': { field: 'ignore', value: null } } } } }));
  ok(otherLine.spec.problems.some(x => x.pairSecond), 'F7: the answer is for that line only');
  const z = row({ title: 'Zodiac REVAMP Stud Earrings', sku: 'A', quantity: 2, variations: [V('Options', 'Silver • 2 symbols')], personalization: ['balance et lion'] });
  ok(z.spec.problems.some(x => x.pairSecond) && counts(z).pieces === 4, 'F7: "2 symbols" on a stud line names one SKU: it waits (4 pieces when answered)');
  ok(!row({ title: 'Zodiac Symbol Necklace', sku: 'A', variations: [V('Options', 'Silver • 2 symbols')] }).spec.problems.some(x => x.pairSecond), 'F7: a necklace is never asked about a second ear');
  ok(!mis({ sku: 'MITTENS 1 + MITTENS 2' }).spec.problems.some(x => x.pairSecond), 'F7: two named master designs are a mismatched pair already: nothing to ask');
  eq(lineMismatchedAll(), [false, false, true, true], 'lineMismatched: a necklace SKU with a plus, "w/" in an earring SKU: not mismatched; two SKUs on an earring line and the word Mismatched are');
  function lineMismatchedAll() { return [O.lineMismatched(mk({ title: 'Cat Add On Charm', sku: 'CAT(+FISH) - Cat Only' })), O.lineMismatched(mk({ title: 'Triceratops Stud Earrings', sku: 'Cute Triceratops w/ hearts' })), O.lineMismatched(mk({ title: 'Stud Earrings', sku: 'MITTENS 1 + MITTENS 2' })), O.lineMismatched(mk({ title: 'Mismatched Stud Earrings', sku: 'A' }))]; }

  { const lr = row({ title: 'Mix Match Earrings', sku: 'A', variations: [V('Left Ear Charm', 'A'), V('Right Ear Charm', 'B')] });
    eq([lr.spec.pair.mismatched, lr.spec.pair.source, lr.spec.problems.length, counts(lr).sides], [true, 'options', 0, 'LR'], 'F7: Left and Right options that name two master designs are a mismatched pair: nothing is asked about them');
    const ln = row({ title: 'Mix Match Earrings', sku: 'A', variations: [V('Left Ear Charm', 'Heart'), V('Right Ear Charm', 'Cat')] });
    ok(ln.spec.problems.some(x => x.pairSecond) && !ln.spec.pair.mismatched, 'F7: the same options naming designs the master does not have: the line waits for the second design'); }
  // the run's line record carries the piece count (ADVLIFE): Readiness reads a pair of quantity 1 as 2
  { const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'); const lr = src.slice(src.indexOf('  function lineRecord(row) {'), src.indexOf('  function lineRecord(row) {') + 3000);
    ok(/pieceCount: row\.spec \? \(row\.spec\.pieceCount \|\| row\.spec\.quantity\) : 1/.test(lr), 'lineRecord stores pieceCount beside quantity'); }
}

// ── 8 · the server: a count answer and the old-record guard (the real handler over the in-memory Firestore) ──────────────────────────────
(async () => {
  const { start } = require('./bridge-server.cjs');
  const srv = await start(); const st = srv.st; st.atomic = true;
  const call = async body => { const r = await fetch(`${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, body: await r.json() }; };
  const poolRow = (rid, tid, copy, qty, extra) => Object.assign({ poolId: `${rid}_${tid}_${copy}`, orderId: rid, transactionId: tid, lineKey: `${rid}_${tid}`, sku: 'A', material: 'gold', copy, quantity: qty, state: 'ready', runId: 'run-pi' }, extra || {});
  try {
    // the answer to a count question
    for (const v of ['3', 3, '12']) { const r = await call({ op: 'optionMapPut', listingId: '1718', optionName: 'Letters', optionValue: '3 Letters', map: { field: 'count', value: v }, by: 'Test' }); eq(r.status, 200, 'count ' + v + ': ' + JSON.stringify(r.body)); ok(!r.body.error, 'no error'); }
    const got = (await call({ op: 'optionMapGet' })).body.maps['1718'].letters['3 letters']; eq(got.field, 'count', 'stored as a count'); eq(got.value, '12', 'with its number as text (the last answer)');
    for (const v of ['0', '13', 'x', '', null]) { const r = await call({ op: 'optionMapPut', listingId: '1718', optionName: 'Letters', optionValue: 'five letters', map: { field: 'count', value: v }, by: 'Test' }); ok(r.body.error && /1 to 12/.test(r.body.error), 'count ' + JSON.stringify(v) + ' refused: ' + JSON.stringify(r.body)); }
    eq((await call({ op: 'optionMapPut', listingId: '1718', optionName: 'Size', optionValue: 'Huge', map: { field: 'size', value: 'xl' }, by: 'Test' })).status, 200, 'the other fields work as before');
    // the page reads what the server stored
    const maps = (await call({ op: 'optionMapGet' })).body.maps;
    eq(row({ title: 'Initial Disc Necklace', variations: [V('Letters', '3 Letters')] }, ctx({ optionMaps: maps })).spec.pieceCount, 12, 'the interpretation reads the stored answer');

    // poolPut: a new line writes all its rows; an old line with fewer pieces is left as it was and the answer says so
    const N = ['4170000100', '9100000100'];
    let r = await call({ op: 'poolPut', pools: [poolRow(...N, 1, 2), poolRow(...N, 2, 2)] });
    eq(r.status, 200, JSON.stringify(r.body)); eq(r.body.written, 2, 'a new line writes both pieces'); ok(!r.body.short, 'nothing short');
    eq(st.doc('Charm_Pool', `${N[0]}_${N[1]}_1`).quantity, 2, 'the row keeps the pieces of its line');
    const OLD = ['4170000101', '9100000101'], oldId = copy => `${OLD[0]}_${OLD[1]}_${copy}`;
    st.put('Charm_Pool', oldId(1), poolRow(...OLD, 1, 1, { state: 'written', sheetId: 'sheet-old' }));
    r = await call({ op: 'poolPut', pools: [poolRow(...OLD, 1, 2), poolRow(...OLD, 2, 2)] });
    eq(r.status, 200, JSON.stringify(r.body)); eq(r.body.written, 0, 'an old line with 1 piece is sent 2: nothing is written');
    eq(r.body.short, [{ poolId: oldId(1), had: 1, wants: 2 }], 'the answer says which row is short');
    eq(st.doc('Charm_Pool', oldId(1)).quantity, 1, 'the old row is untouched'); eq(st.doc('Charm_Pool', oldId(1)).sheetId, 'sheet-old', 'still on its sheet'); ok(!st.doc('Charm_Pool', oldId(2)), 'no second row was added');
    // the same line sent as it is: written as before
    r = await call({ op: 'poolPut', pools: [poolRow(...OLD, 1, 1)] }); eq(r.status, 200, JSON.stringify(r.body)); ok(!r.body.short, 'the same count is not short');
    // an old line taken off on purpose (superseded) and made up again at the new count is a new line
    const GONE = ['4170000102', '9100000102'];
    st.put('Charm_Pool', `${GONE[0]}_${GONE[1]}_1`, poolRow(...GONE, 1, 1, { state: 'superseded', sheetId: null }));
    r = await call({ op: 'poolPut', pools: [poolRow(...GONE, 1, 2), poolRow(...GONE, 2, 2)] }); eq(r.body.written, 2, 'a superseded line is made up new, at the new count'); ok(!r.body.short, 'not short');
    // a custom design is never judged
    const CUS = ['4170000103', '9100000103'];
    st.put('Charm_Pool', `${CUS[0]}_${CUS[1]}_1`, poolRow(...CUS, 1, 1, { state: 'ready' }));
    r = await call({ op: 'poolPut', pools: [poolRow(...CUS, 1, 2, { custom: true }), poolRow(...CUS, 2, 2, { custom: true })] }); ok(!r.body.short, 'custom pieces are not judged');
    for (const [k, v] of st.docs) noNested(v, k);
    n++;
  } finally { srv.close(); }
  console.log(`pairs-intake: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
