// A necklace whose listing TITLE also says "Earrings" is a necklace (KITEPAIR; Paul, 13 Oct 2026, order 4199929230, the kite necklace, SKU KITE).
// Paul's standing rules (9-12 Oct): necklaces are NEVER mirrored; a listing that is not earrings never gets duplicate charms; only an earring pair is a Left and a Right.
// Offline, no Etsy, no paid call, nothing live, nothing written anywhere but memory.   node tests/charm-nest/pairs-kite-necklace.cjs
//
// The bug (main until this fix): the line's title is "Kite Necklace Kite Pendant ... Kite Shape Earrings Silver Kite Necklace Birthday Gift", the SKU is the generic "KITE", its options are
// Metal Choice "Silver + Engrave" and Necklace Length 20". lineForm (the form guard) read "necklace" from the Necklace Length option, but lineSignals (EARWORDS) read the strong word "Earrings" in the
// TITLE as "sold as a pair" and pairInfo, which only looks at a form an option NAMED (spec.form; a length is a chain, not a form), took that: spec.pair.earring true, two pieces, sides L / R,
// the Right mirrored, 1 pair / Left and Right in the order window, a Left and a Right engraving job. Nothing about the SKU matters: "KITE" and the master's own "BEADY KITE" read the same.
// What this proves (the real charm-nest-orders.js, charm-nest-pair.js, charm-nest-pair-labels.js, charm-nest-engrave-sides.js on the shared fixtures):
//   1  the kite line (either SKU, quantity 1 and 2): no pair, no side, no mirror, one piece per unit (quantity 2 = two identical copies), nothing to ask, the form guard and the pair reading agree;
//      every reader that decides from the intake's facts agrees (pair module, sticker, engraving slots)
//   2  other shapes of the same mistake (a stud / huggie / hoop word in a necklace title, a two-body design, a MISMATCHED_ name): still a necklace; a Necklace Length option beats a stray ear word in the TITLE only
//   3  real earring listings read exactly as before (stud, huggie hoops, "Earring Style", a Pair option, ZODIAC_EARRINGS, LETTER_EARRING, a single earring, a necklace-and-earrings title with NO necklace
//      option, an option that itself says earrings next to a Necklace Length): a pair, Left and Right, the Right mirrored, never engraved where the rule says so
//   4  the real in-repo order corpora (the Sep 17 snapshot and the audits): every line with a Necklace Length option is no earring pair, and no line that reads as earrings has one
const assert = require('assert');
const fs = require('fs'), path = require('path');
const F = require('./pairs-fixtures.cjs');
const O = require('../../charm-nest-orders.js');
const CP = require('../../charm-nest-pair.js');
const PL = require('../../charm-nest-pair-labels.js');
const Sides = require('../../charm-nest-engrave-sides.js');
const noNested = require('./_noNestedArrays.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };

const order = { receiptId: '4170000001', updateTs: 1789100000 };
const V = (name, value) => ({ name, value });
// the master as the live one holds the kite: one body (a plain charm), under its own SKU and under the generic one; plus the shared two-body fixtures
const MASTER = {
  'KITE': F.entryOf('ONE-PENDANT'), 'BEADY KITE': F.entryOf('ONE-PENDANT'), 'MITTENS-MIS': F.entryOf('MITTENS-MIS'), 'TENNIS-MIS': F.entryOf('TENNIS-MIS'), 'PAIR-FACE-L': F.entryOf('PAIR-FACE-L'),
  'ZODIAC_EARRINGS-3': F.entryOf('PAIR-FACE-L'), 'LETTER_EARRING-19': F.entryOf('PAIR-FACE-L'), 'MISMATCHED_7134': {}
};
const ctx = () => ({ optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: s => MASTER[s] || null });
const KITE_TITLE = 'Kite Necklace Kite Pendant Gift for Her Kite Jewelry Kite Charm Kite Gift Kite Shape Earrings Silver Kite Necklace Birthday Gift';
const kite = over => Object.assign({ transactionId: '5200000001', listingId: '1759609265', sku: 'KITE', title: KITE_TITLE, quantity: 1, metalKey: 'silver', metalLabel: 'Silver + Engrave', personalization: ['Name One'],
  variations: [V('Metal Choice', 'Silver + Engrave'), V('Necklace Length', '20"'), V('Personalization', 'Name One')] }, over);
const read = line => { const spec = O.interpretLine(order, line, ctx()); return { order, line, spec, poolIds: [] }; };
const lineFor = r => Object.assign({ receiptId: order.receiptId }, r.line, { quantity: r.spec.quantity, form: r.spec.form, spec: r.spec });
const sides = r => O.piecesOf(r).map(p => (p.side || '-') + p.bodyIndex).join(' ');
const asks = r => r.spec.problems.filter(p => p.kind !== 'needsMaterial').map(p => p.kind);
const facts = r => { const p = r.spec.pair; return [p.earring, p.single, p.mismatched, p.glued]; };

// ── 1 · the kite line itself ─────────────────────────────────────────────────────────────────────────────────────────────────────────
for (const sku of ['KITE', 'BEADY KITE']) for (const q of [1, 2]) {
  const r = read(kite({ sku, quantity: q })), p = r.spec.pair, lc = lineFor(r), tag = `${sku} q${q}`, none = Array(q).fill(null);
  eq(facts(r), [false, false, false, false], tag + ': not an earring pair, not a single earring, not mismatched');
  eq([r.spec.pieceCount, p.perUnit, p.kind], [q, 1, q === 1 ? 'single' : 'multi'], tag + ': one piece per unit (quantity 2 is two identical copies, never a Left and a Right)');
  eq(p.sides, none, tag + ': no side'); eq(sides(r), Array(q).fill('-0').join(' '), tag + ': piecesOf: no side, the whole design');
  eq([r.spec.lineForm, r.spec.form], ['necklace', null], tag + ': the form guard reads a necklace, as it always did');
  eq(asks(r), [], tag + ': nothing to ask a person'); eq(p.asks, [], tag + ': no pair question');
  ok(r.spec.engraveCandidate && !r.spec.engraveNever, tag + ': the necklace keeps its engraving (the buyer wrote a word for the back); nothing says it is an earring');
  eq([CP.isEarringPair(lc, null), CP.kindOf(lc, null), CP.isGroupLine(lc, null)], [false, q === 1 ? 'single' : 'multi', false], tag + ': the pair module agrees');
  eq(CP.piecesFor(lc, null).map(x => [x.side, x.mirror]), none.map(() => [null, false]), tag + ': no piece is mirrored');
  eq(PL.labelPieces(r, MASTER[sku]), null, tag + ': the sticker is the order\'s one sticker, never a LEFT and a RIGHT page');
  const ids = Array.from({ length: q }, (_, i) => `4170000001_5200000001_${i + 1}`), rowOf = { key: '4170000001_5200000001', poolIds: ids, order: { receiptId: order.receiptId }, line: r.line, spec: Object.assign({}, r.spec, { designSku: sku }) };
  const ectx = { poolRow: id => ({ poolId: id }), charmOf: id => null, entryFor: s => MASTER[s] || null, pair: CP };
  // (PIECES, Paul 16 Oct 2026, charm-nest-engrave-sides.js header: a line of quantity N >= 2 is cut into its units, a unit with no ear or disc slot is "C<u>", each copy its own engraving;
  //  quantity 1 stays whole. Either way never a Left and a Right job.)
  const copies = q === 1 ? [false, none] : [true, ids.map((_, i) => 'C' + (i + 1))];
  eq([Sides.plan(ectx, rowOf, []).split, ids.map(id => Sides.slotOfId(ectx, rowOf, id))], copies, tag + ': engraving: not split into a Left job and a Right job (one job per copy)');
  noNested(r.spec, tag);
}
// the generic SKU and the master's own read the same line the same way, whatever else the line says
{
  const a = read(kite({ sku: 'KITE' })), b = read(kite({ sku: 'BEADY KITE' }));
  eq([a.spec.pair.earring, a.spec.pieceCount, a.spec.pair.kind, a.spec.pair.sides], [b.spec.pair.earring, b.spec.pieceCount, b.spec.pair.kind, b.spec.pair.sides], 'KITE and BEADY KITE: the same reading');
}

// ── 2 · the same mistake in other shapes ─────────────────────────────────────────────────────────────────────────────────────────────
const NECK_OPTS = [V('Metal Choice', 'Gold'), V('Necklace Length in inches', '16"')];
const NECKLACES = [
  ['stud word in a necklace title', { title: 'Cute Animal Necklace Pendant Stud Earrings Gold Gift', variations: NECK_OPTS }],
  ['huggie hoop words in a necklace title', { title: 'Cute Animal Huggie Hoop Earrings Charm Necklace', variations: NECK_OPTS }],
  ['a plain Earrings word, quantity 2', { title: 'Cute Animal Charm Necklace Gift Earrings', variations: NECK_OPTS, quantity: 2 }],
  ['necklace length in inches (centimetres)', { title: 'Cute Animal Earrings Necklace', variations: [V('Metal Choice', 'Silver'), V('Necklace Length', '45 cm')] }],
  ['Single Earring words in the title', { title: 'Cute Animal Single Earring Necklace', variations: NECK_OPTS }]
];
for (const [name, over] of NECKLACES) {
  for (const sku of ['KITE', 'BEADY KITE']) {
    const r = read(kite(Object.assign({ sku }, over))), q = over.quantity || 1, tag = `${sku} · ${name}`;
    eq(facts(r), [false, false, false, false], tag + ': a necklace');
    eq([r.spec.pieceCount, sides(r)], [q, Array(q).fill('-0').join(' ')], tag + ': one piece per unit, no side');
    eq(asks(r), [], tag + ': nothing to ask');
  }
}
// a design that draws two bodies, on the kite-shaped necklace line, is ONE piece with no side (never split into ears: Paul, 10 Oct), as every other necklace
for (const sku of ['MITTENS-MIS', 'TENNIS-MIS']) {
  const r = read(kite({ sku })), p = r.spec.pair, charm = F.charmOf(sku), lc = lineFor(r);
  eq([p.earring, p.mismatched, r.spec.pieceCount, sides(r)], [false, false, 1, '-0'], sku + ': a two-body design on the kite necklace line is one piece');
  ok(p.notes.some(x => /two bodies/.test(x) && /not an earring pair/.test(x)), sku + ': a plain note says what is made');
  eq([CP.isEarringPair(lc, charm), CP.piecesFor(lc, charm).map(x => x.side + ':' + x.mirror + ':' + x.bodyIndex).join(' ')], [false, 'null:false:0'], sku + ': the pair module: whole design, no side, no mirror');
}
// the shop's own MISMATCHED_ name on a line that bought a necklace length is a necklace too (the name makes a pair only when nothing else on the line says otherwise)
{
  const r = read(kite({ sku: 'MISMATCHED_7134', title: 'Mittens Custom Charms', variations: NECK_OPTS }));
  eq([r.spec.pair.earring, r.spec.pair.mismatched, r.spec.pieceCount], [false, false, 1], 'MISMATCHED_7134 with a Necklace Length: one piece');
}
// a Necklace Length beats the title's ear words ONLY: the buyer's own option words still say what they say (a "Stud earrings" style next to a length is the buyer's word: a pair, as before)
{
  const r = read(kite({ sku: 'PAIR-FACE-L', title: 'Cute Animal Charm Jewelry', variations: [V('Metal Choice', 'Gold'), V('Style', 'Stud earrings'), V('Necklace Length', '16"')] }));
  eq([r.spec.pair.earring, r.spec.pieceCount, sides(r)], [true, 2, 'L0 R0'], 'an option that itself says earrings stays a pair, whatever else is on the line (unchanged)');
}

// ── 3 · real earring listings: exactly as before ─────────────────────────────────────────────────────────────────────────────────────
const EARRINGS = [
  ['stud earrings', 'PAIR-FACE-L', { title: 'Cute Animal Charm Stud Earrings', variations: [V('Metal Choice', 'Gold')] }],
  ['huggie hoops', 'PAIR-FACE-L', { title: 'Cute Animal Huggie Hoop Earrings Hinged Hoops', variations: [V('Metal Choice', 'Gold'), V('HOOP SIZE', '8.5mm')] }],
  ['Earring Style option', 'PAIR-FACE-L', { title: 'Cute Animal Charm Jewelry', variations: [V('Metal Choice', 'Gold'), V('Earring Style', 'Huggie')] }],
  ['Pair of earrings option', 'PAIR-FACE-L', { title: 'Cute Animal Charm Jewelry', variations: [V('Metal Choice', 'Gold'), V('Type', 'Pair of earrings')] }],
  ['earrings, words only', 'PAIR-FACE-L', { title: 'Cute Animal Earrings', variations: [V('Metal Choice', 'Gold')] }],
  ['ZODIAC_EARRINGS', 'ZODIAC_EARRINGS-3', { title: 'Cancer Zodiac Stud Earrings Astrological Symbol Earrings', variations: [V('Metal Choice', 'Gold'), V('Zodiac Sign', 'Cancer')] }],
  ['LETTER_EARRING', 'LETTER_EARRING-19', { title: 'Initial Letter Stud Earrings Personalized Letter Earrings', variations: [V('Metal Choice', 'Gold'), V('Letter', 'T')] }],
  ['title names a necklace too and NO necklace option (nothing on the line settles it: as before)', 'PAIR-FACE-L', { title: 'Cute Animal Necklace and Earrings Set Gold Gift', variations: [V('Metal Choice', 'Gold')] }],
  ['earrings title, a length that is no necklace length (Hoop Size, Drop Length)', 'PAIR-FACE-L', { title: 'Cute Animal Dangle Earrings Gold', variations: [V('Metal Choice', 'Gold'), V('Drop Length', '2"')] }]
];
for (const [name, sku, over] of EARRINGS) for (const q of [1, 2]) {
  const r = read(kite(Object.assign({ sku, quantity: q, personalization: [] }, over))), tag = `${name} · q${q}`;
  eq([r.spec.pair.earring, r.spec.pair.single, r.spec.pieceCount, sides(r)], [true, false, 2 * q, Array.from({ length: q }, () => 'L0 R0').join(' ')], tag + ': a pair of earrings, a Left and a Right per unit');
  eq(CP.piecesFor(lineFor(r), F.charmOf(sku === 'PAIR-FACE-L' ? 'PAIR-FACE-L' : 'PAIR-FACE-L')).map(x => x.side + (x.mirror ? 'm' : '.')).join(' '), Array.from({ length: q }, () => 'L. Rm').join(' '), tag + ': the Right is the mirror of the Left');
}
// the earring rule that no earring is engraved is untouched, and so are single earrings
{
  const z = read(kite({ sku: 'ZODIAC_EARRINGS-3', title: 'Cancer Zodiac Huggie Hoop Earrings', variations: [V('Metal Choice', 'Gold'), V('Zodiac Sign', 'Cancer')], personalization: ['Cancer'] }));
  ok(z.spec.engraveNever && !z.spec.engraveCandidate && z.spec.pair.earring, 'zodiac huggie hoops: still a pair, never engraved');
  const s = read(kite({ sku: 'PAIR-FACE-L', title: 'Custom Single Replacement Cat Huggie Earring Left Ear', variations: [V('Metal Choice', 'Gold')], personalization: [] }));
  eq([s.spec.pair.earring, s.spec.pair.single, s.spec.pieceCount, sides(s)], [false, true, 1, 'L0'], 'a single earring naming its ear: unchanged');
}

// ── 4 · the real corpora in the repo ──────────────────────────────────────────────────────────────────────────────────────────────────
{
  const FX = path.join(__dirname, 'fixtures'), lines = [];
  const add = (title, sku, vars, q) => lines.push({ title: String(title || ''), sku: String(sku || ''), vars: (vars || []).map(v => Array.isArray(v) ? { name: v[0], value: v[1] } : { name: v.name || v.formatted_name, value: v.value || v.formatted_value }), q: q || 1 });
  const json = f => JSON.parse(fs.readFileSync(f, 'utf8'));
  for (const l of json(path.join(FX, 'addon-snapshot-lines.json')).lines) add(l.title, l.sku, l.vars, l.q);
  for (const l of json(path.join(FX, 'adv-count-lines.json')).lines) add(l.title, l.sku, l.vars, l.q);
  for (const l of json(path.join(__dirname, 'options-link-data.json')).lines) add(l.title, l.sku, l.variations, l.quantity);
  const seen = new Set(); let withLength = 0, ears = 0;
  for (const l of lines) {
    const k = l.title + '|' + l.sku + '|' + JSON.stringify(l.vars); if (seen.has(k)) continue; seen.add(k);
    const line = { transactionId: '1', listingId: '1', sku: l.sku, title: l.title, quantity: l.q, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: l.vars };
    const spec = O.interpretLine(order, line, { optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: null }), neck = l.vars.some(v => /necklace/i.test(v.name) && /length/i.test(v.name));
    if (neck) { withLength++; ok(!spec.pair.earring && !spec.pair.single, `real line "${l.title.slice(0, 60)}" (${l.sku}) has a Necklace Length option and reads as no earring`); }
    if (spec.pair.earring) { ears++; ok(!neck, `real earring line "${l.title.slice(0, 60)}" has no Necklace Length option`); }
  }
  ok(withLength >= 100 && ears >= 50, `the corpora are what they were (${withLength} lines with a Necklace Length, ${ears} earring-pair lines)`);
}
console.log(`pairs-kite-necklace: ${n} checks passed`);
