// A necklace is never a Left and a Right (NECKFIX; Paul, 9 Oct 2026 18:46/18:47 and 10 Oct: "only EARRING pairs are Left and Right, the Right the exact mirror
// of the Left; necklaces, pendants and singles are never mirrored"). Offline, no Etsy, no paid call, nothing live.
//   node tests/charm-nest/pairs-neck.cjs
// The bug (MIRRORCATALOG, Sep 17 orders): pairInfo took ANY design that draws two bodies for an earring line unless the line was sold as "single", so the 5 Wolf
// NECKLACE lines (SKU Wolf Charm / WOLF, whose file holds the wolf and the moon) were made as a Left and a Right with the moon mirrored. The same held for all
// 42 two-body SKUs (26 files) sold as a necklace, pendant, charm only, bracelet or key ring.
// What this proves (the real charm-nest-orders.js, charm-nest-pair.js, charm-nest-pair-labels.js, charm-nest-engrave-sides.js, charm-nest-pair-remove.js and
// netlify/functions/_stationLive.js, on the shared two-body fixtures):
//   1  the five real Sep 17 Wolf lines: ONE piece per unit, no side, no mirror, kind single, nothing to ask; every reader agrees (orders, pair module, stickers, engraving)
//   2  a two-body design sold as a necklace, pendant, charm only, bracelet, key ring or a plain charm (quantity 1 and 2) is no pair; sold as earrings (stud, huggie
//      hoops, earrings, a Pair option, a Huggie CHARM SET with its one-body twin) it is a Left and a Right of its two bodies, as before
//   3  the twin readers that guessed a pair from the DESIGN alone: the pair module with no intake facts, the sticker, the engraving slots, the station console, the removal wording
//   4  lines that must not change (a matching earring pair, a mismatched earring pair, a single earring naming its ear, a counted-option necklace, a plain
//      quantity-N line) are exactly what they were before this fix
const assert = require('assert');
const fs = require('fs'), path = require('path'), vm = require('vm');
const F = require('./pairs-fixtures.cjs');
const O = require('../../charm-nest-orders.js');
const CP = require('../../charm-nest-pair.js');
const PL = require('../../charm-nest-pair-labels.js');
const Sides = require('../../charm-nest-engrave-sides.js');
const Remove = (() => { const g = globalThis; g.window = g.window || g; try { return require('../../charm-nest-pair-remove.js'); } catch (_) { return null; } })();
const Live = require('../../netlify/functions/_stationLive.js');
const noNested = require('./_noNestedArrays.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };

const order = { receiptId: '4170000001', updateTs: 1789100000 };
const V = (name, value) => ({ name, value });
const TWO = { v: 1, bodies: 2, mismatched: true };
// the catalogue as the live index holds it after the 9 Oct re-index: the Wolf family and a mismatched mitten pair are two bodies in one file, a huggie twin is one body
const MASTER = {
  'WOLF CHARM': { pair: TWO }, 'WOLF': { pair: TWO }, 'WOLF (HUGGIE)': {}, 'WOLF + MOON': { pair: TWO },
  'MITTENS-MIS': F.entryOf('MITTENS-MIS'), 'TENNIS-MIS': F.entryOf('TENNIS-MIS'), 'TENNIS-MIS (HUGGIE)': {},
  'MITTENS 1': {}, 'MITTENS 2': {}, 'ONE-PENDANT': F.entryOf('ONE-PENDANT'), 'PAIR-FACE-L': F.entryOf('PAIR-FACE-L'), 'DISC-14': F.entryOf('DISC-14'), 'MISMATCHED_7134': {}
};
const ctx = () => ({ optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: s => MASTER[s] || null });
const mk = over => Object.assign({ transactionId: '5200000001', listingId: '1718', sku: 'ONE-PENDANT', title: 'Plain Charm Necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: [V('Metal Choice', 'Gold')] }, over);
const read = line => { const spec = O.interpretLine(order, line, ctx()); return { order, line, spec, poolIds: [] }; };
const lineFor = r => Object.assign({ receiptId: order.receiptId }, r.line, { quantity: r.spec.quantity, form: r.spec.form, spec: r.spec });
const sides = r => O.piecesOf(r).map(p => (p.side || '-') + p.bodyIndex).join(' ');
const asks = r => r.spec.problems.filter(p => p.kind !== 'needsMaterial').map(p => p.kind);
const wolf = F.charmOf('MITTENS-MIS');   // (a charm CharmNestPair.bodiesOf reads as two different bodies: what the stored two-body Wolf file reads as)
ok(CP.isMismatched(wolf) && CP.bodiesOf(wolf).length === 2, 'the fixture design draws two bodies');

// ── 1 · the five real Sep 17 lines (titles and options as the snapshot holds them) ───────────────────────────────────────────────
const WOLF_TITLE = 'Wolf Necklace Gold Wolf Pendant Charm Jewelry Gift for Her Women’s Animal Jewelry Wolf Birthday Gift for Wild Women';
const FIVE = [
  ['4175825267', mk({ sku: 'Wolf Charm', title: WOLF_TITLE, variations: [V('Metal Choice', 'Silver'), V('Necklace Length in inches', '14"'), V('Personalization', 'EJK')], personalization: ['EJK'] })],
  ['4174402679', mk({ sku: 'Wolf Charm', title: WOLF_TITLE, variations: [V('Metal Choice', 'Silver + Engrave'), V('Necklace Length in inches', '14"'), V('Personalization', 'Livvy')], personalization: ['Livvy'] })],
  ['4174365103', mk({ sku: 'Wolf Charm', title: 'Wolf Necklace Charm Necklace Gold Wolf Pendant Gift for Her Wolf Charm Necklace Women&#39;s Wolf Jewelry Birthday', variations: [V('Metal Choice', 'Gold'), V('Necklace Length in inches', '16"')] })],
  ['4174861588', mk({ sku: 'Wolf Charm', title: 'Wolf Necklace Charm Necklace Gold Wolf Pendant Gift for Her Wolf Charm Necklace Women&#39;s Wolf Jewelry Birthday', variations: [V('Metal Choice', 'Gold'), V('Necklace Length in inches', '14"')] })],
  ['4171147527', mk({ sku: 'WOLF', title: 'Wolf Charm Charm Add On Charm Gold Wolf Pendant Wolf Jewelry Women’s Wolf Charm Birthday Gift Animal Jewelry', variations: [V('Metal Choice', '14K SOLID GOLD'), V('Charm Type', 'Necklace CHARM')] })]
];
for (const [rid, line] of FIVE) {
  const r = read(line), p = r.spec.pair, tag = 'receipt ' + rid;
  ok(MASTER[r.spec.designSku] && MASTER[r.spec.designSku].pair, tag + ': the design is the two-body Wolf');
  eq([p.earring, p.mismatched, p.single, p.glued, p.twoBodies], [false, false, false, false, true], tag + ': not an earring pair; the line is not a mismatched pair; it says the design draws two bodies');
  eq([r.spec.pieceCount, p.perUnit, p.kind], [1, 1, 'single'], tag + ': ONE piece, kind single');
  eq(p.sides, [null], tag + ': no side'); eq(sides(r), '-0', tag + ': piecesOf: no side, the whole design');
  eq(asks(r), [], tag + ': nothing to ask a person');
  ok(p.notes.some(x => /two bodies/.test(x) && /not an earring pair/.test(x)), tag + ': a plain note says what is made');
  const lc = lineFor(r);
  eq([CP.isEarringPair(lc, wolf), CP.kindOf(lc, wolf), CP.isGroupLine(lc, wolf)], [false, 'single', false], tag + ': the pair module agrees (with the design charm)');
  eq(CP.piecesFor(lc, wolf).map(x => [x.side, x.bodyIndex, x.mirror, x.of]), [[null, 0, false, 1]], tag + ': one piece, no side, body 0, never mirrored');
  eq(CP.piecesFor(lc, null).map(x => x.side), [null], tag + ': and without the charm');
  eq(PL.labelPieces(r, MASTER[r.spec.designSku]), null, tag + ': the sticker is the order\'s one sticker, never a LEFT and a RIGHT page');
  noNested(r.spec, tag);
}
// quantity 2 of the same necklace: two independent copies (a plain quantity-N line), no side, no group
{
  const r = read(Object.assign({}, FIVE[0][1], { quantity: 2 })), lc = lineFor(r);
  eq([r.spec.pieceCount, r.spec.pair.kind, sides(r)], [2, 'multi', '-0 -0'], 'quantity 2: two copies, no side');
  eq([CP.isGroupLine(lc, wolf), CP.piecesFor(lc, wolf).map(x => x.side + ':' + x.mirror)], [false, ['null:false', 'null:false']], 'not a group; neither copy is mirrored');
  eq(PL.labelPieces(r, MASTER['WOLF CHARM']), null, 'one order sticker');
}

// ── 2 · every kind of line on a two-body design ──────────────────────────────────────────────────────────────────────────────────
const NOT_PAIR = [
  ['necklace pendant title', { title: 'Cute Animal Necklace Gold Pendant Charm Jewelry Gift for Her', variations: [V('Metal Choice', 'Gold'), V('Necklace Length in inches', '16"')] }],
  ['form option Necklace CHARM', { title: 'Cute Animal Charm Add On Charm', variations: [V('Metal Choice', 'Gold'), V('Charm Type', 'Necklace CHARM')] }],
  ['pendant, no option', { title: 'Cute Animal Pendant Jewelry', variations: [V('Metal Choice', 'Gold')] }],
  ['charm only option', { title: 'Cute Animal Charm', variations: [V('Metal Choice', 'Gold'), V('Charm Type', 'Charm only')] }],
  ['bracelet', { title: 'Cute Animal Charm Bracelet', variations: [V('Metal Choice', 'Gold')] }],
  ['key ring', { title: 'Cute Animal Keychain Key Ring', variations: [V('Metal Choice', 'Gold')] }],
  ['hoop only in a necklace title', { title: 'Cute Animal Gold Hoop Charm Necklace Pendant', variations: [V('Metal Choice', 'Gold')] }],
  ['plain charm, no product word', { title: 'Cute Animal Charm', variations: [V('Metal Choice', 'Gold')] }],
  ['earring title, form necklace', { title: 'Cute Animal Earrings or Necklace', variations: [V('Metal Choice', 'Gold'), V('Charm Type', 'Necklace CHARM')] }]
];
const IS_PAIR = [
  ['stud earrings', { title: 'Cute Animal Charm Stud Earrings', variations: [V('Metal Choice', 'Gold')] }],
  ['huggie hoops', { title: 'Cute Animal Huggie Hoop Earrings Hinged Hoops', variations: [V('Metal Choice', 'Gold'), V('HOOP SIZE', '8.5mm')] }],
  ['earrings (words only)', { title: 'Cute Animal Earrings', variations: [V('Metal Choice', 'Gold')] }],
  ['pair-of-earrings option', { title: 'Cute Animal Charm Jewelry', variations: [V('Metal Choice', 'Gold'), V('Type', 'Pair of earrings')] }]
];
for (const sku of ['WOLF CHARM', 'WOLF', 'MITTENS-MIS', 'TENNIS-MIS', 'WOLF + MOON']) {
  const charm = F.charmOf(MASTER[sku].pair ? 'MITTENS-MIS' : 'ONE-PENDANT');
  for (const q of [1, 2]) {
    for (const [name, over] of NOT_PAIR) {
      const r = read(mk(Object.assign({ sku, quantity: q }, over))), p = r.spec.pair, lc = lineFor(r), tag = `${sku} · ${name} · q${q}`;
      eq([p.earring, p.mismatched, r.spec.pieceCount, sides(r)], [false, false, q, Array(q).fill('-0').join(' ')], tag + ': no pair, q pieces, no side');
      eq([CP.isEarringPair(lc, charm), CP.piecesFor(lc, charm).map(x => x.side + ':' + x.mirror + ':' + x.bodyIndex).join(' ')], [false, Array(q).fill('null:false:0').join(' ')], tag + ': the pair module: whole design, no side, no mirror');
      eq(asks(r), [], tag + ': nothing to ask');
    }
    for (const [name, over] of IS_PAIR) {
      const r = read(mk(Object.assign({ sku, quantity: q }, over))), p = r.spec.pair, tag = `${sku} · ${name} · q${q}`;
      eq([p.earring, p.mismatched, r.spec.pieceCount], [true, true, 2 * q], tag + ': a mismatched earring pair, two pieces per unit');
      eq(sides(r), Array.from({ length: q }, () => 'L0 R1').join(' '), tag + ': the Left is the left body, the Right the right body');
      eq(CP.piecesFor(lineFor(r), charm).map(x => x.side + x.bodyIndex).join(' '), Array.from({ length: q }, () => 'L0 R1').join(' '), tag + ': and the pair module');
    }
  }
}
// a Huggie CHARM SET reads the design's own one-body huggie twin: a matching pair (Left and Right of one body)
{
  const r = read(mk({ sku: 'TENNIS-MIS', title: 'Cute Animal Pendant Add On Charm', variations: [V('Metal Choice', 'Gold'), V('Charm Type', 'Huggie CHARM SET')] }));
  eq([r.spec.designSku, r.spec.pair.earring, r.spec.pieceCount, sides(r), r.spec.pair.twoBodies], ['TENNIS-MIS (HUGGIE)', true, 2, 'L0 R0', undefined], 'Huggie CHARM SET: the one-body huggie twin, a Left and a Right');
}
// the shop's own MISMATCHED_ names still make a pair when the line says nothing else; another product or form makes it a plain piece
{
  const mis = read(mk({ sku: 'MISMATCHED_7134', title: 'Mittens Custom Charms', variations: [V('Metal Choice', 'Gold')] }));
  eq([mis.spec.pair.earring, mis.spec.pair.mismatched, mis.spec.pieceCount], [true, true, 2], 'a SKU named MISMATCHED_7134 on a line that names no other product is a mismatched pair (unchanged)');
  const neck = read(mk({ sku: 'MISMATCHED_7134', title: 'Mittens Necklace Pendant', variations: [V('Metal Choice', 'Gold')] }));
  eq([neck.spec.pair.earring, neck.spec.pair.mismatched, neck.spec.pieceCount], [false, false, 1], 'but sold as a necklace it is one piece');
  const form = read(mk({ sku: 'MISMATCHED_7134', title: 'Mittens Custom Charms', variations: [V('Metal Choice', 'Gold'), V('Charm Type', 'Charm only')] }));
  eq([form.spec.pair.earring, form.spec.pieceCount], [false, 1], 'or with the form Charm only');
}
// two designs the line itself names (a SKU "A + B"): a pair on an earring line (unchanged), one piece on a necklace line
{
  const ear = read(mk({ sku: 'MITTENS 1 + MITTENS 2', title: 'Mittens Mismatched Studs' })), neck = read(mk({ sku: 'MITTENS 1 + MITTENS 2', title: 'Mittens Necklace Pendant' }));
  eq([ear.spec.pair.earring, ear.spec.pair.mismatched, ear.spec.pair.source, ear.spec.pieceCount, sides(ear)], [true, true, 'skus', 2, 'L0 R1'], 'two named designs on an earring line: a mismatched pair, as before');
  eq([neck.spec.pair.earring, neck.spec.pair.mismatched, neck.spec.pair.twoNamed, neck.spec.pair.twoBodies, neck.spec.pieceCount, sides(neck)], [false, false, true, undefined, 1, '-0'], 'two named designs on a necklace line: one piece, no side, said in a plain note');
  ok(neck.spec.pair.notes.some(x => /names two designs but is not an earring pair/.test(x)), 'the note');
}
// a single earring of a two-body design is unchanged: one ear, its side only when the line names it
{
  const named = read(mk({ sku: 'MITTENS-MIS', title: 'Mittens Single Stud Earring Right Ear' }));
  eq([named.spec.pair.single, named.spec.pair.earring, named.spec.pair.mismatched, named.spec.pair.twoBodies, sides(named)], [true, false, true, undefined, 'R0'], 'a single earring that names its ear keeps its side');
}

// ── 3 · the readers that used to guess a pair from the design alone ────────────────────────────────────────────────────────────
{
  // the pair module with no intake facts at all (a raw Etsy line): its own form or title says it is no earring line
  eq([CP.isEarringPair({ form: 'necklace', quantity: 1 }, wolf), CP.isEarringPair({ form: 'charm', quantity: 1 }, wolf), CP.isEarringPair({ title: 'Wolf Necklace Gold Pendant', quantity: 1 }, wolf),
      CP.isEarringPair({ title: 'Wolf Charm Bracelet', variations: [V('Metal Choice', 'Gold')], quantity: 1 }, wolf)], [false, false, false, false], 'raw line: a necklace, charm, pendant or bracelet of a two-body design is no pair');
  eq([CP.isEarringPair({ title: 'Wolf Stud Earrings', quantity: 1 }, wolf), CP.isEarringPair({ title: 'Wolf Necklace or Huggie Hoop Earrings', quantity: 1 }, wolf), CP.isEarringPair({ form: 'huggie', quantity: 1 }, wolf), CP.isEarringPair({ quantity: 1 }, wolf)], [true, true, true, true], 'a line that says earrings (or says nothing) is still a pair of a two-body design');
  eq(CP.piecesFor({ receiptId: '1', transactionId: '2', quantity: 1, form: 'necklace' }, wolf).map(x => [x.side, x.bodyIndex, x.mirror]), [[null, 0, false]], 'raw necklace: one piece, whole design');
  eq(CP.piecesFor({ receiptId: '1', transactionId: '2', quantity: 1, form: 'earrings' }, wolf).map(x => [x.side, x.bodyIndex]), [['L', 0], ['R', 1]], 'raw earrings: Left and Right of the two bodies');
  eq([CP.kindOf({ receiptId: '1', transactionId: '2', quantity: 1, form: 'necklace' }, wolf), CP.kindOf({ receiptId: '1', transactionId: '2', quantity: 1 }, wolf)], ['single', 'mismatched'], 'raw kind: a necklace is single, a bare line of a two-body design is as it was');
  ok(CP.plainLine({ spec: { pair: { earring: false, single: false } } }) && !CP.plainLine({ spec: { pair: { earring: false, single: true } } }) && !CP.plainLine({ spec: { pair: { earring: true } } }) && !CP.plainLine(null), 'plainLine: only a line the intake says is no earring and no single');

  // the sticker: the printed LEFT and RIGHT pages come only from an earring line
  const entry = MASTER['WOLF CHARM'], neck = read(FIVE[0][1]), ear = read(mk({ sku: 'WOLF CHARM', title: 'Wolf Stud Earrings' }));
  eq(PL.labelPieces(neck, entry), null, 'sticker: a necklace of a two-body design prints the order\'s one sticker');
  eq(PL.labelPieces(ear, entry).map(p => p.side), ['L', 'R'], 'sticker: the same design as earrings prints a LEFT and a RIGHT page');
  eq(PL.labelPieces({ quantity: 1 }, entry).map(p => p.side), ['L', 'R'], 'sticker: a row that carries no facts is as it was');
  eq(PL.stickerPieces([neck, ear], r => entry).map(p => p.side), ['L', 'R'], 'a card with a necklace beside an earring pair: only the pair prints ears');

  // the engraving slots: a quantity-2 necklace of a two-body design is two necklaces, not a Left and a Right job
  const q2 = read(Object.assign({}, FIVE[0][1], { quantity: 2 })), qe = read(mk({ sku: 'WOLF CHARM', title: 'Wolf Stud Earrings' }));
  const rowOf = (r, ids) => ({ key: '4170000001_5200000001', poolIds: ids, order: { receiptId: order.receiptId }, line: r.line, spec: Object.assign({}, r.spec, { designSku: 'WOLF CHARM' }) });
  const pool = new Map([['4170000001_5200000001_1', { poolId: '4170000001_5200000001_1' }], ['4170000001_5200000001_2', { poolId: '4170000001_5200000001_2' }]]);
  const ectx = { poolRow: id => pool.get(id), charmOf: id => null, entryFor: sku => MASTER[sku] || null, pair: CP };
  const ids = [...pool.keys()];
  eq([Sides.plan(ectx, rowOf(q2, ids), []).split, ids.map(id => Sides.slotOfId(ectx, rowOf(q2, ids), id))], [false, [null, null]], 'engraving: a quantity-2 necklace of a two-body design is not split into Left and Right jobs');
  eq([Sides.plan(ectx, rowOf(qe, ids), []).slots, ids.map(id => Sides.slotOfId(ectx, rowOf(qe, ids), id))], [['L', 'R'], ['L', 'R']], 'engraving: the same design as earrings, a row with no stored side, is read as it always was');

  // the station console: a piece the page did not split is a Left and a Right only when its line is an earring line
  const M = new Map([['WOLF CHARM', { pair: { bodies: 2, mismatched: true } }]]);
  const card = label => ({ rid: '4170000001', pieces: [{ id: '5200000001-1', label, sku: 'Wolf Charm' }], pieceCount: 1 });
  const expand = label => { const c = card(label); Live.expandPairs(c, M); return c; };
  eq([expand('Wolf Necklace Gold Wolf Pendant Charm Jewelry Gift for Her').pieces.length, expand('Wolf Necklace Gold Wolf Pendant Charm Jewelry Gift for Her').pieceCount], [1, 1], 'console: a Wolf necklace stays one piece');
  eq([expand('Wolf Charm Bracelet').pieces.length, expand('Wolf Key Ring').pieces.length], [1, 1], 'console: a bracelet and a key ring too');
  eq(expand('Wolf Stud Earrings').pieces.map(p => p.side), ['L', 'R'], 'console: earrings of the design are split into Left and Right as before');
  eq(expand('Wolf Necklace or Huggie Hoop Earrings').pieces.length, 2, 'console: a title that names earrings too is as before');
  eq(expand('Wolf Charm Jewelry').pieces.length, 2, 'console: a title that names no product is as before');

  // the sorter's live card and the list picture: the real livePairOf and pairRow cut out of the bridge (a necklace of a two-body design is no "Left + Right", "both" glued)
  {
    const src = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
    const a = src.indexOf('const livePairOf = (r, rid) => {'), b = src.indexOf('const CNLive = window.CNLive', a);
    ok(a > 0 && b > a, 'livePairOf is in the bridge');
    const win = { CharmNestPair: CP, Master: { entryFor: sku => MASTER[sku] || null } };
    const c = vm.createContext({ window: win, String, Math, Number, Array, Object, JSON });
    vm.runInContext(src.slice(a, b) + ';this.livePairOf = livePairOf;', c);
    const J = x => JSON.parse(JSON.stringify(x));
    eq(J(c.livePairOf(neck, '4170000001')), [], 'live card: a necklace of a two-body design is told as one plain piece (not "Left + Right", not both)');
    eq(J(c.livePairOf(ear, '4170000001')).map(p => p.side), ['L', 'R'], 'live card: the same design as earrings is a Left and a Right');
    const pa = src.indexOf('const pairRow = row => {'), pb = src.indexOf('\n', pa);
    ok(pa > 0 && pb > pa, 'pairRow is in the bridge');
    vm.runInContext(src.slice(pa, pb) + '\n;this.pairRow = pairRow;', c);
    eq([c.pairRow(neck), c.pairRow(ear)], [false, true], 'list picture: "Left + Right" is said of the earring line, not of the necklace');
  }

  // the removal wording: a necklace piece of a two-body design is no left earring
  if (Remove) eq([Remove.sideOfPiece({ poolId: '4170000001_5200000001_1', form: 'necklace' }, true), Remove.sideOfPiece({ poolId: '4170000001_5200000001_1', form: 'earrings' }, true), Remove.sideOfPiece({ poolId: '4170000001_5200000001_2', form: null }, true)], [null, 'L', 'R'], 'removal: a necklace piece is no ear; an earring row or an old row of a two-body design is read as before');
}

// ── 4 · lines that must not change: what they were before this fix (recorded from the code as it was) ───────────────────────────────
{
  // [name, line, pieces, kind, piecesOf (side+body), pair module pieces (side body mirror of), pair module kind, isGroupLine]
  const SAME = [
    ['matching earring pair (studs)', mk({ sku: 'PAIR-FACE-L', title: 'Crab Charm Stud Earrings' }), 2, 'pair', 'L0 R0', 'L0.2 R0m2', 'pair', true],
    ['matching earring pair x2 (huggie hoops)', mk({ sku: 'PAIR-FACE-L', title: 'Cancer Hinged Hoops Huggie Hoop Earrings', quantity: 2 }), 4, 'multi', 'L0 R0 L0 R0', 'L0.4 R0m4 L0.4 R0m4', 'multi', true],
    ['mismatched earring pair (stud words)', mk({ sku: 'MITTENS-MIS', title: 'Mittens Mismatched Stud Earrings' }), 2, 'mismatched', 'L0 R1', 'L0.2 R1m2', 'mismatched', true],
    ['mismatched earring pair x2 (earrings)', mk({ sku: 'TENNIS-MIS', title: 'Tennis Ball and Racket Earrings', quantity: 2 }), 4, 'multi', 'L0 R1 L0 R1', 'L0.4 R1m4 L0.4 R1m4', 'multi', true],
    ['mismatched earring pair (Pair option)', mk({ sku: 'MITTENS-MIS', title: 'Mittens Charm Jewelry', variations: [V('Metal Choice', 'Gold'), V('Type', 'Pair of earrings')] }), 2, 'mismatched', 'L0 R1', 'L0.2 R1m2', 'mismatched', true],
    ['single earring naming its ear (left)', mk({ sku: 'PAIR-FACE-L', title: 'Custom Single Replacement Cat Huggie Earring Left Ear' }), 1, 'single', 'L0', 'L0.1', 'single', false],
    ['single earring naming its ear (mismatched design, right)', mk({ sku: 'MITTENS-MIS', title: 'Mittens Single Stud Earring Right Ear' }), 1, 'mismatched', 'R0', 'R1m1', 'mismatched', false],
    ['single earring naming no ear (mismatched design)', mk({ sku: 'MITTENS-MIS', title: 'Mittens Stud Earrings', variations: [V('Metal Choice', 'Gold'), V('Type', 'Single earring')] }), 1, 'mismatched', '-0', '-0.1', 'mismatched', false],
    ['counted-option necklace (3 discs)', mk({ sku: 'DISC-14', title: 'Initial Disc Necklace', variations: [V('Number of Discs / Metal', '3 discs • gold')] }), 3, 'multi', '-0 -0 -0', '-0.3 -0.3 -0.3', 'multi', true],
    ['counted-option necklace x2 (2 Disc)', mk({ sku: 'DISC-14', title: 'Initial Disc Necklace', quantity: 2, variations: [V('Necklace Options', 'ROSEGOLD - 2 Disc')] }), 4, 'multi', '-0 -0 -0 -0', '-0.4 -0.4 -0.4 -0.4', 'multi', true],
    ['plain quantity-4 line (one body)', mk({ quantity: 4 }), 4, 'multi', '-0 -0 -0 -0', '-0.4 -0.4 -0.4 -0.4', 'multi', false],
    ['plain single charm', mk({ title: 'Lighthouse Charm', variations: [V('Charm Type', 'Necklace CHARM')] }), 1, 'single', '-0', '-0.1', 'single', false]
  ];
  for (const [name, line, pieces, kind, o, cp, cpKind, group] of SAME) {
    const r = read(line), lc = lineFor(r), charm = MASTER[r.spec.designSku] && F.charmOf(r.spec.designSku);
    eq([r.spec.pieceCount, r.spec.pair.kind, sides(r), CP.piecesFor(lc, charm).map(x => (x.side || '-') + x.bodyIndex + (x.mirror ? 'm' : '.') + x.of).join(' '), CP.kindOf(lc, charm), CP.isGroupLine(lc, charm)], [pieces, kind, o, cp, cpKind, group], name + ': exactly as before');
    ok(!r.spec.pair.twoBodies, name + ': carries no two-body note');
  }
}
console.log(`pairs-neck: ${n} checks passed`);
