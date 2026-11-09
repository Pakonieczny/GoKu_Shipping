// PAIRCOUNT (Paul, 9 Nov 2026, order 4213610315): "Same with this listing just like the one above … This listing is correctly matched so it shouldn't be put on hold. Please fix this once and for all."
//   · listing 836824934 "Cactus Stud Earrings Minimalist Dainty Earrings…", SKU CACTUS_4934 (matched ✓, a Left and a Right), GF, was held:
//     "Options: option "Would you like a Single earring or a Pair?: Pair" not mapped — Etsy keeps one SKU (CACTUS_4934) for every choice of this listing: map this option once".
//   Single or Pair is a COUNT, never a design: Pair = the design as the master has it (Left + Right, 2 pieces), Single = one piece of it (the side the order names, else the master's first).
//   It is read, never asked, and the line says it ("Pair · 2 pieces" / "Single · 1 piece").
// The real reader (charm-nest-orders.js interpretLine / piecesOf) on the line as modelled from the order, the other phrasings of the shop's listings, and: a real design pick on a one-SKU listing still asks;
// a line held only for this question is released when read again (questionGone / staleQuestionHold).
//   node tests/charm-nest/pair-count-holds.cjs      Offline: no browser, no network, no Etsy call.
'use strict';
const assert = require('assert'), path = require('path');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);
const kinds = sp => (sp.problems || []).map(p => p.kind);
const asks = sp => (sp.problems || []).filter(p => p.kind === 'needsMapping');

/* ── the library: the cactus stud (one drawing, cut as a Left and a mirrored Right), a two-body Left/Right design, a lettered family ── */
const F = f => 'charmnest/master/' + f + '.ai';
function world() {
  const lib = new Map(), put = (sku, e) => lib.set(sku.toUpperCase(), Object.assign({ sku: sku.toUpperCase(), aiPath: F(sku.toUpperCase()), sizes: null, blocked: null, widthPt: 20, heightPt: 30, members: 1, holes: 0, engravable: false }, e || {}));
  put('CACTUS_4934'); put('SAGUARO_59310');
  const byFile = new Map(); for (const [k, e] of lib) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
  const loose = {}, like = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; (like[l] = like[l] || []).push(k); }
  return { masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: s => { const e = lib.get(String(s || '').toUpperCase()); return e ? byFile.get(e.aiPath) || [] : []; },
    masterLike: s => (like[O.looseKey(s)] || []).slice(), masterSkus: () => [...lib.keys()] };
}
const W = world();
const LC = '836824934', SKU = 'CACTUS_4934', ORD = { receiptId: '4213610315', updateTs: 1, buyerMessage: '', staffNote: '' };
const TITLE = 'Cactus Stud Earrings Minimalist Dainty Earrings Gold Cactus Earrings Desert Jewelry Southwest Studs Plant Earrings';
const ONE_SKU = { [LC]: { at: Date.now(), n: 2, uni: SKU } };
const ctxOf = (tables, extra) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: tables || {} }, W, extra || {});
const read = (line, c) => O.interpretLine(ORD, JSON.parse(JSON.stringify(line)), c || ctxOf(ONE_SKU));
const pieces = (line, sp) => O.piecesOf({ order: ORD, line, spec: sp, poolIds: [] }).map(p => (p.side || '-') + ':' + sp.designSku);
const QNAME = 'Would you like a Single earring or a Pair?';
const cactus = (name, value, over) => Object.assign({ transactionId: '4900000001', listingId: LC, productId: '20000001', sku: SKU, title: TITLE, quantity: 1, metalKey: 'gold', metalLabel: 'Gold Filled', personalization: [], buyerMessage: '',
  variations: [{ name, value, propertyId: '513', valueId: '7001' }] }, over || {});
const notes = sp => (sp.pair && sp.pair.notes) || [];

/* ── 1 · order 4213610315 as it is: "Would you like a Single earring or a Pair?: Pair" ── */
for (const tables of [ONE_SKU, {}]) {
  const line = cactus(QNAME, 'Pair'), sp = read(line, ctxOf(tables));
  eq(kinds(sp), [], 'CACTUS_4934 with "' + QNAME + ': Pair" asks nothing: no "Etsy keeps one SKU (CACTUS_4934) … map this option once" (' + JSON.stringify(sp.problems) + ')');
  eq([sp.designSku, sp.pieceCount, sp.pair && sp.pair.sides, pieces(line, sp)], [SKU, 2, ['L', 'R'], ['L:' + SKU, 'R:' + SKU]], 'two pieces: the Left and the Right');
  eq((sp.options.find(o => o.name === QNAME) || {}).mapped, { field: 'count', value: '2', source: 'rule:pair-count' }, 'the option is read as a count of 2');
  ok(notes(sp).some(x => /^Pair · 2 pieces/.test(x) && x.includes(QNAME)), 'the line says "Pair · 2 pieces": ' + JSON.stringify(notes(sp)));
  ok(sp.material === 'gold', 'GF as before');
}
pass('4213610315: CACTUS_4934 "Would you like a Single earring or a Pair?: Pair" is not held; 2 pieces, Left + Right; the line says "Pair · 2 pieces"');

/* ── 2 · the same with Single: one piece, and the line says which ── */
{ const line = cactus(QNAME, 'Single'), sp = read(line);
  eq(kinds(sp), [], 'Single asks nothing (' + JSON.stringify(sp.problems) + ')');
  eq([sp.pieceCount, pieces(line, sp).length, sp.pair.single], [1, 1, true], 'one piece');
  eq((sp.options.find(o => o.name === QNAME) || {}).mapped, { field: 'count', value: '1', source: 'rule:pair-count' }, 'read as a count of 1');
  ok(notes(sp).some(x => /^Single · 1 piece/.test(x) && /first piece/.test(x) && /names no ear/.test(x)), 'the line says Single · 1 piece, the master\'s first piece, no ear named: ' + JSON.stringify(notes(sp)));
  ok(!notes(sp).some(x => /does not say left or right/.test(x)), 'one line, not two'); }
for (const [v, side] of [['Single - Left', 'L'], ['Single earring (Right)', 'R'], ['Single stud - right ear', 'R']]) {
  const line = cactus('Single or Pair', v), sp = read(line);
  eq([kinds(sp), sp.pieceCount, sp.pair.sides], [[], 1, [side]], `"Single or Pair: ${v}" is one ${side} piece, nothing asked`);
  ok(notes(sp).some(x => /^Single · 1 piece/.test(x) && x.includes(side === 'L' ? 'the Left' : 'the Right')), 'the line names the side: ' + JSON.stringify(notes(sp)));
}
{ const line = cactus(QNAME, 'Single earring', { personalization: ['left ear please'] }), sp = read(line);
  eq([kinds(sp), sp.pieceCount, sp.pair.sides], [[], 1, ['L']], 'a single whose note names the left ear: one Left piece');
  ok(notes(sp).some(x => /the Left, as the order says/.test(x)), 'said so on the line'); }
pass('"Single" is one piece, nothing asked; the line says Single · 1 piece and which (the side the order names, else the master\'s first)');

/* ── 3 · every phrasing of the shop's listings ── */
const PHRASES = [
  ['Single or Pair', 'Pair', 2], ['Single or Pair', 'Single', 1], ['Single/Pair', 'Pair', 2], ['Single/Pair', 'Single', 1], ['Single / Pair', 'A pair', 2],
  ['Quantity', 'Single', 1], ['Quantity', 'Pair', 2], ['Quantity', '1 pc', 1], ['Quantity', '2 pcs', 2], ['Qty', '1 pc', 1], ['Pieces', '2 pcs', 2], ['How many?', 'One', 1], ['How many?', 'Both', 2],
  ['Style', 'Pair of earrings', 2], ['Style', 'Single stud', 1], ['Option', 'Single stud', 1], ['Type', 'Single earring', 1],
  [QNAME, 'Pair (2)', 2], [QNAME, 'A Pair', 2], [QNAME, 'Single earring', 1], [QNAME, 'Single Earring (+$0.00)', 1], [QNAME, 'Pair (+$8.00)', 2], [QNAME, 'Just one', 1], [QNAME, 'Both', 2], [QNAME, '2 earrings', 2], [QNAME, '1 earring', 1],
  ['Single or pair?', 'pair', 2], ['Single earring or pair', 'Single (1 earring)', 1], ['Amount', 'Set of 2', 2],
];
for (const [name, value, want] of PHRASES) {
  const line = cactus(name, value), sp = read(line);
  eq([kinds(sp), sp.pieceCount], [[], want], `"${name}: ${value}" → ${want} piece(s), nothing asked (${JSON.stringify(sp.problems)})`);
  ok(notes(sp).some(x => x.startsWith(want === 1 ? 'Single · 1 piece' : 'Pair · 2 pieces')), `"${name}: ${value}" is said on the line: ${JSON.stringify(notes(sp))}`);
}
// the reader itself
eq(['Pair', 'Pair (2)', 'Pair of earrings', '2 pcs', 'Single', 'Single earring', 'Single stud', '1 pc', 'Just one'].map(v => (O.pairCountOf('Single or Pair', v) || {}).n), [2, 2, 2, 2, 1, 1, 1, 1, 1], 'pairCountOf reads the values');
eq([O.pairCountOf('Quantity', '2'), O.pairCountOf('Quantity', 'One')].map(x => x && x.n), [2, 1], 'a bare number under a count name');
eq([O.pairCountOf('Letter', '1'), O.pairCountOf('Size', '2'), O.pairCountOf('Charm', 'Cactus'), O.pairCountOf('Chain Length', 'Single'), O.pairCountOf('Animal', 'Both')], [null, null, null, null, null], 'never a design pick, a size or a length');
ok(O.nonDesignOption(QNAME, 'Pair').kinds.includes('count'), 'nameKind / valueKind read "Single or Pair" as a count');
pass('Single or Pair, Single/Pair, Quantity: Single, 1 pc / 2 pcs, Pair of earrings, Single stud, Pair (2), Both, Just one: read, never asked');

/* ── 4 · a real design pick on a one-SKU listing still asks ── */
{ const sp = read(cactus('Letter', 'A')); ok(asks(sp).length === 1 && asks(sp)[0].optionValue === 'A', 'a letter still asks: ' + JSON.stringify(sp.problems)); }
{ const sp = read(cactus('Symbol', 'Saguaro')); ok(asks(sp).length === 1 && asks(sp)[0].optionValue === 'Saguaro', 'a symbol name still asks: ' + JSON.stringify(sp.problems)); }
{ const sp = read(cactus('Design', 'Pair of cacti')); ok(asks(sp).length === 1, '"Design: Pair of cacti" (a design name with "pair" in it) still asks: ' + JSON.stringify(sp.problems)); }
// a necklace line with a "Pair" option is no earring pair: not read as one (it asks, as before)
{ const sp = read(cactus('Single or Pair', 'Pair', { title: 'Cactus Necklace Dainty Gold Cactus Pendant' })); ok(asks(sp).length === 1, 'a necklace\'s "Pair" is not read as earrings: ' + JSON.stringify(sp.problems)); }
pass('a one-SKU listing whose option is a real design pick (a letter, a symbol name) still asks; a necklace "Pair" is not taken for earrings');

/* ── 5 · the held line is released when it is read again ── */
{ const line = cactus(QNAME, 'Pair'), sp = read(line);
  const prev = { problems: [{ kind: 'needsMapping', listingId: LC, optionName: QNAME, optionValue: 'Pair' }] };
  const row = { state: 'held', spec: sp, hold: null, poolIds: [], problems: [], reason: `Options: option "${QNAME}: Pair" not mapped — Etsy keeps one SKU (${SKU}) for every choice of this listing: map this option once` };
  ok(O.questionGone(row, prev), 'a row held only for the question is released on the next read');
  ok(O.staleQuestionHold(row), 'and a stored record that names it too');
  ok(!O.questionGone(Object.assign({}, row, { hold: { by: 'Paul' } }), prev), 'never a line a person held'); }
pass('order 4213610315\'s held line is released by the next read; a person\'s hold is kept');

console.log(`pair-count-holds: ${n} checks passed`);
