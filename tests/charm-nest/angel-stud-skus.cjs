// ANGELNUM (Paul, 19 Oct 2026): the angel-number charms of the design file SKUs_CORRECTED_Numbers.ai, four Etsy listings, read as the library holds them.
//   · Two stud-earring listings sell the same studs ("Angel Number Earrings 111 Studs 444 …" and "Angel Number Stud Earrings Celestial …"), drop-down "Angel Number" 000 … 999.
//     Listing A's variation SKUs are Stud_Angel_0 … Stud_Angel_9, listing B's Stud_1_Angel_0 … Stud_1_Angel_9 (000 → _0, 111 → _1, … 999 → _9). The design file labels each
//     stud drawing with BOTH lines (Stud_Angel_N over Stud_1_Angel_N), so the library holds the two SKUs as one design (one file): an order of either is a PAIR of studs, Left and Right,
//     the same design, never engraved.
//   · Two necklace listings: Necklace_Angel_N and Necklace_1_Angel_N, both labelled under each necklace drawing (with its ring): one necklace piece of that drawing, never a stud drawing;
//     a stud SKU never reads as a necklace drawing.
//   · The file draws 111 … 888 only: 000 and 999 wait as "not in any master file" (never another number's drawing) until Paul adds them.
//   node tests/charm-nest/angel-stud-skus.cjs      Offline: no browser, no network, no Etsy call.
'use strict';
const assert = require('assert'), path = require('path');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);

/* ── the library as the indexer writes it from the labelled file (charmMaster-background.js): every further line under a charm is the same design (same file) under another SKU ── */
const F = f => 'charmnest/master/' + f + '.ai';
const DRAWN = [1, 2, 3, 4, 5, 6, 7, 8];
function world() {
  const lib = new Map(), put = (sku, file, e) => lib.set(sku.toUpperCase(), Object.assign({ sku: sku.toUpperCase(), aiPath: F(file), sizes: null, blocked: null }, e));
  for (const d of DRAWN) {   // (sizes and holes as the reader gives them for the labelled file: studs 10.8 x 6.2 mm, no hole but the 8's counters; necklaces 12.3 x 8.9 mm with the ring)
    const stud = { widthPt: 30.6, heightPt: 17.6, members: d === 8 ? 9 : 1, holes: d === 8 ? 8 : 0, engravable: false, charmHash: 'stud' + d };
    put('Stud_Angel_' + d, 'STUD_ANGEL_' + d, stud); put('Stud_1_Angel_' + d, 'STUD_ANGEL_' + d, stud);
    const neck = { widthPt: 34.9, heightPt: 25.2, members: d === 8 ? 10 : d === 4 || d === 6 ? 5 : 2, holes: d === 8 ? 9 : d === 4 || d === 6 ? 4 : 1, engravable: false, charmHash: 'neck' + d };
    put('Necklace_Angel_' + d, 'NECKLACE_ANGEL_' + d, neck); put('Necklace_1_Angel_' + d, 'NECKLACE_ANGEL_' + d, neck);
  }
  const byFile = new Map(); for (const [k, e] of lib) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
  const loose = {}, like = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; (like[l] = like[l] || []).push(k); }
  return { masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: s => { const e = lib.get(String(s || '').toUpperCase()); return e ? byFile.get(e.aiPath) || [] : []; },
    masterLike: s => (like[O.looseKey(s)] || []).slice(), masterSkus: () => [...lib.keys()] };
}
/* ── Etsy's inventory table of each listing (the cloud's listingSkus: the page fetches it for any line that still waits on an option, charm-nest-bridge.js): every product's own SKU, by its option values ── */
const LA = '1900000001', LB = '1900000002', LN = '1900000003', LN1 = '1900000004', P_METAL = '513', P_ANGEL = '514', P_LEN = '515';
const METALS = { silver: ['1001', 'Sterling Silver'], gold: ['1002', 'Gold Vermeil'] }, ANGEL_VAL = d => String(2000 + d);
const tableOf = (prefix, third) => ({ at: 1, n: 1, products: [].concat(...[0, 1, 2, 3, 4, 5, 6, 7, 8, 9].map(d => Object.values(METALS).map(([mv], i) => ({ id: prefix + d + i, sku: prefix + d, d: 0, pv: [[P_METAL, mv], [P_ANGEL, ANGEL_VAL(d)]].concat(third ? [third] : []) })))), names: {}, props: {} });
const TABLES = { [LA]: tableOf('Stud_Angel_'), [LB]: tableOf('Stud_1_Angel_'), [LN]: tableOf('Necklace_Angel_', [P_LEN, '3018']), [LN1]: tableOf('Necklace_1_Angel_', [P_LEN, '3018']) };
const ctxOf = (w, tables) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: tables === undefined ? TABLES : tables }, w || world());
const W = world();
let tx = 7400000000;
const ORDER = { receiptId: '4210000000', updateTs: 1, buyerMessage: '', staffNote: '' };
const read = (line, c) => O.interpretLine(ORDER, JSON.parse(JSON.stringify(line)), c || ctxOf(W));
const kinds = sp => (sp.problems || []).map(p => p.kind);
const pieces = (line, sp) => O.piecesOf({ order: ORDER, line, spec: sp, poolIds: [] }).map(p => (p.side || '-') + ':' + (p.sku || sp.designSku));
const num = d => String(d).repeat(3);

/* ── the lines as Etsy gives them (listing ids unknown: made up here) ── */
const TA = 'Angel Number Earrings 111 Studs 444 Earrings 777 Studs Sterling Silver Numerology Jewelry Dainty Minimalist Gift';
const TB = 'Angel Number Stud Earrings Celestial Numerology Studs Gold Silver Dainty Spiritual Gift for Her';
const TN = 'Angel Number Necklace 111 222 333 444 Pendant Numerology Charm Necklace Dainty Spiritual Gift';
const angel = d => ({ name: 'Angel Number', value: num(d), propertyId: P_ANGEL, valueId: ANGEL_VAL(d) }), metal = k => ({ name: 'Metal', value: METALS[k][1], propertyId: P_METAL, valueId: METALS[k][0] });
const studA = d => ({ transactionId: String(++tx), listingId: LA, sku: 'Stud_Angel_' + d, title: TA, quantity: 1, metalKey: 'silver', metalLabel: 'Sterling Silver', personalization: [], buyerMessage: '', variations: [metal('silver'), angel(d)] });
const studB = d => ({ transactionId: String(++tx), listingId: LB, sku: 'Stud_1_Angel_' + d, title: TB, quantity: 1, metalKey: 'gold', metalLabel: 'Gold Vermeil', personalization: [], buyerMessage: '', variations: [angel(d), metal('gold')] });
const neckOf = (listingId, prefix) => (d, over) => Object.assign({ transactionId: String(++tx), listingId, sku: prefix + d, title: TN, quantity: 1, metalKey: 'gold', metalLabel: 'Gold Vermeil', personalization: [], buyerMessage: '',
  variations: [angel(d), metal('gold'), { name: 'Length', value: '18"', propertyId: P_LEN, valueId: '3018' }] }, over || {});
const neck = neckOf(LN, 'Necklace_Angel_'), neck1 = neckOf(LN1, 'Necklace_1_Angel_');

/* ── 1 · both stud listings: every drawn number is a pair of the one stud design, Left and Right, never engraved ── */
for (const [name, make, prefix] of [['listing A', studA, 'STUD_ANGEL_'], ['listing B', studB, 'STUD_1_ANGEL_']]) {
  for (const d of DRAWN) {
    const line = make(d), sp = read(line), sku = prefix + d;
    eq([sp.designSku, sp.skuSource, kinds(sp), sp.pieceCount, sp.pair && sp.pair.earring, sp.pair && sp.pair.sides, pieces(line, sp)],
      [sku, 'transaction', [], 2, true, ['L', 'R'], ['L:' + sku, 'R:' + sku]], `${name} ${num(d)}: ${sku} on the Left and on the Right, no question`);
    ok(!sp.pair.mismatched && sp.lineForm === 'earring' && !sp.formHold, `${name} ${num(d)}: a matching pair of studs (form earring), nothing held`);
    ok(!sp.engraveCandidate && !(sp.engraving && String(sp.engraving).trim()), `${name} ${num(d)}: nothing to engrave (the "${num(d)}" choice is the design, not words)`);
    eq((sp.options.find(o => o.name === 'Angel Number') || {}).mapped, { field: 'design', value: sku, source: 'sku' }, `${name} ${num(d)}: the drop-down is answered by its own SKU (Etsy's table ties each value to one SKU)`);
  }
}
// the two SKUs of one number are one design file: whichever listing sold it, the same drawing is cut
for (const d of DRAWN) ok(W.masterEntry('Stud_Angel_' + d).aiPath === W.masterEntry('Stud_1_Angel_' + d).aiPath, `${num(d)}: Stud_Angel_${d} and Stud_1_Angel_${d} are one drawing`);
// the SKU as Etsy writes it, in any case
eq([['stud_angel_3', studA], ['STUD_ANGEL_3', studA], ['Stud_1_angel_3', studB], ['STUD_1_ANGEL_3', studB]].map(([s, make]) => read(Object.assign(make(3), { sku: s })).designSku), ['STUD_ANGEL_3', 'STUD_ANGEL_3', 'STUD_1_ANGEL_3', 'STUD_1_ANGEL_3'], 'the SKU in any case reads as its library spelling');
pass('stud listings A (Stud_Angel_N) and B (Stud_1_Angel_N): 111 … 888 are a pair of the same stud design, Left and Right, never engraved');

/* ── 2 · both necklace listings: Necklace_Angel_N / Necklace_1_Angel_N are the necklace drawing (with its ring), one piece ── */
for (const [name, make, prefix] of [['necklace listing', neck, 'NECKLACE_ANGEL_'], ['necklace listing 1', neck1, 'NECKLACE_1_ANGEL_']]) {
  for (const d of DRAWN) {
    const line = make(d), sp = read(line), sku = prefix + d;
    eq([sp.designSku, sp.skuSource, kinds(sp), sp.pieceCount, sp.lineForm, pieces(line, sp)], [sku, 'transaction', [], 1, 'necklace', ['-:' + sku]], `${name} ${num(d)}: ${sku}, one necklace piece, no question`);
    ok(!/^STUD/.test(sp.designSku) && !(sp.pair && sp.pair.earring) && W.masterEntry(sku).holes > 0, `${name} ${num(d)}: the ringed necklace drawing, never a stud drawing, never an earring pair`);
  }
}
for (const d of DRAWN) ok(W.masterEntry('Necklace_Angel_' + d).aiPath === W.masterEntry('Necklace_1_Angel_' + d).aiPath && W.masterEntry('Necklace_Angel_' + d).aiPath !== W.masterEntry('Stud_Angel_' + d).aiPath, `${num(d)}: the two necklace SKUs are one drawing, not the stud's`);
// a stud SKU never reads as a necklace drawing, and a necklace SKU never as a stud drawing, whichever listing it comes on (the transaction's SKU alone: no inventory table that would give the listing's own SKU)
const bare = ctxOf(null, {});
for (const d of DRAWN) {
  for (const s of ['Stud_Angel_', 'Stud_1_Angel_']) { const sp = read(neck(d, { sku: s + d }), bare); ok(!/^NECKLACE/.test(sp.designSku || ''), `${s}${d} on a necklace listing: never a necklace drawing (read ${sp.designSku})`); }
  for (const s of ['Necklace_Angel_', 'Necklace_1_Angel_']) { const sp = read(Object.assign(studA(d), { sku: s + d }), bare); ok(!/^STUD/.test(sp.designSku || ''), `${s}${d} on a stud listing: never a stud drawing (read ${sp.designSku})`); }
}
pass('necklace listings: Necklace_Angel_N and Necklace_1_Angel_N are the one ringed necklace drawing; a stud SKU never reads as a necklace drawing, nor the reverse');

/* ── 2b · before the page holds Etsy's table: the design and the pair are already right, and (FALSEHOLD, Paul 19 Oct 2026, order 4207131438) the Angel Number drop-down is answered by the line's own SKU:
   "333" names member 3 of STUD_ANGEL_N, the SKU's own number, so nothing waits for the table any more (it used to ask until the table tied each value to its SKU). A value naming another number still asks. ── */
{ const sp = read(studA(3), ctxOf(null, {}));
  eq([sp.designSku, sp.pieceCount, sp.pair.sides, kinds(sp), (sp.options.find(o => o.name === 'Angel Number') || {}).mapped], ['STUD_ANGEL_3', 2, ['L', 'R'], [], { field: 'design', value: 'STUD_ANGEL_3', source: 'sku:number' }], 'no table yet: STUD_ANGEL_3 as a pair, the drop-down answered by its own number');
  const x = read(Object.assign(studA(3), { sku: 'Stud_Angel_5' }), ctxOf(null, {}));
  ok(kinds(x).length === 1 && x.problems[0].contradiction, 'no table, "333" on Stud_Angel_5: a contradiction, asked'); }
pass('without Etsy\'s table the design and the pair are right and the drop-down is answered by the SKU\'s own number; a value naming another number asks');

/* ── 3 · 000 and 999 are not drawn: they wait as not in any master file, never another number's drawing ── */
for (const d of [0, 9]) {
  for (const [name, line] of [['listing A', studA(d)], ['listing B', studB(d)], ['necklace listing', neck(d)], ['necklace listing 1', neck1(d)]]) {
    const sp = read(line);
    ok(!W.masterEntry(sp.designSku) && (!sp.designSku || sp.designSku === line.sku.toUpperCase()), `${name} ${num(d)}: no library drawing on the row, only the SKU as bought (read ${sp.designSku})`);
    // (Etsy's table names the option's SKU, so the row says it in one plain line on the drop-down: "Etsy's SKU for this option, STUD_ANGEL_0, is not in any master file")
    const sku = line.sku.toUpperCase(), waits = sp.problems.filter(p => (p.kind === 'unmatchedSku' && p.reason === 'not in any master file' && p.sku === sku) || (p.kind === 'needsMapping' && p.optionName === 'Angel Number' && p.why === `Etsy's SKU for this option, ${sku}, is not in any master file`));
    ok(waits.length === 1 && sp.problems.length === 1, `${name} ${num(d)}: waits with ONE plain line, "${sku}" not in any master file (problems ${JSON.stringify(sp.problems)})`);
    const nt = read(line, bare);   // (before Etsy's table: the drop-down is asked, the row holds; never another number's drawing)
    ok(!W.masterEntry(nt.designSku) && kinds(nt).length >= 1, `${name} ${num(d)} without the table: waits, no library drawing (read ${nt.designSku}, ${JSON.stringify(kinds(nt))})`);
  }
}
pass('000 and 999 (Stud_Angel_0/9, Stud_1_Angel_0/9, Necklace_Angel_0/9, Necklace_1_Angel_0/9) wait as not in any master file until their drawings are added');

console.log(`angel-stud-skus: ${n} checks passed`);
