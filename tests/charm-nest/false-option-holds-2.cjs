// FALSEHOLD 2 (Paul, 9 Nov 2026, order 4215861378): "This listing is correctly matched so it shouldn't be put on hold. Please fix this once and for all."
//   Order 4215861378 (Carolin Irle, GF), listing 1581230865 "Red and White Wine Earring, Dainty Earring, Mix and Match Gold Earring…", SKU RED_0865 (✓, the two wine-glass studs, Left and Right), was held:
//   "option "Wine Choice: Combo- 1 red 1 white" not mapped — Etsy keeps one SKU (RED_0865) for every choice of this listing: map this option once". The red / white is the enamel colour of the wine.
// Checks: colour is a non-design kind (name or value, with the strict one-word rule); the one-SKU principle (an option whose value does not look like a design never holds a line whose one SKU is a matched
// design); a value that looks like a design still asks; a whole-option answer ("*") covers every value, a new one too; it releases the open lines held on that option alone; the bridge offers both answers.
//   node tests/charm-nest/false-option-holds-2.cjs      Offline: no browser, no network, no Etsy call.
'use strict';
const assert = require('assert'), path = require('path'), fs = require('fs');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);
const asks = sp => (sp.problems || []).filter(p => p.kind === 'needsMapping');

/* ── the library: the wine studs, a rose charm, a letter family, an animal charm ── */
function world() {
  const lib = new Map(), put = (sku, e) => lib.set(sku, Object.assign({ sku, aiPath: 'charmnest/master/' + sku + '.ai', sizes: null, blocked: null, widthPt: 20, heightPt: 30, holes: 0 }, e || {}));
  put('RED_0865'); put('ROSE_CHARM'); put('ANIMAL_CHARM', { holes: 1 }); put('LETTER_STUD');   // (one drawing for every letter: the letter itself is no design of the library, so nothing answers it but a person)
  const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
  return { masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: () => [], masterLike: () => [], masterSkus: () => [...lib.keys()] };
}
const W = world();
const ctxOf = (tables, extra) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: tables || {} }, W, extra || {});
const read = (order, line, c) => O.interpretLine(order, JSON.parse(JSON.stringify(line)), c);

const LW = '1581230865', ORD = { receiptId: '4215861378', updateTs: 1, buyerMessage: '', staffNote: '' };
const TITLE = 'Red and White Wine Earring, Dainty Earring, Mix and Match Gold Earring, Wine Lover Gift, Wine Glass Studs';
const ONE = { [LW]: { at: Date.now(), n: 3, uni: 'RED_0865' } };
const wine = (value, over) => Object.assign({ transactionId: '4900000001', listingId: LW, productId: '20000001', sku: 'RED_0865', title: TITLE, quantity: 1, metalKey: 'gold', metalLabel: 'Gold Filled', personalization: [], buyerMessage: '',
  variations: [{ name: 'Wine Choice', value, propertyId: '513', valueId: '7001' }] }, over || {});

/* ── 1 · order 4215861378 and the listing's other choices: not held, the line says the option does not change the charm ── */
for (const tables of [ONE, {}]) for (const v of ['Combo- 1 red 1 white', '2 red', '2 white']) {
  const tag = (Object.keys(tables).length ? 'one-SKU table' : 'no table') + ' · ' + v, sp = read(ORD, wine(v), ctxOf(tables));
  eq(sp.problems, [], `${tag}: nothing asked, not held (${JSON.stringify(sp.problems)})`);
  eq([sp.designSku, sp.material, sp.pieceCount, sp.pair && sp.pair.sides], ['RED_0865', 'gold', 2, ['L', 'R']], `${tag}: RED_0865, GF, the two wine-glass studs Left and Right`);
  const o = sp.options.find(x => x.name === 'Wine Choice');
  eq([o.mapped.field, o.mapped.source], ['ignore', 'rule:non-design'], `${tag}: the option is a colour, changes nothing`);
  ok(sp.optionNotes.length === 1 && / · does not change the charm$/.test(sp.optionNotes[0]) && sp.optionNotes[0].startsWith('Wine Choice: '), `${tag}: the line note: ${sp.optionNotes}`);
}
eq(read(ORD, wine('Combo- 1 red 1 white'), ctxOf(ONE)).optionNotes, ['Wine Choice: Combo 1 red 1 white · does not change the charm'], 'the note says it as Paul reads it');
pass('4215861378: "Wine Choice: Combo- 1 red 1 white" (and "2 red", "2 white") on RED_0865 is not held; the line says "Wine Choice: Combo 1 red 1 white · does not change the charm"');

/* ── 2 · colour words: a colour name or a confirmed colour value is never a design; a lone ambiguous word is ── */
for (const [name, value] of [['Color', 'Red'], ['Enamel', 'White'], ['Enamel', 'Rose'], ['Enamel Colour', 'Pink'], ['Wine Choice', 'Red / White'], ['Wine Choice', 'Mix and match red and white'], ['Option', '1 red 1 white'], ['Enamel', 'Black & Blue']]) {
  const nd = O.nonDesignOption(name, value); ok(!!nd, `"${name}: ${value}" is a non-design option (${JSON.stringify(nd)})`);
  ok(!asks(read(ORD, wine(value, { variations: [{ name, value }] }), ctxOf(ONE))).length, `"${name}: ${value}" is not held`);
}
ok(O.nonDesignOption('Wine Choice', 'Combo- 1 red 1 white').kinds.includes('colour'), 'the combo is the colour kind');
for (const [name, value] of [['Style', 'Rose'], ['Option', 'Red'], ['Birthstone Color', 'Ruby'], ['Birth Flower', 'Rose'], ['Stone', 'Red']]) ok(O.nonDesignOption(name, value) === null, `"${name}: ${value}": one ambiguous word, no colour name: not taken as non-design by the colour rule`);
// a lone "Rose" with no colour name, on a one-SKU listing: still asked (a rose is a flower and a charm)
{ const sp = read(ORD, wine('Rose', { variations: [{ name: 'Style', value: 'Rose' }] }), ctxOf(ONE)), a = asks(sp);
  ok(a.length === 1 && a[0].optionValue === 'Rose' && a[0].oneSku === 'RED_0865', 'a lone "Rose" is still asked about: ' + JSON.stringify(sp.problems)); }
pass('colour names (Color, Enamel) and colour values (combos, counts, two colours) are never asked; a lone "Rose" with no colour name still asks');

/* ── 3 · the one-SKU principle: a value that looks like a design still asks; one that does not, does not ── */
{ const LL = '1600000001', ONEL = { [LL]: { at: Date.now(), n: 26, uni: 'LETTER_STUD' } };
  const letter = (name, value) => ({ transactionId: '1', listingId: LL, productId: '9', sku: 'LETTER_STUD', title: 'Letter Stud Earrings', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], buyerMessage: '', variations: [{ name, value }] });
  for (const [name, value] of [['Letter', 'A'], ['Choice', 'Letter B'], ['Option', 'B'], ['Style', 'Initial C']]) {
    const sp = read(ORD, letter(name, value), ctxOf(ONEL)); ok(asks(sp).length === 1, `one-SKU letter listing, "${name}: ${value}" looks like a design: still asked (${JSON.stringify(sp.problems)})`);
  }
  for (const [name, value] of [['Option', 'Leo'], ['Option', 'March'], ['Option', 'Heart'], ['Option', '♥'], ['Option', 'Number 7'], ['Option', 'Animal Charm'], ['Animal', 'Cat']]) {
    ok(asks(read(ORD, wine(value, { variations: [{ name, value }] }), ctxOf(ONE))).length === 1, `"${name}: ${value}" looks like a design: still asked`);
  }
  for (const [name, value] of [['Option', 'Merlot'], ['Wine Choice', 'Red'], ['Wine Choice', 'Rosé'], ['Style', 'Classic']]) {
    const sp = read(ORD, wine(value, { variations: [{ name, value }] }), ctxOf(ONE)), o = sp.options.find(x => x.name === name);
    ok(!asks(sp).length && o.mapped && o.mapped.field === 'ignore' && (sp.optionNotes || []).length === 1, `"${name}: ${value}" does not look like a design on the one-SKU listing: not held, noted (${JSON.stringify(sp.problems)})`);
  }
  // a person once said this option picks the charm: it is still asked, value by value
  const maps = { [LW]: { 'wine choice': { 'red': { field: 'design', value: 'RED_0865' } } } };
  ok(asks(read(ORD, wine('Merlot'), ctxOf(ONE, { optionMaps: maps }))).length === 1, 'an option a person marked as picking the charm still asks for a new value');
  // the SKU is not every choice's (the table gives each product its own): the principle does not apply
  const own = { [LW]: { at: Date.now(), n: 2, products: [{ id: '20000001', sku: 'RED_0865', pv: [['513', '7001']] }, { id: '20000002', sku: 'ROSE_CHARM', pv: [['513', '7002']] }] } };
  ok(O.oneSkuOf(own[LW]) === '' && O.oneSkuOf(ONE[LW]) === 'RED_0865' && O.oneSkuOf({ products: [{ sku: 'x' }, { sku: 'X' }] }) === 'X', 'oneSkuOf: the one SKU of every choice, or none');
  // a blocked design is not ✓: the option keeps its question
  const blockedW = Object.assign({}, W, { masterEntry: s => { const e = W.masterEntry(s); return e && e.sku === 'RED_0865' ? Object.assign({}, e, { blocked: { reason: 'flip check' } }) : e; } });
  ok(asks(read(ORD, wine('Merlot'), Object.assign(ctxOf(ONE), { masterEntry: blockedW.masterEntry }))).length === 1, 'a blocked design is not ✓: the one-SKU rule leaves the option asked'); }
pass('one SKU for every choice + a matched design: a value that does not look like a design is passed and noted; a letter, a number, a sign, a month, a shape, a symbol, a charm\'s name, or an option a person marked as a design still asks');

/* ── 4 · "map this option once" is ONCE: a whole-option answer ("*") covers every value, a new one too ── */
{ const all = { [LW]: { 'wine choice': { [O.OPTION_ALL]: { field: 'ignore', value: null } } } };
  for (const v of ['Combo- 1 red 1 white', '2 red', 'Letter A', 'Leo', 'a value never seen before']) {
    const sp = read(ORD, wine(v), ctxOf(ONE, { optionMaps: all })), o = sp.options.find(x => x.name === 'Wine Choice');
    ok(!asks(sp).length && o.mapped.field === 'ignore', `the whole-option answer covers "${v}" (${JSON.stringify(sp.problems)})`);
  }
  eq(O.optionLookup(all, LW, 'Wine  Choice', 'anything'), { source: 'listing', all: true, field: 'ignore', value: null }, 'optionLookup honours the "*" value, any spelling of the name');
  ok(O.optionLookup(all, '999', 'Wine Choice', 'Red') === null, 'and only on its own listing');
  const both = { [LW]: { 'wine choice': { [O.OPTION_ALL]: { field: 'ignore', value: null }, 'leo': { field: 'design', value: 'ROSE_CHARM' } } } };
  eq(O.optionLookup(both, LW, 'Wine Choice', 'Leo'), { source: 'listing', field: 'design', value: 'ROSE_CHARM' }, 'an answer for the exact value wins over the whole-option one');
  const design = { [LW]: { 'wine choice': { [O.OPTION_ALL]: { field: 'design', value: 'RED_0865' } } } };
  ok(!asks(read(ORD, wine('Letter A'), ctxOf(ONE, { optionMaps: design }))).length, '"Same answer for every value of this option" (a design): every value answered'); }
pass('a whole-option answer stored once ("*") answers every value of the option on that listing, a new value too; an exact answer still wins');

/* ── 5 · saving it releases the open lines held on that option alone ── */
{ const held = (key, v, extra) => Object.assign({ key, state: 'held', hold: null, poolIds: [], line: wine(v, { transactionId: key }), problems: [{ kind: 'needsMapping', listingId: LW, optionName: 'Wine Choice', optionValue: v }] }, extra || {});
  const rows = [held('a', 'Combo- 1 red 1 white'), held('b', 'Leo'), held('c', '2 red', { hold: 'held by Paul' }), held('d', '2 red', { poolIds: ['x_1'] }),
    held('e', '2 white', { problems: [{ kind: 'needsMapping', optionName: 'Wine Choice' }, { kind: 'needsMaterial' }] }), held('f', '2 white', { line: Object.assign(wine('2 white'), { listingId: '42' }) }), held('g', '2 white', { problems: [{ kind: 'needsMapping', optionName: 'Gift Wrap' }] })];
  eq(O.heldOnOption(rows, [LW], 'wine choice').map(r => r.key), ['a', 'b'], 'held on "Wine Choice" alone, this listing, no person\'s hold, nothing made: a and b');
  // read again with the answer, each of them asks nothing: the existing release (questionGone) puts it back in line
  const all = { [LW]: { 'wine choice': { [O.OPTION_ALL]: { field: 'ignore', value: null } } } };
  for (const r of O.heldOnOption(rows, [LW], 'Wine Choice')) {
    const prev = { problems: r.problems.slice() }, sp = read(ORD, r.line, ctxOf(ONE, { optionMaps: all }));
    ok(O.questionGone(Object.assign({}, r, { spec: sp, problems: [] }), prev), `line ${r.key} goes back in line (questionGone)`);
  }
  // order 4215861378 itself: with no answer at all, the rule alone releases its held line (live) and its stored record ("Options: …")
  const sp = read(ORD, wine('Combo- 1 red 1 white'), ctxOf(ONE)), reason = 'Options: option "Wine Choice: Combo- 1 red 1 white" not mapped — Etsy keeps one SKU (RED_0865) for every choice of this listing: map this option once';
  ok(O.questionGone({ state: 'held', spec: sp, problems: [], hold: null, poolIds: [] }, { problems: [{ kind: 'needsMapping', optionName: 'Wine Choice', optionValue: 'Combo- 1 red 1 white' }] }), '4215861378 is released on its next reading');
  ok(O.staleQuestionHold({ state: 'held', spec: sp, reason, hold: null, poolIds: [], fromRecord: true }), 'and so is its stored record'); }
pass('a whole-option answer releases every open line of the listing held on that option alone (not a person\'s hold, not a line with pieces, not one held for anything else); 4215861378 is released by the rule alone');

/* ── 6 · the page: the Review card offers both answers, saves "*" and releases; the order window and the pool carry the note; cache tokens ── */
{ const bridge = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8'), html = fs.readFileSync(path.join(__dirname, '../../charm-nest-1.html'), 'utf8');
  ok(bridge.includes('This option never changes the charm (all choices)') && /data-a="ignoreAll"/.test(bridge) && /p\.oneSku \?/.test(bridge), 'the one-SKU card offers "This option never changes the charm (all choices)"');
  ok(/optionValue: O\.OPTION_ALL, map: \{ field: "ignore" \}/.test(bridge) && /O\.heldOnOption\(Orders\.rows\(\), ids, p\.optionName\)/.test(bridge) && /for \(const rr of held\) if \(!rowsOf\(it\)\.includes\(rr\)\) await repool\(rr\)/.test(bridge), 'it saves "*" for the option and reads the held lines again (the existing release path)');
  ok(bridge.includes('Same answer for every value of this option') && /optionValue: allv \? O\.OPTION_ALL : p\.optionValue/.test(bridge), 'a design pick offers "Same answer for every value of this option"');
  ok(/cell\("Note", sp\.optionNotes\.join\(" · "\)/.test(bridge) && /optionNote: sp\.optionNotes\.join/.test(bridge), 'the order window shows the note; the pool row carries it');
  for (const f of ['charm-nest-orders.js', 'charm-nest-bridge.js']) { const m = new RegExp('src="' + f.replace(/\./g, '\\.') + '\\?v=([^"]*)"').exec(html); ok(m && /-fh1(?:-|$)/.test(m[1]) && /-fh2(?:-|$)/.test(m[1]), f + ' cache token carries -fh1 and -fh2'); }
  const lib = fs.readFileSync(path.join(__dirname, '../../netlify/functions/charmNestLibrary.js'), 'utf8');
  ok(/const ref = db\.collection\(\(field === "design" \? "" : PREFIX\) \+ OPTMAP\)/.test(lib), 'the cloud keeps the "*" value as it keeps any value (optionMapPut)'); }
pass('the Review card: "This option never changes the charm (all choices)" and "Same answer for every value of this option"; the note in the order window and on the pool row');

console.log(`false-option-holds-2: ${n} checks passed`);
