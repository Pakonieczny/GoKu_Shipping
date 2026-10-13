// SETBAR-A (Paul, 13 Oct 2026, order 4202291826): Etsy listing 532401548, SKU BAR_1548, "Bar Necklace SET of 2". Paul: "This listing in particular is for a set of 2 bars, the bar chosen by the system was accurate but
// was missing the other smaller bar ... You must adapt our system to recognize this particular listing and all of the font options and engraving if chosen." And: "The thinner bar always sits on the shorter chain
// unless the user special requests a different layout."
// What this checks (offline: no Etsy, no Firebase, no paid call; the real intake, the real pool in a vm; the master slice of the real library read 11 Oct 2026, tests/charm-nest/fixtures/huggie-guard-world.json):
//   A · the rule row (charm-nest-orders.js SET_RULES) against the real master slice: both designs exist, one body each, the same length, SKINNY_0191 the thinner one
//   B · the reading: a set of BAR_1548 + SKINNY_0191, 2 pieces per set, quantity N = N sets = 2N pieces; only listing + SKU; every option value of the listing (6 metal values, 20 lengths and fonts, the typed 18"& 20",
//       TYPEWRTE, CUSTOM); ENGRAVED / BLANK; a set that names a design the master lacks waits; no earring, no other listing, no person's other pick is a set
//   C · the piece model setPiecesOf and the bar-to-length rule barLengthOf (the thinner bar on the shorter chain), a special-layout note waits for a person, a person's "ignore" ends the wait
//   D · the Pool (the bridge's real source in a vm): both bars are cut and pooled, one of each per set, one group, no side; a line pooled BEFORE the rule is never offered a change nor made up again
//   node tests/charm-nest/set-bar.cjs               (SETBAR_ROOT=/path/to/another/checkout runs the same checks against that checkout: the base commit must fail them)
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path'), vm = require('vm');
const root = process.env.SETBAR_ROOT ? path.resolve(process.env.SETBAR_ROOT) : path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const noNested = require('./_noNestedArrays.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a === undefined ? null : a)), b, m); n++; };
const failures = [];
const section = async (name, fn) => { try { await fn(); console.log('  ✓', name); } catch (e) { failures.push(name); console.log('  ✗', name, '\n     ', String(e && e.message || e).split('\n').slice(0, 3).join('\n      ')); } };

const LID = '532401548', BAR = 'BAR_1548', SKINNY = 'SKINNY_0191';
const TITLE = 'Bar Necklace SET of 2, Gold, Rose Gold, Silver, Custom Gold Bar Set, Engraved Necklace, Customized Name Bar Set, Personalized Bar Necklace';
const NOTE = '33*0\'45" N 96*45\'53" W / Ginger';   // (the buyer typed * for the degree sign: order 4202291826)
const guard = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/huggie-guard-world.json'), 'utf8'));
const real = sku => guard.master.find(e => e.sku === sku);
const masters = [BAR, SKINNY].map(s => Object.assign({ holes: 2 }, real(s)));
const mk = list => { const m = new Map(list.map(e => [e.sku.toUpperCase(), e])), loose = new Map(); for (const k of m.keys()) loose.set(O.looseKey(k), k); return { masterEntry: s => m.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose.get(O.looseKey(s)) || '' }; };
const ctx = (extra, list) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] } }, mk(list || masters), extra);
const V = (name, value) => ({ name, value });
const ORDER = { receiptId: '4999999999', updateTs: 1791826702 };
/** The synthetic line of listing 532401548: option 1 "Metal · #Characters?", option 2 "Necklace Length·FONT", the personalisation box (words: [] = the box left empty). */
const lineOf = (metal, length, over) => {
  const o = over || {}, words = o.words === undefined ? [NOTE] : o.words;
  return Object.assign({ transactionId: '5300000001', listingId: LID, sku: BAR, title: TITLE, quantity: 1, metalKey: o.metalKey || 'gold', metalLabel: 'Gold', personalization: words,
    variations: [V('Metal · #Characters?', metal || 'GOLD·Set·ENGRAVED'), V('Necklace Length·FONT', length || '16"&18"·Set·ANGELINA')].concat(words.length ? [V('Personalization', words.join('\n'))] : []) }, o.line || {});
};
const read = (line, c) => O.interpretLine(ORDER, line, c || ctx());

(async () => {
  await section('A · the rule row against the real master slice: both designs, one body each, the same length, SKINNY_0191 the thinner', async () => {
    const rule = (O.SET_RULES || []).find(r => r.listings.includes(LID));
    ok(rule, 'charm-nest-orders.js has a SET_RULES row for listing ' + LID + ' (the base commit has none: it has no table of sets)');
    eq([rule.listings, rule.skus], [[LID], [BAR]], 'keyed on the listing id AND the Etsy SKU');
    eq(rule.designs.map(d => [d.sku, d.role, d.thickness]), [[BAR, 'large', 'thicker'], [SKINNY, 'small', 'thinner']], 'the two exact master designs, BAR_1548 first (the line\'s own design), SKINNY_0191 the small, thinner bar');
    const big = real(BAR), small = real(SKINNY);
    ok(big && small, 'both are in the real master slice (read 11 Oct 2026: 6,856 SKUs)');
    ok(Math.abs(big.widthPt - small.widthPt) < 0.2, `the same length: ${(big.widthPt * 25.4 / 72).toFixed(1)} and ${(small.widthPt * 25.4 / 72).toFixed(1)} mm`);
    ok(big.heightPt > small.heightPt + 2, `SKINNY_0191 is the thinner bar: ${(small.heightPt * 25.4 / 72).toFixed(1)} mm against ${(big.heightPt * 25.4 / 72).toFixed(1)} mm tall`);
    eq(rule.designs.slice().sort((a, b) => real(a.sku).heightPt - real(b.sku).heightPt).map(d => d.thickness), ['thinner', 'thicker'], 'the row\'s thickness words agree with the drawings');
    ok(big.widthPt < 80 && big.heightPt < 20, 'BAR_1548\'s file is ONE bar (73 x 14 pt), not a row of two bodies: the other bar is a different design, not a second body of this file');
    eq([big.aiPath, small.aiPath].map(p => path.basename(p)), ['BAR_9501.ai', 'SKINNY_0191.ai'], 'two different design files');
    ok(Object.isFrozen(O.SET_RULES) && Object.isFrozen(rule), 'a table row, read only: delete the row to undo it');
  });

  await section('B1 · the reading: BAR_1548 + SKINNY_0191, two pieces for one set, nothing asked, the font and the engraving kept', async () => {
    const sp = read(lineOf());
    noNested(sp, 'spec');
    eq([sp.designSku, sp.pieceCount, sp.pair.perUnit, sp.pair.kind], [BAR, 2, 2, 'multi'], 'one set is 2 pieces (the base commit makes ONE: the missing small bar)');
    eq([sp.pair.set && sp.pair.set.designs, sp.pair.set && sp.pair.set.source, sp.pair.set && sp.pair.set.rule], [[BAR, SKINNY], 'rule', 'BAR_1548-SET'], 'the set, from the rule');
    eq([sp.pair.earring, sp.pair.mismatched, sp.pair.single, sp.pair.glued], [false, false, false, false], 'not an earring, not a mismatched pair: two DIFFERENT designs of one set is not a duplicate');
    eq(sp.pair.sides, [null, null], 'no Left or Right'); eq(sp.problems, [], 'nothing waits for a person');
    eq([sp.material, sp.font.id, sp.font.asked, sp.engraveCandidate], ['gold', 'mynerve', 'ANGELINA', true], 'gold, Angelina (Mynerve), the buyer\'s words are to be engraved');
    eq([sp.pair.set.lengths, sp.pair.set.engraved], [['16"', '18"'], true], 'the pair of necklaces 16" and 18", ENGRAVED');
    ok(sp.pair.notes.some(t => /a set of 2 designs \(BAR_1548-SET\): large BAR_1548 and small SKINNY_0191, one of each/.test(t) && /no Left or Right, nothing mirrored/.test(t)), 'the order window says where it comes from: ' + sp.pair.notes.join(' | '));
    eq(O.setDesignsOf(sp), [BAR, SKINNY], 'the pool, the pictures and the engraving slots read it through the one module call they already use');
    eq(O.piecesOf(sp).map(p => [p.sku, p.unit, p.side]), [[BAR, 1, null], [SKINNY, 1, null]], 'piece 1 is the large bar, piece 2 the small bar, one set');
    // quantity N is N sets
    for (const q of [1, 2, 3]) { const s = read(lineOf(null, null, { line: { quantity: q } })); eq([s.pieceCount, O.piecesOf(s).map(p => p.sku)], [2 * q, [].concat(...Array.from({ length: q }, () => [BAR, SKINNY]))], `quantity ${q}: ${q} set${q > 1 ? 's' : ''}, ${2 * q} pieces, a large and a small bar each`); eq(O.piecesOf(s).map(p => p.unit), [].concat(...Array.from({ length: q }, (_, i) => [i + 1, i + 1])), 'the units are the sets'); }
    // the stations count a line from its words alone (no master): the same two pieces
    const raw = lineOf(); eq([O.pieceCountOf(raw), O.piecesOf(raw).map(p => p.sku)], [2, [BAR, SKINNY]], 'a raw Etsy line (the way a station counts it) is 2 pieces too');
    eq(O.pieceCountOf(Object.assign({}, raw, { quantity: 4 })), 8, 'and 4 sets are 8');
  });

  await section('B2 · only this listing and this SKU: no exact match = no match', async () => {
    const other = read(lineOf(null, null, { line: { listingId: '999000111' } })); eq([other.pieceCount, !!other.pair.set], [1, false], 'another listing with the same SKU: one piece, as before');
    const sku = read(lineOf(null, null, { line: { sku: 'BAR_9501' } })); eq([sku.pieceCount, !!sku.pair.set], [1, false], 'this listing with another SKU: not a set');
    const low = read(lineOf(null, null, { line: { sku: 'bar_1548' } })); eq(low.pair.set && low.pair.set.designs, [BAR, SKINNY], 'the SKU is matched as upper case, as every SKU is');
    // a person pointed the line at another design: their word stands, the rule ends
    const alias = read(lineOf(), ctx({ aliases: { [LID]: { bySku: { [BAR]: 'BAR_9501' } } } }, masters.concat([Object.assign({}, masters[0], { sku: 'BAR_9501' })])));
    eq([alias.designSku, !!alias.pair.set, alias.pieceCount], ['BAR_9501', false, 1], 'a person\'s saved pick of another design for this SKU: one piece of that design, no set');
    // a person's saved pair for this listing and SKU is read first (the SIBLINGS way)
    const saved = read(lineOf(), ctx({ aliases: { [LID]: { pairBySku: { [BAR]: { L: BAR, R: SKINNY } } } } })); eq(saved.pair.set && saved.pair.set.source, 'alias', 'a person\'s saved pair stands over the table');
    // an earring line of the listing is never a set
    const ears = read(lineOf(null, null, { line: { title: 'Bar Stud Earrings Set of 2', variations: [V('Metal · #Characters?', 'GOLD·Set·ENGRAVED')] } })); ok(!ears.pair.set, 'an earring line is never a set of bars: ' + JSON.stringify(ears.pair.set));
    // a set design the master lacks holds the line: never made as the first bar alone
    const noSmall = read(lineOf(), ctx({}, masters.filter(e => e.sku !== SKINNY)));
    ok(noSmall.problems.some(p => p.kind === 'unmatchedSku' && p.sku === SKINNY && p.setRule === 'BAR_1548-SET' && /never made as one piece of BAR_1548/.test(p.reason)), 'SKINNY_0191 missing: the line waits and says why: ' + JSON.stringify(noSmall.problems.map(p => p.reason)));
    const noBig = read(lineOf(), ctx({}, masters.filter(e => e.sku !== BAR))); ok(noBig.problems.some(p => p.kind === 'unmatchedSku'), 'BAR_1548 missing: the line waits as every unmatched SKU does');
  });

  await section('B3 · every option value of the listing: metal, lengths, fonts, the typed 18"& 20", TYPEWRTE, CUSTOM', async () => {
    // the metal: the Design Station's one classifier (design-1.html and design.html), the six Metal · #Characters? values
    for (const page of ['design-1.html', 'design.html']) {
      const html = fs.readFileSync(path.join(root, page), 'utf8');
      const api = vm.runInNewContext(`${html.slice(html.indexOf('const METALS = ['), html.indexOf('const PAGE_SIZE ='))}\n${html.slice(html.indexOf('function normMetalText(s)'), html.indexOf('/* ═══ 5 · ETSY REQUEST PIPELINE'))}\n({ metalKeyForTx })`);
      for (const [value, key] of [['GOLD·Set·ENGRAVED', 'gold'], ['ROSE·Set·ENGRAVED', 'rose'], ['SILVER·Set·ENGRAVED', 'silver'], ['GOLD·Set·BLANK', 'gold'], ['ROSE·Set·BLANK', 'rose'], ['SILVER·Set·BLANK', 'silver']])
        eq(api.metalKeyForTx({ variations: [{ formatted_name: 'Metal · #Characters?', formatted_value: value }, { formatted_name: 'Necklace Length·FONT', formatted_value: '16"&18"·Set·ANGELINA' }] }), key, `${page}: ${value} is ${key}`);
    }
    // the lengths and the fonts, all 20 values: no question, the font read, the pair of lengths read, two pieces
    const FONTS = { ANGELINA: 'mynerve', COMIC: 'source-sans-3', STYLISH: 'playwrite-us-trad', TYPEWRTE: 'crimson-text', PRISTINA: 'marck-script' };
    const PAIRS = [['15"&17"', ['15"', '17"']], ['16"&18"', ['16"', '18"']], ['17"&19"', ['17"', '19"']], ['18"&20"', ['18"', '20"']]];
    for (const [font, id] of Object.entries(FONTS)) for (const [pair, lens] of PAIRS) {
      const value = font === 'COMIC' && pair === '18"&20"' ? '18"& 20"·Set·COMIC' : `${pair}·Set·${font}`;   // (the one value the shop typed with a space)
      const sp = read(lineOf('GOLD·Set·ENGRAVED', value));
      eq([sp.problems, sp.font && sp.font.id, sp.font && sp.font.asked, sp.pair.set.lengths, sp.pieceCount], [[], id, font, lens, 2], `${value}: font ${font} is ${id}, lengths ${lens.join(' & ')}, no question, 2 pieces`);
    }
    // the raw Etsy spelling of the middle dot and the quotes: the same reading
    { const sp = read(lineOf('GOLD&middot;Set&middot;ENGRAVED', '16&quot;&amp;18&quot;&middot;Set&middot;TYPEWRTE')); eq([sp.problems, sp.font && sp.font.id, sp.pair.set.lengths, sp.pair.set.engraved], [[], 'crimson-text', ['16"', '18"'], true], 'entity-spelled values (&middot; &quot; &amp;) read the same'); }
    // CUSTOM · (add note): never guessed. It waits for a person (the option is not mapped): no font, no lengths, no chain, and the line is held with that one question
    { const sp = read(lineOf('SILVER·Set·ENGRAVED', 'CUSTOM · (add note)', { metalKey: 'silver' }));
      eq(sp.problems.map(p => [p.kind, p.optionName, p.optionValue]), [['needsMapping', 'Necklace Length·FONT', 'CUSTOM · (add note)']], 'CUSTOM waits for a person: the one question, the option value as the shop wrote it');
      eq([sp.font === undefined || sp.font === null, sp.pair.set.lengths, sp.chain || null], [true, [], null], 'no font, no length, no chain was guessed from it');
      eq([sp.pieceCount, sp.material], [2, 'silver'], 'the rest of the line is read as usual'); }
  });

  await section('B4 · ENGRAVED and BLANK', async () => {
    const yes = read(lineOf('GOLD·Set·ENGRAVED')); eq([yes.pair.set.engraved, yes.engraveCandidate, yes.engraving || null], [true, true, null], 'ENGRAVED with words: engraved, the words are a candidate (no front word anywhere: the back, as every line is read)');
    const blank = read(lineOf('SILVER·Set·BLANK', null, { words: [] })); eq([blank.pair.set.engraved, blank.engraveCandidate, blank.pieceCount, blank.problems], [false, false, 2, []], 'BLANK with no words: no engraving, nothing to engrave, still both bars');
    const stray = read(lineOf('ROSE·Set·BLANK')); eq([stray.pair.set.engraved, stray.engraving && stray.engraving.mode, stray.engraving && stray.engraving.needsPerson, stray.engraving && stray.engraving.reviewKind], [false, 'none', true, 'conflict'], 'BLANK with words written: a person decides (never engraved by a guess)');
    for (const sp of [yes, blank, stray]) eq(sp.pair.sides, [null, null], 'the same two bars');
  });

  await section('B5 · the words are never split across the two bars by a guess ("coordinates / Ginger" is not bar 1 and bar 2)', async () => {
    const S = require(path.join(root, 'charm-nest-engrave-sides.js')), say = sp => S.wordsForDiscs(sp, ['D1', 'D2'], { engravingNote: O.engravingNote });
    const set = read(lineOf());
    eq(say(set), null, 'the sample line: the generic list reading (which would give bar 1 the coordinates and bar 2 "Ginger") stands aside for a set the rule names');
    const numbered = read(lineOf(null, null, { words: ['Disc 1: Ginger, Disc 2: Catalina'] })); eq(say(numbered) && say(numbered).words, ['Ginger', 'Catalina'], 'words that name the bar by number are still read');
    const same = read(lineOf(null, null, { words: ['Ginger on each disc'] })); eq(say(same) && say(same).words, ['Ginger', 'Ginger'], 'words that say the same on both are still read');
    const saved = read(lineOf(), ctx({ aliases: { [LID]: { pairBySku: { [BAR]: { L: BAR, R: SKINNY } } } } })); eq(say(saved) && say(saved).how, 'list', 'a set a person saved (no rule) is read as it always was');
    eq(say(read(lineOf(null, null, { words: ['16"= 33*0\'45" N 96*45\'53" W 18"= Ginger'] }))), null, 'the per-length wording is left to the reader of it');
  });

  await section('C1 · the piece model setPiecesOf and the bar-to-length rule barLengthOf', async () => {
    ok(typeof O.setPiecesOf === 'function' && typeof O.barLengthOf === 'function', 'setPiecesOf and barLengthOf are exported (the base commit has neither)');
    const sp = read(lineOf()), pcs = O.setPiecesOf(sp);
    eq(pcs.map(p => Object.keys(p).sort()), [Object.keys(pcs[0]).sort(), Object.keys(pcs[0]).sort()], 'one shape for every piece');
    eq(Object.keys(pcs[0]).sort(), ['engraved', 'label', 'layoutAsked', 'layoutOpen', 'length', 'lengths', 'n', 'of', 'role', 'rule', 'slot', 'slotKey', 'sku', 'source', 'suggestedLength', 'thickness', 'unit'].sort(), 'the documented shape');
    eq(pcs.map(p => [p.n, p.of, p.unit, p.slot, p.slotKey, p.sku, p.role, p.label, p.thickness]), [[1, 2, 1, 0, 'D1', BAR, 'large', 'Large bar', 'thicker'], [2, 2, 1, 1, 'D2', SKINNY, 'small', 'Small bar', 'thinner']], 'piece 1 the large bar (slot D1), piece 2 the small bar (slot D2)');
    eq(pcs.map(p => [p.lengths, p.length, p.suggestedLength, p.layoutAsked, p.layoutOpen, p.engraved, p.rule, p.source]), [[['16"', '18"'], '18"', '18"', false, false, true, 'BAR_1548-SET', 'rule'], [['16"', '18"'], '16"', '16"', false, false, true, 'BAR_1548-SET', 'rule']], 'the thinner bar (SKINNY_0191) on the shorter chain 16", the thicker (BAR_1548) on the longer 18"');
    // quantity 2: two sets, each its own pair of bars, the same rule for each
    const two = O.setPiecesOf(read(lineOf(null, null, { line: { quantity: 2 } })));
    eq(two.map(p => [p.n, p.unit, p.sku, p.length]), [[1, 1, BAR, '18"'], [2, 1, SKINNY, '16"'], [3, 2, BAR, '18"'], [4, 2, SKINNY, '16"']], 'quantity 2: four pieces, two sets');
    // the rule on its own: any order of the pair, any pair
    const bars = [{ n: 1, sku: BAR, thickness: 'thicker' }, { n: 2, sku: SKINNY, thickness: 'thinner' }];
    eq(O.barLengthOf(bars, ['18"', '20"']).map(x => x.length), ['20"', '18"'], '18"&20": the thinner bar on 18"');
    eq(O.barLengthOf(bars, ['20"', '18"']).map(x => x.length), ['20"', '18"'], 'the shorter is the smaller number, whatever the order written');
    eq(O.barLengthOf(bars, ['15"', '17"']).map(x => [x.sku, x.length, x.how]), [[BAR, '17"', 'the thicker bar on the longer chain'], [SKINNY, '15"', 'the thinner bar on the shorter chain']], 'each piece says how');
    eq(O.barLengthOf(bars, []).map(x => x.length), [null, null], 'no pair (CUSTOM): nothing assigned'); eq(O.barLengthOf(bars, ['16"']).map(x => x.length), [null, null], 'one length: nothing assigned');
    eq(O.barLengthOf(bars, ['16"', '16"']).map(x => x.length), [null, null], 'two equal lengths: nothing assigned');
    eq(O.barLengthOf([{ n: 1, thickness: null }], ['16"', '18"']).map(x => x.length), [null], 'a piece whose thickness is not known: nothing assigned');
    // a set a person saved (no row): no role, no thickness, no length: nothing guessed
    const saved = O.setPiecesOf(read(lineOf(), ctx({ aliases: { [LID]: { pairBySku: { [BAR]: { L: BAR, R: SKINNY } } } } })));
    eq(saved.map(p => [p.sku, p.role, p.thickness, p.length, p.rule, p.source]), [[BAR, null, null, null, null, 'alias'], [SKINNY, null, null, null, null, 'alias']], 'a saved pair is a set without the rule\'s words');
    // every other line has no set pieces
    eq([O.setPiecesOf(read(lineOf(null, null, { line: { listingId: '1' } }))), O.setPiecesOf(null), O.setPiecesOf({})], [null, null, null], 'a plain line, nothing, an empty object: null');
  });

  await section('C2 · a buyer who asks for a different layout waits for a person; the default stays only the suggestion', async () => {
    const asks = ['please put the thick bar on the short chain', 'Swap the bars please', 'both bars on one chain', 'I want the smaller one on the longer chain', 'put bar 2 on the 16 inch', 'a different layout please', 'thin bar on the 18"'];
    for (const where of ['buyerMessage', 'personalization', 'staffNote']) for (const text of asks) {
      const line = lineOf(null, null, { words: where === 'personalization' ? [NOTE + '\n' + text] : [NOTE], line: where === 'personalization' ? {} : { [where]: text } });
      const sp = read(line), q = sp.problems.filter(p => p.kind === 'needsMapping' && p.optionName === O.SET_LAYOUT_OPT);
      ok(q.length === 1 && q[0].optionValue === '4999999999/5300000001' && /layout requested/.test(q[0].setLayout.why) && /waits for a person/.test(q[0].setLayout.why) && /thinner bar \(SKINNY_0191\) on the shorter chain/.test(q[0].setLayout.why), `${where}: “${text}” waits: ${JSON.stringify(q.map(x => x.setLayout && x.setLayout.why))}`);
      ok(q[0].note === q[0].setLayout.why, 'the Review card shows the reason');
      const pcs = O.setPiecesOf(sp);
      eq(pcs.map(p => [p.sku, p.layoutAsked, p.layoutOpen, p.length, p.suggestedLength]), [[BAR, true, true, null, '18"'], [SKINNY, true, true, null, '16"']], 'no length is assigned; the default is the suggestion only');
      eq([sp.pieceCount, sp.pair.set.layout.asked, sp.pair.set.layout.answered], [2, true, false], 'both bars are still the set');
    }
    // a message from the shop's conversation counts too
    { const sp = read(lineOf(null, null, { line: { messages: [{ text: 'Can the thick bar go on the shorter chain?' }] } })); ok(sp.problems.some(p => p.optionName === O.SET_LAYOUT_OPT), 'a conversation message waits too'); }
    // ordinary words: no request, no question (the per-length wording names no bar; a name with "long" or "small" is no request)
    for (const text of [NOTE, '16"= 33*0\'45" N 96*45\'53" W 18"= Ginger', 'Long Island / Katie', 'Small Town Girl', 'Ginger', 'Bar Bar Black Sheep', 'Short and sweet']) {
      const sp = read(lineOf(null, null, { words: [text] })); eq([sp.problems, sp.pair.set.layout], [[], null], `“${text}” is no layout request`);
      eq(O.setPiecesOf(sp).map(p => [p.length, p.layoutOpen]), [['18"', false], ['16"', false]], 'the default stands');
    }
    ok(O.setLayoutAskOf(['thick bar on the short chain']).length === 1 && O.setLayoutAskOf([NOTE]).length === 0, 'setLayoutAskOf reads the words and says them back');
    // a person looked and pressed "Nothing - ignore it" for that one order line: no more wait, the default stands, the answer is for THAT line only
    const said = { [LID]: { [O.norm(O.SET_LAYOUT_OPT)]: { [O.norm('4999999999/5300000001')]: { field: 'ignore', value: null } } } };
    { const sp = read(lineOf(null, null, { line: { buyerMessage: 'swap the bars' } }), ctx({ optionMaps: said })); eq(sp.problems, [], 'answered: nothing waits');
      eq(O.setPiecesOf(sp).map(p => [p.layoutAsked, p.layoutOpen, p.length]), [[true, false, '18"'], [true, false, '16"']], 'the person looked: the default layout stands');
      const next = read(lineOf(null, null, { line: { transactionId: '5300000002', buyerMessage: 'swap the bars' } }), ctx({ optionMaps: said })); ok(next.problems.length === 1, 'another order line of the listing is asked on its own'); }
  });

  await section('D · the Pool: both bars are made, one of each per set, one group, no side; a line pooled before the rule is left as it was', async () => {
    const F = require('./pairs-fixtures.cjs');
    const keyWorld = require('./_keyWorld.cjs')(root), { DESIGNS, world, lineRow, Pair } = keyWorld;
    DESIGNS[BAR] = () => F.charmOf('ONE-PENDANT'); DESIGNS[SKINNY] = () => F.charmOf('PAIR-HOOP');   // (two different one-body designs: by size they have different hashes)
    const worldOf = () => {
      const w = world([BAR, SKINNY]), reviews = [];
      w.c.P.parseSource = async (_b, name) => ({ name });
      w.c.P.groupCharmsAsync = async parsed => ({ charms: [structuredClone(DESIGNS[String(parsed.name).replace(/\.ai$/, '')]())], orphans: [] });
      w.c.Review = { add: it => reviews.push(it) }; w.c.B.review = { items: reviews }; w.c.Sets = { ofRun: () => [] }; w.c.Gate = { modern: () => false };
      return Object.assign(w, { reviews });
    };
    const rowOf = (qty, over) => { const line = lineOf(null, null, { line: Object.assign({ quantity: qty }, over || {}) }), spec = read(line); spec.material = 'gold'; return Object.assign(lineRow('4999999999', '5300000001', spec.designSku, qty, {}), { spec, line: { transactionId: line.transactionId, listingId: line.listingId, sku: line.sku, title: line.title } }); };
    for (const qty of [1, 2]) {
      const w = worldOf(), row = rowOf(qty); await w.Pool.poolAdd(row, null);
      const pools = w.calls.find(x => x.body.op === 'poolPut').body.pools, charms = w.pages.gold.charms;
      eq(pools.map(p => p.sku), qty === 1 ? [BAR, SKINNY] : [BAR, SKINNY, BAR, SKINNY], `×${qty}: the large bar and the small bar for every set, in order`);
      eq([row.state, charms.length, pools.length, w.Pool.pieceCountOf(row)], ['pooled', 2 * qty, 2 * qty, 2 * qty], `pooled whole, counted as ${2 * qty}`);
      ok(!pools.some(p => 'side' in p || 'mirror' in p || 'bodyIndex' in p), 'no side, no mirror, no body number: nothing is turned over');
      eq(new Set(pools.map(p => p.groupKey)).size, 1, 'ONE group: one order line, one sheet'); ok(pools.every(p => p.groupSize === 2 * qty), 'the group is every piece of the line');
      ok(Pair.mustShareSheet(pools[0], pools[1]), 'the pieces must share a sheet');
      eq(new Set(pools.map(p => p.charmHash)).size, 2, 'two different charms'); ok(charms[0].name.includes(BAR) && charms[1].name.includes(SKINNY), 'each named by the design it is cut from');
      noNested(pools, 'pools');
    }
    // the engraving: each bar is its own piece with its own job (slot D1 the large bar, D2 the small bar), the slot setPiecesOf says
    { const S = require(path.join(root, 'charm-nest-engrave-sides.js')), SC = { pair: Pair, entryFor: () => null, poolRow: () => null, charmOf: () => null };
      for (const qty of [1, 2]) { const row = rowOf(qty); row.poolIds = Array.from({ length: 2 * qty }, (_, i) => `${row.key}_${i + 1}`);
        const plan = S.plan(SC, row, []), pcs = O.setPiecesOf(row);
        eq([plan.split, plan.slots], [true, ['D1', 'D2']], `×${qty}: two engraving slots: the large bar and the small bar`);
        eq(row.poolIds.map(id => plan.of.get(id)), pcs.map(p => p.slotKey), 'copy n is the slot setPiecesOf names'); eq(pcs.map(p => p.sku).slice(0, 2), [BAR, SKINNY], 'D1 is the large bar, D2 the small bar'); } }
    // a line pooled BEFORE the rule (one piece of BAR_1548: order 4202291826's way): it keeps that piece, nothing is offered, nothing is made up again, nothing is written
    const pooledBefore = (w, row) => { const id = '4999999999_5300000001_1'; row.poolIds = [id]; w.c.B.pool.rows.set(id, { poolId: id, state: 'ready', sku: BAR, copy: 1, quantity: 1, charmHash: 'x' }); return id; };
    for (const [what, tweak] of [['not on a sheet', () => {}], ['on a draft sheet of a modern set', (w, id) => { w.pages.gold.charms.push({ poolId: id }); w.pages.gold.draft = true; w.pages.gold.setId = 's1'; w.c.Gate.modern = () => true; }], ['on a sheet that was released', (w, id) => { w.pages.gold.charms.push({ poolId: id }); w.pages.gold.fileBase = 'SS Draft 9'; w.pages.gold.releaseFull = true; }], ['on a cut sheet', (w, id) => { w.pages.gold.charms.push({ poolId: id }); w.pages.gold.roseCutAt = 1; }]]) {
      const w = worldOf(), row = rowOf(1), id = pooledBefore(w, row); tweak(w, id);
      w.Pool.pinPooled(row); w.Pool.pinPooled(row);
      eq([row.changePending || false, row.repoolChanged || false, w.reviews.length, row.poolIds], [false, false, 0, [id]], `a line pooled before the rule (${what}): no flag, no Order changed card, its piece is left alone`);
      eq([row.spec.pieceCount, row.spec.pair.legacy === true, O.setDesignsOf(row.spec), O.setPiecesOf(row)], [1, true, null, null], 'it keeps the ONE piece it has (pinned: an old line), it is not read as a set');
      ok(/pooled as 1 piece before the pair and count rule; the rule now gives 2/.test(row.spec.pieceNote || ''), 'its order window says in plain words what the rule gives now: ' + row.spec.pieceNote);
      eq(w.calls.filter(c => c.body && /pool|sheet|alias|optionMap/i.test(c.body.op || '')).length, 0, 'nothing is written');
    }
    // the same line taken off and made up again (Hold, Release hold: its old pool row is abandoned) is the set
    { const w = worldOf(), row = rowOf(1), id = pooledBefore(w, row); w.c.B.pool.rows.get(id).state = 'abandoned'; w.Pool.pinPooled(row);
      eq([row.spec.pieceCount, O.setDesignsOf(row.spec)], [2, [BAR, SKINNY]], 'made up again, it is both bars'); }
  });

  if (failures.length) { console.log(`set-bar: ${failures.length} section(s) FAILED: ${failures.join(' | ')}`); process.exit(1); }
  console.log(`set-bar: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
