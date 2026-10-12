// BFWHY (Paul, 12 Oct 2026 15:20 UTC): order 4200828980, "Birth Flower Necklace" (Etsy listing "...Bridesmaid Gift", shop CustomBrites), transaction SKU BIRTHFLOWER_NECKLACE_DISC-9,
// Birth Flower = Marigold (the 10th of the twelve months = index 9, so DISC-9 is the right charm; the vector on the row was right) was held in Review with
// "Options: option "Birth Flower: Marigold" not mapped - Etsy's SKU list for listing N is not loaded yet", though FLAGWHY (62150a4, 12983ea) clears exactly that for ZODIAC_CONSTE_GEM-7,
// ZODIAC_EARRINGS-3 and LETTER_EARRING-19.
// The cause: FLAGWHY settles the option by the transaction's numbered master SKU only when ONE option of the line is still unread. This listing has TWO drop-downs: Birth Flower and a second
// one whose values are Gold - 16" - 2 sides, Silver- 18"- 1 side, Rose- 17"- 2 sides (metal + chain length + number of sides). Its NAME has no Metal / Colour / Finish / Plating word, so
// interpretLine did not take it for the metal option and read it as a second unread option ("two could be two charms: a person says"), and neither question was settled.
// The fix: an option whose VALUE is only a metal and a chain length and / or a number of sides is the metal option whatever it is called (metalSpecValue / isMetalVariation).
// The charm rule is unchanged (a numbered master family read from the master itself, no list of listings), and what still asks, still asks.
//   node tests/charm-nest/option-own-sku-spec.cjs
const assert = require('assert/strict'), path = require('path'), fs = require('fs');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const step = name => console.log('  ✓', name);

// the master: the numbered families the way the live index holds them (BIRTHFLOWER_NECKLACE_DISC-0..29, the 12-month and 30-value siblings, zodiac, letters, goddesses with "_n") and single designs
const lib = new Map();
const family = (base, count, sep = '-') => { for (let i = 0; i < count; i++) lib.set(base + sep + i, { sku: base + sep + i }); };
family('BIRTHFLOWER_NECKLACE_DISC', 30); family('BIRTHFLOWER_NECKLACE_OVAL', 12); family('BIRTHFLOWER_NECKLACE_9MM', 30); family('BIRTHFLOWER_EARRINGS', 12); family('BIRTHFLOWER_RING_OVAL', 12);
family('ZODIAC_NECKLACE_DISC', 12); family('ZODIAC_EARRINGS', 12); family('LETTER_EARRING', 26); family('INITIAL_NECKLACE_SCRIPT', 26); family('GREEKGODDESS_NECKLACE', 5, '_'); family('RUNE_NECKLACE_CHARM', 24);
for (const s of ['BIRTH_6106', 'KEY_5598', 'GREEK GODDESS', 'MAPLE_8065', 'BIRTHFLOWER_0243', 'BIRTHFLOWER_2392']) lib.set(s, { sku: s });
const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const entryFor = s => lib.get(String(s || '').toUpperCase()) || null;
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: entryFor, masterLoose: s => loose[O.looseKey(s)] || '' }, extra || {});
const order = rid => ({ receiptId: String(rid), updateTs: 1, staffNote: '', buyerMessage: 'Typewriter font\n\nGeorgia Andy Margot Jaarsma\nOctober 26 2020' });
let tx = 5100000000;
const line = (listing, sku, title, vars, personalization) => ({ transactionId: String(++tx), listingId: listing, productId: String(++tx), sku, title, quantity: 1, metalKey: /silver/i.test(JSON.stringify(vars)) ? 'silver' : /rose/i.test(vars[0] && vars[0][1] || '') ? 'rose' : 'gold', metalLabel: '', personalization: personalization || [], variations: vars.map(([name, value, p, v]) => ({ name, value, propertyId: p || '', valueId: v || '' })) });
const read = (rid, l, extra) => O.interpretLine(order(rid), l, ctx(extra));
const open = sp => sp.problems.filter(p => p.kind === 'needsMapping' && !p.pair && !p.count);

const TITLE = 'Birth Flower Necklace, Gold Birth Flower Necklace, Silver Birth Flower Necklace, Gold Flower Necklace, Birth Month Necklace, Bridesmaid Gift';
const PERS = ['Typewriter font', '', 'Georgia Andy Margot Jaarsma', 'October 26 2020'];
// the second drop-down of the listing, all 18 values as Etsy's list shows them (screenshot 2; the spellings with and without a space after the metal are the shop's own)
const SIDE_VALUES = ['Gold - 16" - 1 side', 'Gold - 16" - 2 sides', 'Gold - 17" - 1 side', 'Gold - 17" - 2 sides', 'Gold - 18" - 1 side', 'Gold - 18" - 2 sides',
  'Silver - 16"- 1 side', 'Silver- 16"- 2 sides', 'Silver - 17"- 1 side', 'Silver- 17"- 2 sides', 'Silver - 18"- 1 side', 'Silver- 18"- 2 sides',
  'Rose- 16"- 1 side', 'Rose- 16"- 2 sides', 'Rose- 17"- 1 side', 'Rose- 17"- 2 sides', 'Rose- 18"- 1 side', 'Rose- 18"- 2 sides'];
const PAUL = [['Birth Flower', 'Marigold', '514', '209'], ['Metal, length and sides', 'Gold - 16" - 2 sides', '513', '101']];   // (the second drop-down's own label is hidden behind the open list in the screenshot; the rule never reads it)

/* ── 1 · the value reads as the metal option, whatever the option is called ── */
function valueRule() {
  for (const v of ['Gold - 16" - 1 side', 'Gold - 16" - 2 sides', 'Gold - 17" - 2 sides', 'Gold - 18" - 1 side', 'Silver - 16"- 1 side', 'Silver- 16"- 2 sides', 'Silver - 17"- 1 side', 'Silver- 18"- 2 sides', 'Rose- 16"- 1 side', 'Rose- 17"- 2 sides', 'Rose- 18"- 2 sides',
    'Gold - 16&quot; - 2 sides', 'Gold • 14" • 1 side', 'Sterling Silver - 18"', '14k Solid Gold - 16 inch', 'Gold Filled - 16" - Pair', 'Rose Gold 18" two sided']) ok(O.metalSpecValue(v), 'a metal + length + sides value: ' + v);
  // a metal word alone is also a flower ("Rose"), a length alone or the sides alone is no metal, and anything else in the value makes it a choice nobody has read
  for (const v of ['Rose', 'Gold', 'Silver', 'Marigold', '16"', '2 sides', 'Gold - Pair', 'Gold Filled - Single', 'Rose Quartz - 16"', 'Gold Fish - 2 sides', 'Heart - 16"', 'Gold - 16" - Heart', 'Silver Moon 18"', 'Rose - Aries', '', null, undefined]) ok(!O.metalSpecValue(v), 'not a metal / length value: ' + v);
  step('metalSpecValue: 17 metal + length / sides values read (Paul\'s listing spellings included); a bare metal word (Rose is a flower), a bare length, bare sides, Pair / Single alone and any other word do not');
}

/* ── 2 · Paul's order: the charm and the flower were right, nothing is left to ask ── */
function paulsOrder() {
  for (const optName of ['Metal, length and sides', 'Primary option', 'Option', 'Select an option', 'Sides', 'Chain length', 'Material', 'Engraving', 'Metal / Length']) {
    for (const flowerFirst of [true, false]) {
      const vars = [['Birth Flower', 'Marigold', '514', '209'], [optName, 'Gold - 16" - 2 sides', '513', '101']]; if (!flowerFirst) vars.reverse();
      for (const extra of [{}, { listingSkus: {} }]) {   // (no table: not loaded yet, which is the live state)
        const l = line('1700000009', 'BIRTHFLOWER_NECKLACE_DISC-9', TITLE, vars, PERS), sp = read('4200828980', l, extra);
        eq(sp.problems, [], optName + (flowerFirst ? ' (flower first)' : ' (metal first)') + ': no problem of any kind: ' + JSON.stringify(sp.problems));
        eq(sp.designSku, 'BIRTHFLOWER_NECKLACE_DISC-9'); eq(sp.skuSource, 'transaction'); eq(sp.material, 'gold');
        const bf = sp.options.find(o => o.name === 'Birth Flower'); ok(bf && bf.mapped && bf.mapped.field === 'design' && bf.mapped.value === 'BIRTHFLOWER_NECKLACE_DISC-9' && bf.mapped.source === 'sku:index', JSON.stringify(sp.options));
        ok(!sp.options.some(o => o.name === optName), 'the metal / length / sides value is no unread option: ' + JSON.stringify(sp.options));
        ok(!sp.noDesign && !sp.special);
      }
    }
  }
  // every value of the listing's second drop-down, with every flower: no question, the design is the flower's own SKU
  const months = ['Carnation', 'Violet', 'Daffodil', 'Daisy', 'Lily of the Valley', 'Rose', 'Water Lily', 'Poppy', 'Morning Glory', 'Marigold', 'Chrysanthemum', 'Pointsetta'];
  let cases = 0;
  for (const [i, flower] of months.entries()) for (const val of SIDE_VALUES) {
    const l = line('1700000009', 'BIRTHFLOWER_NECKLACE_DISC-' + i, TITLE, [['Birth Flower', flower, '514', String(200 + i)], ['Primary option', val, '513', '101']], PERS), sp = read('4200828980', l);
    eq(sp.problems, [], flower + ' / ' + val + ': ' + JSON.stringify(sp.problems)); eq(sp.designSku, 'BIRTHFLOWER_NECKLACE_DISC-' + i); cases++;
  }
  ok(SIDE_VALUES.length === 18 && cases === 12 * 18);
  step(`order 4200828980 reads clean with no table under 9 different names of the second option, flower first or last; ${cases} flower x metal-length-sides values too (Rose the flower included)`);
  // the font line and the buyer's words raise no option question (they are not options): the engraving reading is another matter
  const sp = read('4200828980', line('1700000009', 'BIRTHFLOWER_NECKLACE_DISC-9', TITLE, PAUL, PERS));
  ok(!sp.problems.some(p => p.kind === 'needsMapping' || p.kind === 'fontMissing'), 'no option or font question comes from the personalisation text');
  step('the font line "Typewriter font" in the buyer\'s words raises no option hold (it is the engraving reading\'s: BFFONT)');
  // the stored held row clears on reload with nobody re-saving it
  const old = { problems: [{ kind: 'needsMapping', optionName: 'Birth Flower', optionValue: 'Marigold' }] };
  for (const reason of ['option "Birth Flower: Marigold" not mapped — Etsy\'s SKU list for listing 1700000009 is not loaded yet', 'Options: option "Birth Flower: Marigold" not mapped — Etsy\'s SKU list for listing 1700000009 is not loaded yet']) {
    const row = { state: 'held', hold: null, poolIds: [], spec: sp, problems: [], reason };
    ok(O.questionGone(row, old), 'a row held for the old question goes back to the intake when it reads clean'); ok(O.staleQuestionHold(row), 'a stored held row with that reason is stale: ' + reason);
  }
  step('the already-held row clears on reload (questionGone / staleQuestionHold), nothing saved by anyone');
}

/* ── 3 · the whole pattern: any numbered master family, any naming style, any name of the metal / length / sides option ── */
function wholePattern() {
  const cases = [
    ['Birth Flower', 'Marigold', 'BIRTHFLOWER_NECKLACE_DISC-9', 'Birth Flower Necklace'], ['Birth Flower', 'Violet', 'BIRTHFLOWER_NECKLACE_DISC-1', 'Birth Flower Necklace'], ['Birth Flower', 'Pointsetta', 'BIRTHFLOWER_NECKLACE_DISC-11', 'Birth Flower Necklace'],
    ['Birth Flower', 'Daisy', 'BIRTHFLOWER_NECKLACE_OVAL-3', 'Birth Flower Oval Necklace'], ['Birth Flower', 'Poppy', 'BIRTHFLOWER_NECKLACE_9MM-7', 'Birth Flower Necklace 9mm'],        // NECKLACE_DISC-n, NECKLACE_OVAL-n, NECKLACE_9MM-n (30-value family)
    ['Birth Flower', 'Water Lily', 'BIRTHFLOWER_EARRINGS-6', 'Birth Flower Stud Earrings'], ['Zodiac Sign', 'Scorpio', 'ZODIAC_NECKLACE_DISC-7', 'Zodiac Disc Necklace'], ['Letter', 'T', 'LETTER_EARRING-19', 'Initial Letter Stud Earrings'],   // -n
    ['Goddess Symbol', 'Vesta', 'GREEKGODDESS_NECKLACE_4', 'Greek Goddess Necklace'], ['Viking Rune Symbol', 'Algiz', 'RUNE_NECKLACE_CHARM-14', 'Rune Necklace'], ['Initial', 'C', 'INITIAL_NECKLACE_SCRIPT-2', 'Script Initial Necklace'],   // _n
  ];
  for (const [name, value, sku, title] of cases) for (const side of ['Gold - 16" - 2 sides', 'Silver- 18"- 1 side', 'Rose- 17"- 2 sides']) for (const second of ['Primary option', 'Metal Choice', 'Chain & sides']) {
    const l = line('1800000001', sku, title, [[name, value, '514', '210'], [second, side, '513', '110']], PERS), sp = read('4100000001', l);
    eq(sp.problems, [], name + ': ' + value + ' / ' + second + ': ' + side + ' -> ' + JSON.stringify(sp.problems)); eq(sp.designSku, sku); eq(sp.options.find(o => o.name === name).mapped.source, 'sku:index');
  }
  // a family the master gets TOMORROW works the same: nothing here is a list of listings or of option names
  family('BIRTHFLOWER_PENDANT_SQUARE', 12); lib.set('BIRTHFLOWER_PENDANT_SQUARE_X', { sku: 'BIRTHFLOWER_PENDANT_SQUARE_X' });
  const l = line('1800000002', 'BIRTHFLOWER_PENDANT_SQUARE-4', 'Birth Flower Pendant Necklace', [['Birth Flower', 'Rose', '514', '211'], ['Select an option', 'Silver- 18"- 2 sides', '513', '111']], PERS);
  eq(read('4100000002', l).problems, [], 'a numbered family added to the master is settled with no code change');
  for (const k of [...lib.keys()].filter(k => k.startsWith('BIRTHFLOWER_PENDANT_SQUARE'))) lib.delete(k);
  eq(open(read('4100000002', l)).map(p => p.optionName), ['Birth Flower'], 'gone from the master, gone from the rule: the flower asks again (it is the master that says so); the metal / length / sides drop-down is never asked');
  step('every numbered family (DISC-n, OVAL-n, 9MM-n, EARRINGS-n, ZODIAC_NECKLACE_DISC-n, LETTER_EARRING-n, GREEKGODDESS_NECKLACE_n, RUNE_..-n, INITIAL_..-n) settles its option beside a metal / length / sides drop-down of any name; a family added later too');
}

/* ── 4 · what still asks ── */
function stillAsks() {
  const asks = (rid, l, extra, why) => { const sp = read(rid, l, extra), q = open(sp)[0]; ok(q, rid + ' still asks: ' + JSON.stringify(sp.problems)); if (why) ok(why.test(q.why || ''), rid + ': ' + (q.why || '(no reason)')); return sp; };
  const SIDE = ['Primary option', 'Gold - 16" - 2 sides', '513', '101'];
  // (a) a listing whose options share ONE SKU (ZODIAC REVAMP: "Zodiac REVAMP", in no master): the sign still asks, the metal / length / sides drop-down is no question
  let sp = asks('4176706298', line('1706155793', 'Zodiac REVAMP', 'Pisces Zodiac Stud Earrings', [['Zodiac Sign', 'Pisces', '514', '207'], SIDE]), { listingSkus: {} }, /not loaded yet/);
  eq(open(sp).map(p => p.optionName), ['Zodiac Sign'], 'only the sign asks: the metal / length / sides option is read, not asked');
  // (b) one shared SKU that IS a master design (the umbrella "GREEK GODDESS"): nothing says it is the option's
  asks('4174408832', line('1712164498', 'GREEK GODDESS', 'Greek Goddess Necklace', [['Goddess Symbol', 'Vesta', '514', '208'], SIDE]));
  // (c) a SKU the master does not hold (DISC-30 is past the family; -09 and -9-X are no members): asks, never turned into another design (a SKU that only differs in punctuation from ONE master SKU is that SKU's spelling, as before)
  for (const bad of ['BIRTHFLOWER_NECKLACE_DISC-30', 'BIRTHFLOWER_NECKLACE_DISC-9-X', 'BIRTHFLOWER_NECKLACE_DISC-09']) {
    sp = asks('4100000003', line('1700000009', bad, TITLE, [['Birth Flower', 'Marigold', '514', '209'], SIDE]));
    ok(sp.problems.some(p => p.kind === 'needsMapping' || p.kind === 'unmatchedSku'), bad);
    ok(!lib.has((sp.designSku || '').toUpperCase()) || O.looseKey(sp.designSku) === O.looseKey(bad), bad + ' never turned into another design: ' + sp.designSku);
  }
  // (d) a SKU that is no master SKU at all (the old listing-wide SKU): waits
  asks('4100000004', line('1700000009', 'Birth_6106', TITLE, [['Birth Flower', 'Marigold', '514', '209'], SIDE]));
  asks('4100000005', line('1700000009', 'NOT_A_MASTER-9', TITLE, [['Birth Flower', 'Marigold', '514', '209'], SIDE]));
  // (e) a blank SKU: nothing to read
  sp = read('4100000006', line('1700000009', '', TITLE, [['Birth Flower', 'Marigold', '514', '209'], SIDE])); ok(!sp.designSku && sp.problems.length > 0, 'a blank SKU waits');
  // (f) two options that may both pick a charm (a value no metal / length / sides rule reads): one SKU cannot say which, a person says
  asks('4100000007', line('1700000009', 'BIRTHFLOWER_NECKLACE_DISC-9', TITLE, [['Birth Flower', 'Marigold', '514', '209'], ['Birth Month', 'June', '515', '302'], SIDE]));
  asks('4100000008', line('1700000009', 'BIRTHFLOWER_NECKLACE_DISC-9', TITLE, [['Birth Flower', 'Marigold', '514', '209'], ['Primary option', 'Gold - 16" - Heart', '513', '101']]));   // (a word that is no metal / length / sides: not read)
  // (g) a bare metal word under a generic name is no metal option (Rose is the June flower): the line's one unread option is still settled by the SKU, not skipped; with a SKU that settles nothing it asks
  sp = read('4100000009', line('1700000009', 'BIRTHFLOWER_NECKLACE_DISC-5', TITLE, [['Birth Flower', 'Rose', '514', '205'], SIDE]));
  eq(sp.problems, [], 'Birth Flower: Rose on its own SKU reads clean'); eq(sp.options.find(o => o.name === 'Birth Flower').mapped.source, 'sku:index');
  asks('4100000010', line('1700000009', 'Birth_6106', TITLE, [['Birth Flower', 'Rose', '514', '205'], SIDE]));
  // (h) a numbered SKU of another family than the line is never cut as it stands
  asks('4100000011', line('1700000009', 'BIRTHFLOWER_EARRINGS-9', 'Birth Flower Stud Earrings', [['Birth Flower', 'Marigold', '514', '209'], ['Primary option', 'Gold - Pair']]));   // (no length / sides: the second option is unread, as before)
  asks('4100000012', line('1700000009', 'BIRTHFLOWER_NECKLACE_DISC-9', 'Birth Flower Stud Earrings', [['Birth Flower', 'Marigold', '514', '209'], SIDE]));
  // (i) a person's saved pick still stands over the SKU
  const om = { '1700000009': { 'birth flower': { marigold: { field: 'design', value: 'BIRTHFLOWER_NECKLACE_DISC-3' } } } };
  sp = read('4200828980', line('1700000009', 'BIRTHFLOWER_NECKLACE_DISC-9', TITLE, PAUL, PERS), { optionMaps: om }); eq(sp.designSku, 'BIRTHFLOWER_NECKLACE_DISC-3'); eq(sp.skuSource, 'option'); eq(sp.problems, []);
  // (j) the page's own orders show one SKU on two flowers (a seller's SKU copied onto every product): asks, and says why
  const mk = (sku, flower, val) => line('1700000009', sku, TITLE, [['Birth Flower', flower, '514', 'v-' + flower], ['Primary option', val || 'Gold - 16" - 2 sides', '513', '101']], PERS);
  const lines = [mk('BIRTHFLOWER_NECKLACE_DISC-9', 'Marigold'), mk('BIRTHFLOWER_NECKLACE_DISC-9', 'Poppy')];
  sp = asks('4100000013', lines[0], { seen: O.seenSkus(lines) }); ok(/is on orders with other choices of/.test(open(sp)[0].why), open(sp)[0].why);
  // the same SKU on different METAL / length values is the normal state of this listing (the SKU is the flower's, never the metal's): no question
  const same = [mk('BIRTHFLOWER_NECKLACE_DISC-9', 'Marigold', 'Gold - 16" - 2 sides'), mk('BIRTHFLOWER_NECKLACE_DISC-9', 'Marigold', 'Silver- 18"- 1 side'), mk('BIRTHFLOWER_NECKLACE_DISC-5', 'Rose', 'Rose- 17"- 2 sides')];
  for (const l of same) eq(read('4100000014', l, { seen: O.seenSkus(same) }).problems, [], 'one flower SKU on several metal / length values is fine');
  step('still asks: Zodiac REVAMP (one shared SKU), GREEK GODDESS umbrella, SKUs the master lacks or misspells, blank SKU, a second option that may pick a charm, another family, one SKU on two flowers; a saved pick still wins');
}

/* ── 5 · Etsy's table, when the page holds one, still decides ── */
function withTable() {
  // the listing: 12 flowers x 18 metal / length / sides products = 216, each flower's products all carry the flower's SKU (the SKU is per flower, never per metal)
  const FP = '514', MP = '513', products = []; let id = 9000;
  for (let f = 0; f < 12; f++) for (let m = 0; m < 18; m++) products.push({ id: String(id++), sku: 'BIRTHFLOWER_NECKLACE_DISC-' + f, d: 0, pv: [[FP, String(200 + f)], [MP, String(100 + m)]] });
  const table = { at: 1, n: products.length, products };
  const l = line('1700000009', 'BIRTHFLOWER_NECKLACE_DISC-9', TITLE, [['Birth Flower', 'Marigold', FP, '209'], ['Primary option', 'Gold - 16" - 2 sides', MP, '101']], PERS); l.productId = products.find(p => p.pv[0][1] === '209' && p.pv[1][1] === '101').id;
  const sp = read('4200828980', l, { listingSkus: { [l.listingId]: table } });
  eq(sp.problems, [], 'with the table loaded the flower is tied to its SKU by the table: ' + JSON.stringify(sp.problems)); eq(sp.designSku, 'BIRTHFLOWER_NECKLACE_DISC-9');
  eq(sp.options.find(o => o.name === 'Birth Flower').mapped.source, 'sku', 'the table\'s own reading answers the flower');
  ok(!sp.options.some(o => o.name === 'Primary option'), 'and the metal / length / sides value is still no option');
  // a table that says every product has the one SKU: the flower asks (the rule never overrides what Etsy says is shared)
  const one = { at: 1, n: 216, uni: 'BIRTHFLOWER_NECKLACE_DISC-9' };
  const sp2 = read('4200828980', l, { listingSkus: { [l.listingId]: one } }); ok(open(sp2).length === 1 && open(sp2)[0].optionName === 'Birth Flower' && /one SKU/.test(open(sp2)[0].why), JSON.stringify(sp2.problems));
  step('with Etsy\'s table loaded (216 products, one SKU per flower): the table ties the flower; a table that says one SKU for everything still asks');
}

/* ── 6 · nothing else moved: the real master's families, and lines that never asked ── */
function unchanged() {
  // a design line with a metal option named for the metal, a length and a size reads as before
  const l = line('900000020', 'MAPLE_8065', 'Maple Leaf Necklace', [['Metal', 'Gold'], ['Necklace Length', '16"'], ['Size', 'M']]), sp = read('4100000020', l);
  eq(sp.problems, []); eq(sp.designSku, 'MAPLE_8065'); ok(sp.options.every(o => o.mapped));
  // a metal option named for the metal keeps its old reading (skipped, never listed as an option)
  const m = read('4100000021', line('900000021', 'BIRTHFLOWER_NECKLACE_DISC-9', TITLE, [['Metal Choice / Necklace Length', 'Gold - 16"', '513', '101'], ['Birth Flower', 'Marigold', '514', '209']])); eq(m.problems, []); ok(!m.options.some(o => /metal/i.test(o.name)));
  // the real master list (when the files are there): the slim master has DISC-0..29 and the same 24 numbered families
  const f = '/mnt/project-files/plans/flag-fix-1011/master-skus-1011.json';
  if (fs.existsSync(f)) {
    const M = new Map(JSON.parse(fs.readFileSync(f, 'utf8')).map(e => [String(e.sku).toUpperCase(), e])), me = s => M.get(String(s).toUpperCase()) || null;
    eq(O.skuIndexFamily('BIRTHFLOWER_NECKLACE_DISC-9', me), { base: 'BIRTHFLOWER_NECKLACE_DISC-', n: 9 }, 'the real master holds the family');
    const lo = {}; for (const k of M.keys()) { const x = O.looseKey(k); lo[x] = x in lo ? '' : k; }
    const sp = O.interpretLine(order('4200828980'), line('1700000009', 'BIRTHFLOWER_NECKLACE_DISC-9', TITLE, PAUL, PERS), { optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: me, masterLoose: s => lo[O.looseKey(s)] || '' });
    eq(sp.problems, [], 'with the real master list: Paul\'s order reads clean'); eq(sp.designSku, 'BIRTHFLOWER_NECKLACE_DISC-9');
    step('with the real master list (6,856 SKUs): BIRTHFLOWER_NECKLACE_DISC-9 is a family member and Paul\'s order reads clean');
  } else console.log('  - real master list not present here: skipped');
  ok(typeof O.metalSpecValue === 'function');
}

const run = [valueRule, paulsOrder, wholePattern, stillAsks, withTable, unchanged]; let failed = 0;
for (const f of run) { try { f(); } catch (e) { failed++; console.log('  ✗ ' + f.name + ': ' + (e && e.stack ? e.stack.split('\n').slice(0, 7).join('\n') : e)); } }
console.log(failed ? `option-own-sku-spec: ${failed} failed` : `option-own-sku-spec: all passed (${n} checks)`); process.exit(failed ? 1 : 0);
