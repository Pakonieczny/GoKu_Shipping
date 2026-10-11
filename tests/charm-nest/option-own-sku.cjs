// FLAGWHY (Paul, 11 Oct 2026 12:33 UTC): three Review rows said "REVIEW REQUIRED: option "Zodiac Sign: Scorpio" / "Zodiac Sign: Cancer" / "Letter: T" not mapped, Etsy's SKU
// list for listing N is not loaded yet", yet the vector design on each row was the CORRECT charm: "These 3 were chosen correctly so I'm not sure why they got flagged".
//   4197589560  ZODIAC_CONSTE_GEM-7  listing 1160815385  Necklace      Metal Colour Silver + Zodiac Sign Scorpio
//   4195168363  ZODIAC_EARRINGS-3    listing 856808566   Stud earrings Metal Choice "Gold - Pair" + Zodiac Sign Cancer
//   4194545985  LETTER_EARRING-19    listing 883649717   Stud earrings Color Sterling Silver + Letter T
// Paul's later rows of the same kind (12:55 UTC): 4199773250 ZODIAC_EARRINGS-1 listing 1717684561 ("Metal? Single earring or a Pair?" RoseGold - Pair + Zodiac Sign Taurus) and
// 4196676605 RUNE_EARRING_CHARM-0 listing 1726076665 (Metal Choice Sterling Silver + Rune Symbol "Fehu - Luck/Success"): both clear the same way.
// The cause (charm-nest-orders.js interpretLine): each transaction already carried the product's OWN master SKU (the -7 / -3 / -19 is the index of the sign or letter, which is why
// the right thumbnail was chosen), so resolveSku returned it. But an option was only ever ANSWERED by a SKU through the listing's inventory table (tiesToOption), which the page does
// not hold until a line waits and Etsy has been asked; with no table the option fell through every rule to the generic "not mapped" hold, with inventoryWhy's "list ... not loaded
// yet". Now a master SKU on the transaction settles the one unread option when it is demonstrably the option's own (a numbered master family BASE-0, BASE-1, BASE-2…, or the orders
// the page holds carry a different SKU per value), never when something says it is shared. No Etsy call, no table, no AI, no hard-coded listing.
//   node tests/charm-nest/option-own-sku.cjs
const assert = require('assert/strict'), path = require('path');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const step = name => console.log('  ✓', name);

// the master: numbered families the way the live index holds them (zodiac 0-11, letters 0-25, runes 0-23, birth flowers 0-11, goddesses 0-4) and a few single designs
const lib = new Map();
const family = (base, count, sep = '-') => { for (let i = 0; i < count; i++) lib.set(base + sep + i, { sku: base + sep + i }); };
family('ZODIAC_CONSTE_GEM', 12); family('ZODIAC_EARRINGS', 12); family('ZODIAC_NECKLACE_CHARM', 12); family('LETTER_EARRING', 26); family('RUNE_NECKLACE_CHARM', 24); family('RUNE_EARRING_CHARM', 24);
family('BIRTHFLOWER_EARRINGS', 12); family('GREEKGODDESS_EARRING', 5, '_'); family('INITIAL_NECKLACE_SCRIPT', 26);
for (const s of ['KEY_5598', 'GREEK GODDESS', 'PISCES_68933', 'ARIES_1', 'TAURUS_2', 'MAPLE_8065', 'ZODIAC_EARRINGS-LONE', 'LONE_DESIGN-7', 'TWO_DESIGN-0', 'TWO_DESIGN-1', 'TWO_DESIGN-5', 'ZODIAC_ICONS_LIBRA']) lib.set(s, { sku: s });
const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const entryFor = s => lib.get(String(s || '').toUpperCase()) || null;
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: entryFor, masterLoose: s => loose[O.looseKey(s)] || '' }, extra || {});
const order = rid => ({ receiptId: String(rid), updateTs: 1, staffNote: '', buyerMessage: '' });
let tx = 5000000000;
const line = (listing, sku, title, vars) => ({ transactionId: String(++tx), listingId: listing, productId: String(++tx), sku, title, quantity: 1, metalKey: /silver/i.test(JSON.stringify(vars)) ? 'silver' : 'gold', metalLabel: '', personalization: [], variations: vars.map(([name, value, p, v]) => ({ name, value, propertyId: p || '', valueId: v || '' })) });
const read = (rid, l, extra) => O.interpretLine(order(rid), l, ctx(extra));
const open = sp => sp.problems.filter(p => p.kind === 'needsMapping' && !p.pair && !p.count);

// Paul's three rows, as the screenshot shows them (the option names and values are Etsy's own words; ids stand in for Etsy's)
const R1 = ['4197589560', line('1160815385', 'ZODIAC_CONSTE_GEM-7', 'Zodiac Constellation Necklace, Zodiac Gemstone Charm Necklace', [['Metal Colour', 'Silver', '513', '101'], ['Zodiac Sign', 'Scorpio', '514', '201']])];
const R2 = ['4195168363', line('856808566', 'ZODIAC_EARRINGS-3', 'Zodiac Stud Earrings, Astrology Earrings', [['Metal Choice :', 'Gold - Pair', '513', '102'], ['Zodiac Sign', 'Cancer', '514', '202']])];
const R3 = ['4194545985', line('883649717', 'LETTER_EARRING-19', 'Initial Letter Stud Earrings, Personalized Letter Earrings', [['Color', 'Sterling Silver', '513', '103'], ['Letter', 'T', '514', '203']])];

// Paul's two later rows (the metal option is Etsy's "Metal? Single earring or a Pair?": a metal option, never the charm-picking one)
const R4 = ['4199773250', line('1717684561', 'ZODIAC_EARRINGS-1', 'Zodiac Stud Earrings, Astrology Earrings', [['Metal? Single earring or a Pair?', 'RoseGold - Pair', '513', '104'], ['Zodiac Sign', 'Taurus', '514', '204']])];
const R5 = ['4196676605', line('1726076665', 'RUNE_EARRING_CHARM-0', 'Viking Rune Stud Earrings, Elder Futhark Rune Earrings', [['Metal Choice', 'Sterling Silver', '513', '105'], ['Rune Symbol', 'Fehu - Luck/Success', '514', '205']])];

/* ── 1 · the cause: with no table the three rows asked, and said "not loaded yet" (the line's own SKU was already the right charm) ── */
function cause() {
  for (const [rid, l] of [R1, R2, R3, R4, R5]) {
    // what the OLD reading did, reproduced: the same line with the new rule switched off (a SKU that is no numbered family member), and the same ids so inventoryWhy speaks
    const off = JSON.parse(JSON.stringify(l)); off.sku = 'ZODIAC_EARRINGS-LONE'; off.variations = l.variations;
    const sp = read(rid, off, { listingSkus: {} }), q = open(sp)[0];
    ok(q && /SKU list for listing \d+ is not loaded yet/.test(q.why), rid + ': a SKU nothing says is the option\'s own still asks, with the plain reason: ' + JSON.stringify(sp.problems));
    ok(sp.designSku === 'ZODIAC_EARRINGS-LONE', 'resolveSku already returned the line\'s own SKU');
  }
  step('cause: the transaction SKU was already the line\'s master design; only the option was left "not mapped" (no table to tie it to the option)');
}

/* ── 2 · the three rows now settle by themselves: no hold, no "not mapped" line, no question ── */
function threeRows() {
  const want = { '4197589560': ['ZODIAC_CONSTE_GEM-7', 'silver'], '4195168363': ['ZODIAC_EARRINGS-3', 'gold'], '4194545985': ['LETTER_EARRING-19', 'silver'], '4199773250': ['ZODIAC_EARRINGS-1', 'gold'], '4196676605': ['RUNE_EARRING_CHARM-0', 'silver'] };
  for (const [rid, l] of [R1, R2, R3, R4, R5]) {
    for (const extra of [{}, { listingSkus: {} }, { listingSkus: { [l.listingId]: undefined } }]) {   // (no table at all, an empty store, a store with no table for the listing: the same)
      const sp = read(rid, l, extra);
      eq(sp.problems, [], rid + ': no problem of any kind: ' + JSON.stringify(sp.problems));
      eq(sp.designSku, want[rid][0], rid + ': the charm is the line\'s own SKU'); eq(sp.skuSource, 'transaction', rid);
      eq(sp.material, want[rid][1], rid + ': the metal still comes from its own option');
      const opt = sp.options.find(o => !/metal|colou?r|choice/i.test(o.name)); eq(sp.options.length, 1, rid + ': the metal option is not a charm-picking option: ' + JSON.stringify(sp.options));
      ok(opt && opt.mapped && opt.mapped.field === 'design' && opt.mapped.value === want[rid][0] && opt.mapped.source === 'sku:index', rid + ': the option is answered by the SKU: ' + JSON.stringify(opt));
      ok(!sp.noDesign && !sp.special, rid);
    }
  }
  step('Paul\'s five rows: ZODIAC_CONSTE_GEM-7 / ZODIAC_EARRINGS-3 / LETTER_EARRING-19 / ZODIAC_EARRINGS-1 / RUNE_EARRING_CHARM-0 read with no problem, no table, the metal from its own option');
  // the rows that were flagged clear themselves: a row held for the old question, read again, is a line to make up again (no re-save by anybody)
  for (const [rid, l] of [R1, R2, R3, R4, R5]) {
    const old = JSON.parse(JSON.stringify(read(rid, Object.assign({}, l, { sku: 'ZODIAC_EARRINGS-LONE' })))), fresh = read(rid, l);
    const row = { state: 'held', hold: null, poolIds: [], spec: fresh, problems: [], reason: 'option "x" not mapped' };
    ok(O.questionGone(row, old), rid + ': a row held for the old "not mapped" question goes back to the intake when it reads clean');
    ok(O.staleQuestionHold(Object.assign({}, row, { reason: 'option "Zodiac Sign: Scorpio" not mapped — Etsy\'s SKU list for listing 1160815385 is not loaded yet' })), rid + ': a stored held row with that reason is stale');
  }
  step('the already-flagged rows clear on reload (questionGone / staleQuestionHold read the clean line), nothing saved by anyone');
}

/* ── 3 · every listing whose drop-down picks the charm, with no list of listings: any numbered master family, whatever the option is called ── */
function allFamilies() {
  const cases = [
    ['Zodiac Sign', 'Aries', 'ZODIAC_NECKLACE_CHARM-0'], ['Zodiac Sign', 'Pisces', 'ZODIAC_NECKLACE_CHARM-11'], ['Viking Rune Symbol', 'Algiz - Divine Plan', 'RUNE_NECKLACE_CHARM-14'],
    ['Birth Flower', 'Morning Glory', 'BIRTHFLOWER_EARRINGS-8'], ['Goddess Symbol', 'Vesta', 'GREEKGODDESS_EARRING_4'], ['Initial', 'A', 'INITIAL_NECKLACE_SCRIPT-0'], ['Letter', 'Z', 'INITIAL_NECKLACE_SCRIPT-25'],
    ['Select an option', 'whatever words the shop writes', 'ZODIAC_NECKLACE_CHARM-3']
  ];
  for (const [name, value, sku] of cases) {
    const l = line('900000001', sku, O.skuFamily(sku) === 'earring' ? 'Charm Earrings' : 'Charm Necklace', [['Metal', 'Gold'], [name, value, '514', '210']]), sp = read('4100000001', l);   // (the title names the SKU's own family: another family is never settled, see stillAsks)
    eq(sp.problems, [], name + ': ' + value); eq(sp.designSku, sku); eq(sp.options.find(o => o.name === name).mapped.source, 'sku:index');
  }
  // a family the master gets TOMORROW works the same, with no change to the code: nothing here is a list of listings or of option names
  family('NUMBER_CHARM', 10);
  const l = line('900000002', 'NUMBER_CHARM-7', 'Number Necklace', [['Metal', 'Gold'], ['Number', '7', '514', '211']]), sp = read('4100000002', l);
  eq(sp.problems, [], 'a numbered family added to the master is settled with no code change'); eq(sp.designSku, 'NUMBER_CHARM-7');
  for (const k of [...lib.keys()].filter(k => k.startsWith('NUMBER_CHARM'))) lib.delete(k);
  eq(O.skuIndexFamily('NUMBER_CHARM-7', entryFor), null, 'and it is the master that says so: gone from the master, gone from the rule');
  eq(O.skuIndexFamily('ZODIAC_EARRINGS-3', entryFor), { base: 'ZODIAC_EARRINGS-', n: 3 });
  for (const s of ['ZODIAC_EARRINGS-03', 'ZODIAC_EARRINGS3', 'ZODIAC EARRINGS 3', 'ZODIAC_EARRINGS-', 'PISCES_68933', 'LONE_DESIGN-7', 'TWO_DESIGN-5', 'KEY_5598', 'GREEK GODDESS', 'ZODIAC_EARRINGS-LONE']) eq(O.skuIndexFamily(s, entryFor), null, s + ' is not a member of a numbered family (0, 1, 2 present)');
  step('every numbered master family (zodiac, letters, initials, runes, birth flowers, goddesses…) settles its option, whatever the option is called; a family added later too');
}

/* ── 4 · what still asks, and why ── */
function stillAsks() {
  const asks = (rid, l, extra, why) => { const sp = read(rid, l, extra), q = open(sp)[0]; ok(q, rid + ' still asks: ' + JSON.stringify(sp.problems)); if (why) ok(why.test(q.why || ''), rid + ': ' + (q.why || '(no reason)')); return sp; };
  // (a) ZODIAC REVAMP: the listing's one SKU "Zodiac REVAMP" is in no master
  let sp = asks('4176706298', line('1706155793', 'Zodiac REVAMP', 'Pisces Zodiac Stud Earrings', [['Metal Choice :', 'Silver • 1 symbol', '513', '103'], ['Zodiac Sign', 'Pisces', '514', '207']]), { listingSkus: {} }, /not loaded yet/);
  ok(!sp.designSku || !lib.has(sp.designSku));
  // (b) one shared SKU that IS a master design (the umbrella "GREEK GODDESS", one master file for the listing): nothing says it is the option's
  asks('4174408832', line('1712164498', 'GREEK GODDESS', 'Greek Goddess Earrings', [['Metal Choice', 'Gold Filled', '513', '104'], ['Goddess Symbol', 'Vesta', '514', '208']]));
  // (c) the key listing: one SKU for every letter (KEY_5598 is no numbered family member): the Initial option still asks, as option "Initial" / value "A" (what the letter mapping reads)
  sp = asks('4196617547', line('1099225598', 'KEY_5598', 'Key Necklace Personalized Gift for Her Initial Jewelry Key Pendant', [['Finish', 'Gold • 14" • heart', '513', '105'], ['Initial', 'A', '514', '209']]));
  const q = open(sp)[0]; ok(q.optionName === 'Initial' && q.optionValue === 'A', 'a one-letter option reaches the generic option question untouched: ' + JSON.stringify(q));
  // (d) a SKU the master does not hold waits for a person and is never a look-alike (ZODIAC_EARRINGS-12 is not a master SKU; -11 and -1 are)
  for (const bad of ['ZODIAC_EARRINGS-12', 'ZODIAC_EARRINGS-31', 'ZODIAC EARRING 3', 'LETTER_EARRING-26', 'ZODIAC_EARRINGS-3-X']) {
    sp = asks('4100000003', line('856808566', bad, 'Zodiac Stud Earrings', [['Metal Choice', 'Gold - Pair'], ['Zodiac Sign', 'Cancer', '514', '202']]));
    ok(!lib.has((sp.designSku || '').toUpperCase()) || O.looseKey(sp.designSku) === O.looseKey(bad), bad + ': never turned into another design: ' + sp.designSku);
    ok(sp.problems.some(p => p.kind === 'needsMapping' || p.kind === 'unmatchedSku'), bad);
  }
  // (e) blank SKU: nothing to read
  sp = read('4100000004', line('856808566', '', 'Zodiac Stud Earrings', [['Metal Choice', 'Gold - Pair'], ['Zodiac Sign', 'Cancer', '514', '202']]));
  ok(!sp.designSku && sp.problems.length > 0, 'a blank SKU waits');
  // (f) two options nothing else reads: one SKU cannot say which of them picks the charm, a person says
  asks('4100000005', line('900000003', 'ZODIAC_EARRINGS-3', 'Zodiac Stud Earrings', [['Metal Choice', 'Gold - Pair'], ['Zodiac Sign', 'Cancer', '514', '202'], ['Birth Month', 'June', '515', '302']]));
  // (f2) a yes / no answer is no choice of charm: an unrelated "Gift Wrap: Yes" on a numbered-SKU line is still asked
  asks('4100000009', line('900000007', 'ZODIAC_EARRINGS-3', 'Zodiac Stud Earrings', [['Metal Choice', 'Gold - Pair'], ['Gift Wrap', 'Yes']]));
  eq(read('4100000009', line('900000007', 'ZODIAC_EARRINGS-3', 'Zodiac Stud Earrings', [['Metal Choice', 'Gold - Pair'], ['Rune Symbol', 'Nobility']])).problems, [], 'a value that merely starts with "no" is a value');
  // (g) a numbered SKU of another family than the line (a necklace pendant on an earrings line) is never cut as it stands
  asks('4100000006', line('900000004', 'ZODIAC_NECKLACE_CHARM-3', 'Zodiac Stud Earrings', [['Metal Choice', 'Gold - Pair'], ['Zodiac Sign', 'Cancer', '514', '202']]));
  // (h) a person's saved pick for the option stands over everything (and a person's pick still names the design as before)
  const om = { '1160815385': { 'zodiac sign': { scorpio: { field: 'design', value: 'ZODIAC_CONSTE_GEM-5' } } } };
  sp = read(R1[0], R1[1], { optionMaps: om }); eq(sp.designSku, 'ZODIAC_CONSTE_GEM-5'); eq(sp.skuSource, 'option'); eq(sp.problems, [], 'the saved pick still reads exactly as before');
  // (i) a SKU on a numbered family but 0, 1, 2 not all in the master: not a family (a lone SKU named BASE-7)
  asks('4100000007', line('900000005', 'LONE_DESIGN-7', 'Charm Necklace', [['Metal', 'Gold'], ['Style', 'Seven', '514', '212']]));
  asks('4100000008', line('900000006', 'TWO_DESIGN-5', 'Charm Necklace', [['Metal', 'Gold'], ['Style', 'Five', '514', '213']]));
  step('still asks: ZODIAC REVAMP, GREEK GODDESS (one shared master SKU), KEY_5598 (one letter value reaches the option question), SKUs not in the master (never a look-alike), blank SKU, two unread options, another family, lone BASE-n');
}

/* ── 5 · the page's own orders: a SKU on two choices of the option is shared (ask); a different SKU per choice is the option's own ── */
function seen() {
  const L = '900000010', mk = (sku, value, metal) => line(L, sku, 'Zodiac Necklace', [['Metal', metal || 'Gold'], ['Zodiac Sign', value, '514', 'v-' + value]]);
  const sign = (sku, value, metal) => [mk(sku, value, metal)];
  // shared: the SAME numbered SKU on two different choices (a seller's one SKU copied to every product): asks, and says why
  let lines = [mk('ZODIAC_CONSTE_GEM-7', 'Scorpio'), mk('ZODIAC_CONSTE_GEM-7', 'Aries')];
  let seenIx = O.seenSkus(lines), sp = read('4100000010', lines[0], { seen: seenIx }), q = open(sp)[0];
  ok(q, 'the SKU is on two choices of the option: it does not say which charm, a person is asked: ' + JSON.stringify(sp.problems));
  ok(/SKU ZODIAC_CONSTE_GEM-7 is on orders with other choices of “Zodiac Sign”/.test(q.why), q.why);
  eq(O.seenVerdict(seenIx, L, 'Zodiac Sign', 'Scorpio', 'ZODIAC_CONSTE_GEM-7'), 'shared');
  // the same line with nothing else seen is settled by the numbered family
  eq(read('4100000010', lines[0], { seen: O.seenSkus([lines[0]]) }).problems, []);
  // follows: SKUs that are in no numbered family (PISCES_68933, ARIES_1), one per choice, in the orders the page holds
  lines = [mk('PISCES_68933', 'Pisces'), mk('ARIES_1', 'Aries'), mk('PISCES_68933', 'Pisces', 'Silver')];
  seenIx = O.seenSkus(lines);
  for (const l of lines) { const r = read('4100000011', l, { seen: seenIx }); eq(r.problems, [], 'a different SKU for each choice: the option is the SKU\'s: ' + JSON.stringify(r.problems)); eq(r.options.find(o => o.name === 'Zodiac Sign').mapped.source, 'sku:seen'); }
  eq(O.seenVerdict(seenIx, L, 'Zodiac Sign', 'Pisces', 'PISCES_68933'), 'follows');
  // alone, nothing says the SKU is the option's: asks (as before)
  eq(open(read('4100000011', lines[0], { seen: O.seenSkus([lines[0]]) })).length, 1, 'one order of the listing proves nothing');
  eq(open(read('4100000011', lines[0], {})).length, 1, 'no page index: as before');
  // another LISTING's orders say nothing about this one
  eq(open(read('4100000011', lines[0], { seen: O.seenSkus([line('900000099', 'ARIES_1', 'x', [['Zodiac Sign', 'Aries']])]) })).length, 1);
  // the same choice with another SKU (the SKU follows the metal, not the sign): says nothing
  lines = [mk('PISCES_68933', 'Pisces', 'Gold'), mk('ARIES_1', 'Pisces', 'Silver'), mk('TAURUS_2', 'Aries')];
  eq(O.seenVerdict(O.seenSkus(lines), L, 'Zodiac Sign', 'Pisces', 'PISCES_68933'), '');
  eq(open(read('4100000012', lines[0], { seen: O.seenSkus(lines) })).length, 1, 'a SKU that varies within one choice is not that option\'s');
  // a shared SKU in the page's orders beats the numbered family only when it is THIS SKU; another SKU's sharing does not matter
  lines = [mk('ARIES_1', 'Aries'), mk('ARIES_1', 'Taurus'), mk('ZODIAC_CONSTE_GEM-7', 'Scorpio')];
  eq(read('4100000013', lines[2], { seen: O.seenSkus(lines) }).problems, [], 'ARIES_1 being shared says nothing about ZODIAC_CONSTE_GEM-7');
  // the index never grows with repeats: the same line twice is one row
  eq(O.seenSkus([lines[2], lines[2], lines[2]])[L].rows.length, 1);
  step('the page\'s own orders: one SKU on two choices is shared (asks, says so); a different SKU for each choice is the option\'s own; one order alone proves nothing');
}

/* ── 6 · Etsy's table, when the page holds one, is still the last word ── */
function withTable() {
  const [rid, l] = R1;
  // the table ties the SKU to the sign (the product's own): settled, by the table's reading as before
  const tied = { at: 1, n: 3, products: [{ id: l.productId, sku: 'ZODIAC_CONSTE_GEM-7', d: 0, pv: [['513', '101'], ['514', '201']] }, { id: '9001', sku: 'ZODIAC_CONSTE_GEM-0', d: 0, pv: [['513', '101'], ['514', '202']] }, { id: '9002', sku: 'ZODIAC_CONSTE_GEM-1', d: 0, pv: [['513', '101'], ['514', '203']] }] };
  let sp = read(rid, l, { listingSkus: { [l.listingId]: tied } }); eq(sp.problems, []); eq(sp.options.find(o => o.name === 'Zodiac Sign').mapped.source, 'sku', 'with the table the table\'s own reading answers it');
  // the table says the SKU is on several products (the listing's own): the numbered family does not override it
  const wide = { at: 1, n: 3, products: [201, 202, 203].map((v, i) => ({ id: i ? String(9000 + i) : l.productId, sku: 'ZODIAC_CONSTE_GEM-7', d: 0, pv: [['513', '101'], ['514', String(v)]] })) };
  sp = read(rid, l, { listingSkus: { [l.listingId]: wide } }); ok(open(sp).length === 1, 'the table says the SKU is shared: still asked: ' + JSON.stringify(sp.problems)); ok(/shared by several choices/.test(open(sp)[0].why || ''), open(sp)[0].why);
  // the table says every product has the one SKU
  sp = read(rid, l, { listingSkus: { [l.listingId]: { at: 1, n: 12, uni: 'zodiac_conste_gem-7' } } }); ok(open(sp).length === 1, 'the listing\'s only SKU: still asked: ' + JSON.stringify(sp.problems)); ok(/one SKU/.test(open(sp)[0].why), open(sp)[0].why);
  // a gone listing, or a table that knows nothing of this SKU (older than the order), cannot say it is shared: the numbered family reads it
  eq(read(rid, l, { listingSkus: { [l.listingId]: { gone: true } } }).problems, []);
  step('Etsy\'s table, when the page holds one, still decides: tied settles it, a SKU on several products (or the listing\'s only SKU) still asks and says why');
}

/* ── 7 · nothing else moved: lines that never asked read the same ── */
function unchanged() {
  // a plain design line with a length option and a size option reads as before (no options left unread)
  const l = line('900000020', 'MAPLE_8065', 'Maple Leaf Necklace', [['Metal', 'Gold'], ['Necklace Length', '16"'], ['Size', 'M']]);
  const sp = read('4100000020', l); eq(sp.problems, []); eq(sp.designSku, 'MAPLE_8065'); ok(sp.options.every(o => o.mapped && o.mapped.source !== 'sku:index' && o.mapped.source !== 'sku:seen'));
  // a numbered SKU with NO unread option has nothing to settle: no option is reported as answered by the SKU
  const m = line('900000021', 'ZODIAC_CONSTE_GEM-7', 'Zodiac Necklace', [['Metal', 'Gold'], ['Necklace Length', '16"']]);
  ok(read('4100000021', m).options.every(o => !o.mapped || o.mapped.source !== 'sku:index'), 'the SKU settles only an option nothing else read');
  // the exports the page and the other workers read
  ok(typeof O.skuIndexFamily === 'function' && typeof O.seenSkus === 'function' && typeof O.seenVerdict === 'function');
  step('lines that never asked read the same; the SKU settles only an option nothing else read');
}

/* ── 8 · FLAGAUDIT's 211 cases (plans/flag-fix-1011/FLAGAUDIT-cases.json, with the real master list), when the files are there: every "clear" row has no option question with no table,
      every "ask" / "wait" row still has one, the design is unchanged. The key listing's letters (KEY_5598, one design SKU, not a numbered family) are KEYLETTER's, not this rule's. ── */
function audit() {
  const dir = '/mnt/project-files/plans/flag-fix-1011/', fs = require('fs');
  if (!fs.existsSync(dir + 'FLAGAUDIT-cases.json') || !fs.existsSync(dir + 'master-skus-1011.json')) { console.log('  - audit cases not present here: skipped'); return; }
  const D = JSON.parse(fs.readFileSync(dir + 'FLAGAUDIT-cases.json', 'utf8')), M = new Map(JSON.parse(fs.readFileSync(dir + 'master-skus-1011.json', 'utf8')).map(e => [e.sku.toUpperCase(), e]));
  const lo = {}; for (const k of M.keys()) { const l = O.looseKey(k); lo[l] = l in lo ? '' : k; }
  const c = { optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, listingSkus: {}, masterEntry: s => M.get(String(s).toUpperCase()) || null, masterLoose: s => lo[O.looseKey(s)] || '' };
  let rows = 0, skipped = 0;
  for (const r of D.cases) {
    if (r.id === 'C208') { skipped++; continue; }
    const l = { transactionId: 'T1', listingId: r.listingId, productId: r.productId, sku: r.sku, title: r.title, quantity: 1, metalKey: 'gold', metalLabel: '', personalization: [], buyerMessage: '',
      variations: [{ name: r.metalField, value: r.metalValue, propertyId: r.metalPropertyId || null, valueId: r.metalValueId || null }, { name: r.optionField, value: r.optionValue, propertyId: r.optionPropertyId || null, valueId: r.optionValueId || null }] };
    const sp = O.interpretLine({ receiptId: 'R1', updateTs: 1 }, l, c), asks = sp.problems.some(p => p.kind === 'needsMapping' && p.optionName === r.optionField);
    if (r.optional === 'yes') continue;
    eq(asks, r.expected !== 'clear', r.id + ' ' + r.sku + ' ' + r.optionField + ': ' + r.optionValue + ' (expected ' + r.expected + ')'); eq(sp.designSku || '', r.designNow || '', r.id + ': the design is unchanged'); rows++;
  }
  step(`FLAGAUDIT: ${rows} cases (clear ${D.cases.filter(r => r.expected === 'clear').length - skipped}, ask / wait ${D.cases.filter(r => r.expected !== 'clear' && r.optional !== 'yes').length}) behave as expected with no table; KEY_5598 left to the letter mapping`);
}

const run = [cause, threeRows, allFamilies, stillAsks, seen, withTable, unchanged, audit]; let failed = 0;
for (const f of run) { try { f(); } catch (e) { failed++; console.log('  ✗ ' + f.name + ': ' + (e && e.stack ? e.stack.split('\n').slice(0, 6).join('\n') : e)); } }
console.log(failed ? `option-own-sku: ${failed} failed` : `option-own-sku: all passed (${n} checks)`); process.exit(failed ? 1 : 0);
