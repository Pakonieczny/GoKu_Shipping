// KEYLETTER (Paul, 11 Oct 2026 12:42 UTC, order 4196617547, listing 1099225598 "Key Necklace ... Key to My Heart", line SKU KEY_5598, "Initial: A" of an A to Z drop-down):
//   "You are not correctly mapping these letter "Image 1" options to the SKU's. In this case the user chose the letter "A". so the Key with the SKU should have the letter "A" in it already."
// The row said: option "Initial: A" not mapped - Etsy's SKU list for listing 1099225598 is not loaded yet.
// What the master really holds (read from the shop's own master file and the live index, 11 Oct): KEY_5598 and KEY_6641 are ONE drawing (same file): two heart keys, one with an H, one with an M. A heart key with
// an A is in the master FILE next to them but carries no SKU label, so the index has no key for the letter A, and the master has no letter family for keys. So a one-letter option now
//   · finds its design by the letter's place in the master's 26-letter families (A = 0, T = 19) when the line, its numbered siblings or a person's earlier answer name the family,
//   · otherwise waits with ONE plain line "no master design for letter A on this listing" (never "not mapped / SKU list not loaded") and never takes a look-alike (KEY_5598 shows an H and an M, not an A),
//   · and suggestCharms no longer drops a one-character value (the 3-character filter) and offers the letter's member of the master's letter families first.
//   node tests/charm-nest/letter-options.cjs
const assert = require('assert/strict'), path = require('path'), fs = require('fs');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const step = name => console.log('  ✓', name);
const A_Z = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';

// the master the way the live index holds it: the four 26-letter families (the stud family has no I, index 8), zodiac 12, runes 24 and birth flowers 30 (numbered, but not letters), the key designs
const lib = new Map();
const family = (base, count, sep = '-', skip = []) => { for (let i = 0; i < count; i++) if (!skip.includes(i)) lib.set(base + sep + i, { sku: base + sep + i }); };
family('LETTER_EARRING', 26); family('INITIAL_NECKLACE_SCRIPT', 26); family('INITIAL_HEART_EARRING', 26, '_'); family('INITIAL_LETTER_STUD_EARRINGS', 26, '-', [8]);
family('ZODIAC_EARRINGS', 12); family('RUNE_NECKLACE_CHARM', 24); family('BIRTHFLOWER_CHOCKER', 30);
for (const s of ['KEY_5598', 'KEY_6641', 'KEY_4669', 'KEY_49869', 'T_2800', 'T', 'MAPLE_8065']) lib.set(s, { sku: s });
const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const entryFor = s => lib.get(String(s || '').toUpperCase()) || null;
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: entryFor, masterLoose: s => loose[O.looseKey(s)] || '' }, extra || {});
const order = rid => ({ receiptId: String(rid), updateTs: 1, staffNote: '', buyerMessage: '' });
let tx = 6000000000;
const line = (listing, sku, title, vars, extra) => Object.assign({ transactionId: String(++tx), listingId: listing, productId: String(++tx), sku, title, quantity: 1, metalKey: /silver/i.test(JSON.stringify(vars)) ? 'silver' : 'gold', metalLabel: '', personalization: [], buyerMessage: '', variations: vars.map(([name, value, propertyId, valueId]) => ({ name, value, propertyId: propertyId || null, valueId: valueId || null })) }, extra || {});
const read = (rid, l, extra) => O.interpretLine(order(rid), l, ctx(extra));
const open = sp => sp.problems.filter(p => p.kind === 'needsMapping' && !p.pair && !p.count);
const KEYTITLE = 'Key Necklace Personalized Gift for Her Initial Jewelry Key Pendant Key to My Heart Charm Necklace';
const keyLine = (letter, sku = 'KEY_5598') => line('1099225598', sku, KEYTITLE, [['Finish', 'Gold • 14" • heart', '513', '105'], ['Initial', letter, '514', 'v-' + letter]], { personalization: ['AT'] });

/* ── 1 · what counts as a letter ── */
function values() {
  const want = { 'A': 0, 'a': 0, 'T': 19, 'Z': 25, ' "T" ': 19, 'Letter: T': 19, 'initial: S￼': 18, 'Initial R': 17, '“B”': 1, 'A.': 0 };
  for (const [v, i] of Object.entries(want)) eq(O.letterIndex(v), i, JSON.stringify(v));
  for (const v of ['AT', 'AB', 'Initial', 'Letter', 'XL', '', '7', '10', 'Ab', 'A B', 'ANT']) eq(O.letterIndex(v), -1, JSON.stringify(v) + ' is not ONE letter');
  eq(O.oneChar('7'), '7'); eq(O.oneChar('Number: 7'), '7'); eq(O.oneChar('77'), ''); eq(O.oneChar(''), '');
  step('a one-letter value is read in any case, in quotes, after "Letter:" or "Initial", with Etsy\'s trailing object mark; AT, AB, a number and a size word are not a letter');
}

/* ── 2 · the key necklace: the order that made Paul write ── */
function keyRow() {
  const sp = read('4196617547', keyLine('A'), { listingSkus: {} }), q = open(sp);
  eq(q.length, 1, 'the one letter option waits: ' + JSON.stringify(sp.problems));
  eq(q[0].optionName, 'Initial'); eq(q[0].optionValue, 'A');
  eq(q[0].why, 'no master design for letter A on this listing', 'one plain line, not "not mapped, Etsy\'s SKU list is not loaded yet"');
  eq(q[0].letter, { letter: 'A', why: 'no master design for letter A on this listing', clash: false });
  ok(!/not loaded|not mapped/.test(JSON.stringify(q[0])), 'nothing of the old wording');
  eq(sp.designSku, 'KEY_5598', 'the line keeps its own SKU: it is never swapped for another design'); ok(!sp.viaLetter, 'nothing was chosen for it');
  // with no table store at all (an older page), and with Etsy's table loaded and saying the SKU is shared by every letter: the same one line
  eq(open(read('4196617547', keyLine('A')))[0].why, 'no master design for letter A on this listing');
  const uni = { at: 1, n: 26, uni: 'KEY_5598' };
  eq(open(read('4196617547', keyLine('A'), { listingSkus: { '1099225598': uni } }))[0].why, 'no master design for letter A on this listing', 'Etsy keeps one SKU for the listing: still the plain line');
  // Etsy's table, loaded, with a SKU for each letter that no master file holds: the one line also says that (the table named a SKU the master lacks)
  const kl = keyLine('A'), prod = (id, sku, L) => ({ id, sku, d: 0, pv: [['513', '105'], ['514', 'v-' + L]] });
  const per = { at: 1, n: 2, products: [prod(kl.productId, 'KEY_A_5598', 'A'), prod('9001', 'KEY_B_5598', 'B')] };
  eq(open(read('4196617547', kl, { listingSkus: { '1099225598': per } }))[0].why, "no master design for letter A on this listing (Etsy's SKU for this option, KEY_A_5598, is not in any master file)");
  // ... and when the table's per-letter SKUs ARE master designs, the table settles the option as it always did (this rule never gets in its way)
  lib.set('KEY_A_5598', { sku: 'KEY_A_5598' }); lib.set('KEY_B_5598', { sku: 'KEY_B_5598' });
  const sp2 = read('4196617547', kl, { listingSkus: { '1099225598': per } }); eq(sp2.problems, [], JSON.stringify(sp2.problems)); eq(sp2.designSku, 'KEY_A_5598'); eq(sp2.skuSource, 'inventory');
  lib.delete('KEY_A_5598'); lib.delete('KEY_B_5598');
  // every letter of the A-Z drop-down says its own letter; none takes a look-alike (KEY_5598 shows an H and an M, not the letter chosen)
  for (const L of A_Z) { const r = read('4100000001', keyLine(L)), p = open(r)[0]; eq(p.why, `no master design for letter ${L} on this listing`, L); eq(r.designSku, 'KEY_5598', L); }
  // the other key designs of the listing family: the same
  for (const sku of ['KEY_6641', 'KEY_4669']) eq(open(read('4100000002', keyLine('H', sku)))[0].why, 'no master design for letter H on this listing', sku + ': even H, which KEY_5598 shows, is not taken from a drawing');
  // the line is held (the row waits for a person), the metal and everything else read as before
  eq(sp.material, 'gold'); ok(sp.problems.every(p => p.kind === 'needsMapping'), JSON.stringify(sp.problems));
  step('the key necklace row: "Initial: A" on KEY_5598 waits with ONE plain line, with or without Etsy\'s table, for every letter, and the line keeps its own SKU');
}

/* ── 3 · the letter finds its member of the family ── */
function familyMember() {
  // (1) the SKU is the letter's own member: nothing asked (FLAGWHY's index rule settles it; the letter agrees)
  for (const [fam, L] of [['LETTER_EARRING-', 'T'], ['LETTER_EARRING-', 'A'], ['LETTER_EARRING-', 'Z'], ['INITIAL_NECKLACE_SCRIPT-', 'A'], ['INITIAL_NECKLACE_SCRIPT-', 'M'], ['INITIAL_HEART_EARRING_', 'Z']]) {
    const sku = fam + A_Z.indexOf(L), l = line('883649717', sku, O.skuFamily(sku) === 'earring' ? 'Initial Letter Earrings' : 'Initial Letter Necklace', [['Color', 'Sterling Silver', '513', '103'], ['Letter', L, '514', 'v-' + L]]), sp = read('4194545985', l);
    eq(sp.problems, [], sku + ' / ' + L); eq(sp.designSku, sku); eq(sp.options.find(o => o.name === 'Letter').mapped.source, 'sku:index');
  }
  // lower case, quotes, an object mark: the same letter
  for (const v of ['t', '"T"', 'T￼', 'Letter: T']) eq(read('4194545985', line('883649717', 'LETTER_EARRING-19', 'Letter Earrings', [['Color', 'Silver'], ['Letter', v, '514', 'x']])).problems, [], v);
  // (1b) the SKU is ANOTHER letter's member: a clash, one line, never cut as it stands, the right member offered first
  let sp = read('4194545985', line('883649717', 'LETTER_EARRING-3', 'Letter Earrings', [['Color', 'Silver'], ['Letter', 'T', '514', 'x']])), q = open(sp);
  eq(q.length, 1, JSON.stringify(sp.problems)); eq(q[0].why, 'SKU LETTER_EARRING-3 is the letter D but the buyer chose T: a person says'); ok(q[0].letter.clash);
  eq(q[0].picks, [{ sku: 'LETTER_EARRING-19', why: 'the letter T of the same family' }]); eq(sp.designSku, 'LETTER_EARRING-3', 'the line\'s own SKU stays until a person says');
  // a SKU of a letter family that is itself not in the master: waits, plainly (the stud family has no I)
  sp = read('4100000003', line('856808566', 'INITIAL_LETTER_STUD_EARRINGS-8', 'Initial Stud Earrings', [['Color', 'Gold'], ['Letter', 'I', '514', 'x']])); q = open(sp);
  eq(q.length, 1, JSON.stringify(sp.problems)); eq(q[0].why, 'no master design for letter I on this listing (INITIAL_LETTER_STUD_EARRINGS-8 is not in the master)');
  // (2) the SKU is the base of a family the master holds (a new listing's family, KEYA_1234-0 ... -25, named by the shop tomorrow): the letter's member is the design
  family('KEYA_1234', 26); lib.set('KEYA_1234', { sku: 'KEYA_1234' });
  for (const L of ['A', 'H', 'Z']) { sp = read('4100000004', line('1234', 'KEYA_1234', KEYTITLE, [['Finish', 'Gold'], ['Initial', L, '514', 'x']])); eq(sp.problems, [], L + ': ' + JSON.stringify(sp.problems)); eq(sp.designSku, 'KEYA_1234-' + A_Z.indexOf(L)); eq(sp.skuSource, 'letter'); eq(sp.viaLetter.how, 'base'); eq(sp.viaLetter.from, 'KEYA_1234'); eq(sp.options.find(o => o.name === 'Initial').mapped, { field: 'design', value: 'KEYA_1234-' + A_Z.indexOf(L), source: 'rule:letter-base' }); }
  // ... a family with a hole: the missing letter waits and says which SKU is missing, the others settle
  lib.delete('KEYA_1234-7'); sp = read('4100000004', line('1234', 'KEYA_1234', KEYTITLE, [['Finish', 'Gold'], ['Initial', 'H', '514', 'x']]));
  eq(open(sp)[0].why, 'no master design for letter H on this listing (KEYA_1234-7 is not in the master)'); eq(read('4100000004', line('1234', 'KEYA_1234', KEYTITLE, [['Finish', 'Gold'], ['Initial', 'G', '514', 'x']])).designSku, 'KEYA_1234-6');
  for (const k of [...lib.keys()].filter(k => k.startsWith('KEYA_1234'))) lib.delete(k);
  // (2b) siblings named by the letter (KEYB_5678-A, KEYB_5678_A ...): exactly one master SKU names the letter
  lib.set('KEYB_5678', { sku: 'KEYB_5678' }); lib.set('KEYB_5678-A', { sku: 'KEYB_5678-A' }); lib.set('KEYB_5678-B', { sku: 'KEYB_5678-B' });
  sp = read('4100000005', line('5678', 'KEYB_5678', KEYTITLE, [['Finish', 'Gold'], ['Initial', 'A', '514', 'x']])); eq(sp.problems, [], JSON.stringify(sp.problems)); eq(sp.designSku, 'KEYB_5678-A'); eq(sp.viaLetter.how, 'named');
  eq(open(read('4100000005', line('5678', 'KEYB_5678', KEYTITLE, [['Finish', 'Gold'], ['Initial', 'C', '514', 'x']])))[0].why, 'no master design for letter C on this listing', 'no KEYB_5678-C: waits');
  lib.set('KEYB_5678_A', { sku: 'KEYB_5678_A' }); eq(open(read('4100000005', line('5678', 'KEYB_5678', KEYTITLE, [['Finish', 'Gold'], ['Initial', 'A', '514', 'x']]))).length, 1, 'two SKUs name the letter: nothing is guessed, a person says');
  for (const k of [...lib.keys()].filter(k => k.startsWith('KEYB_5678'))) lib.delete(k);
  step('the SKU that is the letter\'s own member settles; another letter\'s member is a clash (one line, right member offered); a base SKU with a numbered family or letter-named siblings maps the letter; a hole waits and names the missing SKU');
}

/* ── 4 · a person's answer for one letter carries the rest of the alphabet ── */
function personsAnswer() {
  const L = '1099225598', maps = a => ({ [L]: { initial: Object.fromEntries(Object.entries(a).map(([k, v]) => [k, { field: 'design', value: v }])) } });
  // A answered by a person as INITIAL_NECKLACE_SCRIPT-0: that answer stands for A (as it always did); B and Z follow it
  let sp = read('4100000006', keyLine('A'), { optionMaps: maps({ a: 'INITIAL_NECKLACE_SCRIPT-0' }) }); eq(sp.problems, []); eq(sp.designSku, 'INITIAL_NECKLACE_SCRIPT-0'); eq(sp.skuSource, 'option');
  for (const [l, sku] of [['B', 'INITIAL_NECKLACE_SCRIPT-1'], ['T', 'INITIAL_NECKLACE_SCRIPT-19'], ['Z', 'INITIAL_NECKLACE_SCRIPT-25']]) {
    sp = read('4100000007', keyLine(l), { optionMaps: maps({ a: 'INITIAL_NECKLACE_SCRIPT-0' }) }); eq(sp.problems, [], l + ': ' + JSON.stringify(sp.problems)); eq(sp.designSku, sku, l); eq(sp.skuSource, 'letter'); eq(sp.viaLetter.how, 'said'); eq(sp.viaLetter.from, 'KEY_5598');
  }
  // an answer that is not that letter's own member (A answered as INITIAL_NECKLACE_SCRIPT-5, or a key design) is no evidence: the rest still waits, plainly
  for (const bad of ['INITIAL_NECKLACE_SCRIPT-5', 'KEY_5598', 'KEY_4669']) { sp = read('4100000008', keyLine('B'), { optionMaps: maps({ a: bad }) }); eq(open(sp)[0].why, 'no master design for letter B on this listing', bad); eq(sp.designSku, 'KEY_5598'); }
  // two answers naming two different families: nothing is chosen between them
  sp = read('4100000009', keyLine('C'), { optionMaps: maps({ a: 'INITIAL_NECKLACE_SCRIPT-0', b: 'LETTER_EARRING-1' }) }); eq(open(sp)[0].why, 'no master design for letter C on this listing');
  // another listing's answer, and a shop-wide map, say nothing about this listing
  sp = read('4100000010', keyLine('B'), { optionMaps: { '777': { initial: { a: { field: 'design', value: 'INITIAL_NECKLACE_SCRIPT-0' } } } } }); eq(open(sp).length, 1);
  // a person's exact answer for the letter always wins
  sp = read('4100000011', keyLine('T'), { optionMaps: maps({ a: 'INITIAL_NECKLACE_SCRIPT-0', t: 'LETTER_EARRING-19' }) }); eq(sp.designSku, 'LETTER_EARRING-19'); eq(sp.skuSource, 'option');
  step('a person\'s answer that is a letter\'s own family member carries every other letter of the listing; a wrong or two-family answer carries nothing; the exact answer still wins');
}

/* ── 5 · options that are not letters are not touched ── */
function notLetters() {
  // Size S / M / L, a length, a metal: read by their own rules as before
  for (const [name, value] of [['Size', 'S'], ['Size', 'M'], ['Necklace Length', '16"'], ['Metal Choice', 'Gold Filled']]) {
    const sp = read('4100000012', line('900000020', 'MAPLE_8065', 'Maple Leaf Necklace', [['Metal', 'Gold'], [name, value]])); eq(sp.problems.filter(p => p.letter), [], name + ': ' + value); ok(!sp.viaLetter);
  }
  // a one-letter value on an option nobody names a letter option, on a line with no letter family: as before (the generic option question, no letter wording)
  let sp = read('4100000013', line('900000021', 'MAPLE_8065', 'Maple Leaf Necklace', [['Metal', 'Gold'], ['Select an option', 'A', '514', 'x']])); eq(open(sp).length, 1); ok(!open(sp)[0].letter, 'not a letter option: the old question');
  // a multi-letter value ("AT"), a number and a word are no letter: the old question, no letter wording
  for (const v of ['AT', 'AB', 'ANT', '7', 'Gold']) { sp = read('4100000014', keyLine(v)); const q = sp.problems.filter(p => p.kind === 'needsMapping'); eq(q.length, 1, v); ok(!q[0].letter, v + ': no letter wording'); }   // ("Initial: 7" is the count question it always was)
  // two letter options on one line may be two charms: a person says, as before
  sp = read('4100000015', line('900000022', 'KEY_5598', KEYTITLE, [['Metal', 'Gold'], ['Initial 1', 'A', '514', 'a'], ['Initial 2', 'B', '515', 'b']])); eq(open(sp).length, 2); ok(open(sp).every(p => !p.letter));
  // a font option or a count option named for a letter is read by its own rule first (font: Stylish; "Number of Discs: 3")
  eq(read('4100000016', line('900000023', 'MAPLE_8065', 'Initial Disc Necklace', [['Metal', 'Gold'], ['Number of Discs / Metal', '3 discs • gold']])).problems.filter(p => p.letter), []);
  step('sizes, lengths, metals, a generic option, a word, a number and two letter options are not read as one letter');
}

/* ── 6 · the card's offers: one-character values and the letter families ── */
function suggestions() {
  const skus = [...lib.keys()];
  // the old filter dropped "A" ("length >= 3"): nothing was ever offered for a letter. Now the letter's own member of every letter-named family, the line's family first
  const necklace = keyLine('A'); necklace.sku = 'KEY_5598';
  let s = O.suggestCharms({ value: 'A', line: necklace, skus: new Set(skus), max: 8 });
  eq(s.map(x => x.sku).sort(), ['INITIAL_HEART_EARRING_0', 'INITIAL_LETTER_STUD_EARRINGS-0', 'INITIAL_NECKLACE_SCRIPT-0', 'LETTER_EARRING-0'], JSON.stringify(s));
  eq(s[0].why, 'the letter A of the INITIAL_NECKLACE_SCRIPT-0 to INITIAL_NECKLACE_SCRIPT-25 designs');
  ok(s[0].sku === 'INITIAL_NECKLACE_SCRIPT-0', 'a necklace line is offered the necklace letter first');
  // the same for T, an iterator of SKUs (the page passes Map.keys()), lower case and a label
  for (const v of ['T', 't', 'Letter: T']) { s = O.suggestCharms({ value: v, line: necklace, skus: lib.keys() }); ok(s.some(x => x.sku === 'INITIAL_NECKLACE_SCRIPT-19') && s.some(x => x.sku === 'LETTER_EARRING-19'), v + ': ' + JSON.stringify(s)); }
  // never a look-alike by loose word matching: the design T_2800 (a "T" word) is not offered for the letter T, and KEY_5598 is not offered for any letter
  s = O.suggestCharms({ value: 'T', line: necklace, skus: skus }); ok(!s.some(x => x.sku === 'T_2800') && !s.some(x => /^KEY/.test(x.sku)), JSON.stringify(s));
  // a master SKU named exactly the character IS offered (a design called "T")
  ok(s.some(x => x.sku === 'T' && x.why === 'named exactly like the option'), JSON.stringify(s));
  // the hole: the stud family has no I, so it offers nothing for I while the other three do
  s = O.suggestCharms({ value: 'I', line: necklace, skus: skus }); ok(!s.some(x => x.sku.startsWith('INITIAL_LETTER_STUD_EARRINGS')) && s.some(x => x.sku === 'LETTER_EARRING-8'), JSON.stringify(s));
  // a digit is a value too: a SKU named "7" is found; numbers add no family
  lib.set('7', { sku: '7' }); s = O.suggestCharms({ value: '7', line: necklace, skus: lib.keys() }); eq(s.map(x => x.sku), ['7']); lib.delete('7');
  // the Etsy-own pick still comes first, then the letter
  s = O.suggestCharms({ value: 'T', line: necklace, picks: [{ sku: 'LETTER_EARRING-19', why: 'Etsy\'s own SKU for this option' }], skus: skus }); eq(s[0].sku, 'LETTER_EARRING-19'); eq(s[0].why, 'Etsy\'s own SKU for this option');
  // words of 3+ letters work exactly as before ("Pisces" finds PISCES_68933)
  lib.set('PISCES_68933', { sku: 'PISCES_68933' }); s = O.suggestCharms({ value: 'Pisces', line: necklace, skus: lib.keys() }); eq(s.map(x => x.sku), ['PISCES_68933']); lib.delete('PISCES_68933');
  step('suggestCharms keeps a one-character value, offers the letter\'s member of every letter family (the line\'s family first, no look-alike, the hole skipped), and reads words as before');
}

/* ── 7 · the row text, in the page's own code ── */
function rowText() {
  const bridge = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
  ok(/p\.kind === "needsMapping" \? \(p\.letter \? `option "\$\{p\.optionName\}: \$\{p\.optionValue\}" \\u2014 \$\{p\.letter\.why\}` : p\.pairSecond/.test(bridge), 'Review.problemText says the plain letter line first');
  ok(/p\.letter \? `<div class="why">\$\{esc\(p\.letter\.why\)\}<\/div>`/.test(bridge), 'the Needs-mapping card shows it too');
  step('the Review row reads: option "Initial: A" — no master design for letter A on this listing; the card shows the same line');
}

/* ── 8 · a clean reading of the old held row: the key row asks, the old wording is stale ── */
function oldWording() {
  const old = { state: 'held', hold: null, poolIds: [], reason: 'option "Initial: A" not mapped — Etsy\'s SKU list for listing 1099225598 is not loaded yet', spec: read('4196617547', keyLine('A')) };
  ok(!O.staleQuestionHold(old), 'the key row still has a question (a person says), so the stored row is not cleared as stale');
  step('the stored old-wording row is still a question, not stale: it waits for a person, with the new line');
}

values(); keyRow(); familyMember(); personsAnswer(); notLetters(); suggestions(); rowText(); oldWording();
console.log(`letter-options: ${n} checks passed`);
