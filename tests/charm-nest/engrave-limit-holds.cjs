// ENGLIMIT (Paul, 9 Nov 2026, order 4218451815): "Same with this order, the 1-3 characters [is] just for how many characters can be added as engraving to the bar."
//   · listing 531495758 "TINY Gold Bar Necklace, Engraved Horizontal Bar Necklace, …", SKU TINY_5758, GF, "Number of Characters: GOLD 1-3 Characters", personalisation "D (cross) E".
//     It was held: option "Number of Characters: GOLD 1-3 Characters" not mapped — Etsy keeps one SKU (TINY_5758) for every choice of this listing: map this option once.
//     Root cause: nameKind read "Number of …" as a COUNT (FALSEHOLD never passes a count) and valueKind read neither the metal nor the character range, so the option fell to the one-SKU question.
//   · TINY_5758 is in no master file (the live master index, read 9 Nov 2026: no SKU holds "5758" but RUNNING_57581). The open option question hid that ("an unknown SKU waits while an option is unanswered"):
//     with the option read, the line says the real reason, "not in any master file", and nothing is matched by guesswork.
// The real reader (charm-nest-orders.js interpretLine), offline: no browser, no network, no Etsy call.
//   node tests/charm-nest/engrave-limit-holds.cjs
'use strict';
const assert = require('assert'), path = require('path'), fs = require('fs');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);
const kinds = sp => (sp.problems || []).map(p => p.kind);
const asks = sp => (sp.problems || []).filter(p => p.kind === 'needsMapping');

function world(skus) {
  const lib = new Map(); for (const s of skus) lib.set(s.toUpperCase(), { sku: s.toUpperCase(), aiPath: 'charmnest/master/' + s + '.ai', sizes: null, blocked: null, widthPt: 45, heightPt: 11 });
  const loose = {}, like = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; (like[l] = like[l] || []).push(k); }
  return { masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: s => lib.has(String(s || '').toUpperCase()) ? [String(s).toUpperCase()] : [],
    masterLike: s => (like[O.looseKey(s)] || []).slice(), masterSkus: () => [...lib.keys()] };
}
const LID = '531495758', SKU = 'TINY_5758', ORD = { receiptId: '4218451815', updateTs: 1 };
const TITLE = 'TINY Gold Bar Necklace, Engraved Horizontal Bar Necklace, Initial necklace, Personalized necklace, Engraved, Initial Bar, Bridesmaid Gift';
const ONE_SKU = { [LID]: { at: Date.now(), n: 6, uni: SKU } };
const ctxOf = (W, tables) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: tables || {} }, W);
const bar = over => Object.assign({ transactionId: '5218451815', listingId: LID, productId: '9001', sku: SKU, title: TITLE, quantity: 1, metalKey: 'gold', metalLabel: 'Gold Filled', personalization: ['D (cross) E'], buyerMessage: '',
  variations: [{ name: 'Number of Characters', value: 'GOLD 1-3 Characters', propertyId: '700', valueId: '7001' }] }, over || {});
const read = (line, c) => O.interpretLine(ORD, JSON.parse(JSON.stringify(line)), c);
const WITH = world([SKU, 'TINY_4758']), LIVE = world(['TINY_4758', 'TINY_7557', 'TINY GOLD BAR_0090', 'TINY BAR_7427']);

/* ── 1 · order 4218451815 with TINY_5758 in a master: nothing asked, the limit read as a check ── */
for (const tables of [ONE_SKU, {}]) {
  const tag = Object.keys(tables).length ? 'with Etsy\'s one-SKU table' : 'with no table', sp = read(bar(), ctxOf(WITH, tables));
  eq(kinds(sp), [], `${tag}: nothing asked (no "Number of Characters … not mapped"): ${JSON.stringify(sp.problems)}`);
  eq([sp.designSku, sp.material, sp.pieceCount], [SKU, 'gold', 1], `${tag}: TINY_5758, GF, one bar`);
  eq(sp.options[0].mapped, { field: 'ignore', value: null, source: 'rule:engrave-limit' }, `${tag}: the option is the words limit, never a design`);
  const L = sp.engraveLimit;
  eq([L.min, L.max, L.unit, L.metal, L.used, L.over, !!L.metalDiffers], [1, 3, 'character', 'gold', 3, false, false], `${tag}: up to 3 characters, gold, "D (cross) E" is 3`);
  ok(L.line === 'Up to 3 characters · engraving “D (cross) E” = 3 characters (a bracketed symbol word counts as one sign) ✓', `${tag}: the line says the check: ${L.line}`);
}
pass('4218451815: "Number of Characters: GOLD 1-3 Characters" on TINY_5758 is never asked; "Up to 3 characters · engraving “D (cross) E” = 3 characters ✓"');

/* ── 2 · the live master (no TINY_5758): the one real reason is said, nothing is matched by guesswork ── */
{ const sp = read(bar(), ctxOf(LIVE, ONE_SKU)), un = (sp.problems || []).filter(p => p.kind === 'unmatchedSku');
  ok(!asks(sp).length, 'no option question once the limit is read');
  ok(un.length === 1 && un[0].sku === SKU && un[0].reason === 'not in any master file', 'the line says TINY_5758 is in no master file (the "!" on the SKU and "No vector available"): ' + JSON.stringify(sp.problems));
  ok(sp.designSku === SKU, 'TINY_5758 stays the SKU: no near SKU (TINY_4758) is put in its place');
  // before ENGLIMIT this was hidden behind the option question: an open option still hides it (a real design option may pick the charm)
  const design = read(bar({ variations: [{ name: 'Animal', value: 'Cat' }] }), ctxOf(LIVE, ONE_SKU));
  ok(asks(design).length === 1 && !design.problems.some(p => p.kind === 'unmatchedSku'), 'a real design option still asks first, as before'); }
pass('with the live master (no TINY_5758): "not in any master file" is the line\'s one reason; no look-alike is matched');

/* ── 3 · the other ways a shop writes the limit: never a design, never a count, read as the limit ── */
for (const [name, value, want] of [
  ['Number of Characters', 'GOLD 1-3 Characters', [1, 3, 'character', 'gold']], ['Characters', 'Up to 3 letters', [1, 3, 'letter', '']], ['Letters', 'Up to 3 letters', [1, 3, 'letter', '']],
  ['How many letters?', '4-6 characters', [4, 6, 'character', '']], ['How many initials', 'Up to 3 initials', [1, 3, 'initial', '']], ['Character count', 'Silver 1-3 characters', [1, 3, 'character', 'silver']],
  ['Engraving length', 'Max 10 characters', [1, 10, 'character', '']], ['Lines of text', '2 lines', [1, 2, 'line', '']], ['Number of lines', '2', [1, 2, 'line', '']],
  ['Initials', '1 initial', [1, 1, 'initial', '']], ['Number of Initials', '1 Initial', [1, 1, 'initial', '']], ['Number of Characters', 'GOLD 1-3 Characters (+$5.00)', [1, 3, 'character', 'gold']],
  ['Option', '7-10 Characters - Rose Gold', [7, 10, 'character', 'rose']], ['Text', '1-12 characters', [1, 12, 'character', '']], ['Characters', 'GOLD · 4-6 Characters', [4, 6, 'character', 'gold']]]) {
  const r = O.engraveLimitRead(name, value); ok(r && JSON.stringify([r.min, r.max, r.unit, r.metal]) === JSON.stringify(want), `"${name}: ${value}" reads as ${JSON.stringify(want)}: ${JSON.stringify(r)}`);
  ok(O.optionCount(name, value) === null, `"${name}: ${value}" is no count of pieces`);
  const sp = read(bar({ variations: [{ name, value, propertyId: '700', valueId: '7002' }], personalization: ['D'] }), ctxOf(WITH, ONE_SKU));
  ok(!kinds(sp).length && sp.engraveLimit && sp.engraveLimit.max === want[1], `"${name}: ${value}" holds nothing and is the line's limit (${JSON.stringify(sp.problems)})`);
}
// a limit beside a font: the font is read as before and the limit too
{ const sp = read(bar({ variations: [{ name: 'Font / Characters', value: 'Stylish • 1-3 characters' }], personalization: ['D E'] }), ctxOf(WITH, ONE_SKU));
  ok(!kinds(sp).length && sp.font && sp.font.id === 'playwrite-us-trad' && sp.engraveLimit && sp.engraveLimit.max === 3 && sp.engraveLimit.used === 2, 'font and limit in one option: Stylish, up to 3 characters, "D E" = 2: ' + JSON.stringify([sp.problems, sp.font, sp.engraveLimit])); }
eq(O.nonDesignOption('Number of Characters', 'GOLD 1-3 Characters').kinds, ['limit'], 'nonDesignOption: "Number of Characters: GOLD 1-3 Characters" is a limit');
pass('every phrasing of an engraving limit (names, values, a metal, a price, a font beside it) is the line\'s limit, never a design and never a count');

/* ── 4 · what is NOT a limit stays as it was ── */
for (const [name, value] of [['Number of Letters', '3'], ['Initials', '3 Initials'], ['Number of Discs', '1-3 discs'], ['Character', 'Mickey'], ['Initial', 'A'], ['Letter', 'Rose'], ['Style', '1 Initial'], ['Animal', 'Cat']])
  ok(O.engraveLimitRead(name, value) === null, `"${name}: ${value}" is no engraving limit`);
ok(O.optionCount('Number of Letters', '3').ask && O.optionCount('Initials', '3 Initials').ask, 'a bare count of letters / initials is still the count question (separate pieces, or all on one?)');
ok(O.nonDesignOption('Character', 'Mickey') === null && O.nonDesignOption('Initial', 'A') === null, 'a character or an initial named in the value may pick a design: not taken as non-design');
pass('a bare count of letters, a range of discs, a character, an initial or a letter is not a limit: read as before');

/* ── 5 · an engraving longer than the limit: one visible note, never a hold; a metal that disagrees: said too ── */
{ const sp = read(bar({ personalization: ['Elizabeth'] }), ctxOf(WITH, ONE_SKU)), L = sp.engraveLimit;
  eq(kinds(sp), [], 'a longer engraving holds nothing');
  eq([L.used, L.over], [9, true], '"Elizabeth" is 9 characters, over 3');
  ok(L.line === 'Up to 3 characters · engraving “Elizabeth” = 9 characters: longer than the 3 characters the buyer paid for', 'the note: ' + L.line); }
{ const sp = read(bar({ personalization: ['D (heart) E ♡'] }), ctxOf(WITH, ONE_SKU));
  ok(sp.engraveLimit.used === 4 && sp.engraveLimit.over, 'a bracketed symbol word and a typed sign count one each: "D (heart) E ♡" = 4'); }
{ const sp = read(bar({ metalKey: 'silver', metalLabel: 'Sterling Silver' }), ctxOf(WITH, ONE_SKU)), L = sp.engraveLimit;
  ok(!asks(sp).length && L.metalDiffers && /the option says gold, the line is SS: check the metal/.test(L.line), 'GOLD in the option on an SS line is said: ' + L.line); }
{ const sp = read(bar({ metalKey: '14k', metalLabel: '14K Gold' }), ctxOf(WITH, ONE_SKU));
  ok(!sp.engraveLimit.metalDiffers, 'GOLD on a 14K gold line agrees'); }
{ const sp = read(bar({ variations: [{ name: 'Lines of text', value: '1 line' }], personalization: ['Mom\nDad'] }), ctxOf(WITH, ONE_SKU));
  ok(!kinds(sp).length && sp.engraveLimit.used === 2 && sp.engraveLimit.over && /2 lines: longer than the 1 line/.test(sp.engraveLimit.line), 'two lines on a one-line limit: said, not held: ' + sp.engraveLimit.line); }
eq([O.limitLength('D (cross) E', 'character'), O.limitLength('D † E', 'character'), O.limitLength('A.B', 'letter'), O.limitLength('Mom\n\nDad', 'line')], [3, 3, 2, 2], 'limitLength');
pass('an engraving longer than the paid limit (or a metal that disagrees) is one plain note on the line, never a hold');

/* ── 6 · a line held only for the limit question is released when it is read again ── */
{ const sp = read(bar(), ctxOf(WITH, ONE_SKU));
  const held = { key: '4218451815_5218451815', order: ORD, line: bar(), spec: sp, problems: [], state: 'held', hold: null, poolIds: [], fromRecord: true,
    reason: 'Options: option "Number of Characters: GOLD 1-3 Characters" not mapped — Etsy keeps one SKU (TINY_5758) for every choice of this listing: map this option once' };
  ok(O.staleQuestionHold(held), 'the stored "Options: … not mapped" hold is released on its next reading');
  ok(!O.staleQuestionHold(Object.assign({}, held, { hold: 'held by Paul' })), 'a person\'s hold stays');
  const live = read(bar(), ctxOf(LIVE, ONE_SKU));
  ok(!O.staleQuestionHold(Object.assign({}, held, { spec: live })), 'with no TINY_5758 in the master the line stays held, now for the real reason'); }
pass('a line held only for the limit option is released; one whose SKU is in no master stays held for that');

/* ── 7 · the page shows the check, and the cache tokens carry -fh4 ── */
{ const bridge = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8'), html = fs.readFileSync(path.join(__dirname, '../../charm-nest-1.html'), 'utf8');
  ok(/cell\("Engraving limit", sp\.engraveLimit\.line\)/.test(bridge), 'the order window lists the limit check');
  ok(/sp\.engraveLimit\.over \|\| sp\.engraveLimit\.metalDiffers\) && sp\.engraveLimit\.line\) wants\.push/.test(bridge), 'the Engrave card says a longer engraving beside the words');
  for (const f of ['charm-nest-orders.js', 'charm-nest-bridge.js']) { const m = new RegExp('src="' + f.replace(/\./g, '\\.') + '\\?v=([^"]*)"').exec(html); ok(m && /-fh4(?:-|$)/.test(m[1]) && /-fh1(?:-|$)/.test(m[1]), f + ' cache token carries -fh4 (and keeps -fh1)'); } }
pass('the order window and the Engrave card show the check; the cache tokens carry -fh4');
console.log(`engrave-limit-holds: ${n} checks passed`);
