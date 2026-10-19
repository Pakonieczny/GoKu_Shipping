// FALSEHOLD (Paul, 19 Oct 2026, orders 4207131438 / 4204710975): "Both of these orders got put on hold even though they were matched correctly from the charm repo and I could not find any issues.
// If something wasn't mapped, then please map it so I don't see these issues again … I don't want false holds."
//   · 4207131438: listing 1708211735 (angel number studs, listing B: Stud_1_Angel_0..9), SKU Stud_1_Angel_4, "Angel Number: 444", SS. It read as STUD_1_ANGEL_4 (Left + Right) but was held:
//     "option "Angel Number: 444" not mapped — Etsy keeps one SKU (ANGEL NUMBERS) for every choice of this listing: map this option once". The page's table of the listing was the old one
//     ({ uni: "ANGEL NUMBERS" }) from before Paul gave every variation its own SKU: stale.
//   · 4204710975: listing 982791277 (the bar necklace, BARS profile), SKU VERTICAL BAR_1277, GF, 3 bars, Sophie / Sam / Eddy. It read as 3 bars but was held on "Font/ Chain Length: stylish • 19 inches",
//     a font and a chain length, never a design.
// The real reader (charm-nest-orders.js interpretLine) on both lines as modelled from the orders, plus: a contradiction (444 with Stud_1_Angel_7) still asks, as a contradiction; a real design option on a
// listing with one SKU for every choice ("Animal: Cat") still asks; a line held only for the false question is released when it is read again (questionGone / staleQuestionHold), never one a person held.
//   node tests/charm-nest/false-option-holds.cjs      Offline: no browser, no network, no Etsy call.
'use strict';
const assert = require('assert'), path = require('path');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);
const kinds = sp => (sp.problems || []).map(p => p.kind);
const asks = sp => (sp.problems || []).filter(p => p.kind === 'needsMapping');

/* ── the library: the angel studs as the labelled file gives them (Stud_Angel_N and Stud_1_Angel_N one drawing), the bar necklace, an animal charm ── */
const F = f => 'charmnest/master/' + f + '.ai';
function world() {
  const lib = new Map(), put = (sku, file, e) => lib.set(sku.toUpperCase(), Object.assign({ sku: sku.toUpperCase(), aiPath: F(file), sizes: null, blocked: null }, e || {}));
  for (let d = 1; d <= 8; d++) { const stud = { widthPt: 30.6, heightPt: 17.6, members: 1, holes: 0, engravable: false }; put('Stud_Angel_' + d, 'STUD_ANGEL_' + d, stud); put('Stud_1_Angel_' + d, 'STUD_ANGEL_' + d, stud); }
  put('VERTICAL BAR_1277', 'VERTICAL_BAR_1277', { widthPt: 73.4, heightPt: 14 });
  put('ANIMAL_CHARM', 'ANIMAL_CHARM', { widthPt: 30, heightPt: 30, holes: 1 });
  const byFile = new Map(); for (const [k, e] of lib) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
  const loose = {}, like = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; (like[l] = like[l] || []).push(k); }
  return { masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: s => { const e = lib.get(String(s || '').toUpperCase()); return e ? byFile.get(e.aiPath) || [] : []; },
    masterLike: s => (like[O.looseKey(s)] || []).slice(), masterSkus: () => [...lib.keys()] };
}
const W = world();
const ctxOf = (tables, extra) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: tables || {} }, W, extra || {});
const read = (order, line, c) => O.interpretLine(order, JSON.parse(JSON.stringify(line)), c);
const pieces = (order, line, sp) => O.piecesOf({ order, line, spec: sp, poolIds: [] }).map(p => (p.side || '-') + ':' + (p.sku || sp.designSku));

/* ── 1 · order 4207131438: the angel stud, the stale "one SKU for every choice" table ── */
const LA = '1708211735', ORD1 = { receiptId: '4207131438', updateTs: 1, buyerMessage: '', staffNote: '' };
const TITLE1 = 'Angel Number Earrings 111 Studs 444 Earrings 888 Earrings Celestial Numbers Gold Angel Number 222 Studs Sterling Silver Numerology Jewelry';
const STALE = { [LA]: { at: Date.now() - 3 * 86400000, n: 20, uni: 'ANGEL NUMBERS' } };
const angel = (d, sku, over) => Object.assign({ transactionId: '4800000001', listingId: LA, productId: '19000000' + d, sku, title: TITLE1, quantity: 1, metalKey: 'silver', metalLabel: 'Sterling Silver', personalization: [], buyerMessage: '',
  variations: [{ name: 'Angel Number', value: String(d).repeat(3), propertyId: '514', valueId: String(2000 + d) }, { name: 'Metal', value: 'Sterling Silver', propertyId: '513', valueId: '1001' }] }, over || {});
{ const line = angel(4, 'Stud_1_Angel_4'), sp = read(ORD1, line, ctxOf(STALE));
  eq(kinds(sp), [], 'the angel stud of 4207131438 asks nothing: no "Etsy keeps one SKU (ANGEL NUMBERS) … map this option once"');
  eq([sp.designSku, sp.skuSource, sp.pieceCount, sp.pair && sp.pair.sides, pieces(ORD1, line, sp)], ['STUD_1_ANGEL_4', 'transaction', 2, ['L', 'R'], ['L:STUD_1_ANGEL_4', 'R:STUD_1_ANGEL_4']], 'STUD_1_ANGEL_4 on the Left and on the Right, as it read before');
  eq((sp.options.find(o => o.name === 'Angel Number') || {}).mapped, { field: 'design', value: 'STUD_1_ANGEL_4', source: 'sku:stale-table' }, 'the drop-down is answered by the line\'s own SKU (the table is stale)');
  eq([sp.staleTable.uni, sp.staleTable.sku], ['ANGEL NUMBERS', 'STUD_1_ANGEL_4'], 'the line says the table is stale, so the page asks Etsy for it again');
  ok(sp.material === 'silver', 'the metal is read as before (SS)'); }
// every drawn number, both stud listings' SKUs, in any case: the same
for (const d of [1, 2, 3, 5, 6, 7, 8]) for (const s of ['Stud_1_Angel_' + d, 'STUD_1_ANGEL_' + d, 'Stud_Angel_' + d]) {
  const sp = read(ORD1, angel(d, s), ctxOf(STALE)); ok(!kinds(sp).length && sp.designSku === s.toUpperCase(), `${String(d).repeat(3)} / ${s} on the stale table: ${sp.designSku}, nothing asked (${JSON.stringify(sp.problems)})`);
}
// a table whose one SKU is another spelling of the very design the transaction names is not stale (nothing changed on Etsy)
ok(!O.staleShared({ at: 1, n: 2, uni: 'stud 1 angel 4' }, angel(4, 'Stud_1_Angel_4'), W.masterEntry, W.masterLoose) && !!O.staleShared(STALE[LA], angel(4, 'Stud_1_Angel_4'), W.masterEntry, W.masterLoose), 'another spelling of the same design is not a stale table; "ANGEL NUMBERS" is');
pass('4207131438: Stud_1_Angel_4 with "Angel Number: 444" on a stale one-SKU table reads STUD_1_ANGEL_4 as a pair and asks nothing; the stale table is flagged for a fresh read');

/* ── 2 · a contradiction is a real question, said as one ── */
{ const sp = read(ORD1, angel(4, 'Stud_1_Angel_7'), ctxOf(STALE)), a = asks(sp);
  ok(a.length === 1 && a[0].optionValue === '444' && a[0].contradiction && a[0].contradiction.sku === 'STUD_1_ANGEL_7', 'option 444 with SKU Stud_1_Angel_7: a question (' + JSON.stringify(sp.problems) + ')');
  ok(/contradict/.test(a[0].why) && /444/.test(a[0].why) && /STUD_1_ANGEL_7/.test(a[0].why) && !/keeps one SKU/.test(a[0].why), 'said as a contradiction: ' + a[0].why);
  const nt = read(ORD1, angel(4, 'Stud_1_Angel_7'), ctxOf({})), b = asks(nt);
  ok(b.length === 1 && b[0].contradiction && /contradict/.test(b[0].why), 'without a table too: ' + (b[0] || {}).why); }
pass('option 444 with SKU Stud_1_Angel_7 still asks, as a contradiction (stale table or none)');

/* ── 3 · no table at all: the value names the SKU's member of its family (444 ↔ _4), nothing says the SKU is shared: answered ── */
{ const sp = read(ORD1, angel(4, 'Stud_1_Angel_4'), ctxOf({}));
  eq([kinds(sp), sp.designSku, (sp.options.find(o => o.name === 'Angel Number') || {}).mapped], [[], 'STUD_1_ANGEL_4', { field: 'design', value: 'STUD_1_ANGEL_4', source: 'sku:number' }], 'no table: 444 is STUD_1_ANGEL_4\'s own number, nothing asked');
  // the page's other orders show the same SKU on another choice of the option: it IS shared, the question stays
  const other = angel(5, 'Stud_1_Angel_4', { transactionId: '4800000002' }), seen = O.seenSkus([angel(4, 'Stud_1_Angel_4'), other]);
  ok(asks(read(ORD1, angel(4, 'Stud_1_Angel_4'), ctxOf({}, { seen }))).length === 1, 'the same SKU on 555 elsewhere on the page: shared, still asked');
  // a table that is NOT stale and says the SKU is every choice's: still asked
  ok(asks(read(ORD1, angel(4, 'Stud_1_Angel_4'), ctxOf({ [LA]: { at: Date.now(), n: 20, uni: 'Stud_1_Angel_4' } }))).length === 1, 'Etsy\'s table says Stud_1_Angel_4 is every choice\'s SKU: still asked'); }
pass('without a table the number in the value answers the drop-down when it is the SKU\'s; a SKU shown to be shared still asks');

/* ── 4 · order 4204710975: the bar necklace, "Font/ Chain Length: stylish • 19 inches" ── */
const LB = '982791277', SKUB = 'VERTICAL BAR_1277', ORD2 = { receiptId: '4204710975', updateTs: 1 };
const TITLE2 = 'Skinny MINI Vertical Bar Necklace with Heart, Personalized Long Name Gold Bar Necklace, Personalized Bar Necklace, Gold Bar, Vertical Bar';
const SHARED_BAR = { [LB]: { at: Date.now(), n: 45, uni: SKUB } };
const bar = (font, over) => Object.assign({ transactionId: '5004710975', listingId: LB, productId: '12345', sku: SKUB, title: TITLE2, quantity: 1, metalKey: 'gold', metalLabel: 'Gold Filled', personalization: ['Bar names:\nSophie\nSam\nEddy'], buyerMessage: '',
  variations: [{ name: 'Engraving & # of Bars?', value: 'gold • 3 bars ($45.60)', propertyId: '600', valueId: '6003' }, { name: 'Font/ Chain Length', value: font, propertyId: '601', valueId: '6019' }] }, over || {});
for (const tables of [SHARED_BAR, {}]) {
  const line = bar('stylish • 19 inches'), sp = read(ORD2, line, ctxOf(tables)), tag = Object.keys(tables).length ? 'with the one-SKU table' : 'with no table';
  eq(kinds(sp), [], `${tag}: nothing asked (no "Font/ Chain Length … not mapped")`);
  eq([sp.designSku, sp.material, sp.pieceCount, sp.pair.noun], [SKUB, 'gold', 3, 'bar'], `${tag}: VERTICAL BAR_1277, GF, 3 bars`);
  eq(sp.pieceProfile.words, { D1: 'Sophie', D2: 'Sam', D3: 'Eddy' }, `${tag}: Sophie / Sam / Eddy on bars 1 / 2 / 3`);
  const fo = sp.options.find(o => o.name === 'Font/ Chain Length');
  eq([fo.mapped.field, fo.mapped.value, fo.mapped.source], ['chain', '19 inches', 'rule:non-design'], `${tag}: the option is the chain length (and the font), never a design`);
  eq([sp.chain, sp.font && sp.font.id], ['19 inches', 'playwrite-us-trad'], `${tag}: chain 19 inches, font Stylish`);
}
// the other ways a shop writes such an option: still never a design question
for (const [name, value] of [['Font/ Chain Length', 'Typewriter / 16"'], ['Font & Length', 'Pristina - 18 inch'], ['Metal / Length', 'Gold Filled • 20"'], ['Chain Length + Font', '18" · Stylish'], ['Font, Chain length', 'stylish, 19 in'],
  ['Engraving Side', 'Front only'], ['Text Alignment', 'Center'], ['Choose your Font', 'Stylish'], ['Necklace Length', '19 inches long'], ['Option', 'stylish • 19 inches']]) {
  const line = bar('x', { variations: [{ name: 'Engraving & # of Bars?', value: 'gold • 3 bars', propertyId: '600', valueId: '6003' }, { name, value, propertyId: '601', valueId: '6020' }] }), sp = read(ORD2, line, ctxOf(SHARED_BAR));
  ok(!asks(sp).length, `"${name}: ${value}" is not a design option (${JSON.stringify(sp.problems)})`);
}
// the kinds, read alone
eq(O.nonDesignOption('Font/ Chain Length', 'stylish • 19 inches').kinds, ['font', 'length'], '"Font/ Chain Length" is a font and a length');
ok(O.nonDesignOption('Zodiac & Length', 'Leo • 18"') === null, '"Zodiac & Length: Leo • 18"" may pick a design: not taken as non-design');
ok(O.nonDesignOption('Birth Flower', 'Rose') === null && O.nonDesignOption('Animal', 'Cat') === null && O.nonDesignOption('Angel Number', '444') === null, 'a flower, an animal, an angel number: design options');
ok(O.nonDesignOption('Style', 'Ace') === null, 'a lone font-like word under a name that says nothing is not enough');
// a profile's own drop-down that cannot be read still asks, with its own plain line
{ const sp = read(ORD2, bar('stylish • 19 inches', { variations: [{ name: 'Engraving & # of Bars?', value: 'gold • 6 bars ($66.00)' }, { name: 'Font/ Chain Length', value: 'stylish • 19 inches' }] }), ctxOf(SHARED_BAR)), a = asks(sp);
  ok(a.length === 1 && a[0].optionName === 'Engraving & # of Bars?' && /1 to 5/.test(a[0].why), 'a sixth bar still asks, in the profile\'s words: ' + JSON.stringify(a)); }
pass('4204710975: "Font/ Chain Length: stylish • 19 inches" is the chain and the font, never asked; 3 bars, Sophie / Sam / Eddy; compound names and values of fonts, lengths, metals, faces read the same');

/* ── 5 · a real design option on a listing with one SKU for every choice still asks ── */
{ const LC = '1555000001', line = { transactionId: '1', listingId: LC, productId: '77', sku: 'ANIMAL_CHARM', title: 'Animal Charm Necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], buyerMessage: '',
    variations: [{ name: 'Animal', value: 'Cat', propertyId: '700', valueId: '7001' }, { name: 'Metal', value: 'Gold', propertyId: '701', valueId: '7011' }] };
  const sp = read({ receiptId: '9', updateTs: 1 }, line, ctxOf({ [LC]: { at: Date.now(), n: 10, uni: 'ANIMAL_CHARM' } })), a = asks(sp);
  ok(a.length === 1 && a[0].optionName === 'Animal' && /keeps one SKU \(ANIMAL_CHARM\)/.test(a[0].why) && !sp.staleTable, '"Animal: Cat" on a listing with one SKU for every choice: asked as before (' + JSON.stringify(a) + ')');
  const nt = read({ receiptId: '9', updateTs: 1 }, line, ctxOf({}));
  ok(asks(nt).length === 1, 'and with no table: asked as before'); }
pass('"Animal: Cat" with one SKU (ANIMAL_CHARM) for every choice still asks to map the option');

/* ── 6 · a line held only for the false question goes back to its normal flow when read again; a person's hold, another question, pieces made are left alone ── */
{ const line = angel(4, 'Stud_1_Angel_4');
  const before = { problems: [{ kind: 'needsMapping', listingId: LA, optionName: 'Angel Number', optionValue: '444', why: 'Etsy keeps one SKU (ANGEL NUMBERS) for every choice of this listing: map this option once' }] };
  const now = read(ORD1, line, ctxOf(STALE));
  const held = { key: '4207131438_4800000001', order: ORD1, line, spec: now, problems: now.problems.slice(), state: 'held', reason: 'option "Angel Number: 444" not mapped — Etsy keeps one SKU (ANGEL NUMBERS) for every choice of this listing: map this option once', hold: null, poolIds: [] };
  ok(O.questionGone(held, before), 'the held angel stud, read again, goes back to the intake (questionGone)');
  ok(O.staleQuestionHold(Object.assign({}, held, { fromRecord: true })), 'and its stored record (staleQuestionHold)');
  ok(O.staleQuestionHold(Object.assign({}, held, { fromRecord: true, reason: 'Options: option "Angel Number: 444" not mapped' })), 'the record\'s "Options: …" reason too');
  ok(!O.questionGone(Object.assign({}, held, { hold: 'held by Paul' }), before) && !O.staleQuestionHold(Object.assign({}, held, { hold: 'held by Paul' })), 'a line a person put on hold is never released');
  ok(!O.questionGone(Object.assign({}, held, { poolIds: ['x_1'] }), before), 'a line with pieces made is left alone');
  const still = read(ORD1, angel(4, 'Stud_1_Angel_7'), ctxOf(STALE));
  ok(!O.questionGone(Object.assign({}, held, { spec: still, problems: still.problems.slice() }), before), 'a line that still asks (the contradiction) stays held');
  ok(!O.staleQuestionHold(Object.assign({}, held, { reason: 'the design could not be loaded' })), 'a line held for another reason is not released');
  const barNow = read(ORD2, bar('stylish • 19 inches'), ctxOf(SHARED_BAR));
  ok(O.questionGone({ state: 'held', spec: barNow, problems: [], hold: null, poolIds: [] }, { problems: [{ kind: 'needsMapping', optionName: 'Font/ Chain Length', optionValue: 'stylish • 19 inches' }] }), 'the held bar necklace goes back to the intake too'); }
pass('a line held only for the false option question is released on its next reading; a person\'s hold, a remaining question, pieces made and another reason are left alone');

/* ── 7 · the stale table is asked for again: the cloud asks Etsy once more for a listing the page names in refreshIds (stored more than ten minutes ago), never otherwise; the page asks once per listing ── */
(async () => {
  const L = require(path.join(__dirname, '../../netlify/functions/_charmNestListingSkus.js')), fs = require('fs');
  const store = new Map(), asked = [], now = Date.now();
  const ref = id => ({ id, async get() { const d = store.get(id); return { exists: !!d, data: () => d && JSON.parse(JSON.stringify(d)) }; }, async set(d) { store.set(id, JSON.parse(JSON.stringify(d))); } });
  const db = { collection: () => ({ doc: ref }), async getAll(...refs) { return Promise.all(refs.map(r => r.get())); } };
  const inv = { products: [4, 5].map(d => ({ product_id: 190 + d, sku: 'Stud_1_Angel_' + d, is_deleted: false, property_values: [{ property_id: 514, value_ids: [2000 + d], values: [String(d).repeat(3)] }] })) };
  const env = extra => Object.assign({ db, now, fetchInventory: async id => { asked.push(id); return inv; } }, extra);
  store.set(LA, { at: now - 3 * 86400000, n: 20, uni: 'ANGEL NUMBERS' });
  let r = await L.lookup([LA], env());
  ok(asked.length === 0 && r.tables[LA].uni === 'ANGEL NUMBERS', 'a stored table under a week old is not asked of Etsy again without a reason');
  r = await L.lookup([LA], env({ refreshIds: [LA] }));
  ok(asked.length === 1 && Array.isArray(r.tables[LA].products) && r.tables[LA].products.map(p => p.sku).join() === 'Stud_1_Angel_4,Stud_1_Angel_5', 'refreshIds: the stale one-SKU table is asked of Etsy again and the per-variation SKUs come back');
  r = await L.lookup([LA], env({ refreshIds: [LA] }));
  ok(asked.length === 1, 'a table stored less than ten minutes ago is not asked again, refreshIds or not (never a loop)');
  // and the line reads the fresh table: the drop-down is tied to its own SKU, nothing is stale any more
  const sp = read(ORD1, angel(4, 'Stud_1_Angel_4'), ctxOf({ [LA]: r.tables[LA] }));
  ok(!kinds(sp).length && !sp.staleTable && (sp.options.find(o => o.name === 'Angel Number') || {}).mapped.source === 'sku', 'with the fresh table the drop-down is tied to Stud_1_Angel_4 by Etsy itself');
  const bridge = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8'), lib = fs.readFileSync(path.join(__dirname, '../../netlify/functions/charmNestLibrary.js'), 'utf8');
  ok(/const tableRefreshed = new Set\(\)/.test(bridge) && /refreshIds: rf/.test(bridge) && /tableRefreshed\.add\(id\)/.test(bridge), 'the page asks for a stale table again, once per listing (askTables, refreshIds)');
  ok(/if \(O\.questionGone && O\.questionGone\(row, prev\)\) \{ row\.state = "pulled"/.test(bridge) && /else if \(row\.fromRecord && !prev && [^\n]*option "[^\n]*O\.staleQuestionHold\(row\)\) \{ row\.state = "pulled"; row\.reason = null; \}/.test(bridge), 'every reading releases a line held for an option question it no longer raises (a live line: questionGone; a record\'s line: staleQuestionHold, option questions only)');
  ok(/refreshIds = Array\.isArray\(b\.refreshIds\)/.test(lib) && /lookup\(ids, \{ db, nameIds, refreshIds,/.test(lib), 'the cloud passes refreshIds on');
  const html = fs.readFileSync(path.join(__dirname, '../../charm-nest-1.html'), 'utf8');
  for (const f of ['charm-nest-orders.js', 'charm-nest-bridge.js']) { const m = new RegExp('src="' + f.replace(/\./g, '\\.') + '\\?v=([^"]*)"').exec(html); ok(m && /-fh1(?:-|$)/.test(m[1]), f + ' cache token carries -fh1'); }
  pass('the stale table is asked of Etsy again (refreshIds, once, older than ten minutes), and the fresh one ties the drop-down to its SKU');
  console.log(`false-option-holds: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
