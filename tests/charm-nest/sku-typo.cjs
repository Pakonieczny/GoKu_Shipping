// SKUTYPO: the one SKU Etsy carries misspelled, "nitial_Disc_4571" (the I is missing) on ALL 180 products of the initial disc listing 1008014571, is read as the
// master design INITIAL_DISC_4571 (Paul, 10 Oct 2026: "adapt the system to understand all 180 as they currently are"). Offline: no Etsy, no live read, no write, no paid call.
//   node tests/charm-nest/sku-typo.cjs
// What it proves (charm-nest-orders.js KNOWN_SKU_TYPOS / knownTypo / masterSku / resolveSku; the bridge, the search and the Design Station read the same table):
//   · ONE definition: a table of one exact entry, compared as every SKU is (case, spacing and punctuation set aside, nothing fuzzier); no other source file spells it
//   · order 4175370240 (INITIAL_DISC_4571, Necklace, ROSEGOLD - 2 Disc, Fonts 16"/ Typewriter; the real line of the Sep 17 snapshot, pairs-fixtures EXAMPLES.discs2) reads as
//     INITIAL_DISC_4571 with no question, EXACTLY as the same line carrying the correct SKU does (form, family, size, chain, disc count, font option, questions)
//   · every one of the listing's 180 products: Etsy keeps ONE SKU for the whole listing (compact() stores it as { n: 180, uni }); a line for each of 180 option combinations
//     (INVENTED values on the real shape: two options, Necklace Options x Fonts), with the transaction's SKU as Etsy sends it and with none at all, reads as INITIAL_DISC_4571
//   · a different misspelling (nitial_Disc_9999, NITIAL_8391, a letter lost elsewhere, an extra letter, -CO of the typo, a huggie set of it) still waits as "not in any master file"
//   · the master must hold the right design (without it the line waits as before); a person's saved answer (a SKU alias, a listing's alias, an option map) still wins
//   · nothing stored is rewritten: the inputs are frozen, a read leaves them as they were; a line whose SKU is not exactly this typo reads exactly as before
//   · the other places that turn a SKU into a design read the same table: the bridge's re-read signature, the order search, the Design Station's live thumbnails
const assert = require('assert');
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require('../../charm-nest-orders.js');
const Skus = require('../../netlify/functions/_charmNestListingSkus.js');
const F = require('./pairs-fixtures.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const deepFreeze = x => { if (x && typeof x === 'object' && !Object.isFrozen(x)) { Object.freeze(x); for (const k of Object.keys(x)) deepFreeze(x[k]); } return x; };

const DATA = JSON.parse(fs.readFileSync(path.join(__dirname, 'options-link-data.json'), 'utf8'));
const LISTING = '1008014571', TYPO = 'nitial_Disc_4571', GOOD = 'INITIAL_DISC_4571';

// ── the master: the designs the cases need (what the live index holds under these names), read the way the page reads it (Master.entryFor, Master.looseFor) ──
const MASTER = new Map(['INITIAL_DISC_4571', 'INITIAL_DISC_9999', 'INITIAL_8391', 'OTHER_DESIGN_1', 'OTHER_DESIGN_2', 'RUNE_NECKLACE_CHARM-14', 'NITIAL_FAKE'].map(s => [s, { sku: s }]));
const entryFor = s => MASTER.get(String(s || '').toUpperCase()) || null;
const looseFor = s => { const k = O.looseKey(s); let hit = ''; for (const m of MASTER.keys()) if (O.looseKey(m) === k) hit = hit ? '' : m; return hit; };
const order = rid => deepFreeze({ receiptId: String(rid), updateTs: 0 });
const ctx = extra => deepFreeze(Object.assign({ optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: looseFor }, extra || {}));
const read = (rid, line, c) => O.interpretLine(order(rid), deepFreeze(JSON.parse(JSON.stringify(line))), c || ctx());
const problems = sp => sp.problems.map(p => p.kind + (p.reason ? ':' + p.reason : ''));
const stripSource = sp => { const c = JSON.parse(JSON.stringify(sp)); delete c.skuSource; delete c.boughtSku; delete c.sources; return c; };

// ── 1 · one definition, one exact entry, compared as every SKU is ─────────────────────────────────────────────────────────────────────────
eq(Object.keys(O.KNOWN_SKU_TYPOS), ['NITIAL_DISC_4571'], 'the table has ONE entry');
eq(O.KNOWN_SKU_TYPOS.NITIAL_DISC_4571, 'INITIAL_DISC_4571', 'it maps the typo to the master design');
ok(Object.isFrozen(O.KNOWN_SKU_TYPOS), 'the table cannot be changed at run time');
for (const s of ['nitial_Disc_4571', 'NITIAL_DISC_4571', '  nitial_disc_4571 ', 'Nitial-Disc-4571', 'nitial disc 4571', 'NITIALDISC4571', 'nitial.Disc/4571']) eq(O.knownTypo(s), GOOD, `${JSON.stringify(s)}: the typo, compared as every SKU is (case, spacing and punctuation set aside)`);
for (const s of ['INITIAL_DISC_4571', 'nitial_Disc_9999', 'NITIAL_8391', 'NITIAL_DISC_457', 'NITIAL_DISC_45711', 'NITIAL_DISC_4572', 'ITIAL_DISC_4571', 'INITIA_DISC_4571', 'NIITIAL_DISC_4571', 'NITIAL_DISC_4571-CO', 'NITIAL_DISC_4571 (HUGGIE)', 'TACO', '', null, undefined])
  eq(O.knownTypo(s), '', `${JSON.stringify(s)}: not the typo, nothing fuzzier (no rule that guesses a missing letter)`);
// no other source file spells it: the sorter, the bridge, the search and the server read this one table
{ const skip = new Set(['node_modules', '.git', 'tests', 'plans', 'public-site', 'cherry-viewer-dist', 'netlify/production-functions']);
  const found = [];
  const walk = d => { for (const f of fs.readdirSync(path.join(root, d), { withFileTypes: true })) {
    const rel = d ? d + '/' + f.name : f.name;
    if (f.isDirectory()) { if (!skip.has(f.name) && !skip.has(rel) && !['vendor', 'seeds', 'assets', 'docs', 'investor', 'cherry-viewer', 'shopify'].includes(f.name)) walk(rel); }
    else if (/\.(js|cjs|mjs|html)$/.test(f.name) && fs.readFileSync(path.join(root, rel), 'utf8').match(/(?<![a-z])nitial[_ .\-]*disc[_ .\-]*4571/i)) found.push(rel);
  } };
  walk('');
  eq(found, ['charm-nest-orders.js'], 'only charm-nest-orders.js spells the typo (the listing id and the master SKU are written by the listing, not here)'); }

// ── 2 · order 4175370240, the real line ────────────────────────────────────────────────────────────────────────────────────────────────
const EX = F.EXAMPLES.discs2;
eq([String(EX.listing), EX.sku, EX.orders], [LISTING, TYPO, ['4175370240']], 'pairs-fixtures EXAMPLES.discs2 names the same listing, SKU and order');
const real = DATA.lines.find(l => l.receipt === '4175370240');
ok(real && real.listing === LISTING && real.sku === TYPO, 'order 4175370240 is in the Sep 17 snapshot lines with the typo SKU');
const lineOf = (x, over) => Object.assign({ transactionId: x.tx, listingId: x.listing, productId: x.product || '', sku: x.sku, title: x.title, quantity: x.quantity || 1, metalKey: 'rose', metalLabel: 'Rose Gold', personalization: [],
  variations: x.variations.map(v => Object.assign({}, v)) }, over || {});
const L = lineOf(real);
const before = JSON.stringify(L);
{ // without the right design in the master the line waits, as it did (the alias stands only for a design the master really holds)
  const no = new Map(MASTER); no.delete('INITIAL_DISC_4571');
  const sp0 = read('4175370240', L, ctx({ masterEntry: s => no.get(String(s).toUpperCase()) || null, masterLoose: () => '' }));
  eq(problems(sp0), ['unmatchedSku:not in any master file'], 'the master without INITIAL_DISC_4571: the line still waits "not in any master file"');
  eq([sp0.designSku, sp0.skuSource], ['NITIAL_DISC_4571', 'transaction'], 'and keeps the SKU as Etsy wrote it'); }
const sp = read('4175370240', L);
eq([sp.designSku, sp.skuSource], [GOOD, 'typo'], 'order 4175370240 reads as INITIAL_DISC_4571 (source: typo)');
eq(problems(sp), [], 'no question: not "unmatchedSku", not "needsMapping" (the font and the disc count were already read)');
const good = read('4175370240', Object.assign({}, L, { sku: GOOD }));
eq(stripSource(sp), stripSource(good), 'the whole reading is the one the correctly spelled SKU gives (form, size, chain, count, font, options, questions, engraving flags)');
eq([O.lineFamily(L), O.purchaseDetails(L, sp).type, sp.form, sp.size], ['necklace', 'Necklace', null, null], 'a necklace (the family and the details type); the listing has no Type or Size option, so the form and size are empty as for the correct SKU');
eq([sp.pieceCount, O.pieceCountOf({ spec: sp }), sp.pair.discs, sp.options.find(o => o.name === 'Necklace Options').mapped], [2, 2, 2, { field: 'count', value: '2', source: sp.options[0].mapped.source }], '2 Disc: the disc count is unchanged (2 pieces)');
eq([sp.chain, sp.font.asked, sp.options.find(o => o.name === 'Fonts').mapped.field], ['16"', 'Typewriter', 'font'], 'the Fonts option is unchanged: length 16", the font asked for, Typewriter');
eq(JSON.stringify(L), before, 'the line was not changed by the read');
eq(O.resolveSku(deepFreeze({ sku: TYPO, listingId: LISTING }), {}, entryFor, {}, looseFor), { sku: GOOD, source: 'typo' }, 'resolveSku itself');
eq(O.masterSku(TYPO, entryFor, looseFor), GOOD, 'masterSku (the resolver the inventory, the pair members and the Review text share)');
eq(O.masterSku(TYPO, s => null, () => ''), '', 'masterSku: a master without the design holds nothing');

// ── 3 · the listing's 180 products ──────────────────────────────────────────────────────────────────────────────────────────────────────
// Etsy keeps ONE SKU for the whole listing (the stored table is { n: 180, uni }). The per-product option values are not stored, so the 180 below are
// INVENTED on the real shape (two options: "Necklace Options" = metal and disc count, property 513, and "Fonts" = length and font, property 514).
const real180 = DATA.tables[LISTING];
eq([real180.n, real180.uni], [180, TYPO], 'the real stored table: 180 products, ONE SKU, nitial_Disc_4571');
const METALS = ['GOLD', 'SILVER', 'ROSEGOLD'], COUNTS = [1, 2, 3, 4, 5], FONTS = ['Typewriter', 'Stylish', 'Pristina'], LENGTHS = ['14"', '16"', '18"', '20"'];
const necklaceOptions = [], fontOptions = [];
for (const m of METALS) for (const c of COUNTS) necklaceOptions.push({ value: `${m} - ${c} Disc`, metal: m, count: c });
for (const l of LENGTHS) for (const f of FONTS) fontOptions.push({ value: `${l}/ ${f}`, length: l, font: f });
eq([necklaceOptions.length, fontOptions.length, necklaceOptions.length * fontOptions.length], [15, 12, 180], '15 x 12 = 180 products');
const products = []; let vid = 1000;
for (const a of necklaceOptions) for (const b of fontOptions) { a.id = a.id || ++vid; b.id = b.id || ++vid; products.push({ product_id: 6900000000 + products.length, sku: TYPO, is_deleted: false, property_values: [{ property_id: 513, value_ids: [a.id] }, { property_id: 514, value_ids: [b.id] }], a, b }); }
const table = Skus.compact({ products: products.map(p => ({ product_id: p.product_id, sku: p.sku, is_deleted: p.is_deleted, property_values: p.property_values })) }, 1);
eq([table.n, table.uni, 'products' in table], [180, TYPO, false], 'compact() of 180 products with one SKU stores { n: 180, uni } exactly as the real table is');
const withTable = deepFreeze({ [LISTING]: table });
const metalKeyOf = { GOLD: 'gold', SILVER: 'silver', ROSEGOLD: 'rose' };
let asked = 0, read2 = 0, viaInventory = 0;
for (const p of products) {
  const base = { tx: String(5300000000 + (p.product_id % 1e6)), listing: LISTING, product: String(p.product_id), title: real.title, quantity: 1,
    variations: [{ name: 'Necklace Options', value: p.a.value, propertyId: '513', valueId: String(p.a.id) }, { name: 'Fonts', value: p.b.value, propertyId: '514', valueId: String(p.b.id) }] };
  for (const sku of [TYPO, '']) {   // the SKU as Etsy sends it (the listing's), and a transaction that came with none (the inventory's one SKU then stands in)
    const line = lineOf(Object.assign({}, base, { sku }), { metalKey: metalKeyOf[p.a.metal], metalLabel: p.a.metal });
    const s = read('41' + String(p.product_id).slice(-8), line, ctx({ listingSkus: withTable }));
    const label = `${p.a.value} | ${p.b.value} | tx SKU ${JSON.stringify(sku)}`;
    ok(s.designSku === GOOD, `${label}: INITIAL_DISC_4571 (got ${s.designSku})`);
    ok(!s.problems.some(q => q.kind === 'unmatchedSku'), `${label}: the SKU is no question any more (${problems(s)})`);
    // (a value that says "1 Disc" is an option question of its own, for the correct SKU as well: the count rules ask about it, which this change leaves alone)
    ok(p.a.count === 1 ? s.problems.every(q => q.kind === 'needsMapping') : !s.problems.length, `${label}: no question about the SKU or anything else (${problems(s)})`);
    ok(s.pieceCount === p.a.count && O.pieceCountOf({ spec: s }) === p.a.count, `${label}: ${p.a.count} disc(s) -> ${p.a.count} piece(s) (got ${s.pieceCount})`);
    ok(s.chain === p.b.length && s.font.asked === p.b.font, `${label}: length ${p.b.length} and font ${p.b.font} read as before`);
    ok(O.lineFamily(line) === 'necklace', `${label}: a necklace`);
    if (sku === TYPO) { const g = read('41' + String(p.product_id).slice(-8), Object.assign({}, line, { sku: GOOD }), ctx({ listingSkus: withTable })); eq(stripSource(s), stripSource(g), `${label}: reads as the correct SKU reads`); asked++; }
    else { ok(s.skuSource === 'inventory' && s.viaInventory && s.viaInventory.by === 'listing', `${label}: from the listing's inventory SKU (${s.skuSource})`); viaInventory++; }
    read2++;
  }
}
eq([asked, viaInventory, read2], [180, 180, 360], '180 products read twice (with the transaction SKU, and with none)');
{ // the same 180 with no table loaded at all (a fresh page): the transaction's own SKU still resolves
  let c = 0; for (const p of products) { const s = read('4100000001', lineOf({ tx: '1', listing: LISTING, product: String(p.product_id), sku: TYPO, title: real.title, variations: [{ name: 'Necklace Options', value: p.a.value, propertyId: '513', valueId: String(p.a.id) }] })); if (s.designSku === GOOD && !s.problems.some(q => q.kind === 'unmatchedSku')) c++; }
  eq(c, 180, 'with no inventory table loaded the 180 products still resolve (the transaction carries the SKU)'); }

// ── 4 · any other misspelling still waits ───────────────────────────────────────────────────────────────────────────────────────────────
const waits = (sku, why, extra) => {
  const s = read('4170000009', lineOf(Object.assign({}, real, { sku })));
  eq([s.designSku, problems(s)], [String(sku).trim().toUpperCase(), ['unmatchedSku:not in any master file'].concat(extra || [])], `${JSON.stringify(sku)}: ${why}`);
  eq(s.skuSource, 'transaction', `${JSON.stringify(sku)}: no alias applied`);
};
waits('nitial_Disc_9999', 'a different number: the master has INITIAL_DISC_9999, a look-alike is never taken');
waits('NITIAL_8391', 'another listing with a letter lost (the master has INITIAL_8391)');
waits('ITIAL_DISC_4571', 'two letters lost');
waits('INITIA_DISC_4571', 'the last letter of the word lost');
waits('nitial_Disc_4571x', 'a letter added');
waits('NIITIAL_DISC_4571', 'a letter doubled');
waits('NITIAL_DISC_4571-CO', 'the typo with the charm-only mark is not exactly the typo');
waits('NITIAL_DISC_4571 (HUGGIE)', 'a huggie version of the typo is not exactly the typo', ['needsMapping']);   // (EARWORDS: an earring SKU under a necklace title is also asked once, "is this a pair of earrings?")
waits('Nitial_Disc_4572', 'another number');
{ const s = read('4170000009', lineOf(Object.assign({}, real, { sku: 'INITIAL_8391' }))); eq([s.designSku, s.skuSource, problems(s)], ['INITIAL_8391', 'transaction', []], 'a SKU the master holds reads as it always did'); }
{ const s = read('4170000009', lineOf(Object.assign({}, real, { sku: 'initial disc 4571' }))); eq([s.designSku, s.skuSource], [GOOD, 'spelling'], 'the correct SKU in other spacing and case is the master spelling rule, not the typo table'); }
{ const s = read('4170000009', lineOf(Object.assign({}, real, { sku: 'NITIAL_FAKE' }))); eq([s.designSku, s.skuSource], ['NITIAL_FAKE', 'transaction'], 'a master SKU that happens to start with NITIAL is its own design (held first)'); }
eq(read('4170000009', lineOf(Object.assign({}, real, { sku: 'NITIAL_8391' })), ctx({ masterEntry: s => (s === 'NITIAL_8391' ? { sku: s } : null), masterLoose: () => '' })).designSku, 'NITIAL_8391', 'a master that holds a SKU under that very spelling keeps it (the typo is never applied over a design)');

// ── 5 · a person's answer still wins ────────────────────────────────────────────────────────────────────────────────────────────────────
{ const a = read('4175370240', L, ctx({ aliases: { [LISTING]: { bySku: { NITIAL_DISC_4571: 'OTHER_DESIGN_1' }, v: 2 } } }));
  eq([a.designSku, a.skuSource, problems(a)], ['OTHER_DESIGN_1', 'alias', []], 'a "Use this charm" saved for the SKU on this listing wins');
  const b = read('4175370240', L, ctx({ aliases: { [LISTING]: { sku: 'OTHER_DESIGN_2' } } }));
  eq([b.designSku, b.skuSource], ['OTHER_DESIGN_2', 'alias'], "a listing's own alias (saved before the per-SKU answers) wins");
  const c = read('4175370240', L, ctx({ optionMaps: { [LISTING]: { Fonts: { '16"/ typewriter': { field: 'design', value: 'OTHER_DESIGN_1' } } } } }));
  eq([c.designSku, c.skuSource], ['OTHER_DESIGN_1', 'option'], 'an option map that picks the charm wins');
  const d = read('4175370240', L, ctx({ optionMaps: { [LISTING]: { Fonts: { '16"/ typewriter': { field: 'ignore' } } } } }));
  eq([d.designSku, d.skuSource], [GOOD, 'typo'], 'an option map that does something else leaves the SKU alone'); }
{ const noMatch = read('4175370240', L, ctx({ aliases: { '999': { bySku: { NITIAL_DISC_4571: 'OTHER_DESIGN_1' }, v: 2 } } }));
  eq([noMatch.designSku, noMatch.skuSource], [GOOD, 'typo'], "another listing's alias is not this listing's"); }

// ── 6 · nothing stored is touched ───────────────────────────────────────────────────────────────────────────────────────────────────────
{ // every input was deep-frozen above (the line, the order, the maps, the table): a read that tried to write would have thrown. The shared table is frozen too.
  const stored = JSON.stringify(withTable) + JSON.stringify(DATA.tables[LISTING]);
  read('4175370240', L, ctx({ listingSkus: withTable }));
  eq(JSON.stringify(withTable) + JSON.stringify(DATA.tables[LISTING]), stored, 'the listing table is as it was');
  eq(JSON.stringify(O.KNOWN_SKU_TYPOS), '{"NITIAL_DISC_4571":"INITIAL_DISC_4571"}', 'the table is as it was'); }

// ── 7 · inventory SKU of one product: names a design, the question text (listing with a SKU per product) ──────────────────────────────────
{ // a per-product table where one product carries the typo and the others other SKUs: the inventory's SKU for the product bought stands in for a missing transaction SKU
  const t = deepFreeze({ at: 1, n: 2, products: [{ id: '70', sku: TYPO, d: 0, pv: [['513', '1']] }, { id: '71', sku: 'RUNE_NECKLACE_CHARM-14', d: 0, pv: [['513', '2']] }] });
  const line = lineOf({ tx: '9', listing: LISTING, product: '70', sku: '', title: real.title, variations: [{ name: 'Necklace Options', value: 'GOLD - 1 Disc', propertyId: '513', valueId: '1' }] });
  const s = read('4170000011', line, ctx({ listingSkus: { [LISTING]: t } }));
  eq([s.designSku, s.skuSource], [GOOD, 'inventory'], "a product's inventory SKU that is the typo reads as the design");
  eq(O.inventoryWhy(line, t, true, entryFor, looseFor), "Etsy's SKU INITIAL_DISC_4571 is shared by several choices of this option", 'the Review text names the design, not "not in any master file"');
  const none = O.inventoryWhy(line, t, true, () => null, () => '');
  eq(none, "Etsy's SKU for this option, NITIAL_DISC_4571, is not in any master file", 'and a master without it says what it said'); }

// ── 8 · the other places that turn a SKU into a design read the same table ──────────────────────────────────────────────────────────────
{ const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), search = fs.readFileSync(path.join(root, 'charm-nest-search.js'), 'utf8'), live = fs.readFileSync(path.join(root, 'netlify/functions/_stationLive.js'), 'utf8');
  ok(/const typoOf = sku => \(O\.knownTypo \? O\.knownTypo\(sku\) : ""\);/.test(bridge), 'the bridge asks the shared table');
  ok(/libFacts\(typoOf\(l\.sku\)\)/.test(bridge) && /libFacts\(typoOf\(own\.sku\)\)/.test(bridge), "the bridge re-reads a line when the library gets, loses or blocks the typo's design (inputsOf)");
  ok(/O\.knownTypo\(x\)/.test(search) && /const O = W\.CharmNestOrders/.test(search), 'the order search finds an order by the right spelling too');
  ok(/require\("\.\.\/\.\.\/charm-nest-orders\.js"\)/.test(live) && /ordersLib\.knownTypo/.test(live) && (live.match(/designKey\(/g) || []).length >= 4, 'the Design Station live data looks the vector design up under the right spelling (the same module)');
  // the Review question and the pool's "not in any master file" are raised from the resolved design: spec.designSku
  ok(/Master\.entryFor\(sp\.designSku\) \|\| await Master\.fetchEntry\(sp\.designSku\)/.test(bridge), 'the pool asks the master for the RESOLVED design');
  const server = fs.readFileSync(path.join(root, 'netlify/functions/_charmNestListingSkus.js'), 'utf8');
  ok(!/knownTypo|NITIAL/i.test(server), "the stored listing table is Etsy's own text: never rewritten, read through the module above"); }
(async () => {
  // the Design Station live card: a piece that came with Etsy's SKU gets the vector design of INITIAL_DISC_4571; its own SKU text is untouched
  const L2 = require('../../netlify/functions/_stationLive.js');
  const URL = 'https://firebasestorage.googleapis.com/v0/b/shop/o/thumbs%2Finitial-disc-4571.png?alt=media&token=t';
  const docs = new Map([['INITIAL_DISC_4571', { sku: 'INITIAL_DISC_4571', thumbUrl: URL }]]), reads = [];
  const db = { collection: name => ({ doc: id => ({ id, name, get: async () => { reads.push(name + '/' + id); const d = name === 'Charm_Master_Index' ? docs.get(id) : null; return { exists: !!d, id, data: () => d }; } }) }),
    getAll: async (...refs) => Promise.all(refs.filter(r => r && r.get).map(r => r.get())) };
  const c = { kind: 'order', rid: '4175370240', customer: 'Sam P.', pieceCount: 3, pieces: [{ id: '4175370240_5217980219_1', label: 'Initial disc', sku: 'nitial_Disc_4571' }, { id: '4175370240_5217980219_2', label: 'Initial disc', sku: 'NITIAL_DISC_4571' }, { id: '4175370240_5217980219_3', label: 'Initial disc', sku: 'NITIAL_DISC_9999' }] };
  const errors = await L2._t.dress({ db, prefix: '', cache: {}, now: 1, life: 1e12 }, [c]);
  eq(errors, [], 'the live read had no errors');
  eq(c.pieces.map(p => p.vectorUrl), [URL, URL, ''], 'both spellings of the typo show the INITIAL_DISC_4571 vector design; another misspelling shows none');
  eq(c.pieces.map(p => p.sku), ['nitial_Disc_4571', 'NITIAL_DISC_4571', 'NITIAL_DISC_9999'], "the piece keeps the SKU it came with");
  ok(reads.includes('Charm_Master_Index/INITIAL_DISC_4571') && !reads.includes('Charm_Master_Index/NITIAL_DISC_4571'), 'the master index was asked for the right spelling only');
  console.log(`sku-typo: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
