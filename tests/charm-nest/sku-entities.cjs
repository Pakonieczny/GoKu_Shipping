// ACORNSKU (Paul, 11 Oct 2026 12:57 UTC): order 4196630529, listing 1816160617 ("Leaf Charm Gift for Her ..."), a Charm Only necklace charm, Gold Filled, said
// "Unknown SKU: SKU ACORN&#39;S LEAF: not in any master file", the SKU shown as ACORN&#39;S LEAF: Etsy hands text over HTML-escaped, the shop's SKU is ACORN'S LEAF.
// Two things: (1) the text is decoded ONCE where the sorter takes the orders in, so display, matching and master lookups all read ACORN'S LEAF; (2) the SKU is recorded in the
// master list under the charm LEAF_1972 (a second index entry with the very same design file; the live write is one masterPutIndex of one entry).
//   node tests/charm-nest/sku-entities.cjs
// What it proves (charm-nest-orders.js decodeEntities / cleanLine / cleanOrders / cleanMaps / bySkuOf, upSku, norm; the bridge calls them at DesignLink, rowFromRecord, loadMaps):
//   · the standard entities (&#39; &apos; &amp; &quot; &lt; &gt;) and numeric / hex forms decode; double-escaped text decodes; plain text, a bare "&", a control code and a number that
//     is no character are left as they are; decoding twice is decoding once
//   · a line's SKU, title and option names and values are decoded, the customer's own words are not; the line the station answers to orders.snapshot / orders.detail is decoded in
//     place, an orders.check answer is not touched
//   · order 4196630529 (the real line) reads ACORN'S LEAF with the entity form exactly as with the plain form; before the master holds it, it waits as "Unknown SKU: ACORN'S LEAF"
//     (not "ACORN&#39;S LEAF"); with the master entry (the same design file as LEAF_1972) it has a design, no question, the necklace charm form
//   · ACORN'S LEAF, ACORN S LEAF, ACORNS LEAF, a curly apostrophe and the entity form all resolve to that one master SKU; no other SKU is changed by it
//   · a SKU with no entity resolves exactly as before (LEAF_1972, MOON & STARS_3703 with a literal "&"), and "MOON &amp; STARS_3703" now finds the master that holds "MOON & STARS_3703"
//   · a person's saved answer, option map, no-design SKU or pair made while the text still carried an entity is still found: nothing is lost
//   · the bridge wires the three intake points
const assert = require('assert/strict'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const step = name => console.log('  ✓', name);

// ── 1 · the decoder ───────────────────────────────────────────────────────────────────────────────────────────────────────────────
for (const [raw, want] of [
  ['ACORN&#39;S LEAF', "ACORN'S LEAF"], ['ACORN&apos;S LEAF', "ACORN'S LEAF"], ['ACORN&#x27;S LEAF', "ACORN'S LEAF"], ['ACORN&#X27;S LEAF', "ACORN'S LEAF"], ['ACORN&#039;S LEAF', "ACORN'S LEAF"],
  ['MOON &amp; STARS_3703', 'MOON & STARS_3703'], ['16&quot; chain', '16" chain'], ['a &lt;b&gt; c', 'a <b> c'], ['it&#8217;s', 'it’s'], ['&#x1F600;', '\u{1F600}'],
  ['A&amp;#39;B', "A'B"], ['&amp;amp;quot;', '"'], ['&amp;amp;amp;amp;', '&'],                                      // (double escaped: as many layers as there are)
  ['ACORN\'S LEAF', "ACORN'S LEAF"], ['MOON & STARS_3703', 'MOON & STARS_3703'], ['Tom & Jerry', 'Tom & Jerry'], ['&', '&'], ['a&b;c', 'a&b;c'], ['&nbsp;', '&nbsp;'],   // (nothing but the standard set)
  ['&#1;', '&#1;'], ['&#0;', '&#0;'], ['&#xD800;', '&#xD800;'], ['&#99999999;', '&#99999999;'], ['&#x110000;', '&#x110000;'], ['&AMP;', '&AMP;'],    // (a control code, a surrogate, a number that is no character: as written)
  ['', ''], [null, ''], [undefined, ''], [42, '42'],
]) eq(O.decodeEntities(raw), want, `decodeEntities(${JSON.stringify(raw)})`);
for (const s of ['ACORN&#39;S LEAF', 'A&amp;#39;B', 'MOON &amp; STARS', '&amp;amp;amp;amp;', 'plain', '&#x27;&quot;']) eq(O.decodeEntities(O.decodeEntities(s)), O.decodeEntities(s), `decoding twice is decoding once: ${s}`);
step('decodeEntities: the standard entities, numeric and hex forms, double-escaped text; plain text and what is no entity left alone; idempotent');

// ── 2 · a line, an order, the station's answers ───────────────────────────────────────────────────────────────────────────────────
const REAL = () => ({ transactionId: '4300000001', listingId: '1816160617', productId: '', sku: 'ACORN&#39;S LEAF', title: 'Leaf Charm Gift for Her Nature Jewelry Leaf Charm Silver Leaf Charm Nature Lover Gift Autumn Jewelry Leaf Charm', quantity: 1,
  metalKey: 'gold', metalLabel: 'Gold Filled', personalization: [], variations: [{ name: 'Metal Choice', value: 'Gold Filled' }, { name: 'Charm Type', value: 'Necklace CHARM' }] });
{ const l = O.cleanLine(Object.assign(REAL(), { title: 'Mom&#39;s &amp; Dad&#39;s leaf', personalization: ['Mom&#39;s'], buyerMessage: 'it&#39;s a gift',
    variations: [{ name: 'Women&#39;s size', value: 'S &amp; M', propertyId: '5', valueId: '9' }, { formatted_name: 'Pet&#39;s name', formatted_value: 'Max &quot;the dog&quot;' }, null, 'x'] }));
  eq([l.sku, l.title], ["ACORN'S LEAF", "Mom's & Dad's leaf"], 'a line: the SKU and the title are decoded');
  eq(l.variations.slice(0, 2), [{ name: "Women's size", value: 'S & M', propertyId: '5', valueId: '9' }, { formatted_name: "Pet's name", formatted_value: 'Max "the dog"' }], 'the options: names and values (both spellings of the field) are decoded, the ids are not touched');
  eq([l.personalization, l.buyerMessage], [['Mom&#39;s'], 'it&#39;s a gift'], "the customer's own words are not touched here");
  eq(O.cleanLine(null), null, 'a missing line is answered as it is'); eq(O.cleanLine({ sku: 5, title: null }), { sku: 5, title: null }, 'a non-text field is left as it is'); }
{ const res = { total: 2, orders: [{ receiptId: '1', lines: [REAL(), { sku: 'LEAF_1972', title: 'Leaf', variations: [] }] }, { receiptId: '2', lines: [] }, { receiptId: '3' }, null] };
  const same = O.cleanOrders(res); ok(same === res, 'orders.snapshot: the answer itself');
  eq([res.orders[0].lines[0].sku, res.orders[0].lines[1].sku, res.total], ["ACORN'S LEAF", 'LEAF_1972', 2], 'every line of every order is decoded in place; the others are as they were');
  const det = { order: { receiptId: '1', lines: [REAL()] }, gone: false }; O.cleanOrders(det); eq(det.order.lines[0].sku, "ACORN'S LEAF", 'orders.detail: the one order');
  const chk = { orders: { '1': { open: true, updateTs: 5 } }, swept: false }, before = JSON.stringify(chk); O.cleanOrders(chk); eq(JSON.stringify(chk), before, 'orders.check (a map of verdicts): nothing to decode, nothing changed');
  eq([O.cleanOrders(null), O.cleanOrders('x'), O.cleanOrders({})], [null, 'x', {}], 'not an answer with orders: as it is'); }
step('cleanLine / cleanOrders: SKU, title, option names and values decoded in place; the customer\'s words, ids and an orders.check answer untouched');

// ── 3 · the master: LEAF_1972 and the new entry under it ───────────────────────────────────────────────────────────────────────────
// the live entry of LEAF_1972 (read 11 Oct, masterGet) and the entry the app's own path writes for another SKU under the same charm (charm-nest-bridge.js writeIndex: "the same design under
// another SKU": the entry copied with only the sku changed, one masterPutIndex)
const LEAF = { sku: 'LEAF_1972', masterHash: 'e6e4ec002848e6fbb94f6801edefbecaa463032326e561238253ef421173593b', masterName: 'MASTER SKU_LIVING_MV_2026-0914.ai', charmHash: '5457a941', widthPt: 22.637039999999615, heightPt: 40.41909999999916,
  areaPt2: 461.5, members: 3, holes: 1, engravable: true, upAngle: 85.88174993624193, upSource: 'hole', aiPath: 'charmnest/sandbox/master/LEAF_1972.ai', thumbPath: 'charmnest/sandbox/master/LEAF_1972.png', labelSource: 'text', open: false, blocked: null };
const ACORNS = Object.assign({}, LEAF, { sku: "ACORN'S LEAF" });
const mk = skus => { const m = new Map(skus.map(e => [e.sku.toUpperCase(), e])); const loose = {}; for (const k of m.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
  return { entryFor: s => m.get(String(s || '').toUpperCase()) || null, looseFor: s => loose[O.looseKey(s)] || '' }; };
const others = ['LEAF_1972', 'ACORN_4721', 'MOON & STARS_3703', 'LEAF_5555', 'MAPLE_8065'].map(sku => Object.assign({}, LEAF, { sku }));
const WITHOUT = mk(others), WITH = mk(others.concat([ACORNS]));
const ctx = (M, extra) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: M.entryFor, masterLoose: M.looseFor }, extra || {});
const order = rid => ({ receiptId: String(rid), updateTs: 1, staffNote: '', buyerMessage: '' });
const problems = sp => sp.problems.map(p => p.kind + (p.sku ? ':' + p.sku : '') + (p.reason ? ':' + p.reason : ''));
const read = (line, M, extra) => O.interpretLine(order('4196630529'), line, ctx(M, extra));

// order 4196630529 as the station hands it (escaped), taken in once, then read
const taken = O.cleanLine(REAL());
eq(taken.sku, "ACORN'S LEAF", 'taken in: the SKU is ACORN\'S LEAF');
{ const sp = read(taken, WITHOUT);
  eq([sp.designSku, sp.skuSource, problems(sp)], ["ACORN'S LEAF", 'transaction', ["unmatchedSku:ACORN'S LEAF:not in any master file"]], "before the master holds it: waits as Unknown SKU ACORN'S LEAF (the entity form is gone from the question)"); }
const sp = read(taken, WITH);
eq([sp.designSku, sp.skuSource, problems(sp)], ["ACORN'S LEAF", 'transaction', []], 'with the master entry: it has a design and no question');
eq([sp.form, sp.material, sp.quantity, sp.pieceCount], ['necklace', 'gold', 1, 1], 'Charm Only necklace charm, Gold Filled, one piece');
{ const e = WITH.entryFor(sp.designSku), l = WITH.entryFor('LEAF_1972');
  ok(e && !e.blocked && !e.sizes && e.aiPath === l.aiPath && e.thumbPath === l.thumbPath && e.charmHash === l.charmHash && e.masterHash === l.masterHash && e.widthPt === l.widthPt && e.heightPt === l.heightPt, 'the entry is the design of LEAF_1972: its file, picture, hash, size');
  ok(Math.max(e.widthPt, e.heightPt) * 25.4 / 72 < 60 && Math.min(e.widthPt, e.heightPt) * 25.4 / 72 > 3, '8.0 x 14.3 mm: inside the cut size limits (no oversize question)'); }
// the same line read UNDECODED (a copy the intake never saw) is the same reading
{ const raw = read(REAL(), WITH); eq([raw.designSku, raw.skuSource, problems(raw)], [sp.designSku, sp.skuSource, []], 'the entity form given straight to the reader resolves to the same SKU');
  eq(JSON.stringify(Object.assign({}, raw, { options: 0 })), JSON.stringify(Object.assign({}, sp, { options: 0 })), 'and the whole reading is the same'); }
// every spelling meets the one master SKU
for (const s of ["ACORN'S LEAF", "acorn's leaf", 'ACORN S LEAF', 'ACORNS LEAF', 'ACORN’S LEAF', 'ACORN-S LEAF', 'ACORN&#39;S LEAF', 'ACORN&apos;S LEAF', 'ACORN&#x27;S LEAF', 'Acorn&#39;s Leaf '])
  eq([O.resolveSku({ sku: s, listingId: '1816160617' }, {}, WITH.entryFor, {}, WITH.looseFor).sku, O.masterSku(s, WITH.entryFor, WITH.looseFor)], ["ACORN'S LEAF", "ACORN'S LEAF"], `${JSON.stringify(s)} resolves to ACORN'S LEAF`);
eq(O.looseKey('ACORN&#39;S LEAF'), O.looseKey('ACORN S LEAF'), 'the loose key reads the entity form as the decoded SKU');
// nothing else is changed by the new entry: every other SKU reads the same with and without it
for (const s of ['LEAF_1972', 'ACORN_4721', 'LEAF_5555', 'MAPLE_8065', 'LEAF_9999', 'ACORN', 'LEAF', 'ACORN_4721-CO', 'MAPLE_8065-CO', '']) {
  const l = Object.assign(REAL(), { sku: s, title: 'x' });
  eq([read(l, WITH).designSku, problems(read(l, WITH))], [read(l, WITHOUT).designSku, problems(read(l, WITHOUT))], `${JSON.stringify(s)}: read exactly as before`); }
eq([read(Object.assign(REAL(), { sku: 'LEAF_1972' }), WITH).designSku, read(Object.assign(REAL(), { sku: 'LEAF_1972' }), WITH).skuSource], ['LEAF_1972', 'transaction'], 'LEAF_1972 itself is untouched');
step("order 4196630529: ACORN'S LEAF waits as Unknown SKU before the master entry, resolves with it (the design file of LEAF_1972, necklace charm, no question); every spelling resolves; other SKUs read as before");

// ── 4 · a SKU with no entity resolves as it did; one with an entity finds the master that holds the plain text ──────────────────────────
{ const l = s => Object.assign(REAL(), { sku: s, title: 'Moon', variations: [] });
  eq([read(l('MOON & STARS_3703'), WITH).designSku, problems(read(l('MOON & STARS_3703'), WITH))], ['MOON & STARS_3703', []], 'a master SKU with a literal "&" still matches itself');
  eq([read(l('MOON &amp; STARS_3703'), WITH).designSku, problems(read(l('MOON &amp; STARS_3703'), WITH))], ['MOON & STARS_3703', []], 'Etsy\'s "MOON &amp; STARS_3703" finds the master SKU "MOON & STARS_3703"');
  eq([read(l('MOON &amp; STARS_9999'), WITH).designSku, problems(read(l('MOON &amp; STARS_9999'), WITH))], ['MOON & STARS_9999', ['unmatchedSku:MOON & STARS_9999:not in any master file']], 'an unknown SKU still waits, never a look-alike (and shows decoded)'); }
step('SKUs with no entity read as before; "&amp;" finds the master SKU with a literal "&"; an unknown one still waits');

// ── 5 · a saved answer made while the text carried an entity is still found ─────────────────────────────────────────────────────────
{ const al = { '1816160617': { v: 2, bySku: { 'ACORN&#39;S LEAF': 'LEAF_1972' } } };
  const sp1 = read(taken, WITHOUT, { aliases: al });
  eq([sp1.designSku, sp1.skuSource, problems(sp1)], ['LEAF_1972', 'alias', []], 'an alias saved under the escaped SKU still answers the decoded line (bySkuOf)');
  const alDirect = { '1816160617': { v: 2, bySku: { "ACORN'S LEAF": 'LEAF_5555', 'ACORN&#39;S LEAF': 'LEAF_1972' } } };
  eq(read(taken, WITHOUT, { aliases: alDirect }).designSku, 'LEAF_5555', 'a key saved under the decoded SKU wins over the escaped one');
  eq(O.bySkuOf({ 'A&amp;B': 1, C: 2 }, 'A&B'), 1, 'bySkuOf: the escaped key'); eq([O.bySkuOf({ C: 2 }, 'C'), O.bySkuOf({ C: 2 }, 'D'), O.bySkuOf(null, 'C'), O.bySkuOf({ C: 2 }, '')], [2, undefined, undefined, undefined], 'bySkuOf: plain keys, a miss, no map, no SKU');
  const maps = O.cleanMaps({ '1816160617': { v: 2, bySku: { 'ACORN&#39;S LEAF': 'LEAF_1972', 'PLAIN': 'X' }, pairBySku: { 'HUGGIE &amp; CO': { L: 'A', R: 'B' } }, sku: 'LEAF_5555' }, '7': { sku: 'Z' } }, { skus: ['MOM&#39;S CHARM', 'BOX-01'], patterns: ['^CHAIN'], rows: [] });
  eq(maps[0]['1816160617'].bySku, { "ACORN'S LEAF": 'LEAF_1972', PLAIN: 'X' }, 'cleanMaps: the answer is read under the decoded SKU, the plain one stays');
  eq(maps[0]['1816160617'].pairBySku, { 'HUGGIE & CO': { L: 'A', R: 'B' } }, 'cleanMaps: a saved pair too'); eq([maps[0]['1816160617'].sku, maps[0]['7']], ['LEAF_5555', { sku: 'Z' }], 'and the rest of the record is as it was');
  eq(maps[1].skus, ["MOM'S CHARM", 'BOX-01'], 'cleanMaps: the no-design SKUs');
  const plain = { '1': { bySku: { A: 'B' } } }, plainNd = { skus: ['A'] }; ok(O.cleanMaps(plain, plainNd)[0] === plain && plain['1'].bySku.A === 'B' && plainNd.skus[0] === 'A', 'maps with no entity are the same objects, untouched');
  ok(O.isNoDesign("MOM'S CHARM", { skus: ['MOM&#39;S CHARM'] }) && O.isNoDesign('MOM&#39;S CHARM', { skus: ["MOM'S CHARM"] }), 'a no-design SKU saved with the entity still says "nothing to cut" for the decoded SKU, and the other way round');
  const pairAl = { '1816160617': { pairBySku: { 'ACORN&#39;S LEAF': { L: 'LEAF_1972', R: 'LEAF_5555' } } } };
  const pm = O.pairMembers({ listingId: '1816160617', sku: "ACORN'S LEAF", title: 'x', variations: [] }, { me: WITH.entryFor, loose: WITH.looseFor, aliases: pairAl });
  eq([pm && pm.source, pm && pm.members.map(m => m.side + ':' + m.sku)], ['alias', ['L:LEAF_1972', 'R:LEAF_5555']], 'a pair of designs saved under the escaped SKU is found (pairMembers)');
  // option maps: the keys are compared decoded
  const om = { '555': { "women&#39;s size": { 'S &amp; M': { field: 'size', value: 'S' } } } };
  eq(O.optionLookup(om, '555', "Women's size", 'S & M'), { source: 'listing', field: 'size', value: 'S' }, 'an option map saved under escaped names and values still meets the decoded option');
  eq(O.optionLookup(om, '555', 'Women&#39;s size', 'S &amp; M'), { source: 'listing', field: 'size', value: 'S' }, 'and the escaped option too');
  eq([O.norm('16&quot; chain'), O.norm(' A &amp;  B '), O.norm(null)], ['16" chain', 'a & b', ''], 'norm: decoded, lower case, single spaces'); }
step('answers saved before: an alias, a pair, a no-design SKU and an option map made under the escaped text are all still found; maps with no entity are untouched');

// ── 6 · the option an entity used to spoil ─────────────────────────────────────────────────────────────────────────────────────────────
{ const l = Object.assign(REAL(), { sku: 'LEAF_1972', variations: [{ name: 'Charm Type', value: 'Necklace &amp; Charm' }, { name: 'Metal Choice', value: 'Gold Filled' }] });
  const spEsc = read(l, WITH), spDec = read(O.cleanLine(JSON.parse(JSON.stringify(l))), WITH);
  eq([spDec.form, problems(spDec)], ['necklace', []], '"Necklace & Charm" (as Etsy writes "Necklace &amp; Charm") is the necklace form once decoded');
  ok(spEsc && spEsc.designSku === 'LEAF_1972', 'an undecoded line still reads (nothing throws)'); }
step('an option value Etsy escaped ("Necklace &amp; Charm") reads as the form it names once decoded');

// ── 7 · the bridge calls it at the three intake points ──────────────────────────────────────────────────────────────────────────────────
{ const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  ok(/if \(\/\^orders\\\.\/\.test\(p\.type\)\) O\.cleanOrders\(d\.result\)/.test(src), 'DesignLink: every orders.* answer is decoded as it arrives');
  ok(/O\.cleanLine\(line\);[^\n]*\n\s*order\.lines = \[line\];/.test(src), 'rowFromRecord: a stored line is read decoded');
  ok(/O\.cleanMaps\(next\[1\], next\[2\]\)/.test(src), 'loadMaps: the saved answers are read with their SKUs decoded');
  ok(/O\.bySkuOf\(a\.pairBySku,/.test(src), "the re-read signature reads a pair link by the same keys"); }
step('charm-nest-bridge.js: DesignLink, rowFromRecord and loadMaps call the decoder');

console.log(`sku-entities: all passed (${n} checks)`);
