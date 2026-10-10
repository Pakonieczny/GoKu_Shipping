// OPTLINK (Paul, 10 Oct 2026, 02:04 UTC: "I thought you were supposed to check all of the drop down menu SKU links. Seems like none of these
// were correctly checked and matched."): the drop-down options that pick the charm (Viking Rune / Rune Symbol, Goddess Symbol, Zodiac Sign,
// Birth Flower) read through the real code, offline, with the REAL lines of Paul's Review rows (the Sep 17 sandbox snapshot) and the REAL
// Etsy inventory tables of their listings (tests/charm-nest/options-link-data.json: read once on 10 Oct 2026, 10 listings, no buyer data).
//
//  what the data showed (so the test says it again, line by line):
//   · every one of these transactions carries the LISTING's shared SKU ("Viking Rune", "Greek Goddess", "Zodiac REVAMP", "Birth_6106"…),
//     never the option's: only the listing's inventory knows the SKU Etsy keeps for the option bought (step 2 of Paul's order);
//   · the sandbox never asks Etsy and production had stored nothing for these listings, so the page had no table and every row waited;
//   · with the table, the code already named the charm for 14 lines; one waits on purpose (Greek Goddess EARRINGS: Etsy's SKU for Vesta is
//     the NECKLACE pendant GREEKGODDESS_NECKLACE_4, the earring is GREEKGODDESS_EARRING_4) and one more code gap is fixed here (the listing's
//     umbrella design "GREEK GODDESS" is in the master, so the inventory was never asked about the option);
//   · the stored table kept its pairs as an array in an array, which Firestore refuses: it is stored packed now and read back as pairs.
// No network, no Etsy, no AI.   node tests/charm-nest/options-link.cjs
const path = require('path'), fs = require('fs'), assert = require('assert/strict');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
const O = require(path.join(root, 'charm-nest-orders.js'));
const T = require(path.join(fnDir, '_charmNestListingSkus.js'));
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const DATA = JSON.parse(fs.readFileSync(path.join(__dirname, 'options-link-data.json'), 'utf8'));
const ok = name => console.log('  ✓', name);
const clone = x => JSON.parse(JSON.stringify(x));

/* ── the world: master index, lines ── */
const upper = s => String(s).toUpperCase();
const lib = new Map(DATA.master.map(s => [upper(s), {}]));
const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: s => lib.get(upper(s)) || null, masterLoose: s => loose[O.looseKey(s)] || '' }, extra);
// the station classifies the metal from the Metal option; a plain stand-in for it (the real classifier is the Design Station's)
const metalOf = vars => { const v = (vars.find(x => /metal|colou?r/i.test(x.name)) || vars.find(x => /gold|silver/i.test(x.value)) || { value: '' }).value; return /silver/i.test(v) ? 'silver' : /rose/i.test(v) ? 'rose' : /14k solid/i.test(v) ? '14k' : 'gold'; };
const lineOf = l => ({ transactionId: l.tx, listingId: l.listing, productId: l.product, sku: l.sku, title: l.title, quantity: l.quantity, metalKey: metalOf(l.variations), metalLabel: '', personalization: [], buyerMessage: '', variations: clone(l.variations) });
const orderOf = l => ({ receiptId: l.receipt, updateTs: 1, staffNote: '', buyerMessage: '' });
const find = (receipt, value) => { const l = DATA.lines.find(x => x.receipt === receipt && x.variations.some(v => v.value === value)); assert(l, 'no line ' + receipt + ' ' + value); return l; };
const read = (l, extra) => O.interpretLine(orderOf(l), lineOf(l), ctx(extra));
const open = sp => sp.problems.filter(p => p.kind === 'needsMapping' && !p.pair && !p.count);

// Paul's rows: [receipt, option value, the charm Etsy's own SKU names | null (waits), why it waits]
const ROWS = [
  ['4174818039', 'Algiz - Divine Plan', 'RUNE_NECKLACE_CHARM-14'], ['4174818039', 'Kenaz - Health', 'RUNE_NECKLACE_CHARM-5'],
  ['4171987098', 'Ansuz - Inspiration', 'RUNE_NECKLACE_CHARM-3'], ['4171987098', 'Raidho - Nobility', 'RUNE_NECKLACE_CHARM-4'], ['4171987098', 'Sowilo - Guidance', 'RUNE_NECKLACE_CHARM-15'],
  ['4170819759', 'Berkano - Growth', 'RUNE_NECKLACE_CHARM-17'], ['4175451238', 'Sowilo - Guidance', 'RUNE_NECKLACE_CHARM-15'], ['4174975806', 'Tiwaz - Justice', 'RUNE_NECKLACE_CHARM-16'],
  ['4173182895', 'Ingwaz - Fertility', 'RUNE_EARRING_CHARM-21'],
  ['4176706298', 'Pisces', 'ZODIAC_EARRINGS-11'], ['4175402612', 'Libra', 'ZODIAC_EARRINGS-6'],
  ['4171709020', 'Virgo', 'ZODIAC_CONSTE_GEM-5'], ['4171709020', 'Morning Glory', 'BIRTHFLOWER_EARRINGS-8'],
  ['4174408832', 'Vesta', null, /necklace design and this line is earrings/]
];

/* ── 1 · the sandbox page had no table: every row waited, and says why ── */
function noTable() {
  for (const [rid, value, , ] of ROWS) {
    const l = find(rid, value), sp = read(l, { listingSkus: {} }), q = open(sp).find(p => p.optionValue === value);
    assert(q, `${rid} ${value}: without a table the option is asked: ` + JSON.stringify(sp.problems));
    assert.match(q.why, /SKU list for listing \d+ is not loaded yet/, 'the row says why in one plain line: ' + q.why);
    assert(!sp.designSku || !lib.has(upper(sp.designSku)) || /GREEK GODDESS/.test(sp.designSku), `${rid}: the listing's own SKU "${l.sku}" is no option's charm`);
  }
  // the transaction SKU of every one of these lines is the listing's shared SKU, never the option's
  const shared = new Set(DATA.lines.map(l => l.listing + '|' + l.sku.toUpperCase()));
  assert.equal(shared.size, Object.keys(DATA.tables).length, 'one transaction SKU per listing, whatever the option bought');
  ok('no table: each of the 14 lines waits on its option, and its row says "Etsy\'s SKU list is not loaded yet"');
}

/* ── 2 · the cloud: production asks Etsy once per listing, stores each table (no array in an array), the sandbox only reads ── */
const rawOf = (t, base = 1) => t.products
  ? { products: t.products.map(p => { const by = {}; for (const [a, b] of p.pv) (by[a] = by[a] || []).push(+b); return { product_id: +p.id, sku: p.sku, is_deleted: !!p.d, property_values: Object.entries(by).map(([k, v]) => ({ property_id: +k, value_ids: v, values: ['x'] })) }; }) }
  : { products: Array.from({ length: t.n }, (_, i) => ({ product_id: base + i, sku: t.uni, is_deleted: false, property_values: [{ property_id: 513, value_ids: [i + 1], values: ['x'] }] })) };
async function cloud() {
  const store = new Map(), asked = [];
  const refOf = (c, id) => ({ id, _k: c + '/' + id, async get() { const d = store.get(c + '/' + id); return { exists: !!d, id, data: () => (d ? clone(d) : undefined) }; }, async set(d) { refuseNestedArrays(d, c + '/' + id); store.set(c + '/' + id, clone(d)); } });
  const q = c => ({ orderBy() { return q(c); }, limit() { return q(c); }, startAfter() { return q(c); }, async get() { return { size: 0, docs: [], empty: true }; }, doc: id => refOf(c, id) });
  const db = { collection: c => q(c), async getAll(...refs) { return Promise.all(refs.map(r => r.get())); } };
  const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => ({ __ts: true }), increment: n => n, delete: () => undefined }, FieldPath: { documentId: () => '__name__' } }), storage: () => ({ bucket: () => ({}) }) };
  const fakeEtsy = { getListingInventory: async id => { asked.push(String(id)); assert(DATA.tables[id], 'asked about a listing nobody waits on: ' + id); return rawOf(DATA.tables[id]); } };
  const Module = require('module'), realLoad = Module._load;
  Module._load = function (req, ...rest) { if (req === 'firebase-admin' || /[\/]firebaseAdmin(\.js)?$/.test(req) || req === './firebaseAdmin') return fakeAdmin; if (/[\/]_etsyMailEtsy(\.js)?$/.test(req)) return fakeEtsy; return realLoad.call(this, req, ...rest); };
  let tables;
  try {
    delete process.env.EDIT_PASSCODE;
    const lib2 = require(path.join(fnDir, 'charmNestLibrary.js'));
    const post = body => lib2.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }).then(r => JSON.parse(r.body || '{}'));
    const get = qs => lib2.handler({ httpMethod: 'GET', headers: {}, queryStringParameters: qs }).then(r => JSON.parse(r.body || '{}'));
    const ids = Object.keys(DATA.tables);
    // the sandbox (where Paul's rows are) reads only what production stored: nothing yet, no Etsy call
    let r = await post({ op: 'listingSkus', listingIds: ids, sandbox: true });
    assert.equal(asked.length, 0); assert.equal(r.why, 'cache'); assert.equal(r.pending.length, ids.length); assert.deepEqual(r.tables, {});
    // production: 6 a request at most, one per listing, 10 listings = 10 calls in all
    r = await post({ op: 'listingSkus', listingIds: ids });
    assert.equal(r.etsyCalls, 6); assert.equal(r.why, 'batch'); assert.equal(asked.length, 6);
    r = await post({ op: 'listingSkus', listingIds: ids });
    assert.equal(r.etsyCalls, 4); assert.equal(asked.length, 10); assert.equal(new Set(asked).size, 10, 'each listing exactly once'); assert.deepEqual(r.cap, { used: 10, max: 60 });
    r = await get({ op: 'listingSkus', listingIds: ids.join(','), cacheOnly: '1' });
    assert.equal(r.etsyCalls, 0); assert.equal(asked.length, 10, 'a stored table is not asked of Etsy again');
    ok('cloud: production asks Etsy once per listing (6 + 4 calls for the 10 listings, cap 60 a day); a second ask costs none');
    // every stored document passed the no-array-in-array check (the fake refuses it); what the page reads has the shape it always had
    for (const [k, v] of store) refuseNestedArrays(v, k);
    r = await post({ op: 'listingSkus', listingIds: ids, sandbox: true });
    assert.equal(asked.length, 10, 'the sandbox never asks Etsy'); assert.deepEqual(r.pending, []);
    tables = r.tables;
    const norm = t => { const x = clone(t); delete x.at; if (x.products) for (const p of x.products) p.pv.sort((a, b) => a[0].localeCompare(b[0]) || a[1].localeCompare(b[1])); return x; };   // (the order of a product's pairs says nothing)
    for (const id of ids) assert.deepEqual(norm(tables[id]), norm(DATA.tables[id]), 'the sandbox reads the table exactly as Etsy gave it: ' + id);
    assert(tables['1712164498'].products.every(p => p.pv.every(a => Array.isArray(a) && a.length === 2 && /^\d+$/.test(a[0]) && /^\d+$/.test(a[1]))), 'pairs on the way out');
    assert(Object.values(DATA.tables).filter(t => t.uni).length === 3 && ['1008014571', '234758391', '1744372161'].every(id => tables[id].uni), 'a listing with one SKU for everything is stored as one word');
    ok('cloud: tables are stored with no array in an array and read back, in the sandbox, as the same pairs');
  } finally { Module._load = realLoad; }
  return tables;
}

/* ── 3 · with the table: each row resolves, or waits for one stated reason ── */
function withTable(tables) {
  for (const [rid, value, want, why] of ROWS) {
    const l = find(rid, value), sp = read(l, { listingSkus: tables });
    if (want) {
      assert.equal(sp.designSku, want, `${rid} ${value}`); assert.equal(sp.skuSource, 'inventory', `${rid}: Etsy's SKU for the option bought`);
      assert(!open(sp).length, `${rid} ${value}: no question about the option: ` + JSON.stringify(sp.problems));
      assert(lib.has(want), `${want} is a master SKU, spelt exactly`);
      assert.equal(sp.viaInventory.tx, l.sku.toUpperCase(), 'the transaction carried the listing\'s SKU');
    } else {
      const q = open(sp).find(p => p.optionValue === value); assert(q, `${rid} ${value} waits: ` + JSON.stringify(sp.problems)); assert.match(q.why, why);
      assert(!sp.viaInventory, 'never cut as the necklace pendant');
    }
  }
  ok('with the table: 13 of the 14 lines name their charm from Etsy\'s own SKU for the option (inventory), no question; Vesta waits');

  // the same rune number in the necklace and the earring family is the same rune; every rune SKU Etsy holds for these listings is a master SKU
  for (const id of ['1734126693', '1734124689', '1711906692', '1712164498', '1706155793', '1538136106', '1844264213']) {
    const t = tables[id], skus = [...new Set(t.products.filter(p => !p.d).map(p => p.sku.toUpperCase()))];
    const blank = skus.filter(s => !s), absent = skus.filter(s => s && !lib.has(s));
    assert.deepEqual(absent, [], id + ': every SKU Etsy gives an option is in the master');
    assert(blank.length <= 1);
  }
  ok('every SKU Etsy keeps for these 7 listings\' options is a master SKU, spelt exactly (case only differs)');

  // Vesta: why, and the master SKUs to offer in order
  const v = find('4174408832', 'Vesta'), sp = read(v, { listingSkus: tables }), q = open(sp)[0];
  assert.equal(sp.inventoryHeld.sku, 'GreekGoddess_Necklace_4'.toUpperCase()); assert.equal(sp.designSku, 'GREEK GODDESS', 'unchanged: the listing\'s own SKU, still waiting on its option');
  assert.deepEqual(q.picks.map(p => p.sku), ['GREEKGODDESS_EARRING_4', 'GREEKGODDESS_NECKLACE_4']); assert(q.picks[1].conflict && !q.picks[0].conflict);
  assert.deepEqual(O.suggestCharms({ value: 'Vesta', line: lineOf(v), picks: q.picks, skus: lib.keys() }).map(x => x.sku), ['GREEKGODDESS_EARRING_4', 'GREEKGODDESS_NECKLACE_4']);
  // …and once a person answers it for the listing, that answer reads (saved per listing and value, as the Review card does)
  const maps = { '1712164498': { 'goddess symbol': { vesta: { field: 'design', value: 'GREEKGODDESS_EARRING_4' } } } };
  const sp2 = read(v, { listingSkus: tables, optionMaps: maps }); assert.equal(sp2.designSku, 'GREEKGODDESS_EARRING_4'); assert.equal(sp2.skuSource, 'option'); assert(!open(sp2).length);
  ok('Vesta: waits for a person with the earring twin offered first; the person\'s answer for the listing resolves it');
}

/* ── 4 · the rules behind it ── */
function rules(tables) {
  const T0 = tables;
  // families
  assert.equal(O.skuFamily('RUNE_NECKLACE_CHARM-3'), 'necklace'); assert.equal(O.skuFamily('ZODIAC_EARRINGS-6'), 'earring'); assert.equal(O.skuFamily('HUGGIE HOOPS- ZODIAC'), 'earring');
  assert.equal(O.skuFamily('BIRTHFLOWER_CHOCKER-4'), 'necklace'); assert.equal(O.skuFamily('ZODIAC_CONSTE_GEM-5'), '', 'no family word: no opinion'); assert.equal(O.skuFamily('NECKLACE_EARRING_SET'), '', 'two families: no opinion');
  assert.equal(O.skuFamily('PENDANTNECKLACES'), '', 'whole words only');
  assert.equal(O.lineFamily(lineOf(find('4174408832', 'Vesta'))), 'earring'); assert.equal(O.lineFamily(lineOf(find('4174818039', 'Algiz - Divine Plan'))), 'necklace');
  assert.equal(O.lineFamily({ title: 'Wolf Charm Necklace or Earrings', variations: [] }), '', 'a title that names both says nothing');
  assert.equal(O.lineFamily({ title: 'Wolf Charm Necklace', variations: [{ name: 'Type', value: 'Charm only' }] }), '', 'a charm sold alone is no family');
  assert.equal(O.lineFamily({ title: 'Wolf Charm Necklace or Earrings', variations: [{ name: 'Type', value: 'Earrings' }] }), 'earring', 'what the buyer chose beats the title');
  assert.deepEqual(O.familyTwins('GreekGoddess_Necklace_4', 'earring', ctx().masterEntry, ctx().masterLoose), ['GREEKGODDESS_EARRING_4']);
  assert.deepEqual(O.familyTwins('GreekGoddess_Necklace_4', 'necklace', ctx().masterEntry, ctx().masterLoose), [], 'already that family'); assert.deepEqual(O.familyTwins('FOO_NECKLACE_9', 'earring', ctx().masterEntry, ctx().masterLoose), [], 'no twin in the master: nothing invented');
  ok('families: whole-word NECKLACE / EARRING… of a SKU against what the line is; twins are offered only when the master holds them');

  // the same Etsy table on a NECKLACE line of the same listing: Etsy's SKU is the right family, so it decides without a question
  const v = lineOf(find('4174408832', 'Vesta')); v.title = 'Greek Goddess Necklace • Feminine + Delicate Asteroid Signs';
  let sp = O.interpretLine(orderOf(find('4174408832', 'Vesta')), v, ctx({ listingSkus: T0 }));
  assert.equal(sp.designSku, 'GREEKGODDESS_NECKLACE_4'); assert.equal(sp.skuSource, 'inventory'); assert(!open(sp).length);
  ok('the umbrella SKU "GREEK GODDESS" (a master design of its own) no longer hides Etsy\'s SKU for the option, when it is the right family');

  // …never when the line already resolves: a person's pick stands; a SKU some product still carries is the product's own
  const algiz = find('4174818039', 'Algiz - Divine Plan');
  sp = read(algiz, { listingSkus: T0, optionMaps: { '1734126693': { 'viking rune symbol': { 'algiz - divine plan': { field: 'design', value: 'RUNE_NECKLACE_CHARM-0' } } } } });
  assert.equal(sp.designSku, 'RUNE_NECKLACE_CHARM-14', 'Paul\'s order: Etsy\'s SKU for the option outranks an earlier person\'s pick (already so before)');
  const gg = find('4174408832', 'Vesta'); const ggNeck = lineOf(gg); ggNeck.title = 'Greek Goddess Necklace';
  sp = O.interpretLine(orderOf(gg), ggNeck, ctx({ listingSkus: T0, optionMaps: { '1712164498': { 'goddess symbol': { vesta: { field: 'design', value: 'GREEKGODDESS_NECKLACE_2' } } } } }));
  assert.equal(sp.designSku, 'GREEKGODDESS_NECKLACE_2', 'an answered option is not read differently by the new rule'); assert(!sp.viaInventory);
  // a transaction SKU that one product of the listing still carries is that product's own: never replaced by another product's SKU
  const mini = { at: 1, n: 2, products: [{ id: '1', sku: 'RUNE_NECKLACE_CHARM-3', d: 0, pv: [['513', '71']] }, { id: '2', sku: 'RUNE_NECKLACE_CHARM-4', d: 0, pv: [['513', '72']] }] };
  const held = lineOf(algiz); held.sku = 'RUNE_NECKLACE_CHARM-3'; held.productId = '2'; held.variations = [{ name: 'Viking Rune Symbol', value: 'Raidho - Nobility', propertyId: '513', valueId: '72' }];
  sp = O.interpretLine(orderOf(algiz), held, ctx({ listingSkus: { '1734126693': mini } }));
  assert.equal(sp.designSku, 'RUNE_NECKLACE_CHARM-3', 'a transaction SKU that is another product\'s own, naming a design, is never replaced'); assert(!sp.viaInventory);
  ok('lines that already resolve read as before (a person\'s answer for the option, a SKU that is some product\'s own)');

  // a SKU Etsy gives the option that the master does not hold waits, and is never a look-alike
  const lib0 = new Map(lib); lib.delete('RUNE_NECKLACE_CHARM-14');
  try {
    sp = read(algiz, { listingSkus: T0 }); const q = open(sp).find(p => p.optionValue === 'Algiz - Divine Plan');
    assert(q, 'waits'); assert.match(q.why, /RUNE_NECKLACE_CHARM-14, is not in any master file/); assert(!sp.viaInventory || sp.designSku !== 'RUNE_NECKLACE_CHARM-13');
    assert(!(q.picks || []).length, 'nothing invented to offer');
    assert(!O.suggestCharms({ value: algiz.variations[1].value, line: lineOf(algiz), picks: q.picks, skus: lib.keys() }).some(x => /^RUNE_NECKLACE_CHARM-(13|15)$/.test(x.sku)), 'no neighbour offered as the rune');
  } finally { for (const [k, v] of lib0) lib.set(k, v); }
  ok('a value whose Etsy SKU is not in the master waits, says so, and no neighbour is offered as its charm');

  // a value Etsy keeps NO SKU for (one Zodiac REVAMP value and one Birth Flower value have none): waits, says so
  for (const [id, prop] of [['1706155793', '514'], ['1844264213', '513']]) {
    const t = T0[id], blank = t.products.find(p => !p.sku), mate = DATA.lines.find(x => x.listing === id);
    const bought = Object.assign({}, mate, { product: blank.id, variations: mate.variations.map(vv => vv.propertyId === prop ? Object.assign({}, vv, { valueId: blank.pv.find(a => a[0] === prop)[1], value: 'A new choice' }) : Object.assign({}, vv, { valueId: (blank.pv.find(a => a[0] === vv.propertyId) || [0, vv.valueId])[1] })), sku: '' });
    sp = O.interpretLine(orderOf(bought), lineOf(bought), ctx({ listingSkus: T0 }));
    const q = open(sp)[0]; assert(q, id + ' waits: ' + JSON.stringify(sp.problems)); assert.match(q.why, /Etsy has no SKU on this option/); assert(!sp.designSku);
  }
  ok('an option Etsy keeps no SKU for waits, and says "Etsy has no SKU on this option" (Zodiac REVAMP and Birth Flower earrings each have one such value)');

  // one SKU for every choice of the listing (the disc listing): a FONT option (Fonts: 16"/ Typewriter, Font: Stylish) has no SKU and picks no charm, so it is no question
  // any more (OPTFONT, charm-nest-orders.js fontRead): it is read as the font asked for and shown at engraving, whatever Etsy keeps for the listing
  for (const [rid, val, font] of [['4175370240', '16"/ Typewriter', 'Typewriter'], ['4172791262', 'Stylish', 'Stylish']]) {
    sp = read(find(rid, val), { listingSkus: T0 }); assert.equal(open(sp).length, 0, rid + ': a font option asks nothing: ' + JSON.stringify(sp.problems)); assert.equal(sp.font && sp.font.asked, font, rid);
  }
  ok('a font option is read as the font asked for and waits for nothing: no SKU is looked for it, none is offered');
}

/* ── 5 · the Review Options list ── */
function suggestions(tables) {
  const skus = [...lib.keys()];
  const libra = lineOf(find('4175402612', 'Libra'));
  // no table: the master SKUs named like the value, all of them, exact first
  let s = O.suggestCharms({ value: 'Libra', line: libra, picks: [], skus });
  assert.deepEqual(s.map(x => x.sku), ['ZODIAC ICONS LIBRA', 'ZODIAC ICONS LIBRA (HUGGIE)']);
  s = O.suggestCharms({ value: 'Morning Glory', line: lineOf(find('4171709020', 'Morning Glory')), picks: [], skus });
  assert.equal(s[0].sku, 'MORNING GLORY', 'the SKU named exactly like the value comes first'); assert.match(s[0].why, /exactly/); assert.equal(s[1].sku, 'MORNING GLORY MARIGOLD PEONY NARCISSUS');
  // more than three, nothing cut after the thirteenth: 30 master SKUs that hold the word
  const many = Array.from({ length: 30 }, (_, i) => 'ZODIAC_X' + String(i).padStart(2, '0') + ' LEO'); many.push('LEO');
  s = O.suggestCharms({ value: 'Leo', line: libra, picks: [], skus: many, max: 40 }); assert.equal(s.length, 31); assert.equal(s[0].sku, 'LEO');
  assert.equal(O.suggestCharms({ value: 'Leo', line: libra, picks: [], skus: many }).length, 8, 'at most eight by default');
  // a "(HUGGIE)" twin never ahead of its plain design unless the line is a huggie; another family's design after the line's
  s = O.suggestCharms({ value: 'Pisces', line: libra, picks: [], skus: ['PISCES_68933 (HUGGIE)', 'ZODIAC DISCS PISCES', 'PISCES_68933'] });
  assert.deepEqual(s.map(x => x.sku), ['PISCES_68933', 'ZODIAC DISCS PISCES', 'PISCES_68933 (HUGGIE)'].sort((a, b) => a.length - b.length || a.localeCompare(b)).filter(x => !/HUGGIE/.test(x)).concat(['PISCES_68933 (HUGGIE)']));
  const hug = lineOf(find('4171010675', 'Silver')); s = O.suggestCharms({ value: 'Pisces', line: hug, picks: [], skus: ['PISCES_68933', 'PISCES_68933 (HUGGIE)'] }); assert.equal(s.length, 2);
  s = O.suggestCharms({ value: 'Zodiac Sign', line: libra, picks: [], skus: ['ZODIAC_NECKLACE_DISC-1', 'ZODIAC_EARRINGS-1'] }); assert.equal(s[0].sku, 'ZODIAC_EARRINGS-1', 'the line\'s own family first');
  // the lead word of "Name - meaning" values finds a master named for the name alone
  s = O.suggestCharms({ value: 'Algiz - Divine Plan', line: lineOf(find('4174818039', 'Algiz - Divine Plan')), picks: [], skus: ['ALGIZ', 'ALGIZ RUNE HEART', 'KENAZ'] });
  assert.deepEqual(s.map(x => x.sku), ['ALGIZ', 'ALGIZ RUNE HEART']);
  // Etsy's own SKU for the option, when a person must still choose, is always first (and is not repeated by the name search)
  const bad = tables['1712164498'], l = find('4174408832', 'Vesta');
  s = O.suggestCharms({ value: 'Vesta', line: lineOf(l), picks: O.inventoryPicks(lineOf(l), bad, ctx().masterEntry, ctx().masterLoose), skus });
  assert.deepEqual(s.map(x => x.sku), ['GREEKGODDESS_EARRING_4', 'GREEKGODDESS_NECKLACE_4']); assert.equal(new Set(s.map(x => x.sku)).size, s.length);
  ok('Review Options: Etsy\'s SKU (or its family twin) first, then the SKU named exactly like the value, then the rest; nothing cut at three');
}

(async () => {
  let failed = 0, tables = null;
  const run = async (name, fn) => { try { await fn(); } catch (e) { failed++; console.log('  ✗ ' + name + ': ' + (e && e.stack ? e.stack.split('\n').slice(0, 5).join('\n') : e)); } };
  await run('no table', noTable);
  await run('cloud', async () => { tables = await cloud(); });
  if (tables) { await run('with table', () => withTable(tables)); await run('rules', () => rules(tables)); await run('suggestions', () => suggestions(tables)); }
  console.log(failed ? `options-link: ${failed} failed` : 'options-link: all passed'); process.exit(failed ? 1 : 0);
})();
