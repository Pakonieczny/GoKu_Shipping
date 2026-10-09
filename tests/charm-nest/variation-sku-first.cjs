// The SKU Etsy keeps for the variation bought is read before anything else (Paul, 9 Oct 2026: "Some Etsy listings actually have
// SKUs that are directly tied to the options in the drop-down menus … the choice the user made in the drop-down menu will
// directly correlate to the charm that has to be located from the repository").
//  1 · interpretation, offline (charm-nest-orders.js): the transaction's SKU, then the listing inventory's SKU for the product
//      bought; the option a SKU is tied to is not asked about; a SKU shared by every value of the option still is; a SKU no master
//      holds waits for a person and never becomes a sibling's design; metal still comes from its own option.
//  2 · the cloud table (charmNestLibrary listingSkus): the exact Etsy and Firestore cost, offline with fakes.
// No network, no Etsy, no AI.   node tests/charm-nest/variation-sku-first.cjs
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
const O = require(path.join(root, 'charm-nest-orders.js'));
const T = require(path.join(fnDir, '_charmNestListingSkus.js'));
const ok = name => console.log('  ✓', name);

/* ── part 1 · interpretation ── */
function part1() {
  const lib = {};
  for (const k of ['ARIES_1', 'TAURUS_2', 'PISCES_68933', 'JAN_GARNET', 'GREEK GODDESS', 'SPORTS 12 - BULLSEYE', 'MAPLE_8065', 'BR-ALS-02']) lib[k] = {};
  const loose = {}; for (const k of Object.keys(lib)) loose[O.looseKey(k)] = k;
  const ctx = (extra) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: s => lib[s] || null, masterLoose: s => loose[O.looseKey(s)] || '' }, extra);
  const order = { receiptId: '4900000001', updateTs: 1, staffNote: '', buyerMessage: '' };
  // a Zodiac-style listing: Metal Colour (property 200) and Zodiac Sign (property 513); the SKU names the sign, the metal is its own option
  const METAL = { gold: ['200', '1'], silver: ['200', '2'] }, SIGN = { aries: ['513', '11'], taurus: ['513', '12'], pisces: ['513', '13'] };
  const vr = (name, value, ids) => ({ name, value, propertyId: ids[0], valueId: ids[1] });
  const mk = (sku, metal, sign, over) => Object.assign({ transactionId: '49000000011', listingId: '1538136106', productId: '', sku, title: 'Zodiac Gem Necklace, Zodiac Sign Disc', quantity: 1,
    metalKey: metal, metalLabel: metal === 'gold' ? 'Gold' : 'Silver', personalization: [], variations: [vr('Metal Colour', metal === 'gold' ? 'Gold' : 'Silver', METAL[metal]), vr('Zodiac Sign', sign[0].toUpperCase() + sign.slice(1), SIGN[sign])] }, over || {});
  const sku2 = { aries: 'ARIES_1', taurus: 'TAURUS_2', pisces: 'PISCES_68933' };
  // inventory: one product per metal and sign; each sign's SKU is on both metals' products (so it is tied to the sign, never to the metal)
  let pid = 100;
  const products = []; for (const m of ['gold', 'silver']) for (const s of ['aries', 'taurus', 'pisces']) products.push({ id: String(++pid), sku: sku2[s], d: 0, pv: [METAL[m], SIGN[s]] });
  const tied = { at: 1, n: products.length, products };
  const kinds = sp => sp.problems.map(p => p.kind);

  // before: no table. A SKU the master holds names the design, but the Zodiac Sign option is still asked what it decides, as today
  let sp = O.interpretLine(order, mk('ARIES_1', 'gold', 'aries'), ctx());
  assert.equal(sp.designSku, 'ARIES_1'); assert.deepEqual(kinds(sp), ['needsMapping'], 'without the listing\'s table the option is asked, as before: ' + JSON.stringify(sp.problems));
  ok('no table: behaves as before (the SKU is the design; the sign option is asked)');

  // with the table: the SKU is the sign's own (never on another sign) → the option is answered by the SKU; metal still from its own option
  sp = O.interpretLine(order, mk('ARIES_1', 'gold', 'aries'), ctx({ listingSkus: { '1538136106': tied } }));
  assert.equal(sp.designSku, 'ARIES_1'); assert.deepEqual(kinds(sp), [], 'a SKU tied to the option answers it: ' + JSON.stringify(sp.problems));
  assert.equal(sp.material, 'gold', 'the metal comes from its own option, not the SKU'); assert.equal(sp.skuSource, 'transaction');
  const z = sp.options.find(o => o.name === 'Zodiac Sign'); assert(z && z.mapped && z.mapped.field === 'design' && z.mapped.value === 'ARIES_1' && z.mapped.source === 'sku', JSON.stringify(z));
  sp = O.interpretLine(order, mk('PISCES_68933', 'silver', 'pisces'), ctx({ listingSkus: { '1538136106': tied } }));
  assert.equal(sp.designSku, 'PISCES_68933'); assert.equal(sp.material, 'silver'); assert.deepEqual(kinds(sp), []);
  ok('table: a SKU tied to the sign answers the sign option; the metal is still read from the metal option');

  // another customer's choice never stands in: a person's pick for Aries does not turn this Taurus order into anything else
  const om = { '1538136106': { 'zodiac sign': { taurus: { field: 'design', value: 'PISCES_68933' } } } };
  sp = O.interpretLine(order, mk('TAURUS_2', 'gold', 'taurus'), ctx({ optionMaps: om }));
  assert.equal(sp.designSku, 'PISCES_68933', 'without the table a person\'s pick is the answer, as before');
  sp = O.interpretLine(order, mk('TAURUS_2', 'gold', 'taurus'), ctx({ optionMaps: om, listingSkus: { '1538136106': tied } }));
  assert.equal(sp.designSku, 'TAURUS_2', 'the SKU Etsy keeps for the variation outranks a charm somebody picked for the option'); assert.equal(sp.skuSource, 'transaction'); assert.deepEqual(kinds(sp), []);
  sp = O.interpretLine(order, mk('ARIES_1', 'gold', 'aries'), ctx({ optionMaps: om, listingSkus: { '1538136106': tied } }));
  assert.equal(sp.designSku, 'ARIES_1', 'and a pick for another sign changes nothing for this one');
  ok('table: the variation\'s SKU outranks a person\'s pick for the option; picks stand when there is no table');

  // a SKU shared by every sign (Greek Goddess style) says nothing about which was bought: the option is still asked
  const shared = { at: 1, n: 3, products: [101, 102, 103].map((id, i) => ({ id: String(id), sku: 'GREEK GODDESS', d: 0, pv: [['200', '1'], Object.values(SIGN)[i]] })) };
  // (every product has the one SKU, so the cloud stores it as { uni }, but a hand-made table with several equal SKUs must read the same)
  sp = O.interpretLine(order, mk('GREEK GODDESS', 'gold', 'aries'), ctx({ listingSkus: { '1538136106': shared } }));
  assert.equal(sp.designSku, 'GREEK GODDESS'); assert.deepEqual(kinds(sp), ['needsMapping'], 'a shared SKU does not answer the option: ' + JSON.stringify(sp.problems));
  sp = O.interpretLine(order, mk('GREEK GODDESS', 'gold', 'aries'), ctx({ listingSkus: { '1538136106': { at: 1, n: 3, uni: 'GREEK GODDESS' } } }));
  assert.deepEqual(kinds(sp), ['needsMapping'], 'the compact table of one SKU says the same');
  ok('table: a SKU every value shares still raises the option question');

  // the transaction came with no SKU: the product bought (by id, else by its option ids) gives it
  sp = O.interpretLine(order, mk('', 'gold', 'taurus', { productId: '102' }), ctx({ listingSkus: { '1538136106': tied } }));
  assert.equal(sp.designSku, 'TAURUS_2'); assert.equal(sp.skuSource, 'inventory'); assert.equal(sp.viaInventory.by, 'product');
  sp = O.interpretLine(order, mk('', 'silver', 'taurus'), ctx({ listingSkus: { '1538136106': tied } }));
  assert.equal(sp.designSku, 'TAURUS_2'); assert.equal(sp.skuSource, 'inventory'); assert.equal(sp.viaInventory.by, 'options'); assert.deepEqual(kinds(sp), []);
  sp = O.interpretLine(order, mk('', 'silver', 'taurus'), ctx());
  assert(!sp.designSku); assert(kinds(sp).length > 0, 'no SKU and no table: waits for a person, as before');
  ok('table: a line without a SKU takes the SKU of its product (by product id, else by option values)');

  // only the listing's own SKU came through: it masks the variation's. The product's own SKU (a master design) is taken.
  const own = { at: 1, n: 7, products: products.concat([{ id: '999', sku: 'ZODIAC_ALL', d: 0, pv: [] }]) };
  const wideProducts = products.map(p => Object.assign({}, p, { sku: 'ZODIAC_ALL' })); wideProducts[1].sku = 'TAURUS_2';
  const wide = { at: 1, n: 6, products: wideProducts };
  sp = O.interpretLine(order, mk('ZODIAC_ALL', 'gold', 'taurus', { productId: wideProducts[1].id }), ctx({ listingSkus: { '1538136106': wide } }));
  assert.equal(sp.designSku, 'TAURUS_2', 'the listing\'s own SKU does not stand over the product\'s SKU'); assert.equal(sp.viaInventory.tx, 'ZODIAC_ALL'); assert.equal(sp.skuSource, 'inventory');
  // …even when the master happens to hold the listing's SKU too
  lib['ZODIAC_ALL'] = {};
  sp = O.interpretLine(order, mk('ZODIAC_ALL', 'gold', 'taurus', { productId: wideProducts[1].id }), ctx({ listingSkus: { '1538136106': wide } }));
  assert.equal(sp.designSku, 'TAURUS_2', 'a listing-level SKU in the master does not mask the variation\'s'); delete lib['ZODIAC_ALL'];
  // a transaction SKU that is the product\'s own, naming a design, is never replaced
  sp = O.interpretLine(order, mk('ARIES_1', 'gold', 'aries', { productId: '101' }), ctx({ listingSkus: { '1538136106': own } }));
  assert.equal(sp.designSku, 'ARIES_1'); assert(!sp.viaInventory);
  ok('table: the listing\'s own SKU never masks the SKU of the product bought; the product\'s own SKU is never replaced');

  // a SKU no master file holds waits for a person, and never becomes another option\'s design
  const unknown = { at: 1, n: 6, products: products.map((p, i) => Object.assign({}, p, { sku: 'BIRTH_' + (p.pv[1][1]) })) };
  sp = O.interpretLine(order, mk('BIRTH_12', 'gold', 'taurus', { productId: unknown.products[1].id }), ctx({ listingSkus: { '1538136106': unknown } }));
  assert.equal(sp.designSku, 'BIRTH_12'); assert(!lib[sp.designSku], 'not a sibling\'s design'); assert(kinds(sp).length > 0, 'the SKU is not in the master: a person decides (the option first, as today): ' + JSON.stringify(sp.problems));
  sp = O.interpretLine(order, mk('BIRTH_12', 'gold', 'taurus', { productId: unknown.products[1].id, variations: [] }), ctx({ listingSkus: { '1538136106': unknown } }));
  assert.equal(sp.designSku, 'BIRTH_12'); assert.deepEqual(kinds(sp), ['unmatchedSku'], 'with no option left to ask, the unknown SKU itself is the card');
  // the listing\'s own SKU (unknown) came with a product whose own SKU is unknown too: the card names the product\'s, not the listing\'s
  const unk2 = { at: 1, n: 6, products: products.map(p => Object.assign({}, p, { sku: 'BIRTH_6106' })) }; unk2.products[1].sku = 'BIRTH_12';
  sp = O.interpretLine(order, mk('BIRTH_6106', 'gold', 'taurus', { productId: unk2.products[1].id, variations: [] }), ctx({ listingSkus: { '1538136106': unk2 } }));
  assert.equal(sp.designSku, 'BIRTH_12', 'the question is about the product\'s SKU'); assert.equal(sp.skuSource, 'inventory'); assert.deepEqual(kinds(sp), ['unmatchedSku']); assert.equal(sp.problems[0].sku, 'BIRTH_12');
  // …but a transaction SKU that names a master design is not given up for a product SKU the master does not hold
  lib['KNOWN_ALL'] = {};
  sp = O.interpretLine(order, mk('KNOWN_ALL', 'gold', 'taurus', { productId: unk2.products[1].id }), ctx({ listingSkus: { '1538136106': unk2 } }));
  assert.equal(sp.designSku, 'KNOWN_ALL'); assert(!sp.viaInventory); delete lib['KNOWN_ALL'];
  ok('table: a SKU the master does not hold waits for a person and never falls back to a sibling');

  // two customers, one listing: each line is read with its own SKU
  const a = O.interpretLine(order, mk('ARIES_1', 'gold', 'aries'), ctx({ listingSkus: { '1538136106': tied } })), b = O.interpretLine(order, mk('TAURUS_2', 'silver', 'taurus'), ctx({ listingSkus: { '1538136106': tied } }));
  assert.equal(a.designSku, 'ARIES_1'); assert.equal(b.designSku, 'TAURUS_2'); assert.equal(a.material, 'gold'); assert.equal(b.material, 'silver');
  // another listing\'s table is not this listing\'s
  sp = O.interpretLine(order, mk('ARIES_1', 'gold', 'aries'), ctx({ listingSkus: { '42': tied } })); assert.deepEqual(kinds(sp), ['needsMapping']);
  ok('table: each line keeps its own SKU; another listing\'s table says nothing about this one');

  // spelling: Etsy\'s spacing and punctuation (the master\'s "SPORTS 12 - BULLSEYE" for "SPORTS 12- BULLSEYE"); never when two master SKUs read alike
  sp = O.interpretLine(order, mk('Sports 12- Bullseye', 'gold', 'aries', { variations: [] }), ctx());
  assert.equal(sp.designSku, 'SPORTS 12 - BULLSEYE'); assert.equal(sp.skuSource, 'spelling'); assert.deepEqual(kinds(sp), []);
  sp = O.interpretLine(order, mk('maple_8065-co', 'gold', 'aries', { variations: [] }), ctx());
  assert.equal(sp.designSku, 'MAPLE_8065', 'a charm-only mark on a SKU the master spells another way'); assert.equal(sp.skuSource, 'variation');
  lib['SPORTS 12-BULLSEYE'] = {}; loose[O.looseKey('SPORTS 12-BULLSEYE')] = '';   // two master SKUs read alike: the index says so with ''
  sp = O.interpretLine(order, mk('Sports 12 Bullseye', 'gold', 'aries', { variations: [] }), ctx());
  assert.equal(sp.designSku, 'SPORTS 12 BULLSEYE'); assert(kinds(sp).includes('unmatchedSku'), 'two master SKUs read alike: a person decides'); delete lib['SPORTS 12-BULLSEYE'];
  ok('spelling: the master\'s own spelling of a SKU is found; two that read alike are left to a person');

  // an exact master SKU is untouched, and an older page (no masterLoose, no table) reads as before
  sp = O.interpretLine(order, mk('ARIES_1', 'gold', 'aries', { variations: [] }), { optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: s => lib[s] || null });
  assert.equal(sp.designSku, 'ARIES_1'); assert.deepEqual(kinds(sp), []);
  ok('no table, no spelling index: unchanged');
}

/* ── part 2 · the cloud table ── */
async function part2() {
  const store = new Map(); let reads = 0, writes = 0;
  const docRef = (coll, id) => ({ id, _k: coll + '/' + id, async set(d) { writes++; store.set(coll + '/' + id, JSON.parse(JSON.stringify(d))); } });
  const snap = ref => { const d = store.get(ref._k); return { exists: !!d, id: ref.id, data: () => (d ? JSON.parse(JSON.stringify(d)) : undefined) }; };
  const db = { collection: c => ({ doc: id => docRef(c, id) }), async getAll(...refs) { reads += refs.length; return refs.map(snap); } };
  const inv = (skuByValue) => ({ products: Object.entries(skuByValue).map(([v, s], i) => ({ product_id: 5000 + i, sku: s, is_deleted: false, property_values: [{ property_id: 513, value_ids: [+v], values: ['x'] }, { property_id: 200, value_ids: [1], values: ['Gold'] }] })) });
  // compact: one SKU for every product → { uni }; several → the products
  assert.deepEqual(T.compact(inv({ 11: 'ALL', 12: 'ALL' }), 5), { at: 5, n: 2, uni: 'ALL' });
  const c3 = T.compact(inv({ 11: 'A1', 12: 'T2' }), 5); assert.equal(c3.products.length, 2); assert.deepEqual(c3.products[0].pv, [['513', '11'], ['200', '1']]); assert.equal(c3.products[0].id, '5000');
  assert.deepEqual(T.compact({ products: [] }, 5), { at: 5, n: 0, uni: '' });
  ok('table: a listing with one SKU is stored as one word, one with several as its products');

  const now = 1790000000000; let calls = [];
  const env = (extra) => Object.assign({ db, now, fetchInventory: async id => { calls.push(id); return inv({ 11: 'A1', 12: 'T2' }); } }, extra);
  // 10 listings asked at once: 6 Etsy calls at most; the rest are pending, the stored ones are not asked again
  const ids = Array.from({ length: 10 }, (_, i) => String(7000000 + i));
  let r = await T.lookup(ids, env());
  assert.equal(r.etsyCalls, 6); assert.equal(calls.length, 6); assert.equal(Object.keys(r.tables).length, 6); assert.equal(r.pending.length, 4); assert.equal(r.why, 'batch'); assert.deepEqual(r.cap, { used: 6, max: 60 });
  assert.equal(reads, 11, 'one read per listing asked about, plus the day\'s budget'); assert.equal(writes, 7, 'one write per listing fetched, plus the budget');
  calls = []; reads = writes = 0;
  r = await T.lookup(ids, env({ now: now + 60000 }));
  assert.equal(calls.length, 4, 'only the four not yet stored are asked: ' + calls.join()); assert.equal(r.pending.length, 0); assert.equal(r.cap.used, 10);
  calls = []; reads = writes = 0;
  r = await T.lookup(ids, env({ now: now + 120000 }));
  assert.equal(calls.length, 0, 'a stored table is not asked of Etsy again inside 7 days'); assert.equal(writes, 0, 'and nothing is written'); assert.equal(reads, 11);
  ok('cloud: at most 6 Etsy calls a request; a stored table costs one Firestore read and no Etsy call');

  // the day\'s 60: the 61st is not asked; a new day starts again; a week later a table is asked again
  const many = Array.from({ length: 25 }, (_, i) => String(8000000 + i)); calls = [];
  let total = 0, last;
  for (let i = 0; i < 12; i++) { last = await T.lookup(many.map((x, j) => String(8000000 + i * 25 + j)), env({ now: now + 3600000 + i * 1000 })); total += last.etsyCalls; }
  assert.equal(total, 60 - 10, 'the day\'s 60 include the 10 already made'); assert.equal(last.why, 'day'); assert(last.pending.length > 0);
  calls = []; r = await T.lookup(['8100000'], env({ now: now + 86400000 + 3600000 * 2 }));
  assert.equal(calls.length, 1, 'next day: asked'); assert.equal(r.cap.used, 1);
  calls = []; r = await T.lookup([ids[0]], env({ now: now + 8 * 86400000 }));
  assert.equal(calls.length, 1, 'after 7 days a table is asked again');
  ok('cloud: 60 Etsy calls a day for the whole shop; a table older than 7 days is refreshed');

  // an Etsy failure (limit, network) is not stored, is not retried in the same request, and the older table still answers
  calls = []; store.set('Charm_Listing_Skus/9000001', { at: now - 9 * 86400000, n: 2, uni: 'OLD' });
  const boom = async id => { calls.push(id); const e = new Error('429'); e.status = 429; throw e; };
  r = await T.lookup(['9000001', '9000002', '9000003'], env({ fetchInventory: boom, now: now + 20 * 86400000 }));
  assert.equal(calls.length, 1, 'after a refusal nothing more is asked in the request'); assert.equal(r.why, 'error'); assert.equal(r.tables['9000001'].uni, 'OLD', 'the table already held still answers');
  assert(!store.has('Charm_Listing_Skus/9000002'), 'a failed ask is not stored');
  const gone = async id => { const e = new Error('404'); e.status = 404; throw e; };
  r = await T.lookup(['9000004'], env({ fetchInventory: gone, now: now + 20 * 86400000 })); assert.equal(r.tables['9000004'].gone, true); assert.equal(r.pending.length, 0);
  ok('cloud: a refused ask is not stored and not repeated; a deleted listing is remembered for the week');

  // the sandbox and a cache-only request never call Etsy
  calls = []; r = await T.lookup(['9100001', '9100002'], env({ cacheOnly: true })); assert.equal(calls.length, 0); assert.equal(r.etsyCalls, 0); assert.equal(r.why, 'cache'); assert.equal(r.pending.length, 2);
  // bad ids are dropped
  calls = []; r = await T.lookup(['x', '', '12', "9'; drop"], env()); assert.equal(calls.length, 0); assert.deepEqual(r.tables, {});
  ok('cloud: the sandbox and cache-only requests make no Etsy call; ids that are not listing ids are dropped');
}

/* ── part 3 · the op itself (charmNestLibrary) with fakes: the sandbox does not call Etsy, production does ── */
async function part3() {
  const store = new Map(); let etsyCalls = 0;
  const docRef = (coll, id) => ({ id, _k: coll + '/' + id, async get() { const d = store.get(coll + '/' + id); return { exists: !!d, id, data: () => (d ? JSON.parse(JSON.stringify(d)) : undefined) }; }, async set(d) { store.set(coll + '/' + id, JSON.parse(JSON.stringify(d))); } });
  const q = coll => ({ orderBy() { return q(coll); }, limit() { return q(coll); }, startAfter() { return q(coll); }, async get() { return { size: 0, docs: [], empty: true }; }, doc: id => docRef(coll, id) });
  const db = { collection: c => q(c), async getAll(...refs) { return Promise.all(refs.map(r => r.get())); } };
  const TS = { __ts: true };
  const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => TS, increment: n => n, delete: () => undefined }, FieldPath: { documentId: () => '__name__' } }), storage: () => ({ bucket: () => ({}) }) };
  const fakeEtsy = { getListingInventory: async id => { etsyCalls++; return { products: [{ product_id: 1, sku: 'A1', property_values: [{ property_id: 513, value_ids: [11] }] }, { product_id: 2, sku: 'T2', property_values: [{ property_id: 513, value_ids: [12] }] }] }; } };
  const Module = require('module'), realLoad = Module._load;
  Module._load = function (req, ...rest) { if (req === 'firebase-admin' || /[\/]firebaseAdmin(\.js)?$/.test(req) || req === './firebaseAdmin') return fakeAdmin; if (/[\/]_etsyMailEtsy(\.js)?$/.test(req)) return fakeEtsy; return realLoad.call(this, req, ...rest); };
  try {
    delete process.env.EDIT_PASSCODE;
    const lib = require(path.join(fnDir, 'charmNestLibrary.js'));
    const post = body => lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }).then(r => JSON.parse(r.body || '{}'));
    let r = await post({ op: 'listingSkus', listingIds: ['1538136106'], sandbox: true });
    assert.equal(etsyCalls, 0, 'the sandbox asks Etsy nothing: ' + JSON.stringify(r).slice(0, 200)); assert.deepEqual(r.pending, ['1538136106']); assert.equal(r.why, 'cache');
    r = await post({ op: 'listingSkus', listingIds: ['1538136106'] });
    assert.equal(etsyCalls, 1, JSON.stringify(r).slice(0, 300)); assert.equal(r.tables['1538136106'].products.length, 2);
    r = await post({ op: 'listingSkus', listingIds: ['1538136106'] });
    assert.equal(etsyCalls, 1, 'the second ask is answered from the stored table'); assert.equal(r.etsyCalls, 0);
    r = await post({ op: 'listingSkus', listingIds: ['1538136106'], sandbox: true });
    assert.equal(r.tables['1538136106'].products.length, 2, 'the sandbox reads what production stored'); assert.equal(etsyCalls, 1);
    ok('op: production asks Etsy once per listing a week; the sandbox reads the stored table and never asks');
  } finally { Module._load = realLoad; }
}

(async () => {
  let failed = 0;
  for (const [name, fn] of [['interpretation', part1], ['cloud table', part2], ['op', part3]]) {
    try { await fn(); } catch (e) { failed++; console.log('  ✗ ' + name + ': ' + (e && e.stack ? e.stack.split('\n').slice(0, 4).join('\n') : e)); }
  }
  console.log(failed ? `variation-sku-first: ${failed} failed` : 'variation-sku-first: all passed'); process.exit(failed ? 1 : 0);
})();
