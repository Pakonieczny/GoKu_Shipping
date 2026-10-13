// "Put all the queued SKUs on Etsy" in ONE press (ETSYBATCH, 13 Oct 2026; Paul, 02:51 UTC: "I need you to do all of that automatically to all etsy listings I have already asked you to update."),
// OFFLINE. Real Chromium on the stand-in site; the committed queue (charm-nest-etsy-queue.json) is read by the page itself. The Library function is a mock in front of the stand-in
// server: it answers the repo password door like the real one (a throwaway password that exists only in this process, a session token as the real door gives) and answers the op
// listingEtsySkuWrite with the REAL op (netlify/functions/_charmNestEtsyLink.js `make`) on the in-memory Firestore and Storage (tests/charm-nest/_sandboxFakes.cjs) and a FAKE ETSY
// that serves the eleven listings of the queue in the shapes Paul's shop has: a single SKU (swan 9, cat 12, dove 12, lion 12 products), two SKUs X / X-CO (monkey 60, bike 40), 40 blank + 5
// shop-wide placeholder SKUs (chameleon 60, wolf 60), the 816-product bar and the kite necklace (KITE: Metal Choice x Necklace Length, SKUs KITE / KITE-CO / blank, one disabled product: a STAND-IN grid, the real products are unread), and the reptile chameleon necklace (REPTILE 3-CHAMELEON / blank / REPTILE 3-CHAMELEON-CO over Metal Choice x Necklace Length: another STAND-IN grid). Bike, monkey, wolf, chameleon, dove and lion are the REAL products (ids, SKUs, option names and texts of the
// read-only Etsy reads of 13 Oct 2026: tests/charm-nest/etsy-link-families-data.json, ETSYFAM); prices, stock, readiness, the SKU list, swan, cat and the bar's options are stand-ins. NO real Etsy request exists here: every request that is not loopback is aborted, the fake Etsy is an object.
// EVERYTHING IN THIS FILE RAN ON THE OFFLINE FAKE ETSY. Only Paul's press settles the real write (listings_w, Etsy accepting the PUTs, the 816-product write time).
// It proves, for the page and the one press:
//   · the Etsy chip's panel gets ONE extra line, "Put all N queued SKUs on Etsy" (N = the number of listings in the committed queue file: 11 since the reptile chameleon, never a number written in the code); looking sends nothing; pressing it shows ONE plain confirm line and every listing with its SKU, in the queue's order;
//   · the repo password is asked ONCE (a wrong one writes nothing and no Etsy request; cancelling it sends nothing), then the page runs the listings in order, ONE request per listing, one at a time,
//     one progress row each with a small labelled spinner on the one in progress, and the same words the single press gives;
//   · each listing gets its safety copy BEFORE its PUT and exactly one PUT; only the SKUs change; at most 3 Etsy requests per listing and about 30 for the whole run;
//   · a listing that stops in the middle is reported and the rest still run; a second press says "Already there" for everything and makes no Etsy request; "needs listings_w" stops the whole run; the
//     chameleon waits for the swan; a closed tab loses nothing and pressing again resumes; the password is never in a URL, a log, the page or a stored value.
//   PW_DIR=<dir with playwright-core> CHROMIUM=<chrome> node tests/charm-nest/etsy-link-batch.cjs      (skipped without a browser)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert'), crypto = require('crypto');
const root = path.join(__dirname, '../..');
const fnDir = process.env.ETSY_OP_DIR || path.join(root, 'netlify/functions');   // (the op under test; a developer may point it at another checkout's functions)
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (e) { console.log('skipped: no playwright-core (' + pwDir + ')'); process.exit(0); }
const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(exe)) { console.log('skipped: no browser at ' + exe); process.exit(0); }
process.env.SHOP_ID = '987654'; process.env.CLIENT_ID = 'test-key'; process.env.CLIENT_SECRET = 'test-secret'; delete process.env.EDIT_PASSCODE;
const Fk = require('./_sandboxFakes.cjs');
const { store, blobs, writes, fileWrites, db, bucket } = Fk;
const FV = Fk.admin.firestore.FieldValue;
const Mod = require(path.join(fnDir, '_charmNestEtsyLink.js'));
const Batch = require(path.join(root, 'charm-nest-etsy-batch.js'));
const { start, Timestamp } = require('./bridge-server.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };
const RIGHT = 'r' + crypto.randomBytes(9).toString('hex'), WRONG = 'w' + crypto.randomBytes(9).toString('hex');   // throwaway passwords that exist only in this process
const SHOTS = process.env.SHOTS_DIR || '';
const clone = x => JSON.parse(JSON.stringify(x));
const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ═══ the committed queue ═══ */
const QUEUE_FILE = path.join(root, 'charm-nest-etsy-queue.json');
const QUEUE = Batch.normalize(JSON.parse(fs.readFileSync(QUEUE_FILE, 'utf8')));
const WANT = [   // the eleven, in the order of the run (Paul's asks of 12-13 Oct; ETSYLIST's runOrder: single SKUs first, the pairs and placeholders next, the 816-product bar; then the kite necklace, asked 13 Oct 04:42 UTC and appended as the 10th, then the reptile chameleon necklace, asked 13 Oct 13:43 UTC and appended as the 11th)
  ['1700763608', 'BEADY SWAN'], ['1801964156', 'CAT 1 - SITTING CAT'], ['1816178147', 'BEADY DOVE 4'], ['1801980518', 'BEADY CUTE LION'], ['4494184284', 'CHAMELEON_28151'],
  ['4494182906', 'WOLF_67948'], ['1745415868', 'BEADY MONKEY HEAR NO EVIL3'], ['1734130419', 'SPORTS 27'], ['1830130714', 'CURB'], ['1759609265', 'BEADY KITE'], ['1726597659', 'REPTILE 3']];
const NN = WANT.length, KITE = '1759609265', REPT = '1726597659';   // (every count below is NN, so the next listing Paul asks for is one more line in the queue file and in WANT)
const BUDGET = 3 * NN + 3;   // Etsy requests for a whole run: at most 3 per listing and 3 to spare (33 for the ten, 36 for the eleven)

/* ═══ the fake Etsy (the same behaviour as tests/charm-nest/etsy-link.cjs, many listings): it escapes the text it serves, stores what it is sent, gives every product a new id on each PUT, wants decimal
   prices, refuses a partial *_on_property set, refuses a SKU that varies over a property its sku_on_property does not name, and records every request ═══ */
const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
const unesc = s => String(s).replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
const money = x => ({ amount: Math.round(x * 100), divisor: 100, currency_code: 'USD' });
const etsyErr = (status, error) => Object.assign(new Error('Etsy API ' + status + ': ' + (error || '')), { status, data: { error: error || 'HTTP ' + status } });
const propOrder = products => { const o = []; for (const p of products) for (const v of p.property_values) if (!o.includes(v.property_id)) o.push(v.property_id); return o; };
function mkInv(rows, lists) {
  const ids = new Map(); let next = 5000, pid = 100;
  const vid = (p, t) => { const k = p + '|' + t; if (!ids.has(k)) ids.set(k, next++); return ids.get(k); };
  return Object.assign({
    products: rows.map(r => { const id = pid++; return { product_id: id, sku: r.sku, is_deleted: false,
      property_values: r.pv.map(([p, name, text]) => ({ property_id: p, property_name: name, scale_id: null, value_ids: [vid(p, text)], values: [text] })),
      offerings: [{ offering_id: id * 10, price: r.price, quantity: r.qty, is_enabled: r.on !== false, readiness_state_id: 7001 }] }; }),
    price_on_property: [], quantity_on_property: [], sku_on_property: [] }, lists || {});
}
function makeEtsy() {
  const L = new Map(), calls = [], puts = [], E = { L, calls, puts, fail: null };
  const serve = id => { const S = L.get(id).stored; return { count: S.products.length,
    products: S.products.map(p => ({ product_id: p.product_id, sku: esc(p.sku), is_deleted: false,
      property_values: p.property_values.map(v => ({ property_id: v.property_id, property_name: esc(v.property_name), scale_id: v.scale_id, value_ids: v.value_ids.slice(), values: v.values.map(esc) })),
      offerings: p.offerings.map(f => ({ offering_id: f.offering_id, price: money(f.price), quantity: f.quantity, is_enabled: f.is_enabled, is_deleted: false, readiness_state_id: f.readiness_state_id })) })),
    price_on_property: S.price_on_property.slice(), quantity_on_property: S.quantity_on_property.slice(), sku_on_property: S.sku_on_property.slice() }; };
  function applyPut(id, b) {
    const bad = m => { throw etsyErr(400, m); };
    if (!b || !Array.isArray(b.products) || !b.products.length) bad('products required');
    for (const p of b.products) {
      if (typeof p.sku !== 'string' || p.sku.length > 32) bad('sku must be a string of at most 32 characters');
      if (!Array.isArray(p.offerings) || p.offerings.length !== 1) bad('exactly one offering per product');
      const f = p.offerings[0];
      if (typeof f.price !== 'number' || !(f.price >= 0.2)) bad('price must be a decimal number');
      if (!Number.isInteger(f.quantity) || f.quantity < 0) bad('quantity must be a whole number');
      if (typeof f.is_enabled !== 'boolean') bad('is_enabled must be a boolean');
    }
    const total = propOrder(b.products).length, lists = ['price_on_property', 'quantity_on_property', 'sku_on_property'];
    if (total && lists.some(f => (b[f] || []).length === total)) for (const f of lists) { const k = (b[f] || []).length; if (k > 0 && k < total) bad(f + ': unsupported number of property IDs. Supports only zero or all ' + total + ' variation properties'); }
    for (const [f, key] of [['price_on_property', 'price'], ['sku_on_property', 'sku']]) {
      const decl = new Set((b[f] || []).map(Number)), seen = new Map();
      for (const p of b.products) {
        const g = p.property_values.filter(v => decl.has(v.property_id)).map(v => v.property_id + ':' + v.values.join('/')).sort().join('|'), val = key === 'sku' ? p.sku : p.offerings[0].price;
        if (seen.has(g) && seen.get(g) !== val) bad(f + ' does not cover how ' + key + ' varies');
        seen.set(g, val);
      }
    }
    const S = L.get(id); let seq = E.seq = (E.seq || 900000);
    S.stored = { products: b.products.map(p => { const f = p.offerings[0]; return { product_id: ++seq, sku: p.sku, is_deleted: false,
      property_values: p.property_values.map(v => ({ property_id: v.property_id, property_name: v.property_name, scale_id: v.scale_id, value_ids: v.value_ids.slice(), values: v.values.slice() })),
      offerings: [{ offering_id: ++seq, price: f.price, quantity: f.quantity, is_enabled: f.is_enabled, readiness_state_id: f.readiness_state_id }] }; }),
      price_on_property: b.price_on_property.slice(), quantity_on_property: b.quantity_on_property.slice(), sku_on_property: b.sku_on_property.slice() };
    E.seq = seq;
  }
  async function handle(method, p, q, b) {
    const m = /^\/listings\/(\d+)(\/inventory)?$/.exec(p), id = m && m[1];
    const call = { method, id, kind: m && m[2] ? 'inventory' : 'listing' }; calls.push(call);
    const fail = E.fail && E.fail(call); if (fail) throw fail;
    if (!m || !L.has(id)) throw etsyErr(404, 'no such listing');
    if (!m[2] && method === 'GET') return { listing_id: +id, state: L.get(id).state };
    if (m[2] && method === 'GET') return serve(id);
    if (m[2] && method === 'PUT') {
      assert.deepStrictEqual(q, { legacy: false }, 'the inventory is always written with legacy=false');
      puts.push({ id, safetyCopyFirst: [...blobs.keys()].some(k => k.startsWith('charmnest/custom-saves/' + id + '/') && k.endsWith('-before.json')) });
      applyPut(id, b); return serve(id);
    }
    throw etsyErr(405, 'unexpected request');
  }
  E.api = { get: (p, q) => handle('GET', p, q), put: (p, q, b) => handle('PUT', p, q, b) };
  E.add = (id, inv) => L.set(id, { stored: clone(inv), original: clone(inv), state: 'active' });
  return E;
}

/* the eleven listings, in the shapes the findings files describe (the real read of each is in plans/*sku-1013/dryrun; here the grids are rebuilt: sizes, SKU families, option lists) */
const metals = k => Array.from({ length: k }, (_, i) => 'Metal ' + (i + 1));
const grid = (ms, ls, skuFor, lists, extra) => mkInv(ms.flatMap((m, i) => ls.map((l, j) => ({ sku: skuFor(i, j, m, l), pv: [[514, 'Metal Choice', m], [513, 'Necklace Length', l]], price: 20 + i + j * 2, qty: 999, on: !(extra && extra.off && extra.off(i, j)) }))), lists);
const REAL = require('./etsy-link-families-data.json');   // the real products of bike, monkey, wolf, chameleon, dove, lion (stored as plain text; the fake serves them escaped, as Etsy does)
const realInv = d => ({ products: d.products.map((p, i) => ({ product_id: p.product_id, sku: p.sku, is_deleted: false,
    property_values: p.property_values.map(v => ({ property_id: v.property_id, property_name: v.property_name == null ? v.property_name : unesc(v.property_name), scale_id: v.scale_id, value_ids: v.value_ids.slice(), values: v.values.map(unesc) })),
    offerings: [{ offering_id: p.product_id * 10, price: 28 + (i % 4) * 4 + (i % 7), quantity: i === 5 ? 0 : 999, is_enabled: i !== 5, readiness_state_id: 7001 }] })),   // (stand-ins: prices, stock, one disabled product, readiness)
  price_on_property: d.price_on_property.slice(), quantity_on_property: d.quantity_on_property.slice(), sku_on_property: d.sku_on_property.slice() });
const SHAPE = {
  '1700763608': () => grid(metals(3), ['16"', '18"', '20"'], () => 'Swan_3608', { price_on_property: [514, 513] }),
  '1801964156': () => grid(metals(4), ['Necklace CHARM', 'Huggie CHARM SET', 'CHARM + Engraving'], () => 'CAT 1- Sitting Cat', { price_on_property: [514, 513] }),
  '1816178147': () => realInv(REAL.dove),
  '1801980518': () => realInv(REAL.lion),
  '4494184284': () => realInv(REAL.chameleon),
  '4494182906': () => realInv(REAL.wolf),
  '1745415868': () => realInv(REAL.monkey),
  '1734130419': () => realInv(REAL.bike),
  '1830130714': () => grid(metals(12), Array.from({ length: 68 }, (_, j) => 'Band ' + (j + 1)), () => 'BEADY', { price_on_property: [514, 513] }, { off: (i, j) => j % 12 === 11 }),   // 816 products, 68 of them disabled
  /* the kite necklace (STAND-IN grid in the shape of the shop's other Metal Choice x Necklace Length listings, wolf/monkey: SKU varies by Metal Choice only): the plain metals KITE, the engraved ones a blank SKU, the charm-only ones KITE-CO; Silver + Engrave / 20" is the order's own line; one product disabled */
  [KITE]: () => mkInv(['Silver', 'Silver + Engrave', 'Gold', 'Gold + Engrave', 'Silver-Charm Only'].flatMap((m, i) => ['16"', '18"', '20"', 'Charm Only-No Chain'].map((l, j) => ({ sku: i < 2 ? 'KITE' : i < 4 ? '' : 'KITE-CO', pv: [[514, 'Metal Choice', m], [513, 'Necklace Length', l]], price: 24 + i * 6 + j * 2, qty: 999, on: !(i === 4 && j === 3) }))), { price_on_property: [514, 513], sku_on_property: [514] }),   // 20 products, 1 disabled
  /* the reptile chameleon necklace (STAND-IN grid, the real products are unread: Paul's order shows Metal Choice silver and the SKU "REPTILE 3-CHAMELEON"): the plain metals carry the SKU the order reads today, REPTILE 3-CHAMELEON, the engraved ones a blank SKU, the charm-only ones a -CO variant; one product disabled */
  [REPT]: () => mkInv(['Silver', 'Silver + Engrave', 'Gold', 'Gold + Engrave', 'Silver-Charm Only'].flatMap((m, i) => ['16"', '18"', '20"'].map((l, j) => ({ sku: i === 0 || i === 2 ? 'REPTILE 3-CHAMELEON' : i < 4 ? '' : 'REPTILE 3-CHAMELEON-CO', pv: [[514, 'Metal Choice', m], [513, 'Necklace Length', l]], price: 22 + i * 5 + j * 2, qty: 999, on: !(i === 4 && j === 2) }))), { price_on_property: [514, 513], sku_on_property: [514] })   // 15 products, 1 disabled
};
const COUNTS = { '1700763608': 9, '1801964156': 12, '1816178147': 12, '1801980518': 12, '4494184284': 60, '4494182906': 60, '1745415868': 60, '1734130419': 40, '1830130714': 816, [KITE]: 20, [REPT]: 15 };
const MIRRORED = new Set(['1700763608', '1801964156', '1816178147', '1801980518']);   // (these have the inbox mirror's state: 2 Etsy requests; the others 3, the kite too: whether the mirror knows it is unread)
const masterEntry = sku => ({ sku, masterHash: 'm'.repeat(8), masterName: 'MASTER SKU_LIVING_MV_2026-0914.ai', widthPt: 33.29, heightPt: 25.74, holes: 1, engravable: true, aiPath: 'charmnest/master/' + sku.replace(/\W+/g, '_') + '.ai' });
const stripIds = inv => { const c = clone(inv); for (const p of c.products) { delete p.product_id; for (const f of p.offerings) delete f.offering_id; } return c; };

let tick = 0;
function world() {   // a fresh database, a fresh fake Etsy with the eleven listings, the REAL op on them
  store.clear(); blobs.clear(); writes.length = 0; fileWrites.length = 0;
  const E = makeEtsy();
  for (const [id, key] of WANT) {
    E.add(id, SHAPE[id]());
    store.set('Charm_Master_Index/' + key, masterEntry(key));
    if (MIRRORED.has(id)) store.set('EtsyMail_Listings/' + id, { state: 'active', title: 'a listing' });
  }
  const mod = Mod.make({ db, FV, bucket, etsy: E.api, meter: { bumpSimple() {}, flushNow: async () => {} }, now: () => 1760000000000 + (++tick) });
  return { E, mod, ops: [], inflight: 0, maxInflight: 0, delayMs: 0, unlocks: [], held: new Map() };
}
let W = null;

(async () => {
  /* ═══ 0 · the committed queue (no browser) ═══ */
  ok(QUEUE.length === NN && QUEUE.map(q => q.id + '|' + q.key).join('~') === WANT.map(w => w.join('|')).join('~'), 'the committed queue holds the ' + NN + ' listings Paul asked for, with their exact repo SKUs, in the order of the run: ' + QUEUE.map(q => q.id).join(','));
  ok([['1816178147', 'dove'], ['1801980518', 'lion'], ['4494184284', 'chameleon'], ['4494182906', 'wolf'], ['1745415868', 'monkey'], ['1734130419', 'bike']].every(([id, k]) => REAL[k].listingId === id && QUEUE.find(q => q.id === id).key === REAL[k].key), 'the real product data (ETSYFAM) is of exactly the queue\'s listings and keys');
  ok(QUEUE.every(q => /^[\x21-\x7e]+(?: [\x21-\x7e]+)*$/.test(q.key) && q.key.length <= 32), 'every key is plain printable ASCII, single spaces, at most 32 characters (the op would refuse anything else with no Etsy request)');
  ok(QUEUE[8].id === '1830130714' && QUEUE[9].id === KITE && QUEUE[10].id === REPT && QUEUE.find(q => q.id === '4494184284').after === '1700763608' && QUEUE.filter(q => q.after).length === 1, 'the 816-product bar is the 9th, the kite necklace the 10th, the reptile chameleon necklace the 11th and last (neither waits for anything); the chameleon comes after the swan (Paul: the chameleon after the swan press succeeds)');
  const rawKite = JSON.parse(fs.readFileSync(QUEUE_FILE, 'utf8')).listings[9];
  ok(QUEUE[9].key === 'BEADY KITE' && [...QUEUE[9].key].map(c => c.codePointAt(0).toString(16)).join(' ') === '42 45 41 44 59 20 4b 49 54 45' && rawKite.listingId === KITE && rawKite.note === 'kite necklace' && rawKite.askedAt === '2026-10-13T04:42Z' && !('products' in rawKite) && !('after' in rawKite), 'the 10th entry is listing 1759609265 with the key BEADY KITE (the title of the card with BEADY KITE, KITE and KITE_2181: ten plain characters, one space), asked 13 Oct 04:42Z, no product count known, no after');
  const rawRept = JSON.parse(fs.readFileSync(QUEUE_FILE, 'utf8')).listings[10];
  ok(QUEUE[10].key === 'REPTILE 3' && [...QUEUE[10].key].map(c => c.codePointAt(0).toString(16)).join(' ') === '52 45 50 54 49 4c 45 20 33' && rawRept.listingId === REPT && rawRept.note === 'reptile chameleon necklace' && rawRept.askedAt === '2026-10-13T13:43Z' && !('products' in rawRept) && !('after' in rawRept), 'the 11th entry is listing 1726597659 with the key REPTILE 3 (the title of the one-SKU card of Paul\'s image 1, not the master REPTILE 3-CHAMELEON the order reads today: nine plain characters, one space), asked 13 Oct 13:43Z, no product count known, no after');
  ok(JSON.stringify(JSON.parse(fs.readFileSync(QUEUE_FILE, 'utf8')).listings.slice(0, 10).map(l => [l.listingId, l.key])) === JSON.stringify(WANT.slice(0, 10)), 'the ten earlier entries are exactly as they were (listing and key)');
  const raw = JSON.parse(fs.readFileSync(QUEUE_FILE, 'utf8'));
  ok(raw.listings.every(l => Object.keys(l).every(k => ['listingId', 'key', 'products', 'askedAt', 'note', 'after'].includes(k))) && !/@|name|address|email|buyer/i.test(JSON.stringify(raw.listings)), 'the queue file holds listing numbers, SKUs, counts and dates only: no buyer data');
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), build = fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8');
  ok(/<script src="charm-nest-etsy-batch\.js\?v=[^"]+"><\/script>/.test(html) && html.indexOf('charm-nest-etsy-batch.js') > html.indexOf('charm-nest-etsy-link.js') && /charm-nest-etsy-link\.js\?v=[^"]*-bt1"/.test(html), 'the page loads the batch file after the single press\'s file, and the single press\'s cache token carries -bt1 (its hook line changed)');
  ok(/"charm-nest-etsy-batch\.js"/.test(build) && /"charm-nest-etsy-queue\.json"/.test(build), 'both new public files are in the public build manifest (scripts/build-public.cjs)');
  const sm = Batch.summary([{ state: 'done' }, { state: 'done' }, { state: 'already' }, { state: 'stop' }, { state: 'skip' }]);
  ok(sm.text === '5 listings: 2 done, 1 already there, 1 stopped, 1 not tried.' && sm.kind === 'bad' && Batch.summary([{ state: 'done' }]).text === '1 listing: 1 done.' && Batch.summary([{ state: 'already' }, { state: 'already' }]).kind === 'ok', 'the end-of-run line counts done / already there / stopped / not tried: ' + sm.text);
  const norm = Batch.normalize({ listings: [{ listingId: '1234', key: 'A' }, { listingId: '1234', key: 'B' }, { listingId: 'abc', key: 'C' }, { listingId: '5678', key: '' }, null, { listingId: 9876543, key: 'D E', after: '1234' }, { listingId: '5555', key: 'F', after: 'zz' }] });
  ok(JSON.stringify(norm) === JSON.stringify([{ id: '1234', key: 'A', after: '' }, { id: '9876543', key: 'D E', after: '1234' }, { id: '5555', key: 'F', after: '' }]), 'the queue reader keeps a listing once, drops a bad number or an empty SKU, keeps the order: ' + JSON.stringify(norm));
  ok(Batch.normalize(null).length === 0 && Batch.normalize({}).length === 0 && Batch.normalize([{ listingId: '1234', key: 'A' }]).length === 1, 'nothing queued (or a file that is not the queue) gives an empty list: no line is shown');

  const srv = await start({ receipts: [], repoSeed: false });
  const { st, sorterOrigin } = srv;
  const at = Date.UTC(2026, 9, 8, 12);
  const entry = (sku, extra) => st.put('Charm_Master_Index', sku, Object.assign({ sku, masterHash: 'ab'.repeat(32), widthPt: 33.3, heightPt: 25.7, areaPt2: 443, members: 3, holes: 1, upAngle: 73, aiPath: `charmnest/master/${sku}.ai`, aiUrl: srv.blobUrl(`charmnest/master/${sku}.ai`), thumbPath: `charmnest/master/${sku}.png`, indexedAt: Timestamp.fromMillis(at) }, extra));
  entry('BEADY SWAN'); entry('CAT 1 - SITTING CAT', { widthPt: 29.7, heightPt: 33.9, holes: 0, upAngle: 90 });
  // the card of Paul's image 2 as the live library holds it (read-only, 13 Oct 2026): BEADY KITE, KITE and KITE_2181 are three names of ONE drawing file; 9.6 x 12.0 mm, 0 holes, up 150
  const KP = 'charmnest/master/KITE_2181.ai';
  for (const k of ['BEADY KITE', 'KITE', 'KITE_2181']) entry(k, { widthPt: 27.3046, heightPt: 34.153, areaPt2: 394.86, holes: 0, upAngle: 149.863, upSource: 'long', aiPath: KP, aiUrl: srv.blobUrl(KP), thumbPath: 'charmnest/master/KITE_2181.png', charmHash: '0ea228a3' });
  // the card of Paul's image 1 (REPTILE 3: ONE SKU, its own drawing file, the chameleon WITH its bail loop: 11.7 x 11.2 mm, 1 hole, up 87) and the master the order reads today (REPTILE 3-CHAMELEON: the same animal WITHOUT the loop, 9.0 x 7.1 mm, 0 holes), as the live library holds them (read-only GET, 13 Oct 2026)
  entry('REPTILE 3', { widthPt: 33.11227, heightPt: 31.7486, areaPt2: 507.89, holes: 1, upAngle: 86.7243380331011, upSource: 'hole', aiPath: 'charmnest/master/REPTILE_3.ai', aiUrl: srv.blobUrl('charmnest/master/REPTILE_3.ai'), thumbPath: 'charmnest/master/REPTILE_3.png', charmHash: '00377854', members: 3, labelSource: 'text' });
  entry('REPTILE 3-CHAMELEON', { widthPt: 25.413, heightPt: 20.143, areaPt2: 264.47, holes: 0, upAngle: 90, upSource: 'drawn', aiPath: 'charmnest/master/REPTILE_3-CHAMELEON.ai', aiUrl: srv.blobUrl('charmnest/master/REPTILE_3-CHAMELEON.ai'), thumbPath: 'charmnest/master/REPTILE_3-CHAMELEON.png', charmHash: '4438ee09', members: 2, labelSource: 'text' });
  st.put('Charm_Master_Files', 'f1', { masterHash: 'ab'.repeat(32), name: 'LIVING.ai', indexedAt: Timestamp.fromMillis(at), charms: 5, labelled: 5, skus: ['BEADY SWAN', 'CAT 1 - SITTING CAT', 'BEADY KITE', 'KITE', 'KITE_2181', 'REPTILE 3', 'REPTILE 3-CHAMELEON'], indexedBy: 'browser' });
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const aborted = [];   // every request that was not loopback (there must be none that matter)

  async function newContext() {
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 }, timezoneId: 'UTC', locale: 'en-US' });
    ctx.__urls = []; ctx.__bodies = []; ctx.__console = []; ctx.__errors = [];
    ctx.on('request', r => { ctx.__urls.push(r.url()); let b = null; try { b = r.postData(); } catch (_) { b = null; } if (b) ctx.__bodies.push({ url: r.url(), body: b }); });
    await ctx.route(url => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => { aborted.push(r.request().url()); return r.abort(); });   // (first = last in line: the stand-ins below answer before it)
    await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
    await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => r.fulfill({ status: 200, contentType: 'application/octet-stream', body: 'x' }));
    await ctx.route(/\/\.netlify\/functions\/charmNestLibrary$/, async route => {
      const req = route.request(); let b = {}; try { b = req.postDataJSON() || {}; } catch (_) { b = {}; }
      const json = async (code, body) => { try { await route.fulfill({ status: code, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(body) }); } catch (_) { /* the tab was closed */ } };
      if (b.op === 'repoUnlock') {
        W.unlocks.push({ action: b.action, ok: b.password === RIGHT });
        if (b.password === RIGHT) { const token = 'tk-' + crypto.randomBytes(8).toString('hex'); W.held.set(token, { exp: Date.now() + 3600000 }); return json(200, Object.assign({ ok: true, token, expiresAt: Date.now() + 3600000, once: false, action: b.action || 'other' }, b.session ? { session: true } : {})); }
        return json(200, { ok: false, error: 'wrong' });
      }
      if (b.op === 'listingEtsySkuWrite') {
        const t = W.held.get(b.repoToken);
        if (!b.repoToken || !t || Date.now() > t.exp) return json(401, { error: 'repo-locked', need: 'modify', once: false, why: !b.repoToken ? 'missing' : 'invalid' });
        const rec = { id: b.listingId, sku: b.sku, by: b.by, keys: Object.keys(b).sort().join(','), token: !!b.repoToken, n: W.ops.length, out: null };
        W.ops.push(rec); W.inflight++; W.maxInflight = Math.max(W.maxInflight, W.inflight);
        try {
          if (W.delayMs) await sleep(W.delayMs);
          const body = Object.assign({}, b); delete body.repoToken;
          rec.out = await W.mod.listingEtsySkuWrite(body);
        } finally { W.inflight--; }
        return json(200, rec.out);
      }
      return route.fallback();
    });
    await ctx.addInitScript(() => {
      try { localStorage.setItem('cn.employee', 'Tester'); } catch (_) {}
      window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
      if (window.Notification) { try { Object.defineProperty(window, 'Notification', { value: undefined }); } catch (_) {} }
    });
    return ctx;
  }
  async function openPage(ctx) {
    const page = await ctx.newPage();
    page.on('pageerror', e => ctx.__errors.push('pageerror: ' + e.message));
    page.on('console', m => { ctx.__console.push(m.text()); if (m.type() === 'error' && !/Failed to load resource|favicon|net::ERR|__blob|CORS policy/.test(m.text())) ctx.__errors.push('console: ' + m.text().slice(0, 200)); });
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.CN.S && window.Master && window.CharmNestRepoLock && window.CharmNestEtsyLink && window.CharmNestEtsyBatch && CN.S.cloud.ok !== null, null, { timeout: 60000 });
    await page.evaluate(() => { const s = CN.S.settings; s.review = 'off'; s.naming = 'off'; s.notify = 'off'; s.sound = 'off'; s.runMode = 'manual'; });
    await page.evaluate(() => CN.setMode('master'));
    await page.waitForFunction(() => document.querySelectorAll('#mGrid .skuTile').length >= 2, null, { timeout: 30000 });
    return page;
  }
  const tile = (page, sku) => page.locator(`#mGrid .skuTile[data-sku="${sku}"]`);
  const openChip = async (page, sku = 'BEADY SWAN') => { await tile(page, sku).locator('[data-etsylink]').click(); await page.waitForSelector(`#mGrid .skuTile[data-sku="${sku}"] .elBatch:not([hidden])`, { timeout: 10000 }); };
  const state = page => page.evaluate(() => {
    const b = document.querySelector('#mGrid .elBatch'); if (!b) return null;
    const vis = e => !!e && !e.hidden && getComputedStyle(e).display !== 'none';
    return { hidden: b.hidden, say: b.querySelector('.elBtSay').textContent, sayCls: b.querySelector('.elBtSay').className, go: { text: b.querySelector('.elBtGo').textContent, vis: vis(b.querySelector('.elBtGo')) }, yes: { text: b.querySelector('.elBtYes').textContent, vis: vis(b.querySelector('.elBtYes')) }, no: { text: b.querySelector('.elBtNo').textContent, vis: vis(b.querySelector('.elBtNo')) },
      rows: [...b.querySelectorAll('.elBtRow')].map(r => ({ head: r.firstChild.textContent, cls: r.className.replace('elBtRow ', '').replace('elBtRow', ''), st: r.querySelector('.st').textContent, spin: !!r.querySelector('.spin') })), dialogs: document.querySelectorAll('dialog[open]').length,
      panelInert: [...document.querySelector('#mGrid .elPanel').children].filter(c => !c.classList.contains('elBatch')).every(c => c.inert) };
  });
  const finished = page => page.waitForFunction(() => { const b = document.querySelector('#mGrid .elBatch'); return b && !b.querySelector('.elBtGo').hidden && b.querySelectorAll('.elBtRow').length > 0 && !b.querySelector('.elBtRow.work') && !b.querySelector('.elBtRow.wait'); }, null, { timeout: 60000 });
  const typeAndGo = async (page, pw) => { const f = page.locator('dialog.repoDlg[open] #repoPw'); await f.fill(pw); await f.press('Enter'); };
  const press = async (page) => { await page.locator('#mGrid .elBatch .elBtGo').click(); await page.locator('#mGrid .elBatch .elBtYes').click(); };
  const reset = () => { W = world(); };
  const sku = (inv) => [...new Set(inv.products.map(p => unesc(p.sku)))];
  const check = (E, id, key) => {   // every product carries the key and nothing else moved (a disabled product is sent at zero stock, the sku list may be widened to every option property: both are the op's proven rules)
    const S = E.L.get(id), a = S.original, b = S.stored; if (a.products.length !== b.products.length) return false;
    const props = [...new Set(a.products.flatMap(p => p.property_values.map(v => v.property_id)))];
    const okLists = JSON.stringify(a.price_on_property) === JSON.stringify(b.price_on_property) && JSON.stringify(a.quantity_on_property) === JSON.stringify(b.quantity_on_property)
      && (JSON.stringify(a.sku_on_property) === JSON.stringify(b.sku_on_property) || (a.sku_on_property.length > 0 && a.sku_on_property.length < props.length && b.sku_on_property.length === props.length));
    return okLists && b.products.every((p, i) => { const o = a.products[i], f = p.offerings[0], g = o.offerings[0];
      return p.sku === key && JSON.stringify(p.property_values) === JSON.stringify(o.property_values) && f.price === g.price && f.is_enabled === g.is_enabled && f.readiness_state_id === g.readiness_state_id && f.quantity === (g.is_enabled ? g.quantity : 0); });
  };
  const word = (key, id, cnt) => `Done: ${key} is now on all ${cnt} products of listing ${id}.`;

  /* ═══ 1 · looking, the line, the confirm ═══ */
  reset();
  let ctx = await newContext(), page = await openPage(ctx);
  await openChip(page);
  let s = await state(page);
  ok(s && !s.hidden && s.go.text === `Put all ${NN} queued SKUs on Etsy` && s.go.vis && !s.yes.vis && !s.no.vis && s.rows.length === 0 && s.say === '', 'the Etsy panel has ONE extra line, a button "Put all ' + NN + ' queued SKUs on Etsy", and nothing else: ' + JSON.stringify(s.go));
  const names = await page.evaluate(() => [...document.querySelector('#mGrid .elPanel').children].map(c => c.className.split(' ')[0]).join(','));
  ok(names === 'elHead,elPicks,elIn,elSay,elActs,elBatch', 'it sits last in the panel, inside the card (no pop-up): ' + names);
  ok(W.ops.length === 0 && W.E.calls.length === 0 && W.unlocks.length === 0 && st.calls.every(c => c.op !== 'listingEtsySkuWrite'), 'looking and opening the panel sent nothing to the write, asked for no password and made no Etsy request');
  await page.locator('#mGrid .elBatch .elBtGo').click();
  s = await state(page);
  const confirmLine = `Every product of these ${NN} Etsy listings will carry the SKU beside it, one listing after another. Prices, stock and options stay as they are; each listing as it is now is kept first, and one already done is skipped.`;
  ok(s.say === confirmLine && s.rows.length === NN && s.rows.every((r, i) => r.head === WANT[i][0] + '  ' + WANT[i][1] && r.st === ''), 'pressing it shows ONE plain confirm line and the ' + NN + ' listings with the SKU each will carry, in the order of the run: ' + s.rows.map(r => r.head).join(' | '));
  ok(s.yes.vis && s.yes.text === `Put all ${NN} SKUs on Etsy` && s.no.vis && s.no.text === 'Cancel' && !s.go.vis && s.dialogs === 0, 'with the one button that goes, and Cancel; no password dialog yet');
  ok(W.ops.length === 0 && W.unlocks.length === 0 && W.E.calls.length === 0, 'the confirm sent nothing');
  if (SHOTS) { await page.waitForTimeout(300); await tile(page, 'BEADY SWAN').screenshot({ path: path.join(SHOTS, 'etsybatch-1-confirm.png') }); }
  await page.locator('#mGrid .elBatch .elBtNo').click();
  s = await state(page);
  ok(s.go.vis && !s.yes.vis && s.rows.length === 0 && W.ops.length === 0, 'Cancel puts the line back, nothing sent');

  /* ═══ 2 · a wrong password writes nothing; cancelling the dialog sends nothing ═══ */
  await press(page);
  await page.waitForSelector('dialog.repoDlg[open]', { timeout: 5000 });
  const d1 = await page.evaluate(() => { const x = document.querySelector('dialog.repoDlg[open]'); return { action: x.dataset.repoAction, count: document.querySelectorAll('dialog[open]').length, text: x.textContent.replace(/\s+/g, ' '), go: x.querySelector('[data-k=go]').textContent }; });
  ok(d1.action === 'modify' && d1.count === 1 && d1.text.includes(`Putting the SKUs of ${NN} queued Etsy listings on Etsy, one after another.`) && d1.go === 'Put SKUs on Etsy', 'locked: the repo password dialog opens ONCE (action modify), the only pop-up, and says what it is for: ' + d1.text.slice(0, 120));
  s = await state(page);
  ok(s.rows.length === NN && s.rows.every(r => r.st === 'Waiting') && /Asking for the repo password once/.test(s.say) && s.panelInert, 'behind it every listing waits; the single-press controls are frozen while the run lives');
  await typeAndGo(page, WRONG);
  await page.waitForFunction(() => /not right/.test((document.querySelector('dialog.repoDlg[open] #repoMsg') || {}).textContent || ''), null, { timeout: 5000 });
  ok(W.ops.length === 0 && W.E.calls.length === 0 && W.unlocks.length === 1 && W.unlocks[0].ok === false, 'a WRONG password: the dialog says so, no listing was sent, no Etsy request was made');
  await page.click('dialog.repoDlg[open] [data-k=cancel]'); await page.waitForFunction(() => !document.querySelector('dialog.repoDlg'), null, { timeout: 5000 });
  await finished(page);
  s = await state(page);
  ok(s.say === 'Nothing was sent: the repo stayed locked.' && s.sayCls.includes('bad') && s.rows.every(r => r.st === 'Not tried.' && r.cls === 'skip') && s.go.vis && !s.panelInert, 'cancelling the dialog stops the run: "' + s.say + '"; every listing says Not tried');
  ok(W.ops.length === 0 && W.E.calls.length === 0 && ![...store.keys()].some(k => k.startsWith('Charm_Custom_Saves/')) && writes.length === 0 && fileWrites.length === 0, 'and nothing was written anywhere (no record, no safety copy, no Etsy request)');
  ok(await page.evaluate(() => { const c = document.querySelector('#mGrid .skuTile[data-sku="BEADY SWAN"] [data-etsylink]'); c.click(); return !document.querySelector('.elPanel'); }), 'the run is over, so the panel can be folded again');

  /* ═══ 3 · the right password: ONE entry, every listing in turn ═══ */
  reset(); W.delayMs = 350;
  await openChip(page);
  await press(page);
  await page.waitForSelector('dialog.repoDlg[open]'); await typeAndGo(page, RIGHT);
  await page.waitForFunction(() => document.querySelectorAll('#mGrid .elBatch .elBtRow.work .spin').length === 1 && document.querySelectorAll('#mGrid .elBatch .elBtRow.done').length >= 2 && !document.querySelector('dialog[open]'), null, { timeout: 20000 });
  s = await state(page);
  const wi = s.rows.findIndex(r => r.cls === 'work');
  ok(s.rows.filter(r => r.cls === 'work').length === 1 && s.rows.slice(0, wi).every(r => r.cls === 'done') && s.rows.slice(wi + 1).every(r => r.cls === 'wait' && r.st === 'Waiting'), 'while it runs ONE listing is in progress (row ' + (wi + 1) + ' of ' + NN + '), those before it are done, those after it wait');
  ok(s.rows[wi].spin && s.rows[wi].st === 'Putting the SKU on Etsy…' && /Putting the SKUs on Etsy, one listing at a time/.test(s.say) && s.panelInert && !s.go.vis && !s.yes.vis, 'it has a small spinner and the same label as the single press: "' + s.rows[wi].st + '"; no button can start a second run');
  ok(await page.evaluate(() => { const c = document.querySelector('#mGrid .skuTile[data-sku="BEADY SWAN"] [data-etsylink]'); c.click(); return !!document.querySelector('#mGrid .elPanel'); }), 'the chip cannot fold the panel while the run is going (nothing is hidden from Paul)');
  if (SHOTS) await tile(page, 'BEADY SWAN').screenshot({ path: path.join(SHOTS, 'etsybatch-2-running.png') });
  await finished(page);
  W.delayMs = 0;
  s = await state(page);
  if (SHOTS) { await page.waitForTimeout(300); await tile(page, 'BEADY SWAN').screenshot({ path: path.join(SHOTS, 'etsybatch-3-done.png') }); }
  ok(s.rows.length === NN && s.rows.every((r, i) => r.head === WANT[i][0] + '  ' + WANT[i][1]), 'the result list is the confirm list, one row per listing, in the same order');
  ok(s.rows.every(r => r.cls === 'done') && s.say === `${NN} listings: ${NN} done.` && s.sayCls.includes('ok'), 'every listing is done: "' + s.say + '"');
  const sayings = s.rows.map(r => r.st);
  const REPLACED = new Set(['4494184284', '4494182906', '1745415868', '1734130419', KITE, REPT]);   // (listings whose products carried two or more different SKUs: the answer says what was replaced)
  ok(WANT.every(([id, key], i) => (REPLACED.has(id) ? sayings[i].startsWith(word(key, id, COUNTS[id])) && /It replaced \d+ x /.test(sayings[i]) : sayings[i] === word(key, id, COUNTS[id]))), 'each row uses the single press\'s own words, a single-SKU listing exactly as before and a two-SKU listing with what it replaced: ' + sayings[0] + ' / ' + sayings[7]);
  ok(W.ops.length === NN && W.ops.map(o => o.id).join() === WANT.map(w => w[0]).join() && W.ops.every((o, i) => o.sku === WANT[i][1]), 'ONE request per listing, in the queue\'s order: ' + W.ops.map(o => o.id).join(','));
  ok(W.ops.every(o => o.keys === 'by,listingId,op,repoToken,sku' && o.by === 'Tester' && o.token) && W.maxInflight === 1, 'each request is exactly {op, listingId, sku, by, repoToken} and they never overlap (at most ' + W.maxInflight + ' at a time)');
  ok(W.unlocks.length === 1 && W.unlocks[0].ok, 'the password was entered ONCE: one repoUnlock call for the whole run');
  ok(WANT.every(([id, key]) => check(W.E, id, key)), 'on the fake Etsy every product of all ' + NN + ' listings now carries its SKU, and EVERYTHING else (prices, stock, enabled state, options, readiness) is as it was, 816-product bar included');
  ok(W.E.puts.length === NN && new Set(W.E.puts.map(p => p.id)).size === NN && W.E.puts.every(p => p.safetyCopyFirst), 'each listing got exactly ONE PUT, and its safety copy was already in Storage when the PUT left');
  ok(WANT.every(([id]) => [...blobs.keys()].filter(k => k.startsWith('charmnest/custom-saves/' + id + '/')).length === 1), 'one safety copy per listing, none overwritten');
  const per = {}; for (const c of W.E.calls) per[c.id] = (per[c.id] || 0) + 1;
  ok(Object.values(per).every(k => k <= 3) && W.E.calls.length === 4 * 2 + (NN - 4) * 3 && W.E.calls.length <= BUDGET, 'Etsy requests: at most 3 per listing and ' + W.E.calls.length + ' in all (budget: about ' + BUDGET + ' for ' + NN + ' listings, no polling, no retries): ' + JSON.stringify(per));
  ok(WANT.every(([id, key]) => (store.get('Charm_Custom_Saves/link-' + id) || {}).link && store.get('Charm_Custom_Saves/link-' + id).link.state === 'linked' && store.get('Charm_Custom_Saves/link-' + id).sku === key), 'every listing has its record (Charm_Custom_Saves/link-<id>) saying linked');
  ok(!/name|email|address|buyer/i.test(JSON.stringify([...store.entries()].filter(([k]) => k.startsWith('Charm_Custom_Saves/')))), 'no buyer data on any record');
  ok(await page.evaluate(n => document.getElementById('toasts').textContent.includes(n + ' listings: ' + n + ' done.'), NN), 'one toast says the result');
  ok(s.dialogs === 0 && aborted.length === 0, 'no further password dialog and no outside request during the run');
  // the password is nowhere
  const everywhere = JSON.stringify(ctx.__urls) + JSON.stringify(ctx.__console);
  ok(!everywhere.includes(RIGHT) && !everywhere.includes(WRONG) && ctx.__bodies.filter(b => b.body.includes(RIGHT)).length === 1 && ctx.__bodies.filter(b => b.body.includes(RIGHT))[0].body.includes('repoUnlock'), 'the password is in no URL and no console line, and in exactly ONE request body (the lock\'s own repoUnlock)');
  ok(!(await page.evaluate(([a, b]) => { const dump = JSON.stringify(Object.assign({}, localStorage)) + JSON.stringify(Object.assign({}, sessionStorage)) + document.documentElement.outerHTML; return dump.includes(a) || dump.includes(b); }, [RIGHT, WRONG])), 'and in no stored value or page text');
  ok(!JSON.stringify([...store.entries()]).includes(RIGHT) && !JSON.stringify(W.ops).includes(RIGHT), 'nor in any record the server wrote');

  /* ═══ 4 · a second press: "Already there" for everything, no PUT, no Etsy request, no password ═══ */
  const calls0 = W.E.calls.length, puts0 = W.E.puts.length, w0 = writes.length, f0 = fileWrites.length;
  await press(page);
  await finished(page); s = await state(page);
  ok(s.say === `${NN} listings: ${NN} already there.` && s.rows.every((r, i) => r.cls === 'already' && r.st === `Already there: all ${COUNTS[WANT[i][0]]} products of listing ${WANT[i][0]} carry ${WANT[i][1]}. Nothing was changed.`), 'pressing again: every listing says "Already there", with the same words as the single press: "' + s.rows[0].st + '"');
  ok(W.E.calls.length === calls0 && W.E.puts.length === puts0 && W.ops.length === 2 * NN && fileWrites.length === f0 && s.dialogs === 0 && W.unlocks.length === 1, 'and it made no Etsy request, no PUT, wrote no safety copy and asked for no password');
  ok(ctx.__errors.length === 0, 'no page error: ' + ctx.__errors.join(' | '));
  await ctx.close();

  /* ═══ 5 · a failure in the middle is reported, the rest still run, and pressing again finishes it ═══ */
  reset();
  W.E.fail = c => (c.id === '4494182906' && c.kind === 'inventory' && c.method === 'GET' && !W.healed ? etsyErr(500, 'Internal') : null);   // Etsy does not answer for the wolf (the 6th listing), once
  ctx = await newContext(); page = await openPage(ctx); await openChip(page); await press(page);
  await page.waitForSelector('dialog.repoDlg[open]'); await typeAndGo(page, RIGHT);
  await finished(page); s = await state(page);
  ok(s.rows.map(r => r.cls).join() === WANT.map((w, i) => (i === 5 ? 'stop' : 'done')).join() && s.say === `${NN} listings: ${NN - 1} done, 1 stopped.` && s.sayCls.includes('bad'), 'the wolf stopped, the other ' + (NN - 1) + ' were done: ' + s.say);
  ok(s.rows[5].st === 'Etsy did not answer.' && W.E.puts.length === NN - 1 && !W.E.puts.some(p => p.id === '4494182906'), 'its row says why in plain words ("' + s.rows[5].st + '"); nothing was written to it');
  ok(W.ops.length === NN && W.ops.map(o => o.id).join() === WANT.map(w => w[0]).join(), 'the listings after it were still tried, in order');
  ok(W.E.calls.filter(c => c.id === '4494182906').length === 2 && W.E.calls.length <= BUDGET && !W.E.calls.some(c => c.id === '4494182906' && c.method === 'PUT'), 'no blind retry: the wolf cost 2 Etsy requests, no PUT; the run cost ' + W.E.calls.length + ' in all');
  W.healed = true; const callsBefore = W.E.calls.length;
  await press(page); await finished(page); s = await state(page);
  ok(s.rows.map(r => r.cls).join() === WANT.map((w, i) => (i === 5 ? 'done' : 'already')).join() && s.say === `${NN} listings: 1 done, ${NN - 1} already there.`, 'pressing again finishes the wolf; the other ' + (NN - 1) + ' are "Already there": ' + s.say);
  ok(W.E.calls.length - callsBefore <= 3 && W.E.puts.length === NN && new Set(W.E.puts.map(p => p.id)).size === NN, 'and that second press cost ' + (W.E.calls.length - callsBefore) + ' Etsy requests (only the wolf); every listing has exactly one PUT in all');
  ok(WANT.every(([id, key]) => check(W.E, id, key)), 'all ' + NN + ' listings carry their SKU in the end');
  await ctx.close();

  /* ═══ 6 · "needs listings_w" stops the whole run ═══ */
  reset();
  W.E.fail = c => (c.id === '1801964156' && c.kind === 'inventory' ? etsyErr(403, 'missing scope listings_w') : null);   // Etsy refuses at the cat (the 2nd listing)
  ctx = await newContext(); page = await openPage(ctx); await openChip(page); await press(page);
  await page.waitForSelector('dialog.repoDlg[open]'); await typeAndGo(page, RIGHT);
  await finished(page); s = await state(page);
  ok(s.rows.map(r => r.cls).join() === 'done,stop,' + Array(NN - 2).fill('skip').join() && s.say === `The run stopped here: the listings after it would fail the same way, so they were not tried. ${NN} listings: 1 done, 1 stopped, ${NN - 2} not tried.`, 'the swan was done; the cat was refused; the other ' + (NN - 2) + ' were NOT tried: "' + s.say + '"');
  ok(s.rows[1].st === 'Etsy refused the change. Paul needs to re-authorise Etsy with listings_w.' && s.rows.slice(2).every(r => r.st === 'Not tried.'), 'the plain line is the single press\'s own: "' + s.rows[1].st + '"');
  ok(W.ops.length === 2 && W.E.puts.length === 1 && W.E.calls.filter(c => c.id !== '1700763608' && c.id !== '1801964156').length === 0 && W.E.calls.length <= 4, 'only two requests left the page and only ' + W.E.calls.length + ' Etsy requests were made: none for the listings after the refusal');
  await ctx.close();
  // the same on a rate limit (Etsy busy)
  reset(); W.E.fail = c => (c.id === '1816178147' && c.kind === 'inventory' ? etsyErr(429, 'rate limit') : null);
  ctx = await newContext(); page = await openPage(ctx); await openChip(page); await press(page);
  await page.waitForSelector('dialog.repoDlg[open]'); await typeAndGo(page, RIGHT);
  await finished(page); s = await state(page);
  ok(s.rows.map(r => r.cls).join() === 'done,done,stop,' + Array(NN - 3).fill('skip').join() && s.rows[2].st === 'Etsy is busy, try again in a minute.' && W.ops.length === 3, 'Etsy busy (429) also stops the whole run at once: ' + s.rows.map(r => r.cls).join());
  await ctx.close();

  /* ═══ 7 · the chameleon waits for the swan ═══ */
  reset();
  W.E.fail = c => (c.id === '1700763608' && c.kind === 'inventory' ? etsyErr(500, 'Internal') : null);   // the swan does not go through
  ctx = await newContext(); page = await openPage(ctx); await openChip(page); await press(page);
  await page.waitForSelector('dialog.repoDlg[open]'); await typeAndGo(page, RIGHT);
  await finished(page); s = await state(page);
  ok(s.rows.map(r => r.cls).join() === WANT.map((w, i) => (i === 0 ? 'stop' : i === 4 ? 'skip' : 'done')).join() && s.rows[4].st === 'Not tried: listing 1700763608 has to be done first.', 'the swan stopped, so the chameleon was not tried ("' + s.rows[4].st + '"); the other ' + (NN - 2) + ' went through, the kite necklace and the reptile chameleon necklace too');
  ok(!W.E.calls.some(c => c.id === '4494184284') && W.ops.length === NN - 1, 'it made no request for the chameleon, no Etsy request either');
  await ctx.close();

  /* ═══ 8 · a closed tab loses nothing; pressing again resumes ═══ */
  reset(); W.delayMs = 250;
  ctx = await newContext(); page = await openPage(ctx); await openChip(page); await press(page);
  await page.waitForSelector('dialog.repoDlg[open]'); await typeAndGo(page, RIGHT);
  await page.waitForFunction(() => document.querySelectorAll('#mGrid .elBatch .elBtRow.done').length >= 4, null, { timeout: 30000 });
  await page.close();   // the tab is closed in the middle of the run
  for (let i = 0; i < 100 && W.inflight; i++) await sleep(50);   // (the server finishes the request it already has, as a Netlify function does)
  await sleep(300);
  const doneBefore = W.E.puts.length, opsBefore = W.ops.length;
  ok(doneBefore >= 4 && doneBefore < NN && opsBefore === doneBefore && W.inflight === 0, 'the tab was closed after ' + doneBefore + ' of ' + NN + ' listings: those are complete, the rest are untouched, nothing is half-done');
  ok(WANT.slice(doneBefore).every(([id, key]) => W.E.L.get(id).stored.products.some(p => p.sku !== key) && !store.has('Charm_Custom_Saves/link-' + id)), 'the listings not reached have no record, no safety copy and their old SKUs');
  W.delayMs = 0;
  page = await openPage(ctx); await openChip(page);
  ok((await page.evaluate(() => !!window.CharmNestRepoLock.find('modify'))), 'a new tab of this computer still holds the repo unlock (the lock\'s own session), so it will not ask again');
  await press(page);
  await finished(page); s = await state(page);
  ok(s.dialogs === 0 && W.unlocks.length === 1, 'the resume asked for no password');
  ok(s.rows.slice(0, doneBefore).every(r => r.cls === 'already') && s.rows.slice(doneBefore).every(r => r.cls === 'done') && s.say === `${NN} listings: ${NN - doneBefore} done, ${doneBefore} already there.`, 'pressing again resumed: the first ' + doneBefore + ' say "Already there", the other ' + (NN - doneBefore) + ' are done: ' + s.say);
  ok(W.E.puts.length === NN && new Set(W.E.puts.map(p => p.id)).size === NN && W.E.puts.every(p => p.safetyCopyFirst) && WANT.every(([id, key]) => check(W.E, id, key)), 'in all: ' + NN + ' PUTs, one per listing, never two, each after its safety copy; all ' + NN + ' carry their SKU');
  ok(W.E.calls.length <= BUDGET, 'Etsy requests over both presses: ' + W.E.calls.length + ' (at most ' + BUDGET + ')');
  ok(ctx.__errors.length === 0, 'no page error: ' + ctx.__errors.join(' | '));
  await ctx.close();

  /* ═══ 9 · an expired unlock mid-run: the run stops, it never pops a second password dialog ═══ */
  reset(); W.delayMs = 150;
  ctx = await newContext(); page = await openPage(ctx); await openChip(page); await press(page);
  await page.waitForSelector('dialog.repoDlg[open]'); await typeAndGo(page, RIGHT);
  await page.waitForFunction(() => document.querySelectorAll('#mGrid .elBatch .elBtRow.done').length >= 2, null, { timeout: 30000 });
  await page.evaluate(() => { CharmNestRepoLock.forget(); });   // (the unlock ends: Lock now on another tab)
  await finished(page); s = await state(page);
  const nDone = s.rows.filter(r => r.cls === 'done').length;
  ok(nDone >= 2 && nDone < NN && s.rows.slice(nDone).every(r => r.cls === 'skip' && r.st === 'Not tried.') && /^The repo unlock ended, so nothing more was sent\. Press again to unlock and carry on: what is done is skipped\./.test(s.say) && s.dialogs === 0, 'the unlock ended after ' + nDone + ' listings: the run stopped without a surprise dialog: "' + s.say.slice(0, 90) + '"');
  W.delayMs = 0;
  await press(page);
  await page.waitForSelector('dialog.repoDlg[open]'); await typeAndGo(page, RIGHT);
  await finished(page); s = await state(page);
  ok(s.say === `${NN} listings: ${NN - nDone} done, ${nDone} already there.` && W.E.puts.length === NN && W.unlocks.length === 2, 'pressing again asks for the password (once) and carries on: ' + s.say);
  await ctx.close();

  /* ═══ 10 · the single press still works beside it ═══ */
  reset();
  ctx = await newContext(); page = await openPage(ctx); await openChip(page);
  await tile(page, 'BEADY SWAN').locator('.elPanel input.elIn').fill('1700763608');
  await tile(page, 'BEADY SWAN').locator('.elPanel .btn.gold').click();
  await page.waitForSelector('dialog.repoDlg[open]'); await typeAndGo(page, RIGHT);
  await page.waitForFunction(() => /^Done:/.test(document.querySelector('#mGrid .elPanel .elSay').textContent), null, { timeout: 10000 });
  ok((await tile(page, 'BEADY SWAN').locator('.elPanel .elSay').textContent()) === word('BEADY SWAN', '1700763608', 9) && W.ops.length === 1 && W.E.puts.length === 1, 'the single press is unchanged: ' + word('BEADY SWAN', '1700763608', 9));
  await tile(page, 'BEADY SWAN').locator('.elBatch .elBtGo').click(); await tile(page, 'BEADY SWAN').locator('.elBatch .elBtYes').click();
  await finished(page); s = await state(page);
  ok(s.rows[0].cls === 'already' && s.rows.slice(1).every(r => r.cls === 'done') && W.E.puts.length === NN, 'a listing done by the single press is "Already there" in the batch, the rest are done');
  await ctx.close();

  /* ═══ 11 · the kite and the reptile chameleon necklaces: Paul's earlier press already did the first nine (04:06 UTC); the two newest are new, and the BEADY KITE card's own chip carries the line ═══ */
  reset();
  const OLD = WANT.slice(0, 9), FRESH = WANT.slice(9), FRESH_IDS = FRESH.map(w => w[0]);
  for (const [id, key] of OLD) { const r0 = await W.mod.listingEtsySkuWrite({ listingId: id, sku: key, by: 'Paul' }); assert(r0.ok && !r0.already, 'the earlier press of ' + id + ' ran for real on the fake: ' + JSON.stringify(r0)); }   // (the earlier press, done on the fake Etsy through the REAL op: each has its record)
  const calls11 = W.E.calls.length, puts11 = W.E.puts.length;
  ok(puts11 === 9 && !W.E.puts.some(p => FRESH_IDS.includes(p.id)) && FRESH.every(([id, key]) => W.E.L.get(id).stored.products.every(p => p.sku !== key)), 'before the press: the first 9 listings carry their SKU (as after Paul\'s earlier press) and the kite and reptile chameleon necklaces do not');
  ctx = await newContext(); page = await openPage(ctx);
  await openChip(page, 'BEADY KITE');
  const kcard = await page.evaluate(() => { const t = document.querySelector('#mGrid .skuTile[data-sku="BEADY KITE"]'), sel = t.querySelector('.elPanel select.elSel'); return { title: t.querySelector('.sku').textContent, others: [...t.querySelectorAll('.meta')][0].innerText.replace(/\s+/g, ' ').trim(), size: [...t.querySelectorAll('.meta')][1].innerText.replace(/\s+/g, ' ').trim(), opts: [...sel.options].map(o => o.textContent), picked: sel.value }; });
  ok(kcard.title === 'BEADY KITE' && kcard.others === 'KITE KITE_2181' && /9\.6 × 12\.0 mm · 0 holes · 3 SKUs/.test(kcard.size), 'the library card of the kite image: title BEADY KITE, lines KITE and KITE_2181, "9.6 × 12.0 mm · 0 holes · 3 SKUs"');
  ok(kcard.opts.join('|') === 'BEADY KITE|KITE|KITE_2181' && kcard.picked === 'BEADY KITE', 'its own Etsy chip offers the card\'s three SKUs and starts on BEADY KITE, the card title');
  s = await state(page);
  ok(s.go.text === `Put all ${NN} queued SKUs on Etsy` && s.go.vis, 'the one-press line on the BEADY KITE card says the new count: "' + s.go.text + '"');
  await press(page);
  await page.waitForSelector('dialog.repoDlg[open]'); await typeAndGo(page, RIGHT);
  await finished(page); s = await state(page);
  ok(s.say === `${NN} listings: ${FRESH.length} done, 9 already there.` && s.rows.slice(0, 9).every((r, i) => r.cls === 'already' && r.st === `Already there: all ${COUNTS[WANT[i][0]]} products of listing ${WANT[i][0]} carry ${WANT[i][1]}. Nothing was changed.`) && s.rows.slice(9).every(r => r.cls === 'done'), 'the press: the 9 earlier listings say "Already there", the kite and the reptile chameleon necklaces are done: ' + s.say);
  ok(W.E.calls.length - calls11 === 6 && W.E.calls.slice(calls11).every(c => FRESH_IDS.includes(c.id)) && W.E.puts.length === puts11 + 2 && W.E.puts.slice(puts11).map(p => p.id).join() === FRESH_IDS.join() && W.E.puts.slice(puts11).every(p => p.safetyCopyFirst), 'the press made 6 Etsy requests, 3 for each new listing (state, inventory, ONE PUT after its safety copy); the 9 earlier made none');
  ok(W.ops.length === NN && W.ops.slice(0, 9).every(o => o.out && o.out.ok && o.out.calls === 0) && W.ops[9].id === KITE && W.ops[9].sku === 'BEADY KITE' && W.ops[10].id === REPT && W.ops[10].sku === 'REPTILE 3', 'the server answered the 9 earlier ones from their records (0 Etsy calls each) and ran the kite and the reptile chameleon necklaces');
  ok(check(W.E, KITE, 'BEADY KITE') && W.E.L.get(KITE).stored.products.length === 20 && /^Done: BEADY KITE is now on all 20 products of listing 1759609265\. It replaced 8 x \(no SKU\), 8 x KITE and 4 x KITE-CO; the listing as it was is kept\.$/.test(s.rows[9].st), 'every kite product (Metal Choice x Necklace Length; KITE / blank / KITE-CO before) now carries BEADY KITE, only sku changed; words: ' + s.rows[9].st);
  ok(check(W.E, REPT, 'REPTILE 3') && W.E.L.get(REPT).stored.products.length === 15 && /^Done: REPTILE 3 is now on all 15 products of listing 1726597659\. It replaced 6 x \(no SKU\), 6 x REPTILE 3-CHAMELEON and 3 x REPTILE 3-CHAMELEON-CO; the listing as it was is kept\.$/.test(s.rows[10].st), 'every reptile chameleon necklace product (REPTILE 3-CHAMELEON / blank / REPTILE 3-CHAMELEON-CO before) now carries REPTILE 3, only sku changed; words: ' + s.rows[10].st);
  ok(OLD.every(([id, key]) => check(W.E, id, key)), 'the earlier 9 listings were not touched again');
  const calls11b = W.E.calls.length, puts11b = W.E.puts.length;
  await press(page); await finished(page); s = await state(page);
  ok(s.say === `${NN} listings: ${NN} already there.` && s.rows[9].st === 'Already there: all 20 products of listing 1759609265 carry BEADY KITE. Nothing was changed.' && s.rows[10].st === 'Already there: all 15 products of listing 1726597659 carry REPTILE 3. Nothing was changed.' && W.E.calls.length === calls11b && W.E.puts.length === puts11b && s.dialogs === 0, 'a second press: "' + s.say + '", the kite and the reptile chameleon necklaces say "Already there", no Etsy request, no PUT, no password');
  ok(ctx.__errors.length === 0 && aborted.length === 0, 'no page error, no outside request: ' + ctx.__errors.concat(aborted).join(' | '));
  await ctx.close();

  /* ═══ 12 · the reptile chameleon necklace (REPTSKU, Paul 13 Oct 13:43 UTC): the other ten are already done (04:06 and the kite's press, 13:22 UTC); only REPTILE 3 is new, and the REPTILE 3 card's own chip carries the line ═══ */
  reset();
  for (const [id, key] of WANT.slice(0, NN - 1)) { const r0 = await W.mod.listingEtsySkuWrite({ listingId: id, sku: key, by: 'Paul' }); assert(r0.ok && !r0.already, 'the earlier press of ' + id + ' ran for real on the fake: ' + JSON.stringify(r0)); }
  const calls12 = W.E.calls.length, puts12 = W.E.puts.length;
  ok(puts12 === NN - 1 && !W.E.puts.some(p => p.id === REPT) && W.E.L.get(REPT).stored.products.every(p => p.sku !== 'REPTILE 3') && W.E.L.get(REPT).stored.products.some(p => p.sku === 'REPTILE 3-CHAMELEON'), 'before the press: the first ' + (NN - 1) + ' listings carry their SKU and the reptile chameleon necklace does not (it carries REPTILE 3-CHAMELEON, blanks and a -CO variant)');
  ctx = await newContext(); page = await openPage(ctx);
  await page.locator('#mSearch').fill('REPTILE 3');
  await page.waitForFunction(() => document.querySelectorAll('#mGrid .skuTile').length === 2, null, { timeout: 10000 });
  const twins = await page.evaluate(() => [...document.querySelectorAll('#mGrid .skuTile')].map(t => ({ sku: t.dataset.sku, title: t.querySelector('.sku').textContent, lines: [...t.querySelectorAll('.meta')].map(m => m.innerText.replace(/\s+/g, ' ').trim()) })));
  ok(twins.map(t => t.title).join('|') === 'REPTILE 3|REPTILE 3-CHAMELEON', 'searching REPTILE 3 shows two separate cards, REPTILE 3 and REPTILE 3-CHAMELEON (two masters, two drawings): ' + twins.map(t => t.title).join(' | '));
  const rcard = twins[0], tcard = twins[1];
  ok(rcard.lines.length === 2 && /^11\.7 × 11\.2 mm · 1 hole$/.test(rcard.lines[0]) && /^up 87° · text$/.test(rcard.lines[1]), 'the REPTILE 3 card reads exactly as Paul\'s image 1: title REPTILE 3, no other SKU line, "11.7 × 11.2 mm · 1 hole", "up 87° · text": ' + JSON.stringify(rcard.lines));
  ok(tcard.lines.length === 2 && /^9\.0 × 7\.1 mm · 0 holes$/.test(tcard.lines[0]), 'the REPTILE 3-CHAMELEON card is the other drawing (no loop): ' + JSON.stringify(tcard.lines));
  await tile(page, 'REPTILE 3').locator('[data-etsylink]').click(); await page.waitForSelector('#mGrid .skuTile[data-sku="REPTILE 3"] .elBatch:not([hidden])', { timeout: 10000 });
  const rpanel = await page.evaluate(() => { const t = document.querySelector('#mGrid .skuTile[data-sku="REPTILE 3"]'); return { sel: !!t.querySelector('.elPanel select.elSel'), input: !!t.querySelector('.elPanel input.elIn'), head: t.querySelector('.elPanel .elHead').textContent }; });
  ok(!rpanel.sel && rpanel.input && rpanel.head === 'Put the SKU on an Etsy listing', 'its own Etsy chip has no SKU box (the card has one SKU, REPTILE 3, so there is nothing to choose) and the same panel as every card');
  s = await state(page);
  ok(s.go.text === `Put all ${NN} queued SKUs on Etsy` && s.go.vis, 'the one-press line on the REPTILE 3 card says the count: "' + s.go.text + '"');
  await press(page);
  await page.waitForSelector('dialog.repoDlg[open]'); await typeAndGo(page, RIGHT);
  await finished(page); s = await state(page);
  ok(s.say === `${NN} listings: 1 done, ${NN - 1} already there.` && s.rows.slice(0, NN - 1).every((r, i) => r.cls === 'already' && r.st === `Already there: all ${COUNTS[WANT[i][0]]} products of listing ${WANT[i][0]} carry ${WANT[i][1]}. Nothing was changed.`) && s.rows[NN - 1].cls === 'done', 'the press: the ' + (NN - 1) + ' earlier listings say "Already there", the reptile chameleon necklace is done: ' + s.say);
  ok(W.E.calls.length - calls12 === 3 && W.E.calls.slice(calls12).every(c => c.id === REPT) && W.E.puts.length === puts12 + 1 && W.E.puts[puts12].id === REPT && W.E.puts[puts12].safetyCopyFirst, 'the press made 3 Etsy requests, every one for the reptile chameleon necklace (state, inventory, ONE PUT after its safety copy); the earlier ' + (NN - 1) + ' made none');
  ok(W.ops.length === NN && W.ops.slice(0, NN - 1).every(o => o.out && o.out.ok && o.out.calls === 0) && W.ops[NN - 1].id === REPT && W.ops[NN - 1].sku === 'REPTILE 3', 'the server answered the ' + (NN - 1) + ' earlier ones from their records (0 Etsy calls each) and ran REPTILE 3');
  ok(check(W.E, REPT, 'REPTILE 3') && W.E.L.get(REPT).stored.products.length === 15 && new Set(W.E.L.get(REPT).stored.products.map(p => p.sku)).size === 1, 'all 15 products carry exactly REPTILE 3 (the REPTILE 3-CHAMELEON, blank and -CO ones replaced), and nothing but sku moved: prices, stock, enabled state, options and readiness are as they were; words: ' + s.rows[NN - 1].st);
  ok(WANT.slice(0, NN - 1).every(([id, key]) => check(W.E, id, key)), 'the earlier ' + (NN - 1) + ' listings were not touched again');
  const rbefore = JSON.parse(blobs.get([...blobs.keys()].find(k => k.startsWith('charmnest/custom-saves/' + REPT + '/') && k.endsWith('-before.json'))).buf.toString());
  ok(rbefore && rbefore.products && new Set(rbefore.products.map(p => unesc(p.sku))).size === 3 && rbefore.products.some(p => unesc(p.sku) === 'REPTILE 3-CHAMELEON') && rbefore.products.some(p => unesc(p.sku) === '') && rbefore.products.some(p => unesc(p.sku) === 'REPTILE 3-CHAMELEON-CO'), 'the safety copy kept the listing as it was: its three old SKUs (REPTILE 3-CHAMELEON, blank, REPTILE 3-CHAMELEON-CO) can be put back');
  const calls12b = W.E.calls.length, puts12b = W.E.puts.length;
  await press(page); await finished(page); s = await state(page);
  ok(s.say === `${NN} listings: ${NN} already there.` && s.rows[NN - 1].st === 'Already there: all 15 products of listing 1726597659 carry REPTILE 3. Nothing was changed.' && W.E.calls.length === calls12b && W.E.puts.length === puts12b && s.dialogs === 0, 'a second press: "' + s.say + '", the reptile chameleon necklace says "Already there", no Etsy request, no PUT, no password');
  ok(ctx.__errors.length === 0 && aborted.length === 0, 'no page error, no outside request: ' + ctx.__errors.concat(aborted).join(' | '));
  await ctx.close();

  console.log('\netsy-link-batch: ' + n + ' checks passed (ALL on the offline fake Etsy: no Etsy request was made)');
  await browser.close(); srv.close && srv.close();
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
