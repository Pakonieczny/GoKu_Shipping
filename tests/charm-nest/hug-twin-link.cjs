// HUGTWIN (Paul, 14 Oct 2026): order 4200546688 (Etsy listing 1749318013 "Playing Card Earring...", stud earrings, Etsy SKU "CARDS 2 - (4 CARDS)", a Left / Right pair) was read as the master
// "CARDS 2 - (4 CARDS) (HUGGIE)" (8.1 x 10.4 mm, 7 holes: the huggie charm set) instead of "CARDS 2 - (4 CARDS)" (9.7 x 9.7 mm, 1 hole: the stud pair). Paul tapped the plain master under Review's
// "Similar charms in the library. One tap, kept for this listing" while the SANDBOX was on, and asked to be SURE that (1) the correction is saved and survives (reload, the next check, the next pull),
// (2) it is tied to the LISTING so every later order of it comes out right without fixing it again, and (3) it is tested: "I don't wanna be fixing the same problem multiple times."
//
// WHY A STUD LINE CAN READ THE TWIN (what the intake does; the exact SKU alone never does it: the control below). Three ways, each reproduced here:
//   · INVENTORY (the one that fits Paul's screens: the line was a stud pair, Review offered the plain master as a tap, and after the tap the order window said "Saved link: now CARDS 2 - (4 CARDS)", which
//     is only said for a line read through a saved link under a SKU other than the plain one): Etsy's SKU table for the listing (charm-nest-orders.js interpretLine 3113-3134) gives the product bought the SKU
//     "CARDS 2 - (4 CARDS) (HUGGIE)" while the transaction carries the plain one, so the line's SKU is replaced by Etsy's (the table may be the page's kept copy, up to 14 days old). The form guard then reads
//     the line AFTER that swap (lineForm, :3151): the "(HUGGIE)" in the swapped SKU makes it a huggie line, so "an earring line never takes a huggie design" has nothing to hold.
//   · a "Huggie CHARM SET" option value on the line (huggieSku, :929, appends the mark to ANY SKU);
//   · a link somebody saved earlier for this listing and SKU (a saved link beats the exact master SKU, DACHMAP).
//
// WHAT THIS RUNS (offline: no network, no Etsy, no AI, nothing real): the REAL intake (charm-nest-orders.js interpretLine), the REAL saved-link planner (charm-nest-review-link.js, the code a Review tap runs),
// and the REAL Library function (charmNestLibrary aliasPut / aliasGet / listingKindPut / sandboxReset) over an in-memory Firestore that refuses nested arrays, in BOTH modes (production, and the sandbox: every
// call tagged sandbox:true); then the REAL page (headless Chromium, the repo's offline fake site) taps the plain master under "Similar charms in the library". The master is the three real CARDS entries.
//   0 · the control: a stud line with the metal option only reads the PLAIN master by its exact SKU
//   1-4 · for each way above: the line reads the twin; the person's tap is ONE aliasPut read back by the cloud, in the SHARED store (never a Sandbox_ copy), under the listing and the SKU the line was matched with;
//         a reload; then every NEXT order of the listing (new receipt, new numbers, every metal it sells, with and without Etsy's table on the page) is the plain master, no question, two pieces, Left and Right;
//         another listing with the same SKU is not touched; the later tap replaces the earlier one
//   4c · the sandbox Reset does not delete the link; production reads the very same document
//   5 · a sandbox copy of an older answer (kept before REVLINK) never takes the pick back, not even after the sandbox writes the listing's kind (the gap on origin/main: that write moved the copy's updatedAt)
//   6 · the page: Review shows the twin as the charm it is matched to now and the plain master as one tap; the tap writes once; after a reload the next orders read the plain master
//   node tests/charm-nest/hug-twin-link.cjs        (PW_DIR=<a folder holding playwright-core>, CHROMIUM=<chrome> for part 6; skipped, and said so, when missing)
'use strict';
const assert = require('assert'), path = require('path');
const root = path.join(__dirname, '../..');
const Fk = require('./_sandboxFakes.cjs'); Fk.install();
const O = require(path.join(root, 'charm-nest-orders.js'));
const RL = require(path.join(root, 'charm-nest-review-link.js'));
const SF = require(path.join(root, 'charm-nest-sandbox-families.js'));
const lib = require(path.join(root, 'netlify/functions/charmNestLibrary.js'));
const repoStamp = require('./_repoToken.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), JSON.parse(JSON.stringify(b)), m); n++; };
const pass = name => console.log('  ✓', name);

const PLAIN = 'CARDS 2 - (4 CARDS)', TWIN = 'CARDS 2 - (4 CARDS) (HUGGIE)', LOOSE = 'CARDS2- 4 CARDS';
const MM = 1 / 0.352778;   // the master keeps points
const E = [   // the real entries: size in mm (Paul's cards), holes, design file (the lookalike findings of 12 Oct for CARDS2- 4 CARDS)
  { sku: PLAIN, holes: 1, widthPt: 9.7 * MM, heightPt: 9.7 * MM, aiPath: 'charmnest/sandbox/master/CARDS_2_-_4_CARDS_.ai', charmHash: '1fbfa8b7', upAngle: 155 },
  { sku: TWIN, holes: 7, widthPt: 8.1 * MM, heightPt: 10.4 * MM, aiPath: 'charmnest/sandbox/master/CARDS_2_-_4_CARDS_HUGGIE_.ai', charmHash: 'c0ffee01', upAngle: 180 },
  { sku: LOOSE, holes: 0, widthPt: 32.728, heightPt: 35.413, aiPath: 'charmnest/sandbox/master/DECK_0120.ai', charmHash: '8b11a601' }
].map(e => Object.assign({ members: 2, sizes: null, blocked: null }, e));
const lib0 = new Map(E.map(e => [e.sku, e]));
for (const [k, e] of lib0) Fk.store.set('Charm_Master_Index/' + k, JSON.parse(JSON.stringify(e)));
const loose = {}, like = {}, byFile = new Map();
for (const k of lib0.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; (like[l] = like[l] || []).push(k); }
for (const [k, e] of lib0) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
const entryFor = s => lib0.get(String(s || '').toUpperCase()) || null, sharedFor = s => { const e = entryFor(s); return e ? byFile.get(e.aiPath) || [] : []; };

const post = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(await repoStamp('charmNestLibrary', body)) }); return { status: r.statusCode, body: JSON.parse(r.body) }; };
const must = async body => { const r = await post(body); assert.strictEqual(r.status, 200, JSON.stringify(body).slice(0, 140) + ' → ' + JSON.stringify(r.body).slice(0, 300)); return r.body; };

/** One page of the sorter in a mode: the saved links it holds are what the Library answers (aliasGet / optionMapGet, as loadMaps reads them); `tables` is the page's Etsy SKU tables. A new page() is a reload. */
function page(sb, tables) {
  const P = { maps: { optionMaps: {}, aliases: {} }, tables: tables || {}, calls: [] };
  const tag = b => Object.assign({}, b, sb ? { sandbox: true } : {});
  P.api = async (fn, body) => { P.calls.push(body.op); return must(tag(body)); };
  P.load = async () => { P.maps.aliases = (await must(tag({ op: 'aliasGet' }))).aliases; P.maps.optionMaps = (await must(tag({ op: 'optionMapGet' }))).maps; };
  P.ctx = () => ({ optionMaps: P.maps.optionMaps, aliases: P.maps.aliases, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: P.tables, masterEntry: entryFor, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: sharedFor, masterLike: s => (like[O.looseKey(s)] || []).slice(), masterSkus: () => [...lib0.keys()] });
  P.Orders = { loadMaps: P.load, ctx: P.ctx };
  P.read = line => { const order = { receiptId: 'R' + line.transactionId, updateTs: 1, createTs: 1 }; return { line, order, spec: O.interpretLine(order, JSON.parse(JSON.stringify(line)), P.ctx()) }; };
  P.save = (row, sku) => RL.save(row, sku, { by: 'Name One', api: P.api, orders: P.Orders });   // (what a tap on a similar charm runs for an order in Review)
  return P;
}
const TITLE = 'Playing Card Earring, Handmade Jewelry available in 14k Gold Fill, Sterling Silver, 14k Rose Gold Fill or 14K Solid Gold Card Earrings';
const HUGSET = ['Charm Type', 'Huggie CHARM SET'];
// the listing's products, one per metal (Etsy's product and option ids): the gold one is the product Etsy's table gives the HUGGIE SKU, the others keep the plain one
const PRODUCTS = [{ pid: '501', vid: '3001', metalKey: 'gold', metal: 'Gold Filled', sku: TWIN }, { pid: '502', vid: '3002', metalKey: 'silver', metal: 'Sterling Silver', sku: PLAIN }, { pid: '503', vid: '3003', metalKey: 'rose', metal: 'Rose Gold Filled', sku: PLAIN }];
const tableOf = () => ({ at: Date.now(), products: PRODUCTS.map(p => ({ id: p.pid, sku: p.sku, d: 0, pv: [['2001', p.vid]] })) });
const PLAIN_METALS = PRODUCTS.map(p => ({ metalKey: p.metalKey, metal: p.metal }));   // (no ids: a line of the listing that came with no product)
let tx = 8200000000, lidN = 0;
const newListing = () => '17493' + String(18013 + (++lidN) * 1000).padStart(5, '0');   // made-up listing numbers (the real 1749318013 is used by the page part)
/** A line of the listing. v: { metalKey, metal } or a PRODUCTS entry (its ids ride on the line, as Etsy gives them); extra: one more option [name, value]. */
const mk = (listing, sku, v, extra) => { v = v || PLAIN_METALS[0];
  const variation = { name: 'Metal Choice', value: v.metal }; if (v.pid) { variation.propertyId = '2001'; variation.valueId = v.vid; }
  return Object.assign({ transactionId: String(++tx), listingId: String(listing), sku, title: TITLE, quantity: 1, metalKey: v.metalKey, metalLabel: '', personalization: [], buyerMessage: '', variations: [variation].concat(extra ? [{ name: extra[0], value: extra[1] }] : []) }, v.pid ? { productId: v.pid } : {}); };
const design = sp => [sp.designSku, sp.skuSource];
const asks = sp => (sp.problems || []).map(p => p.kind);
const L = lid => ({ Alias: () => Fk.store.get('Charm_Sku_Aliases/' + lid), Own: () => Fk.store.get('Sandbox_Charm_Sku_Aliases/' + lid) });

/** The NEXT orders of a listing: another receipt and new numbers each, one per product / metal, read on a page that has just loaded the saved links (a reload). */
function nextOrders(page_, listing, W, variants, extra) {
  for (const v of variants) {
    const sp = page_.read(mk(listing, PLAIN, v, extra)).spec;
    ok(sp.designSku === PLAIN, W + `: the next order (${v.metal}) reads ${PLAIN}, not ${sp.designSku} (${sp.skuSource})`);
    eq(asks(sp), [], W + `: the next order (${v.metal}) asks nothing, so it never reaches Review`);
    ok(sp.pieceCount === 2 && sp.pair && sp.pair.kind === 'pair' && sp.pair.sides.join('') === 'LR', W + `: a Left / Right pair of two pieces (${v.metal})`);
  }
}
const reload = async (sb, tables) => { const p = page(sb, tables); await p.load(); return p; };

async function mode(sb) {
  const W = sb ? 'sandbox' : 'production';

  /* ── 0 · the control: the exact plain SKU is the plain master ── */
  { const P = page(sb), sp = P.read(mk(newListing(), PLAIN)).spec;
    eq(design(sp), [PLAIN, 'transaction'], W + ': a stud line with the metal option only reads the plain master by its exact SKU');
    eq(asks(sp), [], W + ': with no question'); ok(sp.lineForm === 'earring' && sp.pieceCount === 2, W + ': an earring pair, two pieces');
    ok(O.masterForm(TWIN, { masterEntry: entryFor, masterShared: sharedFor }).form === 'huggie', W + ': the twin is a huggie design in the library');
    pass(W + ' · 0 · the exact plain SKU is the plain master: nothing in the SKU itself makes a stud line read the twin'); }

  /* ── 1-4 · the three ways to the twin, and the correction for each ── */
  const ROUTES = [
    { name: 'Etsy\'s SKU table gives the product bought the (HUGGIE) SKU', key: TWIN, tables: lid => ({ [lid]: tableOf() }), first: PRODUCTS[0], variants: PRODUCTS, other: PLAIN,
      bad: (sp, lid) => ok(sp.skuSource === 'inventory' && sp.viaInventory && sp.viaInventory.sku === TWIN && sp.viaInventory.tx === PLAIN, W + ': the line\'s SKU was replaced by Etsy\'s "' + TWIN + '": ' + JSON.stringify(sp.viaInventory)) },
    { name: 'a "Huggie CHARM SET" option on the line', key: TWIN, extra: HUGSET, variants: PLAIN_METALS, other: TWIN,
      bad: sp => ok(sp.skuSource === 'transaction' && sp.boughtSku === TWIN, W + ': the Etsy SKU was read as "' + sp.boughtSku + '" (' + sp.skuSource + ')') },
    { name: 'a link saved earlier for the listing and the SKU', key: PLAIN, variants: PLAIN_METALS, other: PLAIN,
      cause: async (P, listing) => { await P.api('charmNestLibrary', { op: 'aliasPut', listingId: listing, sku: TWIN, fromSku: PLAIN, by: 'Name One', title: TITLE }); await P.load(); },
      bad: sp => ok(sp.skuSource === 'alias', W + ': the earlier link (a person\'s tap) beat the exact master SKU: ' + sp.skuSource) }
  ];
  for (const R of ROUTES) {
    const listing = newListing(), l = L(listing), P = page(sb, R.tables && R.tables(listing));
    if (R.cause) await R.cause(P, listing);
    const row = P.read(mk(listing, PLAIN, R.first, R.extra)), sp = row.spec;
    eq(sp.designSku, TWIN, W + ' · ' + R.name + ': Etsy SKU "' + PLAIN + '" is read as the HUGGIE twin (the bug)'); R.bad(sp, listing);

    // 2 · the person's tap on the plain master: ONE write, read back, shared store, under the SKU the line was matched with
    const plan = RL.planOf(row, PLAIN);
    eq([plan.ok, plan.via, plan.body.op, plan.body.listingId, plan.body.fromSku, plan.body.sku], [true, 'alias', 'aliasPut', listing, R.key, PLAIN], W + ': the link is kept for the listing and the SKU the line was matched with (' + R.key + ')');
    Fk.writes.length = 0;
    const r = await P.save(row, PLAIN);
    ok(r.ok && r.verified === true && r.via === 'alias' && r.readBack.kind === 'bySku' && r.readBack.key === R.key && r.readBack.sku === PLAIN, W + ': saved and read back by the cloud: ' + JSON.stringify(r.readBack));
    eq(Fk.writes.filter(w => /Aliases|Option_Map/.test(w.path)).map(w => w.kind + ' ' + w.path), ['set Charm_Sku_Aliases/' + listing], W + ': exactly ONE write, in the SHARED store');
    ok(!l.Own(), W + ': nothing in a Sandbox_ copy');
    eq(l.Alias().bySku[R.key], PLAIN, W + ': stored under ' + R.key);
    ok(r.proof.ok && r.proof.designSku === PLAIN, W + ': the page\'s own proof: a new order of the listing reads as the pick: ' + JSON.stringify(r.proof));
    if (R.first) ok(r.proof.withoutTable && r.proof.withoutTable.ok === true, W + ': and on a page that holds no Etsy table: ' + JSON.stringify(r.proof.withoutTable));

    // 3 · a reload (with the table the page kept, and with none), then the next orders of the listing, and the same line read again at the next pull
    for (const [what, tables] of R.tables ? [['no table', {}], ['the kept table', R.tables(listing)]] : [['', {}]]) {
      const fresh = await reload(sb, tables), w = W + (what ? ' (' + what + ')' : '');
      nextOrders(fresh, listing, w, R.variants, R.extra);
      if (R.first) eq(design(fresh.read(mk(listing, PLAIN, R.first, R.extra)).spec), [PLAIN, 'alias'], w + ': the product the table named is matched by the saved link');
      const again = fresh.read(row.line).spec; ok(again.designSku === PLAIN && !asks(again).length, w + ': the very same line, read again at the next pull, is the plain master');
      // 4 · another listing with the same SKU is its own: the link is per listing
      const o = fresh.read(mk(newListing(), PLAIN, null, R.extra)).spec;
      eq(o.designSku, R.other, w + ': another listing with the same SKU is NOT touched by this listing\'s link (it reads ' + R.other + ')');
    }
    pass(W + ' · 1-4 · ' + R.name + ': the tap is saved once (shared store), read after a reload, and every next order of the listing is the plain master with no question');
  }

  /* ── 4b · the later tap for a listing and SKU replaces the earlier one (a person decides, always) ── */
  { const listing = newListing(), l = L(listing), P = page(sb);
    await P.api('charmNestLibrary', { op: 'aliasPut', listingId: listing, sku: TWIN, fromSku: PLAIN, by: 'Name One' }); await P.load();
    const row = P.read(mk(listing, PLAIN)); eq(row.spec.designSku, TWIN, W + ': (bug) the earlier tap');
    await P.save(row, PLAIN); eq(l.Alias().bySku, { [PLAIN]: PLAIN }, W + ': one key, the latest tap');
    await P.save(P.read(mk(listing, PLAIN)), TWIN); await P.load(); eq(P.read(mk(listing, PLAIN)).spec.designSku, TWIN, W + ': a later tap for the huggie twin is the listing\'s answer in its turn');
    await P.save(P.read(mk(listing, PLAIN)), PLAIN); await P.load(); eq(P.read(mk(listing, PLAIN)).spec.designSku, PLAIN, W + ': and back');
    pass(W + ' · 4b · the latest tap for a listing and SKU is the one that stands'); }
}

(async () => {
  await repoStamp.prime();
  for (const sb of [false, true]) await mode(sb);

  /* ── 4c · THE SANDBOX: the pick is in the shared store, survives the Reset, and production reads the very same link ── */
  { const listing = newListing(), sbp = page(true, { [listing]: tableOf() }), prod = page(false), l = L(listing);
    const row = sbp.read(mk(listing, PLAIN, PRODUCTS[0])); eq(row.spec.designSku, TWIN, 'sandbox: (bug) read as the twin');
    const r = await sbp.save(row, PLAIN); ok(r.ok && r.proof.ok, 'the tap made in the sandbox: ' + JSON.stringify(r.proof));
    const mine = () => Fk.canon(Fk.store.get('Charm_Sku_Aliases/' + listing)), before = mine();
    await prod.load(); eq(design(prod.read(mk(listing, PLAIN, PRODUCTS[0])).spec), [PLAIN, 'alias'], 'production reads the link the sandbox saved (the product the link names)');
    eq(prod.maps.aliases[listing].bySku, { [TWIN]: PLAIN }, 'the link itself, as production reads it');
    for (let calls = 0, r2 = { more: true }; r2.more && calls < 50; calls++) r2 = (await post({ op: 'sandboxReset', sandbox: true })).body;
    eq(mine(), before, 'RESET: the saved link is byte-identical in the shared store');
    ok(SF.isProtected('firestore', 'Charm_Sku_Aliases/' + listing) && !SF.firestoreCollections().includes('Charm_Sku_Aliases') && SF.firestoreCollections().includes('Sandbox_Charm_Sku_Aliases'), 'the registry the Reset and the Purge both read keeps Charm_Sku_Aliases and wipes only the sandbox copy');
    const after = await reload(true, { [listing]: tableOf() }); nextOrders(after, listing, 'after the Reset', PRODUCTS);
    eq(after.maps.aliases[listing], (await reload(false)).maps.aliases[listing], 'the sandbox and production read the very same document');
    ok(!l.Own(), 'no sandbox copy was ever made'); pass('4c · the Reset never reaches the tap; sandbox and production read the same link'); }

  /* ── 5 · a sandbox copy of an OLDER answer (kept before REVLINK, 12 Oct) never takes a pick back ── */
  { const listing = newListing(), l = L(listing), sbp = page(true), old = Date.now() - 3 * 86400000;
    // the old rehearsal answer: this listing and SKU were once linked to the huggie twin in the sandbox's own copy (the way the sandbox kept answers until REVLINK)
    Fk.store.set('Sandbox_Charm_Sku_Aliases/' + listing, { listingId: listing, by: 'Name One', bySku: { [PLAIN]: TWIN }, updatedAt: new Fk.TS(old) });
    await sbp.load(); const row = sbp.read(mk(listing, PLAIN)); eq(design(row.spec), [TWIN, 'alias'], 'sandbox: (bug) the old sandbox copy makes the plain SKU read as the twin');
    const r = await sbp.save(row, PLAIN); ok(r.ok && r.proof.ok, 'the tap after it: ' + JSON.stringify(r.proof));
    nextOrders(await reload(true), listing, 'sandbox, after the tap', PLAIN_METALS);
    // the sandbox writes the listing's KIND (a person marks the listing in Review): that write moves the copy's update time, not its answers' time
    await must({ op: 'listingKindPut', listingId: listing, kind: 'regular', by: 'Name One', sandbox: true });
    ok(l.Own() && l.Own().listingKind && l.Own().bySku[PLAIN] === TWIN, 'the sandbox copy still holds the old answer and now a listing kind too');
    const later = await reload(true); nextOrders(later, listing, 'sandbox, after the listing kind was written', PLAIN_METALS);
    eq(later.maps.aliases[listing].bySku[PLAIN], PLAIN, 'the link as the sandbox reads it is still the person\'s tap');
    eq((await reload(false)).maps.aliases[listing].bySku[PLAIN], PLAIN, 'production reads the tap');
    // an answer made in the sandbox copy AFTER the tap is still the later write (the rule as it was): nothing here forbids the sandbox's own newer answer
    Fk.store.set('Sandbox_Charm_Sku_Aliases/' + listing, Object.assign({}, Fk.store.get('Sandbox_Charm_Sku_Aliases/' + listing), { bySku: { [PLAIN]: TWIN }, answeredAt: new Fk.TS(Date.now() + 60000), updatedAt: new Fk.TS(Date.now() + 60000) }));
    eq((await reload(true)).maps.aliases[listing].bySku[PLAIN], TWIN, 'a sandbox answer written later than the tap still stands (the later write of the two, as before)');
    pass('5 · an older sandbox answer never overrides the pick, also after the listing kind is written'); }

  /* ── 6 · the REAL page: Review's "Similar charms" tap, the saved link, a reload, the next orders (sandbox and production) ── */
  const skipped = await browserPart();
  console.log(`hug-twin-link: ${n} checks ok` + (skipped ? ` (part 6, the page, was skipped: ${skipped})` : ''));
})().catch(e => { console.error(e); process.exit(1); });

/** The sorter page in a headless Chromium over the repo's offline fake site (tests/charm-nest/bridge-server.cjs: the real charmNestLibrary handler over an in-memory Firestore; no Etsy, no AI). */
async function browserPart() {
  let chromium; try { chromium = require(path.join(process.env.PW_DIR || path.join(root, 'node_modules'), 'playwright-core')).chromium; } catch (_) { return 'playwright-core not found, set PW_DIR'; }
  const exe = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
  if (!require('fs').existsSync(exe)) return 'no Chromium at ' + exe;
  const { start } = require('./bridge-server.cjs');
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  try { for (const sb of [false, true]) await browserMode(browser, start, sb); } finally { await browser.close(); }
  return '';
}
async function browserMode(browser, start, sb) {
  const W = 'page ' + (sb ? 'sandbox' : 'production'), srv = await start({ receipts: [] }), { st, sorterOrigin } = srv, SHARED = 'Charm_Sku_Aliases', OWN = 'Sandbox_Charm_Sku_Aliases';
  try {
    for (const e of E) st.put('Charm_Master_Index', e.sku, Object.assign({}, e, { masterHash: 'mh-' + e.sku, areaPt2: e.widthPt * e.heightPt * .7, aiUrl: srv.blobUrl(e.aiPath), thumbPath: 'charmnest/master/' + e.sku + '.png', thumbUrl: srv.blobUrl('charmnest/master/' + e.sku + '.svg') }));
    for (const e of E) st.blobs.set('charmnest/master/' + e.sku + '.svg', { buf: Buffer.from(`<svg xmlns="http://www.w3.org/2000/svg" width="168" height="168"><rect width="168" height="168" fill="#ece7dc"/><circle cx="84" cy="84" r="${30 + e.holes * 4}" fill="#555"/></svg>`), generation: 1, meta: { contentType: 'image/svg+xml', metadata: { firebaseStorageDownloadTokens: 't' } } });
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(cfg => { try { localStorage.setItem('cn.settings', JSON.stringify(cfg)); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} window.prompt = () => 'Test Operator'; },
      Object.assign({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off' }, sb ? { sandbox: 'on' } : {}));
    const pg = await context.newPage(); pg.setDefaultTimeout(30000);
    const errors = [], etsy = []; pg.on('pageerror', e => errors.push(e.message)); pg.on('dialog', d => d.dismiss().catch(() => {}));
    pg.on('request', q => { let h = ''; try { h = new URL(q.url()).hostname; } catch (_) {} if (/etsy/i.test(h)) etsy.push(q.url()); });
    const ready = async () => { await pg.goto(`${sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' }); await pg.waitForFunction(() => window.CN && window.Orders && window.Review && CN.S.cloud.ok === true && window.B && B.master && B.master.entries && B.master.entries.size >= 3, null, { timeout: 60000 }); };
    const LB = '1749318013', LC = '1749318015';   // LB: Paul's listing; LC: another stud listing with the same SKU
    const table = tableOf(), KEPT = 'cn.listingSkus.v1';
    // the page keeps the SKU table it was told for up to 14 days (localStorage): Etsy's table for LB says the gold product carries the HUGGIE SKU
    await ready(); await pg.evaluate(([k, lb, t]) => localStorage.setItem(k, JSON.stringify({ v: 1, tables: { [lb]: t } })), [KEPT, LB, table]); await ready();
    const addOrder = (rid, listing, v) => pg.evaluate(([rid, listing, title, sku, v]) => {
      const order = { receiptId: rid, orderNumber: rid, createTs: 1789000000, updateTs: 1789000000, shipBy: 1790000000, buyer: { name: 'Test Buyer' }, buyerMessage: '', staffNote: '', messages: [],
        lines: [Object.assign({ transactionId: 'T' + rid, listingId: listing, sku, title, quantity: 1, variations: [Object.assign({ name: 'Metal Choice', value: v.metal }, v.pid ? { propertyId: '2001', valueId: v.vid } : {})], metalKey: v.metalKey, metalLabel: '', personalization: [], buyerMessage: '' }, v.pid ? { productId: v.pid } : {})] };
      const line = order.lines[0], key = CharmNestOrders.lineKey(order, line), row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(key, row); Orders.interpretAll();
      return { key, design: row.spec.designSku, source: row.spec.skuSource, bought: row.spec.boughtSku, asks: (row.problems || []).map(p => p.kind), pieces: row.spec.pieceCount, form: row.spec.lineForm };
    }, [rid, listing, TITLE, PLAIN, v]);
    const first = await addOrder('4200546688', LB, PRODUCTS[0]);
    eq([first.design, first.source, first.bought, first.asks, first.pieces], [TWIN, 'inventory', TWIN, [], 2], W + ': listing ' + LB + ', a stud pair: Etsy\'s kept table gives the product the (HUGGIE) SKU, so it reads the twin with no question (the bug)');
    await pg.evaluate(async rb => { const x = await CharmNestReviewMark.mark(rb, { by: 'Test Operator' }); if (!x.ok) throw new Error(JSON.stringify(x)); Review.syncOrderItems && Review.syncOrderItems(); CN.setMode('review'); Review.render(); }, '4200546688');
    await pg.waitForSelector('#rvList .reviewListRow');
    { const rid = '4200546688', offer = pg.locator(`#rvList .reviewListRow[data-rid="${rid}"][data-mkey^="ord:rvm:"] .sibOffer`);
      await offer.waitFor();
      eq(await offer.locator('[data-cur]').getAttribute('data-cur'), TWIN, W + ': Review shows the twin as the charm it is matched to now (not a tap)');
      const taps = await offer.locator('button[data-dsku]').evaluateAll(l => l.map(x => x.dataset.dsku));
      ok(taps.includes(PLAIN), W + ': the plain master is offered as one tap under "Similar charms in the library": ' + taps.join(' | '));
      eq(await offer.locator('.sibAsk').first().innerText(), 'Similar charms in the library. One tap, kept for this listing:', W + ': the words Paul tapped under');
      const was = st.calls.length;
      await offer.locator(`button[data-dsku="${PLAIN}"]`).click();
      await pg.waitForFunction(r => !CharmNestReviewMark.isMarked(r), rid, { timeout: 20000 });
      const puts = st.calls.slice(was).filter(c => c.name === 'charmNestLibrary' && c.op === 'aliasPut');
      eq(puts.length, 1, W + ': ONE saved-link write for the tap');
      eq([String(puts[0].body.listingId), puts[0].body.sku, puts[0].body.fromSku, puts[0].body.via], [LB, PLAIN, TWIN, ['P501', 'O2001:3001']], W + ': the write is for the listing, the SKU the line was matched with, and the product');
      ok(!sb || puts[0].body.sandbox === true, W + ': the page said so (sandbox: ' + puts[0].body.sandbox + ')');
      const info = await pg.evaluate(r => CharmNestReviewMark.info(r), rid); ok(info && info.state === 'done' && info.how === 'pick' && info.sku === PLAIN, W + ': the review closed as a pick of ' + PLAIN + ': ' + JSON.stringify(info)); }
    eq(st.doc(SHARED, LB).bySku, { [TWIN]: PLAIN }, W + ': listing ' + LB + ' keeps its link in the shared store, under the SKU as bought');
    ok(!st.docs.has(OWN + '/' + LB), W + ': nothing in a Sandbox_ copy');
    // a RELOAD of the page, with the table the page kept and again with none, then the next orders (a new receipt each, the three metals) and the next check of the same lines
    let seq = 4200546700;
    for (const kept of [true, false]) {
      if (!kept) await pg.evaluate(k => localStorage.removeItem(k), KEPT);
      await ready(); await pg.evaluate(() => Orders.loadMaps(true));
      for (const [what, listing] of [['a stud order of ' + LB, LB], ['a stud order of ANOTHER listing with the same SKU', LC]])
        for (const v of PRODUCTS) {
          const x = await addOrder(String(++seq), listing, listing === LB ? v : Object.assign({}, v, { pid: undefined, vid: undefined }));
          eq([x.design, x.asks, x.pieces], [PLAIN, [], 2], W + ': after the reload (' + (kept ? 'the kept table' : 'no table') + '), ' + what + ' (' + v.metal + ') reads ' + PLAIN + ', no question, two pieces');
        }
      // (the page restores the orders it held before the reload: Paul's own order 4200546688 is read again with the saved links, at the next check, and is the plain master too)
      const rechecked = await pg.evaluate(() => { Orders.interpretAll(); return Orders.rows().map(r => r.order.receiptId + ' ' + r.spec.designSku); });
      ok(rechecked.length >= 6 && rechecked.every(x => x.endsWith(' ' + PLAIN)) && rechecked.some(x => x.startsWith('4200546688 ')), W + ': the next check of every line, Paul\'s own order included, reads the plain master: ' + rechecked.join(' | '));
    }
    ok(!errors.length && !etsy.length, W + ': no page error, no Etsy call: ' + errors.concat(etsy).join(' | '));
    pass(W + ' · 6 · the tap on the plain master under "Similar charms" is saved once, for the listing, and the next orders read it after a reload');
  } finally { srv.close(); }
}
