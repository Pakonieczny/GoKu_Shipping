// SKUGEN (11 Oct 2026; plans/save-to-repo-1011/CONTRACT.md sections 3, 7, 8): the SKU a custom design gets when it is saved to the charm
// repository (charm-nest-sku.js) and the read-only cloud check of it (netlify/functions/_charmNestSkuCheck.js, op customSkuCheck).
// Offline: the in-memory Firestore and Storage of _sandboxFakes.cjs, no network, no Etsy, no write.
//   1  the module loads in a page (no require) and answers exactly as in node
//   2  NAME: the examples (ACORN&#39;S LEAF, Test_Charm.ai, Fox head FINAL v2.ai), a long name, accents, "&", empty / digits / default names
//   3  reserved: every SKU_RULES family read out of charm-nest-orders.js, -CO, (HUGGIE), -12 endings are never produced
//   4  property: 500 names of the real master x random listing ids: not reserved, the real resolveSku says "transaction" and the real
//      interpretLine reads no special order once the master holds the SKU (and a reserved SKU still does: the control)
//   5  collisions walk TAIL4, 5 digits, 6 digits, _A; a look-alike twin; an alias key and value; a sibling product of the listing
//   6  group modes (one product, 32 sharing one SKU, distinct SKUs, a subset sharing one, a deleted product, no table, by options)
//   7  aliasWorks (false for CUSTOM_6673)
//   8  the op: free / taken per reason, odd ids, at most 12 reads and no scan, no write, no Etsy, not even the placement counter
//      node tests/charm-nest/save-sku.cjs
'use strict';
const assert = require('assert'), path = require('path'), fs = require('fs'), vm = require('vm');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
const Fk = require('./_sandboxFakes.cjs'); Fk.install();
const { store, writes, fileWrites } = Fk;
let checks = 0;
const ok = (cond, msg) => { checks++; assert(cond, msg); };
const eq = (a, b, msg) => { checks++; assert.deepStrictEqual(a, b, msg); };
let fetched = 0; const realFetch = global.fetch; global.fetch = async (...a) => { fetched++; return realFetch(...a); };   // (an Etsy call, or any call at all, would show here)
const warn = console.warn; console.warn = () => {};

const O = require(path.join(root, 'charm-nest-orders.js'));
const Sku = require(path.join(root, 'charm-nest-sku.js'));
const Check = require(path.join(fnDir, '_charmNestSkuCheck.js'));
const lib = require(path.join(fnDir, 'charmNestLibrary.js'));
const etsyLoaded = () => Object.keys(require.cache).filter(k => /_etsyMailEtsy\.js$|etsyListingInventory/.test(k));
const etsyBefore = etsyLoaded().length;

/* the oracle: the real code of charm-nest-orders.js, asked directly (not through Sku.reserved) */
const oracleWhy = sku => {
  if (!/^[A-Z0-9][A-Z0-9_-]*$/.test(sku)) return 'characters';
  if (sku.length > 32) return 'length';
  if (O.specialOf({ sku, title: '', variations: [] }, {})) return 'special';
  if (O.variationBase(sku)) return '-CO';
  if (/\(HUGGIE\)/.test(sku)) return 'huggie';
  if (/[-_]\d{1,3}$/.test(sku) || O.skuIndexFamily(sku, () => true)) return 'numbered';
  if (O.splitSkus(sku)) return 'two designs';
  return '';
};
const LID = '1096264821';   // tail 4821, 64821, 264821

(async () => {
  /* ═══ 1 · the module in a page ═══ */
  {
    const page = vm.createContext({}); page.self = page;
    vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-orders.js'), 'utf8'), page);
    vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-sku.js'), 'utf8'), page);
    ok(page.CharmNestSku && typeof page.CharmNestSku.plan === 'function', 'charm-nest-sku.js defines CharmNestSku in a page');
    for (const f of ['Test_Charm.ai', 'ACORN&#39;S LEAF.ai', 'Fox head FINAL v2.ai', 'custom leaf.ai', 'Café Étoilé.ai'])
      eq(JSON.parse(JSON.stringify(page.CharmNestSku.candidates(LID, page.CharmNestSku.nameOf(f)))), Sku.candidates(LID, Sku.nameOf(f)), 'page and node agree on ' + f);
    // the page may load the two scripts in either order: the orders module is looked for when first needed
    const early = vm.createContext({}); early.self = early;
    vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-sku.js'), 'utf8'), early);
    vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-orders.js'), 'utf8'), early);
    eq(early.CharmNestSku.nameOf('Test_Charm.ai'), 'TEST_CHARM', 'loaded before the orders script');
    ok(Check.Sku === Sku, 'the cloud check requires the very same module as the page (one rule)');
  }

  /* ═══ 2 · NAME ═══ */
  const name = (f, t) => Sku.nameOf(f, t);
  eq(name('ACORN&#39;S LEAF'), 'ACORNS_LEAF', 'ACORN&#39;S LEAF');
  eq(name('ACORN&#39;S LEAF.ai'), 'ACORNS_LEAF', 'with its extension');
  eq(name("ACORN'S LEAF.dxf"), 'ACORNS_LEAF', 'decoded apostrophe');
  eq(name('ACORN&#x27;S LEAF.ai'), 'ACORNS_LEAF', 'hex entity');
  eq(name('ACORN&amp;#39;S LEAF.ai'), 'ACORNS_LEAF', 'double-escaped entity');
  eq(name('Acorn’s Leaf.ai'), 'ACORNS_LEAF', 'typographic apostrophe');
  eq(name('Test_Charm.ai'), 'TEST_CHARM', 'Test_Charm.ai');
  eq(Sku.candidates('4196630529', 'TEST_CHARM')[0], 'TEST_CHARM_0529', 'the SKU of a listing ending 0529');
  eq(name('Fox head FINAL v2.ai'), 'FOX_HEAD', 'Fox head FINAL v2.ai');
  eq(name('Fox_Head_final_V12.AI'), 'FOX_HEAD', 'upper-case extension and version');
  eq(name('Fox (2).ai'), 'FOX', 'counter (2)'); eq(name('Fox copy (2).ai'), 'FOX', 'copy (2)'); eq(name('Fox - Copy.ai'), 'FOX', 'dash copy');
  eq(name('Heart outlined final.ai'), 'HEART', 'two junk words'); eq(name('Heart V1.2.ai'), 'HEART', 'version with a dot');
  eq(name('New York.ai'), 'NEW_YORK', 'the first word is never junk'); eq(name('FINAL.ai'), 'FINAL', 'a single junk word is the name');
  eq(name('Rose cut.ai'), 'ROSE', 'trailing CUT'); eq(name('Fox new.ai'), 'FOX', 'trailing NEW');
  eq(name('Café Étoilé Ñandú.ai'), 'CAFE_ETOILE_NANDU', 'accents folded');
  eq(name('Fußball Æsir.ai'), 'FUSSBALL_AESIR', 'sharp s and ligature');
  eq(name('Salt & Pepper.ai'), 'SALT_AND_PEPPER', '& is AND'); eq(name('Salt&amp;Pepper.ai'), 'SALT_AND_PEPPER', 'escaped &'); eq(name('Salt&Pepper.ai'), 'SALT_AND_PEPPER', '& without spaces');
  eq(name('  heart   &   soul!!! .ai'), 'HEART_AND_SOUL', 'runs of other characters are one underscore, ends trimmed');
  eq(name('C:\\Users\\Ana\\Designs\\Fox Head.ai'), 'FOX_HEAD', 'a path'); eq(name('/tmp/x/fox.head.ai'), 'FOX_HEAD', 'a dot inside a name');
  eq(name('Fox.ai.ai'), 'FOX', 'a doubled extension');
  eq(name({ name: 'Test_Charm.ai' }), 'TEST_CHARM', 'an object with .name');
  // long names
  eq(name('Supercalifragilisticexpialidocious charm.ai'), 'SUPERCALIFRAGILIST', 'one long word is cut hard at 18');
  eq(name('Mountain landscape with pine trees and moon.ai'), 'MOUNTAIN_LANDSCAPE', 'cut at the word that ends at 18');
  eq(name('Mountain landscapes with pine trees.ai'), 'MOUNTAIN_LANDSCAPE', 'no "_" in the last 8 of the 18 (the only one is at 8): cut hard');
  eq(name('Big dragon curled around moon.ai'), 'BIG_DRAGON_CURLED', 'a "_" in the last 8: the whole words that fit');
  eq(name('Big dragonfly curled around.ai'), 'BIG_DRAGONFLY', 'a "_" at 13 is in the last 8: cut there, not mid-word');
  for (const n of ['Mountain landscapes with pine trees.ai', 'Big dragonfly curled around.ai', 'a'.repeat(60) + '.ai', 'Word '.repeat(30) + '.ai']) ok(name(n).length <= 18 && !/^_|_$/.test(name(n)), 'NAME is at most 18, no "_" at its ends: ' + name(n));
  // empty, digits and default names go to the listing's title
  eq(name('1234.ai', 'Heart Charm Necklace, 14k gold filled'), 'HEART_CHARM', 'digits only: the title lead, cut at 18');
  eq(name('', 'Lion Charm - custom'), 'LION_CHARM', 'empty name: the title lead (up to the dash)');
  eq(name('.ai', 'Acorn&#39;s Leaf | Necklace'), 'ACORNS_LEAF', 'title lead up to a pipe, decoded');
  eq(name('Untitled-1.ai', 'Add a Lion Charm, gift'), 'LION_CHARM', 'an Illustrator default name: the title, "Add a" left off');
  eq(name('Artboard 3.ai', 'Moon Phase Necklace'), 'MOON_PHASE', 'ARTBOARD 3 is a default name: the title, cut at the "_" that lies at 10 (in the last 8 of the 18)');
  eq(name('猫.ai'), 'CHARM', 'nothing in A-Z 0-9 and no title: CHARM'); eq(name('1234.ai', '1234'), 'CHARM', 'digits in both: CHARM'); eq(name(null, null), 'CHARM', 'nothing at all');
  eq(name('Fox.ai', 'Heart'), 'FOX', 'the file wins over the title');
  // candidates
  eq(Sku.candidates(LID, 'TEST_CHARM'), ['TEST_CHARM_4821', 'TEST_CHARM_64821', 'TEST_CHARM_264821', 'TEST_CHARM_264821_A', 'TEST_CHARM_264821_B', 'TEST_CHARM_264821_C', 'TEST_CHARM_264821_D', 'TEST_CHARM_264821_E', 'TEST_CHARM_264821_F'], 'the order: TAIL4, 5 digits, 6 digits, 6 digits + _A..._F');
  eq(Sku.candidates('4821', 'FOX').slice(0, 2), ['FOX_4821', 'FOX_4821_A'], 'a short id: no duplicate tails');
  eq(Sku.candidates('', 'FOX'), [], 'no listing id, no candidate'); eq(Sku.candidates('abc', 'FOX'), [], 'an id with no digits');
  eq(Sku.candidates('12/34 5678', 'FOX')[0], 'FOX_5678', 'only the digits of the id count');
  {   // 32 characters at most, whatever the name and the tail
    const longest = 'X'.repeat(18);
    for (const lid of ['1234567890', '9'.repeat(12)]) for (const nm of [longest, 'CUSTOM_' + 'Y'.repeat(11)]) for (const s of Sku.candidates(lid, nm)) ok(s.length <= 32 && !oracleWhy(s), 'at most 32, allowed: ' + s);
    ok(Sku.candidates(LID, 'CUSTOM_' + 'Y'.repeat(11)).every(s => s.startsWith('CHARM_CUSTOM_')), 'a reserved name gets CHARM_ in front, even when the whole is cut to fit');
  }

  /* ═══ 3 · reserved ═══ */
  eq([Sku.reserved('CUSTOM_6673'), Sku.reserved('CUSTOM-N-001-665441'), Sku.reserved('CSTM_X'), Sku.reserved('CUST_1')], [true, true, true, true], 'custom families');
  eq([Sku.reserved('FOX-CO'), Sku.reserved('FOX_CO'), Sku.reserved('FOX CO'), Sku.reserved('BUNNY (HUGGIE)')], [true, true, true, true], '-CO and (HUGGIE)');
  eq([Sku.reserved('FOX-12'), Sku.reserved('FOX_7'), Sku.reserved('FOX_123'), Sku.reserved('FOX_007'), Sku.reserved('ZODIAC_EARRINGS-3')], [true, true, true, true, true], 'numbered families (-12, _7, _123, _007)');
  eq([Sku.reserved('FOX_1234'), Sku.reserved('TEST_CHARM_4821'), Sku.reserved('FOX_264821_A'), Sku.reserved('A_4821')], [false, false, false, false], 'ordinary SKUs');
  eq([Sku.reserved('FOX HEAD_4821'), Sku.reserved('FOX/HEAD_4821'), Sku.reserved('FOX+HEAD'), Sku.reserved('FOX,HEAD'), Sku.reserved('FOX&HEAD'), Sku.reserved('FOX;HEAD'), Sku.reserved(''), Sku.reserved('_FOX_4821'), Sku.reserved('X'.repeat(33))], [true, true, true, true, true, true, true, true, true], 'characters, empty, start and length');
  eq(Sku.reserved('fox_4821'), false, 'the case of a SKU does not matter'); eq(Sku.reserved('NITIAL_DISC_4571'), true, 'a known Etsy typo');
  ok(/custom/i.test(Sku.reservedWhy('CUSTOM_6673')) && Sku.reservedWhy('TEST_CHARM_4821') === '', 'reservedWhy says why');
  {   // every SKU_RULES family is read out of the orders module itself, so a family added there is tested here
    const src = fs.readFileSync(path.join(root, 'charm-nest-orders.js'), 'utf8'), from = src.indexOf('const SKU_RULES = ['), block = src.slice(from, src.indexOf('\n  ];', from));
    const words = new Set(['CUSTOM', 'CSTM', 'CUST', 'RE', 'RW', 'REWORK', 'RE-WORK', 'REPAIR', 'MODIFICATION', 'MODIFY', 'MOD', 'FIX', 'REMAKE', 'CHAIN', 'CHN', 'EXTENDER', 'EXT', 'ADD', 'ADDON', 'ADD-ON', 'EXTRA', 'UPGRADE', 'UPG', 'GIFT', 'GIFTBOX', 'GIFTWRAP', 'GIFTBAG', 'BOX', 'POUCH', 'BAG', 'WRAP', 'RUSH', 'EXPRESS', 'PRIORITY', 'INSURANCE', 'SHIP', 'SHIPPING', 'CARD', 'SPECIAL', 'SPCL', 'PRIVATE', 'RESERVED', 'MTO', 'BESPOKE', 'DEPOSIT', 'PAYMENT', 'BALANCE', 'DIFF', 'DIFFERENCE', 'ORDER', 'RE5', 'RW12']);
    for (const m of block.matchAll(/\(\?:([A-Z0-9|\\-]+)\)/g)) for (const w of m[1].split('|')) words.add(w.replace(/\\/g, ''));
    ok(words.size >= 50, 'the families were read out of charm-nest-orders.js (' + words.size + ' words)');
    let reservedNames = 0;
    for (const w of words) for (const form of [w, w + ' thing', w + '-thing', w + '_5', w + '5', w + ' 12', w.toLowerCase() + ' Leaf']) {
      const nm = Sku.nameOf(form + '.ai'), cands = Sku.candidates('7771234564821', nm);
      ok(cands.length >= 3, 'candidates for ' + form);
      for (const s of cands) ok(!oracleWhy(s) && !Sku.reserved(s), `${form} -> ${s} is reserved (${oracleWhy(s)})`);
      if (oracleWhy(nm + '_4821')) { reservedNames++; ok(cands.every(s => s.startsWith('CHARM_')) && cands.slice(0, 3).every(s => s.startsWith('CHARM_' + nm)), `${form}: a reserved name gets CHARM_ in front of every candidate (the name is only shortened when 32 would be passed): ${cands.join(' ')}`); }
    }
    ok(reservedNames >= 80, 'many of the forms are reserved names (' + reservedNames + ') and every one was moved out of the way');
  }
  for (const f of ['Fox CO.ai', 'Fox-CO.ai', 'Bunny (HUGGIE).ai', 'Fox-12.ai', 'Fox_7.ai', 'RE_5460.ai', 'Custom_6673.ai', 'Zodiac-3.ai']) for (const s of Sku.candidates(LID, Sku.nameOf(f))) ok(!oracleWhy(s), f + ' -> ' + s);
  eq(Sku.candidates('12', 'FOX').filter(s => /[-_]\d{1,3}$/.test(s)), [], 'a 2-digit tail never makes a numbered-family SKU: FOX_12 is skipped');

  /* ═══ 4 · property: 500 names of the real master ═══ */
  {
    let seed = 20261011; const rnd = () => (seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296;
    const file = process.env.MASTER_SKUS || '/mnt/project-files/plans/flag-fix-1011/master-skus-1011.json';
    let names, source;
    if (fs.existsSync(file)) { const all = JSON.parse(fs.readFileSync(file, 'utf8')).map(e => e.sku); const pick = new Set(); while (pick.size < 500) pick.add(all[Math.floor(rnd() * all.length)]); names = [...pick]; source = 'the real master (' + all.length + ' SKUs)'; }
    else { const w = ['heart', 'star', 'moon', "acorn's", 'leaf', 'fox', 'custom', 'ring', 'card', 'box', 'RE', 'CHAIN', 'add', 'Café', 'x&y', '(HUGGIE)', 'CO', '12', 'v2', 'final']; names = Array.from({ length: 500 }, () => Array.from({ length: 1 + Math.floor(rnd() * 4) }, () => w[Math.floor(rnd() * w.length)]).join(rnd() < 0.5 ? ' ' : '_')); source = 'a synthetic list (the real master file is not on this machine)'; }
    const order = { receiptId: '4100000001', updateTs: 1, staffNote: '', buyerMessage: '' };
    let moved = 0, titled = 0, short = 0;
    for (const n of names) {
      const lid = String(1000000000 + Math.floor(rnd() * 8999999999)), cands = Sku.candidates(lid, Sku.nameOf(n + '.ai', 'Silver heart charm necklace, 14k'));
      ok(cands.length >= 6, 'candidates for ' + n); const sku = cands[0];
      ok(sku.endsWith('_' + lid.slice(-4)) && sku.length <= 32 && !oracleWhy(sku) && !Sku.reserved(sku), `${n} -> ${sku} (${oracleWhy(sku)})`);
      if (sku.startsWith('CHARM_') && !/^CHARM(_|$)/.test(Sku.nameOf(n + '.ai'))) moved++;
      const held = new Map([[sku, { sku }]]), me = s => held.get(String(s || '').toUpperCase()) || null, line = { transactionId: '41000000011', listingId: lid, sku, title: 'Silver heart charm necklace', quantity: 1, metalKey: 'silver', metalLabel: 'Silver', personalization: [], variations: [] };
      const r = O.resolveSku(line, {}, me, {}, () => '');
      ok(r.source === 'transaction' && r.sku === sku, `resolveSku says transaction for ${sku}: ${JSON.stringify(r)}`);
      const sp = O.interpretLine(order, line, { optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: me, masterLoose: () => '' });
      ok(!sp.special, `${sku} is no special order: ${JSON.stringify(sp.special)}`);
      eq(O.splitSkus(sku), null, sku + ' names one design');
    }
    // the control: a reserved SKU IS a special order even when the master holds it (so the checks above can fail)
    const bad = 'CUSTOM_4821', heldBad = new Map([[bad, { sku: bad }]]), meBad = s => heldBad.get(s) || null;
    const spBad = O.interpretLine({ receiptId: '4100000001', updateTs: 1, staffNote: '', buyerMessage: '' }, { transactionId: '41000000011', listingId: LID, sku: bad, title: 'Silver heart charm necklace', quantity: 1, metalKey: 'silver', metalLabel: 'Silver', personalization: [], variations: [] }, { optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: meBad, masterLoose: () => '' });
    ok(spBad.special && spBad.special.signals[0] === 'sku', 'control: CUSTOM_4821 in the master is still a custom order');
    console.log(`   property: ${names.length} names from ${source}; ${moved} names were reserved and got CHARM_ in front`);
  }

  /* ═══ 5 · collisions ═══ */
  {
    const master = new Set(), twins = new Map();   // twins: looseKey → master SKU
    const hasMaster = s => master.has(s), hasLoose = s => { const t = twins.get(O.looseKey(s)); return t && t !== s ? t : ''; };
    const base = { file: 'Fox head.ai', title: 'Fox charm', listingId: LID, line: { sku: '', variations: [] }, hasMaster, hasLoose, aliases: {}, table: null };
    let p = Sku.plan(base);
    ok(p.ok && p.sku === 'FOX_HEAD_4821' && p.name === 'FOX_HEAD' && p.group.mode === 'unknown' && p.aliasWorks === true && p.why === '', 'nothing taken: TAIL4 ' + JSON.stringify(p));
    eq(p.candidates.length, 6, 'six are sent'); eq(p.candidates, Sku.candidates(LID, 'FOX_HEAD').slice(0, 6), 'the first six');
    master.add('FOX_HEAD_4821'); p = Sku.plan(base); eq(p.sku, 'FOX_HEAD_64821', 'TAIL4 taken in the master: 5 digits'); eq(p.taken[0], { sku: 'FOX_HEAD_4821', by: 'master', detail: 'already a master design' }, 'told why');
    master.add('FOX_HEAD_64821'); p = Sku.plan(base); eq(p.sku, 'FOX_HEAD_264821', '5 digits taken: 6 digits');
    master.add('FOX_HEAD_264821'); p = Sku.plan(base); eq(p.sku, 'FOX_HEAD_264821_A', '6 digits taken: _A');
    eq(p.candidates, ['FOX_HEAD_264821_A', 'FOX_HEAD_264821_B', 'FOX_HEAD_264821_C', 'FOX_HEAD_264821_D', 'FOX_HEAD_264821_E', 'FOX_HEAD_264821_F'], 'the rest is sent, six at most');
    for (const L of 'ABCDEF') master.add('FOX_HEAD_264821_' + L);
    p = Sku.plan(base); ok(!p.ok && p.sku === '' && p.candidates.length === 0 && p.why === 'No free SKU for this design', 'every candidate taken: ' + JSON.stringify(p.why));
    master.clear();
    // a look-alike twin in the master ("FOXHEAD4821" for FOX_HEAD_4821)
    twins.set(O.looseKey('FOXHEAD4821'), 'FOXHEAD4821'); p = Sku.plan(base); eq(p.sku, 'FOX_HEAD_64821', 'a twin that reads the same without spacing and punctuation'); eq(p.taken[0].by, 'master', 'twin counts as the master');
    twins.clear();
    // an alias: key, value, whole-listing answer, huggie, a pair, any listing
    for (const [al, why] of [
      [{ '555': { bySku: { 'FOX_HEAD_4821': 'ZED' } } }, 'a bySku key of another listing'],
      [{ '555': { bySku: { 'OLD': 'fox head 4821' } } }, 'a bySku value (looser spelling)'],
      [{ '555': { sku: 'FOX_HEAD_4821', v: 2 } }, 'a whole-listing answer'], [{ '555': { huggie: 'FOX_HEAD_4821' } }, 'a huggie answer'],
      [{ '555': { pairBySku: { 'X': { L: 'FOX_HEAD_4821', R: 'Q' } } } }, 'a pair'], [{ [LID]: { bySku: { 'FOX_HEAD_4821': 'Z' } } }, 'this listing'],
      [{ '555': { bySku: { 'FOX&#95;HEAD_4821': 'Z' } } }, 'an entity inside a key'], [{ '555': { keep: { sku: 'FOX_HEAD_4821' } } }, 'a design saved before']
    ]) { p = Sku.plan(Object.assign({}, base, { aliases: al })); eq(p.sku, 'FOX_HEAD_64821', 'taken by an alias: ' + why); eq(p.taken[0].by, 'alias', why); }
    p = Sku.plan(Object.assign({}, base, { aliases: { '555': { sku: 'OTHER_1111' }, '556': null, '557': 'x', '558': { bySku: null } } })); eq(p.sku, 'FOX_HEAD_4821', 'unrelated and odd aliases change nothing');
    // a product of the listing already carries it (the cached table), outside the group too
    const table = { at: 1, n: 3, products: [{ id: '1', sku: 'OLD', d: 0, pv: [['513', '1']] }, { id: '2', sku: 'FOX_HEAD_4821', d: 0, pv: [['513', '2']] }, { id: '3', sku: 'OLD', d: 0, pv: [['513', '3']] }] };
    p = Sku.plan(Object.assign({}, base, { table, line: { sku: 'OLD', productId: '1' } })); eq(p.sku, 'FOX_HEAD_64821', 'a sibling product of the listing holds TAIL4'); eq(p.taken[0].by, 'listing', 'listing');
    p = Sku.plan(Object.assign({}, base, { table: { at: 1, n: 3, uni: 'fox head 4821' } })); eq(p.sku, 'FOX_HEAD_64821', 'the listing-wide SKU of a {uni} table, compared loosely');
    p = Sku.plan(Object.assign({}, base, { table: { at: 1, n: 2, products: [{ id: '9', sku: 'FOX_HEAD_4821', d: 1, pv: [] }] } })); eq(p.sku, 'FOX_HEAD_64821', 'a deleted product still holds its SKU');
    p = Sku.plan(Object.assign({}, base, { table: { gone: true, at: 1, n: 0 }, aliases: null, hasMaster: null, hasLoose: undefined })); eq(p.sku, 'FOX_HEAD_4821', 'a gone listing, no aliases, no master hooks');
    // no listing id
    p = Sku.plan(Object.assign({}, base, { listingId: '', line: {} })); ok(!p.ok && /listing has no id/.test(p.why), 'no listing id'); p = Sku.plan(Object.assign({}, base, { listingId: '', line: { listingId: '1096264821' } })); eq(p.sku, 'FOX_HEAD_4821', 'the id is the line\'s when none is given');
    p = Sku.plan(); ok(!p.ok, 'plan() with nothing does not throw');
    // a reserved name: the plan hands out CHARM_CUSTOM_LEAF_4821
    p = Sku.plan(Object.assign({}, base, { file: 'Custom leaf.ai' })); eq(p.sku, 'CHARM_CUSTOM_LEAF_4821', 'reserved name'); eq(p.name, 'CUSTOM_LEAF', 'the NAME itself is unchanged');
  }

  /* ═══ 6 · group ═══ */
  {
    const prod = (id, sku, pv, d) => ({ id: String(id), sku, d: d ? 1 : 0, pv: pv || [] });
    const G = (t, l) => Sku.group(t, l);
    let g = G({ at: 1, n: 1, products: [prod(11, 'OLD')] }, { productId: '11' }); eq([g.mode, g.productIds, g.sku, g.boughtId], ['all', ['11'], 'OLD', '11'], 'one product');
    const many = Array.from({ length: 32 }, (_, i) => prod(100 + i, 'ACORN&#39;S LEAF', [['513', String(1000 + i)]]));
    g = G({ at: 1, n: 32, products: many }, { productId: '117' }); eq([g.mode, g.productIds.length, g.sku], ['all', 32, "ACORN'S LEAF"], '32 products sharing one SKU: all, and the SKU is read decoded');
    const distinct = Array.from({ length: 5 }, (_, i) => prod(200 + i, 'S' + i));
    g = G({ at: 1, n: 5, products: distinct }, { productId: '202' }); eq([g.mode, g.productIds], ['some', ['202']], 'distinct SKUs: only the bought one');
    const subset = [prod(1, 'A'), prod(2, 'SHARED'), prod(3, 'SHARED'), prod(4, 'B'), prod(5, 'SHARED')];
    g = G({ at: 1, n: 5, products: subset }, { productId: '3' }); eq([g.mode, g.productIds], ['some', ['2', '3', '5']], 'a subset sharing one SKU');
    g = G({ at: 1, n: 5, products: [prod(1, 'A'), prod(2, 'A'), prod(3, 'A', [], true)] }, { productId: '1' }); eq([g.mode, g.productIds, g.live], ['all', ['1', '2'], 2], 'a deleted product is not live, and is not in the group');
    g = G({ at: 1, n: 3, products: [prod(1, 'A'), prod(2, 'A', [], true)] }, { productId: '2' }); eq(g.mode, 'unknown', 'the product bought is deleted: unknown');
    g = G({ at: 1, n: 3, products: [prod(1, ''), prod(2, ''), prod(3, ''), prod(4, 'X'), prod(5, 'X')] }, { productId: '1' }); eq([g.mode, g.productIds, g.sku], ['some', ['1', '2', '3'], ''], 'an empty SKU equals an empty SKU');
    g = G({ at: 1, n: 2, products: [prod(1, ' old '), prod(2, 'OLD')] }, { productId: '1' }); eq([g.mode, g.productIds.length], ['all', 2], 'case and spaces do not matter');
    g = G(null, { productId: '1' }); eq([g.mode, g.productIds], ['unknown', []], 'no table'); g = G({ gone: true }, {}); eq(g.mode, 'unknown', 'a listing Etsy no longer has'); g = G({ at: 1, n: 0 }, {}); eq(g.mode, 'unknown', 'a table of neither kind'); g = G(undefined, undefined); eq(g.mode, 'unknown', 'nothing at all');
    g = G({ at: 1, n: 40, uni: 'ONE' }, { productId: '1' }); eq([g.mode, g.productIds, g.sku], ['all', [], 'ONE'], 'a {uni} table: every product has the one SKU');
    g = G({ at: 1, n: 40, uni: '' }, {}); eq([g.mode, g.sku], ['all', ''], 'a {uni} table with no SKU at all');
    // the bought product by its option pairs when the transaction has no product id
    const opt = [prod(1, 'A', [['513', '10'], ['514', '20']]), prod(2, 'B', [['513', '10'], ['514', '21']]), prod(3, 'C', [['513', '11'], ['514', '20']])];
    const lineOf = (...pairs) => ({ variations: pairs.map(([p, v]) => ({ propertyId: p, valueId: v })) });
    g = G({ at: 1, n: 3, products: opt }, lineOf(['514', '21'], ['513', '10'])); eq([g.mode, g.productIds], ['some', ['2']], 'by the option pairs, in any order');
    g = G({ at: 1, n: 3, products: opt }, lineOf(['513', '10'])); eq(g.mode, 'unknown', 'one pair names two products: unknown');
    g = G({ at: 1, n: 3, products: opt }, lineOf(['513', '99'], ['514', '20'])); eq(g.mode, 'unknown', 'no product has these pairs: unknown');
    g = G({ at: 1, n: 3, products: opt }, { variations: [{ name: 'Note', value: 'x' }] }); eq(g.mode, 'unknown', 'a line with no ids: unknown');
    g = G({ at: 1, n: 3, products: opt }, { productId: '777', variations: lineOf(['513', '11'], ['514', '20']).variations }); eq([g.mode, g.productIds], ['some', ['3']], 'a product id the table does not hold: the option pairs decide');
    g = G({ at: 1, n: 3, products: opt.map(p => Object.assign({}, p, { pv: p.pv.map(a => a.join(':')) })) }, lineOf(['513', '11'], ['514', '20'])); eq(g.productIds, ['3'], 'a table as stored ("513:11") reads the same');
    g = G({ at: 1, n: 2, products: [prod(1, 'A', [['513', '10']]), prod(2, 'A', [['513', '10']])] }, lineOf(['513', '10'])); eq(g.mode, 'unknown', 'two products with the same pairs: unknown');
    // the mode never changes the SKU text
    const same = ['all', 'some', 'unknown'].map((m, i) => Sku.plan({ file: 'Fox.ai', title: 't', listingId: LID, line: { sku: 'OLD', productId: '3' }, table: [{ at: 1, n: 5, products: subset }, { at: 1, n: 5, products: distinct.map(p => Object.assign({}, p, { id: p.id })) }, null][i] }).sku);
    eq(new Set(same).size, 1, 'the SKU is the same in every mode');
  }

  /* ═══ 7 · aliasWorks ═══ */
  eq([Sku.aliasWorks('CUSTOM_6673', 'FOX_4821'), Sku.aliasWorks('CUSTOM-N-001-665441', 'FOX_4821'), Sku.aliasWorks('cust_1', 'FOX_4821'), Sku.aliasWorks('CSTM-9', 'FOX_4821')], [false, false, false, false], 'a custom SKU keeps the line a custom order');
  eq([Sku.aliasWorks('', 'FOX_4821'), Sku.aliasWorks(null, 'FOX_4821'), Sku.aliasWorks("ACORN&#39;S LEAF", 'FOX_4821'), Sku.aliasWorks('FOX HEAD', 'FOX_4821')], [true, true, true, true], 'an empty or ordinary SKU: an alias answers');
  eq(Sku.aliasWorks('RE_5460', 'FOX_4821'), true, 'a rework-looking SKU yields once the master holds the new design (specialOf skips the loose rules for a known SKU)');
  eq(Sku.plan({ file: 'Test_Charm.ai', title: 'Custom charm', listingId: LID, line: { sku: 'CUSTOM_6673' } }).aliasWorks, false, 'plan: aliasWorks false for CUSTOM_6673');
  eq(Sku.plan({ file: 'Test_Charm.ai', title: 'Custom charm', listingId: LID, line: { sku: "ACORN&#39;S LEAF" } }).aliasWorks, true, 'plan: aliasWorks true for ACORN\'S LEAF');
  // the real resolution, end to end, with the alias the repository step writes
  {
    const NEW = Sku.plan({ file: 'Test_Charm.ai', title: 'x', listingId: LID, line: { sku: "ACORN'S LEAF" } }).sku, held = new Map([[NEW, { sku: NEW }]]), me = s => held.get(s) || null;
    const order = { receiptId: '4196630529', updateTs: 1, staffNote: '', buyerMessage: '' };
    const lineA = { transactionId: '41966305291', listingId: LID, sku: "ACORN'S LEAF", title: 'Acorn necklace', quantity: 1, metalKey: 'gold', metalLabel: 'GF', personalization: [], variations: [] };
    const al = { [LID]: { bySku: { "ACORN'S LEAF": NEW } } };
    eq(O.resolveSku(lineA, al, me, {}, () => '').sku, NEW, 'an order still carrying the old SKU resolves to the new one through the alias');
    const lineB = Object.assign({}, lineA, { sku: NEW });
    const sp = O.interpretLine(order, lineB, { optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: me, masterLoose: () => '' });
    ok(sp.designSku === NEW && sp.skuSource === 'transaction' && !sp.special, 'a later order whose transaction carries the new SKU: ' + NEW + ' ' + sp.skuSource);
  }

  /* ═══ 8 · the op ═══ */
  {
    const counted = () => { const c = { reads: 0 }; const mk = (cn, id) => { const r = Fk.db.collection(cn).doc(id); return Object.assign({}, r, { get: async () => { c.reads++; return r.get(); } }); };
      c.db = { collection: cn => ({ doc: id => mk(cn, id) }), getAll: (...refs) => { const mask = refs.length && typeof refs[refs.length - 1].get !== 'function'; c.reads += refs.length - (mask ? 1 : 0); return Fk.db.getAll(...refs); } }; return c; };
    const run = async (body, c = counted()) => ({ out: await Check.check(c.db, body), reads: c.reads });
    const FREE = ['FOX_4821', 'FOX_64821', 'FOX_264821', 'FOX_264821_A', 'FOX_264821_B', 'FOX_264821_C'];
    const w0 = writes.length, f0 = fileWrites.length;

    let r = await run({ listingId: LID, candidates: FREE });
    eq(r.out, { ok: true, free: 'FOX_4821', taken: [], checked: { master: true, alias: true, listing: 'none' } }, 'nothing held: the first is free'); eq(r.reads, 6 + 2, 'six candidates: 6 master documents, the listing\'s alias, its cached table');
    store.set('Charm_Master_Index/FOX_4821', { sku: 'FOX_4821', masterHash: 'h' });
    r = await run({ listingId: LID, candidates: FREE }); eq([r.out.free, r.out.taken], ['FOX_64821', [{ sku: 'FOX_4821', by: 'master', detail: 'already a master design' }]], 'taken by the master (exact document id)');
    store.set('Charm_Sku_Aliases/' + LID, { listingId: LID, bySku: { 'FOX_64821': 'ZED', 'OLD': 'ZED' }, sku: 'fox 264821', v: 2, title: 'Fox' });
    r = await run({ listingId: LID, candidates: FREE }); eq(r.out.free, 'FOX_264821_A', 'alias key and (looser) whole-listing value'); eq(r.out.taken.map(t => [t.sku, t.by]), [['FOX_4821', 'master'], ['FOX_64821', 'alias'], ['FOX_264821', 'alias']], 'by whom');
    store.set('Charm_Listing_Skus/' + LID, { at: 1, n: 2, products: [{ id: '1', sku: 'OLD', d: 0, pv: ['513:1'] }, { id: '2', sku: 'FOX_264821_A', d: 0, pv: ['513:2'] }], nm: [], pn: [] });
    r = await run({ listingId: LID, candidates: FREE }); eq(r.out.free, 'FOX_264821_B', 'a product of the listing holds _A (cached table, every product)'); eq(r.out.checked.listing, 'cache', 'the table was found'); eq(r.out.taken[3], { sku: 'FOX_264821_A', by: 'listing', detail: 'the SKU of a product of this listing' }, 'listing');
    store.set('Charm_Listing_Skus/' + LID, { at: 1, n: 50, uni: 'FOX_264821_B' });
    r = await run({ listingId: LID, candidates: FREE }); eq([r.out.free, r.out.taken.find(t => t.sku === 'FOX_264821_B').by], ['FOX_264821_A', 'listing'], 'a {uni} table: the listing\'s one SKU');
    store.set('Charm_Listing_Skus/' + LID, { at: 1, n: 0, gone: true }); r = await run({ listingId: LID, candidates: FREE }); eq(r.out.checked.listing, 'none', 'a listing Etsy no longer has is no table');
    store.delete('Charm_Listing_Skus/' + LID);
    // every one taken
    for (const s of FREE) store.set('Charm_Master_Index/' + s, { sku: s });
    r = await run({ listingId: LID, candidates: FREE }); eq([r.out.ok, r.out.free, r.out.taken.length, r.out.extended], [true, 'FOX_264821_D', 6, true], 'all taken: the order goes on with the next letter (KEEP K8: "No free SKU" cannot be reached)');
    for (const s of FREE) store.delete('Charm_Master_Index/' + s);
    store.delete('Charm_Sku_Aliases/' + LID);
    // the rule, per candidate: never a document id for what the rule refuses
    r = await run({ listingId: LID, candidates: ['CUSTOM_6673', 'FOX-12', 'A/B', 'X'.repeat(80), '', null, 7, 'FOX HEAD', 'FOX_4821'] });
    eq(r.out.free, 'FOX_4821', 'refused shapes are skipped, the first good one is free'); ok(r.out.taken.length === 6 && r.out.taken.every(t => t.by === 'rule'), 'by rule (the empty, null and number candidates are one empty one): ' + JSON.stringify(r.out.taken.map(t => t.by))); eq(r.reads, 1 + 2, 'only the allowed candidate became a document id (1 master + alias + table)');
    r = await run({ listingId: LID, candidates: ['A/B', '../x', 'a b'] }); eq([r.out.free, r.reads], ['', 2], 'no allowed candidate: no master read at all');
    r = await run({ listingId: LID, candidates: ['fox_4821', ' FOX_4821 ', 'Fox_4821'] }); eq([r.out.free, r.out.taken.length, r.reads], ['FOX_4821', 0, 3], 'case and spaces: one candidate, once');
    // odd listing ids and malformed requests: HTTP 400 shape ({ error }) and not one read
    for (const bad of ['12/34', '../x', 'abc', '', null, undefined, '12', '1234567890123456789012', '123 456', '12.5', { a: 1 }, [1, 2, 3]]) { r = await run({ listingId: bad, candidates: FREE }); ok(r.out.error && !r.out.ok && r.reads === 0, 'a bad listing id is refused, nothing read: ' + JSON.stringify(bad)); }
    for (const bad of [undefined, null, [], 'FOX_4821', { 0: 'x' }, Array.from({ length: 10 }, (_, i) => 'FOX_' + i + '4821')]) { r = await run({ listingId: LID, candidates: bad }); ok(r.out.error && r.reads === 0, 'bad candidates are refused, nothing read'); }
    r = await run(undefined); ok(r.out.error && r.reads === 0, 'no body'); r = await run({ listingId: ' ' + LID + ' ', candidates: [FREE[0]] }); eq(r.out.free, 'FOX_4821', 'a listing id with spaces around it is the id'); r = await run({ listingId: 1096264821, candidates: [FREE[0]] }); eq(r.out.free, 'FOX_4821', 'a number is an id');
    // nine candidates is the most: at most 12 reads, no scan (the stand-in has no collection query at all)
    const nine = Sku.candidates(LID, 'FOX').slice(0, 9); r = await run({ listingId: LID, candidates: nine, sandbox: true }); ok(r.reads <= 12, 'at most 12 reads: ' + r.reads); eq(r.reads, 9 + 3, 'nine candidates in the sandbox: 9 + the alias, the sandbox\'s alias, the table');
    // the sandbox reads its own alias copy too; production does not
    store.set('Sandbox_Charm_Sku_Aliases/' + LID, { listingId: LID, bySku: { 'OLD': 'FOX_4821' } });
    r = await run({ listingId: LID, candidates: FREE }); eq(r.out.free, 'FOX_4821', 'production ignores the sandbox\'s copy'); r = await run({ listingId: LID, candidates: FREE, sandbox: true }); eq([r.out.free, r.out.taken[0].by], ['FOX_64821', 'alias'], 'the sandbox sees its own answer'); store.delete('Sandbox_Charm_Sku_Aliases/' + LID);

    // plan and check agree: what the page proposes is free in the cloud
    for (const f of ['Test_Charm.ai', 'ACORN&#39;S LEAF.ai', 'custom leaf.ai', 'Fox head FINAL v2.ai']) { const p = Sku.plan({ file: f, title: 'T', listingId: LID, line: {} }); r = await run({ listingId: LID, candidates: p.candidates }); eq(r.out.free, p.sku, 'the cloud agrees with the page on ' + f); }

    // through the library's HTTP door: wired, answers 200, and writes nothing at all (not even the placement counter an unlisted read would raise)
    const post = async b => { const x = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b), queryStringParameters: {} }); return { status: x.statusCode, body: JSON.parse(x.body || '{}') }; };
    ok(typeof lib.ops.customSkuCheck === 'function', 'customSkuCheck is in the library\'s OPS');
    store.set('Charm_Master_Index/TEST_CHARM_4821', { sku: 'TEST_CHARM_4821' });
    let h = await post({ op: 'customSkuCheck', listingId: LID, candidates: ['TEST_CHARM_4821', 'TEST_CHARM_64821'] }); eq([h.status, h.body.free, h.body.taken.length], [200, 'TEST_CHARM_64821', 1], 'through the handler');
    h = await post({ op: 'customSkuCheck', listingId: LID, candidates: ['TEST_CHARM_4821', 'TEST_CHARM_64821'], sandbox: true }); eq([h.status, h.body.free], [200, 'TEST_CHARM_64821'], 'in the sandbox too (it only reads)');
    h = await post({ op: 'customSkuCheck', listingId: 'x/y', candidates: ['A_4821'] }); eq([h.status, !!h.body.error], [400, true], 'a malformed request is HTTP 400 with { error }, as every op answers it');
    h = await post({ op: 'customSkuCheck', sandbox: true }); eq(h.status, 400, 'bare');
    store.delete('Charm_Master_Index/TEST_CHARM_4821');
    eq(writes.length - w0, 0, 'not one Firestore write in the whole run (the write log is empty)'); eq(fileWrites.length - f0, 0, 'not one Storage write'); eq(fetched, 0, 'no network call at all, so no Etsy call');
    eq(etsyLoaded().length, etsyBefore, 'the Etsy modules were not even loaded');
    const src = fs.readFileSync(path.join(fnDir, '_charmNestSkuCheck.js'), 'utf8').replace(/\/\*[\s\S]*?\*\//, '');
    ok(!/\.(?:set|update|create|delete)\(|\.add\(\{|batch\(|runTransaction|fetch|etsy|\.get\(\)|\.where\(|\.limit\(|\.orderBy\(/i.test(src.replace(/\/\/.*$/gm, '')), 'the check\'s source has no write, no query and no Etsy');
    const skuSrc = fs.readFileSync(path.join(root, 'charm-nest-sku.js'), 'utf8').replace(/\/\*[\s\S]*?\*\//, '');
    ok(!/fetch|XMLHttpRequest|document\.|window\.|Date\.now|Math\.random|localStorage/.test(skuSrc), 'the module is pure: no DOM, no network, no clock, no randomness');
  }

  /* ═══ the manifests ═══ */
  {
    const entries = JSON.parse(fs.readFileSync(path.join(root, 'scripts/netlify-function-entries.json'), 'utf8'));
    ok(entries.modules.includes('_charmNestSkuCheck.js') && !entries.endpoints.includes('_charmNestSkuCheck.js'), 'the check is a helper module of the function manifest, not an endpoint');
    ok(/"charm-nest-sku\.js"/.test(fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8')), 'charm-nest-sku.js is in the public asset list (the page can load it)');
  }
  console.warn = warn;
  console.log(`save-sku: ${checks} checks passed`);
})().catch(e => { console.warn = warn; console.error(e && e.stack || e); process.exit(1); });
