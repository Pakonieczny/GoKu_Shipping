// NOTITLE (Paul, 12 Oct 2026 20:20 UTC, about the feature that matched a listing with no SKU by the words of its TITLE): "I did not ask for this! If there is no exact match, then there's no match.
// You can't fabricate matches. This will screw everything up." The feature was removed. THE RULE, tested here: a line with no exact match (no SKU on the transaction, no SKU in Etsy's list for the
// product, no saved link for the listing, no option's pick) is UNMATCHED, whatever its title says. Nothing is guessed from the title: no automatic match, no ranked candidates built from title
// similarity, no "matched by the listing's title", no "Use another charm", no "Tried: ..." line. The plain old question stays ("no SKU on the transaction and no alias for the listing") with the
// search box where a PERSON picks the charm. What is exact (a real SKU, the Etsy list's SKU, a saved link, an option's pick, a person's pick through the search box) resolves exactly as before.
//   node tests/charm-nest/no-title-match.cjs [playwright-core dir]
// Part 1 (node, offline, neutral made-up designs and listings: no buyer, no real order). Part 2 (Chromium, the real sorter on the fake site: in memory, no network, no Etsy, no AI): the Review row, the card, the
// order window and the writes of an unmatched line; the search box saves the person's pick once.
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const step = name => console.log('  ✓', name);

/* ── the world: a library is a Map of entries (one design file under each SKU), read the way the page reads it ── */
const E = (sku, file, w, h) => ({ sku, aiPath: 'charmnest/master/' + file + '.ai', widthPt: w || 28, heightPt: h || 30, members: 1, sizes: null, blocked: null });
const ENTRIES = [E('FOXGLOVE_100', 'FOXGLOVE_100'), E('TULIP_200', 'TULIP_200'), E('TULIP_201', 'TULIP_201'), E('MOON_300', 'MOON_300'), E('OTTER_400', 'OTTER_400'), E('OTTER_400 (HUGGIE)', 'OTTER_400_HUGGIE_'), E('CO-GOLDCHARMO', 'CO_GOLDCHARMO')];
const lib = new Map(ENTRIES.map(e => [e.sku.toUpperCase(), e])), keys = [...lib.keys()], loose = {};
for (const k of keys) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const byFile = new Map(); for (const [k, e] of lib) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
const entryFor = s => lib.get(String(s || '').toUpperCase()) || null, sharedFor = s => { const e = entryFor(s); return e && e.aiPath ? byFile.get(e.aiPath) || [] : []; };
// (the context the bridge hands the reader, the library read WHOLE: every extra the removed feature used to ask for is there, so a title match, if code still made one, could happen)
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: s => loose[O.looseKey(s)] || '',
  masterShared: sharedFor, masterSkus: () => keys, masterKey: lib, masterSize: lib.size, masterReady: true }, extra || {});

const LID = '7000000001', PRODUCT = '9000000001';
let tx = 7100000000;
const line = (title, sku, over) => Object.assign({ transactionId: String(++tx), listingId: LID, sku, title, quantity: 1, productId: PRODUCT, metalKey: 'gold', metalLabel: 'Gold', personalization: ['A first name'], buyerMessage: '',
  variations: [['Metal Choice', 'Gold'], ['Necklace Length in inches', '14&quot;']].map(([name, value]) => ({ name, value })) }, over || {});
const read = (l, extra) => O.interpretLine({ receiptId: 'R' + l.transactionId, updateTs: 1 }, JSON.parse(JSON.stringify(l)), ctx(extra));
const asked = sp => (sp.problems || []).find(p => p.kind === 'unmatchedSku');
const BLANK_TABLE = { at: 1, n: 1, products: [{ id: PRODUCT, sku: '', d: 0, pv: [] }], names: {}, props: {} };   // (Etsy's inventory of the listing, READ: the product bought has no SKU)
const FEATURE = /matched by the listing|Use another charm|Tried:|Matched by title|title match/i;

/* ── 1 · a line with no exact match stays unmatched, whatever its title says ── */
function unmatched() {
  const TITLES = ['Foxglove Charm Necklace', 'Foxglove Charm Necklace Gold Personalized Gift for Her', 'Tulip Charm Necklace', 'Moon Charm Necklace', 'Otter Charm Necklace', 'Foxglove Flower Necklace Dainty Gold'];
  for (const title of TITLES) for (const table of [null, BLANK_TABLE]) {
    const l = line(title, ''), sp = read(l, table ? { listingSkus: { [LID]: table } } : {}), p = asked(sp), where = JSON.stringify(title) + (table ? ' (Etsy list read, blank)' : ' (Etsy list not read)');
    ok(sp.designSku === null && sp.skuSource === null, 'no design, no source: ' + where + ' → ' + sp.designSku + '/' + sp.skuSource);
    ok(!sp.titleMatch && !('nosku' in (p || {})), 'no title match and no ranked candidates on the problem: ' + where);
    eq(sp.problems.map(x => x.kind), ['unmatchedSku'], 'one question, the plain one: ' + where);
    eq(p.reason, 'no SKU on the transaction and no alias for the listing', 'the old plain reason, word for word: ' + where);
    ok(!FEATURE.test(JSON.stringify(sp)) && !/candidates|titleMatch|nosku/i.test(JSON.stringify(sp)), 'nothing of the removed feature anywhere in the reading: ' + where);
  }
  // the readers' own words: nothing that the feature exported is left on the reader
  for (const name of ['noSkuRead', 'noSkuLine', 'noSkuShort', 'titleIndexFor', 'buildTitleIndex', 'designWordsOf']) ok(O[name] === undefined, name + ' is gone from the reader\'s exports');
  // a title that names exactly ONE drawing, the library whole, the form known (a necklace by its options and title): still nothing
  { const sp = read(line('Foxglove Charm Necklace', '')); ok(sp.designSku === null, 'one drawing named, the form known, still unmatched'); }
  // a Huggie CHARM SET with no SKU keeps its own old question
  { const sp = read(line('Otter Huggie Hoop Earrings Charm Set', '', { variations: [['Metal Choice', 'Gold'], ['Charm Type', 'Huggie Charm Set']].map(([name, value]) => ({ name, value })) })); const p = asked(sp);
    ok(!sp.titleMatch && !FEATURE.test(JSON.stringify(sp)), 'a no-SKU huggie set is not matched by its title either'); if (p) ok(!/Tried|candidates/i.test(JSON.stringify(p)), 'and carries no candidates'); }
  step('a line with no exact match is unmatched whatever its title says: no match, no candidates, the old plain reason (' + TITLES.length * 2 + ' readings)');
}

/* ── 2 · what is exact still resolves, exactly as before ── */
function exact() {
  // a real SKU on the transaction
  { const sp = read(line('Anything At All Necklace', 'FOXGLOVE_100')); eq([sp.designSku, sp.skuSource, sp.problems.length], ['FOXGLOVE_100', 'transaction', 0], 'a real SKU resolves'); }
  { const sp = read(line('Foxglove Charm Necklace', 'tulip_200')); eq([sp.designSku, sp.skuSource], ['TULIP_200', 'transaction'], 'the SKU wins over the title, in any case'); }
  // Etsy's own SKU for the product bought (the listing's inventory table)
  { const t = { at: 1, n: 1, products: [{ id: PRODUCT, sku: 'MOON_300', d: 0, pv: [] }], names: {}, props: {} }, sp = read(line('Foxglove Charm Necklace', ''), { listingSkus: { [LID]: t } });
    eq([sp.designSku, sp.problems.length], ['MOON_300', 0], 'the SKU Etsy lists for the product bought resolves (' + sp.skuSource + ')'); ok(sp.skuSource !== 'title', 'and is not a title match'); }
  // a saved link for the listing (what "Use this charm" saves on a line with no SKU)
  { const sp = read(line('Foxglove Charm Necklace', ''), { aliases: { [LID]: { listingId: LID, sku: 'OTTER_400', v: 2 } } }); eq([sp.designSku, sp.skuSource, sp.problems.length], ['OTTER_400', 'alias', 0], 'a saved link resolves, source alias'); }
  { const sp = read(line('Foxglove Charm Necklace', ''), { aliases: { [LID]: { listingId: LID, sku: 'OTTER_400' } }, listingSkus: { [LID]: BLANK_TABLE } }); eq([sp.designSku, sp.skuSource], ['OTTER_400', 'alias'], 'the saved link beats the title (the title says foxglove, the person said otter)'); }
  // another listing's link is not this listing's
  { const sp = read(line('Foxglove Charm Necklace', ''), { aliases: { '7000000999': { listingId: '7000000999', sku: 'OTTER_400' } } }); eq([sp.designSku, sp.problems.map(x => x.kind)], [null, ['unmatchedSku']], 'another listing\'s link does not apply'); }
  // a saved link keyed by a SKU the transaction carries (the SKU as bought) still beats an exact match of it (DACHMAP)
  { const sp = read(line('Some Necklace', 'FOXGLOVE_100'), { aliases: { [LID]: { listingId: LID, bySku: { FOXGLOVE_100: 'TULIP_200' } } } }); eq([sp.designSku, sp.skuSource], ['TULIP_200', 'alias'], 'a saved link for this listing and SKU beats the exact master match'); }
  // a person's pick for an option
  { const l = line('Foxglove Charm Necklace', '', { variations: [['Metal Choice', 'Gold'], ['Flower', 'Tulip']].map(([name, value]) => ({ name, value })) });
    const sp = read(l, { optionMaps: { [LID]: { Flower: { Tulip: { field: 'design', value: 'TULIP_200' } } } } }); eq([sp.designSku, sp.skuSource, sp.problems.length], ['TULIP_200', 'option', 0], 'an option\'s pick resolves, source option'); }
  step('a real SKU, the Etsy list\'s SKU, a saved link (and one for the SKU as bought), an option\'s pick: all resolve as before');
}

/* ── 3 · the holds that stay ── */
function holds() {
  // the shop-wide charm-only placeholder (CHAMMAP): still held, never a design by itself, never by the title
  { const sp = read(line('Foxglove Charm Necklace', 'CO-GoldCharmO')), p = asked(sp);
    ok(sp.designSku === null && p && p.generic === LID && !JSON.stringify(sp).includes('CO_GOLDCHARMO') && !FEATURE.test(JSON.stringify(sp)), 'the generic placeholder SKU is held: ' + JSON.stringify(p && p.reason)); }
  { const sp = read(line('Foxglove Charm Necklace', 'CO-GoldCharmO'), { aliases: { [LID]: { listingId: LID, bySku: { 'CO-GOLDCHARMO': 'FOXGLOVE_100' } } } }); eq([sp.designSku, sp.skuSource, sp.problems.length], ['FOXGLOVE_100', 'alias', 0], 'the listing\'s saved answer settles the placeholder'); }
  // the lookalike hold (LOOKALIKE) needs the context's masterLike: with no lookalike it reads as before; its own test (lookalike.cjs) covers the hold itself
  { const sp = read(line('Foxglove Charm Necklace', 'FOXGLOVE_100'), { masterLike: s => [String(s).toUpperCase()] }); eq([sp.designSku, sp.problems.length], ['FOXGLOVE_100', 0], 'a design with no lookalike reads as before'); }
  step('the generic-SKU hold and the lookalike reading are untouched by the removal');
}

/* ── the page: Chromium on the fake site ── */
async function browser() {
  const pwDir = process.argv[2] || process.env.PW_DIR || (fs.existsSync('/opt/node22/lib/node_modules/playwright/node_modules') ? '/opt/node22/lib/node_modules/playwright/node_modules' : path.join(root, 'node_modules'));
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return false; }
  const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
  if (!fs.existsSync(chromePath)) { console.log('  – no chrome: the browser checks were not run'); return false; }
  const { start } = require('./bridge-server.cjs');
  const SHIP = Math.floor(Date.UTC(2026, 9, 19, 17) / 1000), DAY = 86400;
  const o = { receiptId: '4100000555', orderNumber: '4100000555', createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Test Buyer' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
    lines: [Object.assign(line('Foxglove Charm Necklace', ''), { transactionId: '9100000555', expectedShipDate: SHIP })] };
  const srv = await start({ receipts: [] }), b = await chromium.launch({ executablePath: chromePath, args: ['--no-sandbox'] });
  try {
    const context = await b.newContext({ viewport: { width: 1440, height: 950 } });
    const PHOTO = '<svg xmlns="http://www.w3.org/2000/svg" width="240" height="240"><rect width="240" height="240" fill="#c9a25a"/><circle cx="120" cy="120" r="34" fill="#4a7a3a"/></svg>';
    await context.route(/\/\.netlify\/functions\/imageProxy/, r => r.fulfill({ status: 200, contentType: 'image/svg+xml', headers: { 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: PHOTO }));
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [], writes = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    page.on('request', rq => { if (/charmNestLibrary/.test(rq.url()) && rq.method() === 'POST') { try { const body = JSON.parse(rq.postData() || '{}'); if (/Put|Decide|Delete|Reopen|Reset|Purge|Update|Apply|Save|Commit/.test(body.op || '')) writes.push(body); } catch (_) {} } });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.OrderWin && CN.S.cloud.ok === true && B.master.loadedAt > 0 && !B.master.loading, null, { timeout: 60000 });
    await page.evaluate(async ({ entries, o, table }) => {
      CustomRead.later = () => {};   // (Claude's reading of lines is never started by a test: nothing is paid for)
      B.master.entries = new Map(entries.map(e => [e.sku, Object.assign({ charmHash: 'h' + e.sku.length, thumbUrl: '/.netlify/functions/imageProxy?url=thumb-' + encodeURIComponent(e.sku), members: 1 }, e)])); B.master.index = { seeded: true }; B.master.loadedAt = Date.now();
      await Orders.loadMaps(true); B.maps.listingSkus = Object.assign({}, B.maps.listingSkus, table);   // (the library is whole, and Etsy's list for the listing is read and blank: everything the removed feature asked for)
      for (const line of o.lines) { const key = CharmNestOrders.lineKey(o, line); const row = { key, order: o, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, { entries: ENTRIES, o, table: { [LID]: BLANK_TABLE } });
    const KEY = await page.evaluate(o => CharmNestOrders.lineKey(o, o.lines[0]), o);
    const st = await page.evaluate(k => { const r = B.orders.byKey.get(k), s = r.spec; return { sku: s.designSku, src: s.skuSource, tm: !!s.titleMatch, problems: r.problems.map(p => p.kind), state: r.state, why: (s.problems[0] || {}).reason || '', items: B.review.items.filter(i => i.kind === 'unmatchedSku').length, pooled: (r.poolIds || []).length }; }, KEY);
    eq(st, { sku: null, src: null, tm: false, problems: ['unmatchedSku'], state: st.state, why: 'no SKU on the transaction and no alias for the listing', items: 1, pooled: 0 }, 'the page reads the line as unmatched, one Review item, nothing pooled: ' + JSON.stringify(st));
    ok(['pulled', 'held', 'unmatched'].includes(st.state), 'it waits (never pooled, nested or written): ' + st.state);
    /* the Review row: the plain reason, the search box, no chips from the title, no feature wording */
    const rowSel = `#rvList .reviewListRow[data-row="${KEY}"]`;
    await page.waitForSelector(rowSel);
    const rv = await page.evaluate(sel => { const n = document.querySelector(sel); return { text: n.textContent.replace(/\s+/g, ' '), chips: n.querySelectorAll('[data-dsku], .sibOffer, .titleOffer, .dsPick').length, reason: (n.querySelector('.reviewReason') || {}).textContent || '' }; }, rowSel);
    ok(rv.chips === 0, 'no one-tap chips of any kind from the title in the Review row'); ok(!FEATURE.test(rv.text), 'no feature wording in the Review row: ' + rv.text.slice(0, 300));
    ok(/no SKU on the transaction and no alias for the listing/.test(rv.reason), 'the row says the plain reason: ' + rv.reason);
    /* the card (what the order window shows under Needs a decision): the search box stays, a person picks */
    await page.evaluate(() => { document.querySelectorAll('#cardHost').forEach(h => h.remove()); const host = document.createElement('div'); host.id = 'cardHost'; document.body.appendChild(host); Review.cardIn(host, B.review.items.find(i => i.kind === 'unmatchedSku')); });
    const card = await page.evaluate(() => { const h = document.getElementById('cardHost'); return { text: h.textContent.replace(/\s+/g, ' '), input: !!h.querySelector('[data-f=sku]'), button: (h.querySelector('[data-a=alias]') || {}).textContent || '', chips: h.querySelectorAll('[data-dsku], .sibOffer, .titleOffer, .dsPick').length }; });
    ok(card.input && card.button === 'Use this charm', 'the card keeps the search box and Use this charm: ' + JSON.stringify({ input: card.input, button: card.button }));
    ok(card.chips === 0 && !FEATURE.test(card.text), 'no chips and no feature wording in the card: ' + card.text.slice(0, 300));
    await page.evaluate(() => document.getElementById('cardHost').remove());
    /* the order window: no SKU, no "matched by", no chip, the old head-card sentence */
    await page.evaluate(k => OrderWin.open(k), KEY);
    await page.waitForFunction(() => OrderWin.isOpen(), null, { timeout: 20000 });
    await page.waitForFunction(() => /Needs a decision/.test(document.getElementById('owNowCard').textContent), null, { timeout: 20000 });
    await page.waitForTimeout(500);
    const win = await page.evaluate(() => ({ sku: document.getElementById('owSku').textContent, head: document.getElementById('owNowCard').textContent.replace(/\s+/g, ' ').trim(), chips: [...document.querySelectorAll('#owMeta .owfChip')].map(x => x.textContent.trim()), all: document.getElementById('owMeta').textContent.replace(/\s+/g, ' ') }));
    ok(!FEATURE.test(win.sku + ' ' + win.head + ' ' + win.chips.join(' | ') + ' ' + win.all), 'the order window never says it: ' + [win.sku, win.head, win.chips.join(' | ')].join(' || '));
    ok(/Needs a decision/.test(win.head) && /no SKU on the transaction and no alias for the listing/.test(win.head), 'the head card gives the old plain sentence: ' + win.head);
    ok(/—|-/.test(win.sku) && !/FOXGLOVE/.test(win.sku), 'the SKU line is empty, not a guess: ' + win.sku);
    await page.click('#owClose');
    /* nothing was written by reading the line; one tap of a PERSON through the search box saves one link and the line resolves */
    eq(writes.length, 0, 'reading the line, the Review row and the order window wrote nothing: ' + JSON.stringify(writes.map(w => w.op)));
    await page.evaluate(() => { document.querySelectorAll('#cardHost').forEach(h => h.remove()); const host = document.createElement('div'); host.id = 'cardHost'; document.body.appendChild(host); Review.cardIn(host, B.review.items.find(i => i.kind === 'unmatchedSku')); });
    await page.evaluate(() => { const f = document.querySelector('#cardHost [data-f=sku]'); f.value = 'OTTER_400'; f.dispatchEvent(new Event('input', { bubbles: true })); f.dispatchEvent(new Event('change', { bubbles: true })); document.querySelector('#cardHost [data-a=alias]').click(); });
    await page.waitForFunction(k => B.orders.byKey.get(k).spec.skuSource === 'alias', KEY, { timeout: 30000 });
    const after = await page.evaluate(k => { const r = B.orders.byKey.get(k); return { sku: r.spec.designSku, src: r.spec.skuSource, tm: !!r.spec.titleMatch }; }, KEY);
    eq(after, { sku: 'OTTER_400', src: 'alias', tm: false }, 'the person\'s pick resolves the line from then on');
    const puts = writes.filter(w => w.op === 'aliasPut'); eq(puts.length, 1, 'one write, the person\'s: ' + JSON.stringify(writes.map(w => w.op))); eq([puts[0].listingId, puts[0].sku], [LID, 'OTTER_400']);
    eq(errors, [], 'no page errors');
    await context.close();
  } finally { await b.close(); srv.close(); }
  console.log('  ✓ Chromium: an unmatched line shows the plain reason and the search box, no chips, no "matched by", no "Use another charm"; reading it writes nothing; a person\'s pick saves one link and resolves it');
  return true;
}

(async () => {
  unmatched(); exact(); holds();
  const ran = await browser();
  console.log((ran ? 'No title match OK:' : 'No title match (node part only, no browser):') + ` ${n} checks. No exact match = no match: a title never picks a charm, offers ranked candidates or says "matched by the listing's title"; what is exact still resolves.`);
})().catch(e => { console.error(e); process.exit(1); });
