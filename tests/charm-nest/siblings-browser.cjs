// SIBLINGS in a real browser (12 Oct 2026, order 4198409468: Etsy SKU "SCHRODINGER CAT" on a necklace of two cats).
//  The Review ROW of an unknown SKU whose numbered designs ("SCHRODINGER CAT 1", "SCHRODINGER CAT 2") sit in the library, on a
//  necklace, shows ONE quiet tap "Use both: X 1 + X 2" with the two thumbnails and each design as a single choice; an earring
//  line and a huggie never get it; one tap saves the pair link for the listing and SKU (Charm_Sku_Aliases/{listing}.pairBySku),
//  the line becomes two pieces of one group and the row clears. Fake data only; the page's server is the in-memory one; no Etsy.
//   node tests/charm-nest/siblings-browser.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOT=<png path> for a picture of the row)
const path = require('path'), assert = require('assert/strict'), fs = require('fs');
const root = path.join(__dirname, '../..');
const svg = inner => `<svg xmlns="http://www.w3.org/2000/svg" width="168" height="168" viewBox="0 0 168 168"><rect width="168" height="168" fill="#ece7dc"/><path d="M30 60 L38 22 L62 44 Q84 36 106 44 L130 22 L138 60 Q146 110 84 134 Q22 110 30 60Z" fill="#f4efe4" stroke="#111" stroke-width="9"/><circle cx="84" cy="44" r="9" fill="none" stroke="#111" stroke-width="8"/>${inner}</svg>`;
const EYES = '<circle cx="62" cy="84" r="13" fill="#2a4bd6"/><circle cx="106" cy="84" r="13" fill="#2a4bd6"/><circle cx="62" cy="84" r="5" fill="#ece7dc"/><circle cx="106" cy="84" r="5" fill="#ece7dc"/>';
const SKULL = '<path d="M46 80 Q84 50 122 80 Q124 108 106 110 L102 120 L66 120 L62 110 Q44 108 46 80Z" fill="none" stroke="#e8433a" stroke-width="9"/><ellipse cx="68" cy="88" rx="10" ry="7" fill="none" stroke="#e8433a" stroke-width="9"/><ellipse cx="100" cy="88" rx="10" ry="7" fill="none" stroke="#e8433a" stroke-width="9"/>';

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const put = (sku, w, h) => st.put('Charm_Master_Index', sku, { sku, masterHash: 'mh-' + sku, widthPt: w, heightPt: h, areaPt2: w * h * .7, members: 3, holes: 1, upAngle: 0, aiPath: 'charmnest/master/' + sku + '.ai', aiUrl: srv.blobUrl('charmnest/master/' + sku + '.ai'), thumbPath: 'charmnest/master/' + sku + '.png', thumbUrl: srv.blobUrl('charmnest/master/' + sku + '.svg') });
  const pic = (sku, inner) => st.blobs.set('charmnest/master/' + sku + '.svg', { buf: Buffer.from(svg(inner)), generation: 1, meta: { contentType: 'image/svg+xml', metadata: { firebaseStorageDownloadTokens: 't' } } });
  for (const [k, w, h, inner] of [['SCHRODINGER CAT 1', 29.4, 28.3, EYES], ['SCHRODINGER CAT 2', 31.1, 29.2, SKULL], ['SCHRODINGER CAT (HUGGIE)', 20.1, 24.4, EYES], ['PLAIN STUD', 12, 12, EYES], ['GRAD CAP 1 (HUGGIE)', 18, 18, EYES], ['GRAD CAP 2', 24, 24, SKULL]]) { put(k, w, h); pic(k, inner); }
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  let failed = 0, n = 0;
  const check = async (name, fn) => { n++; try { await fn(); console.log('  ✓ ' + name); } catch (e) { failed++; console.log('  ✗ ' + name + ': ' + String(e.message).split('\n')[0]); } };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(s => { try { localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} window.prompt = () => 'Test Operator'; }, { v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off' });
    const page = await context.newPage(); page.setDefaultTimeout(30000);
    const errors = []; page.on('pageerror', e => errors.push(e.message));
    const etsy = []; page.on('request', q => { if (/etsy/i.test(q.url())) etsy.push(q.method() + ' ' + q.url()); });
    const dialogs = []; page.on('dialog', d => { dialogs.push(d.type()); d.dismiss().catch(() => {}); });
    await page.goto(`${sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && CN.S.cloud.ok === true, null, { timeout: 60000 });
    // four fake lines: the necklace of the complaint, the same SKU as an earring, a necklace of one numbered family with a single design, and a necklace that has a plain design of its own
    const made = await page.evaluate(async () => {
      await Orders.loadMaps(true);
      const mk = (rid, tid, lid, sku, title, vars) => { const order = { receiptId: rid, orderNumber: rid, createTs: 1789000000, updateTs: 1789000000, shipBy: 1790000000, buyer: { name: 'Test Buyer' }, buyerMessage: '', staffNote: '', messages: [], lines: [{ transactionId: tid, listingId: lid, sku, title, quantity: 1, variations: vars, metalKey: 'silver', metalLabel: 'Silver', personalization: [], buyerMessage: '' }] };
        const line = order.lines[0], key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); return row.key; };
      const neck = [{ name: 'Metal Choice', value: 'Silver' }, { name: 'Necklace Length in inches', value: '18"' }];
      const keys = { neck: mk('4198409468', '5300000001', '1190000001', 'SCHRODINGER CAT', 'Schrodinger Cat Necklace, Dainty Cat Charm Necklace', neck),
        ear: mk('4198409469', '5300000002', '1190000002', 'SCHRODINGER CAT', 'Schrodinger Cat Stud Earrings', [{ name: 'Metal Choice', value: 'Silver' }, { name: 'Style', value: 'Studs' }]),
        huggie: mk('4198409470', '5300000003', '1190000003', 'SCHRODINGER CAT', 'Schrodinger Cat Huggie Hoop Earrings', [{ name: 'Metal Choice', value: 'Silver' }]),
        lone: mk('4198409471', '5300000004', '1190000004', 'MOON CHARM', 'Moon Necklace', neck),
        grad: mk('4171756052', '5300000005', '1190000005', 'GRAD CAP 1', 'Graduation Cap Necklace', neck) };
      Orders.interpretAll(); Review.syncOrderItems && Review.syncOrderItems(); CN.setMode('review'); Review.render();
      return keys;
    });
    await page.waitForSelector('#rvList .reviewListRow');
    const rowOf = rid => page.locator(`#rvList .reviewListRow[data-rid="${rid}"]`);
    const nk = rowOf('4198409468'), offer = nk.locator('.sibOffer');
    await check('the necklace row (SKU SCHRODINGER CAT) is in the Review list, once, with the offer', async () => { assert.equal(await nk.count(), 1); assert.equal(await offer.count(), 1); });
    await check('the question is plain: "SCHRODINGER CAT has a 1 and a 2 in the library. Is this necklace both charms?"', async () => assert.equal(await offer.locator('.sibAsk').innerText(), 'SCHRODINGER CAT has a 1 and a 2 in the library. Is this necklace both charms?'));
    await check('ONE "Use both" button first, then each numbered design alone, and never the huggie', async () => assert.deepEqual((await offer.locator('button').allInnerTexts()).map(s => s.trim()), ['Use both: SCHRODINGER CAT 1 + SCHRODINGER CAT 2', 'SCHRODINGER CAT 1', 'SCHRODINGER CAT 2']));
    if (process.env.DEBUGPIC) console.log(JSON.stringify(await offer.locator('img').evaluateAll(l => l.map(i => ({ src: (i.currentSrc || i.src || '').slice(0, 80), done: i.complete, w: i.naturalWidth })))));
    await check('the "Use both" button carries the two designs\' thumbnails (the SKU picker\'s own pictures), loaded', async () => { const h = await page.waitForFunction(() => { const b = document.querySelector('#rvList [data-rid="4198409468"] [data-both]'), im = b && [...b.querySelectorAll('img')]; return im && im.length === 2 && im.every(i => i.complete && i.naturalWidth > 0) ? im.map(i => i.getBoundingClientRect().width) : false; }, null, { timeout: 25000 }); const w = await h.jsonValue(); assert.ok(w.every(x => x >= 18), JSON.stringify(w)); });
    await check('each single choice has its thumbnail', async () => assert.equal(await offer.locator('[data-dsku] img').count(), 2));
    await check('the row keeps its own buttons (Print QR label, Complete Order, Send to Sheet, Hold)', async () => { const own = (await nk.locator('.rowActions').innerText()).replace(/\s+/g, ' '); assert.ok(/Print QR label/.test(own) && /Complete Order/.test(own) && /Send to Sheet/.test(own) && /Hold/.test(own), own); });
    await check('no offer on a stud line or a necklace whose SKU names nothing similar; GRAD CAP 1 (master: only GRAD CAP 1 (HUGGIE) and GRAD CAP 2) has no "Use both" (a correct hold) and, since REVSUGGEST, its one similar charm GRAD CAP 2 as a single tap, never the huggie', async () => { for (const rid of ['4198409469', '4198409471']) { assert.equal(await rowOf(rid).count(), 1, rid + ' is listed'); assert.equal(await rowOf(rid).locator('.sibOffer').count(), 0, rid + ' has an offer'); } const gc = rowOf('4171756052').locator('.sibOffer'); assert.equal(await gc.count(), 1); assert.equal(await gc.locator('[data-both]').count(), 0); assert.deepEqual((await gc.locator('button').allInnerTexts()).map(t => t.trim()), ['GRAD CAP 2']); assert.equal(await page.locator('#rvList .sibOffer').count(), 2, 'two offers in the whole list'); });
    await check('the huggie line (same SKU, so under the necklace\'s one unknown-SKU card) is not offered the pair: the card says it covers 2 orders', async () => { assert.match((await nk.locator('.groupScope').innerText()), /2 orders/); });
    if (process.env.SHOT) { await page.waitForTimeout(600); fs.mkdirSync(path.dirname(process.env.SHOT), { recursive: true }); await nk.screenshot({ path: process.env.SHOT }); }
    // the tap
    await offer.locator('[data-both]').click();
    await page.waitForFunction(() => !document.querySelector('#rvList [data-rid="4198409468"] .sibOffer') || !document.querySelector('#rvList [data-rid="4198409468"]'), null, { timeout: 20000 });
    await page.waitForTimeout(800);
    const al = st.doc('Charm_Sku_Aliases', '1190000001');
    await check('the tap saved the pair link for this listing and SKU, for good', () => { assert.ok(al && al.pairBySku, JSON.stringify(al)); const p = al.pairBySku['SCHRODINGER CAT']; assert.deepEqual([p.L, p.R], ['SCHRODINGER CAT 1', 'SCHRODINGER CAT 2']); assert.ok(p.by && p.at, 'who and when are kept'); });
    await check('only the necklace\'s listing was written: the stud and huggie listings (same SKU) have no link', async () => { for (const lid of ['1190000002', '1190000003', '1190000004']) assert.ok(!(st.doc('Charm_Sku_Aliases', lid) || {}).pairBySku, lid); });
    const after = await page.evaluate(() => { const r = Orders.rows().find(x => x.order.receiptId === '4198409468'); return { problems: r.problems.map(p => p.kind), set: r.spec && r.spec.pair && r.spec.pair.set || null, perUnit: r.spec && r.spec.pair && r.spec.pair.perUnit, pair: r.spec && r.spec.pair ? { sides: r.spec.pair.sides, kind: r.spec.pair.kind, source: r.spec.pair.set && r.spec.pair.set.source } : null, items: Review.items().filter(it => it.row && it.row.order.receiptId === '4198409468').map(it => it.kind) }; });
    await check('the line now reads as the two designs (two pieces of one group), no Left and Right', () => { assert.deepEqual(after.problems, []); assert.deepEqual(after.set && after.set.designs, ['SCHRODINGER CAT 1', 'SCHRODINGER CAT 2']); assert.equal(after.set.source, 'alias'); });
    await check('the row left the Review list (no unknown-SKU question remains for the order)', () => assert.deepEqual(after.items, []));
    await check('the stud and huggie lines are untouched: still asking (the huggie and the lone necklace for their unknown SKU), no design chosen for them', async () => { const left = await page.evaluate(() => Orders.rows().filter(r => ['4198409469', '4198409470', '4198409471', '4171756052'].includes(r.order.receiptId)).map(r => [r.order.receiptId, r.problems.length > 0, r.spec && r.spec.pair && r.spec.pair.set ? 'set' : 'no set'])); assert.deepEqual(left.sort(), [['4171756052', true, 'no set'], ['4198409469', true, 'no set'], ['4198409470', true, 'no set'], ['4198409471', true, 'no set']], JSON.stringify(left)); });
    await check('no confirm or prompt pop-up, no page error, no Etsy call', () => { assert.deepEqual(dialogs, []); assert.deepEqual(errors, []); assert.deepEqual(etsy, []); });
    // the same row again for the listing: a new order of that listing is linked already (no question)
    const again = await page.evaluate(async () => {
      const order = { receiptId: '4198409999', orderNumber: '4198409999', createTs: 1789100000, updateTs: 1789100000, shipBy: 1790000000, buyer: { name: 'Test Buyer' }, buyerMessage: '', staffNote: '', messages: [], lines: [{ transactionId: '5300000099', listingId: '1190000001', sku: 'SCHRODINGER CAT', title: 'Schrodinger Cat Necklace, Dainty Cat Charm Necklace', quantity: 1, variations: [{ name: 'Metal Choice', value: 'Gold' }, { name: 'Necklace Length in inches', value: '16"' }], metalKey: 'gold', metalLabel: 'Gold', personalization: [], buyerMessage: '' }] };
      const line = order.lines[0], key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); Orders.interpretAll();
      return { problems: row.problems.map(p => p.kind), set: row.spec && row.spec.pair && row.spec.pair.set && row.spec.pair.set.designs };
    });
    await check('the next order of that listing is already linked: two designs, no question', () => { assert.deepEqual(again.problems, []); assert.deepEqual(again.set, ['SCHRODINGER CAT 1', 'SCHRODINGER CAT 2']); });
  } finally { await browser.close(); await srv.close(); }
  console.log(`siblings-browser: ${n} checks, ${failed} failed`); process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
