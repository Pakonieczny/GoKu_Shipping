// SIBLINGS with the form guard (12 Oct 2026, order 4198409468): the REAL shape of the library: a plain "SCHRODINGER CAT" that is the HUGGIE drawing (one design file with
//  "SCHRODINGER CAT 2 (HUGGIE)"), plus "SCHRODINGER CAT 1" and "SCHRODINGER CAT 2". The form guard (charm-nest-orders.js, HUGGIEGUARD) holds the necklace line instead of
//  taking the huggie; the held row (problem.siblings, formHold) offers "Use both"; the tap saves the pair link, which wins over the guard from then on; a huggie listing with the same SKU is never linked.
//  Skipped (not failed) on a page without the guard (CharmNestOrders.siblingsOf). Fake data only; the page's server is the in-memory one; no Etsy.
//   node tests/charm-nest/siblings-guard.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOT=<png path> for a picture of the held row)
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
  const put = (sku, w, h, ai) => st.put('Charm_Master_Index', sku, { sku, masterHash: 'mh-' + (ai || sku), widthPt: w, heightPt: h, areaPt2: w * h * .7, members: 3, holes: 1, upAngle: 0, aiPath: 'charmnest/master/' + (ai || sku) + '.ai', aiUrl: srv.blobUrl('charmnest/master/' + (ai || sku) + '.ai'), thumbPath: 'charmnest/master/' + (ai || sku) + '.png', thumbUrl: srv.blobUrl('charmnest/master/' + (ai || sku) + '.svg') });
  const pic = (name, inner) => st.blobs.set('charmnest/master/' + name + '.svg', { buf: Buffer.from(svg(inner)), generation: 1, meta: { contentType: 'image/svg+xml', metadata: { firebaseStorageDownloadTokens: 't' } } });
  put('SCHRODINGER CAT 1', 29.4, 28.3); pic('SCHRODINGER CAT 1', EYES); put('SCHRODINGER CAT 2', 31.1, 29.2); pic('SCHRODINGER CAT 2', SKULL);
  put('SCHRODINGER CAT 2 (HUGGIE)', 21.2, 24.3); pic('SCHRODINGER CAT 2 (HUGGIE)', SKULL); put('SCHRODINGER CAT', 21.2, 24.3, 'SCHRODINGER CAT 2 (HUGGIE)');   // (the plain name holds the huggie drawing)
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  let failed = 0, n = 0;
  const check = async (name, fn) => { n++; try { await fn(); console.log('  ✓ ' + name); } catch (e) { failed++; console.log('  ✗ ' + name + ': ' + String(e.message).split('\n')[0]); } };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(s => { try { localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} window.prompt = () => 'Test Operator'; }, { v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off' });
    const page = await context.newPage(); page.setDefaultTimeout(30000);
    const errors = []; page.on('pageerror', e => errors.push(e.message));
    const etsy = []; page.on('request', q => { if (/etsy/i.test(new URL(q.url()).hostname)) etsy.push(q.method() + ' ' + q.url()); });   // (an Etsy host: the page's own charm-nest-*etsy*.js scripts are not Etsy calls)
    await page.goto(`${sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && CN.S.cloud.ok === true, null, { timeout: 60000 });
    // (the master library is read after the cloud is up: the guard reads its numbered siblings, so the lines are read only once the library holds them; under load it came later and the line was read without them)
    await page.waitForFunction(() => window.Master && !!Master.entryFor('SCHRODINGER CAT 1') && !!Master.entryFor('SCHRODINGER CAT 2'), null, { timeout: 60000 });
    if (!(await page.evaluate(() => typeof CharmNestOrders.siblingsOf === 'function'))) { console.log('  - skipped: this page has no form guard (CharmNestOrders.siblingsOf)'); return; }
    const made = await page.evaluate(async () => {
      await Orders.loadMaps(true);
      const mk = (rid, tid, lid, sku, title, vars) => { const order = { receiptId: rid, orderNumber: rid, createTs: 1789000000, updateTs: 1789000000, shipBy: 1790000000, buyer: { name: 'Test Buyer' }, buyerMessage: '', staffNote: '', messages: [], lines: [{ transactionId: tid, listingId: lid, sku, title, quantity: 1, variations: vars, metalKey: 'silver', metalLabel: 'Silver', personalization: [], buyerMessage: '' }] };
        const line = order.lines[0], key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); return row.key; };
      const neck = [{ name: 'Metal Choice', value: 'Silver' }, { name: 'Necklace Length in inches', value: '18"' }];
      mk('4198409468', '5300000001', '1190000001', 'SCHRODINGER CAT', 'Schrodinger Cat Necklace, Dainty Cat Charm Necklace', neck);
      mk('4198409470', '5300000003', '1190000003', 'SCHRODINGER CAT (HUGGIE)', 'Schrodinger Cat Huggie Hoop Earrings', [{ name: 'Metal Choice', value: 'Silver' }]);
      Orders.interpretAll(); Review.syncOrderItems && Review.syncOrderItems(); CN.setMode('review'); Review.render();
      const r = Orders.rows().find(x => x.order.receiptId === '4198409468');
      return { hold: !!(r.spec && r.spec.formHold), design: r.spec && r.spec.designSku, problems: r.problems.map(p => p.kind), sibs: (r.spec && r.spec.siblings || []).map(x => x.sku) };
    });
    await check('the necklace line is HELD by the form guard (no huggie vector), asking the unknown-SKU question, with its two siblings', () => { assert.equal(made.hold, true, JSON.stringify(made)); assert.equal(made.design, null); assert.deepEqual(made.problems, ['unmatchedSku']); assert.deepEqual(made.sibs, ['SCHRODINGER CAT 1', 'SCHRODINGER CAT 2']); });
    await page.waitForSelector('#rvList .reviewListRow');
    const nk = page.locator('#rvList .reviewListRow[data-rid="4198409468"]'), offer = nk.locator('.sibOffer');
    await check('its Review row offers "Use both" and the two designs, never the huggie', async () => { assert.equal(await offer.count(), 1); assert.deepEqual((await offer.locator('button').allInnerTexts()).map(s => s.trim()), ['Use both: SCHRODINGER CAT 1 + SCHRODINGER CAT 2', 'SCHRODINGER CAT 1', 'SCHRODINGER CAT 2']); });
    await check('the row says why it waits (the guard\'s one plain reason)', async () => { assert.match(await nk.locator('.reviewReason').innerText(), /huggie/i); });
    await page.waitForFunction(() => { const im = [...document.querySelectorAll('#rvList [data-rid="4198409468"] .sibOffer img')]; return im.length === 4 && im.every(i => i.complete && i.naturalWidth > 0); }, null, { timeout: 25000 }).catch(() => {});
    if (process.env.SHOT) { fs.mkdirSync(path.dirname(process.env.SHOT), { recursive: true }); await nk.screenshot({ path: process.env.SHOT }); }
    await offer.locator('[data-both]').click();
    await page.waitForFunction(() => !document.querySelector('#rvList [data-rid="4198409468"]'), null, { timeout: 20000 });
    await page.waitForTimeout(600);
    await check('the tap saved the pair link for this listing and SKU, for good', () => { const al = st.doc('Charm_Sku_Aliases', '1190000001'); const p = al && al.pairBySku && al.pairBySku['SCHRODINGER CAT']; assert.ok(p, JSON.stringify(al)); assert.deepEqual([p.L, p.R], ['SCHRODINGER CAT 1', 'SCHRODINGER CAT 2']); });
    await check('the saved link wins over the guard: two pieces of one group (1 and 2), not held, no huggie', async () => { const a = await page.evaluate(() => { const r = Orders.rows().find(x => x.order.receiptId === '4198409468'); return { hold: !!(r.spec && r.spec.formHold), problems: r.problems.map(p => p.kind), set: r.spec.pair && r.spec.pair.set && r.spec.pair.set.designs, per: r.spec.pair && r.spec.pair.perUnit }; }); assert.deepEqual(a, { hold: false, problems: [], set: ['SCHRODINGER CAT 1', 'SCHRODINGER CAT 2'], per: 2 }); });
    await check('the huggie listing was not linked and no confirm, error or Etsy call happened', async () => { assert.ok(!(st.doc('Charm_Sku_Aliases', '1190000003') || {}).pairBySku); assert.deepEqual(errors, [], JSON.stringify(errors)); assert.deepEqual(etsy, [], JSON.stringify(etsy)); });
    // another listing of the same SKU: still held until a person says (the link is per listing)
    const other = await page.evaluate(async () => { const order = { receiptId: '4198409999', orderNumber: '4198409999', createTs: 1789100000, updateTs: 1789100000, shipBy: 1790000000, buyer: { name: 'Test Buyer' }, buyerMessage: '', staffNote: '', messages: [], lines: [{ transactionId: '5300000099', listingId: '1190000077', sku: 'SCHRODINGER CAT', title: 'Schrodinger Cat Necklace', quantity: 1, variations: [{ name: 'Necklace Length in inches', value: '16"' }], metalKey: 'silver', metalLabel: 'Silver', personalization: [], buyerMessage: '' }] };
      const line = order.lines[0], key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); Orders.interpretAll(); return { hold: !!(row.spec && row.spec.formHold), problems: row.problems.map(p => p.kind) }; });
    await check('a necklace of ANOTHER listing with the same SKU is still held (the link is for this listing)', () => assert.deepEqual(other, { hold: true, problems: ['unmatchedSku'] }));
  } finally { await browser.close(); await srv.close(); }
  console.log(`siblings-guard: ${n} checks, ${failed} failed`); process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
