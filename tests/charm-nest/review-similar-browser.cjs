// REVSUGGEST in a real browser (12 Oct 2026): similar charms for every order in Review, matched or not, offline (the page's own in-memory server), fake neutral data.
//  Paul: "every time an order goes into the review tab it should go through that process ... present ... multiple options that match the current SKU ... If there are no similar matches
//  ... don't show any suggestive charms" and (HARD) nothing is ever chosen for him: no exact match = no match.
//   node tests/charm-nest/review-similar-browser.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SANDBOX=1 for the page in Sandbox mode, SHOT=<png> for a picture of the matched row)
const path = require('path'), assert = require('assert/strict'), fs = require('fs');
const root = path.join(__dirname, '../..'), SANDBOX = process.env.SANDBOX === '1';
const svg = inner => `<svg xmlns="http://www.w3.org/2000/svg" width="168" height="168" viewBox="0 0 168 168"><rect width="168" height="168" fill="#ece7dc"/>${inner}</svg>`;
const SHAPE = (c, r) => `<circle cx="84" cy="84" r="${r}" fill="${c}"/>`;

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const put = (sku, w, h, extra) => st.put('Charm_Master_Index', sku, Object.assign({ sku, masterHash: 'mh-' + sku, widthPt: w, heightPt: h, areaPt2: w * h * .7, members: 3, holes: 1, upAngle: 0, aiPath: 'charmnest/master/' + sku + '.ai', aiUrl: srv.blobUrl('charmnest/master/' + sku + '.ai'), thumbPath: 'charmnest/master/' + sku + '.png', thumbUrl: srv.blobUrl('charmnest/master/' + sku + '.svg') }, extra || {}));
  const pic = (sku, inner) => st.blobs.set('charmnest/master/' + sku + '.svg', { buf: Buffer.from(svg(inner)), generation: 1, meta: { contentType: 'image/svg+xml', metadata: { firebaseStorageDownloadTokens: 't' } } });
  const MASTER = [['AMBER OWL', 22, 24, SHAPE('#c8892b', 50)], ['AMBER OWL 2', 22, 25, SHAPE('#a86a1a', 44)], ['AMBER OWL 5', 21, 24, SHAPE('#8a5512', 38)], ['AMBER OWL (HUGGIE)', 12, 13, SHAPE('#c8892b', 20)], ['AMBER OWL 9', 22, 24, SHAPE('#222', 30), { blocked: 'two files' }],
    ['MOON CHARM', 20, 20, SHAPE('#5b7fa8', 46)], ['NOVA STAR', 18, 18, SHAPE('#d6c53a', 40)], ['NOVA STAR 3', 19, 19, SHAPE('#c9b82a', 36)], ['LUNA OWL 2', 21, 23, SHAPE('#7a5aa8', 40)], ['GOLDCHARMO', 15, 15, SHAPE('#999', 30)]];
  for (const [k, w, h, inner, extra] of MASTER) { put(k, w, h, extra); pic(k, inner); }
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  let failed = 0, n = 0;
  const check = async (name, fn) => { n++; try { await fn(); console.log('  ✓ ' + name); } catch (e) { failed++; console.log('  ✗ ' + name + ': ' + String(e.message).split('\n')[0]); } };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(s => { try { localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} window.prompt = () => 'Test Operator'; },
      Object.assign({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off' }, SANDBOX ? { sandbox: 'on' } : {}));
    const page = await context.newPage(); page.setDefaultTimeout(30000);
    const errors = []; page.on('pageerror', e => errors.push(e.message));
    const etsy = []; page.on('request', q => { let h = ''; try { h = new URL(q.url()).hostname; } catch (_) {} if (/etsy/i.test(h)) etsy.push(q.method() + ' ' + q.url()); });   // (the page's own fake functions are named etsy*, they are on localhost: only a call to Etsy's hosts counts)
    const dialogs = []; page.on('dialog', d => { dialogs.push(d.type()); d.dismiss().catch(() => {}); });
    await page.goto(`${sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && CN.S.cloud.ok === true, null, { timeout: 60000 });
    // the lines: a matched necklace with similar charms, a matched necklace with nothing similar, a two-line order (one similar, one not), an unmatched line with similar charms, an unmatched one with nothing,
    // a line with no SKU, and a generic placeholder. Each Send to Review order is an item of kind reviewMark (charm-nest-review-mark.js shape: rows, row, rid).
    await page.waitForFunction(() => window.B && B.master && B.master.entries && B.master.entries.size >= 10, null, { timeout: 30000 });
    const made = await page.evaluate(async () => {
      await Orders.loadMaps(true);
      const neck = [{ name: 'Metal Choice', value: 'Silver' }, { name: 'Necklace Length in inches', value: '18"' }];
      const mkOrder = (rid, lines) => { const order = { receiptId: rid, orderNumber: rid, createTs: 1789000000, updateTs: 1789000000, shipBy: 1790000000, buyer: { name: 'Test Buyer' }, buyerMessage: '', staffNote: '', messages: [], lines: lines.map(([tid, lid, sku, title]) => ({ transactionId: tid, listingId: lid, sku, title, quantity: 1, variations: neck, metalKey: 'silver', metalLabel: 'Silver', personalization: [], buyerMessage: '' })) };
        return order.lines.map(line => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); return row; }); };
      const a = mkOrder('4100000001', [['5100000001', '1100000001', 'AMBER OWL', 'Amber Owl Necklace']]), b = mkOrder('4100000002', [['5100000002', '1100000002', 'MOON CHARM', 'Moon Necklace']]),
        c = mkOrder('4100000003', [['5100000003', '1100000003', 'NOVA STAR', 'Nova Star Necklace'], ['5100000004', '1100000004', 'MOON CHARM', 'Moon Necklace Two']]),
        d = mkOrder('4100000004', [['5100000005', '1100000005', 'LUNA OWL', 'Luna Owl Necklace']]), e = mkOrder('4100000005', [['5100000006', '1100000006', 'ZZZ UNKNOWN', 'Mystery Necklace']]),
        f = mkOrder('4100000006', [['5100000007', '1100000007', '', 'Plain Necklace']]), g = mkOrder('4100000007', [['5100000008', '1100000008', 'GOLDCHARMO', 'Generic Necklace']]);
      Orders.interpretAll();
      // (the REAL mark: charm-nest-review-mark.js writes the order's one record and the order becomes a Review item of kind reviewMark with a slot for each line)
      for (const rows of [a, b, c, f, g]) { const r = await CharmNestReviewMark.mark(rows[0].order.receiptId, { by: 'Test Operator' }); if (!r.ok) throw new Error('mark failed: ' + JSON.stringify(r)); }
      Review.syncOrderItems && Review.syncOrderItems(); CN.setMode('review'); Review.render();
      return { amber: a[0].key, state: Orders.rows().filter(r => ['4100000001', '4100000004', '4100000005'].includes(r.order.receiptId)).map(r => [r.order.receiptId, r.state, r.spec && r.spec.designSku, r.problems.map(p => p.kind)]) };
    });
    await page.waitForSelector('#rvList .reviewListRow');
    const rowOf = rid => page.locator(`#rvList .reviewListRow[data-rid="${rid}"][data-mkey^="ord:rvm:"]`);
    const unRow = rid => page.locator(`#rvList .reviewListRow[data-rid="${rid}"]:not([data-mkey^="ord:rvm:"])`);
    const btns = loc => loc.locator('button').allInnerTexts().then(l => l.map(s => s.trim()));
    const slot = (rid, k) => rowOf(rid).locator('.rvMarkSlot' + (k ? `[data-row="${k}"]` : ''));
    await check('setup: the amber owl line is matched (exact SKU), the Luna line and the mystery line wait on their SKU', () => { const m = Object.fromEntries(made.state.map(x => [x[0], x])); assert.equal(m['4100000001'][2], 'AMBER OWL'); assert.deepEqual(m['4100000001'][3], []); assert.deepEqual(m['4100000004'][3], ['unmatchedSku']); assert.deepEqual(m['4100000005'][3], ['unmatchedSku']); });

    // 1 · a matched order sent to Review: the suggestions, the matched charm marked
    const amber = rowOf('4100000001'), aOffer = amber.locator('.sibOffer');
    await check('the matched order\'s row is listed once and carries a suggestion area for its line', async () => { assert.equal(await amber.count(), 1); assert.equal(await aOffer.count(), 1); assert.equal(await slot('4100000001').count(), 1); });
    await check('the charm it is matched to (AMBER OWL) is marked and is NOT a button', async () => { const cur = aOffer.locator('[data-cur]'); assert.equal(await cur.count(), 1); assert.equal(await cur.getAttribute('data-cur'), 'AMBER OWL'); assert.equal(await cur.getAttribute('aria-current'), 'true'); assert.equal(await cur.evaluate(x => x.tagName), 'SPAN'); assert.equal(await aOffer.locator('button[data-dsku="AMBER OWL"]').count(), 0); });
    await check('several similar charms are offered as taps: the numbered ones and nothing blocked, no huggie design for a necklace', async () => { const ds = await aOffer.locator('button[data-dsku]').evaluateAll(l => l.map(b => b.dataset.dsku)); assert.deepEqual(ds, ['AMBER OWL 2', 'AMBER OWL 5']); });
    await check('the existing process still speaks first: the numbered pair question and its "Use both" tap', async () => { assert.match(await aOffer.locator('.sibAsk').innerText(), /AMBER OWL has a 2 and a 5 in the library/); assert.equal(await aOffer.locator('[data-both]').count(), 1); });
    if (process.env.DEBUGPIC) console.log(JSON.stringify(await aOffer.locator('img').evaluateAll(l => l.map(i => ({ src: (i.currentSrc || i.src || '').slice(0, 80), done: i.complete, w: i.naturalWidth })))), await aOffer.innerHTML());
    await check('each charm has its thumbnail, loaded once the row is on screen (the SKU picker\'s own pictures, asked for only near the screen)', async () => { await amber.scrollIntoViewIfNeeded(); await page.waitForFunction(() => [...document.querySelectorAll('#rvList [data-rid="4100000001"] .sibOffer img')].length >= 3 && [...document.querySelectorAll('#rvList [data-rid="4100000001"] .sibOffer img')].every(i => i.complete && i.naturalWidth > 0), null, { timeout: 15000 }); });
    if (process.env.SHOT) { await page.waitForTimeout(500); fs.mkdirSync(path.dirname(process.env.SHOT), { recursive: true }); await amber.screenshot({ path: process.env.SHOT }); }

    // 2 · nothing similar: no area at all (no empty frame, no heading)
    await check('a matched line with nothing similar (MOON CHARM) has NO suggestion area at all', async () => { assert.equal(await rowOf('4100000002').count(), 1); assert.equal(await rowOf('4100000002').locator('.sibOffer, .sibAsk, .sibPicks').count(), 0); const s = rowOf('4100000002').locator('.rvMarkSlot'); assert.ok((await s.count()) === 0 || (await s.evaluate(x => x.childElementCount === 0 && getComputedStyle(x).display === 'none'))); });
    await check('a line with no SKU, and the shop-wide placeholder SKU, have none either (nothing is guessed from the title)', async () => { for (const rid of ['4100000006', '4100000007']) { assert.equal(await rowOf(rid).count(), 1, rid); assert.equal(await rowOf(rid).locator('.sibOffer').count(), 0, rid); } });
    await check('a two-line order has one slot for the line that has suggestions, and none drawn for the one that has not', async () => { const r = rowOf('4100000003'); assert.equal(await r.locator('.sibOffer').count(), 1); assert.equal(await r.locator('.sibOffer [data-dsku]').evaluateAll(l => l.map(b => b.dataset.dsku)).then(l => l.join()), 'NOVA STAR 3'); assert.equal(await r.locator('.sibOffer [data-cur]').getAttribute('data-cur'), 'NOVA STAR'); });

    // 3 · an order that arrived by itself (unmatched SKU): the same suggestions
    await check('an unmatched SKU row (LUNA OWL) shows the similar charms too, LUNA OWL 2 as a tap', async () => { const u = unRow('4100000004'); assert.equal(await u.count(), 1); assert.deepEqual(await u.locator('.sibOffer [data-dsku]').evaluateAll(l => l.map(b => b.dataset.dsku)), ['LUNA OWL 2']); assert.equal(await u.locator('.sibOffer [data-cur]').count(), 0); });
    await check('an unmatched SKU with nothing similar (ZZZ UNKNOWN) has no area, only the usual card', async () => { const u = unRow('4100000005'); assert.equal(await u.count(), 1); assert.equal(await u.locator('.sibOffer').count(), 0); });

    // 4 · nothing was applied: no link, no re-reading, no write, until a person taps
    const before = await page.evaluate(() => Orders.rows().map(r => [r.order.receiptId, r.state, r.spec && r.spec.designSku, r.problems.length]).sort().join('|'));
    await check('nothing was chosen or written by showing the suggestions: no saved link, the matched line still reads AMBER OWL, the waiting ones still wait', async () => {
      assert.equal(st.list((SANDBOX ? 'Sandbox_' : '') + 'Charm_Sku_Aliases').length + st.list('Charm_Sku_Aliases').length, 0);
      const m = await page.evaluate(() => Object.fromEntries(Orders.rows().map(r => [r.order.receiptId + ':' + r.line.sku, [r.spec && r.spec.designSku, r.problems.map(p => p.kind).join()]])));
      assert.deepEqual(m['4100000001:AMBER OWL'], ['AMBER OWL', '']); assert.equal(m['4100000004:LUNA OWL'][1], 'unmatchedSku'); assert.equal(m['4100000005:ZZZ UNKNOWN'][1], 'unmatchedSku');
      assert.deepEqual(await page.evaluate(() => ['4100000001', '4100000002', '4100000003'].map(r => CharmNestReviewMark.isMarked(r))), [true, true, true], 'every mark is still open');
    });

    // 5 · a tap is the person's pick: REVLINK saves it (one write, read back) and resolves the mark as a pick
    const callsBefore = st.calls.length;
    await aOffer.locator('button[data-dsku="AMBER OWL 2"]').click();
    await page.waitForFunction(() => !CharmNestReviewMark.isMarked('4100000001'), null, { timeout: 20000 });
    await check('the tap closed the review as a pick: who, which charm, after ONE saved-link write that the server read back', async () => {
      const info = await page.evaluate(() => CharmNestReviewMark.info('4100000001')); assert.ok(info && info.state === 'done' && info.how === 'pick' && info.sku === 'AMBER OWL 2' && info.doneBy === 'Test Operator', JSON.stringify(info));
      const puts = st.calls.slice(callsBefore).filter(c => c.name === 'charmNestLibrary' && c.op === 'aliasPut'); assert.equal(puts.length, 1, 'one saved-link write'); assert.equal(String(puts[0].body.listingId), '1100000001'); assert.equal(puts[0].body.sku, 'AMBER OWL 2'); assert.equal(puts[0].body.fromSku, 'AMBER OWL');
      assert.equal(st.calls.slice(callsBefore).filter(c => c.name === 'charmNestLibrary' && c.op === 'reviewMarkPut' && c.body.action === 'resolve').length, 1, 'one resolve write');
    });
    await check('the matched line itself is not touched by the pick (nothing is lifted or re-made: it is not on a sheet, the link is for the NEXT order; the other orders are still marked)', async () => {
      const m = await page.evaluate(() => ({ now: Orders.rows().filter(r => r.order.receiptId === '4100000001').map(r => [r.state, r.poolIds.length]), others: ['4100000002', '4100000003'].map(r => CharmNestReviewMark.isMarked(r)) }));
      assert.deepEqual(m.now, [['pulled', 0]]); assert.deepEqual(m.others, [true, true]);
    });
    await check('the link is read back by the page: this listing and SKU now name AMBER OWL 2 (a new order of the listing is matched by it)', async () => {
      const al = await page.evaluate(() => { const a = B.maps.aliases && B.maps.aliases['1100000001']; return a && a.bySku ? JSON.stringify(a.bySku) : null; }); assert.ok(al && /AMBER OWL 2/.test(al), String(al));
      const next = await page.evaluate(() => { const order = { receiptId: '4100000099', orderNumber: '4100000099', createTs: 1789100000, updateTs: 1789100000, shipBy: 1790000000, buyer: { name: 'Test Buyer' }, buyerMessage: '', staffNote: '', messages: [], lines: [{ transactionId: '5100000099', listingId: '1100000001', sku: 'AMBER OWL', title: 'Amber Owl Necklace', quantity: 1, variations: [{ name: 'Metal Choice', value: 'Silver' }, { name: 'Necklace Length in inches', value: '18"' }], metalKey: 'silver', metalLabel: 'Silver', personalization: [], buyerMessage: '' }] };
        const line = order.lines[0], key = CharmNestOrders.lineKey(order, line), row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); Orders.interpretAll(); return { sku: row.spec.designSku, source: row.spec.skuSource, problems: row.problems.map(p => p.kind) }; });
      assert.equal(next.sku, 'AMBER OWL 2'); assert.equal(next.source, 'alias'); assert.deepEqual(next.problems, []);
    });
    await check('the other listings and lines were not linked by that tap', async () => { const all = [...st.list('Charm_Sku_Aliases'), ...st.list('Sandbox_Charm_Sku_Aliases')]; assert.ok(all.every(d => d._id === '1100000001'), JSON.stringify(all.map(d => d._id))); });

    // 6 · an unmatched row's tap also saves for good, and the row clears (the existing path, with the new charm)
    await unRow('4100000004').locator('.sibOffer [data-dsku="LUNA OWL 2"]').click();
    await page.waitForFunction(() => !document.querySelector('#rvList .reviewListRow[data-rid="4100000004"]:not([data-mkey^="ord:rvm:"])'), null, { timeout: 20000 });
    await check('an unmatched row\'s tap saved the link for ITS listing and the question left Review; no mark was touched', async () => {
      const all = [...st.list('Charm_Sku_Aliases'), ...st.list('Sandbox_Charm_Sku_Aliases')], d = all.find(x => x._id === '1100000005'); assert.ok(d && JSON.stringify(d).includes('LUNA OWL 2'), JSON.stringify(all.map(x => x._id)));
      assert.deepEqual(await page.evaluate(() => ['4100000002', '4100000003', '4100000006', '4100000007'].map(r => CharmNestReviewMark.isMarked(r))), [true, true, true, true]);
    });
    await check('no confirm or prompt pop-up, no page error, no Etsy call', () => { assert.deepEqual(dialogs, [], 'dialogs'); assert.deepEqual(errors, [], 'errors'); assert.deepEqual(etsy, [], 'etsy'); });
  } finally { await browser.close(); await srv.close(); }
  console.log(`review-similar-browser (${SANDBOX ? 'sandbox' : 'production'} mode): ${n} checks, ${failed} failed`); process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
