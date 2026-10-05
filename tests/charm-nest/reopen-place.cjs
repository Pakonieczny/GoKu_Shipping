// A reopened order goes back to its own place (Paul, 29 Sep 02:05: "I wanted to go back in the same place where it came
// from and the list and not get re-added somewhere down below so it's hard to find"). Nine rework orders arrive in one
// pull (one arrival time, as a real pull stamps them); the middle one is completed with Complete Order, reopened from
// Completed, and must stand at the same index under Open and under Custom Orders, keep its seal, be brought into view
// and marked; the order window's "N of M" reads as before. Then the same from the order window (Complete Order, Reopen). The sorter runs in headless Chromium against the local fake
// site (bridge-server.cjs); every request off the loopback is aborted and nothing is printed.
//   node tests/charm-nest/reopen-place.cjs [playwright-core dir]   (SHOTS=dir keeps a screenshot)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, i) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY - i * 3600, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + i }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: String(rid) + '1', listingId: String(1800000100 + i), sku: 'RE_54' + (60 + i), title: 'MODIFICATION REWORK FREE SHIPPING', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Price', value: String(100 + i) }], metalKey: '', metalLabel: '', personalization: [], buyerMessage: '' }] });
// newest order first, as the list shows them
const ORDERS = Array.from({ length: 9 }, (_, i) => order(4176576270 + i, i));
// (the middle one of the list as the page shows it: the lists stand newest activity first since 30 Sep, and orders of one pull whose
//  events fall in different milliseconds stand in no fixed order of their own, so the card is chosen once the list is drawn)
const MID = 4; let RID, KEY;

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 720 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    const outside = [];
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      if (/etsy|anthropic/i.test(u)) outside.push(u);
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.OrderWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      const at = Date.now();   // one pull: one arrival time for all
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: at, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, ORDERS);
    let card;
    const order = () => page.evaluate(() => [...document.querySelectorAll('#rvList .reviewListRow')].map(n => n.dataset.rid));
    const settle = () => page.waitForFunction(() => !document.querySelector('.cuStat, .btn.working, .cuSealHost, #motionLayer .mGhost, .sealTool, .seal.pending'), null, { timeout: 15000 });
    const seg = async s => { await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close && n.close())); await page.click(`#reviewView .rvSeg [data-cseg="${s}"]`); };
    const nOfM = async () => { await page.evaluate(k => OrderWin.open(k), KEY); await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k && document.getElementById('owPos').textContent, KEY); const t = await page.evaluate(() => document.getElementById('owPos').textContent); await page.click('#owClose'); await page.waitForFunction(() => !OrderWin.isOpen()); return t; };

    // before: every card under Open, and under Custom Orders; the order window's place
    const open0 = await order();
    assert.equal(open0.length, 9, 'nine cards: ' + open0);
    assert.equal(new Set(open0).size, 9, 'each once: ' + open0);
    RID = open0[MID]; KEY = RID + '_' + RID + '1'; card = `#rvList .reviewListRow[data-rid="${RID}"]`;
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    const custom0 = await order();
    const pos0 = await nOfM();
    assert.match(pos0, /^\d+ of 9$/, 'the order window says where it is: ' + pos0);

    // Complete Order on the middle card (it leaves for Completed), then Reopen there
    await page.click(card + ' [data-cu-complete]');
    await page.waitForFunction(k => B.maps.customDone[k] && !document.querySelector('#rvList .reviewListRow[data-rid="' + k.split('_')[0] + '"]'), KEY); await settle();
    await seg('done');
    await page.waitForSelector(card + ' [data-cu-reopen]');
    await page.waitForTimeout(1200);   // (reopened a while after it was raised: a new card would now come last)
    await page.click(card + ' [data-cu-reopen]');
    await page.waitForFunction(k => !B.maps.customDone[k] && B.maps.customKept[k], KEY); await settle();

    // Open: at the same index, brought into view and marked, its seal on Complete Order
    await seg('open');
    await page.waitForSelector(card);
    await page.waitForFunction(sel => { const n = document.querySelector(sel), p = n.closest('.scroll').getBoundingClientRect(), r = n.getBoundingClientRect(); return r.top >= p.top - 1 && r.bottom <= p.bottom + 1; }, card, { timeout: 5000 });
    const seen = await page.evaluate(sel => { const n = document.querySelector(sel); return { lit: n.classList.contains('mFound'), top: n.closest('.scroll').scrollTop, seal: [...n.querySelectorAll('.rowActions [data-cu-complete][data-seal-btn] + .sealRow .seal')].length }; }, card);
    assert(seen.lit, 'marked where it stands'); assert(seen.top > 0, 'the list was scrolled to it');
    assert.equal(seen.seal, 1, 'its Complete Order seal stays on the button that made it');
    if (process.env.SHOTS) { fs.mkdirSync(process.env.SHOTS, { recursive: true }); await page.waitForTimeout(250); await page.screenshot({ path: path.join(process.env.SHOTS, 'reopened-at-its-place.png') }); console.log('  · screenshot: ' + path.join(process.env.SHOTS, 'reopened-at-its-place.png')); }
    assert.deepEqual(await order(), open0, 'Open: every card where it was');
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    assert.deepEqual(await order(), custom0, 'Custom Orders: every card where it was');
    // (the order window walks the Orders tab, which lists by latest activity, a reopen among it: the order stands first there now. Its "N of M"
    //  still counts the same nine, and N is the order's place in that list)
    const pos1 = await nOfM(), at1 = await page.evaluate(k => Orders.visibleRows().findIndex(r => r.key === k) + 1, KEY);
    assert.equal(pos1, at1 + ' of 9', 'the order window: "N of M" is the order\'s place among the same nine: ' + pos1);
    const rec = srv.st.doc('Charm_Custom_Orders', KEY);
    assert.deepEqual([rec.state, (rec.stamps || []).map(s => s.how)], ['open', ['button']], 'the record keeps its seal: ' + JSON.stringify(rec));

    // the same through the order window: Complete Order there (the window has no Reopen button since 5 Oct, round 8: Reopen stays in the Review
    // tab); closed, the card is reopened under Completed and is where it was, marked
    await page.evaluate(() => { document.querySelector('#reviewView .egPane.scroll').scrollTop = 0; });
    await page.evaluate(k => OrderWin.open(k), KEY);
    await page.waitForSelector('#owPcSum [data-cu-complete]');
    await page.click('#owPcSum [data-cu-complete]');
    await page.waitForFunction(k => B.maps.customDone[k], KEY); await settle();
    await page.waitForSelector('#owPcSum [data-cu-done]');
    assert.equal(await page.evaluate(() => document.querySelectorAll('#orderWin [data-cu-reopen]').length), 0, 'the order window has no Reopen button');
    await page.click('#owClose'); await page.waitForFunction(() => !OrderWin.isOpen());
    await seg('done'); await page.waitForSelector(card + ' [data-cu-reopen]');
    await page.waitForTimeout(1200);
    await page.click(card + ' [data-cu-reopen]');
    await page.waitForFunction(k => !B.maps.customDone[k] && B.maps.customKept[k], KEY); await settle();
    await seg('open'); await page.waitForSelector(card);
    await page.waitForFunction(sel => { const n = document.querySelector(sel), p = n.closest('.scroll').getBoundingClientRect(), r = n.getBoundingClientRect(); return n.classList.contains('mFound') && r.top >= p.top - 1 && r.bottom <= p.bottom + 1; }, card, { timeout: 5000 });
    assert.deepEqual(await order(), custom0, 'completed in the order window, reopened in Review: every card where it was');
    assert.deepEqual(srv.st.doc('Charm_Custom_Orders', KEY).stamps.map(s => s.how), ['button', 'button'], 'both seals kept');

    assert.deepEqual(outside, [], 'no Etsy or AI call');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ completed in the middle of the list, reopened (from Completed; and completed from the order window, reopened from Completed): back at its index under Open and Custom Orders, in view and marked, seals kept, same N of M');
    console.log('Reopen place OK');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
