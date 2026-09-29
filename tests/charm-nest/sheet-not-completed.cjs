// An order sent to a sheet from a Review card is never listed under Completed (Paul, 29 Sep: "None of the orders that are
// Send to Sheet from the review tab should appear under the completed tab. Please remove them from the completed tab").
// Completed holds what was finished by hand (Complete Order, a QR label printed) and the decisions answered. Here: an
// Unknown SKU card sent with its own design leaves no entry, and counts for nothing, under Completed; one completed by
// hand (Complete Order) still is there; records of a send already kept in the workspace (older ones carry no flag, only
// their words) are left out of the list and its count on the first draw after a reload, and stay stored, whole. Runs the
// sorter in headless Chromium against the local fake site (bridge-server.cjs): every request off the loopback is aborted,
// nothing is printed or written live.
//   node tests/charm-nest/sheet-not-completed.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const line = (tid, sku, title, variations, metal) => ({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: variations.map(([name, value]) => ({ name, value })), metalKey: metal, metalLabel: metal === 'silver' ? 'Sterling Silver' : 'GF 14/20', personalization: [], buyerMessage: '' });
const SENT = '4179100001', HAND = '4179100002';
const ORDERS = [
  order(SENT, [line(41791000011, 'ROSE_88', 'Rose Charm', [['Metal', 'Sterling Silver']], 'silver')]),      // Unknown SKU: sent to a sheet with its own design
  order(HAND, [line(41791000021, 'DAVID_99', 'David Star Necklace', [['Metal Choice', 'Gold Filled']], 'gold')])   // Unknown SKU: completed by hand
];
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DESIGN_DXF = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, 18, 20, 0, 42, 0.4, 10, 18, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, 9, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
// what the workspace held before this fix: a send recorded under Completed (its words only), and one with a flag
const OLD = [
  { key: 'ord:sku:LEGACY_1:4179100003', row: null, kind: 'unmatchedSku', why: "sent to the sheets with the order's own designs", lines: 1, orders: ['4179100003'], by: 'Test Operator', t: Date.now() - 3600000 },
  { key: 'ord:opt:Style\u0000Wavy:4179100004', row: null, kind: 'needsMapping', why: 'sent', how: 'sheet', lines: 1, orders: ['4179100004'], by: 'Test Operator', t: Date.now() - 7200000 }
];

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1720, height: 950 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    const outside = [];
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js('window.pdfMake = { createPdf() { return { getBlob(cb) { cb(new Blob([""])); } }; } };'));
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill(js(''));
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      if (/etsy/i.test(u)) outside.push(u);
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.CustomSheet && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders, old }) => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Review.settled().push(...old);
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, { orders: ORDERS, old: OLD });
    const card = rid => `#rvList .reviewListRow[data-rid="${rid}"]`;
    // what Completed shows: its count on the switch, the cards listed, the chips and their counts
    const completed = () => page.evaluate(() => ({ n: +document.querySelector('#reviewView .rvSeg [data-cseg="done"] b').textContent, rids: [...document.querySelectorAll('#rvList .reviewListRow')].map(n => n.dataset.rid).sort(), chips: [...document.querySelectorAll('#reviewView .ordBar .egTab')].map(b => b.textContent.trim()) }));
    const WANT = { n: 1, rids: [HAND], chips: ['Unknown SKU1'] };

    // 1 · Send to Sheet on an Unknown SKU card: a design dropped, its metal picked, sent
    await page.click('#reviewView .egTab[data-k="unmatchedSku"]');
    await page.evaluate(({ sel, text }) => {
      const dt = new DataTransfer(); dt.items.add(new File([text], 'rose.dxf'));
      const n = document.querySelector(sel);
      for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt }));
    }, { sel: card(SENT), text: DESIGN_DXF });
    await page.waitForFunction(() => document.querySelector('#cuDlg[open] .cuFile .cuThumb img'), null, { timeout: 30000 });
    await page.click('#cuDlg [data-x]');
    await page.waitForFunction(sel => document.querySelector(sel + ' .cuDesigns.ready') && !document.querySelector('dialog[open]'), card(SENT));
    await page.click(card(SENT) + ' [data-cu-send]');
    const sentKey = SENT + '_41791000011';
    await page.waitForFunction(k => CustomSheet.sentOf(B.orders.byKey.get(k)) && !document.querySelector('#rvList .reviewListRow[data-rid="4179100001"]'), sentKey, { timeout: 30000 });

    // 2 · Complete Order on the other one, by hand
    await page.click(card(HAND) + ' [data-cu-complete]');
    await page.waitForFunction(k => B.maps.customDone[k] && !document.querySelector('#rvList .reviewListRow[data-rid="4179100002"]'), HAND + '_41791000021', { timeout: 30000 });
    await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close && n.close()));

    // 3 · Completed: the hand-completed order only, and the switch counts what is listed (the sent one is on the sheets,
    //     the two records of older sends are stored but not listed)
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    assert.deepEqual(await completed(), WANT, 'Completed, before the reload');
    assert.deepEqual(await page.evaluate(() => Review.settled().map(d => d.orders[0])), ['4179100003', '4179100004'], 'nothing was written for the send, nothing kept was deleted');
    assert(await page.evaluate(() => CustomSheet.sentOf(B.orders.byKey.get('4179100001_41791000011'))), 'the send itself stands');
    assert.deepEqual(await page.evaluate(sel => [...document.querySelector(sel).querySelectorAll('.rowActions button')].map(b => b.textContent.trim()), card(HAND)), ['Print QR label', 'Reopen'], 'the hand-completed order keeps its seal and buttons');

    // 4 · a reload: the workspace comes back with its stored records, and Completed is the same
    await page.evaluate(() => Session.flush(true));
    await page.reload({ waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Review && window.Orders && CN.S.cloud.ok === true && Review.settled().length > 0, null, { timeout: 60000 });
    await page.evaluate(async () => { await Orders.loadMaps(true); Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.view().filter = null; Review.view().cseg = 'done'; Review.render(); });
    await page.waitForSelector(card(HAND));
    assert.deepEqual(await completed(), WANT, 'Completed, after the reload');
    assert.deepEqual(await page.evaluate(() => Review.settled().map(d => d.orders[0])), ['4179100003', '4179100004'], 'the stored records are still stored after the reload');
    assert(await page.evaluate(() => CustomSheet.sentOf(B.orders.byKey.get('4179100001_41791000011'))), 'the send stands after the reload');

    assert.deepEqual(outside, [], 'no Etsy call');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('Sheet not under Completed OK: a send from a Review card is not listed or counted under Completed, before or after a reload; Complete Order still is; older stored send records are hidden and kept');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
