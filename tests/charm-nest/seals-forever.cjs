// Seals are kept for good (Paul, 29 Sep 00:35: "the seals can never ever disappear … even though you can reopen an order,
// the seal must always remain and follow that order forever"). The sorter runs in headless Chromium against the local fake
// site (bridge-server.cjs); every request off the loopback is aborted and the label's PDF is a stub.
// Print → Completed → Reopen → Complete Order → Reopen → reload → Print again: every seal stays on the server record
// (reopen only changes its state and adds a history entry and a timeline note), the Open card shows each seal on the
// button that made it, as it was (same time, same turn), never stamped again on a redraw or a reload; the order window
// shows them small; a print after reopen is Print Nº 2 beside the others.
//   node tests/charm-nest/seals-forever.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const line = (tid, sku, title, variations) => ({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: variations.map(([name, value]) => ({ name, value })), metalKey: '', metalLabel: '', personalization: '' });
const RID = '4176744752', KEY = '4176744752_41767447521';
const ORDERS = [order(RID, [line(41767447521, 'CHAIN_8941', 'CHAIN REPLACEMENT', [['Metal', 'Gold'], ['Length', '17 Inches']])])];

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    const PDFMAKE = `window.pdfMake = { createPdf(dd) { return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title><script>window.print = () => { window.parent.parent.__prints = (window.parent.parent.__prints || 0) + 1; };<\\/script>'], { type: 'text/html' })); } }; } };`;
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js(PDFMAKE));
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill(js(''));
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
    const boot = async () => {
      await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.Motion && CN.S.cloud.ok === true, null, { timeout: 60000 });
      await page.evaluate(async orders => {
        await Orders.loadMaps(true);
        for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
        Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
        // every stamp pressed: none may come from a redraw or a reload
        window.__stamped = 0; const on = Seal.stampOn, pr = Seal.press;
        Seal.stampOn = function (h, spec) { if (!(spec && spec.still)) __stamped++; return on.apply(this, arguments); };   // (a seal carried as it lies is not a stamp) Seal.press = function () { __stamped++; return pr.apply(this, arguments); };
      }, ORDERS);
      await page.click('#reviewView .egTab[data-k="customOrder"]');
    };
    const card = `#rvList .reviewListRow[data-rid="${RID}"]`;
    const settle = () => page.waitForFunction(() => !document.querySelector('.cuStat, .btn.working, .cuSealHost, #motionLayer .mGhost, .sealTool, .seal.pending'), null, { timeout: 15000 });
    const seg = async s => { await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close())); await page.click(`#reviewView .rvSeg [data-cseg="${s}"]`); await page.waitForSelector(card); };
    // the card's seals: each with the button just before its row, its time, its turn, and whether it waits to be pressed
    const seals = () => page.evaluate(sel => [...document.querySelectorAll(sel + ' .sealRow .seal')].map(s => {
      const r = s.closest('.sealRow'); let b = r.previousElementSibling; while (b && b.classList.contains('sealRow')) b = b.previousElementSibling;
      return { how: s.classList.contains('seal-button') ? 'button' : 'print', at: +s.dataset.at, rot: s.style.getPropertyValue('--rot'), btn: b && b.matches('[data-seal-btn]') ? (b.matches('[data-cu-complete]') ? 'complete' : 'print') : null, pending: s.classList.contains('pending'), face: (m => [m.action, m.n])(JSON.parse(s.querySelector('svg').dataset.sealModel)) };
    }), card);
    const rec = () => srv.st.doc('Charm_Custom_Orders', KEY);
    const stamped = () => page.evaluate(() => __stamped);

    await boot();
    // 1 · Print QR label from Open: one seal, the card goes to Completed
    await page.click(card + ' [data-cu-print]');
    await page.waitForFunction(k => B.maps.customDone[k], KEY); await settle();
    await seg('done');
    const first = await seals();
    assert.deepEqual(first.map(s => [s.how, s.btn, s.face]), [['print', 'print', ['QR LABEL PRINTED', 1]]], 'one print seal: ' + JSON.stringify(first));

    // 2 · Reopen: the record stays with its seal, open; history and a timeline note (not a seal) say who and when
    await page.click(card + ' [data-cu-reopen]');
    await page.waitForFunction(k => !B.maps.customDone[k] && B.maps.customKept[k], KEY); await settle();
    let r = rec();
    assert.equal(r.state, 'open'); assert.equal(r.stamps.length, 1, 'its seal stays on the server'); assert.deepEqual(r.history.map(h => [h.how, h.by]), [['reopen', 'Test Operator']]);
    const notes = () => srv.st.list('Order_Timeline').filter(e => e.type === 'note' && /^customReopen-/.test(String(e._id).split('~').pop()));
    assert.equal(notes().length, 1, 'a timeline note for the reopen'); assert.match(notes()[0].text, /reopened by Test Operator/);
    const n0 = await stamped();
    await seg('open');
    let open = await seals();
    assert.deepEqual(open.map(s => [s.how, s.btn, s.at, s.rot, s.pending]), first.map(s => [s.how, 'print', s.at, s.rot, false]), 'the Open card shows it as it was, on Print QR label: ' + JSON.stringify(open));
    for (let i = 0; i < 3; i++) await page.evaluate(() => Review.render());
    await page.waitForTimeout(400);
    assert.equal(await stamped(), n0, 'never stamped again on a redraw');

    // 3 · Complete Order (a seal of its own), then Reopen again: both seals on the Open card, each on its button
    await page.click(card + ' [data-cu-complete]');
    await page.waitForFunction(k => B.maps.customDone[k] && !B.maps.customKept[k], KEY); await settle();
    await seg('done');
    assert.deepEqual((await seals()).map(s => s.how), ['print', 'button']);
    await page.click(card + ' [data-cu-reopen]');
    await page.waitForFunction(k => !B.maps.customDone[k] && B.maps.customKept[k], KEY); await settle();
    r = rec(); assert.deepEqual([r.state, r.stamps.map(x => x.how).join()], ['open', 'print,button']);
    await seg('open');
    open = await seals();
    assert.deepEqual(open.map(s => [s.how, s.btn]), [['print', 'print'], ['button', 'complete']], 'each seal on the button that made it: ' + JSON.stringify(open));
    const kept = open.map(s => [s.how, s.at, s.rot]);

    // 4 · the order window's bar draws at most one small seal (the latest the card above it does not show) and a "+N" that opens
    //     the Timeline for the rest (Paul, 2 Oct: each seal once on the overview). Display only: nothing leaves the record or
    //     the Timeline, so both seals are checked there, at a readable size, within the 84 px canon
    await page.evaluate(k => OrderWin.open(k), KEY);
    await page.waitForFunction(() => document.querySelectorAll('#owCustom .sealRow .seal').length === 1 && document.querySelector('#owCustom [data-cu-more]'), null, { timeout: 15000 });
    const owBar = await page.evaluate(() => ({ seals: [...document.querySelectorAll('#owCustom .sealRow .seal')].map(s => [s.classList.contains('seal-button') ? 'button' : 'print', s.offsetWidth, s.offsetHeight]), more: document.querySelector('#owCustom [data-cu-more]').textContent }));   // (unrotated box: a stamp lies at an angle)
    assert.deepEqual([owBar.seals.length, owBar.seals[0][0], owBar.more], [1, 'complete'.replace('complete', 'button'), '+1'], 'one seal, the latest, and "+1" for the other: ' + JSON.stringify(owBar));
    assert.ok(owBar.seals.every(([, w, h]) => w >= 24 && w <= 84 && h >= 24 && h <= 84), 'the order window seal is drawn at a readable size, within the 84 px canon: ' + JSON.stringify(owBar));
    r = rec(); assert.equal(r.stamps.map(x => x.how).join(), 'print,button', 'the record still keeps both seals');
    await page.click('#owCustom [data-cu-more]');
    await page.waitForFunction(() => document.querySelector('#owTimeline .tlSt[data-key^="labelPrinted~"]') && document.querySelector('#owTimeline .tlSt[data-key^="sealCompleted~"]'), null, { timeout: 15000 });
    const onTimeline = await page.evaluate(() => [...document.querySelectorAll('#owTimeline .tlSt[data-key^="labelPrinted~"], #owTimeline .tlSt[data-key^="sealCompleted~"]')].map(b => { const m = b.querySelector('svg[data-seal-model]'); try { return JSON.parse(m.getAttribute('data-seal-model')).action; } catch (_) { return ''; } }));
    assert.deepEqual(onTimeline.slice().sort(), ['ORDER COMPLETE', 'QR LABEL PRINTED'], 'the "+1" opens the Timeline, which lists both the print and the Complete seal: ' + JSON.stringify(onTimeline));
    await page.keyboard.press('Escape'); await page.waitForTimeout(300);

    // 5 · a reload: the kept record is read again; the same seals, none stamped
    await boot();
    await page.waitForSelector(card + ' .sealRow .seal');
    await settle();
    assert.deepEqual((await seals()).map(s => [s.how, s.at, s.rot]), kept, 'the same seals after a reload');
    assert.equal(await stamped(), 0, 'nothing stamped on a reload');

    // 6 · Print again after the reopen: Print Nº 2 beside the others, nothing replaced
    await page.click(card + ' [data-cu-print]');
    await page.waitForFunction(k => B.maps.customDone[k], KEY); await settle();
    assert.equal(await stamped(), 1, 'the new seal, stamped once');
    r = rec(); assert.deepEqual([r.state, r.prints, r.stamps.map(x => x.how).join()], ['completed', 2, 'print,button,print']);
    await seg('done');
    const all = await seals();
    assert.deepEqual(all.map(s => s.face), [['QR LABEL PRINTED', 1], ['ORDER COMPLETE', 0], ['QR LABEL PRINTED', 2]], 'every seal: ' + JSON.stringify(all));
    assert.deepEqual(all.slice(0, 2).map(s => [s.how, s.at, s.rot]), kept, 'the old ones unchanged');
    assert.equal(new Set(all.map(s => s.at)).size, 3, 'no seal doubled');
    // the timeline: one event per seal, and the reopen notes
    const tl = srv.st.list('Order_Timeline').filter(e => e.orderId === RID);
    assert.equal(tl.filter(e => e.type === 'sealPrinted').length, 2); assert.equal(tl.filter(e => e.type === 'sealCompleted').length, 1); assert.equal(notes().length, 2);
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ print, reopen, Complete Order, reopen, reload, print again: every seal kept, on its button, stamped only when new');
    console.log('Seals forever OK');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
