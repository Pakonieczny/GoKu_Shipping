// Print QR label: the seal first, then the print screen (Paul, 28 Sep 23:56: "make sure that the stamp animation first
// plays through its completion. The seal is visible and then show the print screen."). The sorter runs in headless
// Chromium against the local fake site (bridge-server.cjs); every request off the loopback is aborted, and the label's
// PDF is a stub page whose print() only records when it was called and what the sorter showed at that moment.
// Checks: the stamp has finished and its seal rests, visible, on the button before print() is called, and print() follows
// within a few frames; a double press prints once and stamps once; a label that fails keeps its seal, says calmly that
// the print didn't open and offers Retry print, which prints under the same seal; with reduced motion the seal is
// placed at once and the print follows.
//   node tests/charm-nest/print-after-seal.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const line = (tid, sku, title, variations) => ({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: variations.map(([name, value]) => ({ name, value })), metalKey: '', metalLabel: '', personalization: '' });
const ORDERS = [
  order(4174476673, [line(41744766731, 'CUSTOM_6673', 'CUSTOM CHARM', [['Price', '28']])]),
  order(4176576272, [line(41765762721, 'RE_5460', 'MODIFICATION REWORK FREE SHIPPING', [['Price', '144']])]),
  order(4176744752, [line(41767447521, 'CHAIN_8941', 'CHAIN REPLACEMENT', [['Metal', 'Gold'], ['Length', '17 Inches']])])
];

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    // the "PDF": a page whose print() records the time and what the sorter shows then (the seal, the stamp tool)
    const PDFMAKE = `window.pdfMake = { createPdf(dd) { if (window.parent.__failPrint) throw new Error('printer stub failure');
      return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title><script>window.print = () => { const t = window.parent.parent; t.__printLog.push(Object.assign({ at: t.performance.now() }, t.__sealNow())); };<\\/script>'], { type: 'text/html' })); } }; } };`;
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
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.Motion && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      // the record: every press, every stamp (when it began and when it had lifted), every print() and what showed then
      window.__printLog = []; window.__stamps = []; window.__presses = [];
      document.addEventListener('click', e => { if (e.target.closest && e.target.closest('[data-cu-print]')) __presses.push(performance.now()); }, true);
      const stampOn = Seal.stampOn;
      Seal.stampOn = async function (host, spec) { const t0 = performance.now(); try { return await stampOn.apply(this, arguments); } finally { __stamps.push({ t0, t1: performance.now(), still: !!spec.still, onCard: host.classList.contains('cuSealHost') }); } };
      window.__sealNow = () => {
        const s = [...document.querySelectorAll('.cuSealHost .seal.seal-print')].pop(), b = document.querySelector('.sealedPrint[data-cu-print]');
        if (!s) return { seal: false };
        const r = s.getBoundingClientRect(), cs = getComputedStyle(s), br = b && b.getBoundingClientRect(), cx = r.left + r.width / 2, cy = r.top + r.height / 2;
        return { seal: true, opacity: +cs.opacity, wet: s.classList.contains('wet'), tools: document.querySelectorAll('.sealTool').length, running: s.getAnimations().length + [...document.querySelectorAll('.sealTool')].reduce((n, t) => n + t.getAnimations().filter(a => a.playState === 'running').length, 0),
          onButton: !!br && cx >= br.left && cx <= br.right && cy >= br.top - 2 && cy <= br.bottom + 2 };
      };
    }, ORDERS);
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    const card = rid => `#rvList .reviewListRow[data-rid="${rid}"]`;
    const log = () => page.evaluate(() => ({ prints: __printLog.slice(), stamps: __stamps.slice(), presses: __presses.slice() }));

    // 1 · one press: the stamp plays to its end, the seal rests on the button, then print() — and soon after, not later
    await page.click(card(4176744752) + ' [data-cu-print]');
    await page.waitForFunction(() => B.maps.customDone['4176744752_41767447521'] && !document.querySelector('.cuStat, .btn.working'), null, { timeout: 30000 });
    let L = await log();
    assert.equal(L.prints.length, 1, 'one print');
    const cardStamps = L.stamps.filter(s => s.onCard && !s.still);
    assert.equal(cardStamps.length, 1, 'one seal stamped on the button: ' + JSON.stringify(L.stamps));
    const p = L.prints[0], st = cardStamps[0];
    assert(st.t1 <= p.at, `the stamp had finished (${st.t1.toFixed(0)}) before print() (${p.at.toFixed(0)})`);
    assert(st.t1 - st.t0 >= 1100, 'the stamp played its full length: ' + (st.t1 - st.t0).toFixed(0) + ' ms');
    assert(p.at - L.presses[0] >= st.t1 - st.t0, 'print() came after the whole stamp');
    // (after the tool lifts, the seal's ink settles in ~0.5 s, part of the stamp; then two frames; the label was made meanwhile)
    assert(p.at - st.t1 < 800, 'print() follows the stamp at once: ' + (p.at - st.t1).toFixed(0) + ' ms');
    assert.equal(p.seal, true, 'the seal shows when print() is called'); assert(p.opacity > 0.8, 'fully inked: ' + p.opacity);
    assert.equal(p.wet, false); assert.equal(p.tools, 0, 'the stamp tool has lifted away'); assert.equal(p.running, 0, 'nothing still moving');
    assert.equal(p.onButton, true, 'the seal rests on the button');
    const moved = L.stamps.filter(s => s.still).length;
    assert(moved <= 1, 'the card carries its seal to Completed without a second stamp');
    await page.waitForFunction(() => !document.querySelector('.cuSealHost') && !document.querySelector('#motionLayer .mGhost'), null, { timeout: 8000 });
    const rec1 = srv.st.doc('Charm_Custom_Orders', '4176744752_41767447521');
    assert(rec1 && rec1.prints === 1 && rec1.stamps.length === 1, 'recorded once: ' + JSON.stringify(rec1));
    console.log(`  ✓ stamp ${(st.t1 - st.t0).toFixed(0)} ms, then print() ${(p.at - st.t1).toFixed(0)} ms after it, seal resting on the button`);

    // 2 · a double press prints once and stamps once
    await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close()));
    const n0 = (await log()).stamps.filter(s => s.onCard && !s.still).length;
    await page.dblclick(card(4176576272) + ' [data-cu-print]');
    await page.waitForTimeout(250);
    await page.click(card(4176576272) + ' [data-cu-print]').catch(() => {});   // and a third, while the seal comes down
    await page.waitForFunction(() => B.maps.customDone['4176576272_41765762721'] && !document.querySelector('.cuStat, .btn.working'), null, { timeout: 30000 });
    await page.waitForTimeout(1600);
    L = await log();
    assert.equal(L.prints.length, 2, 'the double press printed once: ' + L.prints.length);
    assert.equal(L.stamps.filter(s => s.onCard && !s.still).length - n0, 1, 'and stamped once');
    assert.equal(srv.st.doc('Charm_Custom_Orders', '4176576272_41765762721').prints, 1, 'recorded once');
    console.log('  ✓ double press: one stamp, one print, one record');

    // 3 · a label that fails: the seal stays, a calm note, Retry print prints under the same seal (no second one)
    await page.evaluate(() => { document.querySelectorAll('.mNote').forEach(n => n.close()); window.__failPrint = true; });
    const n1 = (await log()).stamps.filter(s => s.onCard && !s.still).length;
    await page.click(card(4174476673) + ' [data-cu-print]');
    await page.waitForFunction(r => { const n = document.querySelector(r); return n && /^Not printed: the print didn't open/.test(n.querySelector('.rowActions').textContent) && !document.querySelector('.cuSealHost'); }, card(4174476673), { timeout: 30000 });
    const failed = await page.evaluate(r => { const n = document.querySelector(r), b = n.querySelector('[data-cu-print]'); return { btn: b.textContent.trim(), sealed: b.classList.contains('sealedPrint'), seals: n.querySelectorAll('.sealRow .seal.seal-print').length, calm: n.querySelector('.cuFail').classList.contains('calm'), toast: [...document.querySelectorAll('#toasts .toast')].map(t => t.textContent).join(' | ') }; }, card(4174476673));
    assert.deepEqual([failed.btn, failed.sealed, failed.seals, failed.calm], ['Retry print', true, 1, true], 'its seal stays on the button, Retry print offered: ' + JSON.stringify(failed));
    assert.match(failed.toast, /The print didn't open/);
    assert.equal(srv.st.doc('Charm_Custom_Orders', '4174476673_41744766731'), undefined, 'nothing marked printed');
    assert.equal((await log()).prints.length, 2, 'no print()');
    await page.evaluate(() => { window.__failPrint = false; });
    await page.click(card(4174476673) + ' [data-cu-print]');
    await page.waitForFunction(() => B.maps.customDone['4174476673_41744766731'] && !document.querySelector('.cuStat, .btn.working'), null, { timeout: 30000 });
    L = await log();
    assert.equal(L.prints.length, 3, 'the retry printed');
    assert.equal(L.stamps.filter(s => s.onCard && !s.still).length - n1, 1, 'under the same seal: stamped once for both presses');
    assert.equal(srv.st.doc('Charm_Custom_Orders', '4174476673_41744766731').prints, 1);
    console.log('  ✓ a failed label keeps its seal, says so calmly, Retry print prints under the same seal');

    // 4 · reduced motion: the seal is placed at once, then the print
    await page.waitForFunction(() => !document.querySelector('#motionLayer .mGhost'), null, { timeout: 8000 });
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close()));
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    await page.waitForSelector(card(4176744752) + ' [data-cu-print]');
    const t = await page.evaluate(() => __presses.length);
    await page.click(card(4176744752) + ' [data-cu-print]');
    await page.waitForFunction(n => __printLog.length >= 4 && !document.querySelector('.cuStat, .btn.working'), t, { timeout: 30000 });
    L = await log();
    const q = L.prints[3], rs = L.stamps.filter(s => s.onCard).pop();
    assert(rs.t1 - rs.t0 < 50, 'placed at once: ' + (rs.t1 - rs.t0).toFixed(0) + ' ms'); assert(rs.t1 <= q.at);
    assert.equal(q.seal, true, 'the seal shows when print() is called'); assert(q.at - L.presses[t] < 900, 'no wait for a stamp: ' + (q.at - L.presses[t]).toFixed(0) + ' ms');
    for (const t0 = Date.now(); srv.st.doc('Charm_Custom_Orders', '4176744752_41767447521').prints !== 2; ) { if (Date.now() - t0 > 8000) throw new Error('the reprint was not recorded'); await new Promise(r => setTimeout(r, 100)); }
    console.log('  ✓ reduced motion: seal at once, then print');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('Print after seal OK');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
