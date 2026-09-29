// The seal lens (Paul, 29 Sep 02:08: "when I click or hover over these two small seals, they don't show me a zoom version
// so I can't really read them"). A completed custom order carries a print seal and a Complete Order seal; in the order
// window they are 34 px and overlap. Hovering one grows it, readable, beside it; moving onto the other glides the lens to
// it; leaving lets it go; a click keeps it (the pointer may rest on it); Esc puts it away and leaves the window open; Tab
// reaches each seal. The lens moves by transform and opacity only, and the small seals look as they did. The Review tab's
// full-size seals zoom the same way. Runs in headless Chromium against the local fake site (bridge-server.cjs).
//   SHOTS=<dir> node tests/charm-nest/seal-zoom.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const RID = '4176744752', KEY = '4176744752_41767447521';
const ORDERS = [{ receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer 4752' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: '41767447521', listingId: '1800047521', sku: 'CHAIN_8941', title: 'CHAIN REPLACEMENT', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'Gold' }, { name: 'Length', value: '17 Inches' }], metalKey: '', metalLabel: '', personalization: '' }] }];

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  // the order's record, completed: a QR label printed, then Complete Order, both by Paul (as in his screenshot)
  const t0 = Date.now() - 3 * 3600e3;
  srv.st.put('Charm_Custom_Orders', KEY, { key: KEY, receiptId: RID, transactionId: '41767447521', sku: 'CHAIN_8941', title: 'CHAIN REPLACEMENT', category: 'Rework', kind: '', state: 'completed', how: 'print',
    completedAt: t0, completedBy: 'Paul', printedAt: t0, printedBy: 'Paul', lastPrintedAt: t0, lastPrintedBy: 'Paul', prints: 1, hasLabel: true, updatedAtMs: t0 + 60e3,
    stamps: [{ how: 'print', at: t0, by: 'Paul' }, { how: 'button', at: t0 + 60e3, by: 'Paul' }] }, false);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 }, deviceScaleFactor: 2 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.OrderWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      window.__prints = 0; CustomPrint.print = function () { __prints++; };   // (no label is made here)
    }, ORDERS);

    const lens = () => page.evaluate(() => {
      const ls = [...document.querySelectorAll('.sealLens')].filter(l => !l.getAnimations().some(a => a.playState === 'running' && a.effect.getKeyframes().some(k => k.opacity === 0 || k.opacity === '0')));
      const l = ls[ls.length - 1]; if (!l) return null;
      const r = l.getBoundingClientRect(), f = l.querySelector('.lf svg').getBoundingClientRect(), ids = [...document.querySelectorAll('[id]')].map(n => n.id);
      return { n: document.querySelectorAll('.sealLens').length, cap: l.querySelector('.lc').textContent, w: Math.round(r.width), face: Math.round(f.width), pinned: l.classList.contains('pinned'), rect: { l: r.left, t: r.top, r: r.right, b: r.bottom },
        inView: r.left >= 0 && r.top >= 0 && r.right <= innerWidth && r.bottom <= innerHeight, top: l.querySelector('textPath').textContent, dupIds: ids.length - new Set(ids).size, inWindow: !!l.closest('#orderWin') };
    });
    const settle = () => page.waitForFunction(() => ![...document.querySelectorAll('.sealLens')].some(l => l.getAnimations({ subtree: true }).some(a => a.playState === 'running')), null, { timeout: 5000 });
    const sealBox = i => page.evaluate(i => { const s = document.querySelectorAll('#owCustom .sealRow.mini .seal')[i], r = s.getBoundingClientRect(), b = document.querySelector('#owCustom [data-cu-print]').getBoundingClientRect(); return { x: r.left, y: r.top, w: r.width, h: r.height, b: { l: b.left, t: b.top, r: b.right, bt: b.bottom }, look: getComputedStyle(s).transform + '|' + s.offsetWidth + '|' + getComputedStyle(s).opacity }; }, i);
    // a point on seal i clear of the other seal and of the print button (the press there is the button's, as before)
    const clearPoint = i => page.evaluate(i => {
      const all = [...document.querySelectorAll('#owCustom .sealRow.mini .seal')], s = all[i], r = s.getBoundingClientRect(), b = document.querySelector('#owCustom [data-cu-print]').getBoundingClientRect();
      for (let y = r.top + 3; y < r.bottom - 3; y += 2) for (let x = r.left + 3; x < r.right - 3; x += 2) { if (x >= b.left && x <= b.right && y >= b.top && y <= b.bottom) continue; const e = document.elementFromPoint(x, y); if (e && e.closest('.seal') === s) return { x, y }; }
      return null;
    }, i);

    // 1 · the order window: its two small seals overlap beside Print again
    await page.evaluate(k => OrderWin.open(k), KEY);
    await page.waitForFunction(() => document.querySelectorAll('#owCustom .sealRow.mini .seal').length === 2, null, { timeout: 15000 });
    await page.waitForTimeout(900);
    const look0 = [(await sealBox(0)).look, (await sealBox(1)).look];
    if (shots) await page.screenshot({ path: path.join(shots, '1-order-window-seals.png'), clip: await page.evaluate(() => { const r = document.getElementById('owCustom').getBoundingClientRect(); return { x: r.left - 20, y: r.top - 260, width: r.width + 40, height: r.height + 290 }; }) });

    // 2 · hover the print seal: the lens grows beside it, the same artwork at 200 px, with its line; by transform and opacity only
    const p0 = await clearPoint(0), p1 = await clearPoint(1); assert(p0 && p1, 'each seal has a place of its own to point at');
    await page.mouse.move(p0.x, p0.y);
    await page.waitForSelector('.sealLens');
    const props = await page.evaluate(() => [...new Set(document.getAnimations().filter(a => a.effect && a.effect.target && a.effect.target.closest && a.effect.target.closest('.sealLens')).flatMap(a => a.effect.getKeyframes().flatMap(k => Object.keys(k))))].filter(k => !['offset', 'computedOffset', 'easing', 'composite'].includes(k)).sort());
    assert.deepEqual(props, ['opacity', 'transform'], 'the lens moves by transform and opacity only: ' + props);
    await settle();
    let L = await lens();
    assert(L && L.n === 1 && L.w === 200 && L.face >= 180, 'one lens, drawn large: ' + JSON.stringify(L));
    assert.equal(L.top, 'QR LABEL PRINTED'); assert.match(L.cap, /^QR label printed by Paul · 3 hours ago$/);
    assert(L.inView && L.inWindow && L.dupIds === 0, 'inside the window, in view, no id twice: ' + JSON.stringify(L));
    const s0 = await sealBox(0);
    assert(L.rect.b <= s0.y || L.rect.t >= s0.y + s0.h, 'beside the seal, never over it');
    assert.deepEqual([(await sealBox(0)).look, (await sealBox(1)).look], look0, 'the small seals look as they did');
    assert.equal(await page.evaluate(() => document.querySelectorAll('#owCustom .seal')[0].hasAttribute('title')), false, 'no tooltip over the lens');
    if (shots) await page.screenshot({ path: path.join(shots, '2-hover-print-seal.png') });

    // 3 · onto the other seal: the lens glides to it
    await page.mouse.move(p1.x, p1.y, { steps: 4 });
    await settle();
    L = await lens();
    assert(L && L.n === 1 && L.top === 'ORDER COMPLETED' && /^Completed by Paul, no label printed · /.test(L.cap), 'the seal under the pointer: ' + JSON.stringify(L));
    if (shots) await page.screenshot({ path: path.join(shots, '3-hover-completed-seal.png') });

    // 4 · away: it goes back into its seal, and the seal's tooltip comes back
    await page.mouse.move(40, 900);
    await page.waitForFunction(() => !document.querySelector('.sealLens'), null, { timeout: 3000 });
    assert.equal(await page.evaluate(() => [...document.querySelectorAll('#owCustom .seal')].every(s => /^(QR label printed|Completed with the Complete Order button) by Paul · /.test(s.title))), true);

    // 5 · a click keeps it: the pointer may rest on the lens; Esc puts it away and the window stays open; nothing printed
    await page.mouse.click(p1.x, p1.y);
    await settle();
    L = await lens(); assert(L && L.pinned, 'kept open by the click');
    await page.mouse.move((L.rect.l + L.rect.r) / 2, (L.rect.t + L.rect.b) / 2, { steps: 6 });
    await page.waitForTimeout(500);
    assert((await lens()) && (await lens()).pinned, 'the pointer rests on it and it stays');
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.querySelector('.sealLens'), null, { timeout: 3000 });
    assert.equal(await page.evaluate(() => OrderWin.isOpen()), true, 'Esc put the lens away, not the window');
    assert.equal(await page.evaluate(() => __prints), 0, 'a click on a seal away from its button prints nothing');
    // a click elsewhere puts a kept one away too
    await page.mouse.click(p0.x, p0.y); await settle(); assert((await lens()).pinned);
    await page.mouse.click(40, 900);
    await page.waitForFunction(() => !document.querySelector('.sealLens'), null, { timeout: 3000 });

    // 6 · the keyboard: Tab from Print again reaches each seal and shows it; Esc, then Esc again closes the window
    await page.focus('#owCustom [data-cu-print]');
    await page.keyboard.press('Tab'); await settle();
    L = await lens(); assert(L && L.top === 'QR LABEL PRINTED', 'Tab onto the first seal shows it: ' + JSON.stringify(L));
    if (shots) await page.screenshot({ path: path.join(shots, '5-keyboard-tab.png'), clip: await page.evaluate(() => { const r = document.getElementById('owCustom').getBoundingClientRect(); return { x: r.left - 20, y: r.top - 300, width: r.width + 40, height: r.height + 330 }; }) });
    await page.keyboard.press('Tab'); await settle();
    L = await lens(); assert(L && L.top === 'ORDER COMPLETED' && L.n === 1, 'Tab onto the second: ' + JSON.stringify(L));
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.querySelector('.sealLens'), null, { timeout: 3000 });
    assert.equal(await page.evaluate(() => OrderWin.isOpen()), true);
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !OrderWin.isOpen(), null, { timeout: 5000 });

    // 7 · the Review tab's Custom Orders card: its full-size seals zoom the same way, on the page's own layer
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    const card = `#rvList .reviewListRow[data-rid="${RID}"]`;
    await page.waitForSelector(card + ' .sealRow .seal');
    await page.waitForTimeout(700);
    const q = await page.evaluate(sel => { const s = document.querySelector(sel + ' .sealRow .seal.seal-button'), r = s.getBoundingClientRect(); for (let y = r.top + 6; y < r.bottom; y += 3) for (let x = r.right - 6; x > r.left; x -= 3) { const e = document.elementFromPoint(x, y); if (e && e.closest('.seal') === s) return { x, y }; } return null; }, card);
    await page.mouse.move(q.x, q.y); await page.waitForSelector('.sealLens'); await settle();
    L = await lens(); assert(L && L.top === 'ORDER COMPLETED' && !L.inWindow && L.inView, 'the Review card seal: ' + JSON.stringify(L));
    if (shots) await page.screenshot({ path: path.join(shots, '4-review-card-seal.png') });
    await page.mouse.move(40, 900);
    await page.waitForFunction(() => !document.querySelector('.sealLens'), null, { timeout: 3000 });

    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ hover grows a readable lens beside the seal, glides between overlapping seals, a click keeps it, Esc and a click away put it away, Tab reaches each seal');
    console.log('Seal zoom OK');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
