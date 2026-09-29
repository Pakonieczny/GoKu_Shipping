// "Open order" from the charm inspector (Paul, 28 Sep): an order's charm shows the button on the inspector's heading;
// pressed (or Enter), the order view grows out of it while the inspector fades and settles back underneath (never one
// pop-up over another), and Esc brings the inspector back as it was. The sheet report hides the button. The Engrave
// card carries the same button. Headless Chromium against the fake site; every request off the loopback is aborted.
//   node tests/charm-nest/open-order-inspector.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<dir>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const A = { rid: '4172131078', tid: '41721310781', sku: 'FIREBIRD_2' };
const order = { receiptId: A.rid, orderNumber: A.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Hannah Whitford' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: A.tid, listingId: '1800000781', sku: A.sku, title: 'Firebird charm necklace', quantity: 1, expectedShipDate: SHIP, variations: [], personalization: [] }] };

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ok: true, n: 0, engagements: [], active: null, conversation: null }) }));
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
    const key = await page.evaluate(async order => {
      await Orders.loadMaps(true);
      const line = order.lines[0], key = CharmNestOrders.lineKey(order, line);
      B.orders.rows.push({ key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null });
      Orders.interpretAll();
      return key;
    }, order);
    // an order's charm on a sheet not saved yet: the inspector
    await page.evaluate(({ rid, key }) => {
      const c = { id: 'c1', poolId: rid + '_1', order: rid, lineKey: key, name: rid + ' · FIREBIRD_2', thumb: '', index: 0, sourceName: 'pool', sourceId: 'pool', widthPt: 40, heightPt: 40, areaPt2: 1200, members: [1], hash: 'h1', qty: 1 };
      window.__sh = { metal: 'gold', placements: [{ id: 'c1', cxPt: 50, cyPt: 50, angle: 0, wPt: 40, hPt: 40 }], charms: [c], log: [], rejects: [] };
      openCharm(window.__sh, c);
    }, { rid: A.rid, key });
    await page.waitForFunction(() => document.getElementById('dlgReport').open && !document.getElementById('rpOrder').classList.contains('hidden'));
    const head = await page.evaluate(() => { const b = document.getElementById('rpOrder'), h = b.closest('.dlgHead'); return { inHead: !!h, label: b.textContent.trim(), h: h.getBoundingClientRect().height }; });
    assert(head.inHead && /^Open order/.test(head.label), 'the button sits on the inspector heading: ' + JSON.stringify(head));
    await page.waitForTimeout(700);   // the inspector's own opening lands

    // pressed: frames measured through the flight, one picture mid-way
    await page.evaluate(() => { window.__f = []; let t0 = performance.now(), last = t0; const tick = t => { window.__f.push(t - last); last = t; if (t - t0 < 800) requestAnimationFrame(tick); }; requestAnimationFrame(tick); });
    await page.click('#rpOrder');
    await page.waitForTimeout(230);
    if (process.env.SHOTS) await page.screenshot({ path: path.join(process.env.SHOTS, 'c-inspector-mid.png') });
    await page.waitForTimeout(700);
    if (process.env.SHOTS) await page.screenshot({ path: path.join(process.env.SHOTS, 'c-order-view.png') });
    const open = await page.evaluate(() => ({ ow: OrderWin.isOpen(), key: OrderWin.key(), src: +getComputedStyle(document.getElementById('dlgReport')).opacity, srcOpen: document.getElementById('dlgReport').open, frames: window.__f.slice(1) }));
    assert(open.ow && open.key === key, 'the order view shows this charm\'s line: ' + JSON.stringify(open.key));
    assert(open.srcOpen && open.src < 0.05, 'the inspector has settled back underneath, kept open: ' + open.src);
    const worst = Math.max(...open.frames);
    console.log(`  frames through the open: worst ${worst.toFixed(1)} ms of ${open.frames.length}; after the first: ${Math.max(...open.frames.slice(open.frames.indexOf(worst) + 1)).toFixed(1)} ms · ${open.frames.map(f => Math.round(f)).join(",")}`);

    // Esc: the view goes back into the button, the inspector comes back as it was
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 });
    await page.waitForTimeout(500);
    const back = await page.evaluate(() => { const d = document.getElementById('dlgReport'); return { open: d.open, op: +getComputedStyle(d).opacity, tf: getComputedStyle(d).transform, title: document.getElementById('rpTitle').textContent, anims: d.getAnimations().length }; });
    assert(back.open && back.op > 0.99 && (back.tf === 'none' || back.tf === 'matrix(1, 0, 0, 1, 0, 0)') && back.anims === 0, 'the inspector is back as it was: ' + JSON.stringify(back));
    assert(/FIREBIRD_2/.test(back.title), 'the same charm');

    // the keyboard: Enter on the button opens it, the × brings the inspector back
    await page.focus('#rpOrder'); await page.keyboard.press('Enter');
    await page.waitForFunction(() => OrderWin.isOpen());
    await page.waitForTimeout(700);
    await page.click('#owClose');
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 });
    await page.waitForTimeout(500);
    assert.equal(await page.evaluate(() => +getComputedStyle(document.getElementById('dlgReport')).opacity), 1, 'the × brings it back too');

    // the sheet report shows no Open order (a sheet of many orders); its tiles open the inspector, which does
    await page.evaluate(() => openReport(Object.assign(window.__sh, { outputs: null, verification: null, aiReview: null })));
    assert(await page.evaluate(() => document.getElementById('rpOrder').classList.contains('hidden')), 'the report hides the button');
    await page.click('#rpBody .charmTile');
    await page.waitForFunction(() => !document.getElementById('rpOrder').classList.contains('hidden'));
    await page.evaluate(() => closeDlg(document.getElementById('dlgReport')));

    // an uploaded file's charm (no order) shows none
    await page.evaluate(() => { const c = Object.assign({}, window.__sh.charms[0], { poolId: null, order: 'src1/layer' }); openCharm(window.__sh, c); });
    assert(await page.evaluate(() => document.getElementById('rpOrder').classList.contains('hidden')), 'no order, no button');
    await page.evaluate(() => closeDlg(document.getElementById('dlgReport')));

    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ Open order from the inspector: hand-off, Esc and × back, Enter, hidden on the report and on a file\'s charm');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('open-order-inspector OK')).catch(e => { console.error(e); process.exit(1); });
