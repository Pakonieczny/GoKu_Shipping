// Every QR label printed is a seal on the order's timeline (Paul, 29 Sep 02:08: "Any time a label is printed whether it be
// through this software or through the charm sorting software that must be recorded that the label was printed for this
// order and the time it was printed … Please use those [the card's seals] to show in the timeline").
// Server: a record's seal minutes after another is a print of its own (never folded into the recorded one before it).
// Browser: the sorter in headless Chromium against the local fake site (bridge-server.cjs), every request off the
// loopback aborted, the label's PDF a stub. Paul's order 4176576272 is printed from its Custom Orders card, then printed
// again at once (the same minute); a Sorting-station sticker and a shipping label arrive through the stations' door.
// The order window's Timeline: one seal per print (the Charm Sorter's on Office, Nº 1 and Nº 2; the Sorting station's on
// Sorting), the card's own QR LABEL PRINTED face, a hover card with the number, time, who and where, none for the shipping
// label, and the tab's count still the number of recorded steps. A third print from the order window adds a seal; none go.
//   node tests/charm-nest/timeline-prints.cjs [playwright-core dir]      (SHOTS=<dir> saves two screenshots there)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const Timeline = require(path.join(root, 'netlify/functions/_orderTimeline.js'));
const SHOTS = process.env.SHOTS || '';

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const RID = '4176576272', KEY = '4176576272_41765762721';
const ORDERS = [{ receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Jessica Strom' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: '41765762721', listingId: '1800062721', sku: 'RE_5460', title: 'MODIFICATION REWORK FREE SHIPPING', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Price', value: '144' }], metalKey: '', metalLabel: '', personalization: '' }] }];

// ── 1 · the server: the record's seals against the recorded ones ──
{
  const T = Date.UTC(2026, 8, 29, 2, 5, 13), seal = (at, x) => Object.assign({ orderId: RID, type: 'sealPrinted', at, by: 'paul', lineKey: KEY, data: { how: 'print' } }, x);
  // print 2 recorded, print 1's timeline write lost: the record keeps both, 2 minutes apart
  const kept = Timeline.dedupe([seal(T + 120000, { id: 'r2' })], [seal(T, { id: 'd1', derived: true, series: 's' }), seal(T + 120000, { id: 'd2', derived: true, series: 's' })]);
  assert.deepEqual(kept.map(e => e.id), ['d1'], 'the first print stays, the second is the recorded one: ' + JSON.stringify(kept.map(e => e.id)));
  assert.equal(Timeline.sameEvent(seal(T), seal(T + 400)), true, 'the same press, told twice');
  assert.equal(Timeline.sameEvent(seal(T), seal(T + 60000)), false, 'a print a minute later is a print of its own');
  console.log('  ✓ server: a record\'s seal minutes after another is its own print');
}

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    const PDFMAKE = `window.pdfMake = { createPdf(dd) { return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title><script>window.print = () => {};<\\/script>'], { type: 'text/html' })); } }; } };`;
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js(PDFMAKE));
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill(js(''));
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'paul'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.OrderTimelineUI && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, ORDERS);
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    const card = `#rvList .reviewListRow[data-rid="${RID}"]`;
    const settle = () => page.waitForFunction(() => !document.querySelector('.cuStat, .btn.working, .cuSealHost, #motionLayer .mGhost, .sealTool, .seal.pending'), null, { timeout: 15000 });
    const rec = () => srv.st.doc('Charm_Custom_Orders', KEY);
    const flushed = async () => { await page.evaluate(() => OrderTimeline.flush()); await page.waitForFunction(() => !OrderTimeline.pending(), null, { timeout: 30000 }); };
    const mine = type => srv.st.list('Order_Timeline').filter(e => e.orderId === RID && (!type || e.type === type));

    // 2 · Print QR label, then Print again at once (the same minute): two prints, each recorded
    await page.click(card + ' [data-cu-print]');
    await page.waitForFunction(k => B.maps.customDone[k], KEY); await settle();
    await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close()));
    await page.click('#reviewView .rvSeg [data-cseg="done"]'); await page.waitForSelector(card + ' [data-cu-print]');
    await page.click(card + ' [data-cu-print]');
    await page.waitForFunction(k => B.maps.customDone[k] && B.maps.customDone[k].prints === 2, KEY); await settle();
    await flushed();
    const lp = mine('labelPrinted'), sp = mine('sealPrinted');
    assert.equal(rec().prints, 2); assert.equal(sp.length, 2, 'the server stamps each print');
    assert.equal(lp.length, 2, 'the page records each print, even two in one minute: ' + JSON.stringify(lp.map(e => e._id)));
    assert.deepEqual(lp.map(e => [e.station, e.device, e.by, e.data.label, e.data.print]).sort((a, b) => a[4] - b[4]), [['design', 'charm-nest-1', 'paul', 'custom', 1], ['design', 'charm-nest-1', 'paul', 'custom', 2]]);
    console.log('  ✓ two prints in one minute: two labelPrinted (Nº 1, Nº 2) and two sealPrinted recorded');

    // 3 · a Sorting-station sticker and a shipping label, through the stations' door as sorting.html and shipping send them
    await page.evaluate(async RID => {
      const at = Date.now();
      const ev = [{ orderId: RID, type: 'labelPrinted', at, by: 'Ana P.', station: 'sorting', device: 'sorting-1', id: `sorting-1-${RID}-labelPrinted-${at}`, text: 'Order QR sticker printed at the Sorting station by Ana P.', data: { label: 'orderQR', via: 'QR Printer', how: 'print', login: 'name' } },
        { orderId: RID, type: 'labelPrinted', at: at + 1000, by: 'Dana K.', station: 'shipping', device: 'shipping-1', id: `shipping-1-${RID}-labelPrinted-${Math.floor(at / 60000)}`, text: 'Chit Chats label printed', data: { label: 'shipping', printed: true } }];
      const r = await fetch('/.netlify/functions/firebaseOrders', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ timeline: ev }) });
      if (!r.ok) throw new Error('station door ' + r.status);
    }, RID);
    assert.equal(mine('labelPrinted').length, 4);

    // 4 · the order window's Timeline: one seal per print, on its lane, the card's face, the hover card
    await page.evaluate(k => OrderWin.open(k), KEY);
    await page.click('[data-ow-view="timeline"]');
    const PRINTS = '#owTimeline .tlSt[data-key^="labelPrinted"], #owTimeline .tlSt[data-key^="sealPrinted"]';
    await page.waitForFunction(sel => document.querySelectorAll(sel).length === 3, PRINTS, { timeout: 15000 });
    await page.waitForTimeout(900);
    const drawn = () => page.evaluate(sel => [...document.querySelectorAll(sel)].map(b => {
      const r = b.getBoundingClientRect(), y = r.top + r.height / 2;
      const lane = [...document.querySelectorAll('#owTimeline .tlLane')].find(l => { const q = l.getBoundingClientRect(); return y >= q.top && y <= q.bottom; });
      return { key: b.dataset.key, lane: lane && lane.dataset.lane, say: b.getAttribute('aria-label') };
    }), PRINTS);
    let d = await drawn();
    assert.deepEqual(d.map(x => [x.lane, (/Print Nº \d · [\w ]*\w/.exec(x.say) || [''])[0]]), [['office', 'Print Nº 1 · Charm Sorter'], ['office', 'Print Nº 2 · Charm Sorter'], ['sorting', 'Print Nº 1 · Sorting station']], JSON.stringify(d));
    assert(!d.some(x => /shipping-1/.test(x.key)), 'a shipping label stays with Shipped: no QR seal');
    const count = await page.$eval('#owTlCount', c => c.textContent);
    assert.equal(count, String(mine().length), `the Timeline tab still counts the recorded steps (${count} of ${mine().length})`);
    // hover Nº 2: the card's QR LABEL PRINTED seal, and the card under it says the number, when, who and where
    await page.hover(`#owTimeline .tlSt[data-key="${d[1].key}"]`); await page.waitForTimeout(450);
    const hov = await page.evaluate(() => { const X = [...document.querySelectorAll('#owTimeline .tlExp, .tlExp')].find(x => getComputedStyle(x).display === 'block'); return { face: [...document.querySelectorAll('#owTimeline .tlLoupe text')].map(t => t.textContent).join(' | '), card: X ? X.textContent : '' }; });
    assert.match(hov.face, /QR LABEL PRINTED/); assert.match(hov.face, /PRINT Nº 2/); assert.match(hov.face, /PAUL/);
    assert.match(hov.card, /QR label printed · Print Nº 2 · Charm Sorter/); assert.match(hov.card, /[A-Z]{3} \d{1,2}:\d{2} [AP]M · paul/);
    await shot(page, 'timeline-prints-1-hover');
    await page.mouse.move(5, 5);
    await page.click(`#owTimeline .tlSt[data-key="${d[0].key}"]`); await page.waitForTimeout(400);
    const det = await page.evaluate(() => ({ h: document.querySelector('#owTimeline .tlDetail h3').textContent, badge: document.querySelector('#owTimeline .tlDetail .tlBadge em').textContent, big: [...document.querySelectorAll('#owTimeline .tlBig text')].map(t => t.textContent).join(' | ') }));
    assert.equal(det.h, 'QR label printed · Print Nº 1 · Charm Sorter'); assert.equal(det.badge, 'Charm Sorter'); assert.match(det.big, /QR LABEL PRINTED.*PRINT Nº 1/);
    console.log(`  ✓ Timeline: 3 print seals (Charm Sorter Nº 1, Nº 2 on Office; Sorting station Nº 1 on Sorting), none for the shipping label; hover and detail say the number, time, who and where; the tab still says ${count}`);

    // 5 · a third print from the order window: a new seal, the others stay
    await page.click('[data-ow-view="info"]');
    await page.click('#owCustom [data-cu-print]');
    await page.waitForFunction(k => B.maps.customDone[k] && B.maps.customDone[k].prints === 3, KEY); await settle();
    await flushed();
    await page.click('[data-ow-view="timeline"]');
    await page.waitForFunction(sel => document.querySelectorAll(sel).length === 4, PRINTS, { timeout: 15000 });
    await page.waitForTimeout(900);
    const d2 = await drawn();
    for (const x of d) assert(d2.some(y => y.key === x.key), 'no seal goes: ' + x.key);
    assert.match(d2.find(y => !d.some(x => x.key === y.key)).say, /Print Nº 3 · Charm Sorter/);
    await shot(page, 'timeline-prints-2-reprint');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ a print from the order window adds Nº 3; every earlier seal stays');
  } finally { await browser.close(); srv.close(); }
  console.log('Timeline prints OK');
})().catch(e => { console.error(e); process.exit(1); });

async function shot(page, name) { if (!SHOTS) return; await page.waitForTimeout(200); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); }
