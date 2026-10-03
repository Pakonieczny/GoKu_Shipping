// The order window's Overview shows each seal once (Paul, 2 Oct 21:07, with a screenshot of a completed custom order:
// "There are too many seals showing here, consolidate this and only show the seals that are necessary. I think there's
// too much information here and it's not really useful."): the Now card showed "Order complete", and the custom-order
// bar under it showed it again beside "QR label printed". Display only: every seal stays in the order's record and on
// its Timeline tab. Runs in headless Chromium against the local fake site (bridge-server.cjs), offline.
//   SHOTS=<dir> node tests/charm-nest/ow-seals-once.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const Timeline = require(path.join(root, 'netlify/functions/_orderTimeline.js'));

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const RID = '4176744752', KEY = '4176744752_41767447521';
const ORDERS = [{ receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer 4752' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: '41767447521', listingId: '1800047521', sku: 'CHAIN_8941', title: 'CHAIN REPLACEMENT', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'Gold' }, { name: 'Length', value: '17 Inches' }], metalKey: '', metalLabel: '', personalization: '' }] }];

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const H = 36e5, TB = Date.now() - 30 * H, TP = TB + 22 * H;   // completed by hand, then its QR label printed later
  const stamps = [{ how: 'button', at: TB, by: 'Paul' }, { how: 'print', at: TP, by: 'Paul' }];
  srv.st.put('Charm_Custom_Orders', KEY, { key: KEY, receiptId: RID, transactionId: '41767447521', sku: 'CHAIN_8941', title: 'CHAIN REPLACEMENT', category: 'Unknown SKU', kind: '', state: 'completed', how: 'button',
    completedAt: TB, completedBy: 'Paul', printedAt: TP, printedBy: 'Paul', lastPrintedAt: TP, lastPrintedBy: 'Paul', prints: 1, hasLabel: true, updatedAtMs: TP + 60e3, stamps }, false);
  const T0 = Date.now() - 4 * 24 * H, ev = (type, at, x) => Object.assign({ id: `${RID}~${type}~${at}`, orderId: RID, type, at, by: 'Paul', source: 'sorter' }, x || {});
  const evs = Timeline.chronology([ev('arrived', T0, { source: 'etsy', by: 'Etsy' }),
    ev('sealCompleted', TB, { lineKey: KEY, data: { how: 'button' }, text: 'Custom order completed' }), ev('sealPrinted', TP, { lineKey: KEY, data: { how: 'print', prints: 1 }, text: 'Custom QR label printed' })]).sort(Timeline.byTime);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.OrderWin && window.OrderTimeline && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders, evs, where }) => {
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      window.__prints = 0; CustomPrint.print = function () { __prints++; };   // (no label is made here)
      const wait = window.setTimeout.bind(window);   // (the order's timeline: the steps it has taken, answered here)
      window.__evs = evs; OrderTimeline.get = async () => { await new Promise(r => wait(r, 20)); return JSON.parse(JSON.stringify({ id: orders[0].receiptId, events: window.__evs, cancelled: null, where })); };
    }, { orders: ORDERS, evs, where: Timeline.whereOf(evs, null, { record: true }) });
    await page.evaluate(k => OrderWin.open(k), KEY);
    await page.waitForFunction(() => document.querySelector('#owNowCard .tlNowSeal') && document.querySelectorAll('#owRail .tlStop .tlSeal').length > 1 && document.querySelector('#owCustom [data-cu-reopen]'), null, { timeout: 15000 });
    await page.waitForTimeout(1500); await page.mouse.move(700, 880);
    if (shots) await page.screenshot({ path: path.join(shots, 'overview.png') });

    // every seal the Overview draws, by what it says (its action, date and time): the Now card's, the header strip's stops, the bar's
    const overview = () => page.evaluate(() => {
      const out = [], root = document.getElementById('orderWin'), pane = root.querySelector('.owVInfo') || root;
      const of = s => { const m = s.querySelector('svg[data-seal-model]'); if (!m) return null; try { const j = JSON.parse(m.getAttribute('data-seal-model')); return j.action + ' · ' + j.date + ' ' + j.time; } catch (_) { return null; } };
      for (const s of root.querySelectorAll('.seal, .tlNowSeal, .tlSeal, .tlSt, .tlBig')) {
        const r = s.getBoundingClientRect(); if (!(r.width > 4 && r.height > 4) || !s.offsetParent) continue;
        const where = s.closest('#owCustom') ? 'bar' : s.closest('#owNowCard') ? 'now' : s.closest('#owRail') ? 'strip' : s.closest('.owVTimeline,.owVTl,.tlRoot') ? 'timeline' : 'other';
        out.push({ where, what: of(s), w: Math.round(r.width), cls: String(s.className).split(' ')[0] });
      }
      return out.filter(x => x.what);
    });
    const ov = await overview();
    console.log('  Overview seals: ' + ov.map(x => `${x.where}:${x.what} (${x.w}px)`).join(' | '));
    const counts = {}; for (const x of ov.filter(x => x.where !== 'strip')) counts[x.what] = (counts[x.what] || 0) + 1;
    check(Object.values(counts).every(n => n <= 1), 'each seal is drawn once on the Overview (outside the progress strip): ' + JSON.stringify(counts));
    const barSeals = await page.evaluate(() => [...document.querySelectorAll('#owCustom .seal')].map(s => ({ kind: [...s.classList].find(c => /^seal-/.test(c)), at: +s.dataset.at, w: s.offsetWidth })));
    check(barSeals.length <= 1, `the bar shows at most one seal (${barSeals.length}): ${JSON.stringify(barSeals)}`);
    check(barSeals.length === 1 && barSeals[0].kind === 'seal-print' && barSeals[0].at === TP && barSeals[0].w >= 18 && barSeals[0].w <= 30, 'it is the QR label seal, the latest one the Now card above does not show, small (22px; 18 is the legible least)');
    const nowSeal = await page.evaluate(() => JSON.parse(document.querySelector('#owNowCard .tlNowSeal svg[data-seal-model]').getAttribute('data-seal-model')));
    check(nowSeal.action === 'ORDER COMPLETE' && +nowSeal.at === TB, 'the Now card keeps its Order complete seal');
    const more = await page.evaluate(() => { const m = document.querySelector('#owCustom [data-cu-more]'); return m ? { text: m.textContent.trim(), tag: m.tagName } : null; });
    check(more === null, 'no "+N" when everything is on the screen already: ' + JSON.stringify(more));

    // Print again works as before
    await page.click('#owCustom [data-cu-print]');
    await page.waitForFunction(() => window.__prints >= 1, null, { timeout: 5000 }).catch(() => {});
    check(await page.evaluate(() => window.__prints) >= 1, 'Print again still prints');
    check(await page.evaluate(() => /Print again/.test(document.querySelector('#owCustom [data-cu-print]')?.textContent || '')), 'the button still reads Print again');
    const view = () => page.evaluate(() => document.querySelector('#orderWin [data-ow-view][aria-selected="true"]')?.dataset.owView);

    // a third seal (printed again): the bar still shows one seal, the latest it can, and "+1" opens the Timeline
    const TP2 = TP + 2 * H;
    await page.evaluate(({ KEY, TP2 }) => { const rec = B.maps.customDone[KEY]; rec.stamps = rec.stamps.concat([{ how: 'print', at: TP2, by: 'Paul' }]); rec.prints = 2; rec.lastPrintedAt = TP2; OrderWin.paint(); }, { KEY, TP2 });
    await page.waitForFunction(() => document.querySelector('#owCustom [data-cu-more]'), null, { timeout: 5000 });
    const bar3 = await page.evaluate(() => ({ seals: [...document.querySelectorAll('#owCustom .seal')].map(s => ({ kind: [...s.classList].find(c => /^seal-/.test(c)), at: +s.dataset.at })), more: document.querySelector('#owCustom [data-cu-more]').textContent.trim(), now: document.querySelectorAll('#owNowCard .tlNowSeal').length }));
    check(bar3.seals.length === 1 && bar3.seals[0].kind === 'seal-print' && bar3.seals[0].at === TP2 && bar3.more === '+1' && bar3.now === 1, 'three seals: the Now card keeps one, the bar one (the latest print) and a quiet +1: ' + JSON.stringify(bar3));
    const ov3 = await overview(), counts3 = {}; for (const x of ov3.filter(x => x.where !== 'strip')) counts3[x.what] = (counts3[x.what] || 0) + 1;
    check(Object.values(counts3).every(n => n <= 1), 'still each seal once on the Overview: ' + JSON.stringify(counts3));
    if (shots) await page.screenshot({ path: path.join(shots, 'overview-three.png'), clip: { x: 0, y: 90, width: 1040, height: 220 } });
    await page.click('#owCustom [data-cu-more]');
    await page.waitForSelector('#orderWin .tlSt[data-key]', { timeout: 8000 });
    check(await view() === 'timeline', 'the +1 opens the Timeline tab');
    await page.waitForTimeout(500);
    const tl = await page.evaluate(() => [...document.querySelectorAll('#orderWin .tlSt[data-key] svg[data-seal-model]')].map(s => { try { const m = JSON.parse(s.getAttribute('data-seal-model')); return m.action + '@' + m.time; } catch (_) { return ''; } }));
    check(tl.filter(x => /^QR LABEL PRINTED/.test(x)).length >= 1 && tl.some(x => /^ORDER COMPLETE/.test(x)) && tl.some(x => /^ORDER RECEIVED/.test(x)), 'the Timeline tab lists the seals (the two the Overview shows and the order\'s arrival): ' + tl.join(', '));
    if (shots) await page.screenshot({ path: path.join(shots, 'timeline.png') });

    // Reopen works as before, and takes no seal away
    await page.click('#orderWin [data-ow-view="info"]'); await page.waitForSelector('#owCustom [data-cu-reopen]');
    await page.click('#owCustom [data-cu-reopen]');
    await page.waitForFunction(k => !B.maps.customDone[k], KEY, { timeout: 20000 });
    const rec = srv.st.doc('Charm_Custom_Orders', KEY);
    check(rec.state === 'open', 'Reopen still reopens the order (record: ' + rec.state + ')');
    check(rec.stamps.length >= 2 && rec.stamps[0].how === 'button' && rec.stamps.some(x => x.how === 'print' && x.at === TP), 'and the record keeps its stamps, untouched (' + rec.stamps.length + ')');
    // reopened (the timeline says so a moment later): the Now card steps back to the order's arrival, and the bar still
    // shows one seal of the record's, small and on the button that made it, with "+1" for the other
    await page.waitForFunction(() => document.querySelector('#owCustom [data-cu-complete]'), null, { timeout: 10000 });
    await page.evaluate(({ KEY, at }) => { __evs = __evs.concat([{ id: 'reopen', key: 'reopen', orderId: '4176744752', type: 'note', at, by: 'Paul', source: 'sorter', lineKey: KEY, text: 'Reopened', data: { reopened: 'reopen' } }]); OrderWin._feed().refresh({ force: true }); }, { KEY, at: Date.now() - H });
    await page.waitForFunction(() => document.querySelector('#owCustom [data-cu-more]'), null, { timeout: 10000 });
    await page.waitForTimeout(500);
    const open = await page.evaluate(() => ({ seals: [...document.querySelectorAll('#owCustom .seal')].map(s => ({ kind: [...s.classList].find(c => /^seal-/.test(c)), w: s.offsetWidth, onBtn: !!s.closest('.sealRow').previousElementSibling?.matches('[data-seal-btn]') })), more: document.querySelector('#owCustom [data-cu-more]')?.textContent.trim() || '', btns: [...document.querySelectorAll('#owCustom button')].map(b => b.textContent.trim()) }));
    check(open.seals.length === 1 && open.seals[0].w >= 18 && open.seals[0].w <= 30 && open.seals[0].onBtn && open.more === '+1', 'reopened: one small seal on its button and +1: ' + JSON.stringify(open));
    if (shots) await page.screenshot({ path: path.join(shots, 'overview-reopened.png'), clip: { x: 0, y: 90, width: 1040, height: 220 } });
    check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error('\n' + fails.length + ' failed:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('ow-seals-once: ok');
})().catch(e => { console.error(e); process.exit(1); });
