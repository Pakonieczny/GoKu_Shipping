// Adversarial checks of how the order view opens and closes (wave 2): a look-up left running when the view moves to
// another order or closes, a cloud read that fails, a cancelled (gone) line of the pull, and where the view shrinks back
// to after it moved to another order. Loopback only (bridge-server.cjs); every other request is aborted.
//   node tests/charm-nest/adv-order-view.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const A = { rid: '4176208841', tid: '41762088411', sku: 'TINY_TAG' }, B2 = { rid: '4176200172', tid: '41762001721', sku: 'LEAF_CHARM' };
const C = { rid: '4175000123', tid: '41750001231', sku: 'ASTER_FLOWER' };   // outside the pull, its run kept
const G = { rid: '4176300999', tid: '41763009991', sku: 'MOON_CHARM' };     // in the pull, cancelled on Etsy (gone)
const poolOf = o => `${o.rid}_${o.tid}_1`, RUN = 'run-adv-ov';
const order = (o, buyer) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: o.tid, listingId: '1800000' + o.tid.slice(-3), sku: o.sku, title: o.sku.replace(/_/g, ' ') + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: ['Initial: H'], buyerMessage: '' }] });

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  srv.st.put('Charm_Pool', poolOf(C), { poolId: poolOf(C), orderId: C.rid, transactionId: C.tid, lineKey: `${C.rid}_${C.tid}`, sku: C.sku, material: 'gold', copy: 1, quantity: 1, state: 'placed', runId: RUN, updatedAt: Date.now() });
  srv.st.put('Charm_Nest_Runs', RUN, { runId: RUN, day: '2026-09-20', status: 'complete', step: 'complete', updatedAt: Date.now() - 7 * DAY * 1000,
    lines: { [`${C.rid}_${C.tid}`]: { state: 'committed', poolIds: [poolOf(C)], sku: C.sku, material: 'gold', quantity: 1, orderId: C.rid, transactionId: C.tid, createTs: SHIP - 12 * DAY, arrivedAt: 0,
      snap: { title: 'Aster birth flower necklace', listingId: '1800004321', metalKey: 'gold', metalLabel: 'GF 14/20', orderNumber: C.rid, buyer: 'Janet Steptoe', shipBy: SHIP - 4 * DAY, isGift: false, vars: ['Metal␟14k Gold Filled'], pers: ['September'] } },
      [`${G.rid}_${G.tid}`]: { state: 'pulled', poolIds: [], sku: G.sku, material: 'gold', quantity: 1, orderId: G.rid, transactionId: G.tid, createTs: SHIP - 3 * DAY, arrivedAt: 0,
      snap: { title: 'Moon charm necklace', listingId: '1800000991', metalKey: 'gold', metalLabel: 'GF 14/20', orderNumber: G.rid, buyer: 'Gone Buyer', shipBy: SHIP, isGift: false, vars: [], pers: [] } } } });
  srv.st.put('Charm_Pool', poolOf(G), { poolId: poolOf(G), orderId: G.rid, transactionId: G.tid, lineKey: `${G.rid}_${G.tid}`, sku: G.sku, material: 'gold', copy: 1, quantity: 1, state: 'abandoned', runId: RUN, updatedAt: Date.now() });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const bad = [];
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    // the cloud: slow (hold) or failing (fail) for the look-up's reads, as the test says
    const net = { hold: 0, fail: false };
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      if (/fonts\.googleapis|fonts\.gstatic/.test(r.request().url())) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await context.route(/\/\.netlify\/functions\/charmNestLibrary/, async r => {
      let op = ''; try { op = JSON.parse(r.request().postData() || '{}').op; } catch (_) {}
      if (['poolList', 'findSheets', 'runGet', 'customGet'].includes(op)) {
        if (net.fail) return r.fulfill({ status: 503, contentType: 'text/html', body: '<html><title>Service Unavailable</title></html>' });
        if (net.hold) await new Promise(res => setTimeout(res, net.hold));
      }
      return r.continue();
    });
    const notes = { saved: [], record: {} };
    await context.route(/\/\.netlify\/functions\/firebaseOrders/, async r => {
      const u = new URL(r.request().url()), q = u.searchParams.get('orderId');
      if (r.request().method() === 'GET' && q && notes.record[q] != null) return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ success: true, data: { 'Staff Note': notes.record[q] } }) });
      if (r.request().method() === 'POST') { let b = {}; try { b = JSON.parse(r.request().postData() || '{}'); } catch (_) {} if (typeof b.staffNote === 'string') { notes.saved.push([String(b.orderNumber), b.staffNote]); notes.record[String(b.orderNumber)] = b.staffNote; return r.fulfill({ status: 200, contentType: 'application/json', body: '{"success":true}' }); } }
      return r.continue();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders }) => {
      await Orders.loadMaps(true);
      for (const [order, state] of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null, _state: state }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll();
      for (const r of B.orders.rows) if (r._state === 'gone') { r.state = 'gone'; r.reason = 'cancelled on Etsy'; }
      CN.setMode('orders'); Orders.render();
    }, { orders: [[order(A, 'Hannah Whitford'), 'pulled'], [order(B2, 'Ava Patel'), 'pulled'], [order(G, 'Gone Buyer'), 'gone']] });
    const rowA = `#ordItems [data-key="${A.rid}_${A.tid}"]`, rowB = `#ordItems [data-key="${B2.rid}_${B2.tid}"]`;
    await page.waitForSelector(rowA);
    const closed = () => page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 });
    const check = (ok, msg) => { if (!ok) { bad.push(msg); console.log('  ✗ ' + msg); } else console.log('  ✓ ' + msg.replace(/^.*?: /, '')); };

    // 1 · an order outside the pull is still being looked up when the view moves to an order of the pull (the header
    //     search): the look-up's spinner line must not stay over that order
    net.hold = 1500;
    await page.evaluate(rid => { OrderWin.openOrder(rid, { highlight: true }); }, C.rid);
    await page.waitForFunction(() => !document.getElementById('owLoading').hidden);
    await page.evaluate(rid => { OrderWin.openOrder(rid, { highlight: true }); }, A.rid);
    await page.waitForTimeout(3500);
    let s = await page.evaluate(() => ({ title: document.getElementById('owTitle').textContent, loading: !document.getElementById('owLoading').hidden && document.getElementById('owLoading').textContent }));
    check(s.title === `Order ${A.rid}` && !s.loading, `swap mid-look-up: no look-up spinner left over order A (${JSON.stringify(s)})`);
    await page.keyboard.press('Escape'); await closed();

    // 2 · the view closed while an order outside the pull is looked up, then opened on a line of the pull
    await page.evaluate(rid => { OrderWin.openOrder(rid, {}); }, C.rid);
    await page.waitForFunction(() => !document.getElementById('owLoading').hidden);
    await page.keyboard.press('Escape'); await closed();
    await page.click(rowB);
    await page.waitForTimeout(3500);
    s = await page.evaluate(() => ({ title: document.getElementById('owTitle').textContent, loading: !document.getElementById('owLoading').hidden && document.getElementById('owLoading').textContent }));
    check(s.title === `Order ${B2.rid}` && !s.loading, `closed mid-look-up: no stale spinner when the next order opens (${JSON.stringify(s)})`);
    await page.keyboard.press('Escape'); await closed();
    net.hold = 0;

    // 3 · a failing cloud read says so, instead of "no line in the sorter's records"
    net.fail = true;
    await page.evaluate(() => OrderWin.openOrder('4170000001', {}));
    await page.waitForFunction(() => document.getElementById('owLoading').hidden && !/Reading the order/.test(document.getElementById('owNotes').textContent), null, { timeout: 15000 });
    s = await page.evaluate(() => { OrderWin.paint(); return { said: document.getElementById('owNotes').textContent, open: OrderWin.isOpen() }; });
    check(s.open && /could not be read/i.test(s.said) && !/never pulled/.test(s.said), `failed read: a clear message, kept through a repaint (${s.said})`);
    net.fail = false;
    // …and Try again reads it again: here, the order found (C)
    const retry = await page.$('#owNotes [data-ow-retry]');
    if (retry) { await retry.click(); await page.waitForFunction(() => document.getElementById('owLoading').hidden && /Janet Steptoe|no line/.test(document.getElementById('owSub').textContent + document.getElementById('owNotes').textContent), null, { timeout: 15000 }); }
    check(!!retry, 'failed read: a Try again button');
    await page.keyboard.press('Escape'); await closed();
    // an order that does not exist (the cloud answering): the plain message stays
    await page.evaluate(() => OrderWin.openOrder('4170000002', {}));
    await page.waitForFunction(() => document.getElementById('owLoading').hidden && !/Reading the order/.test(document.getElementById('owNotes').textContent), null, { timeout: 15000 });
    s = await page.evaluate(() => document.getElementById('owNotes').textContent);
    check(/no line in the sorter's records/.test(s), `order that does not exist: the plain message (${s})`);
    await page.keyboard.press('Escape'); await closed();

    // 4 · a cancelled order whose line is still in the pull as gone (the Cancelled tab opens it by number): the view
    //     has no Skip switch at all (it is gone from every order window), and the line stays gone
    await page.evaluate(rid => { OrderWin.openOrder(rid, {}); }, G.rid);
    await page.waitForFunction(rid => document.getElementById('owLoading').hidden && document.getElementById('owTitle').textContent === 'Order ' + rid && !/Reading the order/.test(document.getElementById('owNotes').textContent), G.rid, { timeout: 15000 });
    s = await page.evaluate(key => { const skip = !!document.getElementById('owSkip') || !!document.getElementById('owSkipBox'); return { skip, sub: document.getElementById('owSub').textContent, said: document.getElementById('owNotes').textContent, state: Orders.rows().find(r => r.key === key).state }; }, `${G.rid}_${G.tid}`);
    check(/Gone Buyer/.test(s.sub) && !/never pulled/.test(s.said), `gone line: the pull's line is shown, not "never pulled" (${JSON.stringify(s)})`);
    check(!s.skip && s.state === 'gone', `gone line: no Skip switch, and it stays gone (${JSON.stringify(s)})`);
    await page.keyboard.press('Escape'); await closed();

    // 5 · opened from row A, walked to B with Next (or Previous): Close goes back into B's row, not A's
    await page.click(rowA);
    await page.waitForFunction(rid => OrderWin.isOpen() && document.getElementById('owTitle').textContent.startsWith('Order ' + rid), A.rid);
    await page.waitForTimeout(700);
    const nextTo = await page.evaluate(() => { const b = [document.getElementById('owNext'), document.getElementById('owPrev')].find(x => !x.hidden && !x.disabled); if (!b) return null; b.click(); return document.getElementById('owTitle').textContent; });
    if (nextTo) {
      const into = await page.evaluate(() => { OrderWin.close(); const a = document.getElementById('orderWin').getAnimations().map(x => x.effect.getKeyframes()).filter(k => k.length && k[k.length - 1].clipPath).map(k => k[k.length - 1].clipPath); return a[0] || null; });
      const want = await page.evaluate(t => { const rid = t.replace(/\D/g, '').slice(0, 10); const n = [...document.querySelectorAll('#ordItems [data-key]')].find(x => x.dataset.key.startsWith(rid)); const r = n.getBoundingClientRect(); return Math.round(Math.max(0, r.top)); }, nextTo);
      const top = into ? +(/inset\((\d+(?:\.\d+)?)px/.exec(into) || [])[1] : null;
      check(top != null && Math.abs(top - want) < 2, `after Next: it shrinks into the row on screen now (clip top ${top}, row top ${want})`);
      await closed();
    } else check(false, 'after Next: Next was not offered');

    // 6 · the Order notes box is gone (Paul, 5 Oct 2026): while the order is still looked up and once it is read there is
    //     no box to type in, nothing is saved to the order's record, and the old note stays in the record untouched
    notes.record[C.rid] = 'Gift box, please'; notes.saved.length = 0;
    net.hold = 1500;
    await page.evaluate(rid => { OrderWin.openOrder(rid, {}); }, C.rid);
    await page.waitForFunction(() => !document.getElementById('owLoading').hidden);
    check(!(await page.$('#owNote')), 'no notes box while the order is looked up');
    await page.waitForFunction(() => document.getElementById('owLoading').hidden && /Janet Steptoe/.test(document.getElementById('owSub').textContent), null, { timeout: 15000 });
    await page.waitForTimeout(1200);
    check(!(await page.$('#owNote')) && !(await page.evaluate(() => /Order notes|next person who opens/i.test(document.getElementById('orderWin').textContent))), 'no notes box once the order is read');
    await page.keyboard.press('Escape'); await closed();
    await page.waitForTimeout(800);
    check(notes.saved.length === 0 && notes.record[C.rid] === 'Gift box, please', `the order window saves no note and leaves the record's note as it was (${JSON.stringify(notes.saved)})`);
    net.hold = 0;

    // 7 · Ctrl+K while the view shrinks closed opens no search over it
    await page.click(rowA);
    await page.waitForFunction(() => OrderWin.isOpen()); await page.waitForTimeout(700);
    const was = await page.evaluate(() => { OrderWin.close(); return document.getElementById('orderWin').open; });
    await page.keyboard.press('Control+k');
    const during = await page.evaluate(() => document.getElementById('orderWin').open);
    check(was && during, 'Ctrl+K pressed while the view was still shrinking');
    await closed(); await page.waitForTimeout(300);
    s = await page.evaluate(() => ({ search: !!(window.OrderSearch && OrderSearch.isOpen && OrderSearch.isOpen()), dialogs: document.querySelectorAll('dialog[open]').length }));
    check(!s.search && s.dialogs === 0, `Ctrl+K while closing: ignored (${JSON.stringify(s)})`);

    check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  if (bad.length) throw new Error(bad.length + ' check(s) failed');
}
main().then(() => console.log('Adversarial order view OK')).catch(e => { console.error(e.stack || e); process.exit(1); });
