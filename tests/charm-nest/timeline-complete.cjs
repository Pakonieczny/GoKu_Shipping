// Complete Order and Reopen on the order's timeline (Paul, 29 Sep 02:08: "The timeline also doesn't have the seals or the
// points for when an order was manually completed by pressing the completed button in the Review tab").
// Each press of Complete Order (a Custom Orders card, any other Review card, the order window) is a point of its own on the
// Office lane ("Operator"), with the card's green "Order completed" seal, its time, who pressed it and where; a Reopen is a
// point of its own and never takes the Complete point away; a later Complete adds another.
// Part 1 runs the real charmNestLibrary ops (customPut, customReopen, timelineGet with the records' history derived) over
// the in-memory store: what each press records, an order completed before (only its record, or its record and the event
// its press recorded then) answered once per press. Part 2 opens the sorter in headless Chromium against the local
// stand-in (bridge-server.cjs; every other request is aborted): the presses themselves, then the order window's Timeline.
//   node tests/charm-nest/timeline-complete.cjs [playwright-core dir]      (SHOTS=<dir> keeps the screenshots)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || '';
const sleep = ms => new Promise(r => setTimeout(r, ms));
// a Complete press, or a Reopen, as the server answers it
const opOf = e => (e.type === 'sealCompleted' && !(e.data && e.data.how === 'print') ? 'complete' : e.type === 'note' && e.data && e.data.reopened ? 'reopen' : '');
const opsOf = evs => evs.filter(opOf).map(e => [opOf(e), e.by, (e.data && e.data.pressedIn) || '', !!e.derived]);

(async () => {
  const srv = await start({ receipts: [] });
  const call = async body => JSON.parse((await srv.st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} })).body);
  const tl = async rid => { const r = await call({ op: 'timelineGet', orderId: rid }); assert(!r.error, r.error); return r; };
  let browser = null;
  try {
    /* ── 1 · what a press records, and the orders completed before ── */
    const T = Date.parse('2026-09-29T02:05:13Z'), M = 60000, sec = ms => Math.floor(ms / 1000);
    const custom = (rid, tid, extra) => srv.st.put('Charm_Custom_Orders', `${rid}_${tid}`, Object.assign({ key: `${rid}_${tid}`, receiptId: rid, transactionId: tid, sku: 'RE_5460', title: 'MODIFICATION REWORK FREE SHIPPING', category: 'Rework', kind: 'rework', updatedAtMs: T }, extra));
    // Paul's order as its records keep it (completed with the button, printed after): its record and the event the press recorded
    const P = '4170000001', kp = `${P}_41700000011`;
    srv.st.put('Charm_Nest_Arrivals', P, { id: P, firstSeenAt: T - 42 * M, createTs: sec(T - 42 * M) });
    custom(P, '41700000011', { state: 'completed', completedAt: T, completedBy: 'paul', how: 'button', prints: 1, printedAt: T + M, printedBy: 'paul', lastPrintedAt: T + M, lastPrintedBy: 'paul', stamps: [{ how: 'button', at: T, by: 'paul' }, { how: 'print', at: T + M, by: 'paul' }] });
    const rec = (rid, type, at, extra) => srv.st.put('Order_Timeline', `${rid}~${type}~${extra.lineKey}-${at}`, Object.assign({ orderId: rid, type, at, by: 'paul', source: 'sorter', station: 'sorter', device: '', transactionId: '', sheetId: '', sheet: '', setId: '', milestone: false }, extra));
    rec(P, 'sealCompleted', T, { lineKey: kp, text: 'RE_5460 · Complete Order', data: { how: 'button', prints: 0, completed: true, sku: 'RE_5460' } });
    rec(P, 'sealPrinted', T + M, { lineKey: kp, text: 'RE_5460 · print 1', data: { how: 'print', prints: 1, completed: true, sku: 'RE_5460' } });
    let a = await tl(P);
    assert.deepEqual(opsOf(a.events), [['complete', 'paul', '', false]], 'the completion once, the event its press recorded (the record says the same): ' + JSON.stringify(opsOf(a.events)));
    assert.equal(a.events.find(opOf).at, T, 'at its exact time');
    // the same order with no event recorded (a press the timeline missed): its record alone says it
    const Q = '4170000002';
    srv.st.put('Charm_Nest_Arrivals', Q, { id: Q, firstSeenAt: T - 42 * M, createTs: sec(T - 42 * M) });
    custom(Q, '41700000021', { state: 'completed', completedAt: T, completedBy: 'paul', how: 'button', stamps: [{ how: 'button', at: T, by: 'paul' }] });
    assert.deepEqual(opsOf((await tl(Q)).events), [['complete', 'paul', '', true]], 'read from the record: once, not twice (its stamp and its completion are one press)');
    // completed and reopened before the reopen was on the timeline: its history says so, a point of its own
    const C = '4170000003';
    srv.st.put('Charm_Nest_Arrivals', C, { id: C, firstSeenAt: T - 42 * M, createTs: sec(T - 42 * M) });
    custom(C, '41700000031', { state: 'open', completedAt: T, completedBy: 'Cy', how: 'button', stamps: [{ how: 'button', at: T, by: 'Cy' }], history: [{ how: 'reopen', at: T + 5 * M, by: 'Cy', from: 'Order window' }], reopenedAt: T + 5 * M, reopenedBy: 'Cy' });
    assert.deepEqual(opsOf((await tl(C)).events), [['complete', 'Cy', '', true], ['reopen', 'Cy', 'Order window', true]], 'the Complete point stays, the Reopen beside it');
    // a whole cycle through the ops: Complete (a Custom Orders card), Reopen, Complete again (the order window)
    const B = '4170000004', kb = `${B}_41700000041`, put = (by, from) => call({ op: 'customPut', how: 'button', key: kb, by, from, receiptId: B, transactionId: '41700000041', sku: 'CUSTOM_6673', title: 'CUSTOM CHARM', category: 'Custom charm', kind: 'customCharm' });
    srv.st.put('Charm_Nest_Arrivals', B, { id: B, firstSeenAt: Date.now() - 60 * M, createTs: sec(Date.now() - 60 * M) });
    assert.equal((await put('Ann', 'Review · Custom Orders')).ok, true); await sleep(1100);
    assert.equal((await call({ op: 'customReopen', key: kb, by: 'Ann', how: 'reopen', from: 'Review · Custom Orders' })).ok, true); await sleep(1100);
    assert.equal((await put('Bea', 'Order window')).ok, true);
    const recB = srv.st.list('Order_Timeline').filter(e => e.orderId === B).sort((x, y) => x.at - y.at);
    assert.deepEqual(opsOf(recB), [['complete', 'Ann', 'Review · Custom Orders', false], ['reopen', 'Ann', 'Review · Custom Orders', false], ['complete', 'Bea', 'Order window', false]], 'each press recorded, with where it was pressed');
    assert.deepEqual(srv.st.doc('Charm_Custom_Orders', kb).history.map(h => [h.how, h.by, h.from]), [['reopen', 'Ann', 'Review · Custom Orders']], 'the record\'s history says where too');
    a = await tl(B);
    assert.deepEqual(opsOf(a.events), [['complete', 'Ann', 'Review · Custom Orders', false], ['reopen', 'Ann', 'Review · Custom Orders', false], ['complete', 'Bea', 'Order window', false]], 'three points in order, none twice: ' + JSON.stringify(opsOf(a.events)));
    console.log('  ✓ each Complete press and each Reopen recorded with who, when and where; an order completed before answers its presses once (event or record), its reopen from its history');

    /* ── 2 · the sorter: the presses, then the order window's Timeline ── */
    const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
    let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
    browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.OrderWin && window.OrderTimelineUI && CN.S.cloud.ok === true, null, { timeout: 60000 });
    const SHIP = Math.floor(Date.now() / 1000) + 5 * 86400;
    const order = (rid, tid, sku, title, variations, extra) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 8 * 86400, updateTs: SHIP - 8 * 86400 + 60, shipBy: SHIP, buyer: { name: 'Jessica Strom' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
      lines: [Object.assign({ transactionId: tid, listingId: '1800000' + tid.slice(-3), sku, title, quantity: 1, expectedShipDate: SHIP, variations: variations.map(([name, value]) => ({ name, value })), metalKey: '', metalLabel: '', personalization: [], buyerMessage: '' }, extra || {})] });
    const RID = '4176576272', K = `${RID}_41765762721`, U = '4179000004', KU = `${U}_41790000041`;
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, [order(RID, '41765762721', 'RE_5460', 'MODIFICATION REWORK FREE SHIPPING', [['Price', '144']]), order(U, '41790000041', 'UNKNOWN_77', 'Mystery Charm', [['Metal', 'Sterling Silver']], { metalKey: 'silver', metalLabel: 'Sterling Silver' })]);
    const idle = () => page.waitForFunction(() => !document.querySelector('.cuStat, .btn.working'), null, { timeout: 30000 });
    const calm = () => page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close()));
    // Complete Order on its Custom Orders card, then Reopen under Completed
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    await page.click(`#rvList .reviewListRow[data-rid="${RID}"] [data-cu-complete]`);
    await page.waitForFunction(k => B.maps.customDone[k], K); await idle(); await page.waitForTimeout(1600); await calm();
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    await page.click(`#rvList .reviewListRow[data-rid="${RID}"] [data-cu-reopen]`);
    await page.waitForFunction(k => !B.maps.customDone[k], K); await idle(); await page.waitForTimeout(1600); await calm();
    await page.click('#reviewView .rvSeg [data-cseg="open"]');
    // Complete Order on another Review tab's card (Unknown SKU)
    await page.click('#reviewView .egTab[data-k="unmatchedSku"]');
    await page.click(`#rvList .reviewListRow[data-rid="${U}"] [data-cu-complete]`);
    await page.waitForFunction(k => B.maps.customDone[k], KU); await idle(); await page.waitForTimeout(1600); await calm();
    // and in the order window
    await page.evaluate(k => OrderWin.open(k), K);
    await page.waitForSelector('#owCustom [data-cu-complete]', { state: 'visible' });
    await page.click('#owCustom [data-cu-complete]');
    await page.waitForFunction(k => B.maps.customDone[k], K); await idle();
    const recorded = rid => opsOf(srv.st.list('Order_Timeline').filter(e => e.orderId === rid).sort((x, y) => x.at - y.at));
    assert.deepEqual(recorded(RID), [['complete', 'Test Operator', 'Review · Custom Orders', false], ['reopen', 'Test Operator', 'Review · Custom Orders', false], ['complete', 'Test Operator', 'Order window', false]], 'the page says where each was pressed: ' + JSON.stringify(recorded(RID)));
    assert.deepEqual(recorded(U), [['complete', 'Test Operator', 'Review · Unknown SKU', false]], 'a card of another Review tab says its tab');
    console.log('  ✓ pressed on a Custom Orders card, reopened, pressed on an Unknown SKU card and in the order window: each recorded with where');

    // the Timeline tab (its feed read again once the press was saved): the two Complete seals and the Reopen, on the Office lane
    await page.evaluate(() => OrderWin.setView('timeline'));
    await page.waitForFunction(() => document.querySelectorAll('#owTimeline .tlSt[data-key^="sealCompleted~"]').length === 2 && document.querySelectorAll('#owTimeline .tlSt[data-key^="reopened~"]').length === 1, null, { timeout: 15000 });
    await page.waitForTimeout(900);
    const lane = await page.evaluate(() => {
      const mid = r => r.top + r.height / 2, office = document.querySelector('#owTimeline .tlLane[data-lane="office"]'), L = mid(office.getBoundingClientRect());
      const ops = [...document.querySelectorAll('#owTimeline .tlSt[data-key^="sealCompleted~"], #owTimeline .tlSt[data-key^="reopened~"]')];
      return { label: office.textContent, off: ops.map(b => Math.round(mid(b.getBoundingClientRect()) - L)), ink: ops.map(b => { const g = b.querySelector('svg g[fill]'); return g ? g.getAttribute('fill') : ''; }), drawn: [...document.querySelectorAll('#owTimeline .tlSt[data-key]')].map(b => b.dataset.key.split('~')[0]),
        count: document.getElementById('owTlCount').textContent, sum: (document.querySelector('#owTlTools .tlSum') || document.querySelector('#owTimeline .tlSum') || {}).textContent };
    });
    assert.match(lane.label, /Office\s*Operator/, 'the Office lane, "Operator"');
    assert(lane.off.every(d => Math.abs(d) <= 3), 'every Complete and Reopen point sits on the Office lane: ' + lane.off);
    assert.deepEqual(lane.ink, ['#19663f', '#3b362f', '#19663f'], 'the Complete seals in the card\'s own green (Seal: #19663f), the Reopen its own ink: ' + lane.ink);
    assert.deepEqual(lane.drawn.filter(t => t === 'sealCompleted' || t === 'reopened'), ['sealCompleted', 'reopened', 'sealCompleted'], 'in the order they were pressed');
    const answer = await page.evaluate(rid => OrderTimeline.get(rid), RID);
    assert.equal(lane.count, String(answer.events.length), 'the Timeline tab counts each event once: ' + lane.count);
    console.log(`  ✓ the Timeline: 2 Complete seals and the Reopen on the Office lane ("Operator"), in the card's green, in press order; the tab counts ${lane.count} (${lane.sum})`);

    // hover: the seal zooms up with ORDER COMPLETED, who and where; the card under it says the same in words
    const hoverOn = async key => {
      await page.hover(`#owTimeline .tlSt[data-key="${key}"]`); await page.waitForTimeout(450);
      return page.evaluate(() => { const L = document.querySelector('#owTimeline .tlLoupe'), X = [...document.querySelectorAll('.tlExp')].find(x => getComputedStyle(x).display === 'block' && x.textContent.trim()); return { disp: L && getComputedStyle(L).display, face: L ? [...L.querySelectorAll('text')].map(t => t.textContent).join(' | ') : '', exp: X ? X.textContent : '' }; });
    };
    const keys = await page.evaluate(() => [...document.querySelectorAll('#owTimeline .tlSt[data-key^="sealCompleted~"], #owTimeline .tlSt[data-key^="reopened~"]')].map(b => b.dataset.key));
    let h = await hoverOn(keys[2]);
    assert.equal(h.disp, 'block');
    assert.match(h.face, /ORDER COMPLETED/); assert.match(h.face, /ORDER WINDOW/); assert.match(h.face, /TEST OPERATOR/); assert.match(h.face, /\d{1,2}:\d\d [AP]M/);
    assert.match(h.exp, /Completed with Complete Order · Order window/); assert.match(h.exp, /Test Operator/);
    if (SHOTS) { fs.mkdirSync(SHOTS, { recursive: true }); await page.screenshot({ path: path.join(SHOTS, 'timeline-complete-hover.png') }); }
    h = await hoverOn(keys[1]);
    assert.match(h.face, /REOPENED/); assert.match(h.face, /REVIEW · CUSTOM ORDERS/); assert.match(h.exp, /Reopened: back to Open · Review · Custom Orders/);
    h = await hoverOn(keys[0]);
    assert.match(h.face, /ORDER COMPLETED/); assert.match(h.face, /REVIEW · CUSTOM ORDERS/);
    await page.mouse.move(700, 930); await page.waitForTimeout(300);
    if (SHOTS) await page.screenshot({ path: path.join(SHOTS, 'timeline-complete.png') });
    console.log('  ✓ hover: ORDER COMPLETED / REOPENED, the time, who and where pressed on the seal, and in words under it');

    // the host is handed the records as they came (onEvents): a Reopen is still its note
    await page.evaluate(() => OrderWin.close());
    const mount = (rid, ans) => page.evaluate(({ rid, ans }) => {
      if (window.__t) { window.__t.destroy(); document.querySelectorAll('.tlTestHost').forEach(x => x.remove()); }
      if (ans) { const real = OrderTimeline.get; OrderTimeline.get = id => (id === rid ? Promise.resolve(JSON.parse(JSON.stringify(ans))) : real(id)); }
      const h = document.createElement('div'); h.className = 'tlTestHost'; h.style.cssText = 'position:fixed;inset:0;z-index:2147480000;background:#fff;display:flex;flex-direction:column'; document.body.appendChild(h);
      window.__el = document.createElement('div'); window.__el.style.cssText = 'flex:1 1 auto;display:flex;flex-direction:column'; h.appendChild(window.__el);
      window.__t = OrderTimelineUI.mount(window.__el, { orderId: rid, live: false, onEvents: l => { window.__ev = l; } });
    }, { rid, ans: ans || null });
    await mount(RID);
    await page.waitForFunction(() => window.__el.querySelectorAll('.tlSt[data-key^="reopened~"]').length === 1 && window.__ev, null, { timeout: 10000 });
    const host = await page.evaluate(() => window.__ev.filter(e => e.data && e.data.reopened).map(e => [e.type, e.text]));
    assert.deepEqual(host.map(x => x[0]), ['note'], 'type note'); assert.match(host[0][1], /^Custom order reopened by Test Operator/, 'its own words');
    // Paul's order as its records keep it: its one completion drawn (the rim: the sorter, where older presses were made)
    await mount(P, await tl(P));
    await page.waitForFunction(() => window.__el.querySelectorAll('.tlSt[data-key]').length >= 2, null, { timeout: 10000 });
    const old = await page.evaluate(() => ({ drawn: [...window.__el.querySelectorAll('.tlSt[data-key]')].map(b => b.dataset.key.split('~')[0]), lane: window.__el.querySelector('.tlLane[data-lane="office"] span').textContent }));
    assert.deepEqual(old.drawn, ['arrived', 'sealCompleted'], 'Order in, then the completion: ' + old.drawn);
    assert.equal(old.lane, 'Operator');
    h = await (async () => { await page.hover('.tlTestHost .tlSt[data-key^="sealCompleted~"]'); await page.waitForTimeout(450); return page.evaluate(() => [...window.__el.querySelectorAll('.tlLoupe text')].map(t => t.textContent).join(' | ')); })();
    assert.match(h, /ORDER COMPLETED/); assert.match(h, /PAUL/); assert.match(h, /SORTER/);
    await page.evaluate(() => window.__t.destroy());
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ an order completed before: its completion drawn from what its records keep, on the Office lane, by paul');
  } finally {
    if (browser) await browser.close();
    srv.close();
  }
})().catch(e => { console.error(e); process.exit(1); });
