// Adversarial (28 Sep, wave 2): what the old order window did must still work in the full-screen view without losing
// anyone's work. The Order notes box is gone from the window (Paul, 5 Oct 2026): Next and Previous walk an order's lines
// and the other orders with no box on any of them, and nothing writes a staff note; the Customer tab points at the
// order on screen from the first frame (it kept the order shown before, so a message typed meanwhile went to that
// buyer); Previous and Next stay once the line leaves the list it was opened from (a hold released in On hold).
// Headless Chromium against the fake site (bridge-server.cjs); every request that is not to the loopback is aborted and
// the inbox link (etsyMailOrderLink) is answered here: no message leaves the test.
//   node tests/charm-nest/adv-orderwin.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const A = { rid: '4176208841', tids: ['41762088411', '41762088412'], sku: 'TINY_TAG' }, B2 = { rid: '4176200172', tids: ['41762001721'], sku: 'LEAF_CHARM' };
const C = { rid: '4175000123', tid: '41750001231', sku: 'ASTER_FLOWER' };   // an order outside the pull
const RUN = 'run-adv-ow';
const order = (o, buyer, note) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: note || '', messages: [],
  lines: o.tids.map((tid, i) => ({ transactionId: tid, listingId: '1800000' + tid.slice(-3), sku: o.sku, title: o.sku.replace(/_/g, ' ') + ' necklace ' + (i + 1), quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: ['Initial: H'], buyerMessage: '' })) });

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  srv.st.put('Brites_Orders', A.rid, { 'Staff Note': 'Old note' });
  srv.st.put('Brites_Orders', C.rid, { 'Staff Note': 'C note from the shop' });
  // C: the pool's piece and the run that pulled it, with its line kept
  srv.st.put('Charm_Pool', `${C.rid}_${C.tid}_1`, { poolId: `${C.rid}_${C.tid}_1`, orderId: C.rid, transactionId: C.tid, lineKey: `${C.rid}_${C.tid}`, sku: C.sku, material: 'gold', copy: 1, quantity: 1, state: 'committed', runId: RUN, updatedAt: Date.now() });
  srv.st.put('Charm_Nest_Runs', RUN, { runId: RUN, day: '2026-09-20', status: 'complete', step: 'complete', updatedAt: Date.now() - 7 * DAY * 1000,
    lines: { [`${C.rid}_${C.tid}`]: { state: 'committed', poolIds: [`${C.rid}_${C.tid}_1`], sku: C.sku, material: 'gold', quantity: 1, orderId: C.rid, transactionId: C.tid, createTs: SHIP - 12 * DAY, arrivedAt: 0,
      snap: { title: 'Aster birth flower necklace', listingId: '1800004321', metalKey: 'gold', metalLabel: 'GF 14/20', orderNumber: C.rid, buyer: 'Janet Steptoe', shipBy: SHIP - 4 * DAY, isGift: false, vars: ['Metal␟14k Gold Filled'], pers: ['September'] } } } });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const mail = [];   // what the page asked of the inbox link
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      if (/fonts\.googleapis|fonts\.gstatic/.test(r.request().url())) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    // the inbox link, answered here: connected, no conversation yet; a message "sent" is only recorded
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => {
      let b = {}; try { b = JSON.parse(r.request().postData() || '{}'); } catch (_) {}
      mail.push(b);
      const body = b.op === 'ask' ? { ok: true } : b.op === 'order' ? { engagements: [], active: null, conversation: null } : { ok: true, n: 0, engagements: [] };
      return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.mail.station', JSON.stringify('k-adv-test')); localStorage.setItem('cn.mail.tab', JSON.stringify('customer')); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.CustomerMail && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, [order(A, 'Hannah Whitford', 'Old note'), order(B2, 'Ava Patel')]);
    const a1 = `${A.rid}_${A.tids[0]}`, a2 = `${A.rid}_${A.tids[1]}`, b1 = `${B2.rid}_${B2.tids[0]}`;
    // the Order notes box is gone from the order window (Paul, 5 Oct 2026): it is never there, and the window never writes a staff note
    const serverNote = rid => (srv.st.doc('Brites_Orders', rid) || {})['Staff Note'];
    const noBox = async why => assert.equal(await page.$('#owNote'), null, 'no notes box: ' + why);
    const noteWrites = []; page.on('request', rq => { if (rq.method() === 'POST' && /firebaseOrders/.test(rq.url()) && /staffNote/.test(rq.postData() || '')) noteWrites.push(rq.postData()); });
    const until = async (f, ms = 6000) => { for (const t0 = Date.now(); !f(); ) { if (Date.now() - t0 > ms) return false; await new Promise(r => setTimeout(r, 100)); } return true; };

    // 1 · Next and Previous walk the lines of an order and the other orders; no notes box shows on any of them
    await page.evaluate(k => OrderWin.open(k), a1);
    await page.waitForFunction(() => OrderWin.isOpen());
    await noBox('first line of the order');
    const seq = await page.evaluate(() => Orders.visibleRows().map(r => r.key));
    assert.deepEqual(seq, [b1, a1, a2], 'the other order, then the two lines of this one side by side: ' + seq.join(','));
    await page.click('#owNext');
    await page.waitForFunction(k => OrderWin.key() === k, a2);
    await page.waitForTimeout(600);
    await noBox('second line of the same order');
    await page.click('#owPrev');
    await page.waitForFunction(k => OrderWin.key() === k, a1);
    await page.click('#owPrev');
    await page.waitForFunction(k => OrderWin.key() === k, b1);
    await noBox('another order');
    await page.click('#owNext');
    await page.waitForFunction(k => OrderWin.key() === k, a1);
    // a view switched and back, then Esc: still no box, nothing written
    await page.click('.owTabsV [data-ow-view="timeline"]');
    await page.click('.owTabsV [data-ow-view="info"]');
    await noBox('a view switched and back');
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 });
    await page.evaluate(k => OrderWin.open(k), a2);
    await page.waitForFunction(() => OrderWin.isOpen());
    await noBox('opened again on its other line');
    await page.click('#owClose');
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 });
    await page.waitForTimeout(900);
    assert.equal(noteWrites.length, 0, 'opening, walking and closing orders writes no staff note: ' + noteWrites.join(' | '));
    assert.equal(serverNote(A.rid), 'Old note', 'the order\'s record keeps its note, untouched');

    // 2 · an order outside the pull, read slowly from the records: the Customer tab points at it from the first frame,
    //     and the message written there goes to that order's buyer
    let hold = null; const gate = new Promise(r => { hold = r; });
    await page.route(/\/\.netlify\/functions\/charmNestLibrary/, async r => { let b = {}; try { b = JSON.parse(r.request().postData() || '{}'); } catch (_) {} if (b.op === 'poolList' && String(b.orderId) === C.rid) await gate; return r.continue(); });
    await page.evaluate(rid => { OrderWin.openOrder(rid, {}); }, C.rid);
    await page.waitForFunction(() => OrderWin.isOpen() && !document.getElementById('owLoading').hidden);
    await page.waitForFunction(() => !document.getElementById('owPaneCust').hidden && !document.querySelector('#owPaneCust [data-cm=comp]').hidden, null, { timeout: 5000 });
    const orderReads = mail.filter(m => m.op === 'order').map(m => String(m.receiptId));
    assert.equal(orderReads[orderReads.length - 1], C.rid, 'the Customer tab reads the order on screen while it loads, not the one shown before: ' + orderReads.join(','));
    await page.fill('#owPaneCust [data-cm=input]', 'Hello, a question about your order');
    await page.click('#owPaneCust [data-cm=send]');
    assert(await until(() => mail.some(m => m.op === 'ask')), 'the message went to the inbox link (answered here)');
    const asked = mail.filter(m => m.op === 'ask').map(m => String(m.receiptId));
    assert.deepEqual(asked, [C.rid], 'a message written while it loads goes to this order\'s buyer: ' + asked.join(','));
    hold();
    await page.waitForFunction(() => document.getElementById('owLoading').hidden && /Janet Steptoe/.test(document.getElementById('owSub').textContent), null, { timeout: 15000 });
    await noBox('an order read from the records');
    assert.equal(noteWrites.length, 0, 'nothing written to the order\'s notes while it was read: ' + noteWrites.join(' | '));
    await page.click('#owClose');
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 });

    // 3 · On hold: a line skipped before (the order window has no Skip switch any more: the stored skip stays as it was) opened there,
    //     then released from On hold (Release hold: Review.repool), leaves that list; Previous and Next stay
    await page.evaluate(k => { const r = B.orders.byKey.get(k); r.state = 'skipped'; r.hold = r.reason = 'piece skipped by Test Operator'; }, b1);
    await page.evaluate(k => { const r = B.orders.byKey.get(k); r.state = 'skipped'; r.hold = r.reason = 'piece skipped by Test Operator'; }, a1);
    await page.evaluate(() => { Orders.view().pile = 'hold'; Orders.render(); });
    assert.deepEqual((await page.evaluate(() => Orders.visibleRows().map(r => r.key))).sort(), [a1, b1].sort(), 'On hold lists the two skipped lines');
    const first = await page.evaluate(() => Orders.visibleRows()[0].key);
    await page.evaluate(k => OrderWin.open(k), first);
    await page.waitForFunction(k => OrderWin.key() === k, first);
    assert(await page.evaluate(k => !document.getElementById('owSkip') && B.orders.byKey.get(k).state === 'skipped', first), 'the window offers no Skip switch, and opening it leaves the stored skip alone');
    await page.evaluate(k => Review.repool(B.orders.byKey.get(k)), first);
    await page.waitForFunction(k => B.orders.byKey.get(k).state !== 'skipped', first);
    const nav = await page.evaluate(() => ({ next: !document.getElementById('owNext').hidden, dis: document.getElementById('owNext').disabled, list: Orders.visibleRows().map(r => r.key) }));
    assert(nav.next && !nav.dis, 'Next stays once the line has left the On hold list: ' + JSON.stringify(nav));
    await page.click('#owNext');
    await page.waitForFunction(k => OrderWin.key() !== k, first);
    assert.equal(await page.evaluate(() => OrderWin.key()), nav.list[0], 'Next goes on to the line that was after it');
    await page.evaluate(() => { Orders.view().pile = null; Orders.render(); });

    assert.deepEqual(errors, [], 'no page errors');
    assert(!mail.some(m => m.op === 'ask' && String(m.receiptId) !== C.rid), 'nothing went to another order');
    console.log('  ✓ no notes box across the order\'s lines, a view switch and Esc, and nothing written; the Customer tab while an order is read; Next after leaving On hold');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('adv-orderwin OK')).catch(e => { console.error(e); process.exit(1); });
