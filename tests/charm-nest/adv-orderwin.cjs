// Adversarial (28 Sep, wave 2): what the old order window did must still work in the full-screen view without losing
// anyone's work. The staff note is the order's, not the line's: typed on one line of an order and carried to its next
// line by Next, it is what that line shows (it showed the old note, and the next edit there put the old words back); a
// note typed while an order outside the pull is still being read is kept and saved, and the Customer tab points at the
// order on screen from the first frame (it kept the order shown before, so a message typed meanwhile went to that
// buyer); Previous and Next stay once the line leaves the list it was opened from (a skip undone in On hold).
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
    const noteIs = v => page.waitForFunction(v => document.getElementById('owNote').value === v, v, { timeout: 5000 }).then(() => true, () => false);
    const serverNote = rid => (srv.st.doc('Brites_Orders', rid) || {})['Staff Note'];
    const until = async (f, ms = 6000) => { for (const t0 = Date.now(); !f(); ) { if (Date.now() - t0 > ms) return false; await new Promise(r => setTimeout(r, 100)); } return true; };

    // 1 · a note typed on the first line of an order, Next to its second line: the same order, the same note
    await page.evaluate(k => OrderWin.open(k), a1);
    await page.waitForFunction(() => OrderWin.isOpen() && document.getElementById('owNote').value === 'Old note');
    const seq = await page.evaluate(() => Orders.visibleRows().map(r => r.key));
    assert.deepEqual(seq, [b1, a1, a2], 'the other order, then the two lines of this one side by side: ' + seq.join(','));
    // (the save is slow: the order's record still says the old note when the next line reads it)
    let saved = null; const slow = new Promise(r => { saved = r; });
    await page.route(/\/\.netlify\/functions\/firebaseOrders/, async r => { if (r.request().method() === 'POST' && /staffNote/.test(r.request().postData() || '')) await slow; return r.continue(); });
    await page.fill('#owNote', 'Call the buyer about the chain');
    await page.click('#owNext');
    await page.waitForFunction(k => OrderWin.key() === k, a2);
    await page.waitForTimeout(600);   // (its note read from the record meanwhile)
    assert(await noteIs('Call the buyer about the chain'), 'the next line of the same order shows the note just typed, not the old one: ' + await page.inputValue('#owNote'));
    saved();   // (the route lets everything through from here)
    assert(await until(() => serverNote(A.rid) === 'Call the buyer about the chain'), 'saved to the order');
    // back to its first line, on to another order and back: still the note typed
    await page.click('#owPrev');
    await page.waitForFunction(k => OrderWin.key() === k, a1);
    assert(await noteIs('Call the buyer about the chain'), 'and on its first line');
    await page.click('#owPrev');
    await page.waitForFunction(k => OrderWin.key() === k, b1);
    assert(await noteIs(''), 'another order: its own (empty) note');
    await page.click('#owNext');
    await page.waitForFunction(k => OrderWin.key() === k, a1);
    assert(await noteIs('Call the buyer about the chain'), 'back on the order: the note typed');
    // typed, then a view switched and back, then Esc: kept and saved
    await page.fill('#owNote', 'Call the buyer about the chain · gift wrap');
    await page.click('.owTabsV [data-ow-view="timeline"]');
    await page.click('.owTabsV [data-ow-view="info"]');
    assert(await noteIs('Call the buyer about the chain · gift wrap'), 'a view switched and back keeps the note');
    await page.focus('#owNote'); await page.keyboard.type(' · rush');
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 });
    assert(await until(() => serverNote(A.rid) === 'Call the buyer about the chain · gift wrap · rush'), 'Esc while typing saves the note: ' + serverNote(A.rid));
    await page.evaluate(k => OrderWin.open(k), a2);
    assert(await noteIs('Call the buyer about the chain · gift wrap · rush'), 'opened again on its other line: the note as saved');
    await page.click('#owClose');
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 });
    // the record read as the order opens answers late, after a note typed meanwhile was saved: the note typed stays
    let late = null; const answer = new Promise(r => { late = r; });
    await page.route(/\/\.netlify\/functions\/firebaseOrders\?orderId=/, async r => { const res = await r.fetch(); await answer; return r.fulfill({ response: res }); });
    await page.evaluate(k => OrderWin.open(k), a1);
    await page.waitForFunction(() => OrderWin.isOpen());
    await page.fill('#owNote', 'Newest words');
    await page.click('#owSub');
    assert(await until(() => serverNote(A.rid) === 'Newest words'), 'saved');
    late(); await page.waitForTimeout(500);
    assert(await noteIs('Newest words'), 'an older answer of the record does not put the old note back: ' + await page.inputValue('#owNote'));
    await page.click('#owClose');
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 });

    // 2 · an order outside the pull, read slowly from the records: the Customer tab points at it from the first frame,
    //     and a note typed meanwhile is kept and saved (to that order)
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
    const ro = await page.evaluate(() => document.getElementById('owNote').readOnly);
    if (!ro) {
      await page.fill('#owNote', 'Typed while it loads');
      await page.click('#owSub');   // (the note left before the order is read)
    }
    hold();
    await page.waitForFunction(() => document.getElementById('owLoading').hidden && /Janet Steptoe/.test(document.getElementById('owSub').textContent), null, { timeout: 15000 });
    if (!ro) {
      // (the note box stays open while the order is read: what was typed is kept and the order's own note put before it)
      assert(await noteIs('C note from the shop\nTyped while it loads'), 'the note typed while the order was read is still there, after its own note: ' + await page.inputValue('#owNote'));
      assert(await until(() => serverNote(C.rid) === 'C note from the shop\nTyped while it loads'), 'and saved to that order: ' + serverNote(C.rid));
    } else {
      assert(await noteIs('C note from the shop'), 'the order\'s own note once it is read: ' + await page.inputValue('#owNote'));
      assert.equal(await page.evaluate(() => document.getElementById('owNote').readOnly), false, 'the note opens for typing once the order is read');
    }
    await page.click('#owClose');
    await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 3000 });

    // 3 · On hold: a skipped line opened there, its skip undone, leaves that list; Previous and Next stay
    await page.evaluate(k => { const r = B.orders.byKey.get(k); r.state = 'skipped'; r.hold = r.reason = 'piece skipped by Test Operator'; }, b1);
    await page.evaluate(k => { const r = B.orders.byKey.get(k); r.state = 'skipped'; r.hold = r.reason = 'piece skipped by Test Operator'; }, a1);
    await page.evaluate(() => { Orders.view().pile = 'hold'; Orders.render(); });
    assert.deepEqual((await page.evaluate(() => Orders.visibleRows().map(r => r.key))).sort(), [a1, b1].sort(), 'On hold lists the two skipped lines');
    const first = await page.evaluate(() => Orders.visibleRows()[0].key);
    await page.evaluate(k => OrderWin.open(k), first);
    await page.waitForFunction(k => OrderWin.key() === k && !document.getElementById('owSkipBox').hidden, first);
    await page.click('#owSkip');
    await page.waitForFunction(k => B.orders.byKey.get(k).state !== 'skipped', first);
    const nav = await page.evaluate(() => ({ next: !document.getElementById('owNext').hidden, dis: document.getElementById('owNext').disabled, list: Orders.visibleRows().map(r => r.key) }));
    assert(nav.next && !nav.dis, 'Next stays once the line has left the On hold list: ' + JSON.stringify(nav));
    await page.click('#owNext');
    await page.waitForFunction(k => OrderWin.key() !== k, first);
    assert.equal(await page.evaluate(() => OrderWin.key()), nav.list[0], 'Next goes on to the line that was after it');
    await page.evaluate(() => { Orders.view().pile = null; Orders.render(); });

    assert.deepEqual(errors, [], 'no page errors');
    assert(!mail.some(m => m.op === 'ask' && String(m.receiptId) !== C.rid), 'nothing went to another order');
    console.log('  ✓ the note follows the order across its lines, a view switch and Esc; the Customer tab and the note while an order is read; Next after leaving On hold');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('adv-orderwin OK')).catch(e => { console.error(e); process.exit(1); });
