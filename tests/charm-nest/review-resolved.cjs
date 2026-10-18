// RVRESOLVED (Paul, 18 Oct 2026): the Review tab's switch reads Open | Resolved, the Changed and Marked chips are gone, and
//   "All orders that get Release Hold, Print QR and all Custom orders must all be placed under the Resolved tab".
// The real sorter page in headless Chromium over the repo's fake site (bridge-server.cjs): made-up buyers, every request off
// the loopback aborted, nothing printed or written live. One world, checked as a person reads it:
//   1  the switch is two segments, "Open" and "Resolved" (its aria label too); no word of the bar says Completed
//   2  an order Etsy changed after the pull (orderChanged) and one a person sent to Review (reviewMark) have no chip of their
//      own: no Changed, no Marked chip in either segment; both are still listed in Open as plain cards (nothing vanishes),
//      and a filter or a note's Show naming them lets go, so the card is shown in all of Open
//   3  Resolved lists an order released to the nesting queue (Release Hold: its line's frontAt, as the run's lines keep it),
//      an order whose QR label was printed, and a custom order sent to the sheets, each once; Open lists none of them
//   4  a custom order still waiting for its label stays in Open, under Custom Orders; the waiting order with a question too
//   5  a card leaving Open when its order is released flies to the Resolved switch, with "moved to Resolved"
//   node tests/charm-nest/review-resolved.cjs [playwright-core dir]   (PW_DIR=... works too)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const line = (tid, sku, title, variations, metal) => ({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: variations.map(([name, value]) => ({ name, value })), metalKey: metal, metalLabel: metal === 'silver' ? 'Sterling Silver' : 'GF 14/20', personalization: [], buyerMessage: '' });
const ASK = '4189200001', CHG = '4189200002', MARK = '4189200003', REL = '4189200004', PRINTED = '4189200005', SENT = '4189200006', CHAIN = '4189200007';
const ORDERS = [
  order(ASK, [line(41892000011, 'ASK_11', 'Ask Charm', [['Metal', 'Sterling Silver']], 'silver')]),          // Unknown SKU: a question, Open
  order(CHG, [line(41892000021, 'CHG_21', 'Changed Charm', [['Metal', 'Sterling Silver']], 'silver')]),      // Etsy changed it after the pull
  order(MARK, [line(41892000031, 'MARK_31', 'Marked Charm', [['Metal', 'Sterling Silver']], 'silver')]),     // a person sent it to Review
  order(REL, [line(41892000041, 'REL_41', 'Released Charm', [['Metal', 'Sterling Silver']], 'silver')]),     // released from its hold
  order(PRINTED, [line(41892000051, 'PRINT_51', 'Printed Charm', [['Metal', 'Sterling Silver']], 'silver')]),// its QR label printed
  order(SENT, [line(41892000061, 'CUSTOM_0061', 'CUSTOM CHARM', [['Price', '28']])]),                       // a custom order sent to the sheets
  order(CHAIN, [line(41892000071, 'CHAIN_0071', 'CHAIN REPLACEMENT', [['Metal', 'Gold'], ['Length', '17 Inches']])])   // a custom order waiting for its label
];
const KEY = (rid, tid) => `${rid}_${tid}`;
const T0 = Date.now() - 3600000;
const PRINT_REC = { key: KEY(PRINTED, 41892000051), receiptId: PRINTED, transactionId: '41892000051', sku: 'PRINT_51', title: 'Printed Charm', category: 'Unknown SKU', kind: '', state: 'completed', how: 'print', completedAt: T0, completedBy: 'Name One', printedAt: T0, printedBy: 'Name One', prints: 1, lastPrintedAt: T0, lastPrintedBy: 'Name One', hasLabel: true, updatedAtMs: Date.now() - 5000 };

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [], ok = (cond, msg, extra) => { if (cond) console.log('  ✓ ' + msg); else { fails.push(msg); console.log('  ✗ ' + msg + (extra ? ' · ' + extra : '')); } };
  try {
    srv.st.put('Charm_Custom_Orders', PRINT_REC.key, PRINT_REC);
    const context = await browser.newContext({ viewport: { width: 1600, height: 950 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    const outside = [];
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js('window.pdfMake = { createPdf() { return { getBlob(cb) { cb(new Blob([""])); } }; } };'));
      if (/vfs_fonts/.test(u)) return r.fulfill(js(''));
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      if (/etsy/i.test(u)) outside.push(u);
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.CustomSheet && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders, K }) => {
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      await Orders.loadMaps(true);
      // the custom order sent to the sheets: its decision on the line (what Send to Sheet keeps)
      B.maps.customSent = Object.assign({}, B.maps.customSent, { [K.sent]: { at: Date.now() - 1800000, by: 'Name Two', id: 'designSent-rvresolved' } });
      // a person's Send to Review on MARK (the record the mark API keeps, charm-nest-review-mark.js)
      B.maps.reviewMarks = Object.assign({}, B.maps.reviewMarks, { [K.mark + '_review']: { key: K.mark + '_review', kind: 'reviewMark', category: 'Review', receiptId: K.mark, state: 'open', review: { state: 'open', markedAt: Date.now() - 600000, markedBy: 'Name Three' } } });
      Orders.interpretAll();
      // CHG, MARK and REL ask no question of their own (their designs are read as known here): only the change, the mark and the release say anything
      for (const k of [K.chg, K.markKey, K.rel]) { const r = B.orders.byKey.get(k); r.problems = []; r.state = 'pulled'; }
      // REL was on hold and released (Release Hold: OrderHold.release / Review.repool put it first in line, frontAt)
      B.orders.byKey.get(K.rel).frontAt = Date.now() - 300000;
      Review.syncOrderItems();
      const chg = B.orders.byKey.get(K.chg);
      Review.add({ kind: 'orderChanged', key: 'chg:' + chg.key, row: chg, old: { text: '', spec: chg.spec }, why: 'Etsy updated order ' + chg.order.receiptId + ' after the pull (variations)' });
      CN.setMode('review'); Review.view().cseg = 'open'; Review.view().filter = null; Review.render();
      const reconcile = Motion.reconcile; Motion.reconcile = function (host, nodes, opts) { if (opts && opts.leave && host.id === 'rvList') window.__leave = opts.leave; return reconcile.call(this, host, nodes, opts); };
    }, { orders: ORDERS, K: { sent: KEY(SENT, 41892000061), mark: MARK, markKey: KEY(MARK, 41892000031), chg: KEY(CHG, 41892000021), rel: KEY(REL, 41892000041) } });

    const bar = () => page.evaluate(() => ({
      segs: [...document.querySelectorAll('#reviewView .rvSeg [data-cseg]')].map(b => b.dataset.cseg),
      words: [...document.querySelectorAll('#reviewView .rvSeg button')].map(b => b.firstChild.textContent.trim()),
      aria: document.querySelector('#reviewView .rvSeg').getAttribute('aria-label'),
      titles: [...document.querySelectorAll('#reviewView .rvSeg button')].map(b => b.title),
      chips: [...document.querySelectorAll('#reviewView .ordBar .egTab')].map(b => ({ k: b.dataset.k, t: b.textContent.trim(), title: b.title })),
      text: document.querySelector('#reviewView .ordBar').textContent,
      rids: [...document.querySelectorAll('#rvList .reviewListRow')].map(n => n.dataset.rid),
      mkeys: [...document.querySelectorAll('#rvList .reviewListRow')].map(n => n.dataset.mkey),
      counts: [...document.querySelectorAll('#reviewView .rvSeg b')].map(b => +b.textContent),
      filter: Review.view().filter, cseg: Review.view().cseg
    }));
    const seg = async s => { await page.evaluate(s => { const b = document.querySelector(`#reviewView .rvSeg [data-cseg="${s}"]`); b.click(); }, s); await page.waitForFunction(s => document.querySelector(`#reviewView .rvSeg [data-cseg="${s}"].on`), s); return bar(); };

    // 1 · the switch
    const open = await seg('open');
    ok(JSON.stringify(open.segs) === '["open","done"]' && JSON.stringify(open.words) === '["Open","Resolved"]', 'the switch reads Open | Resolved', JSON.stringify(open.words));
    ok(open.aria === 'Open or resolved', 'its aria label says Open or resolved', open.aria);
    // (a title may still say what a piece was: "completed by hand"; no word names a Completed segment)
    ok(!/Completed/i.test(open.text) && !open.titles.some(t => /Completed/.test(t)) && !open.chips.some(c => /Completed|\bcompleted$/.test(c.title)), 'no word of the bar says Completed', JSON.stringify(open.titles));

    // 2 · no Changed, no Marked chip; both cards are in Open
    const noChip = b => !b.chips.some(c => c.k === 'orderChanged' || c.k === 'reviewMark' || /^(Changed|Marked)\d*$/.test(c.t));
    ok(noChip(open), 'Open: no Changed and no Marked chip', JSON.stringify(open.chips));
    ok(open.mkeys.includes('chg:' + KEY(CHG, 41892000021)), 'the changed order is listed in Open as a plain card', JSON.stringify(open.mkeys));
    ok(open.mkeys.includes('ord:rvm:' + MARK), 'the order sent to Review is listed in Open as a plain card', JSON.stringify(open.mkeys));
    ok(open.rids.includes(ASK), 'the order with a question is in Open');
    ok(open.rids.includes(CHAIN) && open.chips.some(c => c.k === 'customOrder'), 'the custom order waiting for its label stays in Open, under Custom Orders', JSON.stringify(open.chips));
    ok(![REL, PRINTED, SENT].some(r => open.rids.includes(r)), 'Open lists none of the released, printed or sent orders', JSON.stringify(open.rids));
    ok(open.counts[0] === open.rids.length, 'the Open count is what Open lists', JSON.stringify([open.counts, open.rids.length]));
    // a filter left on a chipless kind (a saved workspace, the Orders tab's "In Review" Show) lets go: the card is shown in all of Open
    await page.evaluate(() => { Review.view().filter = 'orderChanged'; Review.render(); });
    const f1 = await bar();
    ok(f1.filter === null && f1.mkeys.length === open.mkeys.length, 'a saved filter on Changed lets go', JSON.stringify([f1.filter, f1.mkeys.length]));
    await page.evaluate(mk => Review.showCard(mk, 'open', 'reviewMark'), 'ord:rvm:' + MARK);
    const f2 = await bar();
    ok(f2.filter === null && f2.cseg === 'open' && f2.mkeys.includes('ord:rvm:' + MARK), 'Show for an order sent to Review opens all of Open at its card', JSON.stringify([f2.filter, f2.cseg]));

    // 5 · a card leaving Open when its order is released flies to the Resolved switch (its old mkey is not drawn: as after the release)
    const flight = await page.evaluate(rid => { const r = window.__leave('ord:blocked:REL_41', { dataset: { rid } }); return r && { to: r.to, there: !!document.querySelector(r.to) }; }, REL);
    ok(flight && flight.to === '#reviewView .rvSeg [data-cseg="done"]' && flight.there, 'a card of a released order flies to the Resolved switch', JSON.stringify(flight));
    await page.evaluate(() => { const acted = document.querySelector('#rvList .reviewListRow'); if (acted) acted.click(); });

    // 3 · Resolved
    const done = await seg('done');
    ok(noChip(done), 'Resolved: no Changed and no Marked chip', JSON.stringify(done.chips));
    ok([REL, PRINTED, SENT].every(r => done.rids.filter(x => x === r).length === 1), 'Resolved lists the released, the printed and the sent order, once each', JSON.stringify(done.rids));
    ok(![ASK, CHG, MARK, CHAIN].some(r => done.rids.includes(r)), 'Resolved lists nothing that still waits', JSON.stringify(done.rids));
    ok(done.counts[1] === done.rids.length, 'the Resolved count is what Resolved lists', JSON.stringify([done.counts, done.rids.length]));
    const relCard = await page.evaluate(rid => { const n = document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"]`); return n && { why: n.querySelector('.rowExcerpt').textContent, label: n.querySelector('.purchaseLabel').textContent, q: n.querySelector('.queueLabel').textContent }; }, REL);
    ok(relCard && /^Released to the nesting queue/.test(relCard.why) && relCard.label === 'Released', 'the released order\'s card says it was released to the nesting queue', JSON.stringify(relCard));
    const sentQ = await page.evaluate(rid => document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"] .queueLabel`)?.textContent, SENT);
    ok(sentQ === 'Resolved · sent to sheet', 'the sent custom order\'s card says Resolved · sent to sheet', sentQ);
    // the empty list's words
    await page.evaluate(() => { Review.view().q = '0000000'; Review.render({ still: true }); });
    const emptyQ = await page.evaluate(() => document.querySelector('#rvList .libEmpty')?.textContent || '');
    await page.evaluate(() => { Review.view().q = ''; Review.render({ still: true }); });
    ok(/No matching order numbers/.test(emptyQ), 'a search with no match says so under Resolved', emptyQ);

    ok(outside.length === 0, 'no Etsy call', JSON.stringify(outside));
    ok(errors.length === 0, 'no page errors', JSON.stringify(errors));
  } catch (e) { fails.push('the test stopped: ' + (e && e.stack || e)); console.log('  FAIL ' + (e && e.stack || e)); }
  finally { await browser.close(); srv.close(); }
  if (fails.length) { console.log(`\n${fails.length} FAILED:\n- ` + fails.join('\n- ')); process.exit(1); }
  console.log('\nRVRESOLVED OK: Open | Resolved, no Changed / Marked chip (their cards stay in Open), and the released, printed and sent orders are under Resolved');
})().catch(e => { console.error(e); process.exit(1); });
