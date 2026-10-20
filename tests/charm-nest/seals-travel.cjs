// SEALTRAVEL (Paul, 20 Oct 2026): "Depending on which button is pressed, the order may go to different tabs or folders, however all the particular seals must always follow this order. For example, Print QR label and Cancel Order
// can be part of the same order, so both those seals would have to travel with a given order until that order's eventual conclusion. All of these seals must be visible in all places where a given button is relevant."
// The real sorter page in headless Chromium over the repo's fake site (bridge-server.cjs): made-up buyers, nothing off the loopback. The seals are seeded in the cloud's records only (the custom record of a QR print, and the
// order's Review document: { at, by, how, id } under "release", "cancel" and "sheet", the release seal's own shape), so what the page shows comes from the same cloud record a reload or another computer reads.
//   1  an order with a QR print seal and an ORDER CANCELLED seal shows both on its card in Orders › Cancelled
//   2  an order with QR print, Release Hold and Sent to Sheet seals shows all three on its card in Review › Resolved, and on its Orders card
//   3  an order held with Hold Order shows ORDER HELD on its Orders card and its Review card (the Review document's mark)
//   4  after a reload: the same
//   5  a kind the page does not know is drawn in a neutral ink with its stored label (CNSealTravel.html)
//   node tests/charm-nest/seals-travel.cjs [playwright-core dir]   (PW_DIR=... works too)
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 22, 17) / 1000);
const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const line = (tid, sku, title) => ({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'Sterling Silver' }], metalKey: 'silver', metalLabel: 'Sterling Silver', personalization: [], buyerMessage: '' });
const CXA = '4207100001', TRV = '4207100002', HLD = '4207100003', TA = 42071000011, TB = 42071000021, TH = 42071000031;
const T0 = Date.now() - 3 * 3600000;
const printRec = (rid, tid, sku, at) => ({ key: `${rid}_${tid}`, receiptId: rid, transactionId: String(tid), sku, title: 'Travel Charm', category: 'Unknown SKU', kind: '', state: 'completed', how: 'print', completedAt: at, completedBy: 'Name One', printedAt: at, printedBy: 'Name One', prints: 1, lastPrintedAt: at, lastPrintedBy: 'Name One', hasLabel: true, updatedAtMs: Date.now() - 5000 });
const seal = (how, rid, at, by) => ({ at, by, how, id: `${how}:${rid}:${at}` });
const reviewDoc = (rid, extra) => Object.assign({ key: rid + '_review', kind: 'reviewMark', category: 'Review', receiptId: rid, state: 'open', hasLabel: false, updatedAtMs: Date.now() - 4000 }, extra);
const CANCEL_REC = { orderId: CXA, by: 'Name Two', why: 'Customer asked', at: T0 + 1800000, buyer: 'Buyer 0001', placedAt: T0 - 86400000, shipBy: 0, sheets: [], lines: [{ transactionId: String(TA), sku: 'TRAVEL_A', title: 'Travel Charm', quantity: 1, material: 'silver' }] };

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [], ok = (cond, msg, extra) => { if (cond) console.log('  ✓ ' + msg); else { fails.push(msg); console.log('  ✗ ' + msg + (extra ? ' · ' + extra : '')); } };
  try {
    // the cloud's records: CXA printed, then cancelled; TRV printed, released with Release Hold, sent to its sheet
    const C = 'Charm_Custom_Orders';
    srv.st.put(C, `${CXA}_${TA}`, printRec(CXA, TA, 'TRAVEL_A', T0));
    srv.st.put(C, CXA + '_review', reviewDoc(CXA, { cancel: seal('cancel', CXA, T0 + 1800000, 'Name Two') }));
    srv.st.put(C, `${TRV}_${TB}`, printRec(TRV, TB, 'TRAVEL_B', T0 + 60000));
    srv.st.put(C, TRV + '_review', reviewDoc(TRV, { release: seal('releaseHold', TRV, T0 + 1200000, 'Name Three'), sheet: seal('sheet', TRV, T0 + 2400000, 'Name Four') }));
    // HLD: held with Hold Order (the Review document's open mark: ORDER HELD)
    srv.st.put(C, HLD + '_review', reviewDoc(HLD, { review: { state: 'open', markedAt: T0 + 600000, markedBy: 'Name Six' } }));
    const context = await browser.newContext({ viewport: { width: 1600, height: 950 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js('window.pdfMake = { createPdf() { return { getBlob(cb) { cb(new Blob([""])); } }; } };'));
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    // the cancel records, as charmNestLibrary answers them
    await page.route(/\/\.netlify\/functions\/charmNestLibrary/, r => {
      let b = {}; try { b = r.request().postDataJSON() || {}; } catch (_) {}
      const reply = body => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
      if (b.op === 'cancelList') return b.idsOnly ? reply({ ids: [CXA], truncated: false }) : reply({ list: [CANCEL_REC], truncated: false });
      if (b.op === 'cancelCheck') { const out = {}; for (const id of b.orderIds || []) if (String(id) === CXA) out[id] = { at: CANCEL_REC.at, by: CANCEL_REC.by, why: CANCEL_REC.why, source: 'sorter', sheets: [] }; return reply({ cancelled: out, now: Date.now() }); }
      return r.continue();
    });

    const boot = async () => {
      await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Cancelled && window.CNSealTravel && CN.S.cloud.ok === true, null, { timeout: 60000 });
      await page.evaluate(async ({ os }) => {
        for (const o of os) if (!B.orders.byKey.has(CharmNestOrders.lineKey(o, o.lines[0]))) { const key = CharmNestOrders.lineKey(o, o.lines[0]); const row = { key, order: o, line: o.lines[0], arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
        Orders.interpretAll();
        await Orders.loadMaps(true); await Cancelled.load(true); await Cancelled.history();
        Orders.interpretAll();
        for (const r of B.orders.rows) if (os.some(o => String(r.order.receiptId) === o.receiptId)) { r.problems = []; r.state = 'pulled'; }
        Review.syncOrderItems();
      }, { os: [order(TRV, [line(TB, 'TRAVEL_B', 'Travel Charm')]), order(HLD, [line(TH, 'TRAVEL_H', 'Held Charm')])] });
    };
    const kindsIn = sel => page.evaluate(sel => { const n = document.querySelector(sel); if (!n) return null;
      return { print: n.querySelectorAll('.seal.seal-print').length, cancel: n.querySelectorAll('[data-seal-kind="cancel"] .seal').length, release: n.querySelectorAll('[data-rel-seal] .seal').length, sheet: n.querySelectorAll('[data-seal-kind="sheet"] .seal').length, held: n.querySelectorAll('[data-seal-kind="held"] .seal').length, text: n.textContent.replace(/\s+/g, ' ').slice(0, 400) }; }, sel);
    const cancelledCard = async () => { await page.evaluate(() => { CN.setMode('orders'); Orders.showPile ? Orders.showPile('cancelled') : document.querySelector('#ordChips [data-pile="cancelled"]').click(); Orders.renderNow(); }); await page.waitForFunction(rid => document.querySelector(`#ordBody .cxRow[data-rid="${rid}"]`), CXA); return kindsIn(`#ordBody .cxRow[data-rid="${CXA}"]`); };
    const resolvedCard = async () => { await page.evaluate(() => { CN.setMode('review'); Review.view().cseg = 'done'; Review.view().filter = null; Review.render(); }); await page.waitForFunction(rid => document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"]`), TRV); return kindsIn(`#rvList .reviewListRow[data-rid="${TRV}"]`); };
    const ordersCard = async rid => {
      await page.evaluate(() => { CN.setMode('orders'); if (Orders.showPile) Orders.showPile(''); Orders.renderNow(); });
      await page.waitForFunction(rid => [...document.querySelectorAll('#ordBody [data-key], #ordBody .ordGroup')].some(n => n.textContent.includes(rid)), rid);
      return page.evaluate(rid => { document.querySelectorAll('[data-test-card]').forEach(x => x.removeAttribute('data-test-card')); const n = [...document.querySelectorAll('#ordBody [data-key], #ordBody .ordGroup')].find(x => x.textContent.includes(rid) && !x.parentElement.closest('[data-key], .ordGroup')); n.setAttribute('data-test-card', '1'); return true; }, rid).then(() => kindsIn('#ordBody [data-test-card="1"]'));
    };
    const openCard = async rid => { await page.evaluate(() => { CN.setMode('review'); Review.view().cseg = 'open'; Review.view().filter = null; Review.render(); }); await page.waitForFunction(rid => document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"]`), rid); return kindsIn(`#rvList .reviewListRow[data-rid="${rid}"]`); };
    const all3 = c => !!c && c.print >= 1 && c.release === 1 && c.sheet === 1;

    for (const pass of ['first load', 'after a reload']) {
      await boot();
      const cx = await cancelledCard();
      ok(cx && cx.print >= 1 && cx.cancel === 1, `${pass}: the cancelled order's card in Orders › Cancelled shows its QR print seal and its ORDER CANCELLED seal`, JSON.stringify(cx));
      ok(cx && /Order Cancelled/.test(cx.text) && /by Name Two/.test(cx.text), `${pass}: the ORDER CANCELLED seal says its label and who`, cx && cx.text);
      const rv = await resolvedCard();
      ok(all3(rv), `${pass}: Review › Resolved: the order's card shows its QR print, Release Hold and Sent to Sheet seals`, JSON.stringify(rv));
      const oc = await ordersCard(TRV);
      ok(all3(oc), `${pass}: its Orders card shows the QR print, Release Hold and Sent to Sheet seals`, JSON.stringify(oc));
      const ho = await ordersCard(HLD), hr = await openCard(HLD);
      ok(ho && ho.held === 1 && /Order Held/.test(ho.text) && hr && hr.held === 1, `${pass}: an order held with Hold Order shows its ORDER HELD seal on its Orders card and its Review card`, JSON.stringify([ho, hr]));
      if (pass === 'first load') await page.evaluate(() => sessionStorage.setItem('__seeded', '1'));
    }
    // 4 · a kind the page does not know: neutral ink, its stored label
    const odd = await page.evaluate(() => { const h = CNSealTravel.html([{ kind: 'giftWrap', how: 'giftWrap', at: Date.now() - 60000, by: 'Name Five', label: 'Gift Wrapped' }]); const d = document.createElement('div'); d.innerHTML = h; const s = d.querySelector('[data-seal-kind="giftWrap"]'); return s && { label: s.querySelector('b').textContent, style: s.getAttribute('style'), svg: !!s.querySelector('svg') }; });
    ok(odd && odd.label === 'Gift Wrapped' && /--stInk:#5f625c/.test(odd.style) && odd.svg, 'an unknown kind is drawn with its stored label in the neutral ink', JSON.stringify(odd));
    ok(errors.length === 0, 'no page errors', JSON.stringify(errors));
  } catch (e) { fails.push('the test stopped: ' + (e && e.stack || e)); console.log('  FAIL ' + (e && e.stack || e)); }
  finally { await browser.close(); srv.close(); }
  if (fails.length) { console.log(`\n${fails.length} FAILED:\n- ` + fails.join('\n- ')); process.exit(1); }
  console.log('\nSEALTRAVEL OK: every seal an order collected travels with it: Cancelled, Resolved and its Orders card, after a reload too');
})().catch(e => { console.error(e); process.exit(1); });
