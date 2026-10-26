// SEALSHEET (Paul, 20 Oct 2026: "every action button stamps and leaves a custom seal beside it, persisted on the order so the seal travels with it everywhere until its conclusion"):
// Send to Sheet stamps its SENT TO SHEET seal (its own teal family) on the button it was pressed on; once the send is through the seal is kept on the order's Review document,
// sheet { at, by, how: "sheet", id } (the same pattern as Release Hold's release and Cancel Order's cancel); it rests beside the button (Send to Sheet in its done state on the
// Review card), and it is all still there after a reload. A failed send keeps no seal.
// Paul, 26 Oct 2026: "Remove this extra duplicate and unneeded 'Sent to Sheet' button + Seal and the second 'Seals' menu completely from the UI of the Detailed Order modal." The order window's
// header no longer keeps Send to Sheet's place after the send (#owSheetSent, the "Sent to Sheet" done-state button with its seal, is gone; charm-nest-bridge.js paintSent only removes a leftover one),
// and the window has no Seals row (#owSealRow). What this test proves now about the order window: for the sent order, before and after a reload, its header has no #owSheetSent, no "Sent to Sheet"
// button and no SENT TO SHEET seal, Send to Sheet itself is not shown, there is no Seals row, and the seal is drawn nowhere in the window outside its Timeline tab, while the window still knows the
// order's sheet seal (OrderWin.seals()). The seal itself is proved where it stays: on the order's Review document, in CharmNestReviewMark.sealsOf, and beside "Sent to Sheet" on the Review card
// (it is also in the Timeline, not checked here).
// The sorter in headless Chromium over the fake site (bridge-server.cjs: the real charmNestLibrary over an in-memory Firestore; nothing leaves the loopback).
//   node tests/charm-nest/seal-sheet.cjs [playwright node_modules]   (PW_DIR, CHROMIUM)
const path = require('path'), assert = require('assert/strict');
const here = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = w => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 22, 17) / 1000), RID = '4175438801', FAIL = '4175438802', WHO = 'Test Operator';
const order = rid => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-2) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: rid + '1', listingId: '18000' + rid.slice(-5), sku: 'OWL-CUSTOM-' + rid.slice(-2), title: 'Owl Charm Necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] });
const ORDERS = [order(RID), order(FAIL)], Seal_PREPARED = '#65737f';

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(here, 'node_modules');
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const context = await browser.newContext({ viewport: { width: 1440, height: 950 }, reducedMotion: 'reduce' });
  let passed = 0; const ok = (c, what) => { assert.ok(c, what); passed++; console.log('  ok   ' + what); };
  try {
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = []; page.setDefaultTimeout(30000);
    page.on('pageerror', e => errors.push(String(e)));
    const boot = async () => {
      await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && window.CharmNestReviewMark && CN.S.cloud.ok === true, null, { timeout: 60000 });
      await page.evaluate(async orders => {
        await Orders.loadMaps(true);
        for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); if (B.orders.byKey.get(key)) continue; const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
        Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
        const day = new Date().toISOString().slice(0, 10);
        if (!B.run) B.run = { runId: `run-${day}-ss`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
      }, ORDERS);
    };
    await boot();
    const card = rid => `#rvList .reviewListRow[data-rid="${rid}"]`;
    const dropDesign = async rid => {
      await page.evaluate(() => { CN.setMode('review'); Review.render(); });
      await page.waitForSelector(card(rid));
      await page.evaluate(({ sel, text }) => { const dt = new DataTransfer(); dt.items.add(new File([text], 'owl.dxf')); const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); { const a = document.querySelector('#cuAsk [data-ask-yes]'); if (a) a.click(); } }, { sel: card(rid), text: DXF(18) });
      await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === 1, null, { timeout: 30000 });
      await page.click('#cuDlg .cuFile .cuM[data-m="gold"]');
    };

    // ── 1 · a failed send: no seal kept ──
    await dropDesign(FAIL);
    srv.st.fail.charmNestLibrary = true;   // (every write refused: the send is saved for retry)
    await page.evaluate(() => { window.__stamped = 0; const mo = new MutationObserver(() => { if (document.querySelector('.cuSealHost .seal.seal-sheet')) window.__stamped++; }); mo.observe(document.body, { subtree: true, childList: true }); window.__mo = mo; document.querySelector('#cuDlg [data-send]').click(); });
    await page.waitForFunction(() => [...document.querySelectorAll('#toasts .toast')].some(t => /send saved for retry|unavailable|retry/i.test(t.textContent)), null, { timeout: 30000 }).catch(() => {});
    await page.waitForTimeout(1500);
    srv.st.fail.charmNestLibrary = false;
    const failed = await page.evaluate(rid => ({ rec: CharmNestReviewMark.sheetSent(rid), sent: !!CustomSheet.decisionOf(Orders.rows().find(x => String(x.order.receiptId) === rid)) }), FAIL);
    ok(!failed.sent && !failed.rec && !(srv.st.doc('Charm_Custom_Orders', FAIL + '_review') || {}).sheet, 'a send that failed keeps no SENT TO SHEET seal, on the page or in the cloud: ' + JSON.stringify(failed) + JSON.stringify(srv.st.doc('Charm_Custom_Orders', FAIL + '_review') || null));
    await page.evaluate(() => { try { CustomSheet.shut(); } catch (_) {} const d = document.querySelector('#cuDlg[open]'); if (d) d.close(); });

    // ── 2 · Send to Sheet: the stamp on the button, then the seal kept on the order and beside the button ──
    await dropDesign(RID);
    await page.evaluate(() => { window.__stamped = 0; window.__told = []; document.addEventListener('cn-order-seal', e => window.__told.push(e.detail && e.detail.rid)); document.querySelector('#cuDlg [data-send]').click(); });
    await page.waitForFunction(rid => { const row = Orders.rows().find(x => String(x.order.receiptId) === rid); return !!CustomSheet.decisionOf(row) && !document.querySelector('#cuDlg[open]'); }, RID, { timeout: 30000 });
    ok(await page.evaluate(() => window.__stamped > 0), 'Send to Sheet stamps its SENT TO SHEET seal on the button it was pressed on');
    await page.waitForFunction(rid => { const r = CharmNestReviewMark.sheetSent(rid); return r && !(B.maps.reviewMarks[rid + '_review'] || {}).pending; }, RID, { timeout: 15000 });
    const cloud = srv.st.doc('Charm_Custom_Orders', RID + '_review');
    ok(cloud && cloud.sheet && cloud.sheet.how === 'sheet' && cloud.sheet.by === WHO && +cloud.sheet.at > 0 && /^custom-sheet:/.test(cloud.sheet.id), 'the seal is kept on the order\'s Review document: sheet { at, by, how: "sheet", id }: ' + JSON.stringify(cloud && cloud.sheet));
    const sent = await page.evaluate(rid => { const row = Orders.rows().find(x => String(x.order.receiptId) === rid); return CustomSheet.sentOf(row).sent || CustomSheet.decisionOf(row); }, RID);
    ok(+cloud.sheet.at === +sent.at && cloud.sheet.id === sent.id, 'the seal is the send\'s own: the same time and id as its decision');
    ok(await page.evaluate(rid => CharmNestReviewMark.sealsOf(rid).some(s => s.how === 'sheet'), RID), 'CharmNestReviewMark.sealsOf lists it with the order\'s other seals, by kind');
    ok(await page.evaluate(rid => (window.__told || []).includes(rid), RID), 'the seal written fires "cn-order-seal" {rid}, so an open order window repaints its seals at once');
    const beside = async () => page.evaluate(rid => {
      CN.setMode('review'); const seg = document.querySelector('#reviewView .rvSeg [data-cseg="done"]'); if (seg) seg.click(); Review.render();
      const n = document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"]`), b = n && n.querySelector('[data-cu-sent]'), row = b && b.nextElementSibling, s = row && row.matches('.sealRow') && row.querySelector('.seal.seal-sheet'), svg = s && s.querySelector('svg');
      return { card: !!n, btn: !!b, label: b ? b.textContent : '', cls: b ? b.className : '', seal: !!s, family: svg ? svg.dataset.sealFamily : '', words: svg ? svg.textContent : '', ink: svg ? svg.querySelector('g').getAttribute('fill') : '', want: Seal.FAMILY.sheet && Seal.FAMILY.sheet.ink, aria: s ? s.getAttribute('aria-label') : '' };
    }, RID);
    let b = await beside();
    ok(b.card && b.btn && b.label === 'Sent to Sheet' && /sealedSheet/.test(b.cls), 'the Review card keeps Send to Sheet in its done state, "Sent to Sheet", in its seal\'s teal: ' + JSON.stringify(b));
    ok(b.seal && b.family === 'sheet' && /SENT TO SHEET/.test(b.words) && b.ink === b.want && b.ink !== Seal_PREPARED && new RegExp('Sent to sheet by ' + WHO).test(b.aria), 'its SENT TO SHEET seal rests right after it, in its own ink, with who and when: ' + JSON.stringify(b));
    // the order window (Paul, 26 Oct 2026): no "Sent to Sheet" button + seal in its header, no Seals row; the seal is still the order's (the window's own reader has it)
    const ow = async () => {
      const key = await page.evaluate(rid => { const row = Orders.rows().find(x => String(x.order.receiptId) === rid); OrderWin.open(row.key); return row.key; }, RID);
      await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k && !!document.querySelector('#orderWin .owHead #owPos'), key, { timeout: 20000 });
      await page.waitForTimeout(1200);
      return page.evaluate(() => {
        const head = document.querySelector('#orderWin .owHead'), send = document.getElementById('owSendSheet');
        const outside = sel => [...document.getElementById('orderWin').querySelectorAll(sel)].filter(n => !n.closest('[data-view="timeline"]')).length;   // (anywhere in the window but its Timeline tab)
        return { open: OrderWin.isOpen(), sheetSent: !!document.getElementById('owSheetSent'), headBtn: !!head.querySelector('[data-cu-sent], .cuSentState'), headSeal: !!head.querySelector('.seal.seal-sheet, [data-seal-kind="sheet"], svg[data-seal-family="sheet"]'),
          sendShown: !!send && !send.hidden && !send.closest('[hidden]'), row: !!document.getElementById('owSealRow') || !!document.querySelector('.owSealRow, .owSealRowHd, [data-testid="ow-seal-row"]'),
          known: (OrderWin.seals() || []).some(s => s.kind === 'sheet'), elsewhere: { sentBtns: outside('[data-cu-sent]'), sheetSeals: outside('.seal.seal-sheet, [data-seal-kind="sheet"]') } };
      });
    };
    const none = w => w.open && !w.sheetSent && !w.headBtn && !w.headSeal && !w.sendShown && !w.row;
    let w = await ow();
    ok(none(w), 'the order window\'s header has no "Sent to Sheet" button and no SENT TO SHEET seal (no #owSheetSent; Send to Sheet itself is not shown for a sent order), and the window has no Seals row: ' + JSON.stringify(w));
    ok(w.known && !w.elsewhere.sentBtns && !w.elsewhere.sheetSeals, 'the window still knows the order\'s sheet seal (OrderWin.seals()) but draws it nowhere outside its Timeline tab (it stays on the Review card): ' + JSON.stringify(w.elsewhere));
    await page.evaluate(() => { try { OrderWin.close(); } catch (_) {} });

    // ── 3 · a reload: the seal comes back from the cloud ──
    await boot();
    await page.waitForFunction(rid => !!CharmNestReviewMark.sheetSent(rid), RID, { timeout: 30000 });
    const back = await page.evaluate(rid => CharmNestReviewMark.sheetSent(rid), RID);
    ok(back && back.how === 'sheet' && back.by === WHO && back.at === +cloud.sheet.at, 'after a reload the order\'s seal record is read back as it was kept');
    await page.waitForFunction(rid => { const row = Orders.rows().find(x => String(x.order.receiptId) === rid); return row && CustomSheet.decisionOf(row); }, RID, { timeout: 30000 }).catch(() => {});
    b = await beside();
    ok(b.btn && b.seal && b.family === 'sheet', 'after a reload the Review card still shows the seal beside Sent to Sheet: ' + JSON.stringify(b));
    w = await ow();
    ok(none(w) && w.known && !w.elsewhere.sentBtns && !w.elsewhere.sheetSeals, 'after a reload the order window\'s header still has no "Sent to Sheet" button or seal and no Seals row; the window still knows the seal and draws it nowhere outside its Timeline tab: ' + JSON.stringify(w));
    assert.deepEqual(errors, [], 'no page errors');
  } finally { await context.close(); await browser.close(); await srv.close(); }
  console.log(`seal-sheet OK (${passed} checks)`);
})().catch(e => { console.error(e); process.exit(1); });
