// Decided and Completed are one Completed tab in Review (Paul, 2 Oct: "Combined a decided and completed together into one
// completed tab that is global for the main Review Tab"). It replaces the rule of 29 Sep that kept an order sent to a sheet
// out of Completed (this file's old name still says so). The switch is Open | Completed, with no Decided segment; Completed
// lists, in one list with one count, an Unknown SKU card sent with its own design (Send to Sheet) and an order completed by
// hand (Complete Order), each once, newest first; the filter chips filter that list by kind; records of older sends
// kept in the workspace stay stored and are still not listed; and a workspace saved while Review still had a Decided
// segment (its view says "sent") opens on Completed after a reload. Runs the sorter in headless Chromium against the local
// fake site (bridge-server.cjs): every request off the loopback is aborted, nothing is printed or written live.
//   node tests/charm-nest/sheet-not-completed.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const line = (tid, sku, title, variations, metal) => ({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: variations.map(([name, value]) => ({ name, value })), metalKey: metal, metalLabel: metal === 'silver' ? 'Sterling Silver' : 'GF 14/20', personalization: [], buyerMessage: '' });
const SENT = '4179100001', HAND = '4179100002';
const ORDERS = [
  order(SENT, [line(41791000011, 'ROSE_88', 'Rose Charm', [['Metal', 'Sterling Silver']], 'silver')]),      // Unknown SKU: sent to a sheet with its own design
  order(HAND, [line(41791000021, 'DAVID_99', 'David Star Necklace', [['Metal Choice', 'Gold Filled']], 'gold')])   // Unknown SKU: completed by hand
];
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DESIGN_DXF = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, 18, 20, 0, 42, 0.4, 10, 18, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, 9, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
// what the workspace held from the days sends were recorded under Completed (their words only, or with a flag): they stay
// stored, whole, and are still not listed
const OLD = [
  { key: 'ord:sku:LEGACY_1:4179100003', row: null, kind: 'unmatchedSku', why: "sent to the sheets with the order's own designs", lines: 1, orders: ['4179100003'], by: 'Test Operator', t: Date.now() - 3600000 },
  { key: 'ord:opt:Style\u0000Wavy:4179100004', row: null, kind: 'needsMapping', why: 'sent', how: 'sheet', lines: 1, orders: ['4179100004'], by: 'Test Operator', t: Date.now() - 7200000 }
];

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1720, height: 950 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    const outside = [];
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js('window.pdfMake = { createPdf() { return { getBlob(cb) { cb(new Blob([""])); } }; } };'));
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill(js(''));
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
    await page.evaluate(async ({ orders, old }) => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Review.settled().push(...old);
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      // every note the list shows, kept for the checks below
      // where a card that leaves the list flies to is decided by the list's last drawing: kept to ask it
      const reconcile = Motion.reconcile; Motion.reconcile = function (host, nodes, opts) { if (opts && opts.leave && host.id === 'rvList') window.__leave = opts.leave; return reconcile.call(this, host, nodes, opts); };
      window.__notes = []; new MutationObserver(() => document.querySelectorAll('.mNoteT').forEach(n => { if (!window.__notes.includes(n.textContent)) window.__notes.push(n.textContent); })).observe(document.body, { childList: true, subtree: true, characterData: true });
    }, { orders: ORDERS, old: OLD });
    const card = rid => `#rvList .reviewListRow[data-rid="${rid}"]`;
    // a press while a wooden stamp is still coming down is swallowed (by design): wait for the seals to rest, then press
    const press = async sel => { await page.evaluate(() => Seal.whenIdle()); await page.click(sel); };
    // the switch as drawn: its segments, the Completed count, whether Completed is the one on
    const bar = () => page.evaluate(() => ({ segs: [...document.querySelectorAll('#reviewView .rvSeg [data-cseg]')].map(b => b.dataset.cseg), words: [...document.querySelectorAll('#reviewView .rvSeg button')].map(b => b.firstChild.textContent.trim()), on: (document.querySelector('#reviewView .rvSeg .on') || {}).dataset?.cseg, n: +(document.querySelector('#reviewView .rvSeg [data-cseg="done"] b') || {}).textContent, sentSeg: !!document.querySelector('#reviewView [data-cseg="sent"]'), aria: document.querySelector('#reviewView .rvSeg').getAttribute('aria-label'), text: document.querySelector('#reviewView .ordBar').textContent }));
    // what Completed shows: its count on the switch, the cards listed (in the order drawn), the chips and their counts
    const completed = async count => {
      await page.waitForFunction(n => document.querySelector('#reviewView .rvSeg [data-cseg="done"].on') && document.querySelectorAll('#rvList .reviewListRow').length === n, count, { timeout: 15000 }).catch(async e => {
        throw new Error('Completed did not draw ' + count + ' cards: ' + JSON.stringify(await page.evaluate(() => ({ on: (document.querySelector('#reviewView .rvSeg .on') || {}).dataset?.cseg, rows: [...document.querySelectorAll('#rvList .reviewListRow')].map(n => n.dataset.rid), counts: [...document.querySelectorAll('#reviewView .rvSeg b')].map(b => b.textContent), cseg: Review.view().cseg, empty: (document.querySelector('#rvList .libEmpty') || {}).textContent }))));
      });
      return page.evaluate(() => ({ n: +document.querySelector('#reviewView .rvSeg [data-cseg="done"] b').textContent, rids: [...document.querySelectorAll('#rvList .reviewListRow')].map(n => n.dataset.rid), chips: [...document.querySelectorAll('#reviewView .ordBar .egTab')].map(b => b.textContent.trim()) }));
    };
    const WANT = { n: 2, rids: [HAND, SENT], chips: ['Custom Orders1', 'Unknown SKU1'] };   // newest first: the hand completion came after the send

    // 0 · the switch is Open | Completed, nothing else
    const b0 = await bar();
    assert.deepEqual(b0.segs, ['open', 'done'], 'two segments');
    assert.equal(b0.sentSeg, false, 'no Decided segment'); assert.doesNotMatch(b0.text, /Decided/, 'no Decided word in the bar'); assert.equal(b0.aria, 'Open or completed');

    // 1 · Send to Sheet on an Unknown SKU card: a design dropped, its metal picked, sent
    await press('#reviewView .egTab[data-k="unmatchedSku"]');
    await page.evaluate(({ sel, text }) => {
      const dt = new DataTransfer(); dt.items.add(new File([text], 'rose.dxf'));
      const n = document.querySelector(sel);
      for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt }));
    }, { sel: card(SENT), text: DESIGN_DXF });
    await page.waitForFunction(() => document.querySelector('#cuDlg[open] .cuFile .cuThumb img'), null, { timeout: 30000 });
    await page.click('#cuDlg [data-x]');
    await page.waitForFunction(sel => document.querySelector(sel + ' .cuDesigns.ready') && !document.querySelector('dialog[open]'), card(SENT));
    await page.click(card(SENT) + ' [data-cu-send]');
    const sentKey = SENT + '_41791000011';
    await page.waitForFunction(k => CustomSheet.sentOf(B.orders.byKey.get(k)) && !document.querySelector('#rvList .reviewListRow[data-rid="4179100001"]'), sentKey, { timeout: 30000 });

    // 2 · Complete Order on the other one, by hand
    await page.click(card(HAND) + ' [data-cu-complete]');
    await page.waitForFunction(k => B.maps.customDone[k] && !document.querySelector('#rvList .reviewListRow[data-rid="4179100002"]'), HAND + '_41791000021', { timeout: 30000 });
    // the notes said where the cards went: Completed, never Decided
    const notes = await page.evaluate(() => window.__notes.slice());
    assert(!notes.some(t => /Decided/.test(t)), 'no note says Decided: ' + JSON.stringify(notes));
    await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close && n.close()));

    // 3 · Completed: both orders, once each, one count (the sum), newest first; the two records of older sends are stored but not listed
    await press('#reviewView .rvSeg [data-cseg="done"]');
    assert.deepEqual(await completed(2), WANT, 'Completed, before the reload');
    assert.deepEqual((await bar()).segs, ['open', 'done'], 'still two segments');
    assert.deepEqual(await page.evaluate(() => Review.settled().map(d => d.orders[0])), ['4179100003', '4179100004'], 'nothing was written for the send, nothing kept was deleted');
    assert(await page.evaluate(() => CustomSheet.sentOf(B.orders.byKey.get('4179100001_41791000011'))), 'the send itself stands');
    const btn = rid => page.evaluate(sel => [...document.querySelector(sel).querySelectorAll('.rowActions button')].map(b => b.textContent.trim()), card(rid));
    assert.deepEqual(await btn(HAND), ['Print QR label', 'Reopen'], 'the hand-completed order keeps its seal and buttons');
    assert.deepEqual((await btn(SENT)).filter(t => /sheet|History/i.test(t)), ['Open sheet', 'History'], 'the sent order keeps its sheet and history links');
    assert.equal(await page.evaluate(sel => document.querySelectorAll(sel + ' .seal-sheet').length, card(SENT)), 1, 'and its Sent to Sheet seal');
    assert.match(await page.evaluate(sel => document.querySelector(sel + ' .queueLabel').textContent, card(SENT)), /^Completed · sent to sheet$/, 'its card says Completed, not Decided');

    const mkeys = await page.evaluate(([a, b]) => ({ [a]: document.querySelector(`#rvList [data-rid="${a}"]`).dataset.mkey, [b]: document.querySelector(`#rvList [data-rid="${b}"]`).dataset.mkey }), [SENT, HAND]);

    // 4 · the chips filter the one list by kind; the count stays the sum; Open shows its own counts again
    await press('#reviewView .egTab[data-k="customOrder"]');
    assert.deepEqual(await completed(1), { n: 2, rids: [SENT], chips: ['Custom Orders1', 'Unknown SKU1'] }, 'Custom Orders chip: the sent one');
    await press('#reviewView .egTab[data-k="unmatchedSku"]');
    assert.deepEqual(await completed(1), { n: 2, rids: [HAND], chips: ['Custom Orders1', 'Unknown SKU1'] }, 'Unknown SKU chip: the hand-completed one');
    await press('#reviewView .egTab[data-k="unmatchedSku"]');
    assert.deepEqual(await completed(2), WANT, 'the chip pressed again lets go');
    await press('#reviewView .rvSeg [data-cseg="open"]');
    await page.waitForFunction(() => document.querySelector('#reviewView .rvSeg [data-cseg="open"].on'));
    await page.evaluate(mk => { window.__mk = mk; }, mkeys);
    assert.deepEqual(await page.evaluate(() => ({ open: +document.querySelector('#reviewView .rvSeg [data-cseg="open"] b').textContent, done: +document.querySelector('#reviewView .rvSeg [data-cseg="done"] b').textContent })), { open: 0, done: 2 }, 'Open counts what waits, Completed what is listed');

    // the cards that left Open for Completed fly to the one Completed switch (there is no other for the sent one), which is drawn
    const flights = await page.evaluate(([a, b]) => [a, b].map(rid => { const r = window.__leave(window.__mk[rid], { dataset: { rid } }); return { to: r && r.to, there: !!(r && document.querySelector(r.to)) }; }), [SENT, HAND]);
    assert.deepEqual(flights, [{ to: '#reviewView .rvSeg [data-cseg="done"]', there: true }, { to: '#reviewView .rvSeg [data-cseg="done"]', there: true }], 'both fly to Completed: ' + JSON.stringify(flights));

    // 5 · an old state: the workspace is saved while Review is on the old Decided segment ("sent"), and reloaded
    await page.evaluate(() => Session.flush(true));
    const saved = await page.evaluate(() => new Promise((resolve, reject) => {
      const open = indexedDB.open('charm-nest-workspace', 1);
      open.onerror = () => reject(open.error);
      open.onsuccess = () => {
        const db = open.result, tx = db.transaction('workspaces', 'readwrite'), store = tx.objectStore('workspaces'), keys = store.getAllKeys();
        keys.onsuccess = () => {
          const scope = keys.result.includes('production') ? 'production' : keys.result[0], get = store.get(scope);
          get.onsuccess = () => { const snap = get.result; if (!snap || !snap.reviewView) { resolve(null); return; } snap.reviewView.cseg = 'sent'; store.put(snap, scope); };
        };
        tx.oncomplete = () => { db.close(); resolve('sent'); };
      };
    }));
    assert.equal(saved, 'sent', 'the saved workspace now names the old Decided segment');
    await page.reload({ waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Review && window.Orders && CN.S.cloud.ok === true && Review.settled().length > 0, null, { timeout: 60000 });
    await page.evaluate(async () => { await Orders.loadMaps(true); Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render(); });
    assert.deepEqual(await completed(2), WANT, 'Completed, after the reload from an old Decided state');
    assert.equal(await page.evaluate(() => Review.view().cseg), 'done', 'the old segment is Completed now');
    const b1 = await bar();
    assert.deepEqual(b1.segs, ['open', 'done']); assert.equal(b1.sentSeg, false); assert.equal(b1.on, 'done');
    assert.deepEqual(await page.evaluate(() => Review.settled().map(d => d.orders[0])), ['4179100003', '4179100004'], 'the stored records are still stored after the reload');
    assert(await page.evaluate(() => CustomSheet.sentOf(B.orders.byKey.get('4179100001_41791000011'))), 'the send stands after the reload');
    // a note's Show for the old name lands on Completed too
    await page.evaluate(() => { Review.view().cseg = 'open'; Review.render(); Review.showCard('cu:custom:' + '4179100001', 'sent'); });
    assert.equal(await page.evaluate(() => Review.view().cseg), 'done', 'Show with the old name opens Completed');

    assert.deepEqual(outside, [], 'no Etsy call');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('Completed OK: one Open | Completed switch, no Decided; a send from a Review card and a hand completion are listed once each under one count, newest first, filtered by the chips; older stored send records are kept and unlisted; an old Decided state opens Completed');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
