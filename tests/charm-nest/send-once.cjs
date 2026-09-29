// Send to Sheet puts an order line on its sheet once (Paul, 29 Sep 02:26 UTC: "When I approved and sent to sheet in
// order from the review tab ... it incorrectly added a duplicate order": one custom lion-head design twice on an RG 14/20
// sheet). The cause: a second placement of the same line that ran while Send to Sheet's own was on its way (an answer on
// the order's card repooling its lines, or an intake) made its own copy of the pieces, and both went onto the sheet under
// one pool id. Now one placement of a line runs at a time (Pool: placing), a piece a sheet already holds is never put on
// again (attachPool), a sent card never sends again, and the cloud refuses a second placement of a line on a saved sheet
// (poolPut: placed) and a sheet record that puts one piece on twice (putSheet).
// Part 1 runs against the fake cloud (bridge-server.cjs, no network). Part 2 opens the sorter in headless Chromium on the
// fake site: every request that is not to the loopback is aborted.
//   node tests/charm-nest/send-once.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<dir> for pictures)
const path = require('path'), fs = require('fs'), assert = require('assert/strict');
const here = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = w => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
// one order per case: its line's SKU is in no master file (an Unmatched SKU card, which takes the order's own designs)
const CASES = [['rose', 'repool'], ['rose', 'intake'], ['gold', 'intake'], ['silver', 'intake'], ['gold10k', 'intake'], ['gold14k', 'intake']];
const ridOf = i => String(4175429900 + i);
const ORDERS = CASES.map(([metal], i) => ({ receiptId: ridOf(i), orderNumber: ridOf(i), createTs: SHIP - 5 * DAY + i, updateTs: SHIP - 5 * DAY + 60 + i, shipBy: SHIP, buyer: { name: 'Buyer ' + i }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: ridOf(i) + '1', listingId: String(1800099110 + i), sku: `LION-HEAD-CUSTOM-0${i}`, title: 'Lion Head Charm Necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Rose Gold Filled' }], metalKey: 'rose', metalLabel: 'RG 14/20', personalization: [], buyerMessage: '' }] }));

async function cloud(srv) {
  const lib = (body) => fetch(`${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }).then(async r => ({ status: r.status, body: await r.json() }));
  const P = '4175420001_41754200011_1', row = { poolId: P, runId: 'run-once', orderId: '4175420001', sku: 'CUSTOM', material: 'rose', copy: 1, quantity: 1, state: 'ready', sheetId: null, setId: null };
  let r = await lib({ op: 'poolPut', pools: [row] }); assert.equal(r.body.written, 1);
  // the line is written onto a saved sheet
  r = await lib({ op: 'putSheet', sheet: { id: 'sheet-once-1', metal: 'rose', poolIds: [P], placements: [{ id: 'c1', cxPt: 10, cyPt: 10, angle: 0 }], charms: [{ id: 'c1', poolId: P }] } }); assert.equal(r.body.ok, true, JSON.stringify(r.body));
  r = await lib({ op: 'poolUpdate', poolIds: [P], patch: { state: 'written', sheetId: 'sheet-once-1', sheetName: 'RG_Sheet-1' } }); assert.equal(r.body.ok, true);
  // placed again (a retry, a second sorter, a second press): answered as placed, the record left as it is
  r = await lib({ op: 'poolPut', pools: [row] });
  assert.equal(r.body.written, 0, 'a line on a saved sheet is not recorded as placed again');
  assert.deepEqual(r.body.placed.map(x => [x.poolId, x.sheetId]), [[P, 'sheet-once-1']], 'it is answered as placed, with where it is');
  assert.equal(srv.st.doc('Charm_Pool', P).sheetId, 'sheet-once-1'); assert.equal(srv.st.doc('Charm_Pool', P).state, 'written');
  // taken off (the sheet window's Hold): it may go on again
  r = await lib({ op: 'poolUpdate', poolIds: [P], patch: { state: 'abandoned', sheetId: null, setId: null } });
  r = await lib({ op: 'poolPut', pools: [row] }); assert.equal(r.body.written, 1, 'a line taken off goes on again'); assert.equal(r.body.placed.length, 0);
  // a sheet whose record says a line is on it, but the sheet record no longer lists it (a deleted or rewritten sheet): not held
  r = await lib({ op: 'poolUpdate', poolIds: [P], patch: { state: 'written', sheetId: 'sheet-gone' } });
  r = await lib({ op: 'poolPut', pools: [row] }); assert.equal(r.body.written, 1, 'a stale sheet id holds nothing'); assert.equal(r.body.placed.length, 0);
  // a sheet record that would put one piece on twice is refused, in words; one saved so before saves as it was
  r = await lib({ op: 'putSheet', sheet: { id: 'sheet-once-2', metal: 'gold', poolIds: [P, P], placements: [] } });
  assert.equal(r.status, 409, JSON.stringify(r)); assert.match(r.body.error, /twice/);
  assert.equal(srv.st.doc('Charm_Nest_Sheets', 'sheet-once-2'), undefined, 'nothing written');
  srv.st.put('Charm_Nest_Sheets', 'sheet-once-3', { id: 'sheet-once-3', metal: 'gold', poolIds: [P, P] });
  r = await lib({ op: 'putSheet', sheet: { id: 'sheet-once-3', metal: 'gold', poolIds: [P, P], status: 'complete' } }); assert.equal(r.body.ok, true, 'a sheet saved with it before keeps saving: ' + JSON.stringify(r.body));
  console.log('  ok   cloud: a line on a saved sheet is answered as placed and left as it is; taken off, it goes on again; a sheet record placing a piece twice is refused');
}

async function browserPart(srv) {
  const pwDir = process.env.PW_DIR || path.join(here, 'node_modules');
  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  try {
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = []; page.setDefaultTimeout(30000);
    page.on('pageerror', e => errors.push(String(e)));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      // a run under way, past its pool step: a line sent now goes onto its sheet at once
      const day = new Date().toISOString().slice(0, 10);
      B.run = { runId: `run-${day}-once`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    }, ORDERS);
    const copiesOf = (rid) => page.evaluate(rid => { const row = Orders.rows().find(x => String(x.order.receiptId) === rid); const on = allSheets().flatMap(p => p.charms.filter(c => (row.poolIds || []).includes(c.poolId) || c.order === rid).map(c => [p.metal, c.poolId])); return { state: row.state, poolIds: row.poolIds, on, sent: !!CustomSheet.sentOf(row) }; }, rid);
    for (const [i, [metal, how]] of CASES.entries()) {
      const rid = ridOf(i), card = `#rvList .reviewListRow[data-rid="${rid}"]`;
      // the tour plays for the first; the others send with motion reduced (the placement is the same, the test quicker)
      if (i === 1) await page.emulateMedia({ reducedMotion: 'reduce' });
      await page.evaluate(() => { CN.setMode('review'); Review.render(); });
      await page.waitForSelector(card);
      await page.evaluate(({ sel, text }) => { const dt = new DataTransfer(); dt.items.add(new File([text], 'lion-head.dxf')); const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); }, { sel: card, text: DXF(18) });
      await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === 1, null, { timeout: 30000 });
      await page.click(`#cuDlg .cuFile .cuM[data-m="${metal}"]`);
      // Send to Sheet, pressed twice at once, and while its placement is on its way the same line is placed again: by an
      // answer's repool of the order's lines, or by an intake (Arrivals → LiveNest.add → Pool.addAll)
      await page.evaluate(({ rid, how }) => {
        const row = Orders.rows().find(x => String(x.order.receiptId) === rid), b = document.querySelector('#cuDlg [data-send]');
        window.__ax = Review.actFor(row.key);   // what the card's own Send to Sheet acts on
        b.click(); b.click();
        const t = setInterval(() => { if (!CustomSheet.sentOf(row)) return; clearInterval(t); if (how === 'repool') Review.repool(row); else Pool.addAll(B.run); }, 1);
      }, { rid, how });
      await page.waitForFunction(rid => { const row = Orders.rows().find(x => String(x.order.receiptId) === rid); return row.state === 'pooled' && !(window.SendTour && SendTour.playing()) && !document.querySelector('#cuDlg[open]'); }, rid, { timeout: 30000 });
      if (i === 0) {
        // after the tour: Send to Sheet again, from the code the card and the window call, and the Review list's own card
        const again = await page.evaluate(async rid => { const had = !!window.__ax; if (had) await CustomSheet.send(window.__ax); return { had, button: !!document.querySelector(`#rvList .reviewListRow[data-rid="${rid}"] [data-cu-send]`), toast: [...document.querySelectorAll('#toasts .toast')].map(t => t.textContent).join(' | ') }; }, rid);
        assert.ok(again.had, 'the card acted for the order'); assert.equal(again.button, false, 'a sent card offers no Send to Sheet');
        assert.match(again.toast, /already on the sheets/, 'pressed again, it says so and sends nothing: ' + again.toast);
        await page.waitForTimeout(1500);
      }
      await page.waitForTimeout(600);
      const c = await copiesOf(rid);
      assert.equal(c.on.length, 1, `${metal} · ${how}: the line is on its sheet once: ${JSON.stringify(c)}`);
      assert.equal(c.on[0][0], metal, `${metal}: on its own metal's sheet`);
      assert.deepEqual(c.poolIds, [c.on[0][1]]);
      console.log(`  ok   ${metal} · Send to Sheet pressed twice while ${how === 'repool' ? "an answer repools the order's line" : 'an intake runs'}: one copy on the ${metal} sheet`);
    }
    // a line already on its sheet is never placed again, whatever asks: here its record is read afresh as the next run's
    // carry-over does (pulled, no pool ids) and an intake runs
    const rid0 = ridOf(0);
    await page.evaluate(async rid => { const row = Orders.rows().find(x => String(x.order.receiptId) === rid); row.poolIds = []; row.state = 'pulled'; delete row.poolTry; await Pool.addAll(B.run); }, rid0);
    const c0 = await copiesOf(rid0);
    assert.equal(c0.on.length, 1, 'a line on its sheet is not put on again: ' + JSON.stringify(c0));
    assert.equal(c0.state, 'pooled'); assert.deepEqual(c0.poolIds, [c0.on[0][1]], 'it is back on the copy it has');
    console.log('  ok   a line already on its sheet is not placed again by an intake (read afresh, no pool ids)');
    if (process.env.SHOTS) {
      fs.mkdirSync(process.env.SHOTS, { recursive: true });
      await page.evaluate(() => { CN.setMode('nest'); CN.showPage('rose', 0); });
      await page.waitForTimeout(800);
      const cardEl = await page.$('.sheetCard[data-m="rose"]');
      if (cardEl) await cardEl.screenshot({ path: path.join(process.env.SHOTS, 'rg-sheet-one-copy.png') });
    }
    assert.deepEqual(errors, [], 'no page errors');
  } finally { await context.close(); await browser.close(); }
}

(async () => {
  const srv = await start({ receipts: [] });
  try { await cloud(srv); await browserPart(srv); }
  finally { await srv.close(); }
  console.log('send-once OK');
})().catch(e => { console.error(e); process.exit(1); });
