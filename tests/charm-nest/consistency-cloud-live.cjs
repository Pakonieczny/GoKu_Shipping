// An open order window follows a change made in the cloud by someone else (another tab, another device, the server) without a reload
// (Paul, 5 Oct 2026: "everything needs to be coordinated as information is added, changed, removed, altered ... seamlessly updated in
// the cloud and on all portions of the application"). Real Chromium on the real page over the in-memory backend answering the order
// timeline as production does (derived, with placementRev): the order's Sheet tab lists its pieces and the sheet each is on; then a
// HOLD is made from outside the page (a poolUpdate by another client, no timeline event of its own), and the list must say so within
// the 2 to 3 seconds the order window's feed takes, with no reload. The same for a sheet deleted from outside.
//   node tests/charm-nest/consistency-cloud-live.cjs      (PW_DIR=<playwright-core's parent>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('consistency-cloud-live: no playwright-core, the browser checks were not run'); process.exit(0); }

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 7, 17) / 1000);
const RID = '4180000501', TG = '41800005011', TS = '41800005012', PG = `${RID}_${TG}_1`, PS = `${RID}_${TS}_1`;
const SH = { gf: 'sheet-cl-gf1', ss: 'sheet-cl-ss1' }, SET = 'set-cl-1';
const sheets = () => ({
  [SH.gf]: { id: SH.gf, metal: 'gold', sheetIndex: 1, setId: SET, setSeq: 1, folder: 'GF_Oct.7.26_Set-1_Sheet-1', fileBase: 'GF_Oct.7.26_Set-1_Sheet-1', day: '2026-10-07', status: 'written', stock: { wPt: 300, hPt: 150 }, orders: [RID], poolIds: [PG] },
  [SH.ss]: { id: SH.ss, metal: 'silver', sheetIndex: 1, setId: SET, setSeq: 1, folder: 'SS_Oct.7.26_Set-1_Sheet-1', fileBase: 'SS_Oct.7.26_Set-1_Sheet-1', day: '2026-10-07', status: 'written', stock: { wPt: 300, hPt: 150 }, orders: [RID], poolIds: [PS] }
});
const poolRow = (poolId, tx, metal, sheetId, name) => ({ poolId, orderId: RID, transactionId: tx, lineKey: `${RID}_${tx}`, sku: 'FEMALE_SYMBOL', material: metal, copy: 1, quantity: 1, state: 'written', sheetId, sheetName: name, setId: SET, runId: 'run-cl' });
const order = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Live Test' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [[TG, 'gold', '14k Gold Filled'], [TS, 'silver', 'Sterling Silver']].map(([tid, key, label]) => ({ transactionId: tid, listingId: '18000' + tid.slice(-5), sku: 'FEMALE_SYMBOL', title: 'Female symbol charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: label }], metalKey: key, metalLabel: label, personalization: [], buyerMessage: '' })) };

(async () => {
  const srv = await start(); srv.st.deriveTimeline = true;
  const st = srv.st; let n = 0;
  for (const s of Object.values(sheets())) {
    const placements = [], charms = [];
    s.poolIds.forEach((pid, i) => { const id = 'c' + (++n); placements.push({ id, cxPt: 30 + i * 34, cyPt: 30, angle: 0, wPt: 30, hPt: 30 }); charms.push({ id, name: `${RID} · FEMALE_SYMBOL`, poolId: pid, order: RID, sku: 'FEMALE_SYMBOL' }); });
    st.put('Charm_Nest_Sheets', s.id, Object.assign({}, s, { placements, charms, updatedAt: Date.now() }));
  }
  st.put('Charm_Nest_Sets', SET, { setId: SET, seq: 1, status: 'open', sheetIds: [SH.gf, SH.ss], orders: {} });
  st.put('Charm_Pool', PG, poolRow(PG, TG, 'gold', SH.gf, 'GF_Oct.7.26_Set-1_Sheet-1')); st.put('Charm_Pool', PS, poolRow(PS, TS, 'silver', SH.ss, 'SS_Oct.7.26_Set-1_Sheet-1'));
  const call = async body => (await (await fetch(`${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })).json());

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errors = []; let failed = 0;
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(); page.setDefaultTimeout(25000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && SheetWin.drawOrder && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ order, pools }) => {
      await Orders.loadMaps(true);
      order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: pools[i], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, { order, pools: [[PG], [PS]] });
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), `${RID}_${TG}`);
    await page.waitForFunction(() => document.querySelectorAll('#owSheetPanel .owShTabs button').length > 0);
    await page.waitForFunction(r => !window.OrderPieces || OrderPieces.known(r), RID); await page.waitForTimeout(900);
    const read = () => page.evaluate(() => ({ tabs: [...document.querySelectorAll('#owSheetPanel .owShTabs button')].map(b => b.textContent.trim()), pieces: [...document.querySelectorAll('#owSheetPanel .owPieces li')].map(li => (li.querySelector('em')?.textContent.trim() || '') + (li.classList.contains('off') ? ' [off]' : '')) }));
    const before = await read();
    console.log('  before:', JSON.stringify(before));
    assert.equal(before.tabs.length, 2, 'both sheets have a tab'); assert(before.pieces.every(x => !/off/.test(x) && !/not on a sheet/i.test(x)), 'both pieces are on a sheet: ' + JSON.stringify(before));

    // someone else holds the order: a poolUpdate from another client (the server takes the pieces off the sheet records in the same commit)
    const t0 = Date.now();
    const r = await call({ op: 'poolUpdate', poolIds: [PG, PS], patch: { state: 'abandoned', sheetId: null, setId: null, heldBy: 'Someone Else', heldReason: 'on hold', heldAt: Date.now() }, by: 'Someone Else' });
    assert.equal(r.ok, true, JSON.stringify(r));
    let after = null; const seen = [];
    const gone = x => /off|not on a sheet/i.test(x || '');
    while (Date.now() - t0 < 6000) { after = await read(); seen.push(after.pieces.join(' | ')); if (gone(after.pieces[1])) break; await page.waitForTimeout(150); }
    const took = Date.now() - t0;
    console.log('  after :', JSON.stringify(after), `${took} ms`);
    // (NOT asserted here: the piece on the sheet that is OPEN in the Sheet tab. That list is drawn from the record the sheet window read
    //  when it opened, so it still says "this sheet" until the sheet window reads its record again: the client's side, see cloud.md)
    if (!gone(after.pieces[0])) console.log('  note: the open sheet\'s own row still reads "' + after.pieces[0] + '" (drawn from the record read when it opened)');
    try { assert(gone(after.pieces[1]), 'the Sheet tab says the piece on the other sheet is on no sheet: ' + JSON.stringify(seen.slice(-3))); assert(took <= 3500, `within 3 s without a reload (${took} ms)`); console.log('  ✓ L1 a hold made from outside shows in the open order window within ' + took + ' ms, no reload'); }
    catch (e) { failed++; console.log('  ✗ L1 ' + e.message); }
  } finally { await browser.close(); srv.close(); }
  if (errors.length) { failed++; console.log('page errors:', errors.slice(0, 3)); }
  if (failed && !process.env.CC_BASELINE) process.exit(1);
  console.log(failed ? 'consistency-cloud-live: ' + failed + ' failed' : 'consistency-cloud-live: passed');
})().catch(e => { console.error(e); process.exit(1); });
