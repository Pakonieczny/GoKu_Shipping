// The sheet side of each order's timeline (Paul, 28 Sep: C2, C4, D1-D3). The sorter records from the browser the changes
// a person or the page makes to the sheets an order is on, each once, with its sheet ("14K Sheet 1") and sheet id, and
// never a type the server stamps (placed and setCommitted come from op poolUpdate; removed, moved, laser, seals, …).
//   1. Include in current set, ticked and unticked on the card: included, then excluded, for each order on that sheet,
//      with who; the same choice made again (a Retry) adds nothing.
//   2. Options → Merge sheets → Move all onto Sheet 1: merged for each order that moves, from Sheet 2 to Sheet 1; the
//      charms placed meanwhile send no "placed" from the page.
//   3. Apply size: sizeChanged for each order on the metal's sheets, with the old and the new mm.
//   4. A QR label made (Sets.onSheetSaved), then made again with the same codes: qrLabel once for each order on it.
//   5. The helper itself: a nest by hand (not one that only adds pieces), a green line prepared twice, an undone commit.
// Every event goes out in idle time, never inside the action. The page boots against the fake server; OrderTimeline is
// a spy defined before the page loads (the real one then stands aside). No network beyond loopback.
//   NODE_PATH=… PW_DIR=… CHROMIUM=… node tests/charm-nest/timeline-events-sheets.cjs
const path = require('path'), assert = require('assert/strict'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
const SERVER_TYPES = ['placed', 'setCommitted', 'removed', 'moved', 'laserDone', 'roseCut', 'engraveApproved', 'sealPrinted', 'sealCompleted', 'arrived', 'cancelled', 'etsyCancelled'];
const MINE = ['renested', 'merged', 'sizeChanged', 'included', 'excluded', 'roseLine', 'qrLabel', 'recalled', 'held', 'released', 'note'];

// a nest started by hand is recorded as it starts (a static check: the real nest needs the solver and its workers)
{
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), at = html.indexOf('if (sh._byHand) window.SheetEvents?.renested(sh);');
  assert(at > html.indexOf('function startNestReady(') && at < html.indexOf('delete sh._byHand; delete sh.resumeWait;', at) && html.indexOf('delete sh._byHand; delete sh.resumeWait;', at) - at < 200, 'startNestReady records a nest by hand before it forgets it was one');
}

(async () => {
  const srv = await start({ receipts: [] });
  const { sorterOrigin, stationOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 } });
  // nothing leaves the machine; the page's CDN scripts are local stand-ins (routes registered later are asked first)
  await ctx.route(u => /^https?:$/.test(u.protocol) && !['127.0.0.1', 'localhost'].includes(u.hostname), r => r.abort());
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.addInitScript(({ sorter, station }) => {
    if (location.origin !== sorter) return;
    localStorage.setItem('cn.employee', 'Tester');
    const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: 'off', sandboxStream: 'off', dsOrigin: station, pollOrders: 'off' }); localStorage.setItem('cn.settings', JSON.stringify(s));
    window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
    // the spy: every event the page records, as it was handed over
    window.__tl = [];
    window.OrderTimeline = { TYPES: {}, STATION_TYPES: new Set(), config: o => o, record(e) { window.__tl.push(JSON.parse(JSON.stringify(e))); return e; }, flush() {}, get: async () => ({ events: [] }), cancelCheck: async () => ({ cancelled: {} }), onRecord: () => () => {}, pending: () => 0 };
  }, { sorter: sorterOrigin, station: stationOrigin });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push(e.message));
  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.Gate && window.SheetEvents && window.Sets && document.querySelector('.sheetCard[data-m="gold14k"]'), null, { timeout: 60000 });
    // a 14K card with two sheets: six orders on Sheet 1, four on Sheet 2; the nest is a stand-in that lays the charms
    // down one by one (as the merge test does)
    await page.evaluate(() => {
      const { S, addPage, showPage, renderCard } = CN, MM = 72 / 25.4;
      CharmNestPDF.drawCharm = (g, c, tx, k) => { const [x, y] = tx(0, 0); g.beginPath(); g.arc(x, y, 3.5 * MM * k, 0, 7); g.stroke(); };
      const real = CharmNestPDF.pathToCanvas; CharmNestPDF.pathToCanvas = (g, p, tx) => { if (p && p.circle) { const [x, y] = tx(0, 0), [x1] = tx(p.r, 0); g.moveTo(x1, y); g.arc(x, y, Math.abs(x1 - x), 0, 7); return; } return real(g, p, tx); };
      let n = 0;
      const charm = (order, orderDate) => { const i = n++; return { id: 'c' + i, name: 'Charm ' + i, index: i, order, orderDate, poolId: `${order}_77${i}_1`, lineKey: `${order}_77${i}`, orderInfo: { receiptId: order, transactionId: '77' + i, sku: 'SKU' + i, copy: 1 }, thumb: 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20"><circle cx="10" cy="10" r="8" fill="none" stroke="#c8a24e"/></svg>'), sourceId: 's', centerPt: [0, 0], bbox: [-10, -10, 10, 10], outline: { circle: 1, r: 3.5 * MM }, members: [], widthPt: 7 * MM, heightPt: 7 * MM, areaPt2: 110, qty: 1, metal: 'gold14k', arrivedAt: i }; };
      window.__lay = list => list.map((c, i) => ({ id: c.id, cxPt: (6 + (i % 10) * 9) * MM, cyPt: (6 + Math.floor(i / 10) * 9) * MM, angle: 0, wPt: 7 * MM, hPt: 7 * MM }));
      S.settings.stock = S.settings.stock || {}; S.settings.stock.gold14k = [96 / 25.4, 46 / 25.4];
      const p1 = S.sheets.gold14k;
      p1.charms = Array.from({ length: 6 }, (_, i) => charm(String(4170000100 + i), 1 + i)); p1.placements = __lay(p1.charms);
      Object.assign(p1, { sheetId: 'gold14k-t1', fileBase: '14K_working_gold14k-t1', status: 'complete', dirty: false, verification: { ok: true }, persistedDone: true, runId: null });
      const p2 = addPage('gold14k');
      p2.charms = Array.from({ length: 4 }, (_, i) => charm(String(4170000200 + i), 50 + i)); p2.placements = __lay(p2.charms);
      Object.assign(p2, { sheetId: 'gold14k-t2', fileBase: '14K_working_gold14k-t2', status: 'complete', dirty: false, verification: { ok: true }, persistedDone: true });
      window.startNest = sh => {
        Object.assign(sh, { status: 'nesting', persistedDone: false, dirty: false, stage: 'Placing', progressKind: 'place', progress: [sh.placements.length, sh.charms.length] }); renderCard(sh);
        const keep = new Set(sh.appendOnly ? sh.placements.map(p => p.id) : []), todo = sh.charms.filter(c => !keep.has(c.id)); if (!sh.appendOnly) sh.placements = [];
        const step = () => {
          const c = todo.shift();
          if (!c) { Object.assign(sh, { status: 'complete', stage: '', verification: { ok: true } }); renderCard(sh); setTimeout(() => { sh.persistedDone = true; renderCard(sh); }, 60); return; }
          sh.placements.push(__lay(sh.charms)[sh.placements.length]);
          sh.progress = [sh.placements.length, sh.charms.length]; renderCard(sh); setTimeout(step, 40);
        };
        setTimeout(step, 60);
      };
      showPage('gold14k', 0);
      document.querySelector('.sheetCard[data-m="gold14k"]').scrollIntoView({ block: 'start' });
    });
    const events = type => page.evaluate(t => window.__tl.filter(e => e.type === t), type);
    const settle = () => page.waitForTimeout(400);   // (the events go out when the page is idle)
    const onceEach = (list, key, what) => { const seen = new Map(); for (const e of list) { const k = key(e); seen.set(k, (seen.get(k) || 0) + 1); } for (const [k, n] of seen) assert.equal(n, 1, `${what}: ${k} recorded ${n} times`); return seen; };
    const card = '.sheetCard[data-m="gold14k"]', sheet1 = ['4170000100', '4170000101', '4170000102', '4170000103', '4170000104', '4170000105'], sheet2 = ['4170000200', '4170000201', '4170000202', '4170000203'];
    const ids = list => [...new Set(list.map(e => e.orderId))].sort();

    /* ── 1 · Include Sheet 1 in the current set, then take it out again ── */
    {
      await page.click(`${card} .sheetOptions > summary`);
      const inAction = await page.evaluate(() => { const before = window.__tl.length; document.querySelector('.sheetCard[data-m="gold14k"] [data-solid="include"]').click(); return window.__tl.length - before; });
      assert.equal(inAction, 0, 'nothing is recorded inside the action itself: it waits for the page to be idle');
      await settle();
      const inc = await events('included');
      assert.deepEqual(ids(inc), sheet1, 'included: each order on Sheet 1'); onceEach(inc, e => e.orderId, 'included');
      assert(inc.every(e => e.by === 'Tester' && e.sheet === '14K Sheet 1' && e.sheetId === 'gold14k-t1' && e.data.metal === 'gold14k' && e.data.pieces === 1 && /^\d+_\d+$/.test(e.lineKey) && e.id), 'with who and where: ' + JSON.stringify(inc[0]));
      await page.click(`${card} [data-solid="include"]`);
      await settle();
      const exc = await events('excluded');
      assert.deepEqual(ids(exc), sheet1, 'excluded: each order on Sheet 1'); onceEach(exc, e => e.orderId, 'excluded');
      // the same choice made again (a Retry) is not a change
      await page.evaluate(() => Gate.changeMembership('gold14k', false, CN.pagesOf('gold14k')[0]));
      await settle();
      assert.equal((await events('excluded')).length, 6, 'the same choice again records nothing');
      assert.equal((await events('included')).length, 6);
      console.log('  ✓ include in current set: included, then excluded, for each order on the sheet, with who, once each, in idle time');
    }

    /* ── 2 · Merge sheets → Move all onto Sheet 1 ── */
    {
      if (!(await page.isVisible(`${card} [data-solid="merge-move"]`))) await page.click(`${card} .sheetOptions > summary`);
      await page.click(`${card} [data-solid="merge-move"]`);
      await page.click(`${card} [data-solid="merge-go"]`);
      await page.waitForFunction(() => CN.pagesOf('gold14k').length === 1 && CN.pagesOf('gold14k')[0].placements.length === 10 && CN.pagesOf('gold14k')[0].persistedDone && !Gate.mergeFx().live, null, { timeout: 15000 });
      await settle();
      const merged = await events('merged');
      assert.deepEqual(ids(merged), sheet2, 'merged: each order that moved from Sheet 2'); onceEach(merged, e => e.orderId, 'merged');
      assert(merged.every(e => e.data.kind === 'move' && e.data.from === '14K Sheet 2' && e.data.to === '14K Sheet 1' && e.data.toSheetId === 'gold14k-t1' && e.sheetId === 'gold14k-t2' && e.by === 'Tester' && /Moved from 14K Sheet 2 onto 14K Sheet 1/.test(e.text)), 'from Sheet 2 to Sheet 1: ' + JSON.stringify(merged[0]));
      assert.equal((await events('renested')).length, 0, 'a merge is recorded as a merge, not as a re-nest as well');
      assert.equal((await events('placed')).length, 0, 'the pieces placed meanwhile: "placed" is the server\'s (poolUpdate), never the page\'s');
      console.log('  ✓ merge: each order that moved, from Sheet 2 onto Sheet 1, once; no placed from the page');
    }

    /* ── 3 · Apply size ── */
    {
      if (!(await page.isVisible(`${card} [data-solid="w"]`))) await page.click(`${card} .sheetOptions > summary`);
      await page.fill(`${card} [data-solid="w"]`, '120');
      await page.click(`${card} [data-solid="size"]`);
      await page.waitForFunction(() => Math.abs(CN.S.settings.stock.gold14k[0] * 25.4 - 120) < 1e-6, null, { timeout: 5000 });
      await settle();
      const size = await events('sizeChanged');
      assert.deepEqual(ids(size), [...sheet1, ...sheet2], 'sizeChanged: each of the ten orders now on Sheet 1'); onceEach(size, e => e.orderId + '@' + e.sheetId, 'sizeChanged');
      assert(size.every(e => e.sheet === '14K Sheet 1' && e.by === 'Tester'));
      assert.deepEqual(size[0].data.fromMm, [96, 46]); assert.deepEqual(size[0].data.toMm, [120, 46]);
      assert.match(size[0].text, /96 × 46 mm → 120 × 46 mm/);
      console.log('  ✓ apply size: each order on the sheets, with the old and the new mm, once');
    }

    /* ── 4 · a QR label made, then made again with the same codes ── */
    {
      await page.evaluate(async () => {
        const p1 = CN.pagesOf('gold14k')[0]; CN.S.cloud.ok = false;
        // the size change left Sheet 1 to be nested again: its ten charms placed again, as a written sheet in a set
        p1.placements = __lay(p1.charms);
        const set = { setId: 'set-2026-09-28-1', runId: null, seq: 1, day: '2026-09-28', name: 'Set-1', group: 'dispatch', sheetIds: [], labelFiles: [], materials: [], orders: {}, offline: true };
        Object.assign(p1, { draft: false, setId: set.setId, sheetIndex: 1, fileBase: '14K_Sep.28.26_Set-1_Sheet-1' });
        await Sets.onSheetSaved(p1, p1.charms, undefined, { setOverride: set });
        await Sets.onSheetSaved(p1, p1.charms, undefined, { setOverride: set, labelsOnly: true });
      });
      await settle();
      const qr = await events('qrLabel');
      assert.deepEqual(ids(qr), [...sheet1, ...sheet2], 'qrLabel: each order on the label'); onceEach(qr, e => e.orderId, 'qrLabel');
      assert(qr.every(e => e.sheetId === 'gold14k-t1' && e.sheet === '14K Sheet 1' && e.setId === 'set-2026-09-28-1' && e.data.set === 'Set-1' && e.data.labels === 1), JSON.stringify(qr[0]));
      console.log('  ✓ QR label: each order on it, once; the same label made again adds nothing');
    }

    /* ── 5 · the rest of the helper, directly: a nest by hand, a green line, an undone commit, a recall, a hold ── */
    {
      await page.evaluate(() => {
        const p1 = CN.pagesOf('gold14k')[0];
        p1.appendOnly = true; SheetEvents.renested(p1);   // it keeps what it placed and adds to it: nothing moved
        p1.appendOnly = false; SheetEvents.renested(p1);
        const rose = { metal: 'rose', page: 1, sheetId: 'rose-t1', charms: p1.charms, rosePlan: { stages: [{ n: 1, at: 1790000000000, ids: ['c0', 'c1', 'c6'] }] } };
        SheetEvents.roseLines(rose); SheetEvents.roseLines(rose);   // prepared again (a new allowance): the same line
        SheetEvents.undone({ setId: 'set-2026-09-28-1', name: 'Set-1' }, ['4170000100']);
        SheetEvents.recalled([{ id: 'gold14k-t1', metal: 'gold14k', sheetIndex: 1, setId: 'set-2026-09-28-1', orders: ['4170000100', '4170000101'] }], 'Set-1');
        SheetEvents.order({ type: 'held', orderId: '4170000102', id: 'sw-test-1', sheetId: 'gold14k-t1', sheet: '14K Sheet 1', text: 'Taken off 14K Sheet 1 by Tester, on hold' });
      });
      await settle();
      const re = await events('renested'); assert.deepEqual(ids(re), [...sheet1, ...sheet2], 'a nest by hand from scratch: each order on the sheet, and only once'); onceEach(re, e => e.orderId, 'renested');
      const rose = await events('roseLine');
      assert.deepEqual(ids(rose), ['4170000100', '4170000101', '4170000200'], 'a green line: the orders whose pieces it covers, once');
      assert(rose.every(e => e.at === 1790000000000 && e.sheet === 'RG Sheet 1' && e.data.line === 1 && e.id === 'rose-t1-L1-1790000000000'));
      assert((await events('note')).some(e => e.orderId === '4170000100' && /Set commit undone · Set-1/.test(e.text)), 'an undone commit is a note');
      const rc = await events('recalled'); assert.deepEqual(ids(rc), ['4170000100', '4170000101']); assert(rc.every(e => e.sheet === '14K Sheet 1' && e.by === 'Tester'));
      const held = await events('held'); assert.equal(held.length, 1); assert.equal(held[0].by, 'Tester');
      console.log('  ✓ helper: a nest by hand (not one that only adds), a green line once, an undone commit, a recall, a hold');
    }

    const all = (await page.evaluate(() => window.__tl)).filter(e => MINE.includes(e.type) || SERVER_TYPES.includes(e.type));
    assert.deepEqual(all.filter(e => SERVER_TYPES.includes(e.type)).map(e => e.type), [], 'none of the server-stamped types is sent from the page');
    assert(all.every(e => e.id && /^\d{10}$/.test(e.orderId) && (!e.text || e.text.length <= 200) && JSON.stringify(e.data || {}).length < 2048), 'each event has an id, an order, and fits');
    assert.deepEqual(errors.filter(e => !/firebase stub/.test(e)), [], 'no page errors');
    console.log('timeline-events-sheets: ok');
  } finally { await browser.close(); srv.close(); }
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
