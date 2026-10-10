// Adversarial test (task G, off-sheet paths), a regression on a 14K card with two sheets (six orders on Sheet 1, four
// on Sheet 2) and a stand-in nest:
//   1. Options → Merge sheets → Move all onto Sheet 1, where Sheet 1 has room for only two of Sheet 2's orders: the two
//      that landed get their "merged" (moved) event, once, after the nest; the two that did not fit and stayed on Sheet 2
//      get none. (It was recorded before the nest, for every order of Sheet 2.)
//   (Apply size left the Options window on 7 Oct 2026, with the Sheet dimensions card: a sheet's size is set in Settings only.)
// No network beyond loopback.  NODE_PATH=… PW_DIR=… CHROMIUM=… node tests/charm-nest/adv-merge-size.cjs
const path = require('path'), assert = require('assert/strict'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');

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
      // the stand-in nest lays the charms down one by one; Sheet 1 has room for only two more (window.__room), and what
      // does not fit moves on whole to the merge's next sheet, as the real nest's moveOn does
      window.__nests = []; window.__room = 2;
      window.startNest = sh => {
        window.__nests.push(sh.page);
        Object.assign(sh, { status: 'nesting', persistedDone: false, dirty: false, stage: 'Placing', progressKind: 'place', progress: [sh.placements.length, sh.charms.length] }); renderCard(sh);
        const keep = new Set(sh.appendOnly ? sh.placements.map(p => p.id) : []); if (!sh.appendOnly) sh.placements = [];
        let todo = sh.charms.filter(c => !keep.has(c.id));
        if (sh._mergeNext && sh.appendOnly && todo.length > window.__room) {
          const rest = todo.slice(window.__room), next = sh._mergeNext, gone = new Set(rest.map(c => c.id));
          todo = todo.slice(0, window.__room); sh.charms = sh.charms.filter(c => !gone.has(c.id)); next.charms.push(...rest);
          setTimeout(() => window.startNest(next), 30);
        }
        const step = () => {
          const c = todo.shift();
          if (!c) { Object.assign(sh, { status: 'complete', stage: '', verification: { ok: true } }); renderCard(sh); setTimeout(() => { sh.persistedDone = true; renderCard(sh); }, 60); return; }
          sh.placements.push(Object.assign(__lay(sh.charms)[sh.placements.length], { id: c.id }));
          sh.progress = [sh.placements.length, sh.charms.length]; renderCard(sh); setTimeout(step, 40);
        };
        setTimeout(step, 60);
      };
      showPage('gold14k', 0);
      document.querySelector('.sheetCard[data-m="gold14k"]').scrollIntoView({ block: 'start' });
    });
    const events = type => page.evaluate(t => window.__tl.filter(e => e.type === t), type);
    const card = '.sheetCard[data-m="gold14k"]', ids = list => [...new Set(list.map(e => e.orderId))].sort();

    /* ── 1 · Move all onto Sheet 1: only the orders that landed ── */
    {
      if (!(await page.isVisible(`dialog.osDlg [data-solid="merge-move"]`))) await page.click(`${card} .sheetOptionsBtn`);
      await page.click(`dialog.osDlg [data-solid="merge-move"]`);
      await page.click(`dialog.osDlg [data-solid="merge-go"]`);
      await page.waitForFunction(() => { const [a, b] = CN.pagesOf('gold14k'); return a && b && a.placements.length === 8 && b.placements.length === 2 && a.persistedDone && b.persistedDone && !Gate.mergeFx().live; }, null, { timeout: 20000 });
      const where = await page.evaluate(() => CN.pagesOf('gold14k').map(p => p.charms.map(c => c.order)));
      assert.deepEqual(where[1].sort(), ['4170000202', '4170000203'], 'two orders did not fit and stayed on Sheet 2: ' + JSON.stringify(where));
      // watchMerge ends within its 400 ms look; the events then go out in idle time
      await page.waitForTimeout(1500);
      const merged = await events('merged');
      assert.deepEqual(ids(merged), ['4170000200', '4170000201'], 'merged (moved): only the orders that landed on Sheet 1: ' + JSON.stringify(merged.map(e => e.orderId)));
      assert.equal(merged.length, 2, 'once each');
      assert(merged.every(e => e.data.kind === 'move' && e.data.to === '14K Draft 1' && e.sheetId === 'gold14k-t2' && e.by === 'Tester'), JSON.stringify(merged[0]));
      assert.equal((await events('moved')).length + (await events('renested')).length, 0, 'no other event from the page');
      console.log('  ✓ Move all: "merged" only for the two orders that landed on Sheet 1, after the nest; none for the two that stayed');
    }

    assert.deepEqual(errors, [], 'no page errors');
    console.log('adv-merge-size: all passed');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
