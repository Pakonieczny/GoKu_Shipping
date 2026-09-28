// Adversarial wave 3, area 13 (10K and 14K sheets), on a 14K card with two sheets (six orders on Sheet 1, four on
// Sheet 2) and a stand-in nest, in a real Chromium:
//   1. Merge sheets → Move all onto Sheet 1, the scene on the card: it is seen moving and through to its end (the glow
//      and "+N" come; nothing cuts it off while Sheet 1 nests), it animates only transform and opacity, and it leaves
//      nothing behind. Its frame times and long tasks are printed (the machine is shared: they are not asserted).
//   2. The split-order question: an order on both sheets; ticking "Include Sheet 1" asks, and Cancel leaves both out,
//      "Only 14K Gold Sheet 1" takes Sheet 1 alone, "Include both sheets" takes both; the Options panel is closed first.
//   3. Apply size with Sheet 2 cut (laser done): Sheet 2 keeps its size as well as its layout. It kept its layout but was
//      drawn, measured and shown in the sheet window at the new size (stockFor read the metal's new size), so a cut
//      96 mm sheet showed as 120 mm with a fifth of it empty. Sheet 1, not cut, takes the new size.
// No network beyond loopback.  NODE_PATH=… PW_DIR=… CHROMIUM=… node tests/charm-nest/adv-solid-sheets.cjs
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
    const card = '.sheetCard[data-m="gold14k"]', opts = async () => { if (!(await page.locator(`${card} .sheetOptions[open]:visible`).count())) await page.click(`${card} .sheetOptions > summary:visible`); };

    /* ── 1 · the merge, as it is seen ── */
    {
      await page.evaluate(() => {
        window.__props = new Set(); const an = Element.prototype.animate;
        Element.prototype.animate = function (k, o) { for (const f of (Array.isArray(k) ? k : [])) for (const p of Object.keys(f)) if (!['offset', 'easing', 'composite'].includes(p)) window.__props.add(String(this.className || this.tagName).split(' ').pop() + ':' + p); return an.call(this, k, o); };
        window.__fx = { add: 0, gone: 0, glow: false, plus: false, frames: [], long: [] };
        new PerformanceObserver(l => { for (const e of l.getEntries()) if (__fx.add && !__fx.gone) __fx.long.push(Math.round(e.duration)); }).observe({ entryTypes: ['longtask'] });
        new MutationObserver(recs => { for (const r of recs) {
          for (const n of r.addedNodes) {
            if (n.classList?.contains('mergeFx')) { __fx.add = performance.now(); let last = __fx.add; const f = t => { __fx.frames.push(t - last); last = t; if (!__fx.gone) requestAnimationFrame(f); }; requestAnimationFrame(f); }
            if (n.classList?.contains('mergeFxGlow')) __fx.glow = true; if (n.classList?.contains('mergeFxPlus')) __fx.plus = n.textContent;
          }
          for (const n of r.removedNodes) if (n.classList?.contains('mergeFx')) __fx.gone = performance.now();
        } }).observe(document.body, { childList: true, subtree: true });
      });
      await opts();
      await page.click(`${card} [data-solid="merge-move"]:visible`);
      await page.click(`${card} [data-solid="merge-go"]:visible`);
      await page.waitForFunction(() => __fx.add && performance.now() - __fx.add > 700 && document.querySelector('.sheetCard[data-m="gold14k"] .mergeFx .mergeFxPiece'), null, { timeout: 8000 });
      const pose = () => page.evaluate(() => [...document.querySelectorAll('.mergeFx .mergeFxPiece')].map(n => getComputedStyle(n).transform).join('|'));
      const mid = await pose(); await page.waitForTimeout(120);
      assert.notEqual(await pose(), mid, 'the charms are seen moving mid-way');
      await page.waitForFunction(() => __fx.gone, null, { timeout: 15000 });
      await page.waitForFunction(() => { const [a, b] = CN.pagesOf('gold14k'); return a && b && a.placements.length === 8 && b.placements.length === 2 && a.persistedDone && b.persistedDone && !Gate.mergeFx().live; }, null, { timeout: 20000 });
      const r = await page.evaluate(() => ({ life: Math.round(__fx.gone - __fx.add), glow: __fx.glow, plus: __fx.plus, worst: Math.round(Math.max(0, ...__fx.frames.slice(1))), over34: __fx.frames.slice(1).filter(d => d > 34).length, frames: __fx.frames.length, long: __fx.long,
        props: [...__props].filter(p => /^(mergeFx|shPreviewWrap|mPlus)/.test(p)), left: document.querySelectorAll('.mergeFx').length }));
      console.log('  · merge scene:', JSON.stringify({ life: r.life, frames: r.frames, worst: r.worst, over34: r.over34, long: r.long }));
      // (its "+N" is the charms that set off, 4: the scene plays before the nest has said which fit)
      assert(r.life >= 2800 && r.glow && r.plus === '+4', 'the scene plays to its end (glow, "+4"), not cut off: ' + JSON.stringify(r));
      assert.deepEqual(r.props.filter(p => !/:(transform|opacity)$/.test(p)), [], 'only transform and opacity are animated');
      assert.equal(r.left, 0, 'nothing is left behind');
      console.log(`  ✓ Move all: the merge is seen moving for ${r.life} ms to its glow and "+4", transform and opacity only, nothing left behind`);
    }

    /* ── 2 · the split-order question: include both, only this one, or cancel ── */
    {
      await page.evaluate(() => {
        const [p1, p2] = CN.pagesOf('gold14k'), c0 = p1.charms[0], c = Object.assign({}, c0, { id: 'cx', poolId: c0.order + '_7799_1', lineKey: c0.order + '_7799' });
        p2.charms.push(c); p2.placements.push(Object.assign(__lay(p2.charms)[p2.charms.length - 1], { id: 'cx' }));
        for (const p of [p1, p2]) delete p.solidPick; CN.showPage('gold14k', 0);
      });
      const picks = () => page.evaluate(() => CN.pagesOf('gold14k').map(p => Gate.solidSelected('gold14k', p)));
      const tick = async () => { await opts(); await page.click(`${card} [data-solid="include"]:visible`); await page.waitForSelector('dialog.splitDlg[open]', { timeout: 3000 }); };
      await tick();
      const ask = await page.evaluate(() => { const d = document.querySelector('dialog.splitDlg'); return { text: d.textContent, keys: [...d.querySelectorAll('[data-k]')].map(b => b.dataset.k + ':' + b.textContent), panel: !!document.querySelector('.sheetCard[data-m="gold14k"] .sheetOptions[open]') }; });
      assert.match(ask.text, /14K Gold Sheet 2/); assert.match(ask.text, /4170000100/);
      assert.deepEqual(ask.keys, ['cancel:Cancel', 'one:Only 14K Gold Sheet 1', 'all:Include both sheets']);
      assert.equal(ask.panel, false, 'the Options panel closed first (never a pop-up over a pop-up)');
      await page.click('dialog.splitDlg [data-k="cancel"]');
      assert.deepEqual(await picks(), [false, false], 'Cancel: left as it was');
      await opts(); assert.equal(await page.isChecked(`${card} [data-solid="include"]:visible`), false, 'the box is unticked again');
      await page.click(`${card} [data-solid="include"]:visible`); await page.waitForSelector('dialog.splitDlg[open]', { timeout: 3000 });
      await page.click('dialog.splitDlg [data-k="one"]');
      assert.deepEqual(await picks(), [true, false], 'Only this one');
      await opts(); await page.click(`${card} [data-solid="include"]:visible`);   // taken out: Sheet 2 is out already, nothing to ask
      await page.waitForTimeout(300); assert.equal(await page.locator('dialog.splitDlg').count(), 0, 'no question when nothing splits');
      assert.deepEqual(await picks(), [false, false]);
      await tick(); await page.click('dialog.splitDlg [data-k="all"]');
      assert.deepEqual(await picks(), [true, true], 'Include both');
      await page.waitForTimeout(600); assert.equal(await page.locator('dialog.splitDlg').count(), 0, 'the window is gone');
      console.log('  ✓ split order: Cancel leaves both out, "Only 14K Gold Sheet 1" takes it alone, "Include both sheets" takes both; the panel closes first');
    }

    /* ── 3 · Apply size keeps a cut sheet at its size ── */
    {
      const mm = pt => Math.round(pt * 25.4 / 72 * 100) / 100;
      const before = await page.evaluate(() => { const p2 = CN.pagesOf('gold14k')[1]; p2.laserDoneAt = Date.now() - 60000; return JSON.stringify(p2.placements); });
      await opts();
      await page.fill(`${card} [data-solid="w"]:visible`, '120');
      await page.click(`${card} [data-solid="size"]:visible`);
      await page.waitForFunction(() => Math.abs(CN.S.settings.stock.gold14k[0] * 25.4 - 120) < 1e-6, null, { timeout: 5000 });
      await page.waitForTimeout(800);
      const after = await page.evaluate(() => { const [p1, p2] = CN.pagesOf('gold14k'); return { p1: CN.stockFor('gold14k', p1).wPt, p2: CN.stockFor('gold14k', p2).wPt, p2h: CN.stockFor('gold14k', p2).hPt, placements: JSON.stringify(p2.placements) }; });
      assert.equal(after.placements, before, 'the cut sheet keeps its layout');
      assert.equal(mm(after.p1), 120, 'Sheet 1, not cut, takes the new size');
      assert.deepEqual([mm(after.p2), mm(after.p2h)], [96, 46], 'the cut Sheet 2 keeps its own size: ' + mm(after.p2) + ' mm wide');
      console.log('  ✓ Apply size: the cut Sheet 2 stays 96 × 46 mm (its layout too); Sheet 1 takes 120 mm');
    }
    assert.deepEqual(errors, [], 'no page errors');
    console.log('adv-solid-sheets: all passed');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
