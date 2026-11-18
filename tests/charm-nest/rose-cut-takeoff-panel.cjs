// CUTSHEETPLAN (Paul, 18 Nov 2026, Ana's computer, order 4231640384 on a CUT Rose Gold sheet: "Anna's computer is still not given her the options to choose whether to put the sheet on hold or to cancel the order and the button
// to actually remove their order from the sheet is also missing the whole pop-up is missing"). A computer that follows the run holds a cut Rose Gold sheet only as a saved card (the cloud will not open a cut layout for change).
// Its Take off the sheet… used to open the sheet first, was refused ("This layout was already cut"), and had no options. The panel is now drawn from the saved record:
//   1  the Take off button is there on the sheet window of the saved, cut sheet
//   2  pressing it draws the panel with its choices (Put on hold / Cancel the order) and the button that takes the order off, never the refusal
//   3  the panel says the piece stays on a sheet that was already cut (nothing is rewritten)
//   node tests/charm-nest/rose-cut-takeoff-panel.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');

const A = '4300000001', B = '4300000002', C = '4300000003';
const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', TL = 'Order_Timeline';
const GOLD = [[A, 1, 1], [A, 1, 2], [B, 2, 1], [C, 3, 1]];

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  st.put(SHEETS, 'gold-open-1', { id: 'gold-open-1', metal: 'gold', charms: GOLD.map(([r, t, c], i) => ({ id: 'g' + i, poolId: pid(r, t, c), order: r })), placements: GOLD.map((x, i) => ({ id: 'g' + i, cxPt: 30 + i * 40, cyPt: 40, angle: 0 })), poolIds: GOLD.map(([r, t, c]) => pid(r, t, c)), orders: [A, B, C], sheetIndex: 1, fileBase: 'gold-open-1', runId: 'run-test-1' });
  for (const [r, t, c] of GOLD) st.put(POOL, pid(r, t, c), { poolId: pid(r, t, c), orderId: r, sheetId: 'gold-open-1', state: 'placed', material: 'gold' });

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
  // nothing leaves the machine
  await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    return r.abort();
  });
  await ctx.addInitScript(() => {
    try {
      if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester');
      const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); s.pollOrders = 'off'; s.runMode = 'manual'; localStorage.setItem('cn.settings', JSON.stringify(s));
    } catch (_) { /* about:blank */ }
    window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
    const iv = setInterval(() => {
      if (typeof window.startNest !== 'function' || window.startNest.__stub) return;
      // a stand-in painter for the test's round charms (before and after a reload)
      const MM = 72 / 25.4, P = window.CharmNestPDF, realPath = P.pathToCanvas;
      P.drawCharm = (ctx, c, tx, k) => { const [x, y] = tx(c.centerPt[0], c.centerPt[1]); ctx.beginPath(); ctx.arc(x, y, c.rMm * MM * k, 0, 7); ctx.lineWidth = Math.max(1, .35 * k); ctx.strokeStyle = '#d0312d'; ctx.stroke(); };
      P.pathToCanvas = (ctx, p, tx) => { if (p && p.circle) { const [x, y] = tx(p.cx, p.cy), [x1] = tx(p.cx + p.r, p.cy), r = Math.abs(x1 - x); ctx.moveTo(x + r, y); ctx.arc(x, y, r, 0, Math.PI * 2); return; } return realPath(ctx, p, tx); };
      P.cutLinesOf = () => [];
      const stub = sh => {
        window.__nests = (window.__nests || []).concat([{ metal: sh.metal, page: sh.page, sheetId: sh.sheetId, byHand: !!sh._byHand, charms: sh.charms.map(c => c.poolId) }]);
        sh.status = 'nesting'; sh.persistedDone = false; sh.persisted = Promise.resolve(); sh.jobId = 'job-' + Math.random().toString(36).slice(2, 8); sh.stage = 'test nest';
        if (localStorage.getItem('__nestMode') === 'hang') return;
        setTimeout(async () => {
          try { await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, charms: sh.charms.map(c => ({ id: c.id, poolId: c.poolId, order: c.order })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.map(c => c.poolId) } }, { quiet: true }); }
          catch (e) { sh.problem = e.message; }
          sh.status = 'complete'; sh.dirty = false; sh._byHand = false; sh.stage = ''; sh.persistedDone = true;
          try { CN.renderCard(sh); } catch (_) {}
        }, 300);
      };
      stub.__stub = true; window.startNest = stub; clearInterval(iv);
    }, 0);
  });
  const page = await ctx.newPage(), errors = [], ok = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  const until = async (fn, ms = 15000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 100)); } };
  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Orders && Orders.takeOffGone && window.startNest && window.startNest.__stub, null, { timeout: 60000 });
    await page.evaluate(({ GOLD, A, B: OB, C: OC }) => {
      const MM = 72 / 25.4, pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`, metal = 'gold', id = 'gold-open-1';
      const sh = CN.S.sheets.gold.pages[0]; sh.charms = []; sh.placements = [];
      GOLD.forEach(([r, t, c], i) => {
        const rMm = 5, poolId = pid(r, t, c), cid = 'g' + i;
        sh.charms.push({ id: cid, name: `${r} · TEST-${t}`, poolId, order: r, lineKey: `${r}:${t}`, sourceId: 's', ringGeometryVersion: 3, centerPt: [0, 0], bbox: [-rMm * MM, -rMm * MM, rMm * MM, rMm * MM], outline: { circle: 1, cx: 0, cy: 0, r: rMm * MM }, members: [], rMm, widthPt: 2 * rMm * MM, heightPt: 2 * rMm * MM, areaPt2: Math.PI * (rMm * MM) ** 2 });
        sh.placements.push({ id: cid, cxPt: 30 + i * 40, cyPt: 40, angle: 0, wPt: 2 * rMm * MM, hPt: 2 * rMm * MM });
        window.B.pool.rows.set(poolId, { poolId, orderId: r, sheetId: id, state: 'placed', material: metal });
      });
      Object.assign(sh, { status: 'complete', sheetId: id, fileBase: id, sheetIndex: 1, runId: 'run-test-1', dirty: false, persistedDone: true, persisted: Promise.resolve() });
      CN.renderCard(sh);
      const row = (rid, tx, n, state) => ({ key: `${rid}:${5000000000 + tx}`, order: { receiptId: rid, orderNumber: rid, createTs: 1790000000, updateTs: 1790000000, shipBy: 1790500000, buyer: { name: 'Buyer' }, lines: [], messages: [] },
        line: { transactionId: String(5000000000 + tx), listingId: '', sku: 'TEST-' + tx, title: 'Test charm ' + tx, quantity: n, variations: [], personalization: [] }, spec: { designSku: 'TEST-' + tx, quantity: n, material: 'gold', problems: [] },
        problems: [], state, reason: null, poolIds: Array.from({ length: n }, (_, i) => pid(rid, tx, i + 1)), engrave: null, material: 'gold', arrivedAt: Date.now() - 7200000 });
      window.B.orders.rows = [row(A, 1, 2, 'pooled'), row(OB, 2, 1, 'pooled'), row(OC, 3, 1, 'pooled')];
      window.B.orders.byKey = new Map(window.B.orders.rows.map(r => [r.key, r]));
    }, { GOLD, A, B, C });
    await page.waitForTimeout(300);



    // the sheet as a CUT Rose Gold layout held only as a saved card (this computer follows the run)
    await page.evaluate(({ GOLD, A }) => {
      const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`, id = 'gold-open-1';
      const sh = CN.S.sheets.gold.pages[0], rec = { id, metal: 'rose', roseCutAt: 1794000000000, fileBase: id, runId: 'run-test-1', status: 'complete', charmCount: GOLD.length, placedCount: GOLD.length, poolIds: GOLD.map(([r, t, c]) => pid(r, t, c)) };
      sh.charms = []; sh.placements = []; sh.metal = 'rose';
      Object.assign(sh, { recalled: rec, roseCutAt: rec.roseCutAt, status: 'complete', sheetId: id, followed: true, dirty: false, persistedDone: true });
      for (const [r, t, c] of GOLD) window.B.pool.rows.set(pid(r, t, c), { poolId: pid(r, t, c), orderId: r, sheetId: id, state: 'placed', material: 'rose' });
      CN.renderCard(sh);
    }, { GOLD, A });
    // the cloud refuses to open a cut layout for change, as it does
    await page.evaluate(() => { const CS = window.CharmNestCloudSheets, was = CS.ensureLive; CS.ensureLive = async () => { throw new Error('This layout was already cut. Start a new sheet for its remnant.'); }; window.__ensureWas = was; });
    await page.waitForTimeout(300);
    await page.evaluate(id => SheetWin.open('gold-open-1', { select: id }), pid(A, 1, 1));
    await until(() => page.evaluate(() => !!document.querySelector('#sheetWin [data-r2=off] .swOffBtn, [data-r2=off] .swOffBtn')), 20000, '1 the Take off button');
    ok.push('1 the Take off button is on the sheet window of the saved, cut sheet');
    await page.click('[data-r2=off] .swOffBtn');
    await until(() => page.evaluate(() => { const o = document.querySelector('.swOff'); return !!o && !/Opening this sheet from the cloud/.test(o.textContent); }), 8000, 'the panel leaves the "Opening" line');
    const panel = await page.evaluate(() => { const o = document.querySelector('.swOff'); return { text: o.textContent.replace(/\s+/g, ' ').trim().slice(0, 400), refusal: /could not be opened/.test(o.textContent), hold: !!o.querySelector('[data-then=hold]'), cancel: !!o.querySelector('[data-then=cancel]'), go: !!o.querySelector('[data-o=go]'), who: o.querySelectorAll('input[name=swOffWho]').length }; });
    console.log('   panel:', JSON.stringify(panel));
    assert(!panel.refusal, 'no "could not be opened" refusal: ' + panel.text);
    ok.push('2a the panel is drawn from the saved record, not refused');
    assert(/already cut/i.test(panel.text), 'the panel says the piece stays on a sheet that was already cut: ' + panel.text);
    ok.push('3 it says the piece stays on a sheet that was already cut');
    assert(panel.go || panel.hold || panel.cancel, 'the panel has its choices / its button: ' + JSON.stringify(panel));
    ok.push('2b the panel carries its choices: ' + JSON.stringify({ hold: panel.hold, cancel: panel.cancel, go: panel.go, who: panel.who }));
    await page.click('.swOff [data-then=cancel]'); await new Promise(r => setTimeout(r, 400));
    const p2 = await page.evaluate(() => { const o = document.querySelector('.swOff'); return { text: o.textContent.replace(/\s+/g, ' ').trim().slice(0, 500), go: !!o.querySelector('[data-o=go]'), goText: (o.querySelector('[data-o=go]') || {}).textContent || '', goOff: !!(o.querySelector('[data-o=go]') || {}).disabled }; });
    console.log('   after Cancel the order:', JSON.stringify(p2));
    ok.push('4 after choosing Cancel the order: ' + JSON.stringify({ go: p2.go, goText: p2.goText, disabled: p2.goOff }));
    console.log(ok.map(x => '  ok   ' + x).join('\n'));
    assert.deepStrictEqual(errors.filter(e => !/firebase stub/.test(e)), [], 'no page errors: ' + errors.join(' | ').slice(0, 300));
    console.log('rose-cut-takeoff-panel: all passed');
  } catch (e) {
    console.error('errors:', errors);
    throw e;
  } finally {
    await browser.close(); srv.close();
  }
})().catch(e => { console.error(e); process.exit(1); });
