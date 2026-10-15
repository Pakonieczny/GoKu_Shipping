// Adversarial (ADV-CANCEL, 15 Oct 2026): the connection drops while a cancelled order's pieces come off. The take-off writes
// the pieces' records last-but-one (Pool.update: "abandoned", who, why, when); that write fails (and the try that follows it), the page has already
// taken the pieces off its sheet. The job must heal at the next check: the piece records are written again, the sheet is
// saved without the pieces, and the job is done. It used to check the records again for good, never write them again
// ("its piece records do not agree yet"), so a cancelled order's pieces stayed "placed" on a sheet in the cloud for ever.
//   node tests/charm-nest/adv-cancel-drop.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start, Timestamp } = require('./bridge-server.cjs');
const A = '4500000001', B = '4500000002', C = '4500000003', D = '4500000004', E = '4500000005';
const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', CANCELLED = 'Charm_Nest_Cancelled', TL = 'Order_Timeline';
const GOLD = [[A, 1, 1], [B, 2, 1], [C, 3, 1]], SILVER = [[D, 4, 1], [E, 5, 1]];

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  // the cloud's side: the two sheets as saved, and the piece records
  const saved = (id, metal, list) => st.put(SHEETS, id, { id, metal, charms: list.map(([r, t, c], i) => ({ id: metal[0] + i, poolId: pid(r, t, c), order: r })), placements: list.map((x, i) => ({ id: metal[0] + i, cxPt: 30 + i * 40, cyPt: 40, angle: 0 })), poolIds: list.map(([r, t, c]) => pid(r, t, c)) }, false);
  saved('gold-open-1', 'gold', GOLD); saved('silver-cut-1', 'silver', SILVER);
  for (const [r, t, c] of GOLD) st.put(POOL, pid(r, t, c), { poolId: pid(r, t, c), orderId: r, sheetId: 'gold-open-1', state: 'placed', material: 'gold' });
  for (const [r, t, c] of SILVER) st.put(POOL, pid(r, t, c), { poolId: pid(r, t, c), orderId: r, sheetId: 'silver-cut-1', state: 'committed', material: 'silver' });
  const cancel = (rid, by, extra) => st.put(CANCELLED, rid, Object.assign({ orderId: rid, by, why: '', at: Date.now(), sheets: [], lines: [], createdAt: Timestamp.now() }, extra), false);

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
  // nothing leaves the machine
  await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    return r.abort();
  });
  // the operator, no order checks of its own, and a stand-in nest: it keeps every placement (an append-only nest) and
  // saves the sheet as it stands; "hang" is a nest a reload cuts short
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
  const until = async (fn, ms = 15000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + (what || fn)); await new Promise(r => setTimeout(r, 100)); } };
  const boot = async () => {
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.AutoCancel && AutoCancel.started() && window.startNest && window.startNest.__stub, null, { timeout: 60000 });
  };

  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await boot();
    // the sorter's side: the two pages (a stand-in painter draws their charms), the lines, the piece records
    await page.evaluate(({ GOLD, SILVER, A, B: OB, C, D, E }) => {
      const MM = 72 / 25.4;
      const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
      const fill = (metal, id, list, extra) => {
        const sh = CN.S.sheets[metal].pages[0]; sh.charms = []; sh.placements = [];
        list.forEach(([r, t, c], i) => {
          const rMm = 5, poolId = pid(r, t, c), cid = metal[0] + i;
          sh.charms.push({ id: cid, name: `${r} · TEST-${t}`, poolId, order: r, lineKey: `${r}:${t}`, sourceId: 's', ringGeometryVersion: 3, centerPt: [0, 0], bbox: [-rMm * MM, -rMm * MM, rMm * MM, rMm * MM], outline: { circle: 1, cx: 0, cy: 0, r: rMm * MM }, members: [], rMm, widthPt: 2 * rMm * MM, heightPt: 2 * rMm * MM, areaPt2: Math.PI * (rMm * MM) ** 2 });
          sh.placements.push({ id: cid, cxPt: 30 + i * 40, cyPt: 40, angle: 0, wPt: 2 * rMm * MM, hPt: 2 * rMm * MM });
          window.B.pool.rows.set(poolId, { poolId, orderId: r, sheetId: id, state: metal === 'silver' ? 'committed' : 'placed', material: metal });
        });
        Object.assign(sh, { status: 'complete', sheetId: id, fileBase: id, sheetIndex: 1, runId: 'run-test-1', dirty: false, persistedDone: true, persisted: Promise.resolve() }, extra || {});
        CN.renderCard(sh);
      };
      fill('gold', 'gold-open-1', GOLD);
      fill('silver', 'silver-cut-1', SILVER, { laserDoneAt: Date.now() - 3600000 });
      const row = (rid, tx, n, material, state) => ({ key: `${rid}:${5000000000 + tx}`, order: { receiptId: rid, orderNumber: rid, createTs: 1790000000, updateTs: 1790000000, shipBy: 1790500000, buyer: { name: 'Buyer ' + rid.slice(-1) }, lines: [], messages: [] },
        line: { transactionId: String(5000000000 + tx), listingId: '', sku: 'TEST-' + tx, title: 'Test charm ' + tx, quantity: n, variations: [], personalization: [] }, spec: { designSku: 'TEST-' + tx, quantity: n, material, problems: [] },
        problems: [], state, reason: null, poolIds: Array.from({ length: n }, (_, i) => pid(rid, tx, i + 1)), engrave: null, material, arrivedAt: Date.now() - 7200000 });
      window.B.orders.rows = [row(A, 1, 1, "gold", "pooled"), row(OB, 2, 1, 'gold', 'pooled'), row(C, 3, 1, 'gold', 'pooled'), row(D, 4, 1, 'silver', 'committed'), row(E, 5, 1, 'silver', 'committed')];
      window.B.orders.byKey = new Map(window.B.orders.rows.map(r => [r.key, r]));
      // (the flights: every Motion.fly is counted)
      window.__flies = 0; const fly = Motion.fly; Motion.fly = (g, to, o) => { window.__flies++; return fly(g, to, o); };
      setMode('nest'); window.scrollTo(0, 0);
    }, { GOLD, SILVER, A, B, C, D, E });
    await page.waitForTimeout(300);
    const where = () => page.evaluate(() => ({ gold: CN.S.sheets.gold.pages[0].charms.map(c => c.poolId), goldAt: Object.fromEntries(CN.S.sheets.gold.pages[0].placements.map(p => [CN.S.sheets.gold.pages[0].charms.find(c => c.id === p.id).poolId, [p.cxPt, p.cyPt]])), silver: CN.S.sheets.silver.pages[0].charms.map(c => c.poolId), rows: Orders.rows().map(r => r.order.receiptId), nests: (window.__nests || []).map(n => n.sheetId + (n.byHand ? ':hand' : '')) }));
    const settle = () => page.evaluate(async () => { const due = await AutoCancel.poll(); await AutoCancel.idle(); return due; });



    /* the connection is down for the write that lets the pieces go, and for its second try in the same job (the check of the saved records sends it again once) */
    let dropped = 0;
    await page.route('**/.netlify/functions/charmNestLibrary*', async route => {
      let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {}
      if (b.op === 'poolUpdate' && b.patch && b.patch.state === 'abandoned' && dropped < 2) { dropped++; return route.abort('connectionreset'); }
      return route.continue();
    });
    const atA = Date.now(); cancel(A, 'Etsy', { source: 'etsy', at: atA });
    const due = await settle();
    assert.deepStrictEqual(due, [A], 'the sorter finds the cancelled order it holds: ' + JSON.stringify(due));
    assert.strictEqual(dropped, 2, 'the piece records write was dropped, and its second try');
    let w = await where(), acState = await page.evaluate(() => AutoCancel.state());
    assert(!w.gold.includes(pid(A, 1, 1)), 'the page took the piece off its sheet: ' + JSON.stringify(w.gold));
    assert.strictEqual(st.doc(POOL, pid(A, 1, 1)).state, 'placed', 'the cloud has not heard of it yet (both writes were lost)');
    assert(!acState.done[A], 'the job is not done: ' + JSON.stringify(acState));
    ok.push('the drop: the page has the piece off its sheet, the cloud still has its record placed, the job is not done');

    /* the connection is back: the next checks finish it */
    let again = [];
    for (let i = 0; i < 3 && !(again = await page.evaluate(() => AutoCancel.state())).done[A]; i++) await settle();
    acState = await page.evaluate(() => AutoCancel.state());
    assert.strictEqual(st.doc(POOL, pid(A, 1, 1)).state, 'abandoned', 'at the next check the piece record is written: ' + JSON.stringify(st.doc(POOL, pid(A, 1, 1))));
    assert.strictEqual(st.doc(POOL, pid(A, 1, 1)).removedBy, 'Etsy', 'with who took it off');
    assert(!st.doc(SHEETS, 'gold-open-1').charms.some(c => c.order === A), 'the saved sheet no longer lists it');
    assert(!Object.keys(acState.jobs).length && acState.done[A] && +acState.done[A].at === atA, 'the job is done: ' + JSON.stringify(acState));
    w = await where();
    assert.deepStrictEqual(w.gold, [pid(B, 2, 1), pid(C, 3, 1)], 'the other charms stay');
    ok.push('the connection back: the next check wrote the piece record, the sheet is saved without it, the job is done');

    /* the same drop for a piece that was never placed (pooled, waiting for a sheet): its record is written at the next check too */
    const F = '4500000006', fid = pid(F, 6, 1);
    st.put(POOL, fid, { poolId: fid, orderId: F, state: 'ready', material: 'gold' });
    await page.evaluate(({ F, fid }) => {
      const row = { key: `${F}:5000000006`, order: { receiptId: F, orderNumber: F, createTs: 1790000000, updateTs: 1790000000, shipBy: 1790500000, buyer: { name: 'Buyer F' }, lines: [], messages: [] },
        line: { transactionId: '5000000006', listingId: '', sku: 'TEST-6', title: 'Test charm 6', quantity: 1, variations: [], personalization: [] }, spec: { designSku: 'TEST-6', quantity: 1, material: 'gold', problems: [] },
        problems: [], state: 'pooled', reason: null, poolIds: [fid], engrave: null, material: 'gold', arrivedAt: Date.now() - 7200000 };
      window.B.orders.rows.push(row); window.B.orders.byKey.set(row.key, row);
      window.B.pool.rows.set(fid, { poolId: fid, orderId: F, state: 'ready', material: 'gold' });
    }, { F, fid });
    dropped = 0;
    cancel(F, 'Etsy', { source: 'etsy', at: Date.now() });
    await settle();
    assert(dropped >= 1, 'its record write was dropped');
    assert.strictEqual(st.doc(POOL, fid).state, 'ready', 'the cloud has not heard of it yet');
    for (let i = 0; i < 3 && !(await page.evaluate(() => AutoCancel.state())).done[F]; i++) await settle();
    assert.strictEqual(st.doc(POOL, fid).state, 'abandoned', 'at the next check its record is written: ' + JSON.stringify(st.doc(POOL, fid)));
    assert(await page.evaluate(F => !Orders.rows().some(r => String(r.order.receiptId) === F), F), 'and its line has left Orders');
    ok.push('the same drop for a piece never placed: its record is written at the next check, its line is off Orders');

    assert.deepStrictEqual(errors, [], 'no page errors');
    console.log('adv-cancel-drop: all passed\n  ' + ok.join('\n  '));
  } catch (e) {
    console.error('errors:', errors);
    throw e;
  } finally {
    await browser.close(); srv.close();
  }
})().catch(e => { console.error(e); process.exit(1); });
