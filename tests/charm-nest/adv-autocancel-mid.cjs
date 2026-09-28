// Adversarial (area 2, AutoCancel mid-flight): the sorter's page must not change under AutoCancel between the moment it
// last sees nothing at work and the moment the cancelled order's pieces come off. After that check the job read the
// order's piece records on sheets it has not loaded (poolList) and only then took the pieces off, with the plan it made
// before that read. A nest that was queued on the page meanwhile (the run picking up a dirty page, new orders going on)
// left the cancelled pieces on the page: takeOffGone skips a busy page, the job ended "later", and until the next check
// two minutes on, the nest could save them and the page could be released full, to be cut with a cancelled order on it.
// Now the piece records are read with the cancel record, before that check, and the job waits for the nest as it does
// for any page at work.
//   node tests/charm-nest/adv-autocancel-mid.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
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
  const cancel = (rid, by, extra) => st.put(CANCELLED, rid, Object.assign({ orderId: rid, by, why: '', at: Date.now(), sheets: [], lines: [] }, extra), false);

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

    /* a nest queued on the page while the job reads the order's piece records */
    let hit = 0;
    await page.route('**/.netlify/functions/charmNestLibrary*', async route => {
      let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {}
      if (b.op === 'poolList' && String(b.orderId) === A && !hit++) {
        await page.evaluate(() => { const sh = CN.S.sheets.gold.pages[0]; sh.status = 'queued'; setTimeout(() => { sh.status = 'complete'; }, 1500); });
        await new Promise(r => setTimeout(r, 100));
      }
      return route.continue();
    });
    const atA = Date.now(); cancel(A, 'Etsy', { source: 'etsy', at: atA });
    const due = await settle();
    assert.deepStrictEqual(due, [A], 'the sorter finds the cancelled order it holds: ' + JSON.stringify(due));
    assert(hit >= 1, 'its piece records were read');
    const w = await where(), acState = await page.evaluate(() => AutoCancel.state());
    assert(!w.gold.includes(pid(A, 1, 1)), 'its piece comes off the Gold sheet in this one job, the queued nest waited for: ' + JSON.stringify({ gold: w.gold, jobs: acState.jobs }));
    assert.deepStrictEqual(w.gold, [pid(B, 2, 1), pid(C, 3, 1)], 'the other charms stay');
    assert(!st.doc(SHEETS, 'gold-open-1').charms.some(c => c.order === A), 'the saved sheet no longer lists it');
    assert(!Object.keys(acState.jobs).length && acState.done[A] && +acState.done[A].at === atA, 'its job is done: ' + JSON.stringify(acState));
    assert.strictEqual(st.doc(POOL, pid(A, 1, 1)).state, 'abandoned', 'its piece record is let go');
    ok.push('a nest queued on the page while the job read the piece records: waited for, then the piece came off in the same job');

    assert.deepStrictEqual(errors, [], 'no page errors');
    console.log('adv-autocancel-mid: all passed\n  ' + ok.join('\n  '));
  } catch (e) {
    console.error('errors:', errors);
    throw e;
  } finally {
    await browser.close(); srv.close();
  }
})().catch(e => { console.error(e); process.exit(1); });
