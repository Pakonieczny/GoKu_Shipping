// Hold, Release hold and Cancel Order, end to end (Paul, 5 Oct 2026). Every part is the REAL one, in one real Chromium page over the local
// fake backend (bridge-server.cjs: the real charmNestLibrary handler on an in-memory Firestore; nothing leaves the machine, no Etsy, no model):
//   OrderHold (the engine: plan / run / release / releasePlan), SheetWin.holdKit (the sheet window's machinery), HoldUI (the orange button, the
//   consent popup, the glue), OrderHoldFx (the film), CancelUI (Cancel Order), Orders / Review / OrderWin / Motion / NestFocus of the sorter.
// The only stand-in is the nest itself (the solver): startNest places what is pinned where it was pinned and puts the waiting pieces in the next free
// cells in the order the real nest ranks them (CharmNestOrders.rankDate, frontAt first), saving the sheet as the real nest saves it.
//   node tests/charm-nest/hold-release-cancel-e2e.cjs      (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>, SHOTS=<dir>, ONLY=A,B,C,D,E,F,G)
// A  Hold from an order window piece row (no name saved: the name bar), the consent popup, Not now changes nothing, Continue: the film, On hold, the data
// B  Hold from an open Review card          C  Hold from a completed Review card
// D  Release hold on the On hold card: ahead of 3 incoming orders, the next available sheet at once, the film, home, QR step, timeline
// E  Cancel Order on On hold cards: the flight, the count, the note, Undo          F  Esc and the Skip button during a film
// G  no console or page errors, no leftover overlay, frame time
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); } }
const { start } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || '', ONLY = (process.env.ONLY || '').split(',').filter(Boolean), want = n => !ONLY.length || ONLY.includes(n);
if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });

const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', TL = 'Order_Timeline', RUN = 'run-e2e-1';
const SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000), BASE_TS = Math.floor(Date.UTC(2026, 9, 5, 12) / 1000);
const TID = tx => String(5000000000 + tx), pid = (rid, tx, copy) => `${rid}_${TID(tx)}_${copy}`, kOf = (rid, tx) => `${rid}_${TID(tx)}`;
const H1 = '4170000100', H2 = '4170000110', H3 = '4170000120', H4 = '4170000130', H5 = '4170000140';

// the world: sheets (items [rid, tx, copy, col, row, placed]), the orders (lines [tx, qty, metal, sku] or [tx, 'chain'])
const CHAIN = tx => [tx, 'chain', null, 'CHAIN ONLY ' + tx];
const O = (age, buyer, ...lines) => ({ age, buyer, lines });
const W = n => '41700003' + String(n).padStart(2, '0'), V = n => '41700004' + String(n).padStart(2, '0'), X = n => '41700002' + String(n).padStart(2, '0');
const SPEC = {
  // the plate is 6 x 2 pieces (see world()), so a full sheet has exactly the freed spots to fill, as a real full sheet has
  sheets: [
    { id: 'e2e-gold-2', metal: 'gold', page: 2, full: true, items: [[H1, 1, 1, 0, 0], [H1, 1, 2, 1, 0], [H2, 2, 1, 2, 0], [H3, 2, 1, 3, 0], [H4, 1, 1, 4, 0], [H5, 1, 1, 5, 0], [X(1), 11, 1, 0, 1], [X(2), 12, 1, 1, 1], [X(3), 13, 1, 2, 1], [X(4), 14, 1, 3, 1], [X(8), 18, 1, 4, 1], [X(9), 19, 1, 5, 1]] },
    { id: 'e2e-silver-1', metal: 'silver', page: 1, full: true, items: [[H1, 2, 1, 0, 0], [H1, 2, 2, 1, 0], [H2, 3, 1, 2, 0], [H3, 3, 1, 3, 0], [H4, 2, 1, 4, 0], [H5, 2, 1, 5, 0], [X(5), 15, 1, 0, 1], [X(6), 16, 1, 1, 1], [X(7), 17, 1, 2, 1], [X(10), 20, 1, 3, 1], [X(11), 21, 1, 4, 1], [X(12), 22, 1, 5, 1]] },
    // the newer, incomplete sheets of each metal: the gold one's orders are waiting for a place, the silver one's are placed
    { id: 'e2e-gold-3', metal: 'gold', page: 3, items: [1, 2, 3, 4, 5, 6, 7, 8].map((n, i) => [W(n), 20 + n, 1, i % 6, Math.floor(i / 6), false]) },
    { id: 'e2e-silver-2', metal: 'silver', page: 2, items: [1, 2, 3, 4, 5, 6, 7, 8].map((n, i) => [V(n), 30 + n, 1, i % 6, Math.floor(i / 6)]) },
  ],
  orders: Object.assign({
    [H1]: O(9000, 'Hana One', [1, 2, 'gold', 'GFCHARM_ONE'], [2, 2, 'silver', 'SSCHARM_ONE']),
    [H2]: O(9100, 'Hana Two', CHAIN(1), [2, 1, 'gold', 'GFCHARM_TWO'], [3, 1, 'silver', 'SSCHARM_TWO']),
    [H3]: O(9200, 'Hana Three', CHAIN(1), [2, 1, 'gold', 'GFCHARM_THREE'], [3, 1, 'silver', 'SSCHARM_THREE']),
    [H4]: O(9300, 'Hana Four', [1, 1, 'gold', 'GFCHARM_FOUR'], [2, 1, 'silver', 'SSCHARM_FOUR']),
    [H5]: O(9400, 'Hana Five', [1, 1, 'gold', 'GFCHARM_FIVE'], [2, 1, 'silver', 'SSCHARM_FIVE']),
  }, Object.fromEntries([1, 2, 3, 4, 8, 9].map((n, i) => [X(n), O(8000 + i * 100, 'Gold Other ' + n, [10 + n, 1, 'gold', 'GFOTHER_' + n])])),
    Object.fromEntries([5, 6, 7, 10, 11, 12].map((n, i) => [X(n), O(7500 + i * 100, 'Silver Other ' + n, [10 + n, 1, 'silver', 'SSOTHER_' + n])])),
    Object.fromEntries([1, 2, 3, 4, 5, 6, 7, 8].map((n, i) => [W(n), O(7000 - i * 100, 'Waiting Gold ' + n, [20 + n, 1, 'gold', 'GFWAIT_' + n])])),
    Object.fromEntries([1, 2, 3, 4, 5, 6, 7, 8].map((n, i) => [V(n), O(6000 - i * 100, 'Newer Silver ' + n, [30 + n, 1, 'silver', 'SSNEWER_' + n])])))
};
const INCOMING = ['4170000901', '4170000902', '4170000903'];

const results = [], failures = [], notes = new Set();
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 700)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 5).join('\n         ')); };
const sleep = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
  const outside = [];
  await ctx.route(u => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: "window.pdfMake = { createPdf() { return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title>'], { type: 'text/html' })); } }; } };" });
    if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    if (/etsy/i.test(u)) outside.push(u);
    return r.abort();
  });
  await ctx.addInitScript(() => {
    try {
      if (!sessionStorage.getItem('__seeded')) {
        localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', pollOrders: 'off' }));
        localStorage.removeItem('cn.employee'); sessionStorage.setItem('__seeded', '1');
      }
    } catch (_) { /* about:blank */ }
    window.__asked = { prompt: 0, confirm: 0, alert: 0 };
    window.prompt = () => { window.__asked.prompt++; return 'Prompted'; }; window.confirm = () => { window.__asked.confirm++; return true; }; window.alert = () => { window.__asked.alert++; };
    window.__errs = []; addEventListener('unhandledrejection', e => window.__errs.push('unhandledrejection: ' + String(e.reason && e.reason.message || e.reason).slice(0, 300)));
    // the nest, as the test has it: what is pinned is placed where it was pinned; the waiting pieces go in the next free cells, whole orders at a time,
    // in the order the real nest ranks them (rankDate: an order released from hold first); __cap[sheetId] limits how many a search places; the sheet is saved as the real nest saves it
    const iv = setInterval(() => {
      if (typeof window.startNest !== 'function' || window.startNest.__stub) return;
      const MM = 72 / 25.4, P = window.CharmNestPDF, realPath = P.pathToCanvas;
      P.drawCharm = (c2, c, tx, k) => { const [x, y] = tx(c.centerPt[0], c.centerPt[1]); c2.beginPath(); c2.arc(x, y, c.rMm * MM * k, 0, 7); c2.lineWidth = Math.max(1, .35 * k); c2.strokeStyle = '#d0312d'; c2.stroke(); };
      P.pathToCanvas = (c2, p, tx) => { if (p && p.circle) { const [x, y] = tx(p.cx, p.cy), [x1] = tx(p.cx + p.r, p.cy), r = Math.abs(x1 - x); c2.moveTo(x + r, y); c2.arc(x, y, r, 0, Math.PI * 2); return; } return realPath(c2, p, tx); };
      P.cutLinesOf = () => [];
      const stub = sh => {
        window.__nests = (window.__nests || []).concat([{ metal: sh.metal, sheetId: sh.sheetId, at: Date.now(), charms: sh.charms.map(c => c.poolId) }]);
        sh.status = 'nesting'; sh.persistedDone = false; sh.persisted = Promise.resolve(); sh.jobId = 'job-' + Math.random().toString(36).slice(2, 8); sh.stage = 'test nest';
        setTimeout(async () => {
          try {
            for (const c of sh.charms) if (c.pinned && !sh.placements.some(p => p.id === c.id)) sh.placements.push({ id: c.id, cxPt: c.pinned.cxPt, cyPt: c.pinned.cyPt, angle: c.pinned.angle || 0, wPt: c.widthPt, hPt: c.heightPt });
            sh.placements = sh.placements.filter(p => sh.charms.some(c => c.id === p.id));
            const cells = []; for (let row = 0; row < 2; row++) for (let col = 0; col < 6; col++) cells.push([16 + col * 30, 16 + row * 30]);
            const free = cells.filter(([x, y]) => !sh.placements.some(p => Math.hypot(p.cxPt - x, p.cyPt - y) < 24));
            const rank = c => (window.CharmNestOrders && CharmNestOrders.rankDate ? CharmNestOrders.rankDate(c) : +c.orderDate || 0);
            const waiting = sh.charms.filter(c => !sh.placements.some(p => p.id === c.id)), byOrder = new Map();
            for (const c of waiting) { const k = String(c.order || c.id); if (!byOrder.has(k)) byOrder.set(k, []); byOrder.get(k).push(c); }
            const turn = [...byOrder.values()].sort((a, b) => rank(a[0]) - rank(b[0]) || String(a[0].order).localeCompare(String(b[0].order)));
            let cap = (window.__cap && window.__cap[sh.sheetId] != null) ? window.__cap[sh.sheetId] : 999, n = 0; const placedNow = [];
            for (const grp of turn) { if (grp.length > free.length || grp.length > cap - n) continue; for (const c of grp) { const [x, y] = free.shift(); sh.placements.push({ id: c.id, cxPt: x, cyPt: y, angle: 0, wPt: c.widthPt, hPt: c.heightPt }); placedNow.push(c.poolId); n++; } }
            window.__placed = (window.__placed || []).concat([{ sheetId: sh.sheetId, at: Date.now(), poolIds: placedNow }]);
            sh.feedWait = waiting.filter(c => !sh.placements.some(p => p.id === c.id)).map(c => c.id);
            await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, charms: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => ({ id: c.id, poolId: c.poolId, order: c.order })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => c.poolId) } }, { quiet: true });
          } catch (e) { sh.problem = e.message; }
          sh.status = 'complete'; sh.dirty = false; sh.intakeAppend = false; sh.appendOnly = false; sh.stage = ''; sh.persistedDone = true; sh.density = Math.min(.9, sh.placements.length / 12 * .8); sh.verification = { ok: true };
          try { CN.renderCard(sh); } catch (_) {}
        }, 150);
      };
      stub.__stub = true; window.startNest = stub; clearInterval(iv);
    }, 0);
  });
  const page = await ctx.newPage(), errors = [], consoleLines = [];
  page.setDefaultTimeout(30000);
  page.on('pageerror', e => { errors.push('page: ' + String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')); });
  page.on('console', m => { const t = m.text(); consoleLines.push(`[${m.type()}] ${t}`.slice(0, 400)); if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::/.test(t)) errors.push('console: ' + t.slice(0, 400)); });
  const until = async (fn, ms = 30000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await sleep(100); } };
  const shot = async (name, wait = 350) => { if (!SHOTS) return; await sleep(wait); try { await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } catch (e) { console.log('  (shot failed ' + name + ': ' + e.message + ')'); } };

  /* ── the page and the world ── */
  await page.goto(`${sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Orders && window.Review && window.OrderWin && window.SheetWin && SheetWin.holdKit && window.OrderHold && OrderHold.release && window.HoldUI && window.OrderHoldFx && window.CancelUI && window.Cancelled && window.Motion && window.NestFocus && window.Pool && window.LiveNest && window.startNest && window.startNest.__stub, null, { timeout: 60000 });
  const SHEET_SPEC = SPEC;
  async function world() {
    for (const k of [SHEETS, POOL, TL, 'Charm_Nest_Cancelled', 'Charm_Nest_Runs']) for (const [key] of [...st.docs]) if (key.startsWith(k + '/')) st.docs.delete(key);
    for (const sh of SHEET_SPEC.sheets) {
      const charms = [], placements = [];
      sh.items.forEach(([rid, tx, copy, col, row, placed], i) => { const poolId = pid(rid, tx, copy), id = `${sh.id}-c${i}`; charms.push({ id, poolId, order: rid }); if (placed !== false) placements.push({ id, cxPt: 16 + col * 30, cyPt: 16 + row * 30, angle: 0 }); st.put(POOL, poolId, { poolId, orderId: rid, transactionId: TID(tx), lineKey: kOf(rid, tx), sheetId: sh.id, state: 'placed', material: sh.metal, runId: RUN }); });
      st.put(SHEETS, sh.id, { id: sh.id, metal: sh.metal, charms, placements, poolIds: charms.map(c => c.poolId), sheetIndex: sh.page, fileBase: sh.id, runId: RUN });
    }
    await page.evaluate(async ({ spec, RUN, BASE_TS, SHIP }) => {
      await Orders.loadMaps(true); await Cancelled.load(true);
      localStorage.removeItem('cn.orderhold.run'); localStorage.removeItem('cn.sheetwin.freed'); window.__nests = []; window.__placed = []; window.__cap = {};
      // the plate holds 6 x 2 of these pieces (30 pt apart, the plate 182 x 62 pt) and not one more: a full sheet has room only where a piece was taken from
      CN.S.stockPreset = 'custom'; CN.S.settings.stock = Object.assign({}, CN.S.settings.stock, { gold: [182 / 72, 62 / 72], silver: [182 / 72, 62 / 72] });
      const MM = 72 / 25.4, R = 5, D = Math.ceil(2 * R * MM) + 2, TID = tx => String(5000000000 + tx), pid = (rid, tx, copy) => `${rid}_${TID(tx)}_${copy}`;
      // the charm's picture for the solver is a 4 px per pt bitmap, as a real charm's is (the solver reads it at 2 per pt: a coarser one is read as dots and the sheet looks empty to it)
      const SC = 4, DP = D * SC, mkBits = () => { const b = new Uint8Array(DP * DP); for (let y = 0; y < DP; y++) for (let x = 0; x < DP; x++) if (Math.hypot(x + .5 - DP / 2, y + .5 - DP / 2) <= DP / 2 - SC) b[y * DP + x] = 1; return b; };
      const mkCharm = (id, extra) => Object.assign({ id, name: id, sku: '', sourceId: 's', ringGeometryVersion: 3, centerPt: [D / 2, D / 2], bbox: [0, 0, D, D], outline: { circle: 1, cx: 0, cy: 0, r: R * MM }, members: [], rMm: R, w: DP, h: DP, scale: SC, bits: mkBits(), widthPt: 2 * R * MM, heightPt: 2 * R * MM, areaPt2: Math.PI * (R * MM) ** 2 }, extra || {});
      for (const m of Object.keys(CN.S.sheets)) { const pr = CN.S.sheets[m]; pr.pages.length = 1; const p0 = pr.pages[0]; p0.charms = []; p0.placements = []; p0.sheetId = null; p0.fileBase = null; for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId', 'label', 'keepRelease']) delete p0[k]; p0.status = 'idle'; p0.page = 1; pr.active = 0; }
      window.B.pool.rows.clear(); B.pool.sources.clear && B.pool.sources.clear();
      const ageOf = rid => (spec.orders[rid] || {}).age || 600;
      // the master files: each SKU has a design (a round charm) in the pool's own source cache, so Release hold makes its pieces up the real way
      const skus = new Set(); for (const o of Object.values(spec.orders)) for (const ln of o.lines) if (ln[1] !== 'chain') skus.add(ln[3]);
      window.__regSku = sku => { const key = 'test/' + sku + '.ai'; B.master.entries.set(sku, { sku, updatedAt: 1, aiPath: key, widthPt: 2 * R * MM, heightPt: 2 * R * MM, holes: 0, upAngle: null, labelSource: 'text' }); const src = { id: 'pool:' + sku, pool: true, name: sku, sku, charms: [mkCharm('pool:' + sku + ':0', { sku, name: sku })], usedAt: Date.now() }; B.pool.sources.set(key, src); };
      for (const sku of skus) window.__regSku(sku);
      for (const sh of spec.sheets) {
        const pr = CN.S.sheets[sh.metal]; let page = null;
        if (sh.page === 1) page = pr.pages[0]; else { while (pr.pages.length < sh.page) addPage(sh.metal); page = pr.pages[sh.page - 1]; }
        page.charms = []; page.placements = [];
        sh.items.forEach(([rid, tx, copy, col, row, placed], i) => {
          const poolId = pid(rid, tx, copy), id = `${sh.id}-c${i}`, o = spec.orders[rid], ln = o.lines.find(l => l[0] === tx);
          page.charms.push(mkCharm(id, { name: `${rid} · ${ln[3]}`, sku: ln[3], poolId, order: rid, lineKey: `${rid}_${TID(tx)}`, orderDate: (BASE_TS - ageOf(rid)), orderInfo: { receiptId: rid, transactionId: TID(tx), sku: ln[3], copy, quantity: ln[1] } }));
          if (placed !== false) page.placements.push({ id, cxPt: 16 + col * 30, cyPt: 16 + row * 30, angle: 0, wPt: 2 * R * MM, hPt: 2 * R * MM });
          window.B.pool.rows.set(poolId, { poolId, orderId: rid, transactionId: TID(tx), lineKey: `${rid}_${TID(tx)}`, sheetId: sh.id, state: 'placed', material: sh.metal, runId: RUN });
        });
        Object.assign(page, { status: 'complete', sheetId: sh.id, fileBase: sh.id, sheetIndex: sh.page, page: sh.page, runId: RUN, dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, density: sh.full ? .8 : .3, releaseFull: !!sh.full, verification: { ok: true }, intakeAppend: false, appendOnly: false });
        for (const k of ['laserDoneAt', 'roseCutAt', 'recalled']) delete page[k];
        pr.active = Math.max(0, sh.page - 1); CN.renderCard(page);
      }
      const rows = [];
      for (const [rid, o] of Object.entries(spec.orders)) {
        const lines = o.lines.map(ln => ln[1] === 'chain'
          ? { transactionId: TID(ln[0]), listingId: '19008' + String(ln[0]).padStart(5, '0'), sku: ln[3], title: ln[3] + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Charm', value: 'Chain only' }], metalKey: null, metalLabel: '', personalization: [], buyerMessage: '' }
          : { transactionId: TID(ln[0]), listingId: '19009' + String(ln[0]).padStart(5, '0'), sku: ln[3], title: ln[3].replace(/_/g, ' ') + ' charm', quantity: ln[1], expectedShipDate: SHIP, variations: [{ name: 'Metal', value: ln[2] === 'gold' ? '14k Gold Filled' : 'Sterling Silver' }], metalKey: ln[2], metalLabel: ln[2] === 'gold' ? 'GF 14/20' : 'SS', personalization: [], buyerMessage: '' });
        const order = { receiptId: rid, orderNumber: rid, createTs: BASE_TS - ageOf(rid), updateTs: BASE_TS, shipBy: SHIP, buyer: { name: o.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines };
        for (const line of lines) {
          const ln = o.lines.find(l => TID(l[0]) === line.transactionId), chain = ln[1] === 'chain', key = CharmNestOrders.lineKey(order, line);
          const poolIds = chain ? [] : Array.from({ length: ln[1] }, (_, i) => pid(rid, ln[0], i + 1));
          rows.push({ key, order, line, arrivedAt: Date.now() - 7200000, spec: null, problems: [], state: chain ? 'pulled' : 'pooled', reason: null, claimedBy: null, poolIds, engrave: null, material: chain ? null : ln[2] });
        }
      }
      B.orders.rows.length = 0; B.orders.byKey.clear(); for (const r of rows) { B.orders.rows.push(r); B.orders.byKey.set(r.key, r); }
      B.sets.clear();
      Orders.interpretAll();
      const day = new Date().toISOString().slice(0, 10);
      B.run = { runId: RUN, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: Object.keys(spec.orders) };
      Review.syncOrderItems(); CN.setMode('orders'); Orders.showPile('', ''); Orders.render();
      // the dialogs open at once, ever (never a pop-up over a pop-up); the longest a frame took
      if (!window.__dlg) { window.__dlg = { max: 0 }; new MutationObserver(() => { const n = document.querySelectorAll('dialog[open]').length; if (n > window.__dlg.max) window.__dlg.max = n; }).observe(document.documentElement, { subtree: true, attributes: true, attributeFilter: ['open'], childList: true }); }
    }, { spec: SHEET_SPEC, RUN, BASE_TS, SHIP });
    await sleep(300);
  }
  await world();

  /* ── readers ── */
  const poolDoc = id => st.doc(POOL, id) || {}, sheetDoc = id => st.doc(SHEETS, id) || {};
  const tlOf = (rid, type) => st.list(TL).filter(x => x._id.startsWith(rid + '~') && (!type || x.type === type));
  const onSheets = () => page.evaluate(() => Object.fromEntries(Object.keys(CN.S.sheets).flatMap(m => CN.S.sheets[m].pages.filter(p => p.sheetId).map(p => [p.sheetId, { all: p.charms.map(c => c.poolId), placed: p.placements.map(pl => (p.charms.find(c => c.id === pl.id) || {}).poolId) }]))));
  const rowsOf = rid => page.evaluate(rid => Orders.rows().filter(r => String(r.order.receiptId) === rid).map(r => ({ key: r.key, state: r.state, hold: r.hold || null, pieces: r.poolIds.length, frontAt: r.frontAt || 0, releasing: !!r.releasing })), rid);
  const mutatingCalls = () => st.calls.filter(c => c.name === 'charmNestLibrary' && /^(poolPut|poolUpdate|putSheet|setUpdate|cancelPut|cancelRestore|customPut|customReopen|runPut|timelineAdd)$/.test(String(c.op)));
  const mutating = () => mutatingCalls().length;
  // (the page's own first read of the orders, 'interpreted', is recorded in slices and goes out on the timeline outbox's clock, again after a backoff when a send failed on a
  //  busy machine: it is not what a press did. Every count of writes starts once the outbox is empty and no write has come for 1.5 s, so a press is only blamed for what it wrote itself)
  const outboxQuiet = async () => {
    let was = -1, since = Date.now();
    for (let i = 0; i < 300; i++) {
      const left = await page.evaluate(() => { if (!window.OrderTimeline) return 0; try { OrderTimeline.flush && OrderTimeline.flush(); } catch (_) {} return OrderTimeline.pending ? OrderTimeline.pending() : 0; });
      const n = mutating();
      if (left !== 0 || n !== was) { was = n; since = Date.now(); } else if (Date.now() - since >= 1500) return;
      await sleep(200);
    }
    throw new Error('the timeline outbox never went quiet before the press');
  };
  const idsOf = (rid, ...txs) => txs.flatMap(tx => { const o = SPEC.orders[rid], ln = o.lines.find(l => l[0] === tx); return Array.from({ length: ln[1] }, (_, i) => pid(rid, tx, i + 1)); });
  const sorted = a => a.slice().sort();
  const allIds = () => SPEC.sheets.flatMap(sh => sh.items.map(([rid, tx, copy]) => pid(rid, tx, copy)));
  /** no order is lost: each piece is on exactly one sheet, or on hold with its record saying who held it; the saved sheets list what the page holds */
  async function noLoss(what, expectHeld, gone = []) {
    const sheets = await onSheets(), flat = Object.values(sheets).flatMap(s => s.all);
    assert.equal(new Set(flat).size, flat.length, `${what}: no piece is on two sheets`);
    for (const id of allIds()) {
      const where = Object.entries(sheets).filter(([, s]) => s.all.includes(id)).map(([k]) => k), p = poolDoc(id), held = p.state === 'abandoned' && !!p.heldBy;
      if (gone.includes(id)) { assert.equal(where.length, 0, `${what}: ${id} belongs to a cancelled order, so it is on no sheet: ${JSON.stringify(where)}`); continue; }   // (a cancelled order's pieces are kept in its Cancelled record)
      assert(where.length === 1 ? !held : where.length === 0 && held, `${what}: ${id} is on one sheet or on hold, never lost: sheets ${JSON.stringify(where)}, record ${JSON.stringify(p)}`);
    }
    for (const [sid, s] of Object.entries(sheets)) { const rec = sheetDoc(sid); if (s.placed.length) assert.deepEqual(sorted(rec.poolIds || []), sorted(s.placed.filter(Boolean)), `${what}: the saved ${sid} lists what the page holds`); }
  }
  /** what is on the screen that should not be, when nothing is running */
  const leftovers = () => page.evaluate(() => {
    const out = [];
    if (document.getElementById('hfxLayer')) out.push('film layer #hfxLayer');
    const ml = document.getElementById('motionLayer'); if (ml && ml.children.length) out.push('motion layer children: ' + ml.children.length);
    for (const s of ['.holdDlg', '.holdInline', '.cnBeacon', '.hfxSkip', '.hfxCap', '.hfxGlow', '.hfxSpot', '.hfxChip', '.mGhost', '.cnNameBar', '.nfCap', '.nfGlow', '.nfFocus', '.hfxLit', '.hfxDim', '#sheets.nfDim', '#sheets.nfOn', '#sheets.nfNow', '.cnTick']) { const n = document.querySelectorAll(s).length; if (n) out.push(`${s} x${n}`); }
    const d = [...document.querySelectorAll('dialog[open]')].map(x => x.id || x.className); if (d.length) out.push('dialogs open: ' + d.join(','));
    if (document.documentElement.scrollWidth > innerWidth + 1) out.push('page scrolls sideways');
    return out;
  });
  const quiet = async what => { const l = await until(async () => { const x = await leftovers(); return x.length ? false : true; }, 15000, `${what}: nothing left on the screen`).then(() => [], async () => leftovers()); assert.deepEqual(l, [], `${what}: leftovers on the screen: ${l.join(' | ')}`); };

  /** a film's frame times, the dialogs, the layer's place: sampled in the page while it runs */
  const clearNotes = () => page.evaluate(() => { for (const n of document.querySelectorAll('.mNote')) n.remove(); });
  const sampler = () => page.evaluate(() => {
    const S = window.__film = { run: true, last: performance.now(), gaps: [], caps: [], dlg: 0, layerOver: true, nameBar: 0, topBarCovered: 0, bottomUnder: 0 };
    let lastCap = ''; S.long = [];
    try { new PerformanceObserver(l => { for (const e of l.getEntries()) S.long.push({ t: Math.round(e.startTime), d: Math.round(e.duration), cap: lastCap.slice(0, 40) }); }).observe({ type: 'longtask', buffered: false }); } catch (_) {}
    const tick = now => {
      S.gaps.push(now - S.last); S.last = now;
      const live = document.querySelector('#hfxLayer .hfxLive'), t = live ? live.textContent : '';
      if (t && t !== lastCap) { lastCap = t; S.caps.push({ t: Math.round(now), text: t, mode: CN.S.mode }); }
      if (live) {
        const open = document.querySelectorAll('dialog[open]').length; if (open) S.dlg = Math.max(S.dlg, open);
        const tb = document.querySelector('.topbar'); if (tb) { const r = tb.getBoundingClientRect(); for (const n of document.querySelectorAll('#hfxLayer .hfxCap, #hfxLayer .hfxMark, #hfxLayer .hfxSkip')) { const q = n.getBoundingClientRect(); if (q.height && q.top < r.bottom - 1 && r.bottom > 0) { S.topBarCovered++; break; } } }
        if (document.querySelector('.cnNameBar')) S.nameBar++;
        for (const n of document.querySelectorAll('.mNote')) { S.notes = (S.notes || 0) + 1; (S.noteText = S.noteText || []).includes(n.textContent) || S.noteText.push(n.textContent.slice(0, 90)); }   // (a note drawn while the film plays would sit at the top left, over the tabs)
      }
      if (S.run) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  });
  const sampled = () => page.evaluate(() => { const S = window.__film; S.run = false; const g = S.gaps.slice(2).sort((a, b) => a - b); return { long: S.long.filter(x => x.d >= 100).sort((a, b) => b.d - a.d).slice(0, 6), caps: S.caps, n: g.length, p50: g[Math.floor(g.length * .5)] || 0, p95: g[Math.floor(g.length * .95)] || 0, max: g[g.length - 1] || 0, over100: g.filter(x => x > 100).length, dlg: S.dlg, nameBar: S.nameBar, topBarCovered: S.topBarCovered, notes: S.notes || 0, noteText: S.noteText || [] }; });
  const frames = [];   // every film's frame stats, for the end
  /** the way home is walked once: its start is the one "home" event that says instant or not (an instant walk adds a second one when the card is found) */
  const walks = log => log.filter(e => e.ev === 'home' && 'instant' in e);

  /** the film's own log (OrderHoldFx.events): captions, flights, focus, settle, home */
  const fxLog = () => page.evaluate(() => OrderHoldFx.events().map(e => Object.assign({}, e)));
  const captionsOf = log => log.filter(e => e.ev === 'caption').map(e => e.main + (e.small ? ' · ' + e.small : ''));
  const inOrder = (got, wantRes) => { let i = 0; for (const t of got) if (i < wantRes.length && wantRes[i].test(t)) i++; return i === wantRes.length ? true : `got to ${i} of ${wantRes.length} (${wantRes[i]}) in: ${got.join(' | ')}`; };
  const stepsOf = rid => page.evaluate(rid => (window.__steps || {})[rid] || [], rid);
  /** records every step the engine really emits, per order, and what the film was given (wraps, never replaces) */
  await page.evaluate(() => {
    window.__steps = {}; window.__filmFed = {};
    const E = window.OrderHold, wrap = name => { const orig = E[name]; E[name] = function (rid, o) { o = Object.assign({}, o); const was = o.onStep; window.__steps[rid] = []; o.onStep = s => { window.__steps[rid].push(JSON.parse(JSON.stringify(s))); if (was) was(s); }; return orig.call(this, rid, o); }; };
    wrap('run'); wrap('release');
  });

  /* helpers for the screens */
  const openWin = async key => { await page.evaluate(k => OrderWin.open(k), key); await page.waitForFunction(k => OrderWin.key() === k && document.querySelectorAll('#owPcSum .owPcRow').length >= 1, key, { timeout: 20000 }); await page.waitForFunction(() => document.querySelector('#owPcSum [data-hold-btn]'), null, { timeout: 15000 }); };
  const dlgText = () => page.evaluate(() => { const d = document.querySelector('dialog.holdDlg[open], .holdInline'); return d ? d.innerText : null; });
  const holdBtns = rid => page.evaluate(r => [...document.querySelectorAll(`[data-hold-btn][data-rid="${r}"]`)].map(b => ({ text: b.textContent.trim(), src: b.dataset.src, visible: !b.hidden && b.offsetParent !== null, where: b.closest('#owPcSum') ? 'row' : b.closest('#rvList') ? 'card' : 'other' })), rid);
  const waitHome = async (rid, what) => { await page.waitForFunction(r => !HoldUI.busy(r) && CN.S.mode === 'orders' && Orders.view().pile === 'hold' && !document.getElementById('hfxLayer'), rid, { timeout: 120000 }).catch(async e => { throw new Error(`${what}: never home: ${JSON.stringify(await page.evaluate(() => ({ mode: CN.S.mode, pile: Orders.view().pile, layer: !!document.getElementById('hfxLayer'), busy: HoldUI.busy('x') })))}`); }); };
  const keyOfLine = (rid, tx) => kOf(rid, tx);
  const onHoldCards = rid => page.evaluate(r => [...document.querySelectorAll('#ordItems [data-rid]')].filter(n => n.dataset.rid === r).map(n => ({ text: n.innerText.replace(/\s+/g, ' ').trim(), buttons: [...n.querySelectorAll('button')].map(b => b.textContent.trim()), pill: (n.querySelector('.ost') || {}).textContent })), rid);

  async function flow(name, fn) { if (!want(name.charAt(0))) return; console.log('\n── ' + name); try { await fn(); } catch (e) { fail(name, e); } }
  /** the order is put on hold by the engine directly when a flow needs it held and its own flow did not get there */
  async function ensureHeld(rid) {
    if ((await rowsOf(rid)).every(r => r.hold)) return;
    const r = await page.evaluate(rid => OrderHold.run(rid, { name: 'Paul' }).then(x => ({ ok: x.ok, error: x.error })), rid);
    if (!r.ok) throw new Error('could not put ' + rid + ' on hold to carry on: ' + r.error);
    await page.evaluate(() => { Review.syncOrderItems(); Orders.render(); });
  }

  /* ═════ A · Hold from an order window piece row; no name saved ═════ */
  await flow('A · Hold from the order window\'s piece row (order ' + H1 + ', two sheets)', async () => {
    const before = await onSheets();
    assert.equal(before['e2e-gold-2'].all.filter(id => id.startsWith(H1)).length, 2); assert.equal(before['e2e-silver-1'].all.filter(id => id.startsWith(H1)).length, 2);
    assert.equal(await page.evaluate(() => CNEmployee.name()), '', 'no name saved on this computer');
    await openWin(keyOfLine(H1, 1));
    const rowsUi = await page.evaluate(() => [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => ({ piece: r.dataset.piece, hold: [...r.querySelectorAll('[data-hold-btn]')].map(b => { const c = getComputedStyle(b); return { text: b.textContent.trim(), bg: c.backgroundColor, color: c.color, shown: b.offsetParent !== null }; }) })));
    assert.equal(rowsUi.length, 2, 'two piece rows: ' + JSON.stringify(rowsUi));
    for (const r of rowsUi) { assert.equal(r.hold.length, 1, 'one Hold on each piece row: ' + JSON.stringify(r)); assert(r.hold[0].text === 'Hold' && r.hold[0].shown && r.hold[0].bg === 'rgb(162, 89, 28)' && r.hold[0].color === 'rgb(255, 255, 255)', 'the orange Hold: ' + JSON.stringify(r.hold[0])); }
    pass('the order window shows the orange Hold on each piece row of ' + H1);
    await shot('a1-orderwin-hold-button');
    await outboxQuiet(); const w0 = mutating();
    // the press on the SECOND row's button (the silver piece): the whole order is held whichever piece's button was pressed
    await page.click(`#owPcSum [data-piece="${keyOfLine(H1, 2)}"] [data-hold-btn]`);
    await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 20000 });
    await sleep(400);
    assert.equal(await page.evaluate(() => document.getElementById('orderWin').open), false, 'the order window stepped aside first');
    assert.equal(await page.evaluate(() => document.querySelectorAll('dialog[open]').length), 1, 'one dialog: never a pop-up over a pop-up');
    let text = await dlgText();
    for (const w of [[`Put order ${H1} on hold?`, /Put order 4170000100 on hold\?/], ['4 pieces come off GF Sheet 2 and SS Sheet 1', /4 pieces of this order come off GF Sheet 2 and SS Sheet 1/], ['waiting orders fill', /2 waiting orders and 2 orders from SS Sheet 2 fill the 4 empty spots/], ['QR labels', /QR labels are made on 2 sheets/], ['Release hold', /until someone presses Release hold/], ['nothing deleted', /Nothing is deleted/], ['Continue', /Continue/], ['Not now', /Not now/]]) assert(w[1].test(text), `the consent popup says (${w[0]}): ${text.replace(/\s+/g, ' ')}`);
    assert(!/\blines?\b/i.test(text), '"pieces", never "lines": ' + text);
    assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll('dialog.holdDlg [data-k]')].map(x => x.textContent.trim())), ['Not now', 'Continue'], 'two buttons');
    pass('the consent popup: plain words, what comes off, what moves in, QR labels, the wait in On hold, nothing deleted, Continue and Not now; one dialog, the window stepped aside');
    await shot('a2-consent-popup');
    await page.click('dialog.holdDlg [data-k=no]');
    await page.waitForFunction(() => !document.querySelector('dialog.holdDlg') && OrderWin.isOpen() && OrderWin.key().startsWith('4170000100_'), null, { timeout: 15000 });
    assert.equal(mutating() - w0, 0, 'Not now wrote nothing: ' + JSON.stringify(mutatingCalls().slice(w0).map(c => [c.op, JSON.stringify(c.body || c.payload || c.args || {}).slice(0, 160)])));
    assert.deepEqual(await onSheets(), before, 'Not now changed no sheet');
    assert((await rowsOf(H1)).every(r => !r.hold && r.state === 'pooled'), 'Not now: the order is as it was');
    assert.equal(await page.evaluate(() => document.querySelectorAll('.cnNameBar').length), 0, 'no name asked for a Not now');
    pass('Not now: nothing written, no sheet changed, the order window is back on the same order');
    // Continue: the name bar (no name saved), then the film
    await page.waitForFunction(() => { const b = document.querySelector('#owPcSum [data-hold-btn]'); return b && !b.disabled && b.textContent.trim() === 'Hold'; }, null, { timeout: 8000 });
    await page.click(`#owPcSum [data-piece="${keyOfLine(H1, 1)}"] [data-hold-btn]`);
    await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 20000 });
    await page.evaluate(() => OrderHoldFx.clearEvents());
    await page.click('dialog.holdDlg [data-k=go]');
    await page.waitForFunction(() => document.querySelector('.cnNameBar input'), null, { timeout: 15000 });
    assert.equal((await page.evaluate(() => window.__asked)).prompt, 0, 'the inline name bar, never prompt()');
    assert.equal(mutating() - w0, 0, 'nothing is written before the name is given');
    assert.equal(await page.evaluate(() => (window.__steps[Object.keys(window.__steps)[0]] || []).length), 0, 'the engine has not run');
    await shot('a3-name-bar');
    await sampler();
    await page.fill('.cnNameBar input', 'Paul'); await page.keyboard.press('Enter');
    // the film, in the Nest tab: four frames
    await page.waitForFunction(() => document.getElementById('hfxLayer') && CN.S.mode === 'nest', null, { timeout: 30000 });
    const grabs = [['Taking the pieces off', 'a4-film-1-lift'], ['Filling the empty spots', 'a4-film-2-fill-begin'], ['Placed on', 'a4-film-3-placed'], ['is on hold', 'a4-film-4-held']];
    for (const [re, name] of grabs) { try { await page.waitForFunction(re => { const l = document.querySelector('#hfxLayer .hfxLive'); return l && l.textContent.includes(re); }, re, { timeout: 60000 }); await shot(name, 120); } catch (_) { console.log('  (no frame for ' + re + ')'); } }
    await waitHome(H1, 'A');
    const fr = await sampled(); frames.push(['A', fr]);
    await sleep(900);
    await shot('a5-on-hold-card', 600);
    // ── the film told the story of the real steps ──
    const steps = await stepsOf(H1), T = steps.map(s => s.type);
    assert.equal(T[0], 'start'); assert.deepEqual(T.slice(-3), ['sheetDone', 'held', 'done'], 'the engine ended: ' + T.join(' '));
    assert(T.includes('lift') && T.includes('fillFrom') && T.includes('fillPlaced') && T.includes('qr') && T.includes('sheetDone'), 'the engine told the whole story: ' + T.join(' '));
    const log = await fxLog(), caps = captionsOf(log);
    const sheetStory = lab => [new RegExp('^' + lab), /^Taking the pieces off/, /^Filling the empty spots? ·/, /^Filling the empty spots? from/, /^Placed on/, /^Remaking QR labels/, /is done/];
    const order = inOrder(caps, [/^Putting order 4170000100 on hold/, ...sheetStory('GF Sheet 2 ·'), ...sheetStory('SS Sheet 1 ·'), /^Order 4170000100 is on hold/]);
    assert.equal(order, true, 'the captions followed the steps in order: ' + order);
    // what the film showed matches the data: a lift flight for each piece the engine lifted, a fill flight and a settle for each piece that moved in
    const flights = log.filter(e => e.ev === 'flight').map(e => e.key);
    for (const s of steps.filter(s => s.type === 'lift')) for (const id of s.poolIds) assert(flights.includes(`lift:${s.sheetId}:${id}`), `the film lifted ${id} off ${s.sheetId} (flights: ${flights.join(', ')})`);
    for (const s of steps.filter(s => s.type === 'fillFrom')) for (const id of s.poolIds) assert(flights.includes(`fill:${s.toSheetId}:${id}`), `the film brought ${id} into ${s.toSheetId}`);
    const settled = log.filter(e => e.ev === 'settle').map(e => e.pool);
    for (const s of steps.filter(s => s.type === 'fillPlaced')) for (const id of s.poolIds) assert(settled.includes(id), `the film settled ${id} where the sheet put it (${settled.join(',')})`);
    const liftFlights = flights.filter(k => k.startsWith('lift:')).length, liftN = steps.filter(s => s.type === 'lift').reduce((n, s) => n + s.poolIds.length, 0);
    assert.equal(liftFlights, liftN, 'one lift flight per piece taken off: ' + liftFlights + ' vs ' + liftN);
    const end = log.find(e => e.ev === 'end'); assert(end && end.ok && !end.skipped && !end.timedOut && !end.stalled, 'the film ended on its own, in order: ' + JSON.stringify(end));
    assert.equal(walks(log).length, 1, 'the way home was walked once (the film and the glue both ask for it): ' + JSON.stringify(log.filter(e => e.ev === 'home')));
    pass('the film ran the engine\'s own steps: captions in order, one lift flight per piece taken off, a flight and a settle for each piece that moved in; it ended by itself in ' + end.ms + ' ms');
    // ── where the user ended ──
    assert.deepEqual(await page.evaluate(() => ({ mode: CN.S.mode, pile: Orders.view().pile })), { mode: 'orders', pile: 'hold' }, 'the user ends in Orders > On hold');
    const cards = await onHoldCards(H1);
    assert.equal(cards.length, 2, 'the order has a card per piece line in On hold: ' + JSON.stringify(cards));
    for (const c of cards) { assert(/HELD/i.test(c.pill || '') && /Taken off GF Sheet 2, SS Sheet 1 by Paul/.test(c.text), 'the card says Taken off ... by Paul: ' + c.text); assert(c.buttons.includes('Release hold') && c.buttons.includes('Cancel Order'), 'the card has Release hold and Cancel Order: ' + c.buttons); }
    assert.deepEqual(await holdBtns(H1), [], 'no Hold button stays visible on a held order');
    pass('the user is back in Orders > On hold: the order\'s cards say "Taken off GF Sheet 2, SS Sheet 1 by Paul", with Release hold and Cancel Order; no Hold button remains');
    // ── the data ──
    const sh = await onSheets();
    for (const id of idsOf(H1, 1, 2)) { assert(!sh['e2e-gold-2'].all.includes(id) && !sh['e2e-silver-1'].all.includes(id), id + ' is off every sheet'); const p = poolDoc(id); assert(p.state === 'abandoned' && p.heldBy === 'Paul' && !p.sheetId, 'its record: held by Paul, off its sheet: ' + JSON.stringify(p)); }
    const fromGold = steps.filter(s => s.type === 'fillFrom' && s.toSheetId === 'e2e-gold-2'), fromSilver = steps.filter(s => s.type === 'fillFrom' && s.toSheetId === 'e2e-silver-1');
    assert.deepEqual(fromGold.map(s => [s.source, s.fromSheetId]), [['waiting', 'e2e-gold-3'], ['waiting', 'e2e-gold-3']], 'GF Sheet 2 was refilled from the orders waiting: ' + JSON.stringify(fromGold));
    assert.deepEqual(fromSilver.map(s => [s.source, s.fromSheetId]), [['newerSheet', 'e2e-silver-2'], ['newerSheet', 'e2e-silver-2']], 'SS Sheet 1 (none waiting) from the newer SS Sheet 2: ' + JSON.stringify(fromSilver));
    assert.deepEqual(fromGold.map(s => s.rid), [W(1), W(2)], 'the two oldest waiting orders, the oldest first: ' + JSON.stringify(fromGold.map(s => s.rid)));
    assert.deepEqual(fromSilver.map(s => s.rid), [V(1), V(2)], 'the two oldest orders of the newer SS sheet, the oldest first: ' + JSON.stringify(fromSilver.map(s => s.rid)));
    assert.equal(sh['e2e-gold-2'].placed.length, 12, 'GF Sheet 2 is full again: ' + sh['e2e-gold-2'].placed.length);
    assert.equal(sh['e2e-silver-1'].placed.length, 12, 'SS Sheet 1 is full again: ' + sh['e2e-silver-1'].placed.length);
    const moved = poolDoc(fromGold[0].poolIds[0]); assert(moved.sheetId === 'e2e-gold-2' && moved.movedFrom === 'e2e-gold-3' && moved.movedBy === 'Paul', 'a moved piece says where from and who moved it: ' + JSON.stringify(moved));
    assert((await rowsOf(H1)).every(r => r.state === 'held' && r.hold === 'Taken off GF Sheet 2, SS Sheet 1 by Paul' && r.pieces === 0), 'the lines: held, one reason: ' + JSON.stringify(await rowsOf(H1)));
    const ev = tlOf(H1, 'held'); assert.equal(ev.length, 2, 'one timeline step per sheet'); for (const e of ev) assert(e.by === 'Paul' && e.at > 1.79e12 && /Sheet/.test(e.text) && e.sheetId, 'a step with the sheet, the person and the time: ' + JSON.stringify([e.text, e.by, e.at, e.sheetId]));
    assert(ev.some(e => e.sheetId === 'e2e-gold-2') && ev.some(e => e.sheetId === 'e2e-silver-1'), 'both sheets are on the timeline');
    await noLoss('A', [H1]);
    for (const q of steps.filter(s => s.type === 'qr')) assert(q.sheetId, 'QR step names its sheet');
    assert.equal(steps.filter(s => s.type === 'qr').length, 2, 'QR labels remade on both sheets (the engine says so)');
    pass('the data: all four pieces off every sheet (held by Paul), GF Sheet 2 refilled by the oldest waiting orders, SS Sheet 1 from the newer SS Sheet 2, both sheets full again, one timeline step per sheet with sheet, person and time, no piece lost');
    // the Nest tab shows the refill, not the old sheet
    await page.evaluate(() => CN.setMode('nest')); await sleep(900);
    const cardsText = await page.evaluate(() => Object.fromEntries(Object.keys(CN.S.sheets).map(m => { const p = CN.S.sheets[m].pages.find(x => x.sheetId); return [m, p ? { n: p.charms.length, pl: p.placements.length } : null]; })));
    await shot('a6-nest-after', 200);
    await page.evaluate(() => CN.setMode('orders'));
    await quiet('A');
    pass('nothing left on the screen after the film (no layer, no ghost, no beacon, no dialog, no name bar)');
  });

  // (the work of the later flows rides on this one)
  await ensureHeld(H1).catch(e => fail('H1 could not be held to carry on', e));
  await page.evaluate(() => { document.getElementById('toasts') && (document.getElementById('toasts').innerHTML = ''); });

  /* the saved name from here on */
  await page.evaluate(() => { B.employee = 'Paul'; try { localStorage.setItem('cn.employee', 'Paul'); } catch (_) {} });

  /* ── what B, C and F share: one Hold pressed on a real button, followed to the end, and the checks of what it did ── */
  /** the numbers the consent popup promised, read from its words; and what the engine then did */
  const promised = text => { const n = re => { const m = re.exec(text); return m ? +m[1] : null; }; return { off: n(/(\d+) pieces? of this order come off/), spots: n(/fill the (\d+) empty spots?/), waiting: n(/(\d+) waiting orders?/), newer: n(/(\d+) orders? from [A-Z]{2} Sheet \d+/) }; };
  const delivered = steps => ({ off: steps.filter(s => s.type === 'lift').reduce((n, s) => n + s.poolIds.length, 0), spots: steps.filter(s => s.type === 'fillBegin').reduce((n, s) => n + s.spots, 0),
    // (counted the way the popup counts: each order ONCE, by its order id, however many sheets it fills; when one order fills a spot on two sheets the popup says "1 order", see the note)
    waiting: new Set(steps.filter(s => s.type === 'fillFrom' && s.source === 'waiting').map(s => s.rid)).size,
    newer: new Set(steps.filter(s => s.type === 'fillFrom' && s.source === 'newerSheet' && !steps.some(w => w.type === 'fillFrom' && w.source === 'waiting' && w.rid === s.rid)).map(s => s.rid)).size });
  const oneOrderTwoSheets = (steps, tag) => { const by = new Map(); for (const s of steps.filter(s => s.type === 'fillFrom')) by.set(s.rid, new Set([...(by.get(s.rid) || []), s.toSheetId])); for (const [rid, sh] of by) if (sh.size > 1) notes.add(`${tag}: one order (${rid}) filled the empty spots on ${sh.size} sheets (its gold piece on one, its silver piece on the other); the popup counts it once ("1 order from GF Sheet 3 and SS Sheet 2"), the spots and the pieces are as promised`); };
  const sheetStory = lab => [new RegExp('^' + lab), /^Taking the pieces off/, /^Filling the empty spots? ·/, /^Filling the empty spots? from/, /^Placed on/, /^Remaking QR labels/, /is done/];

  /** Presses the Hold at `btnSel`, reads the popup, presses Continue (the name is saved), follows the film to the end, waits until the person is home. */
  async function holdFrom(rid, btnSel, tag, o = {}) {
    await page.waitForFunction(s => { const b = document.querySelector(s); return b && !b.disabled && b.textContent.trim() === 'Hold'; }, btnSel, { timeout: 20000 });
    await outboxQuiet();
    const before = await onSheets(), w0 = mutating();
    await page.evaluate(() => OrderHoldFx.clearEvents());
    await page.click(btnSel);
    await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open], .holdInline'), null, { timeout: 20000 });
    await sleep(350);
    const text = await dlgText();
    assert.equal(await page.evaluate(() => document.querySelectorAll('dialog[open]').length), 1, `${tag}: one dialog open, never a pop-up over a pop-up`);
    assert.equal(mutating() - w0, 0, `${tag}: nothing is written while the popup is asked`);
    await shot(tag + '1-consent-popup');
    await clearNotes();   // (a note a step before the Hold left, e.g. "moved to Completed" from pressing Complete Order, is that press's, not the film's)
    await sampler();
    if (o.during) o.during();
    await page.click('dialog.holdDlg [data-k=go]');
    assert.equal(await page.evaluate(() => document.querySelectorAll('.cnNameBar').length), 0, `${tag}: the name is saved, so none is asked`);
    await page.waitForFunction(() => document.getElementById('hfxLayer') && CN.S.mode === 'nest', null, { timeout: 30000 });
    for (const [re, name] of o.grabs || []) { try { await page.waitForFunction(re => { const l = document.querySelector('#hfxLayer .hfxLive'); return l && l.textContent.includes(re); }, re, { timeout: 60000 }); await shot(name, 120); } catch (_) { console.log('  (no frame for ' + re + ')'); } }
    if (o.mid) await o.mid();
    await waitHome(rid, tag);
    const fr = await sampled(); frames.push([tag, fr]);
    await sleep(900);
    return { text, before, fr };
  }

  /** what a Hold did, said by the data and the film: every flight, every caption, the cards, the sheets, the records, the timeline */
  async function verifyHold(rid, tag, x, seen) {
    const steps = await stepsOf(rid), T = steps.map(s => s.type);
    assert.equal(T[0], 'start'); assert.deepEqual(T.slice(-3), ['sheetDone', 'held', 'done'], `${tag}: the engine ended: ` + T.join(' '));
    assert.deepEqual(sorted(steps.filter(s => s.type === 'sheetBegin').map(s => s.sheetId)), sorted(x.sheets), `${tag}: the engine worked on these sheets`);
    const begun = steps.filter(s => s.type === 'sheetBegin'), log = await fxLog(), caps = captionsOf(log);
    const order = inOrder(caps, [new RegExp(`^Putting order ${rid} on hold`), ...begun.flatMap(s => sheetStory(s.label + ' ·')), new RegExp(`^Order ${rid} is on hold`)]);
    assert.equal(order, true, `${tag}: the captions followed the steps in order: ` + order);
    const flights = log.filter(e => e.ev === 'flight').map(e => e.key), settled = log.filter(e => e.ev === 'settle').map(e => e.pool);
    for (const s of steps.filter(s => s.type === 'lift')) for (const id of s.poolIds) assert(flights.includes(`lift:${s.sheetId}:${id}`), `${tag}: the film lifted ${id} off ${s.sheetId} (flights: ${flights.join(', ')})`);
    for (const s of steps.filter(s => s.type === 'fillFrom')) for (const id of s.poolIds) assert(flights.includes(`fill:${s.toSheetId}:${id}`), `${tag}: the film brought ${id} into ${s.toSheetId}`);
    for (const s of steps.filter(s => s.type === 'fillPlaced')) for (const id of s.poolIds) assert(settled.includes(id), `${tag}: the film settled ${id} where the sheet put it`);
    const liftN = steps.filter(s => s.type === 'lift').reduce((n, s) => n + s.poolIds.length, 0);
    assert.equal(flights.filter(k => k.startsWith('lift:')).length, liftN, `${tag}: one lift flight per piece taken off`);
    const end = log.find(e => e.ev === 'end'); assert(end && end.ok && !end.skipped && !end.timedOut && !end.stalled, `${tag}: the film ended on its own: ` + JSON.stringify(end));
    assert.equal(walks(log).length, 1, `${tag}: the way home was walked once: ` + JSON.stringify(log.filter(e => e.ev === 'home')));
    assert(seen.fr.caps.filter(c => /^(Taking the pieces off|Filling the empty|Placed on|Remaking QR)/.test(c.text)).every(c => c.mode === 'nest'), `${tag}: the film played in the Nest tab: ` + JSON.stringify(seen.fr.caps.map(c => [c.mode, c.text.slice(0, 30)])));
    assert.equal(seen.fr.notes, 0, `${tag}: no note over the film: ` + JSON.stringify(seen.fr.noteText));
    assert.equal(seen.fr.dlg, 0, `${tag}: no dialog open while the film played (it would hide it)`); assert.equal(seen.fr.nameBar, 0, `${tag}: no name bar over the film`); assert.equal(seen.fr.topBarCovered, 0, `${tag}: the film stays under the top bar`);
    // the popup kept its promise
    const P = promised(seen.text), D = delivered(steps); oneOrderTwoSheets(steps, tag);
    for (const k of ['off', 'spots', 'waiting', 'newer']) if (P[k] != null) assert.equal(P[k], D[k], `${tag}: the popup said ${k} = ${P[k]}, the engine did ${D[k]}: ${seen.text.replace(/\s+/g, ' ')} || steps: ` + JSON.stringify(steps.map(s => [s.type, s.sheetId || s.toSheetId, s.rid, s.source, s.fromSheetId, (s.poolIds || []).length, s.spots, s.why])));
    // where the person ended
    assert.deepEqual(await page.evaluate(() => ({ mode: CN.S.mode, pile: Orders.view().pile })), { mode: 'orders', pile: 'hold' }, `${tag}: the person ends in Orders > On hold`);
    const rows = await rowsOf(rid), cards = await onHoldCards(rid), why = 'Taken off ' + x.names + ' by Paul';
    assert(rows.length >= 2 && rows.every(r => r.state === 'held' && r.hold === why && r.pieces === 0), `${tag}: every line of the order is held, with one reason, no piece left: ` + JSON.stringify(rows));
    assert.equal(cards.length, rows.length, `${tag}: a card per line in On hold: ` + JSON.stringify(cards));
    for (const c of cards) { assert(/HELD/i.test(c.pill || '') && c.text.includes(why), `${tag}: the card says "${why}": ` + c.text); assert(c.buttons.includes('Release hold') && c.buttons.includes('Cancel Order'), `${tag}: Release hold and Cancel Order: ` + c.buttons); assert(!c.buttons.includes('Hold'), `${tag}: no Hold on a held card`); }
    assert.deepEqual(await holdBtns(rid), [], `${tag}: no Hold button stays anywhere for a held order`);
    // the data
    const sh = await onSheets();
    for (const id of x.pieces) { for (const sid of x.sheets) assert(!sh[sid].all.includes(id), `${tag}: ${id} is off ${sid}`); const p = poolDoc(id); assert(p.state === 'abandoned' && p.heldBy === 'Paul' && !p.sheetId, `${tag}: ${id} is held by Paul and off its sheet: ` + JSON.stringify(p)); }
    for (const sid of x.sheets) {
      const fills = steps.filter(s => s.type === 'fillFrom' && s.toSheetId === sid);
      assert.deepEqual(fills.map(s => s.rid), x.fills[sid], `${tag}: ${sid} was refilled by ${JSON.stringify(x.fills[sid])}, oldest first: ` + JSON.stringify(fills.map(s => [s.rid, s.source, s.fromSheetId])));
      assert.deepEqual(fills.map(s => s.fromSheetId), x.from[sid], `${tag}: ${sid} was refilled from ${JSON.stringify(x.from[sid])}`);
      for (const s of fills) for (const id of s.poolIds) { const p = poolDoc(id); assert(p.sheetId === sid && p.movedFrom === s.fromSheetId && p.movedBy === 'Paul', `${tag}: a moved piece says where from and who moved it: ` + JSON.stringify(p)); assert(sh[sid].placed.includes(id), `${tag}: ${id} stands on ${sid}`); }
      assert.equal(sh[sid].placed.length, seen.before[sid].placed.length - x.pieces.filter(id => seen.before[sid].all.includes(id)).length + fills.reduce((n, s) => n + s.poolIds.length, 0), `${tag}: ${sid} holds as many pieces as before the order left it, plus the refill`);
    }
    const ev = tlOf(rid, 'held'); assert.equal(ev.length, x.sheets.length, `${tag}: one timeline step per sheet`);
    for (const e of ev) assert(e.by === 'Paul' && e.at > 1.79e12 && /Sheet/.test(e.text) && e.sheetId && x.sheets.includes(e.sheetId), `${tag}: a step with the sheet, the person and the time: ` + JSON.stringify([e.text, e.by, e.at, e.sheetId]));
    assert.equal(steps.filter(s => s.type === 'qr').length, x.sheets.length, `${tag}: QR labels remade on each sheet`);
    await noLoss(tag, {});
    await quiet(tag);
    return { steps, log };
  }
  // (the machine is shared while this runs: the median says how smooth the film was; the worst frames and the long tasks (the engine's own solver work, which runs on the page's one thread) are listed at the end, with the load of the machine, and not judged)
  // (when other work keeps the machine more than twice as busy as it has cores, a page gets a frame every 100 to 300 ms: how many frames a flight was seen in says how busy the machine was)
  const busyNow = () => { const os = require('os'); return os.loadavg()[0] > os.cpus().length * 2; };
  // (and then a frame time is only reported)
  const frameCheck = () => { const os = require('os'), busy = os.loadavg()[0] > os.cpus().length * 2; for (const [n, f] of frames) { const m = `${n}: smooth frames: median ${f.p50.toFixed(1)} ms, worst ${f.max.toFixed(0)} ms, ${f.over100} of ${f.n} over 100 ms`; if (busy) { if (f.p50 >= 40) notes.add('not judged, the machine was busy (load ' + os.loadavg()[0].toFixed(1) + ' on ' + os.cpus().length + ' cores): ' + m); } else assert(f.p50 < 40, m); } };
  /* ═════ B · Hold from an open Review card ═════ */
  await flow('B · Hold from an open Review card (order ' + H2 + ': a chain line and a piece on each sheet)', async () => {
    const k = kOf(H2, 1), card = `#rvList .reviewListRow[data-row="${k}"]`, btn = card + ' [data-hold-btn]';
    await page.evaluate(() => { CN.setMode('review'); Review.syncOrderItems(); Review.render(); });
    await page.waitForFunction(s => document.querySelector(s), btn, { timeout: 20000 });
    const ui = await page.evaluate(s => { const b = document.querySelector(s); b.closest('.reviewListRow').scrollIntoView({ block: 'center' }); const c = getComputedStyle(b); return { n: document.querySelectorAll('[data-hold-btn][data-rid="4170000110"]').length, text: b.textContent.trim(), bg: c.backgroundColor, shown: b.offsetParent !== null, where: b.closest('.reviewListRow').dataset.row, others: [...b.closest('.reviewListRow').querySelectorAll('.rowActions button')].map(x => x.textContent.trim()) }; }, btn);
    assert(ui.text === 'Hold' && ui.bg === 'rgb(162, 89, 28)' && ui.shown && ui.n === 1, 'the orange Hold on the open Review card, once: ' + JSON.stringify(ui));
    pass('the open Review card shows one orange Hold (beside ' + ui.others.filter(x => x !== 'Hold').join(', ') + ')');
    await shot('b1-review-card-hold-button');
    const seen = await holdFrom(H2, btn, 'b', { grabs: [['Taking the pieces off', 'b2-film-lift'], ['is on hold', 'b3-film-held']] });
    assert(/Put order 4170000110 on hold\?/.test(seen.text) && /Continue/.test(seen.text) && /Not now/.test(seen.text) && /QR label/.test(seen.text) && /Nothing is deleted/.test(seen.text) && !/\blines?\b/i.test(seen.text), 'the consent popup from a Review card: ' + seen.text);
    pass('the consent popup from the Review card: plain words, what comes off, what moves in, QR labels, nothing deleted');
    await verifyHold(H2, 'B', { sheets: ['e2e-gold-2', 'e2e-silver-1'], names: 'GF Sheet 2, SS Sheet 1', pieces: idsOf(H2, 2, 3), fills: { 'e2e-gold-2': [W(3)], 'e2e-silver-1': [V(3)] }, from: { 'e2e-gold-2': ['e2e-gold-3'], 'e2e-silver-1': ['e2e-silver-2'] } }, seen);
    pass('B: the film followed the engine, the order is in On hold with "Taken off GF Sheet 2, SS Sheet 1 by Paul", both sheets refilled (next oldest waiting order and next oldest of the newer SS sheet), one timeline step per sheet, nothing lost, nothing left on the screen');
    // the Review card is redrawn: it is gone, and so is its button
    await page.evaluate(() => { CN.setMode('review'); Review.syncOrderItems(); Review.render(); });
    await sleep(900);
    const stale = await page.evaluate(k => ({ card: !!document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`), btn: document.querySelectorAll('[data-hold-btn][data-rid="4170000110"]').length }), k);
    assert.deepEqual(stale, { card: false, btn: 0 }, 'B: Review is redrawn: no card and no Hold button for the held order');
    await shot('b4-review-after');
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('hold', ''); });
    pass('Review was redrawn: the held order has no card and no Hold button there');
  });
  await ensureHeld(H2).catch(e => fail('H2 could not be held to carry on', e));

  /* ═════ C · Hold from a completed Review card ═════ */
  await flow('C · Hold from a completed Review card (order ' + H3 + ')', async () => {
    const k = kOf(H3, 1), card = `#rvList .reviewListRow[data-row="${k}"]`, btn = card + ' [data-hold-btn]', idle = () => page.evaluate(() => Seal.whenIdle());
    await page.evaluate(() => { CN.setMode('review'); Review.syncOrderItems(); Review.render(); });
    await page.waitForFunction(s => document.querySelector(s + ' [data-cu-complete]'), card, { timeout: 20000 });
    await idle(); await page.click(card + ' [data-cu-complete]');
    await page.waitForFunction(k => B.maps.customDone[k], k, { timeout: 30000 });
    await page.waitForFunction(s => !document.querySelector(s), card, { timeout: 20000 });
    await idle(); await page.evaluate(() => document.querySelector('#reviewView .rvSeg [data-cseg="done"]').click());
    await page.waitForFunction(s => document.querySelector(s + ' [data-hold-btn]'), card, { timeout: 20000 });
    await idle(); await sleep(700);
    // (the card's flight to Completed leaves its copy in the air for a moment; it must be gone soon, and never keep a second Hold on the screen)
    const t1 = Date.now(); let copies = null;
    await page.waitForFunction(() => document.querySelectorAll('[data-hold-btn][data-rid="4170000120"]').length === 1, null, { timeout: 12000 }).catch(async () => { copies = await page.evaluate(() => [...document.querySelectorAll('[data-hold-btn][data-rid="4170000120"]')].map(b => { const c = []; for (let n = b; n && n !== document.body; n = n.parentElement) c.push(n.tagName.toLowerCase() + (n.id ? '#' + n.id : '') + (typeof n.className === 'string' && n.className ? '.' + n.className.split(' ').slice(0, 2).join('.') : '')); return c.slice(0, 6).join(' < '); })); });
    assert.equal(copies, null, 'C: only one Hold stays on the completed card after its flight: ' + JSON.stringify(copies));
    console.log('  (the copy of the card left the air after ' + (Date.now() - t1 + 700) + ' ms)');
    const ui = await page.evaluate(s => { const n = document.querySelector(s); n.scrollIntoView({ block: 'center' }); const holds = [...document.querySelectorAll('[data-hold-btn][data-rid="4170000120"]')]; return { q: (n.querySelector('.queueLabel') || {}).textContent.trim(), btns: [...n.querySelectorAll('.rowActions button')].map(x => x.textContent.trim()), holds: holds.length, shown: holds.every(b => b.offsetParent !== null) }; }, card);
    assert(/completed/i.test(ui.q) && ui.holds === 1 && ui.shown && ui.btns.includes('Reopen') && ui.btns.includes('Hold'), 'the completed card has Hold beside Reopen: ' + JSON.stringify(ui));
    pass('the completed Review card shows one orange Hold (beside ' + ui.btns.filter(x => x !== 'Hold').join(', ') + ')');
    await shot('c1-review-completed-hold-button');
    const seen = await holdFrom(H3, btn, 'c', { grabs: [['Taking the pieces off', 'c2-film-lift']] });
    await verifyHold(H3, 'C', { sheets: ['e2e-gold-2', 'e2e-silver-1'], names: 'GF Sheet 2, SS Sheet 1', pieces: idsOf(H3, 2, 3), fills: { 'e2e-gold-2': [W(4)], 'e2e-silver-1': [V(4)] }, from: { 'e2e-gold-2': ['e2e-gold-3'], 'e2e-silver-1': ['e2e-silver-2'] } }, seen);
    pass('C: same Hold from a completed card: film, On hold, both sheets refilled, timeline, nothing lost');
    await page.evaluate(() => { CN.setMode('review'); Review.syncOrderItems(); Review.render(); });
    await sleep(900);
    const stale = await page.evaluate(k => ({ card: !!document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`), btn: document.querySelectorAll('[data-hold-btn][data-rid="4170000120"]').length }), k);
    assert.equal(stale.btn, 0, 'C: Review (Completed) is redrawn: no Hold button for the held order' + (stale.card ? ' (the completed card itself stays, as a record)' : ''));
    await page.evaluate(() => { document.querySelector('#reviewView .rvSeg [data-cseg="open"]') && document.querySelector('#reviewView .rvSeg [data-cseg="open"]').click(); });
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('hold', ''); });
    pass('the completed list was redrawn: the held order has no card and no Hold button there');
  });
  await ensureHeld(H3).catch(e => fail('H3 could not be held to carry on', e));

  /* ═════ D · Release hold ═════ */
  await flow('D · Release hold on the On hold card of ' + H1 + ', with 3 Etsy-style orders already waiting', async () => {
    await ensureHeld(H1);
    // three orders have come in from Etsy, each OLDER than H1 (so their age alone ranks them first), one gold and one silver piece each,
    // taken in through the real intake (Pool.poolAdd): they wait on the next open sheets, and nothing places them yet (the test nest's cap is 0)
    const intake = await page.evaluate(async ({ INCOMING, BASE_TS, SHIP }) => {
      window.__cap = {}; for (const sh of CN.allSheets()) if (sh.sheetId) window.__cap[sh.sheetId] = 0;
      const TID = tx => String(5000000000 + tx), made = [];
      INCOMING.forEach((rid, i) => {
        const lines = [[900 + i * 2, 'gold', 'GFINC_' + i], [901 + i * 2, 'silver', 'SSINC_' + i]];
        for (const [, , sku] of lines) window.__regSku(sku);
        const order = { receiptId: rid, orderNumber: rid, createTs: BASE_TS - (20000 - i * 500), updateTs: BASE_TS, shipBy: SHIP, buyer: { name: 'Etsy Buyer ' + (i + 1) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
          lines: lines.map(([tx, m, sku]) => ({ transactionId: TID(tx), listingId: '19009' + String(tx).padStart(5, '0'), sku, title: sku.replace(/_/g, ' ') + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: m === 'gold' ? '14k Gold Filled' : 'Sterling Silver' }], metalKey: m, metalLabel: m === 'gold' ? 'GF 14/20' : 'SS', personalization: [], buyerMessage: '' })) };
        for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line), row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); made.push(row); }
        if (Array.isArray(B.run.orders)) B.run.orders.push(rid);
      });
      Orders.interpretAll();
      for (const row of made) await Pool.poolAdd(row, B.run);
      return made.map(r => ({ key: r.key, state: r.state, reason: r.reason, poolIds: r.poolIds, problems: (r.problems || []).map(p => p.kind) }));
    }, { INCOMING, BASE_TS, SHIP });
    for (const r of intake) assert(r.state === 'pooled' && r.poolIds.length === 1, 'the incoming line was taken in by the real intake: ' + JSON.stringify(r));
    const incIds = intake.flatMap(r => r.poolIds);
    const where = ids => page.evaluate(ids => Object.fromEntries(ids.map(id => { const sh = CN.allSheets().find(p => p.charms.some(c => c.poolId === id)), c = sh && sh.charms.find(x => x.poolId === id); return [id, sh ? { sheetId: sh.sheetId, placed: sh.placements.some(p => p.id === c.id), rank: CharmNestOrders.rankDate(c) } : null]; })), ids);
    const inc0 = await where(incIds);
    for (const id of incIds) assert(inc0[id] && !inc0[id].placed, `${id} waits on a sheet, not placed yet: ` + JSON.stringify(inc0[id]));
    // where Release hold will put the order: the next sheet of each metal with room (read only), and the incoming orders are waiting on those same sheets
    const plan = await page.evaluate(rid => OrderHold.releasePlan(rid).then(p => ({ canRelease: p.canRelease, held: p.held, front: p.front, targets: p.targets.map(t => ({ metal: t.metal, sheetId: t.sheetId, label: t.label, newSheet: t.newSheet })), effects: p.effects })), H1);
    assert(plan.canRelease && plan.held && plan.front, 'the plan: the held order can be released to the front: ' + JSON.stringify(plan));
    const gold = plan.targets.find(t => t.metal === 'gold'), silver = plan.targets.find(t => t.metal === 'silver');
    assert(gold && silver && gold.sheetId && silver.sheetId && !gold.newSheet && !silver.newSheet, 'the order goes to the next open sheet of each metal (no new sheet): ' + JSON.stringify(plan.targets));
    assert.deepEqual([gold.sheetId, silver.sheetId], ['e2e-gold-3', 'e2e-silver-2'], 'those are the newer, still filling sheets (the full ones are closed): ' + JSON.stringify(plan.targets));
    const goldInc = incIds.filter((id, i) => i % 2 === 0), silverInc = incIds.filter((id, i) => i % 2 === 1);
    for (const id of goldInc) assert.equal(inc0[id].sheetId, gold.sheetId, `incoming gold ${id} waits on the same sheet the order will go on`);
    for (const id of silverInc) assert.equal(inc0[id].sheetId, silver.sheetId, `incoming silver ${id} waits on the same sheet the order will go on`);
    // the order is older than none of them in age: age alone would put all three first
    const h1Rank = await page.evaluate(rid => Math.min(...Orders.rows().filter(r => String(r.order.receiptId) === rid).map(r => +r.order.createTs)), H1);
    assert(Math.max(...incIds.map(id => inc0[id].rank)) < h1Rank, 'by age alone the incoming orders rank before the held order (older): ' + JSON.stringify([incIds.map(id => inc0[id].rank), h1Rank]));
    await page.evaluate(([g, s]) => { window.__cap[g] = 2; window.__cap[s] = 2; }, [gold.sheetId, silver.sheetId]);   // (a nest here places two pieces: whoever ranks first gets them)
    // On hold, the order's cards, and its Release hold
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('hold', ''); });
    const rel = `#ordItems [data-rid="${H1}"] .relHold:not([data-gate])`;
    await page.waitForFunction(s => document.querySelectorAll(s).length === 2, rel, { timeout: 20000 });
    await page.evaluate(s => document.querySelector(s).scrollIntoView({ block: 'center' }), rel);
    await shot('d1-on-hold-card-release-button', 500);
    await page.evaluate(() => { OrderHoldFx.clearEvents(); window.__relTxt = []; new MutationObserver(() => { for (const b of document.querySelectorAll('#ordItems .relHold')) window.__relTxt.push(b.textContent.trim()); }).observe(document.getElementById('ordItems'), { subtree: true, childList: true, characterData: true, attributes: true }); });
    await outboxQuiet();
    const tPress = await page.evaluate(() => Date.now()), w0 = mutating();
    await sampler();
    await page.click(rel);
    await page.waitForFunction(() => document.getElementById('hfxLayer') && CN.S.mode === 'nest', null, { timeout: 30000 });
    for (const [re, name] of [['First in the queue', 'd2-release-film-1-first-in-line'], ['Finding a spot', 'd2-release-film-2-spot'], ['Placing the pieces', 'd2-release-film-3-placing'], ['Placed on', 'd2-release-film-4-placed']]) { try { await page.waitForFunction(re => { const l = document.querySelector('#hfxLayer .hfxLive'); return l && l.textContent.includes(re); }, re, { timeout: 60000 }); await shot(name, 120); } catch (_) { console.log('  (no frame for ' + re + ')'); } }
    await waitHome(H1, 'D');
    const fr = await sampled(); frames.push(['D', fr]);
    const note = await page.evaluate(() => { const n = document.querySelector('.mNote'); return n ? { text: n.querySelector('.mNoteT').textContent.trim(), btns: [...n.querySelectorAll('.mNoteBtn')].map(b => b.textContent.trim()) } : null; });
    await shot('d3-back-in-on-hold-note', 700);
    await sleep(500);
    // ── what the engine did ──
    const steps = await stepsOf(H1), T = steps.map(s => s.type);
    assert.equal(T[0], 'start'); assert.deepEqual(T.slice(-2), ['released', 'done'], 'the release ended: ' + T.join(' '));
    for (const t of ['queued', 'target', 'flight', 'placed', 'qr']) assert(T.includes(t), `the release told ${t}: ` + T.join(' '));
    assert(steps.find(s => s.type === 'queued').front === true, 'the order carries its place at the front of the queue');
    const targets = steps.filter(s => s.type === 'target'), placedSteps = steps.filter(s => s.type === 'placed');
    assert.deepEqual(sorted(targets.map(s => s.sheetId)), sorted([gold.sheetId, silver.sheetId]), 'the targets are the planned sheets: ' + JSON.stringify(targets.map(s => [s.sheetId, s.label, s.newSheet])));
    assert.deepEqual(sorted(placedSteps.map(s => s.sheetId)), sorted([gold.sheetId, silver.sheetId]), 'it landed on the planned sheets at once');
    assert.deepEqual(sorted(placedSteps.flatMap(s => s.poolIds)), sorted(idsOf(H1, 1, 2)), 'all four pieces were placed');
    assert(placedSteps.every(s => s.placements.every(p => Number.isFinite(p.cxPt) && Number.isFinite(p.cyPt))), 'each placed piece says where it stands');
    const res = steps.find(s => s.type === 'released'); assert(res.placed === true && res.sheets.length === 2, 'released and placed: ' + JSON.stringify(res));
    // ── it went ahead of the three incoming orders ──
    const nests = await page.evaluate(t => window.__placed.filter(x => x.at >= t), tPress);
    for (const t of [gold, silver]) {
      const first = nests.find(x => x.sheetId === t.sheetId); assert(first, `a nest ran on ${t.label}`);
      const mine = idsOf(H1, ...(t.metal === 'gold' ? [1] : [2]));
      assert.deepEqual(sorted(first.poolIds), sorted(mine), `${t.label}: the first nest after the press placed the released order's pieces and nobody else's (the three older incoming orders waited): ` + JSON.stringify(first.poolIds));
    }
    const ranks = await page.evaluate(([mineIds, theirs]) => { const r = id => { const sh = CN.allSheets().find(p => p.charms.some(c => c.poolId === id)); const c = sh.charms.find(x => x.poolId === id); return { rank: CharmNestOrders.rankDate(c), front: c.frontAt || 0 }; }; return { mine: mineIds.map(r), theirs: theirs.map(r) }; }, [idsOf(H1, 1, 2), incIds]);
    assert(ranks.mine.every(m => m.front > 0) && Math.max(...ranks.mine.map(m => m.rank)) < Math.min(...ranks.theirs.map(m => m.rank)), 'the released pieces carry their place at the front and rank before every incoming piece: ' + JSON.stringify(ranks));
    pass('Release hold put the order at the front: the three older incoming orders waited; its four pieces were the first the next nest placed on ' + gold.label + ' and ' + silver.label);
    // ── the data ──
    const sh = await onSheets();
    for (const t of [gold, silver]) for (const id of idsOf(H1, ...(t.metal === 'gold' ? [1] : [2]))) { assert(sh[t.sheetId].placed.includes(id), `${id} stands on ${t.label}`); const p = poolDoc(id); assert(p.state !== 'abandoned' && p.frontAt > 0, `its pool record is no longer abandoned and carries its place at the front: ` + JSON.stringify(p)); if (p.heldBy) notes.add('a released piece\'s pool record still carries heldBy/heldReason/heldAt from the hold (nothing reads them; the saved sheet and the order lines are right)'); }
    const rows = await rowsOf(H1); assert(rows.length === 2 && rows.every(r => r.state === 'pooled' && !r.hold && r.frontAt > 0 && !r.releasing && r.pieces === 2), 'the order is back in line, in front: ' + JSON.stringify(rows));
    const rel1 = tlOf(H1, 'released'); assert.equal(rel1.length, 1, 'one released step on the timeline');
    assert(rel1[0].by === 'Paul' && /^Released from hold by Paul · placed on /.test(rel1[0].text) && rel1[0].sheetId && rel1[0].at > tPress - 5000, 'the step names who, where and when: ' + JSON.stringify([rel1[0].text, rel1[0].by, rel1[0].sheetId, rel1[0].at]));
    assert.equal(tlOf(H1, 'held').length, 2, 'the earlier held steps stay on the timeline');
    assert.equal(mutating() - w0 > 0, true, 'the release wrote its records');
    await noLoss('D', {});
    pass('the data: its four pieces stand on ' + gold.label + ' and ' + silver.label + ' (the saved sheets list them), the lines are back in line with their place at the front, one "Released from hold by Paul" step names the sheet and the time, the held steps are still there');
    // ── the film ──
    const log = await fxLog(), caps = captionsOf(log);
    const order = inOrder(caps, [/^Releasing order 4170000100/, /^First in the queue, ahead of new orders/, /^Finding a spot on/, /^Placing the pieces on/, /^Placed on/, /^Order 4170000100 is released/]);
    assert.equal(order, true, 'the release captions followed the steps in order: ' + order);
    const flights = log.filter(e => e.ev === 'flight').map(e => e.key);
    for (const s of steps.filter(s => s.type === 'flight')) for (const id of s.poolIds) assert(flights.includes(`place:${s.toSheetId}:${id}`), `the film flew ${id} to ${s.toSheetId} (flights: ${flights.join(', ')})`);
    const settled = log.filter(e => e.ev === 'settle').map(e => e.pool);
    for (const s of placedSteps) for (const id of s.poolIds) assert(settled.includes(id), `the film settled ${id} where the sheet put it (settled: ${settled.join(',')}; log: ${JSON.stringify(log.filter(e => ['focus', 'flight', 'settle', 'beat', 'skip', 'end'].includes(e.ev)).map(e => [e.ev, e.type || e.key || e.sheetId || e.pool, e.found]))})`);
    const end = log.find(e => e.ev === 'end'); assert(end && end.ok && !end.skipped && !end.timedOut && !end.stalled, 'the film ended on its own: ' + JSON.stringify(end));
    assert.equal(walks(log).length, 1, 'the way home was walked once: ' + JSON.stringify(log.filter(e => e.ev === 'home')));
    assert(fr.caps.filter(c => /^(Finding a spot|Placing the pieces|Placed on|Remaking QR|QR label)/.test(c.text)).every(c => c.mode === 'nest'), 'the film played in the Nest tab: ' + JSON.stringify(fr.caps.map(c => [c.mode, c.text.slice(0, 30)])));
    assert.equal(fr.notes, 0, 'no note over the film (the list the hold leaves is not on screen): ' + JSON.stringify(fr.noteText));
    assert.equal(fr.dlg, 0, 'no dialog open while the film played'); assert.equal(fr.nameBar, 0, 'no name bar over the film'); assert.equal(fr.topBarCovered, 0, 'the film stays under the top bar');
    const spin = await page.evaluate(() => window.__relTxt.some(t => /Checking sheets/.test(t)));
    assert(spin, 'the button showed a small labelled "Checking sheets…" while the plan was read');
    pass('the film followed the release steps in order (first in line, finding a spot, placing, placed, released), one flight and one settle per piece, ended by itself in ' + end.ms + ' ms, one walk home');
    // ── where the person ended ──
    assert.deepEqual(await page.evaluate(() => ({ mode: CN.S.mode, pile: Orders.view().pile })), { mode: 'orders', pile: 'hold' }, 'the person ends in Orders > On hold');
    assert.equal((await onHoldCards(H1)).length, 0, 'the order has left On hold');
    assert(note && /Order 4170000100 is back in line/.test(note.text) && note.text.includes('placed on ' + gold.label) && note.text.includes(silver.label) && note.btns.includes('Show'), 'a note says where it went: ' + JSON.stringify(note));
    pass('the person is back in Orders > On hold, the order has left it, and a note says "' + note.text + '"');
    // ── the QR label step ──
    const qrs = steps.filter(s => s.type === 'qr');
    assert.equal(qrs.length, 2, 'one QR step per sheet');
    for (const q of qrs) { assert.equal(typeof q.made, 'boolean', 'the QR step says whether the label was made: ' + JSON.stringify(q)); if (!q.made) assert(q.why, 'a label not made says why: ' + JSON.stringify(q)); }
    const qrCaps = caps.filter(c => /QR label/i.test(c));
    for (const q of qrs.filter(q => !q.made)) assert(!qrCaps.some(c => /remak|remad/i.test(c)), `the film must not say a QR label was remade when the engine says it was not (${q.label}: ${q.why}); it said: ` + qrCaps.join(' | '));
    pass('the QR step: ' + qrs.map(q => `${q.label} ${q.made ? 'label made' : 'not made yet (' + q.why + ')'}`).join('; ') + '; the film says the same');
    // ── the order can be held again: its Hold button is back ──
    assert.equal(await page.evaluate(r => HoldUI.shown(r), H1), true, 'a released order can be held again');
    await openWin(keyOfLine(H1, 1));
    const again = await holdBtns(H1); assert(again.length === 2 && again.every(b => b.visible && b.text === 'Hold'), 'the order window shows Hold on each piece row again: ' + JSON.stringify(again));
    await page.evaluate(() => OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 });
    await quiet('D');
    pass('the released order is holdable again (Hold on each piece row) and nothing is left on the screen');
  });

  /* ═════ E · Cancel Order on On hold cards; the seals through Hold, Release hold and Cancel ═════ */
  const chipN = pile => page.evaluate(p => { const b = document.querySelector(`#ordChips [data-pile="${p}"] b`); return b ? b.textContent : null; }, pile);
  const cardsIn = rid => page.evaluate(r => [...document.querySelectorAll('#ordItems [data-rid]')].filter(n => n.dataset.rid === r).length, rid);
  const calls = (op, rid) => st.calls.filter(c => c.name === 'charmNestLibrary' && c.op === op && (!rid || String(c.body.orderId) === String(rid)));
  /** the seals drawn on the cards of the order in Orders (On hold, or the list): only the cards that have any */
  const sealsOn = rid => page.evaluate(r => [...document.querySelectorAll('#ordItems [data-rid]')].filter(n => n.dataset.rid === r).map(n => ({ key: n.dataset.key, seals: [...n.querySelectorAll('.rowActions .seal')].map(x => [...x.classList].filter(c => /^seal-/.test(c)).join('') + ' | ' + (x.getAttribute('aria-label') || '')), shown: [...n.querySelectorAll('.rowActions .seal')].every(x => x.getClientRects().length > 0 && getComputedStyle(x).visibility !== 'hidden') })).filter(c => c.seals.length), rid);
  /** the order's card under Cancelled: its words and its seals */
  const cxCard = rid => page.evaluate(r => { const n = document.querySelector(`#ordBody .cxRow[data-rid="${r}"]`); return n ? { text: n.innerText.replace(/\s+/g, ' ').trim(), seals: [...n.querySelectorAll('.cxSeals .seal')].map(x => x.getAttribute('aria-label') || ''), shown: [...n.querySelectorAll('.cxSeals .seal')].every(x => x.getClientRects().length > 0 && getComputedStyle(x).visibility !== 'hidden') } : null; }, rid);
  const LIBRX = /\/\.netlify\/functions\/charmNestLibrary/;
  const routeLib = async fn => { const h = r => { let b = {}; try { b = r.request().postDataJSON() || {}; } catch (_) {} return fn(r, b); }; await page.route(LIBRX, h); return () => page.unroute(LIBRX, h); };
  /** the cancel flight, sampled every frame: the copies in the air, the ring, the chip's number, the notes, which properties animate, the frame gaps */
  const cxSampler = () => page.evaluate(() => {
    const S = window.__cx = { t0: performance.now(), s: [], props: new Set(), anims: 0, run: true, gaps: [], last: performance.now() };
    const OK = new Set(['transform', 'opacity']);
    const tick = now => {
      S.gaps.push(now - S.last); S.last = now;
      const chip = document.querySelector('#ordChips [data-pile="cancelled"]');
      const ghosts = [...document.querySelectorAll('#motionLayer .mGhost')].map(g => { const r = g.getBoundingClientRect(); return { key: (g.querySelector('.mCopy') || {}).dataset?.key || '', x: r.left + r.width / 2, y: r.top + r.height / 2, w: r.width }; });
      const cr = chip ? chip.getBoundingClientRect() : null;
      S.s.push({ t: Math.round(now - S.t0), ghosts, beacons: document.querySelectorAll('.cnBeacon').length, tick: document.querySelectorAll('.cnTick').length, cx: chip && chip.querySelector('b') ? chip.querySelector('b').textContent : null, chip: cr ? { x: cr.left + cr.width / 2, y: cr.top + cr.height / 2 } : null });
      for (const a of document.getAnimations()) {
        const t = a.effect && a.effect.target; if (!t || !t.closest || !t.closest('#motionLayer, .cnBeacon, .cnTick, .mNote')) continue;
        S.anims++; for (const k of a.effect.getKeyframes()) for (const p of Object.keys(k)) if (!['offset', 'easing', 'composite', 'computedOffset'].includes(p)) S.props.add(OK.has(p) ? p : p + '@' + (t.className || t.tagName));
      }
      if (S.run) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  });
  const cxStop = () => page.evaluate(() => { const S = window.__cx; S.run = false; const g = S.gaps.slice(2).sort((a, b) => a - b); return { s: S.s, props: [...S.props], anims: S.anims, n: g.length, p50: g[Math.floor(g.length * .5)] || 0, p95: g[Math.floor(g.length * .95)] || 0, max: g[g.length - 1] || 0, over100: g.filter(x => x > 100).length }; });
  const settleCx = () => page.waitForFunction(() => !document.querySelector('.cnBeacon, .cnTick') && !(document.getElementById('motionLayer') || { children: [] }).children.length, null, { timeout: 20000 });

  /** presses Cancel Order on the order's first card and follows the flight to the note */
  async function cancelFrom(rid, tag, shots) {
    const sel = `#ordItems [data-rid="${rid}"]`, n0 = await cardsIn(rid), before = { hold: await chipN('hold'), cx: await chipN('cancelled') };
    assert(n0 >= 1, `${tag}: the order has cards in On hold`);
    const unroute = await routeLib(async (r, b) => { if (b.op === 'cancelPut') await sleep(700); return r.fallback(); });   // (the save takes a moment, as a real one does: the pressed state is seen)
    await cxSampler();
    await page.click(`${sel} .cnCancelBtn`);
    const waiting = await page.evaluate(s => { const n = document.querySelector(s), b = n && n.querySelector('.cnCancelBtn'); return b ? { disabled: b.disabled, text: b.textContent.trim(), spin: !!b.querySelector('.spin'), rel: (n.querySelector('.relHold') || {}).disabled, lifted: n.classList.contains('cnBusy'), name: !!document.querySelector('.cnNameBar') } : null; }, sel);
    assert(waiting && waiting.disabled && waiting.spin && /Cancelling/.test(waiting.text) && waiting.rel && waiting.lifted && !waiting.name, `${tag}: pressed: a small labelled spinner, Release hold waits, the card lifts, no name asked: ` + JSON.stringify(waiting));
    if (shots) await shot(shots + '-pressed', 100);
    await page.waitForSelector('#motionLayer .mGhost', { timeout: 15000 });
    if (shots) { await shot(shots + '-flight-1', 60); await shot(shots + '-flight-2', 380); }
    await page.waitForFunction(r => !document.querySelector(`#ordItems [data-rid="${r}"]`), rid, { timeout: 20000 });
    await page.waitForFunction(r => [...document.querySelectorAll('.mNote')].some(n => n.textContent.includes('Order ' + r + ' cancelled')), rid, { timeout: 10000 });
    await page.waitForFunction(n => { const b = document.querySelector('#ordChips [data-pile="cancelled"] b'); return b && b.textContent === n && !b.classList.contains('cnTick'); }, String(+before.cx + 1), { timeout: 15000 });   // (the number has finished rolling)
    await sleep(300);
    const s = await cxStop(); await unroute();
    return { s, before, n0 };
  }
  /** what a Cancel did, in the data and on the screen; nth: how many cancels of this order the server has seen */
  async function verifyCancel(rid, tag, c, who, nth) {
    const { s, before, n0 } = c;
    const puts = calls('cancelPut', rid); assert.equal(puts.length, nth, `${tag}: cancelPut ${nth} time(s) in all for this order: ${puts.length}`); assert.equal(puts[puts.length - 1].body.by, who, `${tag}: by ${who}`);
    const rec = st.doc('Charm_Nest_Cancelled', rid); assert(rec && rec.by === who && String(rec.orderId) === rid, `${tag}: the record is kept under Charm_Nest_Cancelled: ` + JSON.stringify(rec).slice(0, 300));
    assert.equal((await rowsOf(rid)).filter(r => r.state !== 'gone').length, 0, `${tag}: the order is off every list`);
    assert.equal(await page.evaluate(r => Cancelled.has(r), rid), true, `${tag}: Cancelled has it`);
    const mine = x => x.key.includes(rid), air = s.s.filter(x => x.ghosts.some(mine));
    assert(air.length >= (busyNow() ? 2 : 8) && air[air.length - 1].t - air[0].t >= 500, `${tag}: the cards were seen flying for a while: ${air.length} frames over ${air.length ? air[air.length - 1].t - air[0].t : 0} ms`);
    const g0 = air[0].ghosts.find(mine), gN = air[air.length - 1].ghosts.find(mine), chip = air[air.length - 1].chip;
    const d0 = Math.hypot(g0.x - chip.x, g0.y - chip.y), dN = Math.hypot(gN.x - chip.x, gN.y - chip.y);
    assert(dN < d0 * (busyNow() ? 0.9 : 0.35) && gN.w < g0.w * (busyNow() ? 0.95 : 0.6), `${tag}: a card gets to the Cancelled chip and shrinks into it: ${Math.round(d0)} px away at first, ${Math.round(dN)} at the end, width ${Math.round(g0.w)} to ${Math.round(gN.w)}`);
    const ring = s.s.findIndex(x => x.beacons > 0), landed = s.s.findIndex(x => x.tick > 0), flightStart = s.s.findIndex(x => x.ghosts.some(mine));
    assert(ring >= 0 && (busyNow() || ring <= flightStart + 3) && landed > ring, `${tag}: the chip showed a ring as the card set off, then the count rolled (ring ${ring}, flight ${flightStart}, roll ${landed})`);
    const seen = [...new Set(s.s.map(x => x.cx))], was = +before.cx, now = was + 1;
    assert(seen[0] === String(was) && seen[seen.length - 1] === String(now) && seen.some(v => v && v.length > 1 && v.includes(String(was)) && v.includes(String(now))), `${tag}: the Cancelled count read ${was} while the cards flew, rolled (both digits) and ends at ${now}: ` + JSON.stringify(seen));
    assert(s.s.slice(0, landed).filter(x => x.ghosts.some(mine)).every(x => x.cx === String(was)), `${tag}: the number waits for the card`);
    assert.equal(await chipN('cancelled'), String(now), `${tag}: the chip says ${now}`); assert.equal(await chipN('hold'), String(+before.hold - 1), `${tag}: On hold counts one fewer (one order, ${n0} cards)`);
    assert(s.anims > 5 && s.props.every(p => ['transform', 'opacity'].includes(p)), `${tag}: only transform and opacity animate: ${JSON.stringify(s.props)}`);
    const note = await page.evaluate(r => { const n = [...document.querySelectorAll('.mNote')].find(x => x.textContent.includes('Order ' + r + ' cancelled')); return n ? { text: n.querySelector('.mNoteT').textContent, btns: [...n.querySelectorAll('.mNoteBtn')].map(b => b.textContent) } : null; }, rid);
    assert(note && new RegExp(`^Order ${rid} cancelled`).test(note.text) && !/\blines?\b/i.test(note.text) && JSON.stringify(note.btns) === '["Undo","Show"]', `${tag}: the note says so, with Undo and Show: ` + JSON.stringify(note));
    const ev = st.list(TL).filter(x => x._id.startsWith(rid + '~')).map(x => x.type), cx = tlOf(rid, 'cancelled');
    assert(ev.filter(t => t === 'held').length === 2 && cx.length === nth && cx[cx.length - 1].by === who && cx[cx.length - 1].at > 1.79e12, `${tag}: the timeline keeps the two held steps and adds the cancel with who and when: ` + JSON.stringify(st.list(TL).filter(x => x._id.startsWith(rid + '~')).map(x => [x.type, x.by, x.at])));
    frames.push([tag, s]);
    return note;
  }
  /** Undo on the note: the existing restore; the order is back in On hold, still held and off every sheet */
  async function undoFrom(rid, tag, held) {
    const before = { hold: await chipN('hold'), cx: await chipN('cancelled') }, n = calls('cancelRestore', rid).length;
    await page.click('.mNote .mNoteBtn:has-text("Undo")');
    await page.waitForFunction(r => document.querySelectorAll(`#ordItems [data-rid="${r}"]`).length >= 1, rid, { timeout: 25000 });
    await page.waitForFunction(r => [...document.querySelectorAll('.mNote')].some(x => x.textContent.includes('Order ' + r + ' is back on hold')), rid, { timeout: 15000 });
    // (the cards fly back out of the Cancelled chip: they are read once they have landed and show their words)
    await page.waitForFunction(r => { const c = [...document.querySelectorAll('#ordItems [data-rid]')].filter(n => n.dataset.rid === r); return c.length >= 2 && c.every(n => n.innerText.trim().length > 20) && !document.querySelector('#motionLayer .mGhost'); }, rid, { timeout: 15000 });
    await sleep(300);
    assert.equal(calls('cancelRestore', rid).length, n + 1, `${tag}: cancelRestore once`);
    assert.equal(st.doc('Charm_Nest_Cancelled', rid), undefined, `${tag}: the record is out of Cancelled (its history keeps it)`);
    const rows = await rowsOf(rid); assert(rows.length >= 2 && rows.every(r => r.state === 'held' && r.hold === held && r.pieces === 0), `${tag}: every line is back, held, with the same reason: ` + JSON.stringify(rows));
    assert.equal(await chipN('cancelled'), String(+before.cx - 1), `${tag}: Cancelled counts one fewer`); assert.equal(await chipN('hold'), String(+before.hold + 1), `${tag}: On hold counts it again`);
    const cards = await onHoldCards(rid); assert(cards.length === rows.length && cards.every(c => /HELD/i.test(c.pill || '') && c.text.includes(held) && c.buttons.includes('Release hold') && c.buttons.includes('Cancel Order')), `${tag}: a card per line in On hold, with Release hold and Cancel Order: ` + JSON.stringify(cards));
    assert.deepEqual(await holdBtns(rid), [], `${tag}: no Hold button for a held order`);
    const ev = st.list(TL).filter(x => x._id.startsWith(rid + '~')).map(x => x.type); for (const t of ['held', 'cancelled', 'cancelRestored']) assert(ev.includes(t), `${tag}: the timeline keeps ${t}: ` + ev);
    await noLoss(tag, {});
  }

  await flow('E · Cancel Order on On hold cards (' + H2 + ': cancel, Undo, cancel again; ' + H3 + ': the seal through Cancel, Undo and Release hold)', async () => {
    await ensureHeld(H2); await ensureHeld(H3);
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('hold', ''); });
    await clearNotes();
    await page.waitForFunction(r => document.querySelectorAll(`#ordItems [data-rid="${r}"] .cnCancelBtn`).length >= 3, H2, { timeout: 20000 });
    await page.evaluate(r => document.querySelector(`#ordItems [data-rid="${r}"] .cnCancelBtn`).scrollIntoView({ block: 'center' }), H2);
    const cards0 = await onHoldCards(H2);
    assert(cards0.length === 3 && cards0.every(c => c.buttons.includes('Release hold') && c.buttons.includes('Cancel Order') && !c.buttons.includes('Hold')), 'the three cards of the order in On hold each have Release hold and Cancel Order, and no Hold: ' + JSON.stringify(cards0));
    const onHold0 = +await chipN('hold'); assert(onHold0 >= 2, 'at least two orders are on hold (' + H2 + ' and ' + H3 + '): ' + onHold0); assert.equal(await chipN('cancelled'), '0');
    await shot('e1-on-hold-card-cancel-button', 400);
    const held = 'Taken off GF Sheet 2, SS Sheet 1 by Paul';
    pass('On hold shows each card of ' + H2 + ' with Release hold and Cancel Order (and no Hold)');

    /* cancel, then Undo straight away (the note keeps Undo for 12 s) */
    const c1 = await cancelFrom(H2, 'E-cancel', 'e2-cancel');
    const note = await verifyCancel(H2, 'E-cancel', c1, 'Paul', 1);
    await shot('e3-cancel-note', 50);
    assert.equal((await onHoldCards(H3)).length, 3, 'the other held order is untouched'); assert.equal(calls('cancelPut').length, 1, 'no other order was cancelled');
    pass('Cancel Order: the cards flew to the Cancelled chip over ' + c1.s.s.filter(x => x.ghosts.length).length + ' frames, the count rolled from 0 to 1, the note says "' + note.text + '" with Undo and Show, one cancelPut by Paul, the record is kept, the timeline has the cancel, only transform and opacity animated');
    await undoFrom(H2, 'E-undo', held);
    await shot('e5-undo-back-on-hold', 300);
    pass('Undo: one cancelRestore, the record left Cancelled, the order is back in On hold with the same reason, its three cards, no Hold button, the counts back, the timeline keeps held, cancelled and cancelRestored');
    await settleCx(); await clearNotes();

    /* the seal: Hold kept it (C completed this order's chain line by hand), and Cancel, Undo and Release must keep it */
    const kChain = kOf(H3, 1), s0 = await sealsOn(H3);
    assert(s0.length === 1 && s0[0].key === kChain && s0[0].shown && s0[0].seals.length >= 1 && /^seal-button/.test(s0[0].seals[0]) && /Complete Order/.test(s0[0].seals[0]), 'the On hold card of ' + H3 + ' keeps the seal from Complete Order, visible: ' + JSON.stringify(s0));
    await page.evaluate(r => document.querySelector(`#ordItems [data-rid="${r}"] .seal`).scrollIntoView({ block: 'center' }), H3);
    await shot('e6-on-hold-card-seal', 500);
    pass('the On hold card of ' + H3 + ' (held after it was completed by hand) still shows its seal: ' + s0[0].seals[0].split(' | ')[1].slice(0, 80));
    const c3 = await cancelFrom(H3, 'E-seal-cancel', null);
    await verifyCancel(H3, 'E-seal-cancel', c3, 'Paul', 1);
    await page.click('#ordChips [data-pile="cancelled"]'); await page.waitForSelector(`#ordBody .cxRow[data-rid="${H3}"]`, { timeout: 15000 });
    const cx3 = await cxCard(H3);
    assert(cx3 && cx3.seals.length === s0[0].seals.length && cx3.shown && /Complete Order/.test(cx3.seals[0]) && /Cancelled by Paul/.test(cx3.text), 'the Cancelled card of ' + H3 + ' keeps the seal, visible, and says who cancelled: ' + JSON.stringify(cx3));
    await page.evaluate(r => document.querySelector(`#ordBody .cxRow[data-rid="${r}"]`).scrollIntoView({ block: 'center' }), H3);
    await shot('e7-cancelled-card-seal', 150);
    pass('the Cancelled card of ' + H3 + ' keeps its seal');
    await page.click('#ordChips [data-pile="hold"]'); await page.waitForFunction(() => Orders.view().pile === 'hold');
    await undoFrom(H3, 'E-seal-undo', held);   // (the note keeps Undo for 12 s)
    const s1 = await sealsOn(H3); assert(s1.length === 1 && s1[0].key === kChain && s1[0].shown && s1[0].seals.length === s0[0].seals.length, 'after Undo the On hold card has its seal again: ' + JSON.stringify(s1));
    pass('after Undo the On hold card of ' + H3 + ' has its seal again');
    await settleCx(); await clearNotes();

    /* Release hold keeps it too: the order goes back in line, and the Orders list still shows the seal on its chain line */
    const rel = await page.evaluate(r => OrderHold.release(r, { name: 'Paul' }).then(x => ({ ok: x.ok, released: x.released, placed: x.placed, error: x.error })), H3);
    assert(rel.ok && rel.released, 'the engine released ' + H3 + ': ' + JSON.stringify(rel) + ' · lines: ' + JSON.stringify(await page.evaluate(r => Orders.rows().filter(x => String(x.order.receiptId) === r).map(x => ({ key: x.key, state: x.state, reason: x.reason, hold: x.hold, problems: (x.problems || []).map(p => p.kind), pieces: (x.poolIds || []).length, sku: x.line.sku, custom: !!(x.spec && x.spec.customDone), notCut: !!(x.spec && x.spec.special && x.spec.special.notCut) })), H3)));
    await page.evaluate(() => { try { Review.syncOrderItems(); } catch (_) {} CN.setMode('orders'); Orders.showPile('', ''); Orders.renderNow(); });
    await page.waitForFunction(k => document.querySelector(`#ordItems [data-key="${k}"] .rowActions .seal`), kChain, { timeout: 15000 });
    const s2 = await sealsOn(H3); assert(s2.length === 1 && s2[0].key === kChain && s2[0].shown && /Complete Order/.test(s2[0].seals[0]), 'after Release hold the Orders list shows the seal on its chain line: ' + JSON.stringify(s2));
    assert((await rowsOf(H3)).every(r => !r.hold && !r.releasing && r.state !== 'held'), 'a released order has no hold left: ' + JSON.stringify(await rowsOf(H3)));
    await until(() => tlOf(H3, 'released').length, 10000, 'the release step on the timeline').catch(() => {});
    assert(tlOf(H3, 'released').length === 1 && /^Released from hold by Paul · placed on /.test(tlOf(H3, 'released')[0].text), 'its timeline says it was released and where: ' + JSON.stringify(tlOf(H3, 'released').map(e => e.text)) + ' · all steps: ' + JSON.stringify(st.list(TL).filter(x => x._id.startsWith(H3 + '~')).map(x => [x.type, x.text])) + ' · engine said: ' + JSON.stringify(rel));
    assert.deepEqual(await page.evaluate(() => ({ cancel: document.querySelectorAll('#ordItems .cnCancelBtn').length, releaseOnUnheld: [...document.querySelectorAll('#ordItems .relHold:not([data-gate])')].filter(b => !/held/i.test(((b.closest('.ocard, .orderListRow') || document.body).querySelector('.ost') || {}).textContent || '')).length })), { cancel: 0, releaseOnUnheld: 0 }, 'Cancel Order is only on On hold cards (none in the Open Orders list), and Release hold only on held lines');
    await page.evaluate(r => document.querySelector(`#ordItems [data-rid="${r}"] .seal`).scrollIntoView({ block: 'center' }), H3);
    await shot('e8-released-row-seal', 500);
    pass('after Release hold the Orders list still shows the seal on the chain line of ' + H3);
    await noLoss('E-release', {});

    /* the first order is cancelled for good: it stays under Cancelled */
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('hold', ''); });
    await page.waitForFunction(r => document.querySelectorAll(`#ordItems [data-rid="${r}"] .cnCancelBtn`).length >= 3, H2, { timeout: 20000 });
    await clearNotes();   // (the list's own note for the released order, "A piece of order ... is back in line", is still up)
    const c4 =await cancelFrom(H2, 'E-cancel-2', null);
    await verifyCancel(H2, 'E-cancel-2', c4, 'Paul', 2);
    assert.equal(calls('cancelRestore').length, 2, 'two restores in all (one for each Undo)'); assert.equal(await chipN('cancelled'), '1'); assert.equal(await chipN('hold'), String(onHold0 - 2), 'On hold has lost both: one cancelled, one released');
    await settleCx(); await clearNotes();
    await noLoss('E-end', {}, idsOf(H2, 2, 3));
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('hold', ''); });
    await sleep(500); assert.equal(await cardsIn(H3), 0, 'the released order has no card in On hold'); assert.equal(await cardsIn(H2), 0, 'the cancelled order has none either');
    await quiet('E');
    pass('cancelled again for good: it stays under Cancelled (1), On hold is empty of it, no piece of any other order is lost, nothing is left on the screen');
  });

  /* ═════ F · Esc and the Skip pill during a film: the final state is reached, nothing is left, the page answers ═════ */
  /** the engine did what the popup said, whether or not the film was watched to the end (a skipped film never changes the data) */
  async function verifyData(rid, tag, x, seen, skipHow) {
    const steps = await stepsOf(rid), T = steps.map(s => s.type);
    assert.equal(T[0], 'start'); assert.deepEqual(T.slice(-3), ['sheetDone', 'held', 'done'], `${tag}: the engine ended: ` + T.join(' '));
    assert.deepEqual(sorted(steps.filter(s => s.type === 'sheetBegin').map(s => s.sheetId)), sorted(x.sheets), `${tag}: the engine worked on these sheets`);
    const log = await fxLog(), end = log.find(e => e.ev === 'end'), sk = log.find(e => e.ev === 'skip');
    assert(sk && !sk.timedOut && !sk.stalled && !sk.stay, `${tag}: the film saw the ${skipHow}: ` + JSON.stringify(log.filter(e => ['skip', 'end'].includes(e.ev))));
    assert(end && end.ok && end.skipped && !end.timedOut && !end.stalled, `${tag}: the film ended as skipped: ` + JSON.stringify(end));
    assert.equal(walks(log).length, 1, `${tag}: the way home was walked once: ` + JSON.stringify(log.filter(e => e.ev === 'home')));
    assert.equal(seen.fr.dlg, 0, `${tag}: no dialog open while the film played`); assert.equal(seen.fr.nameBar, 0, `${tag}: no name bar over the film`);
    const P = promised(seen.text), D = delivered(steps); oneOrderTwoSheets(steps, tag);
    for (const k of ['off', 'spots', 'waiting', 'newer']) if (P[k] != null) assert.equal(P[k], D[k], `${tag}: the popup said ${k} = ${P[k]}, the engine did ${D[k]}: ${seen.text.replace(/\s+/g, ' ')} || fills: ${JSON.stringify(steps.filter(s => s.type === 'fillFrom').map(s => [s.toSheetId, s.rid, s.source, s.fromSheetId, s.poolIds.length]))}`);
    assert.deepEqual(await page.evaluate(() => ({ mode: CN.S.mode, pile: Orders.view().pile })), { mode: 'orders', pile: 'hold' }, `${tag}: the person ends in Orders > On hold`);
    const rows = await rowsOf(rid), cards = await onHoldCards(rid), why = 'Taken off ' + x.names + ' by Paul';
    assert(rows.length >= 2 && rows.every(r => r.state === 'held' && r.hold === why && r.pieces === 0), `${tag}: every line is held, one reason, no piece left: ` + JSON.stringify(rows));
    assert.equal(cards.length, rows.length, `${tag}: a card per line in On hold`);
    for (const c of cards) assert(/HELD/i.test(c.pill || '') && c.text.includes(why) && c.buttons.includes('Release hold') && c.buttons.includes('Cancel Order') && !c.buttons.includes('Hold'), `${tag}: the card: ` + JSON.stringify(c));
    assert.deepEqual(await holdBtns(rid), [], `${tag}: no Hold button for a held order`);
    const sh = await onSheets();
    for (const id of x.pieces) { for (const sid of x.sheets) assert(!sh[sid].all.includes(id), `${tag}: ${id} is off ${sid}`); const p = poolDoc(id); assert(p.state === 'abandoned' && p.heldBy === 'Paul' && !p.sheetId, `${tag}: ${id} is held by Paul: ` + JSON.stringify(p)); }
    for (const sid of x.sheets) {
      const fills = steps.filter(s => s.type === 'fillFrom' && s.toSheetId === sid), n = fills.reduce((m, s) => m + s.poolIds.length, 0);
      for (const s of fills) for (const id of s.poolIds) { const p = poolDoc(id); assert(p.sheetId === sid && p.movedBy === 'Paul' && sh[sid].placed.includes(id), `${tag}: a moved piece stands on ${sid} and says who moved it: ` + JSON.stringify(p)); }
      assert.equal(sh[sid].placed.length, seen.before[sid].placed.length - x.pieces.filter(id => seen.before[sid].all.includes(id)).length + n, `${tag}: ${sid} holds as many pieces as before, plus the refill (${n})`);
      assert.equal(sh[sid].placed.length, 12, `${tag}: ${sid} is full again`);
    }
    const ev = tlOf(rid, 'held'); assert.equal(ev.length, x.sheets.length, `${tag}: one timeline step per sheet`);
    assert.equal(steps.filter(s => s.type === 'qr').length, x.sheets.length, `${tag}: QR labels remade on each sheet`);
    await noLoss(tag, {}, idsOf(H2, 2, 3));
    return { steps, log };
  }
  const answers = async what => {
    const t0 = Date.now(); await page.evaluate(() => 1); const rt = Date.now() - t0;
    const t1 = Date.now(); await page.click('#modeSeg [data-mode="review"]'); await page.waitForFunction(() => CN.S.mode === 'review', null, { timeout: 3000 }); const tab = Date.now() - t1;
    await page.click('#modeSeg [data-mode="orders"]'); await page.waitForFunction(() => CN.S.mode === 'orders', null, { timeout: 3000 });
    assert(rt < 500 && tab < 1500, `${what}: the page answers (a round trip ${rt} ms, a tab ${tab} ms)`);
    await page.evaluate(() => Orders.showPile('hold', ''));
    return { rt, tab };
  };

  await flow('F · Esc and Skip during a film (' + H4 + ' by Esc from the order window, ' + H5 + ' by the Skip pill, both from the order window)', async () => {
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('', ''); });
    // F1: Esc, a few seconds into the film of an order held from the order window
    await openWin(keyOfLine(H4, 1));
    const btn = `#owPcSum [data-piece="${keyOfLine(H4, 1)}"] [data-hold-btn]`; let t0 = 0, probe = null;
    const seen1 = await holdFrom(H4, btn, 'f1', { grabs: [['Taking the pieces off', 'f1-film-lift']], mid: async () => {
      await sleep(600);
      probe = await page.evaluate(() => ({ layer: !!document.getElementById('hfxLayer'), skip: !!document.querySelector('.hfxSkip'), dlg: document.querySelectorAll('dialog[open]').length }));
      await shot('f1-before-esc', 0);
      t0 = Date.now(); await page.keyboard.press('Escape');
      await page.waitForFunction(() => !document.getElementById('hfxLayer'), null, { timeout: 30000 });
      t0 = Date.now() - t0;
    } });
    assert(probe.layer && probe.skip && probe.dlg === 0, 'F1: the film was playing, with its Skip pill, no dialog over it: ' + JSON.stringify(probe));
    await verifyData(H4, 'F1', { sheets: ['e2e-gold-2', 'e2e-silver-1'], names: 'GF Sheet 2, SS Sheet 1', pieces: idsOf(H4, 1, 2) }, seen1, 'Esc');
    await quiet('F1');
    const a1 = await answers('F1');
    pass('Esc: the film ended ' + t0 + ' ms after the key, the order is held (the engine finished behind it), the user is in On hold with the cards, both sheets full, the page answers (' + a1.rt + ' ms round trip, ' + a1.tab + ' ms for a tab), nothing left on the screen');
    await shot('f1-after-esc', 400);

    // F2: the Skip pill, a little later in the film of another order (its pieces are not on a Review card: the order window's Hold)
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('', ''); });
    await openWin(keyOfLine(H5, 1)); let probe2 = null, tSkip = 0;
    const seen2 = await holdFrom(H5, `#owPcSum [data-piece="${keyOfLine(H5, 1)}"] [data-hold-btn]`, 'f2', { grabs: [['Filling the empty', 'f2-film-fill']], mid: async () => {
      probe2 = await page.evaluate(() => { const b = document.querySelector('.hfxSkip'), r = b && b.getBoundingClientRect(); return { skip: !!b, shown: !!(r && r.width > 0), text: b && b.textContent.trim(), covered: b ? document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2) === b || b.contains(document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2)) : false }; });
      await shot('f2-before-skip', 0);
      tSkip = Date.now(); await page.click('.hfxSkip');
      await page.waitForFunction(() => !document.getElementById('hfxLayer'), null, { timeout: 30000 });
      tSkip = Date.now() - tSkip;
    } });
    assert(probe2.skip && probe2.shown && /^Skip/.test(probe2.text) && probe2.covered, 'F2: the Skip pill is on the screen and can be pressed (nothing over it): ' + JSON.stringify(probe2));
    await verifyData(H5, 'F2', { sheets: ['e2e-gold-2', 'e2e-silver-1'], names: 'GF Sheet 2, SS Sheet 1', pieces: idsOf(H5, 1, 2) }, seen2, 'Skip');
    await quiet('F2');
    const a2 = await answers('F2');
    pass('Skip: the film ended ' + tSkip + ' ms after the press, the order is held, the user is in On hold with the cards, both sheets full, the page answers (' + a2.rt + ' ms, tab ' + a2.tab + ' ms), nothing left on the screen');
    await shot('f2-after-skip', 400);
  });

  /* ═════ summary ═════ */
  try { frameCheck(); pass('smooth frames in every film and flight (median under 40 ms, nothing froze)'); } catch (e) { fail('G · smooth frames', e); }
  { const max = await page.evaluate(() => window.__dlg && window.__dlg.max); if (max > 1) fail('G · one dialog at a time', new Error('up to ' + max + ' dialogs were open together')); else pass('never more than one dialog open at once (the order window steps aside for the popup)'); }
  const shown = frames.map(([n, f]) => `${n}: ${f.n} frames, median ${f.p50.toFixed(1)} ms, p95 ${f.p95.toFixed(1)} ms, worst ${f.max.toFixed(0)} ms, ${f.over100} over 100 ms` + (f.long && f.long.length ? ' · longest tasks: ' + f.long.map(x => `${x.d} ms while "${x.cap}"`).join('; ') : ''));
  if (shown.length) console.log('\n  frame times (load average of this 4-core machine while it ran: ' + require('os').loadavg().map(x => x.toFixed(1)).join(', ') + ')\n    ' + shown.join('\n    '));
  if (errors.length) fail('G · no console or page errors', new Error(errors.join(' || ')));
  else pass('no console errors and no page errors anywhere');
  const pageErrs = await page.evaluate(() => window.__errs);
  if (pageErrs.length) fail('G · no unhandled rejections', new Error(pageErrs.join(' || '))); else pass('no unhandled promise rejections');
  const asked = await page.evaluate(() => window.__asked);
  if (asked.prompt || asked.confirm) fail('G · no browser prompt or confirm', new Error(JSON.stringify(asked))); else pass('no browser prompt(), confirm() or alert() was used');
  if (outside.length) fail('G · no Etsy call', new Error(outside.join(','))); else pass('no Etsy call, nothing left the machine');
  if (notes.size) console.log('\n  notes (not failures)\n    ' + [...notes].join('\n    '));
  if (process.env.CONSOLE_LOG) fs.writeFileSync(process.env.CONSOLE_LOG, consoleLines.join('\n'));
  await browser.close(); srv.close();
  console.log(`\n${results.length} checks passed, ${failures.length} failed`);
  if (failures.length) { console.log('\nFAILURES\n' + failures.map(f => ' - ' + f).join('\n')); process.exit(1); }
  console.log('hold-release-cancel-e2e OK');
})().catch(e => { console.error(e); process.exit(1); });
