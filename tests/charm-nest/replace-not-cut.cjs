// IPL2 (Paul, 9 Nov 2026): "Unless the sheet has been laser cut, the update must be allowed to proceed, and the charm must be allowed to be exchanged for the correct one if the user chose an
// updated SKU or an updated charm from the repository. Nothing can block this unless the sheet has already been laser cut!" Real headless Chromium over the repo's fake site (bridge-server.cjs:
// the REAL charmNestLibrary handler, roseReopen included, on an in-memory Firestore), the stand-ins of replace-in-place.cjs. Each line below is given a new charm by a person (Wrong charm?):
//   G  GF Sheet 1, in Set 1 with SS Sheet 1 (a formed set)         → exchanged in its old spot; GF Sheet 1 stays in Set 1
//   K  14K Sheet 1, its green line drawn and its cut RECORDED      → its line set aside (roseReopen), exchanged, the line owed again
//   R  RG Sheet 1, a saved green line drawn around the piece       → its line set aside (roseReopen, replace), exchanged
//   H  GF Sheet 2, a line a person holds                           → exchanged, its hold stays
//   V  GF Sheet 2, an order in Review (W, the same listing, is given the new charm) → exchanged too, its Review stays open
// (a laser-cut sheet keeps its piece: replace-in-place.cjs (e), replace-in-place-ipl2.cjs)
//   node tests/charm-nest/replace-not-cut.cjs        (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(chromePath)) { console.log('  - no chrome: the browser checks were not run'); process.exit(0); }
const { start } = require('./bridge-server.cjs');

const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', RUN = 'run-notcut-1', SET_ID = 'set-notcut-1';
const DESIGNS = { G_OLD: 8, G_NEW: 8, S_OLD: 8, H_OLD: 8, H_NEW: 8, V_OLD: 8, V_NEW: 8, K_OLD: 8, K_NEW: 8, R_OLD: 8, R_NEW: 8 };
for (let k = 1; k <= 12; k++) DESIGNS['FILL_' + k] = 8;
const SHIP = Math.floor(Date.UTC(2026, 10, 14, 17) / 1000), BASE_TS = Math.floor(Date.UTC(2026, 10, 6, 12) / 1000);
const G = '4195500001', Sx = '4195500002', H = '4195500003', V = '4195500004', K = '4195500005', R = '4195500006', W = '4195500007', F = n => '41955100' + String(n).padStart(2, '0');
const TID = tx => String(5300000000 + tx), pid = (rid, tx, n) => `${rid}_${TID(tx)}_${n || 1}`;
// the metals as an order line names them
const METAL = { gold: { metalKey: 'gold', metalLabel: 'GF 14/20', value: '14k Gold Filled', tag: 'GF' }, silver: { metalKey: 'silver', metalLabel: 'SS', value: 'Sterling Silver', tag: 'SS' },
  gold14k: { metalKey: '14k', metalLabel: '14K Gold', value: '14k Solid Gold', tag: '14K' }, rose: { metalKey: 'rose', metalLabel: 'Rose Gold Filled', value: 'Rose Gold Filled', tag: 'RG' } };
// the sheets: [key] → { id, metal, n (its page of that metal), set?, draft?, line? }
const PAGES = { g1: { id: 'nc-gold-1', metal: 'gold', n: 1, set: true }, s1: { id: 'nc-silver-1', metal: 'silver', n: 1, set: true }, g2: { id: 'nc-gold-2', metal: 'gold', n: 2, draft: true },
  k1: { id: 'nc-14k-1', metal: 'gold14k', n: 1, line: 'cut' }, r1: { id: 'nc-rose-1', metal: 'rose', n: 1, line: 'protected' } };
let fn = 0; const fill = (pg, cells) => cells.map(cell => { fn++; return [F(fn), 100 + fn, '17000005' + String(fn).padStart(2, '0'), 'FILL_' + fn, 8000 - fn, pg, [cell], 1]; });
// [rid, tx, listing, sku, age (s), sheet key, [cells], quantity]
const LINES = [
  [G, 1, '1819500001', 'G_OLD', 9000, 'g1', [0], 1], ...fill('g1', [1, 2]),
  [Sx, 2, '1819500002', 'S_OLD', 8990, 's1', [0], 1], ...fill('s1', [1]),
  [H, 3, '1819500003', 'H_OLD', 8980, 'g2', [0], 1], [V, 4, '1819500004', 'V_OLD', 8970, 'g2', [1], 1], [W, 7, '1819500004', 'V_OLD', 8940, 'g2', [2], 1],
  [K, 5, '1819500005', 'K_OLD', 8960, 'k1', [0], 1], ...fill('k1', [1]),
  [R, 6, '1819500006', 'R_OLD', 8950, 'r1', [0], 1], ...fill('r1', [1])];
const XY = cell => [16 + (cell % 6) * 30, cell >= 6 ? 46 : 16];
const metalOfLine = l => PAGES[l[5]].metal;
const sleep = ms => new Promise(r => setTimeout(r, ms));
let n = 0; const pass = m => { n++; console.log('  ✓ ' + m); };

(async () => {
  const srv = await start({ receipts: [] }), st = srv.st;
  for (const sku of Object.keys(DESIGNS)) st.put('Charm_Master_Index', sku, { sku, aiPath: 'test/' + sku + '.ai', widthPt: DESIGNS[sku], heightPt: DESIGNS[sku], holes: 0 }, false);
  for (const [pg, sh] of Object.entries(PAGES)) {
    const cells = LINES.filter(l => l[5] === pg).flatMap(l => l[6].map((cell, k) => ({ l, cell, k })));
    const line = { profile: null, lines: [{ id: 'L1' }], shapes: cells.slice(0, 1).map((x, i) => ({ id: `${sh.id}-c${i}` })), stages: [], placements: cells.slice(0, 1).map((x, i) => ({ id: `${sh.id}-c${i}`, cxPt: XY(x.cell)[0], cyPt: XY(x.cell)[1], angle: 0, scale: 0.975 })) };
    st.put(SHEETS, sh.id, Object.assign({ id: sh.id, metal: sh.metal, charms: cells.map((x, i) => ({ id: `${sh.id}-c${i}`, poolId: pid(x.l[0], x.l[1], x.k + 1), order: x.l[0], sku: x.l[3] })), placements: cells.map((x, i) => ({ id: `${sh.id}-c${i}`, cxPt: XY(x.cell)[0], cyPt: XY(x.cell)[1], angle: 0 })), poolIds: cells.map(x => pid(x.l[0], x.l[1], x.k + 1)), sheetIndex: sh.n, fileBase: `${METAL[sh.metal].tag} Sheet ${sh.n}`, runId: RUN },
      sh.draft ? { draft: true } : {}, sh.set ? { setId: SET_ID, releaseFull: true } : {}, sh.line === 'cut' ? { roseCutAt: Date.now() - 600000, rosePlanJson: JSON.stringify(line), rosePlanHash: 'h1' } : {}, sh.line === 'protected' ? { roseProtectedJson: JSON.stringify(line) } : {}), false);
    for (const x of cells) st.put(POOL, pid(x.l[0], x.l[1], x.k + 1), { poolId: pid(x.l[0], x.l[1], x.k + 1), orderId: x.l[0], transactionId: TID(x.l[1]), lineKey: `${x.l[0]}_${TID(x.l[1])}`, sku: x.l[3], copy: x.k + 1, quantity: x.l[7], sheetId: sh.id, state: 'placed', material: sh.metal, runId: RUN, updatedAt: Date.now() - 600000 }, false);
  }
  const browser = await chromium.launch({ executablePath: chromePath, args: ['--no-sandbox'] });
  const errors = [];
  async function openPage(who) {
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
    await ctx.route(u => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
      if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: "window.pdfMake = { createPdf() { return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title>'], { type: 'text/html' })); } }; } };" });
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await ctx.addInitScript(who => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', pollOrders: 'off' })); localStorage.setItem('cn.employee', who); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => who; window.confirm = () => true; window.alert = () => {};
      const iv = setInterval(() => {
        if (typeof window.startNest !== 'function' || window.startNest.__stub) return;
        const P = window.CharmNestPDF; if (P) { P.drawCharm = () => {}; P.cutLinesOf = () => []; }
        const put = sh => api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, runId: sh.runId, fileBase: sh.fileBase, sheetIndex: sh.sheetIndex, draft: sh.draft || undefined, charms: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => ({ id: c.id, poolId: c.poolId, order: c.order, sku: (c.orderInfo && c.orderInfo.sku) || c.sku })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => c.poolId) } }, { quiet: true });
        // the nest: what it was asked with (append-only? every placement held? "making room" off?), placed pieces kept, a waiting piece into the next free cell (row 1), the sheet saved
        const stub = sh => {
          let job = null; try { job = buildJob(sh); } catch (_) { job = null; }
          window.__nests = (window.__nests || []).concat([{ sheetId: sh.sheetId, page: sh.page, appendOnly: !!sh.appendOnly, keepPlaced: !!sh._keepPlaced, unjam: job ? !!job.unjam : null, locked: job && job.lockedPlacements ? job.lockedPlacements.map(p => p.id) : null, placed: sh.placements.map(p => p.id), charms: sh.charms.map(c => c.poolId) }]);
          sh.status = 'nesting'; sh.persistedDone = false; sh.persisted = Promise.resolve(); sh.stage = 'test nest';
          setTimeout(async () => {
            try {
              sh.placements = sh.placements.filter(p => sh.charms.some(c => c.id === p.id));
              const cells = []; for (let col = 0; col < 6; col++) cells.push([16 + col * 30, 46]);
              const free = cells.filter(([x, y]) => !sh.placements.some(p => Math.hypot(p.cxPt - x, p.cyPt - y) < 24));
              for (const c of sh.charms.filter(c => !sh.placements.some(p => p.id === c.id))) { if (!free.length) break; const [x, y] = free.shift(); sh.placements.push({ id: c.id, cxPt: x, cyPt: y, angle: 0 }); }
              if (sh.sheetId) await put(sh);
              sh.status = 'complete'; sh.dirty = false; sh.intakeAppend = false; sh.appendOnly = false; sh.stage = ''; sh.persistedDone = true; sh.verification = { ok: true }; delete sh._keepPlaced;
            } catch (e) { sh.problem = e.message; sh.status = 'complete'; sh.persistedDone = true; }
            try { CN.renderCard(sh); } catch (_) {}
          }, 120);
        };
        stub.__stub = true; window.startNest = stub;
        // the write as it stands: what it was asked with, the sheet saved as it is
        const wap = sh => {
          window.__writes = (window.__writes || []).concat([{ sheetId: sh.sheetId, page: sh.page, placements: sh.placements.map(p => ({ id: p.id, cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle })), charms: sh.charms.map(c => c.poolId) }]);
          sh.status = 'finishing'; sh.persistedDone = false; let done; sh.persisted = new Promise(r => { done = r; });
          return new Promise(res => setTimeout(async () => { try { if (sh.sheetId) await put(sh); } catch (e) { sh.problem = e.message; } sh.status = 'complete'; sh.dirty = false; sh.persistedDone = true; sh.verification = { ok: true }; done(); try { CN.renderCard(sh); } catch (_) {} res(!sh.problem); }, 120));
        };
        wap.__stub = true; window.writeAsPlaced = wap; clearInterval(iv);
      }, 0);
    }, who);
    const page = await ctx.newPage(); page.setDefaultTimeout(30000);
    page.on('pageerror', e => errors.push(`${who}: ${String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')}`));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Orders && window.Review && Review.fixCharm && window.CNManual && CNManual.fitsSpot && window.Pool && window.LiveNest && window.startNest && window.startNest.__stub && window.writeAsPlaced && window.writeAsPlaced.__stub && B.master.loadedAt > 0 && !B.master.loading, null, { timeout: 60000 });
    return { ctx, page };
  }
  const setup = page => page.evaluate(async ({ LINES, PAGES, RUN, BASE_TS, SHIP, DESIGNS, SET_ID, METAL }) => {
    await Orders.loadMaps(true);
    CN.S.stockPreset = 'custom'; CN.S.settings.stock = Object.assign({}, CN.S.settings.stock, { gold: [182 / 72, 62 / 72] });
    const TID = tx => String(5300000000 + tx), pid = (rid, tx, n) => `${rid}_${TID(tx)}_${n || 1}`;
    // each design a solid square of its own side, drawn at 2 px/pt (the check's own resolution)
    const sq = (x0, y0, x1, y1) => ({ kind: 'path', closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [x0, y0, x1, y1], subpaths: [[['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']]] });
    const mkCharm = (id, sku, extra) => { const side = DESIGNS[sku], px = side * 2, outline = sq(0, 0, side, side); return Object.assign({ id, name: id, sku, sourceId: 's', ringGeometryVersion: 3, centerPt: [side / 2, side / 2], bbox: [0, 0, side, side], outline, members: [outline], rMm: side / 2 * 25.4 / 72, w: px, h: px, scale: 2, bits: new Uint8Array(px * px).fill(1), widthPt: side, heightPt: side, areaPt2: side * side, hash: 'h-' + sku }, extra || {}); };
    const reg = sku => { const key = 'test/' + sku + '.ai'; B.master.entries.set(sku, { sku, updatedAt: 1, aiPath: key, widthPt: DESIGNS[sku], heightPt: DESIGNS[sku], holes: 0, upAngle: null, labelSource: 'text', charmHash: 'h-' + sku }); B.pool.sources.set(key, { id: 'pool:' + sku, pool: true, name: sku, sku, charms: [mkCharm('pool:' + sku + ':0', sku, { name: sku })], bodiesChecked: true, mirrors: [mkCharm('pool:' + sku + ':0m', sku, { name: sku, mirror: true, bodyIndex: 0 })], usedAt: Date.now() }); };   // (a square is its own mirror image: the Right earring's piece is ready)
    for (const sku of Object.keys(DESIGNS)) reg(sku);
    for (const m of ['gold', 'silver', 'gold14k', 'rose']) CN.S.settings.stock = Object.assign({}, CN.S.settings.stock, { [m]: [182 / 72, 62 / 72] });
    B.pool.rows.clear();
    for (const m of [...new Set(Object.values(PAGES).map(p => p.metal))]) { const pr = CN.S.sheets[m]; pr.pages.length = 1; const p0 = pr.pages[0]; Object.assign(p0, { charms: [], placements: [], sheetId: null, fileBase: null, status: 'idle', page: 1 }); for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId', 'releaseFull', 'rosePlan', 'roseProtected']) delete p0[k]; }
    for (const [pg, sh] of Object.entries(PAGES)) {
      const pr = CN.S.sheets[sh.metal];
      while (pr.pages.length < sh.n) addPage(sh.metal);
      const page = pr.pages[sh.n - 1]; page.charms = []; page.placements = [];
      const cells = LINES.filter(l => l[5] === pg).flatMap(l => l[6].map((cell, k) => ({ l, cell, k })));
      cells.forEach((x, i) => {
        const l = x.l, id = `${sh.id}-c${i}`, poolId = pid(l[0], l[1], x.k + 1);
        page.charms.push(mkCharm(id, l[3], { name: `${l[0]} · ${l[3]}`, poolId, order: l[0], lineKey: `${l[0]}_${TID(l[1])}`, orderDate: BASE_TS - l[4], metal: sh.metal, orderInfo: { receiptId: l[0], transactionId: TID(l[1]), sku: l[3], copy: x.k + 1, quantity: l[7] } }));
        page.placements.push({ id, cxPt: 16 + (x.cell % 6) * 30, cyPt: x.cell >= 6 ? 46 : 16, angle: 0, wPt: DESIGNS[l[3]], hPt: DESIGNS[l[3]], scale: 0.975 });
      });
      Object.assign(page, { status: 'complete', sheetId: sh.id, fileBase: `${METAL[sh.metal].tag} Sheet ${sh.n}`, sheetIndex: sh.n, page: sh.n, runId: RUN, dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, density: .3, verification: { ok: true }, intakeAppend: false, appendOnly: false });
      if (sh.draft) Object.assign(page, { draft: true, setId: null });
      if (sh.set) Object.assign(page, { setId: SET_ID, releaseFull: true });   // (a formed set's sheets: each full, released into it)
      // a green line: drawn and its cut RECORDED (14K), or saved around the piece (Rose Gold): neither is the laser's mark
      const first = page.placements[0], line = { profile: null, lines: [{ id: 'L1' }], shapes: [{ id: first.id }], stages: [], placements: [Object.assign({}, first)] };
      if (sh.line === 'cut') Object.assign(page, { roseCutAt: Date.now() - 600000, rosePlan: line, rosePlanHash: 'h1', solidPick: true });
      if (sh.line === 'protected') Object.assign(page, { roseProtected: line });
      CN.renderCard(page);
    }
    // a set's files: the sheet's QR label (Sets.onSheetSaved) and the set's labels PDF / manifest / set.json (Sets.finalize), each call recorded
    window.__labels = []; window.__finals = [];
    Sets.onSheetSaved = async (sh, items, outputs, opt) => { window.__labels.push({ sheetId: sh.sheetId, labelsOnly: !!(opt && opt.labelsOnly), setId: opt && opt.setOverride ? opt.setOverride.setId : null, pieces: sh.placements.length }); };
    Sets.finalize = async set => { window.__finals.push({ setId: set.setId, committedAt: set.committedAt || null }); set.labels = { pdf: { path: 'labels.pdf', url: 'u' }, files: [] }; return set; };
    B.sets.clear();
    B.sets.set(`${RUN}|dispatch`, { setId: SET_ID, runId: RUN, seq: 1, day: new Date().toISOString().slice(0, 10), group: 'dispatch', name: 'Set 1', sheetIds: [PAGES.g1.id, PAGES.s1.id], materials: ['gold', 'silver'], labelFiles: [], orders: {}, status: 'nesting' });
    const rows = [];
    for (const l of LINES) {
      const [rid, tx, listingId, sku, age, pg, cells, qty, pair] = l;
      const M = METAL[PAGES[pg].metal], line = { transactionId: TID(tx), listingId, sku, title: sku.replace(/_.*/, '').toLowerCase() + ' charm', quantity: qty, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: M.value }], metalKey: M.metalKey, metalLabel: M.metalLabel, personalization: [], buyerMessage: '' };
      const order = { receiptId: rid, orderNumber: rid, createTs: BASE_TS - age, updateTs: BASE_TS, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-2) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [line] };
      const ids = cells.map((_, k) => pid(rid, tx, k + 1));
      rows.push({ key: CharmNestOrders.lineKey(order, line), order, line, arrivedAt: Date.now() - 7200000, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: ids, engrave: null, material: PAGES[pg].metal });
      ids.forEach((id, k) => B.pool.rows.set(id, { poolId: id, orderId: rid, transactionId: TID(tx), lineKey: `${rid}_${TID(tx)}`, sku, copy: k + 1, quantity: qty, ...(pair ? { side: k ? 'R' : 'L', groupKey: `${rid}_${TID(tx)}`, groupSize: 2 } : {}), sheetId: PAGES[pg].id, state: 'placed', material: PAGES[pg].metal, runId: RUN, updatedAt: Date.now() - 600000 }));
    }
    B.orders.rows.length = 0; B.orders.byKey.clear(); for (const r of rows) { B.orders.rows.push(r); B.orders.byKey.set(r.key, r); }
    B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: SET_ID, setIds: [SET_ID], releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [...new Set(rows.map(r => r.order.receiptId))] };
    Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('orders'); Orders.render();
    if (!B.employee) B.employee = 'Paul';
    return rows.map(r => [r.order.receiptId, r.spec.designSku, r.spec.skuSource, r.state]);
  }, { LINES, PAGES, RUN, BASE_TS, SHIP, DESIGNS, SET_ID, METAL });
  const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + (typeof what === 'function' ? await what() : what)); await sleep(150); } };
  // every sheet as it stands: { sheetId: { poolId: [cx, cy, angle, sku] } }
  const layout = page => page.evaluate(() => Object.fromEntries(['gold', 'silver', 'gold14k', 'rose'].flatMap(m => CN.S.sheets[m].pages).filter(p => p.sheetId).map(p => [p.sheetId, Object.fromEntries(p.placements.map(pl => { const c = p.charms.find(z => z.id === pl.id) || {}; return [c.poolId, [+pl.cxPt, +pl.cyPt, +pl.angle || 0, (c.orderInfo && c.orderInfo.sku) || c.sku]]; }))])));
  const settled = page => page.evaluate(() => ['gold', 'silver', 'gold14k', 'rose'].flatMap(m => CN.S.sheets[m].pages).every(p => !['nesting', 'finishing', 'queued', 'ready'].includes(p.status) && !p._inplaceWrite && !p._inplaceHold) && !!document.querySelector('.rlFlow[data-done="1"]') || false);
  const counts = page => page.evaluate(() => ({ nests: (window.__nests || []).length, writes: (window.__writes || []).length }));
  const fix = (page, rid, sku) => page.evaluate(async ({ rid, sku }) => { document.querySelectorAll('.rlFlow').forEach(x => x.remove()); const row = B.orders.rows.find(r => r.order.receiptId === rid); const r = await Review.fixCharm(row, sku, { confirm: () => true }); return { ok: r.ok, why: r.why || '' }; }, { rid, sku });
  const flowLines = page => page.evaluate(() => { const el = document.querySelector('.rlFlow'); return el ? { sum: el.querySelector('.rlSum').textContent, lines: [...el.querySelectorAll('li')].map(li => [li.dataset.s, li.querySelector('.rlT').textContent]) } : null; });
  const without = (L, sheet, ids) => { const o = Object.assign({}, L[sheet]); for (const id of ids) delete o[id]; return o; };
  // no two charms of a sheet overlap and none runs off it: the page's own independent verifier over each sheet as it stands, with the shop's clearance (the nest's final check)
  const noOverlap = page => page.evaluate(() => ['gold', 'silver', 'gold14k', 'rose'].flatMap(m => CN.S.sheets[m].pages).filter(p => !p.laserDoneAt && p.placements.length).map(p => { const r = CharmNestSolver.verify(buildJob(p), p.placements.map(q => ({ id: q.id, cxPt: +q.cxPt, cyPt: +q.cyPt, angle: +q.angle || 0 })), 6); return r.ok ? '' : `${p.sheetId}: ${r.overlapPx} px overlap, ${r.outsidePx} px off the sheet`; }).filter(Boolean).join('; ') || true);
  try {
    const { page } = await openPage('Paul');
    const read0 = await setup(page);
    assert(read0.every(r => r[3] !== 'held'), 'every line reads its design: ' + JSON.stringify(read0));
    const L0 = await layout(page);
    const idG = pid(G, 1), idH = pid(H, 3), idV = pid(V, 4), idK = pid(K, 5), idR = pid(R, 6);
    const pageOf = (pg, id) => page.evaluate(({ pg, id }) => { const sh = ['gold', 'silver', 'gold14k', 'rose'].flatMap(m => CN.S.sheets[m].pages).find(p => p.sheetId === pg); const c = sh && sh.charms.find(z => z.poolId === id), pl = c && sh.placements.find(q => q.id === c.id); return { sku: c && ((c.orderInfo && c.orderInfo.sku) || c.sku), at: pl ? [pl.cxPt, pl.cyPt] : null, setId: sh.setId || null, roseCutAt: sh.roseCutAt || null, plan: !!sh.rosePlan, prot: !!sh.roseProtected, redraw: sh.roseRedraw || null, reopened: sh.roseReopened || null }; }, { pg, id });
    const rowOf = rid => page.evaluate(rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid); return { state: r.state, hold: r.hold || null, review: !!Pool.keepsPlace(r), pieces: (r.poolIds || []).map(id => (B.pool.rows.get(id) || {}).sku) }; }, rid);
    const reopens = (id, more) => st.calls.filter(c => c.op === 'roseReopen' && JSON.stringify(c.body || c).includes(id) && (!more || more(c))).length;
    let Lb = L0; const others = (L, id) => { for (const sid of Object.keys(Lb)) for (const [k, at] of Object.entries(Lb[sid])) if (k !== id) assert.deepEqual(L[sid] && L[sid][k], at, `${k} on ${sid} did not move`); Lb = L; };
    const go = async (rid, sku) => { const r = await fix(page, rid, sku); assert(r.ok, `${rid} relinked to ${sku}: ` + JSON.stringify(r)); await until(() => settled(page), 30000, async () => `${rid} settled: ` + JSON.stringify(await flowLines(page))); await sleep(400); };

    /* 1 · a GF sheet in a real set (Set 1: GF Sheet 1 + SS Sheet 1): exchanged in its old spot, and the sheet keeps its set */
    await go(G, 'G_NEW');
    { const p = await pageOf('nc-gold-1', idG); assert.deepEqual([p.sku, p.at], ['G_NEW', [16, 16]], 'G_NEW is in G\'s old spot on GF Sheet 1: ' + JSON.stringify(p)); }
    others(await layout(page), idG);
    await sleep(2500);   // (the set's own look at its sheets runs meanwhile)
    { const s = await page.evaluate(({ RUN, SET_ID }) => { const set = Sets.ofRun(RUN).find(x => x.setId === SET_ID), g = CN.S.sheets.gold.pages.find(p => p.sheetId === 'nc-gold-1'), sv = CN.S.sheets.silver.pages.find(p => p.sheetId === 'nc-silver-1'); return { g: g.setId, gDraft: !!g.draft, s: sv.setId, ids: set && set.sheetIds }; }, { RUN, SET_ID });
      assert(s.g === SET_ID && !s.gDraft && s.s === SET_ID && (s.ids || []).includes('nc-gold-1'), 'GF Sheet 1 keeps its place in Set 1 after the exchange: ' + JSON.stringify(s)); }
    pass('a sheet in a formed GF + SS set takes the new charm in its old spot and stays in its set');

    /* 2 · a 14K sheet whose green line is drawn and its cut RECORDED (not laser cut): the line is set aside, the charm exchanged, the line owed again */
    const k0 = reopens('nc-14k-1');
    await go(K, 'K_NEW');
    { const p = await pageOf('nc-14k-1', idK), r = await rowOf(K);
      assert.deepEqual([p.sku, p.at], ['K_NEW', [16, 16]], 'K_NEW is in K\'s old spot on 14K Sheet 1: ' + JSON.stringify({ p, r, fl: await flowLines(page) }));
      assert(reopens('nc-14k-1') > k0 && !p.roseCutAt && !p.plan && p.reopened && p.reopened.cut, 'its green line and recorded cut were set aside first (roseReopen): ' + JSON.stringify(p));
      assert(p.redraw && p.redraw.line, 'its green line is owed again for the sheet as it now stands: ' + JSON.stringify(p.redraw));
      assert(r.state !== 'held', 'the line is not held: ' + JSON.stringify(r)); }
    others(await layout(page), idK);
    pass('a 14K sheet with its green line drawn and its cut recorded (not laser cut) takes the new charm: the line is set aside first and owed again for the sheet as it now stands');

    /* 3 · a Rose Gold sheet whose saved green line is drawn around the very piece: set aside for the exchange too */
    const r0 = reopens('nc-rose-1', c => /"replace":true/.test(JSON.stringify(c.body || c)));
    await go(R, 'R_NEW');
    { const p = await pageOf('nc-rose-1', idR), r = await rowOf(R);
      assert.deepEqual([p.sku, p.at], ['R_NEW', [16, 16]], 'R_NEW is in R\'s old spot on RG Sheet 1: ' + JSON.stringify({ p, r, fl: await flowLines(page) }));
      assert(reopens('nc-rose-1', c => /"replace":true/.test(JSON.stringify(c.body || c))) > r0 && !p.prot, 'its saved green line was set aside first (roseReopen, replace): ' + JSON.stringify(p));
      assert(r.state !== 'held', 'the line is not held: ' + JSON.stringify(r)); }
    others(await layout(page), idR);
    pass('a Rose Gold sheet whose saved green line encloses the piece takes the new charm: the line is set aside first (roseReopen, replace)');

    /* 4 · a line a person holds: the charm is exchanged, the hold stays */
    const HOLD = 'Held by Paul: check the engraving';
    await page.evaluate(({ H, HOLD }) => { const r = B.orders.rows.find(x => x.order.receiptId === H); r.hold = r.reason = HOLD; r.state = 'held'; }, { H, HOLD });
    await go(H, 'H_NEW');
    { const p = await pageOf('nc-gold-2', idH), r = await rowOf(H);
      assert.deepEqual([p.sku, p.at], ['H_NEW', [16, 16]], 'H_NEW is in H\'s old spot on GF Sheet 2: ' + JSON.stringify({ p, r, fl: await flowLines(page) }));
      assert.deepEqual([r.hold, r.state, r.pieces], [HOLD, 'held', ['H_NEW']], 'the person\'s hold stays on the line: ' + JSON.stringify(r)); }
    pass('a line a person holds takes the new charm; its hold stays');

    /* 5 · an order in Review: another order of the same listing (W) is given the new charm; V, still in Review, takes it too and its Review stays open
       (a pick made ON an order is that order's own answer to its Review, and closes it, as before) */
    await page.evaluate(async V => { await CharmNestReviewMark.mark(V, { by: 'Paul' }); }, V);
    assert((await rowOf(V)).review, 'V is in Review');
    await go(W, 'V_NEW');
    await until(async () => (await pageOf('nc-gold-2', idV)).sku === 'V_NEW', 20000, async () => 'V exchanged: ' + JSON.stringify({ p: await pageOf('nc-gold-2', idV), r: await rowOf(V), fl: await flowLines(page) }));
    { const p = await pageOf('nc-gold-2', idV), r = await rowOf(V);
      assert.deepEqual(p.at, [46, 16], 'V_NEW is in V\'s old spot on GF Sheet 2: ' + JSON.stringify(p));
      assert(r.review && r.pieces[0] === 'V_NEW', 'the order stays in Review with its new piece: ' + JSON.stringify(r)); }
    { const ov = await noOverlap(page); assert(ov === true, 'no two charms overlap on any sheet: ' + ov); }
    pass('an order still in Review takes the new charm picked on another order of its listing; its Review stays open');

    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`replace-not-cut: ${n} steps passed`);
})().catch(e => { console.error(e); process.exit(1); });
