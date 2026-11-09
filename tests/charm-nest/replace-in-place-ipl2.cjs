// IPL2 (Paul, 9 Nov 2026): "When I update the charm repository, SKU matching charm to a given order gets removed from a sheet and then placed on a brand new sheet which I don't want, the new
// charm should first be attempted to be put on the sheet where it came from because otherwise there's a hole in that sheet." A replaced piece is tried: 1. its old spot; 2. anywhere free on its
// old sheet, nothing else moving (CNManual.freeSpot); 3. the latest unfinished sheet with room for its shape; 4. only then a new sheet. Real headless Chromium over the repo's fake site
// (bridge-server.cjs), the stand-ins of replace-in-place.cjs (the nest, the write as it stands); the fit checks are the page's own over pieces drawn as solid squares.
//   GF Sheet 1 (open draft): A + 3 fillers         → A_BIG (44 pt): beside its old spot on GF Sheet 1
//   GF Sheet 2 (in a FORMED set): B + 3 fillers    → B_BIG (44 pt): beside its old spot on GF Sheet 2, the set kept
//   GF Sheet 3 (open draft, two full rows): C, E   → C_BIG (44 pt): no room there → GF Sheet 4, the latest unfinished sheet;  E_HUGE (61 pt): room nowhere → a new sheet
//   GF Sheet 5 (LASER CUT): D                      → never changed
//   node tests/charm-nest/replace-in-place-ipl2.cjs        (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(chromePath)) { console.log('  - no chrome: the browser checks were not run'); process.exit(0); }
const { start } = require('./bridge-server.cjs');

const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', RUN = 'run-ipl2-1', SET_ID = 'set-ipl2-1';
const LA = '1819400001', LB = '1819400002', LC = '1819400003', LD = '1819400004', LE = '1819400005';
// the designs: [sku, side in pt] (a solid square). The sheets are 182 × 62 pt; the fillers stand 30 pt apart on row 0 (y 16) and row 1 (y 46)
const DESIGNS = { A_OLD: 8, A_BIG: 44, B_OLD: 8, B_BIG: 44, C_OLD: 8, C_BIG: 44, E_OLD: 8, E_HUGE: 61, D_OLD: 8, D_NEW: 8 };
for (let k = 1; k <= 30; k++) DESIGNS['FILL_' + k] = 8;
const SHIP = Math.floor(Date.UTC(2026, 10, 14, 17) / 1000), BASE_TS = Math.floor(Date.UTC(2026, 10, 6, 12) / 1000);
const A = '4194400001', Bo = '4194400002', C = '4194400003', D = '4194400004', E = '4194400005', F = n => '41944100' + String(n).padStart(2, '0');
const TID = tx => String(5200000000 + tx), pid = (rid, tx, n) => `${rid}_${TID(tx)}_${n || 1}`;
// [rid, tx, listing, sku, age (s), sheet page, [cells], quantity]   (a cell: 0-5 on row 0, 6-11 on row 1)
let fn = 0; const fill = (pg, cells) => cells.map(cell => { fn++; return [F(fn), 100 + fn, '17000004' + String(fn).padStart(2, '0'), 'FILL_' + fn, 8000 - fn, pg, [cell], 1]; });
const LINES = [
  // GF Sheet 1, an open draft: A and three fillers on row 0 (the right of the sheet is free)
  [A, 1, LA, 'A_OLD', 9000, 1, [0], 1], ...fill(1, [1, 2, 3]),
  // GF Sheet 2, in a FORMED set (Set 1, not released): B and three fillers on row 0
  [Bo, 2, LB, 'B_OLD', 8990, 2, [0], 1], ...fill(2, [1, 2, 3]),
  // GF Sheet 3, an open draft with no room for a big piece: C and E among fillers on both rows
  [C, 3, LC, 'C_OLD', 8980, 3, [0], 1], [E, 5, LE, 'E_OLD', 8970, 3, [1], 1], ...fill(3, [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
  // GF Sheet 4, the latest open draft: fillers at x 16, 76, 136 on row 0 (room for a 44 pt piece, none for a 61 pt one)
  ...fill(4, [0, 2, 4]),
  // GF Sheet 5, LASER CUT: D
  [D, 4, LD, 'D_OLD', 9500, 5, [0], 1]];
const PAGES = { 1: { id: 'i2-gold-1', draft: true }, 2: { id: 'i2-gold-2', set: true }, 3: { id: 'i2-gold-3', draft: true }, 4: { id: 'i2-gold-4', draft: true }, 5: { id: 'i2-gold-5', cut: true } };
const XY = cell => [16 + (cell % 6) * 30, cell >= 6 ? 46 : 16];
const sleep = ms => new Promise(r => setTimeout(r, ms));
let n = 0; const pass = m => { n++; console.log('  ✓ ' + m); };

(async () => {
  const srv = await start({ receipts: [] }), st = srv.st;
  for (const sku of Object.keys(DESIGNS)) st.put('Charm_Master_Index', sku, { sku, aiPath: 'test/' + sku + '.ai', widthPt: DESIGNS[sku], heightPt: DESIGNS[sku], holes: 0 }, false);
  for (const [pg, sh] of Object.entries(PAGES)) {
    const mine = LINES.filter(l => l[5] === +pg), cells = mine.flatMap(l => l[6].map((cell, k) => ({ l, cell, k })));
    st.put(SHEETS, sh.id, Object.assign({ id: sh.id, metal: 'gold', charms: cells.map((x, i) => ({ id: `${sh.id}-c${i}`, poolId: pid(x.l[0], x.l[1], x.k + 1), order: x.l[0], sku: x.l[3] })), placements: cells.map((x, i) => ({ id: `${sh.id}-c${i}`, cxPt: XY(x.cell)[0], cyPt: XY(x.cell)[1], angle: 0 })), poolIds: cells.map(x => pid(x.l[0], x.l[1], x.k + 1)), sheetIndex: +pg, fileBase: 'GF Sheet ' + pg, runId: RUN }, sh.cut ? { laserDoneAt: Date.now() - 3600000 } : {}, sh.draft ? { draft: true } : {}, sh.set ? { setId: SET_ID } : {}), false);
    for (const x of cells) st.put(POOL, pid(x.l[0], x.l[1], x.k + 1), { poolId: pid(x.l[0], x.l[1], x.k + 1), orderId: x.l[0], transactionId: TID(x.l[1]), lineKey: `${x.l[0]}_${TID(x.l[1])}`, sku: x.l[3], copy: x.k + 1, quantity: x.l[7], ...(x.l[8] ? { side: x.k ? 'R' : 'L', groupKey: `${x.l[0]}_${TID(x.l[1])}`, groupSize: 2 } : {}), sheetId: sh.id, state: 'placed', material: 'gold', runId: RUN, updatedAt: Date.now() - 600000 }, false);
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
  const setup = page => page.evaluate(async ({ LINES, PAGES, RUN, BASE_TS, SHIP, DESIGNS, SET_ID }) => {
    await Orders.loadMaps(true);
    CN.S.stockPreset = 'custom'; CN.S.settings.stock = Object.assign({}, CN.S.settings.stock, { gold: [182 / 72, 62 / 72] });
    const TID = tx => String(5200000000 + tx), pid = (rid, tx, n) => `${rid}_${TID(tx)}_${n || 1}`;
    // each design a solid square of its own side, drawn at 2 px/pt (the check's own resolution)
    const sq = (x0, y0, x1, y1) => ({ kind: 'path', closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [x0, y0, x1, y1], subpaths: [[['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']]] });
    const mkCharm = (id, sku, extra) => { const side = DESIGNS[sku], px = side * 2, outline = sq(0, 0, side, side); return Object.assign({ id, name: id, sku, sourceId: 's', ringGeometryVersion: 3, centerPt: [side / 2, side / 2], bbox: [0, 0, side, side], outline, members: [outline], rMm: side / 2 * 25.4 / 72, w: px, h: px, scale: 2, bits: new Uint8Array(px * px).fill(1), widthPt: side, heightPt: side, areaPt2: side * side, hash: 'h-' + sku }, extra || {}); };
    const reg = sku => { const key = 'test/' + sku + '.ai'; B.master.entries.set(sku, { sku, updatedAt: 1, aiPath: key, widthPt: DESIGNS[sku], heightPt: DESIGNS[sku], holes: 0, upAngle: null, labelSource: 'text', charmHash: 'h-' + sku }); B.pool.sources.set(key, { id: 'pool:' + sku, pool: true, name: sku, sku, charms: [mkCharm('pool:' + sku + ':0', sku, { name: sku })], bodiesChecked: true, mirrors: [mkCharm('pool:' + sku + ':0m', sku, { name: sku, mirror: true, bodyIndex: 0 })], usedAt: Date.now() }); };   // (a square is its own mirror image: the Right earring's piece is ready)
    for (const sku of Object.keys(DESIGNS)) reg(sku);
    const pr = CN.S.sheets.gold; pr.pages.length = 1; const p0 = pr.pages[0]; Object.assign(p0, { charms: [], placements: [], sheetId: null, fileBase: null, status: 'idle', page: 1 }); for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId', 'releaseFull']) delete p0[k];
    B.pool.rows.clear();
    for (const [pg, sh] of Object.entries(PAGES)) {
      while (pr.pages.length < +pg) addPage('gold');
      const page = pr.pages[+pg - 1]; page.charms = []; page.placements = [];
      const cells = LINES.filter(l => l[5] === +pg).flatMap(l => l[6].map((cell, k) => ({ l, cell, k })));
      cells.forEach((x, i) => {
        const l = x.l, id = `${sh.id}-c${i}`, poolId = pid(l[0], l[1], x.k + 1);
        page.charms.push(mkCharm(id, l[3], { name: `${l[0]} · ${l[3]}`, poolId, order: l[0], lineKey: `${l[0]}_${TID(l[1])}`, orderDate: BASE_TS - l[4], orderInfo: Object.assign({ receiptId: l[0], transactionId: TID(l[1]), sku: l[3], copy: x.k + 1, quantity: l[7] }, l[8] ? { side: x.k ? 'R' : 'L' } : {}) }));
        page.placements.push({ id, cxPt: 16 + (x.cell % 6) * 30, cyPt: x.cell >= 6 ? 46 : 16, angle: 0, wPt: DESIGNS[l[3]], hPt: DESIGNS[l[3]], scale: 0.975 });
      });
      Object.assign(page, { status: 'complete', sheetId: sh.id, fileBase: 'GF Sheet ' + pg, sheetIndex: +pg, page: +pg, runId: RUN, dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, density: .3, verification: { ok: true }, intakeAppend: false, appendOnly: false });
      if (sh.cut) page.laserDoneAt = Date.now() - 3600000;
      if (sh.draft) Object.assign(page, { draft: true, setId: null });
      if (sh.set) page.setId = SET_ID;
      CN.renderCard(page);
    }
    // a set's files: the sheet's QR label (Sets.onSheetSaved) and the set's labels PDF / manifest / set.json (Sets.finalize), each call recorded
    window.__labels = []; window.__finals = [];
    Sets.onSheetSaved = async (sh, items, outputs, opt) => { window.__labels.push({ sheetId: sh.sheetId, labelsOnly: !!(opt && opt.labelsOnly), setId: opt && opt.setOverride ? opt.setOverride.setId : null, pieces: sh.placements.length }); };
    Sets.finalize = async set => { window.__finals.push({ setId: set.setId, committedAt: set.committedAt || null }); set.labels = { pdf: { path: 'labels.pdf', url: 'u' }, files: [] }; return set; };
    B.sets.clear();
    B.sets.set(`${RUN}|dispatch`, { setId: SET_ID, runId: RUN, seq: 1, day: new Date().toISOString().slice(0, 10), group: 'dispatch', name: 'Set 1', sheetIds: [PAGES[2].id], materials: ['gold'], labelFiles: [], orders: {}, status: 'nesting' });
    const rows = [];
    for (const l of LINES) {
      const [rid, tx, listingId, sku, age, pg, cells, qty, pair] = l;
      const line = { transactionId: TID(tx), listingId, sku, title: sku.replace(/_.*/, '').toLowerCase() + (pair ? ' charm stud earrings' : ' charm'), quantity: qty, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' };
      const order = { receiptId: rid, orderNumber: rid, createTs: BASE_TS - age, updateTs: BASE_TS, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-2) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [line] };
      const ids = cells.map((_, k) => pid(rid, tx, k + 1));
      rows.push({ key: CharmNestOrders.lineKey(order, line), order, line, arrivedAt: Date.now() - 7200000, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: ids, engrave: null, material: 'gold' });
      ids.forEach((id, k) => B.pool.rows.set(id, { poolId: id, orderId: rid, transactionId: TID(tx), lineKey: `${rid}_${TID(tx)}`, sku, copy: k + 1, quantity: qty, ...(pair ? { side: k ? 'R' : 'L', groupKey: `${rid}_${TID(tx)}`, groupSize: 2 } : {}), sheetId: PAGES[pg].id, state: 'placed', material: 'gold', runId: RUN, updatedAt: Date.now() - 600000 }));
    }
    B.orders.rows.length = 0; B.orders.byKey.clear(); for (const r of rows) { B.orders.rows.push(r); B.orders.byKey.set(r.key, r); }
    B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: SET_ID, setIds: [SET_ID], releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [...new Set(rows.map(r => r.order.receiptId))] };
    Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('orders'); Orders.render();
    if (!B.employee) B.employee = 'Paul';
    return rows.map(r => [r.order.receiptId, r.spec.designSku, r.spec.skuSource, r.state]);
  }, { LINES, PAGES, RUN, BASE_TS, SHIP, DESIGNS, SET_ID });
  const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + (typeof what === 'function' ? await what() : what)); await sleep(150); } };
  // every sheet as it stands: { sheetId: { poolId: [cx, cy, angle, sku] } }
  const layout = page => page.evaluate(() => Object.fromEntries(CN.S.sheets.gold.pages.map(p => [p.sheetId, Object.fromEntries(p.placements.map(pl => { const c = p.charms.find(z => z.id === pl.id) || {}; return [c.poolId, [+pl.cxPt, +pl.cyPt, +pl.angle || 0, (c.orderInfo && c.orderInfo.sku) || c.sku]]; }))])));
  const settled = page => page.evaluate(() => CN.S.sheets.gold.pages.every(p => !['nesting', 'finishing', 'queued', 'ready'].includes(p.status) && !p._inplaceWrite && !p._inplaceHold) && !!document.querySelector('.rlFlow[data-done="1"]') || false);
  const counts = page => page.evaluate(() => ({ nests: (window.__nests || []).length, writes: (window.__writes || []).length }));
  const fix = (page, rid, sku) => page.evaluate(async ({ rid, sku }) => { document.querySelectorAll('.rlFlow').forEach(x => x.remove()); const row = B.orders.rows.find(r => r.order.receiptId === rid); const r = await Review.fixCharm(row, sku, { confirm: () => true }); return { ok: r.ok, why: r.why || '' }; }, { rid, sku });
  const flowLines = page => page.evaluate(() => { const el = document.querySelector('.rlFlow'); return el ? { sum: el.querySelector('.rlSum').textContent, lines: [...el.querySelectorAll('li')].map(li => [li.dataset.s, li.querySelector('.rlT').textContent]) } : null; });
  const without = (L, sheet, ids) => { const o = Object.assign({}, L[sheet]); for (const id of ids) delete o[id]; return o; };
  // no two charms of a sheet overlap and none runs off it: the page's own independent verifier over each sheet as it stands, with the shop's clearance (the nest's final check)
  const noOverlap = page => page.evaluate(() => CN.S.sheets.gold.pages.filter(p => !p.laserDoneAt && p.placements.length).map(p => { const r = CharmNestSolver.verify(buildJob(p), p.placements.map(q => ({ id: q.id, cxPt: +q.cxPt, cyPt: +q.cyPt, angle: +q.angle || 0 })), 6); return r.ok ? '' : `${p.sheetId}: ${r.overlapPx} px overlap, ${r.outsidePx} px off the sheet`; }).filter(Boolean).join('; ') || true);
  try {
    const { page } = await openPage('Paul');
    const read0 = await setup(page);
    assert(read0.every(r => r[3] !== 'held'), 'every line reads its design: ' + JSON.stringify(read0));
    const L0 = await layout(page), w0 = st.calls.length, pages0 = await page.evaluate(() => CN.S.sheets.gold.pages.length);
    const idA = pid(A, 1), idB = pid(Bo, 2), idC = pid(C, 3), idD = pid(D, 4), idE = pid(E, 5);
    const lineOf = async (rid) => { const fl = await flowLines(page); return (fl && fl.lines.find(([, t]) => t.startsWith(`Order ${rid}:`))) || null; };

    /* 1 · step 2: a bigger design that does not fit the old spot, room elsewhere on the same open draft: stays on GF Sheet 1, nothing else moves, no nest */
    let c0 = await counts(page);
    { const r = await fix(page, A, 'A_BIG'); assert(r.ok, 'A relinked: ' + JSON.stringify(r)); }
    await until(() => settled(page), 30000, async () => 'A settled: ' + JSON.stringify(await flowLines(page)));
    let L = await layout(page), c = await counts(page);
    assert(L['i2-gold-1'][idA] && L['i2-gold-1'][idA][3] === 'A_BIG', 'A_BIG is on GF Sheet 1: ' + JSON.stringify(L));
    for (const sid of Object.keys(L0)) assert.deepEqual(without(L, sid, [idA]), without(L0, sid, [idA]), `every other charm on ${sid} is exactly where it was`);
    assert.equal(c.nests - c0.nests, 0, 'no nest');
    { const ov = await noOverlap(page); assert(ov === true, 'no two charms overlap, none runs off its sheet (the page\'s verifier): ' + ov); }
    { const ln = await lineOf(A); assert(ln && /new piece on GF Sheet 1 beside its old spot/.test(ln[1]), 'the panel: beside its old spot: ' + JSON.stringify(ln)); }
    pass('step 2: a bigger design that does not fit the old spot goes elsewhere on its own sheet (GF Sheet 1), nothing else moves, no nest; the line says "on GF Sheet 1 beside its old spot"');

    /* 2 · a sheet in a FORMED set still takes its own piece back (before: the set's sheet was skipped and the piece went to another sheet or a new one) */
    c0 = await counts(page); let Lp = L;
    { const r = await fix(page, Bo, 'B_BIG'); assert(r.ok, 'B relinked: ' + JSON.stringify(r)); }
    await until(() => settled(page), 30000, async () => 'B settled: ' + JSON.stringify(await flowLines(page)));
    L = await layout(page); c = await counts(page);
    assert(L['i2-gold-2'][idB] && L['i2-gold-2'][idB][3] === 'B_BIG', 'B_BIG stays on GF Sheet 2, in Set 1: ' + JSON.stringify(L));
    for (const sid of Object.keys(Lp)) assert.deepEqual(without(L, sid, [idB]), without(Lp, sid, [idB]), `every other charm on ${sid} is exactly where it was`);
    assert.equal(c.nests - c0.nests, 0, 'no nest');
    assert.equal(await page.evaluate(() => CN.S.sheets.gold.pages.length), pages0, 'no new sheet');
    // (its set membership then follows the set's own assembly, as after a hand edit: inPlaceRefile)
    { const ov = await noOverlap(page); assert(ov === true, 'no two charms overlap, none runs off its sheet (the page\'s verifier): ' + ov); }
    pass('a sheet in a formed set takes its own replaced piece back (beside its old spot): no new sheet, nothing else moves');

    /* 3 · step 3: the old sheet has no room: the latest unfinished sheet with room (GF Sheet 4), and the line says why with the sizes */
    c0 = await counts(page); Lp = L;
    { const r = await fix(page, C, 'C_BIG'); assert(r.ok, 'C relinked: ' + JSON.stringify(r)); }
    await until(() => settled(page), 30000, async () => 'C settled: ' + JSON.stringify(await flowLines(page)));
    L = await layout(page);
    assert(!L['i2-gold-3'][idC] && L['i2-gold-4'][idC] && L['i2-gold-4'][idC][3] === 'C_BIG', 'C_BIG went to GF Sheet 4, the latest unfinished sheet: ' + JSON.stringify(L));
    assert.deepEqual(without(L, 'i2-gold-3', [idC]), without(Lp, 'i2-gold-3', [idC]), 'nothing on GF Sheet 3 moved');
    assert.deepEqual(without(L, 'i2-gold-4', [idC]), Lp['i2-gold-4'], 'nothing already on GF Sheet 4 moved');
    assert.equal(await page.evaluate(() => CN.S.sheets.gold.pages.length), pages0, 'no new sheet');
    { const ln = await lineOf(C); assert(ln && /did not fit the old spot \(spot left empty on GF Sheet 3\) — GF Sheet 3 had no room for the new 16 mm design \(needs 16×16 mm, largest free area [\d.]+×[\d.]+ mm\): moved to the latest unfinished sheet, GF Sheet 4/.test(ln[1]), 'the panel says why, with the sizes: ' + JSON.stringify(ln)); }
    pass('step 3: an old sheet with no room sends the new piece to the latest unfinished sheet (GF Sheet 4), nothing on either sheet moves; the line says "GF Sheet 3 had no room for the new 16 mm design (needs 16×16 mm, largest free area …): moved to the latest unfinished sheet, GF Sheet 4"');

    /* 4 · step 4: no sheet has room: only then a new sheet */
    Lp = L;
    { const r = await fix(page, E, 'E_HUGE'); assert(r.ok, 'E relinked: ' + JSON.stringify(r)); }
    await until(() => settled(page), 30000, async () => 'E settled: ' + JSON.stringify(await flowLines(page)));
    L = await layout(page);
    const pagesNow = await page.evaluate(() => CN.S.sheets.gold.pages.map(p => ({ id: p.sheetId, pcs: p.charms.map(c => c.poolId) })));
    assert.equal(pagesNow.length, pages0 + 1, 'one new sheet was made: ' + JSON.stringify(pagesNow));
    assert(pagesNow[pagesNow.length - 1].pcs.includes(idE), 'E_HUGE is on the new sheet: ' + JSON.stringify(pagesNow));
    for (const sid of Object.keys(Lp)) assert.deepEqual(without(L, sid, [idE]), without(Lp, sid, [idE]), `every other charm on ${sid} is exactly where it was`);
    { const ln = await lineOf(E); assert(ln && /GF Sheet 3 had no room for the new 22 mm design .*: moved to a new sheet, .*\(no other sheet of this metal had room\)/.test(ln[1]), 'the panel says a new sheet, and why: ' + JSON.stringify(ln)); }
    pass('step 4: a new sheet is made only when no sheet has room (GF Sheet 4 is passed over: no place for the 61 pt shape there), and the line says so');

    /* 5 · a laser-cut sheet is never changed */
    { const r = await fix(page, D, 'D_NEW'); }
    await sleep(800);
    assert.deepEqual((await layout(page))['i2-gold-5'], L0['i2-gold-5'], 'nothing on the laser-cut GF Sheet 5 moved');
    assert(st.doc(POOL, idD).sku === 'D_OLD', 'D keeps its old piece');
    { const allN = await page.evaluate(() => window.__nests || []), allW = await page.evaluate(() => window.__writes || []); assert(!allN.concat(allW).some(x => x.sheetId === 'i2-gold-5'), 'GF Sheet 5 is never nested or written'); }
    pass('a laser-cut sheet is never changed: its piece stays, it is never nested or written');

    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`replace-in-place-ipl2: ${n} steps passed`);
})().catch(e => { console.error(e); process.exit(1); });
