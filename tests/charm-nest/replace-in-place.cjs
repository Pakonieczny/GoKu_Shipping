// INPLACE (Paul, 20 Oct 2026): "I just wanna make sure that you're not nesting the entire sheet because of one charm/charms change. If the new updated charm does not fit into the spot of the
// previously removed charm/charms then leave that particular spot empty and simply add the new charm/charms to the latest unfinished sheet." Real headless Chromium over the repo's fake site
// (bridge-server.cjs: the REAL charmNestLibrary handler on an in-memory Firestore; nothing leaves the machine). The stand-ins are the nest (startNest: it records every call, keeps every placement
// and puts a waiting piece into the next free cell) and the sheet's write as it stands (writeAsPlaced: it records every call and saves the sheet as it is). The fit check is the page's own
// (CNManual.fitsSpot: Manual Control's check, the nest's grid and verifier) over pieces drawn as solid squares.
// "Unless a sheet has been marked as fully completed and laser cut then that charm needs to come off that particular sheet and be re-added to the same sheet if it fits, and if it doesn't then
// add it to the next unfinished sheet." The stand-ins for a set's files are Sets.onSheetSaved (the sheet's QR label) and Sets.finalize (labels PDF, manifest, set.json): they record each call.
//   GF Sheet 1 (draft, open): E · F1 · Q (a pair of stud earrings: Left, Right) · A · F2      GF Sheet 2: LASER CUT (D, listing of A)      GF Sheet 3: the latest open draft (F3, F4, R)
//   GF Sheet 4: in Set 1, COMMITTED at the station but NOT cut (M, its own listing · F5)
//   (a) E → STAR_NEW, the same size: in E's old spot (same centre and angle), every other charm on every sheet exactly where it was, NO nest started at all, GF Sheet 1 written as it stands
//   (c) Q (a pair) → HEART_NEW, the same size: both new pieces (Left and Right) in the two old spots, together; nothing else moves, no nest
//   (b) A → MOON_BIG, too big for its spot: the spot is left EMPTY on GF Sheet 1 (written as it stands, nothing moved), the piece goes to the latest unfinished sheet, GF Sheet 3 (not the cut
//       GF Sheet 2, not the committed set's GF Sheet 4), appended: its nest is append-only (every placement held, no "making room") and nothing on it moves; the progress panel says so, with no
//       "nested again" line
//   (e) D, the same listing as A, on the LASER-CUT GF Sheet 2: keeps its old piece where it is, nothing of it written, the sheet never started or written; the panel says so (a note)
//   (d) M (on GF Sheet 4, a committed but uncut set) → COMET_NEW, the same size: its piece IS swapped, in the old spot, nothing else on the sheet moves, no nest; GF Sheet 4 written as it stands,
//       Set 1's files written again (the sheet's QR label, the set's labels PDF / manifest / set.json), the line committed again; the panel and the sheet's card say "Set 1 changed after it
//       was released — re-send its files"
//   (c2) R (a pair, on GF Sheet 3) → RING_BIG, too big: both pieces go TOGETHER to the latest unfinished sheet, both old spots stay empty
//   node tests/charm-nest/replace-in-place.cjs        (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(chromePath)) { console.log('  - no chrome: the browser checks were not run'); process.exit(0); }
const { start } = require('./bridge-server.cjs');

const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', RUN = 'run-inplace-1', SET_ID = 'set-inplace-1';
const LA = '1819300001', LE = '1819300002', LQ = '1819300003', LR = '1819300004', LM = '1819300005';
// the designs: [sku, side in pt] (a solid square)
const DESIGNS = { MOON_OLD: 8, MOON_BIG: 40, STAR_OLD: 8, STAR_NEW: 8, HEART_OLD: 8, HEART_NEW: 8, RING_OLD: 8, RING_BIG: 40, COMET_OLD: 8, COMET_NEW: 8, GFFILL_1: 8, GFFILL_2: 8, GFFILL_3: 8, GFFILL_4: 8, GFFILL_5: 8 };
const SHIP = Math.floor(Date.UTC(2026, 9, 24, 17) / 1000), BASE_TS = Math.floor(Date.UTC(2026, 9, 16, 12) / 1000);
const A = '4193300001', E = '4193300002', Q = '4193300003', D = '4193300004', M = '4193300005', R = '4193300006', F = n => '41933100' + String(n).padStart(2, '0');
const TID = tx => String(5100000000 + tx), pid = (rid, tx, n) => `${rid}_${TID(tx)}_${n || 1}`;
// [rid, tx, listing, sku, age (s), sheet page, [cells], quantity, a pair of stud earrings (a Left and a Right piece)]   (a cell: column on row 0 of its sheet, 30 pt apart)
const LINES = [[E, 1, LE, 'STAR_OLD', 9000, 1, [0], 1], [F(1), 2, '1700000301', 'GFFILL_1', 8900, 1, [1], 1], [Q, 3, LQ, 'HEART_OLD', 8800, 1, [2, 3], 1, true], [A, 4, LA, 'MOON_OLD', 8700, 1, [4], 1], [F(2), 5, '1700000302', 'GFFILL_2', 8600, 1, [5], 1],
  [D, 6, LA, 'MOON_OLD', 9500, 2, [0], 1], [F(3), 7, '1700000303', 'GFFILL_3', 7000, 3, [0], 1], [F(4), 8, '1700000304', 'GFFILL_4', 6900, 3, [1], 1], [R, 10, LR, 'RING_OLD', 6800, 3, [2, 3], 1, true],
  [M, 9, LM, 'COMET_OLD', 9600, 4, [0], 1], [F(5), 11, '1700000305', 'GFFILL_5', 9550, 4, [1], 1]];
const PAGES = { 1: { id: 'ip-gold-1', draft: true }, 2: { id: 'ip-gold-2', cut: true }, 3: { id: 'ip-gold-3', draft: true }, 4: { id: 'ip-gold-4', set: true } };
const sleep = ms => new Promise(r => setTimeout(r, ms));
let n = 0; const pass = m => { n++; console.log('  ✓ ' + m); };

(async () => {
  const srv = await start({ receipts: [] }), st = srv.st;
  for (const sku of Object.keys(DESIGNS)) st.put('Charm_Master_Index', sku, { sku, aiPath: 'test/' + sku + '.ai', widthPt: DESIGNS[sku], heightPt: DESIGNS[sku], holes: 0 }, false);
  for (const [pg, sh] of Object.entries(PAGES)) {
    const mine = LINES.filter(l => l[5] === +pg), cells = mine.flatMap(l => l[6].map((cell, k) => ({ l, cell, k })));
    st.put(SHEETS, sh.id, Object.assign({ id: sh.id, metal: 'gold', charms: cells.map((x, i) => ({ id: `${sh.id}-c${i}`, poolId: pid(x.l[0], x.l[1], x.k + 1), order: x.l[0], sku: x.l[3] })), placements: cells.map((x, i) => ({ id: `${sh.id}-c${i}`, cxPt: 16 + x.cell * 30, cyPt: 16, angle: 0 })), poolIds: cells.map(x => pid(x.l[0], x.l[1], x.k + 1)), sheetIndex: +pg, fileBase: 'GF Sheet ' + pg, runId: RUN }, sh.cut ? { laserDoneAt: Date.now() - 3600000 } : {}, sh.draft ? { draft: true } : {}, sh.set ? { setId: SET_ID } : {}), false);
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
    const TID = tx => String(5100000000 + tx), pid = (rid, tx, n) => `${rid}_${TID(tx)}_${n || 1}`;
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
        page.placements.push({ id, cxPt: 16 + x.cell * 30, cyPt: 16, angle: 0, wPt: DESIGNS[l[3]], hPt: DESIGNS[l[3]], scale: 0.975 });
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
    B.sets.set(`${RUN}|dispatch`, { setId: SET_ID, runId: RUN, seq: 1, day: new Date().toISOString().slice(0, 10), group: 'dispatch', name: 'Set 1', committedAt: Date.now() - 1800000, savedCommit: Date.now() - 1800000, sheetIds: [PAGES[4].id], materials: ['gold'], labelFiles: [], orders: {}, status: 'committed' });
    const rows = [];
    for (const l of LINES) {
      const [rid, tx, listingId, sku, age, pg, cells, qty, pair] = l;
      const line = { transactionId: TID(tx), listingId, sku, title: sku.replace(/_.*/, '').toLowerCase() + (pair ? ' charm stud earrings' : ' charm'), quantity: qty, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' };
      const order = { receiptId: rid, orderNumber: rid, createTs: BASE_TS - age, updateTs: BASE_TS, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-2) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [line] };
      const ids = cells.map((_, k) => pid(rid, tx, k + 1));
      rows.push({ key: CharmNestOrders.lineKey(order, line), order, line, arrivedAt: Date.now() - 7200000, spec: null, problems: [], state: pg === 4 ? 'committed' : 'pooled', reason: null, claimedBy: null, poolIds: ids, engrave: null, material: 'gold' });
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
  try {
    const { page } = await openPage('Paul');
    const read0 = await setup(page);
    assert(read0.every(r => r[3] !== 'held'), 'every line reads its design: ' + JSON.stringify(read0));
    const L0 = await layout(page), w0 = st.calls.length;
    const idE = pid(E, 1), idQ1 = pid(Q, 3, 1), idQ2 = pid(Q, 3, 2), idA = pid(A, 4), idD = pid(D, 6), idM = pid(M, 9), idR1 = pid(R, 10, 1), idR2 = pid(R, 10, 2);

    /* (a) the same size: in the old spot, nothing else moves, no nest at all */
    let c0 = await counts(page);
    { const r = await fix(page, E, 'STAR_NEW'); assert(r.ok, 'E is relinked to STAR_NEW: ' + JSON.stringify(r)); }
    await until(() => settled(page), 20000, async () => 'E settled: ' + JSON.stringify(await flowLines(page)));
    let L = await layout(page), c = await counts(page);
    assert.deepEqual(L['ip-gold-1'][idE], [16, 16, 0, 'STAR_NEW'], 'E\'s new piece is in E\'s old spot, at the same centre and angle: ' + JSON.stringify(L['ip-gold-1']));
    for (const sid of Object.keys(L0)) assert.deepEqual(without(L, sid, [idE]), without(L0, sid, [idE]), `every other charm on ${sid} is exactly where it was`);
    assert.equal(c.nests - c0.nests, 0, 'no nest was started: ' + JSON.stringify(await page.evaluate(n0 => (window.__nests || []).slice(n0), c0.nests)));
    const wE = await page.evaluate(n0 => (window.__writes || []).slice(n0), c0.writes);
    assert.deepEqual(wE.map(w => w.sheetId), ['ip-gold-1'], 'GF Sheet 1 alone was written as it stands: ' + JSON.stringify(wE.map(w => w.sheetId)));
    assert.equal(st.doc(POOL, idE).sku, 'STAR_NEW', 'E\'s piece record is STAR_NEW');
    assert(st.doc(SHEETS, 'ip-gold-1').poolIds.includes(idE) && st.doc(SHEETS, 'ip-gold-1').placements.some(p => p.cxPt === 16 && p.cyPt === 16), 'the saved GF Sheet 1 lists E in its spot');
    { const fl = await flowLines(page);
      assert(fl.lines.some(([s, t]) => s === 'done' && t.startsWith(`Order ${E}: new piece placed in the old spot on GF Sheet 1`)), 'the panel: E placed in the old spot: ' + JSON.stringify(fl.lines));
      assert(fl.lines.some(([s, t]) => s === 'done' && /^GF Sheet 1 files saved$/.test(t)), 'the panel: GF Sheet 1 files saved: ' + JSON.stringify(fl.lines));
      assert(!fl.lines.some(([, t]) => /nested again/.test(t)) && /none nested again/.test(fl.sum), 'no "nested again" line: ' + JSON.stringify(fl)); }
    pass('(a) a same-size new design goes into the old spot (same centre and angle), every other charm on every sheet stays exactly where it was, no nest is started, GF Sheet 1 is written as it stands');

    /* (c) a pair (Left and Right), the same size: both into the two old spots, together */
    c0 = await counts(page);
    { const r = await fix(page, Q, 'HEART_NEW'); assert(r.ok, 'Q is relinked: ' + JSON.stringify(r)); }
    await until(() => settled(page), 20000, async () => 'Q settled: ' + JSON.stringify(await flowLines(page)));
    const L1 = L; L = await layout(page); c = await counts(page);
    const spotsQ = [L['ip-gold-1'][idQ1], L['ip-gold-1'][idQ2]].map(x => x && x.slice(0, 3)).sort((a, b) => a[0] - b[0]);
    assert.deepEqual(spotsQ, [[76, 16, 0], [106, 16, 0]], 'Q\'s two new pieces are in Q\'s two old spots: ' + JSON.stringify({ L, q: await page.evaluate(Q => { const r = B.orders.rows.find(x => x.order.receiptId === Q); return { state: r.state, reason: r.reason, hold: r.hold, ids: r.poolIds, pool: r.poolIds.map(id => B.pool.rows.get(id)), spec: [r.spec.designSku, r.spec.skuSource, r.spec.pieceCount] }; }, Q), fl: await flowLines(page) }));
    assert(L['ip-gold-1'][idQ1][3] === 'HEART_NEW' && L['ip-gold-1'][idQ2][3] === 'HEART_NEW', 'both are HEART_NEW');
    { const sides = await page.evaluate(ids => ids.map(id => { const sh = CN.S.sheets.gold.pages.find(p => p.sheetId === 'ip-gold-1'), c = sh.charms.find(z => z.poolId === id) || {}; return (B.pool.rows.get(id) || {}).side || c.side || (c.orderInfo || {}).side || ''; }).sort(), [idQ1, idQ2]);
      assert.deepEqual(sides, ['L', 'R'], 'the two new pieces are the pair\'s Left and Right: ' + JSON.stringify(sides)); }
    for (const sid of Object.keys(L1)) assert.deepEqual(without(L, sid, [idQ1, idQ2]), without(L1, sid, [idQ1, idQ2]), `every other charm on ${sid} is exactly where it was`);
    assert.equal(c.nests - c0.nests, 0, 'no nest was started for Q');
    pass('(c) a pair of stud earrings: both new pieces (Left and Right) go into the two old spots together; nothing else moves, no nest');

    /* (b) too big for its spot: the spot left empty, the piece on the latest unfinished sheet, appended without moving anything */
    c0 = await counts(page);
    const L2 = L;
    { const r = await fix(page, A, 'MOON_BIG'); assert(r.ok, 'A is relinked to MOON_BIG: ' + JSON.stringify(r)); }
    await until(() => settled(page), 30000, async () => 'A settled: ' + JSON.stringify({ fl: await flowLines(page), st: await page.evaluate(() => CN.S.sheets.gold.pages.map(p => [p.sheetId, p.status, !!p._inplaceHold, !!p._inplaceWrite])) }));
    L = await layout(page); c = await counts(page);
    assert(!L['ip-gold-1'][idA], 'A is no longer on GF Sheet 1');
    assert(!Object.values(L['ip-gold-1']).some(([x, y]) => Math.hypot(x - 136, y - 16) < 20), 'A\'s old spot on GF Sheet 1 is left empty: ' + JSON.stringify(L['ip-gold-1']));
    assert.deepEqual(L['ip-gold-1'], without(L2, 'ip-gold-1', [idA]), 'every other charm on GF Sheet 1 is exactly where it was');
    assert(L['ip-gold-3'][idA] && L['ip-gold-3'][idA][3] === 'MOON_BIG', 'A\'s new piece is on GF Sheet 3, the latest unfinished sheet: ' + JSON.stringify(L));
    assert.deepEqual(without(L, 'ip-gold-3', [idA]), L2['ip-gold-3'], 'nothing already on GF Sheet 3 moved');
    for (const sid of ['ip-gold-2', 'ip-gold-4']) assert.deepEqual(L[sid], L0[sid], `${sid} is untouched`);
    const nB = await page.evaluate(n0 => (window.__nests || []).slice(n0), c0.nests), wB = await page.evaluate(n0 => (window.__writes || []).slice(n0), c0.writes);
    assert.deepEqual(nB.map(x => x.sheetId), ['ip-gold-3'], 'one nest, of GF Sheet 3 alone (GF Sheet 1 is never nested): ' + JSON.stringify(nB));
    assert(nB[0].appendOnly && nB[0].keepPlaced && nB[0].unjam === false && Object.keys(L2['ip-gold-3']).length === nB[0].locked.length, 'GF Sheet 3\'s nest is append-only: every placement held, no "making room": ' + JSON.stringify(nB[0]));
    assert.deepEqual(wB.map(w => w.sheetId), ['ip-gold-1'], 'GF Sheet 1 was written as it stands, with the spot empty: ' + JSON.stringify(wB.map(w => w.sheetId)));
    assert(!wB[0].charms.includes(idA), 'GF Sheet 1 was written without A');
    assert(!st.doc(SHEETS, 'ip-gold-1').poolIds.includes(idA) && st.doc(SHEETS, 'ip-gold-3').poolIds.includes(idA), 'the saved sheets: A off GF Sheet 1, on GF Sheet 3');
    const fl = await flowLines(page);
    const has = (state, re) => fl.lines.some(([s, t]) => s === state && re.test(t));
    assert(has('done', new RegExp(`^Order ${A}: did not fit the old spot \\(spot left empty on GF Sheet 1\\) — added to GF Sheet 3`)), 'the panel: A did not fit, spot left empty, added to GF Sheet 3: ' + JSON.stringify(fl.lines));
    assert(has('done', /^GF Sheet 1 files saved$/) && has('done', /^GF Sheet 3 files saved \(the new piece added; nothing on it moved\)$/), 'the panel: both sheets\' files saved: ' + JSON.stringify(fl.lines));
    assert(!fl.lines.some(([, t]) => /nested again/.test(t)) && /none nested again/.test(fl.sum) && !fl.lines.some(([s]) => s === 'failed'), 'no "nested again" line, nothing failed: ' + JSON.stringify(fl));
    pass('(b) a bigger new design that cannot fit: its old spot is left empty on GF Sheet 1 (written as it stands, nothing moved, never nested), the piece is appended to the latest unfinished sheet GF Sheet 3 (append-only, nothing on it moves); the panel says so with no "nested again" line');

    /* (e) the laser-cut sheet: untouched, with a note */
    assert.equal(st.calls.slice(w0).filter(x => /^(poolPut|poolUpdate)$/.test(String(x.op)) && JSON.stringify(x.body).includes(idD)).length, 0, 'no piece record of D (laser cut) is written');
    assert(st.doc(POOL, idD).sku === 'MOON_OLD', 'D keeps its old piece');
    { const allN = await page.evaluate(() => window.__nests || []), allW = await page.evaluate(() => window.__writes || []);
      assert(!allN.concat(allW).some(x => x.sheetId === 'ip-gold-2'), 'the laser-cut GF Sheet 2 is never nested or written'); }
    assert.deepEqual(L['ip-gold-2'], L0['ip-gold-2'], 'nothing on GF Sheet 2 moved');
    { const ln = fl.lines.find(([s, t]) => s === 'warn' && new RegExp(`^Order ${D}: its piece stays MOON_OLD on GF Sheet 2`).test(t));
      assert(ln, 'the panel notes that D keeps its piece on the cut sheet: ' + JSON.stringify(fl.lines));
      const why = await page.evaluate(D => { const li = [...document.querySelectorAll('.rlFlow li')].find(x => x.querySelector('.rlT').textContent.startsWith('Order ' + D)); return li ? li.textContent : ''; }, D);
      assert(/laser cut/.test(why), 'the note says why: laser cut: ' + why); }
    pass('(e) a laser-cut sheet keeps its piece: nothing of it is written, nested or moved, and the panel says so (laser cut)');

    /* (d) a sheet in a committed but UNCUT set: changed in place, the set's files written again, the change flagged */
    c0 = await counts(page);
    const L4 = L, w4 = await page.evaluate(() => ({ labels: window.__labels.length, finals: window.__finals.length }));
    { const r = await fix(page, M, 'COMET_NEW'); assert(r.ok, 'M is relinked to COMET_NEW: ' + JSON.stringify(r)); }
    await until(() => settled(page), 30000, async () => 'M settled: ' + JSON.stringify({ fl: await flowLines(page), st: await page.evaluate(() => CN.S.sheets.gold.pages.map(p => [p.sheetId, p.status, !!p._inplaceHold, !!p._inplaceWrite])) }));
    L = await layout(page); c = await counts(page);
    assert.deepEqual(L['ip-gold-4'][idM], [16, 16, 0, 'COMET_NEW'], 'M\'s new piece is in M\'s old spot on GF Sheet 4 (the committed set\'s sheet changed): ' + JSON.stringify({ L4: L['ip-gold-4'], fl: await flowLines(page), m: await page.evaluate(M => { const r = B.orders.rows.find(x => x.order.receiptId === M); return { state: r.state, reason: r.reason, hold: r.hold }; }, M) }));
    for (const sid of Object.keys(L4)) assert.deepEqual(without(L, sid, [idM]), without(L4, sid, [idM]), `every other charm on ${sid} is exactly where it was`);
    assert.equal(c.nests - c0.nests, 0, 'no nest was started for M');
    { const wM = await page.evaluate(n0 => (window.__writes || []).slice(n0), c0.writes); assert.deepEqual(wM.map(w => w.sheetId), ['ip-gold-4'], 'GF Sheet 4 alone was written as it stands: ' + JSON.stringify(wM.map(w => w.sheetId))); }
    assert.equal(st.doc(POOL, idM).sku, 'COMET_NEW', 'M\'s piece record is COMET_NEW');
    { const f = await page.evaluate(() => ({ labels: window.__labels.slice(), finals: window.__finals.slice() }));
      assert(f.labels.slice(w4.labels).some(x => x.sheetId === 'ip-gold-4' && x.labelsOnly && x.setId === 'set-inplace-1'), 'GF Sheet 4\'s QR label is made again for Set 1 (labels only: the orders stay committed): ' + JSON.stringify(f.labels));
      assert(f.finals.slice(w4.finals).some(x => x.setId === 'set-inplace-1' && x.committedAt), 'Set 1\'s labels PDF, manifest and set.json are written again: ' + JSON.stringify(f.finals)); }
    { const s = await page.evaluate(({ M, RUN }) => { const r = B.orders.rows.find(x => x.order.receiptId === M), set = Sets.ofRun(RUN).find(x => x.setId === 'set-inplace-1'), sh = CN.S.sheets.gold.pages.find(p => p.sheetId === 'ip-gold-4'); if (CN.showPage) CN.showPage('gold', CN.S.sheets.gold.pages.indexOf(sh)); CN.renderCard(sh); return { state: r.state, pool: r.poolIds.map(id => (B.pool.rows.get(id) || {}).state), setStatus: set.status, committedAt: !!set.committedAt, mark: sh.releasedChange, card: (document.querySelector('[data-released-change]') || {}).textContent || '' }; }, { M, RUN });
      assert.equal(s.state, 'committed', 'M\'s line is committed again (its new piece is back on its committed set\'s sheet): ' + JSON.stringify(s));
      assert(s.committedAt && s.setStatus === 'committed', 'Set 1 stays committed: ' + JSON.stringify(s));
      assert(s.mark && s.mark.setId === 'set-inplace-1' && /^Set 1 changed after it was released — re-send its files$/.test(s.mark.word), 'GF Sheet 4 carries the mark: ' + JSON.stringify(s.mark));
      assert(/Set 1 changed after it was released — re-send its files/.test(s.card), 'the sheet\'s card says so: ' + JSON.stringify(s.card));
      await until(() => page.evaluate(ids => ids.every(id => (B.pool.rows.get(id) || {}).state === 'committed'), [idM]), 5000, 'M\'s piece record committed again'); }
    { const fl3 = await flowLines(page), has3 = (state, re) => fl3.lines.some(([s, t]) => s === state && re.test(t));
      assert(has3('done', new RegExp(`^Order ${M}: new piece placed in the old spot on GF Sheet 4`)), 'the panel: M placed in the old spot on GF Sheet 4: ' + JSON.stringify(fl3.lines));
      assert(has3('done', /^GF Sheet 4 files saved$/), 'the panel: GF Sheet 4 files saved: ' + JSON.stringify(fl3.lines));
      assert(has3('warn', /^Set 1 changed after it was released — re-send its files/), 'the panel flags the released set: ' + JSON.stringify(fl3.lines));
      assert(/re-send its files/.test(fl3.sum) && /none nested again/.test(fl3.sum) && !fl3.lines.some(([s]) => s === 'failed'), 'the summary says so, nothing failed: ' + JSON.stringify(fl3)); }
    pass('(d) a sheet in a committed but uncut set changes: the piece is swapped in its old spot (nothing else moves, no nest), the sheet written as it stands, Set 1\'s files written again, the line committed again, and the panel and the card say "Set 1 changed after it was released — re-send its files"');

    /* (c2) a pair too big for its spots: both go together to the latest unfinished sheet, both spots left empty */
    c0 = await counts(page);
    const L3 = L;
    { const r = await fix(page, R, 'RING_BIG'); assert(r.ok, 'R is relinked: ' + JSON.stringify(r)); }
    await until(() => settled(page), 30000, async () => 'R settled: ' + JSON.stringify(await flowLines(page)));
    L = await layout(page);
    const whereR = [idR1, idR2].map(id => Object.keys(L).find(sid => L[sid][id]));
    assert(whereR[0] && whereR[0] === whereR[1], 'R\'s two pieces are on one sheet, together: ' + JSON.stringify(whereR));
    assert(![76, 106].some(x => Object.values(L['ip-gold-3']).some(([cx, cy]) => Math.hypot(cx - x, cy - 16) < 20)), 'R\'s two old spots on GF Sheet 3 are left empty: ' + JSON.stringify(L['ip-gold-3']));
    for (const sid of Object.keys(L3)) for (const [id, at] of Object.entries(L3[sid])) if (![idR1, idR2].includes(id)) assert.deepEqual(L[sid][id], at, `${id} on ${sid} did not move`);
    const fl2 = await flowLines(page);
    assert(fl2.lines.some(([s, t]) => s === 'done' && new RegExp(`^Order ${R}: did not fit the old spot \\(spot left empty on GF Sheet 3\\) — added to GF Sheet 3`).test(t)), 'the panel: R did not fit its spots, added to the latest unfinished sheet (its own): ' + JSON.stringify(fl2.lines));
    pass('(c2) a pair too big for its spots: both pieces go together to the latest unfinished sheet, nothing already placed moves');

    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`replace-in-place: ${n} steps passed`);
})().catch(e => { console.error(e); process.exit(1); });
