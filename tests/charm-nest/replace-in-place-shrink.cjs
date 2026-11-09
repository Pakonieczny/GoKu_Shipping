// INPLACE SHRINK (Paul, 9 Nov 2026): "This has to be checked that the actual new piece fits in the same place as the old piece came off and if need be, it can be shrunk by up to 10% just to fill
// it in such cases." Real headless Chromium over the repo's fake site (bridge-server.cjs: the REAL charmNestLibrary handler on an in-memory Firestore; nothing leaves the machine), with the same
// stand-ins as replace-in-place.cjs (the nest and the write as it stands record their calls). The fit check is the page's own (CNManual.fitsSpot: Manual Control's check, the nest's grid and the
// independent verifier) over pieces drawn as solid squares, the shop's clearance 0.
//   GF Sheet 1 (a draft): a row of 60 pt squares, each order's old piece in a room 60.1 pt wide between two fillers (Q's second room is 66 pt)      GF Sheet 2: the latest unfinished draft
//   (a) A → 5 % larger (63 pt): not at full size, so the SAME old spot again shrunk: about 95 %, every other charm exactly where it was, no nest; the line note says "shrunk to 95% to fit"
//   (b) B → 9 % larger: about 91 %
//   (c) C → 15 % larger: never shrunk (it would need 87 %): it goes on at FULL size, not in its old spot
//   (d) E → the same size: stays at 100 %, no size on it
//   (e) Q, a pair (Left, Right) → 5 % larger: both pieces shrunk by the SAME size (one room would take a piece at full size), or none
//   (f) the size reaches the record (placement.scale, the charm's manual { size, fit: "inplace" }), a reload (fromRecord), the laser export (sheetSpec), the sheet's preview / thumbnail,
//       the engraving composed on the piece (CharmNestExport.parentForm at the piece's scale) and the size words ("95% · 19.1 × 19.1 mm")
//   node tests/charm-nest/replace-in-place-shrink.cjs        (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>)
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
if (!fs.existsSync(chromePath)) { console.log('  - no chrome: the browser checks were not run'); process.exit(0); }
const { start } = require('./bridge-server.cjs');

const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', RUN = 'run-ipshrink-1';
const LA = '1819400001', LB = '1819400002', LC = '1819400003', LE = '1819400004', LQ = '1819400005';
// the designs: [sku, side in pt] (a solid square)
const DESIGNS = { OLD_60: 60, NEW_63: 63, NEW_654: 65.4, NEW_69: 69, NEW_60: 60, PAIR_63: 63, FILL: 60 };
const SHIP = Math.floor(Date.UTC(2026, 10, 14, 17) / 1000), BASE_TS = Math.floor(Date.UTC(2026, 10, 6, 12) / 1000);
const A = '4194400001', B = '4194400002', C = '4194400003', E = '4194400004', Q = '4194400005', F = n => '41944100' + String(n).padStart(2, '0');
const TID = tx => String(5200000000 + tx), pid = (rid, tx, n) => `${rid}_${TID(tx)}_${n || 1}`;
// GF Sheet 1, one row at cy 40: fillers 60 pt wide, each old piece in a room ROOM wide between two of them (x: its centre). GF Sheet 2: one filler.
const ROOM = 60.1, CY = 40;
const xs = (() => { const out = []; let x = 2; const put = (kind, w) => { out.push([kind, x + w / 2]); x += w; }; put('F', 60); for (const k of ['A', 'B', 'C', 'E', 'Q1']) { put(k, ROOM); put('F', 60); } put('Q2', 66); put('F', 60); return out; })();
const at = k => xs.find(x => x[0] === k)[1], fills = xs.filter(x => x[0] === 'F').map(x => x[1]);
// [rid, tx, listing, sku, age (s), sheet page, [centres], quantity, a pair of stud earrings]
const LINES = [[A, 1, LA, 'OLD_60', 9000, 1, [at('A')], 1], [B, 2, LB, 'OLD_60', 8900, 1, [at('B')], 1], [C, 3, LC, 'OLD_60', 8800, 1, [at('C')], 1], [E, 4, LE, 'OLD_60', 8700, 1, [at('E')], 1],
  [Q, 5, LQ, 'OLD_60', 8600, 1, [at('Q1'), at('Q2')], 1, true],
  ...fills.map((x, i) => [F(i + 1), 10 + i, String(1700000400 + i), 'FILL', 8000 - i, 1, [x], 1]), [F(20), 30, '1700000420', 'FILL', 7000, 2, [32], 1]];
const PAGES = { 1: { id: 'ips-gold-1', draft: true }, 2: { id: 'ips-gold-2', draft: true } };
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
        const put = sh => api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, runId: sh.runId, fileBase: sh.fileBase, sheetIndex: sh.sheetIndex, draft: sh.draft || undefined, charms: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => ({ id: c.id, poolId: c.poolId, order: c.order, sku: (c.orderInfo && c.orderInfo.sku) || c.sku, ...(c.manual ? { manual: c.manual } : {}) })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.filter(c => sh.placements.some(p => p.id === c.id)).map(c => c.poolId) } }, { quiet: true });
        // the nest: what it was asked with (append-only? every placement held? "making room" off?), placed pieces kept, a waiting piece into the next free cell (row 1), the sheet saved
        const stub = sh => {
          let job = null; try { job = buildJob(sh); } catch (_) { job = null; }
          window.__nests = (window.__nests || []).concat([{ sheetId: sh.sheetId, page: sh.page, appendOnly: !!sh.appendOnly, keepPlaced: !!sh._keepPlaced, unjam: job ? !!job.unjam : null, locked: job && job.lockedPlacements ? job.lockedPlacements.map(p => p.id) : null, placed: sh.placements.map(p => p.id), charms: sh.charms.map(c => c.poolId) }]);
          sh.status = 'nesting'; sh.persistedDone = false; sh.persisted = Promise.resolve(); sh.stage = 'test nest';
          setTimeout(async () => {
            try {
              sh.placements = sh.placements.filter(p => sh.charms.some(c => c.id === p.id));
              const cells = []; for (let col = 0; col < 8; col++) cells.push([40 + col * 75, 150]);
              const free = cells.filter(([x, y]) => !sh.placements.some(p => Math.hypot(p.cxPt - x, p.cyPt - y) < 70));
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
          window.__writes = (window.__writes || []).concat([{ sheetId: sh.sheetId, page: sh.page, placements: sh.placements.map(p => ({ id: p.id, cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle, scale: p.scale })), charms: sh.charms.map(c => c.poolId) }]);
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
  const setup = page => page.evaluate(async ({ LINES, PAGES, RUN, BASE_TS, SHIP, DESIGNS, CY }) => {
    await Orders.loadMaps(true);
    CN.S.stockPreset = 'custom'; CN.S.settings.stock = Object.assign({}, CN.S.settings.stock, { gold: [820 / 72, 220 / 72] });
    CN.S.settings.clearancePt = 0;   // (the shop's clearance 0: the room each old piece left is exactly what it is)
    const TID = tx => String(5200000000 + tx), pid = (rid, tx, n) => `${rid}_${TID(tx)}_${n || 1}`;
    // each design a solid square of its own side, drawn at 6 px/pt
    const sq = (x0, y0, x1, y1) => ({ kind: 'path', closed: true, stroke: true, fill: false, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [x0, y0, x1, y1], subpaths: [[['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']]] });
    const mkCharm = (id, sku, extra) => { const side = DESIGNS[sku], px = Math.round(side * 6), outline = sq(0, 0, side, side); return Object.assign({ id, name: id, sku, sourceId: 's', ringGeometryVersion: 3, centerPt: [side / 2, side / 2], bbox: [0, 0, side, side], outline, members: [outline], rMm: side / 2 * 25.4 / 72, w: px, h: px, scale: px / side, bits: new Uint8Array(px * px).fill(1), widthPt: side, heightPt: side, areaPt2: side * side, hash: 'h-' + sku }, extra || {}); };
    const reg = sku => { const key = 'test/' + sku + '.ai'; B.master.entries.set(sku, { sku, updatedAt: 1, aiPath: key, widthPt: DESIGNS[sku], heightPt: DESIGNS[sku], holes: 0, upAngle: null, labelSource: 'text', charmHash: 'h-' + sku }); B.pool.sources.set(key, { id: 'pool:' + sku, pool: true, name: sku, sku, charms: [mkCharm('pool:' + sku + ':0', sku, { name: sku })], bodiesChecked: true, mirrors: [mkCharm('pool:' + sku + ':0m', sku, { name: sku, mirror: true, bodyIndex: 0 })], usedAt: Date.now() }); };
    for (const sku of Object.keys(DESIGNS)) reg(sku);
    const pr = CN.S.sheets.gold; pr.pages.length = 1; const p0 = pr.pages[0]; Object.assign(p0, { charms: [], placements: [], sheetId: null, fileBase: null, status: 'idle', page: 1 }); for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId', 'releaseFull']) delete p0[k];
    B.pool.rows.clear();
    for (const [pg, sh] of Object.entries(PAGES)) {
      while (pr.pages.length < +pg) addPage('gold');
      const page = pr.pages[+pg - 1]; page.charms = []; page.placements = [];
      const cells = LINES.filter(l => l[5] === +pg).flatMap(l => l[6].map((cx, k) => ({ l, cx, k })));
      cells.forEach((x, i) => {
        const l = x.l, id = `${sh.id}-c${i}`, poolId = pid(l[0], l[1], x.k + 1);
        page.charms.push(mkCharm(id, l[3], { name: `${l[0]} · ${l[3]}`, poolId, order: l[0], lineKey: `${l[0]}_${TID(l[1])}`, orderDate: BASE_TS - l[4], orderInfo: Object.assign({ receiptId: l[0], transactionId: TID(l[1]), sku: l[3], copy: x.k + 1, quantity: l[7] }, l[8] ? { side: x.k ? 'R' : 'L' } : {}) }));
        page.placements.push({ id, cxPt: x.cx, cyPt: CY, angle: 0, wPt: DESIGNS[l[3]], hPt: DESIGNS[l[3]], scale: CHARM_SCALE });
      });
      Object.assign(page, { status: 'complete', sheetId: sh.id, fileBase: 'GF Sheet ' + pg, sheetIndex: +pg, page: +pg, runId: RUN, dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, density: .3, verification: { ok: true }, intakeAppend: false, appendOnly: false, draft: true, setId: null });
      CN.renderCard(page);
    }
    window.__labels = []; window.__finals = [];
    Sets.onSheetSaved = async (sh, items, outputs, opt) => { window.__labels.push({ sheetId: sh.sheetId }); };
    Sets.finalize = async set => { window.__finals.push({ setId: set.setId }); return set; };
    B.sets.clear();
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
    B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: null, setIds: [], releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [...new Set(rows.map(r => r.order.receiptId))] };
    Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('orders'); Orders.render();
    if (!B.employee) B.employee = 'Paul';
    return rows.map(r => [r.order.receiptId, r.spec.designSku, r.spec.skuSource, r.state]);
  }, { LINES, PAGES, RUN, BASE_TS, SHIP, DESIGNS, CY });
  const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + (typeof what === 'function' ? await what() : what)); await sleep(150); } };
  // every sheet as it stands: { sheetId: { poolId: [cx, cy, angle, sku, scale, size] } } (size: the charm's own, manualSize)
  const layout = page => page.evaluate(() => Object.fromEntries(CN.S.sheets.gold.pages.map(p => [p.sheetId, Object.fromEntries(p.placements.map(pl => { const c = p.charms.find(z => z.id === pl.id) || {}; return [c.poolId, [+(+pl.cxPt).toFixed(3), +(+pl.cyPt).toFixed(3), +pl.angle || 0, (c.orderInfo && c.orderInfo.sku) || c.sku, +pl.scale || 0, manualSize(c)]]; }))])));
  const settled = page => page.evaluate(() => CN.S.sheets.gold.pages.every(p => !['nesting', 'finishing', 'queued', 'ready'].includes(p.status) && !p._inplaceWrite && !p._inplaceHold) && !!document.querySelector('.rlFlow[data-done="1"]') || false);
  const counts = page => page.evaluate(() => ({ nests: (window.__nests || []).length, writes: (window.__writes || []).length }));
  const fix = (page, rid, sku) => page.evaluate(async ({ rid, sku }) => { document.querySelectorAll('.rlFlow').forEach(x => x.remove()); const row = B.orders.rows.find(r => r.order.receiptId === rid); const r = await Review.fixCharm(row, sku, { confirm: () => true }); return { ok: r.ok, why: r.why || '' }; }, { rid, sku });
  const flowLines = page => page.evaluate(() => { const el = document.querySelector('.rlFlow'); return el ? { sum: el.querySelector('.rlSum').textContent, lines: [...el.querySelectorAll('li')].map(li => [li.dataset.s, li.querySelector('.rlT').textContent]) } : null; });
  const agentSays = (page, rid) => page.evaluate(rid => AG.events.filter(e => String(e.text || '').startsWith(rid + ' · ') && /old spot/.test(e.text)).map(e => e.text), rid);   // (the agent log)
  const without = (L, sheet, ids) => { const o = Object.assign({}, L[sheet]); for (const id of ids) delete o[id]; return o; };
  const near = (v, want, tol) => Math.abs(v - want) <= tol;
  const S0 = 0.971;   // (CHARM_SCALE: the written shrink every placed charm has)
  try {
    const { page } = await openPage('Paul');
    const read0 = await setup(page);
    assert(read0.every(r => r[3] !== 'held'), 'every line reads its design: ' + JSON.stringify(read0));
    assert.equal(await page.evaluate(() => CHARM_SCALE), S0, 'the written shrink is 0.971');
    const L0 = await layout(page);
    const idA = pid(A, 1), idB = pid(B, 2), idC = pid(C, 3), idE = pid(E, 4), idQ1 = pid(Q, 5, 1), idQ2 = pid(Q, 5, 2);
    const runOne = async (rid, sku, what) => {
      const c0 = await counts(page), Lb = await layout(page);
      const r = await fix(page, rid, sku); assert(r.ok, `${rid} is relinked to ${sku}: ` + JSON.stringify(r));
      await until(() => settled(page), 30000, async () => `${what} settled: ` + JSON.stringify(await flowLines(page)));
      return { c0, Lb, L: await layout(page), c: await counts(page), fl: await flowLines(page) };
    };

    /* (a) 5 % larger: not at full size; the SAME spot again, shrunk: about 95 % */
    { const { c0, Lb, L, c, fl } = await runOne(A, 'NEW_63', 'A');
      const a = L['ips-gold-1'][idA];
      assert(a && a[3] === 'NEW_63', 'A\'s new piece is on GF Sheet 1: ' + JSON.stringify({ a, fl }));
      assert(Math.abs(a[0] - at('A')) <= 2 && Math.abs(a[1] - CY) <= 2, 'in A\'s old spot (within the 2 pt nudge): ' + JSON.stringify(a));
      assert(near(a[5], 0.95, 0.011), 'shrunk to about 95 %: ' + a[5]);
      assert(a[5] >= 0.9 && a[5] < 1, 'never more than 10 %');
      assert(near(a[4], +(S0 * a[5]).toFixed(5), 1e-6), 'placement.scale is the written shrink of that size: ' + a[4]);
      for (const sid of Object.keys(Lb)) assert.deepEqual(without(L, sid, [idA]), without(Lb, sid, [idA]), `every other charm on ${sid} is exactly where it was`);
      assert.equal(c.nests - c0.nests, 0, 'no nest was started');
      const w = await page.evaluate(n0 => (window.__writes || []).slice(n0), c0.writes);
      assert.deepEqual(w.map(x => x.sheetId), ['ips-gold-1'], 'GF Sheet 1 alone was written as it stands');
      assert(near(w[0].placements.find(p => Math.abs(p.cxPt - a[0]) < 1e-6).scale, a[4], 1e-9), 'the write carries the piece\'s scale');
      const pct = Math.round(a[5] * 100) + '%';
      assert(fl.lines.some(([s, t]) => s === 'done' && t.startsWith(`Order ${A}: new piece placed in the old spot on GF Sheet 1, shrunk to ${pct} to fit`)), 'the progress panel says it: ' + JSON.stringify(fl.lines));
      // (the order line's note, on its timeline: "the new piece was placed in the old spot on GF Sheet 1, shrunk to 95% to fit")
      await until(() => st.list('Order_Timeline').some(e => e._id.startsWith(A + '~') && (e.text || '').includes(`shrunk to ${pct} to fit`)), 5000, async () => 'A\'s line note: ' + JSON.stringify(st.list('Order_Timeline').filter(e => e._id.startsWith(A + '~')).map(e => e.text)));
      const log = await agentSays(page, A);
      assert(log.some(t => /went back into the old spot on .*sheet 1, shrunk/.test(t) && t.includes(`shrunk to ${pct} to fit`)) && log.some(t => t.includes(`new piece placed in the old spot on GF Sheet 1, shrunk to ${pct} to fit`)), 'the agent log says it: ' + JSON.stringify(log));
      // (the saved record: placement.scale and the charm's manual size, "inplace")
      const rec = st.doc(SHEETS, 'ips-gold-1'), rc = rec.charms.find(x => x.poolId === idA), rp = rec.placements.find(p => p.id === rc.id);
      assert(rc.manual && rc.manual.fit === 'inplace' && near(rc.manual.size, a[5], 1e-9), 'the saved charm carries its size: ' + JSON.stringify(rc));
      assert(near(rp.scale, a[4], 1e-9), 'the saved placement carries its scale: ' + JSON.stringify(rp));
      pass(`(a) a redraw 5% larger does not fit its old spot at full size, so it goes into the SAME spot shrunk to ${pct} (placement.scale ${a[4]}); nothing else on the sheet moves, no nest; the panel, the line and the log say "shrunk to ${pct} to fit"`); }

    /* (b) 9 % larger: about 91 % */
    { const { Lb, L } = await runOne(B, 'NEW_654', 'B');
      const b = L['ips-gold-1'][idB];
      assert(b && b[3] === 'NEW_654' && Math.abs(b[0] - at('B')) <= 2, 'B\'s new piece is in its old spot: ' + JSON.stringify(b));
      assert(near(b[5], 0.91, 0.011) && b[5] >= 0.9, 'shrunk to about 91 %: ' + b[5]);
      for (const sid of Object.keys(Lb)) assert.deepEqual(without(L, sid, [idB]), without(Lb, sid, [idB]), `every other charm on ${sid} is exactly where it was`);
      pass(`(b) a redraw 9% larger goes into its old spot shrunk to ${Math.round(b[5] * 100)}%; nothing else moves`); }

    /* (c) 15 % larger: never shrunk, it goes on at full size elsewhere */
    { const { Lb, L, fl } = await runOne(C, 'NEW_69', 'C');
      const where = Object.keys(L).find(sid => L[sid][idC]), c = where && L[where][idC];
      assert(c && c[3] === 'NEW_69', 'C\'s new piece is on a sheet: ' + JSON.stringify({ L, fl }));
      assert(!(where === 'ips-gold-1' && Math.abs(c[0] - at('C')) < 10 && Math.abs(c[1] - CY) < 10), 'not in its old spot: ' + JSON.stringify([where, c]));
      assert.equal(c[5], 1, 'not shrunk: its size is its design size');
      assert(!c[4] || near(c[4], S0, 1e-9), 'its scale is the plain written shrink (or not written yet): ' + c[4]);
      assert(!Object.values(L['ips-gold-1']).some(([x, y]) => Math.abs(x - at('C')) < 20 && Math.abs(y - CY) < 20), 'C\'s old spot is left empty');
      for (const [id, v] of Object.entries(Lb['ips-gold-1'])) if (id !== idC) assert.deepEqual(L['ips-gold-1'][id], v, `${id} on GF Sheet 1 did not move`);
      assert(fl.lines.some(([s, t]) => s === 'done' && t.startsWith(`Order ${C}: did not fit the old spot`) && !/shrunk/.test(t)), 'the panel: it did not fit its old spot, nothing said of a shrink: ' + JSON.stringify(fl.lines));
      assert(!st.list('Order_Timeline').some(e => e._id.startsWith(C + '~') && /shrunk/.test(e.text || '')), 'its line note says nothing of a shrink');
      pass(`(c) a redraw 15% larger is NOT shrunk (it would need 87%): it leaves its old spot empty and goes on at full size (${where})`); }

    /* (d) the same size: stays at 100 % */
    { const { L } = await runOne(E, 'NEW_60', 'E');
      const e = L['ips-gold-1'][idE];
      assert(e && e[3] === 'NEW_60' && Math.abs(e[0] - at('E')) <= 2, 'E\'s new piece is in its old spot: ' + JSON.stringify(e));
      assert.equal(e[5], 1, 'at full size'); assert(near(e[4], S0, 1e-9), 'its scale is the plain written shrink: ' + e[4]);
      const rc = st.doc(SHEETS, 'ips-gold-1').charms.find(x => x.poolId === idE);
      assert(!rc.manual, 'no size on its saved charm: ' + JSON.stringify(rc));
      const fl = await flowLines(page); assert(fl.lines.some(([s, t]) => s === 'done' && t.startsWith(`Order ${E}: new piece placed in the old spot on GF Sheet 1`) && !/shrunk/.test(t)), 'the panel: plain words, no shrink: ' + JSON.stringify(fl.lines));
      pass('(d) a same-size redraw goes back into its old spot at 100%, with no size and no "shrunk" words'); }

    /* (e) a pair: both pieces by the same size */
    { const { Lb, L } = await runOne(Q, 'PAIR_63', 'Q');
      const q = [idQ1, idQ2].map(id => L['ips-gold-1'][id]);
      assert(q.every(x => x && x[3] === 'PAIR_63'), 'both of Q\'s new pieces are on GF Sheet 1: ' + JSON.stringify(q));
      const spots = q.map(x => x[0]).sort((a, b) => a - b);
      assert(Math.abs(spots[0] - at('Q1')) <= 2 && Math.abs(spots[1] - at('Q2')) <= 2, 'in Q\'s two old spots: ' + JSON.stringify(spots));
      assert.equal(q[0][5], q[1][5], 'both at the SAME size: ' + JSON.stringify(q));
      assert(near(q[0][5], 0.95, 0.011), 'about 95 % (one room alone would take a piece at full size): ' + q[0][5]);
      assert(near(q[0][4], q[1][4], 1e-9), 'the same scale on both placements');
      for (const sid of Object.keys(Lb)) assert.deepEqual(without(L, sid, [idQ1, idQ2]), without(Lb, sid, [idQ1, idQ2]), `every other charm on ${sid} is exactly where it was`);
      pass(`(e) a pair (Left and Right) shrinks together: both at ${Math.round(q[0][5] * 100)}%, though one room alone would take a piece at full size`); }

    /* (f) the size reaches the record, a reload, the laser export, the preview, the engraving and the words */
    { const r = await page.evaluate(({ idA, S0 }) => {
        const sh = CN.S.sheets.gold.pages.find(p => p.sheetId === 'ips-gold-1'), c = sh.charms.find(z => z.poolId === idA), pl = sh.placements.find(p => p.id === c.id), byId = new Map(sh.charms.map(z => [z.id, z]));
        // the laser export's sheet (AI / PDF / DXF are written from it): sheetSpec, each placement at its scale
        const ps = S.poolSources || (S.poolSources = {}), had = ps.s; let spec; try { if (!sourceOf('s')) ps.s = { id: 's', parsed: null }; spec = sheetSpec(sh, byId, stockFor('gold', sh), { params: { seed: 1, clearancePt: 0 } }); } finally { if (had === undefined) delete ps.s; }
        const exp = spec.placements.find(p => p.charm === c);
        // the sheet's preview (the card, and the preview.png thumbnail the Library shows): every charm drawn at its placement's scale
        const seen = [], sc = CanvasRenderingContext2D.prototype.scale; CanvasRenderingContext2D.prototype.scale = function (x, y) { seen.push(+x); return sc.call(this, x, y); };
        try { const cv = document.createElement('canvas'); cv.width = 1026; cv.height = 280; paintPreview(cv, sh, true); } finally { CanvasRenderingContext2D.prototype.scale = sc; }
        // the engraving composed on the piece in the laser's files: its parent form is drawn at the piece's own scale, and a file at the plain scale is refused
        const rec = { charms: [{ id: c.id, poolId: idA }], placements: [{ id: c.id, scale: pl.scale }] }, form = s => ({ segments: [{ kind: 'xobj', matrix: [s, 0, 0, s, 0, 0] }] });
        const engr = CharmNestExport.parentScale(form(pl.scale), rec, { poolId: idA });
        let refused = ''; try { CharmNestExport.parentScale(form(S0), rec, { poolId: idA }); } catch (e) { refused = e.message; }
        // a reload: the record's charm read back (fromRecord), the size it is nested and written at (manualSize), and the scale the restore writes
        const man = CNManual.fromRecord({ manual: JSON.parse(JSON.stringify(c.manual)) });
        return { scale: pl.scale, size: manualSize(c), exp: exp && exp.scale, seen: seen.filter(v => Math.abs(v - pl.scale) < 1e-9).length, engr, refused, man, restored: +((typeof CHARM_SCALE === 'number' ? CHARM_SCALE : 0.971) * man.size).toFixed(5), words: cutSizeWords(c), pinned: c.pinned, job: (p => p && { areaPt2: p.areaPt2, scale: p.scale })(buildJob(sh).pieces.find(z => z.id === c.id)), area: c.areaPt2, bscale: c.scale };
      }, { idA, S0 });
      assert(near(r.exp, r.scale, 1e-9), 'the laser export\'s sheet places it at its scale: ' + JSON.stringify(r));
      assert(r.seen >= 1, 'the preview / thumbnail draws it at its scale: ' + JSON.stringify(r));
      assert(near(r.engr, r.scale, 1e-9) && /disagrees/.test(r.refused), 'the engraving is composed on the piece at its scale (a plain-scale file refused): ' + JSON.stringify(r));
      assert(r.man.fit === 'inplace' && near(r.man.size, r.size, 1e-9) && near(r.restored, r.scale, 1e-9), 'a reload keeps the size and the scale: ' + JSON.stringify(r));
      assert(r.job && near(r.job.areaPt2, r.area * r.size ** 2, 1e-6) && near(r.job.scale, r.bscale / r.size, 1e-9), 'a later nest of the sheet holds it at its size (buildJob): ' + JSON.stringify(r));
      assert(r.pinned && near(r.pinned.size, r.size, 1e-9), 'pinned in its spot at its size: ' + JSON.stringify(r.pinned));
      const mm = (63 * r.size * 25.4 / 72).toFixed(1);
      assert.equal(r.words, `${Math.round(r.size * 100)}% · ${mm} mm × ${mm} mm`, 'the charm\'s card says the size it is cut at');
      pass(`(f) the size reaches the saved record (scale ${r.scale}, manual size ${r.size}), a reload, the laser export, the preview / thumbnail, the engraving composed on it, and the card ("${r.words}")`); }

    /* (g) the sheet window's piece facts: "95% · 22.2 × 22.2 mm" (the size it is cut at) */
    { const want = await page.evaluate(idA => { const sh = CN.S.sheets.gold.pages.find(p => p.sheetId === 'ips-gold-1'), c = sh.charms.find(z => z.poolId === idA); return Math.round(manualSize(c) * 100) + '% · ' + fmt.mm(63 * manualSize(c)).replace(' mm', '') + ' × ' + fmt.mm(63 * manualSize(c)); }, idA);
      await page.evaluate(id => SheetWin.open('ips-gold-1', { select: id }), idA);
      let facts = '';
      await until(async () => { facts = await page.evaluate(() => (document.querySelector('.swPiece [data-r2=mm]') || {}).textContent || ''); return facts.endsWith(want); }, 25000, () => 'the sheet window\'s facts: ' + JSON.stringify(facts) + ' (want ' + want + ')');
      await page.evaluate(() => SheetWin.close && SheetWin.close());
      pass(`(g) the sheet window shows the size the piece is cut at: "${facts}"`); }

    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`replace-in-place-shrink: ${n} steps passed`);
})().catch(e => { console.error(e); process.exit(1); });
