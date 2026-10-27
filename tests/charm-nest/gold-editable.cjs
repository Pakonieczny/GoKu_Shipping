// GOLDEDIT (Paul, 27 Oct 2026, testing in the sandbox): "I already requested that the users should be able to add and remove charms from 10 K and 14 K gold sheets even after the green cut line
// has been approved and even after the entire sheet has been approved. This must be allowed to happen until such time a sheet has been cut on a laser and marked as cut by the user at which point a
// sheet is locked and could never be changed." And of the sheet window of 14K Sheet 1 (its set approved for laser cutting, Manual Control greyed): "I should be able to manually control this sheet
// and adjust the charms and add and remove charms from any 10 K and 14 K gold sheet until such time that it has been marked as completed in the laser cutting process."
// Every part is the REAL one, in one real Chromium page over the local stand-in for the site (bridge-server.cjs: the real charmNestLibrary handler, its Rose stock and leftovers, on an in-memory
// Firestore): the sheet hop, the sheet window's Manual Control, RoseStock (reopen, the green line drawn again: rosePlan on the server), Review's released-set mark, the Library's set card.
// The only stand-in is the nest itself (as in sheet-hop-dnd.cjs): what is pinned is placed where it was pinned and the sheet saved, checked.
//   a  a FULL 14K sheet approved into its set with no line (its contour: a full plan, no lines): no "green dash line drawn" refusal; a charm hops onto it and off it; Manual Control works there;
//      it stays in its set, its contour given up (made again by its card)
//   b  a 14K sheet whose green line cut was RECORDED: Manual Control starts (the cut set aside: the stock is the sheet's again, its leftover no longer offered, the card asks for Cut Sheet again);
//      a charm moved by hand is saved and the green line drawn again for the charms as they now stand
//   c  a 14K sheet with its green line drawn, in a set approved for laser cutting: a charm hops onto it; both sheets' lines drawn again, files written, the set's card and the sheet's say the set
//      changed after it was approved for laser cutting
//   d  a 14K sheet cut on the laser (the sheet's mark, or its set's): Manual Control and the hop refuse, "has been cut on the laser: its charms can no longer change", nothing written; the server
//      refuses too
//   e  GF and Rose Gold keep their rules (no Manual Control on GF; a Rose Gold cut still refuses the claim, the reopen and a moved layout; a charm in a Rose Gold saved line stays)
//   node tests/charm-nest/gold-editable.cjs      (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const { start } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || ''; if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', RUN = 'run-ged-1', STOCK = 'Charm_Nest_Rose_Stock', REM = 'Charm_Nest_Remnants';
const Rose = require('../../charm-nest-rose.js');
const BASE_TS = Math.floor(Date.UTC(2026, 9, 15, 12) / 1000);
const TID = tx => String(6000000000 + tx), pid = (rid, tx, copy) => `${rid}_${TID(tx)}_${copy}`;
const R = n => '41800000' + String(n).padStart(2, '0');
// the plate is 6 x 2 pieces, 30 pt apart (182 x 62 pt): a sheet of 12 is full. items: [rid, tx, copy, col, row]
const NOW = Date.now(), CUT_AT = NOW - 3600e3;
// 14K: Sheet 1 FULL in Set 1, approved with no line (its contour is a full plan with no lines); Draft 2; Sheet 3 in Set 1 with its green line cut RECORDED; Sheet 4 in Set 2, approved for laser
// cutting, its green line drawn; Sheet 5 cut on the laser. GF and Rose Gold keep their rules.
const SPEC = [
  { id: 'k14-1', metal: 'gold14k', page: 1, set: 'set-g1', items: Array.from({ length: 12 }, (_, i) => [R(10 + i), 10 + i, 1, i % 6, Math.floor(i / 6)]) },
  { id: 'k14-2', metal: 'gold14k', page: 2, items: [[R(30), 30, 1, 0, 0], [R(31), 31, 1, 1, 0], [R(32), 32, 1, 2, 0], [R(33), 33, 1, 3, 0]] },
  { id: 'k14-3', metal: 'gold14k', page: 3, set: 'set-g1', cut: true, items: [[R(40), 40, 1, 0, 0], [R(41), 41, 1, 1, 0], [R(42), 42, 1, 2, 0]] },
  { id: 'k14-4', metal: 'gold14k', page: 4, set: 'set-g2', line: true, items: [[R(50), 50, 1, 0, 0], [R(51), 51, 1, 1, 0], [R(52), 52, 1, 2, 0]] },
  { id: 'k14-5', metal: 'gold14k', page: 5, set: 'set-g3', laser: true, items: [[R(60), 60, 1, 0, 0], [R(61), 61, 1, 1, 0]] },
  { id: 'gf-1', metal: 'gold', page: 1, items: [[R(70), 70, 1, 0, 0], [R(71), 71, 1, 1, 0]] },
  { id: 'rg-1', metal: 'rose', page: 1, rose: true, items: [[R(80), 80, 1, 0, 0], [R(81), 81, 1, 1, 0]] }
];
const cell = (col, row) => ({ x: 16 + col * 30, y: 16 + row * 30 });
const MMk = 72 / 25.4, RM = 5, DD = Math.ceil(2 * RM * MMk) + 2, SQk = RM * MMk * 0.95;
// each charm's outline for the green line (a square inside its round body), as the page's charms carry it
const outline = () => ({ circle: 1, cx: DD / 2, cy: DD / 2, r: RM * MMk, subpaths: [[['m', [DD / 2 - SQk, DD / 2 - SQk]], ['l', [DD / 2 + SQk, DD / 2 - SQk]], ['l', [DD / 2 + SQk, DD / 2 + SQk]], ['l', [DD / 2 - SQk, DD / 2 + SQk]], ['l', [DD / 2 - SQk, DD / 2 - SQk]]]] });
const W14 = 182, H14 = 62;
const fullProfile = { version: 1, wPt: W14, hPt: H14, axis: 'x', step: 0.5, values: Array.from({ length: Math.ceil(H14 / 0.5) }, () => W14) };
function linePlan(sh) {
  const charms = sh.items.map((_, i) => ({ id: `${sh.id}-c${i}`, outline: outline(), centerPt: [DD / 2, DD / 2], members: [] }));
  const placements = sh.items.map(([, , , col, row], i) => ({ id: `${sh.id}-c${i}`, cxPt: cell(col, row).x, cyPt: cell(col, row).y, angle: 0 }));
  const plan = Rose.plan(Rose.shapes(charms, placements), W14, H14, null, 0.2);
  plan.stages = [{ n: 1, at: CUT_AT, ids: placements.map(p => p.id), lines: [0, plan.lines.length] }];
  return plan;
}

const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 600)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n         ')); };
const sleep = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 }, reducedMotion: process.env.REDUCED ? 'reduce' : 'no-preference' });   // (REDUCED=1: the same checks with reduced motion)
  await ctx.route(u => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: "window.pdfMake = { createPdf() { return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title>'], { type: 'text/html' })); } }; } };" });
    if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    return r.abort();
  });
  await ctx.addInitScript(() => {
    try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', pollOrders: 'off' })); localStorage.setItem('cn.employee', 'Tester'); } catch (_) { /* about:blank */ }
    window.__closes = []; addEventListener('close', e => { if (e.target && e.target.classList && e.target.classList.contains('sheetWin')) window.__closes.push(new Error('closed').stack.slice(0, 600)); }, true);
    window.__clicks = []; addEventListener('click', e => { const t = e.target; window.__clicks.push([t && (t.className || t.tagName), Math.round(performance.now()), window.SheetHop ? Math.round(SheetHop._H.noClick) : 0, e.isTrusted]); if (window.__clicks.length > 30) window.__clicks.shift(); }, true);
    window.prompt = () => 'Prompted'; window.confirm = () => true; window.alert = () => {};
    window.__errs = []; addEventListener('unhandledrejection', e => window.__errs.push('unhandledrejection: ' + String(e.reason && e.reason.message || e.reason).slice(0, 300)));
    // the nest, as the test has it: what is pinned is placed where it was pinned, nothing else moves; the sheet is saved as the real nest saves it
    const iv = setInterval(() => {
      if (typeof window.startNest !== 'function' || window.startNest.__stub) return;
      const MM = 72 / 25.4, Pd = window.CharmNestPDF, realPath = Pd.pathToCanvas;
      Pd.drawCharm = (c2, c, tx, k) => { const [x, y] = tx(c.centerPt[0], c.centerPt[1]); c2.beginPath(); c2.arc(x, y, c.rMm * MM * k * .6, 0, 7); c2.lineWidth = Math.max(1, .35 * k); c2.strokeStyle = '#7a5a1d'; c2.stroke(); };
      Pd.pathToCanvas = (c2, p, tx) => { if (p && p.circle) { const [x, y] = tx(p.cx, p.cy), [x1] = tx(p.cx + p.r, p.cy), r = Math.abs(x1 - x); c2.moveTo(x + r, y); c2.arc(x, y, r, 0, Math.PI * 2); return; } return realPath(c2, p, tx); };
      Pd.cutLinesOf = () => [];
      const stub = sh => {
        window.__nests = (window.__nests || []).concat([{ sheetId: sh.sheetId, charms: sh.charms.map(c => c.poolId) }]);
        sh.status = 'nesting'; sh.persistedDone = false; sh.persisted = Promise.resolve(); sh.jobId = 'job-' + Math.random().toString(36).slice(2, 8); sh.stage = 'test nest';
        setTimeout(async () => {
          try {
            for (const c of sh.charms) if (c.pinned && !sh.placements.some(p => p.id === c.id)) sh.placements.push({ id: c.id, cxPt: c.pinned.cxPt, cyPt: c.pinned.cyPt, angle: c.pinned.angle || 0, wPt: c.widthPt, hPt: c.heightPt });
            sh.placements = sh.placements.filter(p => sh.charms.some(c => c.id === p.id));
            const on = sh.charms.filter(c => sh.placements.some(p => p.id === c.id));
            await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, charms: on.map(c => ({ id: c.id, poolId: c.poolId, order: c.order })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: on.map(c => c.poolId), verification: { ok: true }, dirty: false } }, { quiet: true });
          } catch (e) { sh.problem = e.message; }
          sh.status = 'complete'; sh.dirty = false; sh.stage = ''; sh.persistedDone = true; sh.verification = { ok: true };
          try { CN.renderCard(sh); } catch (_) {}
        }, 150);
      };
      stub.__stub = true; window.startNest = stub; clearInterval(iv);
      // a sheet left with charms that overlap (a charm hopped onto a full sheet, Paul 27 Oct 2026) is written as placed, as the real writeAsPlaced does: every charm where it is, saved,
      // its own check flagged (the test world has no traced sources to make its files from)
      window.writeAsPlaced = async sh => {
        const on = sh.charms.filter(c => sh.placements.some(p => p.id === c.id));
        await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, charms: on.map(c => ({ id: c.id, poolId: c.poolId, order: c.order })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: on.map(c => c.poolId) } }, { quiet: true });
        sh.status = 'complete'; sh.dirty = false; sh.persistedDone = true; sh.verification = { ok: false }; sh.problem = 'Verification flagged: charms overlap';
        try { CN.renderCard(sh); } catch (_) {}
        return true;
      };
    }, 0);
  });
  const page = await ctx.newPage(), errors = [];
  page.setDefaultTimeout(30000);
  page.on('pageerror', e => errors.push('page: ' + String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')));
  page.on('console', m => { const t = m.text(); if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::/.test(t)) errors.push('console: ' + t.slice(0, 400)); });
  const until = async (fn, ms = 30000, what = '') => { const t0 = Date.now(); for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await sleep(100); } };
  const shot = async name => { if (!SHOTS) return; try { await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } catch (_) {} };
  const ready = () => page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.SheetWin && SheetWin.hopKit && window.SheetHop && window.CNManual && window.Pool && window.startNest && window.startNest.__stub, null, { timeout: 60000 });

  /* ── the world: the saved records, and the same sheets live in the sorter (the run that holds them) ── */
  for (const sh of SPEC) {
    const charms = [], placements = [];
    sh.items.forEach(([rid, tx, copy, col, row], i) => { const poolId = pid(rid, tx, copy), id = `${sh.id}-c${i}`, at = cell(col, row); charms.push({ id, poolId, order: rid }); placements.push({ id, cxPt: at.x, cyPt: at.y, angle: 0, wPt: 28.35, hPt: 28.35 }); st.put(POOL, poolId, { poolId, orderId: rid, transactionId: TID(tx), lineKey: `${rid}_${TID(tx)}`, sheetId: sh.id, state: 'placed', material: sh.metal, runId: RUN }); });
    const extra = {}, stockId = 'stk-' + sh.id;
    if (sh.set) Object.assign(extra, { setId: sh.set, draft: false, solidIncluded: true, setSeq: +sh.set.slice(-1) });
    if (sh.page === 1 && sh.metal === 'gold14k') { sh.plan = { version: 1, profile: fullProfile, lines: [], shapes: [], full: true, stages: [], allowanceMm: 0.2, removedPt2: W14 * H14, remainingPt2: 0 }; Object.assign(extra, { releaseFull: true, rosePlanJson: JSON.stringify(sh.plan), rosePlanHash: 'h-full', roseStockId: stockId, roseRevision: 0 }); st.put(STOCK, stockId, { id: stockId, metal: sh.metal, wPt: W14, hPt: H14, revision: 0, profileJson: null, owner: sh.id, available: false }); }
    if (sh.cut || sh.line) { sh.plan = linePlan(sh); Object.assign(extra, { rosePlanJson: JSON.stringify(sh.plan), rosePlanHash: 'h-' + sh.id, roseStockId: stockId, roseRevision: sh.cut ? 1 : 0 }); }
    if (sh.line) st.put(STOCK, stockId, { id: stockId, metal: sh.metal, wPt: W14, hPt: H14, revision: 0, profileJson: null, owner: sh.id, available: false });
    if (sh.cut) {
      Object.assign(extra, { roseCutAt: CUT_AT, roseCutRevision: 1 });
      st.put(STOCK, stockId, { id: stockId, metal: sh.metal, wPt: W14, hPt: H14, revision: 1, profileJson: JSON.stringify(sh.plan.profile), owner: null, available: true, lastCutSheetId: sh.id, lastCutAt: CUT_AT });
      st.put(STOCK + '/' + stockId + '/cuts', sh.id, { sheetId: sh.id, stockId, revision: 1, at: CUT_AT, planHash: 'h-' + sh.id, planJson: JSON.stringify(sh.plan), fileBase: sh.id, by: 'Paul' });
      st.put(REM, stockId + '-1', { v: 1, metal: sh.metal, code: '14K', sheetId: sh.id, sheetName: '14K Sheet 3', stockId, revision: 1, cutAt: CUT_AT, by: 'Paul', status: 'available', statusAt: CUT_AT });
    }
    if (sh.laser) Object.assign(extra, { laserDoneAt: NOW - 600e3 });
    if (sh.rose) Object.assign(extra, { roseCutAt: CUT_AT, setId: 'set-g1', draft: false });
    st.put(SHEETS, sh.id, Object.assign({ id: sh.id, metal: sh.metal, charms, placements, poolIds: charms.map(c => c.poolId), page: sh.page, draft: true, fileBase: sh.id, runId: RUN, verification: { ok: true }, outputs: { ai: { path: sh.id + '.ai', url: 'http://127.0.0.1:9/' + sh.id + '.ai' } } }, extra));
  }
  st.put('Charm_Nest_Sets', 'set-g1', { setId: 'set-g1', runId: RUN, seq: 1, name: 'Set 1', sheetIds: ['k14-1', 'k14-3'] });
  st.put('Charm_Nest_Sets', 'set-g2', { setId: 'set-g2', runId: RUN, seq: 2, name: 'Set 2', sheetIds: ['k14-4'], processReady: true });
  st.put('Charm_Nest_Sets', 'set-g3', { setId: 'set-g3', runId: RUN, seq: 3, name: 'Set 3', sheetIds: ['k14-5'] });

  await page.goto(`${sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await ready();
  await page.evaluate(async ({ spec, RUN, BASE_TS }) => {
    await Orders.loadMaps(true); await Cancelled.load(true);
    CN.S.stockPreset = 'custom'; CN.S.settings.stock = Object.assign({}, CN.S.settings.stock, { gold: [182 / 72, 62 / 72], gold10k: [182 / 72, 62 / 72], gold14k: [182 / 72, 62 / 72] });
    const B_DAY = new Date().toISOString().slice(0, 10), MM = 72 / 25.4, Rm = 5, D = Math.ceil(2 * Rm * MM) + 2, TID = tx => String(6000000000 + tx), pid = (rid, tx, copy) => `${rid}_${TID(tx)}_${copy}`;
    const SC = 4, DP = D * SC, mkBits = () => { const b = new Uint8Array(DP * DP); for (let y = 0; y < DP; y++) for (let x = 0; x < DP; x++) if (Math.hypot(x + .5 - DP / 2, y + .5 - DP / 2) <= DP / 2 - SC) b[y * DP + x] = 1; return b; };
    const SQ = Rm * MM * 0.95, sq = [[['m', [D / 2 - SQ, D / 2 - SQ]], ['l', [D / 2 + SQ, D / 2 - SQ]], ['l', [D / 2 + SQ, D / 2 + SQ]], ['l', [D / 2 - SQ, D / 2 + SQ]], ['l', [D / 2 - SQ, D / 2 - SQ]]]];
    const mkCharm = (id, extra) => Object.assign({ id, name: id, sku: '', sourceId: 's', ringGeometryVersion: 3, centerPt: [D / 2, D / 2], bbox: [0, 0, D, D], outline: { circle: 1, cx: D / 2, cy: D / 2, r: Rm * MM, subpaths: sq }, members: [], rMm: Rm, w: DP, h: DP, scale: SC, bits: mkBits(), widthPt: 2 * Rm * MM, heightPt: 2 * Rm * MM, areaPt2: Math.PI * (Rm * MM) ** 2 }, extra || {});
    for (const m of Object.keys(CN.S.sheets)) { const pr = CN.S.sheets[m]; pr.pages.length = 1; const p0 = pr.pages[0]; p0.charms = []; p0.placements = []; p0.sheetId = null; p0.fileBase = null; for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId', 'label', 'keepRelease']) delete p0[k]; p0.status = 'idle'; p0.page = 1; pr.active = 0; }
    B.pool.rows.clear();
    for (const sh of spec) {
      const pr = CN.S.sheets[sh.metal]; let pg = null;
      if (sh.page === 1) pg = pr.pages[0]; else { while (pr.pages.length < sh.page) addPage(sh.metal); pg = pr.pages[sh.page - 1]; }
      pg.charms = []; pg.placements = [];
      sh.items.forEach(([rid, tx, copy, col, row], i) => {
        const poolId = pid(rid, tx, copy), id = `${sh.id}-c${i}`;
        pg.charms.push(mkCharm(id, { name: `${rid} · charm`, sku: 'K' + tx, poolId, order: rid, lineKey: `${rid}_${TID(tx)}`, orderDate: BASE_TS - 1000 + i, orderInfo: { receiptId: rid, transactionId: TID(tx), copy, quantity: 1 } }));
        pg.placements.push({ id, cxPt: 16 + col * 30, cyPt: 16 + row * 30, angle: 0, wPt: 2 * Rm * MM, hPt: 2 * Rm * MM });
        B.pool.rows.set(poolId, { poolId, orderId: rid, transactionId: TID(tx), lineKey: `${rid}_${TID(tx)}`, sheetId: sh.id, state: 'placed', material: sh.metal, runId: RUN });
      });
      Object.assign(pg, { status: 'complete', sheetId: sh.id, fileBase: sh.id, page: sh.page, runId: RUN, draft: true, dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, density: sh.items.length / 12 * .8, releaseFull: sh.items.length >= 12, verification: { ok: true }, intakeAppend: false, appendOnly: false });
      for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId']) delete pg[k];
      const fp = () => JSON.stringify(pg.placements.map(p => [p.id, +p.cxPt.toFixed(3), +p.cyPt.toFixed(3), p.angle, p.scale || 1, p.hash || null]));
      if (sh.set) Object.assign(pg, { setId: sh.set, draft: false, solidPick: true, seq: +sh.set.slice(-1) });
      if (sh.plan) Object.assign(pg, { rosePlan: JSON.parse(JSON.stringify(sh.plan)), rosePlanHash: sh.page === 1 ? 'h-full' : 'h-' + sh.id, rosePlanKey: fp(), roseRevision: sh.cut ? 1 : 0,
        roseStock: { id: 'stk-' + sh.id, metal: sh.metal, wPt: 182, hPt: 62, revision: sh.cut ? 1 : 0, profileJson: sh.cut ? JSON.stringify(sh.plan.profile) : null, owner: sh.cut ? null : sh.id } });
      if (sh.cut) Object.assign(pg, { roseCutAt: sh.cutAt });
      if (sh.set === 'set-g2') pg.laserSetPending = true;
      if (sh.laser) pg.laserDoneAt = Date.now() - 600e3;
      if (sh.rose) Object.assign(pg, { roseCutAt: sh.cutAt, setId: 'set-g1', draft: false });
      CN.renderCard(pg);
    }
    B.orders.rows.length = 0; B.orders.byKey.clear(); B.sets.clear();
    B.sets.set(RUN + '|g1', { setId: 'set-g1', runId: RUN, seq: 1, name: 'Set 1', day: B_DAY, group: 'g1', sheetIds: ['k14-1', 'k14-3'], orders: {}, status: 'nesting' });
    B.sets.set(RUN + '|g2', { setId: 'set-g2', runId: RUN, seq: 2, name: 'Set 2', day: B_DAY, group: 'g2', sheetIds: ['k14-4'], orders: {}, status: 'nesting', processReady: true });
    B.sets.set(RUN + '|g3', { setId: 'set-g3', runId: RUN, seq: 3, name: 'Set 3', day: B_DAY, group: 'g3', sheetIds: ['k14-5'], orders: {}, status: 'nesting' });

    B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
  }, { spec: SPEC.map(s => Object.assign({}, s, { cutAt: CUT_AT })), RUN, BASE_TS });
  await sleep(300);

  /* ── readers and hands ── */
  const live = () => page.evaluate(() => Object.fromEntries(CN.allSheets().filter(p => p.sheetId).map(p => [p.sheetId, { all: p.charms.map(c => c.poolId), placed: p.placements.map(pl => { const c = p.charms.find(z => z.id === pl.id) || {}; return { id: c.poolId, x: pl.cxPt, y: pl.cyPt, pinned: !!(c.pinned && !c.arrivalPin) }; }) }])));
  const rec = id => st.doc(SHEETS, id) || {}, pool = id => st.doc(POOL, id) || {};
  const writes = () => st.calls.filter(c => c.name === 'charmNestLibrary' && /^(poolPut|poolUpdate|putSheet|setUpdate)$/.test(String(c.op))).length;
  const idOf = (rid, tx, copy = 1) => pid(rid, tx, copy);
  const openSheet = async id => {
    await page.evaluate(id => SheetWin.open(id), id);
    await until(() => page.evaluate(id => { const W = SheetWin.hopKit.W; return SheetWin.isOpen() && W.id === id && W.rec && W.geom && !W.flying && W.pieces.length > 0; }, id), 30000, 'the sheet window shows ' + id);
    await sleep(500);
  };
  const hopState = () => page.evaluate(() => SheetHop.state());
  /** the screen point of a piece on the window's plate */
  const onPlate = poolId => page.evaluate(id => { const K = SheetWin.hopKit, W = K.W, x = W.byPool.get(id); if (!x) throw new Error(`${id} is not drawn on ${W.id} (${[...W.byPool.keys()].join(', ')})`); const r = W.el.fx.getBoundingClientRect(), kc = W.k / W.dpr, R0 = W.R / W.dpr; return { x: r.left + R0 + x.p.cxPt * kc, y: r.top + R0 + x.p.cyPt * kc }; }, poolId);
  const tabOf = side => page.evaluate(s => { const t = document.querySelector(`.shpDock[data-side=${s}] .shpTab`); const r = t.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; }, side);
  const inPeek = (side, pt) => page.evaluate(([s, pt]) => { const dk = SheetHop._H.docks[s], r = dk.cv.getBoundingClientRect(); return { x: r.left + pt.x / dk.st.wPt * r.width, y: r.top + pt.y / dk.st.hPt * r.height, peek: dk.el.classList.contains('peek') }; }, [side, pt]);
  /** pick the piece up, carry it over a dock, wait for the open view, aim at a point of that sheet (pt), let go. With Manual Control on, the press
   *  is its own in-sheet drag first, handed over to the hop at the dock: what each saw is returned ({ inSheet, after, w, writesAtHandover }) */
  async function carry(poolId, side, pt, o = {}) {
    const info = {};
    const a = await until(() => onPlate(poolId).catch(() => null), 10000, poolId + ' is drawn on the plate');   // (the window may be reading its sheet again: it follows the cloud)
    await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(a.x + 12, a.y + 6, { steps: 4 });
    const lifted = () => until(() => page.evaluate(() => !!document.querySelector('.shpLift') && document.querySelector('.shpDock.open')), 5000, 'the charm lifts and the docks open');
    // (with Manual Control on the page the hand is handed over only once it reaches a dock or leaves the plate)
    const handover = await page.evaluate(() => !!window.CNManual);
    if (handover) { info.w = writes(); info.inSheet = await page.evaluate(() => { const d = CNManual._MC.drag; return { drag: d ? { moved: !!d.moved, poolId: d.x && d.x.poolId, mode: d.mode } : null, lifted: !!document.querySelector('.shpLift') }; }); }
    else await lifted();
    const t = await tabOf(side);
    await page.mouse.move(t.x + (side === 'prev' ? 40 : -40), t.y, { steps: 12 });
    if (handover) {
      await page.mouse.move(t.x, t.y, { steps: 4 }); await lifted();
      info.after = await page.evaluate(id => { const M = CNManual._MC, x = SheetWin.hopKit.W.byPool.get(id), d = SheetHop._H.drag; return { drag: !!M.drag, edit: x ? M.edits.get(x.id) || null : null, home: d ? { x: d.unit.pieces[0].p.cxPt, y: d.unit.pieces[0].p.cyPt } : null }; }, poolId);
      info.writesAtHandover = writes();
      const t2 = await tabOf(side); await page.mouse.move(t2.x + (side === 'prev' ? 40 : -40), t2.y, { steps: 6 });
    }
    if (o.before) await o.before();
    if (pt) { await until(async () => (await inPeek(side, pt)).peek, 4000, 'the dock opens into a view of its sheet'); await sleep(450); const b = await inPeek(side, pt); await page.mouse.move(b.x, b.y, { steps: 8 }); await sleep(250); }
    if (o.atDrop) await o.atDrop();
    await page.mouse.up();
    return info;
  }
  /* Manual Control, the real one: its header button in the sheet window */
  const mcBtn = () => page.evaluate(() => { const b = document.querySelector('.sheetWin [data-r=manual]'); return b ? { hidden: b.hidden, pressed: b.getAttribute('aria-pressed'), disabled: b.getAttribute('aria-disabled'), why: b.dataset.why || '' } : null; });
  const mcOn = () => page.evaluate(() => !!(window.CNManual || window.__cnm)._MC.on);
  async function manual(on) {
    if ((await mcOn()) === on) return;
    if (on) await until(async () => { const b = await mcBtn(); return b && !b.hidden && b.disabled === 'false'; }, 10000, 'Manual Control can start on ' + await page.evaluate(() => SheetWin.current()));
    await page.click('.sheetWin [data-r=manual]');
    await until(async () => (await mcOn()) === on, 10000, 'Manual Control ' + (on ? 'on' : 'off'));
    if (!on) await until(() => page.evaluate(() => !SheetWin.hopKit.W.flow), 120000, 'its save ends');
  }
  /** Manual Control's own drag of a charm inside its sheet, to a point of the sheet (points): what it kept for it */
  async function mcMove(poolId, to) {
    const a = await onPlate(poolId), b = await page.evaluate(([x, y]) => CNManual._client(x, y), [to.x, to.y]);
    await page.mouse.move(a.x, a.y); await page.mouse.down();
    for (let i = 1; i <= 10; i++) await page.mouse.move(a.x + (b[0] - a.x) * i / 10, a.y + (b[1] - a.y) * i / 10);
    await sleep(80); await page.mouse.up(); await sleep(400);
    return page.evaluate(id => { const M = CNManual._MC, x = SheetWin.hopKit.W.byPool.get(id); return x ? M.edits.get(x.id) || null : null; }, poolId);
  }
  /** a charm that went onto 10K Draft 1 (where it overlaps others) taken back to its spot on 10K Draft 2 by the hop itself (a press on an overlapping spot may pick the charm drawn over it) */
  const hopBack = async (q, home) => {
    await openSheet('k10-1'); await until(() => page.evaluate(() => !SheetWin.hopKit.W.flow && !SheetWin.hopKit.hopState().writing), 60000, 'nothing being written');
    try { await manual(true); } catch (e) { throw new Error(e.message + ' :: ' + JSON.stringify(await mcBtn())); }
    const ok = await page.evaluate(([id, pt]) => { const pg = CN.allSheets().find(p => p.sheetId === 'k10-1'), c = pg.charms.find(z => z.poolId === id); return SheetHop.hopPiece(pg, c, 'next', pt); }, [q, home]);
    assert.equal(ok, true, 'the hop back ran'); await settled(); await closePops();
  };
  const shownAt = poolId => page.evaluate(id => { const x = SheetWin.hopKit.W.byPool.get(id); return x ? { x: x.p.cxPt, y: x.p.cyPt } : null; }, poolId);
  const recAt = (sheetId, poolId) => { const r = rec(sheetId), c = (r.charms || []).find(z => z.poolId === poolId), p = c && (r.placements || []).find(z => z.id === c.id); return p ? { x: p.cxPt, y: p.cyPt, manual: c.manual || null } : null; };
  const settled = () => until(() => page.evaluate(() => { const s = SheetHop.state(); const K = SheetWin.hopKit, W = K.W; return !s.busy && !s.dragging && !W.flow && !W.hop && !document.querySelector('.shpLift') && !(K.hopState && K.hopState().pending); }), 60000, 'the move settles');   // (MANUALHOP: and its save in the background)
  const popText = () => page.evaluate(() => { const p = document.querySelector('.shpPop:not(.out)'); return p ? p.textContent : ''; });
  const closePops = () => page.evaluate(() => { for (const n of document.querySelectorAll('.shpPop, .mNote')) n.remove(); SheetHop._H.pop = null; });
  /** each piece on exactly one saved record, and the live sheets say the same */
  async function oneEach(what) {
    const all = SPEC.flatMap(sh => sh.items.map(([rid, tx, copy]) => pid(rid, tx, copy)));
    const docs = st.list(SHEETS), lv = await live();
    for (const id of all) {
      const on = docs.filter(d => (d.poolIds || []).includes(id)).map(d => d._id || d.id);
      assert.equal(on.length, 1, `${what}: ${id} is listed on exactly one saved sheet: ${JSON.stringify(on)}`);
      const onLive = Object.entries(lv).filter(([, s]) => s.all.includes(id)).map(([k]) => k);
      assert.deepEqual(onLive, on, `${what}: the sorter holds ${id} where the cloud says`);
    }
  }

  const idle = () => until(() => page.evaluate(() => { const K = SheetWin.hopKit, s = K.hopState(); return !K.W.flow && !s.writing && !s.pending && !CNManual._MC.claiming; }), 120000, 'nothing being written');
  const planOf = id => { const r = rec(id); return r.rosePlanJson ? JSON.parse(r.rosePlanJson) : null; };
  const lastStage = p => (p && p.stages && p.stages.length ? p.stages[p.stages.length - 1] : null);
  const call = (op, body) => page.evaluate(([op, body]) => api('charmNestLibrary', Object.assign({ op }, body), { quiet: true }).then(r => r, e => ({ thrown: String(e && e.message || e) })), [op, body]);
  const LASER = /has been cut on the laser: its charms can no longer change/;

  try {
    /* a · the FULL 14K sheet, approved into its set with no green line */
    try {
      await openSheet('k14-2'); await manual(true);
      await until(async () => { const s = await hopState(); return s.docks.prev && s.docks.next; }, 5000, 'the two docks');
      const why = await page.evaluate(() => { const g = id => CN.allSheets().find(p => p.sheetId === id); return { full: SheetHop.sheetRefusal(g('k14-1')), plan: !!g('k14-1').rosePlan, lines: g('k14-1').rosePlan.lines.length, mc: CNManual.available(g('k14-1')).reason }; });
      assert.deepEqual(why, { full: '', plan: true, lines: 0, mc: '' }, 'a full sheet with a contour and no line: no refusal (the old "green dash line drawn" read its contour as a line): ' + JSON.stringify(why));
      const a = idOf(R(30), 30), w0 = writes();
      await carry(a, 'prev', cell(1, 1)); await settled();
      assert.equal(await popText(), '', 'no refusal pop-up');
      assert((await live())['k14-1'].all.includes(a), 'the charm is on the full 14K Sheet 1');
      await until(() => (rec('k14-1').poolIds || []).includes(a), 20000, 'saved on 14K Sheet 1');
      const r1 = rec('k14-1');
      assert(!r1.rosePlanJson && !r1.rosePlanHash, 'its contour (full, no line) was given up for the new layout');
      assert(r1.setId === 'set-g1' && r1.draft === false, 'it stays in Set 1: ' + JSON.stringify([r1.setId, r1.draft]));
      assert(writes() > w0, 'the move is saved');
      // Manual Control carries on to it (its claim is taken: the sheet is not locked), and the charm hops back off it
      await openSheet('k14-1');
      await until(() => page.evaluate(() => CNManual._MC.on && CNManual._MC.id === 'k14-1' && !CNManual._MC.claiming), 10000, 'Manual Control on 14K Sheet 1');
      assert(rec('k14-1').manualEdit, 'its claim is held');
      const ok = await page.evaluate(id => { const p = CN.allSheets().find(s => s.sheetId === 'k14-1'), c = p.charms.find(z => z.poolId === id); return SheetHop.hopPiece(p, c, 'next', { x: 16, y: 16 }); }, a);
      assert.equal(ok, true, 'the hop off ran'); await settled(); await closePops();
      const lv = await live();
      assert(lv['k14-2'].all.includes(a) && !lv['k14-1'].all.includes(a), 'and it is back on 14K Draft 2');
      await manual(false); await idle();
      assert.equal(pool(a).sheetId, 'k14-2', 'its piece record says 14K Draft 2');
      pass('a · a FULL 14K sheet approved into Set 1 with no line: no "green dash line" refusal, a charm hops onto it and off it, Manual Control works on it, it stays in its set (its full contour given up)');
    } catch (e) { fail('a · full sheet approved with no line', e); }
    await closePops();

    /* b · a 14K sheet whose green line cut was recorded */
    try {
      await openSheet('k14-3');
      const b0 = await mcBtn();
      assert(b0 && !b0.hidden && b0.disabled === 'false', 'Manual Control is offered on a sheet whose green line cut was recorded: ' + JSON.stringify(b0));
      await manual(true);
      await until(() => !rec('k14-3').roseCutAt, 15000, 'the recorded cut set aside');
      await until(() => page.evaluate(() => !CNManual._MC.claiming), 10000, 'Manual Control started');
      const r3 = rec('k14-3'), stock = st.doc(STOCK, 'stk-k14-3') || {}, left = st.doc(REM, 'stk-k14-3-1') || {};
      assert(r3.roseReopened && r3.roseReopened.cut && !r3.rosePlanJson && !r3.roseCutRevision, 'the sheet record: cut set aside, line given up: ' + JSON.stringify(r3.roseReopened));
      assert(stock.owner === 'k14-3' && stock.profileJson == null && stock.revision === 1 && stock.available === false, 'the physical sheet is the sheet\'s again, as it was before the cut: ' + JSON.stringify(stock));
      assert(left.status === 'discarded' && /changed after its green line was recorded/.test(left.reason || ''), 'its leftover is no longer offered: ' + JSON.stringify(left));
      assert((r3.roseReseated || []).some(x => x && x.reopen && x.cut && x.cut.stockId === 'stk-k14-3'), 'the next Cut Sheet records under its own revision (roseReseated)');
      const m = idOf(R(41), 41), e = await mcMove(m, cell(4, 1));
      assert(e && Math.hypot(e.cxPt - cell(4, 1).x, e.cyPt - cell(4, 1).y) < 1.5, 'moved by hand: ' + JSON.stringify(e));
      await manual(false); await idle();
      const at = recAt('k14-3', m);
      assert(at && Math.hypot(at.x - cell(4, 1).x, at.y - cell(4, 1).y) < .01, 'saved where it was put: ' + JSON.stringify(at));
      await until(() => { const p = planOf('k14-3'); return p && p.lines.length > 0; }, 60000, 'the green line drawn again');
      const st3 = lastStage(planOf('k14-3'));
      assert(st3 && st3.ids.includes('k14-3-c1') && st3.at > NOW - 1000, 'the line follows the charms as they now stand, dated now: ' + JSON.stringify(st3));
      assert(st.calls.some(c => c.name === 'charmNestLibrary' && c.op === 'rosePlan'), 'drawn by the green line\'s own call (rosePlan)');
      assert(!rec('k14-3').roseCutAt, 'the cut is not recorded again by itself (a person\'s Cut Sheet press)');
      const card = await page.evaluate(() => { const sh = CN.allSheets().find(p => p.sheetId === 'k14-3'); showPage(sh.metal, CN.S.sheets[sh.metal].pages.indexOf(sh)); CN.renderCard(sh); const n = sh.el && sh.el.querySelector('[data-rose-reopened]'); return n ? n.textContent : ''; });
      assert(/changed after its green line was approved.*press Cut Sheet to approve its green line again/.test(card), 'its card asks for Cut Sheet again: ' + card);
      pass('b · a recorded green line cut is no lock: Manual Control sets it aside (stock the sheet\'s again, leftover discarded), a charm moved by hand is saved, the green line drawn again for the new layout, the card asks for Cut Sheet again');
    } catch (e) { fail('b · recorded green line cut', e); }
    await closePops();

    /* c · a 14K sheet with its line drawn, in a set approved for laser cutting */
    try {
      await openSheet('k14-4');
      const b0 = await mcBtn();
      assert(b0 && !b0.hidden && b0.disabled === 'false', 'Manual Control is offered on a sheet of a set approved for laser cutting: ' + JSON.stringify(b0));
      await manual(true);
      assert.equal(await page.evaluate(() => SheetHop.sheetRefusal(CN.allSheets().find(p => p.sheetId === 'k14-4'))), '', 'the hop takes it too');
      const n = idOf(R(52), 52);
      await carry(n, 'prev', cell(3, 1)); await settled();
      assert.equal(await popText(), '', 'no refusal pop-up');
      await until(() => (rec('k14-3').poolIds || []).includes(n) && !(rec('k14-4').poolIds || []).includes(n), 20000, 'saved on 14K Sheet 3');
      await manual(false); await idle();
      await until(() => { const p = lastStage(planOf('k14-4')); return p && p.at > NOW - 1000; }, 60000, '14K Sheet 4\'s line drawn again');
      await until(() => { const p = lastStage(planOf('k14-3')); return p && p.ids.includes('k14-4-c2'); }, 60000, '14K Sheet 3\'s line drawn again with the charm that came');
      assert(!lastStage(planOf('k14-4')).ids.includes('k14-4-c2'), 'the line of the sheet it left no longer has it');
      const rc = rec('k14-4').releasedChange;
      assert(rc && rc.setId === 'set-g2' && /^Set 2 changed after it was approved for laser cutting: .+ charms were moved between sheets by hand — its files are written again$/.test(rc.word), 'the sheet\'s record says its set changed since its approval: ' + JSON.stringify(rc));
      const said = await page.evaluate(row => {
        const sh = CN.allSheets().find(p => p.sheetId === 'k14-4'); showPage(sh.metal, CN.S.sheets[sh.metal].pages.indexOf(sh)); CN.renderCard(sh);
        const nest = sh.el && sh.el.querySelector('[data-released-change]');
        const set = Sets.ofRun('run-ged-1').find(s => s.setId === 'set-g2'), card = Sets.libraryCard(set, [row], [row]), lib = card.querySelector('[data-set-changed]');
        return { nest: nest ? nest.textContent : '', lib: lib ? lib.textContent : '', approved: !!set.processReady };
      }, Object.assign({}, rec('k14-4'), { id: 'k14-4' }));
      assert(/Set 2 changed after it was approved for laser cutting/.test(said.nest), 'the sheet\'s card says so: ' + said.nest);
      assert(/Set 2 changed after it was approved for laser cutting/.test(said.lib), 'and the set\'s Library card: ' + said.lib);
      assert(said.approved, 'the set keeps its approval');
      pass('c · a set approved for laser cutting is no lock: Manual Control is open, a charm hops off its lined sheet; both sheets written, their green lines drawn again, the set keeps its approval and its card (and the sheet\'s) says it changed since');
    } catch (e) { fail('c · set approved for laser cutting', e); }
    await closePops();

    /* d · cut on the laser: the one lock */
    try {
      await openSheet('k14-5');
      const b0 = await mcBtn();
      assert(b0 && b0.disabled === 'true' && LASER.test(b0.why), 'Manual Control says why it cannot start: ' + JSON.stringify(b0));
      await openSheet('k14-4'); await manual(true);
      const w0 = writes(), o = idOf(R(50), 50);
      await carry(o, 'next', null);
      await until(popText, 10000, 'the pop-up'); assert(/14K (Sheet|Draft) 5 has been cut on the laser: its charms can no longer change/.test(await popText()), await popText());
      await settled(); await closePops();
      assert.equal(writes(), w0, 'nothing written');
      // its SET marked laser cut locks a sheet of it the same way
      const set = await page.evaluate(() => { const set = Sets.ofRun('run-ged-1').find(s => s.setId === 'set-g2'), sh = CN.allSheets().find(p => p.sheetId === 'k14-4'); set.laserDoneAt = Date.now(); const out = { hop: SheetHop.sheetRefusal(sh), mc: CNManual.available(sh).reason }; delete set.laserDoneAt; return out; });
      assert(LASER.test(set.hop) && LASER.test(set.mc), 'its set marked laser cut: ' + JSON.stringify(set));
      await manual(false); await idle();
      const re = await call('roseReopen', { sheetId: 'k14-5' }), cl = await call('manualClaim', { id: 'k14-5', tab: 'tab-test-1', by: 'Tester' });
      assert(LASER.test(re.thrown || re.error || ''), 'the server refuses to set its line aside: ' + JSON.stringify(re));
      assert(cl && cl.ok === false && cl.refused === 'cut', 'and its claim: ' + JSON.stringify(cl));
      pass('d · a sheet cut on the laser (its mark, or its set\'s) is locked: "has been cut on the laser: its charms can no longer change" on Manual Control and the hop, nothing written; the server refuses too');
    } catch (e) { fail('d · laser cut', e); }
    await closePops();

    /* e · GF and Rose Gold keep their rules */
    try {
      await openSheet('gf-1');
      assert.equal((await mcBtn() || { hidden: true }).hidden, true, 'a GF sheet has no Manual Control');
      const r = await page.evaluate(() => {
        const g = id => CN.allSheets().find(p => p.sheetId === id), rg = g('rg-1'), c = rg.charms[0], p = rg.placements[0], keep = rg.roseCutAt;
        const out = { gf: SheetHop.sheetRefusal(g('gf-1')), rgCut: CNManual.fitsSpot(rg, c, { cxPt: p.cxPt, cyPt: p.cyPt, angle: 0 }).reason };
        delete rg.roseCutAt; rg.roseProtected = { placements: [Object.assign({}, p)], lines: [], shapes: [] };
        out.rgLine = CNManual.fitsSpot(rg, c, { cxPt: p.cxPt, cyPt: p.cyPt, angle: 0 }).reason;
        delete rg.roseProtected; rg.roseCutAt = keep;
        return out;
      });
      assert(/not a 10K or 14K sheet/.test(r.gf), r.gf);
      assert(/can no longer change/.test(r.rgCut), 'a Rose Gold cut layout stays as it is: ' + r.rgCut);
      assert(/inside a saved green line/.test(r.rgLine), 'a charm in a Rose Gold saved line stays: ' + r.rgLine);
      const cl = await call('manualClaim', { id: 'rg-1', tab: 'tab-test-1', by: 'Tester' }), re = await call('roseReopen', { sheetId: 'rg-1' });
      const put = await call('putSheet', { sheet: { id: 'rg-1', placements: (rec('rg-1').placements || []).map(p => Object.assign({}, p, { cxPt: p.cxPt + 1 })) } });
      assert(cl && cl.ok === false && cl.refused === 'cut', 'a Rose Gold cut sheet is never claimed: ' + JSON.stringify(cl));
      assert(/Only a 10K or 14K Gold sheet/.test(re.thrown || re.error || ''), 'nor set aside: ' + JSON.stringify(re));
      assert(/already cut/.test(put.thrown || put.error || ''), 'nor its layout moved: ' + JSON.stringify(put));
      pass('e · GF has no Manual Control and no hop; Rose Gold keeps its cut and its saved lines (the claim, the reopen and a moved layout refused)');
    } catch (e) { fail('e · GF and Rose Gold unchanged', e); }

    try {
      const errs = errors.concat(await page.evaluate(() => window.__errs || []));
      assert.deepEqual(errs, [], errs.join('\n'));
      pass('no console or page errors, no unhandled rejections');
    } catch (e) { fail('errors', e); }
  } finally {
    await browser.close(); await srv.close();
  }
  console.log(`\n${results.length} checks passed, ${failures.length} failed`);
  if (failures.length) { console.log('\nFAILURES\n' + failures.map(f => ' - ' + f).join('\n')); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
