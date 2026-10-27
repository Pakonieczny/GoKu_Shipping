// Paul, 27 Oct 2026, the sandbox Library, Set-1: GF Sheet 1, 14K Sheet 1, RG Sheet 1 and SS Sheet 1 drawn as the set's sheets, and under them in red: "Set 1 breaks your rules: 14K Sheet 1
// shares an order with a sheet of this set but is not in it. Press "Rebuild this set correctly"". His words: "To me it looks like the 14K sheet is correctly added, and has all the pieces but for
// some reason, this is erroring out and I don't know why." And the note above it: "orders 4206987902, 4210212342 it shares with GF Sheet 1 and GF Sheet 1 ship in two parts" (two different sheets).
//
// What happened, found from the saved records (the 14K record in Set 1 with solidIncluded true, its contour saved full, "filesOwed" from an artwork edit at 20:30 still owed, its files still the
// ones from before the join): the full 14K sheet was approved into Set 1 (the Library's "Approve", Gate.cutInclude). Later a piece on it had its own artwork edited (the charm editor) and the sheet
// was written again as it stands (writeAsPlaced). The write drafted the sheet on THIS page first (nextSheetSeq), and the cloud refused the write (the saved contour guards the layout and the edited
// piece carries a hash of its own): the record kept the sheet in Set 1, the page held it in no set with its own yes, and the set check (Gate.violations, page memory) drew the red line.
//
// Every part is the page's REAL code, in one real Chromium page over the local stand-in for the site (bridge-server.cjs: the real charmNestLibrary handler on an in-memory Firestore; nothing leaves
// the machine): the Library's set cards and their red line (SetRepair.annotate), the "Approve" press of a full 14K sheet waiting greyed in its set (SetRepair.cutPending, Gate.cutInclude), the run's
// assembly, the charm editor's hooks on the page (CharmNestArtEdit.host: swap, changed), writeAsPlaced, finishNest, nextSheetSeq, persistSheet, the workspace checkpoint and its restore. The only stand-ins
// are the sheet's file making and its two checks (the .ai, the proof and the pixel re-render: the test world has no traced designs), which say "written, verified".
//   1  a full 14K sheet waits greyed in Set 1; its "Approve" puts it in (page and record), no red line
//   2  an artwork edit on a piece of it: the sheet is written again; it STAYS in Set 1 on the page and on its record, no red line (the cloud takes the write: an edited piece is still the piece)
//   3  a write the cloud refuses (forced): a GF sheet of a set keeps its place on the page as its record does; no red line
//   4  another computer approves a 10K sheet into Set 2: this page (holding it in no set) takes its place from the record when the Library reads it, and the page's own older list of Set 2 never
//      takes it off the set's record
//   5  the other 14K sheet included and taken out again: the approved one stays
//   6  a reload (the workspace checkpoint): it is still in Set 1, no red line
//   7  the note under Set 1 names each sheet of another set with its set: "order …, shared with GF Sheet 1 of Set 2, and order …, shared with GF Sheet 1 of Set 3, ship in two parts"
//   node tests/charm-nest/gold-set-stale.cjs      (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>, SHOTS=<dir>)
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const { start, Timestamp } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || ''; if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const SHEETS = 'Charm_Nest_Sheets', SETS = 'Charm_Nest_Sets', POOL = 'Charm_Pool', RUNS = 'Charm_Nest_Runs';
const RUN = 'run-2026-10-26-gss', DAY = '2026-10-27';
const TID = tx => String(5250000000 + tx), pid = (rid, tx, copy = 1) => `${rid}_${TID(tx)}_${copy}`;
const R = n => '42' + String(n).padStart(8, '0');
// the orders a set's sheets share (two pieces each, one on each sheet), the 14K sheet's shared ones, and each sheet's own
const D = R(1), E = R(2), F = R(3), A = R(11), Bo = R(12), C = R(13), G = R(14), H = R(15);
const SETS_SPEC = [
  { setId: 'set-gss-1', seq: 1, sheets: ['gf-1', 'ss-1'] },
  { setId: 'set-gss-2', seq: 2, sheets: ['gf-2', 'ss-2'] },
  { setId: 'set-gss-3', seq: 3, sheets: ['gf-3', 'ss-3'] }
];
// items: [rid, tx, copy]; the plate is 6 x 2 pieces, 30 pt apart (182 x 62 pt)
const SPEC = [
  { id: 'gf-1', metal: 'gold', page: 1, set: 'set-gss-1', items: [[D, 1, 1], [A, 11, 1], [R(31), 31, 1], [R(32), 32, 1], [R(33), 33, 1], [R(34), 34, 1], [R(35), 35, 1], [R(36), 36, 1], [R(37), 37, 1], [R(38), 38, 1]] },
  { id: 'ss-1', metal: 'silver', page: 1, set: 'set-gss-1', items: [[D, 1, 2], [Bo, 12, 1], [R(41), 41, 1], [R(42), 42, 1], [R(43), 43, 1], [R(44), 44, 1], [R(45), 45, 1], [R(46), 46, 1], [R(47), 47, 1], [R(48), 48, 1]] },
  { id: 'gf-2', metal: 'gold', page: 2, set: 'set-gss-2', items: [[E, 2, 1], [C, 13, 1], [H, 15, 1], [R(51), 51, 1], [R(52), 52, 1], [R(53), 53, 1], [R(54), 54, 1], [R(55), 55, 1], [R(56), 56, 1], [R(57), 57, 1]] },
  { id: 'ss-2', metal: 'silver', page: 2, set: 'set-gss-2', items: [[E, 2, 2], [R(61), 61, 1], [R(62), 62, 1], [R(63), 63, 1], [R(64), 64, 1], [R(65), 65, 1], [R(66), 66, 1], [R(67), 67, 1], [R(68), 68, 1], [R(69), 69, 1]] },
  { id: 'gf-3', metal: 'gold', page: 3, set: 'set-gss-3', items: [[F, 3, 1], [G, 14, 1], [R(71), 71, 1], [R(72), 72, 1], [R(73), 73, 1], [R(74), 74, 1], [R(75), 75, 1], [R(76), 76, 1], [R(77), 77, 1], [R(78), 78, 1]] },
  { id: 'ss-3', metal: 'silver', page: 3, set: 'set-gss-3', items: [[F, 3, 2], [R(81), 81, 1], [R(82), 82, 1], [R(83), 83, 1], [R(84), 84, 1], [R(85), 85, 1], [R(86), 86, 1], [R(87), 87, 1], [R(88), 88, 1], [R(89), 89, 1]] },
  // the full 14K sheet: orders A (GF Sheet 1) and B (SS Sheet 1) of Set 1, C (GF Sheet 1 of Set 2) and G (GF Sheet 1 of Set 3): a known split. Its contour is saved, full (no green line)
  { id: 'k14-1', metal: 'gold14k', page: 1, full: true, plan: true, items: [[A, 21, 1], [Bo, 22, 1], [C, 23, 1], [G, 24, 1], [R(91), 91, 1], [R(92), 92, 1], [R(93), 93, 1], [R(94), 94, 1], [R(95), 95, 1], [R(96), 96, 1]] },
  { id: 'k14-2', metal: 'gold14k', page: 2, full: false, items: [[R(97), 97, 1], [R(98), 98, 1], [R(99), 99, 1]] },
  // a full 10K sheet sharing order H with GF Sheet 1 of Set 2: it waits greyed in Set 2 (another computer approves it, 4)
  { id: 'k10-1', metal: 'gold10k', page: 1, full: true, plan: true, items: [[H, 25, 1], [R(101), 101, 1], [R(102), 102, 1], [R(103), 103, 1], [R(104), 104, 1], [R(105), 105, 1], [R(106), 106, 1], [R(107), 107, 1], [R(108), 108, 1], [R(109), 109, 1]] }
];
const TAG = { gold: 'GF', silver: 'SS', gold14k: '14K', gold10k: '10K' };
const at = i => ({ x: 16 + (i % 6) * 30, y: 16 + Math.floor(i / 6) * 30 });
const hashOf = poolId => 'h' + poolId.slice(-6).replace(/_/g, '');
const results = [], failures = [];
const pass = m => { results.push(m); console.log('  ok   ' + m); };
const fail = (m, e) => { failures.push(m + ' :: ' + String(e && e.message || e).split('\n')[0].slice(0, 900)); console.log('  FAIL ' + m + '\n         ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n         ')); };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const assert = (c, m) => { if (!c) throw new Error(m); };

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 }, reducedMotion: 'reduce' });
  const refuse = { putSheet: null };   // (3: the cloud refuses one sheet's write)
  await ctx.route(u => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    return r.abort();
  });
  await ctx.route(/\/\.netlify\/functions\/charmNestLibrary/, r => {
    let b = null; try { b = r.request().postDataJSON(); } catch (_) { b = null; }
    if (b && b.op === 'runList') return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ runs: [] }) });
    if (b && b.op === 'putSheet' && refuse.putSheet && b.sheet && b.sheet.id === refuse.putSheet && b.sheet.saving === true) return r.fulfill({ status: 409, contentType: 'application/json', body: JSON.stringify({ error: 'refused for the test: this sheet was changed elsewhere' }) });
    return r.continue();
  });
  await ctx.addInitScript(() => {
    try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', pollOrders: 'off' })); localStorage.setItem('cn.employee', 'Tester'); } catch (_) { /* about:blank */ }
    window.prompt = () => 'Prompted'; window.confirm = () => true; window.alert = () => {};
    window.__errs = []; addEventListener('unhandledrejection', e => window.__errs.push('unhandledrejection: ' + String(e.reason && e.reason.message || e.reason).slice(0, 300)));
    // the sheet's files and its two checks, as the test world has them (no traced designs to write from): written and verified; everything else of the write is the page's own
    const iv = setInterval(() => {
      if (typeof window.finishNest !== 'function' || !window.CharmNestPDF || window.__stubbed) return;
      window.__stubbed = true; clearInterval(iv);
      const Pd = window.CharmNestPDF, MM = 72 / 25.4;
      Pd.drawCharm = (c2, c, tx, k) => { const [x, y] = tx(c.centerPt[0], c.centerPt[1]); c2.beginPath(); c2.arc(x, y, (c.rMm || 5) * MM * k * .6, 0, 7); c2.lineWidth = Math.max(1, .35 * k); c2.strokeStyle = '#7a5a1d'; c2.stroke(); };
      const realPath = Pd.pathToCanvas;
      Pd.pathToCanvas = (c2, p, tx) => { if (p && p.circle) { const [x, y] = tx(p.cx, p.cy), [x1] = tx(p.cx + p.r, p.cy), r = Math.abs(x1 - x); c2.moveTo(x + r, y); c2.arc(x, y, r, 0, Math.PI * 2); return; } return realPath(c2, p, tx); };
      Pd.cutLinesOf = () => [];
      Pd.buildSheet = async () => new TextEncoder().encode('%PDF-1.4 test sheet');
      window.sheetSpec = sh => ({ placements: sh.placements.map(p => ({ layerName: p.id, layerSig: p.id })) });
      window.geomCheck = async () => ({ ok: true, res: 6, overlapPx: 0, outsidePx: 0, removedPx: 0, protectedMoved: 0, overlappingPairs: [], minGapPt: 2, minEdgePt: 2 });
      window.renderCheck = async () => ({ ok: true, overlapPx: 0, outsidePx: 0, emptyLayers: [] });
      window.ensurePdfJs = async () => {};
      window.previewPng = async () => new Blob([new Uint8Array([137, 80, 78, 71])], { type: 'image/png' });
    }, 0);
  });
  const page = await ctx.newPage(), errors = [];
  page.setDefaultTimeout(30000);
  page.on('pageerror', e => errors.push('page: ' + String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')));
  page.on('console', m => { const t = m.text(); if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::|CORS policy/.test(t)) errors.push('console: ' + t.slice(0, 400)); });
  const until = async (fn, ms = 30000, what = '') => { const t0 = Date.now(); for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await sleep(100); } };
  const shot = async name => { if (!SHOTS) return; try { await page.screenshot({ path: path.join(SHOTS, name + '.png'), fullPage: false }); } catch (_) {} };
  const ready = () => page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Gate && window.SetRepair && window.CharmNestArtEdit && window.__stubbed && window.LibraryDone, null, { timeout: 60000 });

  /* ── the world: the saved records, and the same sheets live in the sorter (the run that holds them) ── */
  const now = Date.now(), ts = ms => Timestamp.fromMillis(ms), image = srv.blobUrl('preview.png');
  const lines = {};
  for (const sh of SPEC) {
    const charms = [], placements = [], poolIds = [];
    sh.items.forEach(([rid, tx, copy], i) => {
      const poolId = pid(rid, tx, copy), id = `${sh.id}-c${i}`, p = at(i);
      charms.push({ id, poolId, order: rid, hash: hashOf(poolId), name: `${rid} · charm`, areaPt2: 300, widthPt: 28.35, heightPt: 28.35 });
      placements.push({ n: i + 1, id, cxPt: p.x, cyPt: p.y, angle: 0, scale: 0.971, wPt: 28.35, hPt: 28.35, hash: hashOf(poolId) });
      poolIds.push(poolId);
      st.put(POOL, poolId, { poolId, orderId: rid, transactionId: TID(tx), lineKey: `${rid}_${TID(tx)}`, sheetId: sh.id, state: 'placed', material: sh.metal, runId: RUN, copy });
      lines[`${rid}_${TID(tx)}`] = { orderId: rid, state: 'written', quantity: 1, poolIds: [poolId], engraveCandidate: false };
    });
    const set = SETS_SPEC.find(x => x.setId === sh.set), name = set ? `${TAG[sh.metal]}_Oct.27.26_Set-${set.seq}_Sheet-1` : `${TAG[sh.metal]}_working_${sh.id}`;
    st.put(SHEETS, sh.id, { id: sh.id, metal: sh.metal, metalLabel: TAG[sh.metal], day: DAY, runId: RUN, page: sh.page, draft: !set, setId: set ? set.setId : null, setSeq: set ? set.seq : null, sheetIndex: set ? 1 : null,
      fileBase: name, folder: name, charms, placements, poolIds, orders: [...new Set(sh.items.map(x => x[0]))], placedCount: charms.length, charmCount: charms.length, density: sh.full === false ? .3 : .78, releaseFull: sh.full !== false,
      solidIncluded: set ? null : (sh.metal === 'gold14k' || sh.metal === 'gold10k' ? false : null), status: 'complete', endedBy: 'complete', verification: { ok: true }, stock: { wIn: 182 / 72, hIn: 62 / 72, wPt: 182, hPt: 62 },
      outputs: { preview: { path: `${name}/preview.png`, url: image }, ai: { path: `${name}/${name}.ai`, url: srv.blobUrl(`${name}.ai`) } }, backPool: [], engraving: {}, listings: [], names: '',
      ...(set ? { label: { files: [{ path: `${name}_label.png`, url: image, sheet: name, part: 1, parts: 1, orders: [], payload: 'x' }], orders: [] } } : {}),
      ...(sh.plan ? { rosePlanJson: JSON.stringify({ version: 1, full: true, lines: [], shapes: charms.map(c => ({ id: c.id })), stages: [] }), rosePlanHash: 'plan-' + sh.id } : {}),
      updatedAt: ts(now - 60000), createdAt: ts(now - 86400000), archived: false });
  }
  for (const s of SETS_SPEC) st.put(SETS, s.setId, { setId: s.setId, seq: s.seq, day: DAY, runId: RUN, group: 'dispatch', name: `Set-${s.seq}`, folder: `charmnest/sets/${DAY}/Set-${s.seq}`, sheetIds: s.sheets.slice(), materials: ['gold', 'silver'], orders: {}, labels: null,
    labelFiles: s.sheets.map(id => ({ sheetId: id, path: `${id}_label.png`, url: image, sheet: id, part: 1, parts: 1 })), status: 'awaiting review', sheetNos: { gold: 1, silver: 1 }, updatedAt: ts(now - 60000), createdAt: ts(now - 86400000) });
  st.put(RUNS, RUN, { runId: RUN, status: 'running', releasePolicy: 2, lines, setIds: SETS_SPEC.map(s => s.setId), setId: 'set-gss-1', day: DAY });

  await page.goto(`${sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await ready();
  await page.evaluate(async ({ spec, sets, RUN, DAY, TAG }) => {
    await Orders.loadMaps(true); await Cancelled.load(true);
    CN.S.stockPreset = 'custom'; CN.S.settings.stock = Object.assign({}, CN.S.settings.stock, { gold: [182 / 72, 62 / 72], silver: [182 / 72, 62 / 72], gold10k: [182 / 72, 62 / 72], gold14k: [182 / 72, 62 / 72] });
    CN.S.settings.maxFill = 0.5;   // (the test's round charms fill 56% of their plate: a sheet written again as it stands is still full)
    const MM = 72 / 25.4, Rm = 5, Dd = Math.ceil(2 * Rm * MM) + 2, TID = tx => String(5250000000 + tx), pid = (rid, tx, copy) => `${rid}_${TID(tx)}_${copy}`, hashOf = poolId => 'h' + poolId.slice(-6).replace(/_/g, '');
    const SC = 4, DP = Dd * SC, mkBits = () => { const b = new Uint8Array(DP * DP); for (let y = 0; y < DP; y++) for (let x = 0; x < DP; x++) if (Math.hypot(x + .5 - DP / 2, y + .5 - DP / 2) <= DP / 2 - SC) b[y * DP + x] = 1; return b; };
    const mkCharm = (id, extra) => Object.assign({ id, name: id, sku: '', sourceId: 's', ringGeometryVersion: 3, centerPt: [Dd / 2, Dd / 2], bbox: [0, 0, Dd, Dd], outline: { circle: 1, cx: Dd / 2, cy: Dd / 2, r: Rm * MM }, members: [], rMm: Rm, w: DP, h: DP, scale: SC, bits: mkBits(), widthPt: 2 * Rm * MM, heightPt: 2 * Rm * MM, areaPt2: Math.PI * (Rm * MM) ** 2 }, extra || {});
    for (const m of Object.keys(CN.S.sheets)) { const pr = CN.S.sheets[m]; pr.pages.length = 1; const p0 = pr.pages[0]; p0.charms = []; p0.placements = []; p0.sheetId = null; p0.fileBase = null; for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId', 'label', 'keepRelease']) delete p0[k]; p0.status = 'idle'; p0.page = 1; pr.active = 0; }
    B.pool.rows.clear(); B.orders.rows.length = 0; B.orders.byKey.clear(); B.sets.clear();
    B.run = { runId: RUN, day: DAY, setId: 'set-gss-1', setIds: sets.map(s => s.setId), releasePolicy: 2, solidIncluded: {}, step: 'engrave', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    const setObj = {};
    sets.forEach((s, i) => {
      const set = { setId: s.setId, seq: s.seq, day: DAY, runId: RUN, group: 'dispatch', name: `Set-${s.seq}`, folder: `charmnest/sets/${DAY}/Set-${s.seq}`, orders: {}, sheetIds: s.sheets.slice(), materials: ['gold', 'silver'], labelFiles: s.sheets.map(id => ({ sheetId: id, path: `${id}_label.png`, url: '', sheet: id, part: 1, parts: 1 })), labels: null, status: 'awaiting review', committedAt: null, sheetNos: { gold: 1, silver: 1 }, formedAt: Date.now() - 50000 + i };
      setObj[s.setId] = set;
      Sets.byRun().set(Sets.keyOf(RUN, i === sets.length - 1 ? 'dispatch' : 'closed:' + s.setId), set);
    });
    for (const sh of spec) {
      const pr = CN.S.sheets[sh.metal]; let pg = null;
      if (sh.page === 1) pg = pr.pages[0]; else { while (pr.pages.length < sh.page) addPage(sh.metal); pg = pr.pages[sh.page - 1]; }
      pg.charms = []; pg.placements = [];
      sh.items.forEach(([rid, tx, copy], i) => {
        const poolId = pid(rid, tx, copy), id = `${sh.id}-c${i}`;
        pg.charms.push(mkCharm(id, { name: `${rid} · charm`, sku: 'K' + tx, poolId, order: rid, hash: hashOf(poolId), lineKey: `${rid}_${TID(tx)}`, orderDate: Date.now() - 86400000 + i, orderInfo: { receiptId: rid, transactionId: TID(tx), copy, quantity: 1 } }));
        pg.placements.push({ id, cxPt: 16 + (i % 6) * 30, cyPt: 16 + Math.floor(i / 6) * 30, angle: 0, scale: 0.971, wPt: 2 * Rm * MM, hPt: 2 * Rm * MM });
        B.pool.rows.set(poolId, { poolId, orderId: rid, transactionId: TID(tx), lineKey: `${rid}_${TID(tx)}`, sheetId: sh.id, state: 'placed', material: sh.metal, runId: RUN, copy });
      });
      const set = sh.set ? setObj[sh.set] : null, name = set ? `${TAG[sh.metal]}_Oct.27.26_Set-${set.seq}_Sheet-1` : `${TAG[sh.metal]}_working_${sh.id}`;
      Object.assign(pg, { status: 'complete', endedBy: 'complete', sheetId: sh.id, fileBase: name, folderPath: set ? `${set.folder}/${name}` : `charmnest/sheets/${DAY}/${name}`, page: sh.page, runId: RUN, group: 'dispatch', draft: !set, setId: set ? set.setId : null, seq: set ? set.seq : null, setDay: DAY, sheetIndex: set ? 1 : null,
        dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, density: sh.full === false ? .3 : .78, releaseFull: sh.full !== false, verification: { ok: true }, intakeAppend: false, appendOnly: false, releasedAt: Date.now() - 50000,
        outputs: { report: { stock: { wIn: 182 / 72, hIn: 62 / 72, wPt: 182, hPt: 62 } } }, cloud: { ai: 'x', preview: 'x' }, liveInfo: { freePt2: 3000, usablePt2: 182 * 62, placedPt2: 182 * 62 - 3000, placed: sh.items.length, total: sh.items.length } });
      for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'solidPick', 'leftSet']) delete pg[k];
      if (sh.plan) pg.rosePlan = { version: 1, full: true, lines: [], shapes: pg.charms.map(c => ({ id: c.id })), stages: [] };
      CN.renderCard(pg);
    }
    await Gate.assemble(B.run);
  }, { spec: SPEC, sets: SETS_SPEC, RUN, DAY, TAG });
  await sleep(400);

  /* ── readers ── */
  const rec = id => st.doc(SHEETS, id) || {}, setRec = id => st.doc(SETS, id) || {};
  const inSetRec = r => r && r.setId && !r.draft && r.solidIncluded !== false ? r.setId : null;
  const live = id => page.evaluate(id => { const p = CN.allSheets().find(q => q.sheetId === id && !q.recalled); return p ? { setId: p.setId || null, draft: !!p.draft, pick: p.solidPick, dirty: !!p.dirty, status: p.status, problem: p.problem || null, fileBase: p.fileBase, persistedDone: !!p.persistedDone } : null; }, id);
  const inSetLive = l => l && l.setId && !l.draft ? l.setId : null;
  // (the rules the red line is drawn for: SetRepair NOT_SHOWN leaves out R1 "waiting for", R5 and the bookkeeping, and a sheet of the set filling again, Decision 3)
  const violations = () => page.evaluate(() => Gate.violations(B.run).filter(v => !SetRepair.NOT_SHOWN.includes(v.rule)).map(v => ({ rule: v.rule, set: v.set, outside: v.outside || null, message: v.message })));
  const library = async () => {
    await page.evaluate(() => { if (CN.S.mode !== 'library') CN.setMode('library'); return CN.loadLibrary(); });
    await until(() => page.evaluate(() => document.querySelectorAll('#libBody .setCard[data-laser-card="set"]').length >= 3), 30000, 'the set cards are drawn');
    await sleep(700);
    return page.evaluate(() => Object.fromEntries([...document.querySelectorAll('#libBody .setCard[data-laser-card="set"]')].filter(c => c._laserSet).map(c => [c._laserSet.setId, {
      sheets: [...c.querySelectorAll('.librarySheet:not(.goldPending) .libCard[data-id]')].map(n => n.dataset.id),
      pending: [...c.querySelectorAll('[data-gold-pending]')].map(n => n.dataset.goldPending),
      breaks: [...c.querySelectorAll('[data-set-break] [data-set-break-line]')].map(e => e.textContent),
      links: (c.querySelector('[data-set-links]') || {}).textContent || ''
    }])));
  };
  const settledSheet = id => until(async () => { const l = await live(id); return l && !l.dirty && !['nesting', 'finishing', 'queued'].includes(l.status) && (l.persistedDone || l.problem); }, 60000, id + ' is written');
  const quiet = () => until(() => page.evaluate(() => { const o = window.CharmNestOperations; return !(o && o.busy && o.busy('production:' + B.run.runId)) && !Gate.state().membershipPending; }), 60000, 'the run is quiet');

  try {
    /* 1 · the full 14K sheet waits greyed in Set 1; its Approve puts it in */
    try {
      const lib = await library();
      assert((lib['set-gss-1'].pending || []).includes('k14-1'), 'it waits greyed inside Set 1: ' + JSON.stringify(lib['set-gss-1']));
      assert(!lib['set-gss-1'].breaks.length, 'no red line while it waits: ' + lib['set-gss-1'].breaks.join(' | '));
      const btn = await page.$('[data-gold-cut="k14-1"]');
      assert(btn && await btn.evaluate(b => b.dataset.goldFull === '1' && b.textContent.trim() === 'Approve'), 'its press says "Approve" (full: no green line)');
      await btn.click();
      await page.waitForSelector('dialog.setRepairDlg [data-k=go]');
      await page.click('dialog.setRepairDlg [data-k=go]');
      await until(async () => inSetLive(await live('k14-1')) === 'set-gss-1' && inSetRec(rec('k14-1')) === 'set-gss-1', 30000, '14K Sheet 1 joins Set 1');
      await quiet();
      const l = await live('k14-1'), r = rec('k14-1');
      assert(l.pick === true && r.solidIncluded === true && !r.roseCutAt, 'by its own yes, no cut recorded: ' + JSON.stringify({ l, solidIncluded: r.solidIncluded, roseCutAt: r.roseCutAt }));
      assert((setRec('set-gss-1').sheetIds || []).includes('k14-1'), 'the set record lists it');
      const lib2 = await library(), v = await violations();
      assert(lib2['set-gss-1'].sheets.includes('k14-1') && !lib2['set-gss-1'].pending.length, 'drawn as a sheet of Set 1: ' + JSON.stringify(lib2['set-gss-1']));
      assert(!lib2['set-gss-1'].breaks.length && !v.some(x => x.set === 'set-gss-1'), 'no red line: ' + JSON.stringify({ breaks: lib2['set-gss-1'].breaks, v }));
      await shot('1-approved');
      pass('1 · the full 14K sheet waits greyed in Set 1; its "Approve" puts it in (page and record, its own yes, no cut), no red line');
    } catch (e) { fail('1 · approve', e); }

    /* 2 · an artwork edit on a piece of it: written again as it stands; it stays in Set 1 */
    try {
      const poolId = pid(A, 21, 1);
      st.put(SHEETS, 'k14-1', { filesOwed: Date.now() });   // (what the server's artPut marks on the sheet that holds the edited piece)
      const w0 = st.calls.length;
      await page.evaluate(async poolId => {
        const H = CharmNestArtEdit.host(), sh = CN.allSheets().find(p => p.sheetId === 'k14-1'), c = sh.charms.find(x => x.poolId === poolId), at = Date.now();
        const rec = { poolId, face: 'front', v: 1, by: 'Tester', at, key: 'k' + at.toString(36), changed: {}, added: [], removed: [] };
        const next = Object.assign({}, c, { hash: String(c.hash) + '~ae' + at.toString(36), artEdit: { front: { at, key: rec.key, by: 'Tester' } } });
        H.swap(poolId, next, c, rec, 'front');
        await H.changed(poolId, 'front', { charm: next, record: rec });
      }, poolId);
      await settledSheet('k14-1'); await quiet(); await sleep(500);
      const l = await live('k14-1'), r = rec('k14-1'), writes = st.calls.slice(w0).filter(c => c.op === 'putSheet' && c.body.sheet && c.body.sheet.id === 'k14-1');
      const v = await violations(), lib = await library();
      if (process.env.DEBUG) console.log(JSON.stringify({ l, rec: { setId: r.setId, draft: r.draft, solidIncluded: r.solidIncluded, filesOwed: r.filesOwed, fileBase: r.fileBase }, writes: writes.map(c => ({ draft: c.body.sheet.draft, setId: c.body.sheet.setId, saving: c.body.sheet.saving, filesOwed: c.body.sheet.filesOwed })), v, lib: lib['set-gss-1'] }, null, 1));
      assert(writes.some(c => c.body.sheet.saving === true), 'the sheet was written again (the edit owes its files)');
      assert(inSetLive(l) === 'set-gss-1' && l.pick === true, 'this page still holds it in Set 1: ' + JSON.stringify(l));
      assert(inSetRec(r) === 'set-gss-1', 'its record still has it in Set 1: ' + JSON.stringify({ setId: r.setId, draft: r.draft, solidIncluded: r.solidIncluded }));
      assert(!writes.some(c => c.body.sheet.draft === true || ('setId' in c.body.sheet && c.body.sheet.setId === null)), 'no write took it out of its set, even for a moment: ' + JSON.stringify(writes.map(c => [c.body.sheet.draft, c.body.sheet.setId])));
      assert(!l.problem && !r.filesOwed, 'the cloud took the write (an edited piece is still the piece it was: same place, same cut line): ' + JSON.stringify({ problem: l.problem, filesOwed: r.filesOwed }));
      assert(/_Set-1_Sheet-1$/.test(r.fileBase || ''), 'its files keep its set\'s name: ' + r.fileBase);
      assert(!v.some(x => x.set === 'set-gss-1'), 'the set check finds nothing: ' + JSON.stringify(v));
      assert(lib['set-gss-1'].sheets.includes('k14-1') && !lib['set-gss-1'].breaks.length, 'the Library draws it in Set 1 with no red line: ' + JSON.stringify(lib['set-gss-1']));
      assert((setRec('set-gss-1').sheetIds || []).includes('k14-1'), 'the set record still lists it');
      await shot('2-after-edit');
      pass('2 · an artwork edit on a piece of the approved 14K sheet: written again as it stands, it stays in Set 1 on this page and on its record (never drafted), the cloud takes the write, no red line');
    } catch (e) { fail('2 · rewrite of the approved 14K sheet', e); }

    /* 3 · a write the cloud refuses: a GF sheet of a set keeps its place on this page, as its record does */
    try {
      refuse.putSheet = 'gf-1';
      const ok = await page.evaluate(() => { const sh = CN.allSheets().find(p => p.sheetId === 'gf-1'); return window.writeAsPlaced(sh); });
      await settledSheet('gf-1'); await sleep(400);
      refuse.putSheet = null;
      const l = await live('gf-1'), r = rec('gf-1'), v = await violations(), lib = await library();
      if (process.env.DEBUG) console.log(JSON.stringify({ ok, l, v, lib: lib['set-gss-1'] }, null, 1));
      assert(ok === false && /refused for the test/.test(l.problem || ''), 'the write was refused and the card says so: ' + JSON.stringify({ ok, problem: l.problem }));
      assert(inSetRec(r) === 'set-gss-1', 'its record keeps it in Set 1');
      assert(inSetLive(l) === 'set-gss-1' && /_Set-1_Sheet-1$/.test(l.fileBase || ''), 'this page gives it back its place in Set 1: ' + JSON.stringify(l));
      assert(!v.some(x => x.set === 'set-gss-1'), 'the set check finds nothing: ' + JSON.stringify(v));
      assert(!lib['set-gss-1'].breaks.length, 'no red line: ' + lib['set-gss-1'].breaks.join(' | '));
      pass('3 · a write the cloud refuses (forced): the GF sheet keeps its place in Set 1 on this page as its record does, no red line (before, this page drafted it and the set check called it outside its set)');
    } catch (e) { fail('3 · refused write', e); refuse.putSheet = null; }

    /* 4 · another computer approves the 10K sheet into Set 2: this page takes its place from the record; its older list of Set 2 never takes it off */
    try {
      const l0 = await live('k10-1');
      assert(!inSetLive(l0), 'this page holds the 10K sheet in no set: ' + JSON.stringify(l0));
      st.put(SHEETS, 'k10-1', { draft: false, setId: 'set-gss-2', setSeq: 2, sheetIndex: 1, solidIncluded: true, fileBase: '10K_Oct.27.26_Set-2_Sheet-1', folder: '10K_Oct.27.26_Set-2_Sheet-1', updatedAt: ts(Date.now()) });
      st.put(SETS, 'set-gss-2', { sheetIds: ['gf-2', 'ss-2', 'k10-1'], materials: ['gold', 'silver', 'gold10k'], labelFiles: (setRec('set-gss-2').labelFiles || []).concat([{ sheetId: 'k10-1', path: 'k10-1_label.png', url: image, sheet: 'k10-1', part: 1, parts: 1 }]), updatedAt: ts(Date.now()) });
      // the page's own list of Set 2 (older: without the 10K sheet) saved by this page, as its next look at the set does when anything of it changes
      await page.evaluate(async () => { const set = Sets.ofRun(B.run.runId).find(s => s.setId === 'set-gss-2'); await Sets.save(Object.assign({}, set, { sheetIds: set.sheetIds.filter(i => i !== 'k10-1'), labelFiles: set.labelFiles.filter(f => f.sheetId !== 'k10-1') })); });
      const sr = setRec('set-gss-2');
      assert((sr.sheetIds || []).includes('k10-1') && (sr.labelFiles || []).some(f => f.sheetId === 'k10-1') && (sr.materials || []).includes('gold10k'), "an older list saved by this page does not take it off the set's record: " + JSON.stringify({ ids: sr.sheetIds, materials: sr.materials }));
      const lib = await library();
      await until(async () => inSetLive(await live('k10-1')) === 'set-gss-2', 15000, 'this page takes its place in Set 2 from the record');
      const l = await live('k10-1'), lib2 = await library(), v = await violations();
      if (process.env.DEBUG) console.log(JSON.stringify({ l, cards: await page.evaluate(() => [...document.querySelectorAll('#libBody .setCard')].map(c => ({ title: (c.querySelector('[data-set-title], .nm') || {}).textContent, set: c._laserSet && c._laserSet.setId, ids: [...c.querySelectorAll('.libCard[data-id]')].map(n => n.dataset.id) }))), row: await page.evaluate(() => (CN.S.library.rows || []).filter(r => r.id === 'k10-1').map(r => ({ setId: r.setId, draft: r.draft, solidIncluded: r.solidIncluded, archived: r.archived }))) }, null, 1));
      assert(l.pick === true, 'with its own yes, as its record says: ' + JSON.stringify(l));
      assert(await page.evaluate(() => Sets.ofRun(B.run.runId).find(s => s.setId === 'set-gss-2').sheetIds.includes('k10-1')), "this page's list of Set 2 has it now");
      assert(lib2['set-gss-2'].sheets.includes('k10-1') && !lib2['set-gss-2'].pending.includes('k10-1') && !lib2['set-gss-2'].breaks.length, 'the Library draws it as a sheet of Set 2, not waiting: ' + JSON.stringify({ first: lib['set-gss-2'], now: lib2['set-gss-2'] }));
      assert(!v.some(x => x.set === 'set-gss-2'), 'the set check finds nothing: ' + JSON.stringify(v));
      await page.evaluate(() => Gate.assemble(B.run)); await quiet(); await sleep(300);
      assert(inSetRec(rec('k10-1')) === 'set-gss-2' && (setRec('set-gss-2').sheetIds || []).includes('k10-1') && inSetLive(await live('k10-1')) === 'set-gss-2', "this page's next look at the sets keeps it there, record and page");
      pass('4 · another computer approves the 10K sheet into Set 2: this page takes its place from the record when the Library reads it (its own yes too), its older list of Set 2 never takes it off the set\'s record, its next look keeps it');
    } catch (e) { fail('4 · another computer', e); }

    /* 5 · the other 14K sheet included and taken out again: the approved one stays */
    try {
      await page.evaluate(async () => { const sh = CN.allSheets().find(p => p.sheetId === 'k14-2'); await Gate.changeMembership('gold14k', true, sh).catch(() => {}); });
      await quiet();
      await page.evaluate(async () => { const sh = CN.allSheets().find(p => p.sheetId === 'k14-2'); await Gate.changeMembership('gold14k', false, sh).catch(() => {}); });
      await quiet(); await sleep(300);
      const l = await live('k14-1'), v = await violations();
      assert(inSetLive(l) === 'set-gss-1' && l.pick === true && inSetRec(rec('k14-1')) === 'set-gss-1', 'the approved 14K sheet stays in Set 1 with its own yes: ' + JSON.stringify(l));
      assert(!v.some(x => x.set === 'set-gss-1'), 'nothing broken: ' + JSON.stringify(v));
      pass('5 · the other 14K sheet included and taken out again: the approved 14K Sheet 1 stays in Set 1 (page and record), nothing broken');
    } catch (e) { fail('5 · the other 14K sheet', e); }

    /* 7 · the note under Set 1 */
    try {
      const lib = await library(), t = lib['set-gss-1'].links;
      const m = /14K Sheet 1 is in this set only: ([^.]*)/.exec(t);
      assert(m, 'the note says what 14K Sheet 1 shares with sheets of other sets: ' + t);
      assert(new RegExp(`order ${C}, shared with GF Sheet 1 of Set 2, and order ${G}, shared with GF Sheet 1 of Set 3, ship in two parts`).test(m[0]), 'each sheet of another set with its set, and its own order: ' + m[0]);
      assert(!/GF Sheet 1 and GF Sheet 1/.test(t), 'never "GF Sheet 1 and GF Sheet 1": ' + t);
      pass(`7 · the note under Set 1: "${m[0]}"`);
    } catch (e) { fail('7 · the note', e); }

    /* 6 · a reload: the workspace checkpoint brings it back in Set 1, no red line */
    try {
      await page.evaluate(() => Session.flushNow());
      await sleep(300);
      await page.reload({ waitUntil: 'load' });
      await ready();
      await until(() => page.evaluate(() => !!B.run && CN.allSheets().some(p => p.sheetId === 'k14-1')), 30000, 'the workspace is restored');
      const l = await live('k14-1'), v = await violations(), lib = await library();
      assert(inSetLive(l) === 'set-gss-1' && l.pick === true, 'still in Set 1 with its own yes: ' + JSON.stringify(l));
      assert(!v.some(x => x.set === 'set-gss-1') && !lib['set-gss-1'].breaks.length, 'no red line: ' + JSON.stringify({ v, breaks: lib['set-gss-1'].breaks }));
      await shot('6-reload');
      pass('6 · after a reload (the workspace checkpoint) 14K Sheet 1 is in Set 1 with its own yes, no red line');
    } catch (e) { fail('6 · reload', e); }

    const errs = errors.concat(await page.evaluate(() => window.__errs || []).catch(() => []));
    if (errs.length) fail('no page error', new Error(errs.slice(0, 4).join(' || ')));
    else pass('no page error');
  } finally { await browser.close(); srv.close(); }
  if (failures.length) { console.log(`\n${failures.length} FAILED, ${results.length} passed`); for (const f of failures) console.log('  - ' + f); process.exit(1); }
  console.log(`\nPASS: gold-set-stale: ${results.length} checks: a full 14K sheet approved into its set stays there on the page as on its record (an artwork edit writes it again in place; a refused write keeps the place; another computer's approval is taken from the record; a reload keeps it), no false red line, and the note names each split sheet with its set`);
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
