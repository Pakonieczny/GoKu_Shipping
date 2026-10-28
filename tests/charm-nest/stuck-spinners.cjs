// STUCKSPIN (Paul, 28 Oct 2026, the sandbox, the sheet window of "14K Draft 2 · 14K Gold · filling · joins a set when full", Manual Control on): "I'm getting a lot of issues and errors here. Just for
// usability it seems like things are working, but they're not actually working correctly. The saving icon seems to never go away. It just keeps on spinning, also, the QR code button does not produce
// a QR code. It just sits there spinning much like the saving UI pill." With the toasts "Run stopped: GF Draft 9 cannot go into Set 1: Set 1 would be bigger than the minimum ..." and "No QR label
// made: Waits for a completed SS sheet ...". The records said: the 14K sheet in Set 3 with its QR label made (the page: a draft, "No QR yet"); the run stopped at its checkpoint.
//
// Every part is the page's REAL code, in one real Chromium page over the local stand-in for the site (bridge-server.cjs: the real charmNestLibrary handler on an in-memory Firestore; nothing leaves
// the machine): the sheet window, its Manual Control and Make QR label, the run's assembly and the server's set rules (putSheet's joinCheck), writeAsPlaced / finishNest / persistSheet. The stand-ins:
// the sheet's file making and its two checks (no traced designs here), and the nest search (what is pinned is placed where it was pinned), which keeps the real nest's first gate (a stopped run's
// sheet waits for Resume: heldForResume).
//   d  the window opened on a 14K sheet the record holds in Set 2 (with its label) while this page holds it as a draft: the page follows the record
//   a  the run is STOPPED: a charm moved by hand on a 14K sheet, Manual Control off: the save ends, written, in seconds (it waited for Resume: four minutes of "Saving…", nothing written);
//      the record keeps its set
//   a2 the run's own turn never comes back (a set look that does not end): the save still ends, with the reason under its step, never an endless spinner
//   b  Make QR label while the run's turn never comes back: the button stops spinning and says why under it
//   c  a full sheet of Set 1 being written again: the run's look does not send GF Draft 9 into Set 1; and when the cloud refuses such an automatic join, the run goes on (no "Run stopped"), the sheet
//      waits outside with the cloud's words
//   node tests/charm-nest/stuck-spinners.cjs      (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>, SHOTS=<dir>)
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const { start, Timestamp } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || ''; if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const SHEETS = 'Charm_Nest_Sheets', SETS = 'Charm_Nest_Sets', POOL = 'Charm_Pool', RUNS = 'Charm_Nest_Runs';
const RUN = 'run-2026-10-26-stk', DAY = '2026-10-28';
const TID = tx => String(5260000000 + tx), pid = (rid, tx, copy = 1) => `${rid}_${TID(tx)}_${copy}`;
const R = n => '43' + String(n).padStart(8, '0');
const SETS_SPEC = [{ setId: 'set-stk-1', seq: 1, sheets: ['gf-1', 'ss-1'] }, { setId: 'set-stk-2', seq: 2, sheets: ['gf-2', 'ss-2', 'k14-2'] }];
const row = (n, base) => Array.from({ length: n }, (_, i) => [R(base + i), base + i, 1]);
const SPEC = [
  { id: 'gf-1', metal: 'gold', page: 1, set: 'set-stk-1', items: row(10, 10) },
  { id: 'ss-1', metal: 'silver', page: 1, set: 'set-stk-1', items: row(10, 30) },
  { id: 'gf-2', metal: 'gold', page: 2, set: 'set-stk-2', items: row(10, 50) },
  { id: 'ss-2', metal: 'silver', page: 2, set: 'set-stk-2', items: row(10, 70) },
  { id: 'gf-9', metal: 'gold', page: 9, items: row(10, 90) },                                   // GF Draft 9: full, waits for a completed SS sheet
  { id: 'k14-2', metal: 'gold14k', page: 2, full: false, recSet: 'set-stk-2', items: row(3, 120) },   // the record: in Set 2 by a person's Approve, label made; the page: a draft
  { id: 'k14-4', metal: 'gold14k', page: 4, full: false, items: row(3, 140) }
];
const TAG = { gold: 'GF', silver: 'SS', gold14k: '14K' };
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
    return r.continue();
  });
  await ctx.addInitScript(() => {
    try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', pollOrders: 'off' })); localStorage.setItem('cn.employee', 'Tester'); } catch (_) { /* about:blank */ }
    window.prompt = () => 'Prompted'; window.confirm = () => true; window.alert = () => {};
    window.__errs = []; addEventListener('unhandledrejection', e => window.__errs.push('unhandledrejection: ' + String(e.reason && e.reason.message || e.reason).slice(0, 300)));
    window.__toasts = [];
    const iv = setInterval(() => {
      if (typeof window.finishNest !== 'function' || typeof window.startNest !== 'function' || !window.CharmNestPDF || window.__stubbed) return;
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
      // the nest search, as the test has it: what is pinned is placed where it was pinned and the sheet is written as it stands (writeAsPlaced: the real finishNest and persistSheet).
      // Its first gate is the real one (startNestReady): a sheet of a stopped run waits for Resume
      const stub = sh => { if (typeof heldForResume === 'function' && heldForResume(sh)) return holdForResume(sh); window.__nests = (window.__nests || 0) + 1; for (const c of sh.charms) if (c.pinned) { const p = sh.placements.find(q => q.id === c.id); if (p) Object.assign(p, { cxPt: c.pinned.cxPt, cyPt: c.pinned.cyPt, angle: c.pinned.angle || 0 }); } delete sh._personWrite; sh.status = 'complete'; return window.writeAsPlaced(sh); };
      stub.__stub = true; window.startNest = stub;
      const t0 = window.toast; if (typeof t0 === 'function') window.toast = (m, k, ms) => { window.__toasts.push(String(m)); return t0(m, k, ms); };
    }, 0);
  });
  const page = await ctx.newPage(), errors = [];
  page.setDefaultTimeout(30000);
  page.on('pageerror', e => errors.push('page: ' + String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')));
  page.on('console', m => { const t = m.text(); if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::|CORS policy/.test(t)) errors.push('console: ' + t.slice(0, 400)); });
  const until = async (fn, ms = 30000, what = '') => { const t0 = Date.now(); for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await sleep(100); } };
  const shot = async name => { if (!SHOTS) return; try { await page.screenshot({ path: path.join(SHOTS, name + '.png'), fullPage: false }); } catch (_) {} };
  const ready = () => page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Gate && window.SetRepair && window.SheetWin && window.CNManual && window.__stubbed, null, { timeout: 60000 });

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
    const set = SETS_SPEC.find(x => x.setId === (sh.set || sh.recSet)), name = set ? `${TAG[sh.metal]}_Oct.28.26_Set-${set.seq}_Sheet-1` : `${TAG[sh.metal]}_working_${sh.id}`;
    st.put(SHEETS, sh.id, { id: sh.id, metal: sh.metal, metalLabel: TAG[sh.metal], day: DAY, runId: RUN, page: sh.page, draft: !set, setId: set ? set.setId : null, setSeq: set ? set.seq : null, sheetIndex: set ? 1 : null,
      fileBase: name, folder: name, charms, placements, poolIds, orders: [...new Set(sh.items.map(x => x[0]))], placedCount: charms.length, charmCount: charms.length, density: sh.full === false ? .3 : .78, releaseFull: sh.full !== false,
      solidIncluded: sh.metal === 'gold14k' ? !!set : null, status: 'complete', endedBy: 'complete', verification: { ok: true }, stock: { wIn: 182 / 72, hIn: 62 / 72, wPt: 182, hPt: 62 },
      outputs: { preview: { path: `${name}/preview.png`, url: image }, ai: { path: `${name}/${name}.ai`, url: srv.blobUrl(`${name}.ai`) } }, backPool: [], engraving: {}, listings: [], names: '',
      ...(set ? { label: { files: [{ path: `${name}_label.png`, url: image, sheet: name, part: 1, parts: 1, orders: [], payload: 'x' }], orders: [] } } : {}),
      updatedAt: ts(now - 60000), createdAt: ts(now - 86400000), archived: false });
  }
  for (const s of SETS_SPEC) st.put(SETS, s.setId, { setId: s.setId, seq: s.seq, day: DAY, runId: RUN, group: 'dispatch', name: `Set-${s.seq}`, folder: `charmnest/sets/${DAY}/Set-${s.seq}`, sheetIds: s.sheets.slice(), materials: ['gold', 'silver'], orders: {}, labels: null,
    labelFiles: s.sheets.map(id => ({ sheetId: id, path: `${id}_label.png`, url: image, sheet: id, part: 1, parts: 1 })), status: 'awaiting review', sheetNos: { gold: 1, silver: 1 }, updatedAt: ts(now - 60000), createdAt: ts(now - 86400000) });
  st.put(RUNS, RUN, { runId: RUN, status: 'running', releasePolicy: 2, lines, setIds: SETS_SPEC.map(s => s.setId), setId: 'set-stk-1', day: DAY });

  await page.goto(`${sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await ready();
  await page.evaluate(async ({ spec, sets, RUN, DAY, TAG }) => {
    await Orders.loadMaps(true); await Cancelled.load(true);
    CN.S.stockPreset = 'custom'; CN.S.settings.stock = Object.assign({}, CN.S.settings.stock, { gold: [182 / 72, 62 / 72], silver: [182 / 72, 62 / 72], gold10k: [182 / 72, 62 / 72], gold14k: [182 / 72, 62 / 72] });
    CN.S.settings.maxFill = 0.5;
    const MM = 72 / 25.4, Rm = 5, Dd = Math.ceil(2 * Rm * MM) + 2, TID = tx => String(5260000000 + tx), pid = (rid, tx, copy) => `${rid}_${TID(tx)}_${copy}`, hashOf = poolId => 'h' + poolId.slice(-6).replace(/_/g, '');
    const SC = 4, DP = Dd * SC, mkBits = () => { const b = new Uint8Array(DP * DP); for (let y = 0; y < DP; y++) for (let x = 0; x < DP; x++) if (Math.hypot(x + .5 - DP / 2, y + .5 - DP / 2) <= DP / 2 - SC) b[y * DP + x] = 1; return b; };
    const SQ = Rm * MM * 0.95, sq = [[['m', [Dd / 2 - SQ, Dd / 2 - SQ]], ['l', [Dd / 2 + SQ, Dd / 2 - SQ]], ['l', [Dd / 2 + SQ, Dd / 2 + SQ]], ['l', [Dd / 2 - SQ, Dd / 2 + SQ]], ['l', [Dd / 2 - SQ, Dd / 2 - SQ]]]];
    const mkCharm = (id, extra) => Object.assign({ id, name: id, sku: '', sourceId: 's', ringGeometryVersion: 3, centerPt: [Dd / 2, Dd / 2], bbox: [0, 0, Dd, Dd], outline: { circle: 1, cx: Dd / 2, cy: Dd / 2, r: Rm * MM, subpaths: sq }, members: [], rMm: Rm, w: DP, h: DP, scale: SC, bits: mkBits(), widthPt: 2 * Rm * MM, heightPt: 2 * Rm * MM, areaPt2: Math.PI * (Rm * MM) ** 2 }, extra || {});
    for (const m of Object.keys(CN.S.sheets)) { const pr = CN.S.sheets[m]; pr.pages.length = 1; const p0 = pr.pages[0]; p0.charms = []; p0.placements = []; p0.sheetId = null; p0.fileBase = null; for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId', 'label', 'keepRelease']) delete p0[k]; p0.status = 'idle'; p0.page = 1; pr.active = 0; }
    B.pool.rows.clear(); B.orders.rows.length = 0; B.orders.byKey.clear(); B.sets.clear();
    B.run = { runId: RUN, day: DAY, setId: 'set-stk-1', setIds: sets.map(s => s.setId), releasePolicy: 2, solidIncluded: {}, step: 'engrave', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    const setObj = {};
    sets.forEach((s, i) => {
      // (the page's copy of Set 2 is older than its record: it lists the GF and SS sheets only, as a page that drafted the 14K sheet for a write that did not go through)
      const mine = s.sheets.filter(id => !/^k14/.test(id));
      const set = { setId: s.setId, seq: s.seq, day: DAY, runId: RUN, group: 'dispatch', name: `Set-${s.seq}`, folder: `charmnest/sets/${DAY}/Set-${s.seq}`, orders: {}, sheetIds: mine, materials: ['gold', 'silver'], labelFiles: mine.map(id => ({ sheetId: id, path: `${id}_label.png`, url: '', sheet: id, part: 1, parts: 1 })), labels: null, status: 'awaiting review', committedAt: null, sheetNos: { gold: 1, silver: 1 }, formedAt: Date.now() - 50000 + i };
      setObj[s.setId] = set;
      Sets.byRun().set(Sets.keyOf(RUN, i === sets.length - 1 ? 'dispatch' : 'closed:' + s.setId), set);
    });
    for (const sh of spec) {
      const pr = CN.S.sheets[sh.metal]; let pg = null;
      if (pr.pages.length === 1 && !pr.pages[0].sheetId) pg = pr.pages[0]; else { addPage(sh.metal); pg = pr.pages[pr.pages.length - 1]; }
      pg.charms = []; pg.placements = [];
      sh.items.forEach(([rid, tx, copy], i) => {
        const poolId = pid(rid, tx, copy), id = `${sh.id}-c${i}`;
        pg.charms.push(mkCharm(id, { name: `${rid} · charm`, sku: 'K' + tx, poolId, order: rid, hash: hashOf(poolId), lineKey: `${rid}_${TID(tx)}`, orderDate: Date.now() - 86400000 + i, orderInfo: { receiptId: rid, transactionId: TID(tx), copy, quantity: 1 } }));
        pg.placements.push({ id, cxPt: 16 + (i % 6) * 30, cyPt: 16 + Math.floor(i / 6) * 30, angle: 0, scale: 0.971, wPt: 2 * Rm * MM, hPt: 2 * Rm * MM });
        B.pool.rows.set(poolId, { poolId, orderId: rid, transactionId: TID(tx), lineKey: `${rid}_${TID(tx)}`, sheetId: sh.id, state: 'placed', material: sh.metal, runId: RUN, copy });
      });
      const set = sh.set ? setObj[sh.set] : null, name = set ? `${TAG[sh.metal]}_Oct.28.26_Set-${set.seq}_Sheet-1` : `${TAG[sh.metal]}_working_${sh.id}`;
      Object.assign(pg, { status: 'complete', endedBy: 'complete', sheetId: sh.id, fileBase: name, folderPath: set ? `${set.folder}/${name}` : `charmnest/sheets/${DAY}/${name}`, page: sh.page, runId: RUN, group: 'dispatch', draft: !set, setId: set ? set.setId : null, seq: set ? set.seq : null, setDay: DAY, sheetIndex: set ? 1 : null,
        dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, density: sh.full === false ? .3 : .78, releaseFull: sh.full !== false, verification: { ok: true }, intakeAppend: false, appendOnly: false, releasedAt: Date.now() - 50000,
        outputs: { report: { stock: { wIn: 182 / 72, hIn: 62 / 72, wPt: 182, hPt: 62 } } }, cloud: { ai: 'x', preview: 'x' }, liveInfo: { freePt2: 3000, usablePt2: 182 * 62, placedPt2: 182 * 62 - 3000, placed: sh.items.length, total: sh.items.length } });
      for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'solidPick', 'leftSet']) delete pg[k];
      CN.renderCard(pg);
    }
  }, { spec: SPEC, sets: SETS_SPEC, RUN, DAY, TAG });
  await sleep(400);

  /* ── readers and hands ── */
  const rec = id => st.doc(SHEETS, id) || {};
  const live = id => page.evaluate(id => { const p = CN.allSheets().find(q => q.sheetId === id && !q.recalled); return p ? { setId: p.setId || null, draft: !!p.draft, pick: p.solidPick, dirty: !!p.dirty, status: p.status, stage: p.stage || '', problem: p.problem || null, persistedDone: !!p.persistedDone, setWait: p.setWait || '' } : null; }, id);
  const openSheet = async id => {
    await page.evaluate(id => SheetWin.open(id), id);
    await until(() => page.evaluate(id => { const W = SheetWin.hopKit.W; return SheetWin.isOpen() && W.id === id && W.rec && W.geom && !W.flying && W.pieces.length > 0; }, id), 30000, 'the sheet window shows ' + id);
    await sleep(400);
  };
  const mcBtn = () => page.evaluate(() => { const b = document.querySelector('.sheetWin [data-r=manual]'); return b ? { hidden: b.hidden, disabled: b.getAttribute('aria-disabled'), why: b.dataset.why || b.title || '' } : null; });
  const mcOn = () => page.evaluate(() => !!CNManual._MC.on);
  const flow = () => page.evaluate(() => { const w = SheetWin.hopKit.W.flow || SheetWin.hopKit.W.work; return w ? { live: !!SheetWin.hopKit.W.flow, state: w.state, title: w.title, note: String(w.note || ''), steps: (w.steps || []).map(s => [s.state, s.detail || '']) } : null; });
  async function manualOn() {
    await until(async () => { const b = await mcBtn(); return b && !b.hidden && b.disabled === 'false'; }, 15000, 'Manual Control can start: ' + JSON.stringify(await mcBtn()));
    await page.click('.sheetWin [data-r=manual]');
    await until(async () => (await mcOn()) && await page.evaluate(() => !CNManual._MC.claiming), 10000, 'Manual Control on');
  }
  async function mcMove(poolId, to) {
    const a = await page.evaluate(id => { const K = SheetWin.hopKit, W = K.W, x = W.byPool.get(id); const r = W.el.fx.getBoundingClientRect(), kc = W.k / W.dpr, R0 = W.R / W.dpr; return { x: r.left + R0 + x.p.cxPt * kc, y: r.top + R0 + x.p.cyPt * kc }; }, poolId);
    const b = await page.evaluate(([x, y]) => CNManual._client(x, y), [to.x, to.y]);
    await page.mouse.move(a.x, a.y); await page.mouse.down();
    for (let i = 1; i <= 10; i++) await page.mouse.move(a.x + (b[0] - a.x) * i / 10, a.y + (b[1] - a.y) * i / 10);
    await sleep(80); await page.mouse.up(); await sleep(400);
    return page.evaluate(id => { const M = CNManual._MC, x = SheetWin.hopKit.W.byPool.get(id); return x ? M.edits.get(x.id) || null : null; }, poolId);
  }
  const recAt = (sheetId, poolId) => { const r = rec(sheetId), c = (r.charms || []).find(z => z.poolId === poolId), p = c && (r.placements || []).find(z => z.id === c.id); return p ? { x: p.cxPt, y: p.cyPt } : null; };
  // (the run's own turn held by work that never ends: a set look, a sheet's save. resolve: let it go)
  const stickRun = () => page.evaluate(RUN => { window.__unstick = null; CharmNestOperations.run({ key: 'stuck-test', label: 'A turn that never ends (test)', resources: ['production:' + RUN] }, () => new Promise(res => { window.__unstick = res; })); return true; }, RUN);
  const unstick = () => page.evaluate(() => { if (window.__unstick) window.__unstick(); window.__unstick = null; });

  try {
    /* d · the page follows a record that put the sheet in a set */
    try {
      assert(rec('k14-2').setId === 'set-stk-2' && rec('k14-2').draft === false, 'the record holds 14K Draft 2 in Set 2');
      const l0 = await live('k14-2'); assert(l0.draft && !l0.setId, 'the page holds it as a draft: ' + JSON.stringify(l0));
      await openSheet('k14-2');
      await until(async () => { const l = await live('k14-2'); return l.setId === 'set-stk-2' && !l.draft; }, 15000, 'the page follows its record: ' + JSON.stringify(await live('k14-2')));
      const l = await live('k14-2');
      assert(l.pick === true, 'its own yes, saved with it (solidIncluded), is held: it stays in its set when written again');
      assert(await page.evaluate(() => Sets.ofRun(B.run.runId).find(s => s.setId === 'set-stk-2').sheetIds.includes('k14-2')), 'Set 2 on this page lists it');
      await until(() => page.evaluate(() => /QR label/.test(SheetWin.hopKit.W.el.foot.textContent) && !!SheetWin.hopKit.W.el.foot.querySelector('img[data-label-sheet]')), 15000, 'the window shows its QR label');
      pass('d · the window opened on a 14K sheet its record holds in Set 2 (label made) while this page held it as a draft: the page follows the record, its QR label is shown');
    } catch (e) { fail('d · page follows the record', e); }
    await shot('d');

    /* a · the run stopped: Manual Control's save is written, not held for Resume */
    try {
      await page.evaluate(() => { B.run.status = 'stopped'; B.run.stoppedBy = 'GF Draft 9 cannot go into Set 1 (test)'; B.run.stopKind = 'error'; });
      if (!(await page.evaluate(() => SheetWin.isOpen() && SheetWin.current() === 'k14-2'))) await openSheet('k14-2');
      await manualOn();
      const q = pid(R(121), 121), to = { x: 136, y: 46 }, e = await mcMove(q, to);
      assert(e && Math.hypot(e.cxPt - to.x, e.cyPt - to.y) < 1.5, 'moved by hand: ' + JSON.stringify(e));
      const t0 = Date.now();
      await page.click('.sheetWin [data-r=manual]');
      await until(async () => !(await mcOn()), 10000, 'Manual Control off');
      await until(async () => { const f = await flow(); return f && !f.live; }, 45000, 'the save ends (it waited for Resume: ' + JSON.stringify(await flow()) + ' · ' + JSON.stringify(await live('k14-2')) + ')');
      const f = await flow(), took = Date.now() - t0;
      assert(f.state === 'done', 'the save says it is done: ' + JSON.stringify(f));
      await until(() => { const p = recAt('k14-2', q); return p && Math.hypot(p.x - to.x, p.y - to.y) < .01; }, 15000, 'the record holds the charm where it was put: ' + JSON.stringify(recAt('k14-2', q)));
      assert(rec('k14-2').setId === 'set-stk-2' && rec('k14-2').draft === false, 'the record keeps its set: ' + JSON.stringify([rec('k14-2').setId, rec('k14-2').draft]));
      const l = await live('k14-2'); assert(l.status !== 'queued' && !/Resume/i.test(l.stage), 'the sheet is not left waiting for Resume: ' + JSON.stringify(l));
      pass(`a · the run stopped: a charm moved by hand on the 14K sheet, Manual Control off: written in ${Math.round(took / 100) / 10} s (it used to wait for Resume), the record keeps Set 2`);
    } catch (e) { fail('a · Manual Control save while the run is stopped', e); }
    await shot('a');

    /* a2 · the run's own turn never comes back: the save still ends, saying why */
    try {
      await page.evaluate(() => { window.CN_SPIN_SCALE = 0.05; });
      if (!(await page.evaluate(() => SheetWin.isOpen() && SheetWin.current() === 'k14-2'))) await openSheet('k14-2');
      await manualOn();
      await stickRun();
      const q = pid(R(122), 122), to = { x: 136, y: 16 };
      const e = await mcMove(q, to); assert(e, 'moved by hand');
      await page.click('.sheetWin [data-r=manual]');
      await until(async () => { const f = await flow(); return f && !f.live; }, 40000, 'the save ends although the run\'s turn never comes back: ' + JSON.stringify(await flow()));
      const f = await flow();
      assert(['done', 'failed'].includes(f.state), 'it ends in an outcome: ' + JSON.stringify(f));
      assert(f.steps.some(([s, d]) => s === 'bad' && /did not answer|not .* in/i.test(d)) || f.state === 'failed', 'the step that waited says why in plain words: ' + JSON.stringify(f.steps));
      assert(!(await page.evaluate(() => [...document.querySelectorAll('.sheetWin .mhChip')].some(n => !n.hidden && !n.classList.contains('ok') && !n.classList.contains('bad')))), 'no "Saving…" chip left spinning');
      pass('a2 · the run\'s own turn never comes back (a set look that does not end): the Manual Control save still ends, the waiting step says why');
    } catch (e) { fail('a2 · the save ends when the run\'s turn never comes back', e); }
    await shot('a2');

    /* b · Make QR label while the run's turn never comes back: the button stops spinning and says why */
    try {
      await openSheet('k14-4');
      await until(() => page.evaluate(() => !!SheetWin.hopKit.W.el.foot.querySelector('[data-r2=label]')), 15000, 'Make QR label is offered: ' + await page.evaluate(() => SheetWin.hopKit.W.el.foot.textContent));
      await page.click('.sheetWin [data-r2=label]');
      await until(() => page.evaluate(() => !!SheetWin.hopKit.W.el.foot.querySelector('.remaking')), 5000, 'it spins while it works');
      await until(() => page.evaluate(() => { const f = SheetWin.hopKit.W.el.foot; return !f.querySelector('.remaking') && /No QR label made: /.test(f.textContent); }), 30000, 'the button stops spinning and says why: ' + await page.evaluate(() => SheetWin.hopKit.W.el.foot.textContent));
      const b = await page.evaluate(() => { const f = SheetWin.hopKit.W.el.foot, btn = f.querySelector('[data-r2=label]'); return { text: f.textContent, btn: btn ? !btn.disabled : null }; });
      assert(b.btn !== false, 'its button can be pressed again: ' + JSON.stringify(b));
      pass('b · Make QR label while the run\'s turn never comes back: the button stops spinning and says why under it ("' + b.text.replace(/\s+/g, ' ').slice(0, 140) + '")');
    } catch (e) { fail('b · Make QR label always ends', e); }
    await shot('b');
    await unstick(); await page.evaluate(() => { window.CN_SPIN_SCALE = 1; });
    await until(() => page.evaluate(() => !CharmNestOperations.busy('production:' + B.run.runId)), 60000, 'the run\'s turn is free again');
    await page.evaluate(() => SheetWin.close && SheetWin.close()); await sleep(600);

    /* c · a full sheet of Set 1 written again: GF Draft 9 stays out; a refused automatic join does not stop the run */
    try {
      await page.evaluate(() => { B.run.status = 'running'; B.run.stoppedBy = null; });
      const gf1 = () => page.evaluate(() => CN.allSheets().find(p => p.sheetId === 'gf-1'));
      // (1) GF Sheet 1's nest finishing: releaseFull is false on this page for those seconds (finishNest), its record full
      await page.evaluate(() => { const p = CN.allSheets().find(q => q.sheetId === 'gf-1'); p._fullWhileWritten = true; p.releaseFull = false; p.status = 'finishing'; p.persistedDone = false; p.keepRelease = { full: true, at: Date.now() }; });
      const calls0 = st.calls.length;
      const r1 = await page.evaluate(() => Gate.assemble(B.run).then(() => 'ok', e => 'threw: ' + e.message));
      assert(r1 === 'ok', 'the look ends without an error: ' + r1);
      const l9 = await live('gf-9'); assert(l9.draft && !l9.setId, 'GF Draft 9 is not sent into Set 1: ' + JSON.stringify(l9));
      assert(!st.calls.slice(calls0).some(c => c.op === 'putSheet' && c.body && c.body.sheet && c.body.sheet.id === 'gf-9' && c.body.sheet.setId), 'nothing asked the cloud to put GF Draft 9 in a set');
      await page.evaluate(() => { const p = CN.allSheets().find(q => q.sheetId === 'gf-1'); delete p._fullWhileWritten; p.releaseFull = true; p.status = 'complete'; p.persistedDone = true; delete p.keepRelease; });
      // (2) this page's copy behind its records (a sheet of Set 1 kept for a rewrite that the page reads as not full): the look asks; the cloud refuses (R7); the run goes on
      await page.evaluate(() => { const p = CN.allSheets().find(q => q.sheetId === 'gf-1'); p.releaseFull = false; p.keepRelease = { full: false, at: Date.now() }; });
      const r2 = await page.evaluate(() => Gate.assemble(B.run).then(() => 'ok', e => 'threw: ' + e.message));
      assert(r2 === 'ok', 'a join the cloud refuses for the set rules does not throw out of the look (it stopped the run): ' + r2);
      const asked = st.calls.filter(c => c.op === 'putSheet' && c.body && c.body.sheet && c.body.sheet.id === 'gf-9' && c.body.sheet.setId === 'set-stk-1').length;
      const l9b = await live('gf-9');
      assert(l9b.draft && !l9b.setId, 'GF Draft 9 stays out on the page: ' + JSON.stringify(l9b));
      assert(rec('gf-9').draft === true && !rec('gf-9').setId, 'and on its record');
      assert(asked === 0 || /cannot go into Set|bigger than the minimum/.test(l9b.setWait), 'when the cloud was asked and refused, the sheet waits with its words: ' + JSON.stringify(l9b));
      const r3 = await page.evaluate(() => Gate.assemble(B.run).then(() => 'ok', e => 'threw: ' + e.message));
      const asked2 = st.calls.filter(c => c.op === 'putSheet' && c.body && c.body.sheet && c.body.sheet.id === 'gf-9' && c.body.sheet.setId === 'set-stk-1').length;
      assert(r3 === 'ok' && asked2 === asked, 'the next look does not ask again: ' + JSON.stringify([r3, asked, asked2]));
      assert(await page.evaluate(() => B.run.status) === 'running', 'the run is still running');
      await page.evaluate(() => { const p = CN.allSheets().find(q => q.sheetId === 'gf-1'); p.releaseFull = true; delete p.keepRelease; });
      pass(`c · a full sheet of Set 1 being written again: GF Draft 9 is not sent into Set 1; with this page behind its records the cloud's refusal (${asked ? 'asked once' : 'not asked'}) leaves GF Draft 9 out, the run running, no second ask`);
    } catch (e) { fail('c · no automatic push of GF Draft 9; a refused automatic join does not stop the run', e); }

    const errs = await page.evaluate(() => window.__errs);
    if (errs.length) console.log('  note: ' + errs.slice(0, 5).join(' | '));
    if (errors.length) console.log('  page errors: ' + errors.slice(0, 5).join(' | '));
  } finally {
    await browser.close(); srv.close && srv.close();
  }
  console.log(`\n${results.length} passed, ${failures.length} failed`);
  if (failures.length) { for (const f of failures) console.log('  - ' + f); process.exit(1); }
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
