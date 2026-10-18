// A charm dragged onto the next or previous sheet, 10K and 14K solid gold only (Paul, 18 Oct 2026: "the user is allowed to drag and drop a
// given charm on the next or previous sheet. If there is not enough room on the given sheet ... a small pop-up where the user is informed
// that there's not enough space and that the charm was unable to be relocated to that particular sheet"). charm-nest-sheet-hop.js.
//
// Every part is the REAL one, in one real Chromium page over the local stand-in for the site (bridge-server.cjs: the real charmNestLibrary
// handler on an in-memory Firestore; nothing leaves the machine): the sheet window, its collision grid (the solver's sheet grid), its moveIn,
// Pool, the sets' Gate, and the sheet window's REAL Manual Control (window.CNManual, charm-nest-sheetwin.js: its header button, its claim, its
// in-sheet drag, its fit test, its save). The only stand-in is the nest itself (as in hold-release-cancel-e2e.cjs): startNest places what is
// pinned where it was pinned and saves the sheet the way the real nest saves it.
//   node tests/charm-nest/sheet-hop-dnd.cjs      (PW_DIR=<dir holding playwright-core>, CHROMIUM=<chrome>, SHOTS=<dir>, REDUCED=1)
// 1 the docks, once Manual Control is on: the previous and next 10K sheet, never a 14K or GF one; a GF sheet has no Manual Control
// 2 drag a single charm to the next sheet where there is room: Manual Control's in-sheet drag is handed over at the dock (it springs back,
//   nothing written), it flies, it is moved (pinned near where it was let go), records consistent, a "moved" step on the order's timeline by the person
// 3 drop on a full sheet: the pop-up says so, the charm stays where it was drawn, nothing is written
// 4 a two-piece order moves whole
// 5 guards: an order on a third sheet (tie), another computer meanwhile, a green line, a sheet nesting, in a set, cut; a dim dock says why
// 6 another metal: a 14K sheet's docks are 14K sheets
// 7 the keyboard way (Alt + Shift + arrow) on the charm in Manual Control's hand
// 8 without window.CNManual on the page: the hop's own small switch is the flag, the window's grid the verifier
// 9 with charms moved by hand and not saved yet: a moved charm hops from where it is drawn, the others stay unsaved; Manual Control turned
//   off while a hop is writing waits for it, then saves the charms that stayed (not the hopped ones)
// 10 after a reload: no piece is on two sheets, the window reads the moved pieces (and the layout saved by hand) from the cloud
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); process.exit(0); }
const { start } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || ''; if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', RUN = 'run-hop-1';
const BASE_TS = Math.floor(Date.UTC(2026, 9, 15, 12) / 1000);
const TID = tx => String(6000000000 + tx), pid = (rid, tx, copy) => `${rid}_${TID(tx)}_${copy}`;
const R = n => '41800000' + String(n).padStart(2, '0');
// the plate is 6 x 2 pieces, 30 pt apart (182 x 62 pt): a sheet of 12 is full. items: [rid, tx, copy, col, row]
const P = R(1), Q = R(2), M = R(3), S = R(4);
const SPEC = [
  { id: 'k10-1', metal: 'gold10k', page: 1, items: [[S, 40, 1, 0, 0], ...Array.from({ length: 11 }, (_, i) => [R(20 + i), 20 + i, 1, (i + 1) % 6, Math.floor((i + 1) / 6)])] },
  { id: 'k10-2', metal: 'gold10k', page: 2, items: [[P, 1, 1, 0, 0], [M, 3, 1, 1, 0], [M, 3, 2, 2, 0], [S, 41, 1, 3, 0], [Q, 2, 1, 4, 0], [R(40), 50, 1, 5, 0]] },
  { id: 'k10-3', metal: 'gold10k', page: 3, items: [[R(41), 51, 1, 0, 0], [R(42), 52, 1, 1, 0], [R(43), 53, 1, 2, 0]] },
  { id: 'k10-4', metal: 'gold10k', page: 4, roseCutAt: Date.now() - 3600e3, items: [[R(44), 54, 1, 0, 0], [R(45), 55, 1, 1, 0]] },
  { id: 'k14-1', metal: 'gold14k', page: 1, items: [[R(50), 60, 1, 0, 0], [R(51), 61, 1, 1, 0]] },
  { id: 'k14-2', metal: 'gold14k', page: 2, items: [[R(52), 62, 1, 0, 0], [R(53), 63, 1, 1, 0]] },
  { id: 'gf-1', metal: 'gold', page: 1, items: [[R(60), 70, 1, 0, 0], [R(61), 71, 1, 1, 0]] }
];
const cell = (col, row) => ({ x: 16 + col * 30, y: 16 + row * 30 });
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
            await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, charms: on.map(c => ({ id: c.id, poolId: c.poolId, order: c.order })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: on.map(c => c.poolId) } }, { quiet: true });
          } catch (e) { sh.problem = e.message; }
          sh.status = 'complete'; sh.dirty = false; sh.stage = ''; sh.persistedDone = true; sh.verification = { ok: true };
          try { CN.renderCard(sh); } catch (_) {}
        }, 150);
      };
      stub.__stub = true; window.startNest = stub; clearInterval(iv);
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
    st.put(SHEETS, sh.id, { id: sh.id, metal: sh.metal, charms, placements, poolIds: charms.map(c => c.poolId), page: sh.page, draft: true, fileBase: sh.id, runId: RUN, ...(sh.roseCutAt ? { roseCutAt: sh.roseCutAt } : {}) });
  }
  await page.goto(`${sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await ready();
  await page.evaluate(async ({ spec, RUN, BASE_TS }) => {
    await Orders.loadMaps(true); await Cancelled.load(true);
    CN.S.stockPreset = 'custom'; CN.S.settings.stock = Object.assign({}, CN.S.settings.stock, { gold: [182 / 72, 62 / 72], gold10k: [182 / 72, 62 / 72], gold14k: [182 / 72, 62 / 72] });
    const MM = 72 / 25.4, Rm = 5, D = Math.ceil(2 * Rm * MM) + 2, TID = tx => String(6000000000 + tx), pid = (rid, tx, copy) => `${rid}_${TID(tx)}_${copy}`;
    const SC = 4, DP = D * SC, mkBits = () => { const b = new Uint8Array(DP * DP); for (let y = 0; y < DP; y++) for (let x = 0; x < DP; x++) if (Math.hypot(x + .5 - DP / 2, y + .5 - DP / 2) <= DP / 2 - SC) b[y * DP + x] = 1; return b; };
    const mkCharm = (id, extra) => Object.assign({ id, name: id, sku: '', sourceId: 's', ringGeometryVersion: 3, centerPt: [D / 2, D / 2], bbox: [0, 0, D, D], outline: { circle: 1, cx: D / 2, cy: D / 2, r: Rm * MM }, members: [], rMm: Rm, w: DP, h: DP, scale: SC, bits: mkBits(), widthPt: 2 * Rm * MM, heightPt: 2 * Rm * MM, areaPt2: Math.PI * (Rm * MM) ** 2 }, extra || {});
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
      if (sh.roseCutAt) pg.roseCutAt = sh.roseCutAt;
      CN.renderCard(pg);
    }
    B.orders.rows.length = 0; B.orders.byKey.clear(); B.sets.clear();
    B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
  }, { spec: SPEC, RUN, BASE_TS });
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
  const shownAt = poolId => page.evaluate(id => { const x = SheetWin.hopKit.W.byPool.get(id); return x ? { x: x.p.cxPt, y: x.p.cyPt } : null; }, poolId);
  const recAt = (sheetId, poolId) => { const r = rec(sheetId), c = (r.charms || []).find(z => z.poolId === poolId), p = c && (r.placements || []).find(z => z.id === c.id); return p ? { x: p.cxPt, y: p.cyPt, manual: c.manual || null } : null; };
  const settled = () => until(() => page.evaluate(() => { const s = SheetHop.state(); const W = SheetWin.hopKit.W; return !s.busy && !s.dragging && !W.flow && !W.hop && !document.querySelector('.shpLift'); }), 60000, 'the move settles');
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

  try {
    /* 1 · the docks */
    try {
      await openSheet('k10-2');
      const b0 = await mcBtn();
      assert(b0 && !b0.hidden && b0.pressed === 'false', 'Manual Control is offered on a 10K sheet the sorter holds: ' + JSON.stringify(b0));
      assert.equal(await page.$('.shpSwitch'), null, 'no stand-in switch: Manual Control is the flag');
      assert.equal((await hopState()).on, false, 'off until Manual Control is on');
      assert.equal(await page.$('.shpDock'), null, 'no dock while off');
      await manual(true);
      await until(() => page.evaluate(() => document.querySelectorAll('.shpDock').length === 2), 4000, 'two docks');
      const s1 = await hopState();
      assert.deepEqual([s1.docks.prev && s1.docks.prev.sheetId, s1.docks.next && s1.docks.next.sheetId], ['k10-1', 'k10-3'], JSON.stringify(s1));
      assert.deepEqual([s1.docks.prev.name, s1.docks.next.name], ['10K Draft 1', '10K Draft 3']);
      const labels = await page.evaluate(() => [...document.querySelectorAll('.shpDock')].map(d => d.textContent).join(' | '));
      assert(!/14K|GF/.test(labels), 'no other metal is offered: ' + labels);
      // a click on a dock opens that sheet (Manual Control ends on the sheet it leaves, as opening any other sheet ends it: nothing was moved, nothing is written)
      const w0 = writes();
      await page.click('.shpDock[data-side=next] .shpTab');
      await until(() => page.evaluate(() => SheetWin.hopKit.W.id === 'k10-3' && SheetWin.hopKit.W.geom), 15000, 'the next sheet opens');
      await until(async () => !(await mcOn()) && !(await page.$('.shpDock')), 5000, 'Manual Control ended on 10K Draft 2; no dock on 10K Draft 3 until it is on there');
      assert.equal(writes(), w0, 'nothing written');
      await until(() => page.evaluate(() => { const W = SheetWin.hopKit.W; return W.rec && W.pieces.length && !W.flying; }), 15000, 'drawn'); await sleep(400);
      await manual(true);
      await until(async () => { const s = await hopState(); return s.docks.prev && s.docks.prev.sheetId === 'k10-2' && s.docks.next && s.docks.next.sheetId === 'k10-4'; }, 5000, 'its docks are its own neighbours');
      await openSheet('gf-1');
      assert.equal((await mcBtn() || { hidden: true }).hidden, true, 'a GF sheet has no Manual Control'); assert.equal(await page.$('.shpDock'), null, 'and no dock');
      await openSheet('k10-2');
      assert.equal(await page.$('.shpDock'), null, 'off again on 10K Draft 2');
      await manual(true);
      await until(() => page.evaluate(() => document.querySelectorAll('.shpDock').length === 2), 4000, 'docks once Manual Control is on again');
      pass('1 · with Manual Control on, the previous and next 10K sheet are the docks (no 14K, no GF); a click opens one (Manual Control ends there, nothing written); a GF sheet has no Manual Control');
    } catch (e) { fail('1 · docks', e); }

    /* 2 · a single charm to the next sheet where there is room */
    try {
      const p = idOf(P, 1), w0 = writes();
      const aim = cell(4, 1);
      let lifted = null;
      const hand = await carry(p, 'next', aim, { atDrop: async () => { lifted = await page.evaluate(() => ({ lift: !!document.querySelector('.shpLift'), ghost: !!(SheetHop._H.docks.next.ghost && SheetHop._H.docks.next.ghost.ok), state: SheetHop._H.docks.next.el.dataset.state })); await shot('2-aim'); } });
      assert.deepEqual(lifted, { lift: true, ghost: true, state: 'armed' }, 'the open view shows where it goes, in sage: ' + JSON.stringify(lifted));
      // the hand-over: the press was Manual Control's own drag inside the sheet; at the dock the hop took it (cancelDrag: no drag left, nothing kept, nothing written)
      assert(hand.inSheet.drag && hand.inSheet.drag.moved && hand.inSheet.drag.poolId === p && hand.inSheet.drag.mode === 'move' && !hand.inSheet.lifted, 'first an in-sheet drag of Manual Control: ' + JSON.stringify(hand.inSheet));
      assert.deepEqual([hand.after.drag, hand.after.edit], [false, null], "handed over: Manual Control's drag stopped and kept nothing: " + JSON.stringify(hand.after));
      assert.equal(hand.writesAtHandover, hand.w, 'nothing written at the hand-over');
      await sleep(300); await shot('2-flying');
      await settled();
      const lv = await live();
      assert(!lv['k10-2'].all.includes(p), 'off 10K Draft 2 (live)');
      const there = lv['k10-3'].placed.find(z => z.id === p);
      assert(there, 'on 10K Draft 3 (live)'); assert(there.pinned, 'pinned there by the person');
      assert(Math.hypot(there.x - aim.x, there.y - aim.y) < 4, `where it was let go: ${there.x},${there.y}`);
      assert(!(rec('k10-2').poolIds || []).includes(p) && (rec('k10-3').poolIds || []).includes(p), 'the saved records say the same');
      assert.equal(pool(p).sheetId, 'k10-3'); assert.equal(pool(p).movedBy, 'Tester'); assert.equal(pool(p).movedTo, 'k10-3'); assert.equal(pool(p).movedFrom, 'k10-2'); assert(pool(p).moveVerifiedAt > 0, 'read back and stamped verified');
      assert(writes() > w0);
      assert.equal(await page.evaluate(() => SheetWin.isOpen()), true, 'the window stays open');
      assert.equal(await page.evaluate(() => SheetHop._H.drag || SheetHop._H.busy ? 1 : 0), 0);
      assert(await page.evaluate(() => [...document.querySelectorAll('.mNote')].some(n => /moved to 10K Draft 3/.test(n.textContent))), 'a note says where it went');
      assert(await page.evaluate(id => !SheetWin.hopKit.W.byPool.has(id), p), 'the window shows 10K Draft 2 without it');
      assert.equal(await mcOn(), true, 'Manual Control is still on for 10K Draft 2');
      await until(async () => { const s = await hopState(); return s.docks.next && s.docks.next.sheetId === 'k10-3'; }, 5000, 'its docks are back');
      const tl = await until(() => st.list('Order_Timeline').find(e => String(e.orderId) === P && e.type === 'moved'), 10000, "the order's timeline step");
      assert.equal(tl.by, 'Tester'); assert(tl.data && tl.data.from === 'k10-2' && tl.data.to === 'k10-3', 'the timeline says from where to where: ' + JSON.stringify(tl.data));
      await oneEach('after the move');
      pass(`2 · dragged to the next sheet: Manual Control's in-sheet drag handed over at the dock (nothing kept or written), flies, lands pinned where it was let go; live, saved records and the piece record agree (moved by Tester); a "moved" step on the order's timeline by Tester (${tl.text})`);
    } catch (e) { fail('2 · move to the next sheet', e); if (process.env.DEBUG) console.log(errors, await page.evaluate(() => ({ errs: window.__errs, st: SheetHop.state(), hop: SheetWin.hopKit.W.hop, flow: SheetWin.hopKit.W.flow && SheetWin.hopKit.W.flow.title, notes: [...document.querySelectorAll('.mNote')].map(n => n.textContent), open: SheetWin.isOpen(), local: SheetHop._H.local, closes: window.__closes, clicks: window.__clicks })), st.list('Order_Timeline').slice(-5)); }
    await closePops();

    /* 3 · a full sheet */
    try {
      const q = idOf(Q, 2), w0 = writes(), q0 = await shownAt(q);
      // (a busy computer: the click that follows the drop comes 700 ms late; it must still not reach the dialog, which reads a click on itself as a click on its backdrop and closes)
      await carry(q, 'prev', cell(2, 1), { atDrop: () => page.evaluate(() => addEventListener('mouseup', function slow() { removeEventListener('mouseup', slow, true); const t = performance.now(); while (performance.now() - t < 700); }, true)) });
      await until(popText, 15000, 'the pop-up');
      const t = await popText(); await shot('3-full');
      assert(t.includes('Not enough space on 10K Draft 1 — the charm stayed on 10K Draft 2'), t);
      await settled();
      const lv = await live();
      assert(lv['k10-2'].all.includes(q) && !lv['k10-1'].all.includes(q), 'the charm stayed');
      assert.equal(writes(), w0, 'nothing was written');
      const q1 = await shownAt(q);
      assert(q1 && Math.hypot(q1.x - q0.x, q1.y - q0.y) < .01, "it is drawn where it was (Manual Control's drag sprang back): " + JSON.stringify([q0, q1]));
      assert.equal(await page.evaluate(id => CNManual._MC.edits.has(SheetWin.hopKit.W.byPool.get(id).id), q), false, 'and Manual Control kept nothing for it');
      assert.equal(await page.evaluate(() => SheetWin.isOpen()), true, 'the window stays open (the click after the drop is not a click on its backdrop)');
      pass('3 · a full sheet: "Not enough space on 10K Draft 1 — the charm stayed on 10K Draft 2", the charm flies back, nothing written');
    } catch (e) { fail('3 · full sheet', e); }
    await closePops();

    /* 4 · a two-piece order moves whole */
    try {
      const m1 = idOf(M, 3, 1), m2 = idOf(M, 3, 2);
      await carry(m1, 'next', cell(5, 1));
      await settled();
      const lv = await live();
      assert(lv['k10-3'].all.includes(m1) && lv['k10-3'].all.includes(m2), 'both pieces of order M are on 10K Draft 3');
      assert(!lv['k10-2'].all.includes(m1) && !lv['k10-2'].all.includes(m2), 'none left on 10K Draft 2');
      assert.equal(pool(m2).sheetId, 'k10-3');
      await oneEach('after the order moved');
      pass('4 · a two-piece order moves whole (its other piece flies after it)');
    } catch (e) { fail('4 · whole order', e); }
    await closePops();

    /* 5 · guards */
    try {
      // an order with a piece on a third sheet: the dock is dim while the hand is over it, and says why
      const s2 = idOf(S, 41), w0 = writes();
      let dim = null;
      await carry(s2, 'next', null, { before: async () => { await sleep(300); dim = await page.evaluate(() => { const d = document.querySelector('.shpDock[data-side=next]'); return { state: d.dataset.state, text: d.textContent }; }); } });
      assert.equal(dim.state, 'dim'); assert(/also has pieces on 10K Draft 1/.test(dim.text), dim.text);
      await until(popText, 10000, 'the pop-up'); assert(/also has pieces on 10K Draft 1/.test(await popText()), await popText());
      await settled(); await closePops();
      assert.equal(writes(), w0, 'nothing written for the tie');
      // two computers: while the hand holds it, another computer puts the order on hold (the piece's record and the saved sheet say so): the move stops
      const o40 = idOf(R(40), 50), keep40 = { pool: Object.assign({}, pool(o40)), rec: (rec('k10-2').poolIds || []).slice() };
      await carry(o40, 'next', cell(3, 1), { atDrop: async () => {
        st.put(POOL, o40, Object.assign({}, keep40.pool, { state: 'abandoned', sheetId: null, heldAt: Date.now(), heldBy: 'Other computer' }));
        st.put(SHEETS, 'k10-2', Object.assign({}, rec('k10-2'), { poolIds: keep40.rec.filter(x => x !== o40) }));
      } });
      await until(popText, 15000, 'the pop-up'); assert(/another computer/.test(await popText()), await popText());
      await settled(); await closePops();
      assert.equal(writes(), w0, 'nothing written when another computer changed the piece');
      assert(!(rec('k10-3').poolIds || []).includes(o40), 'the piece did not go onto 10K Draft 3');
      st.put(POOL, o40, keep40.pool); st.put(SHEETS, 'k10-2', Object.assign({}, rec('k10-2'), { poolIds: keep40.rec }));
      // the sheet-level rules, as the move reads them
      const why = await page.evaluate(() => {
        const pg = id => CN.allSheets().find(p => p.sheetId === id), out = {};
        const t = pg('k10-3'), keep = { status: t.status, setId: t.setId, draft: t.draft };
        out.cut = SheetHop.sheetRefusal(pg('k10-4'));
        t.status = 'nesting'; out.busy = SheetHop.sheetRefusal(t); t.status = keep.status;
        t.setId = 'set-x'; t.draft = false; out.set = SheetHop.sheetRefusal(t); t.setId = keep.setId; t.draft = keep.draft;
        t.laserDoneAt = Date.now(); out.laser = SheetHop.sheetRefusal(t); delete t.laserDoneAt;
        t.rosePlan = { axis: 'x' }; out.line = SheetHop.sheetRefusal(t); delete t.rosePlan;
        out.ok = SheetHop.sheetRefusal(t);
        return out;
      });
      assert(/green line cut/.test(why.cut), why.cut); assert(/being nested or saved/.test(why.busy), why.busy); assert(/in a set|keeps its charms/.test(why.set), why.set);
      assert(/cut by the laser/.test(why.laser), why.laser); assert(/green dash line/.test(why.line), why.line); assert.equal(why.ok, '');
      // a sheet nesting right now, through the hand: refused, nothing written
      await page.evaluate(() => { CN.allSheets().find(p => p.sheetId === 'k10-3').status = 'queued'; });
      const q = idOf(Q, 2), w1 = writes();
      await carry(q, 'next', null);
      await until(popText, 10000, 'the pop-up'); assert(/being nested or saved/.test(await popText()), await popText());
      await page.evaluate(() => { CN.allSheets().find(p => p.sheetId === 'k10-3').status = 'complete'; });
      await settled(); await closePops();
      assert.equal(writes(), w1);
      // the green line, through the hand, from 10K Draft 3 to 10K Draft 4
      await openSheet('k10-3'); await manual(true);
      const p = idOf(P, 1);
      await carry(p, 'next', null);
      await until(popText, 10000, 'the pop-up'); assert(/10K (Sheet|Draft) 4 has its green line cut/.test(await popText()), await popText());   // (a cut sheet keeps the number its file carries: charm-nest-sheet-name.js)
      await settled(); await closePops();
      assert.equal(writes(), w1);
      pass('5 · refused with a plain sentence, the charm flies back, nothing written: an order on a third sheet (dim dock says why), another computer held it meanwhile, nesting, green line cut or drawn, in a set, laser cut');
    } catch (e) { fail('5 · guards', e); }
    await closePops();

    /* 6 · another metal */
    try {
      await openSheet('k14-1'); await manual(true);
      const s = await until(async () => { const x = await hopState(); return x.docks.next ? x : null; }, 5000, '14K docks');
      assert.equal(s.docks.prev, null); assert.equal(s.docks.next.sheetId, 'k14-2'); assert.equal(s.docks.next.name, '14K Draft 2');
      pass('6 · a 14K sheet docks only 14K sheets');
    } catch (e) { fail('6 · another metal', e); }

    /* 7 · the keyboard way */
    try {
      await openSheet('k10-2'); await manual(true);
      const q = idOf(Q, 2), w0 = writes();
      // a press (no move) on the charm puts it in Manual Control's hand: that is the selected charm (the piece view's selection is not used while it is on)
      const a = await onPlate(q); await page.mouse.click(a.x, a.y); await sleep(200);
      assert.equal(await page.evaluate(() => CNManual._MC.sel && CNManual._MC.sel.poolId), q, "in Manual Control's hand");
      await page.keyboard.press('Alt+Shift+ArrowLeft');
      await until(popText, 15000, 'the pop-up'); assert(/Not enough space on 10K Draft 1/.test(await popText()), await popText());
      await settled(); await closePops();
      const lv = await live(); assert(lv['k10-2'].all.includes(q));
      assert.equal(writes(), w0, 'nothing written');
      assert.equal(await page.evaluate(id => CNManual._MC.edits.has(SheetWin.hopKit.W.byPool.get(id).id), q), false, "Manual Control's own arrow keys did not move it (Alt is the hop's)");
      pass("7 · Alt + Shift + ← takes the charm in Manual Control's hand the same way (and the same pop-up); nothing written");
    } catch (e) { fail('7 · keyboard', e); }

    /* 8 · without window.CNManual on the page (the hop's own stand-in, kept for a page without Manual Control): its small switch is the flag, the window's grid the verifier */
    try {
      await manual(false);
      await page.evaluate(() => { window.__cnm = window.CNManual; delete window.CNManual; });
      await until(() => page.$('.shpSwitch'), 4000, 'the stand-in switch shows');
      assert.equal(await page.$('.shpDock'), null, 'no dock until it is switched on');
      await page.click('.shpSwitch');
      await until(() => page.evaluate(() => document.querySelectorAll('.shpDock').length === 2), 4000, 'docks with the switch on');
      const q = idOf(Q, 2), w0 = writes();
      await carry(q, 'prev', cell(2, 1));
      await until(popText, 15000, 'the pop-up'); assert((await popText()).includes('Not enough space on 10K Draft 1 — the charm stayed on 10K Draft 2'), await popText());
      await settled(); await closePops();
      assert.equal(writes(), w0, 'nothing written');
      await page.click('.shpSwitch');
      await until(() => page.evaluate(() => !document.querySelector('.shpDock')), 3000, 'docks go with the switch off');
      pass('8 · without window.CNManual: the stand-in switch is the flag, the window grid the verifier (the same pop-up, nothing written)');
    } catch (e) { fail('8 · stand-in switch', e); }
    await page.evaluate(() => { if (window.__cnm) { window.CNManual = window.__cnm; delete window.__cnm; } SheetHop._H.local = false; });
    await until(() => page.evaluate(() => !document.querySelector('.shpSwitch')), 4000, 'the switch goes once CNManual is back').catch(e => fail('8 · stand-in switch goes', e));
    await closePops();

    /* 9 · with charms moved by hand and not saved yet */
    let eS41 = null;
    try {
      await openSheet('k10-2'); await manual(true);
      const s41 = idOf(S, 41), q = idOf(Q, 2), o40 = idOf(R(40), 50), s41rec0 = recAt('k10-2', s41), w0 = writes();
      // two charms moved inside the sheet by Manual Control (not saved): S41 and Q one row down
      const eS = eS41 = await mcMove(s41, cell(3, 1)), eQ = await mcMove(q, cell(4, 1));
      assert(eS && Math.hypot(eS.cxPt - cell(3, 1).x, eS.cyPt - cell(3, 1).y) < 1.5, 'S41 moved by hand: ' + JSON.stringify(eS));
      assert(eQ && Math.hypot(eQ.cxPt - cell(4, 1).x, eQ.cyPt - cell(4, 1).y) < 1.5, 'Q moved by hand: ' + JSON.stringify(eQ));
      assert.equal(writes(), w0, 'moves by hand write nothing until Manual Control is turned off');
      // Q hops from where Manual Control drew it, not from its saved spot; S41's move stays unsaved
      const hand = await carry(q, 'next', cell(1, 1));
      assert(hand.after.home && Math.hypot(hand.after.home.x - eQ.cxPt, hand.after.home.y - eQ.cyPt) < .01, 'lifted from where Manual Control drew it: ' + JSON.stringify(hand.after));
      assert(hand.after.edit && !hand.after.drag, 'the hand-over kept its earlier move by hand and stopped the drag: ' + JSON.stringify(hand.after));
      await settled(); await shot('9-after-hop');
      let lv = await live();
      assert(lv['k10-3'].all.includes(q) && !lv['k10-2'].all.includes(q), 'Q is on 10K Draft 3');
      const qt = lv['k10-3'].placed.find(z => z.id === q);
      assert(qt && qt.pinned && Math.hypot(qt.x - cell(1, 1).x, qt.y - cell(1, 1).y) < 4, 'pinned where it was let go: ' + JSON.stringify(qt));
      assert.equal(await mcOn(), true, 'Manual Control is still on for 10K Draft 2');
      const sNow = await shownAt(s41);
      assert(sNow && Math.hypot(sNow.x - eS.cxPt, sNow.y - eS.cyPt) < .01, 'S41 is still drawn where it was moved by hand: ' + JSON.stringify(sNow));
      assert(await page.evaluate(id => CNManual._MC.edits.has(SheetWin.hopKit.W.byPool.get(id).id), s41), 'and Manual Control still holds that move');
      const barText = await page.evaluate(() => (document.querySelector('.sheetWin [data-r=mcBar]') || {}).innerText || '');
      assert(/\b1 charm moved\b/.test(barText), "Manual Control's bar counts only the charm still on the sheet: " + barText);
      const s41rec1 = recAt('k10-2', s41);
      assert(s41rec1 && Math.hypot(s41rec1.x - s41rec0.x, s41rec1.y - s41rec0.y) < .01 && !s41rec1.manual, "the hop wrote 10K Draft 2 as it stood: S41's move by hand is not saved yet: " + JSON.stringify(s41rec1));
      await closePops();
      // R40 hops, and Manual Control is turned off while that move is writing: the save waits its turn, then saves S41 alone (Q left the sheet)
      await page.evaluate(() => { window.__flows = []; const W = SheetWin.hopKit.W; let last = ''; window.__fl = setInterval(() => { const t = W.flow ? W.flow.title : ''; if (t !== last) { window.__flows.push(t); last = t; } }, 15); });
      await carry(o40, 'next', cell(2, 1));
      await until(() => page.evaluate(() => /^Moving order/.test(SheetWin.hopKit.W.flow ? SheetWin.hopKit.W.flow.title : '')), 20000, "the hop's move is writing");
      await page.click('.sheetWin [data-r=manual]');
      await until(async () => !(await mcOn()), 5000, 'Manual Control off');
      await until(() => page.evaluate(() => { const W = SheetWin.hopKit.W, w = W.work; return !W.flow && !!w && /moved by hand/.test(w.title) && w.state !== 'working'; }), 120000, 'its save ended');
      const flows = await page.evaluate(() => { clearInterval(window.__fl); return window.__flows.filter(Boolean); });
      assert(flows.length === 2 && /^Moving order 4180000040 to 10K Draft 3/.test(flows[0]) && /^Saving 1 charm moved by hand on 10K Draft 2/.test(flows[1]), 'the save waited for the hop, then saved the one charm that stayed: ' + JSON.stringify(flows));
      const work = await page.evaluate(() => { const w = SheetWin.hopKit.W.work; return { state: w.state, title: w.title, note: w.note }; });
      assert.equal(work.state, 'done', JSON.stringify(work));
      await settled(); await shot('9-saved');
      lv = await live();
      assert(lv['k10-3'].all.includes(o40) && !lv['k10-2'].all.includes(o40), 'R40 is on 10K Draft 3');
      const sl = lv['k10-2'].placed.find(z => z.id === s41);
      assert(sl && sl.pinned && Math.hypot(sl.x - eS.cxPt, sl.y - eS.cyPt) < .01, 'S41 saved and pinned where it was moved by hand: ' + JSON.stringify(sl));
      const sr = recAt('k10-2', s41);
      assert(sr && Math.hypot(sr.x - eS.cxPt, sr.y - eS.cyPt) < .01, 'and so is the saved record: ' + JSON.stringify(sr));
      const mk = await page.evaluate(id => { const c = CN.allSheets().find(p => p.sheetId === 'k10-2').charms.find(z => z.poolId === id); return [c.manual || null, !!c.arrivalPin]; }, s41);
      assert(mk[0] && mk[0].by === 'Tester' && !mk[1], 'the charm is marked moved by hand by Tester, pinned by a person (the stand-in nest writes no charm marks: sheetwin-manual-control.cjs reads them on the record): ' + JSON.stringify(mk));
      const r2 = rec('k10-2');
      assert(r2.manual && r2.manual.n === 1 && r2.manual.by === 'Tester', 'the record says one charm was moved by hand: ' + JSON.stringify(r2.manual));
      assert(!r2.manualEdit, 'the claim was given back');
      assert.deepEqual(r2.poolIds || [], [s41], 'the save wrote no hopped charm back onto 10K Draft 2');
      assert(['P', 'Q', 'R40'].every((_, i) => (rec('k10-3').poolIds || []).includes([idOf(P, 1), q, o40][i])), 'they are on 10K Draft 3');
      assert.equal(await page.evaluate(() => !!CN.allSheets().find(p => p.sheetId === 'k10-2')._manualHold), false, 'the sheet is let go');
      await oneEach('after the hops and the save by hand');
      pass('9 · moved by hand, not saved: a moved charm hops from where it is drawn and the others stay unsaved; Manual Control turned off during a hop waits for it, then saves only the charm that stayed (pinned, marked, claim given back)');
    } catch (e) { fail('9 · hand-over with unsaved moves', e); if (process.env.DEBUG) console.log(await page.evaluate(() => ({ flows: window.__flows, work: SheetWin.hopKit.W.work && { t: SheetWin.hopKit.W.work.title, s: SheetWin.hopKit.W.work.state, n: SheetWin.hopKit.W.work.note, st: SheetWin.hopKit.W.work.steps.map(x => [x.text, x.state, x.detail]) } }))); }
    await closePops();

    /* 10 · after a reload */
    try {
      await oneEach('before the reload');
      await page.reload({ waitUntil: 'load' }); await ready(); await sleep(500);
      const docs = st.list(SHEETS), flat = docs.flatMap(d => d.poolIds || []);
      assert.equal(new Set(flat).size, flat.length, 'no piece on two saved sheets');
      const went = [idOf(P, 1), idOf(M, 3, 1), idOf(M, 3, 2), idOf(Q, 2), idOf(R(40), 50)];
      await openSheet('k10-3');
      const on3 = await page.evaluate(() => [...SheetWin.hopKit.W.byPool.keys()]);
      for (const id of went) assert(on3.includes(id), `${id} is read on 10K Draft 3 after the reload`);
      await openSheet('k10-2');
      const on2 = await page.evaluate(() => [...SheetWin.hopKit.W.byPool.keys()]);
      for (const id of went) assert(!on2.includes(id), `${id} is not on 10K Draft 2 after the reload`);
      assert.deepEqual(on2, [idOf(S, 41)], 'what stayed is still there: ' + on2);
      const s = await shownAt(idOf(S, 41));
      assert(eS41 && s && Math.hypot(s.x - eS41.cxPt, s.y - eS41.cyPt) < .01, 'drawn where it was moved by hand: ' + JSON.stringify(s));
      pass('10 · after a reload: every piece on one saved sheet, the window reads the moved ones where they went (and the layout saved by hand)');
    } catch (e) { fail('10 · reload', e); }

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
