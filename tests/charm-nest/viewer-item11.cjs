// ITEM 11 (Paul 21 Nov 2026): Options → Partial Sheet → "Use this one" pressed on a staff computer (a viewer) while the cloud carries every run (drive mode "on") said
// "That partial sheet could not be reserved: The cloud carries every run now…": the page claimed the partial sheet itself (PartialNest.seat → RoseStock.seatOn → roseClaim, a
// PRODUCTION op the cloud refuses from a page). Dragging a run sheet onto an Available sheet in the Options Sheets grid sent the cloud a seat request instead.
//   Now: on a viewer "Use this one" sends the SAME seat request (runCmdPut { type: 'seat', sheetId, partialId, partialIds }, by, dev); the cloud reserves the partial sheet (the
//   record's own physical sheet, item 11's second half: the id a person picks is the partial sheet's record <stockId>-<revision>, not the stock) and nests the sheet on it,
//   and the panel shows the cloud's steps and its answer. In mode "off" the page seats as before and sends nothing.
//   S  the library in mode "on" (offline, _cloudFakes.cjs): a page's own roseClaim is refused (the reported words); a seat request in the shape the panel sends is taken
//   P  the REAL panel (charm-nest-partial-ui.js with the Options window and the page's Gate, in jsdom) as a viewer, its calls going to that offline cloud; the cloud's commands
//      unit (_charmNestCmdUnit.js) does the request: the partial sheet is reserved for the sheet, a real nest job of the sheet on it is queued; the sheet writer's result is
//      stood in for (a new layout on the record), the cloud answers done, and the panel says so; PartialNest.seat is never called
//   O  mode "off" (not a viewer): Use this one seats through PartialNest.seat as before and sends no request
// Offline only: no Etsy, no live call, nothing deployed. Made-up names.
//   NODE_PATH=<deps>:<jsdom> node tests/charm-nest/viewer-item11.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const fs = require('fs'), path = require('path');
const ROOT = path.join(__dirname, '../..');
let JSDOM; try { ({ JSDOM } = require('jsdom')); } catch (_) { console.log('FAIL viewer-item11: jsdom is not installed (NODE_PATH)'); process.exit(1); }
const T0 = Date.UTC(2026, 10, 21, 15, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
const quiet = () => { const l = console.log, w = console.warn; console.log = () => {}; console.warn = () => {}; return () => { console.log = l; console.warn = w; }; };
const R = require('../../charm-nest-rose'), O = require('../../charm-nest-orders');
const wait = ms => new Promise(r => setTimeout(r, ms));
const { performance } = require('perf_hooks');   // (the real clock: Date.now is the fake cloud's)
const until = async (f, ms) => { const t0 = performance.now(); for (;;) { const v = await f(); if (v) return v; if (performance.now() - t0 > (ms || 20000)) return null; await wait(20); } };

const lib = F.load('charmNestLibrary.js'), Unit = F.load('_charmNestCmdUnit.js');
const CV = 'cloud14-20261121', PAGE = 'd0michelle11';
const RUN = 'run-2026-11-21-item11', SH = 'rose-s1-item11', OLD = 'rgs-old11', STK = 'rgs-part11', PID = STK + '-2';
const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
const MM = 72 / 25.4;
function seed(mode) {
  F.reset(); F.clock.set(T0);
  F.seed('Charm_Nest_Open/drive', { mode, runMode: 'manual', cloudSteps: ['engrave', 'nest', 'cmd:seat', 'cmd:reorder', 'cmd:consolidate', 'cmd:empty'], flippedAt: T0 - 3600e3 }); lib._driveForget();
  F.seed('Charm_Nest_Runs/' + RUN, { runId: RUN, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [] });
  F.seed('Charm_Nest_Open/run', { runId: RUN, at: T0 });
  F.seed('Charm_Nest_Run_Carrier/' + RUN, { runId: RUN, carrier: { dev: 'cloud', by: 'Cloud', at: T0 + 3600e3, since: T0 - 3600e3 } });
  const charms = [0, 1, 2].map(i => ({ id: 'c' + i, poolId: `424111000${i}_42411100${i}1_1`, order: '424111000' + i, sku: 'COMET_9427' }));
  F.seed('Charm_Nest_Sheets/' + SH, { id: SH, runId: RUN, metal: 'rose', page: 1, fileBase: 'RG Sheet 1', charms, poolIds: charms.map(c => c.poolId), placements: charms.map((c, i) => ({ id: c.id, cxPt: 20 + i * 30, cyPt: 20, angle: 0 })), layoutRev: 3, contentRev: 2, roseStockId: OLD });
  F.seed('Charm_Nest_Rose_Stock/' + OLD, { metal: 'rose', wPt: 100 * MM, hPt: 50 * MM, revision: 0, owner: SH, available: false });
  F.seed('Charm_Nest_Rose_Stock/' + STK, { metal: 'rose', wPt: 100 * MM, hPt: 50 * MM, revision: 2, owner: null, available: true, profileJson: '{"rings":[]}', lastCutAt: T0 - 5 * 864e5 });
  F.seed('Charm_Nest_Remnants/' + PID, { metal: 'rose', stockId: STK, revision: 2, status: 'available', sheetWMm: 100, sheetHMm: 50, wMm: 60, hMm: 50, areaMm2: 2400, cutAt: T0 - 5 * 864e5, sourceSheet: 'RG Sheet 4' });
}
/** One pass of the cloud's commands unit, as the driver runs it (its calls are the cloud's own, dev "cloud"). */
const cloudPass = async () => { const back = quiet(); try { return await Unit.commandsUnit({ db: F.db, runId: RUN, run: F.doc('Charm_Nest_Runs/' + RUN), call: (b, u) => lib.call(b, { unit: u }), cloudSteps: F.doc('Charm_Nest_Open/drive').cloudSteps, budgetEnd: Date.now() + 3600e3, say() {} }); } finally { back(); } };
const cmdsOf = () => Object.values((F.doc('Charm_Nest_Run_Cmds/' + RUN) || {}).cmds || {});

async function partS() {
  seed('on');
  const r = await P({ op: 'roseClaim', sheetId: SH, metal: 'rose', wPt: 100 * MM, hPt: 50 * MM, stockId: STK, revision: 2, nesting: true, exact: true, partialId: PID, swap: true, dev: PAGE, cv: CV });
  ok('S1 mode on: the page\'s own claim of the partial sheet is refused (the words Paul saw), nothing is reserved', r.http === 409 && r.cloudCarries === true && /The cloud carries every run now/.test(r.error || '') && !(F.doc('Charm_Nest_Rose_Stock/' + STK) || {}).owner, r);
  const put = await P({ op: 'runCmdPut', runId: RUN, cmd: { metal: 'rose', label: 'Move RG Sheet 1', type: 'seat', sheetId: SH, partialId: PID, partialIds: [PID] }, by: 'Michelle', dev: PAGE, cv: CV });
  const c = cmdsOf().find(k => k.id === put.id);
  ok('S2 ... the seat request in the shape the panel sends is taken (waiting, the sheet, the partial sheet, who asked)', put.http === 200 && !!c && c.state === 'waiting' && c.type === 'seat' && c.args.sheetId === SH && c.args.partialId === PID && JSON.stringify(c.args.partialIds) === JSON.stringify([PID]) && c.by === 'Michelle', { put, c });
}

/* ── the real panel in jsdom (the partial-panel.cjs harness), its calls going to the offline cloud ── */
async function page(viewerOn) {
  const outline = { subpaths: [[['m', [0, 0]], ['l', [10, 0]], ['l', [10, 10]], ['l', [0, 10]], ['h']]] };
  const dom = new JSDOM('<section id="sheet"><div class="shHead"></div><div class="shGate" data-r="gate"></div><div class="shPreviewWrap"></div></section>', { url: 'https://example.test', runScripts: 'outside-only' }), w = dom.window;
  w.IntersectionObserver = class { observe() {} unobserve() {} };
  w.CharmNestRose = R; w.CharmNestOrders = O; w.confirm = () => true; w.Motion = { reduced: () => true };
  const toasts = [], sent = [], WPT = 100 * MM, HPT = 50 * MM;
  const charms = [0, 1, 2].map(i => ({ id: 'c' + i, outline, centerPt: [5, 5], members: [] }));
  const sh = { metal: 'rose', page: 1, el: w.document.getElementById('sheet'), sheetId: SH, runId: RUN, charms, placements: charms.map((c, i) => ({ id: c.id, cxPt: 10 + i * 20, cyPt: 10, angle: 0, scale: 1 })), persistedDone: true, verification: { ok: true }, status: 'complete', dirty: false, draft: true, outputs: {} };
  const api = async (fn, body) => {   // (the page's CN.api: the library over the offline cloud, as a current page (cv) on this computer)
    sent.push(body); const r = await P(Object.assign({ dev: PAGE, cv: CV }, body));
    if (r.http >= 400) throw new Error(r.error || 'HTTP ' + r.http); return r;
  };
  w.CN = { S: { cloud: { ok: true }, settings: { stock: {} }, mode: 'nest', sheets: { rose: { cardEl: sh.el, pages: [sh] } } }, esc: s => String(s).replace(/</g, '&lt;'), stockFor: () => ({ wPt: WPT, hPt: HPT, wIn: WPT / 72, hIn: HPT / 72 }), uid: () => 'test', allSheets: () => [sh], pagesOf: () => [sh], drawPreview() {}, labelOf: () => 'Rose Gold',
    renderCard: p => { w.Gate.renderCard(p); }, toast(m, k) { toasts.push([String(m), k]); }, sheetDirty: p => { p.dirty = true; }, flushManualIntake() {}, api };
  w.sh = sh;
  w.eval(`var C=window.CN,S=C.S,CN=C,B=window.B={run:${viewerOn ? 'null' : `{runId:'${RUN}',releasePolicy:2,status:'running',solidIncluded:{}}`},employee:'Michelle'};
 var O=window.CharmNestOrders,allSheets=()=>[sh],pagesOf=()=>[sh],stockFor=C.stockFor,esc=C.esc,labelOf=()=> 'Rose Gold';
 var Sets={ofRun:()=>[]},RunCtl={},Session=window.Session={schedule(){}},toast=()=>{},refreshAllCards=()=>{},api=C.api;
 var sheetDirty=()=>{},saveSettings=()=>{},startNest=()=>{};`);
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8'); w.eval(fs.readFileSync(path.join(ROOT, 'charm-nest-place.js'), 'utf8')); w.eval(fs.readFileSync(path.join(ROOT, 'charm-nest-assembly-core.js'), 'utf8'));
  w.eval(src.slice(src.indexOf('const Gate ='), src.indexOf('/* ═══ 21', src.indexOf('const Gate ='))));
  // the viewer: CNViewer on, no run held, the run followed (CharmNestRunFollow), this computer's id
  w.CNViewer = { on: () => !!viewerOn };
  w.CharmNestRunFollow = { state: () => ({ runId: RUN }), dev: () => PAGE };
  const card = { id: PID, stockId: STK, revision: 2, metal: 'rose', status: 'available', outline: [[[0, 0], [60, 0], [60, 50], [0, 50]]], sheetWMm: 100, sheetHMm: 50, wMm: 60, hMm: 50, areaMm2: 2400, bboxMm: { x: 0, y: 0, w: 60, h: 50 },
    sourceSheet: 'RG Sheet 4', sourceSet: '', cutAt: Date.now() - 5 * 864e5, lastUsedAt: Date.now() - 5 * 864e5, estimate: { pieces: 7, low: 5, high: 9 } };
  const calls = [];
  w.PartialSheets = { cached: () => null, on() {}, policy: () => ({ mode: 'auto', wMm: 100, hMm: 50 }), changed() { calls.push(['changed']); }, async loadPolicy() { return {}; }, async list() { return { items: [], rev: 1 }; },
    async searchAll() { return { items: [card], rev: 1, more: false }; }, async setPolicy() { return {}; }, async plan() { return {}; } };
  w.PartialNest = { canSeat: () => ({ ok: true }), chain: () => [], on() {},
    async preview(s, ids) { calls.push(['preview', ids.join('+')]); return { ok: true, pieces: 3, fitsAll: true, links: [{ partialId: PID, wMm: 60, hMm: 50, placed: 3 }], continues: { n: 0, next: 'none' } }; },
    async seat(s, ids, o) { calls.push(['seat', ids.join('+')]); o.onStep({ key: 'claim' }); o.onStep({ key: 'nest' }); return { ok: true, moved: 3, continues: 0 }; } };
  w.eval(fs.readFileSync(path.join(ROOT, 'charm-nest-partial-ui.js'), 'utf8')); w.eval(fs.readFileSync(path.join(ROOT, 'charm-nest-options-history.js'), 'utf8')); w.eval(fs.readFileSync(path.join(ROOT, 'charm-nest-options-modal.js'), 'utf8')); w.Gate.renderCard(sh);
  const gate = sh.el.querySelector('.shGate'), menu = gate._optBox, view = () => menu.querySelector('.osSource');
  return { w, dom, sh, toasts, sent, calls, open: () => gate.querySelector('.sheetOptionsBtn').click(), view, studioOpen: () => w.OptionsStudio.isOpen('rose'), busy: () => { const b = view() && view().querySelector('.psAsk .psBusy'); return b ? b.textContent.trim() : ''; } };
}
async function pressUse(pg) {
  pg.open(); await until(() => pg.view() && pg.view().querySelector(`.ohc[data-id="${PID}"] [data-ps="pick"]`));
  pg.view().querySelector(`.ohc[data-id="${PID}"] [data-ps="pick"]`).click();
  const use = await until(() => pg.view() && pg.view().querySelector('[data-ps="use"]:not([disabled])'));
  if (use) use.click(); return !!use;
}

async function partP() {
  seed('on');
  const pg = await page(true);
  try {
    const pressed = await pressUse(pg);
    const req = await until(() => pg.sent.find(b => b.op === 'runCmdPut'), 5000);
    const c = req && cmdsOf()[0];
    ok('P1 a viewer\'s Use this one sends the seat request the Sheets grid sends (runCmdPut seat: the sheet, the partial sheet, its chain, who, this computer) and claims nothing itself',
      pressed && !!req && req.runId === RUN && req.dev === PAGE && req.by === 'Michelle' && req.cmd.type === 'seat' && req.cmd.sheetId === SH && req.cmd.partialId === PID && JSON.stringify(req.cmd.partialIds) === JSON.stringify([PID])
      && !!c && c.state === 'waiting' && !pg.sent.some(b => /^(roseClaim|partialClaim)$/.test(b.op)) && !pg.calls.some(x => x[0] === 'seat'), { pressed, req, c, ops: pg.sent.map(b => b.op) });
    const said0 = await until(() => /cloud/i.test(pg.busy()) && pg.busy(), 3000);
    ok('P2 ... the panel says it went to the cloud and waits (never "done" before the cloud answers); the window stays open', !!said0 && pg.studioOpen() && !pg.toasts.some(t => t[1] === 'ok'), { said0, toasts: pg.toasts });
    // the cloud's pass: the partial sheet is reserved for the sheet, the sheet's nest on it is asked
    await cloudPass();
    const stk = F.doc('Charm_Nest_Rose_Stock/' + STK) || {}, rem = F.doc('Charm_Nest_Remnants/' + PID) || {}, rec = F.doc('Charm_Nest_Sheets/' + SH) || {}, old = F.doc('Charm_Nest_Rose_Stock/' + OLD) || {};
    const jobs = F.list('Charm_Nest_Jobs').filter(j => j.sheetId === SH), c1 = cmdsOf()[0];
    ok('P3 the cloud reserved THAT partial sheet for the sheet (the stock owned by it, the partial sheet in use by it) and gave the old metal back',
      stk.owner === SH && stk.available === false && rem.status === 'inUse' && rem.inUseBySheetId === SH && rec.roseStockId === STK && old.owner == null && old.available === true, { stk, rem: { status: rem.status, by: rem.inUseBySheetId }, rec: { roseStockId: rec.roseStockId }, old, cmd: c1 && { state: c1.state, text: c1.text, step: c1.step } });
    ok('P4 ... and queued the sheet\'s nest on it (one real nest job of the sheet, on that physical sheet); the request runs and says so',
      jobs.length === 1 && jobs[0].roseStockId === STK && jobs[0].status === 'queued' && jobs[0].pieceCount === 3 && c1.state === 'running' && /being nested/.test(c1.step || ''), { jobs: jobs.map(j => ({ id: j._id, st: j.status, stock: j.roseStockId, n: j.pieceCount })), cmd: c1 && { state: c1.state, step: c1.step, text: c1.text } });
    const said1 = await until(() => /being nested/.test(pg.busy()) && pg.busy(), 9000);
    ok('P5 ... the panel shows the cloud\'s own step while it nests', !!said1 && pg.studioOpen(), { busy: pg.busy() });
    // the nest ends (the solver's slots and the sheet writer are the cloud nest lane's own, tested with it: here its result on the record is stood in for)
    F.seed('Charm_Nest_Sheets/' + SH, Object.assign({}, F.doc('Charm_Nest_Sheets/' + SH), { layoutRev: 4, nestJob: { id: jobs[0]._id, status: 'done', at: Date.now() }, placements: [0, 1, 2].map(i => ({ id: 'c' + i, cxPt: 30 + i * 25, cyPt: 30, angle: 90 })) }));
    F.clock.advance(60000);
    await cloudPass();
    const c2 = cmdsOf()[0];
    ok('P6 the cloud answers done in words: the 3 pieces now lie on the partial sheet, the old sheet is back as Available', c2.state === 'done' && /3 pieces now lie on the partial sheet 100 × 50 mm/.test(c2.text || '') && /back in the repository as Available/.test(c2.text || ''), c2);
    const toast = await until(() => pg.toasts.find(t => t[1] === 'ok'), 9000); await until(() => !pg.studioOpen(), 5000);
    ok('P7 ... the panel says the cloud\'s answer (ok), the window closes and the list is read again next time', !!toast && /3 pieces now lie on the partial sheet/.test(toast[0]) && !pg.studioOpen() && pg.calls.some(x => x[0] === 'changed') && !pg.toasts.some(t => t[1] === 'bad'), { toasts: pg.toasts, open: pg.studioOpen() });
    ok('P8 ... PartialNest.seat was never called, no claim was ever sent from the page', !pg.calls.some(x => x[0] === 'seat') && !pg.sent.some(b => /^(roseClaim|partialClaim|roseRelease|nestStart|putSheet)$/.test(b.op)), pg.sent.map(b => b.op));
    // a refusal is said, never silent: the same partial sheet asked again (now this sheet's own)
    seed('on'); F.seed('Charm_Nest_Remnants/' + PID, Object.assign({}, F.doc('Charm_Nest_Remnants/' + PID), { status: 'inUse', inUseBySheetId: 'rose-other', inUseBySheetName: 'RG Sheet 7' }));
    F.seed('Charm_Nest_Rose_Stock/' + STK, Object.assign({}, F.doc('Charm_Nest_Rose_Stock/' + STK), { owner: 'rose-other', available: false }));
    pg.toasts.length = 0; const ok2 = await pressUse(pg); await until(() => cmdsOf().length, 5000); await cloudPass();
    const bad = await until(() => pg.toasts.find(t => t[1] === 'bad'), 9000), c3 = cmdsOf()[0];
    ok('P9 a partial sheet another sheet holds: the cloud refuses in words and the panel shows them (bad), nothing said done', ok2 && c3 && c3.state === 'failed' && !!bad && /not available any more|reserved|in use/i.test(bad[0]) && !pg.toasts.some(t => t[1] === 'ok') && /Partial sheet not used/.test(bad[0]), { c3, toasts: pg.toasts });
  } catch (e) { ok('P run', false, String(e && e.stack || e).split('\n').slice(0, 4).join(' | ')); }
  pg.dom.window.close();
}

async function partO() {
  seed('off');
  const pg = await page(false);
  try {
    const pressed = await pressUse(pg);
    const seat = await until(() => pg.calls.find(x => x[0] === 'seat'), 5000);
    await until(() => pg.toasts.some(t => /seated/.test(t[0])), 5000); await until(() => !pg.studioOpen(), 5000);
    ok('O1 mode off (not a viewer): Use this one seats through PartialNest.seat as before, the window closes, the person is told; no request is sent',
      pressed && !!seat && seat[1] === PID && !pg.sent.some(b => b.op === 'runCmdPut') && !cmdsOf().length && !pg.studioOpen() && pg.toasts.some(t => /3 pieces seated on the partial sheet/.test(t[0]) && t[1] === 'ok'), { seat, ops: pg.sent.map(b => b.op), toasts: pg.toasts });
  } catch (e) { ok('O run', false, String(e && e.stack || e).split('\n').slice(0, 4).join(' | ')); }
  pg.dom.window.close();
}

(async () => {
  await partS();
  await partP();
  await partO();
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  ok('H charm-nest-1.html loads the changed panel under a new token', /charm-nest-partial-ui\.js\?v=[^"]*-i11(?:-\w+)*"/.test(html));
  log0(failed ? `FAIL viewer-item11 (${failed})` : 'PASS viewer-item11');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-item11: ' + (e && e.stack || e)); process.exit(1); });
