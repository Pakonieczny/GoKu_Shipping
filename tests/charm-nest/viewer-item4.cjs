// ITEM 4 (Paul, 21 Nov 2026): Cut Sheet (Rose Gold, 10K, 14K) pressed on a staff computer (a viewer) while the cloud carries every run (mode "on").
//   S  offline (_cloudFakes.cjs, mode "on"): a page's roseCutAsk (cut, line) is taken; its rosePlan / roseRecordCut / roseReopen are still refused; the cloud's rosecut unit
//      (_charmNestGreenLine.js askUnit) acts on each ask: a 14K sheet in its set is claimed, its line drawn and its cut recorded (answer "done"); "line" draws the line and records
//      no cut; a sheet whose layout is not saved is refused in words (answer "refused" with them); mode "off" takes the page's own rosePlan as before.
//   P  the page (charm-nest-rose-ui.js in a vm): on a viewer, Cut Sheet sends ONE roseCutAsk { sheetId, kind: "cut", by }, the card says it was asked, then follows roseCutAskGet:
//      "done" reads the sheet again (cut shown), "refused" puts the cloud's words under the button; the automatic contour asks nothing; Manual Control's reopen (patch) sends no
//      roseReopen; mode "off": the press runs here (roseClaim / rosePlan / roseRecordCut), no ask.
// Offline only: no Etsy, no live call. Made-up names.   NODE_PATH=<deps> node tests/charm-nest/viewer-item4.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1200))); if (!cond) failed++; };
const CV = 'cloud14-20261120', PAGE = 'd0michelle1', R = 'run-2026-11-21-item4';
const outline = { subpaths: [[['m', [0, 0]], ['l', [10, 0]], ['l', [10, 10]], ['l', [0, 10]], ['h']]] };

async function partS() {
  process.env.EDIT_PASSCODE = 'test-pass';
  const T0 = Date.UTC(2026, 10, 21, 15, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = console.log, qw = console.warn; console.log = () => {}; console.warn = () => {};
  try {
    const lib = F.load('charmNestLibrary.js'), Green = F.load('_charmNestGreenLine.js'), Shapes = F.load('_charmNestRoseShapes.js'), Rose = require(path.join(ROOT, 'charm-nest-rose.js'));
    // (the designs are not in this fake cloud: the charms' outlines are the record's own squares, as the page would draw them)
    Shapes.forSheet = async (ctx, rec) => Rose.shapes((rec.charms || []).map(c => ({ id: c.id, outline, members: [], centerPt: [5, 5] })), rec.placements);
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const sheet = (id, metal, o) => Object.assign({ id, runId: R, metal, page: 1, day: '2026-11-21', draft: false, setId: 'set-1', setSeq: 1, sheetIndex: 1, solidIncluded: true, fileBase: metal + '-Set-1-1', status: 'complete',
      outputs: { ai: { path: `charmnest/x/${id}.ai` } }, verification: { ok: true }, stock: { wPt: 300, hPt: 150 }, charms: [{ id: 'c-' + id, poolId: `4100000001_41000000011_1`, order: '4100000001' }],
      placements: [{ id: 'c-' + id, cxPt: 20, cyPt: 20, angle: 0, scale: 1 }], poolIds: ['4100000001_41000000011_1'], density: .1 }, o || {});
    const seed = mode => {
      F.reset(); F.clock.set(T0);
      F.seed('Charm_Nest_Open/drive', { mode, runMode: 'manual', cloudSteps: ['rosecut', 'files'], flippedAt: T0 - 3600e3 }); lib._driveForget();
      F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-21', releasePolicy: 2, sheets: {}, holds: {}, errors: [], orders: [], solidIncluded: {} });
      F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
      F.seed('Charm_Nest_Sets/set-1', { setId: 'set-1', seq: 1, runId: R, day: '2026-11-21', group: 'dispatch', name: 'Set-1', status: 'nesting', formedAt: T0 - 7200e3, sheetIds: ['gold14k-k1', 'gold14k-k2', 'gold14k-k3'], labelFiles: [], materials: ['gold14k'], orders: {}, sheetNos: {} });
      F.seed('Charm_Nest_Sheets/gold14k-k1', sheet('gold14k-k1', 'gold14k'));
      F.seed('Charm_Nest_Sheets/gold14k-k2', sheet('gold14k-k2', 'gold14k', { dirty: true }));
      F.seed('Charm_Nest_Sheets/gold14k-k3', sheet('gold14k-k3', 'gold10k'));
    };
    seed('on');
    const run = F.doc('Charm_Nest_Runs/' + R), ctx = { db: F.db, run, runId: R, call: async (b, u) => lib.call(b, { unit: String(u || 'item4') }), read: async b => lib.call(b, {}), budgetEnd: Date.now() + 600000, say: () => {} };
    // the page's own production ops are still the cloud's
    for (const op of ['rosePlan', 'roseRecordCut', 'roseReopen', 'roseClaim']) {
      const r = await P({ op, sheetId: 'gold14k-k1', stockId: 'rgs-x', metal: 'gold14k', dev: PAGE, cv: CV });
      ok(`S0 mode on: a viewer's ${op} is still refused in words (the cloud's work)`, r.http === 409 && r.cloudCarries === true, r);
    }
    let r = await P({ op: 'roseCutAsk', sheetId: 'gold14k-k1', kind: 'cut', by: 'Michelle', dev: PAGE, cv: CV });
    ok('S1 mode on: a viewer\'s Cut Sheet ask is taken (state asked, by whom)', r.http === 200 && r.ok && r.ask && r.ask.state === 'asked' && r.ask.by === 'Michelle' && r.ask.kind === 'cut', r);
    r = await P({ op: 'roseCutAsk', sheetId: 'gold14k-k3', kind: 'line', by: 'Michelle', dev: PAGE, cv: CV });
    ok('S2 ... the line alone ("line", the Library\'s Add the green dash line) is taken', r.http === 200 && r.ok && r.ask && r.ask.kind === 'line', r);
    r = await P({ op: 'roseCutAsk', sheetId: 'gold14k-k2', kind: 'cut', by: 'Michelle', dev: PAGE, cv: CV });
    ok('S3 ... and a third ask', r.http === 200 && r.ok, r);
    r = await P({ op: 'roseCutAsk', sheetId: 'gold14k-k2', kind: 'nonsense', by: 'Michelle', dev: PAGE, cv: CV });
    ok('S4 ... an unknown kind is refused in words', !!r.error, r);
    await Green.askUnit(ctx);
    const k1 = F.doc('Charm_Nest_Sheets/gold14k-k1'), a1 = F.doc('Charm_Nest_Cut_Asks/gold14k-k1');
    ok('S5 the cloud\'s rosecut unit cut K1 (14K): claimed, line drawn, cut recorded with the person\'s name; the ask says done', a1.state === 'done' && +k1.roseCutAt > 0 && !!k1.roseStockId && !!k1.rosePlanHash && a1.cutAt === k1.roseCutAt,
      { ask: a1, k1: { roseCutAt: k1.roseCutAt, roseStockId: k1.roseStockId, plan: !!k1.rosePlanHash } });
    const cut = k1.roseStockId ? F.list(`Charm_Nest_Rose_Stock/${k1.roseStockId}/cuts`)[0] : null;
    ok('S6 ... the cut record names Michelle', !!cut && cut.by === 'Michelle', cut);
    const k3 = F.doc('Charm_Nest_Sheets/gold14k-k3'), a3 = F.doc('Charm_Nest_Cut_Asks/gold14k-k3');
    ok('S7 "line": K3 (10K) has its green line drawn and dated, NO cut recorded; the ask says done', a3.state === 'done' && !!k3.rosePlanHash && !k3.roseCutAt && JSON.parse(k3.rosePlanJson).stages.length === 1, { ask: a3, k3: { plan: !!k3.rosePlanHash, cut: k3.roseCutAt } });
    const a2 = F.doc('Charm_Nest_Cut_Asks/gold14k-k2');
    ok('S8 a sheet whose layout is not saved is refused in words, nothing cut', a2.state === 'refused' && /nest and save this layout/.test(a2.why || '') && !F.doc('Charm_Nest_Sheets/gold14k-k2').roseCutAt, a2);
    seed('off');
    r = await P({ op: 'roseReopen', sheetId: 'gold14k-k1', by: 'Michelle', dev: PAGE, cv: CV });
    ok('S9 mode off: a page\'s green-line op is not refused (every page as before)', !r.cloudCarries, r);
    ok('S10 Etsy never called', F.etsy.calls === 0);
  } finally { console.log = quiet; console.warn = qw; }
}

/* ── P: charm-nest-rose-ui.js on a page, its io faked ── */
async function partP() {
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-rose-ui.js'), 'utf8');
  function page(o) {
    const calls = [], toasts = [], asks = o.asks || [];
    const el = () => ({ querySelector: () => null, querySelectorAll: () => [], classList: { toggle() {} }, before() {}, insertBefore() {} });
    const sh = Object.assign({ metal: 'gold14k', sheetId: 'gold14k-k1', runId: R, el: null, charms: [{ id: 'c', outline, centerPt: [5, 5], members: [] }], placements: [{ id: 'c', cxPt: 20, cyPt: 20, angle: 0, scale: 1 }],
      persistedDone: true, verification: { ok: true }, status: 'complete', dirty: false, setId: 'set-1', draft: false }, o.sheet || {});
    const w = { console: { log() {}, warn() {}, error() {} }, setTimeout, clearTimeout, Promise, Date, JSON, Math, Object, Array, String, Number, Error, Set, Map, WeakMap, queueMicrotask, structuredClone };
    w.window = w; w.CharmNestRose = require(path.join(ROOT, 'charm-nest-rose.js'));
    w.CNEmployee = { name: () => 'Michelle' };
    if (o.viewer) { w.CNViewer = { on: () => true }; w.CharmNestCloudLanes = name => name === 'rosecut'; }
    w.Session = { schedule() {} };
    w.CN = { S: { cloud: { ok: true }, settings: {}, mode: 'nest' }, esc: s => String(s), toast: (t, k) => toasts.push(String(t)), allSheets: () => [], renderCard() {}, drawPreview() {}, stockFor: () => ({ wPt: 300, hPt: 150 }), uid: () => 'u',
      api: async (fn, body) => {
        calls.push(body);
        if (body.op === 'roseCutAsk') return { ok: true, ask: { sheetId: body.sheetId, kind: body.kind, by: body.by, at: Date.now(), seq: 1, state: 'asked' } };
        if (body.op === 'roseCutAskGet') return { ok: true, asks: asks.length > 1 ? [asks.shift()] : asks.slice() };
        if (body.op === 'getSheet') return { sheet: { id: body.id, metal: 'gold14k', roseCutAt: 1790000000000, rosePlanHash: 'h', rosePlanJson: JSON.stringify({ lines: [[[0, 0], [1, 1]]], stages: [{ n: 1, at: 1, ids: ['c'], lines: [0, 1] }], shapes: [{ id: 'c' }] }), setId: 'set-1', draft: false } };
        if (body.op === 'roseClaim') return { stock: { id: 'rgs-k1', revision: 0 } };
        if (body.op === 'roseGet') return { stock: { id: 'rgs-k1', revision: 0 }, cuts: [] };
        if (body.op === 'roseReopen') return { ok: true };
        return { error: 'not in this test' };
      } };
    vm.createContext(w);
    vm.runInContext(src, w);
    w.RoseStock.ASK.pollMs = 5; w.RoseStock.ASK.waitMs = 2000;
    return { w, sh, calls, toasts };
  }
  { const { w, sh, calls, toasts } = page({ viewer: true, asks: [{ sheetId: 'gold14k-k1', seq: 1, state: 'working' }, { sheetId: 'gold14k-k1', seq: 1, state: 'done', cutAt: 1790000000000 }] });
    const p = w.RoseStock.record(sh, {});
    await new Promise(r => setTimeout(r, 0)); await new Promise(r => setTimeout(r, 0));
    const asking = !!(sh._roseAsk && sh._roseAsk.state === 'asked');
    let err = null; try { await p; } catch (e) { err = e.message; }
    const ask = calls.find(c => c.op === 'roseCutAsk');
    ok('P1 viewer: Cut Sheet sends ONE roseCutAsk { sheetId, kind cut, by } and no production op', !!ask && ask.sheetId === 'gold14k-k1' && ask.kind === 'cut' && ask.by === 'Michelle' && calls.filter(c => c.op === 'roseCutAsk').length === 1 && !calls.some(c => /^rose(Plan|RecordCut|Claim|Reopen)$/.test(c.op)), calls.map(c => c.op));
    ok('P2 ... the card says it was asked while the cloud works, then the cut as the record has it (done)', asking && !err && sh.roseCutAt === 1790000000000 && !sh._roseAsk && toasts.some(t => /asked/.test(t)) && toasts.some(t => /Sheet cut by the cloud/.test(t)), { asking, err, cut: sh.roseCutAt, toasts }); }
  { const { w, sh } = page({ viewer: true, asks: [{ sheetId: 'gold14k-k1', seq: 1, state: 'refused', why: 'Not cut: nest and save this layout before preparing its contour' }] });
    let err = null; try { await w.RoseStock.record(sh, {}); } catch (e) { err = e.message; }
    ok('P3 ... a refusal comes back in the cloud\'s own words, nothing cut', /nest and save this layout/.test(err || '') && !sh.roseCutAt, err); }
  { const { w, sh, calls } = page({ viewer: true });
    await w.RoseStock.plan(sh);
    sh.roseCutAt = null; sh.rosePlanHash = 'h'; sh.rosePlan = { lines: [[[0, 0], [1, 1]]] };
    const r = await w.RoseStock.reopen(sh, { force: true, patch: true, by: 'Michelle' });
    ok('P4 viewer: the automatic contour asks nothing; Manual Control\'s reopen (patch) sends no roseReopen and gives up the page\'s copy', !calls.length && r.changed && !sh.rosePlanHash, { calls: calls.map(c => c.op), r }); }
  { const { w, sh, calls } = page({ viewer: false });
    try { await w.RoseStock.record(sh, {}); } catch (_) { /* the fake answers no plan: only the calls matter */ }
    ok('P5 mode off: Cut Sheet runs on this page as before (roseClaim, then rosePlan), no ask', !calls.some(c => c.op === 'roseCutAsk') && calls.some(c => c.op === 'roseClaim'), calls.map(c => c.op)); }
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  ok('P6 charm-nest-1.html loads the changed files under new tokens', ['charm-nest-rose-ui.js', 'charm-nest-sheetwin.js', 'charm-nest-set-repair.js', 'charm-nest-flow.js'].every(f => new RegExp(f.replace(/\./g, '\\.') + '\\?v=[^"]*-it4(?:-\\w+)*"').test(html)));
}

(async () => {
  await partS();
  await partP();
  log0(failed ? `FAIL viewer-item4 (${failed})` : 'PASS viewer-item4');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-item4: ' + (e && e.stack || e)); process.exit(1); });
