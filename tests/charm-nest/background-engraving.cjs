// Engraving never holds up placement, and an update prepares only the lines that need it (Paul, 24 Sep).
//   1. Pool.addAll makes up a line that could not go on a sheet again only when something it depends on changed.
//   2. Engrave.background reads and fits beside the run, once more when asked meanwhile, and tells the run when done.
//   3. The run's Engraving step starts that work without waiting for it, and looks again when it settles mid-pass.
//   4. Only work that changed something wakes the run again.
//   5. A back file is built and uploaded without the production lock; only recording it on its sheet takes it.
// The real module code runs in a vm; the page, the station and the cloud are small stand-ins. No network.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const O = require('../../charm-nest-orders.js');
const Ops = require('../../charm-nest-operations.js');
const source = fs.readFileSync('charm-nest-bridge.js', 'utf8');
const slice = (from, to) => { const a = source.indexOf(from), b = source.indexOf(to, a); assert(a >= 0 && b > a, 'slice ' + from); return source.slice(a, b); };
const tick = () => new Promise(r => setImmediate(r));

/* ── 1 · Pool.addAll ── */
async function poolTest() {
  let now = 1_000_000;
  const calls = { fetch: 0, bars: [], poolPut: 0, design: 0 };
  const entries = new Map();
  const page = { metal: 'silver', charms: [], placements: [], status: 'idle' };
  const rows = [];
  const mk = (id, sku, extra = {}) => {
    const order = { receiptId: id, createTs: +id, updateTs: 1 }, line = { transactionId: 't' + id, sku, title: sku };
    const r = { key: id + '/t' + id, order, line, spec: null, problems: [], state: 'pulled', reason: null, poolIds: [], engrave: null, ...extra };
    rows.push(r); return r;
  };
  const spec = r => ({ designSku: r.line.sku, material: r.materialOverride || (r.line.sku === 'NEEDS' ? null : 'silver'), quantity: 1, size: null, problems: r.line.sku === 'NEEDS' && !r.materialOverride ? [{ kind: 'needsMaterial' }] : [] });
  const small = { charms: [{ areaPt2: 400, widthPt: 20, heightPt: 20, hash: 'h' }] }, huge = { charms: [{ areaPt2: 9e9, widthPt: 5000, heightPt: 5000, hash: 'H' }] };
  entries.set('OK', { sku: 'OK', aiPath: 'ok.ai', masterHash: 'm1' });
  entries.set('BIG', { sku: 'BIG', aiPath: 'big.ai', masterHash: 'm2' });
  entries.set('BROKEN', { sku: 'BROKEN', aiPath: 'broken.ai', masterHash: 'm3' });
  const sources = new Map([['ok.ai', small], ['big.ai', huge]]);
  const ctx = {
    window: { CNProgress: { start: (label, o) => { calls.bars.push([label, o.total]); return { set() {}, end() {} }; } }, LiveNest: { intakePage: () => page, closed: () => false } },
    Date: { now: () => now }, JSON, Map, Set, Promise, Math, Object, Array, String, Number, Error, console,
    B: { pool: { rows: new Map(), sources }, master: { entries } }, S: { cloud: { ok: true }, settings: { insetPt: 0, maxFill: 0.8, silhouetteRes: 6, minPt: 6 }, sheets: { silver: { active: 0, pages: [page] } }, poolSources: {} },
    O, MM: 25.4 / 72,
    Master: { entryFor: sku => entries.get(String(sku).toUpperCase()) || null, fetchEntry: async () => { calls.fetch++; return null; }, skuRegex: () => /x/ },
    Orders: { rows: () => rows, interpretAll() { for (const r of rows) { r.spec = spec(r); r.problems = r.spec.problems.slice(); } }, render() {}, lineRecord: r => [r.key, r.state] },
    Gate: { plan: async list => ({ take: new Set(list.map(r => r.key)), wait: new Map() }), afterPool: async () => {} },
    Review: { syncOrderItems() {}, problemText: p => p.kind },
    RunCtl: { save: async () => {} },
    api: async (fn, body) => { if (body.op === 'poolPut') { calls.poolPut++; return {}; } if (body.op === 'url') { calls.design++; throw new Error('design file unreachable'); } return {}; },
    CharmNestAssets: { bytes: async () => { throw new Error('unreachable'); } }, P: {},
    stockFor: () => ({ wPt: 1000, hPt: 1000 }), labelOf: m => m, allSheets: () => [page], pagesOf: () => [page], addPage: () => page,
    sheetDirty() {}, renderCard() {}, renderRail() {}, updateTopSub() {}, refreshAllCards() {}, agent() {},
  };
  ctx.CNProgress = ctx.window.CNProgress; ctx.LiveNest = ctx.window.LiveNest;   // the page reads them as globals
  vm.createContext(ctx);
  vm.runInContext(slice('const Pool = window.Pool = (() => {', '/* Carry-forward'), ctx);
  const Pool = ctx.window.Pool;
  const run = { runId: 'run', lines: {} };
  const held = mk('1', 'NEEDS'), unknown = mk('2', 'NOPE'), big = mk('3', 'BIG'), broken = mk('4', 'BROKEN'), fine = mk('5', 'OK');
  await Pool.addAll(run);
  assert.deepEqual(calls.bars, [['Preparing 5 order lines', 5]]);
  assert.equal(held.state, 'held'); assert.equal(unknown.state, 'unmatched'); assert.equal(big.state, 'oversize'); assert.equal(broken.state, 'held'); assert.equal(fine.state, 'pooled');
  // (a design that would not load is asked for by the preload and by the line itself)
  assert.equal(calls.fetch, 1); assert(calls.design >= 1); const design1 = calls.design;

  // the next update: one new line, nothing else changed: only the new line is made up, and no unknown SKU looked up again
  const next = mk('6', 'OK');
  await Pool.addAll(run);
  assert.deepEqual(calls.bars.at(-1), ['Preparing 1 order line', 1], 'only the new line is prepared');
  assert.equal(next.state, 'pooled'); assert.equal(calls.fetch, 1, 'an unknown SKU is not looked up at every update'); assert.equal(calls.design, design1, 'a design that would not load waits');

  // an update with nothing new shows no bar at all
  const bars = calls.bars.length; await Pool.addAll(run); assert.equal(calls.bars.length, bars);

  // what a line depends on changes: a person picks its metal, the library learns its SKU, time passes for a failed design
  held.materialOverride = 'silver'; held.line.sku = 'NEEDS'; entries.set('NEEDS', { sku: 'NEEDS', aiPath: 'ok.ai', masterHash: 'm4' });
  entries.set('NOPE', { sku: 'NOPE', aiPath: 'ok.ai', masterHash: 'm5' });
  now += 5 * 60000;
  await Pool.addAll(run);
  assert.deepEqual(calls.bars.at(-1), ['Preparing 3 order lines', 3]);
  assert.equal(held.state, 'pooled'); assert.equal(unknown.state, 'pooled'); assert.equal(broken.state, 'held'); assert(calls.design > design1, 'tried again once its wait is over');
  assert.equal(big.state, 'oversize', 'an oversize line is not tried again while nothing changed');
  // a plate made bigger lets the oversize line be tried again
  ctx.stockFor = () => ({ wPt: 1e6, hPt: 1e6 });
  sources.set('big.ai', small);
  await Pool.addAll(run);
  assert.equal(big.state, 'pooled');
}

/* ── 2 · Engrave.background ── */
async function backgroundTest() {
  const log = [];
  let release, gate = new Promise(r => { release = r; }), work = 1;
  const ctx = { B: { run: { runId: 'r1' } }, Promise, console,
    classifyAll: async r => { log.push('classify:' + (r && r.runId)); await gate; const n = work; work = 0; return n; },
    fitAll: async r => { log.push('fit:' + (r && r.runId)); return 0; },
    agent: (s, k, t) => log.push('warn:' + t), RunCtl: { backgroundSettled: () => log.push('settled') } };
  vm.createContext(ctx);
  vm.runInContext(slice('  let backgroundPass = null, backgroundAgain = null, settledPasses = 0;', '  function revokeBacks(') + ';this.background=background;this.settled=()=>settledPasses;', ctx);
  const first = ctx.background({ runId: 'r1' });
  const again = ctx.background({ runId: 'r2' });
  assert.equal(first, again, 'a pass asked for while one runs joins it');
  release(); await first;
  assert.deepEqual(log, ['classify:r1', 'fit:r1', 'classify:r2', 'fit:r2', 'settled'], 'one more pass for the later ask, then the run is told once');
  assert.equal(ctx.settled(), 1);
  // a pass with nothing to do does not wake the run
  log.length = 0; await ctx.background(null);
  assert.deepEqual(log, ['classify:r1', 'fit:r1']); assert.equal(ctx.settled(), 1);
}

/* ── 3 · the run's Engraving step ── */
function controllerContext({ background, onSets }) {
  let controller = slice('const RunCtl =', '/* ═══ 24 · Review');
  const renderStart = controller.indexOf('  function renderBanner()'), renderEnd = controller.indexOf('  return { optionsChanged', renderStart);
  controller = controller.slice(0, renderStart) + '  function renderBanner() {}\n' + controller.slice(renderEnd);
  controller = controller.replace('return { optionsChanged', 'return { _loop:loop, optionsChanged');
  const calls = [], jobs = new Map();
  const r = { runId: 'run', step: 'engrave', status: 'running', mode: 'auto', sheets: {}, errors: [], orders: ['order'], workspaceRestored: true };
  const rows = [{ key: 'order:1', order: { receiptId: 'order' }, state: 'pooled', poolIds: ['p'] }];
  const asyncCall = name => async () => { calls.push(name); };
  const ctx = { window: {}, B: { run: r }, S: { cloud: { ok: true }, settings: { autoCommit: 'on' } }, O, Date, Promise, Map, Set, JSON, queueMicrotask, setTimeout, clearTimeout, setInterval, clearInterval,
    allSheets: () => [], Orders: { rows: () => rows, lineRecord: x => [x.key, x], revalidate: async () => ({ changed: [] }), unclaim: asyncCall('unclaim'), pull: async () => rows },
    Pool: { sheetOf: id => (id === 'p' ? { sheetId: 's' } : null), addAll: async () => 0 }, Engrave: { items: () => jobs, pendingCount: () => 0, background: run => { calls.push('background'); return background(run); }, saveBacks: async j => { calls.push('backs:' + j.key); } },
    Review: { count: () => 0 }, Gate: { flush: asyncCall('flush'), nestable: () => true, modern: () => true, assemble: asyncCall('assemble'), upgrade: asyncCall('upgrade') },
    LiveNest: { finish: asyncCall('finish') }, Sets: { ofRun: () => { if (onSets) onSets(); return []; }, releaseIssue: () => null, save: asyncCall('set-save') },
    api: async () => ({}), Session: { schedule() {} }, LiveStrip: { render() {} }, Arrivals: { start() {} },
    agent() {}, toast() {}, notifyPerson() {}, ding() {}, CN: { renderCard() {} }, sheetName: s => s.metal, startNest() {}, sheetDirty() {}, METALS: [] };
  vm.createContext(ctx); vm.runInContext(controller, ctx);
  return { ctl: ctx.window.RunCtl, r, calls, jobs };
}
const count = (calls, name) => calls.filter(c => c === name).length;
async function rest(f) { for (let i = 0; i < 50; i++) { await tick(); if (f.r.status !== 'running') return; } throw new Error('the run did not come to rest'); }
async function runTest() {
  // the step does not wait: a pass that never ends still lets the run come to rest
  const hang = controllerContext({ background: () => new Promise(() => {}) });
  hang.jobs.set('j', { key: 'j', state: 'approved', fit: {}, view: {}, copies: ['p'], row: { poolIds: ['p'], order: { receiptId: 'order' } } });
  // an approval restored without its fit has nothing to write yet: the step leaves it to be fitted again
  hang.jobs.set('n', { key: 'n', state: 'approved', fit: null, view: null, copies: ['p'], row: { poolIds: ['p'], order: { receiptId: 'order' } } });
  await hang.ctl._loop();
  assert.equal(hang.r.status, 'processed', 'the run rests while the words are still being read');
  assert(hang.calls.includes('background')); assert(hang.calls.includes('backs:j'), 'an approved back not written yet is written beside the run');
  assert(!hang.calls.includes('backs:n'), 'an approval with no fit is not sent to be written');

  // a pass that settles after the run passed Engraving (here: during its next step) sends it through the steps again
  let mid, once = false;
  mid = controllerContext({ background: () => Promise.resolve(), onSets: () => { if (!once) { once = true; mid.ctl.backgroundSettled(); } } });
  await mid.ctl._loop(); await rest(mid);
  assert.equal(mid.r.status, 'processed');
  assert.equal(count(mid.calls, 'background'), 2, 'the steps from Engraving on ran again once');

  // at rest, an unchanged poke does nothing, and a settled pass takes the run up again though its record holds the change
  const before = count(mid.calls, 'background');
  mid.ctl.poke(); await tick(); assert.equal(count(mid.calls, 'background'), before, 'an unchanged poke does nothing');
  mid.ctl.backgroundSettled(); await rest(mid); for (let i = 0; i < 10 && count(mid.calls, 'background') === before; i++) await tick();
  assert.equal(count(mid.calls, 'background'), before + 1, 'a settled pass is looked at');
  assert.equal(mid.r.status, 'processed');
}

/* ── 4 · only work that changed something wakes the run (else a pass with nothing to do would start it over and over) ── */
async function wakeTest() {
  let settled = 0;
  const jobs = new Map();
  const ctx = { Promise, Map, Set, Object, Error, console, B: { run: null }, render() {}, refreshBacks() {}, agent() {},
    Orders: { render() {}, rows: () => [], lineRecord: r => [r.key, r] }, Session: { schedule() {} }, Review: { add() {} }, RunCtl: { backgroundSettled: () => { settled++; }, save: async () => {} },
    items: () => jobs, isWorking: () => false, canFit: () => true, Pool: { sheetOf: () => ({ fileBase: 'GF_1' }) }, writeBacks: async () => {}, fitJob: async () => {} };
  vm.createContext(ctx);
  vm.runInContext(slice('  async function saveBacks(job) {', '  /** A reload while an approval') + slice('  /** Fits every job whose words are read', '  /* Reading the words and fitting them') + ';this.saveBacks=saveBacks;this.fitAll=fitAll;', ctx);
  const job = { key: 'k', state: 'approved', approvedAt: 1, backs: [], row: { order: { receiptId: 'o' }, engrave: {} } };
  await ctx.saveBacks(job);
  assert.equal(settled, 0, 'a save that wrote nothing does not wake the run');
  ctx.writeBacks = async j => { j.backs.push({}); j.state = 'written'; };
  await ctx.saveBacks(job); assert.equal(settled, 1, 'a written back wakes it once');
  job.state = 'approved'; ctx.writeBacks = async () => { throw new Error('upload failed'); };
  await ctx.saveBacks(job); assert.equal(settled, 2); assert.equal(job.state, 'review', 'a failed save goes to review and wakes it');
  // a fit that leaves the job ready (its charm not on the sheet yet) counts for nothing; one that moves it on counts
  const ready = { key: 'r', state: 'ready', copies: ['p'], row: { state: 'pooled', order: { receiptId: 'o' }, engrave: {} } };
  jobs.set('r', ready);
  assert.equal(await ctx.fitAll(null), 0);
  ctx.fitJob = async j => { j.state = 'review'; };
  assert.equal(await ctx.fitAll(null), 1);
}

/* ── 5 · back files and the production lock ── */
async function lockTest() {
  const ops = Ops.create();
  const src = slice('  let backQueue = Promise.resolve();', '  /** Parse the written back file');
  const events = [];
  let uploadGate, uploaded = new Promise(r => { uploadGate = r; });
  const sh = { sheetId: 's1', fileBase: 'GF_1', folderPath: 'f', runId: 'run', metal: 'gold', backPool: [] };
  const job = { key: 'k', state: 'approved', approvedAt: 5, approvedBy: 'P', copies: ['p1'], fit: { glyphs: [], size: 5, capMm: 2, weight: 400, angle: 0, centre: [0, 0], rect: null, metrics: {} }, view: { cx: 0, cy: 0, angleDeg: 0, cutMembers: [], upAngle: 0 }, verify: { geometry: {} }, text: 'Hi', lines: ['Hi'], row: { order: { receiptId: 'o' }, line: { transactionId: 't' }, spec: { designSku: 'S' }, engrave: {} }, backs: [] };
  const ctx = { window: { CharmNestOperations: ops }, B: { run: { runId: 'run' }, pool: { rows: new Map() } }, S: { cloud: { ok: true }, settings: {} }, Promise, Map, Set, Object, Error, JSON, console,
    charmFor: () => ({ sourceId: 'x' }), sourceOf: () => ({ parsed: {} }), sheetFor: () => sh, allSheets: () => [sh], PT: 72 / 25.4,
    renderBack: () => ({ toBlob: cb => cb(new Uint8Array(1)), _sizePt: { w: 1, h: 1 } }), fitOpts: () => ({ lineGap: 0.18 }),
    P: { buildBackFile: async () => ({ bytes: new Uint8Array(2), reference: {}, wPt: 1, hPt: 1 }) },
    verifyBackFile: async () => ({ ok: true }),
    uploadBytes: async path => { events.push('upload'); await uploaded; return { path, url: 'https://x/' + path }; },
    api: async (fn, body) => { events.push(body.op); return { sheet: sh }; },
    Pool: { update: async () => {} }, Review: { add() {} }, refreshBacks() {}, scheduleBackOutputs() {}, render() {}, agent() {}, toast() {}, syncEditedBack: async () => {} };
  vm.createContext(ctx);
  vm.runInContext(src + ';this.writeBacks=writeBacks;', ctx);
  const saving = ctx.writeBacks(job);
  await tick(); await tick();
  assert(events.includes('upload'), 'the back is being uploaded');
  // meanwhile the next intake takes the production lock and finishes: it does not wait for the back
  let intakeDone = false;
  await Promise.race([ops.run({ key: 'intake:run', label: 'Adding incoming orders', resources: ['production:run'] }, async () => { intakeDone = true; }),
    new Promise((_, reject) => setTimeout(() => reject(new Error('the intake waited for the back file upload')), 3000))]);
  assert(intakeDone, 'the intake ran while the back file was uploading');
  uploadGate(); await saving;
  assert(events.includes('backPut')); assert.equal(job.state, 'written'); assert.equal(sh.backPool.length, 1);
}

(async () => {
  await poolTest();
  await backgroundTest();
  await runTest();
  await wakeTest();
  await lockTest();
  console.log('Background engraving OK: incremental intake, background pass, run re-check, back files off the production lock');
})().catch(e => { console.error(e); process.exitCode = 1; });
