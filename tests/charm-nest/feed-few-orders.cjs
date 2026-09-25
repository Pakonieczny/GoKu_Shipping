// A big batch goes onto a sheet a few orders at a time, and the charms show in their own line colours as they land
// (Paul, 25 Sep: "put it back to the way it was where I could actually see the charms appearing with their full line
// colours and the charms were coming in a few at a time and then the system was placing them"). A run that took every
// open order put 138 charms on one Gold sheet and weighed them all together, drawn as green and blue masks.
//   1. A sheet's search takes the oldest three orders; the rest wait on the sheet for their turn.
//   2. Once those are placed and the sheet saved, the next three come in on top of them; a full sheet passes the orders
//      still waiting to the next sheet in line, which takes them after the few it is placing.
//   3. The charms that landed and the one being tried are drawn with their artwork, no masks.
// The real page code runs in a vm with a stand-in worker and canvas. No network.
//   node tests/charm-nest/feed-few-orders.cjs
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const root = path.join(__dirname, '../..'), html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
const slice = (from, to) => { const a = html.indexOf(from), b = html.indexOf(to, a); assert(a >= 0 && b > a, 'slice ' + from); return html.slice(a, b); };
const plain = v => JSON.parse(JSON.stringify(v));   // made in the vm, compared here

/* ── 1 · the oldest few orders first ── */
const jobs = [], starts = [], moved = [];
const ctx = {
  window: {}, S: { settings: { insetPt: 1, clearancePt: 0, maxFill: .8, budgetS: 180, seed: 1 }, nestQueue: [], cloud: { ok: true } },
  Set, Map, Math, JSON, Object, Array, String, Number, Uint32Array, crypto: { getRandomValues: a => { a[0] = 7; return a; } }, performance: { now: () => 0 },
  stockFor: () => ({ wPt: 200, hPt: 100 }), activeCharms: sh => sh.charms.filter(c => !c.excluded), angleSet: () => [0, 90],
  labelOf: m => m, renderCard() {}, drawPreview() {}, toast() {}, log() {}, agent: () => null, agentUpdate() {}, cancelGPUComparison() {}, renderProgress() {},
  uid: () => 'job' + jobs.length, parallelCount: () => 1, CharmNestSolver: { publicLayout: l => l },
  ensureWorkers: () => [{ postMessage: m => jobs.push(m.job) }], allSheets: () => [], startNest: sh => starts.push(sh),
  overflowToNextSheet: sh => { moved.push(sh.rejects.slice()); sh.charms = sh.charms.filter(c => !sh.rejects.includes(c.id)); sh.rejects = []; },
};
vm.createContext(ctx);
vm.runInContext(slice('const CAREFUL_ANGLES', 'function renderNestFlow('), ctx);
vm.runInContext(slice('function startNestReady(', '/* ═══ 9b'), ctx);
// ten orders, oldest last in the list; order o7 has two charms
const charm = (id, order, orderDate) => ({ id, order, orderDate, w: 4, h: 4, scale: 1, bits: new Uint8Array(16).fill(1), areaPt2: 16, hash: 'h' + id });
const orders = Array.from({ length: 10 }, (_, i) => 'o' + i);
const gold = { metal: 'gold', runId: 'run-1', page: 1, status: 'ready', placements: [], rejects: [], log: [],
  charms: orders.flatMap((o, i) => o === 'o7' ? [charm(o + 'a', o, 100 - i), charm(o + 'b', o, 100 - i)] : [charm(o, o, 100 - i)]) };
const every = gold.charms.slice();
ctx.allSheets = () => [gold];
ctx.startNestReady(gold, {});
let job = jobs.at(-1);
assert.deepEqual(job.pieces.map(p => p.id).sort(), ['o7a', 'o7b', 'o8', 'o9'], 'the oldest three orders go on first, an order whole');
assert.equal(gold.feedWait.length, 7, 'the other seven wait on the sheet for their turn');
assert.equal(gold.charms.length, 11, 'and stay on it');
assert.deepEqual(plain(gold.progress), [0, 11], 'the card counts every charm the sheet holds');
assert.deepEqual(ctx.nestItems(gold).map(c => c.id).sort(), ['o7a', 'o7b', 'o8', 'o9'], 'the search is judged by the charms it places');

// placed and saved: the next three come in on top of them, which stay where they are
gold.status = 'complete'; gold.placements = job.pieces.map((p, i) => ({ id: p.id, cxPt: 10 + 10 * i, cyPt: 10, angle: 0 })); gold.verification = { ok: true }; gold.endedBy = 'complete';
assert.equal(ctx.feedOn(gold), true); assert.equal(starts.at(-1), gold, 'the next few start');
assert.equal(gold.appendOnly, true, 'as an append: nothing placed moves');
gold.status = 'ready'; ctx.startNestReady(gold, {});
job = jobs.at(-1);
assert.deepEqual(job.pieces.map(p => p.id).sort(), ['o4', 'o5', 'o6', 'o7a', 'o7b', 'o8', 'o9']);
assert.deepEqual(job.lockedPlacements.map(p => p.id).sort(), ['o7a', 'o7b', 'o8', 'o9'], 'the charms placed before are held where they are');
assert.equal(gold.feedWait.length, 4);

// full: the orders still waiting move on to the next sheet together
gold.status = 'complete'; gold.placements = job.pieces.map((p, i) => ({ id: p.id, cxPt: 10 + 10 * i, cyPt: 10, angle: 0 })); gold.releaseFull = true;
const before = starts.length;
assert.equal(ctx.feedOn(gold), true); assert.deepEqual(plain(moved.at(-1)).sort(), ['o0', 'o1', 'o2', 'o3'], 'a full sheet passes the waiting orders on');
assert.equal(starts.length, before, 'and takes no more itself'); assert.equal(gold.feedWait, null);
gold.releaseFull = false;

// a few orders, Rose Gold, a person's own sheet, the learned search: all at once, as before
const few = { ...gold, charms: ['o0', 'o1', 'o2'].map((o, i) => charm(o, o, i + 1)), placements: [], appendOnly: false, status: 'ready' };
ctx.startNestReady(few, {}); assert.equal(jobs.at(-1).pieces.length, 3); assert.equal(few.feedWait, null, 'three orders or fewer go on together');
for (const other of [{ metal: 'rose' }, { runId: null }]) {
  const sh = { ...gold, ...other, charms: every.slice(), placements: [], appendOnly: false, status: 'ready', feedWait: null };
  ctx.startNestReady(sh, {}); assert.equal(jobs.at(-1).pieces.length, 11, JSON.stringify(other) + ' places its charms together'); assert.equal(sh.feedWait, null);
}
// stopped by hand, or in trouble: the waiting orders stay for its next Nest; the run's Stop: they wait for Resume
for (const [state, going] of [[{ endedBy: 'stopped' }, false], [{ problem: 'not saved' }, false], [{ endedBy: 'stopped', resumeWait: true }, true]]) {
  const sh = { ...gold, charms: every.slice(), placements: [{ id: 'o9', cxPt: 1, cyPt: 1, angle: 0 }], feedWait: ['o0'], status: 'partial', ...state };
  ctx.allSheets = () => [sh]; const n = starts.length; assert.equal(ctx.feedOn(sh), going, JSON.stringify(state)); assert.equal(starts.length, n + (going ? 1 : 0));
}

/* ── 2 · an overflow joins the next sheet in line, even while it is placing ── */
{
  const pages = [], c = { S: { sheets: {} }, Set, Map, agent() {}, labelOf: () => 'Gold', orderSummary: () => ({ text: 'one order' }), toast() {}, renderRail() {}, updateTopSub() {}, renderCard() {}, computeSaturation() {},
    window: { LiveNest: { closed: () => false } }, Sets: null, starts: [] };
  c.addPage = () => { const p = { metal: 'gold', page: pages.length + 1, charms: [], placements: [], status: 'idle' }; pages.push(p); return p; };
  c.startNest = p => c.starts.push(p);
  vm.createContext(c);
  vm.runInContext(slice('function manualSheetClosed(', '/** New artwork waits outside the live job.'), c);
  vm.runInContext(slice('function overflowToNextSheet(', 'function inflatedArea('), c);
  const first = c.addPage(), second = c.addPage(); c.S.sheets.gold = { pages };
  first.charms = [{ id: 'kept' }, { id: 'x', order: 'late' }]; first.placements = [{ id: 'kept' }]; first.rejects = ['x']; first.verification = { ok: true };
  second.status = 'nesting'; second.charms = [{ id: 'y' }]; second.feedWait = null;
  c.overflowToNextSheet(first);
  assert.equal(pages.length, 2, 'no new sheet is opened while the next one is busy');
  assert.deepEqual(second.charms.map(x => x.id), ['y', 'x'], 'the order joins the next sheet in line');
  assert.deepEqual(plain(second.feedWait), ['x'], 'and goes on after the few it is placing'); assert.equal(c.starts.length, 0); assert.equal(second.dirty, true, 'marked, so nothing finishes past it');
  second.status = 'complete'; second.persisted = Promise.resolve(); second.persistedDone = true;
  first.charms.push({ id: 'z', order: 'later' }); first.rejects = ['z'];
  c.overflowToNextSheet(first); assert.equal(c.starts.at(-1), second, 'a sheet at rest starts at once, as before');
}

/* ── 3 · charms drawn as they land ── */
{
  const drawn = [], fills = [], strokes = [];
  const canvas = new Proxy({}, { get: (t, k) => k in t ? t[k] : () => {}, set: (t, k, v) => { if (k === 'fillStyle') fills.push(v); if (k === 'strokeStyle') strokes.push(v); t[k] = v; return true; } });
  const cv = { width: 400, height: 200, clientWidth: 400, getContext: () => canvas };
  const d = { S: { settings: { insetPt: 1 } }, Set, Map, Math, window: {}, stockFor: () => ({ wPt: 200, hPt: 100 }), activeCharms: sh => sh.charms, getComputedStyle: () => ({ getPropertyValue: () => 'serif' }),
    cutLinesOf: () => [], document: { body: {} }, CharmNestPDF: { pathToCanvas() {}, drawCharm: (x, c) => drawn.push(c.id) } };
  vm.createContext(d);
  vm.runInContext(slice('function paintPreview(cv, sh, clean, R) {', '/** What a sheet holds while the careful fill'), d);
  const c1 = { id: 'a', centerPt: [0, 0], outline: [] }, c2 = { id: 'b', centerPt: [0, 0], outline: [] }, c3 = { id: 'c', centerPt: [0, 0], outline: [] };
  const sh = { metal: 'gold', status: 'nesting', charms: [c1, c2, c3], placements: [], probePlaced: [{ id: 'a', cxPt: 10, cyPt: 10, angle: 0 }, { id: 'b', cxPt: 30, cyPt: 10, angle: 90 }], probe: { id: 'c', cxPt: 50, cyPt: 10, angle: 14 } };
  d.paintPreview(cv, sh, false, 0);
  assert.deepEqual(drawn, ['a', 'b', 'c'], 'each charm that landed, and the one being tried, is drawn with its artwork');
  assert(!fills.some(f => /rgba\((95,150,90|70,120,200)/.test(f)), 'no green or blue masks');
  assert.equal(strokes.filter(s => /40,90,180/.test(s)).length, 1, 'the one being tried has a thin blue line round it');
}

console.log('Feed few orders OK: a big batch goes on three orders at a time, oldest first and orders whole; each few is placed on top of the last, a full sheet passes the rest to the next sheet in line (which takes them after its own, no new sheet); stops and problems keep them waiting; charms that land and the one being tried are drawn in their own colours');
