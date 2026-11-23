// PLACEFIRST + FITKEEP (Paul, 23 Nov 2026, order 4242795385 piece 3: "Not approved: the back words were not placed: nothing moved for 4 minutes", and "other employees are getting the same problem in different orders").
// A face "to check" whose words had NO placement on the page matched no step of the Approve's wait: the press waited out 4 minutes of silence and failed. And placing words took the old placement away first: a change of the
// piece meanwhile (a record from another computer) ended it with neither. The REAL code runs here (its text is taken from the sources): commitStageNow, fitJob / fitJobOnce (charm-nest-bridge.js), charm-nest-engrave-sync.js.
//   A  Approve on an unplaced piece places it first, then approves; one that cannot be placed says why at once; a placing under way is waited for; one that hangs ends after the silence
//   B  a placing cut short keeps the placement it had; never when the words, the font, the art, the charm or the cloud's place changed; a piece asked for again meanwhile is placed for what it is now
//   C  the sync: placeNow places at the cloud's place and says why it cannot; a record that could not be placed keeps the same placement, never another place
//   PLACE_ROOT=<another checkout> node tests/charm-nest/engrave-place-first.cjs      Offline: no browser, no network.
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const root = process.env.PLACE_ROOT ? path.resolve(process.env.PLACE_ROOT) : path.join(__dirname, '../..');
const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), syncFile = path.join(root, 'charm-nest-engrave-sync.js');
let bad = 0, n = 0; const ok = (c, m) => { n++; if (!c) bad++; console.log((c ? '  ✓ ' : '  ✗ ') + m); };
const cutText = (from, to) => { const a = src.indexOf(from); if (a < 0) throw new Error('not found: ' + from); const b = src.indexOf(to, a); return src.slice(a, b); };
const wait = ms => new Promise(r => setTimeout(r, ms));
const SCALE = 0.006;   // (4 minutes of silence = 1.44 s here)

/* ── A · the commit ── */
const commitText = cutText('  async function commitStageNow(', '  /** ENGSTAGE (coordinator 30 Oct 2026: "when a commit ends');
function commitOf(job, placeNow) {
  const calls = { place: 0, approve: 0, steps: [] }, fitTasks = new WeakMap(), lateTasks = new WeakMap();
  const env = { window: { CN_SPIN_SCALE: SCALE, CharmNestEngraveSetup: {}, CharmNestEngraveSync: placeNow ? { placeNow: j => { calls.place++; return placeNow(j, calls); } } : {} }, lineOf: r => r, jobsOf: () => [job], faceOf: () => 'back', employeeName: () => 'T',
    STAGE_MS: 240000, minutesOf: ms => Math.round(ms) + 'ms', OPENREAD: ['classify', 'reclassify'], isWorking: j => fitTasks.has(j), lateTasks, fitTasks, canFit: () => true, readsFree: () => false, preparingNow: () => false, prepareLate: async () => [],
    setupBusy: new Set(), saveSetup: async () => ({ ok: true }), engravedWords: () => ['x'], faceHasEditorArt: () => false, artOnlyPlacement: () => false, decideWords: async () => {}, viewPointOf: () => null, moveTo: () => false, applyLook: () => {}, lookOf: () => null,
    toast: () => {}, APPROVE_WHY: new Map(), APPROVE_MS: {}, approveWithin: p => p, needEmployee: async () => 'T', EG: {}, fitJob: async j => { j.fit = { centre: [0, 0] }; j.state = 'review'; return j; },
    approve: async j => { calls.approve++; if (!j.fit) return; j.state = 'approved'; j.approvedAt = 1; } };
  const k = Object.keys(env), f = new Function(...k, commitText + '\nreturn commitStageNow;')(...k.map(x => env[x]));
  return { calls, run: async () => { const t = Date.now(), r = await f({ key: 'L', state: 'pooled' }, { choice: 'back', slot: null, faces: { back: { text: 'A+E+T' } } }, { step: (c, w) => calls.steps.push(w || c) }, () => {}); return Object.assign({ ms: Date.now() - t }, r); } };
}
const faceJob = o => Object.assign({ key: 'L#C3', slot: null, state: 'review', fit: null, lines: ['A+E+T'], lineInput: ['A+E+T'], text: 'A+E+T' }, o || {});
async function partA() {
  { const j = faceJob({ fit: { centre: [1, 1] } }), c = commitOf(j, async () => ({ ok: true })), r = await c.run(); ok(r.ok && c.calls.place === 0 && c.calls.approve === 1, 'A1 a placed piece is approved as before, not placed again'); }
  { const j = faceJob(), c = commitOf(j, async x => { await wait(50); x.fit = { centre: [2, 2] }; return { ok: true }; }), r = await c.run();
    ok(r.ok && c.calls.place === 1 && j.state === 'approved', `A2 an unplaced piece is placed first, then approved (${r.ok ? 'approved' : r.message})`); ok(c.calls.steps.some(s => /Placing the back words/.test(s)), 'A2 and the card says it is placing the words'); }
  { const j = faceJob(), c = commitOf(j, async () => ({ ok: false, why: "the charm's drawing could not be read on this computer" })), r = await c.run();
    ok(!r.ok && /drawing could not be read/.test(r.message) && c.calls.approve === 0, 'A3 one that cannot be placed says why: ' + r.message); ok(r.ms < 1000, `A3 at once, not after the silence (${r.ms} ms of 1440)`); ok(c.calls.place === 2, 'A3 after two tries'); }
  { const j = faceJob(); let t0 = 0; const c = commitOf(j, async x => { t0 = t0 || Date.now(); if (Date.now() - t0 > 500) { x.fit = { centre: [3, 3] }; return { ok: true }; } return { ok: false, busy: true }; }), r = await c.run(); ok(r.ok && c.calls.place > 2, `A4 a piece being placed already (the open card) is waited for, then approved (${r.ok ? 'approved' : r.message})`); }
  { const j = faceJob(), c = commitOf(j, () => new Promise(() => {})), r = await c.run(); ok(!r.ok && /not placed: nothing moved/.test(r.message) && r.ms < 4000, 'A5 a placing that never answers ends after the silence, with the reason: ' + r.message); }
  { const j = faceJob(), c = commitOf(j, null), r = await c.run(); ok(r.ok && j.state === 'approved', 'A6 a page with no engraving sync places it itself'); }
}

/* ── B · the placing ── */
const taskText = cutText('  const fitTasks = new WeakMap()', '  /* EDITOR WORDS (27 Oct 2026)'), fitText = cutText('  function fitJob(job) {', '  /** { poolId: art-edit stamp') + cutText('  async function fitJobOnce(job) {', '  /* FOLLOWASK (Paul, 15 Nov 2026)');
async function place(o) {
  const row = { state: 'pooled', order: { receiptId: '1' }, spec: { designSku: 'X' }, engrave: {} }, old = { centre: [1, 1], size: 6, angle: 0, lines: ['A+E+T'] };
  const job = { key: 'L#C3', row, state: 'ready', copies: ['p'], lines: ['A+E+T'], lineInput: ['A+E+T'], text: 'A+E+T', fontKey: 'f', fit: old, view: { v: 1 }, mask: { m: 1 }, verify: { geometry: { ok: true } } };
  const m = new Map([[job.key, job]]), charm = { outline: { a: 1 }, members: 1 }, w = { art: 1, charm, runs: 0 };
  const env = { items: () => m, loadFonts: async () => {}, EG: {}, render: () => {}, faceHasEditorArt: () => false, engravedWords: () => ['x'], artOnlyPlacement: () => false, holdDrawing: () => {}, F_: { ok: true }, setReady: j => j, canFit: () => true, ensureJobFont: async () => { if (o.font) o.font(job, w); },
    G: { glyphCoverage: () => ({ ok: true }), ORIENT: 1 }, fontFor: () => ({}), charmFor: () => w.charm, Master: { entryFor: () => ({}) }, fitInput: () => ({ charm: w.charm, viewOptions: {} }), fitStamp: () => 's',
    fitClient: () => ({ run: async () => { w.runs++; await wait(5); if (o.worker && w.runs === 1) o.worker(job, w); if (o.fail) throw new Error('worker stopped'); return { view: {}, mask: {}, lines: ['A+E+T'], check: {}, fit: { size: 7, capMm: 1, weight: 'R', centre: [9, 9], lines: ['A+E+T'] } }; } }),
    Review: { add() {}, remove() {} }, agent: () => {}, KEPT: new Map(), artStampsOf: () => ({ p: w.art }), applyAsks: () => {} };
  const k = Object.keys(env), f = new Function(...k, taskText + fitText + '\nreturn fitJob;')(...k.map(x => env[x]));
  await f(job); return { job, old, w };
}
async function partB() {
  const review = j => { j.state = 'review'; };
  { const r = await place({}); ok(r.job.fit && r.job.fit.centre[0] === 9 && r.job.state === 'review', 'B1 undisturbed, the new placement is taken'); }
  { const r = await place({ font: review }); ok(r.job.fit === r.old && r.job.verify && r.job.view, 'B2 cut short while the font was fetched: the placement it had stays'); }
  { const r = await place({ worker: review }); ok(r.job.fit === r.old && !!r.job.mask, 'B3 cut short while the words were placed: the placement it had stays'); }
  { const r = await place({ fail: true, worker: review }); ok(r.job.fit === r.old, 'B4 and when the worker fails after the change'); }
  { const r = await place({ worker: j => { j.state = 'review'; j.text = 'Ana'; j.lineInput = ['Ana']; } }); ok(!r.job.fit, 'B5 the words changed: the old placement is not put back'); }
  { const r = await place({ worker: j => { j.state = 'review'; j.fontKey = 'g'; } }); ok(!r.job.fit, 'B6 the font changed: not put back'); }
  { const r = await place({ worker: (j, w) => { j.state = 'review'; w.art = 2; } }); ok(!r.job.fit, 'B7 the art was edited: not put back'); }
  { const r = await place({ worker: (j, w) => { j.state = 'review'; w.charm = { outline: { b: 2 }, members: 1 }; } }); ok(!r.job.fit, 'B8 the charm\'s drawing changed: not put back'); }
  { const r = await place({ worker: j => { j.state = 'review'; j.keepPlace = { centre: [5, 5], size: 6, angle: 0 }; } }); ok(!r.job.fit, 'B9 the cloud names another place: the old one is not put back'); }
  { const r = await place({ worker: j => { j.state = 'none'; } }); ok(!r.job.fit && r.job.state === 'none', 'B10 set to no engraving meanwhile: stays without a placement'); }
  { const r = await place({ worker: j => { j.state = 'ready'; j.fit = null; } }); ok(r.job.fit && r.job.fit.centre[0] === 9 && r.w.runs === 2, 'B11 asked for again while being placed (a second record): placed for what it is now, not left with nothing'); }
}

/* ── C · the sync ── */
async function sync(o) {
  const KEY = '4242795385_5305127482', P = { centre: [16.97, 17.12], angle: 0, size: 6.565, lines: ['A+E+T'], weight: 'Semibold', lineGap: 0.216, fittedMax: 8.33, upAngle: 90, cx: 1, cy: 2, fontKey: 'source-sans-3' };
  const line = { key: KEY, state: 'pooled', order: { receiptId: '4242795385' }, spec: { engraveCandidate: true }, poolIds: [KEY + '_3'], engrave: { needed: true, state: 'review' } };
  const job = { key: KEY + '#C3', slot: 'C3', text: 'A+E+T', lines: ['A+E+T'], lineInput: ['A+E+T'], fontKey: 'source-sans-3', state: 'review', copies: o.noPiece ? [] : [KEY + '_3'], row: { parentRow: line }, fit: o.unplaced ? null : { centre: P.centre.slice(), size: P.size, angle: 0, lines: ['A+E+T'] }, view: { v: 1 }, mask: { m: 1 }, verify: { geometry: { ok: true } } };
  const rec = { v: 1, override: null, engrave: { needed: true, state: 'review' }, jobs: [{ key: job.key, slot: 'C3', text: 'A+E+T', lines: ['A+E+T'], lineInput: ['A+E+T'], fontKey: o.font || 'source-sans-3', state: 'review', place: Object.assign({}, P, o.place || {}) }] };
  const sb = { console: { log() {}, warn() {} }, JSON, Math, Promise, Map, Set, WeakMap, WeakSet, Object, Array, String, Number, Error, Date, setTimeout, clearTimeout, setInterval: () => 0, clearInterval }; sb.self = sb; let fits = 0;
  sb.Engrave = { items: () => new Map([[job.key, job]]), jobsOf: () => [job], ensureJobs() {}, render() {}, fitJob: async j => { fits++; if (o.cannot) { j.reason = o.cannot; return j; } const k = j.keepPlace || P; j.fit = { centre: k.centre.slice(), size: k.size, angle: 0, lines: ['A+E+T'] }; return j; } };
  sb.Orders = { rows: () => [line], render() {} }; sb.Session = { schedule() {} }; sb.Pool = { charmOf: () => (o.noCharm ? null : { outline: {} }), previewCharmOf: () => null, preview: async () => {}, previewIds: () => [] };
  sb.CharmNestMadeThumb = { state: () => ({ dev: 'd1' }), changed() {} }; sb.CN = { api: async () => ({ lines: [{ key: KEY, order: '4242795385', rec: JSON.parse(JSON.stringify(rec)), at: Date.now(), rev: 5, by: 'x', dev: 'd2', sig: null }] }) };
  vm.createContext(sb); vm.runInContext(fs.readFileSync(syncFile, 'utf8'), sb);
  const l = { key: KEY, order: '4242795385', rec, at: Date.now(), rev: 5, by: 'x', dev: 'd2', sig: null };
  return { ES: sb.CharmNestEngraveSync, job, l, P, fits: () => fits };
}
async function partC() {
  const pn = async o => { const s = await sync(o); const r = s.ES.placeNow ? await s.ES.placeNow(s.job) : { ok: false, why: 'no placeNow' }; return Object.assign(s, { r }); };
  { const s = await pn({ unplaced: true }); ok(s.r.ok && s.job.fit && Math.abs(s.job.fit.centre[0] - 16.97) < 1e-6 && s.job.state === 'review', 'C1 placeNow places the words at their place; the piece stays to check'); }
  { const s = await pn({}); ok(s.r.ok && s.fits() === 0, 'C2 a placed piece is left as it is'); }
  { const s = await pn({ unplaced: true, noCharm: true }); ok(!s.r.ok && /drawing could not be read/.test(s.r.why), 'C3 no drawing: ' + s.r.why); }
  { const s = await pn({ unplaced: true, noPiece: true }); ok(!s.r.ok && /no piece/.test(s.r.why), 'C4 no piece: ' + s.r.why); }
  { const s = await pn({ unplaced: true, cannot: 'the words do not fit' }); ok(!s.r.ok && s.r.why === 'the words do not fit', 'C5 no room: ' + s.r.why); }
  { const s = await sync({ cannot: 'worker stopped' }); await s.ES._take(s.l, 'feed'); ok(s.job.fit && s.job.state === 'review' && !!s.job.verify, 'C6 a record whose words could not be placed again keeps the same placement'); }
  { const s = await sync({ cannot: 'worker stopped', place: { centre: [3, 3] } }); await s.ES._take(s.l, 'feed'); ok(!s.job.fit, 'C7 a record with another place never keeps the old one'); }
  { const s = await sync({ cannot: 'worker stopped', font: 'script-9' }); await s.ES._take(s.l, 'feed'); ok(!s.job.fit, 'C8 nor one with another font'); }
  { const s = await sync({ place: { centre: [3, 3] } }); await s.ES._take(s.l, 'feed'); ok(s.job.fit && s.job.fit.centre[0] === 3, 'C9 a record that is placed moves the words there, as before'); }
}
(async () => { await partA(); await partB(); await partC(); console.log(`engrave-place-first: ${n - bad} of ${n} checks passed`); process.exit(bad ? 1 : 0); })().catch(e => { console.log('HARNESS ERROR', e.stack); process.exit(2); });
