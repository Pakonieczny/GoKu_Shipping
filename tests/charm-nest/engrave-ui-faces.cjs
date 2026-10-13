// ENGUI1 (front-back-1011): the ENGRAVE tab tells a line's FACES. A line that engraves the front AND the back is ONE row (every face of every piece told on it, a "Front + back" chip), its editor has the
// very same Front | Back switch as Left | Right and the discs (charm-nest-piece-switch.js, under the piece switch when the line has several pieces), Back / Next walk piece by piece, "Approve both faces" /
// "Approve both ears" / "Approve all discs" approve every face (Front before Back) only when every placement is ready and was shown, the piece chip counts PIECES not jobs ("DISC 2 of 3", never "of 6"),
// the Decided tab tells both faces' seals with a Reopen for each face, and a saved FRONT is opened for editing the way a saved back is (openBack with a face).
//
// What it proves, offline (jsdom against the real Engrave module with contract-shaped fixtures; no network, no paid call, nothing live, nothing written anywhere but memory):
//   1  the real pieces the page leans on (rows, piece switch, faces): a faced line is one row, Front before Back, piece by piece; the switch draws Front | Back under Left | Right / Disc 1 | 2 | 3
//   2  the real Engrave module drawn in a fake page, with order 4195966039 (front W / back 1. 27. 24, Ace), an earring pair with a front and a back on each ear, a 3-disc necklace with both faces,
//      and three back-only lines beside them: lists, tab badges, the editor and its switches, the counter, Back / Next, the setup editor's slot, Decided and its seals, skipping a face
//   3  approval: "Approve both faces / ears / all discs" runs the ORDINARY approval of each face (Front first), only when every placement is ready and was shown; a failed face stops the rest
//   4  openBack(poolId, sheetId, "front"): a saved front is opened the way a saved back is, on its own record, and an edit of it never touches the piece's back
//   5  INERT for back-only lines: what a pendant, an earring pair and a necklace of discs draw (list row, editor, Decided row) is byte for byte what the page drew before faces (SHA-1 of the markup,
//      captured from the unchanged page), and the changed files carry a new cache token
//   node tests/charm-nest/engrave-ui-faces.cjs
'use strict';
const path = require('path'), fs = require('fs'), vm = require('vm'), assert = require('assert/strict'), crypto = require('crypto');
const root = path.join(__dirname, '../..');
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const Sides = require(path.join(root, 'charm-nest-engrave-sides.js'));
const Rows = require(path.join(root, 'charm-nest-engrave-rows.js'));
const Seals = require(path.join(root, 'charm-nest-engraving-seals.js'));
const Activity = require(path.join(root, 'charm-nest-activity.js'));
const Switch = require(path.join(root, 'charm-nest-piece-switch.js'));
const Faces = require(path.join(root, 'charm-nest-engrave-faces.js'));
const F = require('./pairs-fixtures.cjs');
let JSDOM = null; try { ({ JSDOM } = require('jsdom')); } catch (_) { /* the page checks are skipped */ }

let failed = 0, passed = 0;
const t = async (name, fn) => { try { await fn(); passed++; console.log('  ok  ' + name); } catch (e) { failed++; console.log('  FAIL ' + name + '\n      ' + String(e && e.stack || e).split('\n').slice(0, 7).join('\n      ')); } };
const sha1 = s => crypto.createHash('sha1').update(String(s)).digest('hex').slice(0, 12);
const norm = s => String(s).replace(/\s+/g, ' ').trim();

/* ── a world: order 4195966039 (a tag: front W / back 1. 27. 24, font Ace), an earring pair and a 3-disc necklace that engrave both faces, and a pendant, an earring pair and a necklace that engrave the back only ── */
const RID_TAG = '4195966039';
const W = F.world({ orders: [
  { rid: RID_TAG, lines: [{ n: 10, kind: 'single', on: 'sh-gf1' }] },
  { rid: F.rid(2), lines: [{ n: 20, kind: 'pair', on: 'sh-gf1' }] },
  { rid: F.rid(3), lines: [{ n: 30, kind: 'discs', discs: 3, on: 'sh-gf1' }] },
  { rid: F.rid(4), lines: [{ n: 40, kind: 'single', on: 'sh-gf1' }] },
  { rid: F.rid(5), lines: [{ n: 50, kind: 'pair', on: 'sh-gf1' }] },
  { rid: F.rid(6), lines: [{ n: 60, kind: 'discs', discs: 3, on: 'sh-gf1' }] },
] });
/** spec.engraving as the intake writes it (CONTRACT section 2) */
const readingBoth = (front, back, why) => ({ v: 1, mode: 'both', source: 'option', front: { text: front, lines: [front], fontWord: null }, back: { text: back, lines: [back], fontWord: null }, textSource: 'labelled', needsPerson: false, confidence: 0.95, why, option: { name: 'Charm Type', value: 'Tag1 (front+back)' } });
const WHY_TAG = 'Charm Type “Tag1 (front+back)” asks for both faces; the note is labelled front - W / back - 1. 27. 24';
const lineRow = (w, ord, n, extra) => {
  const o = w.orders.find(x => x.rid === (typeof ord === 'string' ? ord : F.rid(ord))), l = o.lines.find(x => x.n === n);
  return Object.assign({ key: F.lineKey(o.rid, F.tx(n)), poolIds: l.pieces.map(p => p.poolId), order: { receiptId: o.rid }, line: { transactionId: F.tx(n), sku: l.sku, title: l.line.title, quantity: l.qty, variations: [{ name: 'Metal choice', value: 'Gold' }] },
    spec: { designSku: l.sku, form: l.form, quantity: l.qty, personalization: ['Mom'], engraveCandidate: true }, state: 'written', material: 'gold', problems: [] }, extra || {});
};
const withReading = (row, reading) => { row.spec = Object.assign({}, row.spec, { engraving: reading }); return row; };
const mkRows = () => ({
  tag: withReading(lineRow(W, RID_TAG, 10), readingBoth('W', '1. 27. 24', WHY_TAG)),
  ears: withReading(lineRow(W, 2, 20), readingBoth('M', 'Mom', 'Charm Type “Studs (front+back)” asks for both faces')),
  discs: withReading(lineRow(W, 3, 30), readingBoth('A', 'Mom', 'Charm Type “3 discs (front+back)” asks for both faces')),
  pend: lineRow(W, 4, 40), pair: lineRow(W, 5, 50), neck: lineRow(W, 6, 60),
});
const poolOf = w => new Map(w.pool.map(p => [p.poolId, p]));
const CTX = (() => { const rows = poolOf(W); return { poolRow: id => rows.get(id), charmOf: id => { const r = rows.get(id); return r ? F.charmOf(r.sku) : null; }, entryFor: sku => F.entryOf(sku), pair: Pair, mergeSeals: (...r) => Seals.merge(...r) }; })();
const readyFit = n => ({ fit: { ok: true, size: 2, capMm: 1.8, weight: 'Regular', angle: 0, centre: [0, (n || 0) / 1000], fittedMax: 3, glyphs: [] }, view: {}, verify: { geometry: { ok: true } } });   // (n: a new placement, so it counts as not yet shown)

/* the bridge as it stands, and the two small lines of it a front job needs before the job engine (ENGMAP) is on the page: where a job is looked up (a front job is kept under `<line key>[#slot]~front`).
   When the page already reads front jobs itself these are the very lines it has and nothing is replaced. */
const SOURCE = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const SLOTKEY_OLD = 'const slotKey = (row, slot) => (slot ? `${row.key}#${slot}` : row.key);';
const JOBSOF_OLD = 'function jobsOf(row) { row = lineOf(row); if (!row) return []; const out = []; for (const s of SLOT_PROBE) { const j = items().get(slotKey(row, s)); if (j) out.push(j); } return out; }';
const SLOTKEY_NEW = 'const slotKey = (row, slot, face) => (face === "front" ? window.CharmNestEngraveFaces.jobKey(row.key, slot, "front") : slot ? `${row.key}#${slot}` : row.key);';
const JOBSOF_NEW = 'function jobsOf(row) { row = lineOf(row); if (!row) return []; const out = []; for (const s of SLOT_PROBE) { const f = items().get(slotKey(row, s, "front")); if (f) out.push(f); const j = items().get(slotKey(row, s)); if (j) out.push(j); } return out; }';
/** One-line `  const NAME = ...` definitions of the page's job engine (face of a job, record keys, piece counts) that sit outside the pieces of code the slice tests cut out: run first, when the page has them */
const outerDefs = (names, within) => names.map(n => { const m = new RegExp('^  const ' + n + ' = .*$', 'm').exec(SOURCE); return m && !(within || '').includes(m[0]) ? m[0] : ''; }).filter(Boolean).join('\n');
const OUTER = ['FACES', 'faceOf', 'isFront', 'faceWord', 'fileViewOf', 'recKeyFor', 'pieceCountOf', 'neverEngraves', 'NEVER_OPEN', 'NEVER_WHY'];   // (the last three: ZODNOBACK's "no earring is ever engraved" door of approve, the page's own lines)
const jobEngineStandIn = src => (src.includes(SLOTKEY_OLD) && src.includes(JOBSOF_OLD) ? src.replace(SLOTKEY_OLD, SLOTKEY_NEW).replace(JOBSOF_OLD, JOBSOF_NEW) : src);

/* the seals module with a plain stand-in for the seal drawing (the real picture needs the page's Seal widget): one <i class="seal"> for each seal the REAL module lists for a job, with its face */
const SealsView = Object.assign({}, Seals, { html: job => { const l = Seals.list(job); return l.length ? '<span class="sealRow engravingSeals">' + l.map(x => `<i class="seal" data-how="${x.how}" data-face="${x.face === 'front' ? 'front' : 'back'}" data-by="${x.by}"></i>`).join('') + '</span>' : ''; } });

/** The Engrave module of `src`, drawn in a fake page with the worlds above. o.faces: build the front jobs; o.setup: a CharmNestEngraveSetup stand-in. */
function makePage(src, o) {
  o = o || {};
  const dom = new JSDOM('<body><div id="engraveView"></div></body>', { pretendToBeVisual: false });
  const document = dom.window.document, jobs = new Map(), frames = [], timers = [], errors = [], toasts = [], pairCalls = [], saves = [], skips = [], setupMounts = [];
  dom.window.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
  dom.window.HTMLCanvasElement.prototype.getContext = () => ({ fillRect() {} });
  Object.assign(dom.window, { CharmNestEngraveRows: Rows, CharmNestPieceSwitch: Switch, CharmNestEngraveSides: Sides, CharmNestPair: Pair, CNListActivity: Activity, CNEngravingSeals: SealsView });
  if (o.faces) dom.window.CharmNestEngraveFaces = Faces;
  if (o.setupReal) dom.window.CharmNestEngraveSetup = require(path.join(root, 'charm-nest-engrave-setup.js'));
  else if (o.setup) dom.window.CharmNestEngraveSetup = { mount(slot, row) { setupMounts.push([slot, row]); slot.hidden = false; slot.textContent = 'Engraves: ' + (row && row.spec && row.spec.engraving ? row.spec.engraving.mode : 'back'); } };
  const pool = poolOf(W), ROW = mkRows();
  if (o.noReading) for (const k of Object.keys(ROW)) delete ROW[k].spec.engraving;   // (lines the intake has not read for faces: the job engine makes no front job)
  const bare = o.setup || o.setupReal ? { CharmNestEngraveSetup: dom.window.CharmNestEngraveSetup } : {};   // (the page's scripts see every window.X as a global X)
  const ListMedia = { pair: row => { pairCalls.push(row); return '<div class="comparePair"><figure><span data-vector></span></figure><figure><span data-listing></span></figure></div>'; }, mount() {}, more() {}, pairRow: () => false };
  const c = vm.createContext({ WORKSPACE_SANDBOX: true, localStorage: { getItem() { return null; }, setItem() {} }, CNListActivity: Activity, CNEngravingSeals: SealsView, confirm: () => true, window: dom.window, document, console: { error: (...x) => errors.push(x), warn() {}, log() {} },
    ResizeObserver: dom.window.ResizeObserver, B: { engrave: { items: jobs, fonts: { ok: true } }, pool: { rows: pool }, orders: { byKey: new Map() } },
    O: require(path.join(root, 'charm-nest-orders.js')), S: { settings: {}, mode: 'engrave' }, PT: 72 / 25.4, MM: 25.4 / 72, SOURCE_LABEL: {},
    Pool: { charmOf: () => null, previewCharmOf: () => null, sheetOf: () => null, update: async () => {} }, P: {}, Review: { items: () => [], count: () => 0, remove() {}, add() {} }, Master: { entryFor: sku => F.entryOf(sku), fetchEntry: async () => null },
    LiveStrip: { render() {} }, RunCtl: { poke() {}, stopIfRunning() {} }, api: async () => ({}), Orders: { render() {}, rows: () => [] }, ListMedia,
    setTimeout: fn => (timers.push(fn), timers.length), clearTimeout() {}, requestAnimationFrame: fn => (frames.push(fn), frames.length), cancelAnimationFrame() {},
    el: (tag, cls, html_ = '') => { const e = document.createElement(tag); e.className = cls; e.innerHTML = html_; return e; },
    esc: v => String(v ?? '').replace(/[&<>"']/g, x => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[x])),
    toast: m => toasts.push(String(m)), allSheets: () => [], refreshAllCards() {}, Session: { schedule() {} }, agent() {}, getComputedStyle: () => ({ flexGrow: '0' }), employeeName: () => 'Paul', humanAct() {}, TL: { rec() {}, line() {} }, ...bare });
  const pmStart = src.indexOf('function purchaseMarkup('); vm.runInContext(src.slice(pmStart, src.indexOf('\n}\n', pmStart) + 3), c);
  const BACK_CALL = 'const bc = renderBack(job, px, { grid: true, editable: true, include: framed, draft: panelOn });';
  let moduleSrc = src.slice(src.indexOf('const Engrave = window.Engrave ='), src.indexOf('/* ═══ 22 · Sets'));
  assert.ok(moduleSrc.includes(BACK_CALL), 'the editor draws its back here (the test stands a plain canvas in for the drawing machinery)');
  moduleSrc = moduleSrc.replace(BACK_CALL, 'const bc = standInBack();');
  if (o.faces) moduleSrc = jobEngineStandIn(moduleSrc);
  c.standInBack = () => { const cv = document.createElement('canvas'); cv._map = { bb: [0, 0, 1, 1], k: 1 }; cv._paint = () => {}; return cv; };
  vm.runInContext(moduleSrc, c);
  const E = dom.window.Engrave;
  const runFrames = () => { for (let n = 0; n < 20 && frames.length; n++) frames.splice(0).forEach(f => f()); };
  const made = {}, fronts = {};
  for (const k of Object.keys(ROW)) {
    const js = E.ensureJobs(ROW[k]);   // (the page's own door: the back jobs, and, when the page's job engine reads spec.engraving, the front jobs too)
    made[k] = js.filter(j => j.face !== 'front');
    const fr = js.filter(j => j.face === 'front'); if (fr.length) fronts[k] = made[k].map(b => fr.find(f => (f.slot || null) === (b.slot || null)));
  }
  const sidesFor = () => ({ poolRow: id => pool.get(id), charmOf: id => { const r = pool.get(id); return r ? F.charmOf(r.sku) : null; }, entryFor: sku => F.entryOf(sku), pair: Pair, mergeSeals: (...r) => Seals.merge(...r) });
  /** a FRONT job beside a back job, the way the job engine makes one (CONTRACT section 5): key <line>[#slot]~front, its own side row, the same pieces */
  const addFront = (k, i) => {
    const back = made[k][i], line = ROW[k];
    const fj = { key: Faces.jobKey(line.key, back.slot, 'front'), face: 'front', rowKey: back.slot ? line.key : undefined, slot: back.slot, groupKey: back.groupKey, lineGap: .216, state: 'review', text: '', lines: [], source: 'spec.engraving', quote: null, confidence: 1, requests: null, questions: [], decision: null, fit: null, view: null, mask: null, backs: [], copies: back.copies.slice(), reason: null, t: Date.now() };
    if (!back.slot) { delete fj.slot; delete fj.rowKey; }
    fj.row = Sides.sideRow(sidesFor(), line, back.slot || null, fj);
    jobs.set(fj.key, fj); (fronts[k] = fronts[k] || [])[i] = fj;
    if (!back.slot && !back.linked) { const own = line.engrave; Object.assign(back, { rowKey: line.key, groupKey: back.groupKey || fj.groupKey, linked: true }); back.row = Sides.sideRow(sidesFor(), line, null, back); if (own && typeof own === 'object') back.engraveRec = Object.assign({}, own); }   // (the back of a charm with a front gets its own row too: each face keeps its own record)
    return fj;
  };
  const jobsFor = k => { const all = []; for (let i = 0; i < made[k].length; i++) { if (fronts[k] && fronts[k][i]) all.push(fronts[k][i]); all.push(made[k][i]); } return all; };
  const relink = k => Sides.linkParent(sidesFor(), ROW[k], jobsFor(k));
  const P_ = { bump: 0 };
  const setup = (js, state, words) => js.forEach((j, i) => { j.state = state; j.lines = Array.isArray(words[0]) ? (words[i] || words[0]).slice() : words.slice(); j.text = j.lines.join('\n'); if (state === 'review') Object.assign(j, readyFit(P_.bump)); });
  const resetRecs = () => { for (const j of jobs.values()) { j.row.engrave = Object.assign({ needed: true, approved: false }, { state: j.state }); delete j.approvedAt; delete j.approvedBy; delete j.engravingSeals; delete j.decidedAt; delete j.decidedBy; j.backs = []; } };
  return { P_, dom, document, jobs, frames, errors, toasts, pairCalls, saves, skips, setupMounts, ROW, made, fronts, addFront, jobsFor, relink, setup, resetRecs, E, runFrames, c,
    rowsOnPage: () => [...document.querySelectorAll('#egQueue .placementRow, .egPlacementList .placementRow')],
    showList: tab => { E.restoreView({ tab: tab || 'place', chosen: true, list: true, focus: null, q: '' }); E.render(); },
    open: k => { E.restoreView({ tab: 'place', chosen: true, list: false, focus: k }); E.render(); runFrames(); return document.querySelector('#egQueue .rvItem'); },
    card: () => document.querySelector('#egQueue .rvItem') };
}

/** a page with the faced lines made and set up: the tag (Front W / Back 1. 27. 24), the earrings (Front M / Back Mom on each ear), the necklace (front A, B, C / back 1, 2, 3 on its discs) */
function facedPage(o) {
  const P = makePage(SOURCE, Object.assign({ faces: true }, o));
  const { made, addFront } = P;
  for (const k of ['tag', 'ears', 'discs']) if (!P.fronts[k]) made[k].forEach((_, i) => addFront(k, i));   // (a page without the job engine gets the front jobs made by hand, in the shape the engine makes them)
  const reset = () => {
    P.P_.bump++;
    P.setup([P.fronts.tag[0]], 'review', ['W']); P.setup(made.tag, 'review', ['1. 27. 24']);
    P.fronts.ears.forEach(j => P.setup([j], 'review', ['M'])); P.setup(made.ears, 'review', ['Mom']);
    P.fronts.discs.forEach((j, i) => P.setup([j], 'review', [['A', 'B', 'C'][i]])); P.setup(made.discs, 'review', [['1'], ['2'], ['3']]);
    P.setup(made.pend, 'review', ['Mom']); P.setup(made.pair, 'review', ['Mom']); P.setup(made.neck, 'review', [['A'], ['B'], ['C']]);
    for (const k of ['tag', 'ears', 'discs']) P.relink(k);
    P.resetRecs(); P.pairCalls.length = 0; P.toasts.length = 0;
  };
  reset();
  return Object.assign(P, { reset });
}


/* the markup the page drew for the BACK-ONLY lines before faces existed: SHA-1 (12 hex) of the normalised markup of each surface, captured from the unchanged page (origin/main at 59c294c) with
   `node tests/charm-nest/engrave-ui-faces.cjs --capture <path to a charm-nest-bridge.js>` (the card without the empty hidden slot the setup editor mounts into; see SETUP_SLOT) */
const SETUP_SLOT = /<section class="pvSec pvSetup"[^>]*>.*?<\/section>/;
const GOLDEN = {
  'row:tag': 'd3bca07bc601',
  'card:tag': '1c1863edbe69',
  'done:tag': '68ba43c384e1',
  'row:ears': '135eef145959',
  'card:ears': 'fb2bc5b1ed7a',
  'done:ears': '3ca5d7fda858',
  'row:discs': 'f3db36e29c63',
  'card:discs': 'd141b0b18a35',
  'done:discs': 'a275636a6759',
  'row:pend': '53ab7d295ac2',
  'card:pend': '0f9af5eae89e',
  'done:pend': '18f8b18c6d27',
  'row:pair': 'd3f2f66d7381',
  'card:pair': '25a4e06306e2',
  'done:pair': '6b3c6691421c',
  'row:neck': '5afec64f788f',
  'card:neck': '0fbca4d582ed',
  'done:neck': 'ef3e62bc02e7'
};

/** Every surface a line draws with its back job only: its row in Placements, its editor card, its row in Decided. */
function surfaces(P, k) {
  const out = {}, line = P.ROW[k], rid = line.order.receiptId, E = P.E;
  const approvedAll = js => { for (const j of js) { j.state = 'approved'; j.approvedBy = 'Paul'; j.approvedAt = 1.7e12; j.backs = []; } };
  const jobs = P.made[k];
  P.reset(); P.showList(); const row = P.rowsOnPage().find(r => r.dataset.rid === rid); out['row:' + k] = row ? sha1(norm(row.outerHTML)) : 'missing';
  const card = P.open(jobs[0].key); out['card:' + k] = card ? sha1(norm(card.outerHTML.replace(SETUP_SLOT, ''))) : 'missing';
  P.reset(); approvedAll(jobs);
  P.relink(k); for (const j of jobs) j.row.engrave = Object.assign({}, j.row.engrave, { state: 'approved', approved: true });
  E.restoreView({ tab: 'done', chosen: true, list: true, focus: null, openDone: null }); E.render();
  const dn = [...P.document.querySelectorAll('#egDone .decidedRow')].find(r => r.dataset.rid === rid); out['done:' + k] = dn ? sha1(norm(dn.outerHTML)) : 'missing';
  P.reset(); return out;
}
const BACK_ONLY = ['pend', 'pair', 'neck'];

(async () => {
  // capture mode: print the goldens of a bridge file (the unchanged page) and stop
  if (process.argv[2] === '--capture') {
    const P = makePage(fs.readFileSync(path.resolve(process.argv[3]), 'utf8'), {});
    P.reset = () => { P.setup(P.made.tag, 'review', ['Mom']); P.setup(P.made.ears, 'review', ['Mom']); P.setup(P.made.discs, 'review', [['A'], ['B'], ['C']]); P.setup(P.made.pend, 'review', ['Mom']); P.setup(P.made.pair, 'review', ['Mom']); P.setup(P.made.neck, 'review', [['A'], ['B'], ['C']]); P.resetRecs(); P.pairCalls.length = 0; };
    const all = {}; for (const k of Object.keys(P.ROW)) Object.assign(all, surfaces(P, k));
    console.log(JSON.stringify(all, null, 1)); process.exit(0);
  }
  console.log('ENGUI1 faces in the Engrave tab');
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), css = fs.readFileSync(path.join(root, 'charm-nest-activity.css'), 'utf8');

  // ═══ 1 · the real pieces the page leans on ═══
  if (!JSDOM) { console.log('  – no jsdom installed: the page checks were not run'); console.log(`\nengrave-ui-faces: ${passed} passed, ${failed} failed`); process.exit(failed ? 1 : 0); }
  const P = facedPage({ setup: true });
  const { document, made, fronts, E, ROW } = P;
  const keysOf = js => js.map(j => j.key);
  await t('1a a line with a front is ONE row with every job of it, piece by piece, Front before Back; a back-only line is the row it was', () => {
    const all = [...P.jobs.values()], rows = Rows.group(all, all, { discs: true });
    const of = k => rows.find(r => Rows.lineKeyOf(r.lead) === ROW[k].key);
    assert.equal(rows.length, 6, 'six lines are six rows');
    assert.deepEqual(keysOf(of('tag').jobs), [fronts.tag[0].key, made.tag[0].key]); assert.equal(of('tag').faces, true);
    assert.deepEqual(keysOf(of('ears').jobs), [fronts.ears[0].key, made.ears[0].key, fronts.ears[1].key, made.ears[1].key]); assert.equal(of('ears').pair, true); assert.equal(of('ears').faces, true);
    assert.deepEqual(keysOf(of('discs').jobs), [0, 1, 2].flatMap(i => [fronts.discs[i].key, made.discs[i].key])); assert.equal(of('discs').discs, true); assert.equal(of('discs').faces, true);
    for (const k of BACK_ONLY) assert.equal(of(k).faces, undefined, k + ': no faces flag');
    assert.equal(Rows.multi(of('tag')), true, 'a front and a back are two placements of one row'); assert.equal(Rows.multi(of('pend')), false);
  });
  await t('1b Back / Next go from piece to piece: a disc with two faces is ONE stop (its Front), a tag and an earring pair are one stop each', () => {
    const all = [...P.jobs.values()], rows = Rows.group(all, all, { discs: true }), D = i => fronts.discs[i].key;
    assert.equal(Rows.step(rows, D(0), 1).job.key, D(1)); assert.equal(Rows.step(rows, D(1), 1).job.key, D(2));
    assert.equal(Rows.step(rows, made.discs[1].key, 1).job.key, D(2), 'from the BACK of disc 2 the next stop is disc 3');
    assert.equal(Rows.step(rows, D(1), -1).job.key, D(0)); assert.equal(Rows.step(rows, made.discs[2].key, -1).job.key, D(1));
    const after = Rows.step(rows, D(2), 1); assert.notEqual(Rows.lineKeyOf(after.row.lead), ROW.discs.key, 'after the last disc: the next line');
    const fromTag = Rows.step(rows, fronts.tag[0].key, 1); assert.notEqual(Rows.lineKeyOf(fromTag.row.lead), ROW.tag.key, 'the tag\'s back is not another step');
  });
  await t('1c the very same switch component draws Front | Back: alone for a single charm, under Left | Right and Disc 1 | 2 | 3 when the line has several pieces; a back-only line draws the one switch it had', () => {
    const sw = (k, key) => Switch.html(Switch.list(P.jobsFor(k), { of: Faces.pieceCount(P.jobsFor(k)) }), key, { faces: true });
    const tag = sw('tag', fronts.tag[0].key); assert.equal((tag.match(/class="egEarSwitch/g) || []).length, 1); assert.match(tag, /egEarSwitch egFaces/); assert.equal((tag.match(/class="egEarTab"/g) || []).length, 2);
    const ears = sw('ears', fronts.ears[0].key); assert.equal((ears.match(/class="egEarSwitch/g) || []).length, 2, 'Left | Right, and the Front | Back of the shown ear'); assert.ok(ears.indexOf('egFaces') > ears.indexOf('LEFT EAR'));
    const discs = sw('discs', made.discs[1].key); assert.equal((discs.match(/class="egEarSwitch/g) || []).length, 2); assert.match(discs, /DISC 2 of 3/); assert.doesNotMatch(discs, /of 6/);
    const back = Switch.html(Switch.list(made.pair), made.pair[0].key, { faces: true }); assert.equal((back.match(/class="egEarSwitch/g) || []).length, 1); assert.doesNotMatch(back, /egFaces/);
  });

  // ═══ 2 · the real Engrave module in a fake page ═══
  await t('2a Placements: a line of two faces is ONE row told face by face with a "Front + back" chip; the tag, the earring pair and the necklace each; a back-only line keeps its row', () => {
    P.showList(); const rows = P.rowsOnPage(); assert.equal(rows.length, 6, 'six lines, six rows (the tag, the earrings and the necklace are not two rows each)');
    const row = k => rows.filter(r => r.dataset.rid === ROW[k].order.receiptId);
    for (const k of ['tag', 'ears', 'discs']) { assert.equal(row(k).length, 1, k + ' is ONE row'); assert.equal(row(k)[0].querySelector('.egMode').textContent, 'Front + back', k + ': the chip'); assert.equal(row(k)[0].querySelectorAll('.comparePair').length, 1); }
    const tag = row('tag')[0];
    assert.deepEqual([...tag.querySelectorAll('.egEar .egPiece')].map(n => n.textContent), ['FRONT', 'BACK']); assert.deepEqual([...tag.querySelectorAll('.egEar .w')].map(n => n.textContent), ['W', '1. 27. 24'], 'the front\'s words and the back\'s');
    assert.equal(tag.dataset.open, fronts.tag[0].key, 'a click opens the Front first'); assert.equal(tag.dataset.jobs, [fronts.tag[0].key, made.tag[0].key].join(' ')); assert.match(tag.getAttribute('aria-label'), /front and back/);
    const ears = row('ears')[0]; assert.deepEqual([...ears.querySelectorAll('.egEar .egPiece')].map(n => n.textContent), ['LEFT EAR · FRONT', 'LEFT EAR · BACK', 'RIGHT EAR · FRONT', 'RIGHT EAR · BACK']);
    assert.deepEqual([...ears.querySelectorAll('.egEar .w')].map(n => n.textContent), ['M', 'Mom', 'M', 'Mom']); assert.match(ears.getAttribute('aria-label'), /Left and Right ears/);
    const discs = row('discs')[0]; assert.deepEqual([...discs.querySelectorAll('.egEar .egPiece')].map(n => n.textContent), ['DISC 1 of 3 · FRONT', 'DISC 1 of 3 · BACK', 'DISC 2 of 3 · FRONT', 'DISC 2 of 3 · BACK', 'DISC 3 of 3 · FRONT', 'DISC 3 of 3 · BACK'], 'the discs are counted as discs: of 3, never of 6');
    assert.match(discs.getAttribute('aria-label'), /3 discs/); assert.doesNotMatch(discs.outerHTML, /of 6/);
    for (const k of BACK_ONLY) { assert.equal(row(k).length, 1); assert.equal(row(k)[0].querySelector('.egMode'), null, k + ': no chip'); assert.doesNotMatch(row(k)[0].outerHTML, /FRONT|BACK|data-face/, k + ': nothing about faces'); }
    // the picture is the LINE's, asked once for each faced line
    for (const k of ['tag', 'ears', 'discs']) assert.equal(P.pairCalls.filter(r => r === ROW[k]).length, 1, k + ': the line\'s own row, once');
  });
  await t('2b the tab badge and the rail count ROWS (6), not jobs (18); the pieces of a line are not counted twice', () => {
    P.showList(); assert.equal(document.querySelector('.egTab[data-tab="place"] b').textContent, '6'); assert.equal(E.pendingRows(), 6, 'rows still to settle'); assert.equal(E.pendingCount(), 18, 'jobs: what the run itself checks (2 + 4 + 6 + 1 + 2 + 3)');
  });
  await t('2c the tag (order 4195966039): the editor opens on its Front with the Front | Back switch; the heading, words and buttons name the face; the reading is told; Back is the same card for the back', () => {
    P.reset(); P.frames.length = 0; P.showList(); P.rowsOnPage().find(r => r.dataset.rid === RID_TAG).click(); P.runFrames();
    let card = P.card(); assert.ok(card); assert.equal(card.dataset.key, fronts.tag[0].key);
    assert.equal(card.querySelector('.rh .kind').textContent, '1 of 6 · 0 done', 'a face is not another step');
    const sws = card.querySelectorAll('.rh .egEarSwitch'); assert.equal(sws.length, 1, 'one switch'); assert.ok(sws[0].classList.contains('egFaces')); assert.equal(card.querySelectorAll('.egSwitchStack').length, 0);
    const tabs = [...sws[0].querySelectorAll('.egEarTab')]; assert.deepEqual(tabs.map(b => b.querySelector('.egPiece').textContent), ['FRONT', 'BACK']); assert.deepEqual(tabs.map(b => b.getAttribute('aria-pressed')), ['true', 'false']);
    assert.deepEqual(tabs.map(b => b.querySelector('.egEarWords').textContent), ['W', '1. 27. 24'], 'both faces\' words are on the switch');
    assert.equal(card.querySelector('.pvMain .pvH').textContent, 'Front · engraving'); assert.equal(card.querySelector('.wordsHead .lbl').textContent, 'Words on the front'); assert.equal(card.querySelector('[data-f="words"]').value, 'W');
    assert.match(card.querySelector('[data-a="approve"]').title, /write the front file/); assert.match(card.querySelector('.ctl [data-a="skip"]').textContent, /^No front engraving/); assert.equal(card.querySelectorAll('.rh .reviewIdentity > .egPiece').length, 0, 'the switch replaces the tag');
    assert.match(card.querySelector('.pvOrder').textContent, /asks for both faces; the note is labelled front - W \/ back - 1\. 27\. 24/, 'the system\'s reading, in its one plain line, beside the buyer\'s words');
    const both = card.querySelector('[data-a="approveBoth"]'); assert.equal(both.textContent, 'Approve both faces'); assert.equal(both.disabled, true, 'not before the back has been on screen'); assert.match(both.title, /back placement first/); assert.equal(card.querySelector('[data-a="approveFaces"]'), null, 'a single piece has just the one press');
    // the Back
    tabs[1].click(); P.runFrames(); card = P.card(); assert.equal(card.dataset.key, made.tag[0].key); assert.equal(card.querySelector('.rh .kind').textContent, '1 of 6 · 0 done');
    assert.equal(card.querySelector('.pvMain .pvH').textContent, 'Back · engraving'); assert.equal(card.querySelector('.wordsHead .lbl').textContent, 'Words on the back'); assert.equal(card.querySelector('[data-f="words"]').value, '1. 27. 24');
    assert.deepEqual([...card.querySelectorAll('.egEarTab')].map(b => b.getAttribute('aria-pressed')), ['false', 'true']); assert.match(card.querySelector('.ctl [data-a="skip"]').textContent, /^No back engraving/);
    const both2 = card.querySelector('[data-a="approveBoth"]'); assert.equal(both2.disabled, false, 'the front was on screen, the back is now'); assert.match(both2.title, /Front, then the Back/);
  });
  await t('2d the setup editor has its slot at the top of the right column and is mounted with the LINE after every drawing; without it the slot is empty and hidden (no layout shift)', () => {
    P.setupMounts.length = 0; const card = P.open(fronts.tag[0].key); const slot = card.querySelector('.pvSide > .pvSetup[data-engsetup]'); assert.ok(slot, 'first section of the right column'); assert.equal(card.querySelector('.pvSide > :not([data-said])'), slot);   /* (ENGNOTE: the customer's words box, when there are words, is drawn above the setup slot) */
    assert.ok(P.setupMounts.length >= 1); const [sl, row] = P.setupMounts[P.setupMounts.length - 1]; assert.equal(sl, slot); assert.equal(row.parentRow || row, ROW.tag, 'the line');
    const bare = facedPage({ setup: false }); const c2 = bare.open(bare.fronts.tag[0].key); const s2 = c2.querySelector('[data-engsetup]'); assert.equal(s2.hidden, true); assert.equal(s2.textContent, '');
  });
  await t('2e an earring pair with a front and a back: two switches, Left | Right with Front | Back right beneath it; each ear and face is its own card with its own words and approval', () => {
    P.reset(); let card = P.open(fronts.ears[0].key);
    assert.equal(card.querySelectorAll('.rh .egSwitchStack').length, 1); const sws = [...card.querySelectorAll('.rh .egSwitchStack > .egEarSwitch')]; assert.equal(sws.length, 2);
    assert.ok(!sws[0].classList.contains('egFaces') && sws[1].classList.contains('egFaces'), 'the piece switch first, the face switch beneath');
    assert.equal(sws[0].compareDocumentPosition(sws[1]) & 4, 4, 'the second follows the first in the page');
    assert.deepEqual([...sws[0].querySelectorAll('.egPiece')].map(n => n.textContent), ['LEFT EAR', 'RIGHT EAR']); assert.deepEqual([...sws[1].querySelectorAll('.egPiece')].map(n => n.textContent), ['FRONT', 'BACK']);
    assert.equal(card.querySelector('.pvMain .pvH').textContent, 'Front · engraving'); assert.equal(card.querySelector('.rh .kind').textContent, '1 of 6 · 0 done');
    sws[1].querySelectorAll('.egEarTab')[1].click(); P.runFrames(); card = P.card(); assert.equal(card.dataset.key, made.ears[0].key, 'the Left ear\'s back'); assert.equal(card.querySelector('[data-f="words"]').value, 'Mom'); assert.equal(card.querySelector('.pvMain .pvH').textContent, 'Back · engraving');
    card.querySelectorAll('.egSwitchStack > .egEarSwitch')[0].querySelectorAll('.egEarTab')[1].click(); P.runFrames(); card = P.card();
    assert.equal(card.dataset.key, fronts.ears[1].key, 'the Right ear opens on its front'); assert.equal(card.querySelector('[data-f="words"]').value, 'M'); assert.equal(card.querySelector('.rh .kind').textContent, '1 of 6 · 0 done', 'an ear is not another step');
    assert.deepEqual([...card.querySelectorAll('.egSwitchStack > .egEarSwitch')[0].querySelectorAll('.egEarTab')].map(b => b.getAttribute('aria-pressed')), ['false', 'true']);
    assert.equal(card.querySelectorAll('[data-f="words"]').length, 1, 'one card at a time');
  });
  await t('2f no "Approve both ears" (ENGAPPROVE: every piece has its own Approve); "Approve both faces" is the shown ear\'s own two and needs that ear\'s two placements on screen', () => {
    P.reset(); P.frames.length = 0; const open = k => P.open(k);
    let card = open(fronts.ears[0].key); const btn = c => ({ ears: c.querySelector('[data-a="approveBoth"]'), faces: c.querySelector('[data-a="approveFaces"]') });
    assert.equal(btn(card).ears, null, 'no press that approves both ears'); assert.equal(btn(card).faces.textContent, 'Approve both faces'); assert.equal(btn(card).faces.disabled, true);
    card = open(made.ears[0].key); assert.equal(btn(card).faces.disabled, false, 'the Left ear\'s front and back were both on screen'); assert.equal(btn(card).ears, null);
    open(fronts.ears[1].key); card = open(made.ears[1].key); assert.equal(btn(card).ears, null, 'all four were shown: still no press for both ears'); assert.equal(btn(card).faces.disabled, false);
    // words that differ between the ears of one face: each ear is its own piece, its own two faces are still one press
    fronts.ears[1].lines = ['N']; fronts.ears[1].text = 'N'; card = open(made.ears[0].key); assert.equal(btn(card).ears, null); assert.ok(btn(card).faces, 'the ear\'s own two faces, whatever the other ear says');
    fronts.ears[1].lines = ['M']; fronts.ears[1].text = 'M';
    // front and back carry different words by nature
    assert.notEqual(made.ears[0].text, fronts.ears[0].text);
  });
  await t('2g the necklace: Disc 1 | 2 | 3 counts discs (of 3), each disc has Front | Back beneath; never an "Approve all discs" / "the 2 discs left" (ENGAPPROVE)', () => {
    P.reset(); let card = P.open(fronts.discs[1].key);
    const sws = [...card.querySelectorAll('.rh .egSwitchStack > .egEarSwitch')]; assert.equal(sws.length, 2); assert.deepEqual([...sws[0].querySelectorAll('.egEarTab .egPiece')].map(n => n.textContent), ['DISC 1 of 3', 'DISC 2 of 3', 'DISC 3 of 3']);
    assert.deepEqual([...sws[0].querySelectorAll('.egEarTab')].map(b => b.getAttribute('aria-pressed')), ['false', 'true', 'false']); assert.doesNotMatch(sws[0].outerHTML + sws[1].outerHTML, /of 6/);
    assert.equal(card.querySelector('.rh .kind').textContent, '1 of 6 · 0 done'); assert.equal(card.querySelector('[data-a="approveBoth"]'), null, 'the front words differ from disc to disc: each disc on its own');
    for (const [i, j] of fronts.discs.entries()) { j.lines = ['A']; j.text = 'A'; made.discs[i].lines = ['Mom']; made.discs[i].text = 'Mom'; }
    P.relink('discs'); card = P.open(fronts.discs[1].key); let b = card.querySelector('[data-a="approveBoth"]'); assert.equal(b, null, 'the same words on every disc: still no press for all of them');
    assert.ok(card.querySelector('[data-a="approveFaces"]'), 'and the shown disc\'s own two faces');
    // disc 1 decided on both faces: two discs left
    for (const j of [fronts.discs[0], made.discs[0]]) { j.state = 'approved'; j.approvedBy = 'Paul'; j.approvedAt = 1.7e12; j.backs = []; }
    P.relink('discs'); P.resetRecs(); card = P.open(fronts.discs[1].key); b = card.querySelector('[data-a="approveBoth"]'); assert.equal(b, null, 'and none for the 2 discs left'); assert.equal(card.querySelectorAll('.rh .egSwitchStack > .egEarSwitch')[0].querySelectorAll('.egEarTab').length, 2, 'the discs still to do');
    assert.deepEqual([...card.querySelectorAll('.rh .egSwitchStack > .egEarSwitch')[0].querySelectorAll('.egEarTab .egPiece')].map(n => n.textContent), ['DISC 2 of 3', 'DISC 3 of 3'], 'still of 3: the count is the order\'s discs');
    for (const j of [fronts.discs[0], made.discs[0]]) { j.state = 'review'; delete j.approvedBy; delete j.approvedAt; } P.reset();
  });
  await t('2h Back / Next go from piece to piece in the editor: Disc 1, 2, 3 of the necklace, then the next line; a tag\'s or an ear\'s other face is not a step', () => {
    P.reset(); const press = (card, a) => { card.querySelector(`[data-a="${a}"]`).click(); P.runFrames(); return P.card(); };
    let card = P.open(fronts.discs[0].key); card = press(card, 'next'); assert.equal(card.dataset.key, fronts.discs[1].key, 'Next from disc 1: disc 2 (its front)'); card = press(card, 'next'); assert.equal(card.dataset.key, fronts.discs[2].key);
    card = press(card, 'next'); assert.notEqual(card.dataset.key, fronts.discs[0].key); assert.ok(!card.dataset.key.startsWith(ROW.discs.key), 'after disc 3: the next line');
    card = press(card, 'prev'); assert.equal(card.dataset.key, fronts.discs[2].key, 'Back from there: disc 3'); card = press(card, 'prev'); assert.equal(card.dataset.key, fronts.discs[1].key);
    card = P.open(made.discs[1].key); card = press(card, 'next'); assert.equal(card.dataset.key, fronts.discs[2].key, 'from the BACK of disc 2, Next is disc 3\'s front');
    card = P.open(fronts.tag[0].key); const n = press(card, 'next'); assert.ok(!n.dataset.key.startsWith(ROW.tag.key), 'the tag\'s back is not another step');
    card = P.open(made.ears[1].key); const p = press(card, 'prev'); assert.ok(!p.dataset.key.startsWith(ROW.ears.key), 'an ear is not another step');
  });
  await t('2i skipping a face: "No front engraving" cuts only the front plain (a seal by the person), the back of the piece stays to do; the row tells it', async () => {
    P.reset(); const card = P.open(fronts.tag[0].key); const b = card.querySelector('.ctl [data-a="skip"]'); b.click(); await new Promise(r => setImmediate(r)); await new Promise(r => setImmediate(r));
    assert.equal(fronts.tag[0].state, 'skipped'); assert.equal(made.tag[0].state, 'review', 'the back is untouched'); assert.ok(Seals.list(fronts.tag[0]).some(x => x.how === 'engravePlain' && x.face === 'front'), 'a plain seal, on the front');
    P.showList(); const row = P.rowsOnPage().find(r => r.dataset.rid === RID_TAG); assert.ok(row, 'the line is still to do (its back)'); assert.deepEqual([...row.querySelectorAll('.egEar .egPiece')].map(n => n.textContent), ['BACK']); assert.equal(row.querySelector('.egMode').textContent, 'Front + back');
    E.restoreView({ tab: 'done', chosen: true, list: true, focus: null }); E.render(); const dn = [...document.querySelectorAll('#egDone .decidedRow')].find(r => r.dataset.rid === RID_TAG); assert.ok(dn, 'and the front is in Decided');
    assert.equal(dn.querySelector('.ost.warn').textContent, 'No engraving'); assert.match(dn.querySelector('.ost.warn').title, /nothing on the front/); assert.equal(dn.querySelector('.seal').dataset.face, 'front');
    P.reset(); P.jobs.get(fronts.tag[0].key).state = 'review';
  });
  await t('2j Decided: both faces decided is ONE row with the chip, each face\'s seal and a Reopen for each face; reopening one sends only that face back', async () => {
    P.reset(); const fj = fronts.tag[0], bj = made.tag[0];
    for (const j of [fj, bj]) { j.state = 'approved'; j.approvedBy = 'Paul'; j.approvedAt = 1.7e12; j.backs = []; Seals.add(j, 'engraveApproved', 'Paul', 1.7e12); }
    P.relink('tag'); E.restoreView({ tab: 'done', chosen: true, list: true, focus: null, openDone: null }); E.render();
    const rows = [...document.querySelectorAll('#egDone .decidedRow')].filter(r => r.dataset.rid === RID_TAG); assert.equal(rows.length, 1, 'one row for the tag'); const dn = rows[0];
    assert.equal(dn.querySelector('.egMode').textContent, 'Front + back'); assert.equal(dn.dataset.ears, [fj.key, bj.key].join(' '));
    assert.deepEqual([...dn.querySelectorAll('.seal')].map(s => s.dataset.face), ['front', 'back'], 'both faces\' seals');
    assert.deepEqual([...dn.querySelectorAll('[data-a="reopen"]')].map(x => x.textContent), ['Reopen front', 'Reopen back']); assert.match(dn.querySelector('[data-a="reopen"]').title, /everything else keeps its approval/);
    dn.querySelectorAll('[data-a="reopen"]')[0].click(); await new Promise(r => setImmediate(r));
    assert.equal(fj.state, 'words', 'the front goes back to its words'); assert.equal(bj.state, 'approved', 'the back keeps its approval'); assert.match(fj.reason || '', /reopened by Paul/);
    P.reset(); fj.state = 'review'; bj.state = 'review';
  });
  await t('2k a search finds a line by "front" or "back" and by either face\'s words, and still shows every face on its row', () => {
    P.reset(); const rowsFor = q => { E.restoreView({ tab: 'place', chosen: true, list: true, focus: null, q }); E.render(); return P.rowsOnPage().map(r => r.dataset.rid); };
    assert.deepEqual(rowsFor('27. 24'), [RID_TAG]); assert.deepEqual(rowsFor('w'), [RID_TAG, ROW.ears.order.receiptId, ROW.discs.order.receiptId, ROW.pend.order.receiptId, ROW.pair.order.receiptId, ROW.neck.order.receiptId].filter(r => rowsFor('w').includes(r)));
    const front = rowsFor('front'); assert.deepEqual(front.sort(), [RID_TAG, ROW.ears.order.receiptId, ROW.discs.order.receiptId].sort(), 'only the lines with a front');
    const row = P.rowsOnPage()[0]; assert.ok(row.querySelectorAll('.egEar').length >= 2); rowsFor('');
  });

  await t('2l the card of a face job without the Engrave tab\'s row (the Review list) has no switch but names its piece and face; a line with the front only is a "Front" row and card', () => {
    P.reset(); let card = E.placementCard(fronts.ears[0], 1);
    assert.equal(card.querySelector('.egEarSwitch'), null); assert.equal(card.querySelector('[data-a="approveBoth"]'), null); assert.equal(card.querySelector('.rh .reviewIdentity > .egPiece').textContent, 'LEFT EAR · FRONT'); assert.equal(card.querySelector('.pvMain .pvH').textContent, 'Front · engraving');
    card = E.placementCard(made.tag[0], 1); assert.equal(card.querySelector('.rh .reviewIdentity > .egPiece').textContent, 'BACK', 'the back of a charm that has a front says so'); assert.equal(card.querySelector('.pvMain .pvH').textContent, 'Back · engraving');
    // a line that engraves the front only: its one job is the front
    const Q = facedPage({ setup: true }); Q.addFront('pend', 0); Q.jobs.delete(Q.made.pend[0].key); Q.setup([Q.fronts.pend[0]], 'review', ['Z']); Q.resetRecs();
    Q.showList(); const row = Q.rowsOnPage().find(r => r.dataset.rid === Q.ROW.pend.order.receiptId); assert.ok(row); assert.equal(row.querySelector('.egMode').textContent, 'Front'); assert.equal(row.querySelector('.egMode').dataset.face, 'front'); assert.deepEqual([...row.querySelectorAll('.egEar .egPiece')].map(n => n.textContent), ['FRONT']);
    const c2 = Q.open(Q.fronts.pend[0].key); assert.equal(c2.querySelector('.egEarSwitch'), null, 'one face: nothing to switch'); assert.equal(c2.querySelector('.rh .reviewIdentity > .egPiece').textContent, 'FRONT'); assert.equal(c2.querySelector('.pvMain .pvH').textContent, 'Front · engraving'); assert.equal(c2.querySelector('.wordsHead .lbl').textContent, 'Words on the front');
    assert.equal(c2.querySelector('[data-a="approveBoth"]'), null, 'one placement: the ordinary Approved'); assert.ok(c2.querySelector('[data-a="approve"]'));
    Q.dom.window.close();
  });
  await t('2n the REAL setup editor (charm-nest-engrave-setup.js) fills that slot: "Engraves: Front + back" with each face\'s words, as the system read them, right in the card', () => {
    const R = facedPage({ setupReal: true }); const card = R.open(R.fronts.tag[0].key); const slot = card.querySelector('.pvSide > .pvSetup'); assert.ok(slot);
    assert.equal(slot.hidden, false, 'it has something to say about this line'); const txt = norm(slot.textContent);
    assert.match(txt, /Front \+ back/); assert.match(txt, /W/); assert.match(txt, /1\. 27\. 24/); assert.match(txt, /asks for both faces/, 'the system\'s one plain line');
    const back = R.open(R.made.tag[0].key); assert.match(norm(back.querySelector('.pvSide > .pvSetup').textContent), /Front \+ back/, 'the same line on the back\'s card');
    const plain = R.open(R.made.pend[0].key); const quiet = norm(plain.querySelector('.pvSide > .pvSetup').textContent); assert.doesNotMatch(quiet, /Front/, 'a back-only line: nothing about a front'); R.dom.window.close();
  });
  await t('2m the public API carries the face helpers the rows and cards use', () => {
    assert.equal(typeof E.approveRow, 'function'); assert.equal(E.faceModeOf(ROW.tag), 'both'); assert.equal(E.faceModeOf(ROW.pend), null, 'a back-only line has no mode chip'); assert.equal(E.lineHasFront(ROW.ears), true); assert.equal(E.lineHasFront(ROW.pair), false);
    assert.equal(E.faceModeOf(fronts.tag[0].row), 'both', 'from a face job\'s own row too');
  });

  // ═══ 3 · approval ═══
  const approvalWorld = (opts) => {
    opts = opts || {};
    const jobsA = new Map(), saved = [], events = [], toasts = [], removed = [], moved = [], names = [];
    const pool = poolOf(W), kind = opts.kind || 'tag', parent = Object.assign({}, mkRows()[kind], { engrave: undefined });
    const win = { CharmNestEngraveRows: Rows, CharmNestEngraveSides: Sides, CharmNestPair: Pair, CharmNestEngraveFaces: Faces };
    const slots = kind === 'tag' ? [null] : kind === 'ears' ? ['L', 'R'] : ['D1', 'D2', 'D3'].slice(0, opts.discs || 3);
    const mk = (slot, face, lines) => { const j = Object.assign({ key: Faces.jobKey(parent.key, slot, face), rowKey: slot ? parent.key : undefined, slot: slot || undefined, groupKey: '', state: 'review', lines, text: lines.join('\n'), copies: Sides.idsOfSlot(CTX, parent, slot), engraveRec: { needed: true, state: 'review', approved: false }, backs: [] }, face === 'front' ? { face: 'front' } : {}, readyFit()); if (!slot) { delete j.slot; delete j.rowKey; } j.view = { cx: 0, cy: 0, cutMembers: [] }; j.fit.glyphs = []; j.row = Sides.sideRow(CTX, parent, slot, j); return j; };
    const list = []; slots.forEach((sl, i) => { const fw = opts.front ? opts.front[i] || opts.front[0] : ['W']; const bw = opts.back ? opts.back[i] || opts.back[0] : ['1']; if (!opts.noFront) list.push(mk(sl, 'front', fw)); list.push(mk(sl, 'back', bw)); });
    for (const j of list) jobsA.set(j.key, j); Sides.linkParent(CTX, parent, list);
    const ctx = { window: win, Date, Promise, Map, Set, WeakMap, Array, Object, String, Number, JSON, Math, console, PT: 72 / 25.4, P: { buildBackFile: async () => ({ bytes: new Uint8Array([1]) }) }, S: { settings: {} }, B: { pool: { rows: pool } },
      charmFor: () => ({ sourceId: 'source' }), sheetFor: () => ({ sheetId: 'sheet-1' }), sourceOf: () => ({ parsed: {} }), fitOpts: () => ({ lineGap: .18 }), fontIdOf: () => 'source-sans-3', verifyBackFile: async () => ({ ok: true }), EG: { cardKey: null, card: null, drafts: {} },
      CNListActivity: Activity, CNEngravingSeals: Object.assign({}, Seals, { press: async () => {} }), employeeName: () => 'Paul', needEmployee: async () => { names.push('asked'); return 'Paul'; }, Review: { remove(k) { removed.push(k); } }, goes(j) { moved.push(j.key); }, EG_TAB: () => '',
      agent() {}, render() {}, saveBacks: async j => { saved.push(j.key); }, toast: m => toasts.push(m), humanAct() {}, TL: { rec: ev => events.push(ev) }, SheetEvents: { label: () => 'Sheet 1' }, SIDES: () => Sides,
      items: () => jobsA, jobsOf: () => [...jobsA.values()], fitTasks: new WeakMap(), DECIDED: ['approved', 'written', 'skipped'], isWorking: () => false, lineOf: r => r.parentRow || r, unplacedPre: () => false, virtualJob: () => false,   // (ENGPRE's helpers of the bridge the sliced approval code calls)
      queuedJobs: js => js.filter(j => ['review', 'words', 'blocked', 'classify', 'ready', 'fitting'].includes(j.state)), decidedJobs: () => [...jobsA.values()].filter(j => ['approved', 'written', 'skipped'].includes(j.state)), esc: s => String(s) };
    vm.createContext(ctx);
    const rowsHelpers = SOURCE.slice(SOURCE.indexOf('  const ROWS = () =>'), SOURCE.indexOf('  // a draft outlives its card only while'));
    const approveCode = SOURCE.slice(SOURCE.indexOf('  async function prepareApproval('), SOURCE.indexOf("  /** An approval's back files are written"));
    const earCode = SOURCE.slice(SOURCE.indexOf('  const earTagText = job'), SOURCE.indexOf('  /** The row of the Engrave lists for one job'));
    const rowApprove = SOURCE.slice(SOURCE.indexOf('  /** The Left | Right switch of a row'), SOURCE.indexOf('  /** The placement review card'));
    vm.runInContext(outerDefs(OUTER, rowsHelpers + approveCode + earCode + rowApprove) + '\n' + jobEngineStandIn(rowsHelpers + approveCode + earCode + rowApprove) + '\nthis.__api = { approve, approveRow, approveBothHtml, rowsIn, queueUniverse, markShown };', ctx);
    if (opts.shown !== false) for (const j of list) ctx.__api.markShown(j);
    const byKey = new Map(list.map(j => [j.key, j]));
    return { ctx, list, parent, saved, events, toasts, moved, names, byKey, F: list.filter(j => j.face === 'front'), B: list.filter(j => j.face !== 'front'), mark: j => ctx.__api.markShown(j), api: ctx.__api };
  };
  const summary = (w, list) => (list || w.list).map(j => ({ key: j.key, state: j.state, by: j.approvedBy, text: j.text, rec: { needed: j.row.engrave.needed, state: j.row.engrave.state, approved: j.row.engrave.approved, text: j.row.engrave.text, approvedBy: j.row.engrave.approvedBy }, seals: Seals.list(j).map(s => [s.how, s.by, s.face || 'back']),
    events: w.events.filter(e => e.data.poolId && j.copies.includes(e.data.poolId)).map(e => [e.type, e.data.face || 'back', e.by]) }));
  const html3 = w => w.api.approveBothHtml(w.list, w.list[0]);

  await t('3a "Approve both faces" writes what two separate presses write: per face a state, a seal, a record on its own row, a back save (Front first); the line reads approved only when both are', async () => {
    const A = approvalWorld(); await A.api.approve(A.F[0], 'Paul'); assert.equal(A.parent.engrave.approved, false, 'one face is not the line'); await A.api.approve(A.B[0], 'Paul');
    const B = approvalWorld(); await B.api.approveRow(B.B[0], undefined);
    assert.deepEqual(summary(B), summary(A), 'the same records, seals and timeline events'); assert.deepEqual(B.saved, A.saved); assert.deepEqual(B.saved, [B.F[0].key, B.B[0].key], 'the Front file, then the Back file');
    assert.ok(B.list.every(j => j.state === 'approved')); assert.notEqual(B.F[0].row.engrave, B.B[0].row.engrave, 'two records'); assert.equal(B.parent.engrave.approved, true); assert.deepEqual(B.names, [], 'a name is known: nobody is asked');
    assert.match(html3(approvalWorld()), />Approve both faces</);
  });
  await t('3b an earring pair: no press approves both ears (a stale one is refused); "Approve both faces" approves only the shown ear\'s two', async () => {
    const A = approvalWorld({ kind: 'ears', front: [['M']], back: [['Mom']] }); const keys = A.list.map(j => j.key);
    assert.deepEqual(keys, ['L~front', 'L', 'R~front', 'R'].map(s => A.parent.key + '#' + s.replace('~front', '') + (s.endsWith('~front') ? '~front' : '')), 'the jobs of the row');
    await A.api.approveRow(A.list[0]); assert.ok(A.list.every(j => j.state === 'review')); assert.match(A.toasts[0], /Approve each piece on its own/); assert.deepEqual(A.saved, [], 'a refused press writes nothing');
    for (const k of [0, 2]) await A.api.approveRow(A.list[k], undefined, 'piece'); assert.ok(A.list.every(j => j.state === 'approved')); assert.deepEqual(A.saved, keys, 'piece by piece, Front before Back');
    const B = approvalWorld({ kind: 'ears' }); await B.api.approveRow(B.list[1], undefined, 'piece');
    assert.deepEqual(B.list.map(j => j.state), ['approved', 'approved', 'review', 'review'], 'only the Left ear\'s faces'); assert.deepEqual(B.saved, [B.list[0].key, B.list[1].key]);
    const h = html3(approvalWorld({ kind: 'ears' })); assert.doesNotMatch(h, /Approve both ears/); assert.doesNotMatch(h, /data-a="approveBoth"/); assert.match(h, />Approve both faces</);
  });
  await t('3c a necklace: no "Approve all discs" / "the N discs left" (ENGAPPROVE); each disc\'s two faces are approved by that disc\'s own press, in disc order', async () => {
    const A = approvalWorld({ kind: 'discs', front: [['A']], back: [['1']] }); const keys = A.list.map(j => j.key); assert.equal(keys.length, 6);
    assert.doesNotMatch(A.api.approveBothHtml(A.list, A.list[0]), /Approve all discs|discs left|data-a="approveBoth"/);
    await A.api.approveRow(A.list[2]); assert.ok(A.list.every(j => j.state === 'review')); assert.match(A.toasts[0], /Approve each piece on its own/); assert.deepEqual(A.saved, []);
    for (const k of [0, 2, 4]) await A.api.approveRow(A.list[k], undefined, 'piece'); assert.ok(A.list.every(j => j.state === 'approved')); assert.deepEqual(A.saved, keys, 'disc 1 front, disc 1 back, disc 2 front, ...');
    const B = approvalWorld({ kind: 'discs' }); B.list.slice(0, 2).forEach(j => { j.state = 'approved'; }); const left = B.list.slice(2); assert.doesNotMatch(B.api.approveBothHtml(left, left[0]), /discs left|data-a="approveBoth"/);
    const C = approvalWorld({ kind: 'discs', discs: 2 }); assert.doesNotMatch(C.api.approveBothHtml(C.list, C.list[0]), /Approve all discs|data-a="approveBoth"/);
  });
  await t('3d different words on the front and the back are fine, and different words on two pieces never matter now: each piece is approved on its own (ENGAPPROVE), no row-level press', async () => {
    const A = approvalWorld({ kind: 'discs', front: [['A'], ['B'], ['A']], back: [['1']] }); assert.doesNotMatch(html3(A), /data-a="approveBoth"/, 'no press for the row');
    await A.api.approveRow(A.list[0]); assert.ok(A.list.every(j => j.state === 'review')); assert.match(A.toasts[0], /Approve each piece on its own/); assert.deepEqual(A.saved, []);
    await A.api.approveRow(A.list[0], undefined, 'piece'); assert.deepEqual(A.list.map(j => j.state), ['approved', 'approved', 'review', 'review', 'review', 'review'], 'Disc 1\'s two faces, whatever the other discs say');
    const B = approvalWorld({ kind: 'discs', front: [['A']], back: [['1'], ['2'], ['3']] }); assert.doesNotMatch(html3(B), /data-a="approveBoth"/, 'the back words differ'); await B.api.approveRow(B.list[0]); assert.match(B.toasts[0], /Approve each piece on its own/);
    const C = approvalWorld({ kind: 'tag', front: [['W']], back: [['1. 27. 24']] }); assert.notEqual(html3(C), '', 'a tag: the two faces carry different words, as they do');
  });
  await t('3e a face that was not put in front of the person is never approved with the other; a face that does not settle stops the faces after it', async () => {
    const A = approvalWorld({ shown: false }); const h = html3(A); assert.match(h, /disabled/); assert.match(h, /placement first/);
    await A.api.approveRow(A.list[0]); assert.ok(A.list.every(j => j.state === 'review')); assert.match(A.toasts[0], /Look at the front placement first/);
    A.mark(A.F[0]); await A.api.approveRow(A.list[0]); assert.match(A.toasts[1], /Look at the back placement first/); assert.deepEqual(A.saved, []); A.mark(A.B[0]); assert.doesNotMatch(html3(A), /disabled/);
    const B = approvalWorld({ kind: 'ears' }); B.ctx.verifyBackFile = async (bytes, job) => (job === B.list[1] ? { ok: false, why: 'engraving intersects a cut-out' } : { ok: true });
    await B.api.approveRow(B.list[0], undefined, 'piece'); assert.deepEqual(B.list.map(j => j.state), ['approved', 'review', 'review', 'review'], 'the Left front settled, the Left back did not pass its file check, so the Right ear was left as it is'); assert.deepEqual(B.saved, [B.list[0].key]);
    assert.ok(B.toasts.some(m => /Left ear Back was not approved, so the placements after it were left as they are/.test(m)), JSON.stringify(B.toasts));
    const C = approvalWorld({ kind: 'tag' }); C.F[0].verify = { geometry: { ok: false } }; await C.api.approveRow(C.B[0]); assert.ok(C.list.every(j => j.state === 'review'), 'a placement that failed its check is never forced'); assert.match(C.toasts[0], /ready to check first/);
  });
  await t('3f a line with one face is approved exactly as before: no "both" press, the ordinary approval', async () => {
    const A = approvalWorld({ kind: 'tag', noFront: true }); assert.equal(A.api.approveBothHtml([A.list[0]], A.list[0]), ''); await A.api.approveRow(A.list[0]); assert.equal(A.list[0].state, 'review'); assert.match(A.toasts[0], /one ear to approve/);
    await A.api.approve(A.list[0], 'Paul'); assert.equal(A.list[0].state, 'approved'); assert.deepEqual(A.saved, [A.list[0].key]);
    const B = approvalWorld({ kind: 'ears', noFront: true }); assert.equal(html3(B), '', 'ears with one face each: no press that approves both ears, and no faces press'); assert.doesNotMatch(html3(B), /approveFaces|both faces/);
  });

  // ═══ 4 · editing a saved front ═══
  const openWorld = () => {
    const jobs = new Map(), calls = [], live = { sheetId: 'sheet', charms: [{ poolId: 'copy1' }], backPool: [] };
    const back = { poolId: 'copy1', sheetId: 'sheet', text: '1. 27. 24', lines: ['1. 27. 24'], sizePt: 4, capMm: 1, centre: [12, 14], angle: 15, approvedAt: 10, approvedBy: 'Tester', copy: 1, engravingSeals: [{ how: 'engraveApproved', at: 5, by: 'Paul' }] };
    const front = { ...back, face: 'front', text: 'W', lines: ['W'], approvedAt: 12, engravingSeals: [{ how: 'engraveApproved', at: 6, by: 'Paul', face: 'front' }] };
    const record = { id: 'sheet', poolIds: ['copy1'], backPool: [back, front], outputs: { ai: { path: 'sets/test/front.ai' } }, fileBase: 'front' };
    const order = { key: 'batch', poolIds: ['copy1'], order: { receiptId: RID_TAG }, line: { transactionId: 'line' }, spec: { designSku: 'sku' }, engrave: { state: 'written' } };
    const views = [];
    const context = { window: { CharmNestEngraveFaces: Faces }, CNEngravingSeals: Seals, B: { engrave: { items: jobs } }, Pool: { charmOf: () => ({ bbox: [0, 0, 20, 30], frontKeepOut: [['front']], backKeepOut: [['back']] }) }, allSheets: () => [live], api: async (fn, b) => { calls.push(b); return { sheet: structuredClone(record) }; },
      G: { backView: (charm, o) => { views.push(o); return {}; }, engraveMask: (v, o) => { views.push({ mask: o.keepOut }); return { cx: 0, cy: 0 }; }, layoutLines: (lines, font, size, gap, angle, centre) => ({ cmds: [], glyphs: [], centre, angle }), verifyInk: () => ({ ok: true }), refitAt: () => ({ ok: true, size: 10 }) },
      F_: { ok: true }, O: require(path.join(root, 'charm-nest-orders.js')), ensureJobFont: async () => {}, fontFor: () => ({}), loadFonts: async () => {}, fitOpts: () => ({}), fitJob: async () => { throw Error('a saved valid placement must not be refitted'); }, S: { settings: {}, library: { rows: [] } },
      document: { querySelectorAll: () => [] }, EG: {}, setMode: mode => calls.push({ mode }), render: () => {}, toast: m => { throw Error(m); }, Orders: { rows: () => [order], render: () => {} }, Review: { remove: () => {}, render: () => {} }, Session: { schedule: () => {} }, refreshBacks: () => {}, MM: 25.4 / 72 };
    jobs.set('batch', { key: 'batch', copies: ['copy1'], row: order, state: 'written', backs: [back] });
    const start = SOURCE.indexOf('  const items = () => B.engrave.items;'), end = SOURCE.indexOf('  const pendingCount =', start);
    vm.createContext(context); vm.runInContext(outerDefs(OUTER, SOURCE.slice(start, end)) + '\n' + SOURCE.slice(start, end), context);
    return { context, jobs, calls, record, views, back, front, order };
  };
  await t('4a openBack(poolId, sheetId, "front") opens the saved FRONT on its own record: its words, its key, its face, drawn as the charm is and on the front keep-out; without a face it is the back, as always', async () => {
    const w = openWorld(); await w.context.openBack('copy1', 'sheet', 'front');
    const fj = w.jobs.get('back:copy1~front'); assert.ok(fj, 'the editing job of the front'); assert.equal(fj.face, 'front'); assert.equal(fj.text, 'W'); assert.equal(fj.expectedApprovedAt, 12); assert.equal(fj.requests.side, 'front'); assert.equal(fj.editOriginal.face, 'front');
    assert.equal(w.jobs.get('back:copy1'), undefined, 'and the back was not opened'); assert.deepEqual(w.views[0].face, 'front', 'the front view is asked for'); assert.deepEqual(w.views[1].mask, [['front']], 'with the master\'s FRONT keep-out');
    assert.deepEqual(Seals.list(fj).map(s => [s.at, s.by, s.face || 'back']), [[6, 'Paul', 'front'], [12, 'Tester', 'front']], 'the front record\'s seals, each saying it is the front\'s');
    assert.ok(Seals.list(fj).every(s => (s.face || 'front') === 'front'), 'none of the back\'s seals are on the front');
    const w2 = openWorld(); await w2.context.openBack('copy1', 'sheet'); const bj = w2.jobs.get('back:copy1'); assert.ok(bj); assert.equal(bj.text, '1. 27. 24'); assert.equal(bj.face, undefined, 'the back job carries no face: it is what it was'); assert.equal(bj.requests.side, 'back');
    assert.equal(w2.views[0].face, undefined, 'the back view is asked for exactly as before'); assert.deepEqual(w2.views[1].mask, [['back']]); assert.equal(w2.jobs.get('back:copy1~front'), undefined);
  });
  await t('4b an edit of the front never touches the piece\'s back: the other face keeps its job, its copies and its record', async () => {
    const w = openWorld(); const batchBack = { key: 'batch', copies: ['copy1'], row: w.order, state: 'written', backs: [w.back] }, batchFront = { key: 'batch~front', face: 'front', copies: ['copy1'], row: w.order, state: 'written', backs: [w.front] };
    w.jobs.set('batch', batchBack); w.jobs.set('batch~front', batchFront);
    await w.context.openBack('copy1', 'sheet', 'front'); const fj = w.jobs.get('back:copy1~front'); fj.state = 'written'; fj.text = 'WW'; fj.approvedAt = 20; fj.approvedBy = 'Seth'; Seals.add(fj, 'engraveApproved', 'Seth', 20);
    w.record.backPool[1] = { ...w.front, text: 'WW', approvedAt: 20, approvedBy: 'Seth', engravingSeals: Seals.keep(fj) };
    await w.context.syncEditedBack(fj);
    assert.deepEqual([...batchBack.copies], ['copy1'], 'the back job keeps the piece'); assert.deepEqual([...batchFront.copies], [], 'the old front job gives the piece to the edited front');
    assert.equal(batchBack.state, 'written'); assert.equal(w.record.backPool[0].text, '1. 27. 24', 'the back record is as it was');
  });

  // ═══ 5 · INERT for back-only lines ═══
  await t('5a a back-only pendant, earring pair and necklace draw byte for byte what the page drew before faces: the row, the editor card (apart from the setup editor\'s empty slot) and the Decided row', () => {
    const got = {}; for (const k of BACK_ONLY) Object.assign(got, surfaces(P, k));
    for (const k of Object.keys(GOLDEN)) if (BACK_ONLY.some(b => k.endsWith(':' + b))) assert.equal(got[k], GOLDEN[k], k);
    assert.equal(Object.keys(got).length, 9);
  });
  await t('5b the same lines on a page that has the faces module but no front job (the job engine not on the page yet): the tag, the earrings and the necklace draw as plain back lines, no chip, no switch of faces', () => {
    const Q = makePage(SOURCE, { faces: true, setup: false, noReading: true });
    Q.reset = () => { Q.setup(Q.made.tag, 'review', ['Mom']); Q.setup(Q.made.ears, 'review', ['Mom']); Q.setup(Q.made.discs, 'review', [['A'], ['B'], ['C']]); Q.setup(Q.made.pend, 'review', ['Mom']); Q.setup(Q.made.pair, 'review', ['Mom']); Q.setup(Q.made.neck, 'review', [['A'], ['B'], ['C']]); Q.resetRecs(); };
    const plain = makePage(SOURCE, { noReading: true }); plain.reset = Q.reset.bind({}); plain.reset = () => { plain.setup(plain.made.tag, 'review', ['Mom']); plain.setup(plain.made.ears, 'review', ['Mom']); plain.setup(plain.made.discs, 'review', [['A'], ['B'], ['C']]); plain.setup(plain.made.pend, 'review', ['Mom']); plain.setup(plain.made.pair, 'review', ['Mom']); plain.setup(plain.made.neck, 'review', [['A'], ['B'], ['C']]); plain.resetRecs(); };
    for (const k of ['tag', 'ears', 'discs']) { const a = surfaces(Q, k), b = surfaces(plain, k); assert.deepEqual(a, b, k + ': with the faces module and no front job, the same as without it'); for (const s_ of Object.keys(a)) assert.equal(a[s_], GOLDEN[s_], s_ + ': what the page drew before faces'); }
    Q.reset(); Q.showList(); assert.equal(Q.document.querySelectorAll('.egMode').length, 0); assert.equal(Q.document.querySelectorAll('[data-face]').length, 0);
    const card = Q.open(Q.made.tag[0].key); assert.equal(card.querySelectorAll('.egEarSwitch').length, 0); assert.equal(card.querySelector('.pvMain .pvH').textContent, 'Back · engraving'); assert.equal(card.querySelector('.wordsHead .lbl').textContent, 'Words on the back');
    assert.equal(card.querySelector('.rh .reviewIdentity > .egPiece'), null, 'a single charm has no tag at all, as before');
  });
  await t('5c the changed files carry a new cache token (-eu1), the existing suffixes are kept, and the stylesheet only gained rules for the chips and the stacked switches', () => {
    assert.match(html, /charm-nest-bridge\.js\?v=[^"]*-eu1/); assert.match(html, /charm-nest-activity\.css\?v=[^"]*-eu1/);
    assert.ok(/\.egSwitchStack\{/.test(css) && /\.egPiece\.egMode\{/.test(css));
  });
  assert.equal(P.errors.length, 0, 'no errors were logged by the page code: ' + JSON.stringify(P.errors.slice(0, 2)));
  P.dom.window.close();
  console.log(`\nengrave-ui-faces: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})();
