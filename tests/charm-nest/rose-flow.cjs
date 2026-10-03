// LibraryFlowRose (charm-nest-flow-rose.js): the Rose Gold guard of the Library's process flow, offline.
// The REAL pieces run together over an in-memory store: the page's charm-nest-rose-ui.js (RoseStock) and the server's
// _charmNestRoseStock.js (rosePlan, roseRecordCut), which refuses a new green line that is not asked for with cut:true.
// What is proved:
//   1 · check() says needsLine for an uncut Rose Gold sheet with no line, and false for a cut one, a non-Rose Gold one,
//       one that already has its line and a full one; for a set it names only the sheets that need one; it never writes
//       (no write call, no change to any sheet), and a sheet it cannot read is flagged unknown, not guessed;
//   2 · confirmBar() mounts without writing, fires onConfirm only from an explicit press on "Add the green dash line"
//       (not a scripted click, not before it is armed, not twice) and onCancel only from "Not now" or Esc; Not now leaves
//       every sheet and the store exactly as they were;
//   3 · calculate() runs the existing path (rosePlan with cut:true, one dated line) only when invoked, re-checks each
//       sheet so a repeat or a stale call adds no second line, touches only the sheets that need one, stops before
//       writing anything when a sheet cannot be given a line, and reports a failure without changing the sheet;
//   4 · the things a drag, a drop or a move can trigger (non-cut plan, ensurePlan, check, mounting the bar) add no line;
//   5 · after the press the bar shows a labelled spinner, then the drawn line on the sheet preview and a tick.
//   node tests/charm-nest/rose-flow.cjs   (jsdom: NODE_PATH=<node_modules with jsdom> when it is not installed here)
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), { JSDOM } = require('jsdom');
const root = path.join(__dirname, '../..');
const R = require(path.join(root, 'charm-nest-rose')), create = require(path.join(root, 'netlify/functions/_charmNestRoseStock')), Readiness = require(path.join(root, 'charm-nest-readiness'));

// ── the in-memory store and the real server handlers (as tests/charm-nest/rose-stock.cjs) ──
const store = new Map(), clone = x => structuredClone(x);
function ref(p) { return { path: p, id: p.split('/').at(-1), collection: n => query(p + '/' + n), get: async () => snap(p) }; }
function snap(p) { return { id: p.split('/').at(-1), ref: ref(p), exists: store.has(p), data: () => clone(store.get(p)) }; }
function query(p, filters = [], order = null, limit = Infinity, after = null) { return { doc: id => ref(p + '/' + id), where: (...f) => query(p, [...filters, f], order, limit, after), orderBy: (...o) => query(p, filters, o, limit, after), limit: n => query(p, filters, order, n, after), startAfter: n => query(p, filters, order, limit, n), get: async () => { let docs = [...store.keys()].filter(k => k.startsWith(p + '/') && !k.slice(p.length + 1).includes('/')).map(snap); docs = docs.filter(d => filters.every(([f, op, v]) => d.data()[f] === v)); if (order) docs.sort((a, b) => (a.data()[order[0]] - b.data()[order[0]]) * (order[1] === 'desc' ? -1 : 1)); if (after !== null) docs = docs.filter(d => d.data()[order[0]] < after); docs = docs.slice(0, limit); return { docs, size: docs.length }; } }; }
let serial = Promise.resolve();
const db = { runTransaction: fn => { const promise = serial.then(async () => { const writes = []; let wrote = false; const result = await fn({ get: async r => { assert(!wrote, 'reads before writes'); return r.get(); }, set: (r, v, o) => { wrote = true; writes.push(() => store.set(r.path, o?.merge ? { ...store.get(r.path), ...clone(v) } : clone(v))); }, update: (r, v) => { wrote = true; writes.push(() => store.set(r.path, { ...store.get(r.path), ...clone(v) })); }, delete: r => { wrote = true; writes.push(() => store.delete(r.path)); } }); writes.forEach(f => f()); return result; }); serial = promise.catch(() => {}); return promise; } };
const server = create({ db, col: query, FV: { serverTimestamp: () => 123456 }, Readiness });

// ── the page: real charm-nest-rose-ui.js and charm-nest-flow-rose.js over a fake CN ──
const dom = new JSDOM('<!doctype html><html><head></head><body><div id="host"></div></body></html>', { url: 'https://example.test', runScripts: 'outside-only', pretendToBeVisual: true });
const w = dom.window, doc = w.document;
{ const add = w.EventTarget.prototype.addEventListener; w.EventTarget.prototype.addEventListener = function (t, f, o) { if (t === 'click' && this.classList && this.classList.contains('lfrGo')) (this._clicks || (this._clicks = [])).push(f); return add.call(this, t, f, o); }; }   // (to hand the button's click handler an event a script cannot make: isTrusted)
const outline = { subpaths: [[['m', [0, 0]], ['l', [10, 0]], ['l', [10, 10]], ['l', [0, 10]], ['h']]]};
const outlineBig = { subpaths: [[['m', [0, 0]], ['l', [100, 0]], ['l', [100, 50]], ['l', [0, 50]], ['h']]]};
const calls = [], painted = [], toasts = [], WRITES = new Set(['roseClaim', 'rosePlan', 'roseRecordCut', 'roseRelease', 'roseTakeOff', 'putSheet', 'laserStatus', 'laserDone']);
const sheets = [];
let cloud = true;
w.CharmNestRose = R;
w.B = { sets: new Map() };
w.CN = { S: { cloud: { get ok() { return cloud; } }, settings: {}, mode: 'nest' }, esc: s => String(s).replace(/</g, '&lt;'), stockFor: () => ({ wPt: 100, hPt: 50 }), uid: () => 'u' + calls.length, allSheets: () => sheets,
  drawPreview() {}, renderCard() {}, toast(m, k) { toasts.push([m, k]); },
  paintPreview(cv, sh, clean, Rr) { painted.push({ id: sh.sheetId, clean, R: Rr, plan: sh.rosePlan ? sh.rosePlan.lines.length : 0, cut: !!sh.roseCutAt }); },
  api: async (name, b) => {
    calls.push({ op: b.op, sheetId: b.sheetId || b.id || null, cut: b.cut, by: b.by });
    if (b.op === 'listSheets') { if (w.failRead) throw new Error('offline'); return { sheets: w.records.filter(r => r.setId === b.setId) }; }
    if (b.op === 'getSheet') { if (w.failRead) throw new Error('offline'); return { sheet: w.records.find(r => r.id === b.id) || null }; }
    if (!server[b.op]) throw new Error('unexpected op ' + b.op);
    return server[b.op](b);
  } };
w.records = []; w.failRead = false;
w.eval(fs.readFileSync(path.join(root, 'charm-nest-readiness.js'), 'utf8'));
w.eval(fs.readFileSync(path.join(root, 'charm-nest-rose-ui.js'), 'utf8'));
w.eval(fs.readFileSync(path.join(root, 'charm-nest-flow-rose.js'), 'utf8'));
const LFR = w.LibraryFlowRose, RS = w.RoseStock;
assert(LFR && typeof LFR.check === 'function' && typeof LFR.calculate === 'function' && typeof LFR.confirmBar === 'function', 'window.LibraryFlowRose has the three calls');

// spies on the existing path: every call still goes through to the real function
const spy = { plan: [], record: [] };
const realPlan = RS.plan, realRecord = RS.record;
RS.plan = (sh, o) => { spy.plan.push({ id: sh.sheetId, cut: !!(o && o.cut) }); return realPlan(sh, o); };
RS.record = (sh, o) => { spy.record.push({ id: sh.sheetId, by: o && o.by }); return realRecord(sh, o); };

// ── fixtures ──
store.set('Charm_Nest_Runs/run-test', { lines: { a: { poolIds: ['pool-a', 'pool-b', 'pool-c', 'pool-d', 'pool-e', 'pool-f', 'pool-g', 'pool-h', 'pool-i', 'pool-j', 'pool-k'], spec: { engraveCandidate: false } } } });
const record = (id, pool, extra = {}) => ({ id, metal: 'rose', verification: { ok: true }, status: 'complete', dirty: false, saving: false, draft: false, setId: 'set-test', runId: 'run-test', poolIds: [pool], placedCount: 1, placements: [{ id: pool, cxPt: 20, cyPt: 20, angle: 0, scale: 1 }], outputs: { ai: { url: 'saved.ai' }, preview: { url: 'saved.png' } }, label: { files: [{ path: 'qr', url: 'qr.png', payload: 'test', orders: [] }] }, orders: [], sheetIndex: 1, ...extra });
// a live Rose Gold sheet as the Nest page holds it, and its saved copy
function live(id, pool, extra = {}, keepRecord = true) {
  const sh = { metal: 'rose', sheetId: id, sheetIndex: 1, runId: 'run-test', setId: 'set-test', draft: false, charms: [{ id: pool, outline, centerPt: [5, 5], members: [] }], placements: [{ id: pool, cxPt: 20, cyPt: 20, angle: 0, scale: 1 }], persistedDone: true, verification: { ok: true }, status: 'complete', dirty: false, outputs: {}, ...extra };
  if (keepRecord) store.set('Charm_Nest_Sheets/' + id, record(id, pool, { sheetIndex: sh.sheetIndex }));
  sheets.push(sh); return sh;
}
const data = sh => JSON.stringify(sh, (k, v) => (k.startsWith('_') || k === 'el') ? undefined : v);
const writeCalls = () => calls.filter(c => WRITES.has(c.op));
const stagesOf = id => (JSON.parse(store.get('Charm_Nest_Sheets/' + id).rosePlanJson || 'null') || {}).stages || [];
const click = (el, o = {}) => el.dispatchEvent(new w.MouseEvent('click', { bubbles: true, cancelable: true, ...o }));
const press = el => { el.dispatchEvent(new w.MouseEvent('pointerdown', { bubbles: true, button: 0 })); el.dispatchEvent(new w.MouseEvent('pointerup', { bubbles: true, button: 0 })); click(el, { detail: 1 }); };
// values made by the page live in the jsdom realm: compared by content
const J = x => JSON.parse(JSON.stringify(x)), deq = (a, b, m) => assert.deepEqual(J(a), J(b), m);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const buttons = bar => ({ go: bar.querySelector('.lfrGo'), no: bar.querySelector('.lfrNo') });

(async () => {
  // a sheet of every kind
  const A = live('rg-sheet-a', 'pool-a');                                               // uncut, no line
  const CUT = live('rg-sheet-cut', 'pool-b', { roseCutAt: 1760000000000, sheetIndex: 2 });   // cut: its green dash line is recorded
  const GOLD = live('gf-sheet-1', 'pool-c', { metal: 'gold', sheetIndex: 3 });             // not Rose Gold
  const LINED = live('rg-sheet-lined', 'pool-d', { sheetIndex: 4 });                       // has its line already
  LINED.rosePlan = { ...R.plan(R.shapes(LINED.charms, LINED.placements), 100, 50, null, .2), stages: [{ n: 1, at: 1760000000000, ids: ['pool-d'], lines: [0, 1] }] };
  const FULL = live('rg-sheet-full', 'pool-e', { sheetIndex: 5, charms: [{ id: 'pool-e', outline: outlineBig, centerPt: [50, 25], members: [] }], placements: [{ id: 'pool-e', cxPt: 50, cyPt: 25, angle: 0, scale: 1 }] }, false);   // fills the sheet
  w.B.sets.set('r|all', { setId: 'set-test', sheetIds: sheets.map(s => s.sheetId) });
  const before = new Map(sheets.map(s => [s, data(s)]));
  const unchanged = what => { for (const [s, d] of before) assert.equal(data(s), d, `${what}: ${s.sheetId} is exactly as it was`); assert.equal(writeCalls().length, 0, `${what}: no write call`); assert.equal(spy.plan.length + spy.record.length, 0, `${what}: no plan or cut was started`); };

  // ── 1 · check: read only ──
  let r = await LFR.check({ kind: 'sheet', id: 'rg-sheet-a' });
  assert.equal(r.needsLine, true, 'a Rose Gold sheet with no green dash line needs one');
  assert.equal(r.sheets.length, 1); assert.equal(r.sheets[0].needsLine, true); assert.equal(r.sheets[0].sheetId, 'rg-sheet-a'); assert.equal(r.sheets[0].label, 'RG Sheet 1'); assert.match(r.sheets[0].why, /No green dash line yet/);
  assert.equal(r.confirm.key, 'roseLine'); assert.equal(r.confirm.label, 'Add the green dash line to RG Sheet 1?'); assert.match(r.confirm.detail, /calculates the cut contour for 1 charm/); assert.match(r.confirm.detail, /Nothing is added until you press/);
  for (const [id, why] of [['rg-sheet-cut', /Already cut/], ['gf-sheet-1', /Not a Rose Gold sheet/], ['rg-sheet-lined', /Already has its green dash line/]]) {
    r = await LFR.check({ kind: 'sheet', id });
    assert.equal(r.needsLine, false, id + ' needs no line'); assert.equal(r.confirm, null); assert.equal(r.sheets[0].needsLine, false); assert.match(r.sheets[0].why, why);
  }
  r = await LFR.check(FULL); assert.equal(r.needsLine, false, 'a full sheet is cut whole: no green line to add'); assert.match(r.sheets[0].why, /full/);
  r = await LFR.check(A); assert.equal(r.needsLine, true, 'a live sheet object is accepted as the item');
  r = await LFR.check({ kind: 'set', id: 'set-test' });
  assert.equal(r.needsLine, true); deq(r.sheets.map(s => s.sheetId).sort(), ['rg-sheet-a', 'rg-sheet-cut', 'rg-sheet-full', 'rg-sheet-lined'].sort(), 'a set lists its Rose Gold sheets');
  deq(r.sheets.filter(s => s.needsLine).map(s => s.sheetId), ['rg-sheet-a'], 'only the uncut sheet without a line needs one');
  assert.equal(r.confirm.label, 'Add the green dash line to RG Sheet 1?');
  assert.equal(calls.length, 0, 'the sheets were on the page: check read nothing from the cloud');
  unchanged('check');
  // a sheet that has a line but charms nested past it needs the next line
  const NEWER = live('rg-sheet-newer', 'pool-f', { sheetIndex: 6, charms: [{ id: 'pool-f', outline, centerPt: [5, 5], members: [] }, { id: 'pool-x', outline, centerPt: [5, 5], members: [] }], placements: [{ id: 'pool-f', cxPt: 20, cyPt: 20, angle: 0, scale: 1 }, { id: 'pool-x', cxPt: 40, cyPt: 20, angle: 0, scale: 1 }] }, false);
  NEWER.rosePlan = { ...R.plan(R.shapes([NEWER.charms[0]], [NEWER.placements[0]]), 100, 50, null, .2), stages: [{ n: 1, at: 1760000000000, ids: ['pool-f'], lines: [0, 1] }] };
  r = await LFR.check(NEWER); assert.equal(r.needsLine, true); assert.match(r.sheets[0].why, /past its last green dash line/);
  sheets.splice(sheets.indexOf(NEWER), 1);
  // saved records only (the Library's copy): a line is told by its plan, and a cut or completed sheet by its marks
  const rec = x => ({ id: 'rec-1', metal: 'rose', sheetIndex: 3, setId: 'set-far', placedCount: 2, ...x });
  assert.equal((await LFR.check({ kind: 'sheet', id: 'rec-1', sheet: rec({}) })).needsLine, true, 'a saved record without a plan needs a line');
  assert.equal((await LFR.check({ kind: 'sheet', id: 'rec-1', sheet: rec({ rosePlanHash: 'h' }) })).needsLine, false, 'a saved record with its plan does not');
  assert.equal((await LFR.check({ kind: 'sheet', id: 'rec-1', sheet: rec({ roseCutAt: 5 }) })).needsLine, false, 'a cut record does not');
  assert.equal((await LFR.check({ kind: 'sheet', id: 'rec-1', sheet: rec({ laserDoneAt: 5 }) })).needsLine, false, 'a laser-completed record does not');
  assert.equal((await LFR.check({ kind: 'sheet', id: 'rec-2', sheet: { ...rec({}), id: 'rec-2', metal: 'silver' } })).needsLine, false, 'a record that is not Rose Gold does not');
  // a set that is only saved: its records are read (listSheets, a read), never written; unreadable is flagged, not guessed
  w.records = [rec({ id: 'far-1', setId: 'set-far' }), rec({ id: 'far-2', setId: 'set-far', rosePlanHash: 'h', sheetIndex: 4 })];
  w.B.sets.set('r2|all', { setId: 'set-far', sheetIds: ['far-1', 'far-2'] });
  r = await LFR.check({ kind: 'set', id: 'set-far' });
  assert.equal(r.needsLine, true); deq(r.sheets.filter(s => s.needsLine).map(s => s.sheetId), ['far-1']); assert.equal(r.unknown, false);
  deq([...new Set(calls.map(c => c.op))], ['listSheets'], 'only a read'); calls.length = 0;
  w.failRead = true; r = await LFR.check({ kind: 'set', id: 'set-far' });
  assert.equal(r.needsLine, false); assert.equal(r.unknown, true); assert(r.sheets.every(s => s.unknown && !s.needsLine), 'an unreadable sheet is flagged unknown'); assert.equal(r.confirm, null);
  r = await LFR.check({ kind: 'sheet', id: 'nowhere-1' }); assert.equal(r.unknown, true); assert.equal(r.needsLine, false);
  w.failRead = false; calls.length = 0;
  unchanged('check of saved records');

  // ── 2 · confirmBar: mounting writes nothing; the press is explicit ──
  const host = doc.getElementById('host');
  let confirmed = 0, cancelled = 0, handed = null;
  const mount = (o = {}) => LFR.confirmBar(host, { sheetLabel: 'RG Sheet 1', onConfirm: c => { confirmed++; handed = c; }, onCancel: () => { cancelled++; }, ...o });
  let bar = mount({ armMs: 400, item: { kind: 'sheet', id: 'rg-sheet-a' } });
  assert(bar && bar.el && host.contains(bar.el), 'the bar sits inline in its host');
  let { go, no } = buttons(bar.el);
  assert.equal(go.textContent, 'Add the green dash line'); assert.equal(no.textContent, 'Not now');
  assert.match(bar.el.textContent, /RG Sheet 1 has no green dash line yet/); assert.match(bar.el.textContent, /never adds one: only the button below does/);
  assert.equal(go.getAttribute('aria-disabled'), 'true', 'the button wakes after a moment');
  assert.equal(doc.activeElement, no, 'the safe button holds the focus: an Enter right after a drop cancels, never adds');
  assert.equal(confirmed + cancelled, 0, 'mounting fires nothing'); unchanged('mounting the bar');
  assert(painted.length >= 1 && painted[0].clean === true && painted[0].R === 0 && painted[0].id === 'rg-sheet-a', 'the sheet preview is drawn with the page painter');
  assert(!bar.el.querySelector('.lfrPv').hidden, 'the preview is shown'); assert(!bar.el.querySelector('.lfrPv').classList.contains('drawn'), 'no line is shown before the press');
  press(go); assert.equal(confirmed, 0, 'a press before the bar is armed does nothing');
  await sleep(450); assert.equal(go.getAttribute('aria-disabled'), null);
  click(go); assert.equal(confirmed, 0, 'a scripted click nobody pressed does nothing');
  click(go, { detail: 1 }); assert.equal(confirmed, 0, 'a click with no press on the button (a ghost click after a touch drag) does nothing');
  go.dispatchEvent(new w.MouseEvent('pointerdown', { bubbles: true, button: 0 })); go.dispatchEvent(new w.MouseEvent('pointerleave')); click(go, { detail: 1 }); assert.equal(confirmed, 0, 'a press that left the button is not a press');
  assert.equal(bar.state, 'ask');
  press(go);
  assert.equal(confirmed, 1, 'an explicit press fires onConfirm'); assert.equal(handed, bar, 'onConfirm is handed the bar to feed its steps');
  assert.equal(bar.state, 'working'); assert(bar.el.querySelector('.lfrStatus .lfrSpin'), 'a spinner shows while it works'); assert.match(bar.el.querySelector('.lfrStatus').textContent, /Working out the green dash line for RG Sheet 1/);
  assert(bar.el.querySelector('.lfrActions').hidden, 'no second press while it works'); press(go); click(go, { detail: 1 }); assert.equal(confirmed, 1, 'onConfirm fires once');
  assert.equal(cancelled, 0); unchanged('pressing the bar (the caller has not calculated)');
  bar.destroy(); assert(!host.contains(bar.el), 'destroy removes the bar without cancelling'); assert.equal(cancelled, 0);

  // Not now: leaves everything unchanged
  confirmed = cancelled = 0; bar = mount({ armMs: 0 }); ({ go, no } = buttons(bar.el));
  assert.equal(go.getAttribute('aria-disabled'), null, 'armMs 0: ready at once');
  click(no, { detail: 1 }); assert.equal(cancelled, 1, 'Not now fires onCancel'); assert.equal(confirmed, 0); assert(!host.contains(bar.el), 'the bar goes'); assert.equal(host.children.length, 0);
  click(no); assert.equal(cancelled, 1, 'once');
  unchanged('Not now');
  // Esc
  bar = mount({ armMs: 0 }); doc.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
  assert.equal(cancelled, 2, 'Esc fires onCancel'); assert.equal(confirmed, 0); assert.equal(host.children.length, 0); unchanged('Esc');
  doc.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'Escape', bubbles: true })); assert.equal(cancelled, 2, 'a bar that is gone ignores Esc');
  // Esc is for a bar that is waiting for an answer, not one that is working
  bar = mount({ armMs: 0 }); press(buttons(bar.el).go); doc.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
  assert.equal(cancelled, 2); assert(host.contains(bar.el), 'Esc does not abandon a calculation under way'); bar.destroy();
  // a keyboard press: Enter on the button, as the browser clicks it
  confirmed = 0; bar = mount({ armMs: 0 }); ({ go } = buttons(bar.el));
  go.dispatchEvent(new w.KeyboardEvent('keydown', { key: 'Enter', bubbles: true })); click(go, { detail: 0 }); assert.equal(confirmed, 1, 'Enter on the button is a press'); bar.destroy();
  // a trusted click with no pointer (a screen reader's activate) is a press: only a real browser makes one, so it is checked on a stand-in
  confirmed = 0; bar = mount({ armMs: 0 }); ({ go } = buttons(bar.el));
  go._clicks[0]({ isTrusted: true, detail: 0, preventDefault() {} });
  assert.equal(confirmed, 1, 'a trusted activation without a pointer (a screen reader) is a press'); bar.destroy();
  confirmed = 0; bar = mount({ armMs: 0 }); ({ go } = buttons(bar.el));
  go._clicks[0]({ isTrusted: true, detail: 1, preventDefault() {} }); assert.equal(confirmed, 0, 'a trusted click with a pointer position but no press on the button (a ghost click) is not a press');
  go._clicks[0]({ isTrusted: false, detail: 0, preventDefault() {} }); assert.equal(confirmed, 0, 'a script\'s click is not a press'); bar.destroy();
  // a second bar in the same host replaces the first, silently
  const b1 = mount({ armMs: 0 }), b2 = mount({ armMs: 0 }); assert.equal(host.querySelectorAll('.lfrBar').length, 1); assert(!host.contains(b1.el) && host.contains(b2.el)); b2.destroy();
  // plurals for a set
  bar = mount({ sheetLabel: 'RG Sheet 1 and RG Sheet 2', armMs: 0 }); assert.match(bar.el.textContent, /RG Sheet 1 and RG Sheet 2 have no green dash line yet/); assert.match(bar.el.textContent, /Moving or dropping them never adds one/); bar.destroy();
  // a failure answer: red, nothing more changed, and a way to press again
  confirmed = 0; bar = mount({ armMs: 0 }); ({ go } = buttons(bar.el)); press(go); bar.fail(new Error('The layout changed'));
  assert.equal(bar.state, 'failed'); assert.match(bar.el.querySelector('[role="alert"]').textContent, /The layout changed\. Nothing more was changed/); assert.equal(go.textContent, 'Try again');
  press(go); assert.equal(confirmed, 2, 'after a failure the person may press again'); bar.destroy();
  // onConfirm that throws, and one that rejects
  bar = LFR.confirmBar(host, { sheetLabel: 'RG Sheet 1', armMs: 0, onConfirm() { throw new Error('boom'); } }); press(buttons(bar.el).go); assert.equal(bar.state, 'failed'); assert.match(bar.el.textContent, /boom/); bar.destroy();
  bar = LFR.confirmBar(host, { sheetLabel: 'RG Sheet 1', armMs: 0, onConfirm: () => Promise.reject(new Error('no luck')) }); press(buttons(bar.el).go); await sleep(10); assert.equal(bar.state, 'failed'); assert.match(bar.el.textContent, /no luck/); bar.destroy();
  unchanged('every bar so far');

  // ── 4 · what a drag, a drop or a move can trigger adds no line ──
  await RS.plan(A); await RS.ensurePlan(A); await RS.plan(CUT);
  r = await LFR.check({ kind: 'set', id: 'set-test' }); bar = mount({ armMs: 0 }); bar.destroy();
  assert.equal(spy.plan.filter(p => p.cut).length, 0, 'nothing asked for a line');
  assert(!A.rosePlan && !stagesOf('rg-sheet-a').length, 'the sheet still has no green dash line');
  assert.equal(writeCalls().filter(c => c.op === 'rosePlan').length, 0, 'the server was never asked to plan'); assert.equal(r.needsLine, true, 'and it still needs one');
  spy.plan.length = 0; calls.length = 0;

  // ── 3 · calculate: only when invoked, the existing path ──
  const steps = [];
  const out = await LFR.calculate({ kind: 'sheet', id: 'rg-sheet-a' }, { by: 'Test Operator', onStep: s => steps.push(s) });
  assert.equal(out.ok, true); assert.equal(out.error, null); assert.equal(out.lines, 1); assert.equal(out.cut, false, 'the cut is left for Cut Sheet');
  deq(steps.map(s => s.key), ['calculating', 'drawn', 'saved']); assert.match(steps[0].label, /Working out the green dash line for RG Sheet 1/); assert.match(steps[1].label, /Green dash line drawn on RG Sheet 1/); assert.match(steps[2].label, /Saved as green line 1/);
  assert(steps.every(s => s.sheetId === 'rg-sheet-a' && s.sheetLabel === 'RG Sheet 1' && s.count === 1 && s.index === 1));
  deq(spy.plan, [{ id: 'rg-sheet-a', cut: true }], 'the Cut Sheet button\'s own contour call, with cut:true'); assert.equal(spy.record.length, 0);
  assert.equal(calls.filter(c => c.op === 'rosePlan').length, 1); assert.equal(calls.find(c => c.op === 'rosePlan').cut, true); assert.equal(calls.filter(c => c.op === 'roseRecordCut').length, 0);
  assert(A.rosePlan && A.rosePlan.lines.length, 'the green dash line is drawn on the sheet'); assert.equal(A.rosePlan.stages.length, 1); assert(Number.isFinite(A.rosePlan.stages[0].at), 'and dated');
  assert.equal(stagesOf('rg-sheet-a').length, 1, 'and saved on the server with its date'); deq(stagesOf('rg-sheet-a')[0].ids, ['pool-a']); assert(!A.roseCutAt, 'no cut was recorded'); assert.equal(A._roseAction, false);
  assert.equal(out.sheets[0].lineAdded, true); assert.equal(out.sheets[0].at, A.rosePlan.stages[0].at);
  // a repeat, or a stale confirm, adds nothing: the sheet is checked again right before it is touched
  calls.length = 0; spy.plan.length = 0;
  const again = await LFR.calculate({ kind: 'sheet', id: 'rg-sheet-a' }, { by: 'Test Operator' });
  assert.equal(again.ok, true); assert.equal(again.lines, 0); assert.equal(calls.length, 0); assert.equal(stagesOf('rg-sheet-a').length, 1, 'no second line');
  assert.equal((await LFR.check(A)).needsLine, false, 'and check agrees');

  // a set: only the sheets that need a line are touched; the cut, the gold and the lined one are left exactly as they were
  const S1 = live('rg-set-1', 'pool-g', { setId: 'set-two', sheetIndex: 1 }), S2 = live('rg-set-2', 'pool-h', { setId: 'set-two', sheetIndex: 2 });
  const S3 = live('rg-set-3', 'pool-i', { setId: 'set-two', sheetIndex: 3, roseCutAt: 1760000000000 }), S4 = live('gf-set-4', 'pool-j', { setId: 'set-two', metal: 'gold', sheetIndex: 4 });
  w.B.sets.set('r3|all', { setId: 'set-two', sheetIds: ['rg-set-1', 'rg-set-2', 'rg-set-3', 'gf-set-4'] });
  const snapshot = new Map([S3, S4, LINED, CUT, GOLD].map(s => [s, data(s)]));
  r = await LFR.check({ kind: 'set', id: 'set-two' }); assert.equal(r.confirm.label, 'Add the green dash line to RG Sheet 1 and RG Sheet 2?'); assert.equal(r.confirm.count, 2); assert.match(r.confirm.detail, /for 2 charms/);
  calls.length = 0; spy.plan.length = 0; steps.length = 0;
  const setOut = await LFR.calculate({ kind: 'set', id: 'set-two' }, { by: 'Test Operator', onStep: s => steps.push(s) });
  assert.equal(setOut.ok, true); assert.equal(setOut.lines, 2); deq(setOut.sheets.map(s => s.sheetId), ['rg-set-1', 'rg-set-2']);
  deq(spy.plan.map(p => p.id), ['rg-set-1', 'rg-set-2']); assert(spy.plan.every(p => p.cut));
  deq(steps.map(s => s.key + ':' + s.index + '/' + s.count), ['calculating:1/2', 'drawn:1/2', 'saved:1/2', 'calculating:2/2', 'drawn:2/2', 'saved:2/2']);
  assert(!calls.some(c => ['rg-set-3', 'gf-set-4', 'rg-sheet-lined', 'rg-sheet-cut'].includes(c.sheetId)), 'no call touched a cut, gold or lined sheet');
  for (const [s, d] of snapshot) assert.equal(data(s), d, s.sheetId + ' is exactly as it was');
  assert.equal(stagesOf('rg-set-1').length, 1); assert.equal(stagesOf('rg-set-2').length, 1);

  // the whole Cut Sheet press, when asked for: the cut is recorded by the person who pressed
  const P = live('rg-press', 'pool-k', { sheetIndex: 7 }); calls.length = 0; spy.plan.length = 0; steps.length = 0;
  const full = await LFR.calculate(P, { by: 'Pat Lee', recordCut: true, onStep: s => steps.push(s) });
  assert.equal(full.ok, true); assert.equal(full.cut, true); assert.equal(full.lines, 1);
  deq(spy.record, [{ id: 'rg-press', by: 'Pat Lee' }], 'Cut Sheet\'s own press, with who pressed'); assert(P.roseCutAt, 'the cut is recorded');
  assert.equal(store.get('Charm_Nest_Sheets/rg-press').roseCutAt, P.roseCutAt); assert(calls.some(c => c.op === 'roseRecordCut'));
  deq(steps.map(s => s.key), ['calculating', 'drawn', 'saved']); assert.equal(stagesOf('rg-press').length, 1);
  const cutRow = store.get(`Charm_Nest_Rose_Stock/${P.roseStock.id}/cuts/rg-press`); assert.equal(cutRow.by, 'Pat Lee', 'the ledger names the person who pressed');

  // nothing is touched until every sheet can be given a line, and a failure changes nothing
  const N1 = live('rg-bad-1', 'pool-a', { setId: 'set-bad', sheetIndex: 1 }), N2 = live('rg-bad-2', 'pool-b', { setId: 'set-bad', sheetIndex: 2, dirty: true });
  w.B.sets.set('r4|all', { setId: 'set-bad', sheetIds: ['rg-bad-1', 'rg-bad-2'] });
  calls.length = 0; spy.plan.length = 0;
  let bad = await LFR.calculate({ kind: 'set', id: 'set-bad' }, {});
  assert.equal(bad.ok, false); assert.match(bad.error, /RG Sheet 2: Its layout is still being nested or saved/); assert.equal(bad.lines, 0);
  assert.equal(calls.length, 0, 'a sheet that is not ready stops the whole press before anything is written'); assert(!N1.rosePlan);
  N2.dirty = false; N2.sheetId = 'rg-bad-2';
  w.records = [rec({ id: 'rec-only', setId: 'set-recs' })];
  w.B.sets.set('r5|all', { setId: 'set-recs', sheetIds: ['rec-only'] });
  bad = await LFR.calculate({ kind: 'set', id: 'set-recs' }, {}); assert.equal(bad.ok, false); assert.match(bad.error, /not open on this page/); assert.match(bad.error, /Nest tab/);
  assert.equal(writeCalls().length, 0, 'a sheet that is only a saved record cannot be given a line here, and nothing is written'); assert(!spy.plan.length);
  cloud = false; calls.length = 0; bad = await LFR.calculate({ kind: 'sheet', id: 'rg-bad-1' }, {}); cloud = true;
  assert.equal(bad.ok, false); assert.match(bad.error, /Reconnect to the cloud/); assert.equal(calls.length, 0); assert(!N1.rosePlan);
  // the server refuses: the sheet's layout changed after it was saved
  N1.placements = [{ id: 'pool-a', cxPt: 30, cyPt: 20, angle: 0, scale: 1 }];
  calls.length = 0; spy.plan.length = 0; steps.length = 0;
  bad = await LFR.calculate({ kind: 'sheet', id: 'rg-bad-1' }, { onStep: s => steps.push(s) });
  assert.equal(bad.ok, false); assert.match(bad.error, /RG Sheet 1: .*layout changed/i); assert.equal(bad.lines, 0); deq(steps.map(s => s.key), ['calculating']);
  assert(!N1.rosePlan, 'no green line on the sheet'); assert.equal(stagesOf('rg-bad-1').length, 0, 'and none saved'); assert.match(N1._roseError, /layout changed/i, 'the failure shows once, under the sheet\'s Cut Sheet button'); assert.equal(N1._roseAction, false);
  // two presses at once add one line
  const Q = live('rg-twice', 'pool-c', { sheetIndex: 8 }); calls.length = 0; spy.plan.length = 0;
  const [q1, q2] = await Promise.all([LFR.calculate({ kind: 'sheet', id: 'rg-twice' }, {}), LFR.calculate({ kind: 'sheet', id: 'rg-twice' }, {})]);
  assert.equal(q1.ok && q2.ok, true); assert.equal(q1.lines + q2.lines, 1, 'one line between the two presses'); assert.equal(calls.filter(c => c.op === 'rosePlan').length, 1); assert.equal(stagesOf('rg-twice').length, 1); assert.equal(Q.rosePlan.stages.length, 1);

  // ── 5 · the bar and calculate together: spinner, the drawn line on the preview, a tick ──
  const V = live('rg-visual', 'pool-d', { sheetIndex: 9 }); painted.length = 0; calls.length = 0; spy.plan.length = 0;
  const seen = [];
  bar = LFR.confirmBar(host, { sheetLabel: 'RG Sheet 9', armMs: 0, item: { kind: 'sheet', id: 'rg-visual' }, onConfirm: ctl => LFR.calculate({ kind: 'sheet', id: 'rg-visual' }, { by: 'Test Operator', onStep: s => { seen.push(bar.el.querySelector('.lfrStatus').textContent + '|' + bar.el.querySelector('.lfrPv').className); ctl.onStep(s); } }) });
  assert.equal(painted.at(-1).plan, 0, 'before the press the preview has no green line'); const pv = bar.el.querySelector('.lfrPv');
  press(buttons(bar.el).go); await sleep(1200);
  assert.equal(bar.state, 'done'); assert.match(bar.el.querySelector('.lfrStatus').textContent, /Saved as green line 1/); assert(bar.el.querySelector('.lfrStatus .lfrTick'), 'a tick says it is saved');
  assert.match(bar.el.querySelector('.lfrTitle').textContent, /^RG Sheet 9 now has its green dash line$/, 'the bar says what is now true'); assert(bar.el.querySelector('.lfrText').hidden, 'and drops its explanation');
  assert(pv.classList.contains('drawn'), 'the drawn green dash line appears on the sheet preview'); assert(painted.some(p => p.id === 'rg-visual' && p.plan > 0), 'the preview was painted again with the line in it');
  assert.equal(spy.plan.length, 1); assert.equal(stagesOf('rg-visual').length, 1);
  bar.destroy();
  // the bar also follows a calculation the caller starts itself (the caller that only hands onConfirm a plain function)
  const W2 = live('rg-visual-2', 'pool-e', { sheetIndex: 10, charms: [{ id: 'pool-e', outline, centerPt: [5, 5], members: [] }], placements: [{ id: 'pool-e', cxPt: 20, cyPt: 20, angle: 0, scale: 1 }] });
  sheets.splice(sheets.indexOf(FULL), 1); store.set('Charm_Nest_Sheets/rg-visual-2', record('rg-visual-2', 'pool-e', { sheetIndex: 10 }));
  bar = LFR.confirmBar(host, { sheetLabel: 'RG Sheet 10', armMs: 0, onConfirm: () => { LFR.calculate({ kind: 'sheet', id: 'rg-visual-2' }, { by: 'Test Operator' }); } });
  press(buttons(bar.el).go); assert.equal(bar.state, 'working'); await sleep(1200);
  assert.equal(bar.state, 'done', 'a pressed bar follows the calculation that runs for its sheet'); assert.match(bar.el.textContent, /Saved as green line 1/); assert.equal(stagesOf('rg-visual-2').length, 1);
  bar.destroy();
  // nothing to add (a stale press): said plainly
  bar = mount({ armMs: 0 }); press(buttons(bar.el).go); bar.finish({ ok: true, lines: 0, sheets: [] }); assert.match(bar.el.querySelector('.lfrStatus').textContent, /Nothing to add: it already has its green dash line/); assert.match(bar.el.querySelector('.lfrTitle').textContent, /already has its green dash line/); bar.destroy();
  // two bars waiting: each follows its own sheet only (RG Sheet 1 is not RG Sheet 11)
  const host2 = doc.createElement('div'); doc.body.append(host2);
  const first = LFR.confirmBar(host, { sheetLabel: 'RG Sheet 1', armMs: 0, onConfirm() {} }), second = LFR.confirmBar(host2, { sheetLabel: 'Add the green dash line to RG Sheet 11?', armMs: 0, onConfirm() {} });
  assert.match(second.el.textContent, /^\s*RG Sheet 11 has no green dash line yet/, 'a label that came as a question is shown as a name'); press(buttons(first.el).go); press(buttons(second.el).go);
  const X = live('rg-visual-3', 'pool-f', { sheetIndex: 11, charms: [{ id: 'pool-f', outline, centerPt: [5, 5], members: [] }], placements: [{ id: 'pool-f', cxPt: 20, cyPt: 20, angle: 0, scale: 1 }] });
  await LFR.calculate({ kind: 'sheet', id: 'rg-visual-3' }, {}); await sleep(1100);
  assert.equal(first.state, 'working', 'a bar for RG Sheet 1 is not moved by RG Sheet 11'); assert.equal(second.state, 'done', 'the bar for RG Sheet 11 follows it'); first.destroy(); second.destroy();
  // a bar its owner cleared away (hide, a re-render) lets go: it neither follows later steps nor makes another bar share them
  const gone1 = LFR.confirmBar(host, { sheetLabel: 'RG Sheet 40', armMs: 0, onConfirm() {} }); press(buttons(gone1.el).go); host.innerHTML = '';
  const live1 = LFR.confirmBar(host2, { sheetLabel: 'Whatever the caller called it', armMs: 0, onConfirm() {} }); press(buttons(live1.el).go);
  live('rg-visual-4', 'pool-g', { sheetIndex: 12 }); await LFR.calculate({ kind: 'sheet', id: 'rg-visual-4' }, {}); await sleep(1100);
  assert.equal(live1.state, 'done', 'the one bar waiting takes the steps whatever label it was given'); assert.equal(gone1.state, 'working', 'a cleared bar stays as it was'); live1.destroy(); host2.innerHTML = '';
  // a touch tap: the pointer leaves after it lifts, and the click comes after that
  confirmed = 0; bar = mount({ armMs: 0 }); ({ go } = buttons(bar.el));
  const touch = (type, extra = {}) => { const e = new w.MouseEvent(type, { bubbles: true, button: 0, ...extra }); Object.defineProperty(e, 'pointerType', { value: 'touch' }); go.dispatchEvent(e); };
  touch('pointerdown'); touch('pointerup'); touch('pointerleave'); click(go, { detail: 1 }); assert.equal(confirmed, 1, 'a touch tap is a press'); bar.destroy();
  confirmed = 0; bar = mount({ armMs: 0 }); ({ go } = buttons(bar.el)); touch('pointerdown'); touch('pointercancel'); click(go, { detail: 1 }); assert.equal(confirmed, 0, 'a touch the browser took over (a scroll) is not a press'); bar.destroy();
  // a bar with a refusal shows it
  const refused = LFR.confirmBar(host, { sheetLabel: 'RG Sheet 1', armMs: 0, onConfirm() {} }); press(buttons(refused.el).go);
  await LFR.calculate({ kind: 'sheet', id: 'rg-bad-1' }, {}); assert.equal(refused.state, 'failed'); assert.match(refused.el.textContent, /Nothing more was changed/); refused.destroy();

  assert.equal(host.children.length, 0, 'no bar is left behind');
  deq(toasts.filter(t => t[1] === 'bad'), [], 'no pop-up repeats a failure');
  console.log('Rose flow OK: check reads only (uncut RG needs a line; cut, gold, lined and full do not), the bar fires only from an explicit press, Not now and Esc change nothing, calculate adds one dated line only when invoked and never twice, and a drop, move or process step adds none');
  dom.window.close();
})().catch(e => { console.error(e); dom.window.close(); process.exit(1); });
