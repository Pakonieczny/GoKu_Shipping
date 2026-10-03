/* A Library with hundreds of sheets, sets and open orders stays quick (Paul, 3 Oct: "the Library felt slow with many sheets").
   The real LaserReview, set card code and charm-nest-library.js in a DOM, offline fixtures only (no cloud, no live set).
   What is counted, not timed (timings differ from machine to machine): a refresh frame, the Library's drawing and the card
   decorations work out what the order rows say about their pieces once, and never search the whole list for a card.
   Beside it, the answers are the ones they always were: one pass (LaserReview.batch) reads what a call would read, a change
   is seen at once outside a pass, and the completion marks sit on the same cards as when each card asked on its own. */
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), { JSDOM } = require('jsdom');
const root = path.join(__dirname, '../..');
const tick = (n = 60) => new Promise(r => setTimeout(r, n));
const SETS = 30, PER = 3, ORDER_PIECES = 6;                                   // 90 sheets in 30 sets, 540 order rows (a count, not a clock: this is enough to see one read per card)
const back = (id, sheetId) => ({ poolId: id, sheetId, approvedAt: 10, approvedBy: 'Paul', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: id + '.ai', url: 'https://example.com/' + id + '.ai' } } });
function sheet(k, i) {
  const id = `s${k}x${i}`, pool = Array.from({ length: ORDER_PIECES }, (_, n) => `${4100000000 + (k * PER + i) * ORDER_PIECES + n}_t${n}_1`), orders = pool.map(p => p.split('_')[0]);
  const ready = k % 2 === 0;                                                  // even sets are ready for the laser, odd ones wait for their engravings
  return { id, metal: 'gold', metalLabel: 'GF 14/20', setId: 'set-' + k, setSeq: k + 1, runId: 'run1', sheetIndex: i + 1, status: 'complete', poolIds: pool, placedCount: pool.length, charmCount: pool.length, density: .7,
    verification: { ok: true }, preview: 'https://example.com/p.png', outputs: { ai: 'https://example.com/f.ai' }, orders, label: { files: [{ path: 'qr.png', url: 'https://example.com/qr.png', payload: 'x', orders }] },
    backPool: ready ? pool.map(p => back(p, id)) : [], engraving: {}, orderReadiness: Object.fromEntries(orders.map(o => [o, { ready: true }])), updatedAt: 1, day: '2026-10-03', folder: `GF_Oct.03.26_Set-${k + 1}_Sheet-${i + 1}` };
}
(async () => {
  const dom = new JSDOM('<div id="stage"><div id="libView"><div id="libTab"><button data-t="current"></button><button data-t="done"></button></div><input id="libSearch"><span id="libDoneCount"></span><div id="libBody"></div><div id="libDone"></div></div></div>', { url: 'https://example.test/#library', runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window, d = w.document;
  w.matchMedia = () => ({ matches: true }); w.CSS = { escape: x => x }; w.HTMLCanvasElement.prototype.getContext = () => ({ measureText: t => ({ width: t.length * 8 }) });
  const sheets = [], rawSets = [];
  for (let k = 0; k < SETS; k++) { const mine = Array.from({ length: PER }, (_, i) => sheet(k, i)); sheets.push(...mine); rawSets.push({ setId: 'set-' + k, seq: k + 1, day: '2026-10-03', runId: 'run1', sheetIds: mine.map(s => s.id), materials: ['gold'], status: 'labelled', updatedAt: 1 }); }
  // the order rows: one per piece, what each says about its engraving is what the sheet's readiness reads
  const rows = []; for (const s of sheets) for (const p of s.poolIds) { const ok = Number(s.setId.slice(4)) % 2 === 0; rows.push({ key: p.replace(/_1$/, ''), order: { receiptId: p.split('_')[0], buyer: { name: 'Buyer' } }, state: 'written', poolIds: [p], engrave: ok ? { needed: true, state: 'approved', approved: true } : { needed: true, state: 'words', approved: false } }); }
  const jobs = new Map(); sheets.filter(s => Number(s.setId.slice(4)) % 2 === 1).slice(0, 150).forEach((s, n) => jobs.set('job' + n, { key: 'job' + n, copies: [s.poolIds[0]], state: 'words', backs: [] }));
  const api = async (_, b) => b.op === 'setList' ? { sets: rawSets, sheets: [] } : b.op === 'listSheets' ? { sheets } : { sheets: [], sets: [], counts: {} };
  Object.assign(w, { S: { mode: 'library', library: { rows: sheets, kind: 'sets', metal: 'all' }, cloud: { ok: true } }, api, allSheets: () => [], Orders: { rows: () => rows }, Engrave: { items: () => jobs, backsMarkup: () => '' },
    esc: x => String(x ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])), el: (tag, cls) => { const e = d.createElement(tag); e.className = cls; return e; },
    Gate: { projectLibraryRecords: x => x }, RoseStock: {}, CODE: { gold: 'GF', silver: 'SS' }, toast: () => null, CNEmployee: { name: () => 'Tester' }, dayShort: x => x, metalOf: r => ({ label: r.metal }), cors: x => x,
    sheetHead: r => `<div class="h"><span class="nm">Sheet ${r.sheetIndex}</span></div>`, openLibrarySheet: () => {}, showLibrary: () => {}, loadLibrary: () => Promise.resolve(), pvRatio: () => '' });
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-orders.js'), 'utf8')); w.O = w.CharmNestOrders;
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-readiness.js'), 'utf8'));
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-activity.js'), 'utf8'));
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-motion.js'), 'utf8'));
  const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  w.eval(bridge.slice(bridge.indexOf('const LaserReview ='), bridge.indexOf('/* ═══ 22 · Sets — one run')));
  const a = bridge.indexOf('  function libraryGroups('), b = bridge.indexOf('  /** A set card whose completion', a), c = bridge.indexOf('  async function renderLibrary(body, opts)', b), e = bridge.indexOf('  return { releaseIssue', c);
  w.eval('window.Sets=(()=>{' + bridge.slice(a, b) + bridge.slice(c, e) + ';return {renderLibrary,libraryCard};})();');
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-library.js'), 'utf8'));
  const L = w.LaserReview, R = w.CharmNestReadiness, LD = w.LibraryDone, body = d.getElementById('libBody');
  // the counters: what the order rows were asked, and how often the whole list was searched for one card
  let decisions = 0, searches = 0; const realDecisions = R.decisions, realFind = w.Element.prototype.querySelector;
  R.decisions = (...x) => { decisions++; return realDecisions(...x); };
  w.Element.prototype.querySelector = function (sel) { if (/\.libCard\[data-id=/.test(String(sel))) searches++; return realFind.call(this, sel); };
  const count = async fn => { decisions = 0; searches = 0; await fn(); return { decisions, searches }; };
  try {
    // 1. the Sets view draws 100 set cards of 3 sheets; its pass reads the order rows a few times, not once per call
    sheets.forEach(L.record);
    const drew = await count(async () => { await w.Sets.renderLibrary(body); await tick(150); });
    assert.equal(body.querySelectorAll('.setCard').length, SETS, 'every set is drawn'); assert.equal(body.querySelectorAll('.libCard').length, SETS * PER);
    assert(drew.decisions <= 6, `drawing 300 sheets and their frames read the order rows ${drew.decisions} times (a handful, never one per sheet)`);
    assert.equal(drew.searches, 0, 'no card is looked for with a search of the whole list');
    assert.equal(body.querySelectorAll('[data-laser-area="ready"] .setCard').length, SETS / 2, 'the ready sets are in Laser cutting'); assert.equal(body.querySelectorAll('[data-laser-area="pending"] .setCard').length, SETS / 2);
    assert(body.querySelectorAll('.flowBox').length >= SETS, 'the step rails are drawn');

    // 2. a refresh frame and the card decorations, with nothing changed
    const frame = await count(async () => { L.changed(); await tick(); });
    assert(frame.decisions <= 2, `a refresh frame reads the order rows ${frame.decisions} times`); assert.equal(frame.searches, 0);
    const deco = await count(async () => { LD.decorate(body); });
    assert(deco.decisions <= 2, `decorating the cards reads the order rows ${deco.decisions} times`); assert.equal(deco.searches, 0);

    // 3. the completion marks sit where each card's own question puts them (sheets in Laser cutting, and the sets that may be completed)
    for (const card of body.querySelectorAll('.libCard[data-id]')) {
      const id = card.dataset.id, mark = card.querySelector(':scope > .ldMark');
      assert.equal(!!mark, LD.canComplete('sheet', id), `sheet ${id}: the mark is there exactly when it may be completed`);
    }
    for (const card of body.querySelectorAll('.setCard')) {
      const id = card._laserSet.setId, mark = card.querySelector(':scope > .sh > .ldMarkSet');
      assert.equal(!!mark, LD.canComplete('set', id), `set ${id}: the mark is there exactly when it may be completed`);
    }
    assert(body.querySelectorAll('.ldMark').length > 0 && body.querySelectorAll('[data-laser-area="pending"] .ldMark').length === 0, 'marks only where the laser may have cut');

    // 4. a pass reads what a call reads: the same projection for every sheet, once per pass, fresh outside one
    const same = (x, y) => JSON.stringify(x) === JSON.stringify(y);
    const outside = sheets.map(s => L.projected(s));
    L.batch(() => { sheets.forEach((s, n) => { const p = L.projected(s); assert(same(p, outside[n]), `${s.id}: the same projection inside a pass`); assert.equal(L.projected(s), p, 'asked again in the pass: the same answer'); }); });
    assert.notEqual(L.projected(sheets[0]), L.projected(sheets[0]), 'outside a pass every call reads afresh');
    const n0 = (await count(async () => { for (let i = 0; i < 10; i++) L.projected(sheets[0]); })).decisions;
    assert.equal(n0, 10, 'outside a pass every call reads the order rows');
    const n1 = (await count(async () => { L.batch(() => { for (let i = 0; i < 10; i++) L.projected(sheets[0]); }); })).decisions;
    assert.equal(n1, 1, 'inside a pass they are read once');

    // 5. a change is seen: by the next call outside a pass, by the next pass, whether a pass ended well or not, and a new record in a pass
    const s0 = sheets[1], pid = s0.poolIds[0], row = rows.find(r => r.poolIds[0] === pid);
    assert.equal(L.projected(s0).engraving[pid].approved, true); row.engrave = { needed: true, state: 'words', approved: false };
    assert.equal(L.projected(s0).engraving[pid].approved, false, 'a row that changed is read at once outside a pass');
    L.batch(() => { assert.equal(L.projected(s0).engraving[pid].approved, false); row.engrave = { needed: true, state: 'approved', approved: true }; });
    assert.equal(L.projected(s0).engraving[pid].approved, true, 'and by the next pass');
    assert.throws(() => L.batch(() => { L.projected(s0); throw new Error('a drawing that failed'); }), /failed/);
    row.engrave = { needed: true, state: 'words', approved: false };
    assert.equal(L.batch(() => L.projected(s0).engraving[pid].approved), false, 'a pass after a failed one starts from the rows as they are');
    row.engrave = { needed: true, state: 'approved', approved: true };
    L.batch(() => { const before = L.projected(s0); L.batch(() => assert.equal(L.projected(s0), before, 'a pass inside a pass shares it')); L.record({ ...s0, updatedAt: 5, preview: 'https://example.com/new.png' }); assert.equal(L.projected(s0).preview, 'https://example.com/new.png', 'a record that changed is read again'); });
    assert.equal(L.projected(s0).preview, 'https://example.com/new.png');

    // 6. the live loop's redraw leaves unchanged cards alone: the same elements, no new work for the rails
    const first = [...body.querySelectorAll('.flowBox')].slice(0, 20), html = first.map(x => x.innerHTML);
    L.changed(); await tick();
    assert(first.every((x, n) => x.isConnected && x.innerHTML === html[n]), 'a frame with nothing changed draws no card again');
    console.log(`PASS: library with ${SETS * PER} sheets in ${SETS} sets and ${rows.length} order rows: drawing ${drew.decisions} reads of the rows, a refresh frame ${frame.decisions}, decorating ${deco.decisions}; no search of the whole list; the answers unchanged`);
  } finally { w.close(); }
})().catch(err => { console.error(err); process.exitCode = 1; });
