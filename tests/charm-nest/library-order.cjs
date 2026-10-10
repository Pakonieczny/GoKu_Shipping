/* The order of the Library's blocks (Paul, 10 Oct 2026: "Actual Sets of Sheets should always be prioritized above all in-progress sheets
   when showing the order of sheets and Sets in the Library tab"; his screenshot had the loose "Sheets" group above Set-1).
   Rule: in Laser cutting, In progress and the Completed list (a number search included) every actual set comes before every group of
   loose sheets (the "Sheets" groups waiting for a set, the 14K / 10K solids). Inside each of the two groups the order is the one the
   Library always had (last activity, newest or oldest first as chosen). An empty group leaves no gap. A display order only.
   The real charm-nest-activity.js, the real LaserReview and Sets view (charm-nest-bridge.js) and the real charm-nest-library.js in a
   DOM, offline fixtures only (no cloud, nothing written).
     node tests/charm-nest/library-order.cjs */
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), { JSDOM } = require('jsdom');
const root = path.join(__dirname, '../..');
const tick = (n = 80) => new Promise(r => setTimeout(r, n));
const A = require(path.join(root, 'charm-nest-activity.js'));

/* ── 1. the order itself (no page): sets first, loose groups after, each group in its own activity order ── */
{
  const set = (id, at) => ({ setId: id, seq: 1, day: '2026-10-03', status: 'labelled', updatedAt: at, sheets: [] });
  const loose = (key, at, extra) => ({ key, setId: null, working: true, standalone: false, day: '2026-10-09', updatedAt: at, sheets: [], ...extra });
  const ids = list => list.map(x => x.setId || x.key);
  // the screenshot: the loose "Sheets" group is newer than Set-1, so it used to be listed above it
  const shot = [loose('working:oct9', 9000), set('Set-1', 3000)];
  assert.deepEqual(ids(shot.slice().sort((a, b) => A.compare(a, b, 'desc'))), ['working:oct9', 'Set-1'], 'by activity alone the newer loose group was first (the old rule)');
  assert.deepEqual(ids(shot.slice().sort((a, b) => A.compareBlocks(a, b, 'desc'))), ['Set-1', 'working:oct9'], 'the set is first now');
  // two sets and loose groups of every kind, mixed in time: newest first, then oldest first
  const mix = [loose('loose-new', 9500), set('set-b', 6000), loose('solid-waiting', 7000, { standalone: true }), set('set-a', 5000), loose('loose-old', 1000), set('set-c', 8000)];
  for (const [direction, want] of [['desc', ['set-c', 'set-b', 'set-a', 'loose-new', 'solid-waiting', 'loose-old']], ['asc', ['set-a', 'set-b', 'set-c', 'loose-old', 'solid-waiting', 'loose-new']]]) {
    assert.deepEqual(ids(mix.slice().sort((a, b) => A.compareBlocks(a, b, direction))), want, `${direction}: sets in their order, then the loose groups in theirs`);
    // a list that is already in the right order stays as it is (a refresh never shuffles it)
    assert.deepEqual(ids(want.map(k => mix.find(x => (x.setId || x.key) === k)).sort((a, b) => A.compareBlocks(a, b, direction))), want);
  }
  // an empty group leaves no gap: only sets, only loose groups, nothing at all
  assert.deepEqual(ids(mix.filter(A.isSet).sort((a, b) => A.compareBlocks(a, b, 'desc'))), ['set-c', 'set-b', 'set-a']);
  assert.deepEqual(ids(mix.filter(x => !A.isSet(x)).sort((a, b) => A.compareBlocks(a, b, 'desc'))), ['loose-new', 'solid-waiting', 'loose-old']);
  assert.deepEqual(A.selectBlocks('library', [], 1), []);
  // what is an actual set: a set record with its id; not a group of loose sheets, not the solids; in Completed a row of kind "set", never a row of one sheet (even one that has a setId)
  assert.equal(A.isSet(set('s', 1)), true);
  assert.equal(A.isSet(loose('k', 1)), false);
  assert.equal(A.isSet(loose('solid-waiting', 1, { standalone: true, working: true })), false);
  assert.equal(A.isSet({ kind: 'set', setId: 's' }), true);
  assert.equal(A.isSet({ kind: 'sheet', id: 'x', setId: 's' }), false, 'a completed sheet of a set that is not complete is a loose row');
  assert.equal(A.isSet({ kind: 'sheet', id: 'x', setId: null }), false);
  assert.equal(A.isSet(null), false);
  // the flat Sheets view (and every other list) keeps CNListActivity.select / compare: a sheet that has a set is not moved up there
  const flat = [{ id: 'n', setId: null, updatedAt: 9000 }, { id: 's', setId: 'Set-1', updatedAt: 1000 }];
  assert.deepEqual(A.select('library', flat, 1).map(x => x.id), ['n', 's'], 'the Sheets view is ordered as before');
}

/* ── 2. the Library page: Laser cutting and In progress, then Completed ── */
const back = (id, sheetId) => ({ poolId: id, sheetId, approvedAt: 10, approvedBy: 'Paul', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: id + '.ai', url: 'https://example.com/' + id + '.ai' } } });
let counter = 0;
/* one sheet, as many-sheets draws it: `ready` sheets have their back engravings approved, the others wait for them */
function sheet(id, o) {
  const n = ++counter, pool = Array.from({ length: 4 }, (_, i) => `${4200000000 + n * 10 + i}_t${i}_1`), orders = pool.map(p => p.split('_')[0]);
  return { id, metal: o.metal || 'gold', metalLabel: 'GF 14/20', setId: o.setId || null, setSeq: o.setSeq || null, runId: o.runId, sheetIndex: o.index || 1, status: 'complete', poolIds: pool, placedCount: pool.length, charmCount: pool.length, density: .7,
    verification: { ok: true }, preview: 'https://example.com/p.png', outputs: { ai: 'https://example.com/f.ai' }, orders, label: { files: [{ path: 'qr.png', url: 'https://example.com/qr.png', payload: 'x', orders }] },
    backPool: o.ready ? pool.map(p => back(p, id)) : [], engraving: {}, orderReadiness: Object.fromEntries(orders.map(x => [x, { ready: true }])), updatedAt: o.at, day: '2026-10-09', folder: `${id}_folder`, fileBase: `Oct10_${id}`, _ready: !!o.ready };
}
function world(scene) {
  const sheets = [], rawSets = [];
  for (const s of scene.sets) {
    const mine = Array.from({ length: 2 }, (_, i) => sheet(`${s.id}-sh${i + 1}`, { setId: s.id, setSeq: s.seq, runId: 'run-' + s.id, index: i + 1, ready: s.ready, at: s.at }));
    sheets.push(...mine); rawSets.push({ setId: s.id, seq: s.seq, day: '2026-10-03', runId: 'run-' + s.id, name: 'Set-' + s.seq, sheetIds: mine.map(x => x.id), materials: ['gold'], status: 'labelled', updatedAt: s.at });
  }
  for (const g of scene.loose) sheets.push(...Array.from({ length: g.n || 2 }, (_, i) => sheet(`${g.id}-sh${i + 1}`, { setId: null, runId: g.id, metal: g.metal, index: i + 1, ready: g.ready, at: g.at })));
  return { sheets, rawSets };
}
async function page(scene, direction) {
  const dom = new JSDOM('<div id="stage"><div id="libView"><div id="libTab"><button data-t="current"></button><button data-t="done"></button></div><input id="libSearch"><span id="libDoneCount"></span><div id="libBody"></div><div id="libDone"></div></div></div>', { url: 'https://example.test/#library', runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window, d = w.document, { sheets, rawSets } = world(scene);
  w.Element.prototype.scrollIntoView = () => {}; w.matchMedia = () => ({ matches: true }); w.CSS = { escape: x => x }; w.HTMLCanvasElement.prototype.getContext = () => ({ measureText: t => ({ width: t.length * 8 }) });
  const rows = []; for (const s of sheets) for (const p of s.poolIds) rows.push({ key: p.replace(/_1$/, ''), order: { receiptId: p.split('_')[0], buyer: { name: 'Buyer' } }, state: 'written', poolIds: [p], engrave: s._ready ? { needed: true, state: 'approved', approved: true } : { needed: true, state: 'words', approved: false } });
  const jobs = new Map(); sheets.filter(s => !s._ready).forEach((s, n) => jobs.set('job' + n, { key: 'job' + n, copies: [s.poolIds[0]], state: 'words', backs: [] }));
  const asked = [];
  const api = async (_, b) => { asked.push(b.op); return b.op === 'setList' ? { sets: rawSets, sheets: [] } : b.op === 'listSheets' ? { sheets } : scene.api ? scene.api(b) : { sheets: [], sets: [], counts: {} }; };
  Object.assign(w, { S: { mode: 'library', library: { rows: sheets, kind: 'sets', metal: 'all' }, cloud: { ok: true } }, api, allSheets: () => [], Orders: { rows: () => rows }, Engrave: { items: () => jobs, backsMarkup: () => '' },
    esc: x => String(x ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])), el: (tag, cls) => { const e = d.createElement(tag); e.className = cls; return e; },
    Gate: { projectLibraryRecords: x => x }, RoseStock: {}, CODE: { gold: 'GF', silver: 'SS' }, toast: () => null, CNEmployee: { name: () => 'Tester' }, dayShort: x => x, metalOf: r => ({ label: r.metal }), cors: x => x,
    swatch: () => '', sheetHead: r => `<div class="h"><span class="nm">Sheet ${r.sheetIndex}</span></div>`, openLibrarySheet: () => {}, showLibrary: () => {}, loadLibrary: () => Promise.resolve(), pvRatio: () => '' });
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-orders.js'), 'utf8')); w.O = w.CharmNestOrders;
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-readiness.js'), 'utf8'));
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-activity.js'), 'utf8'));
  w.CNListActivity.set('library', { direction });
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-motion.js'), 'utf8'));
  const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  w.eval(bridge.slice(bridge.indexOf('const LaserReview ='), bridge.indexOf('/* ═══ 22 · Sets — one run')));
  const a = bridge.indexOf('  function libraryGroups('), b = bridge.indexOf('  /** A set card whose completion', a), c = bridge.indexOf('  async function renderLibrary(body, opts)', b), e = bridge.indexOf('  return { releaseIssue', c);
  w.eval('window.Sets=(()=>{' + bridge.slice(a, b) + bridge.slice(c, e) + ';return {renderLibrary,libraryCard};})();');
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-library.js'), 'utf8'));
  sheets.forEach(w.LaserReview.record);
  return { w, d, sheets, asked };
}
// what a section shows, top to bottom: a set card by its id, a group of loose sheets by the id of its first sheet's run
const names = (d, area) => [...d.querySelectorAll(`#libBody [data-laser-area="${area}"] .laserAreaItems > .setCard`)].map(c => c._laserSet.setId || 'loose:' + c._sheets[0].runId);
const scene = (loose, sets) => ({ loose, sets });

(async () => {
  // Laser cutting (ready) and In progress (not ready), each with loose groups newer than the sets and older than them, and two sets
  const full = scene(
    [{ id: 'L-new', at: 9500, ready: false }, { id: 'L-mid', at: 7000, ready: false, metal: 'gold14k' }, { id: 'L-old', at: 1000, ready: false },
     { id: 'R-new', at: 9400, ready: true }, { id: 'R-old', at: 900, ready: true }],
    [{ id: 'P-a', seq: 1, at: 5000, ready: false }, { id: 'P-b', seq: 2, at: 6000, ready: false }, { id: 'R-a', seq: 3, at: 5100, ready: true }, { id: 'R-b', seq: 4, at: 6100, ready: true }]);
  for (const [direction, wantPending, wantReady] of [
    ['desc', ['P-b', 'P-a', 'loose:L-new', 'loose:L-mid', 'loose:L-old'], ['R-b', 'R-a', 'loose:R-new', 'loose:R-old']],
    ['asc', ['P-a', 'P-b', 'loose:L-old', 'loose:L-mid', 'loose:L-new'], ['R-a', 'R-b', 'loose:R-old', 'loose:R-new']]]) {
    const { w, d } = await page(full, direction), body = d.getElementById('libBody');
    try {
      await w.Sets.renderLibrary(body); await tick();
      assert.deepEqual(names(d, 'pending'), wantPending, `In progress, ${direction}: the two sets first, then the loose groups, each in its own order`);
      assert.deepEqual(names(d, 'ready'), wantReady, `Laser cutting, ${direction}: the two sets first, then the loose groups, each in its own order`);
      // the page's own refresh (a live read, a seal) draws the same order, and a loose group that just got newer does not climb above a set
      const loose = w.LaserReview; loose.changed(); await tick();
      assert.deepEqual(names(d, 'pending'), wantPending, 'a refresh keeps In progress as it is'); assert.deepEqual(names(d, 'ready'), wantReady, 'a refresh keeps Laser cutting as it is');
      // keyboard and drag and drop read the page's own order: the cards are in the document in that order
      const cards = [...body.querySelectorAll('.setCard')].map(c => c._laserSet.setId || 'loose:' + c._sheets[0].runId);
      assert.deepEqual(cards, [...wantReady, ...wantPending], 'the document order is the visual order: Laser cutting, then In progress');
      // a search that keeps sets and loose groups together keeps the rule (the page's own box: a word that every sheet's file name holds)
      d.getElementById('libSearch').value = 'oct10'; await w.Sets.renderLibrary(body, { reuse: true }); await tick();
      assert.deepEqual(names(d, 'pending'), wantPending, 'a word search: the same order'); assert.deepEqual(names(d, 'ready'), wantReady);
    } finally { w.close(); }
  }
  // an empty group leaves no gap: only loose groups, only sets
  for (const [label, sc, pending] of [['no set', scene([{ id: 'L-a', at: 100, ready: false }, { id: 'L-b', at: 200, ready: false }], []), ['loose:L-b', 'loose:L-a']],
                                      ['no loose group', scene([], [{ id: 'P-a', seq: 1, at: 100, ready: false }, { id: 'P-b', seq: 2, at: 200, ready: false }]), ['P-b', 'P-a']]]) {
    const { w, d } = await page(sc, 'desc');
    try { await w.Sets.renderLibrary(d.getElementById('libBody')); await tick(); assert.deepEqual(names(d, 'pending'), pending, `${label}: the other group is all there is`); assert.equal(names(d, 'ready').length, 0); }
    finally { w.close(); }
  }

  // Completed, a number search: the completed sets first, then the loose completed sheets (one of them has a set that is not complete)
  const day = n => Date.parse('2026-10-0' + n + 'T15:00:00Z');
  const done = {
    setRows: [{ kind: 'set', setId: 'D-a', seq: 1, day: '2026-10-03', sheets: [{ id: 'da1', metal: 'gold' }], orders: 2, pieces: 5, fill: .7, at: day(4), activityAt: day(4), by: 'Anna' },
              { kind: 'set', setId: 'D-b', seq: 2, day: '2026-10-03', sheets: [{ id: 'db1', metal: 'gold' }], orders: 2, pieces: 5, fill: .7, at: day(2), activityAt: day(2), by: 'Anna' }],
    rows: [{ kind: 'sheet', id: 'ls-new', metal: 'gold', setId: null, sheetIndex: 1, day: '2026-10-09', orders: 1, pieces: 2, fill: .5, at: day(8), activityAt: day(8), by: 'Anna' },
           { kind: 'sheet', id: 'ls-set', metal: 'silver', setId: 'S-open', setSeq: 5, sheetIndex: 1, day: '2026-10-09', orders: 1, pieces: 2, fill: .5, at: day(6), activityAt: day(6), by: 'Anna' },
           { kind: 'sheet', id: 'ls-old', metal: 'gold', setId: null, sheetIndex: 2, day: '2026-10-01', orders: 1, pieces: 2, fill: .5, at: day(1), activityAt: day(1), by: 'Anna' }],
    sheets: [], sets: [], counts: { sheets: 3, sets: 2 }, matches: { order: true } };
  const order = d => [...d.querySelectorAll('#libDone .ldItem:not(.skel)')].map(x => x.dataset.kind === 'set' ? x.dataset.set : x.dataset.id);
  for (const [direction, want] of [['desc', ['D-a', 'D-b', 'ls-new', 'ls-set', 'ls-old']], ['asc', ['D-b', 'D-a', 'ls-old', 'ls-set', 'ls-new']]]) {
    const { w, d } = await page(Object.assign(scene([], []), { api: b => b.op === 'findSheets' ? done : { rows: [], next: null, counts: done.counts } }), direction);
    try {
      w.LibraryDone.setTab('done'); await tick(100);
      d.getElementById('libSearch').value = '#3700000001'; w.LibraryDone.input(); await tick(900);   // (the page's own box: the number is looked up once the typing pauses)
      assert.deepEqual(order(d), want, `Completed, ${direction}: the completed sets first, then the loose completed sheets, each in its own order`);
    } finally { w.close(); }
  }
  // Completed without a search is the server's page of completed sets, newest first (the server sends sets only): nothing to put in front of anything, and it is not touched
  { const rowsOf = [{ kind: 'set', setId: 'C-1', seq: 1, day: '2026-10-03', sheets: [], orders: 1, pieces: 1, fill: .5, at: day(4), activityAt: day(4) }, { kind: 'set', setId: 'C-2', seq: 2, day: '2026-10-03', sheets: [], orders: 1, pieces: 1, fill: .5, at: day(3), activityAt: day(3) }];
    const { w, d } = await page(Object.assign(scene([], []), { api: b => b.op === 'laserDoneList' ? { kind: 'sets', rows: rowsOf, next: null, counts: { sheets: 0, sets: 2 } } : { sheets: [], sets: [], counts: {} } }), 'desc');
    try { w.LibraryDone.setTab('done'); await tick(250); assert.deepEqual(order(d), ['C-1', 'C-2'], 'the server page stays in its order'); } finally { w.close(); }
  }
  console.log('PASS: Library order: actual sets before every group of loose sheets in Laser cutting, In progress, Completed and the searches; each group in its own order, newest or oldest first; no gap when a group is empty; the Sheets view unchanged');
})().catch(err => { console.error(err); process.exitCode = 1; });
