/* The '!' issues panel (charm-nest-library-issues.js) costs a big Library nothing: 300 sheets in 100 sets, the real LaserReview, set
   card code and Library module in a DOM, offline fixtures only. What is counted, not timed (timings differ from machine to machine):
   with the panel closed the Library draws and refreshes exactly as it does without the module (the same HTML, the same number of
   reads of the order rows) and CharmNestReadiness.issues is never read; with one panel open a refresh frame reads the issues of
   that one sheet, never of the other 299.
     node tests/charm-nest/library-issues-many.cjs        (NODE_PATH=<dir with jsdom>) */
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), crypto = require('node:crypto'), { JSDOM } = require('jsdom');
const root = path.join(__dirname, '../..');
const tick = (n = 60) => new Promise(r => setTimeout(r, n));
const SETS = 100, PER = 3, PIECES = 6;
const back = (id, sheetId) => ({ poolId: id, sheetId, approvedAt: 10, approvedBy: 'Paul', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: id + '.ai', url: 'https://example.com/' + id + '.ai' } } });
function sheet(k, i) {
  const id = `s${k}x${i}`, pool = Array.from({ length: PIECES }, (_, n) => `${4100000000 + (k * PER + i) * PIECES + n}_t${n}_1`), orders = pool.map(p => p.split('_')[0]);
  const ready = k % 2 === 0;
  return { id, metal: 'gold', metalLabel: 'GF 14/20', setId: 'set-' + k, setSeq: k + 1, runId: 'run1', sheetIndex: i + 1, status: 'complete', poolIds: pool, placedCount: pool.length, charmCount: pool.length, density: .7,
    verification: { ok: true }, preview: 'https://example.com/p.png', outputs: { ai: 'https://example.com/f.ai' }, orders, label: { files: [{ path: 'qr.png', url: 'https://example.com/qr.png', payload: 'x', orders }] },
    backPool: ready ? pool.map(p => back(p, id)) : [], engraving: {}, orderReadiness: Object.fromEntries(orders.map((o, n) => [o, ready || n > 1 ? { ready: true } : { ready: false, key: 'pooled', why: 'A piece is not on a saved sheet yet', blocks: [{ key: 'pooled', index: 2, label: 'Charm', poolId: o + '_t_2', lineKey: o + '_t', sheetId: null, sheetLabel: null, why: 'A piece is not on a saved sheet yet' }], onSheets: [id], pieceCount: 2, customer: 'Buyer ' + n, listingId: 'L' + n }])), updatedAt: 1, day: '2026-10-03', folder: `GF_Oct.03.26_Set-${k + 1}_Sheet-${i + 1}` };
}
async function run(withModule) {
  const dom = new JSDOM('<body><div class="topbar"></div><div id="stage"><div id="libView"><div id="libTab"><button data-t="current"></button><button data-t="done"></button></div><input id="libSearch"><span id="libDoneCount"></span><div id="libBody"></div><div id="libDone"></div></div></div></body>', { url: 'https://example.test/', runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window, d = w.document, out = {};
  try {
    w.matchMedia = () => ({ matches: true }); w.CSS = { escape: x => x }; w.HTMLCanvasElement.prototype.getContext = () => ({ measureText: t => ({ width: t.length * 8 }) });
    w.Element.prototype.getClientRects = function () { return [{}]; };
    const sheets = [], rawSets = [];
    for (let k = 0; k < SETS; k++) { const mine = Array.from({ length: PER }, (_, i) => sheet(k, i)); sheets.push(...mine); rawSets.push({ setId: 'set-' + k, seq: k + 1, day: '2026-10-03', runId: 'run1', sheetIds: mine.map(s => s.id), materials: ['gold'], status: 'labelled', updatedAt: 1 }); }
    const rows = []; for (const s of sheets) for (const p of s.poolIds) { const ok = Number(s.setId.slice(4)) % 2 === 0; rows.push({ key: p.replace(/_1$/, ''), order: { receiptId: p.split('_')[0], buyer: { name: 'Buyer' } }, line: { listingId: 'L1' }, state: 'written', poolIds: [p], engrave: ok ? { needed: true, state: 'approved', approved: true } : { needed: true, state: 'words', approved: false } }); }
    const jobs = new Map();
    const api = async (_, b) => b.op === 'setList' ? { sets: rawSets, sheets: [] } : b.op === 'listSheets' ? { sheets } : { sheets: [], sets: [], counts: {} };
    Object.assign(w, { S: { mode: 'library', library: { rows: sheets, kind: 'sets', metal: 'all' }, cloud: { ok: true } }, api, allSheets: () => [], Orders: { rows: () => rows }, Engrave: { items: () => jobs, backsMarkup: () => '' },
      esc: x => String(x ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])), el: (tag, cls) => { const e = d.createElement(tag); e.className = cls; return e; },
      Gate: { projectLibraryRecords: x => x }, RoseStock: {}, CODE: { gold: 'GF', silver: 'SS' }, toast: () => null, CNEmployee: { name: () => 'Tester' }, dayShort: x => x, metalOf: r => ({ label: r.metal }), cors: x => x,
      sheetHead: r => `<div class="h"><span class="nm">Sheet ${r.sheetIndex}</span></div>`, openLibrarySheet: () => {}, showLibrary: () => {}, loadLibrary: () => Promise.resolve(), pvRatio: () => '', ListMedia: { peek: () => null, listing: () => Promise.resolve(null) } });
    w.eval(fs.readFileSync(path.join(root, 'charm-nest-orders.js'), 'utf8')); w.O = w.CharmNestOrders;
    w.eval(fs.readFileSync(path.join(root, 'charm-nest-readiness.js'), 'utf8'));
    w.eval(fs.readFileSync(path.join(root, 'charm-nest-activity.js'), 'utf8'));
    w.eval(fs.readFileSync(path.join(root, 'charm-nest-motion.js'), 'utf8'));
    const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
    w.eval(bridge.slice(bridge.indexOf('const LaserReview ='), bridge.indexOf('/* ═══ 22 · Sets — one run')));
    const a = bridge.indexOf('  function libraryGroups('), b = bridge.indexOf('  /** A set card whose completion', a), c = bridge.indexOf('  async function renderLibrary(body, opts)', b), e = bridge.indexOf('  return { releaseIssue', c);
    w.eval('window.Sets=(()=>{' + bridge.slice(a, b) + bridge.slice(c, e) + ';return {renderLibrary,libraryCard};})();');
    w.eval(fs.readFileSync(path.join(root, 'charm-nest-library.js'), 'utf8'));
    if (withModule) w.eval(fs.readFileSync(path.join(root, 'charm-nest-library-issues.js'), 'utf8'));
    const L = w.LaserReview, R = w.CharmNestReadiness, body = d.getElementById('libBody');
    let decisions = 0, issueReads = 0; const realDecisions = R.decisions, realIssues = R.issues, seen = new Set();
    R.decisions = (...x) => { decisions++; return realDecisions(...x); };
    R.issues = (s, ctx) => { issueReads++; seen.add(s.id || s.sheetId); return realIssues(s, ctx); };
    const count = async fn => { decisions = 0; issueReads = 0; seen.clear(); await fn(); return { decisions, issueReads, sheets: seen.size }; };
    sheets.forEach(L.record);
    out.draw = await count(async () => { await w.Sets.renderLibrary(body); await tick(150); });
    out.cards = body.querySelectorAll('.libCard').length; out.bangs = body.querySelectorAll('[data-issues-open]').length;
    const hash = x => crypto.createHash('sha1').update(x).digest('hex');
    out.html = hash(body.innerHTML);
    out.frame = await count(async () => { L.changed(); await tick(); });
    out.html2 = hash(body.innerHTML);
    if (withModule) {
      // one panel open: the '!' on the Order check step of a sheet whose first two orders are held back by a piece that is not on a sheet
      const target = body.querySelector('.libCard[data-id="s1x0"]'), card = target.closest('[data-laser-card]'), bang = card.querySelector('.flowBox[data-flow-for="sheet:s1x0"] [data-issues-open]');
      assert(bang, 'the sheet has its \'!\''); out.step = bang.dataset.issuesStep;
      out.open = await count(async () => { bang.click(); await tick(80); });
      const p = d.getElementById('libIssuesPanel'); assert(p, 'the panel opens'); out.rows = p.querySelectorAll('.lisRow').length; out.own = p.querySelectorAll('.lisOwn').length; out.title = p.querySelector('.lisHead b').textContent;
      out.openFrame = await count(async () => { L.changed(); await tick(); });
      out.openFrames = []; for (let i = 0; i < 3; i++) out.openFrames.push(await count(async () => { L.changed(); await tick(); }));
      w.eval('LibraryIssues.close()'); await tick(200);
      out.closedAgain = await count(async () => { L.changed(); await tick(); });
    }
  } finally { dom.window.close(); }
  return out;
}
(async () => {
  const base = await run(false), mod = await run(true);
  assert.equal(base.cards, SETS * PER, '300 sheets are drawn'); assert.equal(mod.cards, SETS * PER);
  assert.equal(mod.html, base.html, 'with the module loaded and every panel closed the Library draws exactly the same cards');
  assert.equal(mod.html2, base.html2, 'and a refresh frame leaves them the same');
  assert.equal(mod.draw.decisions, base.draw.decisions, `drawing reads the order rows as often as before (${base.draw.decisions})`); assert.equal(mod.frame.decisions, base.frame.decisions, `a refresh frame too (${base.frame.decisions})`);
  assert.equal(mod.draw.issueReads, 0, 'closed: drawing 300 sheets reads no issues'); assert.equal(mod.frame.issueReads, 0, 'closed: a refresh frame reads no issues');
  assert(mod.bangs >= SETS * PER / 2, `the unready sheets carry their '!' (${mod.bangs})`);
  assert.equal(mod.open.sheets, 1, 'pressing one \'!\' reads the issues of that one sheet'); assert(mod.open.issueReads <= 3, `${mod.open.issueReads} reads to open one panel`);
  assert(mod.rows === 2 || mod.own === 1, `the panel lists what holds the sheet (${mod.rows} rows, ${mod.own} own) on the ${mod.step} step`);
  for (const f of [mod.openFrame, ...mod.openFrames]) { assert(f.sheets <= 1, 'an open panel: a refresh frame reads one sheet\'s issues, never the other 299'); assert(f.issueReads <= 2, `${f.issueReads} issue reads in a frame with a panel open`); assert(f.decisions <= base.frame.decisions + 3, `${f.decisions} reads of the order rows in a frame with a panel open (${base.frame.decisions} without)`); }
  assert.equal(mod.closedAgain.issueReads, 0, 'closed again: nothing is read');
  console.log(`Library issues many OK: 300 sheets, closed panel = same HTML, same ${base.draw.decisions}/${base.frame.decisions} reads of the order rows, 0 issue reads; open panel = ${mod.openFrames[0].issueReads} issue read(s) of ${mod.openFrames[0].sheets} sheet per frame`);
})().catch(e => { console.error(e); process.exitCode = 1; });
