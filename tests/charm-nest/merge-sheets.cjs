// Merge sheets on a 10K or 14K card (Paul, 28 Sep: "add a new button that allows two sheets to be merged into one,
// especially when the user changes the size of the available sheet and now there's more room to fit the charms, but
// they're stuck in the second sheet"). Options → Merge sheets:
//   1. Move all onto Sheet 1: Sheet 1's charms stay exactly where they are and the later sheets' charms go into its free
//      room, oldest orders first, as arrivals do; what does not fit stays on Sheet 2 (at the spot it had there), never on a
//      new sheet; a sheet left empty goes, its saved record archived at the next checkpoint, and the set drops it first.
//   2. Re-nest both (all) sheets: every charm is nested again from Sheet 1 on, oldest first; each sheet refills in turn
//      (overflow follows the rerun's order) and a sheet left empty goes.
//   3. Locked sheets (committed, cut, recalled, another run) are left alone; a busy metal is refused and nothing moves.
//   4. The page's overflow follows the merge's order and puts a charm that comes back at its old spot.
//   5. In the browser: the Options panel closes, the card's own line says what the merge does, the size waits for it, and
//      the merge is seen on the card (the sheets side by side, the charms flying, one sheet) and then leaves nothing
//      behind; reduced motion shows none of it; a hidden tab ends it at once.
// The Gate module runs in a vm with stand-ins for the page; the overflow runs from the page's own code.
//   node tests/charm-nest/merge-sheets.cjs     (PW_DIR: playwright-core's folder, CHROMIUM: the browser)
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const root = path.join(__dirname, '../..');
const Ops = require(path.join(root, 'charm-nest-operations.js')), O = require(path.join(root, 'charm-nest-orders.js'));
const source = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), start = source.indexOf('const Gate ='), end = source.indexOf('/* ═══ 21', start);
const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
const slice = (from, to) => { const a = html.indexOf(from), b = html.indexOf(to, a); assert(a >= 0 && b > a, 'slice ' + from); return html.slice(a, b); };
const tick = ms => new Promise(r => setTimeout(r, ms));

/* ── the page, as far as the Gate reads it ── */
function world() {
  const ops = Ops.create(), sets = [], saved = new Map(), calls = { nest: [], append: [], options: 0, toasts: [] };
  const run = { runId: 'r', releasePolicy: 2, status: 'processed', step: 'complete', solidIncluded: {}, errors: [], sheets: {}, sheetHolds: {} };
  const S = { mode: 'nest', settings: {}, cloud: { ok: true }, library: { rows: [] }, sheets: {} };
  for (const m of ['gold10k', 'gold14k']) S.sheets[m] = { pages: [], active: 0, cardEl: null };
  const pagesOf = m => S.sheets[m].pages, allSheets = () => [...pagesOf('gold10k'), ...pagesOf('gold14k')];
  let nextId = 0;
  const charm = (id, order, orderDate) => ({ id, order, orderDate, poolId: 'p-' + id, pinned: { cxPt: 1, cyPt: 1, angle: 0 }, arrivalPin: true, thumb: 't' });
  const sheet = (metal, page, charms, more = {}) => {
    const sh = { metal, page, runId: 'r', sheetId: `${metal}-s${page}`, draft: true, status: 'complete', outputs: { ai: 'a' }, persistedDone: true, verification: { ok: true }, cloud: { ai: 'u' },
      charms, placements: charms.map((c, i) => ({ id: c.id, cxPt: 10 + i * 12, cyPt: 8 + page, angle: 15 * i })), rejects: [], backPool: charms.map(c => ({ poolId: c.poolId, text: 'back ' + c.id })), ...more };
    pagesOf(metal).push(sh); return sh;
  };
  // the page's own: a sheet to nest again drops its layout; a removed sheet takes its place in the row away
  const sheetDirty = sh => { sh.dirty = true; sh.releaseFull = false; sh.movedOn = null; sh.problem = null; if (!['nesting', 'finishing'].includes(sh.status)) { sh.status = sh.charms.length ? 'ready' : 'idle'; sh.placements = []; sh.rejects = []; sh.outputs = null; sh.verification = null; } };
  const removePage = pg => { const prim = S.sheets[pg.metal], i = prim.pages.indexOf(pg); if (i <= 0) return; prim.pages.splice(i, 1); for (const p of prim.pages.slice(i)) p.page = Math.max(1, p.page - 1); prim.active = Math.min(i, prim.pages.length - 1); };
  const addPage = m => { const pg = { metal: m, page: pagesOf(m).length + 1, runId: 'r', sheetId: null, charms: [], placements: [], rejects: [], status: 'idle', backPool: [] }; pagesOf(m).push(pg); return pg; };
  // a nest: `room` charms fit a sheet, whole orders, oldest first; the rest move on as overflowToNextSheet moves them. A
  // sheet taking arrivals (appendOnly) keeps its placements, locked, and places the new charms around them.
  const world = { room: 99 };
  const startNest = sh => {
    calls.nest.push(sh); calls.append.push(!!sh.appendOnly); sh.status = 'nesting'; sh.persistedDone = false;
    const kept = sh.appendOnly ? sh.placements.filter(p => sh.charms.some(c => c.id === p.id)).map(p => ({ ...p })) : [];
    setTimeout(() => {
      const keep = new Set(kept.map(p => p.id)), byOrder = new Map();
      for (const c of sh.charms.filter(c => !keep.has(c.id)).sort((a, b) => a.orderDate - b.orderDate)) { if (!byOrder.has(c.order)) byOrder.set(c.order, []); byOrder.get(c.order).push(c); }
      const placed = []; let full = false;
      for (const group of byOrder.values()) { if (!full && kept.length + placed.length + group.length <= world.room) placed.push(...group); else full = true; }
      sh.placements = [...kept, ...placed.map((c, i) => ({ id: c.id, cxPt: 300 + (kept.length + i) * 12, cyPt: 30, angle: 0 }))]; sh.rejects = sh.charms.filter(c => !keep.has(c.id) && !placed.includes(c)).map(c => c.id);
      sh.status = sh.rejects.length ? 'partial' : 'complete'; sh.dirty = false; sh.verification = { ok: true }; sh.outputs = { ai: 'a' }; sh.cloud = { ai: 'u' };
      setTimeout(() => {
        sh.persistedDone = true;
        if (sh.rejects.length) {
          const moving = sh.charms.filter(c => sh.rejects.includes(c.id)); sh.charms = sh.charms.filter(c => !sh.rejects.includes(c.id)); sh.rejects = [];
          const i = pagesOf(sh.metal).indexOf(sh), next = pagesOf(sh.metal).includes(sh._mergeNext) ? sh._mergeNext : pagesOf(sh.metal).length - 1 > i ? pagesOf(sh.metal).at(-1) : addPage(sh.metal);
          // (as the page's overflowToNextSheet: one that comes back to the sheet it left goes back to its spot there)
          for (const c of moving) { const was = next._mergeSpots && next._mergeSpots.get(c.id); if (was) { c.pinned = { cxPt: was.cxPt, cyPt: was.cyPt, angle: was.angle }; c.arrivalPin = true; } }
          next.charms.push(...moving); startNest(next);
        }
      }, 30);
    }, 60);
  };
  const ctx = {
    window: { CharmNestOrders: O, CharmNestOperations: ops }, B: { run, sets: new Map() }, S, O, setTimeout, console, allSheets, pagesOf, refreshAllCards() {}, Session: { schedule() {} },
    document: { getElementById: () => null }, toast: (t) => calls.toasts.push(t), agent() {}, sheetDirty, removePage, startNest, activeCharms: sh => sh.charms.filter(c => !c.excluded),
    activePage: m => S.sheets[m].pages[S.sheets[m].active], heldForResume: () => false,
    Pool: { update: async () => {} }, Orders: { rows: () => [] }, Engrave: { items: () => new Map(), saveSheetBacks: async () => {} },
    RunCtl: { save: async () => {}, poke() {}, onSheetDone() {}, membershipUpdated() {}, optionsChanged() { calls.options++; } },
    CN: { sheetFileBase: sh => 'Set-1-Sheet-' + sh.page, renderLibrary() {}, showPage(m, i) { S.sheets[m].active = i; } },
    Sets: { ofRun: () => sets, ensure: async () => { const set = { setId: 'set', runId: 'r', seq: 1, day: '2026-09-28', group: 'dispatch', sheetIds: [], labelFiles: [], materials: [], orders: {} }; sets.push(set); return set; }, save: async () => {}, labelsReady: () => true,
      onSheetSaved: async sh => { const set = sets.find(s => !s.committedAt); if (!set.sheetIds.includes(sh.sheetId)) set.sheetIds.push(sh.sheetId); sh.label = { qr: 'q' }; } },
    api: async (name, body) => { if (body.sheet) saved.set(body.sheet.id, Object.assign(saved.get(body.sheet.id) || {}, body.sheet)); return {}; },
    stockFor: () => ({ wIn: 50 / 25.4, hIn: 46 / 25.4 }), labelOf: x => ({ gold10k: '10K Gold', gold14k: '14K Gold' })[x] || x, esc: x => x,
  };
  vm.createContext(ctx); vm.runInContext(source.slice(start, end), ctx);
  return { Gate: ctx.window.Gate, S, run, sets, saved, calls, charm, sheet, pagesOf, allSheets, world, ops };
}
const ids = pages => [...pages.flatMap(p => [...p.charms.map(c => c.id)])].sort();

(async () => {
  /* ── 1 · Move all onto Sheet 1: its charms stay, the later sheets' go into its free room ── */
  {
    const w = world(), { Gate, run, charm, sheet, pagesOf } = w;
    const s1 = sheet('gold14k', 1, [charm('a', '100', 1), charm('b', '101', 2), charm('c', '102', 3)], { solidPick: true });
    const s2 = sheet('gold14k', 2, [charm('d', '103', 4), charm('e1', '104', 5), charm('e2', '104', 5)], { solidPick: true });
    const k1 = sheet('gold10k', 1, [charm('x', '200', 1)]), k2 = sheet('gold10k', 2, [charm('y', '201', 2)]);
    // both 14K sheets in the current set, with their QR labels
    await Gate.assemble(run);
    assert.deepEqual(w.sets[0].sheetIds.sort(), ['gold14k-s1', 'gold14k-s2'], 'both 14K sheets start in the set');
    const before = ids(pagesOf('gold14k')), beforeK = ids(pagesOf('gold10k'));
    const plan = Gate.mergePlan('gold14k');
    assert.equal(plan.target, s1); assert.deepEqual(plan.sources, [s2]); assert.equal(plan.moving, 3);
    assert.equal(Gate.mergePlan('gold14k').pages.length, 2);
    const layout = JSON.stringify(s1.placements), pins = JSON.stringify(s1.charms.map(c => [c.pinned, c.arrivalPin]));
    const ran = await Gate.mergeSheets('gold14k', 'move');
    assert.equal(ran, true, 'the merge ran');
    assert.deepEqual(pagesOf('gold14k'), [s1], 'Sheet 2 is emptied and removed');
    assert.deepEqual(ids([s1]), before, 'every charm of both sheets is on Sheet 1');
    assert.equal(w.calls.nest[0], s1, 'Sheet 1 takes them');
    assert.equal(w.calls.append[0], true, 'as it takes arrivals: its placements locked, the new charms around them');
    assert.equal(JSON.stringify(s1.placements.slice(0, 3)), layout, "Sheet 1's charms stay exactly where they were");
    assert.equal(s1.placements.length, 6, 'and the others go into its free room');
    assert.equal(JSON.stringify(s1.charms.slice(0, 3).map(c => [c.pinned, c.arrivalPin])), pins, "Sheet 1's own pins are kept");
    assert(s1.charms.slice(3).every(c => !c.pinned && !c.arrivalPin), 'the charms that came may go anywhere in the free room');
    assert.deepEqual([...s1.charms.map(c => c.id)], ['a', 'b', 'c', 'd', 'e1', 'e2'], 'they come after its own, oldest orders first');
    assert.deepEqual(s1.backPool.map(b => b.poolId).sort(), before.map(id => 'p-' + id), 'the approved backs go with their charms');
    assert.equal(s1.solidPick, true, 'Sheet 1 keeps its own tick');
    assert.deepEqual([...run.intakeRecovery.retire], ['gold14k-s2'], "Sheet 2's saved record is archived at the next checkpoint");
    assert.equal(w.saved.get('gold14k-s2').draft, true, 'Sheet 2 left the set before it went');
    assert.equal(w.saved.get('gold14k-s1').draft, true, 'Sheet 1 left the set while it takes the charms');
    assert(!w.sets[0].sheetIds.includes('gold14k-s2'), 'the set no longer names Sheet 2');
    assert(w.calls.options >= 1, 'the run takes up its steps again (a new QR label, the archive)');
    assert.deepEqual(ids(pagesOf('gold10k')), beforeK, '10K is not touched');
    assert.equal(Gate.mergePlan('gold14k'), null, 'one sheet left: nothing more to merge');
    // the set takes Sheet 1 back, with a new label, once it is nested and verified
    s1.outputs = { ai: 'a' }; s1.persistedDone = true; s1.dirty = false;
    await Gate.assemble(run);
    assert.deepEqual(w.sets[0].sheetIds, ['gold14k-s1'], 'Sheet 1 is back in the set');
    // too little room: what does not fit stays on Sheet 2, at the spot it had there, never on a new sheet
    const t2 = sheet('gold14k', 2, [charm('f', '105', 6), charm('g', '106', 7)]), gWas = t2.placements.find(p => p.id === 'g');
    const layout2 = JSON.stringify(s1.placements), retired = run.intakeRecovery.retire.length;
    w.world.room = 7; w.calls.nest.length = 0;
    await Gate.mergeSheets('gold14k', 'move');
    assert.deepEqual(pagesOf('gold14k'), [s1, t2], 'Sheet 2 keeps what did not fit: no new sheet');
    assert.deepEqual([...t2.charms.map(c => c.id)], ['g'], 'the youngest order stays');
    assert.deepEqual(t2.charms[0].pinned, { cxPt: gWas.cxPt, cyPt: gWas.cyPt, angle: gWas.angle }, 'at the spot it had on Sheet 2');
    assert.equal(JSON.stringify(s1.placements.slice(0, 6)), layout2, "Sheet 1's charms stay where they were again");
    assert.deepEqual([...s1.charms.map(c => c.id)], ['a', 'b', 'c', 'd', 'e1', 'e2', 'f'], 'Sheet 1 took what fits');
    assert.deepEqual(ids(pagesOf('gold14k')), [...before, 'f', 'g'].sort(), 'no charm lost');
    assert(pagesOf('gold14k').every(p => !p._mergeNext && !p._mergeSpots), 'the merge leaves nothing behind on the sheets');
    assert.equal(run.intakeRecovery.retire.length, retired, "Sheet 2's record stays: the sheet stays");
    console.log("  ✓ move all onto Sheet 1: its charms stay where they are, the others go into its free room, what does not fit stays on Sheet 2, an empty sheet goes");
  }

  /* ── 2 · Re-nest all the sheets, oldest first ── */
  {
    const w = world(), { Gate, charm, sheet, pagesOf, run } = w;
    const s1 = sheet('gold14k', 1, [charm('a', '100', 5), charm('b', '101', 1)]);
    const s2 = sheet('gold14k', 2, [charm('c1', '102', 2), charm('c2', '102', 2), charm('d', '103', 9)]);
    const s3 = sheet('gold14k', 3, [charm('e', '104', 3)], { cloud: null });
    const all = ids(pagesOf('gold14k'));
    w.world.room = 4;
    assert.equal(Gate.mergePlan('gold14k').pages.length, 3);
    await Gate.mergeSheets('gold14k', 'renest');
    const pages = pagesOf('gold14k');
    assert.deepEqual(ids(pages), all, 'every charm is kept across the sheets');
    assert.deepEqual(pages, [s1, s2], 'the sheets refill in turn; the one left empty goes');
    assert.deepEqual([...s1.charms.map(c => c.id)].sort(), ['b', 'c1', 'c2', 'e'], 'Sheet 1 takes the oldest orders');
    assert.deepEqual([...s2.charms.map(c => c.id)].sort(), ['a', 'd'], 'Sheet 2 the rest');
    for (const p of pages) for (const c of p.charms.filter(c => c.order === '102')) assert.equal(p, s1, 'the copies of one order stay together');
    assert.deepEqual(w.calls.nest.slice(0, 2), [s1, s2], 'Sheet 1 first, then Sheet 2 with what did not fit');
    assert.equal(w.calls.append[0], false, 'from scratch: nothing is kept where it was');
    assert(pages.every(p => !p._mergeNext), 'the rerun leaves no order behind it');
    assert(!(run.intakeRecovery?.retire || []).includes('gold14k-s3'), 'a sheet never saved has no record to archive');
    // everything fits on Sheet 1: both later sheets go
    const w2 = world(), a = w2.sheet('gold14k', 1, [w2.charm('a', '1', 1)]); w2.sheet('gold14k', 2, [w2.charm('b', '2', 2)]); w2.sheet('gold14k', 3, [w2.charm('c', '3', 3)]);
    await w2.Gate.mergeSheets('gold14k', 'renest');
    assert.deepEqual(w2.pagesOf('gold14k'), [a], 'all on Sheet 1');
    assert.deepEqual([...w2.run.intakeRecovery.retire].sort(), ['gold14k-s2', 'gold14k-s3'], 'both saved records archived');
    console.log('  ✓ re-nest all sheets: every charm kept, oldest orders first, sheets refilled in turn, empty ones removed');
  }

  /* ── 3 · Locked sheets are left alone ── */
  {
    const w = world(), { Gate, charm, sheet, pagesOf, sets, run } = w;
    const s1 = sheet('gold14k', 1, [charm('a', '1', 1)]), s2 = sheet('gold14k', 2, [charm('b', '2', 2)]), s3 = sheet('gold14k', 3, [charm('c', '3', 3)]);
    sets.push({ setId: 'old', runId: 'r', seq: 1, committedAt: 1, group: 'dispatch', sheetIds: ['gold14k-s1'], labelFiles: [], materials: [], orders: {} });
    const plan = Gate.mergePlan('gold14k');
    assert.equal(plan.target, s2, 'a sheet in a committed set is not merged into');
    assert.deepEqual(plan.sources, [s3]);
    s3.roseCutAt = 1; assert.equal(Gate.mergePlan('gold14k'), null, 'a cut sheet is not merged'); delete s3.roseCutAt;
    s3.recalled = { placedCount: 1 }; assert.equal(Gate.mergePlan('gold14k'), null, 'a recalled sheet is not merged'); delete s3.recalled;
    s3.laserDoneAt = 1; assert.equal(Gate.mergePlan('gold14k'), null, 'a sheet the laser cut is not merged'); delete s3.laserDoneAt;
    s3.keepRelease = { at: Date.now() }; assert.equal(Gate.mergePlan('gold14k'), null, 'a sheet the sheet window is rewriting is not merged'); delete s3.keepRelease;
    s3.runId = 'other'; assert.equal(Gate.mergePlan('gold14k'), null, "another run's sheet is not merged"); s3.runId = 'r';
    run.status = 'complete'; assert.equal(Gate.mergePlan('gold14k'), null, 'a finished run changes nothing'); run.status = 'processed';
    // a set being committed: nothing merges meanwhile
    let release; const commit = w.ops.run({ key: 'commit:r', label: 'commit', resources: ['x'] }, () => new Promise(r => { release = r; }));
    await tick(5); assert.equal(Gate.mergePlan('gold14k'), null, 'not while a set commits'); release(); await commit; await tick(5);
    // a sheet of the metal nesting (here the committed one): refused in its turn, nothing moves
    const snap = JSON.stringify(pagesOf('gold14k').map(p => p.charms.map(c => c.id)));
    s1.status = 'nesting';
    const refused = await Gate.mergeSheets('gold14k', 'move');
    assert.equal(refused, false, 'refused while a sheet of the metal nests');
    assert.equal(JSON.stringify(pagesOf('gold14k').map(p => p.charms.map(c => c.id))), snap, 'and nothing moved');
    assert(w.calls.toasts.some(t => /nesting or saving/.test(t)), 'it says why');
    s1.status = 'complete';
    // the merge itself leaves Sheet 1 (committed) as it was
    await Gate.mergeSheets('gold14k', 'move');
    assert.deepEqual(pagesOf('gold14k').map(p => [...p.charms.map(c => c.id)]), [['a'], ['b', 'c']], 'Sheet 1 kept, Sheet 3 onto Sheet 2');
    assert.equal(s1.status, 'complete', 'the committed sheet is not nested again'); assert(!w.calls.nest.includes(s1));
    console.log('  ✓ locked sheets left alone: committed, cut, recalled, laser-cut, being rewritten, another run, a committing set, a busy metal');
  }

  /* ── 4 · The page's overflow follows the rerun's order ── */
  {
    const started = [], added = [];
    const S = { settings: {}, sheets: { gold14k: { pages: [] } } };
    const ctx = { S, window: {}, manualSheetClosed: p => !!p.closed, computeSaturation() {}, renderCard() {}, agent() {}, toast() {}, labelOf: m => m, renderRail() {}, updateTopSub() {},
      orderSummary: () => ({ text: '' }), startNest: p => started.push(p), addPage: m => { const pg = { metal: m, page: S.sheets[m].pages.length + 1, charms: [], placements: [], status: 'idle' }; S.sheets[m].pages.push(pg); added.push(pg); return pg; } };
    vm.createContext(ctx); vm.runInContext(slice('function overflowToNextSheet(', 'function inflatedArea('), ctx);
    const pg = (page, charms, more = {}) => ({ metal: 'gold14k', page, charms: charms.map(id => ({ id })), placements: [], rejects: [], status: 'idle', ...more });
    const a = pg(1, ['x', 'y', 'z'], { placements: [{ id: 'x' }], rejects: ['y', 'z'], status: 'complete', verification: { ok: true } }), b = pg(2, []), c = pg(3, []);
    S.sheets.gold14k.pages = [a, b, c];
    a._mergeNext = b; ctx.overflowToNextSheet(a, false);
    assert.deepEqual(b.charms.map(x => x.id), ['y', 'z'], 'the rerun order: the next sheet of the rerun takes the overflow, not the newest');
    assert.equal(started.at(-1), b); assert.equal(c.charms.length, 0);
    // the next sheet of the rerun busy: it takes them after its own work
    b.status = 'nesting'; a.charms.push({ id: 'w' }); a.placements = [{ id: 'x' }]; a.rejects = ['w']; started.length = 0;
    ctx.overflowToNextSheet(a, false);
    assert.deepEqual([...b.feedWait], ['w'], 'a busy sheet of the rerun takes them after its own'); assert.equal(started.length, 0); assert.equal(added.length, 0, 'no new sheet');
    // without the rerun: as before, the metal's newest sheet
    delete a._mergeNext; a.charms.push({ id: 'v' }); a.rejects = ['v']; b.status = 'idle';
    ctx.overflowToNextSheet(a, false);
    assert.deepEqual(c.charms.map(x => x.id), ['v'], 'without a rerun the newest sheet takes it, as before');
    // a charm a Move all could not fit goes back to the spot it had on the sheet it came from (Gate: _mergeSpots)
    const d = pg(4, []); S.sheets.gold14k.pages.push(d);
    a.charms.push({ id: 'u', pinned: { cxPt: 1, cyPt: 1, angle: 0 }, arrivalPin: true }, { id: 't', pinned: { cxPt: 2, cyPt: 2, angle: 0 }, arrivalPin: true });
    a.rejects = ['u', 't']; a._mergeNext = d; d._mergeSpots = new Map([['u', { cxPt: 11, cyPt: 22, angle: 90 }]]);
    ctx.overflowToNextSheet(a, false);
    const u = d.charms.find(x => x.id === 'u'), t = d.charms.find(x => x.id === 't');
    assert.deepEqual({ ...u.pinned }, { cxPt: 11, cyPt: 22, angle: 90 }, 'back at its old spot'); assert.equal(u.arrivalPin, true);
    assert(!t.pinned && !t.arrivalPin, "one the sheet never held has no spot kept for it");
    console.log('  ✓ overflow follows the rerun: next sheet of the rerun, after its own work when busy; a charm back at its old spot; unchanged otherwise');
  }
  /* ── 5 · In the browser: the panel closes, the card's line says what it does, and the merge is seen, then gone ── */
  {
    const http = require('node:http');
    const { chromium } = require(path.join(process.env.PW_DIR || path.join(root, 'node_modules'), 'playwright-core'));
    const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.mjs': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.svg': 'image/svg+xml' };
    const server = http.createServer((req, res) => {
      const u = decodeURIComponent(req.url.split('?')[0]);
      if (u.startsWith('/.netlify/functions/')) { res.writeHead(404, { 'Content-Type': 'application/json' }); return res.end('{"error":"no functions in the test server"}'); }
      const f = path.join(root, u === '/' ? 'charm-nest-1.html' : u);
      if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
      res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' }); fs.createReadStream(f).pipe(res);
    }).listen(0);
    const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
    // a 14K card with two sheets (6 charms placed on Sheet 1, 4 on Sheet 2); the nest itself is a stand-in that keeps a
    // sheet's placements when it takes arrivals and puts the others down one by one
    const setup = () => {
      const { S, addPage, showPage, renderCard } = CN, MM = 72 / 25.4;
      CharmNestPDF.drawCharm = (g, c, tx, k) => { const [x, y] = tx(0, 0); g.beginPath(); g.arc(x, y, 3.5 * MM * k, 0, 7); g.strokeStyle = '#d0312d'; g.stroke(); };
      const real = CharmNestPDF.pathToCanvas; CharmNestPDF.pathToCanvas = (g, p, tx) => { if (p && p.circle) { const [x, y] = tx(0, 0), [x1] = tx(p.r, 0); g.moveTo(x1, y); g.arc(x, y, Math.abs(x1 - x), 0, 7); return; } return real(g, p, tx); };
      let n = 0;
      const charm = (order, orderDate) => { const i = n++; return { id: 'c' + i, name: 'Charm ' + i, index: i, order, orderDate, poolId: 'p' + i, thumb: 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20"><circle cx="10" cy="10" r="8" fill="none" stroke="#c8a24e"/></svg>'), sourceId: 's', centerPt: [0, 0], bbox: [-10, -10, 10, 10], outline: { circle: 1, r: 3.5 * MM }, members: [], widthPt: 7 * MM, heightPt: 7 * MM, areaPt2: 110, qty: 1, metal: 'gold14k', arrivedAt: i }; };
      const lay = list => list.map((c, i) => ({ id: c.id, cxPt: (6 + i * 9) * MM, cyPt: 6 * MM, angle: 0, wPt: 7 * MM, hPt: 7 * MM, xPt: (2.5 + i * 9) * MM, yPt: 2.5 * MM }));
      S.settings.stock = S.settings.stock || {}; S.settings.stock.gold14k = [96 / 25.4, 46 / 25.4];
      const p1 = S.sheets.gold14k;
      p1.charms = Array.from({ length: 6 }, (_, i) => charm(String(100 + i), 1 + i)); p1.placements = lay(p1.charms);
      Object.assign(p1, { status: 'complete', dirty: false, verification: { ok: true }, persistedDone: true, runId: null });
      const p2 = addPage('gold14k');
      p2.charms = Array.from({ length: 4 }, (_, i) => charm(String(200 + i), 50 + i)); p2.placements = lay(p2.charms);
      Object.assign(p2, { status: 'complete', dirty: false, verification: { ok: true }, persistedDone: true });
      window.__nests = [];
      window.startNest = sh => {
        __nests.push([sh.page, !!sh.appendOnly, sh.placements.map(p => p.id)]);
        Object.assign(sh, { status: 'nesting', startedAt: performance.now(), persistedDone: false, dirty: false, stage: 'Placing', progressKind: 'place', progress: [sh.placements.length, sh.charms.length] });
        renderCard(sh);
        const keep = new Set(sh.appendOnly ? sh.placements.map(p => p.id) : []), todo = sh.charms.filter(c => !keep.has(c.id)); if (!sh.appendOnly) sh.placements = [];
        const step = () => {
          const c = todo.shift();
          if (!c) { Object.assign(sh, { status: 'complete', stage: '', verification: { ok: true } }); renderCard(sh); setTimeout(() => { sh.persistedDone = true; renderCard(sh); }, 150); return; }
          const i = sh.placements.length; sh.placements.push({ id: c.id, cxPt: (6 + (i % 10) * 9) * MM, cyPt: (6 + Math.floor(i / 10) * 9) * MM, angle: 0, wPt: 7 * MM, hPt: 7 * MM });
          sh.progress = [sh.placements.length, sh.charms.length]; renderCard(sh); setTimeout(step, 120);
        };
        setTimeout(step, 150);
      };
      showPage('gold14k', 0);
      document.querySelector('.sheetCard[data-m="gold14k"]').scrollIntoView({ block: 'start' });
      return JSON.stringify(p1.placements);
    };
    const card = '.sheetCard[data-m="gold14k"]';
    const open = async (reduced) => {
      const ctx = await browser.newContext({ viewport: { width: 1400, height: 950 }, reducedMotion: reduced ? 'reduce' : 'no-preference' });
      await ctx.route(u => /^https?:$/.test(u.protocol) && !['127.0.0.1', 'localhost'].includes(u.hostname), r => r.abort());
      const page = await ctx.newPage(), errors = []; page.on('pageerror', e => errors.push(e.message));
      await page.goto(`http://127.0.0.1:${server.address().port}/charm-nest-1.html`);
      await page.waitForFunction(() => window.CN && window.Gate && window.Motion && document.querySelector('.sheetCard[data-m="gold14k"]'));
      const layout = await page.evaluate(setup); await page.waitForTimeout(250);
      return { ctx, page, errors, layout };
    };
    const press = async (page, kind) => {
      await page.click(`${card} .sheetOptionsBtn`);
      await page.click(`dialog.osDlg [data-solid="merge-${kind}"]`);
      await page.click(`dialog.osDlg [data-solid="merge-go"]`);
    };
    const leftovers = page => page.evaluate(() => ({ live: Gate.mergeFx().live, layer: document.querySelectorAll('.mergeFx, [class*="mergeFx"]').length,
      hiding: [...document.querySelectorAll('style')].some(s => s.textContent.includes('[data-r="queue"] img[data-cid')),
      running: document.querySelector('.sheetCard[data-m="gold14k"] .shPreviewWrap').getAnimations().length }));
    try {
      // Move all: the panel closes, the line and the size say what goes on, the sheets are seen side by side, then one
      {
        const { ctx, page, errors, layout } = await open(false);
        await press(page, 'move');
        const during = await page.evaluate(() => ({ open: !!document.querySelector('dialog.osDlg[open]'),
          stage: document.querySelector('.sheetCard[data-m="gold14k"] [data-r="stage"]').textContent }));
        assert.equal(during.open, false, 'the Options panel closes first: the sheet is in full view');
        assert.match(during.stage, /^Merge · /, "the card's own line says what the merge does");
        await page.waitForSelector(`${card} .mergeFx`, { timeout: 1500 });
        const scene = await page.evaluate(() => { const fx = document.querySelector('.sheetCard[data-m="gold14k"] .mergeFx'); return { sheets: fx.querySelectorAll('.mergeFxSheet').length, pieces: fx.querySelectorAll('.mergeFxPiece').length, tags: [...fx.querySelectorAll('.mergeFxTag')].map(t => t.textContent), live: Gate.mergeFx().live }; });
        assert.equal(scene.sheets, 2, 'both sheets are seen, side by side');
        assert.equal(scene.pieces, 4, "Sheet 2's charms are the pieces that fly (Sheet 1's stay put)");
        assert.deepEqual(scene.tags, ['Sheet 16', 'Sheet 24'], 'each under its name and how many charms it holds');
        assert.equal(scene.live, 1);
        await page.waitForSelector(`${card} .mergeFx`, { state: 'detached', timeout: 6000 });
        assert.deepEqual(await leftovers(page), { live: 0, layer: 0, hiding: false, running: 0 }, 'the scene leaves nothing behind');
        await page.waitForFunction(() => [...document.querySelectorAll('.mNote')].some(n => /Done · Sheet 1: 10 charms/.test(n.textContent)), null, { timeout: 8000 });
        const after = await page.evaluate(() => ({ pages: CN.pagesOf('gold14k').map(p => [p.page, p.charms.length, p.placements.length]), first: JSON.stringify(CN.pagesOf('gold14k')[0].placements.slice(0, 6)), nests: window.__nests }));
        assert.deepEqual(after.pages, [[1, 10, 10]], 'all on Sheet 1; the empty Sheet 2 is gone');
        assert.equal(after.first, layout, "Sheet 1's charms stayed exactly where they were");
        assert.equal(after.nests[0][1], true, 'Sheet 1 took them as arrivals');
        assert.deepEqual(errors, []);
        await ctx.close();
      }
      // reduced motion: none of it, the merge the same
      {
        const { ctx, page, errors } = await open(true);
        await press(page, 'move');
        for (let i = 0; i < 12; i++) { assert.equal(await page.evaluate(() => document.querySelectorAll('.mergeFx').length), 0, 'reduced motion: no scene'); await page.waitForTimeout(80); }
        await page.waitForFunction(() => [...document.querySelectorAll('.mNote')].some(n => /Done · Sheet 1: 10 charms/.test(n.textContent)), null, { timeout: 8000 });
        assert.deepEqual(errors, []);
        await ctx.close();
      }
      // a Re-nest, and the tab hidden while it plays: it ends at once and leaves nothing behind; the merge goes on
      {
        const { ctx, page, errors } = await open(false);
        await press(page, 'renest');
        await page.waitForSelector(`${card} .mergeFx`, { timeout: 1500 });
        assert.equal(await page.evaluate(() => document.querySelectorAll('.sheetCard[data-m="gold14k"] .mergeFx .mergeFxPiece').length), 10, "a re-nest lifts every charm of both sheets");
        const gone = await page.evaluate(() => {
          Object.defineProperty(document, 'hidden', { configurable: true, get: () => true }); document.dispatchEvent(new Event('visibilitychange'));
          const now = { layer: document.querySelectorAll('.mergeFx').length, live: Gate.mergeFx().live, hiding: [...document.querySelectorAll('style')].some(s => s.textContent.includes('[data-r="queue"] img[data-cid')) };
          delete document.hidden; document.dispatchEvent(new Event('visibilitychange')); return now;
        });
        assert.deepEqual(gone, { layer: 0, live: 0, hiding: false }, 'a hidden tab ends the scene at once');
        await page.waitForFunction(() => [...document.querySelectorAll('.mNote')].some(n => /Done · Sheet 1: 10 charms/.test(n.textContent)), null, { timeout: 8000 });
        assert.deepEqual(await page.evaluate(() => CN.pagesOf('gold14k').map(p => [p.page, p.charms.length, p.placements.length])), [[1, 10, 10]], 'the re-nest went on to the end');
        assert.deepEqual(errors, []);
        await ctx.close();
      }
    } finally { await browser.close(); server.close(); }
    console.log('  ✓ in the browser: the panel closes, the line and the size say a merge runs, the sheets are seen merging and nothing is left behind; reduced motion and a hidden tab show none of it');
  }
  console.log('merge-sheets: ok');
})().catch(e => { console.error(e); process.exit(1); });
