// Adversarial: the commit of a set when the connection drops BETWEEN its steps (ADV-LASER, Paul, 15 Oct 2026: "resilient against lost internet
// connections, interruptions"). The real Sets.commit / record / taken / undo and save, cut out of charm-nest-bridge.js, the real commit step of the
// run loop, and the REAL complete.commit / complete.undo handlers and commit memory of the Design Station (design-1.html), over a stand-in cloud and
// stand-in station rows. Nothing leaves the machine.
//   A  the station marks the orders design-complete, then the set's save drops: the same page's next try only finishes the record (the station is
//      not asked again, the set is saved once, the day and time of the first try stay, one efficiency note);
//   B  the same, but the page was reloaded before the retry (no line for an order the station finished; the set comes back from the cloud open):
//      the station's memory of the run's commits gives those orders back, the set is closed with exactly them and their pool rows are committed;
//   C  the station still lists some orders (refused by it as done or locked): the finished ones are recovered, a genuinely refused one stays refused,
//      and nothing is ever committed through the probe (an order the station did not finish is never "taken");
//   D  the pool update drops (before the set saves): the retry finishes both;
//   E  Undo set: the station reopened the orders but the set's save dropped, pressing Undo again finishes it; any other station error still stops it.
//   node tests/charm-nest/adv-laser-commit.cjs
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const root = path.join(__dirname, '../..');
const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), html = fs.readFileSync(path.join(root, 'design-1.html'), 'utf8');
const cutFrom = (src, from, to) => { const a = src.indexOf(from), b = src.indexOf(to, a + from.length); assert(a >= 0 && b > a, `missing ${from} .. ${to}`); return src.slice(a, b); };
const plain = x => JSON.parse(JSON.stringify(x));

/* ── the station: the page's own commit memory and its two handlers, over stand-in rows ── */
function makeStation() {
  const store = new Map();
  const c = vm.createContext({ console, localStorage: { getItem: k => store.has(k) ? store.get(k) : null, setItem: (k, v) => store.set(k, String(v)) }, JSON, Set, Map, Date, Promise, Error, Array });
  vm.runInContext(`
    const LSK = { commits: 'commits', selected: 'selected' };
    let lastClearedReceipts = [], __commitInFlight = 0, __undoInFlight = 0, __sweepInFlight = 0, pendingJobs = null, pendingLists = null;
    const S = { previewed: null, sorterSelected: new Set(), runId: null };
    const open = new Set(), completedOrders = new Set(), selectedOrders = new Set(), locked = new Set(), commitCalls = [], undoCalls = [];
    let failCommit = false, failUndo = null;
    const Cursor = { act: async () => {}, glide() {} }, ticker = () => {}, closeDlg = () => {}, $ = () => null;
    const knownReceiptIds = () => new Set(open), isLockedElsewhere = id => locked.has(id);
    const commitCompletion = async (ids, o) => { if (failCommit) throw new Error('markBritesDesigned failed'); commitCalls.push({ ids: ids.slice(), setId: o.setId, runId: o.runId }); ids.forEach(id => { open.delete(id); selectedOrders.delete(id); completedOrders.add(id); }); return { completed: ids.slice() }; };
    const handleUndoComplete = async () => { if (failUndo) throw new Error(failUndo); for (const id of lastClearedReceipts) { completedOrders.delete(id); open.add(id); undoCalls.push(id); } lastClearedReceipts = []; };
    const rowFor = id => open.has(id) ? { id } : null;
    ${cutFrom(html, '  const readJson = (k, d) =>', '  function rememberSorterSelection()')}
    function rememberSorterSelection() {}
    const cmds = {
      // (the page's ui.select and complete.preview need its DOM: only their refusals and bookkeeping are stood in; complete.commit and complete.undo are the page's own)
      async 'ui.select'(a) { const selected = [], refused = []; for (const id of (a.receiptIds || []).map(String)) { const row = rowFor(id); if (!row) { refused.push({ id, reason: 'order is not in the list' }); continue; } if (isLockedElsewhere(id)) { refused.push({ id, reason: 'order is locked at another station' }); continue; } if (completedOrders.has(id)) { refused.push({ id, reason: 'order is already design-complete' }); continue; } selectedOrders.add(id); S.sorterSelected.add(id); selected.push(id); } return { selected, refused }; },
      async 'complete.preview'(a) { const ids = (a.receiptIds || []).map(String); const missing = ids.filter(id => !selectedOrders.has(id)); if (missing.length) throw new Error('select the orders first'); S.previewed = new Set(ids); return { jobs: [], notes: {} }; },
      ${cutFrom(html, '    async "complete.commit"(a) {', '    async "complete.undo"(a) {')}
      ${cutFrom(html, '    async "complete.undo"(a) {', '    async release()')}
    };
  `, c);
  return { c, store, run: (cmd, args) => vm.runInContext(`cmds[${JSON.stringify(cmd)}](${JSON.stringify(args)})`, c).then(plain),
    ev: code => vm.runInContext(code, c) };
}

/* ── the sorter: the page's own Sets.commit / taken / record / undo / save and its run loop's commit step ── */
function makeSorter(station, cloud) {
  const setsSection = cutFrom(bridge, '  /** §8.4 · the real lock, the preview, the commit', '  /** The Library\'s Sets view');
  const saveFn = cutFrom(bridge, '  async function save(set, context)', '  const sheetsOf = set =>');
  const stepFn = cutFrom(bridge, '      case "commit": {', '      case "complete": {');
  const c = vm.createContext({ console, JSON, Set, Map, WeakSet, Date, Promise, Error, Array, Object, assert, requestAnimationFrame: fn => fn(), setTimeout: (fn, ms) => { const t = setTimeout(fn, ms); if (t.unref) t.unref(); return t; } });
  c.link = { down: false, calls: [] }; c.cloud = cloud;
  c.stationCall = async (cmd, args) => { if (c.link.down) throw new Error(cmd + ': the Design Station did not answer in time'); c.link.calls.push(cmd); return station.run(cmd, args); };
  c.cloudApi = async (name, body) => cloud.api(body);
  vm.runInContext(`
    const window = {}, S = { cloud: { ok: true }, settings: { autoCommit: 'on' } }, B = { run: null, pool: { rows: new Map() }, sets: new Map() };
    const today = () => cloud.day, employeeName = () => 'Tester', stampWho = () => ({ by: 'Tester', device: 'charm-nest-1' }), agent = () => {};
    const humanActs = [], humanAct = { set: (undo, ids, name) => humanActs.push({ undo, ids: ids.slice(), name }) };
    const rows = [], poolWrites = [];
    const Orders = { rows: () => rows, render() {}, revalidate: async () => ({ changed: [] }) }, Review = { add() {} }, Engrave = { items: () => new Map() };
    const Pool = { update: async (ids, patch) => { if (cloud.failPool) { cloud.failPool--; throw new Error('Failed to fetch'); } poolWrites.push({ ids: ids.slice(), state: patch.state }); } };
    const RunCtl = { save: async () => {}, renderBanner() {} };
    const api = (name, body) => cloudApi(name, body);
    const DesignLink = { ensure: async () => {}, call: (cmd, args, opts) => stationCall(cmd, args) };
    const Gate = { modern: () => true, flush: async () => {}, refreshMembership() {} };
    let evaluated = () => ({ committable: [], held: {}, gone: [] });
    const evaluate = set => evaluated(set), validateRelease = () => {}, deferSet = async () => {}, deferred = [];
    ${saveFn}
    ${setsSection}
    const unrecordedNow = s => typeof unrecorded !== 'undefined' && unrecorded.has(s);
    const Sets = { ofRun: runId => [...B.sets.values()].filter(s => s.runId === runId), releaseIssue: () => null, unrecorded: unrecordedNow, commit };
    // the run loop's own commit step (r = the run), as the loop calls it
    async function step(r) { switch ('commit') { ${stepFn} } }
  `, c);
  return c;
}

/* ── the cloud: the set records the page saves, and the pool rows; a dropped connection is a thrown "Failed to fetch" ── */
function makeCloud() {
  const cloud = { day: '2026-10-15', sets: new Map(), saves: [], failSet: 0, failPool: 0 };
  cloud.api = async b => {
    if (b.op === 'setUpdate') {
      if (cloud.failSet) { cloud.failSet--; throw new Error('Failed to fetch'); }
      assert(!('laserDoneAt' in b.patch) && !('laserDoneBy' in b.patch), 'a commit never writes the laser mark');
      cloud.saves.push({ setId: b.setId, status: b.patch.status, committed: b.patch.committed, committedAt: b.patch.committedAt });
      cloud.sets.set(b.setId, plain(Object.assign({}, cloud.sets.get(b.setId) || {}, b.patch)));
      return { ok: true };
    }
    return {};
  };
  return cloud;
}

const ORDERS = ['4300000001', '4300000002', '4300000003'];
const pool = rid => rid + '_5000000001_1';
function seed(sorter, station, { listed = true } = {}) {
  vm.runInContext(`
    B.run = { runId: 'run-1', status: 'running', step: 'commit' };
    const set = { setId: 'set-2026-10-15-1', runId: 'run-1', group: 'dispatch', seq: 1, day: '2026-10-15', name: 'Set-1', folder: 'charmnest/sets/2026-10-15/Set-1', status: 'labelled', sheetIds: ['gold-1'], materials: ['gold'], labelFiles: [{ path: 'l.png', url: 'u', sheet: 'S1', sheetId: 'gold-1' }], labels: { files: [{ path: 'l.png', url: 'u', sheet: 'S1' }], pdf: { url: 'p' } },
      orders: Object.fromEntries(${JSON.stringify(ORDERS)}.map(rid => [rid, { held: null, lines: { '5000000001': { transactionId: '5000000001', sku: 'A', copies: [{ copy: 1, sheetId: 'gold-1', poolId: rid + '_5000000001_1' }] } } }])) };
    B.sets.set('run-1|dispatch', set);
    for (const rid of ${JSON.stringify(ORDERS)}) B.pool.rows.set(rid + '_5000000001_1', { orderId: rid, state: 'written' });
    ${listed ? `rows.push(...${JSON.stringify(ORDERS)}.map(rid => ({ state: 'written', poolIds: [rid + '_5000000001_1'], order: { receiptId: rid } })));` : ''}
    evaluated = () => ({ committable: rows.filter(r => r.state === 'written').map(r => r.order.receiptId), held: {}, gone: [] });
  `, sorter);
  station.ev(`for (const rid of ${JSON.stringify(ORDERS)}) open.add(rid);`);
}
const setOf = sorter => vm.runInContext(`B.sets.get('run-1|dispatch')`, sorter);
const commitStep = sorter => vm.runInContext(`step(B.run)`, sorter);
const commitCalls = station => plain(station.ev('commitCalls'));

(async () => {
  // ── A · the station committed, the set's save dropped: the same page's next try only finishes the record ──
  {
    const station = makeStation(), cloud = makeCloud(), sorter = makeSorter(station, cloud); seed(sorter, station);
    cloud.failSet = 1;
    await assert.rejects(commitStep(sorter), /Failed to fetch/, 'the dropped save stops the step, as the loop sees it');
    assert.equal(commitCalls(station).length, 1, 'the station marked the orders design-complete once');
    const first = setOf(sorter).committedAt;
    assert(first, 'the page knows the station took the orders');
    assert.equal(cloud.saves.length, 0, 'nothing reached the cloud');
    await commitStep(sorter);   // the loop tries the step again (or the person presses Resume)
    assert.equal(commitCalls(station).length, 1, 'the station is NOT asked a second time');
    assert.equal(cloud.saves.length, 1, 'the set is saved once');
    assert.equal(cloud.saves[0].status, 'complete'); assert.deepEqual(plain(cloud.saves[0].committed).sort(), ORDERS.slice().sort());
    assert.equal(setOf(sorter).committedAt, first, 'the time of the first try stays');
    assert.equal(vm.runInContext(`unrecordedNow(B.sets.get('run-1|dispatch'))`, sorter), false);
    assert.equal(setOf(sorter).savedCommit, first);
    assert.equal(plain(vm.runInContext('humanActs', sorter)).length, 1, 'one efficiency note, not two');
    assert(vm.runInContext(`rows.every(r => r.state === 'committed')`, sorter));
    await commitStep(sorter);   // a third pass changes nothing
    assert.equal(cloud.saves.length, 1, 'a set whose record saved is history: it is not saved again (the server would re-check it)'); assert.equal(commitCalls(station).length, 1);
  }
  // ── D · the pool update drops before the set saves ──
  {
    const station = makeStation(), cloud = makeCloud(), sorter = makeSorter(station, cloud); seed(sorter, station);
    cloud.failPool = 1;
    await assert.rejects(commitStep(sorter), /Failed to fetch/);
    assert.equal(cloud.saves.length, 0);
    await commitStep(sorter);
    assert.equal(commitCalls(station).length, 1); assert.equal(cloud.saves.length, 1); assert.equal(cloud.saves[0].status, 'complete');
    const writes = plain(vm.runInContext('poolWrites', sorter)); assert.equal(writes.length, 1, 'the pool rows are written once, by the try that got through');
    assert.deepEqual(writes[0].ids.sort(), ORDERS.map(pool).sort());
  }
  // ── F · the set is saved meanwhile by the next set's numbering (allocate saves a committed set whose record did not): the step does not save it a second time ──
  {
    const station = makeStation(), cloud = makeCloud(), sorter = makeSorter(station, cloud); seed(sorter, station);
    cloud.failSet = 1;
    await assert.rejects(commitStep(sorter), /Failed to fetch/);
    await vm.runInContext(`save(B.sets.get('run-1|dispatch'))`, sorter);   // (what Sets.ensure does before it numbers the next set)
    assert.equal(cloud.saves.length, 1);
    await commitStep(sorter);
    assert.equal(cloud.saves.length, 1, 'the server is not given the same committed set twice'); assert.equal(commitCalls(station).length, 1);
    assert(vm.runInContext('poolWrites', sorter).length >= 1, 'but its pool rows are still finished (a repeat of that write is harmless)');
    assert.equal(vm.runInContext(`unrecordedNow(B.sets.get('run-1|dispatch'))`, sorter), false);
  }
  // ── G · the cloud is marked offline when the record is due (save() does nothing then): the step says so instead of reporting the set recorded, and finishes once ──
  {
    const station = makeStation(), cloud = makeCloud(), sorter = makeSorter(station, cloud); seed(sorter, station);
    vm.runInContext('S.cloud.ok = false', sorter);
    await assert.rejects(commitStep(sorter), /network is down/, 'a record that could not be saved is not called saved');
    assert.equal(commitCalls(station).length, 1); assert.equal(cloud.saves.length, 0);
    vm.runInContext('S.cloud.ok = true', sorter);   // the connection is back
    await commitStep(sorter);
    assert.equal(commitCalls(station).length, 1, 'the station is not asked again'); assert.equal(cloud.saves.length, 1); assert.equal(cloud.saves[0].status, 'complete');
    assert.equal(plain(vm.runInContext('humanActs', sorter)).length, 1);
  }
  // ── B · the page was reloaded after the station committed: the set is open in the cloud and the orders have no line any more ──
  {
    const station = makeStation(), cloud = makeCloud(), first = makeSorter(station, cloud); seed(first, station);
    cloud.failSet = 99;
    await assert.rejects(commitStep(first), /Failed to fetch/);   // the connection is gone for good: the page is closed
    cloud.failSet = 0;
    assert.equal(commitCalls(station).length, 1);
    const reloaded = makeSorter(station, cloud); seed(reloaded, station, { listed: false });   // a new page: the set as the cloud holds it, no lines for the finished orders
    station.ev(`for (const rid of ${JSON.stringify(ORDERS)}) open.delete(rid);`);   // (the station lists only open orders)
    await commitStep(reloaded);
    assert.equal(commitCalls(station).length, 1, 'the station is not asked to commit them again');
    assert.equal(cloud.saves.length, 1); assert.equal(cloud.saves[0].status, 'complete', 'the set is closed, not "complete-with-holds" with nothing committed');
    assert.deepEqual(plain(cloud.saves[0].committed).sort(), ORDERS.slice().sort());
    assert.deepEqual(plain(vm.runInContext('poolWrites', reloaded))[0].ids.sort(), ORDERS.map(pool).sort(), 'their pool rows are committed from the set\'s own copies');
    assert.equal(plain(vm.runInContext('humanActs', reloaded)).length, 1);
    // and a loop pass after that leaves it alone
    await commitStep(reloaded); assert.equal(cloud.saves.length, 1);
  }
  // ── C · the station still lists some orders: finished ones recovered, a refused one stays refused, nothing is committed through the probe ──
  {
    const station = makeStation(), cloud = makeCloud(), sorter = makeSorter(station, cloud); seed(sorter, station);
    station.ev(`locked.add('${ORDERS[2]}')`);
    cloud.failSet = 1;
    await assert.rejects(commitStep(sorter), /Failed to fetch/);
    assert.deepEqual(commitCalls(station)[0].ids.sort(), ORDERS.slice(0, 2).sort(), 'the locked order was never committed');
    // the page reloads: the committed two are gone from the station's list, the locked one is still there; the page still holds all three lines as written
    const reloaded = makeSorter(station, cloud); seed(reloaded, station);
    station.ev(`open.delete('${ORDERS[0]}'); open.delete('${ORDERS[1]}');`);   // (already so)
    await commitStep(reloaded);
    assert.equal(commitCalls(station).length, 1, 'still one station commit');
    const saved = cloud.saves[cloud.saves.length - 1];
    assert.equal(saved.status, 'complete-with-holds'); assert.deepEqual(plain(saved.committed).sort(), ORDERS.slice(0, 2).sort(), 'only the orders the station finished are recorded');
    const refusedIds = plain(setOf(reloaded).refused).map(x => x.id); assert.deepEqual(refusedIds, [ORDERS[2]]);
    // the probe alone never commits: an order the station did not finish is never "taken"
    const q = await station.run('complete.commit', { receiptIds: [ORDERS[2], '4399999999'], runId: 'run-1', probe: true });
    assert.deepEqual(q.taken, []); assert.equal(commitCalls(station).length, 1);
    const other = await station.run('complete.commit', { receiptIds: [ORDERS[0]], runId: 'another-run', probe: true });
    assert.deepEqual(other.taken, [], 'another run\'s memory is not this run\'s');
    // a station that does not know the flag still commits nothing
    await assert.rejects(station.run('complete.commit', { receiptIds: [ORDERS[2]], runId: 'run-1' }), /without a preview/);
  }
  // ── station down for good: no recovery is invented, the old refusal stands and nothing is closed as committed ──
  {
    const station = makeStation(), cloud = makeCloud(), sorter = makeSorter(station, cloud); seed(sorter, station);
    sorter.link.down = true;
    await assert.rejects(commitStep(sorter), /did not answer in time/);
    assert.equal(cloud.saves.length, 0); assert(!setOf(sorter).committedAt);
  }
  // ── E · Undo set: the station reopened the orders, the set's save dropped ──
  {
    const station = makeStation(), cloud = makeCloud(), sorter = makeSorter(station, cloud); seed(sorter, station);
    await commitStep(sorter);
    assert.equal(cloud.saves.at(-1).status, 'complete');
    cloud.failSet = 1;
    const undo = s => vm.runInContext(`undo(B.sets.get('run-1|dispatch'))`, s);
    await assert.rejects(undo(sorter), /Failed to fetch/);
    assert.deepEqual(plain(station.ev('undoCalls')).sort(), ORDERS.slice().sort(), 'the station reopened the orders');
    assert.equal(cloud.sets.get('set-2026-10-15-1').status, 'complete', 'the cloud still says committed');
    // the page was reloaded: the set comes back from the cloud, still committed, and the person presses Undo set again. The station says "nothing to undo"
    // (they are open already): that is the goal, so the set is finished instead of stuck for good
    const reloaded = makeSorter(station, cloud); seed(reloaded, station);
    vm.runInContext(`Object.assign(B.sets.get('run-1|dispatch'), ${JSON.stringify(cloud.sets.get('set-2026-10-15-1'))}); rows.forEach(r => r.state = 'committed');`, reloaded);
    await undo(reloaded);
    assert.equal(cloud.sets.get('set-2026-10-15-1').status, 'awaiting review'); assert.deepEqual(plain(cloud.sets.get('set-2026-10-15-1').committed), []);
    assert.equal(setOf(reloaded).committedAt, null);
    assert(vm.runInContext('poolWrites', reloaded).some(w => w.state === 'written'), 'the pool rows are open again');
    // an undone set commits again, once
    vm.runInContext(`rows.forEach(r => r.state = 'written')`, reloaded);
    await commitStep(reloaded);
    assert.equal(commitCalls(station).length, 2, 'the undone orders are committed again by the station (their old result was forgotten)'); assert.equal(cloud.sets.get('set-2026-10-15-1').status, 'complete');
    // any other station error still stops the undo, and leaves the set committed
    station.ev(`failUndo = 'the station is busy'`);
    await assert.rejects(undo(reloaded), /busy/); assert.equal(cloud.sets.get('set-2026-10-15-1').status, 'complete');
  }
  console.log('Laser commit interruptions OK: station-done/set-unsaved retried once on the same page and after a reload, partial refusals, pool-update drop, probe commits nothing, Undo set finishes after a dropped save');
})().catch(e => { console.error(e); process.exitCode = 1; });
