// CLOUDDRIVE build step 12, hard-parts §2.9: a set's Undo when the cloud carries the run. The REAL Sets.undo (charm-nest-bridge.js) with the REAL
// CNViewer (charm-nest-viewer.js) as its lane gate, and the REAL complete.undo of the Design Station (design-1.html), over a stand-in cloud. Offline.
//   A  the drive off (another computer carries the run; or the carrier still says "cloud" but the owner turned the drive off): the old local undo runs, the station
//      reopens the orders, and no commitUndoAsk is sent by either page;
//   B  the cloud carries the run and "commit" is in drive.cloudSteps: exactly one commitUndoAsk { runId, setId, by }, nothing undone here, a resting run sent Resume,
//      and the person told in words: asked, then undone (from the set's commit record); undone already; refused with the reason;
//   C  the station's own look: named run carried by the cloud → it sends the press and reopens nothing; a refusal throws the cloud's words.
//   node tests/charm-nest/cloud-commit-undo-page.cjs
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const root = path.join(__dirname, '../..');
const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), html = fs.readFileSync(path.join(root, 'design-1.html'), 'utf8');
const viewer = fs.readFileSync(path.join(root, 'charm-nest-viewer.js'), 'utf8');
const cutFrom = (src, from, to) => { const a = src.indexOf(from), b = src.indexOf(to, a + from.length); assert(a >= 0 && b > a, `missing ${from} .. ${to}`); return src.slice(a, b); };
const fast = fn => setImmediate(fn);
// (values from a page's own realm, made plain so they compare with this file's)
const plain = v => (v && typeof v.then === 'function') ? v.then(plain) : v === undefined ? v : JSON.parse(JSON.stringify(v));

/** The stand-in cloud: the drive's switch, the run's carrier, and the set's commit record (the cloud's commit unit undoes the set by the second read). */
function makeCloud(o) {
  const c = { mode: o.mode, steps: o.steps || [], carrier: o.carrier, calls: [], gets: 0, undoAnswer: o.undoAnswer || { ok: true, asked: true, words: 'The cloud reopens this set\'s orders at the Design Station when the run reaches its commit step' } };
  c.answer = body => {
    c.calls.push(body);
    switch (body.op) {
      case 'driveGet': return { ok: true, mode: c.mode, drive: c.mode === 'off' ? null : { mode: c.mode, cloudSteps: c.steps } };
      case 'runCarrier': return { ok: true, mine: false, alive: true, carrier: { dev: c.carrier, at: Date.now() } };
      case 'commitUndoAsk': return c.undoAnswer;
      case 'commitGet': c.gets++; return { ok: true, runId: body.runId, journals: [{ runId: body.runId, setId: body.setId, stage: c.gets >= 2 ? 'undone' : 'done', completed: ['111', '222'], result: c.gets >= 2 ? { reopened: ['111', '222'] } : null }] };
      case 'runControl': return { ok: true, seq: 1 };
      default: throw new Error('unexpected op ' + body.op);
    }
  };
  return c;
}

/** The Design Station: its commit memory block (with the cloud look) and its real complete.undo, over stand-in rows; fetch goes to the stand-in cloud. */
function makeStation(cloud) {
  const store = new Map(), fetched = [];
  const c = vm.createContext({ console, setTimeout, clearTimeout, localStorage: { getItem: k => store.has(k) ? store.get(k) : null, setItem: (k, v) => store.set(k, String(v)) }, sessionStorage: { getItem: () => '' } });
  c.fetch = async (url, init) => { const body = JSON.parse(init.body); fetched.push(body.op); let data; try { data = cloud.answer(body); } catch (e) { data = { error: e.message }; } const status = data.error ? (data.status || 400) : 200; return { ok: status === 200, status, json: async () => data }; };
  vm.runInContext(`
    const FN = 'https://station.test/.netlify/functions', SANDBOX = false, LSK = { commits: 'commits' };
    let lastClearedReceipts = [], __commitInFlight = 0; const undoCalls = [], ticks = [];
    const completedOrders = new Set(['111', '222']), selectedOrders = new Set();
    const Cursor = { act: async () => {} }, ticker = t => ticks.push(t);
    const handleUndoComplete = async () => { for (const id of lastClearedReceipts) { completedOrders.delete(id); undoCalls.push(id); } lastClearedReceipts = []; };
    ${cutFrom(html, '  const readJson = (k, d) =>', '  function rememberSorterSelection()')}
    const cmds = { ${cutFrom(html, '    async "complete.undo"(a) {', '    async release()')} };
  `, c);
  return { c, fetched, call: (cmd, args) => vm.runInContext(`cmds[${JSON.stringify(cmd)}]`, c)(JSON.parse(JSON.stringify(args))).then(plain), ev: code => plain(vm.runInContext(code, c)) };
}

/** The sorter page: the real CNViewer and the real Sets.undo (and the cloud undo beside it), with the station reached through DesignLink. */
function makeSorter(cloud, station, follow) {
  const c = vm.createContext({ console, assert, setTimeout: fast, clearTimeout() {}, setInterval: () => 0, clearInterval() {} });
  c.cloudCall = async body => { const r = cloud.answer(body); if (r && r.error) throw Object.assign(new Error(r.error), { status: r.status || 400, data: r }); return r; };
  c.stationCall = (cmd, args) => station.call(cmd, args);
  c.followState = follow || null;
  vm.runInContext(`
    var window = this;
    var CN = { api: (name, body) => cloudCall(body) };
    var CharmNestRunFollow = { dev: () => 'dev-A', state: () => followState || { runId: null } };
  `, c);
  vm.runInContext(viewer, c);
  vm.runInContext(`
    const toasts = [], saved = [], poolWrites = [], designCalls = [], logs = [];
    const B = { employee: 'Tester', run: null, pool: { rows: new Map([['p1', { setId: 'set-1' }], ['p2', { setId: 'set-1' }]]) } };
    const rows = [{ state: 'committed', order: { receiptId: '111' } }, { state: 'committed', order: { receiptId: '222' } }];
    const toast = (t, kind) => toasts.push([t, kind || '']), agent = (w, k, t) => logs.push(t), api = (name, body) => cloudCall(body);
    const Orders = { rows: () => rows, render() {} }, Pool = { update: async (ids, p) => poolWrites.push([ids, p.state]) }, RunCtl = { save: async () => {}, renderBanner() {} };
    const DesignLink = { ensure: async () => {}, call: async (cmd, a) => { designCalls.push(cmd); return stationCall(cmd, a); } };
    const save = async s => saved.push(s.setId), humanAct = { set() {} };
    ${cutFrom(bridge, '  async function undo(set) {', '  /** The Library\'s Sets view')}
  `, c);
  return { c, ev: code => plain(vm.runInContext(code, c)) };
}
const aSet = () => ({ setId: 'set-1', runId: 'run-1', name: 'Set 1', status: 'complete', committed: ['111', '222'], committedAt: 1 });
const ops = cloud => cloud.calls.map(b => b.op);
const settle = () => new Promise(r => setTimeout(r, 20));

(async () => {
  /* ── A: the drive off ── */
  for (const [label, carrier] of [['another computer carries the run', 'dev-B'], ['the carrier still says "cloud" but the drive is off', 'cloud']]) {
    const cloud = makeCloud({ mode: 'off', carrier }), station = makeStation(cloud), page = makeSorter(cloud, station);
    await settle();   // (the viewer's first look at the switch)
    page.c.theSet = aSet();
    await page.ev(`undo(theSet)`);
    assert.deepEqual(page.ev('designCalls'), ['complete.undo'], `A (${label}): the station is asked to reopen the set's orders, as today`);
    assert.deepEqual(station.ev('undoCalls'), ['111', '222'], `A (${label}): the station reopened them itself`);
    assert.equal(page.c.theSet.status, 'awaiting review', `A (${label}): the set is back to awaiting review here`); assert.deepEqual(page.ev('saved'), ['set-1']);
    assert.deepEqual(page.ev('rows.map(r => r.state)'), ['written', 'written']); assert.equal(page.ev('poolWrites.length'), 1);
    assert(!ops(cloud).includes('commitUndoAsk'), `A (${label}): no commitUndoAsk is sent (${ops(cloud).join(', ')})`);
    assert(!station.fetched.includes('commitUndoAsk') && !ops(cloud).includes('runControl'));
    console.log(`  A ok · drive off, ${label}: local undo ran (station reopened 111, 222; set awaiting review) · cloud ops seen: ${ops(cloud).join(', ')}`);
  }

  /* ── B: the cloud carries the run, "commit" in drive.cloudSteps ── */
  {
    const cloud = makeCloud({ mode: 'standby', steps: ['engrave', 'commit'], carrier: 'cloud' }), station = makeStation(cloud);
    const page = makeSorter(cloud, station, { runId: 'run-1', run: { status: 'processed' } });
    await settle();
    page.c.theSet = aSet();
    const u = await vm.runInContext(`undo(theSet)`, page.c);   // (its own object: `followed` is the follow's promise)
    const asks = cloud.calls.filter(b => b.op === 'commitUndoAsk');
    assert.equal(asks.length, 1, 'B: exactly one commitUndoAsk');
    assert.deepEqual({ runId: asks[0].runId, setId: asks[0].setId, by: asks[0].by }, { runId: 'run-1', setId: 'set-1', by: 'Tester' }, 'B: with the set\'s run and set, and who pressed');
    assert.deepEqual(page.ev('designCalls'), [], 'B: the station is not asked: nothing is undone on this computer');
    assert.equal(station.ev('undoCalls.length'), 0); assert.equal(page.c.theSet.status, 'complete', 'B: the set is not written here'); assert.deepEqual(page.ev('saved'), []); assert.equal(page.ev('poolWrites.length'), 0);
    assert.equal(u.cloud, true); assert.equal(u.state, 'asked');
    const asked = page.ev('toasts').map(t => t[0]);
    assert(asked.some(t => /Set 1: Undo asked of the cloud/.test(t)), 'B: the person is told it was asked: ' + asked.join(' | '));
    await settle();
    assert.equal(cloud.calls.filter(b => b.op === 'runControl' && b.action === 'resume').length, 1, 'B: a resting run is sent Resume, so it reaches its commit step');
    const f = plain(await u.followed);
    assert.equal(f.state, 'undone', 'B: followed on the commit record until the cloud says it is undone');
    const all = page.ev('toasts').map(t => t[0]);
    assert(all.some(t => /Set 1: completion undone by the cloud · its 2 orders are open again at the Design Station/.test(t)), 'B: and the person is told: ' + all.join(' | '));
    console.log(`  B ok · cloud carries + "commit" lane: commitUndoAsk sent once ${JSON.stringify({ runId: asks[0].runId, setId: asks[0].setId, by: asks[0].by })}, nothing undone here`);
    for (const t of all) console.log(`      said: "${t}"`);

    // undone already (the station's "nothing to undo"), and refused with the reason
    for (const [answer, want, kind] of [
      [{ ok: true, already: true, words: "This set's completion is undone already: its orders are open at the station" }, /^Set 1: already undone · This set's completion is undone already/, 'already'],
      [{ error: 'This set is still being sent to the Design Station (archive): undo it once that has finished', status: 409 }, /^Set 1: not undone · This set is still being sent to the Design Station \(archive\)/, 'refused']]) {
      cloud.undoAnswer = answer; const n0 = page.ev('toasts.length');
      const r = await page.ev(`undo(theSet)`);
      const said = page.ev('toasts').slice(n0).map(t => t[0]);
      assert.equal(r.state, kind); assert(said.some(t => want.test(t)), `B (${kind}): said in words: ${said.join(' | ')}`);
      assert.deepEqual(page.ev('designCalls'), [], `B (${kind}): still nothing undone here`); assert.equal(page.c.theSet.status, 'complete');
      console.log(`  B ok · ${kind}: said "${said.find(t => want.test(t))}"`);
    }
  }

  /* ── C: the station's own look (the sorter names the run and set) ── */
  {
    const cloud = makeCloud({ mode: 'standby', steps: ['commit'], carrier: 'cloud' }), station = makeStation(cloud);
    const r = await station.call('complete.undo', { receiptIds: ['111', '222'], runId: 'run-1', setId: 'set-1', by: 'Tester' });
    assert.equal(r.cloud, true); assert.equal(r.state, 'asked'); assert.equal(station.ev('undoCalls.length'), 0, 'C: the station reopens nothing itself');
    assert.deepEqual(station.fetched, ['runCarrier', 'driveGet', 'commitUndoAsk'], 'C: one look at the carrier and the switch, then the press');
    cloud.undoAnswer = { error: 'The cloud did not send this set to the Design Station: there is nothing for it to undo', status: 404 };
    await assert.rejects(station.call('complete.undo', { receiptIds: ['111'], runId: 'run-1', setId: 'set-1' }), /the cloud did not undo it: The cloud did not send this set/, 'C: a refusal is said, and nothing is reopened');
    assert.equal(station.ev('undoCalls.length'), 0);
    const off = makeCloud({ mode: 'off', carrier: 'dev-B' }), st2 = makeStation(off);
    await st2.call('complete.undo', { receiptIds: ['111'], runId: 'run-1', setId: 'set-1' });
    assert.deepEqual(st2.ev('undoCalls'), ['111'], 'C: drive off: the station reopens as today'); assert(!st2.fetched.includes('commitUndoAsk'));
    console.log('  C ok · station: cloud carries → press sent, nothing reopened; refusal thrown in words; drive off → reopened as today');
  }
  console.log('cloud-commit-undo-page OK');
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
