// The sorter stays on for days (Paul, 24 Sep: "everything must be able to stay on indefinitely ... nothing may accumulate").
//   1. The workspace checkpoint: at most one every ten seconds, never back to back, none when nothing changed; the last
//      change before the page is hidden is still saved; a failed save is one standing line until the next good one.
//   2. Best-layout records go once their sheet is saved, and at start for sheets that were not cut short.
//   3. The checkpoint shares bytes nothing changes, and leaves out master designs nothing uses and the lines of finished
//      orders (made again from their rows at restore).
//   4. A written engraving keeps its placement as numbers and lets go of its back geometry; writing its backs again builds
//      that geometry again, and a charm whose back changed goes to a person instead.
//   5. A sheet saved to the cloud lets go of its bytes; every ding shares one sound device.
// The real module code runs with small stand-ins for IndexedDB, the clock, the cloud and the page. No network.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const root = path.join(__dirname, '../..');
const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const page = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
const slice = (source, from, to) => { const a = source.indexOf(from), b = source.indexOf(to, a); assert(a >= 0 && b > a, 'slice ' + from); return source.slice(a, b); };
// the code runs in this realm (the checkpoint copies plain objects only), its free names bound to the stand-ins
const load = (code, env, names) => new Function(...Object.keys(env), `${code}\n;return {${names}};`)(...Object.values(env));
const tick = async (n = 6) => { for (let i = 0; i < n; i++) await new Promise(r => setImmediate(r)); };

/* IndexedDB as the Session uses it: put copies its value at once (structured clone), completion comes later. */
function database(clock) {
  const records = new Map(), puts = new Map(), control = { fail: null, slowMs: 0 };
  const db = { createObjectStore() {}, transaction() {
    const tx = {}, later = f => setImmediate(() => { if (control.fail) { tx.error = new Error(control.fail); tx.onerror?.(); return; } f(); tx.oncomplete?.(); });
    tx.objectStore = () => ({
      get(k) { const req = {}; later(() => { req.result = structuredClone(records.get(k)); }); return req; },
      put(v, k) { const saved = structuredClone(v); puts.set(k, (puts.get(k) || 0) + 1); later(() => { clock.now += control.slowMs; records.set(k, saved); }); return {}; },
      delete(k) { later(() => records.delete(k)); return {}; },
      getAllKeys() { const req = {}; setImmediate(() => { req.result = [...records.keys()]; req.onsuccess?.(); tx.oncomplete?.(); }); return req; }
    });
    return tx;
  } };
  return { records, puts, control, indexedDB: { open() { const req = {}; setImmediate(() => { req.result = db; req.onupgradeneeded?.(); req.onsuccess(); }); return req; } } };
}

function sessionContext() {
  const clock = { now: 1_000_000 }, timers = new Map(), idb = database(clock), seen = { toasts: [], banners: 0, persist: 0 };
  const winEvents = {}, docEvents = {};
  let timerId = 0;
  const sheet = { metal: 'gold', active: 0, charms: [], placements: [], status: 'idle', cardEl: {} }; sheet.pages = [sheet];
  const env = {
    Date: { now: () => clock.now },
    setTimeout: (f, ms) => { const id = ++timerId; timers.set(id, { f, ms, due: clock.now + Math.max(0, ms || 0) }); return id; },
    clearTimeout: id => { timers.delete(id); },
    indexedDB: idb.indexedDB,
    window: { addEventListener: (t, f) => { winEvents[t] = f; } },
    document: { hidden: false, addEventListener: (t, f) => { docEvents[t] = f; } },
    navigator: { storage: { persisted: async () => false, persist: async () => { seen.persist++; return true; } } },
    WORKSPACE_SANDBOX: true, METALS: [{ key: 'gold' }],
    S: { settings: {}, cloud: { ok: false }, packingCatalog: {}, sources: [], poolSources: {}, unassigned: [], sheets: { gold: sheet }, mode: 'nest' },
    B: { run: null, carry: null, orders: { rows: [], byKey: new Map() }, pool: { rows: new Map() }, sets: new Map(), engrave: { items: new Map() }, review: { items: [] } },
    O: Object.assign({}, require('../../charm-nest-orders.js'), { stepIndex: s => ['pull', 'pool', 'nest', 'checkpoint', 'engrave'].indexOf(s) }),
    Orders: { view: () => ({}), render() {}, lineRecord: row => [row.key, { state: row.state, orderId: row.order.receiptId, fromRow: true }] },
    Engrave: { render() {}, view: () => ({}), restoreView() {} }, Review: { render() {}, view: () => ({}), settled: () => [] },
    Gate: { state: () => ({}) }, Recall: { state: () => ({}) }, LiveStrip: { rows: [] }, CN: { showPage() {} },
    Pool: { repairRecoveredGeometry: async () => 0 }, P: { parseSource: async () => ({}) }, RunCtl: { renderBanner() {} },
    api: async () => ({}), toast: msg => seen.toasts.push(msg), refreshAllCards() {}, renderRail() {}, updateTopSub() {}, setMode() {},
  };
  env.allSheets = () => env.METALS.flatMap(m => env.S.sheets[m.key].pages);
  // the run banner asks for a checkpoint whenever it is drawn, as the real one does
  env.window.RunCtl = { renderBanner: () => { seen.banners++; env.window.Session.schedule(); } };
  const { Session } = load(slice(bridge, 'const Session = window.Session =', '/* Import cadence'), env, 'Session');
  // the timers due at the clock's time, run as the browser would
  const fire = async () => { for (const [id, t] of [...timers]) if (t.due <= clock.now) { timers.delete(id); t.f(); } await tick(); };
  return { env, clock, timers, idb, seen, sheet, winEvents, docEvents, fire, Session };
}

/* ── 1 · the checkpoint's pace ── */
async function paceTest() {
  const t = sessionContext(), { Session, clock, timers, idb, seen, sheet } = t, puts = () => idb.puts.get('sandbox') || 0, next = () => [...timers.values()][0];
  const stored = () => idb.records.get('sandbox').sheets[0].pages[0].status;
  Session.listen(); Session.listen(); await tick();
  assert.equal(seen.persist, 1, 'the browser is asked once to keep the workspace');
  await Session.flush(true); assert.equal(puts(), 1);
  clock.now += 60000; await Session.flush(); await t.fire();
  assert.equal(puts(), 1, 'nothing changed: no checkpoint');
  // a change long after the last checkpoint is saved a second later
  sheet.status = 'first'; Session.schedule();
  assert.equal(timers.size, 1); assert.equal(next().ms, 1000);
  clock.now += 1000; await t.fire(); assert.equal(puts(), 2);
  // changes right after it wait for the next turn, ten seconds after the last one started, however many there are
  for (let i = 0; i < 50; i++) { sheet.status = 'burst ' + i; Session.schedule(); await Session.flush(); }
  assert.equal(puts(), 2, 'a burst of changes writes nothing at once');
  assert.equal(timers.size, 1); assert.equal(next().due, clock.now + 10000, 'one checkpoint every ten seconds at most');
  clock.now += 9000; await t.fire(); assert.equal(puts(), 2);
  clock.now += 1000; await t.fire(); assert.equal(puts(), 3); assert.equal(stored(), 'burst 49', 'the checkpoint holds the latest change');
  // leaving the page: the change made just before is written in the same task; with nothing changed, nothing is written
  sheet.status = 'last'; Session.schedule();
  t.env.document.hidden = true; t.docEvents.visibilitychange();
  assert.equal(puts(), 4, 'the last change before the page is hidden is saved at once'); assert.equal(timers.size, 0);
  await tick(); assert.equal(stored(), 'last');
  t.docEvents.visibilitychange(); t.winEvents.pagehide(); assert.equal(puts(), 4, 'hidden again with nothing changed: nothing written');
  t.env.document.hidden = false;
  // a failed save is one standing line (and one toast), tried again with a growing wait, cleared by the next good save
  const banners = seen.banners;
  idb.control.fail = 'QuotaExceededError'; sheet.status = 'unsaved'; Session.schedule(); clock.now += 20000; await t.fire();
  assert.equal(Session.failure()?.message, 'QuotaExceededError'); assert.equal(seen.toasts.length, 1); assert.equal(seen.banners, banners + 1);
  assert.equal(next().ms, 15000, 'tried again after 15 s');
  clock.now += 15000; await t.fire();
  assert.equal(Session.failure().count, 2); assert.equal(seen.toasts.length, 1, 'the same failure is not said again at each try'); assert.equal(seen.banners, banners + 1);
  assert.equal(next().ms, 30000, 'then after 30 s');
  idb.control.fail = null; clock.now += 30000; await t.fire();
  assert.equal(Session.failure(), null, 'the next good save clears the line'); assert.equal(seen.banners, banners + 2); assert.equal(stored(), 'unsaved');
  assert.equal(timers.size, 0, "the banner's own redraw is not a change to save");
  // a slow save rests as long as it took before the next one starts
  idb.control.slowMs = 25000; sheet.status = 'slow'; Session.schedule(); clock.now += 10000; await t.fire(); idb.control.slowMs = 0;
  assert.equal(stored(), 'slow');
  sheet.status = 'after slow'; Session.schedule();
  assert.equal(next().ms, 25000, 'never back to back');
}

/* ── 2 · best-layout records ── */
async function bestTest() {
  const t = sessionContext(), { Session, idb, sheet } = t, has = k => idb.records.has(k);
  Session.listen();
  Object.assign(sheet, { sheetId: 's1', jobId: 'j1', bestKey: 'k1', bestRevision: 1, best: { placements: [{ id: 'a', angle: 5 }], rejects: [] }, status: 'nesting' });
  await Session.checkpointBest(sheet); assert(has('sandbox:best:s1'));
  // saved final while a new nesting of the same sheet starts: its record stays while it nests
  Session.dropBest('s1'); await Session.flush(true); await tick(); assert(has('sandbox:best:s1'), 'kept while the sheet nests');
  sheet.status = 'complete'; Session.dropBest('s1');
  assert(has('sandbox:best:s1'), 'a crash before the next checkpoint still finds it');
  await Session.flush(true); await tick(); assert(!has('sandbox:best:s1'), 'deleted after the checkpoint that shows the sheet saved');
  // at start: records of sheets not cut short in the restored workspace go; the one being recovered and other scopes stay
  Object.assign(sheet, { sheetId: 's2', jobId: 'j2', bestKey: 'k2', bestRevision: 0, best: { placements: [{ id: 'b', angle: 9 }], rejects: [] }, status: 'nesting' });
  await Session.checkpointBest(sheet); await Session.flush(true);
  idb.records.set('sandbox:best:old', { jobId: 'x' }); idb.records.set('production:best:old', { jobId: 'y' });
  assert.equal(await Session.restore(), true); await tick(10);
  assert.equal(sheet.placements[0].id, 'b', 'the cut-short sheet recovered its best layout');
  assert(has('sandbox:best:s2')); assert(!has('sandbox:best:old'), 'a record left from a sheet saved long ago goes');
  assert(has('production:best:old'), "the other workspace's records are its own");
}

/* ── 3 · what the checkpoint holds ── */
async function contentTest() {
  const t = sessionContext(), { Session, env, idb, sheet } = t;
  // bytes nothing changes go in as they are (put copies them); anything else is still copied
  const bits = new Uint8Array([1, 2, 3]), ai = new Uint8Array([4]), grid = new Uint8Array([5]);
  const out = Session.copy({ mask: { bits }, src: { bytes: bits }, outputs: { ai }, grid });
  assert.equal(out.mask.bits, bits, 'a mask is not copied twice'); assert.equal(out.src.bytes, bits); assert.equal(out.outputs.ai, ai);
  assert.notEqual(out.grid, grid); assert.deepEqual([...out.grid], [5]);
  // master designs: kept for a sheet charm or a line still pooled, left out when nothing uses them
  env.S.poolSources = { 'pool:used_ai': { id: 'pool:used_ai', bytes: bits }, 'pool:pending_ai': { id: 'pool:pending_ai' }, 'pool:done_ai': { id: 'pool:done_ai', bytes: ai } };
  sheet.charms = [{ id: 'c1', sourceId: 'pool:used_ai' }];
  env.B.pool.rows = new Map([['p1', { state: 'pooled', aiPath: 'pending.ai' }], ['p2', { state: 'committed', aiPath: 'done.ai' }]]);
  // the lines of a finished order are made again from their rows; the lines of an order in progress stay
  const rows = [{ key: 'o1/1', state: 'committed', order: { receiptId: 'o1' } }, { key: 'o2/1', state: 'pooled', order: { receiptId: 'o2' } }];
  env.B.orders = { rows, byKey: new Map(rows.map(r => [r.key, r])) };
  env.B.run = { runId: 'r1', status: 'processed', orders: ['o1', 'o2'], lines: { 'o1/1': { state: 'committed', orderId: 'o1' }, 'o2/1': { state: 'pooled', orderId: 'o2' } } };
  const snap = Session.capture();
  assert.deepEqual(Object.keys(snap.poolSources).sort(), ['pool:pending_ai', 'pool:used_ai'], 'a master nothing uses is left out');
  assert.equal(Object.keys(env.S.poolSources).length, 3, 'this page still holds it');
  assert.deepEqual(Object.keys(snap.run.lines), ['o2/1']); assert.deepEqual(snap.run.linesFromRows, ['o1/1']);
  assert.equal(Object.keys(env.B.run.lines).length, 2, 'the run in memory keeps every line');
  Session.listen(); await Session.flush(true);
  assert.deepEqual(Object.keys(idb.records.get('sandbox').poolSources).sort(), ['pool:pending_ai', 'pool:used_ai']);
  assert.equal(await Session.restore(), true);
  assert.deepEqual(env.B.run.lines['o1/1'], { state: 'committed', orderId: 'o1', fromRow: true }, 'restore makes the finished line again from its row');
  assert.deepEqual(env.B.run.lines['o2/1'], { state: 'pooled', orderId: 'o2' }); assert.equal(env.B.run.linesFromRows, undefined);
}

/* ── 4 · written engravings ── */
function engraveContext() {
  const calls = { put: 0, review: [] };
  let charm = { id: 'c', sourceId: 's', bits: new Uint8Array([1, 1, 0, 1]), backKeepOut: [] };
  // geometry stand-ins: the same charm and words give the same back and the same letters, as the real ones do
  const G = {
    backView: (c, o) => ({ mask: { bits: c.bits, w: 2, h: 2, res: 6, ox: 0, oy: 0 }, upAngle: o.upAngle, cx: 0, cy: 0, angleDeg: 0, cutMembers: [], checks: {}, detail: {} }),
    engraveMask: (v, o) => Object.assign({}, v.mask, { marginMm: o.marginMm }),
    layoutLines: (lines, font, size, gap, angle, centre) => { const cmds = [{ type: 'M', x: centre[0], y: centre[1], t: lines.join('|'), size, gap, angle, w: font.weight }]; return { cmds, glyphs: [{ cmds }] }; },
    verifyInk: () => ({ ok: true }),
  };
  const sh = { sheetId: 'sh1', fileBase: 'GF_1', folderPath: 'f', runId: 'r', metal: 'gold', backPool: [] };
  const jobs = new Map();
  const env = { window: {}, B: { run: { runId: 'r' }, pool: { rows: new Map() } }, S: { cloud: { ok: true }, settings: {} },
    F_: { ok: true }, G, EG: { cardKey: null }, items: () => jobs, isWorking: () => false, fontFor: w => ({ weight: w }), fitOpts: j => ({ lineGap: j.lineGap ?? 0.18 }), loadFonts: async () => {},
    charmFor: () => charm, sourceOf: () => ({ parsed: {} }), sheetFor: () => sh, allSheets: () => [sh], PT: 72 / 25.4,
    renderBack: () => ({ toBlob: cb => cb(new Uint8Array(1)), _sizePt: { w: 1, h: 1 } }),
    P: { buildBackFile: async () => ({ bytes: new Uint8Array(2), reference: {}, wPt: 1, hPt: 1 }) },
    verifyBackFile: async () => ({ ok: true }), uploadBytes: async p => ({ path: p, url: 'https://x/' + p }),
    api: async (fn, body) => { if (body.op === 'backPut') calls.put++; return {}; },
    Pool: { update: async () => {} }, Review: { add: x => calls.review.push(x) }, refreshBacks() {}, scheduleBackOutputs() {}, render() {}, agent() {}, toast() {}, syncEditedBack: async () => {} };
  const api = load(slice(bridge, '  let backQueue = Promise.resolve();', '  /** Parse the written back file'), env, 'writeBacks, hasPlacement, shelveWritten');
  const job = (key, copies, saved = copies) => {
    const view = G.backView(charm, { upAngle: 90 }), mask = G.engraveMask(view, { marginMm: 0.8 }), layout = G.layoutLines(['Hi'], { weight: 400 }, 5, 0.18, 0, [1, 2]);
    const j = { key, state: 'written', approvedAt: 5, approvedBy: 'P', copies, text: 'Hi', lines: ['Hi'], lineGap: 0.18, view, mask, verify: { geometry: { ok: true } },
      fit: { size: 5, capMm: 2, weight: 400, angle: 0, centre: [1, 2], rect: null, metrics: {}, lines: ['Hi'], layout, glyphs: layout.glyphs, cmds: layout.cmds },
      backs: saved.map(poolId => ({ poolId, approvedAt: 5, outputs: { ai: { url: 'https://x/' + poolId + '.ai' }, png: { url: 'https://x/' + poolId + '.png' } } })),
      row: { order: { receiptId: 'o' }, line: { transactionId: 't' }, spec: { designSku: 'S' }, engrave: { state: 'written' } } };
    jobs.set(key, j); return j;
  };
  return { api, calls, job, setCharm: c => { charm = c; }, charm: () => charm };
}
async function writtenTest() {
  const e = engraveContext(), { api, calls } = e;
  const saved = e.job('saved', ['p1', 'p2']), partial = e.job('partial', ['p3', 'p4'], ['p3']), odd = e.job('odd', ['p5']);
  odd.fit.cmds = [{ type: 'M', x: 0, y: 0 }];                       // drawn by an older build: this page cannot make it again exactly
  const cmds = JSON.stringify(saved.fit.cmds);
  assert.equal(api.shelveWritten(), 1);
  assert.equal(saved.view, null); assert.equal(saved.mask, null); assert.equal(saved.fit, null, 'the back geometry, mask and letters leave memory');
  assert.deepEqual([saved.writtenFit.size, saved.writtenFit.capMm, saved.writtenFit.upAngle, saved.writtenFit.marginMm, saved.writtenFit.approvedAt], [5, 2, 90, 0.8, 5]);
  assert.equal(saved.writtenFit.glyphs, undefined); assert(api.hasPlacement(saved), 'its placement is still known');
  assert(partial.fit && partial.view, 'a back not saved yet keeps what it is written from');
  assert(odd.fit && odd.view, 'a placement this page cannot make again is kept whole'); assert.equal(odd._shelveTried, 5);
  assert.equal(api.shelveWritten(), 0, 'and not tried again at every checkpoint');
  // a piece moved to another sheet: its backs are written again from the charm, exactly as they were fitted
  assert.notEqual(await api.writeBacks(saved), false);
  assert.equal(JSON.stringify(saved.fit.cmds), cmds); assert.equal(saved.view.upAngle, 90); assert.equal(saved.mask.marginMm, 0.8);
  assert.equal(calls.put, 2); assert.equal(saved.state, 'written');
  assert.equal(api.shelveWritten(), 1, 'written again, it lets go again');
  // the charm's back changed since the words were approved: a person fits them again, nothing is written
  e.setCharm(Object.assign({}, e.charm(), { bits: new Uint8Array([1, 0, 0, 1]) }));
  assert.equal(await api.writeBacks(saved), false);
  assert.equal(saved.state, 'blocked'); assert.match(saved.reason, /fit the words again/); assert.equal(calls.put, 2);
  assert.equal(calls.review.length, 1); assert.equal(calls.review[0].kind, 'placement');
}

/* ── 5 · the page ── */
async function pageTest() {
  const { release } = load(slice(page, 'function releaseSavedOutputs(sh) {', 'function pumpNestQueue()'), { window: { B: { sets: new Map([['g', { committedAt: 1, sheetIds: ['cut'] }]]) } } }, 'release: releaseSavedOutputs');
  const outputs = () => ({ ai: new Uint8Array(9), labelled: new Uint8Array(9), previewPng: new Blob(['p']), report: { ok: true } });
  const cloud = { ai: 'https://x/a.ai', labelled: 'https://x/l.pdf', preview: 'https://x/p.png' };
  const done = { sheetId: 'cut', status: 'complete', persistedDone: true, outputs: outputs(), cloud, _renderAreas: [1] };
  release(done);
  assert.deepEqual(Object.keys(done.outputs), ['report'], 'a sheet saved to the cloud keeps the links, not the bytes');
  assert.equal(done._renderAreas, null, 'a committed sheet lets go of its render areas');
  const open = { sheetId: 'open', status: 'complete', persistedDone: true, outputs: outputs(), cloud, _renderAreas: [1] };
  release(open); assert.deepEqual(open._renderAreas, [1], 'an open sheet keeps them for its next write');
  for (const sh of [{ dirty: true }, { status: 'nesting' }, { problem: 'not saved' }, { persistedDone: false }, { cloud: { labelled: 'https://x/l.pdf' } }]) {
    const s = Object.assign({ sheetId: 'x', status: 'complete', persistedDone: true, outputs: outputs(), cloud }, sh); release(s);
    assert(s.outputs.ai, 'kept: ' + JSON.stringify(sh));
  }
  // one sound device for every ding, woken when the browser put it to sleep, made again only if it was closed
  let made = 0, resumed = 0; const nodes = [];
  class AudioContext {
    constructor() { made++; this.state = 'running'; this.currentTime = 0; this.destination = {}; }
    resume() { resumed++; this.state = 'running'; return Promise.resolve(); }
    createOscillator() { const o = { frequency: {}, connect() {}, disconnect() { o.off = true; }, start() {}, stop() { o.onended?.(); } }; nodes.push(o); return o; }
    createGain() { const g = { gain: { exponentialRampToValueAtTime() {} }, connect() {}, disconnect() { g.off = true; } }; nodes.push(g); return g; }
  }
  const from = page.includes('let dingCtx = null;') ? 'let dingCtx = null;' : 'function ding()';
  const { ding, device } = load(slice(page, from, '/* ═══ 2 · settings'), { S: { settings: { sound: 'on' } }, window: { AudioContext } }, "ding, device: () => typeof dingCtx === 'undefined' ? null : dingCtx");
  ding(); ding(); ding();
  assert.equal(made, 1, 'one sound device for every ding'); assert(nodes.every(n => n.off), 'each ding lets go of its nodes');
  device().state = 'suspended'; ding(); assert.equal(resumed, 1); assert.equal(made, 1);
  device().state = 'closed'; ding(); assert.equal(made, 2);
}

(async () => {
  const failed = [];
  for (const [name, test] of [['checkpoint pace', paceTest], ['best-layout records', bestTest], ['checkpoint content', contentTest], ['written engravings', writtenTest], ['saved sheets and sound', pageTest]]) {
    try { await test(); } catch (e) { failed.push(name); console.error(`${name}:`, e); }
  }
  if (failed.length) { console.error('uptime retention FAILED: ' + failed.join(', ')); process.exitCode = 1; return; }
  console.log('uptime retention OK · checkpoint pace, best-layout clean-up, shared bytes, unused masters, finished lines, written engravings, saved sheets, one sound device');
})();
