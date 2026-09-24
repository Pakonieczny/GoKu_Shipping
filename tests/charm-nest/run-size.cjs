// A run left in Auto for days keeps one record, and every order it ever took used to stay in it (~0.9 KB a line) until
// Firestore refused the document and the run stopped. This drives the real run controller (RunCtl.save, resumeRun,
// poke) and the real library functions against an in-memory Firestore through 2,000 lines arriving over 100 updates,
// most of them committed, and checks that the saved record stays small while resume, history search, a recalled set,
// laser readiness and set completion still find every line — a recalled set and laser readiness reading only the archive
// parts that hold what they ask about, and readiness only their decisions. No network, no real services.
//   node tests/charm-nest/run-size.cjs
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const O = require('../../charm-nest-orders.js'), Readiness = require('../../charm-nest-readiness.js');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');

/* ── in-memory Firestore (as tests/charm-nest/functions.cjs) ── */
const store = new Map();
// what the functions read of the line archive: each part document returned (with the bytes of the fields returned), and
// each array-contains-any query
const partReads = [], queries = [];
const SERVER_TS = { __ts: true };
const FieldValue = { serverTimestamp: () => SERVER_TS, increment: n => ({ __inc: n }), delete: () => ({ __del: true }) };
let clock = 1727000000000;
function applyValues(target, src) { for (const [k, v] of Object.entries(src)) { if (v && v.__inc != null) target[k] = (target[k] || 0) + v.__inc; else if (v && v.__del) delete target[k]; else if (v === SERVER_TS) { const t = ++clock; target[k] = { toMillis: () => t }; } else target[k] = v; } return target; }
function docRef(coll, id) {
  const key = coll + '/' + id;
  return { id, path: key,
    async get() { const d = store.get(key); return { exists: !!d, id, ref: docRef(coll, id), data: () => (d ? JSON.parse(JSON.stringify(d, (k, v) => v && v.toMillis ? v.toMillis() : v)) : undefined) }; },
    collection(sub) { return query(coll + '/' + id + '/' + sub); },
    async set(data, opts) { const cur = (opts && opts.merge && store.get(key)) || {}; store.set(key, applyValues({ ...cur }, JSON.parse(JSON.stringify(data, (k, v) => v === SERVER_TS ? '__TS__' : v), (k, v) => v === '__TS__' ? SERVER_TS : v))); },
    async update(data) { const cur = store.get(key); if (!cur) throw new Error('NOT_FOUND: ' + key); store.set(key, applyValues({ ...cur }, data)); },
    async delete() { store.delete(key); } };
}
// a projection, as Firestore's select() and getAll's fieldMask return: only the fields named
const project = (snap, fields) => (!fields || !snap.exists ? snap : { ...snap, data: () => Object.fromEntries(Object.entries(snap.data()).filter(([k]) => fields.includes(k))) });
function query(coll, filters = [], order = null, lim = 0, fields = null) {
  const q = {
    where(f, op, v) { return query(coll, filters.concat([[f, op, v]]), order, lim, fields); },
    orderBy(f, dir) { return query(coll, filters, [f, dir || 'asc'], lim, fields); },
    limit(n) { return query(coll, filters, order, n, fields); },
    select(...names) { return query(coll, filters, order, lim, names); },
    count() { return { get: async () => { const s = await q.get(); return { data: () => ({ count: s.size }) }; } }; },
    async get() {
      let rows = [...store.keys()].filter(k => k.startsWith(coll + '/') && !k.slice(coll.length + 1).includes('/')).map(k => docRef(coll, k.slice(coll.length + 1)));
      rows = await Promise.all(rows.map(r => r.get()));
      const val = x => (x && x.toMillis ? x.toMillis() : x);
      for (const [f, op, v] of filters) {
        if (op === 'array-contains-any') { assert(Array.isArray(v) && v.length >= 1 && v.length <= 30, 'array-contains-any takes 1 to 30 values'); queries.push(op); }
        rows = rows.filter(r => { const x = val(r.data()[f]); return op === '==' ? x === v : op === '>=' ? x >= v : op === '<=' ? x <= v : op === 'in' ? v.includes(x) : op === 'array-contains-any' ? Array.isArray(x) && x.some(e => v.includes(e)) : op === 'array-contains' ? Array.isArray(x) && x.includes(v) : true; });
      }
      if (order) rows.sort((a, b) => { const x = val(a.data()[order[0]]), y = val(b.data()[order[0]]); const c = x > y ? 1 : x < y ? -1 : 0; return order[1] === 'desc' ? -c : c; });
      if (lim) rows = rows.slice(0, lim);
      rows = rows.map(r => project(r, fields));
      if (coll === 'Charm_Nest_Run_Lines') partReads.push(...rows.map(r => ({ id: r.id, bytes: bytesOf(r.data()), fields: Object.keys(r.data()) })));
      return { size: rows.length, docs: rows, empty: !rows.length };
    },
    doc(id) { return docRef(coll, id); }
  };
  return q;
}
const db = {
  collection: c => query(c),
  batch() { const ops = []; return { set(ref, d, o) { ops.push(() => ref.set(d, o)); }, update(ref, d) { ops.push(() => ref.update(d)); }, delete(ref) { ops.push(() => ref.delete()); }, async commit() { for (const f of ops) await f(); } }; },
  async getAll(...args) {
    const opts = args.length && !args[args.length - 1].get ? args.pop() : {}, snaps = await Promise.all(args.map(r => r.get()));
    const out = snaps.map(s => project(s, opts.fieldMask || null));
    for (const s of out) if (s.exists && s.ref && /^Charm_Nest_Run_Lines\//.test(s.ref.path)) partReads.push({ id: s.id, bytes: bytesOf(s.data()), fields: Object.keys(s.data()) });
    return out;
  },
  async runTransaction(fn) { return fn({ get: r => r.get(), set: (r, d, o) => r.set(d, o), update: (r, d) => r.update(d), delete: r => r.delete() }); }
};
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue }), storage: () => ({ bucket: () => ({ name: 'test', file: () => ({ exists: async () => [false], getMetadata: async () => [{ metadata: {} }] }) }) }) };
require.cache[require.resolve(path.join(fnDir, 'firebaseAdmin.js'))] = { id: 'fake', filename: 'firebaseAdmin.js', loaded: true, exports: fakeAdmin };
const Module = require('module'), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === 'node-fetch') return async () => ({ ok: true, status: 202, text: async () => '' });
  if (req === 'firebase-admin' || /[\/]firebaseAdmin(\.js)?$/.test(req) || req === './firebaseAdmin') return fakeAdmin;
  return realLoad.call(this, req, ...rest);
};
delete process.env.EDIT_PASSCODE;
const lib = require(path.join(fnDir, 'charmNestLibrary.js'));
const serverWarnings = [], realWarn = console.warn;
console.warn = (...a) => { const t = a.join(' '); if (/charmNestLibrary\] run /.test(t)) serverWarnings.push(t); else realWarn(...a); };
const post = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
const docs = prefix => [...store.keys()].filter(k => k.startsWith(prefix + '/'));
const bytesOf = v => Buffer.byteLength(JSON.stringify(v));
const runDoc = id => store.get('Charm_Nest_Runs/' + id);

/* ── the page's run controller, as in tests/charm-nest/process-completion.cjs, with the page's own line record ── */
const source = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
let controller = source.slice(source.indexOf('const RunCtl ='), source.indexOf('/* ═══ 24 · Review'));
controller = controller.slice(0, controller.indexOf('  function renderBanner()')) + '  function renderBanner() {}\n' + controller.slice(controller.indexOf('  return { optionsChanged'));
const lineRecordSource = source.slice(source.indexOf('  const cap = (v, n)'), source.indexOf('  /** The other direction'));
const workTextSource = source.slice(source.indexOf('  function workText(r)'), source.indexOf('  // the state of the work as a short hash'));
assert(lineRecordSource.includes('function lineRecord(row)') && workTextSource.includes('function workText(r)'), 'the page source still has the pieces this test runs');

// The "station": every order it ever listed, and whether it is still open there (committed and gone orders are not).
const station = new Map();
const closedAtStation = o => o.lines.every(l => ['committed', 'gone'].includes(o.fate[l.transactionId])) && o.lines.some(l => o.fate[l.transactionId] === 'committed' || o.lines.every(x => o.fate[x.transactionId] === 'gone'));
function controllerFor({ rows = [], sandbox = false } = {}) {
  const st = { rows, logs: [], toasts: [], calls: [], pulled: null, failArchive: 0, sandbox };
  const api = async (fn, body) => {
    assert.equal(fn, 'charmNestLibrary');
    st.calls.push(body.op);
    if (body.op === 'runArchive' && st.failArchive > 0) { st.failArchive--; throw new Error('HTTP 502'); }
    const r = await post(st.sandbox ? { ...body, sandbox: true } : body);
    if (r.status < 200 || r.status >= 300) throw Object.assign(new Error(r.body.error || 'HTTP ' + r.status), { status: r.status });
    return r.body;
  };
  const freshRow = (o, l) => ({ key: O.lineKey(o, l), order: o, line: l, arrivedAt: o.arrivedAt, spec: { designSku: l.sku, material: 'silver', quantity: 1, engraveCandidate: l.engraved }, problems: [], state: 'pulled', reason: null, hold: null, wait: null, poolIds: [], engrave: null, material: 'silver' });
  const ctx = { window: {}, B: { run: null, pool: { rows: new Map() } }, S: { cloud: { ok: true }, settings: { autoCommit: 'on' } }, O, console,
    Date, Promise, Map, Set, JSON, queueMicrotask, setTimeout, clearTimeout, setInterval, clearInterval,
    allSheets: () => [],
    Orders: { rows: () => st.rows, lineRecord: row => ctx.lineRecord(row), revalidate: async () => ({ changed: [], gone: [] }), unclaim: async () => {}, claim: async () => {}, interpretAll() {},
      // a resume pulls the run's orders again: the station answers with the ones still open there
      pull: async (run, { receiptIds }) => { st.pulled = receiptIds.map(String); const want = new Set(st.pulled); st.rows = [...station.values()].filter(o => want.has(o.receiptId) && !closedAtStation(o)).flatMap(o => o.lines.map(l => freshRow(o, l))); return st.rows; } },
    Pool: { sheetOf: () => null, addAll: async () => 0, charmOf: () => ({}) },
    Engrave: { items: () => new Map(), pendingCount: () => 0, classifyAll: async () => {}, fitAll: async () => {} },
    Review: { count: () => 0 }, Gate: { flush: async () => {}, nestable: () => true, modern: () => true, assemble: async () => {}, upgrade: async () => {} },
    LiveNest: { finish: async () => {} }, Sets: { ofRun: () => [], releaseIssue: () => null, save: async () => {}, finalize: async () => {}, byRun: () => new Map(), keyOf: () => '' },
    api, Session: { schedule() {} }, LiveStrip: { render() {} }, Arrivals: { start() {}, processPending: async () => {} },
    agent: (scope, kind, text) => { st.logs.push({ kind, text }); }, toast: text => st.toasts.push(text), notifyPerson() {}, ding() {}, CN: { renderCard() {} }, sheetName: s => s.metal,
    startNest() {}, sheetDirty() {}, METALS: [] };
  vm.createContext(ctx); vm.runInContext(lineRecordSource, ctx); vm.runInContext(controller, ctx); vm.runInContext(workTextSource, ctx);
  return { ctx, ctl: ctx.window.RunCtl, st };
}
const tick = () => new Promise(r => setImmediate(r));
async function settle(ctx) { for (let i = 0; i < 200; i++) { await tick(); if (ctx.B.run.status !== 'running') return; } throw new Error('the run did not come to rest'); }

/* ── orders as the shop gets them: long titles, personalisation, three options, about 0.9 KB a line in the record ── */
let nextReceipt = 3500000000, nextTx = 4600000000, orderNo = 0;
function newOrder(nLines, u) {
  const receiptId = String(nextReceipt++), n = orderNo++;
  const o = { receiptId, orderNumber: receiptId.slice(-7), buyer: { name: `Customer ${receiptId.slice(-4)} Longfellow-Smythe` }, shipBy: 1727200000 + u * 3600, isGift: n % 7 === 0, createTs: 1727000000 + u * 600, updateTs: 1727000000 + u * 600 + 5, arrivedAt: 1727000000000 + u * 600000, arrivedAtUpdate: u, n, lines: [], fate: {} };
  for (let i = 0; i < nLines; i++) {
    const tx = String(nextTx++);
    o.lines.push({ transactionId: tx, listingId: String(1200000000 + (+tx % 300)), sku: 'NK-INI-' + String(+tx % 40).padStart(2, '0'), engraved: i % 2 === 0, title: `Personalized Sterling Silver Initial Charm Necklace, Dainty Custom Letter Pendant with Birthstone, Gift for Her ${+tx % 97}`,
      metalKey: 'silver', metalLabel: 'Sterling Silver', quantity: 1, variations: [{ name: 'Metal', value: 'Sterling Silver' }, { name: 'Chain Length', value: '18 inches (45 cm)' }, { name: 'Personalization', value: 'Add engraving on the back' }],
      personalization: [`Front: ${String.fromCharCode(65 + n % 26)}  Back: Forever ${receiptId.slice(-5)} — love, Mom`, 'Please gift wrap it, thank you so much!'] });
    o.fate[tx] = 'pulled';
  }
  station.set(receiptId, o);
  return o;
}
// what happens to each order: most are cut and committed two updates after they arrive; some are cancelled, some held
// for a person, some only chains (no design) or skipped — those stay open at the station until they ship, weeks later
const kind = o => (o.n % 11 === 3 ? 'cancelled' : o.n % 13 === 5 ? 'held' : o.n % 17 === 7 ? 'noDesign' : o.n % 19 === 9 ? 'skipped' : 'normal');
function advance(o, u) {
  const age = u - o.arrivedAtUpdate, k = kind(o);
  for (const l of o.lines) {
    const tx = l.transactionId;
    o.fate[tx] = k === 'cancelled' ? (age >= 1 ? 'gone' : 'pooled')
      : k === 'held' ? (age >= 15 ? 'gone' : 'held')
      : k === 'noDesign' ? (age >= 20 ? 'gone' : 'noDesign')
      : k === 'skipped' ? (age >= 20 ? 'gone' : 'skipped')
      : age >= 2 ? 'committed' : age >= 1 ? 'written' : 'pooled';
  }
}
function syncRow(row) {
  const o = row.order, l = row.line, fate = o.fate[l.transactionId];
  row.state = fate;
  row.poolIds = ['pooled', 'written', 'committed'].includes(fate) || (fate === 'gone' && row.poolIds.length) ? [`${o.receiptId}_${l.transactionId}_1`] : row.poolIds;
  row.hold = fate === 'held' ? 'engraving words need a decision' : null; row.reason = row.hold;
  row.engrave = l.engraved && ['written', 'committed'].includes(fate) ? { needed: true, state: 'written', approved: true, text: `FOREVER ${o.receiptId.slice(-5)}` } : row.engrave;
}

(async () => {
  /* ═══ 1 · the defect, and the rule for what leaves the record ═══ */
  const L = (orderId, state) => ({ orderId, state });
  const closed = O.closedOrders({ a1: L('1', 'committed'), a2: L('1', 'noDesign'), b1: L('2', 'skipped'), c1: L('3', 'gone'), c2: L('3', 'gone'), d1: L('4', 'gone'), d2: L('4', 'skipped'), e1: L('5', 'written'), e2: L('5', 'committed'), f1: L('6', 'noDesign'), g1: { state: 'committed' } });
  assert.deepEqual([...closed.keys()].sort(), ['1', '3'], 'only orders committed at the station or wholly gone leave the record; orders still open there (skipped, no design, one line gone, one line still working) stay');

  /* ═══ 2 · a continuous Auto run: 2,000 lines over 100 updates, saved after every update ═══ */
  const main = controllerFor();
  const runId = 'run-2026-09-20-auto01';
  const r = { runId, day: '2026-09-20', setId: null, releasePolicy: 2, step: 'complete', status: 'processed', mode: 'auto', startedAt: 1, updatedAt: 1, lines: {}, sheets: {}, holds: {}, errors: [], orders: [], committed: [], resumable: true };
  main.ctx.B.run = r;
  const sizes = [], sizes2 = [];
  let lines = 0;
  for (let u = 0; u < 100; u++) {
    const arrived = [];
    for (let want = 20; want > 0;) { const n = Math.min(want, [1, 2, 1, 3, 1, 2, 2][orderNo % 7]); arrived.push(newOrder(n, u)); want -= n; }
    for (const o of arrived) { for (const l of o.lines) { const row = { key: O.lineKey(o, l), order: o, line: l, arrivedAt: o.arrivedAt, spec: { designSku: l.sku, material: 'silver', quantity: 1, engraveCandidate: l.engraved }, problems: [], state: 'pulled', reason: null, hold: null, wait: null, poolIds: [], engrave: null, material: 'silver' }; main.st.rows.unshift(row); lines++; } r.orders.push(o.receiptId); }
    for (const o of station.values()) advance(o, u);
    for (const row of main.st.rows) syncRow(row);
    // the commit step's own bookkeeping: committed receipts and held orders accumulate on the run
    for (const o of station.values()) { if (o.lines.every(l => o.fate[l.transactionId] === 'committed') && !r.committed.includes(o.receiptId)) r.committed.push(o.receiptId); if (kind(o) === 'held' && u - o.arrivedAtUpdate === 2) r.holds[o.receiptId] = { line: O.lineKey(o, o.lines[0]), why: 'engraving words need a decision' }; }
    for (let s = 0; s < 4; s++) r.sheets[`sheet-${u}-${s}`] = { metal: 'silver', page: s + 1, status: 'complete', verified: true, placed: 5, rejects: 0, fileBase: `SS_Set-${u}_Sheet-${s + 1}`, error: null };
    r.processingSignature = main.ctx.workText(r);   // what the page kept before the signature was hashed: the whole text
    await main.ctl.save(r);
    const saved = runDoc(runId);
    sizes.push(bytesOf(saved)); sizes2.push(O.indexEntries(saved));
  }
  assert.equal(lines, 2000);
  const everyLine = Object.fromEntries(main.st.rows.map(row => main.ctx.lineRecord(row)));
  const perLine = bytesOf(everyLine) / 2000;
  assert(perLine > 700 && perLine < 1300, `a line record is about 0.9 KB (${Math.round(perLine)} B)`);
  assert(bytesOf(everyLine) > O.RUN_RECORD.bytes, 'kept whole, these lines alone are past the 1 MiB a document holds: the record that used to stop the run');
  const saved = runDoc(runId), maxBytes = Math.max(...sizes), maxEntries = Math.max(...sizes2);
  console.log(`2,000 lines: ${Math.round(bytesOf(everyLine) / 1024)} KB kept whole; the saved record peaked at ${Math.round(maxBytes / 1024)} KB and ${maxEntries} index entries (${[24, 49, 74, 99].map(u => Math.round(sizes[u] / 1024) + ' KB').join(' / ')} after updates 25, 50, 75, 100), ending with ${Object.keys(saved.lines).length} working lines`);
  assert(maxBytes < 0.25 * O.RUN_RECORD.bytes, `the record stays well under the limit (${maxBytes} B)`);
  assert(maxEntries < 0.5 * O.RUN_RECORD.entries, `and under the index-entry limit (${maxEntries})`);
  assert(sizes[99] < 1.5 * sizes[49] && sizes[99] < 1.5 * sizes[24], `it follows the work in hand, not the run's age (${sizes[24]} / ${sizes[49]} / ${sizes[99]} B)`);
  assert(!main.st.logs.some(e => /full/.test(e.text)), 'nothing near the limit, nothing said');
  // what the record keeps: every line of every order still open, nothing of a closed one
  const open = [...station.values()].filter(o => !closedAtStation(o)), shut = [...station.values()].filter(closedAtStation);
  assert.deepEqual(Object.keys(saved.lines).sort(), open.flatMap(o => o.lines.map(l => O.lineKey(o, l))).sort(), 'the record holds exactly the lines of the orders still open');
  assert.deepEqual([...saved.orders].sort(), open.map(o => o.receiptId).sort(), 'and exactly their orders, which a resume pulls again');
  assert(!saved.committed.length && !Object.keys(saved.holds).some(id => shut.some(o => o.receiptId === id)), 'committed ids and holds of closed orders leave with them');
  assert.equal(Object.keys(saved.sheets).length, O.RUN_RECORD.keepSheets, 'the newest sheet notes stay');
  assert.deepEqual(saved.lineArchive && [saved.lineArchive.lines, saved.lineArchive.committed, saved.lineArchive.sheets], [shut.reduce((n, o) => n + o.lines.length, 0), r.committed.length, 400 - O.RUN_RECORD.keepSheets], 'and the record counts what is outside it');
  assert.match(saved.processingSignature, /^[0-9a-f]{16}$/, 'a whole-text work signature is stored as its hash');
  assert.equal(saved.processingSignature, O.textHash(r.processingSignature));
  assert(!('archivedLines' in saved) && !('base' in saved.lineArchive), 'the page\'s own bookkeeping is not stored');
  // the archive: parts of at most 256 KB, and a save with nothing new writes nothing
  const parts = docs('Charm_Nest_Run_Lines');
  assert(parts.length >= 8 && parts.every(k => store.get(k).runId === runId && store.get(k).bytes <= O.RUN_RECORD.partBytes), `archive parts are small (${parts.length})`);
  assert.equal(parts.reduce((n, k) => n + store.get(k).lines, 0), shut.reduce((n, o) => n + o.lines.length, 0), 'each finished line was archived once');
  for (const k of parts) {
    const p = store.get(k), inPart = JSON.parse(p.json);
    assert.deepEqual(p.keys, Object.keys(inPart), 'a part lists its lines');
    assert.deepEqual(p.orders, [...new Set(Object.values(inPart).map(l => String(l.orderId)))], 'and its orders');
    assert.deepEqual(JSON.parse(p.decisions), Object.fromEntries(Object.entries(inPart).map(([key, l]) => [key, Readiness.decisions([l])])), 'and keeps its copies\' engraving decisions by line');
  }
  const decisionShare = parts.reduce((n, k) => n + Buffer.byteLength(store.get(k).decisions), 0) / parts.reduce((n, k) => n + store.get(k).bytes, 0);
  assert(decisionShare < 0.2, `the decisions are a small share of the lines (${Math.round(100 * decisionShare)}%)`);
  const archiveBytes = parts.reduce((n, k) => n + bytesOf(store.get(k)), 0), readKB = list => Math.round(list.reduce((n, x) => n + x.bytes, 0) / 1024) + ' KB';
  console.log(`archive: ${parts.length} parts, ${Math.round(archiveBytes / 1024)} KB; the decisions in them are ${Math.round(100 * decisionShare)}% of the lines' bytes`);
  await main.ctl.save(r);
  assert.equal(docs('Charm_Nest_Run_Lines').length, parts.length, 'saving again archives nothing again');

  /* ═══ 3 · readers find every line ═══ */
  let res = await post({ op: 'runList' });
  assert.equal(res.body.runs.find(x => x.runId === runId).lines, 2000, 'the run list counts the archived lines too');
  const done = shut.find(o => kind(o) === 'normal' && o.lines.length === 2), doneKeys = done.lines.map(l => O.lineKey(done, l));
  res = await post({ op: 'runGet', runId });
  assert(doneKeys.every(k => !res.body.run.lines[k]), 'a plain read (what a resume reads) holds only the work in progress');
  const holding = keys => parts.filter(k => store.get(k).keys.some(x => keys.includes(x))).map(k => k.split('/')[1]);
  let mark = partReads.length, q0 = queries.length;
  res = await post({ op: 'runGet', runId, archived: true, orders: [done.receiptId] });
  assert(doneKeys.every(k => res.body.run.lines[k] && res.body.run.lines[k].state === 'committed'), 'a recalled set asks for its orders and gets their lines from the archive');
  let read = partReads.slice(mark).filter(x => x.fields.includes('json'));
  assert.deepEqual([...new Set(read.map(x => x.id))].sort(), holding(doneKeys).sort(), 'reading only the parts that hold those orders');
  assert.equal(read.length, new Set(read.map(x => x.id)).size, 'each once');
  assert.equal(queries.length - q0, 1, 'found with one query');
  console.log(`a recalled set of one order reads ${readKB(partReads.slice(mark))} of the archive`);
  assert.equal(res.body.run.lines[doneKeys[0]].engrave.text, `FOREVER ${done.receiptId.slice(-5)}`);
  mark = partReads.length;
  res = await post({ op: 'runGet', runId, archived: true });
  assert(res.body.run.archiveTruncated && Object.keys(res.body.run.lines).length > 900, 'a whole run is answered with its newest megabyte of lines, and says so');
  read = partReads.slice(mark);
  assert(read.filter(x => x.fields.includes('json')).length < parts.length && read.reduce((n, x) => n + x.bytes, 0) < 1300000, `and reads that megabyte, not the whole archive (${read.reduce((n, x) => n + x.bytes, 0)} B)`);
  assert(Object.keys(saved.lines).every(k => res.body.run.lines[k]), 'the working lines always among them');
  res = await post({ op: 'history', q: done.receiptId });
  const hit = res.body.runs.find(x => x.runId === runId);
  assert(hit && hit.hitOrders.includes(done.receiptId) && hit.lines === 2000 && hit.sheets === 400, 'history finds an archived order by its number and counts every line and sheet');
  res = await post({ op: 'history', q: `forever ${done.receiptId.slice(-5)}` });
  assert(res.body.runs.some(x => x.runId === runId), 'and by the words engraved on it');
  assert(res.body.scanned.lineParts >= parts.length);

  /* ═══ 4 · laser readiness and set completion read archived lines ═══ */
  const pools = done.lines.map(l => `${done.receiptId}_${l.transactionId}_1`), url = p => ({ path: p, url: 'https://example.com/' + p });
  const back = id => ({ poolId: id, sheetId: 'sheet-arch-1', approvedAt: 10, approvedBy: 'Paul', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: url(id + '.ai') } });
  const sheet = { id: 'sheet-arch-1', setId: 'set-arch', runId, day: '2026-09-20', metal: 'silver', status: 'complete', archived: false, poolIds: pools, placedCount: 2, verification: { ok: true }, outputs: { ai: url('front.ai'), preview: url('front.png') },
    orders: [done.receiptId], label: { files: [{ path: 'qr.png', url: 'https://example.com/qr.png', payload: done.receiptId, orders: [done.receiptId] }] }, backPool: done.lines.filter(l => l.engraved).map(l => back(`${done.receiptId}_${l.transactionId}_1`)) };
  store.set('Charm_Nest_Sheets/sheet-arch-1', sheet);
  store.set('Charm_Nest_Sets/set-arch', { setId: 'set-arch', runId, sheetIds: ['sheet-arch-1'], status: 'awaiting review' });
  mark = partReads.length;
  res = await post({ op: 'laserStatus', sheetIds: ['sheet-arch-1'] });
  const laser = res.body.sheets[0];
  assert.equal(laser.laser.ready, true, 'a sheet of archived lines is ready for the laser: its decisions come from the archive');
  assert.deepEqual([laser.engraving[pools[0]].state, laser.engraving[pools[1]].state], ['written', 'none'], 'the engraved copy reads as written, the plain one as plain');
  read = partReads.slice(mark);
  assert(!read.some(x => x.fields.includes('json')), 'readiness reads decisions, never the lines');
  console.log(`laser readiness of a sheet of archived orders reads ${readKB(read)} of the archive`);
  assert.deepEqual([...new Set(read.filter(x => x.fields.includes('decisions')).map(x => x.id))].sort(), holding(doneKeys).sort(), 'of the parts that hold the copies asked about, and no other');
  // a newer part wins over an older one for the same line, a changed decision and a copy the line no longer has alike
  const archiveOf = async lines => { const r2 = await post({ op: 'runArchive', runId, parts: [{ json: JSON.stringify(lines) }] }); assert.equal(r2.status, 200, JSON.stringify(r2.body)); };
  const doneLines = Object.fromEntries(doneKeys.map(k => [k, everyLine[k]]));
  await archiveOf({ [doneKeys[0]]: { ...doneLines[doneKeys[0]], engrave: { ...doneLines[doneKeys[0]].engrave, state: 'review', approved: false } } });
  res = await post({ op: 'laserStatus', sheetIds: ['sheet-arch-1'] });
  assert.equal(res.body.sheets[0].engraving[pools[0]].state, 'review', 'a line archived again reads as its newest part says');
  await archiveOf({ [doneKeys[1]]: { ...doneLines[doneKeys[1]], poolIds: [] } });
  res = await post({ op: 'laserStatus', sheetIds: ['sheet-arch-1'] });
  assert.deepEqual([res.body.sheets[0].engraving[pools[1]].state, res.body.sheets[0].laser.ready], ['unknown', false], 'a copy its newest line no longer has is not taken from an older part');
  await archiveOf(doneLines);
  res = await post({ op: 'laserStatus', sheetIds: ['sheet-arch-1'] });
  assert.equal(res.body.sheets[0].laser.ready, true, 'and the lines as they were read as ready again');
  // decisions stored under another version of the readiness policy are not trusted: that part is decided again from its lines
  const newest = docs('Charm_Nest_Run_Lines').map(k => [k, store.get(k)]).filter(([, p]) => p.runId === runId && p.keys.includes(doneKeys[0])).sort((a, b) => b[1].at - a[1].at || b[1].seq - a[1].seq)[0];
  assert.match(newest[1].decisionsVersion, /^[0-9a-f]{16}$/, 'a part says which policy decided it');
  const stored = { ...newest[1] };
  store.set(newest[0], { ...stored, decisions: JSON.stringify(Object.fromEntries(doneKeys.map(k => [k, Object.fromEntries(pools.map(id => [id, { needed: true, state: 'review', approved: false }]))]))), decisionsVersion: 'an-older-policy' });
  mark = partReads.length;
  res = await post({ op: 'laserStatus', sheetIds: ['sheet-arch-1'] });
  assert.deepEqual([res.body.sheets[0].engraving[pools[0]].state, res.body.sheets[0].engraving[pools[1]].state, res.body.sheets[0].laser.ready], ['written', 'none', true], 'a part decided under an older policy is decided again from its lines');
  assert.deepEqual(partReads.slice(mark).filter(x => x.fields.includes('json')).map(x => x.id), [newest[0].split('/')[1]], 'reading the lines of that part only');
  store.set(newest[0], stored);
  // hundreds of copies: a query per 30 of them, and each part that holds one read once, for its decisions
  const many = shut.flatMap(o => o.lines.map(l => `${o.receiptId}_${l.transactionId}_1`)).slice(0, 700), expect = Readiness.decisions(Object.values(everyLine));
  const decidedAs = (got, ids) => ids.every(id => JSON.stringify(got[id]) === JSON.stringify(expect[id] || { needed: true, state: 'unknown', approved: false }));
  store.set('Charm_Nest_Sheets/sheet-many', { ...sheet, id: 'sheet-many', setId: 'set-many', poolIds: many, backPool: [] });
  mark = partReads.length; q0 = queries.length;
  res = await post({ op: 'laserStatus', sheetIds: ['sheet-many'] });
  read = partReads.slice(mark).filter(x => x.fields.includes('decisions'));
  assert.equal(queries.length - q0, Math.ceil(700 / 30), 'many copies: one query per 30');
  assert(!partReads.slice(mark).some(x => x.fields.includes('json')) && read.length === new Set(read.map(x => x.id)).size, 'each part read once, never its lines');
  assert(read.length < docs('Charm_Nest_Run_Lines').length, `and only the parts that hold them (${read.length})`);
  console.log(`laser readiness of 700 archived copies reads ${readKB(partReads.slice(mark))} of the archive in ${queries.length - q0} queries (${read.length} of ${docs('Charm_Nest_Run_Lines').length} parts)`);
  assert(decidedAs(res.body.sheets[0].engraving, many), 'each copy decided as its line says');
  store.delete('Charm_Nest_Sheets/sheet-many');
  // a run with fewer parts than the copies asked about take queries reads its every part instead (its decisions only)
  const small = 'run-2026-09-20-small1', smallLines = Object.fromEntries(Object.entries(everyLine).filter(([, l]) => l.state === 'committed').slice(0, 40));
  store.set('Charm_Nest_Runs/' + small, { runId: small, day: '2026-09-20', status: 'processed', lines: {}, lineArchive: { lines: 40, parts: 1 } });
  assert.equal((await post({ op: 'runArchive', runId: small, parts: [{ json: JSON.stringify(smallLines) }] })).status, 200);
  const smallPools = Object.values(smallLines).flatMap(l => l.poolIds);
  store.set('Charm_Nest_Sheets/sheet-small', { ...sheet, id: 'sheet-small', setId: 'set-small', runId: small, poolIds: smallPools, backPool: [] });
  mark = partReads.length; q0 = queries.length;
  res = await post({ op: 'laserStatus', sheetIds: ['sheet-small'] });
  read = partReads.slice(mark);
  assert.equal(queries.length - q0, 0, 'no query per 30 copies for a run of one part');
  assert.deepEqual([read.length, read[0].fields.includes('json')], [1, false], 'its one part read once, for its decisions');
  assert(smallPools.length > 30 && decidedAs(res.body.sheets[0].engraving, smallPools), 'each copy decided as its line says');
  for (const k of ['Charm_Nest_Sheets/sheet-small', 'Charm_Nest_Runs/' + small]) store.delete(k);
  for (const k of docs('Charm_Nest_Run_Lines')) if (store.get(k).runId === small) store.delete(k);
  const withArchive = runDoc(runId); store.set('Charm_Nest_Runs/' + runId, { ...withArchive, lineArchive: undefined });
  res = await post({ op: 'laserStatus', sheetIds: ['sheet-arch-1'] });
  assert.deepEqual([res.body.sheets[0].laser.ready, res.body.sheets[0].laser.waiting, res.body.sheets[0].engraving[pools[1]].state], [false, 1, 'unknown'], 'without the archive the same sheet would wait on its plain copy for good');
  store.set('Charm_Nest_Runs/' + runId, withArchive);
  res = await post({ op: 'setList', includeSheets: true });
  assert(res.body.sheets.find(s => s.id === 'sheet-arch-1').laser.ready, 'the Library\'s set list reads it the same way');
  res = await post({ op: 'setUpdate', setId: 'set-arch', patch: { status: 'complete' } });
  assert.equal(res.status, 200, 'a set of archived lines can still be recorded complete: ' + JSON.stringify(res.body));
  store.set('Charm_Nest_Sheets/sheet-stray', { ...sheet, id: 'sheet-stray', setId: 'set-stray', poolIds: ['9999999999_8888888888_1'], backPool: [] });
  store.set('Charm_Nest_Sets/set-stray', { setId: 'set-stray', runId, sheetIds: ['sheet-stray'] });
  res = await post({ op: 'setUpdate', setId: 'set-stray', patch: { status: 'complete' } });
  assert(res.status >= 400 && /cannot be completed/.test(res.body.error), 'a copy no line knows still blocks completion');
  // (these sheets were made up for the checks above; the resume below restores the run's real sheets, and it has none here)
  for (const k of ['Charm_Nest_Sheets/sheet-stray', 'Charm_Nest_Sets/set-stray', 'Charm_Nest_Sheets/sheet-arch-1', 'Charm_Nest_Sets/set-arch']) store.delete(k);
  // the Rose Gold cut check reads through the same helper
  const RoseFactory = require(path.join(fnDir, '_charmNestRoseStock.js'));
  assert.equal(RoseFactory.length, 1, 'the rose module takes its dependencies in one object');
  assert(/decisionsOfRun\?await decisionsOfRun\(sheet\.runId,runData,sheet\.poolIds/.test(fs.readFileSync(path.join(fnDir, '_charmNestRoseStock.js'), 'utf8')) && /_charmNestRoseStock"\)\(\{[^}]*decisionsOfRun/.test(fs.readFileSync(path.join(fnDir, 'charmNestLibrary.js'), 'utf8')), 'a Rose Gold cut reads archived decisions too');

  /* ═══ 5 · a set undone: its lines come back into the record, and leave again unchanged ═══ */
  const undone = shut.find(o => kind(o) === 'normal' && o !== done);
  for (const row of main.st.rows) if (row.order === undone) row.state = 'written';
  await main.ctl.save(r);
  let rec = runDoc(runId);
  assert(undone.lines.every(l => rec.lines[O.lineKey(undone, l)]?.state === 'written') && rec.orders.includes(undone.receiptId), 'an undone order is back in the record, which a resume reads');
  res = await post({ op: 'runGet', runId, archived: true, orders: [undone.receiptId] });
  assert.equal(res.body.run.lines[O.lineKey(undone, undone.lines[0])].state, 'written', 'the record\'s own line wins over its archived copy');
  const before = docs('Charm_Nest_Run_Lines').length;
  for (const row of main.st.rows) if (row.order === undone) row.state = 'committed';
  await main.ctl.save(r);
  rec = runDoc(runId);
  assert(!rec.lines[O.lineKey(undone, undone.lines[0])] && docs('Charm_Nest_Run_Lines').length === before, 'committed again as it was, it leaves the record with no new archive part');
  assert.equal(rec.lineArchive.lines, saved.lineArchive.lines, 'and the count is unchanged');

  /* ═══ 6 · resume on another browser, from the record alone ═══ */
  const archivedCount = rec.lineArchive.lines, working = Object.keys(rec.lines).length, partsBefore = docs('Charm_Nest_Run_Lines').length;
  const other = controllerFor();
  await other.ctl.resumeRun(runId); await settle(other.ctx);
  const rr = other.ctx.B.run;
  assert.deepEqual([...other.st.pulled].sort(), open.map(o => o.receiptId).sort(), 'a resume pulls exactly the orders still open: nothing already cut comes back');
  assert.equal(other.st.rows.length, working, 'every working line is back');
  const heldRows = other.st.rows.filter(x => kind(x.order) === 'held');
  assert(heldRows.length && heldRows.every(x => x.state === 'held' && x.hold), 'with the state it was saved in');
  assert.equal(docs('Charm_Nest_Run_Lines').length, partsBefore, 'a resume archives nothing again');
  assert.equal(rr.lineArchive.base.lines, archivedCount, 'the archived lines are still counted');
  rec = runDoc(runId);
  assert.equal(rec.lineArchive.lines, archivedCount, 'and the record saved after the resume counts them once');
  assert.equal(Object.keys(rec.lines).length, working);
  res = await post({ op: 'runList' });
  assert.equal(res.body.runs.find(x => x.runId === runId).lines, 2000, 'the run list still counts 2,000 lines');
  assert.equal(rr.status, 'processed', 'the resumed run comes to rest, its held orders still pending');
  assert.match(rr.processingSignature, /^[0-9a-f]{16}$/, 'its work signature is a short hash');
  // a legacy whole-text signature still compares: poke does not restart unchanged work, and does restart changed work
  const legacy = other.ctx.workText(rr); rr.processingSignature = legacy; other.ctl.poke(); await tick();
  assert.equal(rr.status, 'processed', 'unchanged work under an old whole-text signature is not processed again');
  other.st.rows[0].state = 'pulled'; other.ctl.poke(); assert.equal(rr.status, 'running', 'changed work is'); await settle(other.ctx);

  /* ═══ 7 · the guard: a record that nears the limit is said once, in words ═══ */
  const big = controllerFor();
  const bigRun = { runId: 'run-2026-09-21-heavy1', day: '2026-09-21', step: 'engrave', status: 'processed', mode: 'auto', lines: {}, sheets: {}, holds: {}, errors: [], orders: [] };
  big.ctx.B.run = bigRun;
  for (let i = 0; i < 400; i++) { const o = newOrder(2, 200); for (const l of o.lines) big.st.rows.push({ key: O.lineKey(o, l), order: o, line: l, arrivedAt: o.arrivedAt, spec: { designSku: l.sku, material: 'silver', quantity: 1, engraveCandidate: true }, problems: [{ kind: 'needsMapping' }], state: 'held', reason: 'needs an option mapped', hold: 'needs an option mapped', wait: null, poolIds: [], engrave: null, material: 'silver' }); bigRun.orders.push(o.receiptId); }
  await big.ctl.save(bigRun); await big.ctl.save(bigRun);
  const said = big.st.logs.filter(e => e.kind === 'warn' && /% full/.test(e.text));
  assert.equal(said.length, 1, 'said once: ' + big.st.logs.map(e => e.text).join(' | '));
  assert.match(said[0].text, /online record of run run-2026-09-21-heavy1 is \d+% full \([\d,]+ KB of 1,024 KB; [\d,]+ of 40,000 index entries\), with 800 order lines still in progress/);
  assert.match(said[0].text, /a full record cannot be saved and the run stops/);
  assert.equal(big.st.toasts.filter(t => /% full/.test(t)).length, 1, 'and shown once');
  assert.equal(serverWarnings.filter(t => /run-2026-09-21-heavy1/.test(t)).length, bytesOf(runDoc('run-2026-09-21-heavy1')) > 0.7 * O.RUN_RECORD.bytes ? 1 : 0, 'the function log says it once when the bytes are near the limit');
  assert(!main.st.logs.concat(other.st.logs).some(e => /% full/.test(e.text)), 'a small record says nothing');

  /* ═══ 8 · an archive that cannot be written loses nothing ═══ */
  const flaky = controllerFor();
  const flakyRun = { runId: 'run-2026-09-22-flaky1', day: '2026-09-22', step: 'complete', status: 'processed', mode: 'auto', lines: {}, sheets: {}, holds: {}, errors: [], orders: [], committed: [] };
  flaky.ctx.B.run = flakyRun;
  for (let i = 0; i < 10; i++) { const o = newOrder(2, 300); advance(o, 330); for (const l of o.lines) { const row = { key: O.lineKey(o, l), order: o, line: l, arrivedAt: o.arrivedAt, spec: { designSku: l.sku, material: 'silver', quantity: 1, engraveCandidate: l.engraved }, problems: [], state: 'pulled', reason: null, hold: null, wait: null, poolIds: [], engrave: null, material: 'silver' }; syncRow(row); flaky.st.rows.push(row); } flakyRun.orders.push(o.receiptId); }
  flaky.st.failArchive = 1;
  await flaky.ctl.save(flakyRun);
  assert.equal(Object.keys(runDoc(flakyRun.runId).lines).length, 20, 'every line stays in the record while the archive cannot be written');
  assert.equal(flaky.st.logs.filter(e => /could not be moved out of the run record/.test(e.text)).length, 1, 'said once');
  await flaky.ctl.save(flakyRun);
  assert.equal(Object.keys(runDoc(flakyRun.runId).lines).length, 0, 'the next save moves them');
  assert.equal(runDoc(flakyRun.runId).lineArchive.lines, 20);

  /* ═══ 9 · the sandbox keeps its own archive; old records read as before; purge takes the archive too ═══ */
  const sand = controllerFor({ sandbox: true });
  const sandRun = { runId: 'run-2026-09-22-sandb1', day: '2026-09-22', step: 'complete', status: 'processed', mode: 'auto', lines: {}, sheets: {}, holds: {}, errors: [], orders: [], committed: [] };
  sand.ctx.B.run = sandRun;
  const so = newOrder(1, 400); advance(so, 430); const srow = { key: O.lineKey(so, so.lines[0]), order: so, line: so.lines[0], arrivedAt: 1, spec: { designSku: 'X', material: 'silver', quantity: 1, engraveCandidate: false }, problems: [], state: 'pulled', poolIds: [], engrave: null }; syncRow(srow); sand.st.rows.push(srow); sandRun.orders.push(so.receiptId);
  const prodParts = docs('Charm_Nest_Run_Lines').length;
  await sand.ctl.save(sandRun);
  assert(docs('Sandbox_Charm_Nest_Run_Lines').length === 1 && docs('Charm_Nest_Run_Lines').length === prodParts, 'a sandbox run archives into the sandbox only');
  store.set('Charm_Nest_Runs/run-legacy', { runId: 'run-legacy', day: '2026-09-01', status: 'complete', updatedAt: 5, lines: { a: { orderId: '8100', state: 'committed', poolIds: ['8100000000_7100000000_1'], engrave: { needed: true, state: 'written', approved: true, text: 'OLD WORDS' } } } });
  res = await post({ op: 'runList', limit: 200 }); assert.equal(res.body.runs.find(x => x.runId === 'run-legacy').lines, 1, 'a record from before the archive counts as it did');
  res = await post({ op: 'runGet', runId: 'run-legacy', archived: true }); assert.equal(res.body.run.lines.a.engrave.text, 'OLD WORDS', 'and reads as it did');
  res = await post({ op: 'history', q: 'old words' }); assert(res.body.runs.some(x => x.runId === 'run-legacy'), 'and is searched as it was');
  res = await post({ op: 'purgeHistory', code: '975311', force: true });
  assert(!docs('Charm_Nest_Run_Lines').length && !docs('Sandbox_Charm_Nest_Run_Lines').length && !docs('Charm_Nest_Runs').length, 'a purge of history takes the line archive with the runs');

  console.log('Run record size OK: 2,000 lines over 100 updates stay out of the record once their orders close, bounded by the work in hand; resume, run list, history, recalled sets, laser readiness, set completion, undo, sandbox, old records and a failed archive all still find every line; a record near the limit is said once');
})().catch(e => { console.error(e); process.exitCode = 1; });
