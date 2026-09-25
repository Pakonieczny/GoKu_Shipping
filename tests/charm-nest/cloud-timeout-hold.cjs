// A slow cloud never holds an order, and no HTML page reaches the screen (Paul, 25 Sep: order 4330132720 was held, its
// reason the edge's HTML "Inactivity Timeout" page, after a run that took every open order recorded four hundred pool
// rows in one call). And a sheet filling one charm at a time counts its charms as they land.
//   1. api() turns a page, an empty 5xx, a timeout and a network failure into plain words, marked as passing.
//   2. Pool.addAll keeps a line the cloud was slow to record in progress and takes it up again; an order held over such a
//      page before is taken up again; a real problem still holds.
//   3. The one-by-one fill's count: placed charms and how full they make the sheet.
// The real page and module code runs in a vm; the cloud is a stand-in. No network.
//   node tests/charm-nest/cloud-timeout-hold.cjs
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const O = require('../../charm-nest-orders.js');
const root = path.join(__dirname, '../..');
const page = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const slice = (src, from, to) => { const a = src.indexOf(from), b = src.indexOf(to, a); assert(a >= 0 && b > a, 'slice ' + from); return src.slice(a, b); };
const HTML = '<HTML> <HEAD> <TITLE>Inactivity Timeout</TITLE> </HEAD> <BODY BGCOLOR="white" FGCOLOR="black"> <H1>Inactivity Timeout</H1> <HR> <FONT FACE="Helvetica,Arial"><B> Description: Too much time has passed without sending any data for document. </B></FONT> <HR> </BODY>';

(async () => {
  /* ── 1 · api() ── */
  let answer = null;
  const net = { window: {}, S: { passcode: '', cloud: { ok: true } }, FN: '/fn', WORKSPACE_SANDBOX: false, JSON, Object, String, Math, Error, TypeError, TextDecoder, Uint8Array,
    fetch: async () => answer(), cloudRecovered() {} };
  vm.createContext(net);
  vm.runInContext(slice(page, 'async function readBody(res, bar) {', "/* The rail's headline") + ';this.api=api;this.cloudWords=cloudWords;', net);
  const res = (status, text) => ({ status, ok: status >= 200 && status < 300, headers: { get: () => null }, text: async () => text });
  const fails = async () => { try { await net.api('charmNestLibrary', { op: 'poolPut' }); } catch (e) { return e; } assert.fail('the call should have failed'); };
  answer = () => res(504, HTML);
  let e = await fails();
  assert.equal(e.message, 'The cloud did not answer in time (HTTP 504)', 'the edge\'s page reads as plain words'); assert.equal(e.transient, true);
  answer = () => res(200, HTML);
  const odd = await net.api('charmNestLibrary', { op: 'poolPut' });
  assert(!/</.test(odd.error), 'a page with a good status is not passed on as markup either');
  answer = () => res(502, '');
  e = await fails(); assert.equal(e.message, 'The cloud is not answering right now (HTTP 502)'); assert.equal(e.transient, true);
  answer = () => res(400, JSON.stringify({ error: 'bad run id' }));
  e = await fails(); assert.equal(e.message, 'bad run id'); assert.equal(e.transient, false, 'a real refusal is not one that passes');
  answer = () => { throw new TypeError('Failed to fetch'); };
  e = await fails(); assert.equal(e.transient, true, 'the network gone for a moment passes');
  answer = () => { throw Object.assign(new Error('The operation was aborted due to timeout'), { name: 'TimeoutError' }); };
  e = await fails(); assert.match(e.message, /no answer in 60 s — timed out/); assert.equal(e.transient, true);
  assert.equal(net.cloudWords('plain words', 500), 'plain words');

  /* ── 2 · Pool.addAll ── */
  let failPool = 1, requeued = 0;
  const timers = [], rows = [], entries = new Map([['OK', { sku: 'OK', aiPath: 'ok.ai', masterHash: 'm1' }], ['BROKEN', { sku: 'BROKEN', aiPath: 'broken.ai', masterHash: 'm3' }]]);
  const sheet = { metal: 'silver', charms: [], placements: [], status: 'idle' };
  const small = { charms: [{ areaPt2: 400, widthPt: 20, heightPt: 20, hash: 'h' }] };
  const mk = (id, sku, extra = {}) => { const r = { key: id + '/t' + id, order: { receiptId: id, createTs: +id, updateTs: 1 }, line: { transactionId: 't' + id, sku, title: sku }, spec: null, problems: [], state: 'pulled', reason: null, poolIds: [], engrave: null, ...extra }; rows.push(r); return r; };
  const logs = [];
  const ctx = {
    window: { CNProgress: { start: () => ({ set() {}, end() {} }) }, LiveNest: { intakePage: () => sheet, closed: () => false }, Arrivals: { requeue: () => { requeued++; } } },
    Date: { now: () => 1_000_000 }, JSON, Map, Set, Promise, Math, Object, Array, String, Number, Error, console, setTimeout: (f, ms) => { timers.push([f, ms]); return timers.length; },
    B: { pool: { rows: new Map(), sources: new Map([['ok.ai', small]]) }, master: { entries } }, S: { cloud: { ok: true }, settings: { insetPt: 0, maxFill: 0.8, silhouetteRes: 6, minPt: 6 }, sheets: { silver: { active: 0, pages: [sheet] } }, poolSources: {} },
    O, MM: 25.4 / 72,
    Master: { entryFor: sku => entries.get(String(sku).toUpperCase()) || null, fetchEntry: async () => null, skuRegex: () => /x/ },
    Orders: { rows: () => rows, interpretAll() { for (const r of rows) { r.spec = { designSku: r.line.sku, material: 'silver', quantity: 1, size: null, problems: [] }; r.problems = []; } }, render() {}, lineRecord: r => [r.key, r.state] },
    Gate: { plan: async list => ({ take: new Set(list.map(r => r.key)), wait: new Map() }), afterPool: async () => {} },
    Review: { syncOrderItems() {}, problemText: p => p.kind }, RunCtl: { save: async () => {} },
    api: async (fn, body) => { if (body.op === 'poolPut') { if (failPool > 0) { failPool--; throw Object.assign(new Error('The cloud did not answer in time (HTTP 504)'), { status: 504, transient: true }); } return {}; } if (body.op === 'url') throw new Error('design file unreachable'); return {}; },
    CharmNestAssets: { bytes: async () => { throw new Error('unreachable'); } }, P: {},
    stockFor: () => ({ wPt: 1000, hPt: 1000 }), labelOf: m => m, allSheets: () => [sheet], pagesOf: () => [sheet], addPage: () => sheet,
    sheetDirty() {}, renderCard() {}, renderRail() {}, updateTopSub() {}, refreshAllCards() {}, agent: (s, k, t) => logs.push(t),
  };
  ctx.CNProgress = ctx.window.CNProgress; ctx.LiveNest = ctx.window.LiveNest;
  vm.createContext(ctx);
  vm.runInContext(slice(bridge, 'const Pool = window.Pool = (() => {', '/* Carry-forward'), ctx);
  const Pool = ctx.window.Pool, run = { runId: 'run', lines: {} };
  const a = mk('4330132720', 'OK'), b = mk('4330132721', 'OK');
  await Pool.addAll(run);
  for (const r of [a, b]) { assert.equal(r.state, 'pulled', 'a line the cloud was slow to record stays in progress, not held'); assert.match(r.reason, /did not answer in time.*trying again/); }
  assert(logs.some(t => /not held/.test(t)), 'and the log says it was not held');
  assert.equal(timers.length, 1, 'one retry is set'); assert.equal(timers[0][1], 30000, 'thirty seconds on');
  timers[0][0](); assert.equal(requeued, 1, 'which takes the lines up again');
  await Pool.addAll(run);
  for (const r of [a, b]) { assert.equal(r.state, 'pooled', 'and they are recorded once the cloud answers'); assert.equal(r.reason, null); }
  // an order held before over the edge's page is taken up again; one held for a real problem stays held
  const old = mk('4330132722', 'OK', { state: 'held', reason: HTML }), real = mk('4330132723', 'BROKEN');
  await Pool.addAll(run);
  assert.equal(old.state, 'pooled', 'an order held over the page before is taken up again');
  assert.equal(real.state, 'held', 'a design that will not load still holds its line'); assert(!/</.test(real.reason));
  assert(!rows.some(r => /</.test(String(r.reason || ''))), 'no reason carries markup');

  /* ── 3 · the one-by-one fill's count ── */
  const fill = { usableArea: () => 10000, Set, Map, Math };
  vm.createContext(fill);
  vm.runInContext(slice(page, 'function carefulSoFar(sh) {', '/** Inner cut lines of a charm') + ';this.carefulSoFar=carefulSoFar;', fill);
  const plain = v => JSON.parse(JSON.stringify(v));   // made in the vm, compared here
  const charms = [{ id: 'a', areaPt2: 1000 }, { id: 'b', areaPt2: 2000 }, { id: 'c', areaPt2: 500 }];
  assert.equal(fill.carefulSoFar({ charms, placements: [], probePlaced: [] }), null, 'nothing landed yet: the sheet says it is preparing');
  assert.deepEqual(plain(fill.carefulSoFar({ charms, placements: [], probePlaced: [{ id: 'a' }, { id: 'b' }] })), { placed: 2, full: 0.3 }, 'charms that landed one by one are counted');
  assert.deepEqual(plain(fill.carefulSoFar({ charms, placements: [{ id: 'a' }], probePlaced: [{ id: 'a' }, { id: 'c' }] })), { placed: 2, full: 0.15 }, 'with those already on the sheet, each once');
  const drawn = slice(page, 'function paintPreview(cv, sh, clean, R) {', '  (sh.roseCutAt ? [] : sh.placements)');
  assert.match(drawn, /!sh\.probe && !sh\.probePlaced\?\.length/, '"Preparing… rotations and masks" is not drawn over charms that have landed');

  console.log('Cloud timeout hold OK: the edge\'s page, an empty 5xx, a timeout and the network read as plain passing failures; a line the cloud was slow to record stays in progress and is recorded on the next pass; an order held over the page before is taken up again; a real problem still holds; the one-by-one fill counts its charms as they land');
})().catch(e => { console.error(e); process.exitCode = 1; });
