// FLAGLOAD (Paul, 11 Oct 2026, 12:33 UTC): three Review rows (Zodiac Sign: Scorpio, listing 1160815385; Zodiac Sign: Cancer, 856808566;
// Letter: T, 883649717) said `option "…" not mapped — Etsy's SKU list for listing N is not loaded yet`, though each row's charm was right.
//
//  the cause, from the code:
//   · the page asks the cloud (op listingSkus) for the SKU table of every listing a waiting line names, but the SANDBOX's ask was cache-only:
//     the cloud read Charm_Listing_Skus and, on a miss, answered "pending, why: cache" without asking Etsy;
//   · the cache was filled by nothing but a production page asking Etsy for ITS waiting lines, or by hand (OPTLINK filled 10 listings of the
//     Sep 17 snapshot; these three were not among them). A listing only the sandbox's own newest orders carry could never be filled;
//   · so the row stayed "not loaded yet" for good (the page backed off 10 min, 30 min, 90 min … 6 h and asked the same empty cache again).
//     Not the 60-a-day cap, not an Etsy error, not a stale page state: the sandbox never reached Etsy.
//
//  FLAGWHY (same hour) settles the lines whose transaction already carries the option's own master SKU (ZODIAC_CONSTE_GEM-7 …) without any table, so those three rows
//  clear on their own and ask nothing (tests/charm-nest/option-own-sku.cjs). This file is the other half: a line whose transaction carries only the listing's UMBRELLA SKU (the
//  OPTLINK lines: "Zodiac REVAMP", "Birth_6106") or none can be read only through the listing's table, and that table now loads in the sandbox too.
//
//  the fix this test pins: the sandbox asks Etsy for what its waiting lines need, inside the SAME rules (6 a request, one GET per listing per 7 days,
//  and, since NOLIMIT (Paul, 13 Oct 2026), NO daily cap at all: neither for the shop nor for the sandbox; the calls are only counted), stores the shop's table (production finds it), the page asks six listings a request, the wanted
//  ones first (an option question before a missing SKU before a font/count question; the oldest order first), reads each answer at once (the rows clear
//  with nothing saved by a person), shows a labelled spinner while it asks and one plain line when it could not, and never retries in a loop.
// No network, no Etsy, no AI.   node tests/charm-nest/table-load.cjs
const path = require('path'), fs = require('fs'), assert = require('assert/strict'), Module = require('module');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
const O = require(path.join(root, 'charm-nest-orders.js'));
const T = require(path.join(fnDir, '_charmNestListingSkus.js'));
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const ok = name => console.log('  ✓', name);
const clone = x => JSON.parse(JSON.stringify(x));
const DAY = 86400000;

/* ── what Etsy keeps for Paul's three listings: one SKU per sign / letter, whatever the metal ── */
const SIGNS = ['Aries', 'Taurus', 'Gemini', 'Cancer', 'Leo', 'Virgo', 'Libra', 'Scorpio', 'Sagittarius', 'Capricorn', 'Aquarius', 'Pisces'];
const LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
const PAUL = ['1160815385', '856808566', '883649717'], EXTRA = {};
const LISTINGS = {
  '1160815385': { title: 'Zodiac Gem Necklace', metalName: 'Metal Colour', metals: ['Silver', 'Gold'], optName: 'Zodiac Sign', values: SIGNS, sku: i => 'Zodiac_Conste_Gem-' + i, umbrella: 'Zodiac Gem Necklace', picked: 'Scorpio', receipt: '4197589560', createTs: 3000 },
  '856808566': { title: 'Zodiac Stud Earrings', metalName: 'Metal Choice :', metals: ['Gold - Pair', 'Silver - Pair'], optName: 'Zodiac Sign', values: SIGNS, sku: i => 'Zodiac_Earrings-' + i, umbrella: 'Zodiac REVAMP', picked: 'Cancer', receipt: '4195168363', createTs: 1000 },
  '883649717': { title: 'Letter Stud Earrings', metalName: 'Color', metals: ['Sterling Silver', 'Gold'], optName: 'Letter', values: LETTERS, sku: i => 'Letter_Earring-' + i, umbrella: 'Letter Stud', picked: 'T', receipt: '4194545985', createTs: 2000 }
};
const specOf = id => LISTINGS[id] || EXTRA[id];
const inventoryOf = (id, spec = specOf(id)) => ({ products: spec.values.flatMap((v, i) => spec.metals.map((m, k) => ({ product_id: +id * 100 + i * 10 + k, sku: spec.sku(i), is_deleted: false, property_values: [
  { property_id: 100, property_name: spec.metalName, value_ids: [200 + k], values: [m] }, { property_id: 101, property_name: spec.optName, value_ids: [300 + i], values: [v] }] }))) });
// sku: what the transaction carries: the listing's umbrella SKU (shared by every choice, in no master) unless the test says otherwise
const lineOf = (id, spec = specOf(id), value = spec.picked, sku = spec.umbrella || 'UMBRELLA ' + id) => {
  const i = spec.values.indexOf(value), k = 0, productId = String(+id * 100 + i * 10 + k);
  return { transactionId: id + '1', listingId: id, productId, sku, title: spec.title, quantity: 1, metalKey: /silver/i.test(spec.metals[k]) ? 'silver' : 'gold', metalLabel: '', personalization: [], buyerMessage: '',
    variations: [{ name: spec.metalName, value: spec.metals[k], propertyId: '100', valueId: String(200 + k) }, { name: spec.optName, value, propertyId: '101', valueId: String(300 + i) }] };
};
const master = new Map(); for (const id of PAUL) LISTINGS[id].values.forEach((v, i) => master.set(LISTINGS[id].sku(i).toUpperCase(), {}));
const loose = {}; for (const k of master.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: s => master.get(String(s).toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '' }, extra);
const orderOf = (rid, createTs = 1) => ({ receiptId: rid, updateTs: 1, createTs, staffNote: '', buyerMessage: '' });
const open = sp => sp.problems.filter(p => p.kind === 'needsMapping');

/* ── 0 · the three listings: with the umbrella SKU on the transaction the row waits ("not loaded yet"); with the table it is answered by the SKU Etsy ties to the option ── */
function rows() {
  for (const id of PAUL) {
    const spec = LISTINGS[id], line = lineOf(id), sp = O.interpretLine(orderOf(spec.receipt), line, ctx({ listingSkus: {} })), q = open(sp)[0], i = spec.values.indexOf(spec.picked);
    assert(q, id + ': without a table the option waits: ' + JSON.stringify(sp.problems)); assert.equal(q.optionValue, spec.picked);
    assert.match(O.inventoryWhy(line, undefined, true), new RegExp('SKU list for listing ' + id + ' is not loaded yet'));
    assert.match(q.why, new RegExp('SKU list for listing ' + id + ' is not loaded yet'), 'the Review row says so');
    const table = T.fromStored(T.toStored(T.compact(inventoryOf(id), 1))), sp2 = O.interpretLine(orderOf(spec.receipt), line, ctx({ listingSkus: { [id]: table } }));
    assert.equal(open(sp2).length, 0, id + ': with the table the option is answered by the SKU Etsy ties to it: ' + JSON.stringify(sp2.problems));
    assert.equal(sp2.designSku, spec.sku(i).toUpperCase()); assert.equal(sp2.problems.length, 0); assert.equal(sp2.skuSource, 'inventory');
  }
  ok('the three listings (umbrella SKU on the transaction): "not loaded yet" without the table; with it the option is answered by its own SKU and nothing else waits (Scorpio, Cancer, T)');
}

/* ── the cloud: in-memory Firestore (refuses an array in an array), a fake Etsy, the real charmNestLibrary handler (loaded once, over a delegating fake) ── */
let cur = null;
function world() {
  const store = new Map(), w = { store, asked: [], reads: 0, writes: 0, fail: new Set(), inventory: id => inventoryOf(id, specOf(id) || { title: 'x', metalName: 'Metal', metals: ['Gold'], optName: 'Option', values: ['a', 'b', 'c'], sku: i => 'X' + id + '-' + i }) };
  const refOf = (c, id) => ({ id, async get() { w.reads++; const d = store.get(c + '/' + id); return { exists: !!d, id, data: () => (d ? clone(d) : undefined) }; }, async set(d) { w.writes++; refuseNestedArrays(d, c + '/' + id); store.set(c + '/' + id, clone(d)); } });
  const q = c => ({ orderBy() { return q(c); }, limit() { return q(c); }, startAfter() { return q(c); }, async get() { return { size: 0, docs: [], empty: true }; }, doc: id => refOf(c, id) });
  w.db = { collection: c => q(c), async getAll(...refs) { return Promise.all(refs.map(r => r.get())); } };
  w.etsy = { getListingInventory: async id => { w.asked.push(String(id)); if (w.fail.has(String(id))) { const e = new Error('Etsy 500'); e.status = 500; throw e; } return w.inventory(id); } };
  w.budget = () => store.get('Charm_Listing_Skus/_budget');
  return w;
}
const fakeDb = { collection: c => cur.db.collection(c), getAll: (...r) => cur.db.getAll(...r) }, fakeEtsy = { getListingInventory: id => cur.etsy.getListingInventory(id) };
const fakeAdmin = { firestore: Object.assign(() => fakeDb, { FieldValue: { serverTimestamp: () => ({ __ts: true }), increment: n => n, delete: () => undefined }, FieldPath: { documentId: () => '__name__' } }), storage: () => ({ bucket: () => ({}) }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (req === 'firebase-admin' || /[\/]firebaseAdmin(\.js)?$/.test(req) || req === './firebaseAdmin') return fakeAdmin; if (/[\/]_etsyMailEtsy(\.js)?$/.test(req)) return fakeEtsy; return realLoad.call(this, req, ...rest); };
async function withHandler(w, fn) {
  const was = cur; cur = w;
  try {
    delete process.env.EDIT_PASSCODE;
    const lib = require(path.join(fnDir, 'charmNestLibrary.js'));
    const post = body => lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }).then(r => JSON.parse(r.body || '{}'));
    return await fn(post);
  } finally { cur = was; }
}

/* ── 1 · the cloud: the sandbox fills what nothing stored, inside the shop's caps ── */
async function cloud() {
  // (a) the sandbox asks Etsy once per listing, stores the shop's table, and production finds it (no second GET, from either side)
  let w = world();
  await withHandler(w, async post => {
    const ids = PAUL.slice();
    let r = await post({ op: 'listingSkus', listingIds: ids, sandbox: true });
    assert.equal(r.etsyCalls, 3); assert.deepEqual(r.pending, []); assert.equal(r.why, null); assert.deepEqual(w.asked, ids); assert.deepEqual(r.sandboxCap, { used: 3, max: null }); assert.deepEqual(r.cap, { used: 3, max: null }, 'counted, never capped (no daily maximum)');
    for (const id of ids) assert(r.tables[id] && r.tables[id].products.length, id + ' came back with its products');
    assert(w.store.has('Charm_Listing_Skus/1160815385') && !w.store.has('Sandbox_Charm_Listing_Skus/1160815385'), 'stored where production reads it (the shop\'s cache, not a sandbox copy)');
    assert.deepEqual(w.budget(), Object.assign({}, w.budget(), { n: 3, sb: 3 }));
    r = await post({ op: 'listingSkus', listingIds: ids, sandbox: true }); assert.equal(r.etsyCalls, 0); assert.equal(w.asked.length, 3, 'a second sandbox ask is free');
    r = await post({ op: 'listingSkus', listingIds: ids }); assert.equal(r.etsyCalls, 0); assert.equal(w.asked.length, 3, 'production finds the sandbox\'s tables: it asks Etsy nothing');
    assert.deepEqual(w.budget(), Object.assign({}, w.budget(), { n: 3, sb: 3 }), 'and the budget is unchanged');
    // cacheOnly stays free
    const w2 = world(); await withHandler(w2, async p2 => { const c = await p2({ op: 'listingSkus', listingIds: ids, sandbox: true, cacheOnly: true }); assert.equal(c.etsyCalls, 0); assert.equal(c.why, 'cache'); assert.equal(w2.asked.length, 0); });
  });
  ok('cloud: the sandbox asks Etsy once for a listing nothing stored, stores the shop\'s table; production and the next sandbox ask cost no Etsy call; cacheOnly still asks nothing');

  // (b) 6 a request; NOLIMIT (13 Oct 2026): no daily cap at all and no share for the sandbox: 70 listings in a day all go through (the old gate stopped at 30 for the
  //     sandbox, then at 60 for the day), why is never "day" or "sandbox", the calls are counted, and production is not stopped afterwards either
  w = world();
  await withHandler(w, async post => {
    const ids = Array.from({ length: 70 }, (_, i) => String(5000000 + i));
    let calls = 0, last; const whys = new Set();
    for (let i = 0; i < 70; i += 6) { last = await post({ op: 'listingSkus', listingIds: ids.slice(i, i + 6), sandbox: true }); calls += last.etsyCalls; whys.add(last.why); assert(last.etsyCalls <= 6, 'six a request, no more'); assert.deepEqual(last.pending, [], 'nothing is held back at request ' + (i / 6 + 1)); }
    assert.equal(calls, 70, 'all 70 asks went to Etsy: past the old 30 and past the old 60'); assert.equal(w.asked.length, 70);
    assert.deepEqual([...whys], [null], 'no request was ever stopped: why is never "day", "sandbox" or "batch": ' + [...whys]);
    assert.deepEqual(last.sandboxCap, { used: 70, max: null }); assert.deepEqual(last.cap, { used: 70, max: null }, 'the count goes on past 60 and there is no maximum');
    assert.deepEqual(w.budget(), Object.assign({}, w.budget(), { n: 70, sb: 70 }), 'the day\'s document only counts');
    for (const id of ids) assert(last.tables[id] || (await post({ op: 'listingSkus', listingIds: [id], sandbox: true })).tables[id], id + ' is stored');
    const r = await post({ op: 'listingSkus', listingIds: ['7000001', '7000002'] }); assert.equal(r.etsyCalls, 2, 'production still asks, with 70 already spent today'); assert.equal(r.why, null); assert.deepEqual(r.pending, []); assert.deepEqual(r.cap, { used: 72, max: null });
    assert.equal(w.asked.length, 72);
    // one request still asks six at most (CALL_CAP, how many ONE request asks: not a daily limit), the rest wait for the next request
    const more = Array.from({ length: 8 }, (_, i) => String(6000000 + i)), a = await post({ op: 'listingSkus', listingIds: more, sandbox: true });
    assert.equal(a.etsyCalls, 6); assert.equal(a.why, 'batch'); assert.equal(a.pending.length, 2);
    const b = await post({ op: 'listingSkus', listingIds: a.pending, sandbox: true }); assert.equal(b.etsyCalls, 2); assert.equal(b.why, null); assert.deepEqual(b.pending, []);
  });
  ok('cloud: 6 Etsy calls a request and no daily limit: 70 sandbox asks all go through (the old stops at 30 and 60 are gone), why is never "day" or "sandbox", production still asks afterwards, the calls are only counted');

  // (c) the order Etsy is asked in: no table at all, then a table missing the names a line needs, then a week-old one; the page's order inside each group
  w = world(); const now = Date.UTC(2026, 9, 11, 12, 0, 0);
  w.store.set('Charm_Listing_Skus/4000001', { at: now - 9 * DAY, n: 2, uni: 'OLD1' });
  w.store.set('Charm_Listing_Skus/4000002', T.toStored(T.compact(inventoryOf('1160815385'), now - 1000)));   // (stored before Etsy's value names were kept: no names)
  delete w.store.get('Charm_Listing_Skus/4000002').nm; delete w.store.get('Charm_Listing_Skus/4000002').pn;
  const asked = [], env = { db: w.db, now, sandbox: true, nameIds: ['4000002'], fetchInventory: async id => { asked.push(id); return inventoryOf('1160815385'); } };
  const r = await T.lookup(['4000001', '4000002', '4000003', '4000004', '4000005', '4000006', '4000007', '4000008'], env);
  assert.deepEqual(asked, ['4000003', '4000004', '4000005', '4000006', '4000007', '4000008'], 'no table first, in the page\'s order');
  assert.equal(r.etsyCalls, 6); assert.deepEqual(r.pending.sort(), ['4000001', '4000002'], 'the week-old table and the one without names wait their turn'); assert.equal(r.tables['4000001'].uni, 'OLD1', 'and the old table still answers');
  const w3 = world(); w3.store.set('Charm_Listing_Skus/4000001', { at: now - 9 * DAY, n: 2, uni: 'OLD1' }); w3.store.set('Charm_Listing_Skus/4000002', clone(w.store.get('Charm_Listing_Skus/4000002')));
  const asked3 = [], r3 = await T.lookup(['4000001', '4000002', '4000003'], { db: w3.db, now, sandbox: true, nameIds: ['4000002'], fetchInventory: async id => { asked3.push(id); return inventoryOf('1160815385'); } });
  assert.deepEqual(asked3, ['4000003', '4000002', '4000001'], 'no table, then the table that lacks the names a line needs, then the week-old one'); assert.equal(r3.etsyCalls, 3);
  ok('cloud: a listing with no table is asked before one that only needs a refresh; the old table still answers meanwhile');

  // (d) an Etsy failure ends the request, is not stored and not repeated; a deleted listing is remembered; nothing is written but what Etsy gave
  w = world(); w.fail.add('5100002');
  await withHandler(w, async post => {
    let q = await post({ op: 'listingSkus', listingIds: ['5100001', '5100002', '5100003'], sandbox: true });
    assert.deepEqual(w.asked, ['5100001', '5100002'], 'after the failure nothing more is asked in the request'); assert.equal(q.why, 'error'); assert.deepEqual(q.pending.sort(), ['5100002', '5100003']);
    assert(w.store.has('Charm_Listing_Skus/5100001') && !w.store.has('Charm_Listing_Skus/5100002'), 'the failed ask is not stored');
    assert.equal(q.etsyCalls, 2, 'and it counts as a call: the cap is the cap');
  });
  ok('cloud: a failed Etsy call is not stored and not repeated inside the request (the page backs off)');
}

/* ── 2 · the page: the real loader block of charm-nest-bridge.js, run against the real cloud op ── */
function pageBlock(env) {
  const text = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const a = text.indexOf('const TABLES_LS ='), b = text.indexOf('async function pull(run');
  assert(a > 0 && b > a, 'the loader block is where the test looks for it');
  const code = text.slice(a, b);
  const make = new Function('B', 'S', 'rowsOf', 'api', 'interpretAll', 'Review', 'O', 'esc', 'document', 'localStorage', 'setTimeout', 'clearTimeout', 'Date', code + '\n;return { askTables, wantTables, takeTables, needsTable, tableNoteHtml, tableNoteInner, paintTableNotes, tableTry, tableNote, tableRank };');
  return make(env.B, env.S, () => env.B.orders.rows, env.api, env.interpretAll, env.Review, O, s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])),
    { querySelectorAll: () => [] }, { getItem: () => null, setItem: () => {} }, env.setTimeout, env.clearTimeout, env.Date);
}
async function page() {
  const MORE = [['7100001', 100], ['7100002', 4000], ['7100003', 5000], ['7100004', 6000]];
  const mk = async (scenario) => {
    const w = world(); let clock = Date.now(); const timers = [], repooled = [];
    const FakeDate = class extends Date { static now() { return clock; } };
    const B = { orders: { rows: [] }, maps: { listingSkus: {}, optionMaps: {} } }, S = { cloud: { ok: true } };
    const addRow = (line, receipt, createTs) => B.orders.rows.push({ key: receipt + '/' + line.transactionId, order: orderOf(receipt, createTs), line, spec: null, problems: [], state: 'held', reason: 'waiting', arrivedAt: createTs * 1000, poolIds: [] });
    // the wanted lines, NOT in the order they should be asked: Paul's three (ages 3000, 1000, 2000), four more option questions (100, 4000, 5000, 6000)
    // and one line with no SKU at all (age 50: older than all of them, but it only lacks its SKU, so it comes after the option questions)
    for (const id of PAUL) addRow(lineOf(id), LISTINGS[id].receipt, LISTINGS[id].createTs);
    for (const [id, ts] of MORE) { const spec = EXTRA[id] = { title: 'Zodiac X ' + id, metalName: 'Metal', metals: ['Gold'], optName: 'Zodiac Sign', values: SIGNS, sku: i => 'ZX' + id + '-' + i, picked: 'Leo' }; SIGNS.forEach((v, i) => master.set(spec.sku(i).toUpperCase(), {})); addRow(lineOf(id, spec), '41' + id, ts); }
    EXTRA['7100005'] = { title: 'NoSku', metalName: 'Metal', metals: ['Gold'], optName: 'Zodiac Sign', values: SIGNS, sku: i => 'NS-' + i };
    addRow(Object.assign(lineOf('7100001', EXTRA['7100001'], 'Aries'), { listingId: '7100005', sku: '', variations: [{ name: 'Metal', value: 'Gold', propertyId: '100', valueId: '200' }], productId: '71000050' }), '4171000050', 50);
    // a line whose transaction carries the option's own master SKU of a numbered family (FLAGWHY settles it with no table): it must ask for nothing
    const own = EXTRA['7100007'] = { title: 'Zodiac Y', metalName: 'Metal', metals: ['Gold'], optName: 'Zodiac Sign', values: SIGNS, sku: i => 'ZY7100007-' + i, picked: 'Leo' }; SIGNS.forEach((v, i) => master.set(own.sku(i).toUpperCase(), {}));
    addRow(lineOf('7100007', own, 'Leo', own.sku(4).toUpperCase()), '4171000070', 20);
    const interpretAll = () => { for (const r of B.orders.rows) { r.spec = O.interpretLine(r.order, r.line, ctx({ listingSkus: B.maps.listingSkus, optionMaps: B.maps.optionMaps })); r.problems = r.spec.problems.slice(); } };
    interpretAll();
    const Review = { repool: async r => { repooled.push(r.key); r.state = 'pulled'; r.reason = null; } };
    const seen = [];   // what the Review rows of Paul's three said at each request
    const api = async (fn, body) => {
      seen.push({ ids: body.listingIds.slice(), note: B.orders.rows.slice(0, 3).map(r => block.tableNoteHtml([r])) });
      if (scenario.netDown) throw Object.assign(new Error('network'), { transient: true });
      return withHandler(w, post => post(Object.assign({}, body, { sandbox: true })));   // (the page tags every cloud call in the sandbox: api() in charm-nest-1.html)
    };
    const block = pageBlock({ B, S, api, interpretAll, Review, setTimeout: (fn, ms) => { timers.push({ fn, ms }); return timers.length; }, clearTimeout: () => {}, Date: FakeDate });
    return { w, B, S, block, timers, repooled, seen, setClock: ms => { clock = ms; }, clock: () => clock };
  };
  const today = () => new Date(Date.now()).toISOString().slice(0, 10);
  const SCORPIO = 0, CANCER = 1, T_ROW = 2, FIRST_MORE = 3;   // (the rows' places)

  // what a table can and cannot answer: the rank a waiting line gets
  const stub = (...problems) => ({ problems });
  const rk = (...ps) => { const s0 = { askTables() {} }; return pageBlock({ B: { orders: { rows: [] }, maps: { listingSkus: {} } }, S: { cloud: { ok: true } }, api: async () => null, interpretAll() {}, Review: {}, setTimeout() {}, clearTimeout() {}, Date }).tableRank(stub(...ps)); };
  assert.equal(rk({ kind: 'needsMapping', optionName: 'Zodiac Sign' }), 0); assert.equal(rk({ kind: 'unmatchedSku' }), 1); assert.equal(rk({ kind: 'needsMapping', pairSecond: { why: 'x' } }), 1);
  assert.equal(rk({ kind: 'needsMapping', font: { why: 'x' } }), 2); assert.equal(rk({ kind: 'needsMapping', count: { why: 'x' } }), 2); assert.equal(rk({ kind: 'needsMapping', earWords: { why: 'x' } }), 2);
  assert.equal(rk({ kind: 'needsMapping', font: {} }, { kind: 'needsMapping', optionName: 'Zodiac Sign' }), 0, 'one plain option question makes it an option line');
  ok('page: an option question ranks first, a missing SKU (or a second sign) next, a font / count / ear-words question last');

  // (1) the whole round: priority order, six a request, spinner while asking, rows clear with nothing saved, no second ask
  let s = await mk({});
  assert.equal(s.B.orders.rows.filter(r => s.block.needsTable(r)).length, 8, 'eight listings wait (the line with the option\'s own SKU does not): ' + s.B.orders.rows.map(r => r.line.listingId + ':' + r.problems.map(p => p.kind)).join(' '));
  assert.deepEqual(s.B.orders.rows.slice(0, 8).map(r => s.block.tableRank(r)), [0, 0, 0, 0, 0, 0, 0, 1], 'seven option questions, then a missing SKU'); assert(!s.block.needsTable(s.B.orders.rows[8]) && s.B.orders.rows[8].problems.length === 0, 'the line with the option\'s own SKU is settled with no table: it asks for none');
  const before = clone(s.B.maps.optionMaps);
  await s.block.askTables();
  assert.deepEqual(s.w.asked, ['7100001', '856808566', '883649717', '1160815385', '7100002', '7100003', '7100004', '7100005'], 'option questions first, the oldest order first; the line that only lacks its SKU after them');
  assert.deepEqual(s.seen.map(x => x.ids.length), [6, 2], 'six listings a request');
  assert(s.seen[0].note.every(n => /class="cuStat"/.test(n) && /class="spin"/.test(n) && /Loading Etsy's SKU list for listing \d+…/.test(n)), 'while it asks, the Review row shows a labelled spinner: ' + s.seen[0].note[0]);
  assert.equal(s.w.asked.length, new Set(s.w.asked).size, 'one Etsy GET per listing');
  for (const r of s.B.orders.rows.slice(0, 7)) { assert.equal(r.problems.length, 0, r.line.listingId + ' reads clean now: ' + JSON.stringify(r.problems)); assert.equal(s.block.tableNoteHtml([r]), '', 'no note once the table is here'); }
  assert.deepEqual(s.repooled.slice().sort(), s.B.orders.rows.slice(0, 7).map(r => r.key).sort(), 'every held line that now reads clean is made up again, with nothing re-saved by a person');
  assert.deepEqual(s.B.maps.optionMaps, before, 'no option map was written for them');
  assert.equal(s.B.orders.rows[SCORPIO].spec.designSku, 'ZODIAC_CONSTE_GEM-7'); assert.equal(s.B.orders.rows[CANCER].spec.designSku, 'ZODIAC_EARRINGS-3'); assert.equal(s.B.orders.rows[T_ROW].spec.designSku, 'LETTER_EARRING-19');
  assert(!s.w.asked.includes('7100007'), 'and no Etsy call was made for it');
  const n0 = s.w.asked.length, req0 = s.seen.length; await s.block.askTables(); assert.equal(s.seen.length, req0, 'nothing more is asked once every table is here'); assert.equal(s.w.asked.length, n0);
  assert.deepEqual({ n: s.w.budget().n, sb: s.w.budget().sb }, { n: 8, sb: 8 });
  ok('page: eight waiting listings go six a request, option questions first and the oldest order first; a spinner while asking; Scorpio, Cancer and T clear and are made up again with nothing re-saved; no second ask');

  // (2) Etsy fails for one listing: one plain line, nothing retried in the round, asked again after the backoff, then it loads
  s = await mk({}); s.w.fail.add('856808566');
  await s.block.askTables();
  assert.deepEqual(s.w.asked, ['7100001', '856808566'], 'the round stops at the failure: the failed listing is asked once and nothing after it');
  const callsAfter = s.w.asked.length, reqAfter = s.seen.length;
  const failed = s.block.tableNoteHtml([s.B.orders.rows[CANCER]]);
  assert.match(failed, /class="tblFail"/); assert.match(failed, /Etsy did not answer for this listing\. It is asked again at \d\d?:\d\d/, failed);
  assert(!/spin/.test(failed), 'no spinner once it failed');
  assert.equal(s.block.tableNoteHtml([s.B.orders.rows[FIRST_MORE]]), '', 'a listing that loaded has no note'); assert.equal(s.B.orders.rows[FIRST_MORE].problems.length, 0);
  assert.match(s.block.tableNoteHtml([s.B.orders.rows[SCORPIO]]), /Etsy did not answer/, 'the ones behind it say the same, each with its own next try');
  await s.block.askTables(); assert.equal(s.seen.length, reqAfter, 'not asked again at once'); assert.equal(s.w.asked.length, callsAfter);
  assert(s.timers.some(t => t.ms >= 600000 - 1 && t.ms <= 600000 + 2000), 'one look is set for the backoff (10 minutes), not a poll: ' + JSON.stringify(s.timers.map(t => t.ms)));
  s.w.fail.clear(); s.setClock(s.clock() + 11 * 60000); await s.block.askTables();
  assert.equal(s.w.asked.filter(x => x === '856808566').length, 2, 'asked again after the backoff'); assert.equal(s.w.asked.filter(x => x === '7100001').length, 1, 'a listing already loaded is never asked twice');
  for (const r of s.B.orders.rows.slice(0, 7)) assert.equal(r.problems.length, 0, r.line.listingId + ' clears after the backoff'); assert.equal(s.block.tableNoteHtml([s.B.orders.rows[CANCER]]), '');
  ok('page: an Etsy failure shows one plain line with the time of the next try, is not retried in the round, and loads after the backoff');

  // (3) NOLIMIT (13 Oct 2026): the shop's 60 look-ups of the day spent is no reason to stop: the round still asks, every row clears, no "used up" line, nothing waits for midnight
  s = await mk({}); s.w.store.set('Charm_Listing_Skus/_budget', { day: today(), n: 60, sb: 5, at: Date.now() });
  await s.block.askTables();
  assert.equal(s.w.asked.length, 8, 'with 60 already spent today all eight waiting listings are still asked'); assert.deepEqual(s.seen.map(x => x.ids.length), [6, 2], 'six listings a request, both requests made');
  for (const r of s.B.orders.rows.slice(0, 7)) { assert.equal(r.problems.length, 0, r.line.listingId + ' reads clean'); assert.equal(s.block.tableNoteHtml([r]), '', 'no note on a row that loaded'); }
  assert.doesNotMatch(s.seen.map(x => x.note.join(' ')).join(' '), /used up|a day|budget/, 'no budget line was ever shown');
  assert.equal(s.block.tableTry.size, 0, 'nothing is waiting for midnight UTC'); assert.equal(s.w.budget().n, 68, 'the day\'s count goes on past 60 (it only counts)');
  ok('page: the shop\'s 60 spent still asks: both requests are made, every row clears, no "used up" line, nothing waits for midnight');

  // (4) NOLIMIT: neither the sandbox's old share of 30 nor both together (60 / 30) stop it; it still asks and shows no "share" line
  for (const [n, sb] of [[30, 30], [60, 30]]) {
    s = await mk({}); s.w.store.set('Charm_Listing_Skus/_budget', { day: today(), n, sb, at: Date.now() });
    await s.block.askTables();
    assert.equal(s.w.asked.length, 8, `the sandbox still asks with ${n} / ${sb} spent`); for (const r of s.B.orders.rows.slice(0, 7)) assert.equal(r.problems.length, 0);
    assert.doesNotMatch(s.seen.map(x => x.note.join(' ')).join(' ') + s.block.tableNoteHtml([s.B.orders.rows[SCORPIO]]), /sandbox&#39;s share|used up/);
  }
  ok('page: the sandbox with 30 / 60 calls already spent still asks (no share of its own, no daily cap)');

  // (5) the cloud does not answer at all
  s = await mk({ netDown: true }); await s.block.askTables();
  assert.equal(s.seen.length, 1, 'one request, then the round stops'); assert.match(s.block.tableNoteHtml([s.B.orders.rows[SCORPIO]]), /Not loaded: the cloud did not answer\. It is asked again at/);
  ok('page: no answer from the cloud: one request, one plain line, nothing retried in a loop');
}

/* ── 3 · the source (what the page and the cloud promise) ── */
function source() {
  const lib = fs.readFileSync(path.join(fnDir, 'charmNestLibrary.js'), 'utf8'), fam = require(path.join(root, 'charm-nest-sandbox-families.js'));
  assert(!/cacheOnly: *!!PREFIX/.test(lib), 'the sandbox is no longer cache-only'); assert(/sandbox: *!!PREFIX/.test(lib));
  assert.deepEqual(fam.ownerOf('firestore', 'Charm_Listing_Skus/1160815385'), { kind: 'protected', key: 'Charm_Listing_Skus' }, 'the wipe never touches the shop\'s listing tables');
  assert.deepEqual(fam.ownerOf('firestore', 'Charm_Listing_Skus/_budget'), { kind: 'protected', key: 'Charm_Listing_Skus' });
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  assert(/\.tblFail\{/.test(html) && /\.cuStat\{/.test(html), 'the spinner is the page\'s own (.cuStat .spin)');
  assert(/charm-nest-bridge\.js\?v=[^"]*-tb1(-[^"]*)?"/.test(html), 'the bridge\'s cache token carries tb1');
  ok('source: the sandbox is not cache-only; the shop\'s listing tables are on the wipe\'s keep list; the page styles and cache token are in');
}

(async () => {
  let failed = 0;
  const run = async (name, fn) => { try { await fn(); } catch (e) { failed++; console.log('  ✗ ' + name + ': ' + (e && e.stack ? e.stack.split('\n').slice(0, 6).join('\n') : e)); } };
  await run('rows', rows); await run('cloud', cloud); await run('page', page); await run('source', source);
  Module._load = realLoad;
  console.log(failed ? `table-load: ${failed} failed` : 'table-load: all passed'); process.exit(failed ? 1 : 0);
})();
