// Sandbox upkeep: a sandbox order stream left playing for days keeps nothing growing (Paul, 24 Sep 2026: everything must
// be able to stay on indefinitely). Against in-memory Firestore and Storage stand-ins and the real function code:
//  · the emulated Etsy lists, and holds, only the streamed orders not shipped yet (finished at the station: shipped a
//    little later, an undo keeps it open; nobody finishing it: shipped by hand two days past its ship date), reads a shipped
//    order alone as shipped, gives the same orders from the same seed, and a cold instance never replays every step;
//  · an engraving reading Claude was paid for in the sandbox answers every copy of the same words, sandbox jobs and their
//    payloads are the sandbox's own, a paid reading is never overwritten, and production asks Claude exactly as before;
//  · the sandbox archive copies no photo into the production design-archive/ prefix;
//  · sandbox arrivals older than 45 simulated days go, production keeps its ledger;
//  · the reset clears every sandbox record (rehearsals, shape guidance, agent jobs, subcollections, messages under
//    parents never written) and every sandbox file but the snapshot and the master files, in calls that stop on the clock
//    and say more:true, keeping the paid readings and everything in production.
//   node tests/charm-nest/sandbox-upkeep.cjs
'use strict';
const path = require('path'), assert = require('assert');
const fnDir = path.join(__dirname, '../../netlify/functions');

/* ── a clock the test moves: every module reads Date.now at call time ── */
const realNow = Date.now; let skew = 0, slow = 0; Date.now = () => realNow() + skew;

/* ── in-memory Firestore ── */
const store = new Map();                       // "coll/id[/sub/id…]" → data
const TS = ms => ({ toMillis: () => ms });
const SERVER_TS = { __ts: true };
const FieldValue = { serverTimestamp: () => SERVER_TS, increment: n => ({ __inc: n }), delete: () => ({ __del: true }) };
const val = x => (x && typeof x.toMillis === 'function' ? x.toMillis() : x);
const apply = (t, src) => { for (const [k, v] of Object.entries(src)) { if (v && v.__inc != null) t[k] = (t[k] || 0) + v.__inc; else if (v && v.__del) delete t[k]; else if (v === SERVER_TS) t[k] = TS(Date.now()); else t[k] = v; } return t; };
let reads = 0;
function docRef(coll, id) {
  const key = coll + '/' + id;
  return { id, path: key, collection: sub => query(key + '/' + sub),
    async get() { reads++; const d = store.get(key); return { exists: !!d, id, ref: docRef(coll, id), data: () => (d ? { ...d } : undefined), get: f => (d ? d[f] : undefined) }; },
    async set(data, o) { const cur = (o && o.merge && store.get(key)) || {}; store.set(key, apply({ ...cur }, data)); },
    async update(data) { const cur = store.get(key); if (!cur) throw new Error('NOT_FOUND ' + key); store.set(key, apply({ ...cur }, data)); },
    async delete() { store.delete(key); } };
}
function query(coll, filters = [], order = null, lim = 0) {
  const q = {
    where: (f, op, v) => query(coll, filters.concat([[f, op, v]]), order, lim),
    orderBy: (f, dir) => query(coll, filters, [f, dir || 'asc'], lim),
    limit: n => query(coll, filters, order, n),
    select: () => q,
    count: () => ({ get: async () => { const s = await q.get(); return { data: () => ({ count: s.size }) }; } }),
    async get() {
      // a collection lists the documents written in it, never a parent that only holds a subcollection (as Firestore)
      let rows = [...store.entries()].filter(([k]) => k.startsWith(coll + '/') && !k.slice(coll.length + 1).includes('/')).map(([k, v]) => { const id = k.slice(coll.length + 1); return { id, exists: true, data: () => ({ ...v }), get: f => v[f], ref: docRef(coll, id) }; });
      for (const [f, op, v] of filters) rows = rows.filter(r => { const x = val(r.data()[f]), y = val(v); return op === '==' ? x === y : op === '>=' ? x >= y : op === '<=' ? x <= y : op === '>' ? x > y : op === '<' ? x < y : op === 'in' ? y.includes(x) : true; });
      if (order) rows.sort((a, b) => { const x = val(a.data()[order[0]]), y = val(b.data()[order[0]]); const c = x > y ? 1 : x < y ? -1 : 0; return order[1] === 'desc' ? -c : c; });
      if (lim) rows = rows.slice(0, lim);
      reads += Math.max(1, rows.length);
      return { size: rows.length, docs: rows, empty: !rows.length, forEach: fn => rows.forEach(fn) };
    },
    doc: id => docRef(coll, id || 'auto' + Math.random().toString(36).slice(2, 10)),
    async add(data) { const r = q.doc(); await r.set(data); return r; }
  };
  return q;
}
const db = {
  collection: c => query(c),
  // a slow store: every commit takes `slow` ms of the test's clock (where a reset of a big sandbox spends its time)
  batch() { const ops = []; return { set: (r, d, o) => ops.push(() => r.set(d, o)), update: (r, d) => ops.push(() => r.update(d)), delete: r => ops.push(() => r.delete()), async commit() { skew += slow; for (const o of ops) await o(); } }; },
  async getAll(...refs) { if (refs.length && typeof refs[refs.length - 1].get !== 'function') refs.pop(); return Promise.all(refs.map(r => r.get())); },
  async runTransaction(fn) { return fn({ get: r => r.get(), set: (r, d, o) => r.set(d, o), update: (r, d) => r.update(d), delete: r => r.delete() }); }
};
/* ── in-memory Storage ── */
const blobs = new Map();
const bucket = {
  name: 'test-bucket',
  file(p) {
    return { name: p,
      async save(buf, o) { blobs.set(p, { buf: Buffer.from(buf), contentType: o && o.contentType }); },
      async exists() { return [blobs.has(p)]; },
      async download() { const b = blobs.get(p); if (!b) throw Object.assign(new Error('no blob ' + p), { code: 404 }); return [b.buf]; },
      async delete(o) { if (!blobs.has(p) && !(o && o.ignoreNotFound)) throw Object.assign(new Error('no blob ' + p), { code: 404 }); blobs.delete(p); },
      async makePublic() {}, publicUrl: () => 'https://storage.example/' + p,
      async getMetadata() { const b = blobs.get(p); if (!b) throw Object.assign(new Error('no blob'), { code: 404 }); return [{ contentType: b.contentType, size: b.buf.length, metadata: {} }]; } };
  },
  // one page of names in order, as the real listing returns it with autoPaginate:false
  async getFiles(q = {}) {
    assert(q.autoPaginate === false && q.maxResults > 0, 'files are listed a page at a time');
    const names = [...blobs.keys()].filter(k => k.startsWith(q.prefix || '')).sort().filter(k => !q.pageToken || k > q.pageToken), page = names.slice(0, q.maxResults);
    return [page.map(n => this.file(n)), names.length > page.length ? Object.assign({}, q, { pageToken: page[page.length - 1] }) : null];
  }
};
const admin = { firestore: Object.assign(() => db, { FieldValue, Timestamp: { fromMillis: TS } }), storage: () => ({ bucket: () => bucket }) };
const kicks = [];
const Module = require('module'), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === 'node-fetch') return async (url, init) => { kicks.push({ url: String(url), body: JSON.parse((init && init.body) || '{}') }); return { ok: true, status: 202, text: async () => '' }; };
  if (req === 'firebase-admin' || req === './firebaseAdmin' || /[\/]firebaseAdmin(\.js)?$/.test(req)) return admin;
  return realLoad.call(this, req, ...rest);
};
const fetched = [];   // the archive's photo mirroring uses the global fetch
global.fetch = async url => { fetched.push(String(url)); return { ok: true, headers: { get: () => 'image/jpeg' }, arrayBuffer: async () => new Uint8Array([255, 216, 255]).buffer }; };
process.env.ANTHROPIC_API_KEY = 'test';
let claudeCalls = 0;
const anthro = require(path.join(fnDir, '_etsyMailAnthropic.js'));
anthro.callClaudeRaw = async o => {
  const n = ++claudeCalls, txt = String(o.messages[0].content[0].text), order = (/^Order (\S*)/.exec(txt) || [])[1] || '';
  const pers = JSON.parse((/Personalisation field: (\[.*\])/.exec(txt) || [0, '[]'])[1]);
  return { stop_reason: 'end_turn', usage: { input_tokens: 10, output_tokens: 5 }, content: [{ type: 'thinking', thinking: `Order ${order}: the customer typed ${pers.join(' / ')}` }, { type: 'text', text: JSON.stringify({ engrave: pers.length > 0, text: pers.join('\n'), source: 'personalization', sourceQuote: pers[0] || '', requests: { side: 'back', font: null, handwriting: false, image: false }, questions: ['call ' + n], confidence: 0.97 }) }] };
};
const docsIn = c => [...store.keys()].filter(k => k.startsWith(c + '/') && !k.slice(c.length + 1).includes('/'));

(async () => {
  /* ═══ the emulated Etsy while the stream plays ═══ */
  const STEP = 600000, DAY = 86400, snapAt = Date.now() - 3600e3, T0 = Date.now();
  const receipt = (rid, leadDays, hoursAgo, extra) => { const created = Math.floor(snapAt / 1000) - hoursAgo * 3600, ship = created + leadDays * DAY; return Object.assign({ receipt_id: rid, order_number: rid, create_timestamp: created, created_timestamp: created, update_timestamp: created, status: 'Paid', is_paid: true, is_shipped: false, transactions: [{ transaction_id: rid * 10 + 1, receipt_id: rid, listing_id: 1718001, sku: 'BR-TST-0' + (rid % 10), title: 'charm', quantity: 1, create_timestamp: created, expected_ship_date: ship }] }, extra || {}); };
  const snapshot = [receipt(3521000101, 1, 5), receipt(3521000102, 2, 3), receipt(3521000103, 3, 1), receipt(3521000104, 2, 2, { is_shipped: true, status: 'Completed' })];
  blobs.set('charmnest/sandbox/orders-cur.json', { buf: Buffer.from(JSON.stringify({ at: snapAt, count: snapshot.length, receipts: snapshot })), contentType: 'application/json' });
  const meta = { path: 'charmnest/sandbox/orders-cur.json', count: snapshot.length, at: snapAt, takenBy: 'test' };
  store.set('Charm_Sandbox/current', meta);
  const simStart = Math.floor(T0 / STEP) * STEP;
  const streamAt = (tick, startedAt = T0) => ({ on: true, seed: 4242, speed: 50, stepMs: STEP, min: 2, max: 5, simStart, simNow: simStart + tick * STEP, tick, snapshotPath: meta.path, startedAt, tickAt: T0 });
  const load = () => { delete require.cache[require.resolve(path.join(fnDir, 'etsySandbox.js'))]; return require(path.join(fnDir, 'etsySandbox.js')); };
  let etsy = load();
  const call = q => etsy.handler({ httpMethod: 'GET', queryStringParameters: q }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body) }));
  const listAll = async () => { const out = []; for (let offset = 0; ; offset += 100) { const r = (await call({ fn: 'listOpenOrders', offset })).body; out.push(...r.results); if (!r.results.length || out.length >= r.count) return out; } };
  const ids = l => l.map(r => String(r.receipt_id));
  const dueOf = r => Math.max(0, ...['expected_ship_date', 'dispatch_date', 'ship_by_date'].map(f => +r[f] || 0), ...(r.transactions || []).map(t => +t.expected_ship_date || 0));
  const arrived = s => Array.from({ length: s.tick }, (_, i) => etsy.batch(s, snapshot, i + 1, meta).length).reduce((a, b) => a + b, 0);
  /** What the stream should list: the look-back's orders, newest first, less those two days past their ship date and those shipped. */
  function expected(s, shipped) {
    const { MAX_BACK, GRACE_S } = etsy.upkeep, simS = (s.simStart + s.tick * s.stepMs) / 1000, out = [];
    for (let k = s.tick; k >= Math.max(1, s.tick - MAX_BACK + 1); k--) out.push(...etsy.batch(s, snapshot, k, meta).filter(r => dueOf(r) + GRACE_S >= simS && !shipped.has(String(r.receipt_id))).reverse());
    return out;
  }

  // three weeks of simulated time played: the list is the orders not shipped, not every order since the first step
  let s = streamAt(3000); store.set('Charm_Sandbox/stream', s);
  let listed = await listAll();
  assert(etsy.upkeep && typeof etsy.upkeep.size === 'function', 'the emulator bounds what it lists and holds (etsySandbox upkeep)');
  let want = expected(s, new Set());
  const total = arrived(s);
  console.log(`step ${s.tick}: ${total} orders arrived, ${listed.length} listed (expected ${want.length}), ${etsy.upkeep.size()} held`);
  assert.deepStrictEqual(ids(listed), ids(want), 'the list is the arrived orders not shipped yet, newest first');
  assert.strictEqual(JSON.stringify(listed), JSON.stringify(want), 'the same seed gives the same orders, built again from their step');
  assert(listed.length < total * 0.6 && listed.length <= etsy.upkeep.MAX_BACK * 5, `the list stays bounded: ${listed.length} of ${total}`);
  assert.strictEqual(etsy.upkeep.size(), listed.length, 'the instance holds only what it lists');
  assert.strictEqual((await call({ fn: 'status' })).body.stream.arrived, listed.length, 'status counts the orders listed now');

  // the station finishes two orders and undoes one of them: a finished order ships ten minutes later, an undone one stays
  const [A, B, C] = ids(listed);
  for (const id of [A, B]) await db.collection('Sandbox_Design_Completed Orders').doc(id).set({ orderId: id, completed: true, completedAt: FieldValue.serverTimestamp() }, { merge: true });
  skew += etsy.upkeep.ASK_MS + 1000;
  listed = await listAll();
  assert(ids(listed).includes(A) && ids(listed).includes(B), 'a finished order stays listed until it ships');
  store.delete('Sandbox_Design_Completed Orders/' + B);   // undone at the station
  skew += etsy.upkeep.SHIP_MS;
  listed = await listAll();
  assert(!ids(listed).includes(A) && ids(listed).includes(B) && ids(listed).includes(C), 'finished ten minutes ago: shipped; undone at the station: still open');
  assert.deepStrictEqual(ids(listed), ids(expected(s, new Set([A]))), 'the list is the model less the shipped order');
  assert.strictEqual(etsy.upkeep.size(), listed.length, 'the shipped order is not held any more');
  let one = await call({ fn: 'etsyOrderProxy', orderId: A });
  assert(one.status === 200 && one.body.receipt.is_shipped === true && one.body.transactions.length === 1, 'a shipped order read alone reads as shipped, as Etsy reads it');
  one = await call({ fn: 'etsyOrderProxy', orderId: B });
  assert(one.status === 200 && !one.body.receipt.is_shipped, 'an order still listed reads as open');
  const early = etsy.batch(s, snapshot, 5, meta)[0];
  one = await call({ fn: 'etsyOrderProxy', orderId: String(early.receipt_id) });
  assert(one.status === 200 && one.body.receipt.is_shipped === true && JSON.stringify(one.body.transactions) === JSON.stringify(early.transactions), 'an order from the first hour, long shipped, is built again from its number alone');
  const later = etsy.batch(s, snapshot, s.tick + 1, meta)[0];
  assert.strictEqual((await call({ fn: 'etsyOrderProxy', orderId: String(later.receipt_id) })).status, 404, 'an order of a step not come yet does not exist');

  // a cold instance lists the same orders: it plans the steps it looks back at and reads which orders are finished
  etsy = load(); reads = 0;
  const cold = await listAll();
  assert.deepStrictEqual(ids(cold), ids(listed), 'a cold instance lists what a warm one does');
  console.log(`cold instance: ${reads} document reads for ${cold.length} listed orders`);
  // a stream played for months: a cold instance never builds every step from the first
  s = streamAt(400000); store.set('Charm_Sandbox/stream', s); etsy = load();
  let t = realNow(); const far = (await call({ fn: 'listOpenOrders' })).body; t = realNow() - t;
  console.log(`step ${s.tick} from cold: ${far.count} listed in ${t} ms`);
  assert(t < 5000 && far.count === expected(s, new Set()).length && far.count <= etsy.upkeep.MAX_BACK * 5, `a cold instance at step 400000 answers in ${t} ms with the look-back only`);
  // a reset starts a new stream (step 0 again, a new start): the first step's orders, as from the first
  s = streamAt(1, T0 + 1); store.set('Charm_Sandbox/stream', s);
  assert.deepStrictEqual(ids(await listAll()), ids(etsy.batch(s, snapshot, 1, meta)).reverse(), 'a new stream lists its own first step');
  assert.strictEqual(etsy.upkeep.size(), etsy.batch(s, snapshot, 1, meta).length, 'and holds nothing of the old one');

  /* ═══ Claude's engraving readings in the sandbox ═══ */
  const lib = require(path.join(fnDir, 'charmNestLibrary.js')), engrave = require(path.join(fnDir, 'charmEngrave-background.js'));
  const post = b => lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b) }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body) }));
  const words = (order, pers = ['ANNA']) => ({ order, sku: 'BR-TST-01', title: 'Heart charm', form: 'charm', quantity: 1, engravable: true, personalization: pers, buyerMessage: '', staffNote: '', messages: [] });
  const runKick = k => engrave.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(k.body) });
  kicks.length = 0;
  const r1 = await post({ op: 'startAgent', mode: 'engraveIntent', payload: words('3521000101'), sandbox: true });
  const id1 = r1.body.id;
  assert(r1.status === 200 && /^agent-/.test(id1), 'a sandbox reading starts a job');
  assert(store.has('Sandbox_Charm_Nest_Agent/' + id1) && !store.has('Charm_Nest_Agent/' + id1) && store.get('Sandbox_Charm_Nest_Agent/' + id1).sandbox === true, "a sandbox job is the sandbox's own record, marked sandbox");
  assert(blobs.has(`charmnest/sandbox/agent/${id1}.json`) && !blobs.has(`charmnest/agent/${id1}.json`), "its payload is parked under the sandbox's files");
  assert.deepStrictEqual(kicks.map(k => k.body), [{ id: id1, mode: 'engraveIntent', sandbox: true }], 'the kick says it is a sandbox job');
  await runKick(kicks[0]);
  let g = await post({ op: 'getAgent', id: id1, sandbox: true });
  assert(g.body.job && g.body.job.status === 'done' && g.body.job.result.text === 'ANNA' && claudeCalls === 1, 'the job is read back from the sandbox collection');
  assert.strictEqual(docsIn('Sandbox_Charm_Nest_Agent_Cache').length, 1, 'the paid reading is kept for the same words');
  // another copy of the order, same words: answered from the cache — no job, no payload, no kick, no Claude call
  kicks.length = 0; const jobsBefore = docsIn('Sandbox_Charm_Nest_Agent').length, blobsBefore = blobs.size;
  const r2 = await post({ op: 'startAgent', mode: 'engraveIntent', payload: words('3521000999'), sandbox: true });
  assert(r2.status === 200 && r2.body.cached && kicks.length === 0 && docsIn('Sandbox_Charm_Nest_Agent').length === jobsBefore && blobs.size === blobsBefore && claudeCalls === 1, 'the same words are not paid for twice: ' + JSON.stringify(r2.body));
  g = await post({ op: 'getAgent', id: r2.body.id, sandbox: true });
  assert(g.body.job.status === 'done' && g.body.job.result.text === 'ANNA' && /3521000999/.test(g.body.job.result.reasoning) && !/3521000101/.test(JSON.stringify(g.body.job.result)), 'the cached reading is told for this order: ' + JSON.stringify(g.body.job));
  // other words are read by Claude; two copies read at once both pay, and the first reading saved is never overwritten
  kicks.length = 0;
  const ra = await post({ op: 'startAgent', mode: 'engraveIntent', payload: words('3521000201', ['CLARA']), sandbox: true }), rb = await post({ op: 'startAgent', mode: 'engraveIntent', payload: words('3521000202', ['CLARA']), sandbox: true });
  assert(!ra.body.cached && !rb.body.cached && kicks.length === 2, 'new words go to Claude');
  await runKick(kicks[0]); const firstSaved = JSON.stringify(docsIn('Sandbox_Charm_Nest_Agent_Cache').map(k => store.get(k)));
  await runKick(kicks[1]);
  assert.strictEqual(JSON.stringify(docsIn('Sandbox_Charm_Nest_Agent_Cache').map(k => store.get(k))), firstSaved, 'a reading already paid for is never replaced');
  assert.strictEqual(store.get('Sandbox_Charm_Nest_Agent/' + rb.body.id).result.questions[0], 'call ' + claudeCalls, 'the second job keeps its own answer');
  // production: a job in Charm_Nest_Agent, its payload under charmnest/agent/, the same kick as before, never the cache
  kicks.length = 0; const cacheBefore = docsIn('Sandbox_Charm_Nest_Agent_Cache').length;
  const rp = await post({ op: 'startAgent', mode: 'engraveIntent', payload: words('3521000101') });
  assert(/^agent-/.test(rp.body.id) && !rp.body.cached && store.has('Charm_Nest_Agent/' + rp.body.id) && blobs.has(`charmnest/agent/${rp.body.id}.json`), 'production asks Claude every time, as before');
  assert.deepStrictEqual(kicks.map(k => k.body), [{ id: rp.body.id, mode: 'engraveIntent' }], 'with the same kick as before');
  const pj = store.get('Charm_Nest_Agent/' + rp.body.id); assert(!('sandbox' in pj) && !('cacheKey' in pj) && !('order' in pj), 'and the same job record');
  await runKick(kicks[0]);
  assert(store.get('Charm_Nest_Agent/' + rp.body.id).status === 'done' && docsIn('Sandbox_Charm_Nest_Agent_Cache').length === cacheBefore, 'a production reading is not cached');
  store.set('Charm_Nest_Agent/agent-legacy1', { id: 'agent-legacy1', mode: 'engraveIntent', status: 'done', result: { text: 'X' } });
  g = await post({ op: 'getAgent', id: 'agent-legacy1', sandbox: true });
  assert(g.body.job && g.body.job.result.text === 'X', 'a sandbox job started before sandbox jobs had their own collection still reads back');

  /* ═══ the archive's photos ═══ */
  const archive = require(path.join(fnDir, 'designArchive.js'));
  const put = (sandbox, rid) => archive.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: sandbox ? { sandbox: '1' } : {}, body: JSON.stringify({ op: 'put', orders: [{ receiptId: rid, items: [{ transactionId: '1', imageUrl: 'https://i.etsystatic.com/il/x.jpg' }] }] }) }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body) }));
  let a = await put(true, '3521000555');
  assert(a.status === 200 && !fetched.length && ![...blobs.keys()].some(k => k.startsWith('design-archive/')), 'a sandbox order copies no photo into the production design-archive/ prefix');
  const rec = store.get('Sandbox_Design_Order_Archive/3521000555');
  assert(rec && rec.items[0].imageUrl && !rec.items[0].mirrorUrl, "the sandbox record keeps the listing's own image URL");
  a = await put(false, '3521000556');
  assert(a.status === 200 && fetched.length === 1 && blobs.has('design-archive/3521000556/1.jpg') && store.get('Design_Order_Archive/3521000556').items[0].mirrorUrl, 'production still mirrors its photos');

  /* ═══ arrivals ═══ */
  const simNow = Date.now() + 90 * 86400e3;
  store.set('Sandbox_Charm_Nest_Arrivals/900100', { id: '900100', firstSeenAt: simNow - 50 * 86400e3, createTs: 1 });
  store.set('Sandbox_Charm_Nest_Arrivals/900101', { id: '900101', firstSeenAt: simNow - 10 * 86400e3, createTs: 1 });
  store.set('Charm_Nest_Arrivals/900102', { id: '900102', firstSeenAt: Date.now() - 50 * 86400e3, createTs: 1 });
  await post({ op: 'arrivalRecord', orders: [{ id: '900103', createTs: 1 }], now: simNow, sandbox: true });
  assert(!store.has('Sandbox_Charm_Nest_Arrivals/900100') && store.has('Sandbox_Charm_Nest_Arrivals/900101') && store.has('Sandbox_Charm_Nest_Arrivals/900103'), 'sandbox arrivals older than 45 simulated days go');
  await post({ op: 'arrivalRecord', orders: [{ id: '900104', createTs: 1 }] });
  assert(store.has('Charm_Nest_Arrivals/900102'), 'production keeps its arrivals ledger');

  /* ═══ the reset ═══ */
  const seed = (k, v = { x: 1 }) => store.set(k, v);
  ['Sandbox_Charm_Pool/p1', 'Sandbox_Charm_Nest_Sets/s1', 'Sandbox_Design_Bridge/b1', 'Sandbox_Design_Bridge/b1/log/l1', 'Sandbox_Design_Bridge/b1/log/l2', 'Sandbox_Charm_Nest_Rose_Stock/r1', 'Sandbox_Charm_Nest_Rose_Stock/r1/cuts/c1',
    'Sandbox_Charm_Nest_Rose_Rehearsals/rgdemo-1', 'Sandbox_Charm_Nest_Shape_Guidance/g1', 'Sandbox_Design_Completed Orders/900001', 'Sandbox_Brites_Orders/900001/messages/designed-set-a',
    'Sandbox_Brites_Orders/900002', 'Sandbox_Brites_Orders/900002/messages/m1', 'Sandbox_Charm_Nest_Arrivals/900003', 'Sandbox_Brites_Orders/900003/messages/m2', 'Sandbox_Brites_Orders/900009/messages/m3'].forEach(k => seed(k));
  for (let i = 0; i < 3000; i++) seed('Sandbox_Charm_Nest_Run_Lines/run~' + String(i).padStart(5, '0'));
  const PROD = ['Charm_Pool/p1', 'Brites_Orders/900001/messages/x', 'Charm_Nest_Shape_Guidance/g1', 'Design_Completed Orders/900001', 'Charm_Nest_Run_Lines/run~1'];
  PROD.forEach(k => seed(k));
  const keepBlobs = ['charmnest/sandbox/orders-cur.json', 'charmnest/sandbox/master/BRITES-master.ai', 'charmnest/sheets/2026-09-24/prod.ai', 'charmnest/agent/agent-y.json', 'design-archive/3521000556/1.jpg'];
  const dropBlobs = ['charmnest/sandbox/orders-2026-09-01T00-00-00-000Z.json', 'charmnest/sandbox/sheets/2026-10-02/Sheet-1/a.ai', 'charmnest/sandbox/sets/2026-10-02/Set-1/set.json', 'charmnest/sandbox/agent/agent-x.json', 'charmnest/sandbox/sources/x.pdf'];
  for (const k of keepBlobs.concat(dropBlobs)) if (!blobs.has(k)) blobs.set(k, { buf: Buffer.from('x') });
  for (let i = 0; i < 1200; i++) blobs.set(`charmnest/sandbox/charms/c${String(i).padStart(4, '0')}.png`, { buf: Buffer.from('x') });
  slow = 1000;   // each commit a second: a reset this size cannot finish in one call
  const first = await post({ op: 'sandboxReset', sandbox: true });
  assert(first.status === 200 && first.body.more === true, 'a reset that runs out of time says so, for the page to call again: ' + JSON.stringify(first.body));
  let calls = 1, removed = first.body.deleted, files = first.body.files || 0, last = first.body;
  while (last.more && calls < 60) { last = (await post({ op: 'sandboxReset', sandbox: true })).body; calls++; removed += last.deleted; files += last.files || 0; }
  slow = 0;
  console.log(`reset: ${calls} calls, ${removed} records and ${files} files removed`);
  assert(!last.more && !last.filesError, 'the calls finish the reset');
  const left = [...store.keys()].filter(k => k.startsWith('Sandbox_') && !k.startsWith('Sandbox_Charm_Nest_Agent_Cache/'));
  assert.deepStrictEqual(left.filter(k => !k.startsWith('Sandbox_Brites_Orders/900009/')), [], 'every sandbox record went, subcollections and messages under unwritten parents included');
  assert(docsIn('Sandbox_Charm_Nest_Agent_Cache').length >= 2, 'the readings Claude was paid for stay');
  assert(!store.has('Charm_Sandbox/stream') && store.has('Charm_Sandbox/current'), 'the stream starts over; the snapshot stays');
  for (const k of PROD) assert(store.has(k), 'production untouched: ' + k);
  for (const k of keepBlobs) assert(blobs.has(k), 'kept: ' + k);
  const stray = [...blobs.keys()].filter(k => k.startsWith('charmnest/sandbox/') && !keepBlobs.includes(k));
  assert.deepStrictEqual(stray, [], 'every other sandbox file went');
  console.log('left behind by design: a message under an order no other record names:', left.join(', ') || 'none');
  console.log('sandbox upkeep OK');
})().catch(e => { console.error(e); process.exit(1); });
