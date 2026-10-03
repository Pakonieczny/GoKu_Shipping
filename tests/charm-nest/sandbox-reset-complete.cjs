// Reset the sandbox / Purge all run history leave the sandbox with NO old record of any kind (Paul, 3 Oct 2026: "there's
// nothing that seems to purge the system of all old records… the same records just keep on coming back").
// The sandbox plays the real order numbers of the 17 Sep snapshot, so every key below exists on BOTH sides: the same
// custom order, cancel record, pool row, timeline event, sheet card is written once by a sandbox page (Sandbox_…) and
// once by a production page. Against in-memory Firestore and Storage stand-ins and the real charmNestLibrary,
// firebaseOrders and designArchive code, the records are written through the real ops wherever one exists:
//   · every family the sandbox can write is seeded: Review's custom orders (QR label printed, Complete Order, reopened:
//     "kept for good" in production) and custom sheets (Sent to Sheet), pool rows, sets and counters, runs with their
//     line archive and live parts, back records, sheets, releases, the bridge log, arrivals, cancel records and their
//     history, the order timeline, a person's decision on a line (decidedSandbox beside production's decided), Rose
//     Gold stock and its cuts, rehearsals, engraving jobs, shape guidance, the stations' locks, finished orders,
//     messages, staff notes, sign-in sessions, activity with its daily rollups, the design archive, the sorter's
//     customer messages (EtsyMail_OrderLinks, where a sandbox one is flagged olsb_ / sandbox:true), and files;
//   · the reset runs in calls that stop on the clock (more:true) until it is done, and leaves not one Sandbox_ record
//     (but the engraving readings Claude was paid for), no sandbox engagement, no sandbox file but the snapshot and the
//     master files, and no stream; the snapshot stays;
//   · production, key for key the same, is byte-identical afterwards (the shared line readings keep production's own
//     decision; only the sandbox's decision beside it goes): its seals, custom orders, cancel records, everything;
//   · Purge all run history (a wrong passcode refused, an open run named first) clears the sandbox completely too, and
//     keeps its own production behaviour: production's runs, sheets, sets, pool, backs and counters go, its seals, custom
//     orders and cancel records stay.
//   node tests/charm-nest/sandbox-reset-complete.cjs
'use strict';
const path = require('path'), assert = require('assert');
const fnDir = path.join(__dirname, '../../netlify/functions');

/* ── a clock the test moves: every module reads Date.now at call time ── */
const realNow = Date.now; let skew = 0, slow = 0; Date.now = () => realNow() + skew;

/* ── in-memory Firestore: nested merges, dotted paths, in / range / array queries, field masks, cursors, 500-write batches ── */
class TS { constructor(ms) { this.ms = ms; } toMillis() { return this.ms; } toDate() { return new Date(this.ms); } get seconds() { return Math.floor(this.ms / 1000); } get nanoseconds() { return (this.ms % 1000) * 1e6; } }
const store = new Map();   // "coll/id[/sub/id…]" → data
const SERVER_TS = { __sts: true }, DEL = { __del: true };
const FieldValue = { serverTimestamp: () => SERVER_TS, delete: () => DEL, increment: n => ({ __inc: n }) };
const plain = v => !!v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof TS) && !(v instanceof Date) && v !== SERVER_TS && v !== DEL && v.__inc === undefined;
const clone = v => Array.isArray(v) ? v.map(clone) : v instanceof Date ? new Date(v.getTime()) : v === SERVER_TS ? new TS(Date.now()) : (v && v.__inc !== undefined) ? v.__inc : plain(v) ? Object.fromEntries(Object.entries(v).filter(([, x]) => x !== DEL).map(([k, x]) => [k, clone(x)])) : v;
function merge(into, patch, deep) {
  for (const [k, v] of Object.entries(patch)) {
    if (v === DEL) delete into[k];
    else if (v && v.__inc !== undefined) into[k] = (typeof into[k] === 'number' ? into[k] : 0) + v.__inc;
    else if (deep && plain(v) && plain(into[k])) into[k] = merge(clone(into[k]), v, true);
    else into[k] = clone(v);
  }
  return into;
}
const getPath = (d, f) => String(f).split('.').reduce((x, k) => (x == null ? undefined : x[k]), d);
const val = x => (x instanceof TS ? x.ms : x instanceof Date ? x.getTime() : x);
const canon = v => JSON.stringify(v, function (k, x) { const raw = this[k]; if (raw instanceof TS) return { __ts: raw.ms }; if (raw instanceof Date) return { __date: raw.getTime() }; return x && typeof x === 'object' && !Array.isArray(x) ? Object.fromEntries(Object.keys(x).sort().map(key => [key, x[key]])) : x; });
const snapOf = (key, d, mask) => {
  const id = key.slice(key.lastIndexOf('/') + 1), data = d ? (mask ? Object.fromEntries(mask.filter(f => getPath(d, f) !== undefined).map(f => [f, clone(getPath(d, f))])) : clone(d)) : undefined;
  return { id, exists: !!d, ref: docRef(key), data: () => (data ? clone(data) : undefined), get: f => (d ? getPath(d, f) : undefined) };
};
function docRef(key) {
  return { id: key.slice(key.lastIndexOf('/') + 1), path: key, collection: sub => query(key + '/' + sub),
    async get() { return snapOf(key, store.get(key)); },
    async set(data, o) { store.set(key, merge(o && o.merge ? clone(store.get(key) || {}) : {}, data, !!(o && o.merge))); },
    async create(data) { if (store.has(key)) throw new Error('ALREADY_EXISTS ' + key); store.set(key, merge({}, data, false)); },
    async update(data) { const cur = store.get(key); if (!cur) throw new Error('NOT_FOUND ' + key); store.set(key, merge(clone(cur), data, false)); },
    async delete() { store.delete(key); } };
}
function query(coll, filters = [], orders = [], lim = 0, mask = null, after = null) {
  const q = {
    where: (f, op, v) => query(coll, filters.concat([[f, op, v]]), orders, lim, mask, after),
    orderBy: (f, dir) => query(coll, filters, orders.concat([[f, dir || 'asc']]), lim, mask, after),
    limit: n => query(coll, filters, orders, n, mask, after),
    select: (...f) => query(coll, filters, orders, lim, f, after),
    startAfter: (...v) => query(coll, filters, orders, lim, mask, v),
    count: () => ({ get: async () => { const s = await query(coll, filters, orders, 0, null, after).get(); return { data: () => ({ count: s.size }) }; } }),
    async get() {
      // a collection lists the documents written in it, never a parent that only holds a subcollection (as Firestore)
      let rows = [...store.entries()].filter(([k]) => k.startsWith(coll + '/') && !k.slice(coll.length + 1).includes('/')).map(([k, d]) => ({ k, d, id: k.slice(coll.length + 1) }));
      const field = (r, f) => (f === '__name__' ? r.id : val(getPath(r.d, f)));
      for (const [f, op, v] of filters) rows = rows.filter(r => {
        const x = field(r, f), y = Array.isArray(v) ? v.map(val) : val(v), raw = f === '__name__' ? undefined : getPath(r.d, f);
        return op === '==' ? x === y : op === '>=' ? x >= y : op === '<=' ? x <= y : op === '>' ? x > y : op === '<' ? x < y : op === 'in' ? y.includes(x) : op === 'array-contains' ? (raw || []).includes(v) : op === 'array-contains-any' ? (raw || []).some(z => v.includes(z)) : false;
      });
      const ord = orders.length ? orders : [['__name__', 'asc']];
      rows.sort((a, b) => { for (const [f, dir] of ord) { const x = field(a, f), y = field(b, f), c = x > y ? 1 : x < y ? -1 : 0; if (c) return dir === 'desc' ? -c : c; } return 0; });
      if (after) rows = rows.filter(r => { for (const [i, [f, dir]] of ord.entries()) { const x = field(r, f), y = val(after[i]); if (x === y) continue; return dir === 'desc' ? x < y : x > y; } return false; });
      if (lim) rows = rows.slice(0, lim);
      const docs = rows.map(r => snapOf(r.k, r.d, mask));
      return { size: docs.length, docs, empty: !docs.length, forEach: fn => docs.forEach(fn) };
    },
    doc: id => docRef(coll + '/' + (id || 'auto' + Math.random().toString(36).slice(2, 12))),
    // as Firestore: every document that exists, and every one that only holds a subcollection (never written)
    async listDocuments() { return [...new Set([...store.keys()].filter(k => k.startsWith(coll + '/')).map(k => k.slice(coll.length + 1).split('/')[0]))].sort().map(id => docRef(coll + '/' + id)); },
    async add(data) { const r = q.doc(); await r.set(data); return r; }
  };
  return q;
}
const db = {
  collection: c => query(c),
  // every commit takes `slow` ms of the test's clock (where a reset of a big sandbox spends its time)
  batch() { const ops = []; return { set: (r, d, o) => ops.push(() => r.set(d, o)), update: (r, d) => ops.push(() => r.update(d)), delete: r => ops.push(() => r.delete()), create: (r, d) => ops.push(() => r.create(d)),
    async commit() { assert(ops.length <= 500, 'a write batch holds at most 500 writes (' + ops.length + ')'); skew += slow; for (const o of ops) await o(); } }; },
  async getAll(...refs) { let mask = null; if (refs.length && typeof refs[refs.length - 1].get !== 'function') mask = refs.pop().fieldMask || null; return Promise.all(refs.map(async r => snapOf(r.path, store.get(r.path), mask))); },
  async runTransaction(fn) {
    const ops = [], tx = { get: r => r.get(), getAll: (...refs) => db.getAll(...refs), set: (r, d, o) => ops.push(() => r.set(d, o)), update: (r, d) => ops.push(() => r.update(d)), delete: r => ops.push(() => r.delete()), create: (r, d) => ops.push(() => r.create(d)) };
    const out = await fn(tx); for (const o of ops) await o(); return out;
  }
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
  async getFiles(q = {}) {
    assert(q.autoPaginate === false && q.maxResults > 0, 'files are listed a page at a time');
    const names = [...blobs.keys()].filter(k => k.startsWith(q.prefix || '')).sort().filter(k => !q.pageToken || k > q.pageToken), page = names.slice(0, q.maxResults);
    return [page.map(n => this.file(n)), names.length > page.length ? Object.assign({}, q, { pageToken: page[page.length - 1] }) : null];
  }
};
const admin = { firestore: Object.assign(() => db, { FieldValue, Timestamp: Object.assign(TS, { fromMillis: ms => new TS(ms) }), FieldPath: { documentId: () => '__name__' } }), storage: () => ({ bucket: () => bucket }) };
const Module = require('module'), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === 'node-fetch') return async () => { throw new Error('no network in this test'); };
  if (req === 'firebase-admin' || req === './firebaseAdmin' || /[\/]firebaseAdmin(\.js)?$/.test(req)) return admin;
  return realLoad.call(this, req, ...rest);
};
global.fetch = async () => ({ ok: true, headers: { get: () => 'image/jpeg' }, arrayBuffer: async () => new Uint8Array([255, 216, 255]).buffer });
delete process.env.EDIT_PASSCODE;
const lib = require(path.join(fnDir, 'charmNestLibrary.js')), stations = require(path.join(fnDir, 'firebaseOrders.js')), archive = require(path.join(fnDir, 'designArchive.js'));

const post = b => lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b) }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body) }));
const must = async b => { const r = await post(b); assert.strictEqual(r.status, 200, JSON.stringify(b).slice(0, 120) + ' → ' + JSON.stringify(r.body).slice(0, 300)); return r.body; };
const station = (sb, body) => stations.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: sb ? { sandbox: '1' } : {}, body: JSON.stringify(body) }).then(r => { assert.strictEqual(r.statusCode, 200, JSON.stringify(body).slice(0, 100) + ' → ' + r.body.slice(0, 300)); return JSON.parse(r.body); });
const putArchive = (sb, rid) => archive.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: sb ? { sandbox: '1' } : {}, body: JSON.stringify({ op: 'put', orders: [{ receiptId: rid, items: [{ transactionId: '1', imageUrl: 'https://i.etsystatic.com/il/x.jpg' }] }] }) }).then(r => { assert.strictEqual(r.statusCode, 200, r.body); return JSON.parse(r.body); });

/* ── the orders the sandbox plays under their real numbers (the 17 Sep snapshot) ── */
const A = '4173162973', B = '4170408845', C = '4170000555';   // A: a chain-only custom order (QR label printed), B: a custom design sent to a sheet, C: cancelled
const DAY = '2026-09-29', RUN = 'run-20260929-1', SET = `set-${DAY}-1`, SHEET = 'sheet-20260929-gf-1', SESSION = 'sorter-session-1', COMPUTER = 'computer-aaa111';
const NOW = realNow();

/** Everything one side writes: sb true is a sandbox page (every call tagged sandbox), false a production page. The same keys, both sides. */
async function seed(sb) {
  const P = sb ? 'Sandbox_' : '', L = b => must(Object.assign({}, b, sb ? { sandbox: true } : {})), put = (k, v) => store.set(P + k, v);
  // Review → Custom Orders: the QR label printed (twice), Complete Order without a label, then reopened (its seals stay)
  const label = { w: 40, h: 20, qr: 'x'.repeat(40), lines: ['4173162973', 'CHAIN_8941'] };
  await L({ op: 'customPut', key: `${A}_10010`, by: 'paul', label, receiptId: A, transactionId: '10010', sku: 'CHAIN_8941', title: 'CHAIN REPLACEMENT', category: 'Chain only', kind: 'chainOnly' });
  await L({ op: 'customPut', key: `${A}_10010`, by: 'paul', label, receiptId: A });
  await L({ op: 'customPut', key: `${B}_20020`, by: 'ann', how: 'button', receiptId: B, transactionId: '20020', sku: 'CUSTOM-N-001', title: 'Custom necklace', kind: 'custom' });
  await L({ op: 'customReopen', key: `${B}_20020`, by: 'ann', how: 'reopen' });
  // Send to Sheet: a custom design of order B sent to the sheets (the "ON THE SHEETS" strip, the SENT TO SHEET stamp)
  const at = NOW - 3 * 86400000, ck = `custom:${B}:CUSTOM-N-001`;
  await L({ op: 'customSheetPut', record: { ck, rid: B, at: at - 1000, phase: 'sent',
    files: [{ id: 'file-custom-1', name: 'Customer design.ai', kind: 'ai', size: 2400, hash: 'abcde01234567890123456789', cloud: { path: `charmnest/custom/${B}/original.pdf`, url: 'https://saved.example/original.pdf' }, metal: 'silver', qty: 1, pieces: 1, wMm: 14, hMm: 18, maxPt: 52, minPt: 40, maxAreaPt2: 2000, state: 'ready' }],
    sent: { id: `custom-sheet:${ck}:${at}`, at, by: 'paul', lines: { [`${B}_20020`]: [{ f: 'file-custom-1', i: 0 }] } } } });
  // a person's decision on the line: production's own (decided) and the sandbox's (decidedSandbox) sit on the SAME shared document
  await L({ op: 'customDecide', key: `${A}_10010`, kind: 'chainOnly', by: 'paul' });
  // the pool, the sets and their counters, the runs and what they keep apart, the releases, the bridge, the arrivals
  await L({ op: 'poolPut', pools: [`${A}_10010_1`, `${B}_20020_1`, `${C}_30030_1`].map((poolId, i) => ({ poolId, orderId: poolId.split('_')[0], transactionId: poolId.split('_')[1], lineKey: poolId.split('_').slice(0, 2).join('_'), runId: RUN, state: 'pooled', material: ['gold', 'silver', 'gold'][i], copy: 1, quantity: 1, custom: i === 1 })) });
  await L({ op: 'setAllocate', day: DAY, runId: RUN });
  await L({ op: 'runPut', run: { runId: RUN, status: 'complete', step: 'complete', day: DAY, lines: { [`${A}_10010`]: { orderId: A, key: `${A}_10010` } } } });
  await L({ op: 'runArchive', runId: RUN, parts: [{ json: JSON.stringify({ [`${C}_30030`]: { orderId: C, key: `${C}_30030` } }) }] });
  await L({ op: 'releasePut', released: { gf: DAY }, lastReleased: { gf: DAY } });
  await L({ op: 'bridgeLog', session: SESSION, meta: { by: 'paul' }, rows: [{ t: Date.now(), dir: 'cmd', type: 'ping' }, { t: Date.now(), dir: 'evt', type: 'pong' }] });
  await L({ op: 'arrivalRecord', orders: [{ id: A, createTs: 1 }, { id: C, createTs: 1 }] });
  // cancel records ("kept for good") and the history a restore keeps; the order's timeline, written by each op that stamps it
  await L({ op: 'cancelPut', orderId: C, by: 'Etsy', why: 'buyer cancelled', record: { buyer: 'Buyer C', lines: [{ transactionId: '30030', title: 'Charm' }] } });
  await L({ op: 'cancelPut', orderId: A, by: 'paul', why: 'duplicate' });
  await L({ op: 'cancelRestore', orderId: A, by: 'paul' });
  await L({ op: 'timelineAdd', events: [{ orderId: A, type: 'qrLabel', id: 'q1', text: 'QR label for GF Sheet 1', by: 'paul' }, { orderId: B, type: 'designSent', id: 'ds1', text: 'sent', by: 'paul' }, { orderId: C, type: 'note', id: 'n1', text: 'note', by: 'paul' }] });
  // what no op of the page writes in one call: seeded as the ops store them (a sheet, a back record, Rose Gold stock and its cut, and the rest)
  put(`Charm_Nest_Sheets/${SHEET}`, { id: SHEET, metal: 'gold', runId: RUN, setId: SET, orders: [A, B], poolIds: [`${A}_10010_1`], status: 'complete', updatedAt: new TS(NOW), createdAt: new TS(NOW) });
  put(`Charm_Pool_Back/${A}_10010_1`, { poolId: `${A}_10010_1`, sheetId: SHEET, setId: SET, approvedAt: NOW, approvedBy: 'paul', text: 'ANNA', updatedAt: new TS(NOW) });
  put('Charm_Nest_Rose_Stock/stock-1', { stockId: 'stock-1', name: 'RG 14/20', updatedAt: new TS(NOW) });
  put(`Charm_Nest_Rose_Stock/stock-1/cuts/${SHEET}`, { sheetId: SHEET, revision: 1, at: NOW, by: 'paul' });
  put('Charm_Nest_Shape_Guidance/g1', { key: '[]', version: 1, profile: { x: 1 } });
  put('Charm_Nest_Agent/agent-seed-1', { id: 'agent-seed-1', mode: 'engraveIntent', status: 'done', result: { text: 'ANNA' } });
  if (sb) store.set('Sandbox_Charm_Nest_Rose_Rehearsals/rgdemo-1', { demoId: 'rgdemo-1', at: NOW });   // (the rehearsal exists only in the sandbox)
  // the stations (firebaseOrders): locks and claims, finished orders, a message and a note, a sign-in, what was done, a scan
  const F = body => station(sb, body);
  await F({ rtLockIds: [A, B], clientId: 'client-1', page: 'design' });
  await F({ rtClaimIds: [B], claimedBy: 'sorter', claimRun: RUN });
  await F({ completedIds: [A] });
  await F({ newMessage: 'hello team', orderNumber: A, employeeName: 'Paul' });
  await F({ newMessage: 'DESIGNED :)', orderNumber: B, employeeName: 'Paul', designSetId: 'set-a' });
  await F({ orderNumber: B, staffNote: 'engrave the back' });
  await F({ session: { id: 'session-aaaa-0001', event: 'start', station: 'sorting', computerId: COMPUTER, person: 'Paul', computerLabel: 'Bench 1' } });
  await F({ activity: [{ id: 'activity-evt-0001', station: 'sorting', action: 'scan', person: 'Paul', orderId: A, parts: 2, at: NOW - 60000, computer: COMPUTER, session: 'session-aaaa-0001' }, { id: 'activity-evt-0002', station: 'sorting', action: 'complete', person: 'Paul', orderId: A, parts: 2, orders: 1, at: NOW - 30000, computer: COMPUTER }] });
  await F({ timeline: [{ orderId: A, type: 'scan', id: 'scan-1', station: 'sorting', by: 'Paul', text: 'scanned' }] });
  await putArchive(sb, A);
  // files
  for (const f of ['sheets/2026-09-29/GF_Sheet-1.ai', 'sets/2026-09-29/Set-1/set.json', 'agent/agent-seed-1.json', 'charms/c0001.png']) blobs.set(`charmnest/${sb ? 'sandbox/' : ''}${f}`, { buf: Buffer.from('x') });
  if (sb) blobs.set('design-archive/sandbox/listing/0123456789abcdef0123456789abcdef01234567.jpg', { buf: Buffer.from('x') });
  // the sorter's customer messages (EtsyMail_OrderLinks is the inbox's collection: a sandbox one is flagged, id olsb_)
  const eid = `${sb ? 'olsb_' : 'ol_'}${A}_o_k1abc`;
  store.set(`EtsyMail_OrderLinks/${eid}`, { id: eid, receiptId: A, scope: 'order', sandbox: sb, status: 'open', outbox: [{ id: 'x1', text: 'which chain length?', status: sb ? 'sent' : 'queued' }], sim: [], createdAtMs: NOW });
}
/** Records a long-running sandbox holds in bulk: pages and pages of them (a reset deletes 300 at a time and answers on the clock). */
function bulk(sb, n) {
  const P = sb ? 'Sandbox_' : '';
  for (let i = 0; i < n; i++) {
    const rid = String(4170100000 + i), pad = String(i).padStart(5, '0'), line = `${rid}_${100 + (i % 900)}`;
    store.set(`${P}Order_Timeline/${rid}~note~bulk${pad}`, { orderId: rid, type: 'note', at: NOW - i, by: 'System', source: 'sorter', station: 'sorter', text: 'x', createdAt: new TS(NOW) });
    store.set(`${P}Charm_Custom_Orders/${line}`, { key: line, receiptId: rid, state: 'completed', updatedAtMs: NOW - i, stamps: [{ how: 'print', at: NOW - i, by: 'paul' }] });
    store.set(`${P}Charm_Pool/${line}_1`, { poolId: `${line}_1`, orderId: rid, state: 'pooled' });
    store.set(`${P}Charm_Nest_Run_Lines/run~${pad}`, { runId: 'run', json: '{}' });
    store.set(`${P}Station_Activity/bulk-activity-${pad}`, Object.assign({ id: 'bulk-activity-' + pad, person: 'Paul', day: '2026-09-29' }, sb ? { sandbox: true } : {}));
    store.set(`${P}Brites_Orders/${rid}/messages/m${pad}`, { text: 'x' });   // a message under an order that was never written
  }
}

/* ── what is where ── */
const sandboxKeys = () => [...store.keys()].filter(k => k.startsWith('Sandbox_'));
const families = keys => [...new Set(keys.map(k => k.split('/').filter((_, i) => i % 2 === 0).join('/')))].sort();
const isOwn = k => k.startsWith('Sandbox_') || k === 'Charm_Sandbox/stream' || k.startsWith('EtsyMail_OrderLinks/olsb_');
// the shared line reading carries the sandbox's decision beside production's: that field (and the time it was written) is the sandbox's
const shared = k => k.startsWith('Charm_Nest_CustomRead/');
const productionMap = () => new Map([...store.entries()].filter(([k]) => !isOwn(k)).map(([k, v]) => { const c = clone(v); if (shared(k)) { delete c.decidedSandbox; delete c.updatedAt; } return [k, canon(c)]; }));
const KEEP_FAMILY = 'Sandbox_Charm_Nest_Agent_Cache';   // the readings Claude was paid for stay
const seedAll = async () => {
  store.clear(); blobs.clear(); skew = 0; slow = 0;
  store.set('Charm_Sandbox/current', { path: 'charmnest/sandbox/orders-cur.json', count: 2, at: NOW, takenBy: 'test' });
  store.set('Charm_Sandbox/stream', { on: true, v: 2, seed: 7, speed: 50, stepMs: 600000, tick: 12, snapshotPath: 'charmnest/sandbox/orders-cur.json' });
  for (const f of ['orders-cur.json', 'master/BRITES-master.ai']) blobs.set('charmnest/sandbox/' + f, { buf: Buffer.from('x') });
  for (const f of ['charmnest/sheets/2026-09-29/prod.ai', 'charmnest/agent/agent-y.json', 'design-archive/listing/aa.jpg']) blobs.set(f, { buf: Buffer.from('x') });
  store.set(KEEP_FAMILY + '/paid-1', { text: 'ANNA' });
  // the shared line reading Claude was paid for, (production's customDecide adds its decision, the sandbox's its own beside it)
  store.set(`Charm_Nest_CustomRead/${A}_10010`, { reads: { h1: { kind: 'chainOnly' } }, order: A, updatedAt: new TS(NOW - 9000) });
  // production's own engagement of another order
  store.set(`EtsyMail_OrderLinks/ol_${B}_o_prod9`, { id: `ol_${B}_o_prod9`, receiptId: B, sandbox: false, status: 'open', outbox: [] });
  await seed(false); await seed(true);
};
const resetAll = async () => { slow = 1500; let r = await post({ op: 'sandboxReset', sandbox: true }), calls = 1, deleted = r.body.deleted || 0; assert.strictEqual(r.status, 200, JSON.stringify(r.body)); const first = r.body; while (r.body.more && calls < 400) { r = await post({ op: 'sandboxReset', sandbox: true }); assert.strictEqual(r.status, 200, JSON.stringify(r.body)); calls++; deleted += r.body.deleted || 0; } slow = 0; return { first, last: r.body, calls, deleted }; };

(async () => {
  /* ═══ 1 · the sandbox holds every family; the reset clears them all, a page at a time ═══ */
  await seedAll(); bulk(true, 700); bulk(false, 700);
  const seededFamilies = families(sandboxKeys());
  const EXPECT = ['Charm_Custom_Orders', 'Charm_Custom_Sheet', 'Charm_Nest_Cancelled', 'Charm_Nest_Cancelled_History', 'Charm_Nest_Arrivals', 'Charm_Nest_Counters', 'Charm_Nest_Release', 'Charm_Nest_Run_Lines', 'Charm_Nest_Run_Live', 'Charm_Nest_Runs',
    'Charm_Nest_Rose_Rehearsals', 'Charm_Nest_Rose_Stock', 'Charm_Nest_Rose_Stock/cuts', 'Charm_Nest_Sets', 'Charm_Nest_Sheets', 'Charm_Nest_Shape_Guidance', 'Charm_Nest_Agent', 'Charm_Pool', 'Charm_Pool_Back', 'Design_Bridge', 'Design_Bridge/log', 'Design_Completed Orders',
    'Design_Order_Archive', 'Design_RealTime_Selected_Orders', 'Brites_Orders', 'Brites_Orders/messages', 'Order_Timeline', 'Station_Activity', 'Efficiency_Daily', 'Station_Sessions'].map(n => 'Sandbox_' + n);
  for (const f of EXPECT) assert(seededFamilies.includes(f), 'the test seeds the sandbox family ' + f + ' (seeded: ' + seededFamilies.join(', ') + ')');
  // the same keys on both sides: the sandbox plays the real order numbers
  for (const [k] of store) if (/^Sandbox_(Charm_Custom_Orders|Charm_Nest_Cancelled|Charm_Custom_Sheet|Charm_Pool)\//.test(k)) assert(store.has(k.slice('Sandbox_'.length)), 'production holds the same key: ' + k);
  const custom = store.get(`Sandbox_Charm_Custom_Orders/${A}_10010`);
  assert(custom && custom.state === 'completed' && custom.stamps.length === 2 && custom.printedBy === 'paul' && custom.hasLabel === true, 'the custom order has its QR label stamps');
  assert(store.get(`Charm_Nest_CustomRead/${A}_10010`).decidedSandbox.kind === 'chainOnly' && store.get(`Charm_Nest_CustomRead/${A}_10010`).decided.kind === 'chainOnly', "the sandbox's decision sits on the shared reading, beside production's");
  const before = productionMap();
  console.log(`seeded: ${sandboxKeys().length} sandbox records in ${seededFamilies.length} families, ${before.size} production records with the same keys`);

  const run = await resetAll();   // a commit takes 1.5 s of the clock: a call cannot delete much, so the page calls again and again
  assert(run.first.more === true && run.calls > 5, 'a reset that runs out of time says so, for the page to call again: ' + JSON.stringify(run.first) + ' · ' + run.calls + ' calls');
  console.log(`reset: ${run.calls} calls removed ${run.deleted} records and ${run.last.files} files from the sandbox`);
  assert(run.last.more === false && !run.last.filesError, 'the calls finish the reset: ' + JSON.stringify(run.last));
  const left = sandboxKeys().filter(k => !k.startsWith(KEEP_FAMILY + '/'));
  assert.deepStrictEqual(families(left), [], 'the sandbox holds nothing: left ' + families(left).join(', ') + ' (' + left.slice(0, 6).join(', ') + ')');
  assert(![...store.keys()].some(k => k.startsWith('EtsyMail_OrderLinks/olsb_')), 'no customer message of the sandbox is left in the inbox collection');
  assert(!store.has('Charm_Sandbox/stream') && store.has('Charm_Sandbox/current'), 'the stream starts over, the snapshot stays');
  assert(store.has(KEEP_FAMILY + '/paid-1'), 'the readings Claude was paid for stay');
  const read = store.get(`Charm_Nest_CustomRead/${A}_10010`);
  assert(!('decidedSandbox' in read) && read.decided.kind === 'chainOnly' && read.reads.h1.kind === 'chainOnly', "the sandbox's decision goes, production's and the reading stay");
  const after = productionMap();
  assert.deepStrictEqual([...after.keys()].sort(), [...before.keys()].sort(), 'production lost or gained no document');
  for (const [k, v] of before) assert.strictEqual(after.get(k), v, 'production is byte-identical after the reset: ' + k);
  assert(store.get(`Charm_Custom_Orders/${A}_10010`).stamps.length === 2 && store.has(`Charm_Custom_Sheet/card-${require('crypto').createHash('sha256').update(`custom:${B}:CUSTOM-N-001`).digest('hex')}`) && store.has('Charm_Nest_Cancelled/' + C) && [...store.keys()].some(k => k.startsWith('Charm_Nest_Cancelled_History/' + A + '~')), "production's seals, custom sheets and cancel records (and their history) are all there");
  assert(store.has(`Charm_Pool/${A}_10010_1`) && store.has(`EtsyMail_OrderLinks/ol_${A}_o_k1abc`) && store.has(`EtsyMail_OrderLinks/ol_${B}_o_prod9`), "production's pool rows and customer messages stay");
  const stray = [...blobs.keys()].filter(k => (k.startsWith('charmnest/sandbox/') || k.startsWith('design-archive/sandbox/')) && !['charmnest/sandbox/orders-cur.json', 'charmnest/sandbox/master/BRITES-master.ai'].includes(k));
  assert.deepStrictEqual(stray, [], 'every sandbox file but the snapshot and the master files went');
  for (const k of ['charmnest/sheets/2026-09-29/prod.ai', 'charmnest/agent/agent-y.json', 'design-archive/listing/aa.jpg', 'charmnest/sandbox/orders-cur.json', 'charmnest/sandbox/master/BRITES-master.ai']) assert(blobs.has(k), 'kept: ' + k);
  // pressed again on an empty sandbox: nothing to do, nothing wrong
  const again = await post({ op: 'sandboxReset', sandbox: true });
  assert(again.status === 200 && again.body.more === false && again.body.deleted === 0, 'a second reset finds nothing: ' + JSON.stringify(again.body));

  /* ═══ 2 · a page that wrote after the reset (an old tab's late save) is cleared by the next press ═══ */
  await must({ op: 'customPut', key: `${A}_10010`, by: 'paul', receiptId: A, sandbox: true });
  await must({ op: 'cancelPut', orderId: C, by: 'paul', sandbox: true });
  assert(store.has(`Sandbox_Charm_Custom_Orders/${A}_10010`) && store.has('Sandbox_Charm_Nest_Cancelled/' + C), 'a late write lands');
  const late = await post({ op: 'sandboxReset', sandbox: true });
  assert(late.body.more === false && families(sandboxKeys().filter(k => !k.startsWith(KEEP_FAMILY + '/'))).length === 0, 'pressed again, the reset clears it');
  assert(store.get(`Charm_Custom_Orders/${A}_10010`).stamps.length === 2, 'and production is as it was');

  /* ═══ 3 · Purge all run history: production's own behaviour, and the sandbox completely ═══ */
  await seedAll(); bulk(true, 350); bulk(false, 350);
  store.set('Sandbox_Charm_Nest_Runs/run-open', { runId: 'run-open', status: 'running', updatedAt: new TS(Date.now()) });
  const prodBefore = new Map([...store.entries()].filter(([k]) => !isOwn(k)).map(([k, v]) => [k, canon(v)])), sandboxBefore = sandboxKeys().length, snap = () => canon([...store.entries()].sort(([a], [b]) => (a < b ? -1 : 1)));
  const whole = snap();
  let r = await post({ op: 'purgeHistory', code: 'wrong', sandbox: true });
  assert(r.status === 403 && /passcode/.test(r.body.error) && snap() === whole, 'a wrong passcode purges nothing');
  r = await post({ op: 'purgeHistory', sandbox: true });
  assert(r.status === 403 && snap() === whole, 'no passcode purges nothing');
  r = await post({ op: 'purgeHistory', code: '975311', sandbox: true });
  assert(r.status === 409 && /run-open/.test(r.body.error) && snap() === whole, 'an open run is named and nothing is purged until it is given up: ' + JSON.stringify(r.body));
  slow = 1500;
  r = await post({ op: 'purgeHistory', code: '975311', force: true, sandbox: true });
  assert(r.status === 503 && r.body.more === true && /press Purge again/.test(r.body.error), 'a purge that runs out of time is a failure the page shows, never "done": ' + JSON.stringify(r).slice(0, 200));
  let purges = 1; while (r.body.more && purges < 100) { r = await post({ op: 'purgeHistory', code: '975311', force: true, sandbox: true }); purges++; }
  slow = 0;
  assert(r.status === 200 && r.body.ok && !r.body.more, 'forced, the purge runs (until it says it is done): ' + JSON.stringify(r.body).slice(0, 300));
  const leftP = sandboxKeys().filter(k => !k.startsWith(KEEP_FAMILY + '/'));
  assert.deepStrictEqual(families(leftP), [], 'after the purge the sandbox holds nothing: left ' + families(leftP).join(', '));
  assert(![...store.keys()].some(k => k.startsWith('EtsyMail_OrderLinks/olsb_')) && !store.has('Charm_Sandbox/stream') && store.has('Charm_Sandbox/current'), 'no sandbox customer message, no stream, the snapshot stays');
  assert(store.has(KEEP_FAMILY + '/paid-1') && blobs.has('charmnest/sandbox/orders-cur.json') && ![...blobs.keys()].some(k => /^charmnest\/sandbox\/(sheets|sets|agent|charms)\//.test(k)), 'the paid readings and the snapshot stay, the sandbox files go');
  // production: its history families go as before (runs, lines, live parts, sheets, sets, pool, backs, counters, release, bridge log)…
  const GONE = ['Charm_Nest_Runs', 'Charm_Nest_Run_Lines', 'Charm_Nest_Run_Live', 'Charm_Nest_Sheets', 'Charm_Nest_Sets', 'Charm_Pool', 'Charm_Pool_Back', 'Charm_Nest_Counters', 'Charm_Nest_Release', 'Design_Bridge'];
  for (const n of GONE) assert(![...store.keys()].some(k => k.startsWith(n + '/') && !k.slice(n.length + 1).includes('/')), 'production history purged as before: ' + n);
  // …and what it never purged is untouched: its seals, custom orders and sheets, cancel records and their history, timeline, stations, messages
  const purged = n => GONE.some(g => n.startsWith(g + '/'));
  let keptDocs = 0;
  for (const [k, v] of prodBefore) { if (purged(k) || k === 'Charm_Sandbox/stream') continue; keptDocs++; const now = store.get(k); assert(now !== undefined, "production's " + k + ' is never purged'); if (!shared(k)) assert.strictEqual(canon(now), v, "production's " + k + ' is unchanged'); }
  assert(keptDocs > 1000 && store.get(`Charm_Custom_Orders/${A}_10010`).stamps.length === 2 && store.has('Charm_Nest_Cancelled/' + C) && store.has('Charm_Sandbox/current'), "production's seals and cancelled orders are permanent: " + keptDocs + ' documents kept');
  console.log(`purge: ${purges} call(s) removed the sandbox's ${sandboxBefore} records; production's seals, custom orders and cancel records intact (${keptDocs} documents kept)`);
  console.log('sandbox reset complete OK');
})().catch(e => { console.error(e); process.exit(1); });
