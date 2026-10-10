// The offline stand-ins the sandbox wipe's guard test runs on (tests/charm-nest/sandbox-wipe-guard.cjs): an in-memory Firestore
// and Storage in the style of sandbox-reset-complete.cjs, with two additions that make a missed sandbox family visible:
//   · every document it is asked to store goes through _noNestedArrays.cjs (Firestore refuses an array directly inside an array);
//   · every write (set / update / create / delete, in a batch or a transaction too) and every file save/delete is logged,
//     so the test can name each collection and Storage prefix a session touched and compare them with the registry.
'use strict';
const assert = require('assert'), path = require('path'), Module = require('module');
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const realNow = Date.now; let skew = 0, slow = 0; Date.now = () => realNow() + skew;
const writes = [], fileWrites = [];   // [{ kind, path }]  (reset by the test between phases)
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
    async set(data, o) { refuseNestedArrays(data, key); writes.push({ kind: 'set', path: key }); store.set(key, merge(o && o.merge ? clone(store.get(key) || {}) : {}, data, !!(o && o.merge))); },
    async create(data) { if (store.has(key)) throw new Error('ALREADY_EXISTS ' + key); refuseNestedArrays(data, key); writes.push({ kind: 'create', path: key }); store.set(key, merge({}, data, false)); },
    async update(data) { const cur = store.get(key); if (!cur) throw new Error('NOT_FOUND ' + key); refuseNestedArrays(data, key); writes.push({ kind: 'update', path: key }); store.set(key, merge(clone(cur), data, false)); },
    async delete() { writes.push({ kind: 'delete', path: key }); store.delete(key); } };
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
      async save(buf, o) { fileWrites.push({ kind: 'save', path: p }); blobs.set(p, { buf: Buffer.from(buf), contentType: o && o.contentType }); },
      async exists() { return [blobs.has(p)]; },
      async download() { const b = blobs.get(p); if (!b) throw Object.assign(new Error('no blob ' + p), { code: 404 }); return [b.buf]; },
      async delete(o) { if (!blobs.has(p) && !(o && o.ignoreNotFound)) throw Object.assign(new Error('no blob ' + p), { code: 404 }); fileWrites.push({ kind: 'delete', path: p }); blobs.delete(p); },
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
module.exports = { store, blobs, writes, fileWrites, TS, SERVER_TS, DEL, db, bucket, admin, canon, clone, realNow, setSlow: n => { slow = n; }, getSkew: () => skew,
  install() {
    const realLoad = Module._load;
    Module._load = function (req, ...rest) {
      if (req === 'node-fetch') return async () => { throw new Error('no network in this test'); };
      if (req === 'firebase-admin' || req === './firebaseAdmin' || /[\/]firebaseAdmin(\.js)?$/.test(req)) return admin;
      return realLoad.call(this, req, ...rest);
    };
    global.fetch = async () => ({ ok: true, headers: { get: () => 'image/jpeg' }, arrayBuffer: async () => new Uint8Array([255, 216, 255]).buffer, json: async () => ({}), text: async () => '' });
    delete process.env.EDIT_PASSCODE;
  } };
