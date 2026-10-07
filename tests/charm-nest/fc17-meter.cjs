// A metered in-memory Firestore for the Sorter back-end cost tests (FC17, 7 Oct 2026). It answers the calls the charmNest*
// functions make (get, set, update, delete, getAll with a field mask, queries with where / orderBy / limit / select /
// startAfter, count(), batches and transactions) and COUNTS what a real Firestore would bill: documents read (an empty
// answer is one read; an aggregation is one read per 1,000 entries), the bytes of what came back (a field mask or select()
// leaves out the fields not named), writes and deletes. Used by fc17-reads.cjs and by the before/after comparison.
//   const m = require("./fc17-meter.cjs"); m.install(fnDir); ... m.reset(); await op(); m.stats()
"use strict";
const path = require("node:path");

const store = new Map();                               // "coll/id" → data
const stats = { reads: 0, bytes: 0, writes: 0, deletes: 0, calls: 0, byColl: {} };
const SERVER_TS = { __ts: true };
const FieldValue = { serverTimestamp: () => SERVER_TS, increment: n => ({ __inc: n }), delete: () => ({ __del: true }) };
class Timestamp { constructor(s, n) { this.seconds = s; this.nanoseconds = n; } toMillis() { return this.seconds * 1000 + Math.floor(this.nanoseconds / 1e6); } }
let clock = 1_780_000_000_000;
const stamp = () => { clock += 3; return new Timestamp(Math.floor(clock / 1000), (clock % 1000) * 1e6 + 123); };
const size = v => Buffer.byteLength(JSON.stringify(v, (k, x) => (x instanceof Timestamp ? x.toMillis() : x)));
const collOf = key => key.split("/")[0];
function bill(key, data, mask) {
  stats.reads++; stats.byColl[collOf(key)] = (stats.byColl[collOf(key)] || 0) + 1;
  let out = data; if (mask && data) { out = {}; for (const f of mask) if (f in data) out[f] = data[f]; }
  stats.bytes += data ? size(out) + key.length : key.length; return out;
}
function applyValues(target, src, ts) {
  for (const [k, v] of Object.entries(src)) {
    if (v === SERVER_TS) target[k] = ts; else if (v && v.__inc != null) target[k] = (target[k] || 0) + v.__inc; else if (v && v.__del) delete target[k];
    else if (v && typeof v === "object" && !(v instanceof Timestamp) && !Array.isArray(v) && !(v instanceof Date)) target[k] = JSON.parse(JSON.stringify(v));
    else target[k] = v;
  }
  return target;
}
const clone = v => (v instanceof Timestamp ? new Timestamp(v.seconds, v.nanoseconds) : Array.isArray(v) ? v.map(clone) : v && typeof v === "object" && !(v instanceof Date) ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, clone(x)])) : v);
function snapOf(key, mask) {
  const d = store.get(key), id = key.slice(key.lastIndexOf("/") + 1), data = bill(key, d, mask);
  return { exists: !!d, id, ref: docRef(key), data: () => (data ? clone(data) : undefined), updateTime: d ? new Timestamp(1, 1) : undefined };
}
function docRef(key) {
  const id = key.slice(key.lastIndexOf("/") + 1);
  return {
    id, path: key, get: async () => { stats.calls++; return snapOf(key); },
    collection: n => query(key + "/" + n),
    async set(data, opts) { stats.writes++; const ts = stamp(); store.set(key, applyValues({ ...((opts && opts.merge && store.get(key)) || {}) }, data, ts)); },
    async update(data) { stats.writes++; if (!store.has(key)) throw new Error("NOT_FOUND: " + key); store.set(key, applyValues({ ...store.get(key) }, data, stamp())); },
    async delete() { stats.deletes++; store.delete(key); }
  };
}
function query(coll, filters = [], order = [], lim = 0, after = null, mask = null) {
  const q = {
    where: (f, op, v) => query(coll, filters.concat([[f, op, v]]), order, lim, after, mask),
    orderBy: (f, dir) => query(coll, filters, order.concat([[f, dir || "asc"]]), lim, after, mask),
    limit: n => query(coll, filters, order, n, after, mask),
    startAfter: (...v) => query(coll, filters, order, lim, v, mask),
    select: (...f) => query(coll, filters, order, lim, after, f),
    doc: id => docRef(coll + "/" + (id || "auto" + Math.random().toString(36).slice(2, 10))),
    async add(data) { const ref = q.doc(); await ref.set(data); return ref; },
    _rows() {
      const val = (r, f) => (f === "__name__" ? r.id : r.data[f]), t = y => (y && y.toMillis ? y.toMillis() : y);
      let rows = [...store.keys()].filter(k => k.startsWith(coll + "/") && !k.slice(coll.length + 1).includes("/")).map(k => ({ key: k, id: k.slice(coll.length + 1), data: store.get(k) }));
      for (const [f, op, v] of filters) rows = rows.filter(r => { const x = val(r, f); return op === "==" ? t(x) === t(v) : op === "in" ? v.includes(x) : op === "array-contains" ? Array.isArray(x) && x.includes(v) : op === "array-contains-any" ? Array.isArray(x) && x.some(y => v.includes(y)) : op === ">=" ? t(x) >= t(v) : op === "<=" ? t(x) <= t(v) : op === ">" ? t(x) > t(v) : op === "<" ? t(x) < t(v) : true; });
      for (const [f] of order) if (f !== "__name__") rows = rows.filter(r => r.data[f] !== undefined);   // a document without the ordered field is not in the answer
      if (order.length) rows.sort((a, b) => { for (const [f, dir] of order) { const x = t(val(a, f)), y = t(val(b, f)); const c = x > y ? 1 : x < y ? -1 : 0; if (c) return dir === "desc" ? -c : c; } return 0; });
      else rows.sort((a, b) => (a.id < b.id ? -1 : 1));
      if (after && order.length) { const [f, dir] = order[0], a = t(after[0]); rows = rows.filter(r => (dir === "desc" ? t(val(r, f)) < a : t(val(r, f)) > a)); }
      return lim ? rows.slice(0, lim) : rows;
    },
    async get() {
      stats.calls++; const rows = q._rows();
      if (!rows.length) { stats.reads++; return { size: 0, empty: true, docs: [] }; }
      const docs = rows.map(r => snapOf(r.key, mask));
      return { size: docs.length, empty: false, docs };
    },
    count: () => ({ get: async () => { stats.calls++; const n = q._rows().length; stats.reads += Math.max(1, Math.ceil(n / 1000)); return { data: () => ({ count: n }) }; } }),
    async listDocuments() { return [...new Set([...store.keys()].filter(k => k.startsWith(coll + "/")).map(k => k.slice(coll.length + 1).split("/")[0]))].sort().map(id => docRef(coll + "/" + id)); }
  };
  return q;
}
const db = {
  collection: c => query(c),
  batch() { const ops = []; return { set: (r, d, o) => ops.push(() => r.set(d, o)), update: (r, d) => ops.push(() => r.update(d)), delete: r => ops.push(() => r.delete()), commit: async () => { for (const f of ops) await f(); } }; },
  async getAll(...refs) {
    stats.calls++; let mask = null; if (refs.length && typeof refs[refs.length - 1].get !== "function") mask = (refs.pop() || {}).fieldMask || null;
    return refs.map(r => snapOf(r.path, mask));
  },
  async runTransaction(fn) {
    const tx = { get: ref => (ref.get ? ref.get() : ref.get()), getAll: (...refs) => db.getAll(...refs), set: (r, d, o) => r.set(d, o), update: (r, d) => r.update(d), delete: r => r.delete() };
    return fn(tx);
  }
};
const bucket = { name: "test-bucket", file: p => ({ async save() {}, async exists() { return [false]; }, async download() { return [Buffer.alloc(0)]; }, async delete() {}, async getMetadata() { return [{ metadata: {} }]; }, async setMetadata() {}, async getSignedUrl() { return ["https://x"]; } }) };
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue, FieldPath: { documentId: () => "__name__" }, Timestamp }), storage: () => ({ bucket: () => bucket }) };

function install(fnDir) {
  require.cache[require.resolve(path.join(fnDir, "firebaseAdmin.js"))] = { id: "fake", filename: "firebaseAdmin.js", loaded: true, exports: fakeAdmin };
  const Module = require("module"), realLoad = Module._load;
  Module._load = function (req, ...rest) { if (req === "node-fetch") return async () => ({ ok: true, status: 202, text: async () => "" }); return realLoad.call(this, req, ...rest); };
}
const reset = () => { stats.reads = stats.bytes = stats.writes = stats.deletes = stats.calls = 0; stats.byColl = {}; };
const put = (coll, id, data) => store.set(coll + "/" + id, applyValues({}, data, stamp()));
const snapshot = () => ({ reads: stats.reads, bytes: stats.bytes, writes: stats.writes, deletes: stats.deletes });
module.exports = { install, reset, put, stats: snapshot, raw: stats, store, db, FieldValue, Timestamp };
