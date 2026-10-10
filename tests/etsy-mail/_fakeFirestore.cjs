"use strict";
/* An in-memory Firestore for the send-queue tests, faithful where the real one bites:
 *  - refuses an array inside an array, `undefined` field values and documents over 1 MiB (the live database throws on each);
 *  - transactions are optimistic, as the server SDK's are: every read records the document's version, the commit is refused
 *    (and the callback run again, up to 5 times) when any document read, or any collection a query read, changed meanwhile;
 *  - "all reads before all writes" inside a transaction, or it throws exactly like the live one;
 *  - every call yields to the event loop (optionally for a random time) so many callers interleave, which is the point of
 *    the concurrency tests; the pseudo-random generator is seeded, so a failing run replays.
 *  - FieldValue.serverTimestamp / delete / increment / arrayUnion / arrayRemove, Timestamp, where (==, !=, <, <=, >, >=, in,
 *    array-contains), orderBy, limit, select (ignored), getAll, batch, add.
 * The clock is Date.now(), so a test can drive time by replacing it.
 */
const refuseNestedArrays = require("../charm-nest/_noNestedArrays.cjs");

class Timestamp {
  constructor(ms) { this._ms = ms; this.seconds = Math.floor(ms / 1000); this.nanoseconds = (ms % 1000) * 1e6; }
  toMillis() { return this._ms; }
  toDate() { return new Date(this._ms); }
  isEqual(o) { return !!o && o._ms === this._ms; }
  static now() { return new Timestamp(Date.now()); }
  static fromMillis(ms) { return new Timestamp(ms); }
  static fromDate(d) { return new Timestamp(d.getTime()); }
}
const SENT = (kind, extra) => Object.assign({ __fv: kind }, extra || {});
const DELETE_SENTINEL = SENT("delete");     // the real SDK's FieldValue.delete() is one shared object too
const FieldValue = {
  serverTimestamp: () => SENT("ts"),
  delete: () => DELETE_SENTINEL,
  increment: n => SENT("inc", { n }),
  arrayUnion: (...items) => SENT("union", { items }),
  arrayRemove: (...items) => SENT("remove", { items })
};
const isFv = v => v && typeof v === "object" && typeof v.__fv === "string";

function clone(v) {
  if (v instanceof Timestamp) return new Timestamp(v._ms);
  if (Array.isArray(v)) return v.map(clone);
  if (v && typeof v === "object") { const o = {}; for (const [k, x] of Object.entries(v)) o[k] = clone(x); return o; }
  return v;
}
const same = (a, b) => JSON.stringify(a, (k, v) => (v instanceof Timestamp ? { __t: v._ms } : v)) === JSON.stringify(b, (k, v) => (v instanceof Timestamp ? { __t: v._ms } : v));

function checkValue(v, where) {
  if (v === undefined) throw new Error(`Value for argument "data" is not a valid Firestore document. Cannot use "undefined" as a Firestore value (found in field "${where}").`);
  if (Array.isArray(v)) v.forEach((x, i) => checkValue(x, where + "[" + i + "]"));
  else if (v && typeof v === "object" && !(v instanceof Timestamp) && !isFv(v)) for (const [k, x] of Object.entries(v)) checkValue(x, where + "." + k);
}
function resolveFv(cur, v, now) {
  if (!isFv(v)) return v;
  switch (v.__fv) {
    case "ts": return new Timestamp(now);
    case "inc": return (typeof cur === "number" ? cur : 0) + v.n;
    case "union": { const a = Array.isArray(cur) ? cur.slice() : []; for (const it of v.items) if (!a.some(x => same(x, it))) a.push(it); return a; }
    case "remove": return (Array.isArray(cur) ? cur : []).filter(x => !v.items.some(it => same(x, it)));
    default: return v;
  }
}
/* apply a set/merge/update payload to the stored data (a plain object or null) */
function applyWrite(data, payload, mode, now) {
  if (mode === "set") {
    const out = {};
    const deep = v => {
      if (isFv(v)) { if (v.__fv === "delete") throw new Error("FieldValue.delete() cannot be used with set() unless you pass {merge:true}"); return resolveFv(undefined, v, now); }
      if (Array.isArray(v)) return v.map(deep);
      if (v && typeof v === "object" && !(v instanceof Timestamp)) { const o = {}; for (const [k, x] of Object.entries(v)) o[k] = deep(x); return o; }
      return v;
    };
    for (const [k, v] of Object.entries(payload)) out[k] = clone(deep(v));
    return out;
  }
  const out = data ? clone(data) : {};
  const put = (obj, path, v) => {
    const keys = path.split(".");
    let o = obj;
    for (let i = 0; i < keys.length - 1; i++) { if (!o[keys[i]] || typeof o[keys[i]] !== "object" || Array.isArray(o[keys[i]]) || o[keys[i]] instanceof Timestamp) o[keys[i]] = {}; o = o[keys[i]]; }
    const last = keys[keys.length - 1];
    if (isFv(v) && v.__fv === "delete") delete o[last];
    else o[last] = clone(resolveFv(o[last], v, now));
  };
  const mergeMap = (target, src) => {
    for (const [k, v] of Object.entries(src)) {
      if (v && typeof v === "object" && !Array.isArray(v) && !(v instanceof Timestamp) && !isFv(v)) {
        if (!target[k] || typeof target[k] !== "object" || Array.isArray(target[k]) || target[k] instanceof Timestamp) target[k] = {};
        mergeMap(target[k], v);
      } else if (isFv(v) && v.__fv === "delete") delete target[k];
      else target[k] = clone(resolveFv(target[k], v, now));
    }
  };
  if (mode === "merge") mergeMap(out, payload);
  else for (const [k, v] of Object.entries(payload)) put(out, k, v);   // update(): dotted paths
  return out;
}
function getPath(data, field) { return String(field).split(".").reduce((o, k) => (o == null ? undefined : o[k]), data); }
const cmpVal = v => (v instanceof Timestamp ? v._ms : v);
function matches(data, f) {
  const [field, op, want] = f;
  const have = cmpVal(getPath(data, field)), w = cmpVal(want);
  switch (op) {
    case "==": return have === w || (have !== undefined && have !== null && typeof have === "object" && same(have, w));
    case "!=": return have !== undefined && have !== w;
    case "<": return have !== undefined && have !== null && have < w;
    case "<=": return have !== undefined && have !== null && have <= w;
    case ">": return have !== undefined && have !== null && have > w;
    case ">=": return have !== undefined && have !== null && have >= w;
    case "in": return Array.isArray(want) && want.some(x => cmpVal(x) === have);
    case "array-contains": return Array.isArray(getPath(data, field)) && getPath(data, field).some(x => same(x, want));
    default: throw new Error("fake firestore: operator not supported " + op);
  }
}

function createFake(opts = {}) {
  let seed = (opts.seed == null ? 1 : opts.seed) >>> 0;
  const rnd = () => { seed = (seed * 1664525 + 1013904223) >>> 0; return seed / 4294967296; };
  const docs = new Map();            // path -> { data, version }
  const collVersion = new Map();     // collection path -> counter of writes
  let nextVersion = 1, autoId = 1;
  const stats = { reads: 0, writes: 0, txRetries: 0, txCommits: 0, queries: 0, byColl: {} };
  let jitterMs = opts.jitterMs || 0;
  let failNext = null;               // optional fault injection: { match(op,path), error }
  const hooks = { beforeCommit: null };

  // Every call yields to the event loop a seeded number of times, so callers interleave differently from seed to seed
  // and the same seed replays the same interleaving (no real timers: nothing depends on how fast the machine is).
  const tick = async () => { const n = 1 + (jitterMs ? Math.floor(rnd() * jitterMs) : 0); for (let i = 0; i < n; i++) await new Promise(r => setImmediate(r)); };
  const collOf = p => p.split("/").slice(0, -1).join("/");
  const bump = (coll) => collVersion.set(coll, (collVersion.get(coll) || 0) + 1);
  const count = (kind, path) => { stats[kind]++; const c = path.split("/")[0]; stats.byColl[c] = stats.byColl[c] || { reads: 0, writes: 0 }; stats.byColl[c][kind] = (stats.byColl[c][kind] || 0) + 1; };
  const maybeFail = (op, path) => { if (failNext && failNext.match(op, path)) { const e = failNext.error; if (failNext.once) failNext = null; throw e; } };

  function snapOf(path) {
    const d = docs.get(path);
    const id = path.split("/").pop();
    const ref = docRef(path);
    return { id, ref, exists: !!d, data: () => (d ? clone(d.data) : undefined), get: f => (d ? getPath(d.data, f) : undefined), updateTime: d ? new Timestamp(d.version) : undefined };
  }
  function commit(writes, now) {          // synchronous and atomic
    for (const w of writes) {
      const cur = docs.get(w.path);
      if (w.kind === "delete") { docs.delete(w.path); }
      else {
        if (w.kind === "update" && !cur) throw new Error(`5 NOT_FOUND: No document to update: ${w.path}`);
        if (w.kind === "create" && cur) throw new Error(`6 ALREADY_EXISTS: Document already exists: ${w.path}`);
        const data = applyWrite(cur ? cur.data : null, w.data, w.kind === "update" ? "update" : w.merge ? "merge" : "set", now);
        refuseNestedArrays(data, w.path);
        if (JSON.stringify(data).length > 1048576) throw new Error("3 INVALID_ARGUMENT: document too large: " + w.path);
        docs.set(w.path, { data, version: nextVersion++ });
      }
      bump(collOf(w.path));
      count("writes", w.path);
    }
  }
  function prepWrite(kind, path, data, merge) {
    if (data !== undefined) { checkValue(data, path); refuseNestedArrays(data, path); }
    return { kind, path, data, merge: !!merge };
  }

  function docRef(path) {
    const ref = {
      id: path.split("/").pop(), path,
      get parent() { return collRef(collOf(path)); },
      collection: n => collRef(path + "/" + n),
      get: async () => { await tick(); maybeFail("get", path); count("reads", path); return snapOf(path); },
      set: async (data, o) => { await tick(); maybeFail("set", path); commit([prepWrite("set", path, data, o && o.merge)], Date.now()); },
      create: async data => { await tick(); commit([prepWrite("create", path, data)], Date.now()); },
      update: async data => { await tick(); maybeFail("update", path); commit([prepWrite("update", path, data)], Date.now()); },
      delete: async () => { await tick(); commit([{ kind: "delete", path }], Date.now()); }
    };
    return ref;
  }
  function collRef(path, spec) {
    spec = spec || { where: [], order: [], limit: 0 };
    const derive = patch => collRef(path, Object.assign({}, spec, patch));
    const q = {
      id: path.split("/").pop(), path,
      doc: id => docRef(path + "/" + (id || ("auto" + (autoId++)))),
      add: async data => { const id = "auto" + (autoId++) + "_" + Math.floor(rnd() * 1e6).toString(36); await docRef(path + "/" + id).set(data); return docRef(path + "/" + id); },
      where: (f, o, v) => derive({ where: spec.where.concat([[f, o, v]]) }),
      orderBy: (f, d) => derive({ order: spec.order.concat([[f, d || "asc"]]) }),
      limit: n => derive({ limit: n }),
      select: () => q, offset: () => q,
      _run: () => {
        let list = [];
        for (const [p, d] of docs) if (p.startsWith(path + "/") && !p.slice(path.length + 1).includes("/")) {
          if (spec.where.every(f => matches(d.data, f))) list.push(p);
        }
        for (const [f] of spec.order) list = list.filter(p => getPath(docs.get(p).data, f) !== undefined);
        list.sort((a, b) => {
          for (const [f, dir] of spec.order) {
            const x = cmpVal(getPath(docs.get(a).data, f)), y = cmpVal(getPath(docs.get(b).data, f));
            if (x < y) return dir === "desc" ? 1 : -1; if (x > y) return dir === "desc" ? -1 : 1;
          }
          return a < b ? -1 : 1;
        });
        if (spec.limit) list = list.slice(0, spec.limit);
        return list;
      },
      get: async () => {
        await tick(); stats.queries++;
        const paths = q._run();
        for (const p of paths) count("reads", p);
        if (!paths.length) count("reads", path + "/_empty");
        const ds = paths.map(snapOf);
        return { empty: !ds.length, size: ds.length, docs: ds, forEach: f => ds.forEach(f) };
      },
      _collPath: path
    };
    return q;
  }

  async function runTransaction(fn, o) {
    const max = 5;
    for (let attempt = 1; ; attempt++) {
      const readDocs = new Map();       // path -> version at read (0 = absent)
      const readColls = new Map();      // collection path -> collection version at read
      const writes = [];
      let wrote = false;
      const tx = {
        get: async target => {
          if (wrote) throw new Error("Firestore transactions require all reads to be executed before all writes.");
          await tick();
          if (target && typeof target._run === "function") {          // a query
            stats.queries++;
            readColls.set(target._collPath, collVersion.get(target._collPath) || 0);
            const ds = target._run().map(p => { count("reads", p); return snapOf(p); });
            return { empty: !ds.length, size: ds.length, docs: ds, forEach: f => ds.forEach(f) };
          }
          const d = docs.get(target.path);
          if (!readDocs.has(target.path)) readDocs.set(target.path, d ? d.version : 0);
          count("reads", target.path);
          return snapOf(target.path);
        },
        getAll: async (...refs) => { const out = []; for (const r of refs) out.push(await tx.get(r)); return out; },
        set: (ref, data, o2) => { wrote = true; writes.push(prepWrite("set", ref.path, data, o2 && o2.merge)); return tx; },
        create: (ref, data) => { wrote = true; writes.push(prepWrite("create", ref.path, data)); return tx; },
        update: (ref, data) => { wrote = true; writes.push(prepWrite("update", ref.path, data)); return tx; },
        delete: ref => { wrote = true; writes.push({ kind: "delete", path: ref.path }); return tx; }
      };
      let result;
      try { result = await fn(tx); } catch (e) { throw e; }
      await tick();
      // validate and commit with no await in between: atomic, like the server
      let conflict = false;
      for (const [p, ver] of readDocs) { const d = docs.get(p); if ((d ? d.version : 0) !== ver) { conflict = true; break; } }
      if (!conflict) for (const [c, ver] of readColls) if ((collVersion.get(c) || 0) !== ver) { conflict = true; break; }
      if (!conflict && hooks.beforeCommit) hooks.beforeCommit(writes);
      if (!conflict) { commit(writes, Date.now()); stats.txCommits++; return result; }
      stats.txRetries++;
      if (attempt >= max) throw new Error("10 ABORTED: too much contention on these documents.");
    }
  }

  const db = {
    collection: n => collRef(n),
    doc: p => docRef(p),
    runTransaction,
    getAll: async (...refs) => { await tick(); return refs.map(r => { count("reads", r.path); return snapOf(r.path); }); },
    batch: () => {
      const ws = [];
      const b = {
        set: (r, d, o) => { ws.push(prepWrite("set", r.path, d, o && o.merge)); return b; },
        update: (r, d) => { ws.push(prepWrite("update", r.path, d)); return b; },
        delete: r => { ws.push({ kind: "delete", path: r.path }); return b; },
        commit: async () => { await tick(); commit(ws, Date.now()); }
      };
      return b;
    }
  };
  const firestore = () => db;
  firestore.FieldValue = FieldValue;
  firestore.Timestamp = Timestamp;
  const admin = { firestore, apps: [{}], storage: () => ({ bucket: () => ({}) }) };

  return {
    admin, db, Timestamp, FieldValue, stats,
    /* direct access for assertions (no counting, no yielding) */
    peek: path => (docs.has(path) ? clone(docs.get(path).data) : undefined),
    poke: (path, data) => { commit([prepWrite("set", path, data, false)], Date.now()); },
    list: coll => [...docs.keys()].filter(p => p.startsWith(coll + "/") && !p.slice(coll.length + 1).includes("/")).map(p => ({ id: p.split("/").pop(), data: clone(docs.get(p).data) })),
    count: coll => [...docs.keys()].filter(p => p.startsWith(coll + "/") && !p.slice(coll.length + 1).includes("/")).length,
    setJitter: ms => { jitterMs = ms; },
    rnd, hooks,
    failOn: (match, error, once = true) => { failNext = { match, error, once }; },
    clearFail: () => { failNext = null; },
    resetStats: () => { stats.reads = 0; stats.writes = 0; stats.txRetries = 0; stats.txCommits = 0; stats.queries = 0; stats.byColl = {}; }
  };
}

/* Puts a fake in place of firebase-admin for every module loaded afterwards (the way the repo's other offline tests do),
 * and keeps the network shut: node-fetch and global fetch refuse. Returns { restore }. */
function install(fake) {
  const Module = require("module");
  const realLoad = Module._load;
  Module._load = function (req, ...rest) {
    if (req === "node-fetch") return async () => { throw new Error("no network in this test"); };
    if (req === "firebase-admin" || req === "./firebaseAdmin" || /[\/]firebaseAdmin(\.js)?$/.test(req)) return fake.admin;
    return realLoad.call(this, req, ...rest);
  };
  const realFetch = global.fetch;
  global.fetch = async () => { throw new Error("no network in this test"); };
  return { restore() { Module._load = realLoad; global.fetch = realFetch; } };
}

module.exports = { createFake, install, Timestamp, FieldValue };
