// The offline cloud for the cloud-driver tests (cloud plan v2, build step 1a). NOTHING here reaches the network, Etsy or the live cloud:
//   · an in-memory Firestore: documents, queries (where / orderBy / startAfter / select / count / array-contains(-any) / in), getAll with fieldMask, batches,
//     and transactions that behave like Firestore's (all-or-nothing, optimistic: a document read inside one that another writer changed before the commit runs it again;
//     create() fails with ALREADY_EXISTS), a 1 MiB document limit (run-size.cjs's), read and write counts, and hooks that make a read throw;
//   · an in-memory Storage bucket;
//   · a fake clock (Date.now) the tests move;
//   · THE ETSY TRIPWIRES: _etsyMailEtsy is a stub that COUNTS every call (the world build's pool() swallows a throw, so the count is what the tests assert), records every
//     getListingInventory, and throws "Etsy must not be called"; the stop switch's store (_etsyStop._setStore) reads the fake cloud and THROWS on any write; node-fetch and
//     globalThis.fetch go through one router that runs only the named Charm Nest background functions, in-process, when the test drains them; every Etsy address
//     (etsy.com, etsystatic.com) and every Etsy proxy function name (etsy*, listOpenOrders, imageProxy) is a tripwire, never "our function"; any other address is
//     recorded and throws.
//   Install it before anything under netlify/functions is required:   const F = require('./_cloudFakes.cjs')({ now: 1790000000000 });
'use strict';
const path = require('path'), Module = require('module');
const fnDir = path.join(__dirname, '../../netlify/functions');

module.exports = function install(opts = {}) {
  /* ── the clock ── */
  const realNow = Date.now.bind(Date);
  const clock = { t: opts.now || realNow(), fake: opts.clock !== false, now() { return this.fake ? this.t : realNow(); }, set(t) { this.t = t; }, advance(ms) { this.t += ms; return this.t; } };
  if (clock.fake) Date.now = () => clock.t;

  /* ── Firestore values ── */
  class Timestamp { constructor(ms) { this._ms = +ms; } toMillis() { return this._ms; } toDate() { return new Date(this._ms); } get seconds() { return Math.floor(this._ms / 1000); } valueOf() { return this._ms; } static fromMillis(ms) { return new Timestamp(ms); } static now() { return new Timestamp(Date.now()); } static fromDate(d) { return new Timestamp(d.getTime()); } }
  const OPV = Symbol('fieldValue');
  const FieldValue = { serverTimestamp: () => ({ [OPV]: 'ts' }), increment: n => ({ [OPV]: 'inc', n }), delete: () => ({ [OPV]: 'del' }), arrayUnion: (...v) => ({ [OPV]: 'union', v }), arrayRemove: (...v) => ({ [OPV]: 'remove', v }) };
  const isPlain = v => v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Timestamp) && !(v instanceof Date) && !v[OPV] && !Buffer.isBuffer(v);
  const clone = v => (v instanceof Timestamp ? new Timestamp(v._ms) : v instanceof Date ? new Date(v.getTime()) : Array.isArray(v) ? v.map(clone) : isPlain(v) ? Object.fromEntries(Object.entries(v).filter(([, x]) => x !== undefined).map(([k, x]) => [k, clone(x)])) : v);
  const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
  function applyOp(cur, v) {
    if (!v || !v[OPV]) return isPlain(v) ? resolve(v) : clone(v);
    if (v[OPV] === 'ts') return new Timestamp(Date.now());
    if (v[OPV] === 'inc') return (typeof cur === 'number' ? cur : 0) + v.n;
    if (v[OPV] === 'union') { const a = Array.isArray(cur) ? cur.slice() : []; for (const x of v.v) if (!a.some(y => same(y, x))) a.push(clone(x)); return a; }
    if (v[OPV] === 'remove') return (Array.isArray(cur) ? cur : []).filter(y => !v.v.some(x => same(x, y)));
    return undefined;
  }
  const resolve = obj => { const out = {}; for (const [k, v] of Object.entries(obj)) { if (v === undefined) continue; if (v && v[OPV] === 'del') continue; out[k] = applyOp(undefined, v); } return out; };
  function mergeInto(target, src) {   // set(..., { merge: true }): maps merge field by field, as Firestore's
    for (const [k, v] of Object.entries(src)) {
      if (v === undefined) continue;
      if (v && v[OPV] === 'del') { delete target[k]; continue; }
      if (isPlain(v) && isPlain(target[k])) { target[k] = mergeInto(Object.assign({}, target[k]), v); continue; }
      target[k] = applyOp(target[k], v);
    }
    return target;
  }
  function updateInto(target, src) {   // update(): a dotted key is a field path
    for (const [k, v] of Object.entries(src)) {
      const seg = k.split('.'); let o = target;
      for (const s of seg.slice(0, -1)) { if (!isPlain(o[s])) o[s] = {}; o[s] = Object.assign({}, o[s]); o = o[s]; }
      const last = seg[seg.length - 1];
      if (v && v[OPV] === 'del') delete o[last]; else o[last] = applyOp(o[last], v);
    }
    return target;
  }
  const bytesOf = d => Buffer.byteLength(JSON.stringify(d, (k, v) => (v instanceof Timestamp ? v._ms : v)));

  /* ── the store ── */
  const store = new Map(), ver = new Map(), colVer = new Map();
  const stats = { reads: 0, writes: 0, readPaths: [], writePaths: [], txRuns: 0, txRetries: 0 };
  const hooks = { failRead: null, beforeCommit: null, skip: 0 };
  const collOf = p => p.slice(0, p.lastIndexOf('/'));
  function put(p, d) {
    if (d === null) store.delete(p); else { if (bytesOf(d) > 1048576) throw new Error(`INVALID_ARGUMENT: ${p} is past the 1 MiB a document holds`); store.set(p, d); }
    ver.set(p, (ver.get(p) || 0) + 1); colVer.set(collOf(p), (colVer.get(collOf(p)) || 0) + 1);
    stats.writes++; stats.writePaths.push(p);
  }
  function snap(p, d, fields) {
    const id = p.slice(p.lastIndexOf('/') + 1), data = d ? clone(d) : undefined, shown = data && fields ? Object.fromEntries(Object.entries(data).filter(([k]) => fields.includes(k))) : data;
    return { id, exists: !!d, ref: docRef(p), data: () => (shown ? clone(shown) : undefined), get: f => (shown ? shown[f] : undefined), createTime: null, updateTime: null };
  }
  function readDoc(p, fields) {
    if (hooks.failRead && hooks.failRead.test(p)) throw new Error('UNAVAILABLE: the read of ' + p + ' failed (test)');
    stats.reads++; stats.readPaths.push(p);
    return snap(p, store.get(p), fields);
  }
  function docRef(p) {
    return {
      id: p.slice(p.lastIndexOf('/') + 1), path: p, parent: { id: collOf(p).split('/').pop(), path: collOf(p) },
      collection: sub => query(p + '/' + sub),
      async get() { return readDoc(p); },
      async set(d, o) { put(p, o && o.merge ? mergeInto(clone(store.get(p) || {}), d) : resolve(d)); },
      async update(d) { if (!store.has(p)) throw Object.assign(new Error('NOT_FOUND: no document to update: ' + p), { code: 5 }); put(p, updateInto(clone(store.get(p)), d)); },
      async create(d) { if (store.has(p)) throw Object.assign(new Error('ALREADY_EXISTS: ' + p), { code: 6 }); put(p, resolve(d)); },
      async delete() { if (store.has(p)) put(p, null); },
      isEqual: r => r && r.path === p
    };
  }
  const val = x => (x instanceof Timestamp ? x._ms : x instanceof Date ? x.getTime() : x);
  const field = (d, f, id) => (f === '__name__' ? id : String(f).split('.').reduce((o, k) => (o == null ? undefined : o[k]), d));
  function query(coll, q = { filters: [], order: [], lim: 0, after: null, fields: null }) {
    const next = patch => query(coll, Object.assign({}, q, patch));
    const api = {
      path: coll, id: coll.split('/').pop(),
      doc: id => docRef(coll + '/' + (id || 'auto' + Math.random().toString(36).slice(2, 12))),
      where: (f, op, v) => next({ filters: q.filters.concat([[typeof f === 'object' && f && f._id ? '__name__' : f, op, v]]) }),
      orderBy: (f, dir) => next({ order: q.order.concat([[typeof f === 'object' && f && f._id ? '__name__' : f, dir || 'asc']]) }),
      limit: n => next({ lim: n }), select: (...fs) => next({ fields: fs }), startAfter: (...v) => next({ after: v }),
      count: () => ({ get: async () => { const s = await api.get(); return { data: () => ({ count: s.size }) }; } }),
      async get() {
        if (hooks.failRead && hooks.failRead.test(coll + '/')) throw new Error('UNAVAILABLE: the query of ' + coll + ' failed (test)');
        let rows = [...store.entries()].filter(([k]) => collOf(k) === coll).map(([k, d]) => ({ p: k, id: k.slice(coll.length + 1), d }));
        for (const [f, op, v0] of q.filters) {
          const v = val(v0);
          rows = rows.filter(r => { const x = val(field(r.d, f, r.id));
            switch (op) { case '==': return same(x, v); case '!=': return !same(x, v); case '<': return x < v; case '<=': return x <= v; case '>': return x > v; case '>=': return x >= v;
              case 'in': return v.some(y => same(val(y), x)); case 'not-in': return !v.some(y => same(val(y), x)); case 'array-contains': return Array.isArray(x) && x.some(y => same(val(y), v));
              case 'array-contains-any': if (!Array.isArray(v) || v.length < 1 || v.length > 30) throw new Error('array-contains-any takes 1 to 30 values'); return Array.isArray(x) && x.some(y => v.some(w => same(val(y), val(w))));
              default: throw new Error('unsupported operator ' + op); } });
        }
        if (q.order.length) rows.sort((a, b) => { for (const [f, dir] of q.order) { const x = val(field(a.d, f, a.id)), y = val(field(b.d, f, b.id)); if (x === y) continue; const c = x == null ? -1 : y == null ? 1 : x > y ? 1 : -1; return dir === 'desc' ? -c : c; } return 0; });
        if (q.after) rows = rows.filter(r => { for (let i = 0; i < q.after.length && i < q.order.length; i++) { const [f, dir] = q.order[i], x = val(field(r.d, f, r.id)), y = val(q.after[i]); if (x === y) continue; return dir === 'desc' ? x < y : x > y; } return false; });
        if (q.lim) rows = rows.slice(0, q.lim);
        stats.reads += Math.max(1, rows.length); stats.readPaths.push(coll + '/*');
        const docs = rows.map(r => snap(r.p, r.d, q.fields));
        return { size: docs.length, empty: !docs.length, docs, forEach: f => docs.forEach(f) };
      },
      async listDocuments() { return [...new Set([...store.keys()].filter(k => k.startsWith(coll + '/')).map(k => k.slice(coll.length + 1).split('/')[0]))].map(id => docRef(coll + '/' + id)); },
      async add(d) { const r = api.doc(); await r.set(d); return r; }
    };
    return api;
  }
  const getAllArgs = args => { const last = args[args.length - 1]; const o = last && typeof last.get !== 'function' && !last.path ? args.pop() : null; return { refs: args, fields: o && o.fieldMask ? o.fieldMask : null }; };
  const db = {
    collection: c => query(c), doc: p => docRef(p), collectionGroup: () => { throw new Error('collectionGroup is not in the fake'); },
    async getAll(...args) { const { refs, fields } = getAllArgs(args); return refs.map(r => readDoc(r.path, fields)); },
    batch() { const ops = []; return { set(r, d, o) { ops.push(() => r.set(d, o)); return this; }, update(r, d) { ops.push(() => r.update(d)); return this; }, delete(r) { ops.push(() => r.delete()); return this; }, create(r, d) { ops.push(() => r.create(d)); return this; }, async commit() { for (const f of ops) await f(); } }; },
    async runTransaction(fn) {
      for (let attempt = 0; attempt < 12; attempt++) {
        stats.txRuns++;
        const reads = new Map(), qreads = new Map(), writes = [];
        const tx = {
          async get(r) { if (r && typeof r.where === 'function' && !r.set) { qreads.set(r.path, colVer.get(r.path) || 0); return r.get(); } reads.set(r.path, ver.get(r.path) || 0); return readDoc(r.path); },
          async getAll(...args) { const { refs, fields } = getAllArgs(args); for (const r of refs) reads.set(r.path, ver.get(r.path) || 0); return refs.map(r => readDoc(r.path, fields)); },
          set(r, d, o) { writes.push(['set', r.path, d, o]); return tx; }, update(r, d) { writes.push(['update', r.path, d]); return tx; }, delete(r) { writes.push(['delete', r.path]); return tx; }, create(r, d) { writes.push(['create', r.path, d]); return tx; }
        };
        const out = await fn(tx);
        // (a competing writer between this transaction's reads and its commit: hooks.beforeCommit, once, after hooks.skip transactions that write)
        if (hooks.beforeCommit && writes.length) { if (hooks.skip > 0) hooks.skip--; else { const h = hooks.beforeCommit; hooks.beforeCommit = null; await h(attempt); } }
        if ([...reads].some(([p, v]) => (ver.get(p) || 0) !== v) || [...qreads].some(([c, v]) => (colVer.get(c) || 0) !== v)) { stats.txRetries++; continue; }
        // all-or-nothing: every write checked first, then applied (synchronously: nothing else runs between)
        for (const [kind, p] of writes) { if (kind === 'create' && store.has(p)) throw Object.assign(new Error('ALREADY_EXISTS: ' + p), { code: 6 }); if (kind === 'update' && !store.has(p) && !writes.some(w => w[1] === p && w[0] !== 'update')) throw Object.assign(new Error('NOT_FOUND: ' + p), { code: 5 }); }
        const staged = new Map();
        const cur = p => (staged.has(p) ? staged.get(p) : store.get(p) || null);
        for (const [kind, p, d, o] of writes) {
          if (kind === 'delete') staged.set(p, null);
          else if (kind === 'set') staged.set(p, o && o.merge ? mergeInto(clone(cur(p) || {}), d) : resolve(d));
          else if (kind === 'update') staged.set(p, updateInto(clone(cur(p) || {}), d));
          else if (kind === 'create') staged.set(p, resolve(d));
        }
        for (const [p, d] of staged) { if (d && bytesOf(d) > 1048576) throw new Error(`INVALID_ARGUMENT: ${p} is past the 1 MiB a document holds`); }
        for (const [p, d] of staged) put(p, d);
        return out;
      }
      throw new Error('ABORTED: too much contention');
    }
  };

  /* ── Storage ── */
  const blobs = new Map(), signed = [];
  const missing = p => Object.assign(new Error('No such object: ' + p), { code: 404 });   // (as @google-cloud/storage's ApiError for an object that is not there)
  const bucket = { name: 'test-bucket', file(p) { return {
    name: p,
    async save(buf, o = {}) { const b = Buffer.isBuffer(buf) ? buf : Buffer.from(String(buf)); blobs.set(p, { buf: b, meta: { contentType: o.contentType || null, size: b.length, md5Hash: require('crypto').createHash('md5').update(b).digest('base64'), metadata: Object.assign({}, o.metadata && o.metadata.metadata) } }); },
    async exists() { return [blobs.has(p)]; },
    async download() { if (!blobs.has(p)) throw missing(p); return [blobs.get(p).buf]; },
    async delete() { blobs.delete(p); },
    async getMetadata() { const b = blobs.get(p); if (!b) throw missing(p); return [Object.assign({ name: p }, b.meta)]; },
    async setMetadata(m = {}) { const b = blobs.get(p); if (!b) throw missing(p); if (m.contentType) b.meta.contentType = m.contentType; Object.assign(b.meta.metadata, m.metadata || {}); return [b.meta]; },
    async copy(dst) { const b = blobs.get(p); const to = typeof dst === 'string' ? dst : dst.name; blobs.set(to, { buf: Buffer.from(b.buf), meta: JSON.parse(JSON.stringify(b.meta)) }); },
    async getSignedUrl(o = {}) { signed.push({ path: p, action: o.action, expires: o.expires }); return ['https://storage.test.invalid/' + encodeURIComponent(p) + '?action=' + o.action]; }
  }; }, async getFiles() { return [[...blobs.keys()].map(k => bucket.file(k))]; } };

  const admin = { firestore: Object.assign(() => db, { FieldValue, FieldPath: { documentId: () => ({ _id: true }) }, Timestamp }), storage: () => ({ bucket: () => bucket }), apps: [{}] };

  /* ── Etsy: the counted stub (never a real call) ── */
  const etsy = { calls: 0, inventory: [], log: [] };
  const etsyStub = new Proxy({ SHOP_ID: 'test-shop' }, { get(t, k) { if (k in t) return t[k]; if (typeof k !== 'string') return undefined; return async (...a) => { etsy.calls++; etsy.log.push(k); if (k === 'getListingInventory') etsy.inventory.push(a[0]); throw new Error('Etsy must not be called (' + k + ')'); }; } });

  /* ── the router: node-fetch and globalThis.fetch ── */
  const OURS = new Set(['charmNestDrive-background', 'charmNestSolve-background', 'charmNestSheet-background', 'charmNestWorld-background', 'charmEngrave-background']);
  const net = { ours: [], other: [], tripwire: [], kicks: [] };
  const answer = (status, body) => ({ ok: status < 400, status, statusText: String(status), headers: { get: () => null }, text: async () => body || '', json: async () => JSON.parse(body || '{}'), clone() { return this; } });
  async function route(url, init = {}) {
    const u = String((url && url.url) || url), m = /\/\.netlify\/functions\/([\w-]+)/.exec(u), name = m ? m[1] : null;
    if (/etsy\.com|etsystatic\.com/i.test(u) || (name && /^etsy|^listOpenOrders$|^imageProxy$/i.test(name))) { net.tripwire.push(u); throw new Error('TRIPWIRE: Etsy must not be called from a test: ' + u); }
    if (name && OURS.has(name)) { net.ours.push({ name, body: init.body || '' }); net.kicks.push({ name, body: init.body || '' }); return answer(202, ''); }
    net.other.push(u); throw new Error('no network in a test: ' + u);
  }
  const fakeFetch = Object.assign((u, i) => route(u, i), { Response: class { constructor(body, o = {}) { Object.assign(this, answer(o.status || 200, body)); } }, Headers: class { get() { return null; } } });
  globalThis.fetch = fakeFetch;
  /** Runs the queued kicks of our background functions in-process, in order (each with the body it was sent), until none is left (at most max). */
  async function drain(max = 50) { let n = 0; while (net.kicks.length && n < max) { const k = net.kicks.shift(); n++; const h = require(path.join(fnDir, k.name + '.js')); await h.handler({ httpMethod: 'POST', headers: {}, body: k.body }); } return n; }

  const realLoad = Module._load;
  Module._load = function (req, parent, ...rest) {
    if (req === 'node-fetch') return fakeFetch;
    if (req === 'firebase-admin' || req === './firebaseAdmin' || /[\\/]firebaseAdmin(\.js)?$/.test(req)) return admin;
    if (req === './_etsyMailEtsy' || /[\\/]_etsyMailEtsy(\.js)?$/.test(req)) return etsyStub;
    return realLoad.call(this, req, parent, ...rest);
  };

  /* ── the Etsy switch: read from the fake cloud, never written ── */
  const stopWrites = [];
  const stopStore = { async get() { if (hooks.failRead && hooks.failRead.test('EtsyApi_Config/stop')) throw new Error('UNAVAILABLE (test)'); const d = store.get('EtsyApi_Config/stop'); return d ? clone(d) : null; }, async set(p) { stopWrites.push(p); throw new Error('the Etsy switch must not be written by a test'); } };
  require(path.join(fnDir, '_etsyStop.js'))._setStore(stopStore);
  /** A function module of this repo (or of o.dir, another checkout's netlify/functions), loaded fresh when o.fresh (its module cache under that folder dropped first);
      the Etsy switch of that folder gets the fake store that throws on any write. */
  function load(file, o = {}) {
    const dir = o.dir || fnDir;
    if (o.fresh) for (const k of Object.keys(require.cache)) if (k.startsWith(dir + path.sep)) delete require.cache[k];
    const m = require(path.join(dir, file));
    require(path.join(dir, '_etsyStop.js'))._setStore(stopStore);
    return m;
  }

  /** No browser global exists (no page code was loaded). */
  const browserGlobals = () => ['document', 'localStorage', 'sessionStorage', 'indexedDB', 'OffscreenCanvas', 'HTMLElement'].filter(k => typeof globalThis[k] !== 'undefined');

  const doc = p => (store.has(p) ? clone(store.get(p)) : null);
  const seed = (p, d) => { store.set(p, resolve(d)); ver.set(p, (ver.get(p) || 0) + 1); colVer.set(collOf(p), (colVer.get(collOf(p)) || 0) + 1); };
  const list = coll => [...store.entries()].filter(([k]) => collOf(k) === coll).map(([k, d]) => Object.assign({ _id: k.slice(coll.length + 1) }, clone(d)));
  const count = () => ({ reads: stats.reads, writes: stats.writes, writePaths: stats.writePaths.length, readPaths: stats.readPaths.length });
  const since = mark => ({ reads: stats.reads - mark.reads, writes: stats.writes - mark.writes, writePaths: stats.writePaths.slice(mark.writePaths), readPaths: stats.readPaths.slice(mark.readPaths) });
  /** The library's handler, with every op that reaches it recorded (and whether it was the cloud's own, marked call), and every listingSkus body. */
  function spy(lib) {
    const ops = [], listingSkus = [], h = lib.handler;
    lib.handler = async ev => { let b = {}; try { b = JSON.parse(ev.body || '{}'); } catch (_) {} ops.push({ op: b.op, marked: Object.getOwnPropertySymbols(ev).length > 0, dev: b.dev }); return h(ev); };
    const ls = lib.ops.listingSkus; lib.ops.listingSkus = b => { listingSkus.push(JSON.parse(JSON.stringify(b || {}))); return ls(b); };
    return { ops, listingSkus };
  }
  const reset = () => { store.clear(); ver.clear(); colVer.clear(); blobs.clear(); signed.length = 0; net.kicks.length = 0; net.ours.length = 0; };   // (net.other and net.tripwire are kept: every test asserts them at its end)
  return { db, admin, store, bucket, blobs, signed, clock, stats, hooks, etsy, net, drain, stopWrites, browserGlobals, doc, seed, list, count, since, spy, load, reset, Timestamp, FieldValue, fnDir };
};
