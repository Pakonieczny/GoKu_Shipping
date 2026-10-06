// The bridge server's in-memory Firestore (tests/charm-nest/bridge-server.cjs) merges a set(..., { merge: true }) one level deep and does not know
// field increments inside a nested map or dotted field paths in update(). The stations' door keeps its daily rollups exactly that way
// ({ stations: { assembly: { scans: increment(1) } } } merged into the person's day), so over the bridge's store the rollup came out as
// { __inc: 2 } objects. This wraps that store (the same documents, the same Map) with Firestore's own rules for set and update:
//   · set(data)               replaces the document; sentinels (increment, delete, server time) inside nested maps are evaluated
//   · set(data, { merge })    merges nested maps field by field; increments add to what is there
//   · update(data)            "a.b.c" paths set that field; a plain value replaces the field (a nested map is replaced whole)
// Everything else (reads, queries, batches, transactions) is the bridge's own.
'use strict';

function wrapAdmin(admin, st) {
  // (the bridge's FieldValue has no array operators: arrayUnion / arrayRemove are added here, as the inbox's reply record uses arrayUnion)
  const FV = Object.assign({}, admin.firestore.FieldValue, { arrayUnion: (...a) => ({ __au: a }), arrayRemove: (...a) => ({ __ar: a }) });
  const Timestamp = admin.firestore.Timestamp, SERVER_TS = FV.serverTimestamp();
  const plain = v => v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Timestamp) && !Buffer.isBuffer(v) && v.__inc == null && !v.__del && !v.__au && !v.__ar && v !== SERVER_TS;
  const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
  const clone = v => (Array.isArray(v) ? v.map(clone) : plain(v) ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, clone(x)])) : v);

  function setField(target, key, v, merge) {
    if (v === undefined) return;
    if (v && v.__del) { delete target[key]; return; }
    if (v && v.__inc != null) { target[key] = (Number(target[key]) || 0) + v.__inc; return; }
    if (v && v.__au) { const cur = Array.isArray(target[key]) ? target[key].slice() : []; for (const e of v.__au) if (!cur.some(c => same(c, e))) cur.push(clone(e)); target[key] = cur; return; }
    if (v && v.__ar) { target[key] = (Array.isArray(target[key]) ? target[key] : []).filter(c => !v.__ar.some(e => same(c, e))); return; }
    if (v === SERVER_TS) { target[key] = Timestamp.now(); return; }
    if (plain(v)) {
      const base = merge && plain(target[key]) ? target[key] : {};
      target[key] = base;
      for (const [k, x] of Object.entries(v)) setField(base, k, x, merge);
      return;
    }
    target[key] = Array.isArray(v) ? v.map(x => (plain(x) ? (() => { const o = {}; for (const [k, y] of Object.entries(x)) setField(o, k, y, false); return o; })() : x)) : v;
  }
  function setPath(target, path, v) {
    const parts = path.split('.'); let t = target;
    for (let i = 0; i < parts.length - 1; i++) { if (!plain(t[parts[i]])) t[parts[i]] = {}; t = t[parts[i]]; }
    setField(t, parts[parts.length - 1], v, false);
  }
  const keyOf = ref => ref.path;

  function wrapRef(ref) {
    const w = Object.create(ref);
    w.set = async (data, o) => {
      const cur = o && o.merge ? clone(st.docs.get(keyOf(ref)) || {}) : {};
      for (const [k, v] of Object.entries(data || {})) setField(cur, k, v, !!(o && o.merge));
      st.docs.set(keyOf(ref), cur);
    };
    w.update = async data => {
      const prev = st.docs.get(keyOf(ref));
      if (!prev) throw Object.assign(new Error('NOT_FOUND: ' + keyOf(ref)), { code: 5 });
      const cur = clone(prev);
      for (const [k, v] of Object.entries(data || {})) { if (k.includes('.')) setPath(cur, k, v); else setField(cur, k, v, false); }
      st.docs.set(keyOf(ref), cur);
    };
    w.create = async data => {
      if (st.docs.has(keyOf(ref))) throw Object.assign(new Error('ALREADY_EXISTS: ' + keyOf(ref)), { code: 6 });
      const cur = {}; for (const [k, v] of Object.entries(data || {})) setField(cur, k, v, false);
      st.docs.set(keyOf(ref), cur);
    };
    w.collection = sub => wrapColl(ref.collection(sub));
    return w;
  }
  const CHAIN = new Set(['where', 'orderBy', 'limit', 'startAfter', 'select', 'limitToLast', 'offset']);
  function wrapColl(q) {
    return new Proxy(q, { get(t, p) {
      if (p === 'doc') return id => wrapRef(t.doc(id));
      if (p === 'add') return async data => { const id = 'auto' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8); const r = wrapRef(t.doc(id)); await r.set(data); return r; };
      if (CHAIN.has(p)) return (...a) => wrapColl(t[p](...a));
      const v = t[p]; return typeof v === 'function' ? v.bind(t) : v;
    } });
  }
  const db = admin.firestore();
  const db2 = new Proxy(db, { get(t, p) {
    if (p === 'collection') return c => wrapColl(t.collection(c));
    if (p === 'doc') return path => { const parts = path.split('/'); let r = wrapColl(t.collection(parts[0])).doc(parts[1]); for (let i = 2; i < parts.length; i += 2) r = r.collection(parts[i]).doc(parts[i + 1]); return r; };
    const v = t[p]; return typeof v === 'function' ? v.bind(t) : v;
  } });
  return { admin: { firestore: Object.assign(() => db2, { FieldValue: FV, Timestamp, FieldPath: admin.firestore.FieldPath }), storage: admin.storage }, db: db2 };
}
module.exports = { wrapAdmin };
