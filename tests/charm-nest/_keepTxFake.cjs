// Firestore transactions that behave like the real ones, for the KEEP chaos tests (copied from tests/charm-nest/keep-server-chaos.cjs, KB): serialised, optimistic (a document read that moved before the commit
// makes the callback run again), all-or-nothing; hook.beforeApply (a competing writer that lands between the reads and the commit) and hook.crash ({after:k, torn}) for fault injection.
'use strict';
module.exports = function install(Fk) {
  const { store } = Fk;
const ver = new Map(), origSet = store.set.bind(store), origDel = store.delete.bind(store); let indexVer = 0;
const bump = k => { ver.set(k, (ver.get(k) || 0) + 1); if (k.startsWith('Charm_Master_Index/')) indexVer++; };
store.set = (k, v) => { bump(k); return origSet(k, v); }; store.delete = k => { bump(k); return origDel(k); };
const hook = { beforeApply: null, crash: null };   // beforeApply: async (attempt) => {}; crash: { after: k, torn: bool }
let lock = Promise.resolve(), bypass = 0, txRuns = 0, txRetries = 0;
Fk.db.runTransaction = function (fn) {
  const run = async () => {
    for (let attempt = 0; attempt < 8; attempt++) {
      txRuns++;
      const reads = new Map(), ops = []; let queryRead = false; const idx0 = indexVer;
      const tx = {
        get: async r => { if (r && r.path) reads.set(r.path, ver.get(r.path) || 0); else queryRead = true; return r.get(); },
        getAll: async (...refs) => { for (const r of refs) reads.set(r.path, ver.get(r.path) || 0); return Fk.db.getAll(...refs); },
        set: (r, d, o) => ops.push(() => r.set(d, o)), update: (r, d) => ops.push(() => r.update(d)), delete: r => ops.push(() => r.delete()), create: (r, d) => ops.push(() => r.create(d))
      };
      const out = await fn(tx);
      if (hook.beforeApply) { const h = hook.beforeApply; hook.beforeApply = null; bypass++; try { await h(attempt); } finally { bypass--; } }
      if ([...reads].some(([k, v]) => (ver.get(k) || 0) !== v) || (queryRead && indexVer !== idx0)) { txRetries++; continue; }   // somebody wrote what we read: run again, as Firestore does
      const snapshot = new Map(store), snapVer = new Map(ver), c = ops.length ? hook.crash : null; if (c) hook.crash = null;   // (a crash is aimed at the transaction that WRITES: the read-only pass before the files are checked has no writes)
      try {
        let i = 0;
        for (const op of ops) { if (c && c.after === i) throw new Error('crash after ' + i + ' writes'); await op(); i++; }
        if (c && c.after >= ops.length) throw new Error('crash at the end');
      } catch (e) {
        if (!(c && c.torn)) { store.clear(); for (const [k, v] of snapshot) origSet(k, v); ver.clear(); for (const [k, v] of snapVer) ver.set(k, v); }   // all-or-nothing, as Firestore
        throw e;
      }
      return out;
    }
    throw new Error('too much contention');
  };
  if (bypass) return run();
  const p = lock.then(run, run); lock = p.catch(() => {}); return p;
};
  return { hook, stats: () => ({ txRuns, txRetries }) };
};
