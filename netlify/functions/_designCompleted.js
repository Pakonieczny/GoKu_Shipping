/*  netlify/functions/_designCompleted.js
 *  The completion ledger's index (FC3b, Firebase cost).
 *
 *  "Design_Completed Orders"/{receiptId} is the ledger: one document for every order a Design Station ever marked completed (37,314 of them on 7 Oct 2026,
 *  growing with every order, forever). A Design Station page that wants the Completed chip filled reads ALL of them (`?designCompleted=1`, ids only,
 *  but every document is a billed read): 37 k reads a time. The index keeps the same ids, 32 documents of about a thousand ids each, so the same
 *  answer costs 33 reads.
 *
 *    Design_Completed_Index/s00 .. s31   { ids: [receiptId, ...], at }       the ids of the ledger, by a fixed hash of the id
 *    Design_Completed_Index/meta         { v: 1, ready, shards, builtAt, checkedAt, reconciledAt }
 *
 *  Every writer of the ledger (firebaseOrders completedIds / uncompleteIds, the only ones) queues the index change IN THE SAME BATCH as the ledger
 *  write (stage(): arrayUnion / arrayRemove of the ids on their shards, no read), so the two cannot part by a failed write. The ledger stays the
 *  truth and is never changed by anything here: nothing is deleted, moved or rewritten.
 *
 *  list() is the read:
 *    * no index yet (the first read after this shipped), the sandbox, or anything failing: the whole ledger is read as it always was, and, in
 *      production, the index is built from that read (one writer at a time, a lease on meta). `ready` is set only after every shard is written.
 *    * a ready index: its shards are the answer (33 reads), checked cheaply: at most every 15 minutes the ledger's count() (about 38 reads) must
 *      equal the number of ids in the index, and at least once a week the whole ledger is read and compared (reconcile: 37 k reads, about 0.02 USD). A difference is corrected
 *      from the ledger, each id verified against its own ledger document inside a transaction, so a completion or an un-complete that lands
 *      meanwhile is never undone. This catches whatever wrote the ledger without going through firebaseOrders (an edit by hand in the console).
 *    * the answer is the ids in document-id order, as the ledger query returned them.
 *  Production only: the sandbox ledger (Sandbox_Design_Completed Orders) is small and stays as it was, with no index document.                       */
"use strict";
const COLL = "Design_Completed Orders", INDEX = "Design_Completed_Index", META = "meta", SHARDS = 32, V = 1;
const CHECK_MS = 15 * 60000, RECONCILE_MS = 7 * 24 * 3600000, LEASE_MS = 120000, WRITE_IDS = 800, TX_IDS = 100;

const shardName = i => "s" + String(i).padStart(2, "0");
/** The shard an id lives on: a fixed hash of the id (the same everywhere, for ever). */
function shardOf(id) { let h = 0; for (const c of String(id)) h = (h * 31 + c.charCodeAt(0)) >>> 0; return shardName(h % SHARDS); }
const group = ids => { const by = new Map(); for (const id of ids) { const k = shardOf(id); if (!by.has(k)) by.set(k, []); by.get(k).push(id); } return by; };
const chunks = (a, n) => { const out = []; for (let i = 0; i < a.length; i += n) out.push(a.slice(i, i + n)); return out; };
const idxRef = (db, name) => db.collection(INDEX).doc(name);
const usable = FV => !!(FV && typeof FV.arrayUnion === "function" && typeof FV.arrayRemove === "function" && typeof FV.serverTimestamp === "function");

/** Queue the index change for these ids on a batch (or transaction) that also writes the ledger: add = completed, else un-completed. No read. Returns the shards touched. */
function stage(w, db, FV, ids, add) {
  if (!usable(FV)) return 0;
  const by = group([...new Set((ids || []).map(String))]);
  for (const [name, part] of by) w.set(idxRef(db, name), { ids: add ? FV.arrayUnion(...part) : FV.arrayRemove(...part), at: FV.serverTimestamp() }, { merge: true });
  return by.size;
}

/** The index as it stands: { ready, meta, ids: Set } (ids only when ready). One read of the 33 small documents. */
async function readIndex(db) {
  const snap = await db.collection(INDEX).get(), out = { ready: false, meta: null, ids: new Set() };
  for (const d of snap.docs) {
    const x = d.data() || {};
    if (d.id === META) out.meta = x;
    else if (/^s\d\d$/.test(d.id) && Array.isArray(x.ids)) for (const id of x.ids) out.ids.add(String(id));
  }
  out.ready = !!(out.meta && out.meta.ready === true && out.meta.v === V && out.meta.shards === SHARDS);
  return out;
}

/** The whole ledger's ids, in document-id order (ids only: no field crosses the wire, every document is still a read). */
async function wholeLedger(db, prefix) { return (await db.collection((prefix || "") + COLL).select().get()).docs.map(d => d.id); }

/** Build the index from a whole read of the ledger. One builder at a time (a lease on meta); the shards are only ever added to here, so a completion
    that lands meanwhile (it queues its own add) is kept; `ready` is set last, with checkedAt 0 so the very next read counts the ledger against the
    index and corrects an un-complete that landed while this ran (reconcile). Returns whether it built. */
async function build(db, FV, ids, now) {
  const meta = idxRef(db, META);
  const got = await db.runTransaction(async tx => {
    const s = await tx.get(meta), m = s.exists ? s.data() || {} : {};
    if (m.ready === true && m.v === V) return false;
    if (m.buildingAt && now - Number(m.buildingAt) < LEASE_MS) return false;
    tx.set(meta, { buildingAt: now }, { merge: true });
    return true;
  });
  if (!got) return false;
  // (the shards are written at once: 32 documents, each a union with what a completion may have added there meanwhile)
  await Promise.all([...group(ids)].map(async ([name, part]) => { for (const c of chunks(part, WRITE_IDS)) await idxRef(db, name).set({ ids: FV.arrayUnion(...c), at: FV.serverTimestamp() }, { merge: true }); }));
  await meta.set({ v: V, ready: true, shards: SHARDS, builtAt: now, checkedAt: 0, reconciledAt: now, buildingAt: null }, { merge: true });
  return true;
}

/** Correct the index from the ledger: `missing` (in the ledger, not in the index) are added and `extra` (in the index, not in the ledger) removed, each id
    checked against its own ledger document inside the transaction that changes its shard, so what changed meanwhile is left as it now is. */
async function fix(db, FV, missing, extra) {
  let added = 0, removed = 0;
  for (const [add, list] of [[true, missing], [false, extra]]) {
    for (const [name, part] of group(list)) for (const c of chunks(part, TX_IDS)) {
      const n = await db.runTransaction(async tx => {
        const snaps = await tx.getAll(...c.map(id => db.collection(COLL).doc(id)));
        const ok = c.filter((id, i) => snaps[i].exists === add);   // an id to add must still be in the ledger, one to remove must still be gone
        if (ok.length) tx.set(idxRef(db, name), { ids: add ? FV.arrayUnion(...ok) : FV.arrayRemove(...ok), at: FV.serverTimestamp() }, { merge: true });
        return ok.length;
      });
      if (add) added += n; else removed += n;
    }
  }
  return { added, removed };
}

/** The ids of every completed order. opts: prefix ("Sandbox_" for the sandbox), now. Answers { ids, via: "ledger" | "index" | "reconciled", ... }. */
async function list(db, FV, opts = {}) {
  const prefix = opts.prefix || "", now = Number(opts.now) || Date.now();
  if (prefix || !usable(FV)) return { ids: await wholeLedger(db, prefix), via: "ledger" };
  let idx = null;
  try { idx = await readIndex(db); } catch (e) { console.warn("[designCompleted] index not read, reading the ledger:", (e && e.message) || e); }
  if (!idx || !idx.ready) {
    const ids = await wholeLedger(db, "");
    try { await build(db, FV, ids, now); } catch (e) { console.warn("[designCompleted] index not built:", (e && e.message) || e); }
    return { ids, via: "ledger" };
  }
  try {
    const m = idx.meta, daily = now - Number(m.reconciledAt || 0) > RECONCILE_MS;
    let live = null;
    if (!daily && now - Number(m.checkedAt || 0) > CHECK_MS) live = (await db.collection(COLL).count().get()).data().count;
    if (daily || (live != null && live !== idx.ids.size)) {
      // (the index was read first and the ledger after it: a completion or an un-complete in between is at worst a harmless difference, verified below)
      const ids = await wholeLedger(db, ""), have = new Set(ids), fixed = await fix(db, FV, ids.filter(id => !idx.ids.has(id)), [...idx.ids].filter(id => !have.has(id)));
      await idxRef(db, META).set({ checkedAt: now, reconciledAt: now }, { merge: true });
      return { ids, via: "reconciled", fixed };
    }
    if (live != null) await idxRef(db, META).set({ checkedAt: now }, { merge: true });
  } catch (e) { console.warn("[designCompleted] check failed, reading the ledger:", (e && e.message) || e); return { ids: await wholeLedger(db, ""), via: "ledger" }; }
  return { ids: [...idx.ids].sort(), via: "index" };
}

module.exports = { COLL, INDEX, META, SHARDS, CHECK_MS, RECONCILE_MS, shardOf, stage, readIndex, build, fix, list };
