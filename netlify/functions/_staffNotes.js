/*  netlify/functions/_staffNotes.js
 *  The staff-note index (COST, Oct 2026; the pattern of _designCompleted.js).
 *
 *  "Which of these orders have a Staff Note, and what does it say?" used to cost a read per order asked about (staffNotesFor: ten-id "in"
 *  queries on Brites_Orders, about 800 reads for each sweep of the open orders), and a read per noted order for the text (the station's
 *  staffNoteOf, ?orderId=). The index keeps every receipt's note in 4 small documents, so the same answer, texts included, costs 4 reads:
 *
 *    Staff_Notes_Index/n0 .. n3   { v: 1, ready, notes: { receiptId: "the note" | true }, at }     a receipt's shard: a fixed hash of its id
 *                                 (n0 also carries builtAt, reconciledAt, buildingAt, triedAt)
 *
 *  A note longer than TEXT_MAX is kept as `true` (it has a note; the page reads its text from the order). Only receipt ids (digits) are kept:
 *  the scanner relay documents ("weld-scan-1"...) carry a JSON descriptor in "Staff Note" and are never shown as a note.
 *
 *  firebaseOrders, the only writer of "Staff Note", queues the index change IN THE SAME BATCH as the order write (stage(): no read), so the two
 *  cannot part by a failed write. Brites_Orders stays the truth and nothing here changes it.
 *
 *  lookup() is the read:
 *    * no ready index (the first ask after this shipped, or a shard marked spoiled): the notes are read the old way, all at once
 *      (where "Staff Note" != "": a read per noted order, about 3,500), the index is built from that read (one builder at a time, a lease on n0;
 *      a failed build is not tried again for 6 hours, the old per-order asks answer meanwhile) and that read answers this ask.
 *    * a ready index: its 4 shards are the answer (kept 5 s in a warm instance, so the chunks of one sweep share them; this instance's own
 *      writes drop that copy). Once a week the notes are read whole again and compared; each difference is verified against its own
 *      order inside a transaction, so a note written meanwhile is never undone. This catches a note changed outside firebaseOrders.
 *  Production only: the sandbox (Sandbox_Brites_Orders) is small and keeps the per-order asks, with no index.                              */
"use strict";
const COLL = "Brites_Orders", INDEX = "Staff_Notes_Index", SHARDS = 4, V = 1, TEXT_MAX = 1500;
const RECONCILE_MS = 7 * 24 * 3600000, LEASE_MS = 120000, RETRY_MS = 6 * 3600000, CACHE_MS = 5000, TX_IDS = 100;

const isReceipt = id => /^\d{1,20}$/.test(String(id == null ? "" : id));
const shardName = i => "n" + i;
/** The shard a receipt lives on: a fixed hash of the id (the same everywhere, for ever). */
function shardOf(id) { let h = 0; for (const c of String(id)) h = (h * 31 + c.charCodeAt(0)) >>> 0; return shardName(h % SHARDS); }
/** What the index keeps for a note: its text, `true` for a long one, null for none (blank, missing). */
function entryOf(v) { const t = (v == null ? "" : String(v)); return !t.trim() ? null : t.length <= TEXT_MAX ? t : true; }
const idxRef = (db, name) => db.collection(INDEX).doc(name);
const usable = FV => !!(FV && typeof FV.delete === "function" && typeof FV.serverTimestamp === "function");
const group = ids => { const by = new Map(); for (const id of ids) { const k = shardOf(id); if (!by.has(k)) by.set(k, []); by.get(k).push(id); } return by; };
let cache = null;   // { at, idx } this warm instance's copy of a ready index

/** Queue the index change for one order's note on a batch that also writes the order. No read. Returns whether it queued. */
function stage(w, db, FV, id, note) {
  if (!usable(FV) || !isReceipt(id)) return false;
  const e = entryOf(note);
  w.set(idxRef(db, shardOf(id)), { notes: { [String(id)]: e === null ? FV.delete() : e }, at: FV.serverTimestamp() }, { merge: true });
  cache = null;
  return true;
}
/** The batch with the order failed (a shard too large, say) and the order was written alone: its shard is marked not ready and loses the
    entry, so readers ask per order until a build (which only adds what a shard lacks) puts the true note back. Never throws. */
async function spoil(db, FV, id) {
  cache = null;
  try { if (usable(FV) && isReceipt(id)) await idxRef(db, shardOf(id)).set({ ready: false, notes: { [String(id)]: FV.delete() } }, { merge: true }); }
  catch (e) { console.warn("[staffNotes] shard not marked:", (e && e.message) || e); }
}

/** The index as it stands: { ready, meta (n0), notes: Map }. 4 reads. */
async function readIndex(db) {
  const snap = await db.collection(INDEX).get(), out = { ready: false, meta: null, notes: new Map() };
  let good = 0;
  for (const d of snap.docs) {
    const x = d.data() || {};
    if (!/^n\d$/.test(d.id) || Number(d.id.slice(1)) >= SHARDS) continue;
    if (d.id === shardName(0)) out.meta = x;
    if (x.v === V && x.ready === true) good++;
    for (const [id, t] of Object.entries(x.notes || {})) if (t === true || (typeof t === "string" && t.trim())) out.notes.set(id, t);
  }
  out.ready = good === SHARDS;
  return out;
}

/** Every receipt's note, the old way (a read per noted order). */
async function wholeRead(db) {
  const snap = await db.collection(COLL).where("Staff Note", "!=", "").select("Staff Note").get(), notes = new Map();
  for (const d of snap.docs) { if (!isReceipt(d.id)) continue; const e = entryOf((d.data() || {})["Staff Note"]); if (e !== null) notes.set(d.id, e); }
  return notes;
}

/** Take the lease on n0 (one builder or reconciler at a time). retry: also refuse within RETRY_MS of the last try. */
async function lease(db, now, { retry }) {
  return db.runTransaction(async tx => {
    const ref = idxRef(db, shardName(0)), s = await tx.get(ref), m = s.exists ? s.data() || {} : {};
    if (m.buildingAt && now - Number(m.buildingAt) < LEASE_MS) return false;
    if (retry && m.triedAt && now - Number(m.triedAt) < RETRY_MS) return false;
    tx.set(ref, { buildingAt: now, triedAt: now }, { merge: true });
    return true;
  });
}

/** Build the index from a whole read: each shard gains only the receipts it lacks (an entry already there was written with its order, after
    or with this read), in a transaction, and is marked ready with it. */
async function build(db, FV, notes, now) {
  for (let i = 0; i < SHARDS; i++) {
    const name = shardName(i);
    await db.runTransaction(async tx => {
      const ref = idxRef(db, name), s = await tx.get(ref), have = (s.exists && (s.data() || {}).notes) || {}, add = {};
      for (const [id, e] of notes) if (shardOf(id) === name && !Object.prototype.hasOwnProperty.call(have, id)) add[id] = e;
      tx.set(ref, { v: V, ready: true, notes: add, at: FV.serverTimestamp() }, { merge: true });
    });
  }
  await idxRef(db, shardName(0)).set({ builtAt: now, reconciledAt: now, buildingAt: null }, { merge: true });
}

/** Correct the index where it differs from a whole read: each id verified against its own order inside the transaction that changes its shard. */
async function fix(db, FV, ids) {
  let fixed = 0;
  for (const [name, part] of group(ids)) for (let i = 0; i < part.length; i += TX_IDS) {
    const c = part.slice(i, i + TX_IDS);
    fixed += await db.runTransaction(async tx => {
      const snaps = await tx.getAll(...c.map(id => db.collection(COLL).doc(id))), patch = {};
      c.forEach((id, k) => { const e = snaps[k].exists ? entryOf((snaps[k].data() || {})["Staff Note"]) : null; patch[id] = e === null ? FV.delete() : e; });
      tx.set(idxRef(db, name), { notes: patch, at: FV.serverTimestamp() }, { merge: true });
      return c.length;
    });
  }
  return fixed;
}

/** The notes of these receipts: { notes: Map(id -> text | true), via: "index" | "built" | "reconciled" }, or null when the index cannot
    answer (the sandbox, no FieldValue, a build already running or tried in the last 6 hours, anything failing): the caller asks per order. */
async function lookup(db, FV, ids, opts = {}) {
  if (opts.prefix || !usable(FV)) return null;
  const now = Number(opts.now) || Date.now(), want = [...new Set((ids || []).map(String))];
  const pick = (notes, via) => { const out = new Map(); for (const id of want) if (notes.has(id)) out.set(id, notes.get(id)); return { notes: out, via }; };
  try {
    let idx = cache && now - cache.at < CACHE_MS ? cache.idx : null;
    if (!idx) idx = await readIndex(db);
    if (!idx.ready) {
      cache = null;
      if (!(await lease(db, now, { retry: true }))) return null;
      const notes = await wholeRead(db);
      try { await build(db, FV, notes, now); } catch (e) { console.warn("[staffNotes] index not built:", (e && e.message) || e); }
      return pick(notes, "built");
    }
    if (now - Number((idx.meta && idx.meta.reconciledAt) || 0) > RECONCILE_MS && await lease(db, now, { retry: false })) {
      const notes = await wholeRead(db), diff = new Set();
      for (const [id, e] of notes) if (idx.notes.get(id) !== e) diff.add(id);
      for (const id of idx.notes.keys()) if (!notes.has(id)) diff.add(id);
      if (diff.size) await fix(db, FV, [...diff]);
      await idxRef(db, shardName(0)).set({ reconciledAt: now, buildingAt: null }, { merge: true });
      cache = null;
      return pick(notes, "reconciled");
    }
    cache = { at: now, idx };
    return pick(idx.notes, "index");
  } catch (e) { cache = null; console.warn("[staffNotes] index not read, asking per order:", (e && e.message) || e); return null; }
}

module.exports = { COLL, INDEX, SHARDS, TEXT_MAX, RECONCILE_MS, isReceipt, shardOf, entryOf, stage, spoil, readIndex, wholeRead, build, fix, lookup, _reset: () => { cache = null; } };
