/*  netlify/functions/_etsyMailMessageCopies.js
 *
 *  Firestore side of _etsyMailThreadAlign.js: load a thread's stored
 *  messages for matching, remove extra copies (archived first), and sweep
 *  every thread once for copies the old time-based dedupe left.
 *
 *  Used by etsyMailSnapshot.js (on every scrape, and its manual
 *  { op: "dedupeSweep" }) and by etsyMailReapers.js, whose scheduled run
 *  walks all threads a slice at a time until the sweep is done.
 */

"use strict";

const admin = require("./firebaseAdmin");
const align = require("./_etsyMailThreadAlign");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const THREADS_COLL = "EtsyMail_Threads";
const ARCHIVE_COLL = "EtsyMail_MessageArchive";
const SWEEP_DOC    = "messageDedupeSweep";          // in EtsyMail_Config
// Bump to run the whole sweep again (for example after a finder change).
const SWEEP_VERSION = 1;

// Each removed copy is kept in EtsyMail_MessageArchive first, so nothing is
// lost if one turns out to be a real repeat. Only scraped (source "etsy")
// docs are ever removed.
async function removeCopies(tRef, threadId, ids) {
  if (!ids || !ids.length) return 0;
  const list = Array.from(new Set(ids)).slice(0, 400);
  const snaps = await db.getAll(...list.map(id => tRef.collection("messages").doc(id)));
  const batch = db.batch();
  const now = FV.serverTimestamp();
  let removed = 0;
  snaps.forEach(sn => {
    if (!sn.exists || (sn.data() || {}).source !== "etsy") return;
    batch.set(db.collection(ARCHIVE_COLL).doc(`${threadId}__${sn.id}`), {
      threadId, messageId: sn.id, reason: "duplicate_copy", archivedAt: now, data: sn.data()
    });
    batch.delete(sn.ref);
    removed++;
  });
  if (!removed) return 0;
  batch.set(tRef, { messageCount: FV.increment(-removed), duplicatesRemovedAt: now }, { merge: true });
  await batch.commit();
  return removed;
}

// Stored messages of one thread, split into scraped ones and our stand-ins.
async function loadStoredForAlign(tRef) {
  const snap = await tRef.collection("messages")
    .select("contentHash", "senderRole", "text", "imageUrls", "timestamp", "createdAt", "source", "direction")
    .limit(2000).get();
  const ids = new Set(), etsy = [], other = [];
  snap.forEach(d => {
    ids.add(d.id);
    const data = d.data() || {};
    const row = {
      id       : d.id,
      fp       : align.fingerprint(data),
      tsMs     : align.msOf(data.timestamp),
      createdMs: align.msOf(data.createdAt),
      direction: data.direction || (data.senderRole === "customer" ? "inbound" : "outbound")
    };
    if (!row.fp || row.tsMs == null) return;
    if (data.source === "etsy") etsy.push(row); else other.push(row);
  });
  return { ids, etsy, other };
}

// One slice of threads in id order. Stops early at `deadline` (ms epoch);
// `next` is the cursor to continue from, null when the end was reached.
async function sweepPage({ startAfter = null, limit = 20, dryRun = true, deadline = Infinity } = {}) {
  let q = db.collection(THREADS_COLL).orderBy(admin.firestore.FieldPath.documentId()).select().limit(limit);
  if (startAfter) q = q.startAfter(String(startAfter));
  const page = await q.get();
  const threads = [];
  let scanned = 0, removed = 0, last = startAfter || null, stoppedEarly = false;
  for (const t of page.docs) {
    if (Date.now() > deadline) { stoppedEarly = true; break; }
    try {
      const st = await loadStoredForAlign(t.ref);
      const ids = align.findLegacyDuplicates(st.etsy, st.other);
      if (ids.length) {
        const n = dryRun ? ids.length : await removeCopies(t.ref, t.id, ids);
        removed += n;
        threads.push({ threadId: t.id, copies: n });
      }
    } catch (e) {
      threads.push({ threadId: t.id, error: e.message });
    }
    scanned++;
    last = t.id;
  }
  const done = !stoppedEarly && page.size < limit;
  return { scanned, removed, threads, next: done ? null : last };
}

// Scheduled: continue the one-time sweep of every thread within budgetMs.
// Progress lives in EtsyMail_Config/messageDedupeSweep.
async function runScheduledSweep({ budgetMs = 8000 } = {}) {
  const deadline = Date.now() + budgetMs;
  const ref = db.collection("EtsyMail_Config").doc(SWEEP_DOC);
  const snap = await ref.get();
  const prev = snap.exists ? (snap.data() || {}) : {};
  if (prev.version === SWEEP_VERSION && prev.status === "done") return { skipped: "done" };
  const st = prev.version === SWEEP_VERSION ? prev : {
    version: SWEEP_VERSION, status: "running", cursor: null,
    scanned: 0, removed: 0, threadsWithCopies: 0, errors: 0, threads: [], startedAt: Date.now()
  };
  let scanned = 0, removed = 0;
  while (Date.now() < deadline - 1000) {
    const page = await sweepPage({ startAfter: st.cursor, limit: 25, dryRun: false, deadline: deadline - 1000 });
    scanned += page.scanned;
    removed += page.removed;
    for (const t of page.threads) {
      if (t.error) st.errors = (st.errors || 0) + 1;
      else st.threadsWithCopies = (st.threadsWithCopies || 0) + 1;
    }
    st.threads = (st.threads || []).concat(page.threads).slice(-300);
    if (page.next === null) { st.status = "done"; st.finishedAt = Date.now(); break; }
    st.cursor = page.next;
    if (!page.scanned) break;
  }
  st.scanned = (st.scanned || 0) + scanned;
  st.removed = (st.removed || 0) + removed;
  st.updatedAt = Date.now();
  await ref.set(st);
  return { scanned, removed, status: st.status, cursor: st.cursor };
}

module.exports = { ARCHIVE_COLL, removeCopies, loadStoredForAlign, sweepPage, runScheduledSweep };
