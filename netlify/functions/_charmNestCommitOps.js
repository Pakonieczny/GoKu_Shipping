/*  netlify/functions/_charmNestCommitOps.js — the commit journal's ops (cloud plan v2 build step 12, hard-parts §2.3 item 3, §2.4, §2.8, §2.9; m:P8, P12).
 *
 *  Charm_Nest_Commits/<runId>~<setId> is the cloud's memory of sending one set to the Design Station: the station's localStorage commit memory
 *  (design-1.html rememberCommit / forgetCommits / priorCommit, ~8011-8033), kept in the cloud, and written FIRST (the intent) before any side effect,
 *  so a pass that dies half way is finished by the next one, which counts the set's own earlier marks as its own (never "already complete").
 *    { runId, setId, setName, ids:[rid], refused:[{id, reason}], held:{rid:{line, why}}, heldCount, gone:[rid], stage, stageAt, nonce,
 *      completed:[rid], messages:{rid: messageId}, archived:[rid], archiveSkipped:[rid], unlocked, labels, sheetIds, backCount, completedBy,
 *      deferWhy, result, undo:{at, by, dev}, undoneAt, createdAt, updatedAt }
 *  stage: intent → message → ledger → unlock → archive → record → done (→ undone after an undo).
 *
 *  Ops (one line in charmNestLibrary.js adds them to OPS; _charmNestOpClass.js classes them):
 *    commitJournal  CLOUD_ONLY  the only writer of a journal's stages: { runId, setId, from, to, patch }. One transaction, fenced on the cloud's lease and the
 *                               run's carrier (fenceTx): `from` must be the stored stage (null: no journal yet), else 409 { stage, journal } and nothing is
 *                               written (the unit goes on from the stored stage). A replay of the same body is answered by the mid ledger.
 *    commitGet      READ        { runId, setId? } → { journals }: what the cloud committed for a run (the station's priorCommit reads it, P12).
 *    commitUndoAsk  PRESS       { runId, setId, by } a person's Undo of a set the cloud committed: writes journal.undo; the commit unit reopens the orders
 *                               at the station when the run next reaches its commit step (the page sends Resume for a resting run).
 *  DORMANT: only the cloud's commit unit writes a journal, and it runs only while the drive is in standby or on with "commit" in its cloudSteps.        */
"use strict";
const COMMITS = "Charm_Nest_Commits";
const STAGES = new Set(["intent", "message", "ledger", "unlock", "archive", "record", "done", "undone"]);
const PATCH_FIELDS = ["setName", "ids", "refused", "held", "heldCount", "gone", "completed", "messages", "archived", "archiveSkipped", "archiveError", "archiveTries", "unlocked", "labels", "sheetIds",
  "backCount", "completedBy", "deferWhy", "result", "createdAt", "undo", "undoneAt", "reopened"];
const docId = (runId, setId) => runId + "~" + setId;

module.exports = deps => {
  const { db, col, fenceTx, isId, str, num, fenceNonce } = deps;
  const ids2 = b => { const runId = str(b.runId, 80), setId = str(b.setId, 80); return isId(runId) && isId(setId) ? { runId, setId } : null; };
  /** What a page or the unit is told of a journal (the whole of it but the per-order message ids). */
  const slim = d => (d ? { runId: d.runId || null, setId: d.setId || null, setName: d.setName || null, stage: d.stage || null, stageAt: num(d.stageAt) || 0,
    ids: (d.ids || []).map(String), completed: (d.completed || []).map(String), refused: Array.isArray(d.refused) ? d.refused : [], held: d.held || {}, gone: (d.gone || []).map(String),
    archived: (d.archived || []).map(String), unlocked: !!d.unlocked, result: d.result || null, deferWhy: d.deferWhy || null, undo: d.undo || null, undoneAt: num(d.undoneAt) || 0,
    createdAt: num(d.createdAt) || 0 } : null);

  async function commitJournal(b) {
    const k = ids2(b); if (!k) return { error: "bad run or set id", status: 400 };
    const to = str(b.to, 20); if (!STAGES.has(to)) return { error: "unknown commit stage: " + to, status: 400 };
    const from = b.from == null || b.from === "" ? null : str(b.from, 20);
    const p = b.patch && typeof b.patch === "object" && !Array.isArray(b.patch) ? b.patch : {}, patch = {};
    for (const f of PATCH_FIELDS) if (Object.prototype.hasOwnProperty.call(p, f)) patch[f] = p[f] === undefined ? null : p[f];
    const ref = col(COMMITS).doc(docId(k.runId, k.setId)), now = Date.now();
    return db.runTransaction(async tx => {
      await fenceTx(tx);   // (CLOUDDRIVE P7/C7: the cloud's lease and the run's carrier, first)
      const s = await tx.get(ref), cur = s.exists ? s.data() || {} : null, stage = cur ? cur.stage || null : null;
      if (from !== stage) return { error: `the commit of this set is at ${stage || "no stage yet"}, not ${from || "no stage yet"}: nothing was written`, status: 409, stage, journal: slim(cur) };
      const doc = Object.assign({}, patch, { runId: k.runId, setId: k.setId, stage: to, stageAt: now, nonce: (typeof fenceNonce === "function" && fenceNonce()) || null, updatedAt: now });
      if (!(num(doc.createdAt) > 0) && (to === "intent" || !(cur && num(cur.createdAt) > 0))) doc.createdAt = now;
      // (an intent starts the set's journal afresh, whole: a set committed again after an undo keeps nothing of the earlier try but how often it was reopened)
      if (to === "intent") { if (cur && cur.stage === "undone") doc.reopened = num(cur.reopened) + 1; tx.set(ref, doc); return { ok: true, stage: to, journal: slim(doc) }; }
      tx.set(ref, doc, { merge: true });
      return { ok: true, stage: to, journal: slim(Object.assign({}, cur || {}, doc)) };
    });
  }

  async function commitGet(b) {
    const runId = str(b.runId, 80); if (!isId(runId)) return { error: "bad run id", status: 400 };
    if (b.setId != null && b.setId !== "") {
      const setId = str(b.setId, 80); if (!isId(setId)) return { error: "bad set id", status: 400 };
      const s = await col(COMMITS).doc(docId(runId, setId)).get();
      return { ok: true, runId, journals: s.exists ? [slim(s.data())] : [] };
    }
    const q = await col(COMMITS).where("runId", "==", runId).limit(300).get();
    return { ok: true, runId, journals: q.docs.map(d => slim(d.data())) };
  }

  async function commitUndoAsk(b) {
    const k = ids2(b); if (!k) return { error: "bad run or set id", status: 400 };
    const by = str(b.by, 80) || "", dev = str(b.dev, 60) || "", ref = col(COMMITS).doc(docId(k.runId, k.setId)), now = Date.now();
    return db.runTransaction(async tx => {
      const s = await tx.get(ref), d = s.exists ? s.data() || {} : null;
      if (!d) return { error: "The cloud did not send this set to the Design Station: there is nothing for it to undo", status: 404 };
      if (d.stage === "undone") return { ok: true, already: true, words: "This set's completion is undone already: its orders are open at the station" };
      if (d.stage !== "done") return { error: `This set is still being sent to the Design Station (${d.stage}): undo it once that has finished`, status: 409, stage: d.stage };
      if (d.undo && num(d.undo.at) > num(d.undoneAt)) return { ok: true, asked: true, already: true, words: "Undo was asked already: the cloud reopens the orders when the run reaches its commit step" };
      tx.set(ref, { undo: { at: now, by, dev }, updatedAt: now }, { merge: true });
      return { ok: true, asked: true, words: "The cloud reopens this set's orders at the Design Station when the run reaches its commit step" };
    });
  }

  return { commitJournal, commitGet, commitUndoAsk };
};
module.exports.COMMITS = COMMITS;
module.exports.STAGES = STAGES;
module.exports.docId = docId;
