'use strict';
const { randomUUID, createHash } = require('node:crypto');
const ACTIVE = ['JOB_STATE_PENDING', 'JOB_STATE_RUNNING', 'BATCH_STATE_PENDING', 'BATCH_STATE_RUNNING'];
const quotaFailure = (message) => /enqueued token limit|enqueued.*tokens.*limit/i.test(String(message || ''));
// Token-limit refusals do not use one of the five retry attempts, so they
// get their own ceiling: a set the provider keeps refusing stops after this
// many refusals instead of cycling through the queue forever.
const CAPACITY_REFUSAL_LIMIT = 12;
// Refusals this set has had: those carried from earlier jobs and refused
// submissions (capacityRefusals) plus this job's own, when it was one.
const capacityRefusals = (record) => Number(record?.capacityRefusals || 0) +
  (quotaFailure(record?.providerError) ? 1 : 0);
const queuedName = (session, display, sets) => 'batch_local_' + createHash('sha256')
  .update(JSON.stringify([session, display, sets.map(s => s.outputBasePath).sort()])).digest('hex').slice(0, 40);
// Batch requests have a 24-hour completion window. A zero completed count
// does not prove that an in-progress request is stuck. Scheduled sweeps must
// not repeatedly cancel accepted requests inside that window. An explicit
// restart request can still choose an earlier cutoff below.
const STALL_RESTART_MS = 24 * 60 * 60 * 1000;
// Bound explicit/watchdog restarts independently of normal provider retries.
const STALL_RESTART_LIMIT = 60;
// OpenAI validates a new job in a minute or two, and a token-limit refusal
// comes then. Past this age a job still validating tells nothing more: the
// collector stops waiting for it and admission sends the next set (on
// 2026-09-29 one sat in validation for over half an hour and every queued set
// waited behind it). It is cancelled and sent again at STALL_RESTART_MS.
const VALIDATION_WAIT_MS = 15 * 60 * 1000;
// A person can ask the collector to restart stalled jobs sooner than
// STALL_RESTART_MS (Paul, 2026-09-29: "please restart"), never sooner than
// this: a job under half an hour old is normal, and each restart uses one of
// the tries.
const STALL_RESTART_MIN_MS = 30 * 60 * 1000;
const stallCutoffMs = (requested) => {
  const ms = Number(requested);
  return Number.isFinite(ms) && ms > 0 ? Math.min(STALL_RESTART_MS, Math.max(STALL_RESTART_MIN_MS, ms)) : STALL_RESTART_MS;
};
const toMillis = (value) => typeof value?.toMillis === 'function' ? value.toMillis() : Number(value || 0);
// A one-set listing job that has done nothing at all since it was sent.
// `live` is a fresh provider answer: { providerStatus, batchStats }.
function neverStarted(record, live, now, minAgeMs = STALL_RESTART_MS) {
  const stats = live?.batchStats || {};
  const sentAt = toMillis(record?.createdAt);
  return String(record?.batchName || '').startsWith('batch_') && !record.locallyQueued && !record.collected &&
    ['validating', 'in_progress'].includes(live?.providerStatus) &&
    // OpenAI counts a job's requests only once it has validated the file.
    (live.providerStatus === 'validating' || Number(stats.requestCount || 0) > 0) &&
    !Number(stats.successfulRequestCount || 0) && !Number(stats.failedRequestCount || 0) &&
    sentAt > 0 && now - sentAt >= minAgeMs &&
    Number(record.stallRestarts || 0) < STALL_RESTART_LIMIT &&
    record.sets?.length === 1 && record.sets[0]?.setKind !== 'charm_maker';
}

// Reading an error file is not completion of the listing. Keep the durable
// restart intent after collection; only a finished set, a replacement job or
// an explicit stop closes it.
function stallRestartPending(record) {
  return !!record?.stallCancelRequestedAt && !record.retryBatchName && !record.setComplete &&
    !record.stallRestartBlocked && !record.stallRestartClosed &&
    Number(record.stallRestarts || 0) < STALL_RESTART_LIMIT &&
    record.sets?.length === 1 && record.sets[0]?.setKind !== 'charm_maker';
}

function admissionControl(db, collection, timestamp, now = Date.now) {
  const gate = db.collection('LG1_Config').doc('batchAdmission');
  const batches = db.collection(collection);
  const activeQuery = () => batches.where('state', 'in', ACTIVE);
  const activeSize = snap => snap.docs.filter(d => !d.data().collected).length;
  async function reserve(sourceName) {
    const token = randomUUID();
    return db.runTransaction(async tx => {
      const source = batches.doc(sourceName);
      const [gs, live, ss] = await Promise.all([tx.get(gate), tx.get(activeQuery()), tx.get(source)]);
      const g = gs.data() || {}, s = ss.data() || {}, active = activeSize(live);
      if (s.retryBatchName) return { existing: s.retryBatchName };
      if (s.state === 'JOB_STATE_CANCELLED') return { queued: true, reason: 'Cancelled' };
      if (g.owner && (g.phase === 'creating' || now() - Number(g.startedAt || 0) < 15 * 60000))
        return { queued: true, reason: 'Another submission is being confirmed' };
      // One unvalidated batch at a time: a delayed token refusal must stop
      // admission before an entire refill is sent into the same full queue.
      if (g.probeName) {
        const probe = await tx.get(batches.doc(g.probeName));
        const p = probe.data();
        const sentAt = toMillis(p?.createdAt);
        if (!p || p.state === 'JOB_STATE_PENDING' && !(sentAt > 0 && now() - sentAt >= VALIDATION_WAIT_MS))
          return { queued: true, reason: 'Waiting for provider validation' };
      }
      if (active >= 30) return { queued: true, reason: 'Waiting for an active job to finish' };
      if (g.blockedAtActive != null && active >= g.blockedAtActive &&
          (active > 0 || now() - Number(g.blockedAt || 0) < 15 * 60000))
        return { queued: true, reason: 'Waiting for token capacity' };
      tx.set(gate, { owner: token, sourceName, startedAt: now(), phase: 'preparing',
        probeName: null, lastError: null }, { merge: true });
      return { token, active, sourceName };
    });
  }
  async function beforeCreate(claim, prepared) {
    await db.runTransaction(async tx => {
      const gs = await tx.get(gate);
      if (gs.data()?.owner !== claim.token) throw new Error('Submission reservation expired; job remains queued');
      tx.set(batches.doc(claim.sourceName), { preparedSubmission: prepared }, { merge: true });
      tx.set(gate, { phase: 'creating', inputFileName: prepared.inputFileName }, { merge: true });
    });
  }
  async function complete(claim, batchName, prepared, raw) {
    await db.runTransaction(async tx => {
      const gs = await tx.get(gate);
      if (gs.data()?.owner !== claim.token) throw new Error('Submission reservation changed; reconciliation required');
      tx.set(batches.doc(batchName), { ...prepared, batchName, docId: batchName,
        state: 'JOB_STATE_PENDING', rawCreate: raw || null,
        createdAt: timestamp(), updatedAt: timestamp() }, { merge: true });
      tx.set(batches.doc(claim.sourceName), { retryBatchName: batchName, retryStatus: 'submitted',
        preparedSubmission: null, updatedAt: timestamp() }, { merge: true });
      tx.set(gate, { owner: null, phase: 'idle', sourceName: null, inputFileName: null,
        probeName: batchName, lastError: null }, { merge: true });
    });
  }
  async function release(claim, error) {
    await db.runTransaction(async tx => {
      const gs = await tx.get(gate);
      const g = gs.data() || {};
      if (g.owner !== claim.token) return;
      // An ambiguous create response must never release the slot and send
      // the same paid request again. The sweep reconciles by input file ID.
      if (!error && g.phase === 'creating') return;
      const refused = error?.status >= 400 && error.status < 500 && error.status !== 408;
      tx.set(gate, { ...(g.phase === 'creating' && !refused ? {} : { owner: null, phase: 'idle' }),
        ...(error ? { lastError: quotaFailure(error.message) ? null : String(error?.message || error).slice(0, 500) } : {}) }, { merge: true });
    });
  }
  async function rejected(batchName) {
    await db.runTransaction(async tx => {
      const [gs, live] = await Promise.all([tx.get(gate), tx.get(activeQuery())]);
      const g = gs.data() || {};
      if (g.lastRejectedName === batchName) return;
      tx.set(gate, { blockedAtActive: activeSize(live), blockedAt: now(), lastRejectedName: batchName }, { merge: true });
    });
  }
  async function reconcile(listProviderBatches) {
    const gs = await gate.get();
    const g = gs.data() || {};
    if (!g.owner || g.phase !== 'creating' || now() - Number(g.startedAt || 0) < 3 * 60000) return;
    const ss = await batches.doc(g.sourceName).get();
    const prepared = ss.data()?.preparedSubmission;
    if (!prepared?.inputFileName) return;
    const raw = await listProviderBatches(prepared.inputFileName);
    if (raw) await complete({ token: g.owner, sourceName: g.sourceName }, raw.id, prepared, raw);
    else await gate.set({ lastError: 'A submission has an unconfirmed provider response. Admission is paused to prevent duplicate images.' }, { merge: true });
  }
  return { reserve, beforeCreate, complete, release, rejected, reconcile };
}
module.exports = { admissionControl, quotaFailure, queuedName, capacityRefusals, CAPACITY_REFUSAL_LIMIT,
  neverStarted, stallRestartPending, STALL_RESTART_MS, STALL_RESTART_MIN_MS, STALL_RESTART_LIMIT, VALIDATION_WAIT_MS, stallCutoffMs };
