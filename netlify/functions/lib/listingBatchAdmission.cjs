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
// OpenAI sometimes accepts a job and never starts it: 0 of 6 images for 6 to
// 17 hours while jobs sent after it finish in minutes. Each one holds one of
// the thirty places until OpenAI expires it at 24 hours, so the whole queue
// stops: 2026-09-27 and 28, and again on 2026-10-02 when the wait was set to
// the 24-hour window itself (fc9924b): all thirty places sat at 0 of 6 for up
// to 17 hours and nothing was saved for 13. After STALL_RESTART_MS with nothing
// done the collector cancels the job (nothing was made, so nothing is billed)
// and queues its set again. Do not set this to the provider's 24-hour window.
// How long to wait, from the 255 jobs sent 2026-09-29 03:36 to 2026-09-30
// 02:45 UTC: 142 finished, 106 were never started, 7 were still open. Jobs
// that finished took 30 minutes at the median and 133 at the 90th percentile;
// hardly any with nothing done at 150 minutes ever finished. A job with
// nothing done at 45 minutes had a 17% chance of finishing in the next hour,
// a new job 36%, and a restart costs nothing. Waiting three hours made the
// average set take 3.4 hours to get through; 45 minutes gives about 2.5. Jobs
// finished as often with 30 running as with a few, so cancelling does not
// lose a place in some queue.
const STALL_RESTART_MS = 45 * 60 * 1000;
// About four jobs in ten are never started, whichever set they carry (more in
// the US daytime), so a few sets in a big batch need five or six tries. A
// restart costs nothing and takes about an hour, so the limit is only a stop
// for something badly wrong (a day of OpenAI not starting anything): about
// two and a half days of tries.
const STALL_RESTART_LIMIT = 60;
// OpenAI validates a new job in a minute or two, and a token-limit refusal
// comes then. Past this age a job still validating tells nothing more: the
// collector stops waiting for it and admission sends the next set (on
// 2026-09-29 one sat in validation for over half an hour and every queued set
// waited behind it). It is cancelled and sent again at STALL_RESTART_MS.
const VALIDATION_WAIT_MS = 15 * 60 * 1000;
// Preparation has not created a paid batch. Its owner is checked again just
// before creation, so a dead preparer can be replaced without duplicate jobs.
// An uncertain create remains reserved until provider reconciliation confirms it.
const PREPARATION_RESERVATION_MS = 5 * 60 * 1000;
const PREPARATION_FAILURE_LIMIT = 3;
function preparationFailurePatch(record, message, now, timestamp) {
  const failures = Number(record?.preparationFailures || 0) + 1;
  const stopped = failures >= PREPARATION_FAILURE_LIMIT;
  return { preparationFailures: failures, retryError: String(message).slice(0, 500),
    retryNotBefore: stopped ? null : now + 10 * 60000,
    retryRequested: !stopped, repairPending: false,
    ...(stopped ? { state: 'JOB_STATE_FAILED', locallyQueued: false,
      retryStatus: 'preparation_failed', recoveryStatus: 'blocked',
      recoveryReason: `Listing preparation failed ${failures} times. ${String(message).slice(0, 300)}` } : {}),
    updatedAt: timestamp() };
}
// A person can ask the collector to restart stalled jobs sooner than
// STALL_RESTART_MS (Paul, 2026-09-29: "please restart"), never sooner than
// this: a job under half an hour old is normal, and each restart uses one of
// the tries.
const STALL_RESTART_MIN_MS = 30 * 60 * 1000;
const stallCutoffMs = (requested) => {
  const ms = Number(requested);
  return Number.isFinite(ms) && ms > 0 ? Math.min(STALL_RESTART_MS, Math.max(STALL_RESTART_MIN_MS, ms)) : STALL_RESTART_MS;
};
// The collector's own calls on a listing job: asking OpenAI for its status,
// cancelling a stalled job, and saving a finished one. On 2026-10-02 one save
// never answered, the run waited on it for 13 minutes, and nothing else was
// checked or sent in that time. A call that does not answer is given up on
// (the work it started is safe to repeat: saves skip files that exist) and the
// run goes on; the next run asks again. Never use this for a submission: an
// abandoned create could be paid for twice.
const SWEEP_CALL_LIMITS = { batch_status: 90 * 1000, batch_stall_cancel: 90 * 1000, batch_collect: 2 * 60 * 1000 };
function withLimit(promise, ms, onTimeout) {
  let timer;
  const limit = new Promise((resolve) => { timer = setTimeout(() => resolve(onTimeout), ms); });
  return Promise.race([promise, limit]).finally(() => clearTimeout(timer));
}
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
  // Only `collected` is read from the active jobs (activeSize), so the query asks for that one field. A job's
  // record carries its prompts and prepared submission: reading whole records for a head count cost tens of KB
  // per active job on every admission check and every provider refusal.
  const activeQuery = () => { const q = batches.where('state', 'in', ACTIVE); return typeof q.select === 'function' ? q.select('collected') : q; };
  const activeSize = snap => snap.docs.filter(d => !d.data().collected).length;
  async function reserve(sourceName) {
    const token = randomUUID();
    return db.runTransaction(async tx => {
      const source = batches.doc(sourceName);
      const [gs, live, ss] = await Promise.all([tx.get(gate), tx.get(activeQuery()), tx.get(source)]);
      const g = gs.data() || {}, s = ss.data() || {}, active = activeSize(live);
      if (s.retryBatchName) return { existing: s.retryBatchName };
      if (s.retryStatus === 'preparation_failed' || Number(s.retryNotBefore || 0) > now())
        return { queued: true, sourceError: true, reason: s.retryError || 'Listing preparation will be retried later' };
      if (s.state === 'JOB_STATE_CANCELLED') return { queued: true, reason: 'Cancelled' };
      if (g.owner && (g.phase === 'creating' || now() - Number(g.startedAt || 0) < PREPARATION_RESERVATION_MS))
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
        preparationStage: 'checking copied images', probeName: null, lastError: null }, { merge: true });
      return { token, active, sourceName };
    });
  }
  async function progress(claim, stage) {
    await db.runTransaction(async tx => {
      const gs = await tx.get(gate);
      if (gs.data()?.owner !== claim.token) throw new Error('Submission reservation expired; job remains queued');
      tx.set(gate, { preparationStage: stage }, { merge: true });
    });
  }
  async function failPreparation(claim, error) {
    return db.runTransaction(async tx => {
      const [gs, ss] = await Promise.all([tx.get(gate), tx.get(batches.doc(claim.sourceName))]);
      const g = gs.data() || {}, s = ss.data() || {};
      if (g.owner !== claim.token || g.phase !== 'preparing' || s.retryBatchName) return false;
      tx.set(batches.doc(claim.sourceName), preparationFailurePatch(s, error?.message || error, now(), timestamp), { merge: true });
      tx.set(gate, { owner: null, phase: 'idle', sourceName: null,
        lastError: String(error?.message || error).slice(0, 500) }, { merge: true });
      return true;
    });
  }
  async function recoverPreparation(stalled) {
    return db.runTransaction(async tx => {
      const gs = await tx.get(gate);
      const g = gs.data() || {};
      // A provider create may already have succeeded. Only preparation can be
      // retired here; an uncertain create is always reconciled below.
      if (g.phase === 'creating') return;
      const expired = g.owner && now() - Number(g.startedAt || 0) >= PREPARATION_RESERVATION_MS;
      if (g.owner && !expired) return;
      const sourceName = expired ? g.sourceName : stalled?.batchName;
      if (!sourceName) return;
      const ss = await tx.get(batches.doc(sourceName));
      const s = ss.data();
      if (!s || s.retryBatchName || s.setComplete || s.retryStatus === 'preparation_failed' ||
          !s.locallyQueued || stalled?.stalledAt && Number(s.preparationFailureAt || 0) >= stalled.stalledAt) return;
      const message = `Listing preparation stopped before submission${g.preparationStage ? ` while ${g.preparationStage}` : ''}. Other listings can continue.`;
      tx.set(batches.doc(sourceName), { ...preparationFailurePatch(s, message, now(), timestamp),
        preparationFailureAt: stalled?.stalledAt || now() }, { merge: true });
      if (expired) tx.set(gate, { owner: null, phase: 'idle', sourceName: null, lastError: null }, { merge: true });
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
  async function reconcile(listProviderBatches, stalledPreparation = null) {
    await recoverPreparation(stalledPreparation);
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
  return { reserve, progress, failPreparation, beforeCreate, complete, release, rejected, reconcile };
}
module.exports = { admissionControl, quotaFailure, queuedName, capacityRefusals, CAPACITY_REFUSAL_LIMIT,
  neverStarted, stallRestartPending, SWEEP_CALL_LIMITS, withLimit, STALL_RESTART_MS, STALL_RESTART_MIN_MS, STALL_RESTART_LIMIT,
  VALIDATION_WAIT_MS, PREPARATION_RESERVATION_MS, PREPARATION_FAILURE_LIMIT, preparationFailurePatch, stallCutoffMs };
