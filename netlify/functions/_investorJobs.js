/*  netlify/functions/_investorJobs.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — operational job lineage: enqueue-once, claim-once, run-scoped
 *  leases, checkpoints, heartbeats and fail-closed completion.
 *
 *  WHY THIS MODULE EXISTS (blueprint §11.1, D-10)
 *  ------------------------------------------------------------------------
 *  The Manager Meeting runs from an 08:30 ET freeze to a 09:15 ET hard
 *  deadline: 45 minutes across at least four Netlify background invocations,
 *  each capped at 15 minutes. The legacy worker's lease could not span them:
 *  investorCycle-background minted a fresh leaseOwner per invocation, set its
 *  TTL to 16 minutes — deliberately LONGER than the function cap — and
 *  renewed only on an exact leaseOwner match. A second invocation could
 *  neither renew nor claim the first one's lease for 16 minutes, so a
 *  meeting that stalled on handoff failed coverage and, through §17.4's
 *  gate, bought nothing (D-12).
 *
 *  THE TWO LEASES, KEPT APART
 *   · The per-invocation lease (`leaseOwner`, `workerLeaseExpiresAt` on the
 *     job document) prevents two workers writing at once WITHIN a segment.
 *     It is what the legacy worker already had.
 *   · The run-scoped lease (`run_lease_<runId>`) is owned by the MEETING, not
 *     the invocation. Successive segments claim and renew it by presenting
 *     the run id and their attempt lineage. Its TTL (10 minutes) is shorter
 *     than the function cap, so a genuinely dead segment is reclaimed inside
 *     the cap instead of blocking the next segment for 16 minutes. A segment
 *     that yields releases it immediately.
 *
 *  THE NONCE, CONSUMED ATOMICALLY (§11.1)
 *   The dispatcher writes {jobId, task, targetFunction, attempt, payloadHash},
 *   creates a random nonce id, stores only its hash with state UNUSED under
 *   the attempt, and signs the exact binding. The worker's claim transaction
 *   verifies signature, expiry, payload hash and handler identity AND flips
 *   the nonce to CONSUMED while claiming that job attempt. A retry receives a
 *   new attempt and nonce; a replay is refused (409).
 *
 *  This module owns OPERATIONAL runs only (InvestorAI_Jobs). The immutable
 *  investment-decision trace lives in InvestorAI_ManagerRuns and is owned by
 *  _investorManager.js; the two reference each other by id and never share a
 *  state machine (§10.1).
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const REAL_ADMIN = require("./_investorAdmin");
const AUTH = require("./_investorAuth");
const POLICY = require("./_investorPolicy");
/* The Firestore seam. Everything that touches the admin lives inside
   createJobs(admin); the default export is bound to the real admin and
   withAdmin(fake) returns an independent instance for the deploy
   attestation, so a fake never leaks across concurrent callers. */

const SCHEMA_VERSION = "investor-jobs.v1";
const RUN_LEASE_TTL_MS = POLICY.CUTOFFS_ET.platform.runLeaseTtlSeconds * 1000;   // 10 min < 15-min cap
const SEGMENT_LEASE_TTL_MS = 14 * 60 * 1000;   // the invocation dies before its own lease lapses
const NONCE_TTL_MS = 15 * 60 * 1000;
const DISPATCH_BUDGET_MS = POLICY.CUTOFFS_ET.platform.dispatchBudgetSeconds * 1000;
const MAX_JOBS_PER_TICK = POLICY.CUTOFFS_ET.platform.maxJobsPerTick;
const MAX_ATTEMPTS = 6;
const MAX_SUMMARY_BYTES = 200000;

/* ── THE TASK VOCABULARY (§11.1) ───────────────────────────────────────── */
const TASKS = Object.freeze({
  event_ingest: {targetFunction:"investorEvents-background",category:"ingest",heavy:true,engine:"manager"},
  ingest:              { targetFunction: "investorIngest-background",    category: "ingest",    heavy: true,  engine: "manager" },
  premarket_manager:   { targetFunction: "investorManager-background",   category: "manager",   heavy: true,  engine: "manager" },
  focused_research:    { targetFunction: "investorManager-background",   category: "manager",   heavy: true,  engine: "manager" },
  portfolio_synthesis: { targetFunction: "investorManager-background",   category: "manager",   heavy: true,  engine: "manager" },
  event_revision:      { targetFunction: "investorManager-background",   category: "event",     heavy: true,  engine: "manager" },
  execute:             { targetFunction: "investorExecution-background", category: "execute",   heavy: false, engine: "manager" },
  postclose:           { targetFunction: "investorPostclose-background", category: "postclose", heavy: true,  engine: "manager" },
  archive:             { targetFunction: "investorArchive-background",   category: "postclose", heavy: true,  engine: "manager" },
  audit_export:        { targetFunction: "investorArchive-background",   category: "postclose", heavy: true,  engine: "manager" },
  /* Legacy worker tasks, dispatchable only while the legacy engine holds
     the writer epoch (dual run, §13). */
  cycle:               { targetFunction: "investorCycle-background",     category: "legacy",    heavy: true,  engine: "legacy" },
  guard:               { targetFunction: "investorCycle-background",     category: "legacy",    heavy: false, engine: "legacy" },
  evidence:            { targetFunction: "investorCycle-background",     category: "legacy",    heavy: true,  engine: "legacy" },
  legacy_archive:      { targetFunction: "investorCycle-background",     category: "legacy",    heavy: true,  engine: "legacy" },
});
/* Dispatch priority per tick (§11.1): due execute work first; then critical
   event revisions; then existing continuations; then the pre-market
   manager; then ingest; then post-close/archive. */
const DISPATCH_ORDER = Object.freeze(["execute", "event", "continuation", "manager", "ingest", "postclose", "legacy"]);
const JOB_STATUSES = Object.freeze(["queued", "running", "yielded_resumable", "complete", "failed", "dead", "dispatch_failed", "cancelled"]);
const OPEN_STATUSES = Object.freeze(["queued", "running", "yielded_resumable"]);

function sha256(v) {
  return crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(POLICY.canonical(v))).digest("hex");
}
function payloadHash(payload) { return sha256(payload || {}); }
function clean(id) { return String(id || "").replace(/[^a-zA-Z0-9_.:-]/g, "_").slice(0, 160); }
function jobDocId({ task, dedupeId }) { return `${clean(task)}__${clean(dedupeId)}`; }
function runLeaseDocId(runId) { return `run_lease_${clean(runId)}`; }
/** The meeting id: one logical run per task, account and trading date. */
function runIdFor({ task, accountId, tradingDate }) { return `${clean(task)}_${clean(accountId)}_${clean(tradingDate)}`; }
function taskFor(task) { return TASKS[task] || null; }

function createJobs(A) {
  /* ── ENQUEUE ONCE ──────────────────────────────────────────────────────── */
  /** Idempotent by (task, dedupeId). An open or complete job with the same
   *  identity is never duplicated; a failed/dead one may be re-enqueued. */
  async function enqueueOnce({ task, dedupeId, accountId, payload = {}, dueAtMs = Date.now(), priority = 100,
    runId = null, createdBy = "investorJobs", sessionDate = null }) {
    const spec = taskFor(task);
    if (!spec) throw Object.assign(new Error(`unknown task ${task}`), { code: "UNKNOWN_TASK" });
    const jobId = jobDocId({ task, dedupeId });
    const ref = A.col(A.COL.jobs).doc(jobId);
    const hash = payloadHash(payload);
    return A.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      if (snap.exists) {
        const j = snap.data();
        if (OPEN_STATUSES.includes(j.status) || j.status === "complete") {
          return { enqueued: false, duplicate: true, jobId, status: j.status };
        }
      }
      tx.set(ref, {
        schemaVersion: SCHEMA_VERSION, jobId, task, dedupeId: String(dedupeId), accountId: accountId || null,
        targetFunction: spec.targetFunction, category: spec.category, heavy: spec.heavy === true,
        runId: runId || null, sessionDate: sessionDate || null,
        payload, payloadHash: hash, priority: Number(priority) || 100,
        status: "queued", attempts: 0, segments: 0,
        dueAtMs: Number(dueAtMs) || Date.now(), enqueuedAtMs: Date.now(),
        checkpoint: null, lastError: null,
        ...A.envelope({ created_by: createdBy }),
      }, { merge: true });
      return { enqueued: true, jobId, status: "queued" };
    });
  }

  /* ── DUE WORK ──────────────────────────────────────────────────────────── */
  /** Every open job whose due time has passed, ordered for dispatch. Two
   *  bounded queries (queued, yielded_resumable) rather than an `in` query
   *  so no composite index is load-bearing here. */
  async function dueJobs({ nowMs = Date.now(), limit = 50, engine = null } = {}) {
    const rows = [];
    for (const status of ["queued", "yielded_resumable"]) {
      const snap = await A.col(A.COL.jobs).where("status", "==", status).limit(limit).get();
      snap.forEach((d) => {
        const j = d.data();
        if (!j || !TASKS[j.task]) return;
        if (engine && TASKS[j.task].engine !== engine) return;
        if (Number(j.dueAtMs) > nowMs) return;
        if (j.status === "yielded_resumable" && Number(j.resumeAtMs) > nowMs) return;
        rows.push(j);
      });
    }
    return orderForDispatch(rows);
  }
  /** PURE. Order: dispatch category, then continuations ahead of fresh work
   *  inside a category, then priority, then due time, then job id. */
  function orderForDispatch(jobs) {
    const rank = (j) => {
      const spec = TASKS[j.task] || { category: "legacy" };
      const cat = j.status === "yielded_resumable" && spec.category !== "execute" && spec.category !== "event"
        ? "continuation" : spec.category;
      return DISPATCH_ORDER.indexOf(cat) < 0 ? 99 : DISPATCH_ORDER.indexOf(cat);
    };
    return [...jobs].sort((a, b) => rank(a) - rank(b)
      || (Number(a.priority) || 100) - (Number(b.priority) || 100)
      || (Number(a.dueAtMs) || 0) - (Number(b.dueAtMs) || 0)
      || String(a.jobId).localeCompare(String(b.jobId)));
  }
  /** PURE. Choose what one tick launches: at most `maxJobs`, at most one heavy
   *  job per category, and never past the dispatch budget. */
  function dispatchPlan(jobs, { nowMs = Date.now(), startedAtMs = nowMs, budgetMs = DISPATCH_BUDGET_MS,
    maxJobs = MAX_JOBS_PER_TICK, perDispatchMs = 2500 } = {}) {
    const ordered = orderForDispatch(jobs);
    const chosen = [], deferred = [], heavySeen = new Set();
    let projected = nowMs - startedAtMs;
    for (const j of ordered) {
      const spec = TASKS[j.task] || { category: "legacy", heavy: true };
      const continuation = j.status === "yielded_resumable";
      if (chosen.length >= maxJobs) { deferred.push({ jobId: j.jobId, reason: "tick_full" }); continue; }
      if (projected + perDispatchMs > budgetMs) { deferred.push({ jobId: j.jobId, reason: "dispatch_budget" }); continue; }
      /* "At most one NEW heavy job per category per tick": a resumable
         continuation is existing work and does not take the slot. */
      if (spec.heavy && !continuation && heavySeen.has(spec.category)) { deferred.push({ jobId: j.jobId, reason: "one_heavy_per_category" }); continue; }
      if (spec.heavy && !continuation) heavySeen.add(spec.category);
      chosen.push(j);
      projected += perDispatchMs;
    }
    return { chosen, deferred };
  }

  /* ── NONCES: issued by the dispatcher, consumed atomically by the worker ─ */
  /** Sign the exact binding and store only the token hash, UNUSED, under the
   *  attempt. Returns the token to send in the invocation body. */
  async function issueWorkerNonce({ jobId, task, targetFunction, attempt, payloadHash: hash, ttlMs = NONCE_TTL_MS, key = null }) {
    const nonceId = crypto.randomBytes(16).toString("hex");
    const expiresAtMs = Date.now() + ttlMs;
    const token = AUTH.mintBoundWorkerNonce({ jobId, task, targetFunction, attempt, payloadHash: hash, nonceId, expiresAtMs }, key ? { key } : {});
    if (!token) throw Object.assign(new Error("worker signing key unavailable"), { code: "SIGNING_KEY_UNAVAILABLE" });
    await A.col(A.COL.workerNonces).doc(nonceId).set({
      schemaVersion: SCHEMA_VERSION, nonceId, jobId, task, targetFunction, attempt: Number(attempt) || 1,
      payloadHash: hash, tokenHash: sha256(token), status: "UNUSED", expiresAtMs,
      issuedAtMs: Date.now(), consumedAtMs: null, consumedBy: null,
      ...A.envelope({ created_by: "investorJobs.issueWorkerNonce" }),
    });
    return { nonceId, token, expiresAtMs, attempt: Number(attempt) || 1 };
  }

  /** The worker's claim. Verifies the signed binding, consumes the nonce and
   *  claims the job attempt in ONE transaction. A replay is refused. */
  async function claimOnce({ jobId, task, targetFunction, token, payload = null, leaseTtlMs = SEGMENT_LEASE_TTL_MS, key = null }) {
    const binding = AUTH.verifyBoundWorkerNonce(token, targetFunction, key ? { key } : {});
    if (!binding) return { claimed: false, httpStatus: 403, reason: "invalid_or_expired_nonce" };
    if (binding.jobId !== jobId || binding.task !== task) return { claimed: false, httpStatus: 403, reason: "binding_mismatch" };
    if (payload !== null && payloadHash(payload) !== binding.payloadHash) return { claimed: false, httpStatus: 403, reason: "payload_hash_mismatch" };
    const jobRef = A.col(A.COL.jobs).doc(jobId);
    const nonceRef = A.col(A.COL.workerNonces).doc(binding.nonceId);
    const leaseOwner = `${process.env.AWS_LAMBDA_LOG_STREAM_NAME || "worker"}:${Date.now()}:${crypto.randomBytes(3).toString("hex")}`;
    const nowMs = Date.now();
    const out = await A.runTransaction(async (tx) => {
      const [n, j] = await Promise.all([tx.get(nonceRef), tx.get(jobRef)]);
      if (!n.exists) return { claimed: false, httpStatus: 403, reason: "nonce_missing" };
      const nonce = n.data();
      if (nonce.status !== "UNUSED") return { claimed: false, httpStatus: 409, reason: "nonce_consumed", consumedBy: nonce.consumedBy || null };
      if (Number(nonce.expiresAtMs) < nowMs) return { claimed: false, httpStatus: 403, reason: "nonce_expired" };
      if (nonce.tokenHash !== sha256(token) || nonce.jobId !== jobId || nonce.task !== task
          || nonce.targetFunction !== targetFunction || Number(nonce.attempt) !== Number(binding.attempt)) {
        return { claimed: false, httpStatus: 403, reason: "nonce_binding_mismatch" };
      }
      if (!j.exists) return { claimed: false, httpStatus: 404, reason: "job_missing" };
      const job = j.data();
      if (job.task !== task) return { claimed: false, httpStatus: 409, reason: "task_mismatch" };
      if (job.status === "complete") return { claimed: false, httpStatus: 200, reason: "job_complete", duplicate: true };
      if (["failed", "dead", "cancelled"].includes(job.status)) return { claimed: false, httpStatus: 409, reason: `job_${job.status}` };
      if (job.status === "running" && Number(job.workerLeaseExpiresAt) > nowMs) {
        return { claimed: false, httpStatus: 409, reason: "job_in_flight", leaseOwner: job.leaseOwner || null };
      }
      tx.set(nonceRef, { status: "CONSUMED", consumedAtMs: nowMs, consumedBy: leaseOwner }, { merge: true });
      tx.set(jobRef, { status: "running", leaseOwner, workerLeaseExpiresAt: nowMs + leaseTtlMs,
        attempt: Number(binding.attempt), attempts: A.FV.increment(1), segments: A.FV.increment(1),
        startedAtMs: job.startedAtMs || nowMs, segmentStartedAtMs: nowMs, lastHeartbeatAtMs: nowMs,
        nonceId: binding.nonceId }, { merge: true });
      return { claimed: true, httpStatus: 200, job: { ...job, status: "running", attempt: Number(binding.attempt) } };
    });
    if (!out.claimed) return out;
    return { ...out, claim: { jobId, jobRef, task, leaseOwner, leaseTtlMs, attempt: Number(binding.attempt),
      runId: out.job.runId || null, runLease: null, payload: out.job.payload || {}, checkpoint: out.job.checkpoint || null,
      startedAtMs: nowMs } };
  }

  /* ── THE RUN-SCOPED LEASE (D-10) ───────────────────────────────────────── */
  /** Claim the meeting's lease for this segment. Allowed when the lease is
   *  absent, expired, released, or already held by this same segment. A live
   *  lease held by ANOTHER segment refuses — but its TTL is shorter than the
   *  function cap, so a dead segment never blocks the next one for long. */
  async function claimRunLease(claim, { runId = claim.runId, ttlMs = RUN_LEASE_TTL_MS, nowMs = Date.now() } = {}) {
    if (!runId) throw Object.assign(new Error("runId required for a run lease"), { code: "RUN_ID_REQUIRED" });
    const ref = A.col(A.COL.jobs).doc(runLeaseDocId(runId));
    const segmentOwner = `${claim.jobId}#${claim.attempt}`;
    const out = await A.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      const cur = snap.exists ? snap.data() : null;
      const live = cur && Number(cur.leaseExpiresAt) > nowMs && cur.released !== true;
      if (live && cur.segmentOwner !== segmentOwner) {
        return { claimed: false, reason: "run_in_flight", heldBy: cur.segmentOwner, until: cur.leaseExpiresAt };
      }
      const lineage = [...((cur && cur.lineage) || []), { jobId: claim.jobId, attempt: claim.attempt, claimedAtMs: nowMs,
        reclaimedFrom: cur && cur.segmentOwner !== segmentOwner ? cur.segmentOwner || null : null }].slice(-50);
      tx.set(ref, { schemaVersion: SCHEMA_VERSION, kind: "run_lease", runId, segmentOwner, jobId: claim.jobId,
        attempt: claim.attempt, leaseExpiresAt: nowMs + ttlMs, ttlMs, claimedAtMs: nowMs, released: false,
        releasedAtMs: null, segmentsSeen: (cur && cur.segmentsSeen || 0) + (cur && cur.segmentOwner === segmentOwner ? 0 : 1),
        lineage, firstClaimedAtMs: cur && cur.firstClaimedAtMs || nowMs }, { merge: true });
      return { claimed: true, segmentOwner, until: nowMs + ttlMs, reclaimed: !!(cur && cur.segmentOwner && cur.segmentOwner !== segmentOwner) };
    });
    if (out.claimed) claim.runLease = { ref, runId, segmentOwner, ttlMs };
    return out;
  }
  async function renewRunLease(claim) {
    const lease = claim && claim.runLease;
    if (!lease) return { renewed: false, reason: "no run lease" };
    const nowMs = Date.now();
    return A.runTransaction(async (tx) => {
      const snap = await tx.get(lease.ref);
      if (!snap.exists || snap.data().segmentOwner !== lease.segmentOwner || snap.data().released === true) {
        return { renewed: false, reason: "lease not owned" };
      }
      tx.set(lease.ref, { leaseExpiresAt: nowMs + lease.ttlMs, lastHeartbeatAtMs: nowMs }, { merge: true });
      return { renewed: true, until: nowMs + lease.ttlMs };
    });
  }
  async function releaseRunLease(claim, { reason = "released" } = {}) {
    const lease = claim && claim.runLease;
    if (!lease) return { released: false, reason: "no run lease" };
    const out = await A.runTransaction(async (tx) => {
      const snap = await tx.get(lease.ref);
      if (!snap.exists || snap.data().segmentOwner !== lease.segmentOwner) return { released: false, reason: "not owner" };
      tx.set(lease.ref, { released: true, releasedAtMs: Date.now(), releaseReason: reason, leaseExpiresAt: 0 }, { merge: true });
      return { released: true };
    });
    claim.runLease = null;
    return out;
  }

  /* ── CHECKPOINT / HEARTBEAT / COMPLETE / FAIL / YIELD ──────────────────── */
  function ownedUpdate(claim, patch) {
    const nowMs = Date.now();
    return A.runTransaction(async (tx) => {
      const snap = await tx.get(claim.jobRef);
      if (!snap.exists || snap.data().leaseOwner !== claim.leaseOwner || snap.data().status !== "running") {
        return { ok: false, reason: "segment lease lost" };
      }
      tx.set(claim.jobRef, { ...patch, lastHeartbeatAtMs: nowMs,
        workerLeaseExpiresAt: nowMs + claim.leaseTtlMs }, { merge: true });
      return { ok: true };
    });
  }
  /** Durable progress: the cursor the next segment resumes from. Every
   *  checkpoint also renews both leases. */
  async function checkpoint(claim, { stage, cursor = null, data = null, progress = null }) {
    const out = await ownedUpdate(claim, { checkpoint: { stage: String(stage), cursor, data, progress, atMs: Date.now() } });
    if (out.ok) { claim.checkpoint = { stage, cursor, data, progress }; await renewRunLease(claim).catch(() => ({})); }
    return out;
  }
  async function heartbeat(claim) {
    const out = await ownedUpdate(claim, {});
    if (out.ok) await renewRunLease(claim).catch(() => ({}));
    return out;
  }
  function boundedSummary(summary) {
    const text = JSON.stringify(summary || {});
    if (Buffer.byteLength(text, "utf8") <= MAX_SUMMARY_BYTES) return { summary: summary || {}, summaryTruncated: false };
    return { summary: { note: "summary exceeded the document budget; see the content store manifest",
      summaryHash: sha256(text), bytes: Buffer.byteLength(text, "utf8") }, summaryTruncated: true };
  }
  async function complete(claim, summary = {}) {
    const bounded = boundedSummary(summary);
    const out = await ownedUpdate(claim, { status: "complete", finishedAtMs: Date.now(), workerLeaseExpiresAt: 0,
      ...bounded, lastError: null });
    await releaseRunLease(claim, { reason: "complete" }).catch(() => ({}));
    return { ...out, status: out.ok ? "complete" : "lease_lost" };
  }
  /** Fail CLOSED: the job is failed with a typed error; a retryable failure
   *  is re-queued with exponential backoff up to MAX_ATTEMPTS, after which it
   *  is dead. Nothing here ever produces new risk. */
  async function failClosed(claim, { code = "JOB_FAILED", message = "", retryable = false, data = null } = {}) {
    const attempt = Number(claim.attempt) || 1;
    const exhausted = attempt >= MAX_ATTEMPTS;
    const backoffMs = Math.min(30 * 60000, 60000 * Math.pow(2, Math.max(0, attempt - 1)));
    const status = retryable && !exhausted ? "yielded_resumable" : (retryable ? "dead" : "failed");
    const out = await ownedUpdate(claim, { status, finishedAtMs: Date.now(), workerLeaseExpiresAt: 0,
      resumeAtMs: status === "yielded_resumable" ? Date.now() + backoffMs : null,
      lastError: { code: String(code), message: String(message || "").slice(0, 300), retryable: !!retryable,
        attempt, exhausted, data: data || null, atMs: Date.now() } });
    await releaseRunLease(claim, { reason: `fail:${code}` }).catch(() => ({}));
    return { ...out, status, backoffMs: status === "yielded_resumable" ? backoffMs : null };
  }
  /** A segment that ran out of invocation time yields with its checkpoint;
   *  the next kick re-dispatches it with a new attempt and nonce, and the
   *  next segment claims the run lease immediately. */
  async function yieldSegment(claim, { reason = "segment_budget", resumeAtMs = Date.now() + 5000, checkpoint: cp = null } = {}) {
    const out = await ownedUpdate(claim, { status: "yielded_resumable", resumeAtMs, yieldedAtMs: Date.now(),
      yieldReason: String(reason), workerLeaseExpiresAt: 0,
      ...(cp ? { checkpoint: { ...cp, atMs: Date.now() } } : {}) });
    await releaseRunLease(claim, { reason: `yield:${reason}` }).catch(() => ({}));
    return { ...out, status: "yielded_resumable" };
  }
  /** Has this segment enough invocation time left for one more unit of work? */
  function segmentBudgetRemainingMs(claim, { capMs = POLICY.CUTOFFS_ET.platform.functionCapSeconds * 1000, safetyMs = 45000 } = {}) {
    return capMs - safetyMs - (Date.now() - (claim.startedAtMs || Date.now()));
  }

  /* ── MANAGER PACKET PRIORITY (Appendix B, routeEvidenceEvent) ──────────── */
  async function prioritizeNextManagerPacket({ symbol, deltaId, reason = "routine_delta_with_active_mandate" }) {
    const key = clean(symbol);
    await A.col(A.COL.control).doc("control").set({
      managerPacketPriorities: { [key]: { deltaId: deltaId || null, reason, atMs: Date.now() } },
    }, { merge: true });
    return { symbol: key, deltaId: deltaId || null };
  }

  /* ── OPERATOR/DIAGNOSTIC READS ─────────────────────────────────────────── */
  async function readJob(jobId) {
    const snap = await A.col(A.COL.jobs).doc(jobId).get();
    return snap.exists ? snap.data() : null;
  }
  async function readRunLease(runId) {
    const snap = await A.col(A.COL.jobs).doc(runLeaseDocId(runId)).get();
    return snap.exists ? snap.data() : null;
  }


  return {
    enqueueOnce, dueJobs, orderForDispatch, dispatchPlan,
    issueWorkerNonce, claimOnce,
    claimRunLease, renewRunLease, releaseRunLease,
    checkpoint, heartbeat, complete, failClosed, yieldSegment, segmentBudgetRemainingMs,
    prioritizeNextManagerPacket, readJob, readRunLease,
  };
}
const DEFAULT = createJobs(REAL_ADMIN);

module.exports = {
  SCHEMA_VERSION, TASKS, DISPATCH_ORDER, JOB_STATUSES, OPEN_STATUSES,
  RUN_LEASE_TTL_MS, SEGMENT_LEASE_TTL_MS, NONCE_TTL_MS, DISPATCH_BUDGET_MS, MAX_JOBS_PER_TICK, MAX_ATTEMPTS,
  sha256, payloadHash, jobDocId, runLeaseDocId, runIdFor, taskFor,
  ...DEFAULT,
  createJobs,
  /** An independent instance bound to another admin (an in-memory fake for
   *  the deploy attestation). The default instance is never rebound. */
  withAdmin: (admin) => createJobs(admin || REAL_ADMIN),
};
