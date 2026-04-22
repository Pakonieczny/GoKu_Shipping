/*  netlify/functions/etsyMailJobs.js
 *
 *  Job queue interface for the Chrome extension.
 *
 *  Ops:
 *    POST { op: "claim", workerId, jobTypes?: ["scrape", ...] }
 *      Atomically claims the oldest queued job (of requested types, if supplied).
 *      Returns { job } or { job: null }.
 *    POST { op: "complete", jobId, workerId, result? }
 *      Marks job succeeded. Records result payload.
 *    POST { op: "fail",     jobId, workerId, error, retry?: true }
 *      Marks job failed. If retry=true AND attempts < 3, requeues it.
 *    POST { op: "heartbeat", jobId, workerId }
 *      (Optional) extends claim; not required for M2 but here for long scrapes.
 *
 *  All requests require X-EtsyMail-Secret.
 *
 *  Job model lives in EtsyMail_Jobs (see FIRESTORE_SCHEMA.md).
 */

const admin = require("./firebaseAdmin");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");
const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const JOBS_COLL  = "EtsyMail_Jobs";
const AUDIT_COLL = "EtsyMail_Audit";
const MAX_ATTEMPTS = 3;

function json(statusCode, body) { return { statusCode, headers: CORS, body: JSON.stringify(body) }; }
function bad(msg, code = 400)    { return json(code, { error: msg }); }

async function audit(threadId, eventType, actor, payload) {
  await db.collection(AUDIT_COLL).add({
    threadId: threadId || null, draftId: null, eventType,
    actor, payload: payload || {},
    createdAt: FV.serverTimestamp()
  });
}

async function claimNextJob(workerId, jobTypes) {
  // Fetch a small set of candidate queued jobs (oldest first), then
  // atomically claim the first one still queued in a transaction.
  let q = db.collection(JOBS_COLL).where("status", "==", "queued");
  if (Array.isArray(jobTypes) && jobTypes.length) {
    // Firestore 'in' supports up to 30 values — fine for our needs.
    q = q.where("jobType", "in", jobTypes);
  }
  q = q.orderBy("createdAt", "asc").limit(5);

  const candidates = await q.get();
  if (candidates.empty) return null;

  for (const docSnap of candidates.docs) {
    const ref = docSnap.ref;
    try {
      const claimed = await db.runTransaction(async (tx) => {
        const fresh = await tx.get(ref);
        if (!fresh.exists) return null;
        const data = fresh.data();
        if (data.status !== "queued") return null;  // somebody else got it

        tx.update(ref, {
          status    : "claimed",
          claimedBy : workerId,
          claimedAt : FV.serverTimestamp(),
          attempts  : (data.attempts || 0) + 1,
          updatedAt : FV.serverTimestamp()
        });
        return { id: ref.id, ...data, status: "claimed", claimedBy: workerId, attempts: (data.attempts || 0) + 1 };
      });
      if (claimed) return claimed;
    } catch (e) {
      console.warn("claim contention on", ref.id, e.message);
      // try the next candidate
    }
  }
  return null;
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event.httpMethod !== "POST")     return json(405, { error: "Method Not Allowed" });

  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON"); }

  const { op } = body;
  if (!op) return bad("Missing op");

  try {
    /* ── claim ── */
    if (op === "claim") {
      const { workerId, jobTypes = null } = body;
      if (!workerId) return bad("Missing workerId");
      const job = await claimNextJob(workerId, jobTypes);
      if (!job) return json(200, { success: true, job: null });

      await audit(job.threadId, "job_claimed", `worker:${workerId}`, {
        jobId: job.id, jobType: job.jobType, attempt: job.attempts
      });
      return json(200, { success: true, job });
    }

    /* ── complete ── */
    if (op === "complete") {
      const { jobId, workerId, result = null } = body;
      if (!jobId || !workerId) return bad("Missing jobId or workerId");

      const ref = db.collection(JOBS_COLL).doc(jobId);
      const snap = await ref.get();
      if (!snap.exists) return json(404, { error: "Job not found" });
      const data = snap.data();
      if (data.claimedBy && data.claimedBy !== workerId) {
        return json(409, { error: "Job claimed by different worker", claimedBy: data.claimedBy });
      }

      await ref.set({
        status    : "succeeded",
        result    : result || null,
        updatedAt : FV.serverTimestamp()
      }, { merge: true });
      await audit(data.threadId, "job_completed", `worker:${workerId}`, {
        jobId, jobType: data.jobType
      });
      return json(200, { success: true });
    }

    /* ── fail ── */
    if (op === "fail") {
      const { jobId, workerId, error = "unknown", retry = false } = body;
      if (!jobId || !workerId) return bad("Missing jobId or workerId");

      const ref = db.collection(JOBS_COLL).doc(jobId);
      const snap = await ref.get();
      if (!snap.exists) return json(404, { error: "Job not found" });
      const data = snap.data();
      const attempts = data.attempts || 0;

      const willRetry = retry && attempts < MAX_ATTEMPTS;
      const patch = {
        lastError : String(error).slice(0, 500),
        updatedAt : FV.serverTimestamp()
      };
      if (willRetry) {
        patch.status = "queued";
        patch.claimedBy = null;
        patch.claimedAt = null;
      } else {
        patch.status = "failed";
      }
      await ref.set(patch, { merge: true });

      await audit(data.threadId, willRetry ? "job_requeued" : "job_failed", `worker:${workerId}`, {
        jobId, jobType: data.jobType, attempts, error: String(error).slice(0, 500)
      });
      return json(200, { success: true, requeued: willRetry, attempts });
    }

    /* ── heartbeat ── */
    if (op === "heartbeat") {
      const { jobId, workerId } = body;
      if (!jobId || !workerId) return bad("Missing jobId or workerId");
      await db.collection(JOBS_COLL).doc(jobId).set({
        lastHeartbeatAt: FV.serverTimestamp(),
        updatedAt      : FV.serverTimestamp()
      }, { merge: true });
      return json(200, { success: true });
    }

    return bad(`Unknown op '${op}'`);

  } catch (err) {
    console.error("etsyMailJobs error:", err);
    return json(500, { error: err.message || String(err) });
  }
};
