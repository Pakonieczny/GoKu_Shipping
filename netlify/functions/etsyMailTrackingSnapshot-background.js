/* netlify/functions/etsyMailTrackingSnapshot-background.js
 *
 * Background worker for the tracking-image feature.
 *
 * Netlify background functions:
 *   - 15-min execution limit (vs. 10-sec sync cap)
 *   - Always return 202 immediately to the caller
 *   - The client gets no response body; communication happens via
 *     side effects (in our case, Firestore job-status doc)
 *
 * Invocation pattern:
 *   1. Caller POSTs { trackingCode, jobId, forceRefresh? } to this endpoint
 *   2. Netlify immediately responds 202, queues the function
 *   3. Caller persists the jobId and polls Firestore for status updates
 *   4. This function does: fetch carrier data → render SVG → upload PNG
 *      → updates the job doc to status: "ready" (or "failed")
 *
 * The filename MUST end in "-background" — that's how Netlify identifies
 * background functions.
 *
 * Firestore job doc shape (at EtsyMail_TrackingJobs/{jobId}):
 *   {
 *     status     : "pending" | "running" | "ready" | "failed",
 *     trackingCode,
 *     startedAt  : Timestamp,
 *     finishedAt : Timestamp | null,
 *     error      : string | null,
 *     errorCode  : string | null,
 *     carrier, carrierDisplay, statusText, statusKey,
 *     imageUrl, imageStoragePath, imageWidth, imageHeight,
 *     events     : [...] (capped to 20)
 *   }
 */

const admin         = require("./firebaseAdmin");
const { snapshot }  = require("./_etsyMailTracking");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const JOBS_COLL = "EtsyMail_TrackingJobs";

async function updateJob(jobId, patch) {
  try {
    await db.collection(JOBS_COLL).doc(jobId).set({
      ...patch,
      updatedAt: FV.serverTimestamp()
    }, { merge: true });
  } catch (e) {
    console.error(`[tracking-bg] Failed to update job ${jobId}:`, e.message);
  }
}

exports.handler = async (event) => {
  // Background funcs always return 202 to the caller; we don't return meaningful
  // status codes. But we still need to parse the payload.
  let body;
  try { body = JSON.parse(event.body || "{}"); }
  catch {
    console.error("[tracking-bg] Invalid JSON body");
    return { statusCode: 202 };
  }

  const trackingCode = String(body.trackingCode || "").trim();
  const jobId        = String(body.jobId || "").trim();
  const forceRefresh = Boolean(body.forceRefresh);
  const carrierHint  = String(body.carrierHint || "").trim().toLowerCase();

  if (!trackingCode || !jobId) {
    console.error(`[tracking-bg] Missing trackingCode or jobId. trackingCode=${trackingCode} jobId=${jobId}`);
    // Nothing to update since we don't have a jobId — just log and exit
    return { statusCode: 202 };
  }

  console.log(`[tracking-bg] Starting job ${jobId} for ${trackingCode}`);

  await updateJob(jobId, {
    status      : "running",
    trackingCode,
    startedAt   : FV.serverTimestamp(),
    error       : null,
    errorCode   : null
  });

  try {
    const result = await snapshot(trackingCode, { forceRefresh, carrierHint });

    await updateJob(jobId, {
      status           : "ready",
      trackingCode     : result.trackingCode,
      carrier          : result.carrier,
      carrierDisplay   : result.carrierDisplay,
      statusText       : result.status,
      statusKey        : result.statusKey,
      estimatedDelivery: result.estimatedDelivery || null,
      destination      : result.destination || null,
      origin           : result.origin || null,
      shipDate         : result.shipDate || null,
      resolvedAt       : result.resolvedAt || null,
      events           : (result.events || []).slice(0, 20),
      imageUrl         : result.imageUrl,
      imageStoragePath : result.imageStoragePath,
      imageWidth       : result.imageWidth,
      imageHeight      : result.imageHeight,
      cached           : result.cached,
      durationMs       : result.durationMs,
      finishedAt       : FV.serverTimestamp()
    });

    console.log(`[tracking-bg] Job ${jobId} ready (${result.durationMs}ms, cached=${result.cached})`);

  } catch (e) {
    console.error(`[tracking-bg] Job ${jobId} failed:`, e.code, e.message);
    await updateJob(jobId, {
      status    : "failed",
      error     : e.message || "Unknown error",
      errorCode : e.code || "INTERNAL",
      finishedAt: FV.serverTimestamp()
    });
  }

  return { statusCode: 202 };
};
