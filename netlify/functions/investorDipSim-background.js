/*  netlify/functions/investorDipSim-background.js
 *  Runs one dip-reversal historical simulation forward for up to ~11 minutes
 *  per job, then re-enqueues itself until every date is replayed. Invoked
 *  by the job dispatcher, never scheduled. No model gateway.
 */
"use strict";
const preloadedModules = new Set(Object.keys(require.cache));
const A = require("./_investorAdmin");
const AUTH = require("./_investorAuth");
const JOBS = require("./_investorJobs");
const SIM = require("./_investorDipSim");
const { redact } = require("./_investorAuth");
if (Object.keys(require.cache).some((k) => !preloadedModules.has(k) && /_investorOpenai\.js$/.test(k))) throw new Error("the dip simulation must not load the model gateway");
const FN_NAME = "investorDipSim-background";
const TASK = "dip_simulation";

exports.handler = async (event) => {
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { return { statusCode: 400, body: JSON.stringify({ error: "invalid JSON" }) }; }
  const { jobId, task, nonce, payload = {} } = body;
  if (task !== TASK || !jobId || !payload.simId) return { statusCode: 400, body: JSON.stringify({ error: "invalid job shape" }) };
  await AUTH.loadAuthSecrets();
  const claimed = await JOBS.claimOnce({ jobId, task: TASK, targetFunction: FN_NAME, token: nonce, payload });
  if (!claimed.claimed) return { statusCode: claimed.httpStatus || 409, body: JSON.stringify({ ok: false, reason: claimed.reason }) };
  const claim = claimed.claim;
  try {
    const doc = await SIM.advance(A, payload.simId, { budgetMs: 11 * 60000 });
    if (doc.status === "running") {
      const next = await JOBS.enqueueOnce({ task: TASK, dedupeId: `${payload.simId}_${doc.cursor}`, accountId: payload.accountId || null, priority: 40, payload: { simId: payload.simId, accountId: payload.accountId || null, segment: doc.cursor }, createdBy: FN_NAME });
      try { const K = require("./investorKick"); if (K.dispatchJob && next && next.jobId) { const jref = await A.col(A.COL.jobs).doc(next.jobId).get(); if (jref.exists) await K.dispatchJob(jref.data()); } } catch (e) { /* the kick picks it up within a minute */ }
    }
    await JOBS.complete(claim, { simId: payload.simId, status: doc.status, cursor: doc.cursor, days: (doc.dates || []).length });
    return { statusCode: 200, body: JSON.stringify({ ok: true, status: doc.status, cursor: doc.cursor }) };
  } catch (e) {
    console.error("dip simulation failed", redact({ jobId, error: e.message, stack: (e.stack || "").slice(0, 300) }));
    await JOBS.failClosed(claim, { code: e.code || "DIP_SIM_FAILED", message: e.message, retryable: false }).catch(() => ({}));
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(e.message).slice(0, 200) }) };
  }
};
exports.FN_NAME = FN_NAME;
