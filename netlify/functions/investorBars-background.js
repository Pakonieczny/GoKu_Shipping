/*  netlify/functions/investorBars-background.js
 *  Builds the historical price library in ~11-minute segments, newest month
 *  first, every company in the universe, and re-enqueues itself until done.
 *  Invoked by the job dispatcher; never scheduled; no model gateway.
 */
"use strict";
const preloadedModules = new Set(Object.keys(require.cache));
const A = require("./_investorAdmin");
const AUTH = require("./_investorAuth");
const JOBS = require("./_investorJobs");
const BARS = require("./_investorBarStore");
const { redact } = require("./_investorAuth");
if (Object.keys(require.cache).some((k) => !preloadedModules.has(k) && /_investorOpenai\.js$/.test(k))) throw new Error("the price library builder must not load the model gateway");
const FN_NAME = "investorBars-background";
const TASK = "bar_repository";

exports.handler = async (event) => {
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { return { statusCode: 400, body: JSON.stringify({ error: "invalid JSON" }) }; }
  const { jobId, task, nonce, payload = {} } = body;
  if (task !== TASK || !jobId) return { statusCode: 400, body: JSON.stringify({ error: "invalid job shape" }) };
  await AUTH.loadAuthSecrets();
  const claimed = await JOBS.claimOnce({ jobId, task: TASK, targetFunction: FN_NAME, token: nonce, payload });
  if (!claimed.claimed) return { statusCode: claimed.httpStatus || 409, body: JSON.stringify({ ok: false, reason: claimed.reason }) };
  const claim = claimed.claim;
  try {
    const out = await BARS.buildSegment(A, { budgetMs: 11 * 60000 });
    if (out.more) {
      const seg = (Number(payload.segment) || 0) + 1;
      const next = await JOBS.enqueueOnce({ task: TASK, dedupeId: `library_${seg}_${Math.floor(Date.now() / 60000)}`, accountId: payload.accountId || null, priority: 60, payload: { accountId: payload.accountId || null, segment: seg }, createdBy: FN_NAME });
      try { const K = require("./investorKick"); const jref = await A.col(A.COL.jobs).doc(next.jobId).get(); if (K.dispatchJob && jref.exists) await K.dispatchJob(jref.data()); } catch (e) { /* the kick dispatches it within a minute */ }
    }
    await JOBS.complete(claim, out);
    return { statusCode: 200, body: JSON.stringify({ ok: true, ...out }) };
  } catch (e) {
    console.error("price library segment failed", redact({ jobId, error: e.message, stack: (e.stack || "").slice(0, 300) }));
    await BARS.setStatus(A, { status: "failed", note: String(e.message).slice(0, 200) }).catch(() => {});
    await JOBS.failClosed(claim, { code: e.code || "BAR_LIBRARY_FAILED", message: e.message, retryable: true }).catch(() => ({}));
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(e.message).slice(0, 200) }) };
  }
};
exports.FN_NAME = FN_NAME;
