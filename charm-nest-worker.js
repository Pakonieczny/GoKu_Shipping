/*  charm-nest-worker.js — one dedicated Worker per sheet.
 *  Runs CharmNestSolver.solve() off the main thread and streams every
 *  placement back as it commits, so the preview fills in piece by piece.
 *
 *  in : {type:"solve",  jobId, job}            job → see charm-nest-solver.js
 *       {type:"stop",   jobId}                  honoured at the next piece boundary
 *       {type:"packingHints", jobId, hints, pending}  live, bounded search advice
 *       {type:"verify", jobId, job, placements, res}
 *  out: {type:"stage"|"placed"|"reject"|"trial"|"best"|"done"|"verified"|"error", jobId, …}
 */
importScripts("charm-nest-rose.js?v=20260922-rehearsal", "charm-nest-solver.js?v=20260922-remnants");

let current = null;   // { jobId, job, stop }

self.onmessage = async (e) => {
  const m = e.data || {};
  if (m.type === "stop") { if (current && (!m.jobId || current.jobId === m.jobId)) current.stop = true; return; }
  if (m.type === "packingHints") {
    if (current && current.jobId === m.jobId && !current.stop) {
      current.job.packingPending = !!m.pending;
      if (m.hints) current.job.packingHints = m.hints;
    }
    return;
  }
  if (m.type === "incumbent") {
    if (current && current.jobId === m.jobId && current.job.learned && !current.stop) current.job.learned.incumbent = m.layout;
    return;
  }
  if (m.type === "verify") {
    try {
      const v = CharmNestSolver.verify(m.job, m.placements, m.res || 6);
      self.postMessage({ type: "verified", jobId: m.jobId, result: v });
    } catch (err) { self.postMessage({ type: "error", jobId: m.jobId, message: String(err && err.message || err) }); }
    return;
  }
  if (m.type !== "solve") return;
  const state = { jobId: m.jobId, job: m.job, stop: false };
  current = state;
  const post = (msg) => self.postMessage(Object.assign({ jobId: m.jobId }, msg));
  try {
    if (m.job.learned && !self.CharmNestLearned) importScripts("charm-nest-learned.js");
    const solver = m.job.learned ? CharmNestLearned : CharmNestSolver;
    const result = await solver.solve(m.job, {
      shouldStop: () => state.stop,
      onStage: (stage, done, total) => post({ type: "stage", stage, done, total }),
      onPlaced: (placement, info) => post({ type: "placed", placement, info }),
      onReject: (id, trial, reason) => post({ type: "reject", id, trial, reason }),
      onTrial: (summary) => post({ type: "trial", summary }),
      onBest: (best, summary) => post({ type: "best", best: CharmNestSolver.publicLayout(best), summary }),
      onLearned: (progress) => post({ type: "learned", progress })
    });
    if (m.job.verifyResult && !CharmNestSolver.verify(m.job, result.placements, 6).ok) throw new Error("Challenger result failed high-resolution verification; retaining the verified refinement result");
    post({ type: "done", result: CharmNestSolver.publicLayout(result) });
  } catch (err) {
    post({ type: "error", message: String(err && err.stack || err) });
  } finally {
    if (current === state) current = null;
  }
};
