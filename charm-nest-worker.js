/*  charm-nest-worker.js — one dedicated Worker per sheet.
 *  Runs CharmNestSolver.solve() off the main thread and streams every
 *  placement back as it commits, so the preview fills in piece by piece.
 *
 *  in : {type:"solve",  jobId, job}            job → see charm-nest-solver.js
 *       {type:"stop",   jobId}                  honoured at the next piece boundary
 *       {type:"verify", jobId, job, placements, res}
 *  out: {type:"stage"|"placed"|"reject"|"trial"|"best"|"done"|"verified"|"error", jobId, …}
 */
importScripts("charm-nest-solver.js");

let current = null;   // { jobId, stop }

self.onmessage = async (e) => {
  const m = e.data || {};
  if (m.type === "stop") { if (current && (!m.jobId || current.jobId === m.jobId)) current.stop = true; return; }
  if (m.type === "verify") {
    try {
      const v = CharmNestSolver.verify(m.job, m.placements, m.res || 6);
      self.postMessage({ type: "verified", jobId: m.jobId, result: v });
    } catch (err) { self.postMessage({ type: "error", jobId: m.jobId, message: String(err && err.message || err) }); }
    return;
  }
  if (m.type !== "solve") return;
  const state = { jobId: m.jobId, stop: false };
  current = state;
  const post = (msg) => self.postMessage(Object.assign({ jobId: m.jobId }, msg));
  try {
    const result = await CharmNestSolver.solve(m.job, {
      shouldStop: () => state.stop,
      onStage: (stage, done, total) => post({ type: "stage", stage, done, total }),
      onPlaced: (placement, info) => post({ type: "placed", placement, info }),
      onReject: (id, trial, reason) => post({ type: "reject", id, trial, reason }),
      onTrial: (summary) => post({ type: "trial", summary }),
      onBest: (best, summary) => post({ type: "best", best, summary })
    });
    post({ type: "done", result });
  } catch (err) {
    post({ type: "error", message: String(err && err.stack || err) });
  } finally {
    if (current === state) current = null;
  }
};
