/*  netlify/functions/charmNestSolve-background.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  Server-side fallback for the Charm Nesting Station's solver. Same
 *  algorithm, same seed, same result as the browser Worker — it is the same
 *  file (_charmNestSolver.js → charm-nest-solver.js). Used for operators on
 *  weak hardware or sheets above the configurable piece-count threshold.
 *
 *  A -background function gets Netlify's 15-minute budget and cannot stream,
 *  so it writes progress to Charm_Nest_Jobs/{id} every few placements and the
 *  page polls charmNestLibrary op=getJob. It honours stopRequested on the
 *  same document.
 *
 *  Kicked by charmNestLibrary op=startJob with {id, job}; pieces arrive with
 *  their silhouette bits base64-encoded.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const admin = require("./firebaseAdmin");
const Solver = require("./_charmNestSolver");
const { parseBody } = require("./_charmNestAuth");
const db = admin.firestore();
const FV = admin.firestore.FieldValue;
const JOBS = "Charm_Nest_Jobs";
const MAX_BUDGET_MS = 13 * 60 * 1000;         // leave margin under the 15-minute cap

exports.handler = async (event) => {
  const body = parseBody(event);
  const id = String(body.id || "").replace(/[^\w\-]/g, "").slice(0, 80);
  const job = body.job;
  if (!id || !job || !Array.isArray(job.pieces)) { console.warn("[charmNestSolve] bad payload"); return { statusCode: 400, body: "bad payload" }; }
  const ref = db.collection(JOBS).doc(id);
  const snap = await ref.get();
  if (!snap.exists) { console.warn("[charmNestSolve] unknown job", id); return { statusCode: 404, body: "unknown job" }; }
  if (snap.data().status === "running" || snap.data().status === "done") return { statusCode: 200, body: "already handled" };
  await ref.set({ status: "running", startedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, { merge: true });

  const pieces = job.pieces.map(p => {
    const raw = new Uint8Array(Buffer.from(String(p.bits || ""), "base64"));
    const n = (p.w | 0) * (p.h | 0);
    const bits = p.packed ? unpackBits(raw, n) : raw;
    return { id: String(p.id), order: p.order || p.id, orderDate: +p.orderDate || 0, w: p.w | 0, h: p.h | 0, scale: +p.scale || 6, bits, areaPt2: +p.areaPt2 || 0, pinned: p.pinned || null };
  }).filter(p => p.w > 0 && p.h > 0 && p.bits.length === p.w * p.h);
  const solverJob = Object.assign({}, job, { pieces, timeBudgetMs: Math.min(+job.timeBudgetMs || 180000, MAX_BUDGET_MS) });

  let stop = false, lastWrite = 0, lastCheck = 0, best = null, trials = 0;
  const write = async (extra) => {
    lastWrite = Date.now();
    await ref.set(Object.assign({ status: "running", trials, updatedAt: FV.serverTimestamp() }, best ? { best: slimBest(best) } : {}, extra || {}), { merge: true }).catch(e => console.warn("[charmNestSolve] write", e.message));
  };
  const maybePoll = async () => {
    if (Date.now() - lastCheck < 4000) return;
    lastCheck = Date.now();
    try { const s = await ref.get(); if (s.exists && s.data().stopRequested) stop = true; } catch (_) { /* ignore */ }
  };
  try {
    let result = await Solver.solve(solverJob, {
      shouldStop: () => stop,
      yield: async () => { await maybePoll(); if (Date.now() - lastWrite > 2500) await write(); },
      onBest: (b) => { if (Solver.betterLayout(b, best, solverJob.sheet)) best = Solver.publicLayout(b); },
      onTrial: (s) => { trials = s.completedTrials ?? s.trial + 1; }
    });
    result = Solver.bestResult(result, best, solverJob.sheet);
    const verification = Solver.verify(solverJob, result.placements, 4);
    await ref.set({ status: "done", trials: result.trials, best: slimBest(result), result: Object.assign(slimBest(result), { endedBy: result.endedBy, trials: result.trials, elapsedMs: Math.round(result.elapsedMs), params: result.params, density: result.density, verification }), finishedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, { merge: true });
    console.log(`[charmNestSolve] ${id}: ${result.placements.length}/${pieces.length} placed · ${result.endedBy} · ${result.trials} trials`);
  } catch (e) {
    console.error("[charmNestSolve]", id, e);
    await ref.set({ status: "error", ...(best ? { best: slimBest(best) } : {}), error: String(e && e.message || e), updatedAt: FV.serverTimestamp() }, { merge: true });
  }
  return { statusCode: 200, body: "ok" };
};

function unpackBits(buf, n) { const out = new Uint8Array(n); for (let i = 0; i < n; i++) out[i] = (buf[i >> 3] >> (i & 7)) & 1; return out; }
function slimBest(b) {
  return JSON.parse(JSON.stringify(Solver.publicLayout(b)));
}
