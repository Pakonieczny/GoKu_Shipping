/*  netlify/functions/investorKick.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — THE ONLY SCHEDULED FUNCTION IN THE SYSTEM.
 *
 *  A scheduled Netlify function CANNOT be reached over HTTP — Netlify answers
 *  a direct request with a bare 403, no body, no log line. The legacy
 *  googleAdsAutopilotKick.js learned that the hard way when the console it
 *  also served became unreachable the moment it got a cron entry. So the split
 *  here is deliberate and total:
 *
 *    investorKick.js              cron only. Decides what is due, dispatches.
 *    investorApi.js               the console API. NOT scheduled, HTTP works.
 *    investorCycle-background.js  the worker. Invoked, never scheduled.
 *
 *  Netlify's scheduled functions have a hard 30-second ceiling, so this file
 *  must finish fast: it reads control state, decides, dispatches a job id plus
 *  a single-use nonce, and returns. It never fetches a source, never calls a
 *  model, and never touches the ledger.
 *
 *  CADENCE. netlify.toml schedules this every 5 minutes. Netlify documents no
 *  minimum interval and a forum thread documents real drift on sub-hourly
 *  crons (32/28-minute alternation on a 31-minute schedule), so nothing here
 *  assumes exactly 288 fires a day. Every decision is derived from wall-clock
 *  state, never from a fire count.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const { mintWorkerNonce, isScheduledInvocation, redact } = require("./_investorAuth");
const M = require("./_investorMarket");

function baseUrl() {
  return process.env.URL || process.env.DEPLOY_PRIME_URL || "http://localhost:8888";
}

async function control() {
  const ref = A.col(A.COL.control).doc("control");
  const snap = await ref.get();
  const d = snap.exists ? snap.data() : {};
  return {
    enabled: d.enabled !== false,
    mode: d.mode || "research",              // research | approval | shadow | limited_auto
    dryRun: d.dryRun !== false,              // dry run defaults ON
    killSwitch: !!d.killSwitch,
    accountId: d.accountId || "paper-1",
    strategyVersion: d.strategyVersion || "v1",
    cycleSeconds: Number(d.cycleSeconds || 300),
    evidenceEverySeconds: Number(d.evidenceEverySeconds || 900),
    afterHoursCycles: d.afterHoursCycles === true,   // default: market hours only
    lastCycleAt: d.lastCycleAt || null,
    lastEvidenceAt: d.lastEvidenceAt || null,
  };
}

/** Decide which jobs are due. Market-hours gating is the single biggest cost
 *  lever in the system: restricting work to the session cuts model spend ~5x. */
function decide(ctrl, session, nowMs) {
  const tasks = [];
  const reasons = [];

  if (ctrl.killSwitch) return { tasks: [], reasons: ["kill switch engaged"] };
  if (!ctrl.enabled)   return { tasks: [], reasons: ["system disabled"] };

  const sinceCycle = ctrl.lastCycleAt ? nowMs - ctrl.lastCycleAt : Infinity;
  const sinceEvidence = ctrl.lastEvidenceAt ? nowMs - ctrl.lastEvidenceAt : Infinity;

  const inSession = session.tradingDay && (session.open || session.phase === "premarket");
  if (!inSession && !ctrl.afterHoursCycles) {
    reasons.push(`outside session (${session.phase}) — price cycle suppressed to control cost`);
  } else if (sinceCycle >= ctrl.cycleSeconds * 1000 * 0.9) {
    tasks.push("cycle");
    reasons.push(`price cycle due (${Math.round(sinceCycle / 1000)}s since last)`);
  } else {
    reasons.push(`price cycle not due (${Math.round(sinceCycle / 1000)}s of ${ctrl.cycleSeconds}s)`);
  }

  // The evidence sweep runs on a slower clock than the price loop and keeps
  // running after the close, because filings disseminate until 22:00 ET.
  if (sinceEvidence >= ctrl.evidenceEverySeconds * 1000 * 0.9) {
    if (session.tradingDay || sinceEvidence > 6 * 3600e3) {
      tasks.push("evidence");
      reasons.push(`evidence sweep due (${Math.round(sinceEvidence / 1000)}s since last)`);
    }
  }

  return { tasks, reasons };
}

async function dispatch(task, ctrl) {
  const jobId = `${task}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
  const fn = "investorCycle-background";
  const nonce = mintWorkerNonce(jobId, fn);

  await A.col(A.COL.jobs).doc(jobId).set({
    jobId, task, status: "queued",
    dispatchedAt: A.FV.serverTimestamp(),
    leaseExpiresAt: Date.now() + 14 * 60 * 1000,
    attempts: 0,
    mode: ctrl.mode, dryRun: ctrl.dryRun,
    ...A.envelope({ created_by: "investorKick" }),
  });

  // Payload stays tiny: Netlify caps background request bodies at 256KB and
  // the worker must load its own inputs through Firestore pointers anyway.
  const res = await fetch(`${baseUrl()}/.netlify/functions/${fn}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ jobId, task, nonce }),
  });
  return { jobId, task, upstream: res.status };
}

exports.handler = async (event) => {
  // Belt and braces: Netlify already refuses direct HTTP to a scheduled
  // function, but if that ever changes this must not become an open trigger.
  if (!isScheduledInvocation(event)) {
    return { statusCode: 403, body: JSON.stringify({ error: "scheduled invocation only" }) };
  }

  const startedAt = Date.now();
  try {
    const ctrl = await control();
    const session = M.sessionState(new Date());
    const { tasks, reasons } = decide(ctrl, session, startedAt);

    const dispatched = [];
    for (const t of tasks) {
      try { dispatched.push(await dispatch(t, ctrl)); }
      catch (e) { dispatched.push({ task: t, error: String(e.message).slice(0, 160) }); }
    }

    if (dispatched.some((d) => d.task === "cycle" && !d.error)) {
      await A.col(A.COL.control).doc("control").set({ lastCycleAt: startedAt }, { merge: true });
    }
    if (dispatched.some((d) => d.task === "evidence" && !d.error)) {
      await A.col(A.COL.control).doc("control").set({ lastEvidenceAt: startedAt }, { merge: true });
    }

    const summary = {
      ok: true, dispatched, reasons,
      session: { phase: session.phase, tradingDay: session.tradingDay, date: session.date },
      mode: ctrl.mode, dryRun: ctrl.dryRun,
      elapsedMs: Date.now() - startedAt,
    };
    console.log("investorKick", JSON.stringify(redact(summary)));
    return { statusCode: 200, body: JSON.stringify(summary) };
  } catch (e) {
    console.error("investorKick failed", redact({ error: e.message }));
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(e.message).slice(0, 200) }) };
  }
};

exports.control = control;
exports.decide = decide;
