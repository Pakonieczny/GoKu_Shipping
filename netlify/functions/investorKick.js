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
 *  CADENCE. netlify.toml invokes this lightweight dispatcher every minute so a
 *  delayed cron does not skip an entire five-minute decision slot. The control
 *  document's wall-clock cadence still gates the expensive worker (five
 *  minutes by default), so frequent kicks do not imply frequent data/model
 *  calls. Nothing assumes an exact fire count.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const { mintWorkerNonce, isScheduledInvocation, redact,
        loadAuthSecrets } = require("./_investorAuth");
const M = require("./_investorMarket");

function baseUrl() {
  return process.env.URL || process.env.DEPLOY_PRIME_URL || "http://localhost:8888";
}

async function control() {
  const ref = A.col(A.COL.control).doc("control");
  const snap = await ref.get();
  const d = snap.exists ? snap.data() : {};
  const bounded = (value, fallback, min, max) => {
    const n = Number(value);
    return Number.isFinite(n) && n >= min && n <= max ? n : fallback;
  };
  return {
    enabled: d.enabled !== false,
    mode: d.mode || "research",              // research | approval | shadow | limited_auto
    dryRun: d.dryRun !== false,              // dry run defaults ON
    killSwitch: !!d.killSwitch,
    accountId: d.accountId || "paper-1",
    strategyVersion: d.strategyVersion || require("./_investorStrategy").version,
    cycleSeconds: bounded(d.cycleSeconds, 300, 60, 3600),
    guardSeconds: bounded(d.guardSeconds, 60, 60, 300),
    /* The guard also runs while the exchange is shut, on a slower clock. It
       fetches nothing there — it re-reads stored bars — so the marginal cost
       is a Firestore read per holding, against a 17.5-hour blind spot. */
    guardSecondsClosed: bounded(d.guardSecondsClosed, 900, 300, 3600),
    evidenceEverySeconds: bounded(d.evidenceEverySeconds, 900, 300, 86400),
    afterHoursCycles: d.afterHoursCycles === true,   // default: market hours only
    lastCycleAt: d.lastCycleAt || null,
    lastGuardAt: d.lastGuardAt || null,
    lastEvidenceAt: d.lastEvidenceAt || null,
  };
}

/** Decide which jobs are due. Market-hours gating is the single biggest cost
 *  lever in the system: restricting work to the session cuts model spend ~5x. */
function decide(ctrl, session, nowMs) {
  const tasks = [];
  const reasons = [];

  const sinceCycle = ctrl.lastCycleAt ? nowMs - ctrl.lastCycleAt : Infinity;
  const sinceGuard = ctrl.lastGuardAt ? nowMs - ctrl.lastGuardAt : Infinity;
  const sinceEvidence = ctrl.lastEvidenceAt ? nowMs - ctrl.lastEvidenceAt : Infinity;

  const entriesClosed = ctrl.killSwitch || !ctrl.enabled;
  const inSession = session.tradingDay && (session.open || session.phase === "premarket");
  const scanAllowed = !entriesClosed && (inSession || ctrl.afterHoursCycles);
  if (entriesClosed) {
    reasons.push(`entries closed (${ctrl.killSwitch ? "kill switch" : "disabled"}) — full scan suppressed`);
  } else if (!scanAllowed) {
    reasons.push(`outside session (${session.phase}) — price cycle suppressed to control cost`);
  } else if (sinceCycle >= ctrl.cycleSeconds * 1000 * 0.9) {
    tasks.push("cycle");
    reasons.push(`price cycle due (${Math.round(sinceCycle / 1000)}s since last)`);
  } else {
    reasons.push(`price cycle not due (${Math.round(sinceCycle / 1000)}s of ${ctrl.cycleSeconds}s)`);
  }

  /* The guard is independently dispatched even when a full cycle is due. Its
     held-only, risk-reducing transactions are safe to overlap, so a long
     opportunity scan cannot delay a stop.
     It runs on EVERY tick of its clock, in session or not. Positions do not
     stop existing at 16:00 on Friday, and a guard that only ran while the
     exchange was open covered a quarter of the week and left a 17.5-hour gap
     across every overnight. Outside the session it evaluates and arms from
     stored bars; execution still waits for the regular session, because
     pre/post-market bars are not priced under regular-session spread
     assumptions. */
  const guardPeriodMs = (session.open ? ctrl.guardSeconds : ctrl.guardSecondsClosed) * 1000;
  if (sinceGuard >= guardPeriodMs * 0.9) {
    tasks.push("guard");
    reasons.push(`held-position guard due (${Math.round(sinceGuard / 1000)}s since last`
      + `${session.open ? "" : ", closed-market evaluation"})`);
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

/* `manual` exists because the scheduler is the ONLY thing that starts a cycle,
   and an operator with no way to start one has no way to tell a stopped system
   apart from a quiet one. It changes exactly two things: the job id no longer
   collapses into the cadence slot (a manual run inside the current slot would
   otherwise be swallowed as a duplicate and look like nothing happened), and
   the audit trail says an operator asked. It grants nothing — the worker still
   re-reads the control document and still refuses on kill switch or disabled,
   and the nonce is minted the same way for both paths. */
async function dispatch(task, ctrl, session = null, { manual = false } = {}) {
  const now = Date.now();
  const guardSeconds = session && session.open === false
    ? ctrl.guardSecondsClosed : ctrl.guardSeconds;
  const seconds = task === "cycle" ? ctrl.cycleSeconds
    : task === "guard" ? guardSeconds : ctrl.evidenceEverySeconds;
  const slot = Math.floor(now / (Math.max(60, Number(seconds) || 300) * 1000));
  const account = String(ctrl.accountId || "paper-1").replace(/[^a-zA-Z0-9_-]/g, "_");
  const jobId = manual ? `${task}_${account}_manual_${now}` : `${task}_${account}_${slot}`;
  const fn = "investorCycle-background";
  const nonce = mintWorkerNonce(jobId, fn);
  const jref = A.col(A.COL.jobs).doc(jobId);
  const cref = A.col(A.COL.control).doc("control");
  const claimed = await A.runTransaction(async (tx) => {
    const cur = await tx.get(jref);
    if (cur.exists && ["queued", "running", "complete"].includes(cur.data().status)) return false;
    tx.set(jref, {
      jobId, task, slot, accountId: ctrl.accountId, status: "queued",
      dispatchedAt: A.FV.serverTimestamp(), leaseExpiresAt: now + 14 * 60 * 1000,
      attempts: cur.exists ? Number(cur.data().attempts || 0) : 0,
      mode: ctrl.mode, dryRun: ctrl.dryRun, manual: !!manual,
      ...A.envelope({ created_by: manual ? "investorApi.runCycleNow" : "investorKick" }),
    });
    const stamp = task === "cycle" ? "lastCycleAt"
      : task === "guard" ? "lastGuardAt" : "lastEvidenceAt";
    tx.set(cref, { [stamp]: now }, { merge: true });
    return true;
  });
  if (!claimed) return { jobId, task, duplicateSlot: true, upstream: null };

  // Payload stays tiny: Netlify caps background request bodies at 256KB and
  // the worker must load its own inputs through Firestore pointers anyway.
  const res = await fetch(`${baseUrl()}/.netlify/functions/${fn}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ jobId, task, nonce }),
  });
  if (!res.ok) {
    await jref.set({ status: "dispatch_failed", dispatchStatus: res.status,
      dispatchFailedAt: A.FV.serverTimestamp() }, { merge: true });
  }
  return { jobId, task, upstream: res.status };
}

exports.handler = async (event) => {
  // Belt and braces: Netlify already refuses direct HTTP to a scheduled
  // function, but if that ever changes this must not become an open trigger.
  if (!isScheduledInvocation(event)) {
    return { statusCode: 403, body: JSON.stringify({ error: "scheduled invocation only" }) };
  }
  /* The worker nonce is minted from the session secret, so it must resolve
     before dispatch. An unresolved secret mints nothing and the worker
     rejects the invocation — the cycle stops rather than running unsigned. */
  await loadAuthSecrets();

  const startedAt = Date.now();
  try {
    await M.loadMarketSettings();
    const ctrl = await control();
    const session = M.sessionState(new Date());
    const { tasks, reasons } = decide(ctrl, session, startedAt);

    const dispatched = [];
    for (const t of tasks) {
      try { dispatched.push(await dispatch(t, ctrl, session)); }
      catch (e) { dispatched.push({ task: t, error: String(e.message).slice(0, 160) }); }
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
exports.dispatch = dispatch;
