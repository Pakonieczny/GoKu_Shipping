/*  netlify/functions/investorKick.js  (v2.0 — two-tier cadence)
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
 *  TWO TIERS, TWO CLOCKS
 *  ------------------------------------------------------------------------
 *  The desk used to run its entire pipeline — bars for the whole roster,
 *  factor regression, ranking, the gate stack for every name, evidence and
 *  model calls, portfolio selection, learning — every five minutes, and the
 *  "fast" exit guard rode the same five-minute cron whatever its setting
 *  said. That is expensive where it does not need to be and slow where it
 *  matters. Now:
 *
 *    DEEP SCAN  (task "cycle")  Scheduled New York times, default 09:50 and
 *                               13:00 ET (control.planTimesEt). It does the
 *                               heavy analysis and writes what the fast tier
 *                               acts on: proposals, preset exit levels on
 *                               holdings, and ARMED ENTRY LEVELS for names
 *                               that pass every hazard gate but have not yet
 *                               fallen far enough. Also runs for the release
 *                               bootstrap and the immutable daily finalization
 *                               after the close, and whenever an operator asks.
 *    STRIKE PASS (task "guard") Every `guardSeconds` (default 60) while the
 *                               exchange is open; every `guardSecondsClosed`
 *                               when shut. Prices only the held, planned and
 *                               pending symbols, checks stops and targets,
 *                               strikes armed levels, and fills approved
 *                               orders on their first eligible bar. Never
 *                               ranks the roster, never calls a model.
 *
 *  `planMode: "interval"` keeps the legacy behaviour (a deep scan every
 *  `cycleSeconds`) for operators who want it. netlify.toml fires this
 *  dispatcher every minute so the strike clock is real; nothing assumes an
 *  exact fire count.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const { mintWorkerNonce, isScheduledInvocation, redact,
        loadAuthSecrets } = require("./_investorAuth");
const M = require("./_investorMarket");
const STATE = require("./_investorState");
const B = require("./_investorBootstrap");

function baseUrl() {
  return process.env.URL || process.env.DEPLOY_PRIME_URL || "http://localhost:8888";
}

/* ── the deep-scan schedule ─────────────────────────────────────────────── */
const DEFAULT_PLAN_TIMES_ET = Object.freeze(["09:50", "13:00"]);
const PLAN_TIME = /^([01]\d|2[0-3]):([0-5]\d)$/;
/* Scans are meaningful only inside the regular session and after the opening
   auction window (the cost model refuses to open in it anyway), and they need
   time to complete before the closing auction. */
const PLAN_EARLIEST_MIN = 9 * 60 + 45;
const PLAN_LATEST_MIN = 15 * 60 + 30;
const MAX_PLAN_TIMES = 6;

function planTimeMinutes(t) {
  const [h, m] = String(t).split(":").map(Number);
  return h * 60 + m;
}

/** PURE. Clean an operator-entered list of "HH:MM" New York times. Returns
 *  null when nothing usable was supplied so the caller can fall back. */
function normalizePlanTimes(value) {
  const list = Array.isArray(value) ? value
    : typeof value === "string" ? value.split(/[\s,;]+/) : null;
  if (!list) return null;
  const out = [];
  for (const raw of list) {
    const s = String(raw || "").trim();
    if (!PLAN_TIME.test(s)) continue;
    const mm = planTimeMinutes(s);
    if (mm < PLAN_EARLIEST_MIN || mm > PLAN_LATEST_MIN) continue;
    if (!out.includes(s)) out.push(s);
  }
  out.sort();
  return out.length ? out.slice(0, MAX_PLAN_TIMES) : null;
}

/** Is this control document on the scheduled deep-scan clock? Documents
 *  without a plan mode (older fixtures, hand-built contexts) stay on the
 *  interval clock, so nothing that worked before changes silently. */
function scheduledMode(ctrl) {
  return ctrl && ctrl.planMode === "scheduled"
    && Array.isArray(ctrl.planTimesEt) && ctrl.planTimesEt.length > 0;
}

/** PURE. The scheduled deep scan due right now, if any: the LATEST plan time
 *  already passed today that no scan has yet satisfied. A scan that ran for a
 *  later slot satisfies every earlier one, so a desk that comes up at 14:10
 *  runs one catch-up scan for 13:00, not two. Nothing is due before the
 *  opening auction ends or in the final fifteen minutes. */
function duePlanSlot(ctrl, session) {
  if (!session || !session.tradingDay || !session.open) return null;
  if (session.phase !== "regular") return null;
  const closeMin = Number(session.regularCloseMinutesEt) || 16 * 60;
  const times = Array.isArray(ctrl.planTimesEt) ? ctrl.planTimesEt : [];
  const passed = times.filter((t) => {
    const mm = planTimeMinutes(t);
    return mm <= Number(session.minutesEt) && mm < closeMin - 15;
  });
  if (!passed.length) return null;
  const time = passed[passed.length - 1];
  const key = `${session.date} ${time}`;
  const ran = String(ctrl.lastPlanKey || "");
  if (ran.startsWith(`${session.date} `) && ran.slice(11) >= time) return null;
  return { key, time, date: session.date };
}

/** PURE. When the next scheduled deep scan will fire, for the console. */
function nextPlanAtMs(ctrl, nowMs = Date.now()) {
  if (!scheduledMode(ctrl)) return null;
  for (let day = 0; day < 10; day += 1) {
    const probe = new Date(nowMs + day * 86400000);
    const st = M.sessionState(probe);
    if (!st.tradingDay) continue;
    const closeMin = Number(st.regularCloseMinutesEt) || 16 * 60;
    for (const t of ctrl.planTimesEt) {
      const mm = planTimeMinutes(t);
      if (mm >= closeMin - 15) continue;
      if (day === 0 && mm <= st.minutesEt) continue;
      const at = M.nyWallClockToUtcMs(st.date, mm);
      if (Number.isFinite(at) && at > nowMs) return at;
    }
  }
  return null;
}

async function control() {
  const ref = A.col(A.COL.control).doc("control");
  const snap = await ref.get();
  const d = snap.exists ? snap.data() : {};
  const operating = STATE.describe(d);
  const bounded = (value, fallback, min, max) => {
    const n = Number(value);
    return Number.isFinite(n) && n >= min && n <= max ? n : fallback;
  };
  return {
    operatingState: operating.state,
    operatingLabel: operating.label,
    paused: operating.paused,
    entriesFrozen: operating.entriesFrozen,
    paperLedger: operating.paperLedger,
    enabled: !operating.paused,
    mode: operating.stage,                    // compatibility projection
    dryRun: !operating.paperLedger,           // observation-only projection
    killSwitch: operating.paused,
    accountId: d.accountId || "paper-1",
    strategyVersion: d.strategyVersion || require("./_investorStrategy").version,
    bootstrapVersion: Number(d.bootstrapVersion) || 0,
    bootstrapPending: Number(d.bootstrapVersion) !== B.BOOTSTRAP_VERSION,
    /* Deep-scan clock. Scheduled by default; "interval" restores the legacy
       every-cycleSeconds scan. The interval ceiling is a day because a deep
       scan is no longer expected to be frequent. */
    planMode: d.planMode === "interval" ? "interval" : "scheduled",
    planTimesEt: normalizePlanTimes(d.planTimesEt) || [...DEFAULT_PLAN_TIMES_ET],
    lastPlanKey: d.lastPlanKey || null,
    cycleSeconds: bounded(d.cycleSeconds, 300, 60, 86400),
    /* Strike clock. The cron fires every minute, so 60 is now a real floor. */
    guardSeconds: bounded(d.guardSeconds, 60, 60, 600),
    /* The guard also runs while the exchange is shut, on a slower clock. It
       fetches nothing there — it re-reads stored bars — so the marginal cost
       is a Firestore read per holding, against a 17.5-hour blind spot. */
    guardSecondsClosed: bounded(d.guardSecondsClosed, 900, 300, 3600),
    strikeBarTimeframe: /^(1|5)Min$/.test(String(d.strikeBarTimeframe || "")) ? d.strikeBarTimeframe : "1Min",
    evidenceEverySeconds: bounded(d.evidenceEverySeconds, 900, 300, 86400),
    afterHoursCycles: d.afterHoursCycles === true,   // default: market hours only
    lastCycleAt: d.lastCycleAt || null,
    lastGuardAt: d.lastGuardAt || null,
    lastEvidenceAt: d.lastEvidenceAt || null,
    lastArchiveAt: d.lastArchiveAt || null,
    lastArchiveDate: d.lastArchiveDate || null,
    lastDailyFinalizeDate: d.lastDailyFinalizeDate || null,
  };
}

/** Decide which jobs are due. Market-hours gating and the scheduled deep-scan
 *  clock are together the biggest cost lever in the system: two deep scans a
 *  session instead of seventy-eight. */
function decide(ctrl, session, nowMs) {
  const tasks = [];
  const reasons = [];

  const sinceCycle = ctrl.lastCycleAt ? nowMs - ctrl.lastCycleAt : Infinity;
  const sinceGuard = ctrl.lastGuardAt ? nowMs - ctrl.lastGuardAt : Infinity;
  const sinceEvidence = ctrl.lastEvidenceAt ? nowMs - ctrl.lastEvidenceAt : Infinity;

  const entriesClosed = ctrl.paused || ctrl.killSwitch || !ctrl.enabled;
  const inSession = session.tradingDay && (session.open || session.phase === "premarket");
  const scanAllowed = !entriesClosed && (inSession || ctrl.afterHoursCycles);
  const scheduled = scheduledMode(ctrl);
  const finalization = M.dailyFinalizationState(session);
  const finalizationDue = finalization.ready
    && ctrl.lastDailyFinalizeDate !== session.date;
  /* A fresh release bootstraps immediately, including after hours. Waiting for
     the next market scan delays frozen identity, account reconciliation and
     autonomous authorization until the opening session itself. */
  if (ctrl.bootstrapPending) {
    tasks.push("cycle");
    reasons.push("release bootstrap is pending — critical paper identity runs now");
  } else if (finalizationDue) {
    tasks.push("cycle");
    reasons.push(`immutable daily finalization due for ${session.date}`);
  } else if (entriesClosed) {
    reasons.push(`entries closed (${ctrl.killSwitch ? "kill switch" : "disabled"}) — deep scan suppressed`);
  } else if (scheduled) {
    const slot = duePlanSlot(ctrl, session);
    if (slot) {
      tasks.push("cycle");
      reasons.push(`scheduled deep scan for ${slot.time} ET is due (${slot.date})`);
    } else if (!session.open) {
      reasons.push(`outside session (${session.phase}) — deep scans run at ${ctrl.planTimesEt.join(", ")} ET`);
    } else {
      reasons.push(`deep scan not due (scheduled ${ctrl.planTimesEt.join(", ")} ET`
        + `${ctrl.lastPlanKey ? `; last ran for ${ctrl.lastPlanKey}` : ""})`);
    }
  } else if (!scanAllowed) {
    reasons.push(`outside session (${session.phase}) — price cycle suppressed to control cost`);
  } else if (sinceCycle >= ctrl.cycleSeconds * 1000 * 0.9) {
    tasks.push("cycle");
    reasons.push(`price cycle due (${Math.round(sinceCycle / 1000)}s since last)`);
  } else {
    reasons.push(`price cycle not due (${Math.round(sinceCycle / 1000)}s of ${ctrl.cycleSeconds}s)`);
  }

  /* The strike pass is independently dispatched even when a deep scan is due.
     Its held-only, risk-reducing transactions and its level strikes are safe
     to overlap (order locks and idempotent fills serialize them), so a long
     scan cannot delay a stop.
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
    reasons.push(`strike pass due (${Math.round(sinceGuard / 1000)}s since last`
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

  /* Nightly intraday archive: once per trading day, after the close buffer,
     regardless of pause/freeze (it reads and stores prices; it decides
     nothing). Re-dispatch is suppressed for 30 minutes so a dead worker does
     not fan out a retry every tick. */
  const sinceArchive = ctrl.lastArchiveAt ? nowMs - ctrl.lastArchiveAt : Infinity;
  if (session.tradingDay && finalization.ready && ctrl.lastArchiveDate !== session.date
      && sinceArchive > 30 * 60000) {
    tasks.push("archive");
    reasons.push(`nightly intraday archive due for ${session.date}`);
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
function stampField(task) {
  return task === "cycle" ? "lastCycleAt" : task === "guard" ? "lastGuardAt"
    : task === "archive" ? "lastArchiveAt" : "lastEvidenceAt";
}

async function dispatch(task, ctrl, session = null, { manual = false } = {}) {
  const now = Date.now();
  const guardSeconds = session && session.open === false
    ? ctrl.guardSecondsClosed : ctrl.guardSeconds;
  const seconds = task === "cycle" ? ctrl.cycleSeconds
    : task === "guard" ? guardSeconds
    : task === "archive" ? 1800 : ctrl.evidenceEverySeconds;
  const cadenceSlot = Math.floor(now / (Math.max(60, Number(seconds) || 300) * 1000));
  const account = String(ctrl.accountId || "paper-1").replace(/[^a-zA-Z0-9_-]/g, "_");
  /* A scheduled deep scan is keyed by its New York slot, so a delayed cron
     and a retry converge on one job. A manual scan that happens to satisfy a
     due slot also stamps it, so the scheduler does not run a second scan a
     minute after the operator's. */
  const scheduledCtrl = scheduledMode(ctrl) ? ctrl
    : (ctrl.planMode === "scheduled" ? { ...ctrl, planTimesEt: normalizePlanTimes(ctrl.planTimesEt) || [...DEFAULT_PLAN_TIMES_ET] } : null);
  const planSlot = task === "cycle" && session && scheduledCtrl && scheduledMode(scheduledCtrl)
    ? duePlanSlot(scheduledCtrl, session) : null;
  const jobId = manual ? `${task}_${account}_manual_${now}`
    : planSlot ? `cycle_${account}_${planSlot.date}_${planSlot.time.replace(":", "")}`
      : `${task}_${account}_${cadenceSlot}`;
  const fn = "investorCycle-background";
  const nonce = mintWorkerNonce(jobId, fn);
  const jref = A.col(A.COL.jobs).doc(jobId);
  const cref = A.col(A.COL.control).doc("control");
  let previousStamp = null;
  const claimed = await A.runTransaction(async (tx) => {
    const [cur, control] = await Promise.all([tx.get(jref), tx.get(cref)]);
    if (cur.exists && ["queued", "running", "complete"].includes(cur.data().status)) return false;
    previousStamp = control.exists ? (control.data()[stampField(task)] ?? null) : null;
    tx.set(jref, {
      jobId, task, slot: cadenceSlot, accountId: ctrl.accountId, status: "queued",
      sessionDate: session && session.date || null,
      planSlot: planSlot ? planSlot.key : null,
      dispatchedAt: A.FV.serverTimestamp(), leaseExpiresAt: now + 14 * 60 * 1000,
      attempts: cur.exists ? Number(cur.data().attempts || 0) : 0,
      mode: ctrl.mode, dryRun: ctrl.dryRun, manual: !!manual,
      operatingState: ctrl.operatingState || null,
      ...A.envelope({ created_by: manual ? "investorApi.runCycleNow" : "investorKick" }),
    });
    const stamp = stampField(task);
    /* `lastPlanKey` is NOT stamped here. A worker that returns 202 and then
       dies (uncaught throw, OOM, background time limit) would otherwise leave
       the slot marked as satisfied, and duePlanSlot would treat that slot and
       every earlier one as done for the rest of the session — with only two
       slots a day, one such death costs a whole scan and two cost the day,
       with no retry. The per-slot JOB DOC above is what prevents a double
       dispatch, and a dead job's status is not in the claimed set, so the
       next kick re-claims the same slot. The worker stamps lastPlanKey once
       it has actually finished. */
    tx.set(cref, { [stamp]: now }, { merge: true });
    return true;
  });
  if (!claimed) return { jobId, task, duplicateSlot: true, upstream: null };

  // Payload stays tiny: Netlify caps background request bodies at 256KB and
  // the worker must load its own inputs through Firestore pointers anyway.
  let res;
  try {
    res = await fetch(`${baseUrl()}/.netlify/functions/${fn}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ jobId, task, nonce }),
    });
  } catch (e) {
    /* A thrown fetch (socket reset, DNS, timeout) is a dispatch that never
       happened, exactly like a non-2xx response: same rollback. */
    res = { ok: false, status: 0, thrown: String(e && e.message || e).slice(0, 120) };
  }
  if (!res.ok) {
    /* The cadence stamp was advanced inside the claim so two concurrent ticks
       cannot both dispatch the slot. A dispatch that never reached the worker
       must not consume the slot: restore the previous stamp so the next tick
       retries instead of waiting a full cadence; a "dispatch_failed" job row
       is not a claimed status, so the same slot can be dispatched again. */
    const stamp = stampField(task);
    await jref.set({ status: "dispatch_failed", dispatchStatus: res.status,
      dispatchError: res.thrown || null,
      dispatchFailedAt: A.FV.serverTimestamp() }, { merge: true });
    /* Compare-and-set: restore the previous stamp only if the stamp is still
       the one THIS dispatch wrote. If a later dispatch advanced it, that
       later success must not be erased by this failure's rollback. */
    await A.runTransaction(async (tx) => {
      const cur = await tx.get(cref);
      const stored = cur.exists ? cur.data()[stamp] : null;
      if (stored === now) tx.set(cref, { [stamp]: previousStamp }, { merge: true });
    });
  }
  return { jobId, task, upstream: res.status, planSlot: planSlot ? planSlot.key : null };
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
      operatingState: ctrl.operatingState,
      cadence: { planMode: ctrl.planMode, planTimesEt: ctrl.planTimesEt,
        guardSeconds: ctrl.guardSeconds, strikeBarTimeframe: ctrl.strikeBarTimeframe },
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
exports.normalizePlanTimes = normalizePlanTimes;
exports.duePlanSlot = duePlanSlot;
exports.nextPlanAtMs = nextPlanAtMs;
exports.scheduledMode = scheduledMode;
exports.DEFAULT_PLAN_TIMES_ET = DEFAULT_PLAN_TIMES_ET;
