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
 *  The desk historically ran its entire pipeline — bars for the whole roster,
 *  factor regression, ranking, the gate stack for every name, evidence and
 *  model calls, portfolio selection, learning — every five minutes, while the
 *  "fast" exit guard rode that same five-minute cron. The two workloads now
 *  have independent clocks:
 *
 *    DEEP SCAN  (task "cycle")  Every `cycleSeconds` (default five minutes),
 *                               or explicit New York times when the operator
 *                               selects scheduled mode. It does the
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
 *  `planMode: "scheduled"` is the lower-cost opt-in clock (default times
 *  09:50 and 13:00 ET). netlify.toml fires this dispatcher every minute so
 *  the strike clock is real; nothing assumes an exact fire count.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const { mintWorkerNonce, isScheduledInvocation, redact,
        loadAuthSecrets } = require("./_investorAuth");
const M = require("./_investorMarket");
const STATE = require("./_investorState");
const B = require("./_investorBootstrap");
const JOBS = require("./_investorJobs");
const POLICY = require("./_investorPolicy");

function baseUrl() {
  return process.env.URL || process.env.DEPLOY_PRIME_URL || "http://localhost:8888";
}

/* ── the deep-scan schedule ─────────────────────────────────────────────── */
const DEFAULT_PLAN_TIMES_ET = Object.freeze(["09:50", "13:00"]);
/* Preserve the desk's established behaviour: unless an operator explicitly
   selects scheduled mode, the complete roster is rescanned on cycleSeconds
   (five minutes by default). The one-minute guard remains independent. */
const DEFAULT_PLAN_MODE = "interval";
/* v0 briefly made two scheduled scans the implicit/default policy and wrote
   that value into the live control document. Merely changing the code-side
   fallback does not repair that stored value. v1 is a one-time control
   migration back to the operator's required five-minute full-universe clock;
   later explicit choices carry this version and are preserved. */
const FULL_SCAN_CADENCE_VERSION = 1;
const PLAN_TIME = /^([01]\d|2[0-3]):([0-5]\d)$/;
/* ── DISPATCH WINDOWS ARE PER TASK CLASS (blueprint §11.1, D-8 / G1.8) ────
   One module-level window used to bound EVERY configured time, so nothing
   could be dispatched before 09:45 ET and the pre-market Manager Meeting of
   §6.1 could not run at all. Each task class now declares its own New York
   window as a list of [earliestMin, latestMin] segments; a class with no
   declared window is not dispatchable. The scan window is the old constant
   pair, so scan behaviour is bit-identical. */
const TASK_WINDOWS_ET = Object.freeze({
  /* Scans are meaningful only inside the regular session and after the
     opening auction window (the cost model refuses to open in it anyway),
     and they need time to complete before the closing auction. */
  scan:              Object.freeze({ segments: Object.freeze([[9 * 60 + 45, 15 * 60 + 30]]),
                       note: "regular session after the opening auction, before the closing auction" }),
  /* Model work; no order is placed. Freeze at 08:30, hard deadline 09:15. */
  premarket_manager: Object.freeze({ segments: Object.freeze([[4 * 60, 9 * 60 + 15]]),
                       note: "pre-market model work, 04:00–09:15 ET" }),
  focused_research:  Object.freeze({ segments: Object.freeze([[4 * 60, 20 * 60]]),
                       note: "pre-market model work, 04:00–09:15 ET" }),
  portfolio_synthesis: Object.freeze({ segments: Object.freeze([[4 * 60, 20 * 60]]),
                       note: "pre-market model work, 04:00–09:15 ET" }),
  /* A material-event revision may run whenever evidence arrives. */
  event_ingest: Object.freeze({segments:Object.freeze([[8*60+30,20*60]]),note:"bounded daytime evidence polling"}),
  event_revision:    Object.freeze({ segments: Object.freeze([[0, 24 * 60 - 1]]), note: "any time" }),
  /* One overnight full-roster pass ending at the 08:30 freeze (D-11). */
  ingest:            Object.freeze({ segments: Object.freeze([[20 * 60, 24 * 60 - 1], [0, 8 * 60 + 30]]),
                       note: "overnight, ending at the 08:30 ET evidence freeze; no daytime dispatch" }),
  /* Market hours plus extended, per broker. */
  execute:           Object.freeze({ segments: Object.freeze([[4 * 60, 20 * 60]]),
                       note: "market hours plus extended, 04:00–20:00 ET" }),
  /* After the official close. Half days close earlier; the finalization
     state machine, not this window, decides when marks are final. */
  postclose:         Object.freeze({ segments: Object.freeze([[13 * 60 + 15, 24 * 60 - 1]]),
                       note: "after the official close" }),
  archive:           Object.freeze({ segments: Object.freeze([[13 * 60 + 15, 24 * 60 - 1]]),
                       note: "after the official close" }),
});
function dispatchWindow(taskClass) { return TASK_WINDOWS_ET[taskClass] || null; }
/** PURE. Is a task class dispatchable at this New York minute-of-day? */
function taskDispatchable(taskClass, minutesEt) {
  const w = dispatchWindow(taskClass);
  const mm = Number(minutesEt);
  if (!w || !Number.isFinite(mm)) return false;
  return w.segments.some(([a, b]) => mm >= a && mm <= b);
}
const PLAN_EARLIEST_MIN = TASK_WINDOWS_ET.scan.segments[0][0];   // 09:45, unchanged
const PLAN_LATEST_MIN = TASK_WINDOWS_ET.scan.segments[0][1];     // 15:30, unchanged
const MAX_PLAN_TIMES = 6;

function planTimeMinutes(t) {
  const [h, m] = String(t).split(":").map(Number);
  return h * 60 + m;
}

/** PURE. Clean an operator-entered list of "HH:MM" New York times for a task
 *  class (default: the deep scan). Times outside the class's window are
 *  dropped; a class with no window accepts nothing. Returns null when nothing
 *  usable was supplied so the caller can fall back. */
function normalizePlanTimes(value, taskClass = "scan") {
  const list = Array.isArray(value) ? value
    : typeof value === "string" ? value.split(/[\s,;]+/) : null;
  if (!list) return null;
  if (!dispatchWindow(taskClass)) return null;
  const out = [];
  for (const raw of list) {
    const s = String(raw || "").trim();
    if (!PLAN_TIME.test(s)) continue;
    if (!taskDispatchable(taskClass, planTimeMinutes(s))) continue;
    if (!out.includes(s)) out.push(s);
  }
  out.sort();
  return out.length ? out.slice(0, MAX_PLAN_TIMES) : null;
}

function resolvePlanMode(value) {
  return value === "scheduled" ? "scheduled" : DEFAULT_PLAN_MODE;
}

/** PURE. Old control documents predate an explicit cadence-policy marker.
 *  Treat their stored scheduled value as the retired default, not as a new
 *  operator choice. `control()` persists the same answer on the next cron. */
function effectivePlanMode(ctrl) {
  const c = ctrl || {};
  if ((Number(c.fullScanCadenceVersion) || 0) < FULL_SCAN_CADENCE_VERSION) {
    return DEFAULT_PLAN_MODE;
  }
  return resolvePlanMode(c.planMode);
}

/** Is this control document on the scheduled deep-scan clock? Documents
 *  without an explicit plan mode stay on the established five-minute
 *  interval clock. */
function scheduledMode(ctrl) {
  return ctrl && resolvePlanMode(ctrl.planMode) === "scheduled"
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
  if ((Number(d.fullScanCadenceVersion) || 0) < FULL_SCAN_CADENCE_VERSION) {
    const cadenceMigration = {
      planMode: DEFAULT_PLAN_MODE,
      cycleSeconds: 300,
      fullScanCadenceVersion: FULL_SCAN_CADENCE_VERSION,
      planModeSource: "migration:restore_five_minute_full_universe",
      cadenceMigratedAtMs: Date.now(),
    };
    await ref.set(cadenceMigration, { merge: true });
    Object.assign(d, cadenceMigration);
  }
  const operating = STATE.describe(d);
  const bounded = (value, fallback, min, max) => {
    const n = Number(value);
    return Number.isFinite(n) && n >= min && n <= max ? n : fallback;
  };
  return {
    /* The raw control document, for the manager engine's own decisions. */
    raw: d,
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
    /* Deep-scan clock. The full roster keeps its established five-minute
       interval unless the operator explicitly selects scheduled ET times. */
    planMode: resolvePlanMode(d.planMode),
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

/** Decide which jobs are due. Market-hours gating avoids overnight work;
 *  operators can explicitly select the lower-cost scheduled scan clock. */
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

/** PURE. A lost background invocation must not reserve a scan forever.
 *  Complete work is final; queued/running work blocks only while its own
 *  lease is fresh. Dead, yielded, failed and stale work may be reclaimed. */
function jobBlocksDispatch(job, nowMs = Date.now()) {
  const j = job || {};
  if (j.status === "complete") return true;
  if (j.status === "queued") return Number(j.leaseExpiresAt) > nowMs;
  if (j.status === "running") {
    return Number(j.workerLeaseExpiresAt || j.leaseExpiresAt) > nowMs;
  }
  return false;
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
    if (cur.exists && jobBlocksDispatch(cur.data(), now)) return false;
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

/* ═══ THE FUND-MANAGER ENGINE (blueprint §11.1) ═══════════════════════════
 * Two engines share this one-minute dispatcher during the cutover. Which one
 * holds the writer epoch is a control-document state (`engineMode`), flipped
 * only by the cutover sequence of §19.4; until then the legacy path above
 * runs bit-identical. Under the manager engine the dispatcher:
 *   1. enqueues due work idempotently (one logical run per task, account
 *      and trading date);
 *   2. reads every due or resumable job and orders it: execute first, then
 *      critical event revisions, then continuations, then the pre-market
 *      manager, then ingest, then post-close/archive;
 *   3. launches at most four jobs and at most one heavy job per category
 *      inside a 20-second budget, each with a freshly signed, single-use
 *      nonce bound to the exact job attempt and target function.
 * Research, ingest and model latency can therefore never starve order
 * reconciliation. */
function engineMode(ctrl) { return ctrl && ctrl.engineMode === "manager" ? "manager" : "legacy"; }

/** PURE. When should the overnight ingest pass START so that it COMPLETES at
 *  the freeze? The last measured pass duration, plus a margin; the §6.1
 *  default of 3.2 h stands until a pass has been measured. */
function ingestStartMinuteEt(ctrl = {}) {
  const measuredMs = Number(ctrl.lastIngestPass && ctrl.lastIngestPass.elapsedMs);
  const assumedMs = 3.2 * 3600e3;
  const passMs = Number.isFinite(measuredMs) && measuredMs > 0 ? Math.min(measuredMs, 8 * 3600e3) : assumedMs;
  const marginMin = 10;
  const start = POLICY.CUTOFFS_ET.evidenceFreezeMin - Math.ceil(passMs / 60000) - marginMin;
  /* Normalise into the overnight window: a negative start means "the
     previous evening"; express it as minutes on that day. */
  return start >= 0 ? { minuteEt: start, previousEvening: false, passMs } : { minuteEt: 24 * 60 + start, previousEvening: true, passMs };
}

/** The next trading date at or after this session (the date a pre-market
 *  pass serves). */
function nextTradingDate(session, nowMs) {
  if (session && session.tradingDay && Number(session.minutesEt) < POLICY.CUTOFFS_ET.evidenceFreezeMin) return session.date;
  for (let day = 1; day <= 10; day += 1) {
    const st = M.sessionState(new Date(nowMs + day * 864e5));
    if (st.tradingDay) return st.date;
  }
  return session ? session.date : null;
}

/** PURE apart from the calendar. Which manager-engine work is due now. */
function decideManager(ctrl, session, nowMs) {
  const enqueue = [], reasons = [];
  const accountId = String(ctrl.accountId || "paper-1");
  const mm = Number(session.minutesEt);
  const operating = STATE.describe(ctrl);
  const managerPaused = ctrl.managerState === "PAUSED" || operating.paused;
  const executorPaused = ctrl.executorState === "PAUSED_SAFETY" || ctrl.executorEnabled === false;

  /* Ingest: one pass ahead of each trading date, started so it completes at
     the freeze; dispatchable only inside the overnight window (D-11). */
  const target = nextTradingDate(session, nowMs);
  const start = ingestStartMinuteEt(ctrl);
  const startedThisTarget = ctrl.ingestPassState && ctrl.ingestPassState.tradingDate === target;
  const completedThisTarget = ctrl.lastIngestTradingDate === target;
  const startDue = start.previousEvening
    ? (mm >= start.minuteEt || mm < POLICY.CUTOFFS_ET.evidenceFreezeMin)
    : (mm >= start.minuteEt && mm < POLICY.CUTOFFS_ET.evidenceFreezeMin);
  if (target && !completedThisTarget && !startedThisTarget && taskDispatchable("ingest", mm) && startDue) {
    enqueue.push({ task: "ingest", dedupeId: `${accountId}_${target}`, accountId, priority: 300,
      runId: JOBS.runIdFor({ task: "ingest", accountId, tradingDate: target }),
      payload: { accountId, tradingDate: target, perSweep: POLICY.CUTOFFS_ET.ingest.perSweep }, sessionDate: target });
    reasons.push(`overnight ingest pass for ${target} due (start ${start.minuteEt} min ET, measured pass ${Math.round(start.passMs / 60000)} min)`);
  } else if (target && !completedThisTarget && !startedThisTarget) {
    reasons.push(`ingest for ${target} not due (starts at ${start.minuteEt} min ET${start.previousEvening ? " the previous evening" : ""})`);
  }

  /* The Manager Meeting: from the freeze until the holding hard deadline,
     once per trading date; a yielded continuation is a resumable job and
     needs no new enqueue. */
  if (session.tradingDay && taskDispatchable("premarket_manager", mm) && mm >= POLICY.CUTOFFS_ET.evidenceFreezeMin) {
    if (managerPaused) reasons.push("manager paused — no Manager Meeting enqueued");
    else if (ctrl.lastManagerRunDate === session.date) reasons.push(`Manager Meeting for ${session.date} already ran`);
    else {
      enqueue.push({ task: "premarket_manager", dedupeId: `${accountId}_${session.date}`, accountId, priority: 200,
        runId: JOBS.runIdFor({ task: "premarket_manager", accountId, tradingDate: session.date }),
        payload: { accountId, tradingDate: session.date, reason: "SCHEDULED_PREMARKET" }, sessionDate: session.date });
      reasons.push(`Manager Meeting for ${session.date} due after the ${POLICY.CUTOFFS_ET.evidenceFreezeMin} min ET freeze`);
    }
  }

  if (session.tradingDay && taskDispatchable("event_ingest",mm) && !managerPaused) {
    enqueue.push({task:"event_ingest",dedupeId:`${accountId}_${session.date}_${Math.floor(mm/10)}`,accountId,priority:280,
      payload:{accountId,tradingDate:session.date,slot:Math.floor(mm/10)},sessionDate:session.date});
  }

  /* Execution: every minute inside the execute window on a trading day,
     unless the executor is paused for safety. Never a model call. */
  if (session.tradingDay && taskDispatchable("execute", mm) && !executorPaused) {
    enqueue.push({ task: "execute", dedupeId: `${accountId}_${session.date}_${mm}`, accountId, priority: 10,
      payload: { accountId, tradingDate: session.date, minuteEt: mm }, sessionDate: session.date });
  } else if (session.tradingDay && executorPaused) reasons.push("executor paused for safety — reconciliation only");

  /* Post-close and archive: after the official close and the finalization buffer. */
  const finalization = M.dailyFinalizationState(session);
  if (session.tradingDay && finalization.ready && taskDispatchable("postclose", mm) && ctrl.lastPostcloseDate !== session.date) {
    enqueue.push({ task: "postclose", dedupeId: `${accountId}_${session.date}`, accountId, priority: 400,
      payload: { accountId, tradingDate: session.date }, sessionDate: session.date });
    reasons.push(`post-close marks and outcomes due for ${session.date}`);
  }
  if (session.tradingDay && finalization.ready && taskDispatchable("archive", mm) && ctrl.lastArchiveDate !== session.date) {
    enqueue.push({ task: "archive", dedupeId: `${accountId}_${session.date}`, accountId, priority: 500,
      payload: { accountId, tradingDate: session.date }, sessionDate: session.date });
  }
  return { enqueue, reasons };
}

/** Launch one due job: sign a single-use nonce bound to the next attempt and
 *  POST to the task's target function. A dispatch that never reached the
 *  worker leaves the job due for the next tick. */
async function dispatchJob(job) {
  const spec = JOBS.taskFor(job.task);
  if (!spec) return { jobId: job.jobId, error: "unknown task" };
  const attempt = (Number(job.attempts) || 0) + 1;
  const jref = A.col(A.COL.jobs).doc(job.jobId);
  const nonce = await JOBS.issueWorkerNonce({ jobId: job.jobId, task: job.task, targetFunction: spec.targetFunction,
    attempt, payloadHash: job.payloadHash || JOBS.payloadHash(job.payload || {}) });
  await jref.set({ dispatchedAtMs: Date.now(), dispatchAttempts: A.FV.increment(1), lastNonceId: nonce.nonceId }, { merge: true });
  let res;
  try {
    res = await fetch(`${baseUrl()}/.netlify/functions/${spec.targetFunction}`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ jobId: job.jobId, task: job.task, nonce: nonce.token, payload: job.payload || {} }),
    });
  } catch (e) { res = { ok: false, status: 0, thrown: String(e && e.message || e).slice(0, 120) }; }
  if (!res.ok) {
    await jref.set({ lastDispatchError: { status: res.status, thrown: res.thrown || null, atMs: Date.now() },
      dueAtMs: Date.now() + 60000 }, { merge: true });
  }
  return { jobId: job.jobId, task: job.task, targetFunction: spec.targetFunction, attempt, upstream: res.status };
}

async function runManagerEngine(ctrl, session, startedAt) {
  const decided = decideManager(ctrl, session, startedAt);
  const enqueued = [];
  for (const item of decided.enqueue) {
    try { enqueued.push(await JOBS.enqueueOnce({ ...item, createdBy: "investorKick" })); }
    catch (e) { enqueued.push({ task: item.task, error: String(e.message).slice(0, 120) }); }
  }
  const due = await JOBS.dueJobs({ nowMs: Date.now(), engine: "manager" });
  const plan = JOBS.dispatchPlan(due, { nowMs: Date.now(), startedAtMs: startedAt });
  const dispatched = [];
  for (const job of plan.chosen) {
    if (Date.now() - startedAt > JOBS.DISPATCH_BUDGET_MS) { plan.deferred.push({ jobId: job.jobId, reason: "deadline" }); break; }
    try { dispatched.push(await dispatchJob(job)); }
    catch (e) { dispatched.push({ jobId: job.jobId, error: String(e.message).slice(0, 160) }); }
  }
  return { engine: "manager", enqueued, dispatched, deferred: plan.deferred, due: due.length, reasons: decided.reasons };
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
    /* Which engine holds the writer epoch decides what this tick may launch. */
    if (engineMode(ctrl.raw) === "manager") {
      const out = await runManagerEngine(ctrl.raw, session, startedAt);
      const summary = { ok: true, ...out,
        session: { phase: session.phase, tradingDay: session.tradingDay, date: session.date },
        operatingState: ctrl.operatingState, elapsedMs: Date.now() - startedAt };
      console.log("investorKick", JSON.stringify(redact(summary)));
      return { statusCode: 200, body: JSON.stringify(summary) };
    }
    const { tasks, reasons } = decide(ctrl, session, startedAt);

    const dispatched = [];
    for (const t of tasks) {
      try { dispatched.push(await dispatch(t, ctrl, session)); }
      catch (e) { dispatched.push({ task: t, error: String(e.message).slice(0, 160) }); }
    }

    const summary = {
      ok: true, engine: "legacy", dispatched, reasons,
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
  } finally {
    try { await require("./_investorEvals").Simulator.create().schedule({dispatch:dispatchJob}); } catch (error) { console.error("Simulation dispatch failed", String(error.code || error.message)); }
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
exports.DEFAULT_PLAN_MODE = DEFAULT_PLAN_MODE;
exports.FULL_SCAN_CADENCE_VERSION = FULL_SCAN_CADENCE_VERSION;
exports.resolvePlanMode = resolvePlanMode;
exports.effectivePlanMode = effectivePlanMode;
exports.jobBlocksDispatch = jobBlocksDispatch;
exports.TASK_WINDOWS_ET = TASK_WINDOWS_ET;
exports.dispatchWindow = dispatchWindow;
exports.taskDispatchable = taskDispatchable;
exports.PLAN_EARLIEST_MIN = PLAN_EARLIEST_MIN;
exports.PLAN_LATEST_MIN = PLAN_LATEST_MIN;
exports.engineMode = engineMode;
exports.ingestStartMinuteEt = ingestStartMinuteEt;
exports.nextTradingDate = nextTradingDate;
exports.decideManager = decideManager;
exports.dispatchJob = dispatchJob;
exports.runManagerEngine = runManagerEngine;
