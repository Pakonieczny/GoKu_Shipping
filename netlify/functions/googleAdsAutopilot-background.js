// netlify/functions/googleAdsAutopilot-background.js
// ─────────────────────────────────────────────────────────────────────────────
// Heavy worker for the Ad Autopilot. Netlify *background* function: returns 202
// immediately and runs up to ~15 minutes — the same reason verifyCharmSets has a
// "-background" twin. The hourly kicker POSTs here with the tasks due this run.
//
// Does nothing on its own schedule; it is triggered. All real logic lives in the
// engine (./googleAdsAutopilot). This file is just orchestration + time budget +
// the hard safety check (kill switch ⇒ no-op).
//
// POST body: { tasks: ["conversions","measure","mine","prune","budgets","events","anomaly"], token }
//   token must equal EDIT_PASSCODE (defence-in-depth; the kicker passes it).
// ─────────────────────────────────────────────────────────────────────────────

const E = require("./googleAdsAutopilot");

const DEADLINE_MS = 13 * 60 * 1000; // leave headroom under Netlify's 15-min cap
const startedAt = () => Date.now();

async function runEvents(ctrl, log) {
  const due = await E.dueEvents();
  if (!due.length) { log.push("events: none due"); return; }
  for (const d of due) {
    try {
      const assets = await E.generateRSAAssets(d.coll, d.event);
      if (!assets) { log.push(`events: ${d.coll.handle}/${d.event.label} — generation rejected (brand-safety/min)`); continue; }
      const dailyBudget = Number(process.env.GADS_NEW_CAMPAIGN_BUDGET || 8);
      const { ops, tag } = E.buildSearchCampaignOps(d.coll, d.event, assets, { dailyBudget });
      // Always queue new campaigns for human go-live (they are created PAUSED anyway).
      const id = await E.enqueueApproval({
        type: "creative", vetted: !!ctrl.autoApproveVettedTemplates,
        summary: `NEW Search campaign “${tag}” for ${d.event.label} (${d.event.daysLeft}d out) — ${assets.headlines.length} headlines, starts PAUSED`,
        payload: { mutateOperations: ops, finalCollection: d.coll.handle, event: d.event.label },
        experimentId: tag
      });
      log.push(`events: queued campaign ${tag} (approval ${id})`);
    } catch (e) { log.push(`events: ${d.coll.handle} ERROR ${e.message}`); }
  }
}

exports.handler = async (event) => {
  const log = [];
  const t0 = startedAt();
  const over = () => Date.now() - t0 > DEADLINE_MS;

  // auth (defence-in-depth)
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch {}
  if ((process.env.EDIT_PASSCODE || "") && body.token !== process.env.EDIT_PASSCODE) {
    return { statusCode: 401, body: JSON.stringify({ error: "unauthorized" }) };
  }

  const ctrl = await E.control();

  const tasks = Array.isArray(body.tasks) && body.tasks.length
    ? body.tasks
    : ["anomaly", "conversions", "adjustments", "measure", "mine", "prune", "budgets", "events", "pruneLedger"];

  // Read-only tasks (no Google Ads mutations) are allowed even when the kill switch is off.
  const READONLY = new Set(["scanOpportunities", "pruneLedger"]);
  const allReadOnly = tasks.every(t => READONLY.has(t));

  // HARD KILL SWITCH — blocks anything that could mutate. Read-only analysis still runs.
  if (!ctrl.enabled && !allReadOnly) {
    log.push("autopilot DISABLED (kill switch) — no mutations this run");
    return { statusCode: 200, body: JSON.stringify({ status: "dormant", log }) };
  }

  const result = {};

  // anomaly FIRST — if it trips, it flips enabled=false; re-read and bail.
  if (tasks.includes("anomaly") && !over()) {
    try { result.anomaly = await E.anomalyCheck({ ctrl }); log.push("anomaly: " + JSON.stringify(result.anomaly)); }
    catch (e) { log.push("anomaly ERROR " + e.message); }
    if (result.anomaly && result.anomaly.tripped) {
      log.push("CIRCUIT BREAKER TRIPPED — autopilot disabled, halting run");
      return { statusCode: 200, body: JSON.stringify({ status: "tripped", result, log }) };
    }
  }

  for (const task of tasks) {
    if (over()) { log.push("time budget reached — deferring rest to next run"); break; }
    try {
      if (task === "conversions") { result.conversions = await E.uploadConversions({ ctrl }); }
      else if (task === "scanOpportunities") { result.scanOpportunities = { n: ((await E.opportunitiesWithStatus({ force: true })).opportunities || []).length }; }
      else if (task === "adjustments") { result.adjustments = await E.uploadConversionAdjustments({ ctrl }); }
      else if (task === "measure") { result.measure = { campaigns: (await E.measure()).length }; }
      else if (task === "mine")     { result.mine = await E.mineSearchTerms({ ctrl }); }
      else if (task === "prune")    { result.prune = await E.pruneAssets({ ctrl }); }
      else if (task === "budgets")  { result.budgets = await E.reallocateBudgets({ ctrl }); }
      else if (task === "events")   { await runEvents(ctrl, log); result.events = "ok"; }
      else if (task === "pruneLedger") { result.pruneLedger = await E.clearLedger({ keep: 500 }); }
      if (result[task] !== undefined && task !== "events") log.push(task + ": " + JSON.stringify(result[task]));
    } catch (e) { log.push(task + " ERROR " + e.message); }
  }

  return { statusCode: 200, body: JSON.stringify({ status: "ran", dryRun: !!ctrl.dryRun, result, log }) };
};
