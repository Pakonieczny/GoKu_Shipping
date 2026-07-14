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

// CORS: lets the sync/scan be kicked from the storefront's browser console (the token check
// below still gates it). Server-to-server calls from the Kick are unaffected.
const BG_ALLOWED = ["https://britesjewelry.com", "https://www.britesjewelry.com", "https://goldenspike.app", "https://brites-adwords.goldenspike.app"];
function bgCors(event) {
  const origin = (event.headers && (event.headers.origin || event.headers.Origin)) || "";
  return {
    "Access-Control-Allow-Origin": BG_ALLOWED.includes(origin) ? origin : BG_ALLOWED[0],
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Content-Type": "application/json"
  };
}

exports.handler = async (event) => {
  const CORS = bgCors(event);
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  const log = [];
  const t0 = startedAt();
  const over = () => Date.now() - t0 > DEADLINE_MS;

  // auth (defence-in-depth)
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch {}
  if ((process.env.EDIT_PASSCODE || "") && body.token !== process.env.EDIT_PASSCODE) {
    return { statusCode: 401, headers: CORS, body: JSON.stringify({ error: "unauthorized" }) };
  }

  const ctrl = await E.control();

  const tasks = Array.isArray(body.tasks) && body.tasks.length
    ? body.tasks
    : ["anomaly", "monthly", "conversions", "adjustments", "measure", "mine", "prune", "budgets", "ceiling", "events", "pruneLedger"];

  // Read-only tasks (no Google Ads mutations) are allowed even when the kill switch is off.
  const READONLY = new Set(["scanOpportunities", "pmaxGenerate", "pmaxBackfillImages", "pmaxUpgradeAdStrength", "pruneLedger", "bestSellers", "diagnostics", "distill", "generate"]); // drafts/read-only analysis never mutate Google Ads before approval
  const allReadOnly = tasks.every(t => READONLY.has(t));

  // HARD KILL SWITCH — blocks anything that could mutate. Read-only analysis still runs.
  if (!ctrl.enabled && !allReadOnly) {
    log.push("autopilot DISABLED (kill switch) — no mutations this run");
    return { statusCode: 200, headers: CORS, body: JSON.stringify({ status: "dormant", log }) };
  }

  const result = {};

  // anomaly FIRST — if it trips, it flips enabled=false; re-read and bail.
  if (tasks.includes("anomaly") && !over()) {
    try { result.anomaly = await E.anomalyCheck({ ctrl }); log.push("anomaly: " + JSON.stringify(result.anomaly)); }
    catch (e) { log.push("anomaly ERROR " + e.message); }
    if (result.anomaly && result.anomaly.tripped) {
      log.push("CIRCUIT BREAKER TRIPPED — autopilot disabled, halting run");
      return { statusCode: 200, headers: CORS, body: JSON.stringify({ status: "tripped", result, log }) };
    }
  }

  // monthly hard cap — if month-to-date spend hit the limit, it pauses campaigns + disables; bail.
  if (tasks.includes("monthly") && !over()) {
    try { result.monthly = await E.monthlySpendGuard({ ctrl }); log.push("monthly: " + JSON.stringify(result.monthly)); }
    catch (e) { log.push("monthly ERROR " + e.message); }
    if (result.monthly && result.monthly.tripped) {
      log.push("MONTHLY CAP REACHED — campaigns paused, autopilot disabled, halting run");
      return { statusCode: 200, headers: CORS, body: JSON.stringify({ status: "capped", result, log }) };
    }
  }

  for (const task of tasks) {
    if (over()) { log.push("time budget reached — deferring rest to next run"); break; }
    try {
      if (task === "conversions") { result.conversions = await E.uploadConversions({ ctrl }); }
      else if (task === "scanOpportunities") { const sc = await E.opportunitiesWithStatus({ force: true, runId: body.scanRunId || null }); result.scanOpportunities = { n: (sc.opportunities || []).length, pmax: (sc.pmaxList || []).length, pmaxError: sc.pmaxError || null, runId: body.scanRunId || null, auditStatus: sc.scanAudit && sc.scanAudit.status || null }; }
      else if (task === "pmaxGenerate") {
        const gId = String(body.genId || Date.now());
        try { await E.setGenStatus(gId, { phase: "running", startedAt: Date.now(), kind: "pmax" }); } catch (e) {}
        try {
          const out = await E.generatePmaxApproval({ handle: body.handle, dailyBudget: body.dailyBudget, targetRoas: body.targetRoas, days: body.days,
            itemIds:Array.isArray(body.itemIds)?body.itemIds.slice(0,30):[],productTitles:Array.isArray(body.productTitles)?body.productTitles.slice(0,10):[],
            feedLabel:body.feedLabel||null,searchThemes:Array.isArray(body.searchThemes)?body.searchThemes.slice(0,25):[],offerDetails:Array.isArray(body.offerDetails)?body.offerDetails.slice(0,30):[] });
          result.pmax = out;
          try { await E.setGenStatus(gId, { ok: true, ...out }); } catch (e) {}
        } catch (e) {
          const msg = String(e.message || e).slice(0, 400);
          result.pmax = { error: msg };
          try { await E.setGenStatus(gId, { ok: false, error: msg }); } catch (e2) {}
        }
      }
      else if (task === "pmaxBackfillImages") {
        // One-time (re-runnable) job: re-images already-ENABLED PMax asset groups with
        // vision-selected shots. Discovery + upload run here; the actual creative swap
        // is queued to Approvals per asset group — never applied directly from this task.
        const gId = String(body.genId || Date.now());
        try { await E.setGenStatus(gId, { phase: "running", startedAt: Date.now(), kind: "pmax-backfill-images" }); } catch (e) {}
        try {
          const out = await E.backfillPmaxCreative({ ctrl,
            campaignIds: Array.isArray(body.campaignIds) ? body.campaignIds.slice(0, 50) : [],
            onProgress: async (p) => { try { await E.setGenStatus(gId, { phase: "running", ...p, heartbeatAt: Date.now() }); } catch (e) {} } });
          result.pmaxBackfillImages = out;
          try { await E.setGenStatus(gId, { ok: true, ...out }); } catch (e) {}
        } catch (e) {
          const msg = String(e.message || e).slice(0, 400);
          result.pmaxBackfillImages = { error: msg };
          try { await E.setGenStatus(gId, { ok: false, error: msg }); } catch (e2) {}
        }
      }
      else if (task === "pmaxUpgradeAdStrength") {
        // One-time (re-runnable) job: tops up headlines/long headlines/descriptions/
        // sitelinks on already-LIVE PMax campaigns to the quality target — pure ADD,
        // nothing existing is removed. Queued to Approvals per campaign, same as backfill.
        const gId = String(body.genId || Date.now());
        try { await E.setGenStatus(gId, { phase: "running", startedAt: Date.now(), kind: "pmax-upgrade-ad-strength" }); } catch (e) {}
        try {
          const out = await E.upgradePmaxAdStrength({ ctrl,
            campaignIds: Array.isArray(body.campaignIds) ? body.campaignIds.slice(0, 50) : [],
            onProgress: async (p) => { try { await E.setGenStatus(gId, { phase: "running", ...p, heartbeatAt: Date.now() }); } catch (e) {} } });
          result.pmaxUpgradeAdStrength = out;
          try { await E.setGenStatus(gId, { ok: true, ...out }); } catch (e) {}
        } catch (e) {
          const msg = String(e.message || e).slice(0, 400);
          result.pmaxUpgradeAdStrength = { error: msg };
          try { await E.setGenStatus(gId, { ok: false, error: msg }); } catch (e2) {}
        }
      }
      else if (task === "diagnostics") {
        // Self-reporting run: the console polls diag-<runId> and gets either
        // {ok:true, generatedAt} or {ok:false, error} — no more silent deaths.
        const dRunId = String(body.runId || Date.now());
        try { await E.setGenStatus("diag-" + dRunId, { phase: "running", startedAt: Date.now(), campaignId: body.campaignId || null }); } catch (e) {}
        try {
          const dOut = await E.runDiagnostics({ campaignId: body.campaignId || null,
            onProgress: async (done, total) => { try { await E.setGenStatus("diag-" + dRunId, { phase: "running", done, total, heartbeatAt: Date.now() }); } catch (e) {} } });
          result.diagnostics = { generatedAt: dOut.generatedAt, aiError: dOut.aiError || null };
          try { await E.setGenStatus("diag-" + dRunId, { ok: true, generatedAt: dOut.generatedAt, aiError: dOut.aiError || null, tookMs: dOut.tookMs || null }); } catch (e) {}
        } catch (e) {
          const msg = String(e.message || e).slice(0, 400);
          result.diagnostics = { error: msg };
          try { await E.setGenStatus("diag-" + dRunId, { ok: false, error: msg }); } catch (e2) {}
        }
        // Learning loop AFTER the status doc lands — distill latency must not
        // keep the console waiting. Best-effort.
        try { result.distill = await E.distillLessons(); } catch (e) { result.distill = { error: String(e.message || e).slice(0, 200) }; }
      }
      else if (task === "distill") { result.distill = await E.distillLessons(); }
      else if (task === "generate") {
        // Campaign generation (keyword research + high-effort copy) outruns the
        // 26s gateway, so it runs here; the console polls the status doc.
        const genId = String(body.genId || Date.now());
        let out;
        try {
          out = await E.generateForCollection(body.coll, body.event, body.budget,
            { ctrl, startDate: body.startDate, endDate: body.endDate, countries: body.countries,
              maxCpc: body.maxCpc, peakDate: body.peakDate, smartBidding: body.smartBidding });
        } catch (e) { out = { ok: false, reason: String(e.message || e).slice(0, 300) }; }
        try { await E.setGenStatus(genId, JSON.parse(JSON.stringify(out || {}))); } catch (e) {}
        result.generate = { genId, ok: !!(out && out.ok) };
      }
      else if (task === "adjustments") { result.adjustments = await E.uploadConversionAdjustments({ ctrl }); }
      else if (task === "measure") { result.measure = { campaigns: (await E.measure()).length }; }
      else if (task === "mine")     { result.mine = await E.mineSearchTerms({ ctrl }); }
      else if (task === "prune")    { result.prune = await E.pruneAssets({ ctrl }); }
      else if (task === "budgets")  { result.budgets = await E.reallocateBudgets({ ctrl }); }
      else if (task === "ceiling")  { result.ceiling = await E.enforceBudgetCeiling({ ctrl }); }
      else if (task === "events")   { await runEvents(ctrl, log); result.events = "ok"; }
      else if (task === "pruneLedger") { result.pruneLedger = await E.clearLedger({ keep: 500 }); try { result.pruneOrders = await E.clearOrderLog({ keep: 1000 }); } catch (e) {} }
      else if (task === "bestSellers") { result.bestSellers = await require("./shopifyEditor").syncBestSellersCollection({}); }
      if (result[task] !== undefined && task !== "events") log.push(task + ": " + JSON.stringify(result[task]));
    } catch (e) { log.push(task + " ERROR " + e.message); }
  }

  return { statusCode: 200, headers: CORS, body: JSON.stringify({ status: "ran", dryRun: !!ctrl.dryRun, result, log }) };
};
