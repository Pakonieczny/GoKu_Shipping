/*  netlify/functions/etsyPricingScheduleCron.js
 *
 *  Scheduled trigger for the Etsy Pricing Console's batch runs, following
 *  the same pattern as the etsyMail crons: the cron fires every 5 minutes
 *  (declared in netlify.toml), reads a Firestore config doc, and only acts
 *  when an operator-armed schedule is due. Default-off — with no schedule
 *  doc, or enabled:false, it does nothing.
 *
 *  ═══ DECISION TABLE ═══════════════════════════════════════════════════
 *
 *    schedule doc      | active run?  | action
 *    ------------------+--------------+---------------------------------
 *    missing/disabled  | any          | skip
 *    due in the future | any          | skip
 *    due now           | yes          | skip this tick (retry next tick)
 *    due now           | no           | build queue -> start run ->
 *                      |              | advance (daily/weekly) or disarm (once)
 *
 *  netlify.toml addition required:
 *    [functions."etsyPricingScheduleCron"]
 *      schedule = "0-59/5 * * * *"   # every 5 minutes
 */

const admin = require("./firebaseAdmin");

const SITE = (process.env.URL || "").replace(/\/$/, "");
const FN = SITE + "/.netlify/functions";
const DAY = 86400000;

const DAILY_BUDGET = 2500; // keep in sync with etsyRateLimiter.js

function torontoDayKey() {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "America/Toronto", year: "numeric", month: "2-digit", day: "2-digit" })
    .format(new Date(Date.now() + 60000));
}

exports.handler = async () => {
  const db = admin.firestore();
  const schedRef = db.doc("EtsyPricing_Config/schedule");
  // Heartbeat: proves (to the console UI) that the cron is actually armed in
  // netlify.toml — if last_tick never updates, the toml entry is missing.
  await schedRef.set({ last_tick: Date.now() }, { merge: true }).catch(() => {});

  // Auto-resume a run that paused on budget exhaustion, once the Toronto day
  // rolled over and budget is available again.
  try {
    const pausedQ = await db.collection("EtsyPricing_Runs").where("status", "==", "paused").limit(3).get();
    for (const doc of pausedQ.docs) {
      const r = doc.data();
      if (!r.budget_paused) continue;
      const usage = await db.collection("EtsyPricing_ApiUsage").doc(torontoDayKey()).get();
      const used = usage.exists ? Number(usage.data().count || 0) : 0;
      if (used < DAILY_BUDGET * 0.9) {
        await doc.ref.set({ status: "running", paused: false, budget_paused: false, stop_reason: "", current: "Auto-resumed after daily budget reset", updated_at: Date.now() }, { merge: true });
        await fetch(FN + "/etsyPricingBatch-background", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ run_id: doc.id }) }).catch(() => {});
        await db.collection("EtsyPricing_Log").add({ at: Date.now(), listing_id: "", title: "", type: "schedule", ok: true, detail: "Auto-resumed budget-paused run " + doc.id + " after the daily reset." }).catch(() => {});
      }
    }
  } catch (_) { /* resume is best-effort; scheduling below must still run */ }

  const snap = await schedRef.get();
  if (!snap.exists) return { statusCode: 200, body: "no schedule" };
  const sched = snap.data();
  if (!sched.enabled) return { statusCode: 200, body: "disabled" };
  if (Date.now() < Number(sched.next_run_at || 0)) return { statusCode: 200, body: "not due" };

  // Don't stack on top of an active/paused run — retry on the next tick.
  const active = await db.collection("EtsyPricing_Runs").where("status", "in", ["queued", "running", "paused"]).limit(1).get();
  if (!active.empty) return { statusCode: 200, body: "run in progress; deferring" };

  // Build the queue: prepared (both toggles set by the user) and not yet
  // batched — which by design includes listings that FAILED in a previous
  // run, so they get re-processed automatically.
  const all = await db.collection("EtsyPricing_Listings").get();
  const ids = [];
  all.forEach(d => { const x = d.data(); if (x.chain_set && x.engrave_set && !x.batched) ids.push(d.id); });

  const advance = async () => {
    if (sched.repeat === "daily") await schedRef.set({ next_run_at: Number(sched.next_run_at) + DAY, updated_at: Date.now() }, { merge: true });
    else if (sched.repeat === "weekly") await schedRef.set({ next_run_at: Number(sched.next_run_at) + 7 * DAY, updated_at: Date.now() }, { merge: true });
    else await schedRef.set({ enabled: false, updated_at: Date.now() }, { merge: true });
  };

  if (!ids.length) {
    await schedRef.set({ last_result: "Scheduled run at " + new Date().toISOString() + " found no prepared listings; nothing to do.", updated_at: Date.now() }, { merge: true });
    await advance();
    return { statusCode: 200, body: "nothing to batch" };
  }

  const ref = await db.collection("EtsyPricing_Runs").add({
    status: "queued", ids, total: ids.length, done: 0, ok: 0, fail: 0, consec_fail: 0,
    current: "", errors: [], stop: false, paused: false,
    started_by: "schedule", created_at: Date.now(), updated_at: Date.now()
  });
  await fetch(FN + "/etsyPricingBatch-background", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ run_id: ref.id })
  }).catch(() => {});
  await schedRef.set({ last_result: "Started scheduled run " + ref.id + " with " + ids.length + " listings.", last_started_at: Date.now(), updated_at: Date.now() }, { merge: true });
  try { await db.collection("EtsyPricing_Log").add({ at: Date.now(), listing_id: "", title: "", type: "schedule", ok: true, detail: "Scheduled trigger started run with " + ids.length + " listings." }); } catch (_) {}
  await advance();
  return { statusCode: 200, body: "started " + ref.id };
};
