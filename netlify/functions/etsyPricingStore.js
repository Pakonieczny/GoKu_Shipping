// netlify/functions/etsyPricingStore.js
// Persistent per-listing state for the Etsy Pricing Console, stored in
// Firestore collection "EtsyPricing_Listings" (doc id = Etsy listing_id).
//
// Reuses the site's shared firebaseAdmin.js initialization.
//
// Actions (POST JSON):
//   { action:"getAll" }                     -> { docs: { [listing_id]: data } }
//   { action:"set", id, patch }             -> { ok:true } (merge write)
//
// Stored fields (all optional, written by the console):
//   chain_type ("regular"|"beady"), chain_set, engraving, engrave_set,
//   batched, last_batch {at, ok, error}, health {error_count, warning_count,
//   product_count, min_price, max_price}, scanned, approval {mode, at, hash},
//   original_inventory (first pre-write snapshot, for recall), original_saved,
//   original_snapshot_hash, last_save {at, verified}, title, updated_at

const admin = require("./firebaseAdmin");

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "POST,OPTIONS"
};

function json(statusCode, body) {
  return { statusCode, headers: CORS, body: JSON.stringify(body) };
}

const COLLECTION = "EtsyPricing_Listings";

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event.httpMethod !== "POST") return json(405, { error: "Method not allowed" });

  let body;
  try { body = JSON.parse(event.body || "{}"); }
  catch { return json(400, { error: "Request body is not valid JSON." }); }

  const db = admin.firestore();

  try {
    if (body.action === "getAll") {
      const snap = await db.collection(COLLECTION).get();
      const docs = {};
      snap.forEach(d => { docs[d.id] = d.data(); });
      return json(200, { docs });
    }

    if (body.action === "set") {
      const id = String(body.id || "").trim();
      if (!/^\d+$/.test(id)) return json(400, { error: "Missing or invalid id" });
      const patch = body.patch;
      if (!patch || typeof patch !== "object") return json(400, { error: "Missing patch object" });
      if (JSON.stringify(patch).length > 500000) return json(400, { error: "Patch exceeds the 500KB limit." });
      patch.updated_at = Date.now();
      await db.collection(COLLECTION).doc(id).set(patch, { merge: true });
      return json(200, { ok: true });
    }

    /* ---- Batch run management (collection EtsyPricing_Runs) ---- */
    if (body.action === "startRun") {
      const ids = Array.isArray(body.ids) ? body.ids.map(String).filter(x => /^\d+$/.test(x)) : [];
      if (!ids.length) return json(400, { error: "No listing ids supplied." });
      // Refuse a second concurrent run.
      const active = await db.collection("EtsyPricing_Runs").where("status", "in", ["queued", "running", "paused"]).limit(1).get();
      if (!active.empty) return json(409, { error: "A batch run is already in progress (or paused).", run_id: active.docs[0].id });
      const ref = await db.collection("EtsyPricing_Runs").add({
        status: "queued", ids, total: ids.length, done: 0, ok: 0, fail: 0,
        current: "", errors: [], stop: false, created_at: Date.now(), updated_at: Date.now()
      });
      return json(200, { run_id: ref.id });
    }
    if (body.action === "getRun") {
      const snap = await db.collection("EtsyPricing_Runs").doc(String(body.run_id || "")).get();
      if (!snap.exists) return json(404, { error: "Run not found." });
      const d = snap.data(); delete d.ids;
      return json(200, { run: d, run_id: snap.id });
    }
    if (body.action === "activeRun") {
      const active = await db.collection("EtsyPricing_Runs").where("status", "in", ["queued", "running", "paused"]).limit(1).get();
      if (active.empty) return json(200, { run: null });
      const d = active.docs[0].data(); delete d.ids;
      return json(200, { run: d, run_id: active.docs[0].id });
    }
    if (body.action === "pauseRun") {
      await db.collection("EtsyPricing_Runs").doc(String(body.run_id || "")).set({ paused: true, updated_at: Date.now() }, { merge: true });
      return json(200, { ok: true });
    }
    if (body.action === "resumeRun") {
      await db.collection("EtsyPricing_Runs").doc(String(body.run_id || "")).set({ paused: false, status: "running", updated_at: Date.now() }, { merge: true });
      return json(200, { ok: true });
    }
    if (body.action === "stopRun") {
      await db.collection("EtsyPricing_Runs").doc(String(body.run_id || "")).set({ stop: true, updated_at: Date.now() }, { merge: true });
      return json(200, { ok: true });
    }

    if (body.action === "log") {
      const e = body.entry || {};
      await db.collection("EtsyPricing_Log").add({
        at: Date.now(),
        listing_id: String(e.listing_id || ""),
        title: String(e.title || "").slice(0, 200),
        type: String(e.type || "event").slice(0, 40),
        ok: e.ok !== false,
        detail: String(e.detail || "").slice(0, 800)
      });
      return json(200, { ok: true });
    }
    if (body.action === "getLog") {
      let q = db.collection("EtsyPricing_Log").orderBy("at", "desc").limit(Math.min(Number(body.limit) || 300, 500));
      if (body.before) q = q.where("at", "<", Number(body.before));
      const snap = await q.get();
      const entries = [];
      snap.forEach(d => entries.push({ id: d.id, ...d.data() }));
      return json(200, { entries });
    }
    if (body.action === "saveServerToken") {
      const t = body.token || {};
      if (!t.refresh_token) return json(400, { error: "Missing refresh_token" });
      await db.doc("EtsyPricing_Config/etsyOauth").set({
        access_token: String(t.access_token || ""),
        refresh_token: String(t.refresh_token),
        expires_at: Number(t.expires_at) || 0,
        updated_at: Date.now()
      }, { merge: true });
      return json(200, { ok: true });
    }
    if (body.action === "apiUsage") {
      const key = new Intl.DateTimeFormat("en-CA", { timeZone: "America/Toronto", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(Date.now() + 60000));
      const snap = await db.collection("EtsyPricing_ApiUsage").doc(key).get();
      const d = snap.exists ? snap.data() : {};
      return json(200, {
        date: key,
        count: Number(d.count || 0),
        count_since: d.count_since || null, // when THIS counter started — may be mid-day if the tracking code was deployed partway through today
        max_qps: Number(d.max_qps || 0),
        etsy_limit_per_day: d.etsy_limit_per_day != null ? Number(d.etsy_limit_per_day) : null,
        etsy_remaining_today: d.etsy_remaining_today != null ? Number(d.etsy_remaining_today) : null,
        budget: 2500, qps_cap: 2.5
      });
    }
    if (body.action === "getSchedule") {
      const snap = await db.doc("EtsyPricing_Config/schedule").get();
      return json(200, { schedule: snap.exists ? snap.data() : null });
    }
    if (body.action === "setSchedule") {
      const p = body.schedule || {};
      const doc = {
        enabled: !!p.enabled,
        next_run_at: Number(p.next_run_at) || 0,
        repeat: ["once", "daily", "weekly"].includes(p.repeat) ? p.repeat : "once",
        label: String(p.label || "").slice(0, 120),
        updated_at: Date.now()
      };
      if (doc.enabled && doc.next_run_at < Date.now() - 60000) return json(400, { error: "Scheduled time is in the past." });
      await db.doc("EtsyPricing_Config/schedule").set(doc, { merge: true });
      return json(200, { ok: true, schedule: doc });
    }

    return json(400, { error: "Unknown action: " + body.action });
  } catch (err) {
    return json(500, { error: err.message });
  }
};
