// netlify/functions/etsyPricingStore.js
// Persistent per-listing state for the Etsy Pricing Console, stored in
// Firestore collection "EtsyPricing_Listings" (doc id = Etsy listing_id).
//
// Reuses the site's shared firebaseAdmin.js initialization.
//
// Actions (POST JSON):
//   { action:"getAll" }                     -> { docs: { [listing_id]: data }, count, asOf }   (the fields the console reads)
//   { action:"getAll", since: asOf }        -> { docs: <only listings changed since>, delta:true, asOf }
//   { action:"getAll", full:true }          -> the old answer: every field except the two heavy snapshot fields (a full read)
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

/*  ═══ WHY getAll STOPPED WORKING MID-RUN ════════════════════════════════
 *
 *  getAll returned each document RAW, and every batched listing carries an
 *  `original_inventory` — the full pre-change Etsy inventory kept so a listing
 *  can be rolled back. Measured on a real 12-metal x 4-length Beady matrix
 *  that snapshot is ~21 KB of JSON, so the response grew by ~21 KB for every
 *  listing the batch completed:
 *
 *      100 batched -> 2.1 MB      300 batched -> 6.2 MB      1000 -> 20.7 MB
 *
 *  Netlify caps a function response at 6 MB. Somewhere around 300 batched
 *  listings this handler stopped being able to answer and the gateway returned
 *  a bodiless 502 — surfacing in the console as
 *
 *      "Cloud state could not be loaded: Store error HTTP 502"
 *
 *  in the middle of an autorun, and only ever after enough listings had been
 *  processed. Every byte of it was wasted: the browser reads `original_saved`
 *  (a boolean) and never once reads original_inventory.
 *
 *  ROLLBACK IS UNAFFECTED — it reads the single listing's document directly,
 *  where the snapshot still lives untouched.
 */
const HEAVY_FIELDS = ["original_inventory", "original_snapshot_hash"];

/*  Second line of defence. If some future field bloats this response again,
 *  fall back to exactly what the console reads instead of failing outright —
 *  a trimmed payload keeps the operator working; a 502 does not. */
const CONSOLE_FIELDS = [
  "chain_type", "chain_set", "engraving", "engrave_set", "batched",
  "last_batch", "batch_blocked", "health", "scanned", "approval",
  "last_save", "title", "original_saved", "queue_id", "category",
  "listing_kind", "updated_at"
];
const MAX_PAYLOAD_BYTES = Number(process.env.ETSY_STORE_MAX_BYTES || 4000000);

/*  ═══ COST (FC19, 7 Oct 2026) ═══════════════════════════════════════════
 *
 *  getAll used to read EVERY listing document whole (about 4,700 of them, 21 KB
 *  of original_inventory on each batched one) and then delete the heavy fields
 *  from the answer: the bytes were billed as Firestore egress and the reads as
 *  4,700 document reads, on every page load and about every 14 seconds for as
 *  long as a batch ran in an open console.
 *
 *  Now:
 *   1. Only the fields the console reads are fetched (a field mask), so the
 *      snapshot fields are never read. They stay in the documents untouched;
 *      nothing is moved, copied or deleted, and the rollback still reads the
 *      single document it needs.
 *   2. The masked list is kept in this instance's memory and kept current with
 *      a query for the listings written since the last call (every write here
 *      and in the batch worker stamps updated_at). A call that finds nothing
 *      new costs ONE read. The whole collection is read again only when the
 *      instance starts and then at most every SNAP_FULL_TTL_MS, as a safety net
 *      for a document that was written without updated_at.
 *   3. { since } asks for the changes only, straight from Firestore (no
 *      instance memory), so a polling console downloads a few documents.
 *  The answer to a plain getAll has the same shape as before.               */
const SNAP_FULL_TTL_MS = 30 * 60 * 1000;
const DELTA_MARGIN_MS = 15000;          // covers clock skew between instances and the time a write takes to commit
const lcache = { docs: null, asOf: 0, fullAt: 0, loading: null, deltaBroken: false };

const maskedListings = (db) => db.collection(COLLECTION).select(...CONSOLE_FIELDS);
const newer = (cur, doc) => !cur || Number(doc.updated_at || 0) >= Number(cur.updated_at || 0);

const readMasked = async (db) => { const all = await maskedListings(db).get(); const m = new Map(); all.forEach(d => m.set(d.id, d.data() || {})); return m; };

async function listingSnapshot(db) {
  if (!lcache.docs || lcache.deltaBroken || Date.now() - lcache.fullAt > SNAP_FULL_TTL_MS) {
    // one full read at a time; callers that arrive meanwhile wait for it and then catch up with a delta below
    if (!lcache.loading) {
      const t0 = Date.now();
      lcache.loading = readMasked(db).then(m => { lcache.docs = m; lcache.asOf = t0; lcache.fullAt = t0; })
        .finally(() => { lcache.loading = null; });
    }
    await lcache.loading;
    if (lcache.deltaBroken) return { docs: lcache.docs, asOf: lcache.asOf };
  }
  const t1 = Date.now();
  let changed;
  try { changed = await maskedListings(db).where("updated_at", ">", lcache.asOf - DELTA_MARGIN_MS).get(); }
  catch (e) {
    // The changes query could not run (for example the updated_at index is switched off): this instance reads the masked list
    // whole on every call instead (still without the snapshot fields), exactly as correct, only dearer.
    lcache.deltaBroken = true;
    console.warn("[etsyPricingStore] changes query failed, reading the list whole: " + e.message);
    lcache.docs = await readMasked(db); lcache.asOf = t1; lcache.fullAt = t1;
    return { docs: lcache.docs, asOf: t1 };
  }
  changed.forEach(d => { const o = d.data() || {}; if (newer(lcache.docs.get(d.id), o)) lcache.docs.set(d.id, o); });
  lcache.asOf = Math.max(lcache.asOf, t1);
  return { docs: lcache.docs, asOf: t1 };
}

/*  The run document also holds `ids` (every queued listing id, about 15 bytes each: 70 KB for a full catalogue run),
    which the console never reads; the answer deletes it. These are the other fields, all of them written by this file,
    etsyPricingScheduleCron and etsyPricingBatch-background. */
const RUN_FIELDS = [
  "status", "total", "done", "ok", "fail", "current", "errors", "errors_dropped", "stop", "paused", "consec_fail", "blocked",
  "budget_paused", "stop_reason", "fatal_error", "started_by", "created_at", "updated_at", "finished_at", "remaining_ids"
];

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event.httpMethod !== "POST") return json(405, { error: "Method not allowed" });

  let body;
  try { body = JSON.parse(event.body || "{}"); }
  catch { return json(400, { error: "Request body is not valid JSON." }); }

  const db = admin.firestore();

  try {
    if (body.action === "getAll" && body.full !== true) {
      const since = Number(body.since);
      if (body.since != null && Number.isFinite(since) && since > 0) {
        // Changes only: the listings written since the caller's last answer (its asOf), with a margin.
        const t0 = Date.now();
        let changed;
        try { changed = await maskedListings(db).where("updated_at", ">", Math.min(since, t0) - DELTA_MARGIN_MS).get(); }
        catch (e) {   // no usable changes query: answer the whole masked list (no delta:true, so the console replaces its list with it)
          console.warn("[etsyPricingStore] changes query failed, answering the whole list: " + e.message);
          const s = await listingSnapshot(db), all = {};
          s.docs.forEach((o, id) => { all[id] = o; });
          return json(200, { docs: all, count: s.docs.size, asOf: s.asOf });
        }
        const docs = {};
        changed.forEach(d => { docs[d.id] = d.data() || {}; });
        return json(200, { docs, count: Object.keys(docs).length, delta: true, asOf: t0 });
      }
      const s = await listingSnapshot(db);
      const docs = {};
      s.docs.forEach((o, id) => { docs[id] = o; });
      return json(200, { docs, count: s.docs.size, asOf: s.asOf });
    }

    if (body.action === "getAll") {
      // full:true — the previous answer for any caller that wants every field: a full read of the whole collection.
      const snap = await db.collection(COLLECTION).get();
      const docs = {};
      snap.forEach(d => {
        const o = d.data() || {};
        for (const k of HEAVY_FIELDS) delete o[k];
        docs[d.id] = o;
      });

      let payload = { docs, count: Object.keys(docs).length };
      const bytes = Buffer.byteLength(JSON.stringify(payload));
      if (bytes > MAX_PAYLOAD_BYTES) {
        const lean = {};
        for (const id of Object.keys(docs)) {
          const o = docs[id], t = {};
          for (const k of CONSOLE_FIELDS) if (o[k] !== undefined) t[k] = o[k];
          lean[id] = t;
        }
        payload = { docs: lean, count: Object.keys(lean).length,
                    trimmed: true, trimmed_from_bytes: bytes };
        console.warn("[etsyPricingStore] getAll trimmed to console fields: " +
          bytes + " bytes exceeded " + MAX_PAYLOAD_BYTES);
      }
      return json(200, payload);
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
      const active = await db.collection("EtsyPricing_Runs").where("status", "in", ["queued", "running", "paused"]).limit(1).select("status").get();
      if (!active.empty) return json(409, { error: "A batch run is already in progress (or paused).", run_id: active.docs[0].id });
      const ref = await db.collection("EtsyPricing_Runs").add({
        status: "queued", ids, total: ids.length, done: 0, ok: 0, fail: 0,
        current: "", errors: [], stop: false, created_at: Date.now(), updated_at: Date.now()
      });
      return json(200, { run_id: ref.id });
    }
    /*  The run document is polled every ~3.5 seconds for the whole run and its
        `errors` array only grows. The console renders the last 80 lines, so
        shipping thousands of them on every poll is the same mistake as above,
        one collection over. The permanent record is EtsyPricing_Log.        */
    const trimRun = (d) => {
      delete d.ids;
      if (Array.isArray(d.errors) && d.errors.length > 120) {
        d.errors_total = d.errors.length;
        d.errors = d.errors.slice(-120);
      }
      return d;
    };
    if (body.action === "getRun") {
      // COST: `ids` (every queued listing id) is not read; the answer never carried it.
      const [snap] = await db.getAll(db.collection("EtsyPricing_Runs").doc(String(body.run_id || "")), { fieldMask: RUN_FIELDS });
      if (!snap.exists) return json(404, { error: "Run not found." });
      return json(200, { run: trimRun(snap.data()), run_id: snap.id });
    }
    if (body.action === "activeRun") {
      const active = await db.collection("EtsyPricing_Runs").where("status", "in", ["queued", "running", "paused"]).limit(1).select(...RUN_FIELDS).get();
      if (active.empty) return json(200, { run: null });
      return json(200, { run: trimRun(active.docs[0].data()), run_id: active.docs[0].id });
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
