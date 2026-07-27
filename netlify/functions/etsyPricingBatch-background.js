// netlify/functions/etsyPricingBatch-background.js
// Server-driven batch processor for the Etsy Pricing Console.
//
// Runs entirely without a browser: obtains an Etsy token from the site's
// server-side token store (etsyAuth.js, same mechanism as EtsyMail's crons),
// then for each queued listing performs the identical pipeline the console's
// Save button uses — by calling this site's own battle-tested functions over
// HTTP (etsyListingInventoryDetailProxy for the snapshot,
// etsyUpdateListingInventoryProxy for the staleness-checked, read-back-
// verified write with the Personalization field). Progress and results are
// written to Firestore in real time (EtsyPricing_Runs +
// EtsyPricing_Listings), so any browser can attach later and watch.
//
// Netlify background functions get ~15 minutes; if time runs short the
// function re-invokes itself with the same run_id and continues where the
// run doc says it left off. Stop is a flag on the run doc, checked before
// every listing.
//
// The rebuild planner below is a byte-faithful port of the console's
// planStandardRebuild — keep the two in sync if the scheme ever changes.

const admin = require("./firebaseAdmin");
const { etsyFetch } = require("./etsyRateLimiter");

/* Server-side Etsy token manager.
   The site's shipped etsyAuth.js points at "config/etsy/oauth" — a
   THREE-segment path, which Firestore rejects for documents (even number
   of segments required), so it can never work. This uses a valid doc,
   seeded by the console: the browser pushes its own OAuth tokens (which
   carry the listing-write scopes) via etsyPricingStore's saveServerToken
   whenever they are issued or refreshed. */
const TOKEN_DOC = "EtsyPricing_Config/etsyOauth";
async function getValidEtsyAccessToken() {
  const db = admin.firestore();
  const snap = await db.doc(TOKEN_DOC).get();
  const tok = snap.exists ? snap.data() : null;
  if (!tok || !tok.refresh_token) throw new Error("No Etsy token on the server yet. Open the pricing console once while connected to Etsy \u2014 it hands its token to the server automatically \u2014 then retry.");
  if (tok.access_token && tok.expires_at && Date.now() < Number(tok.expires_at) - 120000) return tok.access_token;
  const res = await etsyFetch("https://api.etsy.com/v3/public/oauth/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "refresh_token", client_id: process.env.CLIENT_ID, refresh_token: tok.refresh_token }).toString()
  }, { bucket: "etsy-global" });
  if (!res.ok) throw new Error("Etsy token refresh failed: HTTP " + res.status + " " + (await res.text()).slice(0, 200));
  const j = await res.json();
  const stored = {
    access_token: j.access_token,
    refresh_token: j.refresh_token || tok.refresh_token,
    expires_at: Date.now() + Math.max(0, (Number(j.expires_in) || 3600) - 90) * 1000,
    updated_at: Date.now()
  };
  await db.doc(TOKEN_DOC).set(stored, { merge: true });
  return stored.access_token;
}

const SITE = (process.env.URL || "").replace(/\/$/, "");
const FN = SITE + "/.netlify/functions";
const TIME_BUDGET_MS = 13 * 60 * 1000; // leave headroom under Netlify's 15 min

/* ---------------- Pricing scheme (mirror of the console) ---------------- */
/* ---------------- Pricing scheme ----------------
 * Price sheets, priceFor() and planStandardRebuild() now live in the shared
 * module so this batch runner and etsyPricingApplyOne (called by the Listing
 * Generator) can never price the same listing differently. Update prices in
 * _etsyPricingScheme.js — and in etsy-pricing.html, which still carries its
 * own browser-side copy for the live preview.
 */
const {
  CANON_ORDER, CHARM_ONLY_METALS, NO_CHAIN_VALUE, ENGRAVE_INSTRUCTIONS,
  REGULAR_PRICES, BEADY_FLAT_PRICES, BEADY_SOLID_BY_LENGTH,
  CHARM_ONLY_PRICE_POOLS, CHARM_LISTING_PRICES,
  isNoChainVal, parseLen, titleCaseOpt, firstOffering, deep,
  priceFor, planStandardRebuild, planCharmListingRebuild, planStudRebuild,
  normalizeChainType, listingKindFor,
} = require("./_etsyPricingScheme");
async function logEvent(db, e) {
  try { await db.collection("EtsyPricing_Log").add({ at: Date.now(), listing_id: String(e.listing_id||""), title: String(e.title||"").slice(0,200), type: e.type, ok: e.ok !== false, detail: String(e.detail||"").slice(0,800) }); } catch (_) {}
}
/*  ═══ WHY A RUN USED TO DEADLOCK ═══════════════════════════════════════
 *
 *  The halt rule was "3 consecutive failures -> stop", and a failed listing is
 *  deliberately never marked `batched`, so it stays at the FRONT of the queue.
 *  Together those two rules lock the run: the same three listings that cannot
 *  be priced are re-attempted first on every run, fail again, and halt it
 *  again — 0 succeeded, 3 failed, 134 never touched, forever.
 *
 *  The halt exists to catch a SYSTEMIC fault (token dead, Etsy down, rate
 *  limit) before it burns the whole queue. It was never meant to fire on a
 *  listing whose own data cannot be priced. So failures are now classified:
 *
 *    "blocked"  — this listing needs a human. No chain/kind recorded, the
 *                 planner refused, or Etsy's read-back disagreed after the
 *                 write landed. Recorded on the listing with a reason, does
 *                 NOT count toward the halt, and does NOT hold up the queue.
 *    "systemic" — anything else: HTTP, network, auth, rate limit. Counts
 *                 toward the halt, exactly as before.
 *
 *  Nothing is hidden and nothing is lost: a blocked listing is still not
 *  marked `batched`, it is flagged `batch_blocked` with the reason so the
 *  console can list it under "Needs attention", and clearing that flag puts
 *  it straight back in the queue.                                          */
function blockingError(msg, reason) {
  const e = new Error(msg);
  e.blocking = true;
  e.blockReason = reason || "unpriceable";
  return e;
}
// A budget error must PAUSE the run, never fail listings. Matching only the
// exact token meant a differently-worded budget error from the proxy layer
// counted as three ordinary failures and auto-halted the run instead.
const BUDGET_RE = /DAILY_BUDGET_EXHAUSTED|daily (?:api |call )?budget|budget exhausted|quota exhausted/i;

function compactHealth(h){return h?{error_count:h.error_count||0,warning_count:h.warning_count||0,product_count:h.product_count||0,min_price:h.min_price??null,max_price:h.max_price??null}:null}

/* ---------------- Worker ---------------- */
async function callFn(path, opts) {
  const r = await fetch(FN + path, opts);
  const t = await r.text();
  let d; try { d = JSON.parse(t); } catch { d = { error: t.slice(0, 300) }; }
  if (!r.ok) { const e = new Error(d.error || ("HTTP " + r.status)); e.data = d; throw e; }
  return d;
}

exports.handler = async (event) => {
  let runId;
  try { runId = JSON.parse(event.body || "{}").run_id; } catch { /* noop */ }
  if (!runId) return { statusCode: 400, body: "missing run_id" };

  const db = admin.firestore();
  const runRef = db.collection("EtsyPricing_Runs").doc(String(runId));
  const started = Date.now();

  const snap = await runRef.get();
  if (!snap.exists) return { statusCode: 404, body: "run not found" };
  let run = snap.data();
  if (["done", "stopped"].includes(run.status)) return { statusCode: 200, body: "already finished" };
  if (run.paused) return { statusCode: 200, body: "paused" };
  await runRef.set({ status: "running", updated_at: Date.now() }, { merge: true });
  if (run.done === 0) await logEvent(db, { type: "run_start", detail: "Batch run started (" + (run.started_by || "manual") + ") \u00b7 " + (run.ids || []).length + " listings queued." });

  let accessToken;
  try { accessToken = await getValidEtsyAccessToken(); }
  catch (e) {
    await runRef.set({ status: "done", fatal_error: "Server Etsy token unavailable: " + e.message, updated_at: Date.now() }, { merge: true });
    return { statusCode: 200, body: "no token" };
  }
  const authHeaders = { "access-token": accessToken, "Content-Type": "application/json" };

  const ids = run.ids || [];
  // Consecutive SYSTEM failures before halting. Listing-data problems no
  // longer count, so this can stay tight without deadlocking the queue.
  const HALT_AFTER = Math.max(2, Number(process.env.ETSY_BATCH_HALT_AFTER || 3));
  const lastSystemic = [];
  for (let i = run.done; i < ids.length; i++) {
    // Stop flag + time budget, checked per listing.
    const fresh = (await runRef.get()).data();
    if (fresh.stop) { await runRef.set({ status: "stopped", current: "", updated_at: Date.now() }, { merge: true }); return { statusCode: 200, body: "stopped" }; }
    if (fresh.paused) { await runRef.set({ status: "paused", current: "Paused \u2014 " + run.done + " of " + ids.length + " completed", updated_at: Date.now() }, { merge: true }); return { statusCode: 200, body: "paused" }; }
    if (Date.now() - started > TIME_BUDGET_MS) {
      // Self-chain into a new invocation and exit cleanly.
      fetch(FN + "/etsyPricingBatch-background", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ run_id: runId }) }).catch(() => {});
      await runRef.set({ current: "re-queued (time budget)", updated_at: Date.now() }, { merge: true });
      return { statusCode: 200, body: "chained" };
    }

    const id = String(ids[i]);
    const prepSnap = await db.collection("EtsyPricing_Listings").doc(id).get();
    const d = prepSnap.exists ? prepSnap.data() : {};
    await runRef.set({ current: "#" + id + (d.title ? " \u00b7 " + d.title : ""), updated_at: Date.now() }, { merge: true });

    try {
      const detail = await callFn("/etsyListingInventoryDetailProxy?listingId=" + encodeURIComponent(id) + "&inventory_only=1", { headers: authHeaders });
      // Chain type must be EXPLICIT. This line used to read
      //   d.chain_type === "beady" ? "beady" : "regular"
      // so a listing whose prep doc was missing or had no chain_type (d defaults
      // to {} above) was silently priced off the REGULAR sheet. The generator's
      // endpoint refuses to guess for exactly this reason, which meant the same
      // Beady listing could be skipped by one path and mispriced by the other —
      // Silver $39.69 instead of $56.56. Both paths now use the shared resolver.
      // Which planner this listing needs, resolved the SAME way the generator's
      // endpoint resolves it. Order of preference:
      //   1. an explicit listing_kind on the prep doc
      //   2. queue_id / category, via the shared listingKindFor()
      //   3. the legacy chain_type field (necklaces only)
      // Nothing is ever guessed — an unresolved listing throws and stays queued.
      const kind = ["regular", "beady", "charm", "stud"].includes(String(d.listing_kind || "").toLowerCase())
        ? String(d.listing_kind).toLowerCase()
        : (listingKindFor(d.queue_id, d.category) || normalizeChainType(d.chain_type));
      if (!kind) throw blockingError(
        "No listing kind recorded for this listing (listing_kind=" + JSON.stringify(d.listing_kind || null) +
        ", category=" + JSON.stringify(d.category || null) + ", chain_type=" + JSON.stringify(d.chain_type || null) + "). " +
        "Set chain_type to \"regular\" or \"beady\", or listing_kind to \"charm\" or \"stud\", before batching — " +
        "refusing to guess, because pricing a Beady necklace off the Regular sheet is a silent money error.",
        "no_listing_kind");
      const plan = kind === "charm" ? planCharmListingRebuild(detail.inventory.products)
                 : kind === "stud"  ? planStudRebuild(detail.inventory.products)
                 : planStandardRebuild(detail.inventory.products, kind, d.engraving !== false);
      // The planner refused — wrong sheet for this listing's variation menus
      // ("No metal dropdown found", "Beady pricing covers only 14/16/18"...).
      // That is a fact about this listing, not about the run.
      if (plan.error) throw blockingError(plan.error, "plan_refused");
      // Necklaces only. A charm listing expresses engraving through its Charm Type
      // dropdown and a stud listing has none, so a REQUIRED personalization field
      // would block checkout for every buyer of those listings.
      const wantsPers = (d.engraving !== false) && (kind === "regular" || kind === "beady");
      const pers = wantsPers ? { enabled: true, required: true, max_chars: 1000, instructions: ENGRAVE_INSTRUCTIONS } : null;
      const res = await callFn("/etsyUpdateListingInventoryProxy", {
        method: "POST", headers: authHeaders,
        body: JSON.stringify({ listing_id: Number(id), expected_snapshot_hash: detail.snapshot_hash, inventory: { products: plan.rows }, auto_on_property: true, personalization: pers })
      });
      /*  The PUT already landed on Etsy; only the read-back disagreed. Never
       *  retried blindly: the price tier is drawn at random per listing, so a
       *  re-run would write DIFFERENT prices. Blocked for review.          */
      if (!res.verified) throw blockingError(
        (res.verification_error || "Etsy verification did not match after the write.") +
        " The write was sent \u2014 check this listing before re-queuing it.",
        "not_verified");

      const patch = {
        batched: true,
        last_batch: { at: Date.now(), ok: true },
        last_save: { at: Date.now(), verified: true },
        health: compactHealth(res.fresh && res.fresh.pricing_health),
        scanned: true,
        approval: { mode: "updated", at: Date.now(), hash: (res.fresh && res.fresh.snapshot_hash) || null },
        updated_at: Date.now()
      };
      if (!d.original_saved && res.previous_inventory) {
        patch.original_inventory = res.previous_inventory;
        patch.original_snapshot_hash = res.previous_snapshot_hash || null;
        patch.original_saved = true;
      }
      await db.collection("EtsyPricing_Listings").doc(id).set(patch, { merge: true });
      await logEvent(db, { listing_id: id, title: d.title, type: "batch_ok", detail: "Rebuilt (" + kind + " \u00b7 Engraving " + (wantsPers ? "ON" : "OFF") + ") and verified on Etsy." });
      run.ok = (run.ok || 0) + 1;
      run.consec_fail = 0;
    } catch (e) {
      if (BUDGET_RE.test(String(e.message))) {
        // Not a listing failure: the shared daily budget ran out. Pause the
        // run holding position at this listing; the cron auto-resumes it
        // after the 11:59 PM Toronto reset. done is NOT advanced.
        await runRef.set({ status: "paused", paused: true, budget_paused: true, stop_reason: String(e.message).slice(0, 300), current: "Paused \u2014 daily API budget spent; auto-resumes after the Toronto reset", updated_at: Date.now() }, { merge: true });
        await logEvent(db, { type: "run_end", ok: false, detail: "Run paused: " + String(e.message).slice(0, 250) });
        return { statusCode: 200, body: "budget-paused" };
      }
      const blocking = !!(e && e.blocking);
      const msg = String(e.message).slice(0, 400);
      run.fail = (run.fail || 0) + 1;
      // Only a systemic fault moves the halt counter. A blocked listing RESETS
      // it, because reaching it at all proves the pipeline is working.
      run.consec_fail = blocking ? 0 : (run.consec_fail || 0) + 1;
      run.blocked = (run.blocked || 0) + (blocking ? 1 : 0);

      // `batched` is deliberately NOT set on failure, so nothing is ever lost.
      // `batch_blocked` marks the listing as needing a human and takes it out
      // of the auto-queue until someone clears it in the console.
      const patch = { last_batch: { at: Date.now(), ok: false, error: msg, blocking: blocking }, updated_at: Date.now() };
      if (blocking) patch.batch_blocked = { at: Date.now(), reason: e.blockReason || "unpriceable", error: msg };
      await db.collection("EtsyPricing_Listings").doc(id).set(patch, { merge: true });

      const line = (blocking ? "\u26a0 NEEDS ATTENTION " : "\u2717 ") + "#" + id + (d.title ? " \u00b7 " + d.title : "") + ": " + msg;
      await runRef.set({ errors: admin.firestore.FieldValue.arrayUnion(line),
                         blocked: run.blocked || 0, updated_at: Date.now() }, { merge: true });
      await logEvent(db, { listing_id: id, title: d.title, type: blocking ? "batch_blocked" : "batch_fail", ok: false, detail: msg });
      if (!blocking) lastSystemic.push(msg);
    }
    run.done = i + 1;
    await runRef.set({ done: run.done, ok: run.ok || 0, fail: run.fail || 0,
                       blocked: run.blocked || 0, consec_fail: run.consec_fail || 0,
                       updated_at: Date.now() }, { merge: true });
    if ((run.consec_fail || 0) >= HALT_AFTER) {
      /*  The halt badge used to say only "3 consecutive listings failed",
          which told nobody what to fix. The actual error text goes in the
          reason so the console can explain itself without a log dive.      */
      const why = lastSystemic.slice(-HALT_AFTER).map(function (m, n) { return (n + 1) + ") " + m; }).join("  |  ");
      await runRef.set({
        status: "stopped",
        stop_reason: "Auto-halted after " + HALT_AFTER + " consecutive SYSTEM failures (not listing-data problems). " +
                     "Everything not yet attempted stays queued. Last errors \u2014 " + (why || "no detail captured"),
        current: "", updated_at: Date.now()
      }, { merge: true });
      await logEvent(db, { type: "run_end", ok: false, detail: "Auto-halted: " + (why || "no detail captured") });
      return { statusCode: 200, body: "auto-halted" };
    }
  }

  await runRef.set({ status: "done", current: "", blocked: run.blocked || 0,
                     finished_at: Date.now(), updated_at: Date.now() }, { merge: true });
  await logEvent(db, { type: "run_end", detail: "Batch run finished: " + (run.ok || 0) + " succeeded, " +
    (run.fail || 0) + " failed" + (run.blocked ? " (" + run.blocked + " need attention)" : "") + "." });
  return { statusCode: 200, body: "done" };
};
