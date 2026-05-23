/*  netlify/functions/etsyMailSync-background.js
 *
 *  ═══ WHAT THIS DOES ═══════════════════════════════════════════════════
 *
 *  Background function (15-min budget) supporting two modes:
 *
 *    mode = "buyer"      — On-demand aggregation of a single buyer's
 *                          customer doc. Reads receipts FROM THE
 *                          FIRESTORE MIRROR (EtsyMail_Receipts) — does
 *                          NOT call Etsy. Fast, free (1 Firestore
 *                          query + 1 write).
 *
 *    mode = "backfill"   — One-time historical pull. Paginates Etsy's
 *                          receipts endpoint over a date window
 *                          (default 24 months), writing each receipt
 *                          to EtsyMail_Receipts. Runs in CHUNKS — one
 *                          invocation does up to MAX_BACKFILL_PAGES
 *                          then returns. The UI watches Firestore
 *                          progress and re-fires the function for the
 *                          next chunk until complete.
 *
 *  ═══ ARCHITECTURAL CONTEXT ════════════════════════════════════════════
 *
 *  In May 2026 we discovered that Etsy's getShopReceipts endpoint does
 *  not honor a `buyer_user_id` filter — passing it returns the entire
 *  shop's receipts. At 3,000 orders/month this meant each buyer-sync
 *  invocation paginated up to 12,000 unrelated receipts to find a
 *  handful belonging to the requested buyer, burning daily quota in
 *  hours.
 *
 *  The architectural fix is a Firestore mirror:
 *
 *    etsyMailReceiptsMirrorCron.js   — Every 3 min, pulls receipts
 *                                      modified since last run, writes
 *                                      them to EtsyMail_Receipts.
 *
 *    THIS FILE buyer mode             — Queries the mirror by
 *                                      buyer_user_id. No Etsy calls.
 *
 *    THIS FILE backfill mode          — One-time historical population
 *                                      of the mirror.
 *
 *  After backfill, buyer-sync is essentially free regardless of how
 *  often it's invoked. Snapshot/draftReply triggers can fire on every
 *  scrape without quota concerns.
 *
 *  ═══ INVOCATION ═══════════════════════════════════════════════════════
 *
 *  POST /.netlify/functions/etsyMailSync-background
 *    { mode: "buyer", buyerUserId: "<numeric id>" }
 *
 *  POST /.netlify/functions/etsyMailSync-background
 *    { mode: "backfill", action: "start" }    — Begin a new backfill
 *    { mode: "backfill", action: "chunk"  }    — Process next chunk
 *    { mode: "backfill", action: "cancel" }    — Abort an in-progress backfill
 *
 *  ═══ CUSTOMER DOC SHAPE (unchanged from prior versions) ═══════════════
 *
 *  EtsyMail_Customers/{buyerUserId}:
 *    {
 *      buyerUserId, displayName, currency,
 *      orderCount, totalSpent,
 *      firstOrderAt, lastOrderAt,
 *      isRepeatBuyer,
 *      recentReceipts: [
 *        { receiptId, orderedAt, grandTotal, currency, status, isPaid, isShipped }
 *      ],
 *      updatedAt
 *    }
 */

"use strict";

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");
const meter = require("./_etsyApiMeter");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── Config ────────────────────────────────────────────────────────────────
const SHOP_ID       = process.env.SHOP_ID;
const CLIENT_ID     = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET;

const OAUTH_DOC_PATH    = "config/etsyOauth";
const MIRROR_STATE_PATH = "EtsyMail_Config/receiptsMirrorState";
const SYNC_STATE_PATH   = "EtsyMail_Config/syncState";

const TOKEN_REFRESH_BUFFER_MS = 2 * 60 * 1000;

const PAGE_SIZE = 100;
const RECENT_RECEIPTS_CAP = 10;

// Backfill tuning
const BACKFILL_WINDOW_MONTHS = 24;
const MAX_BACKFILL_PAGES_PER_CHUNK = 60;   // ~60 pages × ~500ms = ~30s; well within 15-min budget
const BACKFILL_PAUSE_MS = 100;             // tiny delay between pages to be polite
const FETCH_TIMEOUT_MS = 30 * 1000;
const MAX_INVOCATION_MS = 13 * 60 * 1000;  // leave 2 min cleanup tail

// ─── OAuth ─────────────────────────────────────────────────────────────────
async function readEtsyToken() {
  const snap = await db.doc(OAUTH_DOC_PATH).get();
  return snap.exists ? snap.data() : null;
}

async function refreshEtsyToken(oldRefreshToken) {
  const _meterToken = meter.bump("sync.oauthRefresh");
  let res;
  try {
    res = await fetch("https://api.etsy.com/v3/public/oauth/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type    : "refresh_token",
        client_id     : CLIENT_ID,
        refresh_token : oldRefreshToken
      })
    });
  } catch (err) {
    _meterToken.failNet();
    throw err;
  }
  _meterToken.fromHttp(res.status);
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Etsy OAuth refresh failed ${res.status}: ${text.slice(0, 300)}`);
  }
  const t = await res.json();
  const expiresAtMs = Date.now() + (t.expires_in || 3600) * 1000;
  const patch = {
    access_token  : t.access_token,
    refresh_token : t.refresh_token || oldRefreshToken,
    expires_in    : t.expires_in,
    expires_at_ms : expiresAtMs,
    refreshed_at  : FV.serverTimestamp()
  };
  await db.doc(OAUTH_DOC_PATH).set(patch, { merge: true });
  return { ...patch };
}

async function getValidEtsyAccessToken() {
  const stored = await readEtsyToken();
  if (!stored || !stored.refresh_token) {
    throw new Error(`Etsy OAuth not initialized — ${OAUTH_DOC_PATH} missing refresh_token`);
  }
  const expiresAtMs = stored.expires_at_ms || 0;
  const needsRefresh = !stored.access_token
    || Date.now() + TOKEN_REFRESH_BUFFER_MS >= expiresAtMs;
  if (needsRefresh) {
    const refreshed = await refreshEtsyToken(stored.refresh_token);
    return refreshed.access_token;
  }
  return stored.access_token;
}

// ─── Money helper ──────────────────────────────────────────────────────────
function moneyAmt(m) {
  if (!m || typeof m.amount !== "number" || typeof m.divisor !== "number") return null;
  return m.amount / m.divisor;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ─── Fetch one page (used by backfill) ─────────────────────────────────────
//
// Backfill-specific page fetch. On 429 daily-limit, writes the lock and
// returns failure — the caller (chunk runner) records the failure to
// backfillProgress.errorMsg and stops.
async function fetchBackfillPage(accessToken, params) {
  const qs = new URLSearchParams(Object.fromEntries(
    Object.entries(params).filter(([_, v]) => v != null && v !== "")
  )).toString();
  const url = `https://api.etsy.com/v3/application/shops/${SHOP_ID}/receipts?${qs}`;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  const _meterToken = meter.bump("backfill.receiptsPage");

  let res;
  try {
    res = await fetch(url, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "x-api-key": `${CLIENT_ID}:${CLIENT_SECRET}`,
        "Content-Type": "application/json"
      },
      signal: controller.signal
    });
  } catch (err) {
    clearTimeout(timeoutId);
    _meterToken.failNet();
    if (err.name === "AbortError") {
      return { ok: false, kind: "timeout", message: "Etsy fetch timeout" };
    }
    return { ok: false, kind: "network", message: err.message };
  }
  clearTimeout(timeoutId);
  _meterToken.fromHttp(res.status);

  if (res.status === 429) {
    const retryAfter = parseInt(res.headers.get("retry-after") || "5", 10);
    const bodyText = await res.text().catch(() => "");
    const isDaily = /daily|day/i.test(bodyText) || retryAfter > 3600;
    if (isDaily) {
      try {
        await db.doc(SYNC_STATE_PATH).set({
          etsyDailyLimitHitAt   : FV.serverTimestamp(),
          etsyDailyLimitResetAt : admin.firestore.Timestamp.fromMillis(Date.now() + retryAfter * 1000),
          etsyDailyLimitDetail  : bodyText.slice(0, 300)
        }, { merge: true });
      } catch {}
      return { ok: false, kind: "daily_rate_limit", retryAfter, message: bodyText.slice(0, 200) };
    }
    return { ok: false, kind: "rate_limited", retryAfter, message: bodyText.slice(0, 200) };
  }

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    return { ok: false, kind: "http_error", status: res.status, message: text.slice(0, 300) };
  }

  return { ok: true, data: await res.json() };
}

// ─── Receipt → mirror doc transform ────────────────────────────────────────
function receiptToMirrorDoc(r) {
  return {
    receipt_id          : String(r.receipt_id),
    buyer_user_id       : r.buyer_user_id ? String(r.buyer_user_id) : null,
    created_timestamp   : r.created_timestamp || null,
    updated_timestamp   : r.updated_timestamp || null,
    status              : r.status || null,
    is_paid             : !!r.is_paid,
    is_shipped          : !!r.is_shipped,
    grandtotal_amount   : moneyAmt(r.grandtotal),
    grandtotal_currency : r.grandtotal ? r.grandtotal.currency_code : null,
    buyer_name          : r.name || null,
    raw                 : r,
    mirrorWrittenAt     : FV.serverTimestamp()
  };
}

async function writeReceiptBatch(receipts) {
  if (!receipts.length) return 0;
  const batch = db.batch();
  for (const r of receipts) {
    if (!r.receipt_id) continue;
    const doc = receiptToMirrorDoc(r);
    batch.set(db.collection("EtsyMail_Receipts").doc(String(r.receipt_id)), doc, { merge: true });
  }
  await batch.commit();
  return receipts.length;
}

// ─── Receipt → customer summary transform (for buyer-mode aggregation) ────
function mirrorToSummary(m) {
  return {
    receiptId  : String(m.receipt_id),
    orderedAt  : m.created_timestamp ? m.created_timestamp * 1000 : null,
    updatedAt  : m.updated_timestamp ? m.updated_timestamp * 1000 : null,
    grandTotal : m.grandtotal_amount,
    currency   : m.grandtotal_currency,
    status     : m.status || null,
    isPaid     : !!m.is_paid,
    isShipped  : !!m.is_shipped,
    buyerUserId: m.buyer_user_id || null,
    buyerName  : m.buyer_name || null
  };
}

// ─── Buyer-mode: aggregate from mirror ─────────────────────────────────────
//
// Reads ALL receipts for the buyer from the mirror, computes totals,
// writes the customer doc. No Etsy calls.
//
// Returns: { receiptsProcessed, customersUpdated, pagesFetched, buyerUserId }
// (pagesFetched stays at 0 — we don't hit Etsy.)
async function runBuyerSyncFromMirror({ buyerUserId }) {
  if (!buyerUserId) {
    throw new Error("runBuyerSyncFromMirror requires buyerUserId");
  }

  // Query the mirror. We pull up to 1000 most recent receipts for the
  // buyer — that's far more than RECENT_RECEIPTS_CAP, but we use the
  // full set to compute orderCount and totalSpent accurately. A buyer
  // with >1000 orders is implausible for our shop.
  const snap = await db.collection("EtsyMail_Receipts")
    .where("buyer_user_id", "==", String(buyerUserId))
    .orderBy("created_timestamp", "desc")
    .limit(1000)
    .get();

  const summaries = [];
  let displayName = null;
  let currency = null;
  let totalSpent = 0;
  let firstMs = null;
  let lastMs = null;

  snap.forEach(doc => {
    const m = doc.data();
    const s = mirrorToSummary(m);
    summaries.push(s);
    if (s.buyerName && !displayName) displayName = s.buyerName;
    if (s.currency && !currency) currency = s.currency;
    if (typeof s.grandTotal === "number") totalSpent += s.grandTotal;
    if (s.orderedAt) {
      if (firstMs === null || s.orderedAt < firstMs) firstMs = s.orderedAt;
      if (lastMs  === null || s.orderedAt > lastMs ) lastMs  = s.orderedAt;
    }
  });

  const orderCount = summaries.length;

  // Build recentReceipts capped at RECENT_RECEIPTS_CAP, newest-first
  const recentReceipts = summaries
    .slice(0, RECENT_RECEIPTS_CAP)
    .map(s => ({
      receiptId : s.receiptId,
      orderedAt : s.orderedAt ? admin.firestore.Timestamp.fromMillis(s.orderedAt) : null,
      grandTotal: s.grandTotal,
      currency  : s.currency,
      status    : s.status,
      isPaid    : s.isPaid,
      isShipped : s.isShipped
    }));

  // Load existing customer doc to preserve fields we don't compute
  const customerRef = db.collection("EtsyMail_Customers").doc(String(buyerUserId));
  const existingSnap = await customerRef.get();
  const existing = existingSnap.exists ? existingSnap.data() : null;

  const finalDisplayName = displayName || (existing && existing.displayName) || "Unknown";
  const finalCurrency    = currency    || (existing && existing.currency)    || "USD";

  const customerDoc = {
    buyerUserId  : String(buyerUserId),
    displayName  : finalDisplayName,
    currency     : finalCurrency,
    orderCount,
    totalSpent   : Math.round(totalSpent * 100) / 100,
    firstOrderAt : firstMs ? admin.firestore.Timestamp.fromMillis(firstMs) : null,
    lastOrderAt  : lastMs  ? admin.firestore.Timestamp.fromMillis(lastMs)  : null,
    isRepeatBuyer: orderCount >= 2,
    recentReceipts,
    updatedAt    : FV.serverTimestamp(),
    // Track the data source so any debugging can confirm which path wrote this
    syncSource   : "mirror"
  };

  await customerRef.set(customerDoc, { merge: true });

  return {
    receiptsProcessed: orderCount,
    customersUpdated : 1,
    pagesFetched     : 0,
    buyerUserId      : String(buyerUserId),
    source           : "mirror"
  };
}

// ─── Backfill — single chunk runner ────────────────────────────────────────
//
// One invocation processes up to MAX_BACKFILL_PAGES_PER_CHUNK pages then
// returns. Caller (UI poller) detects status=running and re-fires the
// function. State lives in EtsyMail_Config/receiptsMirrorState.backfillProgress.
//
// action="start"  → Initialize progress doc. Returns immediately so the
//                   UI can show progress=0. The very next chunk picks up.
// action="chunk"  → Process the next chunk. Updates progress incrementally.
// action="cancel" → Mark progress.status=idle, leaving partial data in place.
async function runBackfill({ action, invocationStartMs }) {
  const deadlineMs = invocationStartMs + MAX_INVOCATION_MS;
  const mirrorRef = db.doc(MIRROR_STATE_PATH);

  // ─── action: cancel ───────────────────────────────────────────────
  if (action === "cancel") {
    await mirrorRef.set({
      backfillProgress: {
        status      : "idle",
        cancelledAt : FV.serverTimestamp()
      }
    }, { merge: true });
    return { ok: true, action: "cancel", status: "idle" };
  }

  // ─── action: start ────────────────────────────────────────────────
  if (action === "start") {
    const now = Date.now();
    const minCreatedSec = Math.floor((now - BACKFILL_WINDOW_MONTHS * 30 * 24 * 60 * 60 * 1000) / 1000);
    const maxCreatedSec = Math.floor(now / 1000);

    const initialProgress = {
      status            : "running",
      startedAt         : FV.serverTimestamp(),
      completedAt       : null,
      totalPagesEstimate: null,    // unknown until first page lands
      pagesProcessed    : 0,
      receiptsProcessed : 0,
      currentOffset     : 0,
      windowMinCreated  : minCreatedSec,
      windowMaxCreated  : maxCreatedSec,
      errorMsg          : null
    };

    await mirrorRef.set({
      backfillProgress: initialProgress
    }, { merge: true });

    return { ok: true, action: "start", status: "running", progress: initialProgress };
  }

  // ─── action: resume ───────────────────────────────────────────────
  // Continues a previously-paused backfill from its saved currentOffset.
  // Unlike "start", this does NOT reset offset/pagesProcessed/receipts-
  // Processed — it just flips status back to "running", clears the
  // errorMsg, and triggers the first chunk. The chain self-continues
  // from there as long as chunks succeed.
  //
  // Safe to call repeatedly: each call just resets status+errorMsg and
  // re-fires a chunk, which will pick up from currentOffset wherever
  // it last committed.
  if (action === "resume") {
    const snap = await mirrorRef.get();
    const cfg = snap.exists ? snap.data() : null;
    const progress = cfg && cfg.backfillProgress;
    if (!progress) {
      return { ok: false, reason: "no_progress_to_resume", action: "resume" };
    }
    // Don't resume if the window itself is missing — that means no
    // backfill was ever started in this Firestore environment.
    if (typeof progress.windowMinCreated !== "number" ||
        typeof progress.windowMaxCreated !== "number") {
      return { ok: false, reason: "window_missing_run_start_first", action: "resume" };
    }
    await mirrorRef.set({
      backfillProgress: {
        status     : "running",
        errorMsg   : null,
        resumedAt  : FV.serverTimestamp()
      }
    }, { merge: true });

    // Kick off the first chunk so the chain restarts. Fire-and-forget;
    // the chunk will read currentOffset from Firestore and continue.
    const fnHost = process.env.URL || process.env.DEPLOY_PRIME_URL || null;
    if (fnHost) {
      setTimeout(() => {
        fetch(`${fnHost}/.netlify/functions/etsyMailSync-background`, {
          method : "POST",
          headers: { "Content-Type": "application/json" },
          body   : JSON.stringify({ mode: "backfill", action: "chunk" })
        }).catch(err => {
          console.warn(`[backfill] resume chunk trigger failed: ${err.message}`);
        });
      }, 500);
    }
    return {
      ok                : true,
      action            : "resume",
      status            : "running",
      currentOffset     : progress.currentOffset || 0,
      pagesProcessed    : progress.pagesProcessed || 0,
      receiptsProcessed : progress.receiptsProcessed || 0
    };
  }

  // ─── action: chunk (default) ──────────────────────────────────────
  // Read existing progress; abort if not running.
  const snap = await mirrorRef.get();
  const cfg = snap.exists ? snap.data() : null;
  const progress = cfg && cfg.backfillProgress;
  if (!progress || progress.status !== "running") {
    return {
      ok    : false,
      reason: "not_running",
      progress
    };
  }

  // Daily rate-limit short-circuit
  try {
    const syncSnap = await db.doc(SYNC_STATE_PATH).get();
    if (syncSnap.exists) {
      const ss = syncSnap.data();
      const resetMs = ss.etsyDailyLimitResetAt && ss.etsyDailyLimitResetAt.toMillis
        ? ss.etsyDailyLimitResetAt.toMillis() : 0;
      if (resetMs > Date.now()) {
        await mirrorRef.set({
          backfillProgress: {
            errorMsg: `daily_rate_limit until ${new Date(resetMs).toISOString()}`
          }
        }, { merge: true });
        return { ok: false, reason: "daily_rate_limit", waitMs: resetMs - Date.now() };
      }
    }
  } catch {}

  const accessToken = await getValidEtsyAccessToken();

  let offset = progress.currentOffset || 0;
  let pagesProcessed = progress.pagesProcessed || 0;
  let receiptsProcessed = progress.receiptsProcessed || 0;
  let done = false;
  let errorMsg = null;
  let pagesThisChunk = 0;

  while (pagesThisChunk < MAX_BACKFILL_PAGES_PER_CHUNK) {
    if (Date.now() > deadlineMs) {
      console.log("[backfill] approaching invocation deadline — stopping chunk");
      break;
    }

    const params = {
      limit       : PAGE_SIZE,
      offset      : offset,
      sort_on     : "created",
      sort_order  : "desc",
      min_created : progress.windowMinCreated,
      max_created : progress.windowMaxCreated
    };

    const result = await fetchBackfillPage(accessToken, params);
    pagesThisChunk++;

    if (!result.ok) {
      errorMsg = `${result.kind}: ${result.message}`;
      console.warn(`[backfill] page fetch failed at offset ${offset}: ${errorMsg}`);
      // For rate_limited (per-second) — caller can retry next chunk
      // For daily_rate_limit — caller will skip until reset
      // For network/http_error — caller can retry
      break;
    }

    const receipts = (result.data && result.data.results) || [];
    const totalCount = result.data && result.data.count;
    if (totalCount && !progress.totalPagesEstimate) {
      progress.totalPagesEstimate = Math.ceil(totalCount / PAGE_SIZE);
    }

    if (!receipts.length) {
      done = true;
      break;
    }

    try {
      await writeReceiptBatch(receipts);
    } catch (e) {
      errorMsg = `firestore_write_failed: ${e.message}`;
      console.error(`[backfill] batch write failed at offset ${offset}: ${e.message}`);
      break;
    }

    pagesProcessed++;
    receiptsProcessed += receipts.length;
    offset += PAGE_SIZE;

    if (receipts.length < PAGE_SIZE) {
      done = true;
      break;
    }

    if (BACKFILL_PAUSE_MS > 0) await sleep(BACKFILL_PAUSE_MS);
  }

  // ─── Persist updated progress ─────────────────────────────────────
  const newStatus = done ? "complete" : (errorMsg ? "error" : "running");
  const updatedProgress = {
    status            : newStatus,
    pagesProcessed,
    receiptsProcessed,
    currentOffset     : offset,
    errorMsg,
    // Heartbeat for the mirror-cron watchdog: if backfill is "running"
    // but this timestamp is stale, the cron re-fires a chunk to restart
    // the chain (covers fire-and-forget self-trigger failures).
    lastChunkAt       : FV.serverTimestamp()
  };
  if (progress.totalPagesEstimate) {
    updatedProgress.totalPagesEstimate = progress.totalPagesEstimate;
  }
  if (done) {
    updatedProgress.completedAt = FV.serverTimestamp();
  }

  await mirrorRef.set({
    backfillProgress: updatedProgress
  }, { merge: true });

  // ─── Self-trigger next chunk ──────────────────────────────────────
  //
  // If the chunk wrapped up with status "running" (more work to do, no
  // error), fire-and-forget a POST to ourselves so the next chunk
  // starts immediately. This makes backfill progress autonomous: the
  // UI just observes, the chain continues even if the operator closes
  // the inbox or the browser. Errors stop the chain (caller must
  // re-start).
  //
  // We do NOT await this fetch — Netlify background functions return
  // 202 immediately. By the time the next chunk starts the current
  // chunk's handler has already returned, freeing the warm container.
  if (newStatus === "running") {
    const fnHost = process.env.URL || process.env.DEPLOY_PRIME_URL || null;
    if (fnHost) {
      // node-fetch is already required at the top of the file. We
      // briefly delay so the Firestore write has time to settle (the
      // next chunk reads it).
      setTimeout(() => {
        fetch(`${fnHost}/.netlify/functions/etsyMailSync-background`, {
          method : "POST",
          headers: { "Content-Type": "application/json" },
          body   : JSON.stringify({ mode: "backfill", action: "chunk" })
        }).catch(err => {
          console.warn(`[backfill] self-trigger next chunk failed: ${err.message}`);
        });
      }, 500);
    } else {
      console.warn("[backfill] no fnHost — cannot self-trigger next chunk");
    }
  }

  return {
    ok                : !errorMsg,
    action            : "chunk",
    status            : newStatus,
    pagesThisChunk,
    pagesProcessed,
    receiptsProcessed,
    offset,
    errorMsg,
    done
  };
}

// ─── Diagnostic log helper ─────────────────────────────────────────────────
//
// Returns a promise. Caller should `await` it to ensure the write commits
// before the handler returns (otherwise Netlify may suspend the container
// before the write lands in Firestore). The internal try/catch ensures
// diagnostic failures never throw — the awaited promise always resolves.
async function writeDiagLog(invocationId, payload) {
  try {
    await db.collection("EtsyMail_DiagnosticLog").doc(invocationId).set(payload, { merge: true });
  } catch (e) {
    console.warn("[sync-bg] diagnostic write failed:", e.message);
  }
}

// ─── Handler ───────────────────────────────────────────────────────────────
exports.handler = meter.wrapHandler(async (event) => {
  const invocationStartMs = Date.now();
  const invocationId = `sync_${invocationStartMs}_${Math.random().toString(36).slice(2, 9)}`;

  // Diagnostic doc — start
  const _h = event.headers || {};
  await writeDiagLog(invocationId, {
    invocationId,
    createdAt    : FV.serverTimestamp(),
    invocationStartMs,
    function     : "etsyMailSync-background",
    phase        : "start",
    callerUA     : (_h["user-agent"]      || _h["User-Agent"]      || null),
    callerReferer: (_h["referer"]         || _h["Referer"]         || null),
    callerOrigin : (_h["origin"]          || _h["Origin"]          || null),
    callerXFF    : (_h["x-forwarded-for"] || _h["X-Forwarded-For"] || null),
    callerHost   : (_h["host"]            || _h["Host"]            || null),
    httpMethod   : event.httpMethod,
    queryString  : event.queryStringParameters || null,
    bodyRaw      : (typeof event.body === "string" ? event.body.slice(0, 500) : null)
  });
  meter.bumpSimple("sync.invocation");

  // Parse body / query
  let mode = null;
  let buyerUserId = null;
  let action = null;
  try {
    if (event.body) {
      const body = JSON.parse(event.body);
      if (body.mode) mode = body.mode;
      if (body.buyerUserId) buyerUserId = String(body.buyerUserId);
      if (body.action) action = body.action;
    }
    if (event.queryStringParameters) {
      if (event.queryStringParameters.mode) mode = event.queryStringParameters.mode;
      if (event.queryStringParameters.buyerUserId) buyerUserId = String(event.queryStringParameters.buyerUserId);
      if (event.queryStringParameters.action) action = event.queryStringParameters.action;
    }
  } catch {}

  await writeDiagLog(invocationId, {
    parsedMode       : mode,
    parsedBuyerUserId: buyerUserId,
    parsedAction     : action
  });

  // ─── Mode dispatch ────────────────────────────────────────────────
  try {
    if (mode === "buyer") {
      if (!buyerUserId) {
        const out = { ok: false, error: "buyer mode requires buyerUserId" };
        await writeDiagLog(invocationId, { phase: "end", outcome: "error", errorMsg: out.error });
        return { statusCode: 400, body: JSON.stringify(out) };
      }
      const result = await runBuyerSyncFromMirror({ buyerUserId });
      const elapsedMs = Date.now() - invocationStartMs;
      await writeDiagLog(invocationId, {
        phase             : "end",
        outcome           : "ok",
        pagesFetched      : 0,
        receiptsProcessed : result.receiptsProcessed,
        customersUpdated  : result.customersUpdated,
        source            : "mirror",
        elapsedMs,
        endedAt           : FV.serverTimestamp()
      });
      return { statusCode: 200, body: JSON.stringify({ ok: true, mode: "buyer", ...result }) };
    }

    if (mode === "backfill") {
      if (!SHOP_ID || !CLIENT_ID || !CLIENT_SECRET) {
        const out = { ok: false, error: "Missing env vars" };
        await writeDiagLog(invocationId, { phase: "end", outcome: "error", errorMsg: out.error });
        return { statusCode: 500, body: JSON.stringify(out) };
      }
      const result = await runBackfill({ action: action || "chunk", invocationStartMs });
      const elapsedMs = Date.now() - invocationStartMs;
      await writeDiagLog(invocationId, {
        phase   : "end",
        outcome : result.ok ? "ok" : "error",
        action  : result.action,
        status  : result.status,
        pagesThisChunk    : result.pagesThisChunk || 0,
        pagesProcessed    : result.pagesProcessed || 0,
        receiptsProcessed : result.receiptsProcessed || 0,
        errorMsg: result.errorMsg || null,
        elapsedMs,
        endedAt : FV.serverTimestamp()
      });
      return { statusCode: 200, body: JSON.stringify(result) };
    }

    // Unknown mode
    const out = {
      ok: false,
      error: `Unsupported mode "${mode}". Supported: buyer, backfill.`
    };
    await writeDiagLog(invocationId, { phase: "end", outcome: "error", errorMsg: out.error });
    return { statusCode: 400, body: JSON.stringify(out) };

  } catch (err) {
    const elapsedMs = Date.now() - invocationStartMs;
    const errorMsg = (err.message || String(err)).slice(0, 500);
    await writeDiagLog(invocationId, {
      phase   : "end",
      outcome : "error",
      errorMsg,
      elapsedMs,
      endedAt : FV.serverTimestamp()
    });
    console.error(`[sync-bg] handler failed: ${errorMsg}`);
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: errorMsg }) };
  }
});
