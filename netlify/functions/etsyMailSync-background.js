/*  netlify/functions/etsyMailSync-background.js
 *
 *  M3 Etsy customer-summaries sync — BUYER-ONLY MODE.
 *
 *  ═══ WHAT THIS DOES ═══
 *
 *  Given a buyerUserId, fetches that buyer's receipts from Etsy and
 *  upserts a single customer doc at EtsyMail_Customers/{buyerUserId}.
 *  That's it.
 *
 *  ═══ WHY ONLY BUYER MODE ═══
 *
 *  This function previously had three modes:
 *    - "full" — windowed 2-year backfill of every shop receipt
 *    - "incremental" — every 25 min, pulled all receipts updated since
 *      the last watermark, regardless of which customers were active
 *    - "buyer" — targeted per-buyer fetch, fired by the snapshot
 *      pipeline when a thread is scraped
 *
 *  The inbox's actual need is narrow: when a customer messages the
 *  shop, show their order history in the right rail. That data is
 *  populated by the snapshot pipeline's per-buyer fanout. The full and
 *  incremental modes pre-populated customer docs for buyers who had
 *  never messaged — data the inbox never uses, since the inbox is
 *  message-driven.
 *
 *  Those two modes also caused a runaway: on May 21 2026, the cron
 *  hit Etsy's daily rate limit, retry-after returned ~2 hours, the
 *  function tried to sleep through it, got killed at the 15-min
 *  function timeout, never cleared its lock, and got re-invoked every
 *  5 min while concurrent invocations from prior ticks were still
 *  spinning. Compounded with the snapshot's buyer-mode fanout, the
 *  daily quota was exhausted by mid-morning every day.
 *
 *  Removing the cron paths eliminates the entire class of problem:
 *    - No scheduled invocations means no lock contention
 *    - No global watermarks means no concurrency races
 *    - Per-buyer fetches are 1–2 Etsy calls each, bounded by the rate
 *      of incoming customer messages (a few per hour at peak)
 *
 *  Customer doc shape (EtsyMail_Customers/{buyerUserId}):
 *    {
 *      buyerUserId, displayName, currency,
 *      orderCount, totalSpent,
 *      firstOrderAt, lastOrderAt,
 *      isRepeatBuyer,
 *      recentReceipts: [
 *        { receiptId, orderedAt, grandTotal, status }
 *      ],
 *      updatedAt
 *    }
 *
 *  ═══ INVOCATION ═══
 *
 *  POST /.netlify/functions/etsyMailSync-background
 *    { mode: "buyer", buyerUserId: "<numeric id>" }
 *
 *  Triggered by:
 *    - etsyMailSnapshot.js after each scrape (fire-and-forget)
 *    - etsyMailDraftReply.js v3.32 lazy recovery when customer doc is
 *      missing despite the thread having buyerUserId set
 *    - Manual operator-triggered refresh from the inbox
 *
 *  Any other `mode` value returns 400. No cron. No backfill.
 */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");
const meter = require("./_etsyApiMeter");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── Config ────────────────────────────────────────────────────────────────
const SHOP_ID       = process.env.SHOP_ID;
const CLIENT_ID     = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET;

const OAUTH_DOC_PATH = "config/etsyOauth";
const TOKEN_REFRESH_BUFFER_MS = 2 * 60 * 1000;

const PAGE_SIZE = 100;
const REQUEST_DELAY_MS = 0;     // No artificial delay between pages
const MAX_INVOCATION_MS = 13 * 60 * 1000;   // 13 min — leaves 2 min cleanup tail
const RECENT_RECEIPTS_CAP = 10;

// ─── Rate-limit guards ─────────────────────────────────────────────────────
//
// Etsy's daily rate limit returns retry-after values measured in hours.
// Netlify background functions die at 15 minutes. Sleeping past that
// budget is futile and leaves the system in a worse state than aborting
// cleanly. These error types let getReceiptsPage signal "this can't
// complete within our function budget" and the handler returns 503 with
// a retry hint — neither logs as an error.
class RateLimitNoBudgetError extends Error {
  constructor(retryAfterSec, remainingMs) {
    super(`Etsy rate limit retry-after=${retryAfterSec}s exceeds remaining function budget=${Math.round(remainingMs/1000)}s — aborting`);
    this.name = "RateLimitNoBudgetError";
    this.code = "RATE_LIMIT_NO_BUDGET";
    this.retryAfterSec = retryAfterSec;
  }
}

class DailyRateLimitError extends Error {
  constructor(detail) {
    super(`Etsy daily rate limit hit: ${detail}`);
    this.name = "DailyRateLimitError";
    this.code = "DAILY_RATE_LIMIT";
  }
}

const FINALLY_TAIL_MS = 30 * 1000;

// ─── OAuth token management ────────────────────────────────────────────────
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

// ─── HTTP helpers ──────────────────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function moneyAmt(m) {
  if (!m || typeof m.amount !== "number" || typeof m.divisor !== "number") return null;
  return m.amount / m.divisor;
}

/**
 * Fetch one page of getShopReceipts. Handles 429 with budget awareness:
 *   - Short retry-after (per-second limit) → sleep if within budget,
 *     else throw RateLimitNoBudgetError.
 *   - Long retry-after (>1 hour) or "daily" in body → throw
 *     DailyRateLimitError immediately. Writes etsyDailyLimitResetAt
 *     to syncState so other callers can pre-empt.
 */
async function getReceiptsPage(accessToken, params, attempt = 1, invocationStartMs = Date.now()) {
  const qs = new URLSearchParams(Object.fromEntries(
    Object.entries(params).filter(([_, v]) => v != null && v !== "")
  )).toString();
  const url = `https://api.etsy.com/v3/application/shops/${SHOP_ID}/receipts?${qs}`;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 30000);

  // METER — bump on EVERY fetch attempt, including retries. The recursive
  // 429-retry below calls this function again, which re-enters here and
  // bumps fresh. That's correct: each retry IS a new API call.
  const _meterToken = meter.bump("sync.receiptsPage");

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
      if (attempt > 3) throw new Error("Etsy API timeout after 3 attempts (30s each)");
      console.warn(`Etsy fetch timeout (attempt ${attempt}/3), retrying…`);
      await sleep(2000);
      return getReceiptsPage(accessToken, params, attempt + 1, invocationStartMs);
    }
    throw err;
  }
  clearTimeout(timeoutId);
  _meterToken.fromHttp(res.status);

  if (res.status === 429) {
    if (attempt > 3) throw new Error("Etsy rate limit exceeded after 3 retries");
    const retryAfter = parseInt(res.headers.get("retry-after") || "5", 10);
    const bodyText = await res.text().catch(() => "");

    const isDaily = /daily|day/i.test(bodyText) || retryAfter > 3600;
    if (isDaily) {
      console.warn(`Etsy DAILY rate limit hit: retry-after=${retryAfter}s, body="${bodyText.slice(0, 200)}"`);
      try {
        await db.collection("EtsyMail_Config").doc("syncState").set({
          etsyDailyLimitHitAt   : FV.serverTimestamp(),
          etsyDailyLimitResetAt : admin.firestore.Timestamp.fromMillis(Date.now() + retryAfter * 1000),
          etsyDailyLimitDetail  : bodyText.slice(0, 300)
        }, { merge: true });
      } catch {}
      throw new DailyRateLimitError(`retry-after=${retryAfter}s body="${bodyText.slice(0, 150)}"`);
    }

    const elapsed     = Date.now() - invocationStartMs;
    const remainingMs = MAX_INVOCATION_MS - elapsed - FINALLY_TAIL_MS;
    const needMs      = retryAfter * 1000;
    if (needMs > remainingMs) {
      throw new RateLimitNoBudgetError(retryAfter, remainingMs);
    }
    console.warn(`Rate limit (attempt ${attempt}), sleeping ${retryAfter}s (budget remaining: ${Math.round(remainingMs/1000)}s)`);
    await sleep(needMs);
    return getReceiptsPage(accessToken, params, attempt + 1, invocationStartMs);
  }

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Etsy getShopReceipts failed ${res.status}: ${text.slice(0, 300)}`);
  }
  return await res.json();
}

// ─── Receipt → customer summary transform ──────────────────────────────────
function receiptToSummary(r) {
  return {
    receiptId  : String(r.receipt_id),
    orderedAt  : r.created_timestamp ? r.created_timestamp * 1000 : null,
    updatedAt  : r.updated_timestamp ? r.updated_timestamp * 1000 : null,
    grandTotal : moneyAmt(r.grandtotal),
    currency   : r.grandtotal ? r.grandtotal.currency_code : null,
    status     : r.status || null,
    isPaid     : !!r.is_paid,
    isShipped  : !!r.is_shipped,
    buyerUserId: r.buyer_user_id || null,
    buyerName  : r.name || null
  };
}

// ─── Customer aggregator ───────────────────────────────────────────────────
//
// In-memory map of buyerUserId → aggregated state. Single-buyer mode
// only ever has one entry, but we keep the aggregator shape for clarity
// and so the upsert/merge logic stays straightforward.
function createCustomerAggregator() {
  const byBuyer = new Map();

  function accumulate(summary) {
    if (!summary.buyerUserId) return;
    const userId = summary.buyerUserId;
    let agg = byBuyer.get(userId);
    if (!agg) {
      agg = { displayName: null, currency: null, freshReceipts: new Map() };
      byBuyer.set(userId, agg);
    }
    if (summary.buyerName && !agg.displayName) agg.displayName = summary.buyerName;
    if (summary.currency  && !agg.currency)    agg.currency    = summary.currency;
    agg.freshReceipts.set(summary.receiptId, summary);
  }

  async function flush() {
    const buyerIds = Array.from(byBuyer.keys());
    if (!buyerIds.length) return 0;

    let customersUpdated = 0;

    // Buyer mode is single-buyer; the chunk loop is theoretical but kept
    // small for safety.
    const CONCURRENCY = 20;
    for (let i = 0; i < buyerIds.length; i += CONCURRENCY) {
      const chunk = buyerIds.slice(i, i + CONCURRENCY);
      const refs = chunk.map(id => db.collection("EtsyMail_Customers").doc(String(id)));
      const snaps = await Promise.all(refs.map(r => r.get()));

      const docsToWrite = chunk.map((userId, idx) => {
        const agg = byBuyer.get(userId);
        const existing = snaps[idx].exists ? snaps[idx].data() : null;

        const existingRecent = (existing && Array.isArray(existing.recentReceipts))
          ? existing.recentReceipts : [];
        const existingReceiptIds = new Set(existingRecent.map(r => r.receiptId));

        let newOrderCount = 0;
        let newTotalSpent = 0;
        let newFirstMs = null;
        let newLastMs = null;

        for (const [rid, s] of agg.freshReceipts.entries()) {
          if (!existingReceiptIds.has(rid)) {
            newOrderCount++;
            if (typeof s.grandTotal === "number") newTotalSpent += s.grandTotal;
            if (s.orderedAt) {
              if (newFirstMs === null || s.orderedAt < newFirstMs) newFirstMs = s.orderedAt;
              if (newLastMs  === null || s.orderedAt > newLastMs)  newLastMs  = s.orderedAt;
            }
          }
        }

        const existingFirst = existing && existing.firstOrderAt && existing.firstOrderAt.toMillis
          ? existing.firstOrderAt.toMillis() : null;
        const existingLast = existing && existing.lastOrderAt && existing.lastOrderAt.toMillis
          ? existing.lastOrderAt.toMillis() : null;

        const orderCount = (existing ? (existing.orderCount || 0) : 0) + newOrderCount;
        const totalSpent = Math.round(
          ((existing ? (existing.totalSpent || 0) : 0) + newTotalSpent) * 100
        ) / 100;

        const firstCandidates = [existingFirst, newFirstMs].filter(v => v != null && v > 0);
        const lastCandidates  = [existingLast,  newLastMs ].filter(v => v != null && v > 0);
        const finalFirst = firstCandidates.length ? Math.min(...firstCandidates) : null;
        const finalLast  = lastCandidates.length  ? Math.max(...lastCandidates)  : null;

        const byId = new Map();
        for (const r of existingRecent) {
          const orderedAtMs = r.orderedAt && r.orderedAt.toMillis
            ? r.orderedAt.toMillis()
            : (typeof r.orderedAt === "number" ? r.orderedAt : null);
          byId.set(r.receiptId, { ...r, orderedAt: orderedAtMs });
        }
        for (const [rid, s] of agg.freshReceipts.entries()) {
          byId.set(rid, {
            receiptId : rid,
            orderedAt : s.orderedAt,
            grandTotal: s.grandTotal,
            currency  : s.currency,
            status    : s.status,
            isPaid    : s.isPaid,
            isShipped : s.isShipped
          });
        }
        const recentReceipts = Array.from(byId.values())
          .sort((a, b) => (b.orderedAt || 0) - (a.orderedAt || 0))
          .slice(0, RECENT_RECEIPTS_CAP)
          .map(r => ({
            ...r,
            orderedAt: r.orderedAt ? admin.firestore.Timestamp.fromMillis(r.orderedAt) : null
          }));

        const displayName = agg.displayName
          || (existing ? existing.displayName : null)
          || "Unknown";
        const currency = agg.currency
          || (existing ? existing.currency : null)
          || "USD";

        return {
          userId,
          doc: {
            buyerUserId   : userId,
            displayName,
            currency,
            orderCount,
            totalSpent,
            firstOrderAt  : finalFirst ? admin.firestore.Timestamp.fromMillis(finalFirst) : null,
            lastOrderAt   : finalLast  ? admin.firestore.Timestamp.fromMillis(finalLast)  : null,
            isRepeatBuyer : orderCount >= 2,
            recentReceipts,
            updatedAt     : FV.serverTimestamp()
          }
        };
      });

      const batch = db.batch();
      for (const { userId, doc } of docsToWrite) {
        batch.set(db.collection("EtsyMail_Customers").doc(String(userId)), doc, { merge: true });
      }
      await batch.commit();
      customersUpdated += docsToWrite.length;
    }

    return customersUpdated;
  }

  return { accumulate, flush, get buyerCount() { return byBuyer.size; } };
}

// ─── Per-buyer sync ────────────────────────────────────────────────────────
async function runBuyerSync({ buyerUserId, invocationStartMs }) {
  const deadlineMs = invocationStartMs + MAX_INVOCATION_MS;
  const accessToken = await getValidEtsyAccessToken();

  if (!buyerUserId) {
    throw new Error("runBuyerSync requires buyerUserId");
  }

  const params = {
    limit         : PAGE_SIZE,
    sort_on       : "created",
    sort_order    : "desc",
    buyer_user_id : String(buyerUserId)
  };

  const aggregator = createCustomerAggregator();
  let offset = 0;
  let pagesFetched = 0;
  let receiptsProcessed = 0;

  while (true) {
    if (Date.now() > deadlineMs) break;

    const page = await getReceiptsPage(accessToken, { ...params, offset }, 1, invocationStartMs);
    pagesFetched++;

    const receipts = page.results || [];
    if (!receipts.length) break;

    for (const r of receipts) {
      aggregator.accumulate(receiptToSummary(r));
    }
    receiptsProcessed += receipts.length;

    if (receipts.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    if (offset >= 12000) break;
    if (REQUEST_DELAY_MS) await sleep(REQUEST_DELAY_MS);
  }

  const customersUpdated = await aggregator.flush();

  console.log(`Buyer sync complete for ${buyerUserId}: pages=${pagesFetched} receipts=${receiptsProcessed} customers=${customersUpdated}`);
  return { receiptsProcessed, customersUpdated, pagesFetched, buyerUserId };
}

// ─── Entry ─────────────────────────────────────────────────────────────────
exports.handler = meter.wrapHandler(async (event) => {
  const invocationStartMs = Date.now();

  // ─── DIAGNOSTIC: per-invocation telemetry ────────────────────────────
  // Bumps a separate meter counter so the UI can see invocation count
  // independent of page-call count. Also logs caller-identifying headers
  // so we can correlate invocations back to their trigger source
  // (snapshot pipeline, draft-reply lazy recovery, manual operator, ...).
  meter.bumpSimple("sync.invocation");
  const _callerUA      = (event.headers && (event.headers["user-agent"] || event.headers["User-Agent"])) || null;
  const _callerReferer = (event.headers && (event.headers["referer"]    || event.headers["Referer"]))    || null;
  const _xForwardedFor = (event.headers && (event.headers["x-forwarded-for"] || event.headers["X-Forwarded-For"])) || null;
  // We can also see "via" if the call came from another Netlify function.
  const _callerOrigin  = (event.headers && (event.headers["origin"]     || event.headers["Origin"]))     || null;
  console.log(`[sync] INVOCATION_START ua="${(_callerUA || "").slice(0, 80)}" referer="${(_callerReferer || "").slice(0, 80)}" xff="${(_xForwardedFor || "").slice(0, 80)}" origin="${(_callerOrigin || "").slice(0, 80)}"`);

  if (!SHOP_ID || !CLIENT_ID || !CLIENT_SECRET) {
    console.error("Missing SHOP_ID / CLIENT_ID / CLIENT_SECRET env vars");
    return { statusCode: 500, body: "Missing env vars" };
  }

  let mode = null;
  let buyerUserId = null;
  try {
    if (event.body) {
      const body = JSON.parse(event.body);
      if (body.mode) mode = body.mode;
      if (body.buyerUserId) buyerUserId = String(body.buyerUserId);
    }
    if (event.queryStringParameters) {
      if (event.queryStringParameters.mode) mode = event.queryStringParameters.mode;
      if (event.queryStringParameters.buyerUserId) {
        buyerUserId = String(event.queryStringParameters.buyerUserId);
      }
    }
  } catch {}

  // DIAGNOSTIC: log the parsed mode + buyerUserId so we can see the
  // payload that triggered this invocation.
  console.log(`[sync] INVOCATION_PARAMS mode="${mode}" buyerUserId="${buyerUserId}"`);

  // Daily rate-limit short-circuit. If a prior invocation hit Etsy's
  // daily limit, refuse to make any Etsy API calls until the reset
  // time passes — every call would just 429 anyway.
  try {
    const stateSnap = await db.collection("EtsyMail_Config").doc("syncState").get();
    const state = stateSnap.exists ? stateSnap.data() : null;
    const resetTs = state && state.etsyDailyLimitResetAt;
    const resetMs = resetTs && resetTs.toMillis ? resetTs.toMillis() : 0;
    if (resetMs > Date.now()) {
      const waitMin = Math.ceil((resetMs - Date.now()) / 60000);
      console.warn(`etsyMailSync-background: Etsy daily limit active, ${waitMin} min until reset — skipping`);
      return {
        statusCode: 200,
        body: JSON.stringify({
          skipped: true,
          reason : "etsy_daily_rate_limit_active",
          waitMinutes: waitMin
        })
      };
    }
  } catch (e) {
    console.warn("etsyMailSync-background: daily-limit pre-check failed (continuing):", e.message);
  }

  // BUYER MODE is the only supported mode.
  if (mode !== "buyer") {
    return {
      statusCode: 400,
      body: JSON.stringify({
        error: "Only mode=buyer is supported. Cron-driven full/incremental sync was removed in 2026-05.",
        receivedMode: mode || null
      })
    };
  }

  if (!buyerUserId) {
    return { statusCode: 400, body: "buyer mode requires buyerUserId" };
  }

  try {
    const result = await runBuyerSync({ buyerUserId, invocationStartMs });
    // DIAGNOSTIC: log invocation completion with page count so we can see
    // per-invocation behavior in the meter + Netlify log.
    const elapsedSec = ((Date.now() - invocationStartMs) / 1000).toFixed(1);
    console.log(`[sync] INVOCATION_END buyerUserId=${buyerUserId} pagesFetched=${result.pagesFetched} receipts=${result.receiptsProcessed} customersUpdated=${result.customersUpdated} elapsed=${elapsedSec}s`);
    return { statusCode: 200, body: JSON.stringify({ ok: true, mode: "buyer", ...result }) };
  } catch (err) {
    if (err.code === "DAILY_RATE_LIMIT" || err.code === "RATE_LIMIT_NO_BUDGET") {
      console.warn(`etsyMailSync-background buyer mode aborted for ${buyerUserId}: ${err.code}`);
      return { statusCode: 503, body: JSON.stringify({ ok: false, code: err.code, retry: true }) };
    }
    console.error(`etsyMailSync-background buyer mode failed for ${buyerUserId}:`, err.message || err);
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: err.message || String(err) }) };
  }
});
