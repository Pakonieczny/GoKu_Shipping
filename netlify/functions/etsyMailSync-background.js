/*  netlify/functions/etsyMailSync-background.js
 *
 *  M3 Etsy customer-summaries sync — LIGHTWEIGHT version.
 *
 *  ═══ WHAT THIS DOES ═══
 *
 *  Builds/updates per-customer summary docs in EtsyMail_Customers based on
 *  receipts fetched from Etsy's getShopReceipts API. Does NOT store full
 *  receipts — when an operator clicks into a specific order in the inbox,
 *  that's a live call to etsyOrderProxy.js for fresh data.
 *
 *  Customer doc shape (EtsyMail_Customers/{buyerUserId}):
 *    {
 *      buyerUserId, displayName, currency,
 *      orderCount, totalSpent,
 *      firstOrderAt, lastOrderAt,
 *      isRepeatBuyer,   // orderCount >= 2
 *      recentReceipts: [   // up to 10, newest first
 *        { receiptId, orderedAt, grandTotal, status }
 *      ],
 *      updatedAt
 *    }
 *
 *  ═══ WHY WINDOWED SYNC ═══
 *
 *  Etsy's offset cap is 12,000. For a 250K-order shop, we can't paginate
 *  straight through. Instead we window by creation date (15-day windows)
 *  — each window has well under 12K orders.
 *
 *  ═══ INVOCATION ═══
 *
 *  Scheduled (netlify.toml):
 *    - If backfillInProgress: resume the windowed backfill from cursor
 *    - Else: run incremental (receipts modified since last watermark)
 *
 *  Manual trigger (via etsyMailSync?action=trigger):
 *    - mode=full: start/restart a 2-year backfill
 *    - mode=incremental: single incremental pass
 *
 *  ═══ STORAGE IMPACT ═══
 *
 *  At ~50K unique buyers over 2 years, ~1 KB each = ~50 MB total.
 *  No receipt storage. Incremental syncs read+merge existing customer
 *  docs so running totals are preserved.
 */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── Config ────────────────────────────────────────────────────────────────
const SHOP_ID       = process.env.SHOP_ID;
const CLIENT_ID     = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET;

const OAUTH_DOC_PATH = "config/etsyOauth";
const TOKEN_REFRESH_BUFFER_MS = 2 * 60 * 1000;

const PAGE_SIZE = 100;
const REQUEST_DELAY_MS = 500;
const DEFAULT_DAYS_BACK = 730;

// 15-day windows leave safety margin for shops with up to ~800 orders/day.
const WINDOW_SIZE_DAYS = 15;

// Stop 13 min into the 15-min budget so there's time for final writes.
const MAX_INVOCATION_MS = 13 * 60 * 1000;

// Cap per-invocation API pages as defense-in-depth.
const MAX_PAGES_PER_INVOCATION = 1200;

// Recent-receipts list length on each customer doc.
const RECENT_RECEIPTS_CAP = 10;

// ─── OAuth token management ────────────────────────────────────────────────
async function readEtsyToken() {
  const snap = await db.doc(OAUTH_DOC_PATH).get();
  return snap.exists ? snap.data() : null;
}

async function refreshEtsyToken(oldRefreshToken) {
  if (!CLIENT_ID) throw new Error("CLIENT_ID env var missing");
  const res = await fetch("https://api.etsy.com/v3/public/oauth/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "refresh_token",
      client_id: CLIENT_ID,
      refresh_token: oldRefreshToken
    })
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Etsy token refresh failed: ${res.status} ${body}`);
  }
  const data = await res.json();
  const expires_at = Date.now() + Math.max(0, (data.expires_in - 120)) * 1000;
  const stored = {
    access_token : data.access_token,
    refresh_token: data.refresh_token || oldRefreshToken,
    expires_at,
    updatedAt    : FV.serverTimestamp()
  };
  await db.doc(OAUTH_DOC_PATH).set(stored, { merge: true });
  return stored.access_token;
}

async function getValidEtsyAccessToken() {
  const tok = await readEtsyToken();
  if (!tok) throw new Error(
    `Etsy OAuth token doc not found at ${OAUTH_DOC_PATH}. Seed via etsyMailSeedTokens first.`
  );
  if (!tok.refresh_token) throw new Error(`No refresh_token in ${OAUTH_DOC_PATH}.`);
  const expiresAt = typeof tok.expires_at === "number" ? tok.expires_at : 0;
  if (!tok.access_token || expiresAt - Date.now() < TOKEN_REFRESH_BUFFER_MS) {
    return await refreshEtsyToken(tok.refresh_token);
  }
  return tok.access_token;
}

// ─── Helpers ───────────────────────────────────────────────────────────────
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function moneyAmt(m) {
  if (!m || typeof m.amount !== "number" || typeof m.divisor !== "number") return null;
  return m.amount / m.divisor;
}

/**
 * Fetch one page of getShopReceipts. Handles 429 by sleeping. Retries up to 3.
 */
async function getReceiptsPage(accessToken, params, attempt = 1) {
  const qs = new URLSearchParams(Object.fromEntries(
    Object.entries(params).filter(([_, v]) => v != null && v !== "")
  )).toString();
  const url = `https://api.etsy.com/v3/application/shops/${SHOP_ID}/receipts?${qs}`;
  const res = await fetch(url, {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "x-api-key": `${CLIENT_ID}:${CLIENT_SECRET}`,
      "Content-Type": "application/json"
    }
  });

  if (res.status === 429) {
    if (attempt > 3) throw new Error("Etsy rate limit exceeded after 3 retries");
    const retryAfter = parseInt(res.headers.get("retry-after") || "5", 10);
    console.warn(`Rate limit (attempt ${attempt}), sleeping ${retryAfter}s`);
    await sleep(retryAfter * 1000);
    return getReceiptsPage(accessToken, params, attempt + 1);
  }

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Etsy getShopReceipts failed ${res.status}: ${text.slice(0, 300)}`);
  }
  return await res.json();
}

/**
 * Extract the minimum fields we need from an Etsy receipt for customer
 * summaries. We never persist the full receipt.
 */
function receiptToSummary(r) {
  return {
    receiptId  : String(r.receipt_id),
    orderedAt  : r.created_timestamp ? r.created_timestamp * 1000 : null,  // ms
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

/**
 * In-memory aggregator. Feed it receipts (summaries), flush at end to
 * upsert customer docs. Merges with any existing customer docs so
 * incremental syncs preserve running totals.
 */
function createCustomerAggregator() {
  // buyerUserId → { displayName, currency, freshReceipts: Map(receiptId → summary) }
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
    if (summary.currency && !agg.currency) agg.currency = summary.currency;
    agg.freshReceipts.set(summary.receiptId, summary);
  }

  /**
   * Flush to Firestore. Reads existing docs in parallel chunks, merges
   * aggregates, writes in batches.
   */
  async function flush(progressFn) {
    const buyerIds = Array.from(byBuyer.keys());
    if (!buyerIds.length) return 0;

    let customersUpdated = 0;
    const CONCURRENCY = 20;
    const BATCH_WRITE = 400;

    for (let i = 0; i < buyerIds.length; i += CONCURRENCY) {
      const chunk = buyerIds.slice(i, i + CONCURRENCY);

      // Parallel read of existing customer docs
      const refs = chunk.map(id => db.collection("EtsyMail_Customers").doc(String(id)));
      const snaps = await Promise.all(refs.map(r => r.get()));

      const docsToWrite = chunk.map((userId, idx) => {
        const agg = byBuyer.get(userId);
        const existing = snaps[idx].exists ? snaps[idx].data() : null;

        // Figure out which of our "fresh" receipts are actually new vs updates
        // to receipts already known to the existing customer doc. We use the
        // recentReceipts list as the known set (capped list — won't catch
        // all historical, but sufficient for incremental dedup).
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

        // Aggregate
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

        // Merge recent receipts: existing + fresh, dedup by receiptId,
        // sort by orderedAt desc, cap to RECENT_RECEIPTS_CAP.
        const byId = new Map();
        for (const r of existingRecent) {
          // Existing doc stores orderedAt as Firestore Timestamp — convert to ms
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

      // Batched writes
      for (let j = 0; j < docsToWrite.length; j += BATCH_WRITE) {
        const batch = db.batch();
        for (const { userId, doc } of docsToWrite.slice(j, j + BATCH_WRITE)) {
          batch.set(db.collection("EtsyMail_Customers").doc(String(userId)), doc, { merge: true });
        }
        await batch.commit();
        customersUpdated += Math.min(BATCH_WRITE, docsToWrite.length - j);
      }

      if (progressFn) progressFn(i + chunk.length, buyerIds.length);
    }

    return customersUpdated;
  }

  return { accumulate, flush, get buyerCount() { return byBuyer.size; } };
}

// ─── State management ──────────────────────────────────────────────────────
async function readSyncState() {
  const snap = await db.collection("EtsyMail_Config").doc("syncState").get();
  return snap.exists ? snap.data() : null;
}

async function writeSyncState(patch) {
  await db.collection("EtsyMail_Config").doc("syncState").set({
    ...patch, updatedAt: FV.serverTimestamp()
  }, { merge: true });
}

// ─── Window fetcher ────────────────────────────────────────────────────────
async function fetchWindow({ accessToken, windowStartSec, windowEndSec, aggregator, deadlineMs }) {
  const baseParams = {
    limit: PAGE_SIZE,
    sort_on: "created",
    sort_order: "asc",
    min_created: windowStartSec,
    max_created: windowEndSec - 1
  };

  let offset = 0;
  let pagesFetched = 0;
  let receiptsProcessed = 0;
  let hitOffsetCap = false;
  let ranOutOfTime = false;

  while (true) {
    if (Date.now() > deadlineMs) { ranOutOfTime = true; break; }

    const page = await getReceiptsPage(accessToken, { ...baseParams, offset });
    pagesFetched++;

    const receipts = page.results || [];
    if (!receipts.length) break;

    for (const r of receipts) aggregator.accumulate(receiptToSummary(r));
    receiptsProcessed += receipts.length;

    if (receipts.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    if (offset >= 12000) { hitOffsetCap = true; break; }

    await sleep(REQUEST_DELAY_MS);
  }

  return { pagesFetched, receiptsProcessed, hitOffsetCap, ranOutOfTime };
}

// ─── Full backfill (windowed, resumable) ───────────────────────────────────
async function runBackfill({ daysBack, invocationStartMs }) {
  const deadlineMs = invocationStartMs + MAX_INVOCATION_MS;
  const accessToken = await getValidEtsyAccessToken();

  const syncState = await readSyncState();
  const nowSec = Math.floor(Date.now() / 1000);
  const oldestTargetSec = nowSec - (daysBack * 86400);

  let windowEndSec;
  let completedWindows;
  let receiptsTotal;

  if (syncState && syncState.backfillInProgress) {
    windowEndSec = syncState.backfillWindowEnd;
    completedWindows = syncState.backfillCompletedWindows || 0;
    receiptsTotal = syncState.backfillReceiptsTotal || 0;
    console.log(`Resuming backfill: completedWindows=${completedWindows} nextWindowEnd=${new Date(windowEndSec*1000).toISOString()}`);
  } else {
    windowEndSec = nowSec;
    completedWindows = 0;
    receiptsTotal = 0;
    console.log(`Starting new backfill: daysBack=${daysBack} oldestTarget=${new Date(oldestTargetSec*1000).toISOString()}`);
  }

  const aggregator = createCustomerAggregator();
  let pagesInvocation = 0;
  let receiptsInvocation = 0;

  while (windowEndSec > oldestTargetSec) {
    if (Date.now() > deadlineMs || pagesInvocation >= MAX_PAGES_PER_INVOCATION) break;

    const windowStartSec = Math.max(oldestTargetSec, windowEndSec - (WINDOW_SIZE_DAYS * 86400));

    const result = await fetchWindow({
      accessToken, windowStartSec, windowEndSec, aggregator, deadlineMs
    });

    pagesInvocation += result.pagesFetched;
    receiptsInvocation += result.receiptsProcessed;
    receiptsTotal += result.receiptsProcessed;

    if (result.hitOffsetCap) {
      console.warn(`Window ${new Date(windowStartSec*1000).toISOString()} hit offset cap. Halving.`);
      const midSec = Math.floor((windowStartSec + windowEndSec) / 2);
      windowEndSec = midSec;
      continue;
    }

    if (result.ranOutOfTime) break;

    windowEndSec = windowStartSec;
    completedWindows++;

    await writeSyncState({
      backfillInProgress      : true,
      backfillMode            : "full",
      backfillWindowEnd       : windowEndSec,
      backfillOldestTarget    : oldestTargetSec,
      backfillCompletedWindows: completedWindows,
      backfillReceiptsTotal   : receiptsTotal,
      lastSyncProgress        : {
        phase: "receipts",
        completedWindows,
        receiptsTotal,
        buyersInBatch: aggregator.buyerCount,
        currentWindow: {
          start: new Date(windowStartSec * 1000).toISOString(),
          end: new Date(windowEndSec * 1000).toISOString()
        }
      }
    });
  }

  // Flush customer aggregates for this invocation
  let customersUpdated = 0;
  if (aggregator.buyerCount > 0) {
    await writeSyncState({
      lastSyncProgress: { phase: "customers", buyersToProcess: aggregator.buyerCount, receiptsTotal }
    });
    customersUpdated = await aggregator.flush((done, total) => {
      if (done % 100 === 0) {
        writeSyncState({
          lastSyncProgress: { phase: "customers", buyersProcessed: done, buyersTotal: total, receiptsTotal }
        }).catch(() => {});
      }
    });
  }

  const done = windowEndSec <= oldestTargetSec;
  const stateUpdate = {
    backfillInProgress: !done,
    lastSyncProgress: {
      phase: done ? "done" : "paused",
      completedWindows,
      receiptsTotal,
      customersThisInvocation: customersUpdated,
      receiptsThisInvocation: receiptsInvocation,
      nextWindowEnd: done ? null : new Date(windowEndSec * 1000).toISOString()
    }
  };

  if (done) {
    stateUpdate.backfillMode = null;
    stateUpdate.backfillWindowEnd = null;
    stateUpdate.backfillCompletedWindows = completedWindows;
    stateUpdate.backfillReceiptsTotal = receiptsTotal;
    stateUpdate.lastSyncCompletedAt = FV.serverTimestamp();
    stateUpdate.lastSyncMode = "full";
    stateUpdate.lastSyncReceiptsCount = receiptsTotal;
    stateUpdate.lastReceiptUpdatedAt = admin.firestore.Timestamp.fromMillis(Date.now());
    stateUpdate.lastSyncDurationMs = Date.now() - invocationStartMs;
  }

  await writeSyncState(stateUpdate);

  console.log(`Backfill ${done ? "COMPLETE" : "PAUSED"}: completedWindows=${completedWindows} receiptsScanned=${receiptsTotal} customersThisInvocation=${customersUpdated}`);
  return { done, completedWindows, receiptsTotal, receiptsInvocation, customersUpdated };
}

// ─── Incremental sync ──────────────────────────────────────────────────────
async function runIncremental({ invocationStartMs }) {
  const deadlineMs = invocationStartMs + MAX_INVOCATION_MS;
  const accessToken = await getValidEtsyAccessToken();
  const syncState = await readSyncState();

  let sinceSec = null;
  if (syncState && syncState.lastReceiptUpdatedAt) {
    const ms = syncState.lastReceiptUpdatedAt.toMillis
      ? syncState.lastReceiptUpdatedAt.toMillis()
      : new Date(syncState.lastReceiptUpdatedAt).getTime();
    sinceSec = Math.floor(ms / 1000);
  }

  const params = { limit: PAGE_SIZE, sort_on: "updated", sort_order: "asc" };
  if (sinceSec) {
    params.min_last_modified = sinceSec;
  } else {
    params.min_created = Math.floor((Date.now() - DEFAULT_DAYS_BACK * 86400 * 1000) / 1000);
  }

  const aggregator = createCustomerAggregator();
  let offset = 0;
  let pagesFetched = 0;
  let receiptsProcessed = 0;
  let newestUpdatedAtMs = 0;
  let hitOffsetCap = false;

  while (true) {
    if (Date.now() > deadlineMs) break;

    const page = await getReceiptsPage(accessToken, { ...params, offset });
    pagesFetched++;

    const receipts = page.results || [];
    if (!receipts.length) break;

    for (const r of receipts) {
      aggregator.accumulate(receiptToSummary(r));
      if (r.updated_timestamp && r.updated_timestamp * 1000 > newestUpdatedAtMs) {
        newestUpdatedAtMs = r.updated_timestamp * 1000;
      }
    }
    receiptsProcessed += receipts.length;

    if (receipts.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    if (offset >= 12000) { hitOffsetCap = true; break; }
    await sleep(REQUEST_DELAY_MS);
  }

  const customersUpdated = await aggregator.flush();

  const nextState = {
    lastSyncMode          : "incremental",
    lastSyncStartedAt     : admin.firestore.Timestamp.fromMillis(invocationStartMs),
    lastSyncCompletedAt   : FV.serverTimestamp(),
    lastSyncReceiptsCount : receiptsProcessed,
    lastSyncCustomersCount: customersUpdated,
    lastSyncPagesFetched  : pagesFetched,
    lastSyncHitOffsetCap  : hitOffsetCap,
    lastSyncInProgress    : false,
    lastSyncError         : null,
    lastSyncErrorAt       : null,
    lastSyncDurationMs    : Date.now() - invocationStartMs,
    lastSyncProgress      : { phase: "done", receiptsProcessed, customersUpdated, pagesFetched }
  };
  if (newestUpdatedAtMs > 0) {
    nextState.lastReceiptUpdatedAt = admin.firestore.Timestamp.fromMillis(newestUpdatedAtMs);
  }
  await writeSyncState(nextState);

  console.log(`Incremental complete: pages=${pagesFetched} receipts=${receiptsProcessed} customers=${customersUpdated}`);
  return { receiptsProcessed, customersUpdated, pagesFetched };
}

// ─── Entry ─────────────────────────────────────────────────────────────────
exports.handler = async (event) => {
  const invocationStartMs = Date.now();

  if (!SHOP_ID || !CLIENT_ID || !CLIENT_SECRET) {
    console.error("Missing SHOP_ID / CLIENT_ID / CLIENT_SECRET env vars");
    return { statusCode: 500, body: "Missing env vars" };
  }

  let mode = null;
  let daysBack = DEFAULT_DAYS_BACK;
  try {
    if (event.body) {
      const body = JSON.parse(event.body);
      if (body.mode) mode = body.mode;
      if (body.daysBack) daysBack = Math.max(1, parseInt(body.daysBack, 10));
    }
    if (event.queryStringParameters) {
      if (event.queryStringParameters.mode) mode = event.queryStringParameters.mode;
      if (event.queryStringParameters.daysBack) {
        daysBack = Math.max(1, parseInt(event.queryStringParameters.daysBack, 10));
      }
    }
  } catch {}

  try {
    const state = await readSyncState();
    const resumingBackfill = state && state.backfillInProgress;

    await writeSyncState({
      lastSyncInProgress: true,
      lastSyncStartedAt : admin.firestore.Timestamp.fromMillis(invocationStartMs),
      lastSyncMode      : mode || (resumingBackfill ? "full" : "incremental"),
      lastSyncError     : null,
      lastSyncErrorAt   : null
    });

    if (mode === "full" || resumingBackfill) {
      const actualDaysBack = resumingBackfill && !mode
        ? (state.backfillOldestTarget
            ? Math.ceil((Date.now()/1000 - state.backfillOldestTarget) / 86400)
            : DEFAULT_DAYS_BACK)
        : daysBack;
      await runBackfill({ daysBack: actualDaysBack, invocationStartMs });
    } else {
      await runIncremental({ invocationStartMs });
    }

    await writeSyncState({ lastSyncInProgress: false });
    return { statusCode: 200, body: "Sync invocation complete" };

  } catch (err) {
    console.error("etsyMailSync-background fatal error:", err);
    try {
      await writeSyncState({
        lastSyncInProgress: false,
        lastSyncError     : err.message || String(err),
        lastSyncErrorAt   : FV.serverTimestamp()
      });
    } catch {}
    throw err;
  }
};
