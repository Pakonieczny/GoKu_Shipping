/*  netlify/functions/etsyMailSync.js
 *
 *  M3 Etsy receipts sync — consolidated single-endpoint function.
 *
 *  Uses existing server-side infrastructure:
 *    - ./firebaseAdmin.js → admin.firestore() (your shared Firebase init)
 *    - ./_etsyMailAuth.js → requireExtensionAuth() (X-EtsyMail-Secret header)
 *
 *  Token management is inlined (same logic as your existing etsyAuth.js:
 *  reads config/etsy/oauth, refreshes if within 2 min of expiry, keeps the
 *  rotated refresh_token in Firestore). Self-contained — no file-path
 *  guesses about where etsyAuth.js lives in your deploy.
 *
 *  ═══ ACTIONS ═══
 *
 *  GET /.netlify/functions/etsyMailSync?action=status
 *      → No auth. Returns sync state + receipt/customer counts. Safe for UI.
 *
 *  GET /.netlify/functions/etsyMailSync?action=run&mode=incremental
 *      → Auth via X-EtsyMail-Secret (or scheduled-invocation header).
 *        Syncs only receipts modified since last watermark.
 *
 *  GET /.netlify/functions/etsyMailSync?action=run&mode=full&daysBack=730
 *      → Auth required. Backfills N days of history.
 *
 *  Scheduled invocation (via netlify.toml):
 *      schedule = "*\u002F30 * * * *"  → auto-runs as incremental
 *
 *  ═══ DATA FLOW ═══
 *
 *    Etsy getShopReceipts (paginated, 500ms pacing)
 *      → EtsyMail_Receipts/{receiptId}       (one doc per receipt)
 *      → EtsyMail_Customers/{buyerUserId}    (derived aggregate per buyer)
 *      → EtsyMail_Config/syncState           (watermark for incremental)
 *
 *  Env vars required: SHOP_ID, CLIENT_ID, CLIENT_SECRET (already set).
 *  ETSYMAIL_EXTENSION_SECRET optional — if unset, falls through (dev mode,
 *  same behavior as all existing EtsyMail write-path functions).
 */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

// ─── Config ────────────────────────────────────────────────────────────────
const SHOP_ID       = process.env.SHOP_ID;
const CLIENT_ID     = process.env.CLIENT_ID;
const CLIENT_SECRET = process.env.CLIENT_SECRET || process.env.ETSY_SHARED_SECRET;

const OAUTH_DOC_PATH = "config/etsy/oauth";
const TOKEN_REFRESH_BUFFER_MS = 2 * 60 * 1000;

const PAGE_SIZE = 100;
const REQUEST_DELAY_MS = 500;    // 2 req/sec, well under Etsy's 10/sec limit
const MAX_PAGES = 200;
const DEFAULT_DAYS_BACK = 730;   // 2 years

// ─── OAuth token management (inlined from etsyAuth.js for self-containment) ───
// Same Firestore doc path your existing etsyAuth.js uses: config/etsy/oauth.
// Logic matches etsyAuth.js: refresh if within 2 min of expiry, rotate refresh
// token when Etsy returns a new one, keep old one if Etsy doesn't rotate.
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
    `Etsy OAuth token doc not found at ${OAUTH_DOC_PATH}. ` +
    `Complete your existing OAuth flow first (via exchangeToken.js).`
  );
  if (!tok.refresh_token) throw new Error(
    `Etsy OAuth doc at ${OAUTH_DOC_PATH} has no refresh_token field. ` +
    `Re-authorize via your existing OAuth flow.`
  );
  const expiresAt = typeof tok.expires_at === "number" ? tok.expires_at : 0;
  if (!tok.access_token || expiresAt - Date.now() < TOKEN_REFRESH_BUFFER_MS) {
    return await refreshEtsyToken(tok.refresh_token);
  }
  return tok.access_token;
}

// ─── Helpers ───────────────────────────────────────────────────────────────
function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function tsToIso(v) {
  if (!v) return null;
  if (v.toMillis) return new Date(v.toMillis()).toISOString();
  if (v instanceof Date) return v.toISOString();
  return null;
}

// Scheduled invocations bypass the secret check. Manual invocations go
// through the same auth helper as every other EtsyMail write-path function.
function checkRunAuth(event) {
  if (event.headers && event.headers["x-netlify-event"] === "schedule") {
    return { ok: true };
  }
  return requireExtensionAuth(event);
}

// ─── Etsy API: getShopReceipts ─────────────────────────────────────────────
async function getReceiptsPage(accessToken, params) {
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
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Etsy getShopReceipts failed ${res.status}: ${text.slice(0, 300)}`);
  }
  return await res.json();
}

// ─── Transform & persist ───────────────────────────────────────────────────
function moneyAmt(m) {
  if (!m || typeof m.amount !== "number" || typeof m.divisor !== "number") return null;
  return m.amount / m.divisor;
}

function receiptToDoc(receipt) {
  const items = Array.isArray(receipt.transactions) ? receipt.transactions.map(t => ({
    transactionId  : t.transaction_id || null,
    listingId      : t.listing_id || null,
    title          : t.title || null,
    quantity       : t.quantity || 0,
    price          : moneyAmt(t.price),
    sku            : t.sku || null,
    imageUrl       : (t.product_data && t.product_data.image_url) || null,
    variations     : Array.isArray(t.variations) ? t.variations.map(v => ({
      formatted_name : v.formatted_name || null,
      formatted_value: v.formatted_value || null
    })) : [],
    personalization: t.variations ? null : (t.personalization || null)
  })) : [];

  return {
    receiptId      : String(receipt.receipt_id),
    etsyReceiptId  : receipt.receipt_id,
    buyerUserId    : receipt.buyer_user_id || null,
    name           : receipt.name || null,
    status         : receipt.status || null,
    currency       : receipt.grandtotal ? receipt.grandtotal.currency_code : null,
    grandTotal     : moneyAmt(receipt.grandtotal),
    totalShipping  : moneyAmt(receipt.total_shipping_cost),
    totalTax       : moneyAmt(receipt.total_tax_cost),
    totalPrice     : moneyAmt(receipt.total_price),
    createdAt      : receipt.created_timestamp ? new Date(receipt.created_timestamp * 1000) : null,
    updatedAt      : receipt.updated_timestamp ? new Date(receipt.updated_timestamp * 1000) : null,
    shippedAt      : receipt.is_shipped && receipt.updated_timestamp
                      ? new Date(receipt.updated_timestamp * 1000) : null,
    isPaid         : !!receipt.is_paid,
    isShipped      : !!receipt.is_shipped,
    shippingAddress: {
      firstLine  : receipt.first_line || null,
      secondLine : receipt.second_line || null,
      city       : receipt.city || null,
      state      : receipt.state || null,
      zip        : receipt.zip || null,
      countryIso : receipt.country_iso || null
    },
    items,
    syncedAt       : FV.serverTimestamp(),
    raw            : receipt
  };
}

async function upsertReceipts(receipts) {
  if (!receipts.length) return { written: 0, affectedUserIds: [] };
  const BATCH = 400;
  let written = 0;
  const affectedUserIds = new Set();
  for (let i = 0; i < receipts.length; i += BATCH) {
    const batch = db.batch();
    for (const r of receipts.slice(i, i + BATCH)) {
      const doc = receiptToDoc(r);
      const ref = db.collection("EtsyMail_Receipts").doc(doc.receiptId);
      batch.set(ref, doc, { merge: true });
      written++;
      if (doc.buyerUserId) affectedUserIds.add(doc.buyerUserId);
    }
    await batch.commit();
  }
  return { written, affectedUserIds: Array.from(affectedUserIds) };
}

async function rebuildCustomers(affectedUserIds) {
  if (!affectedUserIds.length) return { customersUpdated: 0 };
  let customersUpdated = 0;
  for (const userId of affectedUserIds) {
    const snap = await db.collection("EtsyMail_Receipts")
      .where("buyerUserId", "==", userId)
      .get();
    if (snap.empty) continue;

    let orderCount = 0;
    let totalSpent = 0;
    let firstOrderAt = null;
    let lastOrderAt = null;
    let displayName = null;
    let currency = "USD";
    const receiptIds = [];

    snap.forEach(doc => {
      const r = doc.data();
      orderCount++;
      if (typeof r.grandTotal === "number") totalSpent += r.grandTotal;
      if (r.currency) currency = r.currency;
      if (r.name && !displayName) displayName = r.name;
      if (r.createdAt) {
        const ts = r.createdAt.toMillis ? r.createdAt.toMillis() : new Date(r.createdAt).getTime();
        if (!firstOrderAt || ts < firstOrderAt) firstOrderAt = ts;
        if (!lastOrderAt  || ts > lastOrderAt)  lastOrderAt  = ts;
      }
      receiptIds.push(doc.id);
    });

    receiptIds.sort((a, b) => b.localeCompare(a));

    const customerDoc = {
      buyerUserId   : userId,
      displayName   : displayName || "Unknown",
      orderCount,
      totalSpent    : Math.round(totalSpent * 100) / 100,
      currency,
      firstOrderAt  : firstOrderAt ? admin.firestore.Timestamp.fromMillis(firstOrderAt) : null,
      lastOrderAt   : lastOrderAt  ? admin.firestore.Timestamp.fromMillis(lastOrderAt)  : null,
      isRepeatBuyer : orderCount >= 2,
      receiptIds    : receiptIds.slice(0, 50),
      updatedAt     : FV.serverTimestamp()
    };
    await db.collection("EtsyMail_Customers").doc(String(userId)).set(customerDoc, { merge: true });
    customersUpdated++;
  }
  return { customersUpdated };
}

async function readSyncState() {
  const snap = await db.collection("EtsyMail_Config").doc("syncState").get();
  return snap.exists ? snap.data() : null;
}
async function writeSyncState(patch) {
  await db.collection("EtsyMail_Config").doc("syncState").set({
    ...patch, updatedAt: FV.serverTimestamp()
  }, { merge: true });
}

// ─── Run sync ──────────────────────────────────────────────────────────────
async function runSync(mode, daysBack) {
  const started = Date.now();
  const accessToken = await getValidEtsyAccessToken();
  const syncState = await readSyncState();

  const baseParams = { limit: PAGE_SIZE, sort_on: "updated", sort_order: "asc" };
  if (mode === "full") {
    const cutoff = Math.floor((Date.now() - daysBack * 24 * 60 * 60 * 1000) / 1000);
    baseParams.min_created = cutoff;
  } else {
    const since = syncState && syncState.lastReceiptUpdatedAt
      ? (syncState.lastReceiptUpdatedAt.toMillis
          ? syncState.lastReceiptUpdatedAt.toMillis()
          : new Date(syncState.lastReceiptUpdatedAt).getTime())
      : null;
    if (since) {
      baseParams.min_last_modified = Math.floor(since / 1000);
    } else {
      baseParams.min_created = Math.floor((Date.now() - DEFAULT_DAYS_BACK * 24 * 60 * 60 * 1000) / 1000);
    }
  }

  let offset = 0;
  let pagesFetched = 0;
  let receiptsProcessed = 0;
  let newestUpdatedAtMs = 0;
  const allAffectedUserIds = new Set();
  let totalCount = null;
  let hitOffsetCap = false;

  while (pagesFetched < MAX_PAGES) {
    const page = await getReceiptsPage(accessToken, { ...baseParams, offset });
    pagesFetched++;
    if (pagesFetched === 1 && typeof page.count === "number") totalCount = page.count;

    const receipts = page.results || [];
    if (!receipts.length) break;

    const batchResult = await upsertReceipts(receipts);
    receiptsProcessed += batchResult.written;
    batchResult.affectedUserIds.forEach(id => allAffectedUserIds.add(id));

    for (const r of receipts) {
      if (r.updated_timestamp && r.updated_timestamp * 1000 > newestUpdatedAtMs) {
        newestUpdatedAtMs = r.updated_timestamp * 1000;
      }
    }

    if (receipts.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    if (offset >= 12000) { hitOffsetCap = true; break; }
    await sleep(REQUEST_DELAY_MS);
  }

  const customerResult = await rebuildCustomers(Array.from(allAffectedUserIds));

  const nextState = {
    lastSyncMode         : mode,
    lastSyncStartedAt    : admin.firestore.Timestamp.fromMillis(started),
    lastSyncCompletedAt  : FV.serverTimestamp(),
    lastSyncReceiptsCount: receiptsProcessed,
    lastSyncPagesFetched : pagesFetched,
    lastSyncHitOffsetCap : hitOffsetCap,
    lastSyncError        : null,
    lastSyncErrorAt      : null
  };
  if (newestUpdatedAtMs > 0) {
    nextState.lastReceiptUpdatedAt = admin.firestore.Timestamp.fromMillis(newestUpdatedAtMs);
  }
  await writeSyncState(nextState);

  return {
    ok: true,
    mode,
    pagesFetched,
    receiptsProcessed,
    customersUpdated      : customerResult.customersUpdated,
    newestReceiptUpdatedAt: newestUpdatedAtMs > 0 ? new Date(newestUpdatedAtMs).toISOString() : null,
    totalShopReceipts     : totalCount,
    hitOffsetCap,
    durationMs: Date.now() - started
  };
}

// ─── Status action ─────────────────────────────────────────────────────────
async function runStatus() {
  const stateSnap = await db.collection("EtsyMail_Config").doc("syncState").get();
  const state = stateSnap.exists ? stateSnap.data() : null;
  const [receiptsAgg, customersAgg] = await Promise.all([
    db.collection("EtsyMail_Receipts").count().get(),
    db.collection("EtsyMail_Customers").count().get()
  ]);
  return {
    lastSyncCompletedAt  : state ? tsToIso(state.lastSyncCompletedAt) : null,
    lastSyncStartedAt    : state ? tsToIso(state.lastSyncStartedAt)   : null,
    lastSyncMode         : state ? (state.lastSyncMode || null) : null,
    lastSyncReceiptsCount: state ? (state.lastSyncReceiptsCount || 0) : 0,
    lastSyncPagesFetched : state ? (state.lastSyncPagesFetched || 0) : 0,
    lastSyncHitOffsetCap : state ? !!state.lastSyncHitOffsetCap : false,
    lastSyncError        : state ? (state.lastSyncError || null) : null,
    lastSyncErrorAt      : state ? tsToIso(state.lastSyncErrorAt) : null,
    receiptsCount        : receiptsAgg.data().count || 0,
    customersCount       : customersAgg.data().count || 0
  };
}

// ─── Entry ─────────────────────────────────────────────────────────────────
exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: CORS, body: "" };
  }

  const qs = event.queryStringParameters || {};
  const action = qs.action || "status";

  try {
    if (action === "status") {
      const body = await runStatus();
      return json(200, body);
    }

    if (action === "run") {
      const auth = checkRunAuth(event);
      if (!auth.ok) return auth.response;

      if (!SHOP_ID || !CLIENT_ID || !CLIENT_SECRET) {
        return json(500, { error: "Missing SHOP_ID / CLIENT_ID / CLIENT_SECRET env vars" });
      }

      const mode = (qs.mode === "full") ? "full" : "incremental";
      const daysBack = Math.max(1, parseInt(qs.daysBack || DEFAULT_DAYS_BACK, 10));

      try {
        const result = await runSync(mode, daysBack);
        return json(200, result);
      } catch (err) {
        console.error("etsyMailSync run error:", err);
        try {
          await writeSyncState({
            lastSyncError   : err.message || String(err),
            lastSyncErrorAt : FV.serverTimestamp()
          });
        } catch {}
        return json(500, { error: err.message || "Sync failed" });
      }
    }

    return json(400, { error: `Unknown action: ${action}. Use action=status or action=run.` });
  } catch (err) {
    console.error("etsyMailSync top-level error:", err);
    return json(500, { error: err.message || "Unknown error" });
  }
};
