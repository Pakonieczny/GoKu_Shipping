/**
 * shopifyOrderWebhook.js — Netlify function
 *
 * Receives Shopify webhooks and feeds the Google Ads offline-conversion pipeline.
 *   • orders/paid (or orders/create) → enqueueConversion(...)  [a sale]
 *   • refunds/create                 → recordRefund(...)        [retraction/restatement]
 *
 * Security: verifies the Shopify HMAC-SHA256 signature over the RAW request body
 * using SHOPIFY_WEBHOOK_SECRET. Requests that fail verification are rejected (401).
 *
 * Attribution: the Google click id (gclid / gbraid / wbraid) is captured on the
 * storefront by brites-gclid-capture.liquid, written to the cart, and arrives here
 * as an order note_attribute. We fall back to parsing it from landing_site.
 *
 * Setup (Shopify admin → Settings → Notifications → Webhooks, or via API):
 *   - Create webhooks for topics "orders/paid" and "refunds/create"
 *   - URL: https://goldenspike.app/.netlify/functions/shopifyOrderWebhook
 *     (or your mapped path, e.g. https://brites-adwords.goldenspike.app/... )
 *   - Format: JSON
 *   - Set SHOPIFY_WEBHOOK_SECRET in Netlify to the webhook signing secret
 *
 * Requires GADS_CONVERSION_ACTION to be set for the queued rows to actually upload
 * (the 15-min background worker drains the queue).
 */

const crypto = require("crypto");
const E = require("./googleAdsAutopilot");

function rawBody(event) {
  return event.isBase64Encoded
    ? Buffer.from(event.body || "", "base64")
    : Buffer.from(event.body || "", "utf8");
}

function verifyHmac(rawBuf, hmacHeader, secret) {
  if (!secret || !hmacHeader) return false;
  const digest = crypto.createHmac("sha256", secret).update(rawBuf).digest("base64");
  const a = Buffer.from(digest);
  const b = Buffer.from(String(hmacHeader));
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function header(headers, name) {
  if (!headers) return "";
  const lower = name.toLowerCase();
  for (const k in headers) if (k.toLowerCase() === lower) return headers[k];
  return "";
}

function noteAttr(noteAttributes, name) {
  if (!Array.isArray(noteAttributes)) return null;
  const m = noteAttributes.find(n => String(n.name || "").toLowerCase() === name.toLowerCase());
  return m && m.value ? String(m.value) : null;
}

function clickIdsFromLanding(url) {
  if (!url) return {};
  try {
    const u = new URL(url, "https://x.invalid");
    return {
      gclid: u.searchParams.get("gclid"),
      gbraid: u.searchParams.get("gbraid"),
      wbraid: u.searchParams.get("wbraid")
    };
  } catch (e) { return {}; }
}

// Marketing attribution (utm_source / utm_medium / utm_campaign) + product handle from the order's
// landing page or note attributes. Used to log organic vs ad demand for the intelligence layer.
function attributionFrom(payload) {
  const note = payload.note_attributes || [];
  let source = noteAttr(note, "utm_source"), medium = noteAttr(note, "utm_medium"), campaign = noteAttr(note, "utm_campaign");
  let handle = null;
  try {
    const u = new URL(payload.landing_site || "", "https://x.invalid");
    source = source || u.searchParams.get("utm_source");
    medium = medium || u.searchParams.get("utm_medium");
    campaign = campaign || u.searchParams.get("utm_campaign");
    const m = (u.pathname || "").match(/\/products\/([^\/?#]+)/);
    if (m) handle = m[1];
  } catch (e) {}
  return { source: source || null, medium: medium || null, campaign: campaign || null, handle };
}

function lineItemsFrom(payload) {
  const li = Array.isArray(payload.line_items) ? payload.line_items : [];
  return li.map(x => ({ title: String(x.title || x.name || "").trim(), qty: Number(x.quantity) || 1 })).filter(it => it.title).slice(0, 25);
}

function refundAmount(payload) {
  const txns = Array.isArray(payload.transactions) ? payload.transactions : [];
  const fromTxns = txns
    .filter(t => String(t.kind).toLowerCase() === "refund" && String(t.status || "success").toLowerCase() === "success")
    .reduce((s, t) => s + Number(t.amount || 0), 0);
  if (fromTxns > 0) return fromTxns;
  const li = Array.isArray(payload.refund_line_items) ? payload.refund_line_items : [];
  const fromLines = li.reduce((s, x) => s + Number(x.subtotal || 0), 0);
  const ship = (payload.order_adjustments || []).reduce((s, x) => s + Math.abs(Number(x.amount || 0)), 0);
  return fromLines + ship;
}

const LOG = (...a) => console.log("[shopifyWebhook]", ...a);

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") { LOG(event.httpMethod, "-> 405 (POST only)"); return { statusCode: 405, body: "POST only" }; }

  const secret = process.env.SHOPIFY_WEBHOOK_SECRET;
  const hmac = header(event.headers, "x-shopify-hmac-sha256");
  const raw = rawBody(event);
  const topicHdr = String(header(event.headers, "x-shopify-topic") || "").toLowerCase();
  if (!verifyHmac(raw, hmac, secret)) { LOG(topicHdr || "?", "-> 401 (HMAC failed — unsigned/test or wrong secret)"); return { statusCode: 401, body: "hmac verification failed" }; }

  const topic = topicHdr;
  let payload;
  try { payload = JSON.parse(raw.toString("utf8")); }
  catch (e) { LOG(topic, "-> 400 (bad json)"); return { statusCode: 400, body: "bad json" }; }

  try {
    if (topic === "orders/paid" || topic === "orders/create") {
      const orderId = String(payload.id || payload.order_number || payload.name || "");
      const note = payload.note_attributes || [];
      let gclid = noteAttr(note, "gclid");
      let gbraid = noteAttr(note, "gbraid");
      let wbraid = noteAttr(note, "wbraid");
      if (!gclid && !gbraid && !wbraid) {
        const land = clickIdsFromLanding(payload.landing_site);
        gclid = land.gclid; gbraid = land.gbraid; wbraid = land.wbraid;
      }
      const value = Number(payload.total_price || payload.current_total_price || 0);
      const currency = payload.currency || payload.presentment_currency || undefined;
      const attr = attributionFrom(payload);
      const items = lineItemsFrom(payload);
      const clickId = gclid || gbraid || wbraid || null;

      if (!clickId) {
        // Organic / non-ad order: never a Google Ads conversion, but we LOG it so the store
        // intelligence layer can learn what's selling and inform future ad campaigns.
        const reason = attr.campaign === "sag_organic" ? "organic — free Google listing (sag_organic)"
          : (attr.source ? `non-ad — ${attr.source}/${attr.medium || "none"}` : "organic / no Google click id");
        try { await E.recordOrderEvent({ orderId, value, currency, source: attr.source, medium: attr.medium, campaign: attr.campaign, gclid: null, captured: false, reason, items, handle: attr.handle }); } catch (e) { LOG("orderLog ERROR", e.message); }
        LOG(topic, "order", orderId, "-> 200 SKIPPED (" + reason + ")");
        return { statusCode: 200, body: JSON.stringify({ ok: true, skipped: reason, logged: true }) };
      }
      const when = payload.created_at ? E.gAdsTime(new Date(payload.created_at)) : undefined;
      const r = await E.enqueueConversion({ gclid, gbraid, wbraid, value, currency, orderId, conversionDateTime: when });
      try { await E.recordOrderEvent({ orderId, value, currency, source: attr.source, medium: attr.medium, campaign: attr.campaign, gclid: clickId, captured: true, reason: "captured — Google ad click", items, handle: attr.handle }); } catch (e) { LOG("orderLog ERROR", e.message); }
      LOG(topic, "order", orderId, "click", clickId, "value", value, currency, "->", JSON.stringify(r));
      return { statusCode: 200, body: JSON.stringify({ ok: true, result: r, logged: true }) };
    }

    if (topic === "refunds/create") {
      const orderId = String(payload.order_id || "");
      const amt = refundAmount(payload);
      const when = payload.created_at || (payload.processed_at) ? E.gAdsTime(new Date(payload.created_at || payload.processed_at)) : undefined;
      const r = await E.recordRefund({ orderId, refundAmount: amt, when });
      LOG(topic, "order", orderId, "refund", amt, "->", JSON.stringify(r));
      return { statusCode: 200, body: JSON.stringify({ ok: true, result: r }) };
    }

    LOG(topic, "-> 200 (ignored topic)");
    return { statusCode: 200, body: JSON.stringify({ ok: true, ignored: topic }) };
  } catch (e) {
    LOG(topic, "-> 200 (internal error:", e.message + ")");
    // Return 200 on internal errors so Shopify doesn't aggressively retry on our bugs;
    // the failure is logged via the response body and can be replayed manually.
    return { statusCode: 200, body: JSON.stringify({ ok: false, error: e.message }) };
  }
};
