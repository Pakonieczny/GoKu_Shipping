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

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") return { statusCode: 405, body: "POST only" };

  const secret = process.env.SHOPIFY_WEBHOOK_SECRET;
  const hmac = header(event.headers, "x-shopify-hmac-sha256");
  const raw = rawBody(event);
  if (!verifyHmac(raw, hmac, secret)) return { statusCode: 401, body: "hmac verification failed" };

  const topic = String(header(event.headers, "x-shopify-topic") || "").toLowerCase();
  let payload;
  try { payload = JSON.parse(raw.toString("utf8")); }
  catch (e) { return { statusCode: 400, body: "bad json" }; }

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
      if (!gclid && !gbraid && !wbraid) {
        return { statusCode: 200, body: JSON.stringify({ ok: true, skipped: "no Google click id — non-ad / organic order" }) };
      }
      const value = Number(payload.total_price || payload.current_total_price || 0);
      const currency = payload.currency || payload.presentment_currency || undefined;
      const when = payload.created_at ? E.gAdsTime(new Date(payload.created_at)) : undefined;
      const r = await E.enqueueConversion({ gclid, gbraid, wbraid, value, currency, orderId, conversionDateTime: when });
      return { statusCode: 200, body: JSON.stringify({ ok: true, result: r }) };
    }

    if (topic === "refunds/create") {
      const orderId = String(payload.order_id || "");
      const amt = refundAmount(payload);
      const when = payload.created_at || (payload.processed_at) ? E.gAdsTime(new Date(payload.created_at || payload.processed_at)) : undefined;
      const r = await E.recordRefund({ orderId, refundAmount: amt, when });
      return { statusCode: 200, body: JSON.stringify({ ok: true, result: r }) };
    }

    return { statusCode: 200, body: JSON.stringify({ ok: true, ignored: topic }) };
  } catch (e) {
    // Return 200 on internal errors so Shopify doesn't aggressively retry on our bugs;
    // the failure is logged via the response body and can be replayed manually.
    return { statusCode: 200, body: JSON.stringify({ ok: false, error: e.message }) };
  }
};
