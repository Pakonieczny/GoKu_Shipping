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
 *
 * ── Custom Charm Studio ──────────────────────────────────────────────────
 * orders/paid additionally:
 *   • grants design credits for any STUDIO-PACK-<n> line, keyed on the
 *     studio_uid cart attribute and made idempotent on the Shopify order id,
 *     so a webhook retry can never double-credit
 *   • flips customSessions/{sid}.status to "ordered" and records the
 *     full-resolution artwork path for the production queue
 * Both live in britesAuth.js, which already carries Firebase Admin — this file
 * gains no new dependency of its own and the deployment gains no new lambda.
 */

const crypto = require("crypto");
const E = require("./googleAdsAutopilot");
/* Studio wallet + session helpers. Lazy and defensive, in the house style: if
   britesAuth isn't deployed beside this file the ad pipeline keeps working and
   only the studio extras degrade. */
let _studio = null;
function studio() {
  if (_studio !== null) return _studio;
  try { _studio = require("./britesAuth"); }
  catch (e) { console.error("[shopifyWebhook] studio helpers unavailable:", e.message); _studio = false; }
  return _studio;
}

/* Every studio line item carries the same properties object, so one pass over
   the raw line items recovers the design id and the artwork path. Reads
   payload.line_items directly rather than lineItemsFrom(), which caps at 25
   items and drops the properties. */
function studioLinesFrom(payload) {
  const li = Array.isArray(payload.line_items) ? payload.line_items : [];
  const packs = [];
  const designs = new Map();
  for (const x of li) {
    const sku = String(x.sku || "").trim();
    const m = /^STUDIO-PACK-(\d+)$/i.exec(sku);
    if (m) packs.push({ sku, credits: Number(m[1]) * (Number(x.quantity) || 1) });
    const props = Array.isArray(x.properties) ? x.properties : [];
    const get = (n) => {
      const p = props.find((q) => String(q.name || "").toLowerCase() === n);
      return p && p.value ? String(p.value) : null;
    };
    const sid = get("design id");
    if (sid && !designs.has(sid)) {
      designs.set(sid, { sessionId: sid, designPath: get("_design_path"), uid: get("_uid") });
    }
  }
  return { packs, designs: [...designs.values()] };
}

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
    // A Shopping feed link carries its own utm_* (product_sync / sag_organic) ahead of
    // the campaign's final-URL suffix; the suffix, last, names the ad actually clicked.
    const last = k => u.searchParams.getAll(k).pop() || null;
    source = source || last("utm_source");
    medium = medium || last("utm_medium");
    campaign = campaign || last("utm_campaign");
    const m = (u.pathname || "").match(/\/products\/([^\/?#]+)/);
    if (m) handle = m[1];
  } catch (e) {}
  return { source: source || null, medium: medium || null, campaign: campaign || null, handle, ...require('./googleAdsSalesEvidence').clickAttribution(payload.landing_site,note,require('./googleAdsCampaignStyles').attribution(payload.landing_site,note)) };
}

// The shopper's Google consent, as the storefront recorded it on the cart
// (_ad_user_data / _ad_personalization = granted | denied). Never assumed.
function consentFrom(note) {
  const read = k => { const v = String(noteAttr(note, "_" + k) || noteAttr(note, k) || "").trim().toUpperCase().replace(/^CONSENT_/, ""); return v === "GRANTED" || v === "DENIED" ? v : null; };
  const c = { adUserData: read("ad_user_data"), adPersonalization: read("ad_personalization") };
  return c.adUserData || c.adPersonalization ? c : null;
}
// Where the buyer is decides whether Google's EU consent policy applies.
function buyerCountryFrom(payload) {
  const a = [payload.billing_address, payload.shipping_address, payload.customer && payload.customer.default_address].find(x => x && x.country_code);
  return a ? String(a.country_code).toUpperCase() : null;
}

function lineItemsFrom(payload) {
  const li = Array.isArray(payload.line_items) ? payload.line_items : [];
  return li.map(x => {
    const qty = Number(x.quantity) || 1;
    const unitPrice = Number(x.price != null ? x.price : x.pre_tax_price);
    const lineDiscount = Number(x.total_discount || 0);
    const gross = isFinite(unitPrice) ? unitPrice * qty : null;
    const lineRevenue = gross != null ? Math.max(0, gross - (isFinite(lineDiscount) ? lineDiscount : 0)) : null;
    return {
      title: String(x.title || x.name || "").trim(),
      sku: String(x.sku || "").trim(),
      qty,
      productId: x.product_id != null ? String(x.product_id) : null,
      variantId: x.variant_id != null ? String(x.variant_id) : null,
      unitPrice: isFinite(unitPrice) ? unitPrice : null,
      lineRevenue,
      lineDiscount: isFinite(lineDiscount) ? lineDiscount : null
    };
  }).filter(it => it.title || it.sku).slice(0, 25);
}

// In the shop's currency, like the sale it reduces (total_price). Refund transactions
// are in the currency the buyer paid; line, shipping and adjustment amounts carry
// both currencies, which gives the rate between them.
function refundAmount(payload) {
  let shop = 0, shown = 0, shopCode = "", shownCode = "";
  const pair = set => { const s = set && set.shop_money, p = set && set.presentment_money; if (!s || !p) return;
    shopCode = shopCode || String(s.currency_code || "").toUpperCase(); shownCode = shownCode || String(p.currency_code || "").toUpperCase();
    shop += Math.abs(Number(s.amount) || 0); shown += Math.abs(Number(p.amount) || 0); };
  const money = (set, raw) => set && set.shop_money ? Number(set.shop_money.amount || 0) : Number(raw || 0);
  const li = Array.isArray(payload.refund_line_items) ? payload.refund_line_items : [], shipLines = Array.isArray(payload.refund_shipping_lines) ? payload.refund_shipping_lines : [], adjustments = Array.isArray(payload.order_adjustments) ? payload.order_adjustments : [];
  li.forEach(x => { pair(x.subtotal_set); pair(x.total_tax_set); }); shipLines.forEach(x => pair(x.subtotal_amount_set)); adjustments.forEach(x => { pair(x.amount_set); pair(x.tax_amount_set); });
  const rate = shopCode && shownCode && shopCode !== shownCode && shown > 0 ? shop / shown : 1;
  const txns = Array.isArray(payload.transactions) ? payload.transactions : [];
  // A pending refund transaction is money the merchant has already sent back.
  const fromTxns = txns
    .filter(t => String(t.kind).toLowerCase() === "refund" && ["success", "pending"].includes(String(t.status || "success").toLowerCase()))
    .reduce((s, t) => s + Number(t.amount || 0) * (shopCode && String(t.currency || "").toUpperCase() === shopCode ? 1 : rate), 0);
  if (fromTxns > 0) return Math.round(fromTxns * 100) / 100;
  // Tax-inclusive, like the sale's value; a refund discrepancy is not a shipping refund.
  const fromLines = li.reduce((s, x) => s + money(x.subtotal_set, x.subtotal) + money(x.total_tax_set, x.total_tax), 0);
  const ship = shipLines.reduce((s, x) => s + Math.abs(money(x.subtotal_amount_set, 0)), 0) +
    adjustments.filter(x => String(x.kind || "").toLowerCase() === "shipping_refund").reduce((s, x) => s + Math.abs(money(x.amount_set, x.amount)) + Math.abs(money(x.tax_amount_set, x.tax_amount)), 0);
  return Math.round((fromLines + ship) * 100) / 100;
}
function refundItemsFrom(payload) {
  return (Array.isArray(payload.refund_line_items)?payload.refund_line_items:[]).map(x=>{
    const li=x.line_item||{};
    const qty=Math.max(0,Number(x.quantity)||0);
    return {
      title:String(li.title||li.name||"").trim(),sku:String(li.sku||"").trim(),qty:Math.max(1,Number(li.quantity)||qty||1),
      refundedQty:qty,productId:li.product_id!=null?String(li.product_id):null,variantId:li.variant_id!=null?String(li.variant_id):null,
      refundedRevenue:Math.max(0,Number(x.subtotal!=null?x.subtotal:x.total)||0),lineRevenue:Math.max(0,Number(li.price||0)*(Math.max(1,Number(li.quantity)||1))-Number(li.total_discount||0))
    };
  }).filter(x=>x.title||x.sku).slice(0,25);
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
      const orderName = String(payload.name || payload.order_number || "").trim() || null;
      const note = payload.note_attributes || [];
      let gclid = noteAttr(note, "gclid");
      let gbraid = noteAttr(note, "gbraid");
      let wbraid = noteAttr(note, "wbraid");
      if (!gclid && !gbraid && !wbraid) {
        const land = clickIdsFromLanding(payload.landing_site);
        gclid = land.gclid; gbraid = land.gbraid; wbraid = land.wbraid;
      }
      // total_price is in the shop's currency (tax, shipping and discounts included);
      // presentment_currency names the buyer's, a different amount.
      const value = Number(payload.total_price || payload.current_total_price || 0);
      const currency = payload.currency || (((payload.total_price_set || {}).shop_money || {}).currency_code) || undefined;
      const placedAt = Date.parse(payload.created_at || "") || undefined;
      const attr = attributionFrom(payload);
      const items = lineItemsFrom(payload);
      // Canonical Top-200 best sellers: ongoing site sales increment counts on the FIXED CSV list
      // (matched by SKU, then title; unmatched items are ignored — membership never grows here).
      if (topic === "orders/paid") {
        try { const bs = await E.bumpBestSellers(items); if (bs && bs.matched) LOG("bestSellers +", bs.matched, "item(s)"); }
        catch (e) { LOG("bestSellers bump ERROR", e.message); }
      }
      /* ── Custom Charm Studio ──────────────────────────────────────────
         MUST stay above the `if (!clickId)` early return below: organic
         orders take that return, and they are most orders. */
      if (topic === "orders/paid") {
        const S = studio();
        if (S) {
          const { packs, designs } = studioLinesFrom(payload);
          const studioUid = noteAttr(note, "studio_uid") ||
                            (designs.find((d) => d.uid) || {}).uid || null;
          if (packs.length) {
            try {
              const cr = await S.grantStudioCredits({ orderId, uid: studioUid, packs });
              LOG("studioCredits", orderId, "->", JSON.stringify(cr));
            } catch (e) { LOG("studioCredits ERROR", e.message); }
          }
          for (const d of designs) {
            try {
              await S.markSessionOrdered({ sessionId: d.sessionId, orderId, designPath: d.designPath });
              LOG("studioSession", d.sessionId, "-> ordered", d.designPath || "(no path)");
            } catch (e) { LOG("studioSession ERROR", e.message); }
          }
        }
      }

      const clickId = gclid || gbraid || wbraid || null;

      if (!clickId) {
        // Organic / non-ad order: never a Google Ads conversion, but we LOG it so the store
        // intelligence layer can learn what's selling and inform future ad campaigns.
        const reason = attr.campaign === "sag_organic" ? "organic — free Google listing (sag_organic)"
          : /^google$/i.test(attr.source || "") && /^(cpc|ppc|paid_search|paid_shopping|paid_pmax)$/i.test(attr.medium || "") ? "Google ad visit — no click id captured, so not uploaded"
          : (attr.source ? `non-ad — ${attr.source}/${attr.medium || "none"}` : "organic / no Google click id");
        try { await E.recordOrderEvent({ financialStatus:payload.financial_status,cancelledAt:payload.cancelled_at,test:payload.test, orderId, orderName, orderNumericId: String(payload.id || "") || null, ts: placedAt, value, currency, source: attr.source, medium: attr.medium, campaign: attr.campaign, campaignId:attr.campaignId,adGroupId:attr.adGroupId,adId:attr.adId,pipeline:attr.pipeline,designId:attr.designId, gclid: null, captured: false, reason, items, handle: attr.handle }); } catch (e) { LOG("orderLog ERROR", e.message); }
        LOG(topic, "order", orderId, "-> 200 SKIPPED (" + reason + ")");
        return { statusCode: 200, body: JSON.stringify({ ok: true, skipped: reason, logged: true }) };
      }
      // Only a real, paid sale becomes a Google conversion. A test order, a cancelled
      // one, or orders/create before payment is logged but never uploaded;
      // orders/paid uploads the sale once it is paid.
      const notSale = payload.test === true ? "test order" : payload.cancelled_at ? "cancelled order"
        : topic === "orders/create" && String(payload.financial_status || "").toLowerCase() !== "paid" ? "not paid yet" : null;
      const when = payload.created_at ? E.gAdsTime(new Date(payload.created_at)) : undefined;
      const r = notSale ? { skipped: notSale } : await E.enqueueConversion({ gclid, gbraid, wbraid, value, currency, orderId, conversionDateTime: when, consent: consentFrom(note), buyerCountry: buyerCountryFrom(payload) });
      try { await E.recordOrderEvent({ financialStatus:payload.financial_status,cancelledAt:payload.cancelled_at,test:payload.test, orderId, orderName, orderNumericId: String(payload.id || "") || null, ts: placedAt, value, currency, source: attr.source, medium: attr.medium, campaign: attr.campaign, campaignId:attr.campaignId,adGroupId:attr.adGroupId,adId:attr.adId,pipeline:attr.pipeline,designId:attr.designId, gclid: clickId, captured: true, reason: notSale ? "Google ad click — not uploaded (" + notSale + ")" : "captured — Google ad click", items, handle: attr.handle }); } catch (e) { LOG("orderLog ERROR", e.message); }
      LOG(topic, "order", orderId, "click", clickId, "value", value, currency, "->", JSON.stringify(r));
      return { statusCode: 200, body: JSON.stringify({ ok: true, result: r, logged: true }) };
    }

    if (topic === "refunds/create") {
      const orderId = String(payload.order_id || "");
      const amt = refundAmount(payload);
      const when = payload.created_at || (payload.processed_at) ? E.gAdsTime(new Date(payload.created_at || payload.processed_at)) : undefined;
      const r = await E.recordRefund({ orderId, refundAmount: amt, when, refundId: payload.id != null ? String(payload.id) : null, items: refundItemsFrom(payload) });
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
