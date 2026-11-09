/* _etsyOrderWebhook.js — SAL1 (Paul, 9 Nov 2026: "When an order is placed and received on our Etsy store, Etsy should be able to notify us here without
   needing to do constant API calls").

   Etsy Open API v3 sends webhooks (event order.paid, "delivered immediately when an order receives payment"; payload { event_type, resource_url, shop_id })
   signed in the Standard Webhooks form: headers webhook-id, webhook-timestamp, webhook-signature ("v1,<base64>", several separated by spaces), the
   signature an HMAC-SHA256, keyed by the endpoint's signing secret ("whsec_<base64>"), of "<webhook-id>.<webhook-timestamp>.<raw body>".

   The route is charmNestLibrary's own door, so no new endpoint and no build setting changes:
       POST https://goldenspike.app/.netlify/functions/charmNestLibrary?hook=etsy-order
   It makes NO Etsy call: the receipt id is read from resource_url and the moment it was paid stands in for its creation (the mirror's real
   create_timestamp replaces it when the receipt is next mirrored; charm-nest-order-days.js counts a receipt once whichever came first). The same
   receipt sent twice changes nothing the second time (the day document is keyed by receipt id).

   The signing secret is read from the environment, ETSY_WEBHOOK_SECRET, which Paul adds in Netlify after registering the URL in Etsy's developer
   portal. Without it every delivery is refused (503): nothing is ever written from an unsigned request.

   handle(event, { db, OrderDays, bump, secret?, now? }) → a Netlify response. verify(...) is exported for the tests. */
"use strict";
const crypto = require("crypto");
const TOLERANCE_S = 5 * 60;

const json = (statusCode, body) => ({ statusCode, headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }, body: JSON.stringify(body) });
function header(event, name) {
  const h = (event && event.headers) || {};
  for (const [k, v] of Object.entries(h)) if (String(k).toLowerCase() === name) return Array.isArray(v) ? String(v[0]) : String(v);
  return "";
}
function keyOf(secret) {
  const s = String(secret || "").trim(); if (!s) return null;
  const b = Buffer.from(s.replace(/^whsec_/, ""), "base64");
  return b.length ? b : null;
}
/** true when the request is Etsy's: a signature over id.timestamp.body under the secret, sent within TOLERANCE_S of now (s). */
function verify({ id, timestamp, signature, body, secret, nowSec }) {
  const key = keyOf(secret);
  if (!key || !id || !/^\d{1,12}$/.test(String(timestamp)) || !signature) return false;
  if (Math.abs((nowSec || Math.floor(Date.now() / 1000)) - Number(timestamp)) > TOLERANCE_S) return false;
  const want = crypto.createHmac("sha256", key).update(`${id}.${timestamp}.${body}`).digest();
  for (const part of String(signature).split(" ")) {
    const [ver, sig] = part.split(",");
    if (ver !== "v1" || !sig) continue;
    let got; try { got = Buffer.from(sig, "base64"); } catch (_) { continue; }
    if (got.length === want.length && crypto.timingSafeEqual(got, want)) return true;
  }
  return false;
}
/** The receipt id an order.paid payload names (resource_url …/receipts/<id>), or "". */
function receiptOf(p) {
  const direct = p && (p.receipt_id || (p.data && p.data.receipt_id));
  if (direct && /^\d{1,30}$/.test(String(direct))) return String(direct);
  const m = /\/receipts\/(\d{1,30})(?:[/?#]|$)/.exec(String((p && (p.resource_url || p.fetch_url)) || ""));
  return m ? m[1] : "";
}
async function handle(event, deps) {
  if (event.httpMethod !== "POST") return json(405, { error: "POST only" });
  const secret = deps.secret != null ? deps.secret : process.env.ETSY_WEBHOOK_SECRET || "";
  if (!secret) return json(503, { error: "the webhook's signing secret is not set (ETSY_WEBHOOK_SECRET)" });
  const body = event.body ? (event.isBase64Encoded ? Buffer.from(event.body, "base64").toString("utf8") : String(event.body)) : "";
  const id = header(event, "webhook-id"), timestamp = header(event, "webhook-timestamp"), signature = header(event, "webhook-signature");
  const nowMs = deps.now ? deps.now() : Date.now();
  if (!verify({ id, timestamp, signature, body, secret, nowSec: Math.floor(nowMs / 1000) })) return json(401, { error: "bad signature" });
  let p; try { p = JSON.parse(body || "{}"); } catch (_) { return json(400, { error: "bad JSON" }); }
  const type = String((p && (p.event_type || p.type)) || "").toLowerCase();
  if (type && !/order[._]?paid/.test(type)) return json(200, { ok: true, ignored: type });
  const shop = process.env.SHOP_ID;
  if (shop && p && p.shop_id != null && String(p.shop_id) !== String(shop)) return json(200, { ok: true, ignored: "another shop" });
  const rid = receiptOf(p);
  if (!rid) return json(200, { ok: true, ignored: "no receipt named" });
  const m = await deps.OrderDays.merge(deps.db, "", [{ id: rid, createTs: Number(timestamp), hook: true }], nowMs);
  if (!m.ok) return json(500, { error: m.error || "not recorded" });   // (Etsy sends it again)
  if (m.changed && deps.bump) { try { await deps.bump(); } catch (_) { /* the next order check carries it */ } }
  return json(200, { ok: true, receipt: rid, counted: !!m.changed });
}
module.exports = { handle, verify, receiptOf };
