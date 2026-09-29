// netlify/functions/_adsCheckGate.js
// ─────────────────────────────────────────────────────────────────────────────
// The passcode door for the Ad Autopilot's check pages — the ones opened by URL
// rather than from the console: googleAdsAuthCheck, googleAdsDiag,
// googleConnectionsCheck, googleMerchantHealth and shopifyAttributionCheck.
//
// They only read, but what they read is not public (account and customer IDs,
// credential shapes, approvals and the mutate ledger, order IDs and values), and
// every run spends Google Ads, Merchant Center or Shopify API quota. So they
// follow the console's money-safety model (googleAdsAutopilotKick.js), not the
// older "open while EDIT_PASSCODE is unset" rule:
//
//   EDIT_PASSCODE unset → 403 { code: "EDIT_PASSCODE_NOT_SET" }, before any work,
//                          saying nothing about how the site is configured
//   missing / wrong     → 401, before any work
//   right               → the check runs
//
// The passcode is accepted as the X-Edit-Passcode header, a JSON body field
// "passcode", or ?key=<EDIT_PASSCODE> (so a page still opens from the address
// bar). EDIT_PASSCODE is trimmed and unquoted and compared in constant time,
// exactly like passcode()/sameSecret() in googleAdsAutopilotKick.js.
// ─────────────────────────────────────────────────────────────────────────────
"use strict";

const crypto = require("crypto");

const PASSCODE_UNSET = "Set EDIT_PASSCODE in Netlify to run this check";

function passcode() { return String(process.env.EDIT_PASSCODE || "").trim().replace(/^["']|["']$/g, ""); }
function sameSecret(a, b) {
  a = String(a == null ? "" : a).trim(); b = String(b == null ? "" : b);
  if (!a || !b) return false;
  const h = x => crypto.createHash("sha256").update(x).digest();
  return crypto.timingSafeEqual(h(a), h(b));
}

// Every place a caller may put the passcode. Header names match in any case.
function offered(event) {
  const out = [], headers = (event && event.headers) || {};
  for (const k in headers) if (k.toLowerCase() === "x-edit-passcode") out.push(headers[k]);
  try {
    const raw = event && event.body ? (event.isBase64Encoded ? Buffer.from(event.body, "base64").toString("utf8") : event.body) : "";
    const body = raw ? JSON.parse(raw) : null;
    if (body && typeof body === "object") out.push(body.passcode);
  } catch (e) { /* not JSON: nothing offered there */ }
  out.push(((event && event.queryStringParameters) || {}).key);
  return out;
}

// null when the check may run; otherwise the response to send instead of running it.
function refuse(event, what) {
  const headers = { "Content-Type": "application/json", "Cache-Control": "no-store" };
  const pass = passcode();
  if (!pass) return { statusCode: 403, headers, body: JSON.stringify({ ok: false, error: PASSCODE_UNSET, code: "EDIT_PASSCODE_NOT_SET" }) };
  if (offered(event).some(v => sameSecret(v, pass))) return null;
  return { statusCode: 401, headers: { "Content-Type": "text/plain", "Cache-Control": "no-store" }, body: "Add ?key=<EDIT_PASSCODE> to run " + what + "." };
}

module.exports = { refuse, passcode, sameSecret, PASSCODE_UNSET };
