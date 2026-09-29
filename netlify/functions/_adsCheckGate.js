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
//   no passcode         → 403 { code: "EDIT_PASSCODE_NOT_SET" }, before any work,
//                          saying nothing about how the site is configured
//   missing / wrong     → 401, before any work
//   right               → the check runs
//
// The passcode is the console's (_editPasscode.js): EDIT_PASSCODE when set in
// Netlify, otherwise Firestore config/editPasscode. It is accepted as the
// X-Edit-Passcode header, a JSON body field "passcode", or ?key=<passcode> (so a
// page still opens from the address bar), and compared in constant time.
// ─────────────────────────────────────────────────────────────────────────────
"use strict";

const EP = require("./_editPasscode");

const PASSCODE_UNSET = "Locked until a passcode is saved in Firebase (Firestore config/editPasscode)";
const sameSecret = EP.sameSecret;

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

// Resolves to null when the check may run; otherwise to the response to send instead of running it.
async function refuse(event, what, deps) {
  const headers = { "Content-Type": "application/json", "Cache-Control": "no-store" };
  const pass = (await EP.resolve(deps)).value;
  if (!pass) return { statusCode: 403, headers, body: JSON.stringify({ ok: false, error: PASSCODE_UNSET, code: "EDIT_PASSCODE_NOT_SET" }) };
  if (offered(event).some(v => sameSecret(v, pass))) return null;
  return { statusCode: 401, headers: { "Content-Type": "text/plain", "Cache-Control": "no-store" }, body: "Add ?key=<passcode> to run " + what + "." };
}

module.exports = { refuse, sameSecret, PASSCODE_UNSET };
