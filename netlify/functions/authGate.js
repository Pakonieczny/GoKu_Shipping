// netlify/functions/authGate.js
// ─────────────────────────────────────────────────────────────────────────────
// THE SIGN-IN DOOR FOR THE BRITES STATION CONSOLES.
//
// One job: say yes or no to an operator passcode. Nothing else. No Firestore,
// no Google Ads, no Etsy, no node_modules — so it cannot fail for a reason
// that has nothing to do with signing in, and it answers in single-digit
// milliseconds on a cold start.
//
// WHY IT EXISTS
//   britesadwords.html verifies its passcode by calling googleAdsAutopilotApi
//   with action "dashboard". That works there because the console needs the
//   dashboard anyway. The Etsy Pricing console and Listing-Generator-1 have no
//   business calling the Google Ads stack just to open a door, and doing so
//   would couple their sign-in to Firestore availability and to the Ads
//   credentials being healthy. This function is that door on its own.
//
//   The contract is IDENTICAL to googleAdsAutopilotKick's authed() helper, on
//   purpose — same env var, same header, same body field, same status codes:
//
//     env var   EDIT_PASSCODE          (unset ⇒ open, exactly as before)
//     header    X-Edit-Passcode: <pc>
//     body      { "passcode": "<pc>" }
//     accepted  200 { ok: true }
//     rejected  401 { error: "unauthorized" }
//
//   So a page written against this endpoint also works, unchanged, against
//   googleAdsAutopilotApi — which is exactly the fallback the consoles use if
//   this file has not been deployed yet.
//
// ⚠  DO NOT ADD A SCHEDULE ENTRY FOR THIS FUNCTION IN netlify.toml.
//    Netlify refuses HTTP requests to scheduled functions with a bare 403,
//    and every console would show "403" on its sign-in screen with nothing to
//    explain it. That is the bug googleAdsAutopilotApi.js was split out to fix.
// ─────────────────────────────────────────────────────────────────────────────

"use strict";

const crypto = require("crypto");

const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, X-Edit-Passcode",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Max-Age": "86400",
  "Cache-Control": "no-store",
  "Content-Type": "application/json"
};

/*  Constant-time compare.
    Both sides are hashed first so the buffers are always 32 bytes: length
    alone then leaks nothing, and timingSafeEqual cannot throw on a mismatch
    of lengths (which is its documented failure mode).                        */
function sameSecret(a, b) {
  const ha = crypto.createHash("sha256").update(String(a == null ? "" : a), "utf8").digest();
  const hb = crypto.createHash("sha256").update(String(b == null ? "" : b), "utf8").digest();
  return crypto.timingSafeEqual(ha, hb);
}

function json(statusCode, obj) {
  return { statusCode, headers: HEADERS, body: JSON.stringify(obj) };
}

/*  Header lookup that does not care how the platform cased it. Netlify
    lower-cases incoming header names, but local `netlify dev` and some
    proxies do not, and neither does a hand-written test.                     */
function header(event, name) {
  const h = (event && event.headers) || {};
  const want = name.toLowerCase();
  if (h[want] != null) return h[want];
  for (const k in h) if (Object.prototype.hasOwnProperty.call(h, k) && k.toLowerCase() === want) return h[k];
  return "";
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS, body: "" };

  const configured = process.env.EDIT_PASSCODE || "";

  /*  GET → a liveness probe the sign-in screen can point at. It reveals
      whether a passcode is configured at all, never the passcode itself. */
  if (event.httpMethod === "GET") {
    return json(200, { ok: true, service: "authGate", locked: !!configured });
  }

  if (event.httpMethod !== "POST") {
    return json(405, { error: "method not allowed" });
  }

  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch (e) { body = {}; }

  /*  Unset EDIT_PASSCODE means the consoles are open — the same decision
      googleAdsAutopilotKick.authed() makes. Set the env var to lock them. */
  if (!configured) {
    return json(200, { ok: true, open: true, app: body.app || null });
  }

  const supplied = header(event, "x-edit-passcode") || (body && body.passcode) || "";

  if (!supplied || !sameSecret(supplied, configured)) {
    // Log the attempt, never the value.
    console.warn("[authGate] rejected sign-in", JSON.stringify({
      app: (body && body.app) || null,
      ip: header(event, "x-nf-client-connection-ip") || null,
      ua: String(header(event, "user-agent") || "").slice(0, 120)
    }));
    return json(401, { error: "unauthorized" });
  }

  return json(200, { ok: true, app: (body && body.app) || null, ts: Date.now() });
};
