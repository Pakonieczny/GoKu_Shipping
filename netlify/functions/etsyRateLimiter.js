// netlify/functions/etsyRateLimiter.js
// The metered Etsy fetch of the pricing and listing functions. Its own token bucket (5 a second, beside every other function's calls) is gone: the one gate of the whole
// shop is in _etsyStop.js (ETSYGATE: 3 a second, every function together), which the fetch below goes through.

const fetch = require("./_etsyStop").guard(require("node-fetch"), "etsyRateLimiter");

// METERING (added for verified API-usage parity): every call through here is
// recorded and every response has its Etsy rate-limit headers captured, so
// etsyApiUsage can serve the App/Key/QPS widget without ever estimating.
// This is deliberately fire-and-forget and fully guarded — a metering fault
// must never break, delay, or alter an Etsy call. If the usage module can't
// even be required (missing file, Firestore down at init), we degrade to a
// no-op and Etsy traffic is completely unaffected.
let _usage = null;
try {
  _usage = require("./_etsyApiUsage");
} catch (_) {
  _usage = null;
}
function _meter(appId, res) {
  if (!_usage) return;
  try { _usage.recordCall(appId, res); } catch (_) {}
}

async function etsyFetch(url, init = {}, opts = {}) {
  const {
    // Which console to attribute this call to. Callers may override per
    // request; the env default keeps existing call sites working untouched.
    appId = process.env.ETSY_APP_ID || "pricing-console",
  } = opts;

  let res;
  try {
    res = await fetch(url, init);
  } catch (err) {
    // Network failure still consumed a token and may have reached Etsy —
    // count the attempt so the budget can't silently under-report.
    _meter(appId, null);
    throw err;
  }

  // Count every attempt, and harvest the rate-limit headers from every
  // response including 429s (those carry the most important reading).
  _meter(appId, res);

  // ETSYSTOP (Paul, 20 Nov 2026: "Once a 429 is reached the application must stop all work related to Etsy"): a 429 is never asked
  // again. Etsy's own 429 has already turned the shop's switch on (fetch above is _etsyStop's guard), and every later call, here or in
  // any other function, is answered by the switch until Resume Etsy is pressed. The 429 goes to the caller as it came.
  return res;
}

module.exports = { etsyFetch };
