/*  netlify/functions/_etsyMailAuth.js
 *
 *  Tiny shared helper — every EtsyMail write-path function uses this to
 *  validate the shared secret the Chrome extension (and any other trusted
 *  client) sends in the X-EtsyMail-Secret header.
 *
 *  Netlify env var to set:
 *    ETSYMAIL_EXTENSION_SECRET = <a long random string you generate>
 *
 *  If the env var is unset, the helper returns { ok: true } so local dev
 *  doesn't block. In production you MUST set it.
 *
 *  Not imported by the browser inbox (etsy-mail-1.html) — the inbox uses
 *  read-only firestoreProxy + etsyMailThreads, which are open by design
 *  because they only run from the operator UI in your control. If you want
 *  to tighten that later, add this check to those functions too.
 */

const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization,X-EtsyMail-Secret",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
};

function requireExtensionAuth(event) {
  const expected = process.env.ETSYMAIL_EXTENSION_SECRET;

  // v1.5: fail-closed in production. Pre-v1.5, if the env var was unset
  // we'd allow the request through (dev-friendly, production-dangerous).
  // Production deploys must have the secret configured; the helper
  // refuses if it's missing instead of silently passing through.
  //
  // The "production" detection uses Netlify's CONTEXT env var:
  //   - "production"      → main branch, customer-facing
  //   - "deploy-preview"  → PR previews
  //   - "branch-deploy"   → other branches
  //   - "dev" (or unset)  → local netlify dev / netlify functions:serve
  // We only fail-closed for "production". Deploy previews and dev still
  // pass through with a warning — useful for testing and demos.
  if (!expected) {
    if (process.env.CONTEXT === "production") {
      console.error("✗ ETSYMAIL_EXTENSION_SECRET not set in production — refusing request.");
      return {
        ok: false,
        response: {
          statusCode: 500,
          headers: CORS,
          body: JSON.stringify({
            error: "Server misconfigured: ETSYMAIL_EXTENSION_SECRET is required in production",
            errorCode: "AUTH_NOT_CONFIGURED"
          })
        }
      };
    }
    console.warn("⚠ ETSYMAIL_EXTENSION_SECRET not set — allowing request without auth (CONTEXT=" +
      (process.env.CONTEXT || "unknown") + "). MUST set this before promoting to production.");
    return { ok: true };
  }

  // Headers are lowercased by Netlify — check both cases to be safe.
  const got =
    event.headers["x-etsymail-secret"] ||
    event.headers["X-EtsyMail-Secret"] ||
    (event.multiValueHeaders && event.multiValueHeaders["x-etsymail-secret"] && event.multiValueHeaders["x-etsymail-secret"][0]);

  if (!got) {
    return { ok: false, response: { statusCode: 401, headers: CORS, body: JSON.stringify({ error: "Missing X-EtsyMail-Secret header" }) } };
  }
  if (got !== expected) {
    return { ok: false, response: { statusCode: 403, headers: CORS, body: JSON.stringify({ error: "Invalid X-EtsyMail-Secret" }) } };
  }
  return { ok: true };
}

module.exports = { requireExtensionAuth, CORS };
