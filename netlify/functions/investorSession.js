/*  netlify/functions/investorSession.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — session operations (blueprint §11.5, §12.1).
 *
 *    create  {passcode}  → Secure, HttpOnly, SameSite=Strict cookie; the body
 *                          returns expiry and the rotating CSRF token
 *    refresh             → new CSRF token and expiry for the live cookie
 *    revoke              → server-side revocation and a cleared cookie
 *    reauth  {passcode}  → a short-lived reauthentication token bound to the
 *                          session, for the mutations that require it
 *
 *  Origin and Host are enforced (rejected, not merely CORS-filtered). The
 *  passcode is compared in constant time and never logged; secrets are never
 *  returned. A 401 from any other endpoint sends the browser here.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const AUTH = require("./_investorAuth");
const SCHEMAS = require("./_investorApiSchemas");

const MAX_BODY = 4096;
const ACTIONS = Object.freeze(["create", "refresh", "revoke", "reauth"]);

function envelope(event, status, { ok, requestId = null, data = null, error = null, headers = {} }) {
  const body = { ok, requestId, payloadVersion: SCHEMAS.PAYLOAD_VERSION, asOf: new Date().toISOString(), partial: false, partialReason: null, nextCursor: null, data, error };
  return AUTH.json(event, status, body, headers);
}
function fail(event, code, message, requestId = null, headers = {}) {
  return envelope(event, SCHEMAS.HTTP_STATUS[code] || 500, { ok: false, requestId, error: SCHEMAS.errorShape(code, message), headers });
}

/** The handler, admin-injectable for the deploy attestation. */
async function handle(event, { admin = null, nowMs = Date.now() } = {}) {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: AUTH.corsHeaders(event), body: "" };
  if (event.httpMethod !== "POST") return fail(event, "SCHEMA_INVALID", "POST only");
  if ((event.body || "").length > MAX_BODY) return fail(event, "REQUEST_TOO_LARGE", "request too large");
  const origin = AUTH.enforceOrigin(event);
  if (!origin.ok) return fail(event, "ORIGIN_REJECTED", origin.reason);
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { return fail(event, "SCHEMA_INVALID", "invalid JSON body"); }
  const requestId = typeof body.requestId === "string" && /^[A-Za-z0-9_-]{8,64}$/.test(body.requestId) ? body.requestId : `sess_${crypto.randomBytes(6).toString("hex")}`;
  const action = String(body.action || "");
  if (!ACTIONS.includes(action)) return fail(event, "UNKNOWN_ACTION", `unknown session action ${action}`, requestId);
  const secrets = AUTH.authSecrets();
  if (!secrets.passcode || !(secrets.sessionSecret || "").length || secrets.sessionSecret.length < 32) return fail(event, "AUTH_NOT_CONFIGURED", "operator secrets are not configured", requestId);
  const secure = origin.secure !== false;

  if (action === "create") {
    if (!AUTH.verifyPasscode(body.passcode)) return fail(event, "AUTH_MISSING", "invalid passcode", requestId);
    const s = await AUTH.createCookieSession({ subject: "operator", admin, nowMs });
    if (!s) return fail(event, "AUTH_NOT_CONFIGURED", "signing key unavailable", requestId);
    return envelope(event, 200, { ok: true, requestId, data: { subject: s.subject, expiresAt: new Date(s.expiresAtMs).toISOString(), csrfToken: s.csrfToken },
      headers: { "Set-Cookie": AUTH.cookieHeader(s.token, { expiresAtMs: s.expiresAtMs, secure }) } });
  }
  const session = await AUTH.verifyCookieSession(event, { admin, nowMs });
  if (!session) return fail(event, "SESSION_EXPIRED", "no live session", requestId, { "Set-Cookie": AUTH.clearCookieHeader({ secure }) });
  if (action === "refresh") {
    const csrfToken = await AUTH.rotateCsrf(session.sessionId, { admin, nowMs });
    return envelope(event, 200, { ok: true, requestId, data: { subject: session.subject, expiresAt: new Date(session.expiresAtMs).toISOString(), csrfToken } });
  }
  if (action === "revoke") {
    await AUTH.revokeCookieSession(session.sessionId, { admin, nowMs });
    return envelope(event, 200, { ok: true, requestId, data: { revoked: true }, headers: { "Set-Cookie": AUTH.clearCookieHeader({ secure }) } });
  }
  /* reauth: the passcode re-entered inside a live session; CSRF required so a stray form cannot mint one */
  if (!body.csrfToken || !session.csrfHash || !AUTH.timingEqual(crypto.createHash("sha256").update(String(body.csrfToken)).digest("hex"), session.csrfHash)) return fail(event, "CSRF_INVALID", "CSRF token missing or stale", requestId);
  if (!AUTH.verifyPasscode(body.passcode)) return fail(event, "AUTH_MISSING", "invalid passcode", requestId);
  const r = await AUTH.mintReauthToken(session, { admin, nowMs });
  return envelope(event, 200, { ok: true, requestId, data: { reauthToken: r.reauthToken, expiresAt: new Date(r.expiresAtMs).toISOString() } });
}

exports.handler = async (event) => {
  try { await AUTH.loadAuthSecrets(); } catch (e) { return fail(event, "AUTH_NOT_CONFIGURED", `auth secrets unavailable: ${String(e && e.message || e).slice(0, 120)}`); }
  try { return await handle(event); }
  catch (e) { console.error("investorSession", AUTH.redact({ error: e.message, stack: (e.stack || "").slice(0, 300) })); return fail(event, "INTERNAL", String(e.message).slice(0, 200)); }
};
exports.handle = handle;
exports.ACTIONS = ACTIONS;
exports.config = { path: "/.netlify/functions/investorSession", rateLimit: { windowLimit: 30, windowSize: 60, aggregateBy: ["ip", "domain"] } };
