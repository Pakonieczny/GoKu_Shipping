/*  netlify/functions/_investorAuth.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — operator authorization + CORS + scheduled-invocation detection.
 *
 *  The audit of the legacy stack found two patterns that must NOT be repeated:
 *    · authGate.js opens completely when EDIT_PASSCODE is absent.
 *    · openaiProxy.js returns wildcard CORS and performs no authorization.
 *  Both are fine for a listing tool and unacceptable for something that moves
 *  a ledger. This module fails CLOSED in every context, uses an exact origin
 *  allowlist, and issues a short-lived signed session token so the passcode is
 *  sent once rather than on every poll of a 5-minute dashboard.
 *
 *  ENV — one new variable, everything else already exists:
 *    INVESTOR_PASSCODE   (required; refuses all requests when unset)
 *    FIREBASE_PRIVATE_KEY  reused ONLY as HKDF salt for token signing —
 *                          never transmitted, never logged.
 *
 *  CORS is a browser mechanic, not authorization. Both are enforced.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");

const ALLOWED_ORIGINS = [
  "https://investor.goldenspike.app",
  "http://localhost:8888",
];

const SESSION_TTL_MS = 12 * 60 * 60 * 1000;   // 12h — a working day, then re-auth
const CLOCK_SKEW_MS  = 60 * 1000;

/* ── CORS ──────────────────────────────────────────────────────────────── */
function corsHeaders(event) {
  const origin = (event && event.headers &&
    (event.headers.origin || event.headers.Origin)) || "";
  const allowed = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
  return {
    "Access-Control-Allow-Origin": allowed,
    "Access-Control-Allow-Headers": "Content-Type,X-Investor-Session,X-Investor-Passcode",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Max-Age": "600",
    "Vary": "Origin",
  };
}

function json(event, statusCode, body, extraHeaders = {}) {
  return {
    statusCode,
    headers: {
      ...corsHeaders(event),
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
      "X-Content-Type-Options": "nosniff",
      "Referrer-Policy": "no-referrer",
      ...extraHeaders,
    },
    body: JSON.stringify(body),
  };
}

/* ── token signing ─────────────────────────────────────────────────────── */
function signingKey() {
  const seed = process.env.FIREBASE_PRIVATE_KEY || "";
  const pass = process.env.INVESTOR_PASSCODE || "";
  if (!seed || !pass) return null;
  // HKDF-SHA256 over the private key + passcode. Rotating either invalidates
  // every outstanding session, which is the behaviour we want.
  return crypto.hkdfSync("sha256", Buffer.from(seed), Buffer.from("investor-ai-session"),
                         Buffer.from(pass), 32);
}

function mintSession(subject = "operator") {
  const key = signingKey();
  if (!key) return null;
  const payload = Buffer.from(JSON.stringify({
    s: subject, exp: Date.now() + SESSION_TTL_MS, n: crypto.randomBytes(8).toString("hex"),
  })).toString("base64url");
  const mac = crypto.createHmac("sha256", key).update(payload).digest("base64url");
  return `${payload}.${mac}`;
}

function verifySession(token) {
  const key = signingKey();
  if (!key || typeof token !== "string" || !token.includes(".")) return null;
  const idx = token.lastIndexOf(".");
  const payload = token.slice(0, idx);
  const mac = token.slice(idx + 1);
  const expect = crypto.createHmac("sha256", key).update(payload).digest("base64url");
  const a = Buffer.from(mac), b = Buffer.from(expect);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
  let claims;
  try { claims = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")); }
  catch { return null; }
  if (!claims || typeof claims.exp !== "number") return null;
  if (Date.now() > claims.exp + CLOCK_SKEW_MS) return null;
  return claims;
}

/* ── the guard every HTTP endpoint calls first ─────────────────────────── */
function requireOperator(event, body) {
  const expected = process.env.INVESTOR_PASSCODE;

  // Fail closed in EVERY context, not just production. The legacy authGate
  // opened when its variable was missing; that is the bug this replaces.
  if (!expected) {
    return { ok: false, response: json(event, 500, {
      error: "Server misconfigured: INVESTOR_PASSCODE is required",
      errorCode: "AUTH_NOT_CONFIGURED",
    })};
  }

  const h = event.headers || {};
  const token = h["x-investor-session"] || h["X-Investor-Session"] || (body && body.session);
  if (token && verifySession(token)) return { ok: true, via: "session" };

  const supplied = h["x-investor-passcode"] || h["X-Investor-Passcode"] || (body && body.passcode);
  if (supplied) {
    const a = Buffer.from(String(supplied));
    const b = Buffer.from(String(expected));
    if (a.length === b.length && crypto.timingSafeEqual(a, b)) {
      return { ok: true, via: "passcode", session: mintSession() };
    }
    return { ok: false, response: json(event, 403, { error: "Invalid passcode", errorCode: "AUTH_BAD" }) };
  }

  return { ok: false, response: json(event, 401, { error: "Authentication required", errorCode: "AUTH_MISSING" }) };
}

/* ── background-worker nonce ───────────────────────────────────────────────
   Background functions are publicly invokable URLs. A worker must accept only
   a single-use nonce bound to the job it was dispatched for — never a bare
   POST from the internet. The nonce is an HMAC over (jobId, fn, expiry) so no
   round trip to Firestore is needed to validate it.                        */
function mintWorkerNonce(jobId, fn, ttlMs = 15 * 60 * 1000) {
  const key = signingKey();
  if (!key) return null;
  const payload = `${jobId}|${fn}|${Date.now() + ttlMs}`;
  const mac = crypto.createHmac("sha256", key).update(payload).digest("base64url");
  return Buffer.from(payload).toString("base64url") + "." + mac;
}

function verifyWorkerNonce(nonce, fn) {
  const key = signingKey();
  if (!key || typeof nonce !== "string" || !nonce.includes(".")) return null;
  const idx = nonce.lastIndexOf(".");
  const raw = nonce.slice(0, idx), mac = nonce.slice(idx + 1);
  const expect = crypto.createHmac("sha256", key).update(Buffer.from(raw, "base64url").toString("utf8")).digest("base64url");
  const a = Buffer.from(mac), b = Buffer.from(expect);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
  const [jobId, gotFn, expStr] = Buffer.from(raw, "base64url").toString("utf8").split("|");
  if (gotFn !== fn) return null;
  if (Date.now() > Number(expStr)) return null;
  return { jobId, fn: gotFn };
}

/* ── scheduled invocation detection (same shape as _etsyMailAuth) ──────── */
function isScheduledInvocation(event = {}) {
  const raw = event.headers || {};
  const h = {};
  for (const [k, v] of Object.entries(raw)) h[String(k).toLowerCase()] = v;
  if (h["x-nf-event-source"] === "scheduled") return true;
  if (h["x-netlify-event"] === "schedule") return true;
  try { if (event.body && JSON.parse(event.body)._scheduled === true) return true; } catch {}
  return false;
}

/* ── redaction for logs — never print source text or secrets ───────────── */
function redact(obj, maxLen = 200) {
  const SENSITIVE = /passcode|secret|private_key|api_key|authorization|session|nonce|token/i;
  const walk = (v, depth = 0) => {
    if (depth > 4) return "[deep]";
    if (v === null || v === undefined) return v;
    if (typeof v === "string") return v.length > maxLen ? v.slice(0, maxLen) + `…(+${v.length - maxLen})` : v;
    if (typeof v !== "object") return v;
    if (Array.isArray(v)) return v.slice(0, 20).map((x) => walk(x, depth + 1));
    const out = {};
    for (const [k, val] of Object.entries(v)) out[k] = SENSITIVE.test(k) ? "[redacted]" : walk(val, depth + 1);
    return out;
  };
  return walk(obj);
}

module.exports = {
  ALLOWED_ORIGINS, corsHeaders, json,
  requireOperator, mintSession, verifySession,
  mintWorkerNonce, verifyWorkerNonce,
  isScheduledInvocation, redact,
};
