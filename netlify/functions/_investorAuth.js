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
 *  SECRETS — two purpose-specific values are required:
 *    passcode       refuses all requests when unset
 *    sessionSecret  independent 32+ character random secret for sessions and
 *                   worker nonces.
 *
 *  They resolve from InvestorAI_Control/authConfig first and from
 *  INVESTOR_PASSCODE / INVESTOR_SESSION_SECRET second. Every variable a Lambda
 *  can read counts against a 4KB AWS limit that this deployment had already
 *  outgrown before Investor_AI existed.
 *
 *  THE TRADE THIS MAKES. The original comment on signingKey() said a
 *  purpose-specific secret keeps an HTTP-session compromise independent of the
 *  broad Firestore service credential. Storing it in Firestore gives that up:
 *  anything holding the service account can now mint an operator session. On a
 *  paper account with no broker route the blast radius is a disrupted
 *  experiment, and the operator accepted that explicitly. It is recorded here
 *  because a future reader of signingKey() deserves to know it was a decision
 *  rather than an oversight, and because it must be revisited before any real
 *  money is routed.
 *
 *  FAILURE DIRECTION IS UNCHANGED. Firestore unreachable, document missing,
 *  values absent or too short all land where an unset environment variable
 *  always did: signingKey() returns null and requireOperator answers
 *  AUTH_NOT_CONFIGURED. Nothing here can fail open.
 *
 *  CORS is a browser mechanic, not authorization. Both are enforced.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");

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

/* ── secret resolution: Firestore first, environment second ────────────────
 *
 * signingKey(), requireOperator(), mintSession(), verifySession() and both
 * worker-nonce functions are synchronous and are called from non-async paths,
 * so the cache is filled by loadAuthSecrets() at the top of each handler and
 * read synchronously thereafter. The TTL bounds how long a warm container can
 * hold a rotated secret. */
const AUTH_SECRETS_DOC = "authConfig";
const AUTH_SECRETS_TTL_MS = 60000;
const AUTH_BUILD = "v8.6.5-firestore-secrets";
let _authSecrets = null;
let _authSecretsAtMs = 0;

/* Whether the Firestore driver is even loadable in this bundle. If
   @google-cloud/firestore is missing from package.json, or esbuild bundled it
   instead of treating it as external, every Firestore read throws and the
   whole system fails closed with no way to tell that apart from a wrong
   passcode. This is checked once and reported in the misconfiguration
   response, because a lockout you cannot diagnose is its own outage. */
function firestoreDriverState() {
  try {
    /* The module id is assembled at runtime on purpose. A literal require()
       here is a STATIC dependency edge: the function bundler would try to
       resolve @google-cloud/firestore at build time and, if it is not declared
       in package.json, fail the whole function to a 502 — turning a diagnostic
       meant to explain an outage into the cause of one. Built this way the
       bundler cannot see it, and an absent driver is reported, not fatal. */
    const id = ["@google-cloud", "firestore"].join("/");
    const req = eval("require");
    const m = req(id);
    return m && m.Firestore ? "loaded" : "loaded_without_Firestore_export";
  } catch (e) {
    return `unavailable: ${String(e.message).slice(0, 80)}`;
  }
}

/* The gate itself, as a pure predicate so it can be attested without
   depending on cache state. Both values must be present and long enough
   TOGETHER; a half-populated pair is not a configuration anybody chose. */
function usableSecretPair(passcode, sessionSecret) {
  return typeof passcode === "string" && passcode.trim().length >= 16
    && typeof sessionSecret === "string" && sessionSecret.trim().length >= 32;
}

function envAuthSecrets() {
  return {
    passcode: String(process.env.INVESTOR_PASSCODE || ""),
    sessionSecret: String(process.env.INVESTOR_SESSION_SECRET || ""),
    source: process.env.INVESTOR_PASSCODE ? "environment" : "unset",
  };
}

/** Synchronous view. Never throws, never invents a secret. */
function authSecrets() {
  if (_authSecrets && Date.now() - _authSecretsAtMs <= AUTH_SECRETS_TTL_MS) return _authSecrets;
  return envAuthSecrets();
}

/** Refresh the cache from Firestore. Call once at the top of every handler,
 *  BEFORE any nonce or session check. */
async function loadAuthSecrets({ force = false } = {}) {
  if (!force && _authSecrets && Date.now() - _authSecretsAtMs <= AUTH_SECRETS_TTL_MS) {
    return _authSecrets;
  }
  const fallback = envAuthSecrets();
  try {
    const snap = await A.col(A.COL.control).doc(AUTH_SECRETS_DOC).get();
    const d = snap.exists ? (snap.data() || {}) : {};
    const passcode = typeof d.passcode === "string" ? d.passcode.trim() : "";
    const sessionSecret = typeof d.sessionSecret === "string" ? d.sessionSecret.trim() : "";
    /* Both must be present and long enough together, or the document is
       ignored entirely — a half-populated document must not silently pair a
       Firestore passcode with an environment session secret. */
    const usable = usableSecretPair(passcode, sessionSecret);
    _authSecrets = usable
      ? { passcode, sessionSecret, source: "firestore" }
      : { ...fallback, ...(snap.exists && (passcode || sessionSecret)
        ? { note: "authConfig present but passcode<16 or sessionSecret<32; ignored" } : {}) };
  } catch (e) {
    _authSecrets = { ...fallback, note: `authConfig read failed: ${String(e.message).slice(0, 90)}`,
      readFailed: true };
  }
  _authSecretsAtMs = Date.now();
  return _authSecrets;
}

/* ── token signing ─────────────────────────────────────────────────────── */
function signingKey() {
  const s = authSecrets();
  const seed = s.sessionSecret || "";
  const pass = s.passcode || "";
  if (seed.length < 32 || !pass) return null;
  // A purpose-specific secret keeps an HTTP-session compromise independent of
  // the broad Firestore service credential. Rotating either input invalidates
  // every outstanding operator session and worker nonce.
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
  const secrets = authSecrets();
  const expected = secrets.passcode;

  // Fail closed in EVERY context, not just production. The legacy authGate
  // opened when its variable was missing; that is the bug this replaces.
  // Unresolved secrets land here too: a Firestore outage locks the console
  // rather than opening it.
  if (!expected || !(secrets.sessionSecret || "").length
      || secrets.sessionSecret.length < 32) {
    /* Presence and shape only — never a length, never a value. Enough to
       tell "wrong build", "Firestore unreachable" and "document rejected"
       apart from one another without authenticating anything. */
    return { ok: false, response: json(event, 500, {
      error: "Server misconfigured: no usable operator passcode and 32+ character session secret "
        + `resolved from ${AUTH_SECRETS_DOC} or the environment`,
      errorCode: "AUTH_NOT_CONFIGURED",
      diagnostic: {
        build: AUTH_BUILD,
        resolvedFrom: secrets.source,
        firestoreDriver: firestoreDriverState(),
        passcodePresent: !!(secrets.passcode || "").length,
        passcodeMeetsFloor: (secrets.passcode || "").length >= 16,
        sessionSecretPresent: !!(secrets.sessionSecret || "").length,
        sessionSecretMeetsFloor: (secrets.sessionSecret || "").length >= 32,
        readFailed: secrets.readFailed === true,
        ...(secrets.note ? { note: secrets.note } : {}),
      },
    })};
  }

  const h = event.headers || {};
  const token = h["x-investor-session"] || h["X-Investor-Session"] || (body && body.session);
  const claims = token ? verifySession(token) : null;
  if (claims) return { ok: true, via: "session", subject: claims.s || "operator" };

  const supplied = h["x-investor-passcode"] || h["X-Investor-Passcode"] || (body && body.passcode);
  if (supplied) {
    const a = Buffer.from(String(supplied));
    const b = Buffer.from(String(expected));
    if (a.length === b.length && crypto.timingSafeEqual(a, b)) {
      return { ok: true, via: "passcode", subject: "operator", session: mintSession() };
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

/* ── bound worker nonce (blueprint §11.1) ─────────────────────────────────
   The fund-manager dispatcher signs the EXACT binding of a job attempt:
   {jobId, task, targetFunction, attempt, payloadHash, nonceId, expiry}. The
   worker verifies the signature, expiry and handler identity here, then
   _investorJobs.claimOnce atomically flips the stored nonce record from
   UNUSED to CONSUMED while claiming the attempt — so a replayed token is
   refused even though it still verifies. The key is injectable only so the
   deploy attestation can exercise the round trip without a live secret. */
const BOUND_NONCE_VERSION = "wn2";
function mintBoundWorkerNonce({ jobId, task, targetFunction, attempt = 1, payloadHash = "", nonceId, expiresAtMs }, { key = null } = {}) {
  const k = key || signingKey();
  if (!k || !jobId || !task || !targetFunction || !nonceId || !Number.isFinite(Number(expiresAtMs))) return null;
  const payload = [BOUND_NONCE_VERSION, jobId, task, targetFunction, String(Number(attempt) || 1),
    String(payloadHash || ""), nonceId, String(Math.floor(Number(expiresAtMs)))].join("|");
  const mac = crypto.createHmac("sha256", k).update(payload).digest("base64url");
  return Buffer.from(payload).toString("base64url") + "." + mac;
}
function verifyBoundWorkerNonce(token, expectedTargetFunction, { key = null, nowMs = Date.now() } = {}) {
  const k = key || signingKey();
  if (!k || typeof token !== "string" || !token.includes(".")) return null;
  const idx = token.lastIndexOf(".");
  const raw = token.slice(0, idx), mac = token.slice(idx + 1);
  let payload;
  try { payload = Buffer.from(raw, "base64url").toString("utf8"); } catch { return null; }
  const expect = crypto.createHmac("sha256", k).update(payload).digest("base64url");
  const a = Buffer.from(mac), b = Buffer.from(expect);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
  const parts = payload.split("|");
  if (parts.length !== 8 || parts[0] !== BOUND_NONCE_VERSION) return null;
  const [, jobId, task, targetFunction, attempt, payloadHash, nonceId, expStr] = parts;
  if (targetFunction !== expectedTargetFunction) return null;
  if (nowMs > Number(expStr)) return null;
  return { jobId, task, targetFunction, attempt: Number(attempt), payloadHash, nonceId, expiresAtMs: Number(expStr) };
}

/* ── scheduled invocation detection (same shape as _etsyMailAuth) ──────── */
function isScheduledInvocation(event = {}) {
  const raw = event.headers || {};
  const h = {};
  for (const [k, v] of Object.entries(raw)) h[String(k).toLowerCase()] = v;
  if (h["x-nf-event-source"] === "scheduled") return true;
  if (h["x-netlify-event"] === "schedule") return true;
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


/* ═══ fund-manager-v1: origin enforcement, HttpOnly cookie sessions, CSRF,
   reauthentication (blueprint §11.5, §13 "_investorAuth: retain/harden") ═══
   The legacy bearer path above stays for v1 compatibility reads; every v2
   request and every session operation goes through the functions below.
   Cookie sessions are server-state: a Sessions document is the authority,
   so sign-out and rotation revoke immediately regardless of cookie age.  */
const SESSION_COOKIE = "inv_session";
const COOKIE_SESSION_TTL_MS = 12 * 60 * 60 * 1000;
const REAUTH_TTL_MS = 5 * 60 * 1000;
const CSRF_BYTES = 24;

function extraOrigins() { return String(process.env.INVESTOR_EXTRA_ORIGINS || "").split(",").map((s) => s.trim()).filter((s) => /^https?:\/\/[a-z0-9.-]+(:[0-9]+)?$/i.test(s)); }
function allowedOrigins() { return [...ALLOWED_ORIGINS, ...extraOrigins()]; }
function hostOf(origin) { try { return new URL(origin).host.toLowerCase(); } catch { return null; } }
function headerOf(event, name) {
  const h = (event && event.headers) || {};
  const want = String(name).toLowerCase();
  for (const [k, v] of Object.entries(h)) if (String(k).toLowerCase() === want) return v;
  return undefined;
}
/** Reject — do not merely CORS-filter — a request whose Origin or Host is not allowlisted. */
function enforceOrigin(event) {
  const origins = allowedOrigins();
  const hosts = new Set(origins.map(hostOf).filter(Boolean));
  const origin = String(headerOf(event, "origin") || "").trim();
  const host = String(headerOf(event, "x-forwarded-host") || headerOf(event, "host") || "").trim().toLowerCase();
  if (origin && !origins.includes(origin)) return { ok: false, code: "ORIGIN_REJECTED", reason: "origin not allowlisted" };
  if (host && !hosts.has(host)) return { ok: false, code: "ORIGIN_REJECTED", reason: "host not allowlisted" };
  if (!origin && !host) return { ok: false, code: "ORIGIN_REJECTED", reason: "no origin or host" };
  const fetchSite = String(headerOf(event, "sec-fetch-site") || "").toLowerCase();
  if (fetchSite && !["same-origin", "same-site", "none"].includes(fetchSite)) return { ok: false, code: "ORIGIN_REJECTED", reason: `sec-fetch-site ${fetchSite}` };
  return { ok: true, origin: origin || null, host: host || null, secure: /^https:/i.test(origin) || !/localhost|127\.0\.0\.1/.test(host) };
}
function parseCookies(event) {
  const raw = String(headerOf(event, "cookie") || "");
  const out = {};
  for (const part of raw.split(";")) { const i = part.indexOf("="); if (i > 0) out[part.slice(0, i).trim()] = decodeURIComponent(part.slice(i + 1).trim()); }
  return out;
}
function sha256(s) { return crypto.createHash("sha256").update(String(s)).digest("hex"); }
function timingEqual(a, b) { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); }
function verifyPasscode(supplied) {
  const expected = authSecrets().passcode;
  if (!expected || !supplied) return false;
  return timingEqual(String(supplied), String(expected));
}
function purposeKey(purpose) {
  const key = signingKey();
  return key ? crypto.hkdfSync("sha256", key, Buffer.from(`investor-ai-${purpose}`), Buffer.from("v2"), 32) : null;
}
function signPayload(purpose, claims) {
  const key = purposeKey(purpose);
  if (!key) return null;
  const payload = Buffer.from(JSON.stringify(claims)).toString("base64url");
  return `${payload}.${crypto.createHmac("sha256", Buffer.from(key)).update(payload).digest("base64url")}`;
}
function openPayload(purpose, token, { nowMs = Date.now() } = {}) {
  const key = purposeKey(purpose);
  if (!key || typeof token !== "string" || !token.includes(".")) return null;
  const idx = token.lastIndexOf(".");
  const payload = token.slice(0, idx), mac = token.slice(idx + 1);
  const expect = crypto.createHmac("sha256", Buffer.from(key)).update(payload).digest("base64url");
  if (!timingEqual(mac, expect)) return null;
  let claims; try { claims = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")); } catch { return null; }
  if (!claims || typeof claims.exp !== "number" || nowMs > claims.exp + CLOCK_SKEW_MS) return null;
  return claims;
}
function cookieHeader(token, { expiresAtMs, secure = true } = {}) {
  const maxAge = Math.max(0, Math.floor((Number(expiresAtMs) - Date.now()) / 1000));
  return `${SESSION_COOKIE}=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${maxAge}${secure ? "; Secure" : ""}`;
}
function clearCookieHeader({ secure = true } = {}) { return `${SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0${secure ? "; Secure" : ""}`; }

/** Create a cookie session: server record + signed cookie token + rotating CSRF token (returned in the body, never in the cookie). */
async function createCookieSession({ subject = "operator", admin = null, nowMs = Date.now() } = {}) {
  const D = admin || A;
  if (!signingKey()) return null;
  const sessionId = `s_${crypto.randomBytes(12).toString("hex")}`;
  const csrfToken = crypto.randomBytes(CSRF_BYTES).toString("base64url");
  const expiresAtMs = nowMs + COOKIE_SESSION_TTL_MS;
  await D.col(D.COL.sessions).doc(sessionId).set({ sessionId, subject, createdAtMs: nowMs, expiresAtMs, revokedAtMs: null, csrfHash: sha256(csrfToken), csrfRotatedAtMs: nowMs, reauthAtMs: null, lastSeenAtMs: nowMs, kind: "cookie" });
  const token = signPayload("cookie-session", { sid: sessionId, s: subject, exp: expiresAtMs, n: crypto.randomBytes(6).toString("hex") });
  return { sessionId, subject, expiresAtMs, csrfToken, token };
}
/** Verify the cookie against its server record. null when absent, expired, revoked or forged. */
async function verifyCookieSession(event, { admin = null, nowMs = Date.now() } = {}) {
  const D = admin || A;
  const token = parseCookies(event)[SESSION_COOKIE];
  if (!token) return null;
  const claims = openPayload("cookie-session", token, { nowMs });
  if (!claims || !claims.sid) return null;
  const snap = await D.col(D.COL.sessions).doc(String(claims.sid)).get();
  if (!snap.exists) return null;
  const doc = snap.data() || {};
  if (doc.revokedAtMs || Number(doc.expiresAtMs) < nowMs) return null;
  return { sessionId: doc.sessionId, subject: doc.subject || claims.s || "operator", csrfHash: doc.csrfHash || null, reauthAtMs: doc.reauthAtMs || null, expiresAtMs: Number(doc.expiresAtMs), doc };
}
async function rotateCsrf(sessionId, { admin = null, nowMs = Date.now() } = {}) {
  const D = admin || A;
  const csrfToken = crypto.randomBytes(CSRF_BYTES).toString("base64url");
  await D.col(D.COL.sessions).doc(String(sessionId)).set({ csrfHash: sha256(csrfToken), csrfRotatedAtMs: nowMs, lastSeenAtMs: nowMs }, { merge: true });
  return csrfToken;
}
async function revokeCookieSession(sessionId, { admin = null, nowMs = Date.now(), reason = "sign_out" } = {}) {
  const D = admin || A;
  await D.col(D.COL.sessions).doc(String(sessionId)).set({ revokedAtMs: nowMs, revokedReason: reason }, { merge: true });
  return { revoked: true, sessionId };
}
/** A reauthentication token: the passcode re-entered inside a live session; short-lived, bound to the session. */
async function mintReauthToken(session, { admin = null, nowMs = Date.now() } = {}) {
  const D = admin || A;
  await D.col(D.COL.sessions).doc(String(session.sessionId)).set({ reauthAtMs: nowMs }, { merge: true });
  const expiresAtMs = nowMs + REAUTH_TTL_MS;
  return { reauthToken: signPayload("reauth", { sid: session.sessionId, exp: expiresAtMs, n: crypto.randomBytes(6).toString("hex") }), expiresAtMs };
}
function verifyReauthToken(token, session, { nowMs = Date.now() } = {}) {
  const claims = openPayload("reauth", token, { nowMs });
  return !!(claims && session && claims.sid === session.sessionId);
}
/** The v2 guard: cookie session (401), CSRF for mutations (403), reauth when the action demands it (403). */
async function requireOperatorV2(event, body, { mutation = false, reauth = false, admin = null, nowMs = Date.now() } = {}) {
  const secrets = authSecrets();
  if (!secrets.passcode || !(secrets.sessionSecret || "").length || secrets.sessionSecret.length < 32) return { ok: false, code: "AUTH_NOT_CONFIGURED", message: "operator secrets are not configured" };
  const origin = enforceOrigin(event);
  if (!origin.ok) return { ok: false, code: "ORIGIN_REJECTED", message: origin.reason };
  const session = await verifyCookieSession(event, { admin, nowMs });
  if (!session) return { ok: false, code: "SESSION_EXPIRED", message: "no live session; sign in again" };
  if (mutation) {
    const supplied = body && body.csrfToken;
    if (!supplied || !session.csrfHash || !timingEqual(sha256(supplied), session.csrfHash)) return { ok: false, code: "CSRF_INVALID", message: "CSRF token missing or stale; refresh the session" };
  }
  let reauthed = false;
  if (body && body.reauthToken) reauthed = verifyReauthToken(body.reauthToken, session, { nowMs });
  if (reauth && !reauthed) return { ok: false, code: "REAUTH_REQUIRED", message: "this action requires the passcode to be re-entered" };
  return { ok: true, via: "cookie", subject: session.subject, sessionId: session.sessionId, reauthed, session, origin };
}
/** v1 compatibility: accept the cookie session first, then the legacy bearer/passcode path. */
async function requireOperatorAsync(event, body, { admin = null } = {}) {
  try {
    const session = await verifyCookieSession(event, { admin });
    if (session) return { ok: true, via: "cookie", subject: session.subject, sessionId: session.sessionId };
  } catch {}
  return requireOperator(event, body);
}

module.exports = {
  ALLOWED_ORIGINS, corsHeaders, json,
  authSecrets, loadAuthSecrets, usableSecretPair, AUTH_SECRETS_DOC, AUTH_BUILD,
  firestoreDriverState,
  requireOperator, mintSession, verifySession,
  mintWorkerNonce, verifyWorkerNonce, mintBoundWorkerNonce, verifyBoundWorkerNonce, BOUND_NONCE_VERSION,
  isScheduledInvocation, redact,
  /* fund-manager-v1 */
  SESSION_COOKIE, COOKIE_SESSION_TTL_MS, REAUTH_TTL_MS, allowedOrigins, enforceOrigin, parseCookies, headerOf, verifyPasscode, timingEqual,
  cookieHeader, clearCookieHeader, createCookieSession, verifyCookieSession, rotateCsrf, revokeCookieSession, mintReauthToken, verifyReauthToken,
  requireOperatorV2, requireOperatorAsync, signPayload, openPayload,
};
