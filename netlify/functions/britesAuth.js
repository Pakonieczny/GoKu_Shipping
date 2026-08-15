/*  netlify/functions/britesAuth.js  (v1.1)
 *  ---------------------------------------------------------------------------
 *  Custom Charm Studio — authentication & email verification backend.
 *
 *  Deploys to the SAME Netlify site as the storefront functions
 *  (https://goldenspike.app/.netlify/functions/britesAuth) and follows their
 *  conventions exactly: node-fetch, firebaseAdmin.js, the shared
 *  BRITES_EMAIL_ALLOWED_ORIGINS CORS allowlist, and Brites_* Firestore
 *  collections — the same pattern emailCapture.js uses.
 *
 *  Piggybacks entirely on services this stack already runs:
 *    · firebaseAdmin.js      → Firebase Admin (Auth + Firestore)
 *    · _etsyMailGmail.js     → used when co-deployed; otherwise this file
 *                              talks to the Gmail API itself using the SAME
 *                              config/gmailOauth Firestore doc and the same
 *                              GMAIL_CLIENT_ID / GMAIL_CLIENT_SECRET vars,
 *                              so no new credentials are introduced.
 *    · Shopify (optional)    → a verified studio account is also registered
 *                              as a Shopify customer, mirroring emailCapture.
 *
 *  Endpoints (POST { kind, ... }):
 *    kind: "send_code"     → issue a 6-digit code and email it
 *    kind: "verify_code"   → check the code, mark the address verified,
 *                            return a Firebase custom token
 *    kind: "google_verify" → verify a Google Identity Services credential and
 *                            return a Firebase custom token (server-side path;
 *                            the browser normally uses signInWithCredential)
 *
 *  ── v1.1, Custom Charm Studio ───────────────────────────────────────────
 *    kind: "upload_ref"          → accept a customer reference image and write
 *                                  it to Storage with the Admin SDK. THE
 *                                  BROWSER NEVER TOUCHES FIREBASE STORAGE:
 *                                  this mirrors shopifyEditor.js's rule and
 *                                  removes the bucket-CORS blocker entirely.
 *    kind: "wallet_get"          → read users/{uid}.wallet, provisioning the
 *                                  guest free allowance on first contact so a
 *                                  brand-new anonymous visitor really does
 *                                  have credits the generator can debit
 *    kind: "wallet_grant_signup" → grant the signup bonus EXACTLY ONCE per uid,
 *                                  keyed by a walletLedger entry
 *
 *  Also exported (not an endpoint):
 *    grantStudioCredits({ orderId, uid, packs })
 *        called by shopifyOrderWebhook.js when a STUDIO-PACK-* line is paid
 *        for. Transactional and idempotent on the Shopify order id, so a
 *        webhook retry can never double-credit. Lives here because this file
 *        already carries Firebase Admin — no new lambda (§10.8).
 *
 *  SECURITY MODEL
 *    · Codes are never stored in plaintext — only SHA-256(code + pepper).
 *    · 10-minute expiry, single use, max 5 attempts, then the code is burned.
 *    · Per-address rate limits: 60s between sends, 5 sends per hour.
 *    · Per-IP rate limit: 20 sends per hour.
 *    · Constant-time comparison on verification.
 *    · Enumeration-safe: send_code always returns the same shape.
 *    · An anonymous uid can be passed in so the verified email is LINKED to
 *      the existing account — guest designs and credits carry over untouched.
 *
 *  ENV — ZERO NEW VARIABLES. Everything below already exists on this
 *  deployment; nothing needs to be added to the Netlify settings.
 *    FIREBASE_PRIVATE_KEY / _PROJECT_ID / _CLIENT_EMAIL   via firebaseAdmin.js
 *        · FIREBASE_PRIVATE_KEY additionally seeds the code-hashing pepper
 *          through HKDF-SHA256 (server-only secret, never leaves the lambda)
 *    GMAIL_CLIENT_ID / GMAIL_CLIENT_SECRET   Gmail send + Google credential aud
 *        · The config/gmailOauth grant must include https://mail.google.com/
 *          — the scope _etsyMailGmail.js names as preferred. EtsyMail was
 *          seeded read-only because reading Etsy notifications was all it
 *          needed, and Gmail answers messages/send with PERMISSION_DENIED on a
 *          read-only grant. Re-seed with the wider scope; it is a superset, so
 *          EtsyMail is unaffected and no new credential is introduced.
 *    SHOPIFY_STORE / SHOPIFY_CLIENT_ID / SHOPIFY_CLIENT_SECRET   customer sync
 *
 *  Anything that would otherwise be a new variable is instead a field on the
 *  Firestore doc `config/customStudio` — editable without a deploy:
 *    { googleClientId?, fromName?, fromEmail?, siteUrl?, allowedOrigins?[] }
 * ---------------------------------------------------------------------------
 */

const crypto = require("crypto");
const admin = require("./firebaseAdmin");

/* node-fetch to match every other function in this deployment; falls back to
   the Node 18+ global when the package isn't bundled. */
let fetchFn;
try { fetchFn = require("node-fetch"); } catch { fetchFn = globalThis.fetch; }

/* Use the shared Gmail helper when it is co-deployed; otherwise fall back to
   the self-contained sender below (same OAuth doc, same env vars). */
let sharedGmailFetch = null;
try { sharedGmailFetch = require("./_etsyMailGmail").gmailFetch; } catch { /* not on this site */ }

const db = admin.firestore();

/* ── Gmail: identical token handling to _etsyMailGmail.js ─────────────── */
const OAUTH_DOC_PATH = "config/gmailOauth";           // same doc, same tokens
const GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1/users/me";
const TOKEN_REFRESH_BUFFER_MS = 2 * 60 * 1000;

async function refreshGmailToken(oldRefreshToken) {
  const res = await fetchFn("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "refresh_token",
      client_id: process.env.GMAIL_CLIENT_ID,
      client_secret: process.env.GMAIL_CLIENT_SECRET,
      refresh_token: oldRefreshToken,
    }),
  });
  if (!res.ok) {
    const e = new Error("email_not_configured");        // dead/revoked refresh token
    e.status = 503; e.detail = `Gmail token refresh failed: ${res.status} ${(await res.text()).slice(0, 300)}`;
    throw e;
  }
  const data = await res.json();
  await db.doc(OAUTH_DOC_PATH).set({
    access_token: data.access_token,
    refresh_token: data.refresh_token || oldRefreshToken,
    expires_at: Date.now() + Math.max(0, (data.expires_in - 120)) * 1000,
    token_type: data.token_type || "Bearer",
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
  return data.access_token;
}

let sharedFailure = null;
async function gmailSend(rawBase64Url) {
  sharedFailure = null;
  if (sharedGmailFetch) {
    /* Borrowed from _etsyMailGmail when it is deployed alongside — but a
       helper written for another pipeline must never be the single point of
       failure for THIS one. If it throws for any reason, fall through to the
       self-contained path below. */
    try {
      return await sharedGmailFetch("/messages/send", { method: "POST", body: JSON.stringify({ raw: rawBase64Url }) });
    } catch (e) {
      /* Keep the reason. This helper owns the site's WORKING mail credentials,
         so when it fails its message is the most informative thing available —
         swallowing it into a console.warn is what hid the real story here. */
      sharedFailure = "shared gmailFetch: " + ((e && e.message) || e);
      console.warn("[britesAuth]", sharedFailure);
    }
  }
  const snap = await db.doc(OAUTH_DOC_PATH).get();
  if (!snap.exists) {
    const e = new Error("email_not_configured");        // config/gmailOauth missing
    e.status = 503; e.detail = `Gmail OAuth not seeded at ${OAUTH_DOC_PATH}`;
    throw e;
  }
  if (!process.env.GMAIL_CLIENT_ID || !process.env.GMAIL_CLIENT_SECRET) {
    const e = new Error("email_not_configured");        // envs missing on this site
    e.status = 503; e.detail = "GMAIL_CLIENT_ID / GMAIL_CLIENT_SECRET not set";
    throw e;
  }
  const tok = snap.data();
  let token = tok.access_token;
  if (!token || (tok.expires_at || 0) - Date.now() < TOKEN_REFRESH_BUFFER_MS) {
    token = await refreshGmailToken(tok.refresh_token);
  }
  let res = await fetchFn(`${GMAIL_API_BASE}/messages/send`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ raw: rawBase64Url }),
  });
  if (res.status === 401) {                                  // stale token → refresh once
    token = await refreshGmailToken(tok.refresh_token);
    res = await fetchFn(`${GMAIL_API_BASE}/messages/send`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ raw: rawBase64Url }),
    });
  }
  if (!res.ok) {
    const e = new Error("email_send_failed");
    e.status = 502; e.detail = `Gmail send ${res.status}: ${(await res.text()).slice(0, 300)}`;
    throw e;
  }
  return res.json();
}

/* ── Sending ───────────────────────────────────────────────────────────────
   Resend carries the mail. It is the transport emailCapture.js was written
   for, from the domain-verified hello@britesjewelry.com sender, and it is the
   only transactional sender this stack has: Shopify's automations fire on a
   marketing subscribe and cannot be called with a one-time code, and the
   EtsyMail Gmail grant is read-only by design (its own header names
   .readonly as the minimum) so Gmail answers messages/send with
   PERMISSION_DENIED.

   Gmail is kept as a second attempt rather than deleted, because it costs
   nothing when unconfigured and it means a deployment that later widens that
   scope needs no code change. Every transport that fails contributes its own
   words to the detail — "it did not send" without the provider's reason is
   what made this expensive to diagnose. */
async function resendSend({ to, subject, html, text }) {
  const res = await fetchFn("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: "Bearer " + process.env.RESEND_API_KEY,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      from: process.env.BRITES_EMAIL_FROM || `${FROM_NAME} <${FROM_EMAIL}>`,
      to: [to], subject, html, text,
    }),
  });
  if (!res.ok) {
    const e = new Error("email_send_failed");
    e.status = 502; e.detail = `Resend ${res.status}: ${(await res.text()).slice(0, 300)}`;
    throw e;
  }
  return res.json();
}

async function deliverEmail({ to, subject, html, text }) {
  const tried = [];

  if (process.env.RESEND_API_KEY) {
    try { return await resendSend({ to, subject, html, text }); }
    catch (e) { tried.push(e.detail || e.message); }
  } else {
    tried.push("Resend: RESEND_API_KEY not set on this Netlify site");
  }

  /* Gmail sends AS the authorised account and rejects a From it cannot send
     as, so that one case is retried without a From header — Gmail then uses
     the account's own address. Delivered mail beats a perfect envelope. */
  const branded = `${FROM_NAME} <${FROM_EMAIL}>`;
  const viaGmail = (from) => gmailSend(buildRawMessage({ to, subject, html, text, from }));
  try {
    return await viaGmail(branded);
  } catch (e) {
    const detail = String(e.detail || e.message || "");
    if (/from|alias|sendAs|invalid/i.test(detail)) {
      try {
        const sent = await viaGmail(null);
        console.warn("[britesAuth] sent as the authorised Gmail account —",
                     branded, "is not a verified alias on it");
        return sent;
      } catch (e2) { tried.push(`${detail} | retried without From: ${e2.detail || e2.message}`); }
    } else {
      if (sharedFailure) tried.push(sharedFailure);
      else if (!sharedGmailFetch) tried.push("_etsyMailGmail not deployed on this site");
      tried.push(detail);
    }
  }

  const err = new Error(tried.every((t) => /not set|not seeded|not configured|not deployed/i.test(t))
    ? "email_not_configured" : "email_send_failed");
  err.status = err.message === "email_not_configured" ? 503 : 502;
  err.detail = tried.join(" | ");
  throw err;
}

const VERIFY_COL       = "Brites_Studio_Verifications";   // matches Brites_* convention
const RATE_COL         = "Brites_Studio_RateLimits";
const STUDIO_SESSIONS  = "customSessions";
const GRANT_COL        = "Brites_Studio_Grants";           // idempotency claims
const CODE_TTL_MS      = 10 * 60 * 1000;   // 10 minutes
const RESEND_COOLDOWN  = 60 * 1000;        // 60 seconds between sends
const MAX_SENDS_HOUR   = 5;                // per address
const MAX_IP_SENDS_HOUR= 20;               // per IP
const MAX_ATTEMPTS     = 5;                // wrong guesses before burn
const CODE_LENGTH      = 6;

/* Studio limits. Mirrored from config/customStudio so they stay tunable
   without a deploy; these are only the fallbacks. */
const STUDIO_DEFAULTS = {
  guestFreeCredits: 3,
  signupBonusCredits: 7,
  guestFreeUploads: 3,
  signupBonusUploads: 7,
  maxUploadBytes: 15 * 1024 * 1024,
  maxUploadPx: 2048,
};
const UPLOAD_MIME = { "image/jpeg": "jpg", "image/png": "png" };
const MAX_UPLOADS_HOUR = 30;                 // per uid, on top of the allowance

/* Runtime config — a Firestore doc, not env vars. Cached per warm lambda. */
const CONFIG_DOC = "config/customStudio";
const CONFIG_DEFAULTS = {
  fromName: "Brites Jewelry",
  fromEmail: "hello@britesjewelry.com",
  siteUrl: "https://britesjewelry.com",
  googleClientId: null,                       // falls back to GMAIL_CLIENT_ID
  allowedOrigins: [
    "https://britesjewelry.com",
    "https://www.britesjewelry.com",
    "https://brites-jewelry.myshopify.com",
  ],
  ...STUDIO_DEFAULTS,
};
let _cfg = null, _cfgAt = 0;
async function config() {
  if (_cfg && Date.now() - _cfgAt < 60_000) return _cfg;
  try {
    const snap = await db.doc(CONFIG_DOC).get();
    _cfg = { ...CONFIG_DEFAULTS, ...(snap.exists ? snap.data() : {}) };
  } catch { _cfg = { ...CONFIG_DEFAULTS }; }
  _cfgAt = Date.now();
  return _cfg;
}
let FROM_NAME = CONFIG_DEFAULTS.fromName;
let FROM_EMAIL = CONFIG_DEFAULTS.fromEmail;
let SITE_URL = CONFIG_DEFAULTS.siteUrl;

/* ══════════════════════════════ helpers ══════════════════════════════ */

/* Same origins emailCapture.js defaults to; overridable via config/customStudio
   without touching Netlify settings. */
let ALLOWED_ORIGINS = CONFIG_DEFAULTS.allowedOrigins.slice();

function corsHeaders(origin) {
  const ok = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
  return {
    "Access-Control-Allow-Origin": ok,
    "Access-Control-Allow-Methods": "POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Vary": "Origin",
    "Cache-Control": "no-store",
    "Content-Type": "application/json",
  };
}
let _origin = "";
/* Stamped on every response. "Which build is actually deployed?" is otherwise
   unanswerable from the outside, and guessing at it has cost real time. */
const FN_BUILD = "britesAuth-1.9.0";
const json = (statusCode, body) => ({
  statusCode, headers: corsHeaders(_origin),
  body: JSON.stringify(Object.assign({ fn: FN_BUILD }, body)),
});

const normalizeEmail = (e) => String(e || "").trim().toLowerCase();

const validEmail = (e) =>
  /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(e) && e.length <= 254;

/** Document id for an address — hashed so raw addresses never key a doc. */
const addrKey = (email) =>
  crypto.createHash("sha256").update(`addr:${email}`).digest("hex").slice(0, 40);

/* Pepper derivation — no new env var. FIREBASE_PRIVATE_KEY is a high-entropy
   server-only secret that already exists here; HKDF gives us a key that is
   domain-separated from its Firebase use, so the two can never collide.
   (Set BRITES_CODE_PEPPER later if you ever want to rotate independently.) */
let _pepper = null;
function pepper() {
  if (_pepper) return _pepper;
  if (process.env.BRITES_CODE_PEPPER) { _pepper = Buffer.from(process.env.BRITES_CODE_PEPPER); return _pepper; }
  const seed = process.env.FIREBASE_PRIVATE_KEY || process.env.SHOPIFY_CLIENT_SECRET;
  if (!seed) throw new Error("No secret available to derive the code pepper");
  _pepper = Buffer.from(
    crypto.hkdfSync("sha256", Buffer.from(seed), Buffer.from("brites-studio-otp"),
                    Buffer.from("code-pepper-v1"), 32)
  );
  return _pepper;
}

/** Hash a code with the server pepper. Codes are never stored in the clear. */
function hashCode(code, email) {
  return crypto.createHmac("sha256", pepper()).update(`${email}:${code}`).digest("hex");
}

/** Cryptographically strong 6-digit code, uniformly distributed. */
function generateCode() {
  const max = 10 ** CODE_LENGTH;                       // 1_000_000
  const limit = Math.floor(0xffffffff / max) * max;    // reject bias
  let n;
  do { n = crypto.randomBytes(4).readUInt32BE(0); } while (n >= limit);
  return String(n % max).padStart(CODE_LENGTH, "0");
}

const timingSafeEqual = (a, b) => {
  const ba = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ba.length !== bb.length) return false;
  return crypto.timingSafeEqual(ba, bb);
};

const clientIp = (event) =>
  (event.headers["x-nf-client-connection-ip"] ||
   (event.headers["x-forwarded-for"] || "").split(",")[0] ||
   "unknown").trim();

/* ═══════════════════════════ email template ═══════════════════════════
 * Brites design language in email-safe HTML: table layout, inline styles,
 * no web fonts (Georgia stands in for Cormorant), 600px, dark-mode aware,
 * with a plain-text alternative. The code is selectable text — never an
 * image — so it can be copied on any device.
 * ===================================================================== */
function buildVerificationEmail({ code, expiresMin, requestedFrom }) {
  const gold = "#a58a52", ink = "#1c1d1d", hair = "#e6e4e0", band = "#faf9f7", muted = "#6d6a66";

  const html = `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark">
<meta name="supported-color-schemes" content="light dark">
<title>Your Brites verification code</title>
</head>
<body style="margin:0;padding:0;background:${band};">
  <!-- preheader: shows in the inbox preview, hidden in the body -->
  <div style="display:none;max-height:0;overflow:hidden;opacity:0;">
    ${code} is your Brites verification code — it expires in ${expiresMin} minutes.
  </div>

  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:${band};">
    <tr><td align="center" style="padding:34px 16px;">

      <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0"
             style="width:600px;max-width:100%;background:#ffffff;border:1px solid ${hair};">

        <!-- masthead -->
        <tr><td align="center" style="padding:34px 40px 8px;">
          <div style="font-family:Georgia,'Times New Roman',serif;font-size:27px;letter-spacing:.14em;color:${ink};">
            BRITES<span style="color:${gold};">.</span>
          </div>
          <div style="font-family:Helvetica,Arial,sans-serif;font-size:10px;font-weight:bold;letter-spacing:.28em;text-transform:uppercase;color:${muted};padding-top:8px;">
            Custom Charm Studio
          </div>
        </td></tr>

        <tr><td align="center" style="padding:18px 40px 0;">
          <div style="width:54px;height:1px;background:${gold};line-height:1px;font-size:0;">&nbsp;</div>
        </td></tr>

        <!-- headline -->
        <tr><td align="center" style="padding:24px 40px 0;">
          <h1 style="margin:0;font-family:Georgia,'Times New Roman',serif;font-weight:500;font-size:29px;line-height:1.2;color:${ink};">
            Confirm your email
          </h1>
          <p style="margin:12px 0 0;font-family:Helvetica,Arial,sans-serif;font-size:15px;line-height:1.7;color:${muted};">
            Enter this code in the Custom Charm Studio to finish creating your account.
          </p>
        </td></tr>

        <!-- the code -->
        <tr><td align="center" style="padding:26px 40px 0;">
          <table role="presentation" cellpadding="0" cellspacing="0" border="0"
                 style="border:1px solid ${gold};background:${band};">
            <tr><td align="center" style="padding:22px 44px;">
              <div style="font-family:'Courier New',Courier,monospace;font-size:38px;font-weight:bold;letter-spacing:.34em;color:${ink};text-indent:.34em;">
                ${code}
              </div>
            </td></tr>
          </table>
          <p style="margin:14px 0 0;font-family:Helvetica,Arial,sans-serif;font-size:12px;letter-spacing:.18em;text-transform:uppercase;color:${muted};">
            Expires in ${expiresMin} minutes
          </p>
        </td></tr>

        <!-- primary action -->
        <tr><td align="center" style="padding:26px 40px 0;">
          <table role="presentation" cellpadding="0" cellspacing="0" border="0">
            <tr><td align="center" bgcolor="${ink}" style="background:${ink};">
              <a href="${SITE_URL}/pages/custom-charm-studio?verify=1"
                 style="display:inline-block;padding:15px 34px;font-family:Helvetica,Arial,sans-serif;font-size:12px;font-weight:bold;letter-spacing:.3em;text-transform:uppercase;color:#ffffff;text-decoration:none;">
                Return to the studio
              </a>
            </td></tr>
          </table>
        </td></tr>

        <!-- reassurance -->
        <tr><td style="padding:30px 40px 0;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"
                 style="border-top:1px solid ${hair};">
            <tr><td style="padding:20px 0 0;">
              <p style="margin:0 0 10px;font-family:Helvetica,Arial,sans-serif;font-size:13px;line-height:1.75;color:${muted};">
                <strong style="color:${ink};">Didn't request this?</strong> You can safely ignore this email —
                no account is created and nothing is charged without this code.
              </p>
              <p style="margin:0;font-family:Helvetica,Arial,sans-serif;font-size:13px;line-height:1.75;color:${muted};">
                We will never ask you for this code by phone, chat or email. It is only ever typed
                into the studio on ${SITE_URL.replace(/^https?:\/\//, "")}.
              </p>
              ${requestedFrom ? `<p style="margin:10px 0 0;font-family:Helvetica,Arial,sans-serif;font-size:12px;line-height:1.7;color:#9a968f;">Requested from ${requestedFrom}.</p>` : ""}
            </td></tr>
          </table>
        </td></tr>

        <!-- help -->
        <tr><td align="center" style="padding:22px 40px 34px;">
          <p style="margin:0;font-family:Helvetica,Arial,sans-serif;font-size:12.5px;line-height:1.7;color:${muted};">
            Need a hand? Just reply to this email — a real person reads it.
          </p>
        </td></tr>
      </table>

      <!-- footer -->
      <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="width:600px;max-width:100%;">
        <tr><td align="center" style="padding:20px 40px 0;">
          <p style="margin:0;font-family:Helvetica,Arial,sans-serif;font-size:11px;line-height:1.8;letter-spacing:.06em;color:#9a968f;">
            Brites Jewelry · Handmade in Toronto<br>
            <a href="${SITE_URL}" style="color:#9a968f;text-decoration:underline;">${SITE_URL.replace(/^https?:\/\//, "")}</a>
          </p>
        </td></tr>
      </table>

    </td></tr>
  </table>
</body></html>`;

  const text =
`Confirm your email — Brites Custom Charm Studio

Your verification code is: ${code}
It expires in ${expiresMin} minutes.

Enter it in the studio to finish creating your account.
${SITE_URL}/pages/custom-charm-studio

Didn't request this? Ignore this email — no account is created and
nothing is charged without this code. We will never ask you for this
code by phone, chat or email.

Brites Jewelry · Handmade in Toronto`;

  return { html, text };
}

/** RFC 2822 multipart/alternative message, base64url encoded for Gmail. */
function buildRawMessage({ to, subject, html, text, from }) {
  const boundary = "brites_" + crypto.randomBytes(12).toString("hex");
  const lines = [
    /* Omitting From makes Gmail use the authorised account's own address,
       which is the correct behaviour when the branded address is not a
       verified "Send mail as" alias on that account. */
    ...(from === null ? [] : [`From: ${from || `${FROM_NAME} <${FROM_EMAIL}>`}`]),
    `To: ${to}`,
    `Subject: ${subject}`,
    "MIME-Version: 1.0",
    `Content-Type: multipart/alternative; boundary="${boundary}"`,
    "",
    `--${boundary}`,
    'Content-Type: text/plain; charset="UTF-8"',
    "Content-Transfer-Encoding: 7bit",
    "",
    text,
    "",
    `--${boundary}`,
    'Content-Type: text/html; charset="UTF-8"',
    "Content-Transfer-Encoding: base64",
    "",
    Buffer.from(html, "utf8").toString("base64").replace(/(.{76})/g, "$1\n"),
    "",
    `--${boundary}--`,
    "",
  ];
  return Buffer.from(lines.join("\r\n"), "utf8")
    .toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}


/* ═════════════════════ Custom Charm Studio — shared plumbing ═════════════
 * Everything below serves the studio section on the storefront. It reuses the
 * file's existing Firebase Admin handle, CORS helper and Firestore
 * conventions; nothing new is configured and no new environment variable is
 * introduced.
 * ===================================================================== */

/** Verify the caller's Firebase ID token. Anonymous uids are first-class
 *  citizens here — a guest's free allowance is a real wallet, so their token
 *  must be accepted exactly like a signed-in customer's. */
async function requireStudioUser(event) {
  const h = event.headers || {};
  const raw = h.authorization || h.Authorization || "";
  const token = raw.startsWith("Bearer ") ? raw.slice(7).trim() : "";
  if (!token) { const e = new Error("sign_in_required"); e.status = 401; throw e; }
  try {
    const decoded = await admin.auth().verifyIdToken(token);
    return decoded.uid;
  } catch (err) {
    const e = new Error("invalid_token"); e.status = 401; throw e;
  }
}

/** The default Storage bucket. firebaseAdmin.js sets storageBucket in its
 *  initializeApp (shopifyEditor.js relies on the same no-argument call);
 *  FIREBASE_STORAGE_BUCKET is an existing variable and only a fallback. */
function studioBucket() {
  return process.env.FIREBASE_STORAGE_BUCKET
    ? admin.storage().bucket(process.env.FIREBASE_STORAGE_BUCKET)
    : admin.storage().bucket();
}

/** A permanent, CORS-free download URL via Firebase's own download token —
 *  the same shape the Firebase SDKs mint. Chosen over getSignedUrl() because
 *  a signed URL expires in at most 7 days and these URLs are written onto
 *  cart line items and orders, which must stay viewable for good. */
function downloadUrl(bucketName, objectPath, token) {
  return "https://firebasestorage.googleapis.com/v0/b/" + bucketName +
         "/o/" + encodeURIComponent(objectPath) +
         "?alt=media&token=" + token;
}

/** users/{uid}.wallet, provisioned on first contact with the guest allowance
 *  so custom_charm_generate has something real to debit. Idempotent. */
async function ensureWallet(uid, cfg) {
  const ref = db.doc(`users/${uid}`);
  let wallet = null;
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const d = snap.exists ? snap.data() : {};
    if (d.wallet && typeof d.wallet.credits === "number") { wallet = d.wallet; return; }
    wallet = {
      credits: cfg.guestFreeCredits,
      uploadsUsed: 0,
      purchasedAllowance: 0,
      tier: "none",
      lifetimeSpent: 0,
      provisionedAt: Date.now(),
    };
    tx.set(ref, { wallet, updatedAt: Date.now() }, { merge: true });
    tx.create(ref.collection("walletLedger").doc(), {
      delta: cfg.guestFreeCredits, reason: "guest_grant",
      at: admin.firestore.FieldValue.serverTimestamp(),
    });
  });
  return wallet;
}

/** How many uploads this uid is entitled to in total. */
function uploadAllowance(wallet, cfg, signedIn) {
  /* An unlimited account is unlimited in every meter, not just credits —
     otherwise testing stops at the upload cap instead of the credit one.
     The flag lives on users/{uid}.wallet, set from the Firebase console;
     see the note beside studioDebit in geminiImageProxy-background.js. */
  if (wallet && wallet.unlimited === true) return Infinity;
  return cfg.guestFreeUploads
       + (signedIn ? cfg.signupBonusUploads : 0)
       + (Number(wallet.purchasedAllowance) || 0);
}

/** Hourly per-uid ceiling on top of the wallet, so a stolen ID token cannot
 *  burn the allowance in one go. Same shape as checkIpBudget above. */
async function checkUidBudget(uid, key, max) {
  const ref = db.doc(`${RATE_COL}/${key}_${crypto.createHash("sha256").update(uid).digest("hex").slice(0, 32)}`);
  const now = Date.now();
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const d = snap.exists ? snap.data() : {};
    const windowStart = d.windowStart && now - d.windowStart < 3600e3 ? d.windowStart : now;
    const count = windowStart === d.windowStart ? (d.count || 0) : 0;
    if (count >= max) return false;
    tx.set(ref, { windowStart, count: count + 1, updatedAt: now }, { merge: true });
    return true;
  });
}

const slugify = (name) => String(name || "image")
  .toLowerCase().replace(/\.[a-z0-9]+$/, "").replace(/[^a-z0-9]+/g, "-")
  .replace(/^-+|-+$/g, "").slice(0, 40) || "image";

/* ─────────────────────────── kind: upload_ref ────────────────────────────
 * The customer's reference image, already downscaled to ≤2048px in the
 * browser, arrives as a data URL and is written to
 *     custom-studio/{uid}/uploads/{ts}-{slug}.{jpg|png}
 * The upload ALLOWANCE is checked here but NOT consumed: it is metered in
 * custom_charm_precheck, and only when the image is accepted, so a rejected
 * photo costs the customer nothing (§2, §10.6).
 * ===================================================================== */
async function uploadRef(body, event) {
  const uid = await requireStudioUser(event);
  const cfg = await config();

  const m = /^data:([a-z/+.-]+);base64,([A-Za-z0-9+/=\s]+)$/i.exec(String(body.dataUrl || ""));
  if (!m) return json(400, { ok: false, error: "bad_image" });
  const mime = m[1].toLowerCase();
  const ext = UPLOAD_MIME[mime];
  if (!ext) return json(415, { ok: false, error: "unsupported_type" });   // JPG/PNG only, no HEIC

  const buf = Buffer.from(m[2].replace(/\s+/g, ""), "base64");
  if (!buf.length) return json(400, { ok: false, error: "empty_image" });
  if (buf.length > cfg.maxUploadBytes) return json(413, { ok: false, error: "too_large" });

  if (!(await checkUidBudget(uid, "up", MAX_UPLOADS_HOUR))) {
    return json(429, { ok: false, error: "rate_limited" });
  }

  const user = await admin.auth().getUser(uid).catch(() => null);
  const signedIn = !!(user && !!user.email);
  const wallet = await ensureWallet(uid, cfg);
  if ((Number(wallet.uploadsUsed) || 0) >= uploadAllowance(wallet, cfg, signedIn)) {
    return json(402, { ok: false, error: "no_uploads_left" });
  }

  const sessionId = String(body.sessionId || "").replace(/[^A-Za-z0-9_-]/g, "").slice(0, 60) || "loose";
  const objectPath = `custom-studio/${uid}/uploads/${Date.now()}-${slugify(body.filename)}.${ext}`;
  const token = crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString("hex");
  const bucket = studioBucket();
  await bucket.file(objectPath).save(buf, {
    contentType: mime,
    resumable: false,
    metadata: { metadata: {
      firebaseStorageDownloadTokens: token,
      uid, sessionId,
      originalName: String(body.filename || "").slice(0, 120),
    } },
  });

  return json(200, {
    ok: true,
    path: objectPath,
    url: downloadUrl(bucket.name, objectPath, token),
    bytes: buf.length,
  });
}

/* ─────────────────────── kind: upload_asset ──────────────────────────────
 * A picture the customer drops INTO a drawing — a photo to trace, a logo, a
 * texture — rather than the one reference the whole design is based on.
 *
 * WHY IT IS NOT upload_ref
 *   The reference allowance exists because every reference runs a vision
 *   pre-check and anchors a generation. A picture pasted onto a sketch does
 *   neither: it is pixels on a canvas. Charging the reference allowance for
 *   it would mean a customer who drops four photos into one sketch has spent
 *   their whole design budget before describing anything — so this has its
 *   own, far larger, per-account ceiling and touches uploadsUsed not at all.
 *   The hourly per-uid budget still applies, so a stolen token cannot mine
 *   the bucket, and the same MIME and size caps still hold.
 *
 * Written to custom-studio/{uid}/assets/... — the same uid prefix every other
 * studio object uses, so asset_fetch's ownership check is the same one line.
 * ===================================================================== */
const MAX_ASSETS_ACCOUNT = 400;              // a working library, not a host

async function uploadAsset(body, event) {
  const uid = await requireStudioUser(event);
  const cfg = await config();

  const m = /^data:([a-z/+.-]+);base64,([A-Za-z0-9+/=\s]+)$/i.exec(String(body.dataUrl || ""));
  if (!m) return json(400, { ok: false, error: "bad_image" });
  const mime = m[1].toLowerCase();
  const ext = UPLOAD_MIME[mime];
  if (!ext) return json(415, { ok: false, error: "unsupported_type" });

  const buf = Buffer.from(m[2].replace(/\s+/g, ""), "base64");
  if (!buf.length) return json(400, { ok: false, error: "empty_image" });
  if (buf.length > cfg.maxUploadBytes) return json(413, { ok: false, error: "too_large" });

  if (!(await checkUidBudget(uid, "as", MAX_UPLOADS_HOUR))) {
    return json(429, { ok: false, error: "rate_limited" });
  }
  /* the account ceiling, counted where it is cheap to count: one field on the
     wallet doc, incremented transactionally, never a bucket listing */
  const wallet = await ensureWallet(uid, cfg);
  if (wallet.unlimited !== true && (Number(wallet.assetsUsed) || 0) >= MAX_ASSETS_ACCOUNT) {
    return json(402, { ok: false, error: "asset_library_full" });
  }

  const sessionId = String(body.sessionId || "").replace(/[^A-Za-z0-9_-]/g, "").slice(0, 60) || "loose";
  const objectPath = `custom-studio/${uid}/assets/${Date.now()}-${slugify(body.filename)}.${ext}`;
  const token = crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString("hex");
  const bucket = studioBucket();
  await bucket.file(objectPath).save(buf, {
    contentType: mime,
    resumable: false,
    metadata: { metadata: {
      firebaseStorageDownloadTokens: token,
      uid, sessionId, assetKind: "drawing",
      originalName: String(body.filename || "").slice(0, 120),
    } },
  });
  try {
    await db.doc(`users/${uid}`).set({
      wallet: { assetsUsed: admin.firestore.FieldValue.increment(1) },
    }, { merge: true });
  } catch (_e) { /* the object is written; the counter is best-effort */ }

  return json(200, {
    ok: true,
    path: objectPath,
    url: downloadUrl(bucket.name, objectPath, token),
    bytes: buf.length,
  });
}

/* ─────────────────────── kind: asset_fetch ───────────────────────────────
 * Hand a stored object back as a data URL, for the ONE thing a plain <img>
 * cannot do: be drawn into a canvas that is still exportable.
 *
 * Firebase Storage download URLs serve happily to an <img>, so every
 * thumbnail, preview and lightbox in the studio uses the URL directly and
 * never comes near this endpoint. But a canvas that has drawn a cross-origin
 * image without CORS headers is TAINTED — toDataURL throws — and the whole
 * sketchpad depends on exporting its canvas. Rather than make the bucket's
 * CORS configuration a prerequisite for the feature working, the bytes come
 * back through here, same-origin by definition.
 *
 * The client tries an anonymous CORS load first and only falls back to this,
 * so the day the bucket does carry a CORS rule this endpoint goes quiet on
 * its own. Results are cached in the page, so it is once per image per load.
 * ===================================================================== */
async function assetFetch(body, event) {
  const uid = await requireStudioUser(event);
  const path = String(body.path || "");
  /* ownership is the whole check: every studio object lives under the uid
     that owns it, and nothing else in the bucket is reachable from here */
  if (!path || path.indexOf(`custom-studio/${uid}/`) !== 0 || path.indexOf("..") >= 0) {
    return json(403, { ok: false, error: "not_yours" });
  }
  if (!(await checkUidBudget(uid, "af", 600))) {
    return json(429, { ok: false, error: "rate_limited" });
  }
  const bucket = studioBucket();
  const file = bucket.file(path);
  let meta;
  try { [meta] = await file.getMetadata(); }
  catch { return json(404, { ok: false, error: "not_found" }); }
  const size = Number(meta.size) || 0;
  if (size > 12 * 1024 * 1024) return json(413, { ok: false, error: "too_large" });
  const [buf] = await file.download();
  const mime = meta.contentType || "image/png";
  return json(200, {
    ok: true,
    dataUrl: `data:${mime};base64,${buf.toString("base64")}`,
    bytes: buf.length,
  });
}

/* ───────────────────── kind: asset_delete ────────────────────────────────
 * "Delete for good" has to mean it. The record goes from the session
 * document, which the client owns and writes itself; the stored OBJECT can
 * only go from here, because the storefront has no Storage credentials and
 * is never going to be given any.
 *
 * Same one-line ownership check as asset_fetch: every studio object lives
 * under the uid that owns it, so a path that does not start with this
 * caller's own prefix is somebody else's and is refused. An object that has
 * already gone is a success, not an error — deleting twice should not fail.
 * ===================================================================== */
async function assetDelete(body, event) {
  const uid = await requireStudioUser(event);
  const path = String(body.path || "");
  if (!path || path.indexOf(`custom-studio/${uid}/`) !== 0 || path.indexOf("..") >= 0) {
    return json(403, { ok: false, error: "not_yours" });
  }
  if (!(await checkUidBudget(uid, "ad", 400))) {
    return json(429, { ok: false, error: "rate_limited" });
  }
  try {
    await studioBucket().file(path).delete();
    return json(200, { ok: true, deleted: true, path });
  } catch (e) {
    const code = e && (e.code || e.statusCode);
    if (code === 404) return json(200, { ok: true, deleted: false, path, note: "already gone" });
    console.warn("[britesAuth] asset_delete failed:", (e && e.message) || e);
    return json(500, { ok: false, error: "delete_failed" });
  }
}

/* ───────────────────── kind: wallet_get / wallet_grant_signup ────────────
 * The client's credit meter is cosmetic; these two exist so the storefront
 * can mirror the truth and so the signup bonus is granted server-side.
 * ===================================================================== */
async function walletGet(body, event) {
  const uid = await requireStudioUser(event);
  const cfg = await config();
  const wallet = await ensureWallet(uid, cfg);
  const user = await admin.auth().getUser(uid).catch(() => null);
  const signedIn = !!(user && !!user.email);
  return json(200, {
    ok: true,
    uid,
    wallet,
    allowance: {
      uploads: uploadAllowance(wallet, cfg, signedIn),
      uploadsLeft: Math.max(0, uploadAllowance(wallet, cfg, signedIn) - (wallet.uploadsUsed || 0)),
    },
    limits: {
      guestFreeCredits: cfg.guestFreeCredits,
      signupBonusCredits: cfg.signupBonusCredits,
      guestFreeUploads: cfg.guestFreeUploads,
      signupBonusUploads: cfg.signupBonusUploads,
      maxUploadPx: cfg.maxUploadPx,
      maxUploadBytes: cfg.maxUploadBytes,
    },
  });
}

/** Idempotent: the ledger entry with reason "signup" IS the claim, so calling
 *  this twice grants nothing further (acceptance test 3). */
async function walletGrantSignup(body, event) {
  const uid = await requireStudioUser(event);
  const cfg = await config();
  await ensureWallet(uid, cfg);

  const user = await admin.auth().getUser(uid).catch(() => null);
  if (!user || !user.email) return json(400, { ok: false, error: "not_signed_in" });

  const ref = db.doc(`users/${uid}`);
  const claim = ref.collection("walletLedger").doc("signup_bonus");
  let granted = false, wallet = null;
  await db.runTransaction(async (tx) => {
    const [uSnap, cSnap] = await Promise.all([tx.get(ref), tx.get(claim)]);
    const d = uSnap.exists ? uSnap.data() : {};
    wallet = d.wallet || {};
    if (cSnap.exists) return;                            // already granted
    wallet = {
      ...wallet,
      credits: (Number(wallet.credits) || 0) + cfg.signupBonusCredits,
      purchasedAllowance: (Number(wallet.purchasedAllowance) || 0),
      uploadsUsed: Number(wallet.uploadsUsed) || 0,
      tier: wallet.tier || "none",
    };
    tx.set(ref, { wallet, updatedAt: Date.now() }, { merge: true });
    tx.set(claim, {
      delta: cfg.signupBonusCredits, reason: "signup",
      uploadsBonus: cfg.signupBonusUploads,
      at: admin.firestore.FieldValue.serverTimestamp(),
    });
    granted = true;
  });
  return json(200, { ok: true, granted, wallet });
}

/* ───────────────────────── grantStudioCredits ────────────────────────────
 * Called by shopifyOrderWebhook.js on orders/paid. The Shopify order id is
 * the idempotency key — a replayed webhook grants nothing further — and the
 * whole grant is one transaction, exactly like recordRefund's claim-doc
 * pattern in googleAdsAutopilot.js.
 *
 *   grantStudioCredits({ orderId, uid, packs: [{ sku, credits }] })
 * ===================================================================== */
async function grantStudioCredits({ orderId, uid, packs } = {}) {
  if (!uid || !orderId || !Array.isArray(packs) || !packs.length) {
    return { ok: false, reason: "nothing_to_grant" };
  }
  const total = packs.reduce((n, p) => n + (Number(p.credits) || 0), 0);
  if (total <= 0) return { ok: false, reason: "zero_credits" };

  const claimId = String(orderId).replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 180);
  const claim = db.doc(`${GRANT_COL}/${claimId}`);
  const ref = db.doc(`users/${uid}`);
  let duplicate = false, credits = 0;

  await db.runTransaction(async (tx) => {
    const [cSnap, uSnap] = await Promise.all([tx.get(claim), tx.get(ref)]);
    if (cSnap.exists && cSnap.data().status === "complete") { duplicate = true; return; }
    const d = uSnap.exists ? uSnap.data() : {};
    const w = d.wallet || {};
    credits = (Number(w.credits) || 0) + total;
    tx.set(ref, {
      wallet: {
        ...w,
        credits,
        /* purchased credits extend the upload allowance too (§6) */
        purchasedAllowance: (Number(w.purchasedAllowance) || 0) + total,
        lifetimeSpent: (Number(w.lifetimeSpent) || 0),
        uploadsUsed: Number(w.uploadsUsed) || 0,
        tier: w.tier || "pack",
      },
      updatedAt: Date.now(),
    }, { merge: true });
    tx.set(ref.collection("walletLedger").doc(`order_${claimId}`), {
      delta: total, reason: "purchase", ref: String(orderId),
      skus: packs.map((p) => p.sku),
      at: admin.firestore.FieldValue.serverTimestamp(),
    });
    tx.set(claim, {
      orderId: String(orderId), uid, credits: total, status: "complete",
      at: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
  });

  if (duplicate) return { ok: true, duplicate: true, orderId: String(orderId) };
  return { ok: true, granted: total, credits, uid, orderId: String(orderId) };
}

/** Mark a studio session as ordered and hand the production queue the
 *  full-resolution artwork path. Also called from shopifyOrderWebhook.js. */
async function markSessionOrdered({ sessionId, orderId, designPath } = {}) {
  if (!sessionId) return { ok: false, reason: "no_session" };
  try {
    await db.collection(STUDIO_SESSIONS).doc(String(sessionId)).set({
      status: "ordered",
      orderId: orderId ? String(orderId) : null,
      designPath: designPath || null,
      orderedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: Date.now(),
    }, { merge: true });
    return { ok: true };
  } catch (e) {
    console.warn("[britesAuth] markSessionOrdered:", e.message);
    return { ok: false, error: e.message };
  }
}

/* ═════════════════════════════ rate limiting ═════════════════════════ */

async function checkIpBudget(ip) {
  const ref = db.doc(`${RATE_COL}/ip_${crypto.createHash("sha256").update(ip).digest("hex").slice(0, 32)}`);
  const now = Date.now();
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const d = snap.exists ? snap.data() : {};
    const windowStart = d.windowStart && now - d.windowStart < 3600e3 ? d.windowStart : now;
    const count = windowStart === d.windowStart ? (d.count || 0) : 0;
    if (count >= MAX_IP_SENDS_HOUR) return false;
    tx.set(ref, { windowStart, count: count + 1, updatedAt: now }, { merge: true });
    return true;
  });
}

/* ═════════════════════════════ handlers ══════════════════════════════ */

async function sendCode(body, event) {
  const email = normalizeEmail(body.email);
  const ip = clientIp(event);

  // Enumeration-safe: identical response whether or not the address exists.
  const ok = { ok: true, sent: true, cooldownMs: RESEND_COOLDOWN, expiresInMs: CODE_TTL_MS };

  if (!validEmail(email)) return json(400, { ok: false, error: "invalid_email" });
  if (!(await checkIpBudget(ip))) return json(429, { ok: false, error: "rate_limited" });

  const ref = db.doc(`${VERIFY_COL}/${addrKey(email)}`);
  const now = Date.now();

  const decision = await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const d = snap.exists ? snap.data() : {};

    if (d.lastSentAt && now - d.lastSentAt < RESEND_COOLDOWN) {
      return { allow: false, status: 429, error: "cooldown",
               retryInMs: RESEND_COOLDOWN - (now - d.lastSentAt) };
    }
    const windowStart = d.sendWindowStart && now - d.sendWindowStart < 3600e3 ? d.sendWindowStart : now;
    const sends = windowStart === d.sendWindowStart ? (d.sendCount || 0) : 0;
    if (sends >= MAX_SENDS_HOUR) {
      return { allow: false, status: 429, error: "too_many_requests",
               retryInMs: Math.max(0, 3600e3 - (now - windowStart)) };
    }

    const code = generateCode();
    tx.set(ref, {
      emailHash: crypto.createHash("sha256").update(email).digest("hex"),
      codeHash: hashCode(code, email),
      expiresAt: now + CODE_TTL_MS,
      attempts: 0,
      consumed: false,
      lastSentAt: now,
      sendWindowStart: windowStart,
      sendCount: sends + 1,
      linkUid: body.uid || null,          // anonymous uid to upgrade in place
      createdAt: d.createdAt || now,
      updatedAt: now,
    }, { merge: true });
    return { allow: true, code, sendCount: sends + 1 };
  });

  if (!decision.allow) {
    return json(decision.status, { ok: false, error: decision.error, retryInMs: decision.retryInMs });
  }

  const { html, text } = buildVerificationEmail({
    code: decision.code,
    expiresMin: Math.round(CODE_TTL_MS / 60000),
    requestedFrom: body.requestedFrom || null,
  });

  try {
    await deliverEmail({
      to: email,
      subject: `${decision.code} is your Brites verification code`,
      html, text,
    });
  } catch (e) {
    /* The cooldown and the send counter are claimed in the transaction ABOVE,
       before the email is attempted — they have to be, or two fast clicks
       would send two codes. But when the send then fails, that reservation
       punishes the shopper for our failure: they are locked out for a full
       minute and the retry they were just told to make is refused. So give it
       back. The code itself stays valid, so an email that turns out to have
       been delivered after all still works. */
    await ref.set({ lastSentAt: 0, sendCount: Math.max(0, (decision.sendCount || 1) - 1) },
                  { merge: true }).catch(() => {});
    throw e;
  }

  return json(200, ok);
}

async function verifyCode(body) {
  const email = normalizeEmail(body.email);
  const code = String(body.code || "").replace(/\D/g, "");
  if (!validEmail(email) || code.length !== CODE_LENGTH) {
    return json(400, { ok: false, error: "invalid_input" });
  }

  const ref = db.doc(`${VERIFY_COL}/${addrKey(email)}`);
  const now = Date.now();

  const result = await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    if (!snap.exists) return { ok: false, status: 400, error: "no_code" };
    const d = snap.data();

    if (d.consumed)             return { ok: false, status: 400, error: "code_used" };
    if (now > (d.expiresAt || 0)) return { ok: false, status: 400, error: "code_expired" };
    if ((d.attempts || 0) >= MAX_ATTEMPTS) {
      tx.set(ref, { consumed: true, updatedAt: now }, { merge: true });
      return { ok: false, status: 429, error: "too_many_attempts" };
    }

    if (!timingSafeEqual(hashCode(code, email), d.codeHash)) {
      const attempts = (d.attempts || 0) + 1;
      tx.set(ref, { attempts, updatedAt: now }, { merge: true });
      return { ok: false, status: 400, error: "code_incorrect",
               attemptsLeft: Math.max(0, MAX_ATTEMPTS - attempts) };
    }

    tx.set(ref, { consumed: true, verifiedAt: now, updatedAt: now }, { merge: true });
    return { ok: true, linkUid: d.linkUid || null };
  });

  if (!result.ok) return json(result.status, result);

  // ── upgrade the anonymous account in place, or create/find the user ──
  let user = null;
  if (result.linkUid) {
    try {
      user = await admin.auth().updateUser(result.linkUid, { email, emailVerified: true });
    } catch (e) {
      if (e.code !== "auth/user-not-found" && e.code !== "auth/email-already-exists") throw e;
    }
  }
  if (!user) {
    try {
      const existing = await admin.auth().getUserByEmail(email);
      user = await admin.auth().updateUser(existing.uid, { emailVerified: true });
    } catch (e) {
      if (e.code !== "auth/user-not-found") throw e;
      user = await admin.auth().createUser({ email, emailVerified: true });
    }
  }

  await db.doc(`users/${user.uid}`).set({
    profile: { email, emailVerified: true, provider: "email", updatedAt: now },
  }, { merge: true });
  await registerShopifyCustomer(email, user.uid);   // best effort, never blocks

  const customToken = await admin.auth().createCustomToken(user.uid, { emailVerified: true });
  return json(200, { ok: true, uid: user.uid, customToken });
}

/* Mirror the verified account into Shopify so studio customers exist in the
   same place as newsletter captures. Uses the SAME Shopify credentials as
   emailCapture.js; silently skipped when they aren't configured. */
async function registerShopifyCustomer(email, uid) {
  const store = process.env.SHOPIFY_STORE;
  const id = process.env.SHOPIFY_CLIENT_ID, secret = process.env.SHOPIFY_CLIENT_SECRET;
  if (!store || !id || !secret) return;
  try {
    const tokRes = await fetchFn(`https://${store}/admin/oauth/access_token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({ grant_type: "client_credentials", client_id: id, client_secret: secret }),
    });
    if (!tokRes.ok) return;
    const { access_token } = await tokRes.json();
    const api = process.env.SHOPIFY_API_VERSION || "2025-10";
    await fetchFn(`https://${store}/admin/api/${api}/graphql.json`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Shopify-Access-Token": access_token },
      body: JSON.stringify({
        query: `mutation($input: CustomerInput!) { customerCreate(input: $input) {
                  customer { id } userErrors { message } } }`,
        variables: { input: { email, tags: ["custom-charm-studio", `studio-uid:${uid}`] } },
      }),
    });
  } catch (e) {
    console.warn("[britesAuth] shopify customer sync skipped:", e.message);
  }
}

/* Server-side Google path. The browser normally does this itself via
   signInWithCredential(); this exists for headless/ITP-restricted contexts. */
async function googleVerify(body) {
  const credential = String(body.credential || "");
  if (!credential) return json(400, { ok: false, error: "missing_credential" });

  const res = await fetchFn(
    `https://oauth2.googleapis.com/tokeninfo?id_token=${encodeURIComponent(credential)}`
  );
  if (!res.ok) return json(401, { ok: false, error: "invalid_credential" });
  const p = await res.json();

  const cfg = await config();
  const expectedAud = cfg.googleClientId || process.env.GMAIL_CLIENT_ID;   // no new var
  if (!expectedAud) return json(500, { ok: false, error: "google_client_not_configured" });
  if (p.aud !== expectedAud) return json(401, { ok: false, error: "aud_mismatch" });
  if (!["accounts.google.com", "https://accounts.google.com"].includes(p.iss)) {
    return json(401, { ok: false, error: "iss_mismatch" });
  }
  if (Number(p.exp) * 1000 < Date.now()) return json(401, { ok: false, error: "credential_expired" });
  if (p.email_verified !== "true" && p.email_verified !== true) {
    return json(401, { ok: false, error: "google_email_unverified" });
  }

  const email = normalizeEmail(p.email);
  let user;
  try {
    user = await admin.auth().getUserByEmail(email);
  } catch {
    user = await admin.auth().createUser({
      email, emailVerified: true, displayName: p.name || undefined, photoURL: p.picture || undefined,
    });
  }

  // Link the guest session's uid so designs and credits carry across.
  const now = Date.now();
  await db.doc(`users/${user.uid}`).set({
    profile: { email, emailVerified: true, provider: "google",
               name: p.name || null, photoURL: p.picture || null, updatedAt: now },
  }, { merge: true });
  if (body.uid && body.uid !== user.uid) {
    await db.doc(`users/${user.uid}`).set({ mergedFrom: admin.firestore.FieldValue.arrayUnion(body.uid) }, { merge: true });
  }
  await registerShopifyCustomer(email, user.uid);

  const customToken = await admin.auth().createCustomToken(user.uid, { emailVerified: true });
  return json(200, { ok: true, uid: user.uid, customToken, email, name: p.name || null, picture: p.picture || null });
}

/* ═════════ GUEST MERGE — when the Google account already exists ══════════
 * linkWithPopup keeps the uid, so most sign-ups move nothing. But a shopper
 * whose Google account is ALREADY in Firebase — anyone signing in for the
 * second time from a new device or a cleared browser — cannot link:
 * signInWithCredential switches their browser onto the existing uid, and
 * every session they made as a guest is still filed under the abandoned
 * anonymous one. From that moment the ownership checks fire against the
 * shopper themselves: generation 403s, saves are refused, the studio looks
 * broken precisely because the security is working.
 *
 * This kind re-files those sessions. The security question is proof: "merge
 * uid X into me" must not be takeable at the caller's word, or any guest's
 * designs could be stolen by guessing uids. The proof demanded here is the
 * GUEST'S OWN ID TOKEN, minted by the browser before it switched accounts —
 * only the holder of that guest session could ever have had one. And only
 * ANONYMOUS sessions can be absorbed: merging a real account into another
 * would make a stolen token an account-takeover, so it is refused outright.
 *
 * The guest's WALLET deliberately does not move. Free guest credits are
 * per-guest by design; letting them ride a merge would make sign-out → new
 * guest → merge an infinite free-credit pump. Purchased credits are granted
 * by the order webhook against the uid that paid, which for a shopper who
 * links normally is the uid they keep.
 * ===================================================================== */
async function mergeGuest(body, event) {
  const newUid = await requireStudioUser(event);
  const guestToken = String(body?.guestToken || "");
  if (!guestToken) return json(400, { ok: false, error: "missing_guest_token" });

  let decoded;
  try { decoded = await admin.auth().verifyIdToken(guestToken); }
  catch { return json(401, { ok: false, error: "invalid_guest_token" }); }

  const guestUid = decoded.uid;
  if (guestUid === newUid) return json(200, { ok: true, moved: 0, note: "same_uid" });
  const signInProvider = decoded.firebase && decoded.firebase.sign_in_provider;
  if (signInProvider !== "anonymous") {
    return json(403, { ok: false, error: "not_a_guest_session" });
  }

  const snap = await db.collection(STUDIO_SESSIONS).where("uid", "==", guestUid).get();
  const docs = snap.docs || [];
  let moved = 0;
  for (let i = 0; i < docs.length; i += 400) {          // Firestore batch cap is 500
    const batch = db.batch();
    docs.slice(i, i + 400).forEach((d) => {
      batch.update(d.ref, { uid: newUid, mergedFromUid: guestUid });
    });
    await batch.commit();
    moved += Math.min(400, docs.length - i);
  }

  await db.doc(`users/${newUid}`).set(
    { mergedFrom: admin.firestore.FieldValue.arrayUnion(guestUid) },
    { merge: true }
  ).catch(() => {});

  return json(200, { ok: true, moved, guestUid });
}

/* ═════════ CROSS-BROWSER HANDOFF — the in-app browser escape ═════════════
 * Google refuses OAuth inside an app's embedded browser (403
 * disallowed_useragent), so the studio sends the shopper out to Safari or
 * Chrome. That is a DIFFERENT browser with different storage: the anonymous
 * Firebase session — and therefore every design, every credit, hanging off
 * that uid — does not travel with them. These two kinds carry it.
 *
 *   mint  — authenticated. Stores a high-entropy code against the caller's own
 *           uid. Five-minute life, single use.
 *   claim — deliberately UNauthenticated, because the browser doing the
 *           claiming has no session yet; that is the whole point. Presenting
 *           the code IS the proof, so the code has to be unguessable and
 *           short-lived, and it is: 32 random bytes, five minutes, deleted on
 *           first use.
 *
 * The code rides in the URL FRAGMENT, which browsers never transmit to a
 * server, so it does not reach Shopify's logs or any CDN on the way. Only its
 * SHA-256 is stored, so a Firestore read cannot replay a live handoff — the
 * same rule this file already applies to verification codes.
 * ===================================================================== */
const HANDOFF_COL    = "Brites_Studio_Handoffs";
const HANDOFF_TTL_MS = 5 * 60 * 1000;
const HANDOFF_MAX_PER_HOUR = 20;                 // per uid; a human needs one

const handoffKey = (code) =>
  crypto.createHash("sha256").update("handoff:" + String(code)).digest("hex");

async function handoffMint(event) {
  const uid = await requireStudioUser(event);
  /* A human escapes an in-app browser once, maybe twice. A ceiling here stops
     a stolen ID token from farming custom tokens for the same uid. */
  if (!(await checkUidBudget(uid, "handoff", HANDOFF_MAX_PER_HOUR))) {
    return json(429, { ok: false, error: "too_many_requests" });
  }
  const code = crypto.randomBytes(32).toString("base64url");
  const now = Date.now();
  await db.doc(`${HANDOFF_COL}/${handoffKey(code)}`).set({
    uid, createdAt: now, expiresAt: now + HANDOFF_TTL_MS, used: false,
  });
  return json(200, { ok: true, code, expiresInMs: HANDOFF_TTL_MS });
}

async function handoffClaim(body) {
  const code = String((body && body.code) || "");
  /* base64url of 32 bytes is 43 characters. Anything else is not ours, and
     rejecting it here keeps malformed input away from a document path. */
  if (!/^[A-Za-z0-9_-]{20,64}$/.test(code)) return json(400, { ok: false, error: "bad_code" });
  const ref = db.doc(`${HANDOFF_COL}/${handoffKey(code)}`);

  /* A transaction, not a read-then-write: two tabs opened from the same copied
     link must not both succeed, or a stale one could reclaim the uid later. */
  const uid = await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    if (!snap.exists) return null;
    const d = snap.data() || {};
    if (d.used || !d.expiresAt || d.expiresAt < Date.now()) return null;
    tx.set(ref, { used: true, usedAt: Date.now() }, { merge: true });
    return d.uid || null;
  });
  if (!uid) return json(410, { ok: false, error: "handoff_expired" });

  const customToken = await admin.auth().createCustomToken(uid, { handoff: true });
  /* Best-effort tidy-up: the doc has served its purpose and holds nothing but
     a hash, but there is no reason to keep it. */
  ref.delete().catch(() => {});
  return json(200, { ok: true, uid, token: customToken });
}

/* ═══════════════════════════════ router ══════════════════════════════ */

exports.handler = async (event) => {
  _origin = event.headers.origin || event.headers.Origin || "";
  try {
    const cfg = await config();
    FROM_NAME = cfg.fromName; FROM_EMAIL = cfg.fromEmail; SITE_URL = cfg.siteUrl;
    if (Array.isArray(cfg.allowedOrigins) && cfg.allowedOrigins.length) ALLOWED_ORIGINS = cfg.allowedOrigins;
  } catch { /* defaults already in place */ }
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: corsHeaders(_origin), body: "" };
  if (event.httpMethod !== "POST") return json(405, { ok: false, error: "method_not_allowed" });

  let body;
  try { body = JSON.parse(event.body || "{}"); }
  catch { return json(400, { ok: false, error: "bad_json" }); }

  try {
    switch (body.kind) {
      case "send_code":     return await sendCode(body, event);
      case "verify_code":   return await verifyCode(body);
      case "google_verify": return await googleVerify(body);
      /* ── Custom Charm Studio (v1.1) ── */
      case "upload_ref":          return await uploadRef(body, event);
      case "upload_asset":        return await uploadAsset(body, event);
      case "asset_fetch":         return await assetFetch(body, event);
      case "asset_delete":        return await assetDelete(body, event);
      case "wallet_get":          return await walletGet(body, event);
      case "wallet_grant_signup": return await walletGrantSignup(body, event);
      case "merge_guest":         return await mergeGuest(body, event);
      case "handoff_mint":        return await handoffMint(event);
      case "handoff_claim":       return await handoffClaim(body);
      default:              return json(400, { ok: false, error: "unknown_kind" });
    }
  } catch (err) {
    /* requireStudioUser throws 401s — surface them rather than masking the
       reason the storefront needs in order to re-authenticate */
    if (err && err.status) {
      if (err.detail) console.error("[britesAuth]", body && body.kind, err.message, "—", err.detail);
      /* The email_* classes carry the provider's own rejection text. Returning
         it is safe — it is Google's error message, no secrets in it — and it
         means a failing send can be diagnosed from the storefront's network
         tab instead of the Netlify log viewer. Other classes stay opaque. */
      const expose = /^email_/.test(err.message) ? String(err.detail || "").slice(0, 400) : undefined;
      return json(err.status, { ok: false, error: err.message, detail: expose });
    }
    console.error("[britesAuth]", body && body.kind, err);
    return json(500, { ok: false, error: "server_error" });
  }
};

/* Exported for shopifyOrderWebhook.js — see §10.8. Requiring this module does
   not create a second lambda; Netlify bundles it once. */
exports.grantStudioCredits = grantStudioCredits;
exports.markSessionOrdered = markSessionOrdered;
