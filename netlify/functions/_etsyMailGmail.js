/*  netlify/functions/_etsyMailGmail.js
 *
 *  Shared Gmail API helpers for the EtsyMail system. Mirrors the shape of
 *  _etsyMailEtsy.js so the codebase stays consistent — same OAuth refresh
 *  pattern, same Firestore-backed token storage, same fetch wrapper style.
 *
 *  ═══ WHAT THIS DOES ════════════════════════════════════════════════════
 *
 *  This module is the only place in the EtsyMail backend that knows how
 *  to talk to Gmail. Other functions consume:
 *
 *    getValidGmailAccessToken()              → Bearer access token (auto-refresh)
 *    gmailFetch(path, opts)                  → authenticated fetch wrapper
 *    listMessages({ q, pageToken })          → users.messages.list
 *    getMessage(id, { format })              → users.messages.get (full by default)
 *    extractEmailBodyText(message)           → flatten payload to plain+html text
 *    extractEtsyConversationLink(message)    → → { conversationId, conversationUrl } | null
 *    extractHeaderValue(headers, name)       → small lookup helper
 *
 *  ═══ ENV VARS ══════════════════════════════════════════════════════════
 *
 *    GMAIL_CLIENT_ID     — OAuth 2.0 client id  (Google Cloud Console)
 *    GMAIL_CLIENT_SECRET — OAuth 2.0 client secret
 *
 *  Tokens are NOT env vars — they live in Firestore at config/gmailOauth
 *  (2-segment path) and rotate on every refresh, identical pattern to the
 *  Etsy OAuth flow at config/etsyOauth.
 *
 *  ═══ INITIAL SEEDING ═══════════════════════════════════════════════════
 *
 *  Before this module works, an operator must run the OAuth dance once
 *  (e.g. via the Google OAuth Playground or a one-off script) to obtain
 *  a refresh_token, then POST it to etsyMailGmailSeedTokens. After that,
 *  this module auto-refreshes the access_token forever.
 *
 *  ═══ SCOPES ════════════════════════════════════════════════════════════
 *
 *  Read-only is sufficient. The pipeline only LISTS and READS messages —
 *  it never marks them, modifies labels, or sends. Required scope:
 *
 *    https://www.googleapis.com/auth/gmail.readonly
 */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const GMAIL_CLIENT_ID     = process.env.GMAIL_CLIENT_ID;
const GMAIL_CLIENT_SECRET = process.env.GMAIL_CLIENT_SECRET;

const OAUTH_DOC_PATH         = "config/gmailOauth";
const TOKEN_REFRESH_BUFFER_MS = 2 * 60 * 1000;          // refresh if <2min to expiry
const GMAIL_API_BASE          = "https://gmail.googleapis.com/gmail/v1/users/me";
const GOOGLE_TOKEN_ENDPOINT   = "https://oauth2.googleapis.com/token";

// ─── OAuth token management ──────────────────────────────────────────────
// Pattern matches _etsyMailEtsy.js exactly: read current token, refresh if
// stale, persist the new pair (Google does NOT rotate refresh_token on
// refresh, so the refresh_token field on the doc generally never changes
// after seeding — but we still write it in case Google ever does rotate).

async function refreshGmailToken(oldRefreshToken) {
  if (!GMAIL_CLIENT_ID)     throw new Error("GMAIL_CLIENT_ID env var missing");
  if (!GMAIL_CLIENT_SECRET) throw new Error("GMAIL_CLIENT_SECRET env var missing");

  const res = await fetch(GOOGLE_TOKEN_ENDPOINT, {
    method : "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body   : new URLSearchParams({
      grant_type   : "refresh_token",
      client_id    : GMAIL_CLIENT_ID,
      client_secret: GMAIL_CLIENT_SECRET,
      refresh_token: oldRefreshToken
    })
  });

  if (!res.ok) {
    const body = await res.text();
    // 400 invalid_grant on Google's side typically means the refresh_token
    // was revoked (user removed app access in their Google account, or
    // password reset, or 6+ months of inactivity). Re-seed via
    // etsyMailGmailSeedTokens to recover.
    throw new Error(`Gmail token refresh failed: ${res.status} ${body}`);
  }

  const data = await res.json();
  // Google leaves a small safety buffer off expires_in so the access_token
  // doesn't expire mid-request.
  const expires_at = Date.now() + Math.max(0, (data.expires_in - 120)) * 1000;

  await db.doc(OAUTH_DOC_PATH).set({
    access_token : data.access_token,
    refresh_token: data.refresh_token || oldRefreshToken,   // Google rarely rotates this
    expires_at,
    scope        : data.scope || null,
    token_type   : data.token_type || "Bearer",
    updatedAt    : FV.serverTimestamp()
  }, { merge: true });

  return data.access_token;
}

async function getValidGmailAccessToken() {
  const snap = await db.doc(OAUTH_DOC_PATH).get();
  if (!snap.exists) throw new Error(
    `Gmail OAuth not seeded at ${OAUTH_DOC_PATH}. Run etsyMailGmailSeedTokens first.`
  );
  const tok = snap.data();
  if (!tok.refresh_token) throw new Error(`No refresh_token in ${OAUTH_DOC_PATH}.`);

  const expiresAt = typeof tok.expires_at === "number" ? tok.expires_at : 0;
  if (!tok.access_token || expiresAt - Date.now() < TOKEN_REFRESH_BUFFER_MS) {
    return await refreshGmailToken(tok.refresh_token);
  }
  return tok.access_token;
}

// ─── Generic Gmail fetch ─────────────────────────────────────────────────
// Wraps the access-token plumbing so call sites stay clean. Pass relative
// paths starting with "/" (e.g. "/messages?q=...") and the base URL is
// prepended.

async function gmailFetch(path, opts = {}) {
  const token = await getValidGmailAccessToken();
  const url = path.startsWith("http") ? path : `${GMAIL_API_BASE}${path}`;

  const headers = {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
    ...(opts.headers || {})
  };

  // 30s budget per request — Gmail can be slow under load and we don't
  // want a hung response to burn the whole 15-min background invocation.
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 30000);

  let res;
  try {
    res = await fetch(url, { ...opts, headers, signal: controller.signal });
  } catch (err) {
    clearTimeout(timeoutId);
    if (err.name === "AbortError") throw new Error(`Gmail API timeout: ${url}`);
    throw err;
  }
  clearTimeout(timeoutId);

  // 401 means the access token went stale between getValidGmailAccessToken()
  // and the request landing — rare but possible. Force a refresh and retry once.
  if (res.status === 401 && !opts._retried) {
    const stale = await db.doc(OAUTH_DOC_PATH).get();
    if (stale.exists && stale.data().refresh_token) {
      await refreshGmailToken(stale.data().refresh_token);
      return gmailFetch(path, { ...opts, _retried: true });
    }
  }

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Gmail API ${res.status} on ${path}: ${text.slice(0, 300)}`);
  }
  return await res.json();
}

// ─── Domain helpers ──────────────────────────────────────────────────────

/**
 * List messages matching a Gmail search query. Same query syntax the user
 * would type in the Gmail search box: `from:notify@etsy.com newer_than:1d`.
 * Returns the raw API response: { messages, nextPageToken, resultSizeEstimate }.
 *
 * IMPORTANT: This response only contains { id, threadId } stubs per message.
 * Call getMessage(id) to fetch each one's headers and body.
 */
async function listMessages({ q = "", pageToken = null, maxResults = 100 } = {}) {
  const params = new URLSearchParams();
  if (q) params.set("q", q);
  if (pageToken) params.set("pageToken", pageToken);
  if (maxResults) params.set("maxResults", String(maxResults));
  return await gmailFetch(`/messages?${params.toString()}`);
}

/**
 * Fetch a single message by id with full headers + payload. format=full
 * gives us the parsed MIME tree which is what we need for body extraction.
 */
async function getMessage(id, { format = "full" } = {}) {
  return await gmailFetch(`/messages/${encodeURIComponent(id)}?format=${format}`);
}

/**
 * Headers in a Gmail message payload come as an array of {name, value}
 * objects. Lookup by name is case-insensitive (Gmail preserves casing
 * from the wire but RFC 5322 says headers are case-insensitive).
 */
function extractHeaderValue(headers = [], name = "") {
  if (!Array.isArray(headers)) return null;
  const lower = name.toLowerCase();
  for (const h of headers) {
    if (h && h.name && h.name.toLowerCase() === lower) return h.value;
  }
  return null;
}

/**
 * Decode Gmail's base64url body encoding. Gmail uses URL-safe base64
 * (- and _ instead of + and /) without padding. Node's Buffer accepts
 * the standard alphabet, so we have to swap chars first.
 */
function decodeBase64Url(s) {
  if (!s) return "";
  const b64 = String(s).replace(/-/g, "+").replace(/_/g, "/");
  // Pad to 4-char alignment
  const padded = b64 + "=".repeat((4 - (b64.length % 4)) % 4);
  try {
    return Buffer.from(padded, "base64").toString("utf-8");
  } catch {
    return "";
  }
}

/**
 * Walk a message payload's MIME tree and return a single concatenated
 * text blob containing every text/plain and text/html body part. Order:
 * plain first, then html, separated by a marker — keeps regex-based link
 * extraction simple while not losing either rendering.
 *
 * Gmail's payload schema:
 *   payload = { mimeType, headers, body: { data?, attachmentId?, size }, parts? }
 *   parts is recursive (multipart/alternative, multipart/related, etc).
 */
function extractEmailBodyText(message) {
  const out = [];
  function walk(part) {
    if (!part) return;
    const mime = (part.mimeType || "").toLowerCase();
    if ((mime === "text/plain" || mime === "text/html") && part.body && part.body.data) {
      out.push(decodeBase64Url(part.body.data));
    }
    if (Array.isArray(part.parts)) {
      for (const sub of part.parts) walk(sub);
    }
  }
  walk(message && message.payload);
  return out.join("\n\n--BOUNDARY--\n\n");
}

// ─── Etsy conversation link extraction ───────────────────────────────────
//
// Etsy's notification emails contain a CTA link to the conversation. The
// link comes in two forms in the wild:
//
//   1. Direct:    https://www.etsy.com/your/conversations/<id>
//                 https://www.etsy.com/messages/<id>
//                 https://www.etsy.com/your/messages/(buyer|thread)/<id>
//
//   2. Tracked:   https://t.etsy.com/redirect?...&url=https%3A%2F%2Fwww.etsy.com%2Fyour%2Fconversations%2F<id>...
//                 (URL-encoded inside a redirect parameter)
//
// We URL-decode the entire body once, then run the SAME regex patterns
// the Chrome extension's content-thread-scraper.js uses in its
// extractConversationId() — keeping the match surface identical to the
// scraper means any link the scraper can land on, we can detect.

const CONV_ID_PATTERNS = [
  /\/(?:your\/)?conversations\/(\d+)/,
  /\/your\/messages\/(?:buyer|thread)\/(\d+)/,
  /\/messages\/(\d+)/
];

function extractEtsyConversationLink(message) {
  const body = extractEmailBodyText(message);
  if (!body) return null;

  // Decode percent-encoding once. Etsy's t.etsy.com tracker URL-encodes
  // the destination URL inside a query param, so the conversation URL
  // comes out as "https%3A%2F%2Fwww.etsy.com%2Fyour%2Fconversations%2F123".
  // decodeURIComponent on the whole body fails on stray % chars; do a
  // safe pass that decodes valid escapes and leaves invalid ones alone.
  const decoded = body.replace(/%[0-9A-Fa-f]{2}/g, (m) => {
    try { return decodeURIComponent(m); } catch { return m; }
  });

  for (const re of CONV_ID_PATTERNS) {
    const m = decoded.match(re);
    if (m && m[1]) {
      const id = m[1];
      // Sanity check: Etsy conversation IDs are typically 8–12 digits.
      // Reject obviously bogus matches (e.g., a message-id header that
      // happened to look like /messages/123).
      if (id.length < 5 || id.length > 15) continue;
      return {
        conversationId : id,
        // Canonicalize on /your/conversations/<id> — that's the URL the
        // scraper most reliably handles, and it matches the URL operators
        // see in their browser tab when opening the conversation manually.
        conversationUrl: `https://www.etsy.com/your/conversations/${id}`
      };
    }
  }
  return null;
}

/**
 * Convenience: pull a small set of header fields useful for thread linking.
 * Returns { from, to, subject, messageIdHeader, dateHeader, internalDateMs }
 * — internalDateMs is the Gmail-assigned receive time (ms epoch).
 */
function summarizeMessage(message) {
  const headers = (message && message.payload && message.payload.headers) || [];
  const internalDateMs = message && message.internalDate
    ? parseInt(message.internalDate, 10)
    : null;
  return {
    gmailMessageId  : message && message.id ? String(message.id) : null,
    gmailThreadId   : message && message.threadId ? String(message.threadId) : null,
    snippet         : message && message.snippet ? String(message.snippet) : "",
    internalDateMs,
    from            : extractHeaderValue(headers, "From"),
    to              : extractHeaderValue(headers, "To"),
    subject         : extractHeaderValue(headers, "Subject"),
    messageIdHeader : extractHeaderValue(headers, "Message-ID") || extractHeaderValue(headers, "Message-Id"),
    dateHeader      : extractHeaderValue(headers, "Date")
  };
}

module.exports = {
  // Token management
  getValidGmailAccessToken,
  refreshGmailToken,
  // HTTP wrapper
  gmailFetch,
  // High-level Gmail ops
  listMessages,
  getMessage,
  // Parsing helpers
  extractHeaderValue,
  extractEmailBodyText,
  extractEtsyConversationLink,
  decodeBase64Url,
  summarizeMessage,
  // Constants exposed for tests / other modules
  OAUTH_DOC_PATH,
  GMAIL_API_BASE
};
