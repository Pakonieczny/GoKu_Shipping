/*  netlify/functions/_etsyMailGmail.js  (v1.1)
 *
 *  Shared Gmail API helpers for the EtsyMail system.
 *
 *  ═══ v1.1 CHANGE LOG ═══════════════════════════════════════════════════
 *
 *  Added redirect-following for Etsy's SendGrid click-tracking URLs.
 *  Etsy notification emails (from no-reply@account.etsy.com) wrap the
 *  "View message" link in `https://ablink.account.etsy.com/uni/ss/c/...`
 *  trackers — the conversation URL is NOT in the email body anywhere.
 *  We have to fetch the tracker URL with redirect:manual, read the
 *  Location header, and extract the conversation id from there.
 *
 *  extractEtsyConversationLink() became async because of this.
 *
 *  ═══ EXPORTS ═══════════════════════════════════════════════════════════
 *
 *    getValidGmailAccessToken()                  → Bearer access token
 *    gmailFetch(path, opts)                      → authenticated fetch
 *    listMessages({ q, pageToken })              → users.messages.list
 *    getMessage(id, { format })                  → users.messages.get
 *    extractEmailBodyText(message)               → plain+html text blob
 *    extractEtsyConversationLink(message) (async) → → { id, url } | null
 *    extractHeaderValue(headers, name)
 *    summarizeMessage(message)
 *
 *  ═══ ENV VARS ══════════════════════════════════════════════════════════
 *
 *    GMAIL_CLIENT_ID
 *    GMAIL_CLIENT_SECRET
 *
 *  Tokens at config/gmailOauth (Firestore).
 *  Required scope: https://mail.google.com/  (or .readonly minimum)
 */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const GMAIL_CLIENT_ID     = process.env.GMAIL_CLIENT_ID;
const GMAIL_CLIENT_SECRET = process.env.GMAIL_CLIENT_SECRET;

const OAUTH_DOC_PATH         = "config/gmailOauth";
const TOKEN_REFRESH_BUFFER_MS = 2 * 60 * 1000;
const GMAIL_API_BASE          = "https://gmail.googleapis.com/gmail/v1/users/me";
const GOOGLE_TOKEN_ENDPOINT   = "https://oauth2.googleapis.com/token";

// ─── OAuth ─────────────────────────────────────────────────────────────────

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
    throw new Error(`Gmail token refresh failed: ${res.status} ${body}`);
  }

  const data = await res.json();
  const expires_at = Date.now() + Math.max(0, (data.expires_in - 120)) * 1000;

  await db.doc(OAUTH_DOC_PATH).set({
    access_token : data.access_token,
    refresh_token: data.refresh_token || oldRefreshToken,
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

// ─── Generic Gmail fetch ───────────────────────────────────────────────────

async function gmailFetch(path, opts = {}) {
  const token = await getValidGmailAccessToken();
  const url = path.startsWith("http") ? path : `${GMAIL_API_BASE}${path}`;

  const headers = {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
    ...(opts.headers || {})
  };

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

// ─── Gmail message ops ─────────────────────────────────────────────────────

async function listMessages({ q = "", pageToken = null, maxResults = 100 } = {}) {
  const params = new URLSearchParams();
  if (q) params.set("q", q);
  if (pageToken) params.set("pageToken", pageToken);
  if (maxResults) params.set("maxResults", String(maxResults));
  return await gmailFetch(`/messages?${params.toString()}`);
}

async function getMessage(id, { format = "full" } = {}) {
  return await gmailFetch(`/messages/${encodeURIComponent(id)}?format=${format}`);
}

function extractHeaderValue(headers = [], name = "") {
  if (!Array.isArray(headers)) return null;
  const lower = name.toLowerCase();
  for (const h of headers) {
    if (h && h.name && h.name.toLowerCase() === lower) return h.value;
  }
  return null;
}

function decodeBase64Url(s) {
  if (!s) return "";
  const b64 = String(s).replace(/-/g, "+").replace(/_/g, "/");
  const padded = b64 + "=".repeat((4 - (b64.length % 4)) % 4);
  try {
    return Buffer.from(padded, "base64").toString("utf-8");
  } catch {
    return "";
  }
}

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

// ─── Etsy conversation link extraction ─────────────────────────────────────
//
// Etsy emails come in two forms in the wild:
//
//   FORM A (legacy): the conversation URL appears directly in the email
//     body as `etsy.com/your/conversations/<id>` (or URL-encoded). Easy.
//
//   FORM B (current, observed Apr 2026): the body contains ONLY SendGrid
//     click-tracking URLs of shape `https://ablink.account.etsy.com/...`.
//     Each redirects (302) to the real destination only when followed.
//     The conversation URL is NOT in the body — we have to follow at
//     least one tracker to find it.
//
// Strategy:
//   1. Fast path: scan the body for direct conversation URLs. If found
//      (FORM A), return immediately — no network needed.
//   2. Tracker path (FORM B): collect every distinct ablink URL in the
//      body. Filter to ones that look like message-link candidates
//      (the `/uni/ss/c/` flavor, which Etsy uses for in-app deep links;
//      "/ss/c/" without "/uni/" is reserved for marketing footer/nav
//      links — they don't redirect to conversations). Follow each one
//      with redirect:manual until the Location header reveals a
//      conversation URL.
//   3. Cap follows at MAX_TRACKER_FOLLOWS so a malformed email can't
//      burn the function budget.
//
// Returns: { conversationId, conversationUrl } or null

const CONV_ID_PATTERNS = [
  /\/(?:your\/)?conversations\/(\d+)/,
  /\/your\/messages\/(?:buyer|thread)\/(\d+)/,
  /\/messages\/(\d+)/
];

const ETSY_TRACKER_HOST = "ablink.account.etsy.com";
const MAX_TRACKER_FOLLOWS  = 4;     // most "View message" emails have 1-3 candidates
const MAX_REDIRECT_HOPS    = 5;     // tracker → tracker → … → etsy.com
const TRACKER_FETCH_TIMEOUT_MS = 8000;

function decodePercentSafe(s) {
  // Decode %XX escapes only (not full URI). Leave invalid escapes alone.
  return s.replace(/%[0-9A-Fa-f]{2}/g, (m) => {
    try { return decodeURIComponent(m); } catch { return m; }
  });
}

function findConversationIdInString(s) {
  if (!s) return null;
  const decoded = decodePercentSafe(s);
  for (const re of CONV_ID_PATTERNS) {
    const m = decoded.match(re);
    if (m && m[1]) {
      const id = m[1];
      if (id.length >= 5 && id.length <= 15) return id;
    }
  }
  return null;
}

/**
 * Fetch a single URL with manual redirect handling; return the final
 * resolved URL (after following all 3xx hops up to MAX_REDIRECT_HOPS),
 * or null on failure / hop limit.
 *
 * IMPLEMENTATION NOTE: We use GET (not HEAD) with a real-looking
 * User-Agent. SendGrid's click trackers (ablink.*) return 403 on HEAD
 * requests — they only honor GET with a browser-like UA. We use
 * redirect:"manual" so we read the Location header without following
 * the body, keeping each hop cheap (response body never read or buffered
 * past the headers).
 */
async function followToFinalUrl(startUrl, hopBudget = MAX_REDIRECT_HOPS) {
  let current = startUrl;
  for (let hop = 0; hop < hopBudget; hop++) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), TRACKER_FETCH_TIMEOUT_MS);
    let res;
    try {
      res = await fetch(current, {
        method   : "GET",
        redirect : "manual",
        signal   : controller.signal,
        headers  : {
          // Browser-like UA: SendGrid's tracker rejects bot-looking UAs
          // with 403. The conversation URL never returns sensitive data
          // (the ID itself isn't a credential — landing on it without
          // an Etsy session just shows a login page), so spoofing UA
          // here doesn't expose anything.
          "User-Agent"     : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
          "Accept"         : "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "en-US,en;q=0.9"
        }
      });
    } catch (e) {
      clearTimeout(t);
      // Network error or timeout — give up on this URL
      return null;
    }
    clearTimeout(t);

    if (res.status >= 300 && res.status < 400) {
      const loc = res.headers.get("location");
      if (!loc) return null;
      // Resolve relative redirects against the current URL
      try {
        current = new URL(loc, current).toString();
      } catch {
        return null;
      }
      continue;
    }

    // 2xx (terminal) — current is the final URL. We never read the body;
    // discard the response so the connection can be reused.
    if (res.status >= 200 && res.status < 300) {
      try { res.body && res.body.resume && res.body.resume(); } catch {}
      return current;
    }

    // 4xx/5xx terminal — give up
    return null;
  }
  // Hop budget exhausted
  return null;
}

async function extractEtsyConversationLink(message) {
  const body = extractEmailBodyText(message);
  if (!body) return null;

  // ── Fast path: try to find a direct conversation URL in the body ──
  const directId = findConversationIdInString(body);
  if (directId) {
    return {
      conversationId : directId,
      conversationUrl: `https://www.etsy.com/your/conversations/${directId}`
    };
  }

  // ── Tracker path: collect ablink URLs and follow them ──
  // Match http/https URLs containing ablink.account.etsy.com. The URL
  // continues until whitespace, ), >, or end-of-string. Quoted-printable
  // encoded emails sprinkle "=\n" soft-line-breaks into URLs — strip
  // those before extraction.
  const flat = body.replace(/=\r?\n/g, "");
  const trackerRegex = /https?:\/\/ablink\.account\.etsy\.com\/[^\s)<"']+/gi;

  const all = flat.match(trackerRegex) || [];

  // Deduplicate. Many emails repeat the message-link tracker (button +
  // wrapping <a> around the avatar both point to the same destination).
  const seen = new Set();
  const candidates = [];
  for (const u of all) {
    // Trim trailing punctuation that often follows URLs in plain-text
    // dumps: closing parens, periods, commas, question marks.
    const cleaned = u.replace(/[).,!?;:'"]+$/, "");
    if (seen.has(cleaned)) continue;
    seen.add(cleaned);
    candidates.push(cleaned);
  }

  if (!candidates.length) return null;

  // Etsy uses TWO tracker URL flavors in these emails:
  //   /uni/ss/c/   → in-app deep links (the message link uses this; lands
  //                  on /your/conversations/<id> after redirects)
  //   /ss/c/       → marketing/footer links (Home & Living, social, app
  //                  store badges, unsubscribe, etc.) — NOT conversations
  //
  // Prioritize /uni/ss/c/ first to minimize wasted HTTP requests. If
  // none of those resolve to a conversation (Etsy might shuffle this in
  // the future), fall back to trying /ss/c/ links too.
  const uniLinks  = candidates.filter(u => u.includes("/uni/ss/c/"));
  const ssLinks   = candidates.filter(u => !u.includes("/uni/ss/c/"));
  const ordered   = [...uniLinks, ...ssLinks].slice(0, MAX_TRACKER_FOLLOWS);

  for (const trackerUrl of ordered) {
    const finalUrl = await followToFinalUrl(trackerUrl);
    if (!finalUrl) continue;
    const id = findConversationIdInString(finalUrl);
    if (id) {
      return {
        conversationId : id,
        conversationUrl: `https://www.etsy.com/your/conversations/${id}`
      };
    }
  }

  return null;
}

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
  extractEtsyConversationLink,   // now async
  decodeBase64Url,
  summarizeMessage,
  // Tracker resolution (exposed for tests)
  followToFinalUrl,
  findConversationIdInString,
  // Constants
  OAUTH_DOC_PATH,
  GMAIL_API_BASE
};
