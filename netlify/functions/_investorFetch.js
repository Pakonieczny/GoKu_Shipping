/*  netlify/functions/_investorFetch.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the only outbound HTTP path in the system.
 *
 *  Every rule the plan's Control A and §11.3 demand lives here, in one place,
 *  so no adapter can quietly skip one:
 *
 *    · domain allowlist, checked after DNS resolution and again on every hop
 *    · SSRF guard: no localhost, link-local, private, CGNAT, or metadata IPs
 *    · redirects are re-validated rather than followed blindly
 *    · declared User-Agent (SEC requires one and blocks undeclared tools)
 *    · per-host token bucket, defaulting well under SEC's 10 req/s ceiling
 *    · conditional requests via ETag / If-Modified-Since
 *    · content-type and size caps before the body is parsed
 *    · sha256 of the raw bytes, so a document version is byte-identity
 *    · login / anti-bot / paywall detection → fail closed, mark source unhealthy
 *
 *  Nothing here ever fetches a URL supplied by the browser. Callers pass a
 *  registered source id; the URL is built from the registry template.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const dns = require("dns").promises;
const net = require("net");

let fetchFn;
try { fetchFn = require("node-fetch"); } catch { fetchFn = globalThis.fetch; }

const UA = process.env.INVESTOR_USER_AGENT ||
  "InvestorAI-Research/1.0 (private single-user research; contact: shellribas@gmail.com)";

/* ── allowlist. A host not on this list cannot be reached, full stop. ───── */
const ALLOWED_HOSTS = new Set([
  // SEC — primary evidence
  "www.sec.gov", "sec.gov", "data.sec.gov", "efts.sec.gov",
  // Regulators
  "www.fda.gov", "api.fda.gov", "clinicaltrials.gov", "www.ema.europa.eu",
  "eutils.ncbi.nlm.nih.gov", "pubmed.ncbi.nlm.nih.gov",
  "www.federalregister.gov", "www.bis.gov",
  // Public research
  "export.arxiv.org", "api.openalex.org", "api.crossref.org", "pub.orcid.org",
  // Government spend
  "api.usaspending.gov", "www.war.gov",
  // Market data (registry decides which is enabled)
  "data.alpaca.markets", "api.massive.com", "api.polygon.io", "api.tiingo.com",
  "stooq.com",
  // Index / regime
  "cdn.cboe.com", "www.cboe.com",
]);

/* Hosts that are explicitly forbidden even if someone adds them above. */
const DENY_HOSTS = new Set([
  "query1.finance.yahoo.com", "query2.finance.yahoo.com",
  "finance.yahoo.com", "www.yahoo.com", "scholar.google.com",
]);

/* ── per-host rate limiting (token bucket, per lambda instance) ─────────── */
const HOST_RATE = {
  "www.sec.gov":   { rps: 4, burst: 4 },   // SEC ceiling is 10/s; stay well under
  "data.sec.gov":  { rps: 4, burst: 4 },
  "efts.sec.gov":  { rps: 2, burst: 2 },
  "_default":      { rps: 5, burst: 5 },
};
const buckets = new Map();
async function throttle(host) {
  const cfg = HOST_RATE[host] || HOST_RATE._default;
  let b = buckets.get(host);
  const now = Date.now();
  if (!b) { b = { tokens: cfg.burst, last: now }; buckets.set(host, b); }
  b.tokens = Math.min(cfg.burst, b.tokens + ((now - b.last) / 1000) * cfg.rps);
  b.last = now;
  if (b.tokens < 1) {
    const waitMs = Math.ceil(((1 - b.tokens) / cfg.rps) * 1000);
    await new Promise((r) => setTimeout(r, waitMs));
    b.tokens = 0; b.last = Date.now();
  } else {
    b.tokens -= 1;
  }
}

/* ── SSRF guard ────────────────────────────────────────────────────────── */
function isBlockedIp(ip) {
  if (net.isIPv4(ip)) {
    const p = ip.split(".").map(Number);
    if (p[0] === 0 || p[0] === 10 || p[0] === 127) return true;              // this/private/loopback
    if (p[0] === 169 && p[1] === 254) return true;                            // link-local + metadata
    if (p[0] === 172 && p[1] >= 16 && p[1] <= 31) return true;                // private
    if (p[0] === 192 && p[1] === 168) return true;                            // private
    if (p[0] === 100 && p[1] >= 64 && p[1] <= 127) return true;               // CGNAT
    if (p[0] >= 224) return true;                                             // multicast / reserved
    return false;
  }
  if (net.isIPv6(ip)) {
    const s = ip.toLowerCase();
    if (s === "::1" || s === "::") return true;
    if (s.startsWith("fc") || s.startsWith("fd")) return true;                // ULA
    if (s.startsWith("fe80")) return true;                                    // link-local
    if (s.startsWith("::ffff:")) return isBlockedIp(s.split(":").pop());       // v4-mapped
    return false;
  }
  return true;
}

async function assertSafeUrl(urlStr) {
  let u;
  try { u = new URL(urlStr); } catch { throw fail("bad_url", `Unparseable URL: ${urlStr}`); }
  if (u.protocol !== "https:") throw fail("insecure_scheme", `Refused non-HTTPS URL: ${u.protocol}`);
  const host = u.hostname.toLowerCase();
  if (DENY_HOSTS.has(host)) throw fail("denied_host", `Host is on the permanent deny list: ${host}`);
  if (!ALLOWED_HOSTS.has(host)) throw fail("host_not_allowed", `Host not in allowlist: ${host}`);
  let addrs;
  try { addrs = await dns.lookup(host, { all: true }); }
  catch (e) { throw fail("dns_failed", `DNS lookup failed for ${host}: ${e.message}`); }
  for (const a of addrs) {
    if (isBlockedIp(a.address)) throw fail("ssrf_blocked", `${host} resolves to a blocked address (${a.address})`);
  }
  return u;
}

function fail(code, message) {
  const e = new Error(message); e.code = code; e.fetchGuard = true; return e;
}

/* ── login / anti-bot detection ────────────────────────────────────────── */
const BLOCK_MARKERS = [
  /<title>[^<]*(sign in|log in|login|access denied|forbidden|captcha)/i,
  /just a moment\.\.\./i, /cf-browser-verification/i, /__cf_chl/i,
  /please enable (javascript|cookies)/i, /your request has been blocked/i,
  /automated access to this site/i, /request rate threshold/i,
];
function looksBlocked(text, contentType) {
  if (!/html|text/i.test(contentType || "")) return false;
  const head = String(text || "").slice(0, 4000);
  return BLOCK_MARKERS.some((re) => re.test(head));
}

/* ── the fetch ─────────────────────────────────────────────────────────── */
const DEFAULT_MAX_BYTES = 8 * 1024 * 1024;

/**
 * @param {string} url                fully-built URL from a registry template
 * @param {object} opts
 *   {string}  opts.sourceId          for provenance + health accounting
 *   {string}  opts.etag              conditional request
 *   {string}  opts.lastModified      conditional request
 *   {string[]} opts.accept           acceptable content-type substrings
 *   {number}  opts.maxBytes
 *   {number}  opts.timeoutMs
 *   {number}  opts.maxRedirects
 * @returns {Promise<{status,notModified,bytes,text,json,sha256,contentType,etag,lastModified,finalUrl,fetchedAt,elapsedMs}>}
 */
async function fetchPublic(url, opts = {}) {
  const {
    sourceId = "unregistered",
    etag = null, lastModified = null,
    accept = null,
    maxBytes = DEFAULT_MAX_BYTES,
    timeoutMs = 20000,
    maxRedirects = 3,
    headers: extraHeaders = {},
  } = opts;

  const startedAt = Date.now();
  let current = url, hops = 0, res = null, u = null;

  while (hops <= maxRedirects) {
    u = await assertSafeUrl(current);
    await throttle(u.hostname.toLowerCase());

    const h = {
      "User-Agent": UA,
      "Accept-Encoding": "gzip, deflate",
      "Accept": accept ? accept.join(", ") : "*/*",
      ...extraHeaders,
    };
    if (etag && hops === 0) h["If-None-Match"] = etag;
    if (lastModified && hops === 0) h["If-Modified-Since"] = lastModified;

    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), timeoutMs);
    try {
      res = await fetchFn(current, { method: "GET", headers: h, redirect: "manual", signal: ac.signal });
    } catch (e) {
      clearTimeout(timer);
      throw fail(e.name === "AbortError" ? "timeout" : "network", `${sourceId}: ${e.message}`);
    }
    clearTimeout(timer);

    if ([301, 302, 303, 307, 308].includes(res.status)) {
      const loc = res.headers.get("location");
      if (!loc) throw fail("bad_redirect", `${sourceId}: redirect with no Location`);
      current = new URL(loc, current).toString();   // re-validated at loop top
      hops += 1;
      continue;
    }
    break;
  }
  if (hops > maxRedirects) throw fail("too_many_redirects", `${sourceId}: exceeded ${maxRedirects} redirects`);

  const fetchedAt = new Date().toISOString();

  if (res.status === 304) {
    return { status: 304, notModified: true, sourceId, finalUrl: current, fetchedAt,
             elapsedMs: Date.now() - startedAt };
  }
  if (!res.ok) {
    throw fail("http_" + res.status, `${sourceId}: HTTP ${res.status} from ${u.hostname}`);
  }

  const contentType = res.headers.get("content-type") || "";
  if (accept && !accept.some((a) => contentType.toLowerCase().includes(a.toLowerCase()))) {
    throw fail("content_type", `${sourceId}: unexpected content-type "${contentType}" (wanted ${accept.join("/")})`);
  }
  const declared = Number(res.headers.get("content-length") || 0);
  if (declared && declared > maxBytes) {
    throw fail("too_large", `${sourceId}: content-length ${declared} exceeds cap ${maxBytes}`);
  }

  const buf = Buffer.from(await res.arrayBuffer());
  if (buf.length > maxBytes) throw fail("too_large", `${sourceId}: body ${buf.length} exceeds cap ${maxBytes}`);

  const text = buf.toString("utf8");
  if (looksBlocked(text, contentType)) {
    throw fail("blocked_page", `${sourceId}: response looks like a login/anti-bot page — source marked unhealthy`);
  }

  let parsed = null;
  if (/json/i.test(contentType)) { try { parsed = JSON.parse(text); } catch { /* leave null */ } }

  return {
    status: res.status,
    notModified: false,
    sourceId,
    bytes: buf.length,
    text,
    json: parsed,
    sha256: crypto.createHash("sha256").update(buf).digest("hex"),
    contentType,
    etag: res.headers.get("etag") || null,
    lastModified: res.headers.get("last-modified") || null,
    finalUrl: current,
    fetchedAt,
    elapsedMs: Date.now() - startedAt,
  };
}

/** Normalized content hash — boilerplate-insensitive, so a version is only
 *  created when the substance changes rather than on every timestamp tick. */
function normalizedHash(text) {
  const norm = String(text || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&[a-z]+;|&#\d+;/gi, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
  return { hash: crypto.createHash("sha256").update(norm).digest("hex"), text: norm, length: norm.length };
}

module.exports = {
  fetchPublic, normalizedHash, assertSafeUrl,
  ALLOWED_HOSTS, DENY_HOSTS, UA,
};
