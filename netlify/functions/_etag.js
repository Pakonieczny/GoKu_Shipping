/*  netlify/functions/_etag.js — ETag / 304 helper for functions that pages poll (FC15, Firebase cost emergency, 7 Oct 2026).
 *  Browser half: cn-poll.js (sends If-None-Match with the last ETag, treats 304 as "unchanged", one poller per computer).
 *
 *  TWO KINDS OF ETAG, VERY DIFFERENT COST
 *    revision ETag   the function reads ONE tiny document (a revision counter that whoever changes the real data bumps), compares it
 *                    with If-None-Match and answers 304 WITHOUT reading the real data. Saves Firestore reads AND bytes. This is the
 *                    one that cuts the bill: use conditional() with a revision reader.
 *    body-hash ETag  the function reads everything, hashes the answer, answers 304 if the browser already has it. Saves only the bytes
 *                    to the browser (Netlify bandwidth); the Firestore reads were already paid. Use withEtag() only as a second line.
 *
 *  USAGE (Netlify v1 handler style: { statusCode, headers, body } objects, as _charmNestAuth.js json() builds them)
 *    const E = require("./_etag");
 *
 *    // 1. the usual: cheap revision first, expensive read only when it moved
 *    return E.conditional(event, {
 *      revision: async () => (await db.doc("Live_Revisions/library").get()).get("rev") || 0,   // ONE small read
 *      build:    async () => ({ sheets: await readSheets() }),                                // the expensive reads, only when stale
 *      scope: "library",                    // optional: part of the tag, so two endpoints never share one
 *      headers: CORS,                       // optional: response headers (your CORS / Cache-Control)
 *    });
 *    // The revision is read BEFORE the data: a change between the two reads gives the newer data under the older tag, so the next
 *    // poll sees a new revision and fetches once more. A change is never missed, at worst it is read twice.
 *
 *    // 2. a tiny "what is the revision" answer for a page that then decides itself what to fetch
 *    return E.revisionAnswer(event, rev, { count: 3 }, { headers: CORS });      // 200 {rev, count} with ETag, or 304 with no body
 *
 *    // 3. second line of defence on an answer you already built (bytes only): omit the fields that change on every call
 *    return E.withEtag(event, json(200, body), { omit: ["serverTime", "now"] });
 *
 *    // 4. lower-level pieces
 *    E.etagOfRevision(rev, scope)  E.etagOfValue(value, { omit })  E.etagOfText(text)  E.matches(event, etag)  E.notModified(etag, headers)
 *    E.stableStringify(value)  (keys sorted, undefined skipped: same value, same text, any key order)
 *
 *  RULES
 *   - Compare is WEAK (W/"x" matches "x") and honours lists and "*", so a proxy that weakens a tag changes nothing.
 *   - A 304 has no body, keeps the caller's headers (CORS, Cache-Control) and carries the ETag.
 *   - Do not put the request's passcode, PIN or any person's secret into the tag input; revisions and hashes only.
 *   - Responses that vary by who asks must put that in `scope` (the person, the filter), never share a tag across them.
 *   - Nothing here touches Firestore; the revision reader and the builder are yours.                                              */
"use strict";
const crypto = require("crypto");

/** Same value, same text, whatever the key order. undefined and functions are skipped as JSON.stringify skips them. */
function stableStringify(v) {
  if (v === null || typeof v !== "object") return JSON.stringify(v) === undefined ? "null" : JSON.stringify(v);
  if (typeof v.toJSON === "function") return stableStringify(v.toJSON());
  if (Array.isArray(v)) return "[" + v.map(x => (x === undefined || typeof x === "function" ? "null" : stableStringify(x))).join(",") + "]";
  const keys = Object.keys(v).filter(k => v[k] !== undefined && typeof v[k] !== "function").sort();
  return "{" + keys.map(k => JSON.stringify(k) + ":" + stableStringify(v[k])).join(",") + "}";
}

const hash = text => crypto.createHash("sha1").update(String(text), "utf8").digest("base64").replace(/[+/=]/g, c => (c === "+" ? "-" : c === "/" ? "_" : "")).slice(0, 22);
const quote = s => '"' + String(s).replace(/["\\\r\n]/g, "") + '"';

/** Tag of a revision value (a number, a string, a timestamp millis). scope keeps endpoints apart. */
function etagOfRevision(rev, scope) {
  return quote((scope ? String(scope) + "." : "") + "r" + (rev == null ? "0" : String(rev)));
}
/** Tag of a JSON value (a hash of its stable text). opts.omit = top-level field names left out (timestamps that change every call). */
function etagOfValue(value, opts) {
  let v = value;
  const omit = opts && Array.isArray(opts.omit) ? opts.omit : null;
  if (omit && omit.length && v && typeof v === "object" && !Array.isArray(v)) { v = Object.assign({}, v); for (const k of omit) delete v[k]; }
  return quote("h" + hash(stableStringify(v)));
}
/** Tag of a text (a response body as it goes out). */
function etagOfText(text) { return quote("t" + hash(text)); }

function headerOf(event, name) {
  const h = (event && event.headers) || {}, want = String(name).toLowerCase();
  if (h[want] != null) return String(h[want]);
  for (const k of Object.keys(h)) if (k.toLowerCase() === want) return String(h[k]);
  return "";
}
const bare = t => String(t).trim().replace(/^W\//i, "");

/** True when the request's If-None-Match already names this tag (weak compare, list, "*"). */
function matches(event, etag) {
  const raw = headerOf(event, "if-none-match");
  if (!raw || !etag) return false;
  if (raw.trim() === "*") return true;
  const want = bare(etag);
  return raw.split(",").some(t => bare(t) === want);
}

function merge(headers, extra) { return Object.assign({}, headers || {}, extra || {}); }
/** A 304: no body, the ETag, the caller's headers (Content-Type of a body that is not sent is dropped). */
function notModified(etag, headers) {
  const h = merge(headers);
  for (const k of Object.keys(h)) if (/^content-(type|length)$/i.test(k)) delete h[k];
  h.ETag = etag;
  return { statusCode: 304, headers: h, body: "" };
}
/** A 200 JSON answer with its ETag. */
function ok(body, etag, headers) {
  const h = merge({ "Content-Type": "application/json", "Cache-Control": "no-store" }, headers);
  h.ETag = etag;
  return { statusCode: 200, headers: h, body: typeof body === "string" ? body : JSON.stringify(body) };
}

/** The revision-first answer. revision() reads one tiny value; build() the real data only when the caller's tag is stale.
 *  opts: { scope, headers, extra: (body) => body }. A failure of revision() is NOT hidden: it throws to the caller's own error
 *  handling (the page keeps what it has and backs off) instead of answering with data nobody can validate. */
async function conditional(event, opts) {
  const o = opts || {};
  if (typeof o.revision !== "function" || typeof o.build !== "function") throw new Error("_etag.conditional needs revision() and build()");
  const rev = await o.revision();
  const etag = etagOfRevision(rev, o.scope);
  if (matches(event, etag)) return notModified(etag, o.headers);
  return ok(await o.build(rev), etag, o.headers);
}

/** A tiny { rev, ...extra } answer for a revision endpoint; 304 when the caller already has this revision. */
function revisionAnswer(event, rev, extra, opts) {
  const o = opts || {};
  const etag = etagOfRevision(rev, o.scope);
  if (matches(event, etag)) return notModified(etag, o.headers);
  return ok(Object.assign({ rev: rev == null ? 0 : rev }, extra || {}), etag, o.headers);
}

/** Second line of defence on a finished { statusCode, headers, body } answer: tag it by its content, 304 when the caller has it.
 *  Only 200 answers are tagged. opts.omit = JSON field names left out of the hash (serverTime, now ...). Saves bytes, not reads. */
function withEtag(event, response, opts) {
  if (!response || response.statusCode !== 200 || typeof response.body !== "string") return response;
  let etag;
  const omit = opts && Array.isArray(opts.omit) && opts.omit.length ? opts.omit : null;
  if (omit) {
    try { etag = etagOfValue(JSON.parse(response.body), { omit }); } catch (_) { etag = etagOfText(response.body); }
  } else etag = etagOfText(response.body);
  if (matches(event, etag)) return notModified(etag, response.headers);
  return Object.assign({}, response, { headers: merge(response.headers, { ETag: etag }) });
}

/** CORS additions for a cross-origin page: lets the browser send If-None-Match and read ETag. (Same-origin pages need nothing.) */
function corsFor(headers) {
  const h = merge(headers);
  const have = String(h["Access-Control-Allow-Headers"] || "");
  if (!/if-none-match/i.test(have)) h["Access-Control-Allow-Headers"] = (have ? have + ", " : "") + "If-None-Match";
  h["Access-Control-Expose-Headers"] = /etag/i.test(String(h["Access-Control-Expose-Headers"] || "")) ? h["Access-Control-Expose-Headers"] : ((h["Access-Control-Expose-Headers"] ? h["Access-Control-Expose-Headers"] + ", " : "") + "ETag");
  return h;
}

module.exports = {
  stableStringify, etagOfRevision, etagOfValue, etagOfText, matches, notModified, ok, conditional, revisionAnswer, withEtag, corsFor, headerOf
};
