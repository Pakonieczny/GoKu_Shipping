// netlify/functions/geminiImageProxyKick.js
//
// The storefront's door to geminiImageProxy-background.
//
// WHY THIS FILE EXISTS
// --------------------
// geminiImageProxy-background is a Netlify BACKGROUND function. Netlify answers
// those itself with a platform-generated 202 the moment the request lands: no
// response body, no custom headers, and — the part that bites — no ability to
// answer a CORS preflight. The studio kinds inside that file build perfectly
// good CORS headers in studioJson(); they are simply never reached.
//
// That was invisible for as long as the only caller was the Listing Generator,
// which runs ON goldenspike.app. Same origin, no preflight, no CORS. The Custom
// Charm Studio is the first caller from britesjewelry.com, and every one of its
// requests dies at the preflight with "No 'Access-Control-Allow-Origin' header
// is present on the requested resource."
//
// So this is the thin synchronous front door — the same shape as
// verifyCharmSetsKick.js and googleAdsAutopilotKick.js already in this folder:
//
//   · It answers OPTIONS with real CORS headers.
//   · The two FAST kinds (precheck, session status) it runs in-process by
//     requiring the proxy module and calling its handler directly. Netlify
//     bundles that module once, so this does not create a second copy of it.
//   · The two SLOW kinds (generate, refine) run for minutes and must stay in
//     the background, so it POSTs them to the background endpoint server-side
//     — same origin there, so no CORS is involved — and returns 202 at once.
//
// ENV: nothing new. process.env.URL is set by Netlify itself, which is how
// verifyCharmSetsKick.js finds the site too.

const KICK_BUILD = "geminiImageProxyKick-1.0.0";

/* Only these four. This function is deliberately NOT a general-purpose proxy
   to geminiImageProxy-background: that function also carries the Listing
   Generator's internal kinds, which are same-origin, staff-only and have no
   business being reachable from a storefront page. */
/* custom_charm_compose is SYNC on purpose. It runs no model — it files a
   drawing the browser composed itself — so it is a Storage write and a
   Firestore write, well inside a synchronous budget. And it MUST be sync:
   the browser needs the downloadURL and storagePath back in the response to
   put the version on screen, which a background function can never give.
   Leaving it out of both sets is what produced "unknown_kind": the kick
   rejected it with a 400 before the background function was ever reached. */
/* custom_charm_prompt is SYNC for the same reason: the browser needs the
   prompt text back in the response body, which a background function can
   never give. It runs sharp, the material spec and one text call — heavier
   than the other sync kinds, but well inside a synchronous budget, and it
   calls no image model.

   NOTE THE PATTERN. This is the second kind added to the studio that had to
   be registered HERE as well as in the proxy's STUDIO_KINDS. The kick is the
   only door; a kind missing from both sets is rejected with 400 unknown_kind
   before the proxy is ever loaded, and the failure looks exactly like the
   proxy not having the handler. Any new studio kind goes in one of these two
   sets, always. */
const SYNC_KINDS  = new Set(["custom_charm_precheck", "custom_session_status",
                             "custom_charm_compose", "custom_charm_prompt"]);
const ASYNC_KINDS = new Set(["custom_charm_generate", "custom_charm_refine", "custom_charm_render"]);

/* Same list, same order as STUDIO_ALLOWED_ORIGINS in the proxy. */
const ALLOWED_ORIGINS = [
  "https://britesjewelry.com",
  "https://www.britesjewelry.com",
  "https://brites-jewelry.myshopify.com",
];

function corsHeaders(origin) {
  const allow = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
  return {
    "Access-Control-Allow-Origin": allow,
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Max-Age": "86400",
    "Vary": "Origin",
    "Cache-Control": "no-store",
  };
}

function json(statusCode, obj, origin) {
  return {
    statusCode,
    headers: Object.assign({ "Content-Type": "application/json" }, corsHeaders(origin)),
    body: JSON.stringify(Object.assign({ fn: KICK_BUILD }, obj)),
  };
}

/* Netlify's Node 18+ runtime has fetch built in; node-fetch is the fallback
   the rest of this folder uses. */
let fetchFn = globalThis.fetch;
if (!fetchFn) { try { fetchFn = require("node-fetch"); } catch { /* leave undefined */ } }

function siteBase() {
  return process.env.URL ||
         ("https://" + (process.env.SITE_NAME || "goldenspike") + ".netlify.app");
}

exports.handler = async (event) => {
  const h = event?.headers || {};
  const origin = h.origin || h.Origin || "";

  /* The whole reason this file exists. */
  if (event?.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: corsHeaders(origin), body: "" };
  }
  if (event?.httpMethod !== "POST") {
    return json(405, { ok: false, error: "method_not_allowed" }, origin);
  }

  let body;
  try { body = JSON.parse(event.body || "{}"); }
  catch { return json(400, { ok: false, error: "bad_json" }, origin); }

  const kind = String(body.kind || "");
  const auth = h.authorization || h.Authorization || "";

  /* A composed drawing arrives as a base64 PNG in the body, which is the only
     request this endpoint ever receives that carries an image. Netlify caps a
     synchronous request at 6 MB and answers a larger one with an opaque 413,
     so it is measured here and refused in words the studio can show. */
  if (kind === "custom_charm_compose") {
    const bytes = Buffer.byteLength(event.body || "", "utf8");
    if (bytes > 5.5 * 1024 * 1024) {
      return json(413, { ok: false, error: "image_too_large" }, origin);
    }
  }

  /* ── fast kinds: answer here and now ──────────────────────────────────
     Both are short — a vision pass and a Firestore read — so they fit inside
     a synchronous function's budget, and both NEED a real response body,
     which a background function can never give. */
  if (SYNC_KINDS.has(kind)) {
    let proxy;
    try { proxy = require("./geminiImageProxy-background"); }
    catch (err) {
      console.error("[kick] cannot load geminiImageProxy-background:", err && err.message);
      return json(500, { ok: false, error: "proxy_unavailable" }, origin);
    }
    let res;
    try {
      res = await proxy.handler({
        httpMethod: "POST",
        headers: event.headers || {},
        body: event.body,
      });
    } catch (err) {
      console.error("[kick]", kind, err);
      return json(500, { ok: false, error: "server_error" }, origin);
    }
    /* The inner handler sets its own CORS, but it is answering a synthetic
       event and must not be trusted to have got the origin right — restamp. */
    return {
      statusCode: res && res.statusCode ? res.statusCode : 502,
      headers: Object.assign({ "Content-Type": "application/json" },
                             (res && res.headers) || {}, corsHeaders(origin)),
      body: (res && res.body) || JSON.stringify({ ok: false, error: "empty_response" }),
    };
  }

  /* ── slow kinds: hand to the background function ──────────────────────
     A generation runs for minutes, well past a synchronous function's limit,
     so it has to stay in the background. The browser gets 202 immediately and
     watches the version doc in Firestore for the outcome — including failures,
     which the background handler now writes there rather than returning them
     on an HTTP response nobody can read. */
  if (ASYNC_KINDS.has(kind)) {
    /* One check worth making before firing something we cannot report on: a
       request with no bearer token at all can only ever be rejected, and
       rejecting it here means the browser sees a real 401 instead of waiting
       out the client's generation timeout for nothing. */
    if (!auth.startsWith("Bearer ")) {
      return json(401, { ok: false, error: "sign_in_required" }, origin);
    }
    if (!fetchFn) return json(500, { ok: false, error: "no_fetch" }, origin);

    let upstream;
    try {
      upstream = await fetchFn(siteBase() + "/.netlify/functions/geminiImageProxy-background", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: auth },
        body: event.body,
      });
    } catch (err) {
      console.error("[kick] could not reach the background function:", err && err.message);
      return json(502, { ok: false, error: "queue_failed" }, origin);
    }
    /* Netlify answers a background invocation with 202 and an empty body. Any
       other status means the invocation itself did not take, which is worth
       telling the browser about — it is the difference between "your charm is
       being made" and "nothing is happening and never will". */
    if (upstream.status !== 202 && upstream.status !== 200) {
      console.error("[kick] background invocation returned", upstream.status);
      return json(502, { ok: false, error: "queue_failed", upstream: upstream.status }, origin);
    }
    return json(202, { ok: true, queued: true, kind }, origin);
  }

  return json(400, { ok: false, error: "unknown_kind" }, origin);
};

/* exported for the regression pass */
exports.KICK_BUILD = KICK_BUILD;
exports.SYNC_KINDS = SYNC_KINDS;
exports.ASYNC_KINDS = ASYNC_KINDS;
