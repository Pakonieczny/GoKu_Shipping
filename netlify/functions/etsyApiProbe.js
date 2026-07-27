// netlify/functions/etsyApiProbe.js
//
// Spends ONE cheap Etsy call so the whole-key rate-limit headers
// (x-limit-per-day / x-remaining-today) can be refreshed on demand, and
// returns nothing but the outcome — never the response body.
//
// ═══ WHY THIS FILE CHANGED ══════════════════════════════════════════════
//
// It was returning:
//
//   Etsy ping returned HTTP 403: {"error":"Shared secret is required in
//   x-api-key header."}
//
// which turned the whole API widget red even though the counters behind it
// were fine. Etsy's x-api-key header is not one fixed shape: depending on how
// the app is registered it wants either the keystring alone, or
// "keystring:shared_secret". This app wants the pair — the sibling site's
// probe has always sent the pair and has always worked; this one sent the
// keystring alone and got refused.
//
// Rather than hardcode the pair and be wrong again if the registration
// changes, the probe now NEGOTIATES and REMEMBERS:
//
//   * the pair form is tried first, because Etsy explicitly asked for it
//   * on a 401/403 it retries the other form once, and the endpoint fallback
//     once, then stops
//   * the combination that worked is cached for the life of the process, so
//     the steady state is exactly ONE Etsy call per probe
//   * a 429 or 5xx is NOT treated as an auth problem — those are transient
//     and must not cause a re-negotiation
//
// Every attempt goes through etsyFetch, so it is rate-limited and metered
// like any other call; the negotiation can therefore never outrun the budget.

const { etsyFetch } = require("./etsyRateLimiter");

const HEADERS = {
  "Content-Type": "application/json",
  "Cache-Control": "no-store",
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "GET,OPTIONS",
};
const json = (statusCode, payload) => ({ statusCode, headers: HEADERS, body: JSON.stringify(payload) });

// openapi-ping is the cheapest endpoint Etsy publishes and needs no OAuth
// scope. listings/active is the proven fallback: it is what the Listing
// Generator's probe has always called.
const ENDPOINTS = [
  { name: "openapi-ping", url: "https://api.etsy.com/v3/application/openapi-ping" },
  { name: "listings-active", url: "https://api.etsy.com/v3/application/listings/active?limit=1" },
];

function creds() {
  const id = process.env.CLIENT_ID || process.env.ETSY_CLIENT_ID ||
             process.env.ETSY_API_KEY || process.env.API_KEY;
  const secret = process.env.CLIENT_SECRET || process.env.ETSY_CLIENT_SECRET ||
                 process.env.ETSY_SHARED_SECRET;
  return { id: id ? String(id).trim() : "", secret: secret ? String(secret).trim() : "" };
}

// Auth shapes, most-likely first. "pair" is first because Etsy's own 403 text
// asks for the shared secret.
function authForms({ id, secret }) {
  const forms = [];
  if (id && secret) forms.push({ name: "pair", value: id + ":" + secret });
  if (id) forms.push({ name: "keystring", value: id });
  return forms;
}

// Learned across warm invocations. Reset on any auth failure so a credential
// or registration change re-negotiates instead of failing forever.
let learned = null;   // { endpoint, form }

const isAuthFailure = (status) => status === 401 || status === 403;

async function attempt(endpoint, form) {
  const res = await etsyFetch(endpoint.url, {
    method: "GET",
    headers: { Accept: "application/json", "x-api-key": form.value },
  }, { retries: 2 });
  // Drain the body so the connection is released; the content is never used.
  const body = res.ok ? "" : await res.text().catch(() => "");
  return { status: res.status, ok: res.ok, body: String(body).slice(0, 300) };
}

exports.handler = async function (event) {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS, body: "" };
  if (event.httpMethod !== "GET") return json(405, { ok: false, verified: false, error: "Method not allowed" });

  const c = creds();
  if (!c.id) {
    return json(500, { ok: false, verified: false,
      error: "Missing Etsy CLIENT_ID (or ETSY_CLIENT_ID / ETSY_API_KEY) environment variable." });
  }

  const forms = authForms(c);
  const tried = [];

  // Steady state: one call, using whatever worked last time.
  const order = [];
  if (learned) {
    const e = ENDPOINTS.find(x => x.name === learned.endpoint);
    const f = forms.find(x => x.name === learned.form);
    if (e && f) order.push({ endpoint: e, form: f });
  }
  for (const e of ENDPOINTS) for (const f of forms) {
    if (!order.some(o => o.endpoint.name === e.name && o.form.name === f.name)) order.push({ endpoint: e, form: f });
  }

  let lastStatus = 0, lastBody = "", lastNote = "";
  for (const { endpoint, form } of order) {
    let r;
    try {
      r = await attempt(endpoint, form);
    } catch (err) {
      // Network-level failure. Not an auth problem — stop rather than burn
      // the remaining combinations on what is almost certainly transient.
      return json(502, { ok: false, verified: false, probed: true,
        error: "Etsy could not be reached: " + (err && err.message ? err.message : String(err)),
        tried: tried.concat([endpoint.name + "/" + form.name]) });
    }
    tried.push(endpoint.name + "/" + form.name + " -> " + r.status);

    if (r.ok) {
      learned = { endpoint: endpoint.name, form: form.name };
      return json(200, { ok: true, verified: true, probed: true,
        endpoint: endpoint.name, auth_form: form.name, calls_spent: tried.length,
        // Surfaced so the console can say WHY the first shape was refused,
        // instead of the operator seeing a red widget with no explanation.
        renegotiated: tried.length > 1 ? tried : undefined });
    }

    lastStatus = r.status; lastBody = r.body;

    if (isAuthFailure(r.status)) {
      // The learned combination just stopped working — forget it and keep
      // trying the remaining shapes.
      learned = null;
      lastNote = /shared secret/i.test(r.body)
        ? "Etsy wants keystring:shared_secret in x-api-key."
        : /api key|x-api-key/i.test(r.body)
          ? "Etsy rejected the x-api-key value."
          : "Etsy refused the request.";
      continue;
    }
    if (r.status === 429) {
      return json(429, { ok: false, verified: false, probed: true, retryable: true,
        error: "Etsy rate-limited the probe (HTTP 429). The whole-key reading will refresh on the next real call.",
        tried });
    }
    if (r.status >= 500) {
      return json(502, { ok: false, verified: false, probed: true, retryable: true,
        error: "Etsy returned HTTP " + r.status + " on " + endpoint.name + ". Transient — not an authorization problem.",
        tried });
    }
    // 4xx that is not an auth failure: try the next endpoint, not the next key.
  }

  return json(lastStatus && lastStatus < 500 ? lastStatus : 502, {
    ok: false, verified: false, probed: true,
    error: "Etsy refused every x-api-key form the probe knows" +
           (c.secret ? "" : " — and CLIENT_SECRET is NOT set on this site, so the keystring:shared_secret form could not even be attempted") +
           ". " + lastNote + " Last response: HTTP " + lastStatus + " " + lastBody,
    missing_secret: !c.secret,
    tried,
  });
};
