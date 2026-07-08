/* netlify/functions/emailCapture.js
   ---------------------------------------------------------------------------
   Brites Jewelry — smart email capture backend (britesjewelry.com)

   What it does:
   • subscribe: creates/updates the Shopify CUSTOMER (marketing consent
     SUBSCRIBED, tagged with source + audience), mints a UNIQUE single-shopper
     welcome code (discountRedeemCodeBulkAdd onto one shared discount
     container per offer %, appliesOncePerCustomer), logs the capture to
     Firestore, and returns the code instantly. Repeat emails get their
     original code back — no duplicate customers, no code farming.
   • offer: the popup asks this endpoint what the CURRENT offer is. The offer
     lives in a Shopify SHOP METAFIELD (namespace "brites", key "email_offer",
     JSON: {"pct":10,"headline":"","subheading":"","expiresDays":14,
     "enabled":true}) — so seasonal offers are managed from the Shopify
     backend/API and flip site-wide with no theme deploy. Missing metafield =
     sensible defaults.
   • tag: adds the "who are you shopping for" audience tag to the customer
     after the tile pick (fire-and-forget from the popup).

   Env (already configured on goldenspike for the other Shopify functions):
   SHOPIFY_STORE, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET,
   optional SHOPIFY_API_VERSION (default 2025-10),
   optional BRITES_EMAIL_ALLOWED_ORIGINS (comma list; defaults below),
   plus the FIREBASE_* vars used by firebaseAdmin.js.
   --------------------------------------------------------------------------- */

const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");
const db = admin.firestore();

const API_VERSION = process.env.SHOPIFY_API_VERSION || "2025-10";
const CAPTURES = "Brites_Email_Captures";
const META = "Brites_Email_Meta";

const DEFAULT_OFFER = {
  pct: 10,
  expiresDays: 14,
  enabled: true,
  headline: "",   // popup composes contextual copy when blank
  subheading: ""
};

const ALLOWED_ORIGINS = (process.env.BRITES_EMAIL_ALLOWED_ORIGINS ||
  "https://britesjewelry.com,https://www.britesjewelry.com,https://brites-jewelry.myshopify.com"
).split(",").map(s => s.trim()).filter(Boolean);

/* ------------------------------ Shopify auth ------------------------------ */

let _token = null, _tokenExp = 0;
async function getToken() {
  if (_token && Date.now() < _tokenExp - 60000) return _token;
  const store = process.env.SHOPIFY_STORE;
  const res = await fetch(`https://${store}/admin/oauth/access_token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "client_credentials",
      client_id: process.env.SHOPIFY_CLIENT_ID,
      client_secret: process.env.SHOPIFY_CLIENT_SECRET
    })
  });
  const text = await res.text();
  if (!res.ok) throw new Error("Token request failed (" + res.status + "): " + text);
  const data = JSON.parse(text);
  _token = data.access_token;
  _tokenExp = Date.now() + (Number(data.expires_in || 3600) * 1000);
  return _token;
}

async function gql(query, variables, _attempt) {
  const store = process.env.SHOPIFY_STORE;
  const token = await getToken();
  const res = await fetch(`https://${store}/admin/api/${API_VERSION}/graphql.json`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Shopify-Access-Token": token },
    body: JSON.stringify({ query, variables: variables || {} })
  });
  const body = await res.json().catch(() => ({}));
  if (res.status === 429 || (body.errors || []).some(e => (e.extensions || {}).code === "THROTTLED")) {
    if ((_attempt || 0) < 4) { await new Promise(r => setTimeout(r, 1200 * ((_attempt || 0) + 1))); return gql(query, variables, (_attempt || 0) + 1); }
  }
  if (body.errors && body.errors.length) throw new Error("GraphQL: " + JSON.stringify(body.errors).slice(0, 500));
  return body.data;
}

/* --------------------------------- helpers -------------------------------- */

function normEmail(e) {
  e = String(e || "").trim().toLowerCase();
  return /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(e) && e.length <= 254 ? e : null;
}
function randCode(pct) {
  const abc = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"; // no 0/O/1/I/L
  let s = "";
  for (let i = 0; i < 6; i++) s += abc[Math.floor(Math.random() * abc.length)];
  return `BRITES${pct}-${s}`;
}
function corsHeaders(origin) {
  const ok = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
  return {
    "Access-Control-Allow-Origin": ok,
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Vary": "Origin",
    "Content-Type": "application/json"
  };
}
const out = (status, obj, origin) => ({ statusCode: status, headers: corsHeaders(origin), body: JSON.stringify(obj) });

// tiny best-effort per-IP throttle (cold starts reset it; email dedupe is the real guard)
const _hits = new Map();
function throttled(ip) {
  const now = Date.now(), rec = _hits.get(ip) || [];
  const recent = rec.filter(t => now - t < 60000);
  recent.push(now); _hits.set(ip, recent);
  return recent.length > 8;
}

/* --------------------------------- offer ---------------------------------- */

let _offerCache = null, _offerAt = 0;
async function getOffer() {
  if (_offerCache && Date.now() - _offerAt < 5 * 60000) return _offerCache;
  let offer = { ...DEFAULT_OFFER };
  try {
    const d = await gql(`{
      shop { metafield(namespace: "brites", key: "email_offer") { value } }
    }`);
    const raw = d && d.shop && d.shop.metafield && d.shop.metafield.value;
    if (raw) {
      const m = JSON.parse(raw);
      offer = {
        pct: Math.min(60, Math.max(1, Math.round(Number(m.pct) || DEFAULT_OFFER.pct))),
        expiresDays: Math.min(90, Math.max(1, Math.round(Number(m.expiresDays) || DEFAULT_OFFER.expiresDays))),
        enabled: m.enabled !== false,
        headline: String(m.headline || ""),
        subheading: String(m.subheading || "")
      };
    }
  } catch (e) { /* metafield optional — defaults stand */ }
  _offerCache = offer; _offerAt = Date.now();
  return offer;
}

/* ------------------------- unique single-use discounts ---------------------- */

// One DISCOUNT PER SIGNUP — the only way Shopify enforces BOTH properties the
// program needs: usageLimit:1 makes the code truly single-use (a shared
// container can't do per-code limits, so a leaked code would be redeemable by
// unlimited different customers), and endsAt bakes the expiry into the code
// itself in Shopify — not just in our copy.
async function createUniqueDiscount(pct, expiresDays, email) {
  const endsAt = new Date(Date.now() + expiresDays * 86400000).toISOString();
  for (let attempt = 0; attempt < 3; attempt++) {
    const code = randCode(pct);
    const d = await gql(`
      mutation($basicCodeDiscount: DiscountCodeBasicInput!) {
        discountCodeBasicCreate(basicCodeDiscount: $basicCodeDiscount) {
          codeDiscountNode { id }
          userErrors { field message }
        }
      }`, {
      basicCodeDiscount: {
        title: `Welcome ${pct}% \u00b7 ${email}`,
        code,
        startsAt: new Date().toISOString(),
        endsAt,
        usageLimit: 1,
        appliesOncePerCustomer: true,
        customerSelection: { all: true },
        customerGets: { value: { percentage: pct / 100 }, items: { all: true } }
      }
    });
    const node = d.discountCodeBasicCreate;
    const errs = node.userErrors || [];
    if (!errs.length && node.codeDiscountNode) return { code, endsAt, discountId: node.codeDiscountNode.id };
    if (!/exist|taken|duplicate/i.test(JSON.stringify(errs))) {
      throw new Error("discount create: " + JSON.stringify(errs).slice(0, 300));
    }
  }
  throw new Error("could not mint a unique code");
}

/* ------------------------------ customer sync ------------------------------ */

async function upsertCustomer(email, tags) {
  const consent = {
    marketingState: "SUBSCRIBED",
    marketingOptInLevel: "SINGLE_OPT_IN",
    consentUpdatedAt: new Date().toISOString()
  };
  const c = await gql(`
    mutation($input: CustomerInput!) {
      customerCreate(input: $input) {
        customer { id }
        userErrors { field message }
      }
    }`, { input: { email, tags, emailMarketingConsent: consent } });
  const cc = c.customerCreate;
  if (cc.customer && cc.customer.id) return { id: cc.customer.id, created: true };

  const msg = JSON.stringify(cc.userErrors || []);
  if (!/taken|already/i.test(msg)) throw new Error("customerCreate: " + msg.slice(0, 300));

  // existing customer: look up, re-consent, add tags
  const q = await gql(`{ customers(first: 1, query: ${JSON.stringify("email:" + email)}) { nodes { id } } }`);
  const id = q.customers && q.customers.nodes[0] && q.customers.nodes[0].id;
  if (!id) throw new Error("customer exists but lookup failed");
  await gql(`
    mutation($input: CustomerEmailMarketingConsentUpdateInput!) {
      customerEmailMarketingConsentUpdate(input: $input) {
        userErrors { field message }
      }
    }`, { input: { customerId: id, emailMarketingConsent: consent } }).catch(() => {});
  await gql(`
    mutation($id: ID!, $tags: [String!]!) {
      tagsAdd(id: $id, tags: $tags) { userErrors { field message } }
    }`, { id, tags }).catch(() => {});
  return { id, created: false };
}

/* ----------------------------------- ops ----------------------------------- */

async function subscribe(body, ip) {
  if (String(body.hp || "").trim()) return { ok: true, code: null, bot: true }; // honeypot: pretend success
  const email = normEmail(body.email);
  if (!email) return { ok: false, error: "Please enter a valid email address." };
  if (throttled(ip)) return { ok: false, error: "Too many attempts — try again in a minute." };

  const offer = await getOffer();
  const ctx = body.context || {};
  const audience = String(body.audience || "").replace(/[^a-z-]/gi, "").slice(0, 30);

  // repeat signup → same code back while it's still valid; expired → fresh one
  const ref = db.collection(CAPTURES).doc(email);
  const existing = await ref.get();
  if (existing.exists) {
    const e = existing.data();
    if (e.code && e.expiresAt && e.expiresAt > Date.now()) {
      const daysLeft = Math.max(1, Math.ceil((e.expiresAt - Date.now()) / 86400000));
      return { ok: true, already: true, code: e.code, pct: e.pct || offer.pct, expiresDays: daysLeft };
    }
    if (offer.enabled) {
      try {
        const fresh = await createUniqueDiscount(offer.pct, offer.expiresDays, email);
        await ref.set({ code: fresh.code, pct: offer.pct, expiresAt: Date.parse(fresh.endsAt), renewedAt: Date.now() }, { merge: true });
        return { ok: true, already: true, renewed: true, code: fresh.code, pct: offer.pct, expiresDays: offer.expiresDays };
      } catch (err) { console.error("[emailCapture] renew failed:", err.message); }
    }
    return { ok: true, already: true, code: e.code || null, pct: e.pct || offer.pct, expiresDays: offer.expiresDays };
  }

  let code = null, endsAtMs = null;
  if (offer.enabled) {
    try { const m = await createUniqueDiscount(offer.pct, offer.expiresDays, email); code = m.code; endsAtMs = Date.parse(m.endsAt); }
    catch (err) { console.error("[emailCapture] mint failed:", err.message); }
  }

  const tags = ["email-capture", "newsletter"];
  if (audience) tags.push("theme-" + audience);
  if (ctx.productType) tags.push("interest:" + String(ctx.productType).toLowerCase().slice(0, 40));
  let customerId = null, created = false;
  try {
    const cu = await upsertCustomer(email, tags);
    customerId = cu.id; created = cu.created;
  } catch (err) {
    console.error("[emailCapture] customer sync failed:", err.message);
    // capture is still logged; Shopify sync can be replayed from the log
  }

  await ref.set({
    email, code, pct: offer.pct, audience: audience || null,
    context: { path: String(ctx.path || "").slice(0, 200), productTitle: String(ctx.productTitle || "").slice(0, 120),
               productType: String(ctx.productType || "").slice(0, 60), collectionTitle: String(ctx.collectionTitle || "").slice(0, 80) },
    customerId, customerCreated: created,
    at: Date.now(), createdAt: new Date().toISOString(),
    expiresAt: endsAtMs
  });

  return { ok: true, code, pct: offer.pct, expiresDays: offer.expiresDays, already: false };
}

async function tagAudience(body) {
  const email = normEmail(body.email);
  const audience = String(body.audience || "").replace(/[^a-z-]/gi, "").slice(0, 30);
  if (!email || !audience) return { ok: false };
  const ref = db.collection(CAPTURES).doc(email);
  await ref.set({ audience }, { merge: true }).catch(() => {});
  try {
    const q = await gql(`{ customers(first: 1, query: ${JSON.stringify("email:" + email)}) { nodes { id } } }`);
    const id = q.customers && q.customers.nodes[0] && q.customers.nodes[0].id;
    if (id) await gql(`
      mutation($id: ID!, $tags: [String!]!) {
        tagsAdd(id: $id, tags: $tags) { userErrors { field message } }
      }`, { id, tags: ["theme-" + audience] });
  } catch (e) {}
  return { ok: true };
}

/* --------------------------------- handler --------------------------------- */

exports.handler = async (event) => {
  const origin = (event.headers && (event.headers.origin || event.headers.Origin)) || "";
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: corsHeaders(origin), body: "" };

  try {
    if (event.httpMethod === "GET") {
      const op = (event.queryStringParameters || {}).op || "offer";
      if (op === "offer") {
        const o = await getOffer();
        return out(200, { ok: true, pct: o.pct, enabled: o.enabled, expiresDays: o.expiresDays,
                          headline: o.headline, subheading: o.subheading }, origin);
      }
      return out(400, { ok: false, error: "unknown op" }, origin);
    }

    if (event.httpMethod !== "POST") return out(405, { ok: false, error: "method" }, origin);
    let body = {};
    try { body = JSON.parse(event.body || "{}"); } catch (e) {}
    const ip = (event.headers && (event.headers["x-forwarded-for"] || "")).split(",")[0].trim() || "?";

    if (body.op === "subscribe") return out(200, await subscribe(body, ip), origin);
    if (body.op === "tag") return out(200, await tagAudience(body), origin);
    return out(400, { ok: false, error: "unknown op" }, origin);
  } catch (e) {
    console.error("[emailCapture]", e);
    return out(200, { ok: false, error: "Something went sideways — please try again." }, origin);
  }
};
