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
  pct: 15,
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
        customerGets: { value: { percentage: pct / 100 }, items: { all: true } },
        // welcome codes must stack with the site-wide FREESHIP automatic
        // discount; without this the checkout forces an either/or choice
        combinesWith: { shippingDiscounts: true, productDiscounts: false, orderDiscounts: false }
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

/* ------------------------------ welcome email ------------------------------ */
// Sent via Resend (env RESEND_API_KEY + BRITES_EMAIL_FROM, e.g.
// "Brites Jewelry <hello@britesjewelry.com>"). Carries the SAME code the
// shopper already saw, with a /discount deep link that auto-applies it at
// checkout — one tap, no typing. Failure never blocks the signup.
const SITE = "https://britesjewelry.com";
function welcomeEmailHTML({ code, pct, expiresDays }) {
  const applyUrl = SITE + "/discount/" + encodeURIComponent(code) + "?redirect=%2Fcollections%2Fbest-sellers";
  const tile = (label, path) =>
    `<td align="center" style="padding:0 5px"><a href="${SITE}${path}" style="display:block;padding:11px 6px;border:1px solid #ddd6c8;border-radius:9px;color:#3a352d;text-decoration:none;font-size:13px;font-family:Georgia,serif">${label}</a></td>`;
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"></head>
<body style="margin:0;padding:0;background:#f4f1ea">
<div style="display:none;max-height:0;overflow:hidden">Your ${pct}% welcome code is inside — one tap and it's applied. ✨</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f4f1ea;padding:28px 12px">
<tr><td align="center">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background:#ffffff;border-radius:14px;overflow:hidden;border:1px solid #e8e2d4">
  <tr><td align="center" style="padding:30px 24px 6px;font-family:Georgia,'Times New Roman',serif">
    <div style="font-size:22px;letter-spacing:.02em;color:#211d16">Brites <span style="color:#b08d3f">·</span> Jewelry</div>
    <div style="font-size:10.5px;letter-spacing:.24em;color:#9a9284;margin-top:4px">HANDMADE · PERSONAL · MEANT TO BE KEPT</div>
  </td></tr>
  <tr><td align="center" style="padding:22px 34px 6px;font-family:Georgia,serif">
    <div style="font-size:28px;line-height:1.2;color:#211d16">Welcome to the family 💛</div>
    <div style="font-size:15px;line-height:1.55;color:#5c564b;margin-top:10px">Every charm we make carries a little story. Here's ${pct}% off the first one you'll tell — our thank-you for joining.</div>
  </td></tr>
  <tr><td align="center" style="padding:20px 34px 4px">
    <table role="presentation" cellpadding="0" cellspacing="0" style="width:100%;border:1.5px dashed #c9bfa8;border-radius:11px;background:#faf8f2">
      <tr><td align="center" style="padding:16px 12px 6px;font-family:Georgia,serif;font-size:12px;letter-spacing:.18em;color:#9a9284">YOUR PERSONAL CODE</td></tr>
      <tr><td align="center" style="padding:0 12px 4px;font-family:'Courier New',monospace;font-size:26px;font-weight:bold;letter-spacing:.06em;color:#211d16">${code}</td></tr>
      <tr><td align="center" style="padding:0 12px 16px;font-family:Georgia,serif;font-size:12px;color:#9a9284">one use, just for you · valid ${expiresDays} days</td></tr>
    </table>
  </td></tr>
  <tr><td align="center" style="padding:18px 34px 6px">
    <a href="${applyUrl}" style="display:inline-block;background:#211d16;color:#f6f1e4;text-decoration:none;font-family:Georgia,serif;font-size:14px;letter-spacing:.14em;padding:15px 34px;border-radius:9px">SHOP WITH ${pct}% APPLIED →</a>
    <div style="font-family:Georgia,serif;font-size:11.5px;color:#9a9284;margin-top:9px">One tap — your code applies automatically at checkout. Or copy it above.</div>
  </td></tr>
  <tr><td style="padding:24px 30px 8px">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
      ${tile("Necklaces","/collections/necklaces-personalized")}
      ${tile("Bracelets","/collections/bracelets")}
      ${tile("Charms","/collections/charm-only")}
    </tr></table>
  </td></tr>
  <tr><td align="center" style="padding:18px 34px 26px;font-family:Georgia,serif;font-size:12.5px;line-height:1.6;color:#8a8478">
    Made to order in Toronto · on the bench within 24h · 30-day returns<br>
    Questions? Just reply — a real jeweler answers.
  </td></tr>
</table>
<div style="max-width:600px;font-family:Georgia,serif;font-size:11px;color:#a49c8d;padding:16px 8px;text-align:center;line-height:1.6">
  You're receiving this because you signed up at britesjewelry.com.<br>
  Brites Jewelry · Toronto, Canada · <a href="${SITE}/pages/contact" style="color:#a49c8d">Contact</a> · reply STOP to unsubscribe
</div>
</td></tr></table></body></html>`;
}

async function sendWelcomeEmail(email, { code, pct, expiresDays }) {
  const key = process.env.RESEND_API_KEY;
  if (!key || !code) return false;
  try {
    const res = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: { "Authorization": "Bearer " + key, "Content-Type": "application/json" },
      body: JSON.stringify({
        from: process.env.BRITES_EMAIL_FROM || "Brites Jewelry <hello@britesjewelry.com>",
        to: [email],
        subject: "Your " + pct + "% welcome treat is inside \u2728",
        html: welcomeEmailHTML({ code, pct, expiresDays })
      })
    });
    return res.ok;
  } catch (e) { console.error("[emailCapture] email send failed:", e.message); return false; }
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
        const emailedR = await sendWelcomeEmail(email, { code: fresh.code, pct: offer.pct, expiresDays: offer.expiresDays });
        return { ok: true, already: true, renewed: true, code: fresh.code, pct: offer.pct, expiresDays: offer.expiresDays, emailed: emailedR };
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

  const emailed = await sendWelcomeEmail(email, { code, pct: offer.pct, expiresDays: offer.expiresDays });
  return { ok: true, code, pct: offer.pct, expiresDays: offer.expiresDays, already: false, emailed };
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
      if (op === "discountAudit") {
        // Public-safe: reports ONLY Automatic discounts (shoppers see those
        // titles in every cart anyway). Code-discount titles (which contain
        // signup emails) are never selected. Self-diagnosing: real error +
        // hint instead of a generic failure.
        try {
          const d = await gql(`{
            discountNodes(first: 50, query: "status:active") {
              nodes { id discount { __typename
                ... on DiscountAutomaticBasic { title status endsAt
                  customerGets { value { ... on DiscountPercentage { percentage } } } }
                ... on DiscountAutomaticBxgy { title status endsAt }
                ... on DiscountAutomaticFreeShipping { title status endsAt } } }
            } }`);
          const rows = (d.discountNodes.nodes || [])
            .filter(n => /^DiscountAutomatic/.test((n.discount || {}).__typename || ""))
            .map(n => ({
              id: n.id, kind: n.discount.__typename.replace("DiscountAutomatic", "Automatic "),
              title: n.discount.title, status: n.discount.status,
              pct: n.discount.customerGets && n.discount.customerGets.value && n.discount.customerGets.value.percentage != null
                   ? Math.round(n.discount.customerGets.value.percentage * 100) : undefined,
              endsAt: n.discount.endsAt || null
            }));
          return out(200, {
            ok: true,
            automaticDiscounts: rows,
            verdict: rows.length
              ? "These apply to EVERY cart automatically and block code-based discounts from combining. Deactivate them in Shopify Admin \u2192 Discounts to stop auto-applying."
              : "Clean \u2014 no active automatic discounts; only typed codes can discount a cart."
          }, origin);
        } catch (e) {
          const msg = String(e.message || e);
          const scopeIssue = /ACCESS_DENIED|access denied|not approved|required scope/i.test(msg);
          return out(200, {
            ok: false,
            error: msg.slice(0, 300),
            hint: scopeIssue
              ? "The Shopify app behind SHOPIFY_CLIENT_ID is missing discount scopes. In Shopify Admin \u2192 Settings \u2192 Apps and sales channels \u2192 Develop apps \u2192 your app \u2192 Configuration: add read_discounts AND write_discounts, save, then reinstall/re-approve the app. write_discounts is also required for minting welcome codes."
              : "Check SHOPIFY_STORE / SHOPIFY_CLIENT_ID / SHOPIFY_CLIENT_SECRET env vars on this Netlify site and the function logs."
          }, origin);
        }
      }
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
    const detail = event.httpMethod === "GET" ? String(e.message || e).slice(0, 250) : undefined;
    return out(200, { ok: false, error: "Something went sideways — please try again.", detail }, origin);
  }
};
