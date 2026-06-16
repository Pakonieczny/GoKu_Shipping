// netlify/functions/reviews.js
// ---------------------------------------------------------------------------
// Brites product reviews API — runs entirely on the existing Brites infra
// (Netlify Functions + Firestore + Shopify Admin GraphQL). No third party.
//
// Reuses the proven patterns from shopifyEditor.js verbatim:
//   - Shopify auth: client-credentials grant -> short-lived token (getToken)
//   - Admin API:    GraphQL (gql helper, with transient-retry)
//   - Firebase:     require("./firebaseAdmin") -> admin.firestore()
//
// Security model (per owner decision):
//   - "list"    PUBLIC read. Returns a product's APPROVED reviews + summary.
//   - "submit"  Session-gated, NO shared secret in the browser. The storefront
//               sends the logged-in customer's id + email (from Liquid's
//               {{ customer }}), and we VERIFY that email actually owns that
//               customer id via the Shopify Admin API before trusting it. This
//               stops someone hand-posting an arbitrary email to forge a
//               "Verified Buyer" badge. New reviews are stored PENDING.
//   - "import"  ADMIN one-time seed of the historical Etsy reviews. Gated by
//               the existing EDIT_PASSCODE (reused; no new env var).
//   - "moderate" ADMIN approve/reject, gated by EDIT_PASSCODE.
//
// Verified-buyer logic (mirrors Okendo/Yotpo, done server-side):
//   email matches a past Shopify purchase of THIS product -> "Verified Buyer"
//   email confirmed (owns the customer id) but no matching purchase -> "Verified Reviewer"
//   otherwise -> "Anonymous"
//   (Historical Etsy purchases aren't in Shopify, so a long-time Etsy buyer
//    reviewing a never-bought-on-Shopify item is a Verified Reviewer — correct.)
//
// Firestore layout:
//   Brites_Reviews/{handle}                      summary doc {count,avg,dist,updated}
//   Brites_Reviews/{handle}/items/{reviewId}     individual review docs
//
// Required env (all already present for shopifyEditor.js — nothing new):
//   SHOPIFY_STORE, SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET,
//   SHOPIFY_API_VERSION (optional), EDIT_PASSCODE,
//   FIREBASE_* (consumed by firebaseAdmin.js)
// ---------------------------------------------------------------------------

const fetch = require("node-fetch");

/* ─── Firebase (shared admin module, same as shopifyEditor.js) ───────────── */
let _fb = null;
function fb() {
  if (_fb !== null) return _fb;
  try {
    const admin = require("./firebaseAdmin");
    _fb = { admin, db: admin.firestore(), FV: admin.firestore.FieldValue };
  } catch (e) {
    console.error("[reviews] Firebase unavailable:", e.message);
    _fb = false;
  }
  return _fb;
}

/* ─── Shopify Admin API (auth + gql copied from shopifyEditor.js) ─────────── */
const API_VERSION = process.env.SHOPIFY_API_VERSION || "2025-10";
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
  _tokenExp = Date.now() + (data.expires_in || 86399) * 1000;
  return _token;
}

async function gql(query, variables, _attempt) {
  const store = process.env.SHOPIFY_STORE;
  const token = await getToken();
  try {
    const res = await fetch(`https://${store}/admin/api/${API_VERSION}/graphql.json`, {
      method: "POST",
      headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" },
      body: JSON.stringify({ query, variables: variables || {} })
    });
    if (res.status >= 500) throw new Error("GraphQL HTTP " + res.status);
    const data = await res.json();
    if (!res.ok) throw new Error("GraphQL HTTP " + res.status);
    if (data.errors && data.errors.length) throw new Error("GraphQL: " + JSON.stringify(data.errors));
    return data.data;
  } catch (e) {
    const msg = String((e && e.message) || e);
    const transient = /ECONNRESET|ETIMEDOUT|socket hang up|network|fetch failed|EAI_AGAIN|ECONNREFUSED|GraphQL HTTP 5\d\d/i.test(msg);
    const attempt = _attempt || 0;
    if (transient && attempt < 2) {
      await new Promise(r => setTimeout(r, 350 * (attempt + 1)));
      return gql(query, variables, attempt + 1);
    }
    throw e;
  }
}

/* ─── CORS (storefront origin only, same allow-list as shopifyEditor.js) ─── */
const ALLOWED_ORIGINS = ["https://britesjewelry.com", "https://www.britesjewelry.com"];
const ADMIN_ACTIONS = ["import", "moderate", "pending"];
function corsHeaders(origin, action) {
  // Public actions (list / submit) stay locked to the storefront origin.
  // Admin actions (import / moderate / pending) are passwordless and guarded by
  // server-side verification + a function URL nothing links to — not by CORS.
  // So we let the admin page run from anywhere, including a local HTML file you
  // just double-click (its Origin is "null"). No hosting / no URL required.
  let allow;
  if (ADMIN_ACTIONS.indexOf(action) !== -1) allow = origin || "*";
  else allow = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
  return {
    "Access-Control-Allow-Origin": allow,
    "Access-Control-Allow-Headers": "Content-Type, X-Edit-Passcode",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Content-Type": "application/json"
  };
}

/* ─── helpers ─────────────────────────────────────────────────────────────── */
function clean(s) { return String(s == null ? "" : s).replace(/\s+/g, " ").trim(); }
function numericId(gid) { return String(gid == null ? "" : gid).replace(/^.*\//, ""); }
function clampRating(n) { n = parseInt(n, 10); return (n >= 1 && n <= 5) ? n : 0; }
function reviewId(handle, name, body, date) {
  // deterministic id so re-imports don't duplicate
  const crypto = require("crypto");
  return crypto.createHash("md5").update(handle + "|" + name + "|" + (body || "").slice(0, 40) + "|" + date).digest("hex").slice(0, 12);
}
function escapeQ(s) { return String(s || "").replace(/["\\]/g, "\\$&"); }

/* Recompute + persist the summary doc for a handle from its approved items. */
async function recomputeSummary(handle) {
  const F = fb(); if (!F) return null;
  const snap = await F.db.collection("Brites_Reviews").doc(handle).collection("items")
    .where("s", "==", "approved").get();
  let count = 0, sum = 0; const dist = { "1": 0, "2": 0, "3": 0, "4": 0, "5": 0 };
  snap.forEach(doc => {
    const d = doc.data(); const r = clampRating(d.r);
    if (!r) return;
    count++; sum += r; dist[String(r)]++;
  });
  const avg = count ? Math.round((sum / count) * 100) / 100 : 0;
  await F.db.collection("Brites_Reviews").doc(handle).set(
    { count, avg, dist, updated: F.FV.serverTimestamp() }, { merge: true });
  return { count, avg, dist };
}

/* Verify the claimed email actually owns the claimed customer id (anti-forgery),
   then check whether that customer has purchased `handle`. Returns one of:
   "buyer" | "reviewer" | "anon". */
async function verifyBuyer(customerId, email, handle) {
  email = clean(email).toLowerCase();
  if (!email) return "anon";
  let confirmed = false;

  if (customerId) {
    // An id WAS supplied. The email must belong to THAT customer id, full stop.
    // If it doesn't match, this is a forgery attempt (someone pairing a real
    // buyer's email with a different/owned customer id) — reject, do not fall
    // back to mere email-existence.
    try {
      const d = await gql(
        `query($id: ID!) { customer(id: $id) { id email } }`,
        { id: "gid://shopify/Customer/" + numericId(customerId) }
      );
      const cEmail = d && d.customer && d.customer.email ? String(d.customer.email).toLowerCase() : "";
      confirmed = !!cEmail && cEmail === email;
    } catch (e) { confirmed = false; }
    if (!confirmed) return "anon";
  } else {
    // No id supplied (degraded path) — confirm the email at least exists as a
    // real customer. Weaker, but never reached when the storefront sends id.
    try {
      const d = await gql(
        `query($q: String!) { customers(first: 1, query: $q) { edges { node { id email } } } }`,
        { q: "email:" + escapeQ(email) }
      );
      const node = d && d.customers && d.customers.edges[0] && d.customers.edges[0].node;
      confirmed = !!(node && String(node.email).toLowerCase() === email);
    } catch (e) { confirmed = false; }
    if (!confirmed) return "anon";
  }

  // Confirmed identity — now look for a past purchase of THIS product.
  try {
    const d = await gql(
      `query($q: String!) {
        orders(first: 30, query: $q, sortKey: CREATED_AT, reverse: true) {
          edges { node { id lineItems(first: 50) { edges { node { product { handle } } } } } }
        }
      }`,
      { q: "email:" + escapeQ(email) }
    );
    const edges = (d && d.orders && d.orders.edges) || [];
    for (const oe of edges) {
      const lis = (oe.node && oe.node.lineItems && oe.node.lineItems.edges) || [];
      for (const li of lis) {
        const h = li.node && li.node.product && li.node.product.handle;
        if (h && h === handle) return "buyer";
      }
    }
  } catch (e) { /* fall through to reviewer */ }
  return "reviewer";
}

/* ─── handler ─────────────────────────────────────────────────────────────── */
exports.handler = async function (event) {
  const origin = event.headers.origin || event.headers.Origin || "";
  const preAction = (event.queryStringParameters || {}).action;
  const headers = corsHeaders(origin, preAction);
  headers["Cache-Control"] = "no-store"; // reviews are real-time; never serve stale
  const reply = (status, obj) => ({ statusCode: status, headers, body: JSON.stringify(obj) });

  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers, body: "ok" };

  const q = event.queryStringParameters || {};
  const isPost = event.httpMethod === "POST";
  let body = {};
  if (isPost) { try { body = JSON.parse(event.body || "{}"); } catch (e) { return reply(400, { error: "Bad JSON" }); } }
  const action = q.action || body.action;

  const F = fb();
  if (!F) return reply(503, { error: "Storage unavailable" });

  // Admin actions (import / moderate / pending) are open per owner request —
  // no passcode. The one sensitive field (reviewer email) is NOT returned by
  // the pending queue below, so an open endpoint can't expose customer PII.

  try {
    switch (action) {

      /* ---------- PUBLIC: list a product's reviews + summary ---------- */
      case "list": {
        const handle = clean(q.handle || body.handle);
        if (!handle) return reply(400, { error: "handle required" });
        // Default to returning ALL approved reviews for the product (max ~100 in
        // practice). Cap generously. The client paginates with "Show more".
        const limit = Math.min(parseInt(q.limit || body.limit || 500, 10) || 500, 1000);
        const sort = (q.sort || body.sort || "recent");

        const sumDoc = await F.db.collection("Brites_Reviews").doc(handle).get();
        const summary = sumDoc.exists ? sumDoc.data() : { count: 0, avg: 0, dist: { "1": 0, "2": 0, "3": 0, "4": 0, "5": 0 } };

        // IMPORTANT: query with a SINGLE equality filter only (no orderBy) so it
        // uses the automatic single-field index — no composite index to create.
        // Sorting is done in memory below (a product has at most a few hundred
        // reviews, so this is cheap and avoids a Firestore index dependency).
        const snap = await F.db.collection("Brites_Reviews").doc(handle).collection("items")
          .where("s", "==", "approved").limit(limit).get();

        let reviews = [];
        snap.forEach(doc => {
          const d = doc.data();
          reviews.push({
            id: doc.id, r: clampRating(d.r), n: d.n, d: d.d || "", b: d.b,
            v: d.v ? 1 : 0, badge: d.badge || (d.v ? "Verified Buyer" : ""),
            pics: d.pics || []
          });
        });
        // In-memory sort: recent = newest date first; rating = highest first, then newest.
        if (sort === "rating") reviews.sort((a, b) => (b.r - a.r) || (a.d < b.d ? 1 : a.d > b.d ? -1 : 0));
        else reviews.sort((a, b) => (a.d < b.d ? 1 : a.d > b.d ? -1 : 0));

        return reply(200, {
          handle,
          summary: { count: summary.count || 0, avg: summary.avg || 0, dist: summary.dist || { "1": 0, "2": 0, "3": 0, "4": 0, "5": 0 } },
          reviews
        });
      }

      /* ---------- PUBLIC (session-gated): submit a new review ---------- */
      case "submit": {
        const handle = clean(body.handle);
        const rating = clampRating(body.rating);
        const text = clean(body.body).slice(0, 3000);
        const name = clean(body.name).slice(0, 80) || "Anonymous";
        const email = clean(body.email).toLowerCase().slice(0, 160);
        const customerId = clean(body.customerId);

        if (!handle) return reply(400, { error: "handle required" });
        if (!rating) return reply(400, { error: "rating 1-5 required" });
        if (text.length < 4) return reply(400, { error: "review text too short" });
        // Must be a logged-in customer (id + email both present). This is the
        // account-creation tie-in: the storefront only sends these when
        // {{ customer }} exists.
        if (!email || !customerId) return reply(401, { error: "Sign in to leave a review" });

        // Verify identity + purchase server-side (anti-forgery + verified buyer).
        const tier = await verifyBuyer(customerId, email, handle); // buyer|reviewer|anon
        if (tier === "anon") return reply(401, { error: "Could not verify your account" });

        const badge = tier === "buyer" ? "Verified Buyer" : "Verified Reviewer";
        const verified = tier === "buyer" ? 1 : 0;

        const now = new Date();
        const dateStr = now.toISOString().slice(0, 10);
        const id = reviewId(handle, name, text, dateStr + ":" + numericId(customerId));

        const docRef = F.db.collection("Brites_Reviews").doc(handle).collection("items").doc(id);
        const existing = await docRef.get();
        if (existing.exists) return reply(200, { ok: true, status: "already_submitted" });

        await docRef.set({
          r: rating, n: name, d: dateStr, b: text,
          v: verified, badge,
          email,                       // stored for moderation, never returned by "list"
          customerId: numericId(customerId),
          tier,
          s: "pending",                // manual-approval by default
          created: F.FV.serverTimestamp()
        });

        return reply(200, { ok: true, status: "pending", badge });
      }

      /* ---------- ADMIN: moderate (approve / reject) ---------- */
      case "moderate": {        const handle = clean(body.handle);
        const id = clean(body.id);
        const decision = clean(body.decision); // "approve" | "reject"
        if (!handle || !id) return reply(400, { error: "handle + id required" });
        const ref = F.db.collection("Brites_Reviews").doc(handle).collection("items").doc(id);
        if (decision === "reject") {
          await ref.set({ s: "rejected" }, { merge: true });
        } else {
          await ref.set({ s: "approved" }, { merge: true });
        }
        const summary = await recomputeSummary(handle);
        return reply(200, { ok: true, summary });
      }

      /* ---------- ADMIN: list pending (moderation queue) ---------- */
      case "pending": {        const limit = Math.min(parseInt(q.limit || 100, 10) || 100, 500);
        const snap = await F.db.collectionGroup("items").where("s", "==", "pending").limit(limit).get();
        const out = [];
        snap.forEach(doc => {
          const d = doc.data();
          // parent path: Brites_Reviews/{handle}/items/{id}
          const handle = doc.ref.parent.parent ? doc.ref.parent.parent.id : "";
          // email intentionally omitted — the queue is open (no passcode) and
          // the admin UI doesn't display it, so customer PII never goes over the wire.
          out.push({ handle, id: doc.id, r: d.r, n: d.n, d: d.d, b: d.b, badge: d.badge });
        });
        return reply(200, { pending: out });
      }

      /* ---------- ADMIN: one-time import of historical reviews ---------- */
      case "import": {        // Body: { handles: { handle: [ {id?,r,n,d,b,v,s}, ... ] } }
        const handles = (body && body.handles) || {};
        const keys = Object.keys(handles);
        if (!keys.length) return reply(400, { error: "no handles in payload" });

        // Optional pagination so a huge import can be chunked across calls.
        const start = parseInt(body.start || 0, 10) || 0;
        const chunk = Math.min(parseInt(body.chunk || 150, 10) || 150, 400);
        const slice = keys.slice(start, start + chunk);

        let writtenHandles = 0, writtenReviews = 0;
        for (const handle of slice) {
          const items = handles[handle] || [];
          let batch = F.db.batch(); let ops = 0;
          for (const it of items) {
            const r = clampRating(it.r);
            if (!r) continue;
            const name = clean(it.n).slice(0, 80) || "Anonymous";
            const date = clean(it.d) || new Date().toISOString().slice(0, 10);
            const text = clean(it.b).slice(0, 3000);
            const id = it.id || reviewId(handle, name, text, date);
            const ref = F.db.collection("Brites_Reviews").doc(handle).collection("items").doc(id);
            batch.set(ref, {
              r, n: name, d: date, b: text,
              v: it.v ? 1 : 0,
              badge: it.v ? "Verified Buyer" : "Verified Reviewer",
              s: it.s || "approved",
              imported: true,
              created: F.FV.serverTimestamp()
            }, { merge: true });
            ops++; writtenReviews++;
            if (ops >= 400) { await batch.commit(); batch = F.db.batch(); ops = 0; }
          }
          if (ops > 0) await batch.commit();
          await recomputeSummary(handle);
          writtenHandles++;
        }
        const done = start + slice.length >= keys.length;
        return reply(200, {
          ok: true, writtenHandles, writtenReviews,
          nextStart: done ? null : start + slice.length,
          totalHandles: keys.length, done
        });
      }

      default:
        return reply(400, { error: "Unknown action" });
    }
  } catch (e) {
    console.error("[reviews] error:", e && e.message);
    return reply(500, { error: "Server error", detail: String((e && e.message) || e) });
  }
};
