// netlify/functions/productReviewsFeed.js
// ---------------------------------------------------------------------------
// Brites — Google Product Ratings feed exporter.
//
// Emits a Google Product Review Feed (XML, schema v2.4) so Merchant Center can
// show 1–5 star ratings on your product listings / Shopping ads.
//
// Single source of truth: the SAME Firestore that powers reviews.js
//   Brites_Reviews/{handle}/items/{reviewId}   -> { r, n, d, b, v, s, ... }
// We read every item via a collection-group query and emit ONLY approved ones
// (s == "approved", or imported items which are stored approved). This means the
// feed automatically contains BOTH the historical Etsy reviews (once imported)
// AND ongoing on-site reviews, always fresh, and it respects your moderation.
//
// Google fetches this URL on a schedule, so this is a plain HTTP GET endpoint
// (no Netlify schedule, no passcode — the data is public review content; no PII
// is ever read or emitted, exactly like reviews.js "global").
//
// Stable feed URL (point the Merchant Center "Product reviews" data source here):
//   https://goldenspike.app/.netlify/functions/productReviewsFeed
//
// Modes:
//   GET  (default)        -> the full XML feed (gzipped when the client sends
//                            Accept-Encoding: gzip — Google's fetcher does)
//   GET  ?stats=1         -> small JSON summary (counts, by-rating, products,
//                            byte size) for sanity-checking WITHOUT dumping ~MBs
//   GET  ?pretty=1        -> human-readable indented XML (testing only)
//
// Google rules this satisfies automatically:
//   • "Full feed every time" — we always emit every approved review, so nothing
//     gets silently dropped/deleted by Google between fetches.
//   • "Refresh at least monthly" — Google's scheduled fetch re-pulls live data.
//   • "product_url domain must match the registered Merchant Center domain" —
//     all product URLs are on https://britesjewelry.com.
//   • "Include all reviews (not just 5-star)" — we emit ratings 1–5 verbatim.
//
// Required env: FIREBASE_* (consumed by ./firebaseAdmin — nothing new). No
// Shopify/admin creds are needed; this function never calls Shopify.
// ---------------------------------------------------------------------------

const zlib = require("zlib");

/* ─── Firebase (shared admin module, identical to reviews.js) ────────────── */
let _fb = null;
function fb() {
  if (_fb !== null) return _fb;
  try {
    const admin = require("./firebaseAdmin");
    _fb = { admin, db: admin.firestore() };
  } catch (e) {
    console.error("[productReviewsFeed] Firebase unavailable:", e.message);
    _fb = false;
  }
  return _fb;
}

/* ─── Config ─────────────────────────────────────────────────────────────── */
const SHOP_URL      = "https://britesjewelry.com";   // must match Merchant Center domain
const BRAND         = "Brites Jewelry";
const PUBLISHER     = "Brites Jewelry";
const FAVICON       = SHOP_URL + "/favicon.ico";
const FEED_VERSION  = "2.4";
const SCHEMA_URL    = "http://www.google.com/shopping/reviews/schema/product/2.4/product_reviews.xsd";
const CACHE_MS      = 60 * 1000;                      // tiny cache for repeat test fetches

/* ─── Helpers ────────────────────────────────────────────────────────────── */
function clean(s) { return String(s == null ? "" : s).replace(/\s+/g, " ").trim(); }
function clampRating(n) { n = parseInt(n, 10); return (n >= 1 && n <= 5) ? n : 0; }

// Strip characters illegal in XML 1.0, then escape the five XML metacharacters.
function xmlEsc(s) {
  return String(s == null ? "" : s)
    .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

// Accept both "YYYY-MM-DD" (on-site reviews) and "MM/DD/YYYY" (Etsy import)
// and return an RFC 3339 timestamp. Noon UTC avoids any timezone date-shift.
function toRFC3339(d) {
  const s = clean(d);
  if (!s) return null;
  const pad = n => String(n).padStart(2, "0");
  let y, m, day;
  let iso = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  let us  = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (iso)      { y = +iso[1]; m = +iso[2]; day = +iso[3]; }
  else if (us)  { m = +us[1];  day = +us[2]; y = +us[3]; }
  else {
    const dt = new Date(s);
    if (isNaN(dt.getTime())) return null;
    return dt.toISOString().replace(/\.\d{3}Z$/, "Z");
  }
  if (!y || !m || !day || m > 12 || day > 31) return null;
  return `${y}-${pad(m)}-${pad(day)}T12:00:00Z`;
}

function productUrl(handle) { return SHOP_URL + "/products/" + clean(handle); }

// Build one <review> block in the exact element order required by the 2.4 XSD.
// Returns "" if the record can't form a valid review (skipped upstream).
function reviewBlock({ id, name, verified, ts, body, rating, handle }) {
  const url = productUrl(handle);
  const anon = !name || name.toLowerCase() === "anonymous";
  const nameTag = anon
    ? `<name is_anonymous="true">Anonymous</name>`
    : `<name>${xmlEsc(name)}</name>`;
  const verifiedTag = verified ? `<is_verified_purchase>true</is_verified_purchase>` : "";
  return (
    `<review>` +
      `<review_id>${xmlEsc(id)}</review_id>` +
      `<reviewer>${nameTag}</reviewer>` +
      verifiedTag +
      `<review_timestamp>${ts}</review_timestamp>` +
      `<content>${xmlEsc(body)}</content>` +
      `<ratings><overall min="1" max="5">${rating}</overall></ratings>` +
      `<products><product>` +
        `<product_ids><brands><brand>${xmlEsc(BRAND)}</brand></brands></product_ids>` +
        `<product_url>${xmlEsc(url)}</product_url>` +
      `</product></products>` +
    `</review>`
  );
}

/* ─── Core: read Firestore, build feed + stats ───────────────────────────── */
async function buildFeed(pretty) {
  const F = fb();
  if (!F) throw new Error("Firebase unavailable");

  // Same collection-group read reviews.js "global" uses. Unfiltered so we don't
  // need a custom collection-group index; we filter to approved in memory.
  const snap = await F.db.collectionGroup("items").get();

  const blocks = [];
  const byRating = { "1": 0, "2": 0, "3": 0, "4": 0, "5": 0 };
  const products = new Set();
  let total = 0, included = 0;
  const skipped = { notApproved: 0, badRating: 0, noHandle: 0, noBody: 0, badDate: 0 };

  snap.forEach(doc => {
    total++;
    const d = doc.data() || {};
    if (d.s && d.s !== "approved") { skipped.notApproved++; return; }

    const rating = clampRating(d.r);
    if (!rating) { skipped.badRating++; return; }

    const handle = doc.ref.parent.parent ? doc.ref.parent.parent.id : "";
    if (!handle) { skipped.noHandle++; return; }

    const body = clean(d.b);
    if (!body) { skipped.noBody++; return; }       // content is required for a usable review

    const ts = toRFC3339(d.d);
    if (!ts) { skipped.badDate++; return; }

    const name = clean(d.n) || "Anonymous";
    blocks.push(reviewBlock({
      id: doc.id, name, verified: d.v ? 1 : 0, ts, body, rating, handle
    }));
    included++;
    byRating[String(rating)]++;
    products.add(handle);
  });

  const nl = pretty ? "\n" : "";
  const ind = pretty ? "  " : "";
  const reviewsXml = pretty
    ? blocks.map(b => ind + b).join(nl)
    : blocks.join("");

  const xml =
    `<?xml version="1.0" encoding="UTF-8"?>${nl}` +
    `<feed xmlns:vc="http://www.w3.org/2007/XMLSchema-versioning" ` +
    `xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ` +
    `xsi:noNamespaceSchemaLocation="${SCHEMA_URL}">${nl}` +
    `<version>${FEED_VERSION}</version>${nl}` +
    `<publisher><name>${xmlEsc(PUBLISHER)}</name><favicon>${xmlEsc(FAVICON)}</favicon></publisher>${nl}` +
    `<reviews>${nl}` +
    reviewsXml + nl +
    `</reviews>${nl}` +
    `</feed>${nl}`;

  const stats = {
    ok: true,
    total_items_scanned: total,
    reviews_included: included,
    unique_products: products.size,
    by_rating: byRating,
    skipped,
    feed_bytes: Buffer.byteLength(xml, "utf8"),
    schema_version: FEED_VERSION,
    generated_at: new Date().toISOString()
  };
  return { xml, stats };
}

/* ─── small in-memory cache (helps repeat test fetches; short TTL) ───────── */
let _cache = null, _cacheExp = 0;
async function getFeed(pretty) {
  if (pretty) return buildFeed(true);            // never cache the pretty/test variant
  const now = Date.now();
  if (_cache && now < _cacheExp) return _cache;
  _cache = await buildFeed(false);
  _cacheExp = now + CACHE_MS;
  return _cache;
}

/* ─── HTTP handler ───────────────────────────────────────────────────────── */
function json(code, obj) {
  return {
    statusCode: code,
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
    body: JSON.stringify(obj, null, 2)
  };
}

exports.handler = async (event) => {
  const q = (event && event.queryStringParameters) || {};
  try {
    const pretty = q.pretty === "1" || q.pretty === "true";
    const { xml, stats } = await getFeed(pretty);

    if (q.stats === "1" || q.stats === "true") return json(200, stats);

    const headers = (event && event.headers) || {};
    const ae = String(headers["accept-encoding"] || headers["Accept-Encoding"] || "");
    const baseHeaders = {
      "Content-Type": "application/xml; charset=utf-8",
      "Cache-Control": "public, max-age=300"
    };

    // Gzip when the client accepts it (Google's feed fetcher does). Keeps us far
    // under Netlify's ~6 MB function-response limit even as reviews scale up.
    if (/\bgzip\b/i.test(ae) && !pretty) {
      const gz = zlib.gzipSync(Buffer.from(xml, "utf8"));
      return {
        statusCode: 200,
        headers: Object.assign({}, baseHeaders, { "Content-Encoding": "gzip" }),
        body: gz.toString("base64"),
        isBase64Encoded: true
      };
    }
    return { statusCode: 200, headers: baseHeaders, body: xml };
  } catch (e) {
    console.error("[productReviewsFeed] error:", e && e.message);
    return json(500, { error: "Server error", detail: String((e && e.message) || e) });
  }
};
