// netlify/functions/productReviewsFeed.js
// ---------------------------------------------------------------------------
// Brites — Google Product Ratings feed exporter.
//
// Emits a Google Product Review Feed (XML, schema v2.4) so Merchant Center can
// show 1–5 star ratings on your product listings / Shopping ads.
//
// Review source (single source of truth): the SAME Firestore that powers
// reviews.js — Brites_Reviews/{handle}/items/{reviewId} -> { r, n, d, b, v, s }.
// We read every item via a collection-group query and emit ONLY approved ones.
//
// Product identifiers (NEW): read from Brites_ProductIds/{handle} -> { sku, gtin }
// which googleAttributes.js populates during its catalog walk. These let Google
// match reviews to products (GTIN best; SKU / Brand+MPN as fallbacks) — fixing
// the "Missing or invalid product_id" diagnostic. If the collection is empty
// (not yet populated), the feed still works and simply falls back to brand +
// product_url, exactly as before.
//
// Stable feed URL (Merchant Center "Product reviews" scheduled fetch points here):
//   https://goldenspike.app/.netlify/functions/productReviewsFeed
//
// Modes:
//   GET  (default)        -> the full XML feed (gzipped when Accept-Encoding: gzip)
//   GET  ?stats=1         -> JSON summary (counts, by-rating, identifier coverage)
//   GET  ?pretty=1        -> human-readable indented XML (testing only)
//   GET  ?audit=1         -> JSON: probes each product_url and reports how many
//                            reviews point to a LIVE vs RENAMED vs DEAD handle.
//                            This is the "why aren't my stars showing" diagnostic:
//                            a review can be valid ("Ready to serve") yet still
//                            match nothing if its handle URL 404s or 301-redirects
//                            to a different canonical than the product feed uses.
//                            Optional: &offset=N &limit=M &conc=K for chunking.
//
// Required env: FIREBASE_* (consumed by ./firebaseAdmin). No Shopify creds here.
// ---------------------------------------------------------------------------

const zlib = require("zlib");

// fetch: use the Node 18+ global; fall back to node-fetch (in package.json) on
// older runtimes. Used only by the ?audit=1 probe — the feed itself is Shopify-free.
const _fetch = (typeof globalThis !== "undefined" && globalThis.fetch)
  ? globalThis.fetch.bind(globalThis)
  : require("node-fetch");

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
const PRODUCT_IDS_COLLECTION = "Brites_ProductIds";
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

// Validate a GTIN (8/12/13/14 digits + GS1 mod-10 check digit). Returns the
// normalized digit string if valid, else null — so we never emit a junk barcode
// as a GTIN (an invalid GTIN would itself trigger "invalid product_id").
function validGtin(v) {
  const s = String(v == null ? "" : v).replace(/\D/g, "");
  if (![8, 12, 13, 14].includes(s.length)) return null;
  const digits = s.split("").map(Number);
  const check = digits.pop();
  let sum = 0, w = 3;
  for (let i = digits.length - 1; i >= 0; i--) { sum += digits[i] * w; w = (w === 3 ? 1 : 3); }
  const calc = (10 - (sum % 10)) % 10;
  return calc === check ? s : null;
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

// Build the <product_ids> inner XML. Order follows the 2.4 schema sequence:
// gtins, mpns, skus, brands. GTIN only when checksum-valid; SKU doubles as MPN
// so Google gets a Brand+MPN pair (its recommended fallback when no GTIN).
function buildProductIds(ids) {
  const gtin = validGtin(ids && ids.gtin);
  const sku  = clean(ids && ids.sku);
  let out = "";
  if (gtin) out += `<gtins><gtin>${gtin}</gtin></gtins>`;
  if (sku)  out += `<mpns><mpn>${xmlEsc(sku)}</mpn></mpns>`;
  if (sku)  out += `<skus><sku>${xmlEsc(sku)}</sku></skus>`;
  out += `<brands><brand>${xmlEsc(BRAND)}</brand></brands>`;
  return out;
}

// Build one <review> in the exact element order required by the 2.4 XSD.
function reviewBlock({ id, name, verified, ts, body, rating, handle, ids }) {
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
      `<review_url type="group">${xmlEsc(url)}</review_url>` +
      `<ratings><overall min="1" max="5">${rating}</overall></ratings>` +
      `<products><product>` +
        `<product_ids>${buildProductIds(ids)}</product_ids>` +
        `<product_url>${xmlEsc(url)}</product_url>` +
      `</product></products>` +
    `</review>`
  );
}

/* ─── Product-identifier map (populated by googleAttributes.js) ──────────── */
async function loadProductIds(F) {
  const map = {};
  try {
    const snap = await F.db.collection(PRODUCT_IDS_COLLECTION).get();
    snap.forEach(doc => {
      const d = doc.data() || {};
      map[doc.id] = { sku: d.sku || "", gtin: d.gtin || d.barcode || "" };
    });
  } catch (e) {
    // Collection may not exist yet -> empty map (feed falls back to brand only).
    console.warn("[productReviewsFeed] product-id map unavailable:", e && e.message);
  }
  return map;
}

/* ─── Core: read Firestore, build feed + stats ───────────────────────────── */
async function buildFeed(pretty) {
  const F = fb();
  if (!F) throw new Error("Firebase unavailable");

  // One collection-group read of every review item + one read of the id map.
  const [snap, idMap] = await Promise.all([
    F.db.collectionGroup("items").get(),
    loadProductIds(F)
  ]);

  const blocks = [];
  const byRating = { "1": 0, "2": 0, "3": 0, "4": 0, "5": 0 };
  const products = new Set();
  const handleInfo = {};   // handle -> { reviews, gtin, sku }  (drives ?audit=1)
  let total = 0, included = 0;
  let withGtin = 0, withSku = 0, withAnyId = 0, withNone = 0;
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
    if (!body) { skipped.noBody++; return; }

    const ts = toRFC3339(d.d);
    if (!ts) { skipped.badDate++; return; }

    const ids = idMap[handle];
    const gtinOk = !!validGtin(ids && ids.gtin);
    const skuOk  = !!clean(ids && ids.sku);
    if (gtinOk) withGtin++;
    if (skuOk) withSku++;
    if (gtinOk || skuOk) withAnyId++; else withNone++;

    const name = clean(d.n) || "Anonymous";
    blocks.push(reviewBlock({
      id: doc.id, name, verified: d.v ? 1 : 0, ts, body, rating, handle, ids
    }));
    included++;
    byRating[String(rating)]++;
    products.add(handle);

    const hi = handleInfo[handle] || (handleInfo[handle] = { reviews: 0, gtin: false, sku: false });
    hi.reviews++;
    if (gtinOk) hi.gtin = true;
    if (skuOk)  hi.sku  = true;
  });

  const nl = pretty ? "\n" : "";
  const ind = pretty ? "  " : "";
  const reviewsXml = pretty ? blocks.map(b => ind + b).join(nl) : blocks.join("");

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
    identifiers: {
      product_id_map_size: Object.keys(idMap).length,
      reviews_with_gtin: withGtin,
      reviews_with_sku: withSku,
      reviews_with_any_id: withAnyId,
      reviews_with_brand_only: withNone
    },
    skipped,
    feed_bytes: Buffer.byteLength(xml, "utf8"),
    schema_version: FEED_VERSION,
    generated_at: new Date().toISOString()
  };
  return { xml, stats, handleInfo };
}

/* ─── ?audit=1 : does each product_url actually resolve to a live product? ──
   For handmade goods with no GTIN, Google matches a review to a product almost
   entirely by product_url. A review can be perfectly valid ("Ready to serve")
   yet still show no stars if its handle:
     • 404s  (dead)       — product was deleted or the crosswalk guessed wrong, or
     • 301s  (renamed)    — Shopify kept a redirect, but the product feed's canonical
                            link is now a DIFFERENT handle, so Google won't credit it.
   We probe britesjewelry.com/products/{handle} with redirects turned OFF and read
   the raw status. Then we tally how many *reviews* (not just handles) land in each
   bucket, which is the real "why aren't my stars showing" number. ------------- */
function classifyStatus(status) {
  if (status === 200) return "live";
  if ([301, 302, 303, 307, 308].includes(status)) return "redirected";
  if (status === 404 || status === 410) return "dead";
  return "other";
}

async function probeHandle(handle, timeoutMs) {
  const url = SHOP_URL + "/products/" + clean(handle);
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    let res = await _fetch(url, { method: "HEAD", redirect: "manual", signal: ctrl.signal });
    let status = res.status;
    if (status === 405 || status === 501) { // some hosts refuse HEAD — retry GET (body ignored)
      res = await _fetch(url, { method: "GET", redirect: "manual", signal: ctrl.signal });
      status = res.status;
    }
    let location = "";
    if (status >= 300 && status < 400) {
      try { location = res.headers.get("location") || ""; } catch (e) { location = ""; }
    }
    return { status, location };
  } catch (e) {
    return { status: 0, error: (e && e.name === "AbortError") ? "timeout" : ((e && e.message) || "error") };
  } finally {
    clearTimeout(timer);
  }
}

async function runAudit(handleInfo, q) {
  const handles = Object.keys(handleInfo).sort();
  const total = handles.length;
  const offset = Math.max(0, parseInt(q.offset || "0", 10) || 0);
  const limit  = Math.max(1, parseInt(q.limit || String(total), 10) || total);
  const conc   = Math.min(60, Math.max(1, parseInt(q.conc || "40", 10) || 40));
  const budgetMs  = 8500;   // stop pulling new probes past this; return next_offset
  const timeoutMs = 4000;   // per-probe abort
  const started = Date.now();

  const slice = handles.slice(offset, offset + limit);
  const res = {
    live:       { handles: 0, reviews: 0 },
    redirected: { handles: 0, reviews: 0 },
    dead:       { handles: 0, reviews: 0 },
    other:      { handles: 0, reviews: 0 }
  };
  const samples = { redirected: [], dead: [], other: [] };

  let cursor = 0, processed = 0, stoppedEarly = false;
  async function worker() {
    while (cursor < slice.length) {
      if (Date.now() - started > budgetMs) { stoppedEarly = true; return; }
      const h = slice[cursor++];
      const info = handleInfo[h] || { reviews: 0 };
      const { status, location } = await probeHandle(h, timeoutMs);
      const cls = classifyStatus(status);
      res[cls].handles++;
      res[cls].reviews += info.reviews;
      if (cls !== "live" && samples[cls].length < 30) {
        const row = { handle: h, reviews: info.reviews, status };
        if (location) row.redirects_to = location;
        samples[cls].push(row);
      }
      processed++;
    }
  }
  await Promise.all(Array.from({ length: conc }, () => worker()));

  const next_offset = (offset + processed < total) ? (offset + processed) : null;
  return {
    distinct_handles: total,
    probed_this_call: processed,
    offset, next_offset, stopped_early: stoppedEarly,
    resolution: res,
    samples,
    elapsed_ms: Date.now() - started
  };
}

/* ─── small in-memory cache (helps repeat test fetches; short TTL) ───────── */
let _cache = null, _cacheExp = 0;
async function getFeed(pretty) {
  if (pretty) return buildFeed(true);
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

    // Diagnostic: probe every product_url and report live / renamed / dead.
    if (q.audit === "1" || q.audit === "true") {
      const { stats, handleInfo } = await getFeed(false);
      const probe = await runAudit(handleInfo, q);
      const r = probe.resolution;
      const matchable = r.live.reviews;
      const orphaned  = r.redirected.reviews + r.dead.reviews + r.other.reviews;
      return json(200, {
        ok: true,
        what_this_means: "live = product_url returns 200 and can match your product feed. " +
          "redirected = handle was renamed (Shopify 301 to a different canonical) so Google won't credit it. " +
          "dead = 404, no such product. 'reviews' counts are the real driver of whether stars show.",
        totals: {
          reviews_included: stats.reviews_included,
          distinct_handles: probe.distinct_handles,
          reviews_matchable_live: matchable,
          reviews_orphaned: orphaned
        },
        identifier_coverage: stats.identifiers,   // if gtin/sku coverage is high, dead URLs hurt less
        resolution: r,
        samples: probe.samples,
        pagination: { offset: probe.offset, next_offset: probe.next_offset, probed_this_call: probe.probed_this_call, stopped_early: probe.stopped_early },
        elapsed_ms: probe.elapsed_ms
      });
    }

    const { xml, stats } = await getFeed(pretty);

    if (q.stats === "1" || q.stats === "true") return json(200, stats);

    const headers = (event && event.headers) || {};
    const ae = String(headers["accept-encoding"] || headers["Accept-Encoding"] || "");
    const baseHeaders = {
      "Content-Type": "application/xml; charset=utf-8",
      "Cache-Control": "public, max-age=300"
    };

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
