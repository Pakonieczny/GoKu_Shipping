/*  netlify/functions/etsyMailSearch.js
 *
 *  v1.3 — Full-text search across EtsyMail_Threads.
 *
 *  ═══ HOW IT WORKS ════════════════════════════════════════════════════
 *
 *  Each thread doc carries a denormalized `searchableText` field
 *  populated by etsyMailSnapshot.js. It contains a lowercased,
 *  normalized concatenation of:
 *    - customer name, etsy username, subject, linked order id
 *    - the most recent ~6KB of message body text (truncated from the
 *      oldest end as new messages arrive)
 *
 *  This endpoint loads the most recent N threads (default 500, max
 *  2000) ordered by updatedAt desc, runs a substring match against
 *  `searchableText` in memory, and returns the matching docs in the
 *  same shape as firestoreProxy's `op:list` response so the UI can
 *  drop them into the existing thread-list rendering path with no
 *  changes.
 *
 *  ═══ PERFORMANCE ════════════════════════════════════════════════════
 *
 *  At 500 threads × 6KB searchableText each = ~3MB transferred per
 *  search. Firestore read cost: 500 doc-reads per uncached search.
 *  In-memory cache (15s TTL, keyed by query+limit) absorbs rapid
 *  keystrokes from the inbox UI's debounced search input.
 *
 *  When the inbox grows beyond ~5K threads, replace this with
 *  Algolia / Typesense / Meilisearch. For typical operator-inbox
 *  scale (hundreds to a few thousand active threads), this is fast
 *  enough and free.
 *
 *  ═══ REQUEST ════════════════════════════════════════════════════════
 *
 *  GET /.netlify/functions/etsyMailSearch?q=string&limit=N&status=...
 *
 *    q       Required. Search string. Lowercased before matching.
 *            Queries shorter than 2 chars return empty (avoids
 *            scanning everything for "a").
 *    limit   Optional. How many recent threads to scan. Default 500.
 *            Max 2000.
 *    status  Optional. If provided, also filter by thread.status.
 *            Useful for "search within Needs Review only".
 *
 *  ═══ RESPONSE ═══════════════════════════════════════════════════════
 *
 *  {
 *    success: true,
 *    docs   : [ { id, customerName, ..., searchableText (truncated) } ],
 *    q,
 *    count  : matchedCount,
 *    scanned: scannedCount,
 *    cached : true | false
 *  }
 */

const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");

const db = admin.firestore();
const THREADS_COLL = "EtsyMail_Threads";

// In-memory cache. Invalidated by TTL only — operators are unlikely
// to expect "edit a thread, immediately re-search and see the change"
// to work; if they do, they can wait 15 seconds or hit Refresh.
const _searchCache = new Map();
const SEARCH_CACHE_TTL_MS = 15 * 1000;
const MAX_CACHE_ENTRIES = 100;

function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

/* Convert Firestore doc data to JSON-safe form, turning Timestamps into
 * {_ts: true, ms: <millis>} markers so the inbox JS toDate() helper can
 * reconstruct them. Same shape firestoreProxy uses, so the inbox doesn't
 * need a separate code path for our results. */
function serialize(value) {
  if (value === null || typeof value !== "object") return value;
  if (value && typeof value.toDate === "function" && typeof value.toMillis === "function") {
    return { _ts: true, ms: value.toMillis() };
  }
  if (Array.isArray(value)) return value.map(serialize);
  const out = {};
  for (const k of Object.keys(value)) out[k] = serialize(value[k]);
  return out;
}

/** Trim the heaviest internal-only field from results. v1.6: keep
 *  `searchableText` so the UI can run further per-keystroke local
 *  filtering on the result set without an extra round trip; only drop
 *  the larger raw `searchableMessageText` which is just intermediate
 *  data the snapshot uses to rebuild searchableText incrementally. */
function trimResultDoc(data) {
  const { searchableMessageText, ...rest } = data;
  return rest;
}

/** GC cache entries past their TTL. Cheap, runs at most once per call. */
function gcCache() {
  if (_searchCache.size <= MAX_CACHE_ENTRIES) return;
  const cutoff = Date.now() - SEARCH_CACHE_TTL_MS;
  for (const [k, v] of _searchCache.entries()) {
    if (v.at < cutoff) _searchCache.delete(k);
  }
  // If still oversize, drop oldest by insertion order until at limit
  while (_searchCache.size > MAX_CACHE_ENTRIES) {
    const oldest = _searchCache.keys().next().value;
    _searchCache.delete(oldest);
  }
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  // v1.2: every endpoint behind the shared secret
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  if (event.httpMethod !== "GET") {
    return json(405, { error: "Method Not Allowed" });
  }

  const qs = event.queryStringParameters || {};
  const q = String(qs.q || "").trim().toLowerCase();
  const limit = Math.min(Math.max(parseInt(qs.limit || "500", 10), 1), 2000);

  // v1.4: status can be a single value ("pending_human_review") OR a
  // comma-separated list ("auto_replied,queued_for_auto_send") for
  // multi-status folders like Auto-Reply. The UI passes the active
  // folder's status string verbatim. When omitted (empty/null), search
  // is global — used by the "All" folder.
  const statusRaw = qs.status ? String(qs.status).trim() : "";
  const statusList = statusRaw
    ? statusRaw.split(",").map(s => s.trim()).filter(Boolean).slice(0, 10)  // Firestore `in` cap
    : [];

  // Guard against pathological queries that would scan everything for
  // a single character. The inbox UI also debounces and only fires
  // for q.length >= 2, but defense in depth.
  if (q.length < 2) {
    return json(200, { success: true, docs: [], q, count: 0, scanned: 0 });
  }

  const cacheKey = q + "|" + limit + "|" + statusList.sort().join(",");
  const cached = _searchCache.get(cacheKey);
  if (cached && (Date.now() - cached.at) < SEARCH_CACHE_TTL_MS) {
    return json(200, {
      success: true,
      docs: cached.docs,
      q,
      count: cached.docs.length,
      scanned: cached.scanned,
      cached: true
    });
  }

  try {
    let firestoreQuery = db.collection(THREADS_COLL);
    if (statusList.length === 1) {
      firestoreQuery = firestoreQuery.where("status", "==", statusList[0]);
    } else if (statusList.length > 1) {
      // Firestore `in` supports up to 10 values. We capped at 10 above.
      firestoreQuery = firestoreQuery.where("status", "in", statusList);
    }
    firestoreQuery = firestoreQuery.orderBy("updatedAt", "desc").limit(limit);

    const snap = await firestoreQuery.get();

    const matches = [];
    snap.forEach(doc => {
      const data = doc.data() || {};
      const haystack = (data.searchableText || "").toLowerCase();
      // Fallback: if a thread doesn't have searchableText yet (legacy
      // threads from before v1.3 deployment), fall back to checking
      // the metadata fields directly. This avoids missing matches on
      // pre-v1.3 threads while the snapshot path catches up.
      const hit = haystack
        ? haystack.includes(q)
        : [
            data.customerName, data.etsyUsername, data.subject,
            data.linkedOrderId
          ].some(v => v && String(v).toLowerCase().includes(q));

      if (hit) {
        matches.push({ id: doc.id, ...serialize(trimResultDoc(data)) });
      }
    });

    _searchCache.set(cacheKey, { docs: matches, at: Date.now(), scanned: snap.size });
    gcCache();

    return json(200, {
      success: true,
      docs   : matches,
      q,
      count  : matches.length,
      scanned: snap.size,
      cached : false,
      // If matches.length === 0 and scanned === limit, the user might
      // be searching for something deeper than our window. Surface
      // that so the UI can show a "Try a more specific query or
      // broaden the date filter" hint.
      maxedOut: snap.size >= limit
    });
  } catch (err) {
    console.error("etsyMailSearch error:", err);
    return json(500, { error: err.message || String(err) });
  }
};
