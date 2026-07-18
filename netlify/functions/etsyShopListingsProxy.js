// netlify/functions/etsyShopListingsProxy.js
// Backwards-compatible Etsy listing reader for the Etsy Pricing Console.
// Supports live catalog pagination, shop sections, section reads and
// title-only real-time search. No Firestore or persistent cache is used.

const { etsyFetch } = require("./etsyRateLimiter");

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization,Access-Token,access-token",
  "Access-Control-Allow-Methods": "GET,OPTIONS"
};

function json(statusCode, body) {
  return { statusCode, headers: CORS, body: JSON.stringify(body) };
}

async function parseJson(resp) {
  const text = await resp.text();
  if (!text) return {};
  try { return JSON.parse(text); }
  catch { return { error: text.slice(0, 1000) }; }
}

function apiKey() {
  const clientId = process.env.CLIENT_ID;
  const secret = process.env.CLIENT_SECRET;
  if (!clientId) return null;
  return secret ? `${clientId}:${secret}` : clientId;
}

function clampInt(value, min, max, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? Math.min(max, Math.max(min, parsed)) : fallback;
}

function normalizeText(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function titleMatches(title, query, mode) {
  const normalizedTitle = normalizeText(title);
  const normalizedQuery = normalizeText(query);
  if (!normalizedQuery) return true;
  if (mode === "phrase") return normalizedTitle.includes(normalizedQuery);
  const words = [...new Set(normalizedQuery.split(" ").filter(Boolean))];
  return words.every(word => normalizedTitle.includes(word));
}

function allowedIncludes(raw) {
  return String(raw || "Images")
    .split(",")
    .map(value => value.trim())
    .filter(value => [
      "Shipping", "Images", "Shop", "User", "Translations", "Inventory",
      "Videos", "Personalization", "BuyerPrice"
    ].includes(value));
}

async function fetchEtsy(url, headers) {
  const resp = await etsyFetch(url, { headers }, { bucket: "etsy-listing-console" });
  const payload = await parseJson(resp);
  return { resp, payload };
}

exports.handler = async function handler(event) {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }
  if (event.httpMethod !== "GET") return json(405, { error: "Method not allowed" });

  try {
    const q = event.queryStringParameters || {};
    const mode = String(q.mode || "list").toLowerCase();
    const accessToken = event.headers["access-token"] || event.headers["Access-Token"];
    const shopId = process.env.SHOP_ID;
    const key = apiKey();

    if (!accessToken) return json(400, { error: "Missing access token" });
    if (!shopId) return json(500, { error: "Missing SHOP_ID" });
    if (!key) return json(500, { error: "Missing CLIENT_ID" });

    const headers = {
      Authorization: `Bearer ${accessToken}`,
      "x-api-key": key,
      "Content-Type": "application/json"
    };

    if (mode === "sections") {
      const url = `https://openapi.etsy.com/v3/application/shops/${shopId}/sections`;
      const { resp, payload } = await fetchEtsy(url, headers);
      if (resp.ok && payload && typeof payload === "object") {
        payload._meta = { source: "etsy-live", shop_id: Number(shopId) };
      }
      return json(resp.status, payload);
    }

    if (mode === "search") {
      const startedAt = Date.now();
      const query = String(q.query || "").trim();
      const matchMode = String(q.match || "all").toLowerCase() === "phrase" ? "phrase" : "all";
      const resultLimit = clampInt(q.limit, 1, 500, 200);
      const maxPages = clampInt(q.max_pages, 1, 5, 2);
      const pageSize = Math.min(100, resultLimit);
      const sectionId = q.shop_section_id ? String(q.shop_section_id).trim() : "";
      const sortOn = ["created", "price", "updated", "score"].includes(String(q.sort_on)) ? String(q.sort_on) : "score";
      const sortOrder = ["asc", "ascending", "desc", "descending", "up", "down"].includes(String(q.sort_order)) ? String(q.sort_order) : "desc";

      if (query.length < 2) return json(400, { error: "Search query must contain at least two characters" });
      if (sectionId && !/^\d+$/.test(sectionId)) return json(400, { error: "Invalid shop_section_id" });

      const results = [];
      const seen = new Set();
      let candidateCount = 0;
      let pagesFetched = 0;
      let offset = 0;

      for (let page = 0; page < maxPages && results.length < resultLimit; page += 1) {
        const params = new URLSearchParams({
          limit: String(pageSize),
          offset: String(offset),
          keywords: query,
          sort_on: sortOn,
          sort_order: sortOrder,
          legacy: "true"
        });

        let endpoint;
        if (sectionId) {
          params.set("shop_section_ids", sectionId);
          endpoint = `https://openapi.etsy.com/v3/application/shops/${shopId}/shop-sections/listings`;
        } else {
          endpoint = `https://openapi.etsy.com/v3/application/shops/${shopId}/listings/active`;
        }

        const { resp, payload } = await fetchEtsy(`${endpoint}?${params.toString()}`, headers);
        if (!resp.ok) return json(resp.status, payload);

        const rows = Array.isArray(payload.results) ? payload.results : [];
        candidateCount = Math.max(candidateCount, Number(payload.count || 0));
        pagesFetched += 1;

        for (const listing of rows) {
          const id = String(listing.listing_id || "");
          if (!id || seen.has(id)) continue;
          if (sectionId && String(listing.shop_section_id || "") !== sectionId) continue;
          if (!titleMatches(listing.title, query, matchMode)) continue;
          seen.add(id);
          results.push(listing);
          if (results.length >= resultLimit) break;
        }

        offset += pageSize;
        if (!rows.length || offset >= Number(payload.count || 0)) break;
      }

      return json(200, {
        count: results.length,
        results,
        _meta: {
          source: "etsy-live",
          title_only_verified: true,
          query,
          match: matchMode,
          section_id: sectionId ? Number(sectionId) : null,
          etsy_candidate_count: candidateCount,
          title_match_count: results.length,
          pages_fetched: pagesFetched,
          result_limit: resultLimit,
          elapsed_ms: Date.now() - startedAt,
          shop_id: Number(shopId)
        }
      });
    }

    const limit = clampInt(q.limit, 1, 100, 100);
    const offset = clampInt(q.offset, 0, Number.MAX_SAFE_INTEGER, 0);
    const state = ["active", "inactive", "sold_out", "draft", "expired"].includes(String(q.state)) ? String(q.state) : "active";
    const includes = allowedIncludes(q.includes);
    const sectionId = q.shop_section_id ? String(q.shop_section_id).trim() : "";

    const params = new URLSearchParams({
      limit: String(limit),
      offset: String(offset),
      legacy: "true"
    });
    if (includes.length) params.set("includes", includes.join(","));
    if (q.sort_on) params.set("sort_on", String(q.sort_on));
    if (q.sort_order) params.set("sort_order", String(q.sort_order));

    let endpoint;
    if (mode === "section" || sectionId) {
      if (!/^\d+$/.test(sectionId)) return json(400, { error: "Valid shop_section_id is required" });
      params.set("shop_section_ids", sectionId);
      endpoint = `https://openapi.etsy.com/v3/application/shops/${shopId}/shop-sections/listings`;
    } else {
      params.set("state", state);
      endpoint = `https://openapi.etsy.com/v3/application/shops/${shopId}/listings`;
    }

    const { resp, payload } = await fetchEtsy(`${endpoint}?${params.toString()}`, headers);
    if (resp.ok && payload && typeof payload === "object") {
      payload._meta = {
        source: "etsy-live",
        limit,
        offset,
        state,
        section_id: sectionId ? Number(sectionId) : null,
        includes,
        shop_id: Number(shopId)
      };
    }
    return json(resp.status, payload);
  } catch (err) {
    return json(500, { error: err.message });
  }
};
