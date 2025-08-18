/* netlify/functions/testChitChats.js
 *
 * Chit Chats proxy:
 *   - GET  ?resource=batches[&status=open]           → list batches
 *   - GET  ?resource=shipment&id=<shipmentId>        → fetch one shipment
 *   - GET  ?resource=search&orderId=..&tracking=..   → best-effort search (paginates all pages)
 *   - GET  (no resource)                             → quick shipments ping (status=ready)
 *   - POST { action:"create", description? }         → create batch
 *   - POST { action:"create_shipment", shipment:{} } → create shipment
 *   - POST { action:"verify_to", to:{} }             → verify recipient address (best-effort)
 *   - PATCH { action:"refresh", shipment_id, payload:{} } → refresh rates / update pkg
 *   - PATCH { action:"buy",     shipment_id, postage_type } → buy label
 *   - PATCH { action:"add"|"remove", batch_id|batchId, shipmentIds[] }
 *           OR PATCH ?id=<shipmentId> with body { action, batch_id|batchId }
 *
 * Auth: Authorization: <ACCESS_TOKEN>  (raw token, not "Bearer ...")
 * Base: https://chitchats.com/api/v1  (override with CHIT_CHATS_BASE_URL)
 */

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS,PATCH"
};

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  try {
    const BASE         = process.env.CHIT_CHATS_BASE_URL || "https://chitchats.com/api/v1";
    const CLIENT_ID    = process.env.CHIT_CHATS_CLIENT_ID;
    const ACCESS_TOKEN = process.env.CHIT_CHATS_ACCESS_TOKEN;

    if (!CLIENT_ID || !ACCESS_TOKEN) {
      return bad(500, "Missing CHIT_CHATS_CLIENT_ID or CHIT_CHATS_ACCESS_TOKEN");
    }

    const authH = {
      "Authorization": ACCESS_TOKEN, // raw token per Chit Chats docs
      "Content-Type": "application/json; charset=utf-8"
    };
    const url = (p) => `${BASE}/clients/${encodeURIComponent(CLIENT_ID)}${p}`;

    // ---------- helpers ----------
    const wrap = async (resp) => {
      const txt = await resp.text();
      let data; try { data = JSON.parse(txt); } catch { data = txt; }
      return { ok: resp.ok, status: resp.status, data, resp };
    };
    const ok  = (data)       => ({ statusCode: 200, headers: CORS, body: JSON.stringify(data) });
    const bad = (code, err)  => ({
      statusCode: code,
      headers: CORS,
      body: JSON.stringify({ error: typeof err === "string" ? err : (err?.message || JSON.stringify(err)) })
    });

    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    const with429Retry = async (makeFetch, attempts = 3) => {
      for (let i = 1; i <= attempts; i++) {
        const resp = await makeFetch();
        if (resp.status !== 429) return resp;
        const ra = Number(resp.headers.get("Retry-After") || "1");
        await sleep(Math.max(ra, 1) * 1000);
      }
      return makeFetch();
    };

    const normalizeList = (obj) =>
      Array.isArray(obj) ? obj : (obj?.shipments || obj?.data || []);

    const getCount = async (qs) => {
      // Uses official /shipments/count to estimate total pages (status supported)
      // Not all filters are guaranteed server-side for /count; we treat it as a hint.
      const resp = await with429Retry(() =>
        fetch(url(`/shipments/count${qs ? `?${qs}` : ""}`), { headers: authH })
      );
      const out = await wrap(resp);
      if (!out.ok || typeof out.data?.count !== "number") return null;
      return out.data.count;
    };

    // Generic paginator over /shipments that walks every page until exhausted.
    async function paginateShipments({ status, search, pageSize = 500, stopEarlyIf }) {
      const PAGE_SIZE = Math.min(Math.max(Number(pageSize) || 500, 1), 1000); // docs say max 1000
      const qsCore = [
        status ? `status=${encodeURIComponent(status)}` : "",
        search ? `search=${encodeURIComponent(search)}` : "",
        `limit=${PAGE_SIZE}`
      ].filter(Boolean).join("&");

      // Try to estimate total pages from /shipments/count (if available)
      let estPages = null;
      try {
        const countQs = status ? `status=${encodeURIComponent(status)}` : "";
        const cnt = await getCount(countQs);
        if (typeof cnt === "number" && cnt >= 0) {
          estPages = Math.max(1, Math.ceil(cnt / PAGE_SIZE));
        }
      } catch { /* non-fatal */ }

      const results = [];
      const MAX_PAGES_HARDSTOP = estPages || 200; // safety stop if /count unavailable
      for (let page = 1; page <= MAX_PAGES_HARDSTOP; page++) {
        const resp = await with429Retry(() =>
          fetch(url(`/shipments?${qsCore}&page=${page}`), { headers: authH })
        );
        const out = await wrap(resp);
        if (!out.ok) break;

        const arr = normalizeList(out.data);
        if (!arr || arr.length === 0) break;

        for (const sh of arr) {
          results.push(sh);
          if (stopEarlyIf && stopEarlyIf(sh)) return results;
        }

        // If the server returned fewer than PAGE_SIZE, we're at the last page.
        if (arr.length < PAGE_SIZE) break;
      }
      return results;
    }

    // ---------- GET ----------
    if (event.httpMethod === "GET") {
      const qp = event.queryStringParameters || {};
      const resource = (qp.resource || "").toLowerCase();

      // List batches (optional ?status=open|processing|archived)
      if (resource === "batches") {
        try {
          const status = qp.status ? `?status=${encodeURIComponent(qp.status)}` : "";
          const resp = await fetch(url(`/batches${status}`), { headers: authH });
          const out  = await wrap(resp);
          if (!out.ok) return bad(out.status, out.data);

          // normalize: some envs return array; others wrap under .batches/.data
          const list = Array.isArray(out.data) ? out.data : (out.data?.batches || out.data?.data || []);
          return ok({ success: true, batches: list });
        } catch (e) {
          return bad(500, e);
        }
      }

      // Search shipments by orderId or tracking (best-effort with graceful fallbacks)
// --- inside: if (resource === "search") { ... }  REPLACE WHOLE BLOCK ---
if (resource === "search") {
  const orderId  = (qp.orderId || "").toString().trim();
  const tracking = (qp.tracking || "").toString().trim();
  const want     = orderId || tracking;
  if (!want) return ok({ shipments: [] });

  const fastMode = String(qp.fast || "").toLowerCase() === "1" || String(qp.fast || "").toLowerCase() === "true";

  // NEW: scope to batches only
  const parseCSV = (s) => String(s || "").split(",").map(t => t.trim()).filter(Boolean);
  const batchIdsFromQuery = parseCSV(qp.batchIds || qp.batchId);
  const batchesOnly = String(qp.batchesOnly || "").toLowerCase() === "1" || String(qp.batchesOnly || "") === "true" || batchIdsFromQuery.length > 0;

  const norm = (s) => String(s || "").toLowerCase().replace(/[^a-z0-9]/g, "");
  const looksLikeId = (s) => /^[0-9]{6,}$/.test(String(s || "").trim());
  const matches = (sh) => {
    const nOrder    = norm(orderId);
    const nTrack    = norm(tracking);
    const candOrd   = norm(sh.order_id || sh.order_number || sh.reference || "");
    const candTrack = norm(sh.carrier_tracking_code || sh.tracking_code || sh.tracking_number || "");
    return (nOrder && candOrd && (candOrd.includes(nOrder) || nOrder.includes(candOrd)))
        || (nTrack && candTrack && (candTrack.includes(nTrack) || nTrack.includes(candTrack)));
  };

  // helper: list open batches when none explicitly given
  const listOpenBatchIds = async () => {
    const r = await fetch(url(`/batches?status=open`), { headers: authH });
    const o = await wrap(r);
    if (!o.ok) return [];
    const arr = Array.isArray(o.data) ? o.data : (o.data?.batches || o.data?.data || []);
    return (arr || []).map(b => String(b.id)).filter(Boolean);
  };

  // helper: page through shipments for a single batch id
  const fetchBatchShipments = async (batchId, limitPerPage = 100, maxPages = 12) => {
    const found = [];
    for (let page = 1; page <= maxPages; page++) {
      let r, o;

      // Prefer vendor batch-scoped endpoint if present
      try {
        r = await with429Retry(() => fetch(
          url(`/batches/${encodeURIComponent(batchId)}/shipments?limit=${limitPerPage}&page=${page}`),
          { headers: authH }
        ));
        if (r.ok) {
          o = await wrap(r);
        } else if (r.status === 404) {
          o = null; // fall back below
        } else {
          break;
        }
      } catch { o = null; }

      // Fallback: some tenants expose a shipments filter by batch_id instead
      if (!o) {
        try {
          const r2 = await with429Retry(() => fetch(
            url(`/shipments?batch_id=${encodeURIComponent(batchId)}&limit=${limitPerPage}&page=${page}`),
            { headers: authH }
          ));
          o = await wrap(r2);
          if (!o.ok) break;
        } catch { break; }
      }

      const arr = Array.isArray(o.data) ? o.data : (o.data?.shipments || o.data?.data || []);
      for (const sh of (arr || [])) if (matches(sh)) found.push(sh);
      if (!arr || arr.length < limitPerPage) break; // end of pages
    }
    return found;
  };

  // --- Batch-scoped search path (strict) ---
  if (batchesOnly) {
    const ids = batchIdsFromQuery.length ? batchIdsFromQuery : (await listOpenBatchIds());
    if (!ids.length) return ok({ shipments: [] });

    const pageCap = fastMode ? 2 : 12; // fast mode: skim first two pages per batch
    const results = [];
    // Small concurrency without hammering the API
    const CONC = 3;
    for (let i = 0; i < ids.length; i += CONC) {
      const slice = ids.slice(i, i + CONC);
      const chunk = await Promise.all(slice.map(id => fetchBatchShipments(id, 100, pageCap)));
      for (const arr of chunk) results.push(...arr);
      if (fastMode && results.length) break; // early exit if we already have a winner
    }

    // If input looks like a shipment id, optional verification that it belongs to allowed batches
    if (!results.length && looksLikeId(want)) {
      try {
        const r = await fetch(url(`/shipments/${encodeURIComponent(want)}`), { headers: authH });
        const o = await wrap(r);
        if (o.ok && o.data && o.data.id) {
          const bid = String(o.data.batch_id || "");
          if (bid && ids.includes(bid) && matches(o.data)) return ok({ shipments: [o.data] });
        }
      } catch {}
    }

    return ok({ shipments: results });
  }

  // --- Original (global) search path kept for non-batch calls ---
  // 1) try exact id
  if (looksLikeId(want)) {
    try {
      const r = await fetch(url(`/shipments/${encodeURIComponent(want)}`), { headers: authH });
      const o = await wrap(r);
      if (o.ok && o.data && o.data.id) return ok({ shipments: [o.data] });
    } catch {}
  }

  // 2) vendor search
  try {
    const r1 = await fetch(
      url(`/shipments?search=${encodeURIComponent(want)}&limit=100&page=1`),
      { headers: authH }
    );
    const o1 = await wrap(r1);
    if (o1.ok) {
      const arr = Array.isArray(o1.data) ? o1.data : (o1.data?.shipments || o1.data?.data || []);
      const hits = (arr || []).filter(matches);
      if (hits.length) return ok({ shipments: hits });
    }
  } catch {}

  // 3) deep fallback across statuses
  if (fastMode) return ok({ shipments: [] });
  const tryPages = async (status) => {
    const out = [];
    const MAX_PAGES = 12;
    for (let page = 1; page <= MAX_PAGES; page++) {
      try {
        const r = await fetch(url(`/shipments?status=${encodeURIComponent(status)}&limit=100&page=${page}`), { headers: authH });
        const o = await wrap(r);
        if (!o.ok) break;
        const arr = Array.isArray(o.data) ? o.data : (o.data?.shipments || o.data?.data || []);
        for (const sh of (arr || [])) if (matches(sh)) out.push(sh);
        if (!arr || arr.length < 100) break;
      } catch { break; }
    }
    return out;
  };
  for (const st of ["archived", "processing", "ready"]) {
    const found = await tryPages(st);
    if (found.length) return ok({ shipments: found });
  }
  return ok({ shipments: [] });
}

      // Fetch a single shipment
      if (resource === "shipment" && qp.id) {
        try {
          const resp = await fetch(url(`/shipments/${encodeURIComponent(qp.id)}`), { headers: authH });
          const out  = await wrap(resp);
          if (!out.ok) return bad(out.status, out.data);
          return ok(out.data);
        } catch (e) {
          return bad(500, e);
        }
      }

      // Default: quick shipments sanity ping (kept for backward compat)
      const limit  = qp.limit ? Number(qp.limit) : 25;
      const page   = qp.page  ? Number(qp.page)  : 1;
      const status = qp.status || "ready";
      const resp = await fetch(
        url(`/shipments?status=${encodeURIComponent(status)}&limit=${encodeURIComponent(limit)}&page=${encodeURIComponent(page)}`),
        { headers: authH }
      );
      const out  = await wrap(resp);
      if (!out.ok) return bad(out.status, out.data);
      return ok({ success: true, data: out.data });
    }

    // ---------- POST (create batch | create shipment | verify_to) ----------
    if (event.httpMethod === "POST") {
      const body   = safeJSON(event.body);
      const action = (body.action || "").toLowerCase();

      // (0) verify recipient address (best-effort; never throw)
      if (action === "verify_to") {
        const to = body.to || body.address || {};

        // A) dedicated address verify endpoint
        try {
          const r1 = await fetch(url("/addresses/verify"), {
            method: "POST",
            headers: authH,
            body: JSON.stringify({ address: to })
          });
          const o1 = await wrap(r1);
          if (o1.ok) return ok(o1.data); // may contain { suggested | normalized | address }
        } catch {}

        // B) fallback variant some tenants expose
        try {
          const r2 = await fetch(url("/shipments/verify"), {
            method: "POST",
            headers: authH,
            body: JSON.stringify({ to })
          });
          const o2 = await wrap(r2);
          if (o2.ok) return ok(o2.data);
        } catch {}

        // C) Graceful fallback so the UI continues without a scary 500
        return ok({ suggested: null });
      }

      // (1) create batch
      if (action === "create") {
        const payload = { description: (body.description || "").toString() };
        const resp = await fetch(url("/batches"), { method: "POST", headers: authH, body: JSON.stringify(payload) });
        const out  = await wrap(resp);
        if (!out.ok) return bad(out.status, out.data);

        // id is last segment of Location header
        const loc = out.resp.headers.get("Location") || "";
        const id  = loc.split("/").filter(Boolean).pop();
        return ok({ success: true, id, location: loc || null });
      }

      // (2) create shipment
      if (action === "create_shipment") {
        const shipment = body.shipment || body.payload || {};
        const resp = await fetch(url(`/shipments`), { method: "POST", headers: authH, body: JSON.stringify(shipment) });
        const out  = await wrap(resp);
        if (!out.ok) return bad(out.status, out.data);

        const loc = out.resp.headers.get("Location") || "";
        const id  = loc.split("/").filter(Boolean).pop() || null;

        // Return full created resource if possible
        let created = null;
        if (id) {
          try {
            const r2 = await fetch(url(`/shipments/${encodeURIComponent(id)}`), { headers: authH });
            const o2 = await wrap(r2);
            if (o2.ok) created = o2.data;
          } catch {}
        }
        return ok({ success: true, id, shipment: created });
      }

      return bad(400, "action must be create or create_shipment or verify_to");
    }

    // ---------- PATCH (batch add/remove | shipment refresh/buy) ----------
    if (event.httpMethod === "PATCH") {
      const qp     = event.queryStringParameters || {};
      const body   = safeJSON(event.body);
      const action = (body.action || "").toLowerCase();

      // (A) Shipment: refresh rates / update pkg details
      if (action === "refresh") {
        const id = String(body.shipment_id || body.id || qp.id || "");
        if (!id) return bad(400, "shipment_id required for refresh");
        const payload = body.payload || {};
        const resp = await fetch(url(`/shipments/${encodeURIComponent(id)}/refresh`), {
          method: "PATCH", headers: authH, body: JSON.stringify(payload)
        });
        const out = await wrap(resp);
        if (!out.ok) return bad(out.status, out.data);
        return ok(out.data);
      }

      // (B) Shipment: buy postage
      if (action === "buy") {
        const id = String(body.shipment_id || body.id || qp.id || "");
        if (!id) return bad(400, "shipment_id required for buy");
        const payload = { postage_type: (body.postage_type || body.postageType || "unknown") };
        const resp = await fetch(url(`/shipments/${encodeURIComponent(id)}/buy`), {
          method: "PATCH", headers: authH, body: JSON.stringify(payload)
        });
        const out = await wrap(resp);
        if (!out.ok) return bad(out.status, out.data);
        return ok(out.data);
      }

      // (C) Batches: add/remove shipments
      const batchId = numberish(body.batch_id ?? body.batchId ?? qp.batch_id ?? qp.batchId);
      const oneIdFromQuery = qp.id ? String(qp.id) : "";
      const oneIdFromBody  = body.shipment_id ? String(body.shipment_id) : (body.shipmentId ? String(body.shipmentId) : "");
      const manyFromBody   = Array.isArray(body.shipmentIds) ? body.shipmentIds.map(String) : [];
      const shipmentIds = manyFromBody.length ? manyFromBody
                        : (oneIdFromBody ? [oneIdFromBody]
                        : (oneIdFromQuery ? [oneIdFromQuery] : []));

      if (!action || (action !== "add" && action !== "remove")) {
        return bad(400, "action must be refresh|buy|add|remove");
      }
      if (!batchId || !shipmentIds.length) {
        return bad(400, "batch_id + at least one shipment id required");
      }

      const payload = { batch_id: Number(batchId), shipment_ids: shipmentIds };
      const path = action === "add" ? "/shipments/add_to_batch" : "/shipments/remove_from_batch";

      const resp = await fetch(url(path), { method: "PATCH", headers: authH, body: JSON.stringify(payload) });
      const out  = await wrap(resp);
      if (!out.ok) return bad(out.status, out.data);
      return ok({ success: true });
    }

    // ---------- Fallback ----------
    return { statusCode: 405, headers: CORS, body: "Method Not Allowed" };

  } catch (error) {
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: error.message }) };
  }
};

// ---------- small utilities ----------
function safeJSON(txt) {
  if (!txt) return {};
  try { return JSON.parse(txt); } catch { return {}; }
}
function numberish(v) {
  if (v == null) return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}