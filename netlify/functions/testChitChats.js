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
      if (resource === "search") {
        const orderId  = (qp.orderId || "").toString().trim();
        const tracking = (qp.tracking || "").toString().trim();
        const want     = orderId || tracking;
        if (!want) return ok({ shipments: [] });

        const fastMode = String(qp.fast || "").toLowerCase() === "1" || String(qp.fast || "").toLowerCase() === "true";
        const pageSize = qp.pageSize ? Number(qp.pageSize) : 500;

        // helpers
        const norm = (s) => String(s || "").toLowerCase().replace(/[^a-z0-9]/g, "");
        const looksLikeId = (s) => /^[0-9]{6,}$/.test(String(s || "").trim());

        // Match across multiple possible fields (older orders often store refs differently)
        const matches = (sh) => {
          const n = (v) => norm(v);
          const nOrder = n(orderId);
          const nTrack = n(tracking);

          const ordFields = [
            sh.order_id,
            sh.order_number,
            sh.reference,
            sh.reference_number,
            sh.reference_value,
            sh.external_order_id,
            sh.external_id
          ];
          const trkFields = [
            sh.carrier_tracking_code,
            sh.tracking_code,
            sh.tracking_number,
            sh.tracking
          ];

          const candOrd   = n(ordFields.find(Boolean) || "");
          const candTrack = n(trkFields.find(Boolean) || "");

          return (
            (nOrder && candOrd && (candOrd.includes(nOrder) || nOrder.includes(candOrd))) ||
            (nTrack && candTrack && (candTrack.includes(nTrack) || nTrack.includes(candTrack)))
          );
        };

        // 1) Direct by ID if the input looks like a shipment id
        if (looksLikeId(want)) {
          try {
            const r = await fetch(url(`/shipments/${encodeURIComponent(want)}`), { headers: authH });
            const o = await wrap(r);
            if (o.ok && o.data && o.data.id) {
              return ok({ shipments: [o.data] });
            }
          } catch {}
        }

        // 2) Vendor search across *all pages*
        try {
          const all = await paginateShipments({
            search: want,
            pageSize,
            // Stop early if we already have a hit to reduce calls
            stopEarlyIf: (sh) => matches(sh)
          });
          const hits = (all || []).filter(matches);
          if (hits.length) return ok({ shipments: hits });
        } catch { /* non-fatal; continue */ }

        // 3) If fast mode, stop after a full vendor pagination
        if (fastMode) return ok({ shipments: [] });

        // 4) Deep fallback: scan by status pools with pagination & local filter
        const pools = ["archived", "processing", "ready"];
        for (const st of pools) {
          const pageResults = await paginateShipments({ status: st, pageSize });
          const hits = pageResults.filter(matches);
          if (hits.length) return ok({ shipments: hits });
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