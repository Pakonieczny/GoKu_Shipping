/* netlify/functions/testChitChats.js
 *
 * Chit Chats proxy:
 *   - GET  ?resource=batches[&status=open]           → list batches
 *   - GET  ?resource=shipment&id=<shipmentId>        → fetch one shipment
 *   - GET  ?resource=search&orderId=..&tracking=..   → best-effort search (paginates all pages)
 *   - GET  ?resource=label&id=<shipmentId>&format=zpl|pdf|png → fetch label payload (proxy)
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
    // raw (non-JSON) success for ZPL/plain text (so the browser hands it to QZ as-is)
    const okText = (text, contentType = "text/plain; charset=utf-8") => ({
      statusCode: 200,
      headers: { ...CORS, "Content-Type": contentType },
      body: text
    });
    const bad = (code, err)  => ({
      statusCode: code,
      headers: CORS,
      body: JSON.stringify({ error: typeof err === "string" ? err : (err?.message || JSON.stringify(err)) })
    });

    // Server-side YYYY-MM-DD normalization in local day semantics
    const ymdLocal = (d) => {
      const dt = new Date(d.getFullYear(), d.getMonth(), d.getDate());
      const y = dt.getFullYear();
      const m = String(dt.getMonth() + 1).padStart(2, "0");
      const day = String(dt.getDate()).padStart(2, "0");
      return `${y}-${m}-${day}`;
    };
    const normalizeShipDateServer = (v) => {
      const t = String(v || "").trim().toLowerCase();
      const now = new Date();
      const today = ymdLocal(now);
      if (!t || t === "today") return today;
      if (t === "tomorrow") return ymdLocal(new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1));
      if (/^\d{4}-\d{2}-\d{2}$/.test(t)) return t;
      const d = new Date(t);
      return Number.isNaN(d.getTime()) ? today : ymdLocal(d);
    };

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
      const resp = await with429Retry(() =>
        fetch(url(`/shipments/count${qs ? `?${qs}` : ""}`), { headers: authH })
      );
      const out = await wrap(resp);
      if (!out.ok || typeof out.data?.count !== "number") return null;
      return out.data.count;
    };

    // Fill country_code from the existing shipment when the refresh payload doesn't include it
    async function ensureCountryCodeForRefresh(id, payload) {
      if (payload.country_code) return payload;
      try {
        const r = await fetch(url(`/shipments/${encodeURIComponent(id)}`), { headers: authH });
        const o = await wrap(r);
        if (o.ok) {
          const s  = o.data?.shipment || o.data || {};
          const cc = String(
            s.country_code ||
            s.to?.country_code ||
            s.destination?.country_code ||
            s.to_country_code ||
            ""
          ).toUpperCase();
          if (cc) payload.country_code = cc;
        }
      } catch {}
      return payload;
    }

    // --- Flatten nested client payload → API's flat schema ---
    function adaptClientShipment(client = {}) {
      const to      = client.to      || {};
      const pkg     = client.package || {};
      const customs = client.customs || {};

      const out = {
        // Recipient (top level)
        name          : to.name ?? client.name ?? "",
        address_1     : to.address_1 ?? client.address_1 ?? "",
        address_2     : to.address_2 ?? client.address_2 ?? undefined,
        city          : to.city ?? client.city ?? "",
        province_code : to.province_code ?? client.province_code ?? "",
        postal_code   : to.postal_code ?? client.postal_code ?? "",
        country_code  : String(to.country_code ?? client.country_code ?? "").toUpperCase(),
        phone         : to.phone ?? client.phone ?? undefined,
        email         : to.email ?? client.email ?? undefined,

        // Package / required top-levels
        package_type  : pkg.package_type  ?? client.package_type,
        size_unit     : pkg.size_unit     ?? client.size_unit,
        size_x        : numberish(pkg.size_x ?? client.size_x),
        size_y        : numberish(pkg.size_y ?? client.size_y),
        size_z        : numberish(pkg.size_z ?? client.size_z),
        weight_unit   : pkg.weight_unit   ?? client.weight_unit,
        weight        : numberish(pkg.weight ?? client.weight),
        ship_date     : normalizeShipDateServer(pkg.ship_date ?? client.ship_date ?? "today"),
        postage_type  : pkg.postage_type  ?? client.postage_type, // may be "unknown" or omitted

        // Customs / value
        package_contents : customs.package_contents ?? client.package_contents ?? "merchandise",
        description      : customs.description     ?? client.description,
        value            : String((customs.value ?? client.value ?? 0)),
        value_currency   : String((customs.value_currency ?? client.value_currency ?? "cad")).toLowerCase(),
        order_id         : client.order_id ?? client.reference ?? client.order,
        line_items       : Array.isArray(customs.line_items) ? customs.line_items : undefined
      };

      // prune undefined/null to keep payload tidy
      Object.keys(out).forEach(k => (out[k] == null) && delete out[k]);
      return out;
    }

    // --- Pending/open-batch filter (correct scope) ---
    let _openBatchIdsCache = null;
    async function getOpenBatchIdsSet() {
      if (_openBatchIdsCache) return _openBatchIdsCache;
      try {
        const r = await fetch(url(`/batches?status=open`), { headers: authH });
        const o = await wrap(r);
        if (!o.ok) return (_openBatchIdsCache = new Set());
        const list = Array.isArray(o.data) ? o.data : (o.data?.batches || o.data?.data || []);
        _openBatchIdsCache = new Set((list || []).map(b => String(b.id)));
        return _openBatchIdsCache;
      } catch {
        return (_openBatchIdsCache = new Set());
      }
    }
    async function keepPendingOnly(list, enabled = true) {
      if (!enabled) return list || [];
      const open = await getOpenBatchIdsSet();
      return (list || []).filter(sh => {
        const bid = sh?.batch_id == null ? "" : String(sh.batch_id);
        // keep: no batch yet OR in an open (pending) batch
        return !bid || open.has(bid);
      });
    }

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

      // Proxy actual label bytes (solves browser CORS for ZPL/PDF/PNG)
      if (resource === "label") {
        const id  = String(qp.id || "");
        const fmt = String(qp.format || "zpl").toLowerCase();
        if (!id) return bad(400, "id required");

        // 1) Read shipment → find label URLs
        const rs  = await fetch(url(`/shipments/${encodeURIComponent(id)}`), { headers: authH });
        const os  = await wrap(rs);
        if (!os.ok) return bad(os.status, os.data);
        const s   = os.data?.shipment || os.data || {};
        const pick = fmt === "zpl" ? (s.postage_label_zpl_url || s.postageLabelZplUrl)
                   : fmt === "pdf" ? (s.postage_label_pdf_url || s.postageLabelPdfUrl || s.label_pdf_url || s.labelPdfUrl)
                                   : (s.postage_label_png_url || s.postageLabelPngUrl || s.label_png_url || s.labelPngUrl);
        if (!pick) return bad(404, "Label not ready");

        // 2) Fetch label from Chit Chats
        const lr = await fetch(pick);
        if (!lr.ok) return bad(lr.status, await lr.text());
        const ctype = lr.headers.get("content-type") || (fmt === "zpl"
                       ? "text/plain; charset=utf-8"
                       : fmt === "pdf" ? "application/pdf" : "image/png");

        if (fmt === "zpl") {
          const txt = await lr.text();
          return { statusCode: 200, headers: { ...CORS, "Content-Type": ctype }, body: txt };
        } else {
          const ab  = await lr.arrayBuffer();
          const b64 = Buffer.from(ab).toString("base64");
          return {
            statusCode: 200,
            headers: { ...CORS, "Content-Type": ctype },
            isBase64Encoded: true,
            body: b64
          };
        }
      }

      // Allow controlling the pending filter (default on)
      const pendingOnlyOn = !["0","false"].includes(String(qp.pendingOnly ?? "1").toLowerCase());

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

        const applyPendingFilter = (list) => keepPendingOnly(list, pendingOnlyOn);

        // 1) Direct by ID if the input looks like a shipment id
        if (looksLikeId(want)) {
          try {
            const r = await fetch(url(`/shipments/${encodeURIComponent(want)}`), { headers: authH });
            const o = await wrap(r);
            if (o.ok && o.data && o.data.id) {
              const kept = await applyPendingFilter([o.data]);
              return ok({ shipments: kept });
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
          const kept = await applyPendingFilter(hits);
          if (kept.length) return ok({ shipments: kept });
        } catch { /* non-fatal; continue */ }

        // 3) If fast mode, stop after a full vendor pagination
        if (fastMode) return ok({ shipments: [] });

        // 4) Deep fallback: scan by status pools with pagination & local filter
        const pools = ["archived", "processing", "ready"];
        for (const st of pools) {
          const pageResults = await paginateShipments({ status: st, pageSize });
          const hits = pageResults.filter(matches);
          const kept = await applyPendingFilter(hits);
          if (kept.length) return ok({ shipments: kept });
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

      // (2) create shipment  ✅ FLATTEN + validate
      if (action === "create_shipment") {
        const clientPayload = body.shipment || body.payload || {};
        const shipment = adaptClientShipment(clientPayload);

        if (!shipment.country_code) {
          return bad(400, { message: "country_code required" });
        }

        const resp = await fetch(url(`/shipments`), {
          method: "POST",
          headers: authH,
          body: JSON.stringify(shipment)
        });
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

      // (A) Shipment: refresh rates / update pkg details  ✅ FLATTEN + validate
      if (action === "refresh") {
        const id = String(body.shipment_id || body.id || qp.id || "");
        if (!id) return bad(400, "shipment_id required for refresh");
        // Flatten client payload and ensure required bits
        let payload = adaptClientShipment(body.payload || {});
        payload.ship_date = normalizeShipDateServer(payload.ship_date);
        // 🔁 Auto-fill country_code from the existing shipment if missing
        payload = await ensureCountryCodeForRefresh(id, payload);
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
