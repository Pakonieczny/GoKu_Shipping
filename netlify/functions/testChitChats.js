/* netlify/functions/testChitChats.js
 *
 * Chit Chats proxy:
 *   - GET  ?resource=batches[&status=open]           → list batches
 *   - GET  ?resource=shipment&id=<shipmentId>        → fetch one shipment
 *   - GET  (no resource)                             → quick shipments ping (status=ready)
 *   - POST { action:"create", description? }         → create batch
 *   - POST { action:"create_shipment", shipment:{} } → create shipment
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
    const bad = (code, err)  => ({ statusCode: code, headers: CORS, body: JSON.stringify({ error:
    typeof err === "string" ? err : (err?.message || JSON.stringify(err)) }) });

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

    // ---------- POST (create batch | create shipment) ----------
    if (event.httpMethod === "POST") {
      const body   = safeJSON(event.body);
      const action = (body.action || "").toLowerCase();

        // (0) verify recipient address (best-effort; never throw)
    if (action === "verify_to") {
      const to = body.to || body.address || {};

      // A) Try a dedicated address verify endpoint, if available
      try {
        const r1 = await fetch(url("/addresses/verify"), {
          method: "POST",
          headers: authH,
          body: JSON.stringify({ address: to })
        });
        const o1 = await wrap(r1);
        if (o1.ok) {
          const suggested = o1.data?.suggested || o1.data?.normalized || o1.data?.address || o1.data || null;
          return ok({ suggested });
        }
      } catch {}

      // B) Fallback variant some tenants expose
      try {
        const r2 = await fetch(url("/shipments/verify"), {
          method: "POST",
          headers: authH,
          body: JSON.stringify({ to })
        });
        const o2 = await wrap(r2);
        if (o2.ok) {
          const suggested = o2.data?.suggested || o2.data?.normalized || o2.data?.address || o2.data || null;
          return ok({ suggested });
        }
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

      // address verification (best-guess endpoint name; adjust if your tenant differs)
      if (action === "verify_to") {
        const to = body.to || {};
        // Try a dedicated address verification endpoint
        let resp = await fetch(api("/addresses/verify"), {
          method: "POST",
          headers: authJson(),
          body: JSON.stringify({ address: to })
        });
        let out  = await wrap(resp);

        // Fallback: some deployments expose a shipments verify
        if (!out.ok && out.status === 404) {
          resp = await fetch(api("/shipments/verify"), {
            method: "POST",
            headers: authJson(),
            body: JSON.stringify({ to })
          });
          out = await wrap(resp);
        }
        if (!out.ok) return bad(out.status, out.data);

        // Normalize to a simple shape for the client
        const suggested = out.data?.suggested || out.data?.normalized || out.data?.address || out.data;
        return ok({ suggested });
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

      return bad(400, "action must be create or create_shipment");
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