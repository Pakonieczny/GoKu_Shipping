/* netlify/functions/testChitChats.js
 *
 * Chit Chats proxy:
 *   - GET  ?resource=batches[&status=open]     → list batches
 *   - GET                                      → light shipments sanity ping (ready)
 *   - POST { action:"create", description? }   → create batch
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

    // ---------- Helpers ----------
    const wrap = async (resp) => {
      const txt = await resp.text();
      let data; try { data = JSON.parse(txt); } catch { data = txt; }
      return { ok: resp.ok, status: resp.status, data, resp };
    };
    const ok = (data)  => ({ statusCode: 200, headers: CORS, body: JSON.stringify(data) });
    const bad = (code, err) => ({ statusCode: code, headers: CORS, body: JSON.stringify({ error: typeof err === "string" ? err : (err?.message || err) }) });

    // ---------- GET ----------
    if (event.httpMethod === "GET") {
      const qp = event.queryStringParameters || {};
      const resource = (qp.resource || "").toLowerCase();

      // GET batches (for dropdown)
      if (resource === "batches") {
        try {
          const status = qp.status ? `?status=${encodeURIComponent(qp.status)}` : "";
          const resp = await fetch(url(`/batches${status}`), { headers: authH });
          const out  = await wrap(resp);
          if (!out.ok) return bad(out.status, out.data);

          // Some environments return array, some wrap under .batches/.data — normalize:
          const list = Array.isArray(out.data) ? out.data : (out.data?.batches || out.data?.data || []);
          return ok({ success: true, batches: list });
        } catch (e) {
          return bad(500, e);
        }
      }

      // Default: quick shipments sanity ping (kept for backward compat)
      const limit = qp.limit ? Number(qp.limit) : 25;
      const page  = qp.page  ? Number(qp.page)  : 1;
      const status = qp.status || "ready";
      const resp = await fetch(url(`/shipments?status=${encodeURIComponent(status)}&limit=${encodeURIComponent(limit)}&page=${encodeURIComponent(page)}`), { headers: authH });
      const out  = await wrap(resp);
      if (!out.ok) return bad(out.status, out.data);
      return ok({ success: true, data: out.data });
    }

    // ---------- POST (create batch) ----------
    if (event.httpMethod === "POST") {
      const body = safeJSON(event.body);
      if ((body.action || "").toLowerCase() !== "create") {
        return bad(400, "action=create required");
      }
      const payload = { description: (body.description || "").toString() };

      const resp = await fetch(url("/batches"), { method: "POST", headers: authH, body: JSON.stringify(payload) });
      const out  = await wrap(resp);
      if (!out.ok) return bad(out.status, out.data);

      // Batch id is last segment of Location header
      const loc = out.resp.headers.get("Location") || "";
      const id  = loc.split("/").filter(Boolean).pop();
      return ok({ success: true, id, location: loc || null });
    }

    // ---------- PATCH (add/remove shipments to/from batch) ----------
    if (event.httpMethod === "PATCH") {
      const qp   = event.queryStringParameters || {};
      const body = safeJSON(event.body);

      // Accept both shapes from UI:
      const action = (body.action || "").toLowerCase(); // "add" | "remove"
      const batchId = numberish(body.batch_id ?? body.batchId ?? qp.batch_id ?? qp.batchId);
      // shipment(s):
      const oneIdFromQuery = qp.id ? String(qp.id) : "";
      const oneIdFromBody  = body.shipment_id ? String(body.shipment_id) : (body.shipmentId ? String(body.shipmentId) : "");
      const manyFromBody   = Array.isArray(body.shipmentIds) ? body.shipmentIds.map(String) : [];
      const shipmentIds = manyFromBody.length ? manyFromBody
                        : (oneIdFromBody ? [oneIdFromBody]
                        : (oneIdFromQuery ? [oneIdFromQuery] : []));

      if (!batchId || !shipmentIds.length) {
        return bad(400, "batch_id + at least one shipment id required");
      }
      if (action !== "add" && action !== "remove") {
        return bad(400, "action must be add|remove");
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