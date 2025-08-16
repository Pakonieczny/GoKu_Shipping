/* netlify/functions/testChitChats.js */
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
      return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: "Missing CHIT_CHATS_CLIENT_ID or CHIT_CHATS_ACCESS_TOKEN" }) };
    }

    const authH = { "Authorization": ACCESS_TOKEN, "Content-Type": "application/json; charset=utf-8" };
    const url   = (p) => `${BASE}/clients/${encodeURIComponent(CLIENT_ID)}${p}`;
    const wrap  = async (resp) => {
      const txt = await resp.text(); let data; try { data = JSON.parse(txt); } catch { data = txt; }
      return { ok: resp.ok, status: resp.status, data, resp };
    };

    // Create a new batch
    if (event.httpMethod === "POST") {
      const body = event.body ? JSON.parse(event.body) : {};
      if (body.action !== "create") {
        return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "action=create required" }) };
      }
      const resp = await fetch(url("/batches"), { method: "POST", headers: authH, body: JSON.stringify({ description: body.description || "" }) });
      const out  = await wrap(resp);
      if (!out.ok) return { statusCode: out.status, headers: CORS, body: JSON.stringify({ error: out.data }) };

      const loc = out.resp.headers.get("Location") || "";
      const id  = loc.split("/").filter(Boolean).pop();
      return { statusCode: 200, headers: CORS, body: JSON.stringify({ success: true, id, location: loc || null }) };
    }

    // Add/remove shipments to/from a batch
    if (event.httpMethod === "PATCH") {
      const body = event.body ? JSON.parse(event.body) : {};
      const { action, batchId, shipmentIds } = body;

      if (!batchId || !Array.isArray(shipmentIds) || !shipmentIds.length) {
        return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "batchId + shipmentIds required" }) };
      }

      const path = action === "add" ? "/shipments/add_to_batch"
                 : action === "remove" ? "/shipments/remove_from_batch"
                 : null;
      if (!path) {
        return { statusCode: 400, headers: CORS, body: JSON.stringify({ error: "action must be add|remove" }) };
      }

      const payload = { batch_id: Number(batchId), shipment_ids: shipmentIds };
      const resp    = await fetch(url(path), { method: "PATCH", headers: authH, body: JSON.stringify(payload) });
      const out     = await wrap(resp);
      if (!out.ok) return { statusCode: out.status, headers: CORS, body: JSON.stringify({ error: out.data }) };

      return { statusCode: 200, headers: CORS, body: JSON.stringify({ success: true }) };
    }

    // Optional sanity check (lists a few ready shipments)
    if (event.httpMethod === "GET") {
      const resp = await fetch(url("/shipments?status=ready&limit=25&page=1"), { headers: authH });
      const out  = await wrap(resp);
      return { statusCode: out.ok ? 200 : out.status, headers: CORS, body: JSON.stringify(out.ok ? { success: true, data: out.data } : { error: out.data }) };
    }

    return { statusCode: 405, headers: CORS, body: "Method Not Allowed" };

  } catch (error) {
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: error.message }) };
  }
};