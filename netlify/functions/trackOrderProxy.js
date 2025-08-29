// trackOrderProxy.js — Netlify function
// Env: SS_API_KEY, SS_API_SECRET
const fetch = require("node-fetch");

exports.handler = async (event) => {
  try {
    const {
      receiptId: orderNumber,
      tracking: trackingNumber,
      carrier,
      shipDate, // optional: "YYYY-MM-DD" or "today"
    } = JSON.parse(event.body || "{}");

    if (!orderNumber || !trackingNumber || !carrier) {
      return resp(400, { error: "Missing orderNumber / trackingNumber / carrier" });
    }

    const auth = Buffer.from(
      `${process.env.SS_API_KEY}:${process.env.SS_API_SECRET}`
    ).toString("base64");

    const baseURL = "https://ssapi.shipstation.com";
    const headers = { Authorization: `Basic ${auth}` };

    // 1) Find the SS order by Etsy receiptId
    const lookup = await fetch(
      `${baseURL}/orders?orderNumber=${encodeURIComponent(orderNumber)}`,
      { headers }
    );
    if (!lookup.ok) return rawForward(lookup);

    const { orders = [] } = await lookup.json();
    const orderId = orders?.[0]?.orderId;
    if (!orderId) return resp(404, { error: "Order not found in ShipStation" });

    // 2) Normalize carrier for SS
    const norm = normalizeCarrier(carrier);

    // 3) Mark as shipped (notify customer + Etsy)
    const mark = await fetch(`${baseURL}/orders/markasshipped`, {
      method: "POST",
      headers: { ...headers, "Content-Type": "application/json" },
      body: JSON.stringify({
        orderId,
        carrierCode: norm.carrierCode,
        carrierName: norm.carrierName,       // only used when code === "other"
        trackingNumber,
        shipDate: shipDate || new Date().toISOString().slice(0, 10),
        notifyCustomer: true,
        notifySalesChannel: true,
      }),
    });

    // Forward ShipStation's response body + status as JSON
    const text = await mark.text();
    let body;
    try { body = JSON.parse(text); } catch { body = { raw: text }; }
    return resp(mark.status, body);
  } catch (err) {
    return resp(500, { error: err.message });
  }
};

// --- helpers ---
function normalizeCarrier(value) {
  const v = String(value || "").trim().toLowerCase();
  // Known ShipStation codes we care about here:
  if (v === "usps") return { carrierCode: "usps", carrierName: undefined };

  // Fall back to "other" while labeling clearly so it shows up in Etsy
  const label = v === "chitchats" ? "Chit Chats" : capitalize(v || "Other");
  return { carrierCode: "other", carrierName: label };
}

function capitalize(s) { return s.charAt(0).toUpperCase() + s.slice(1); }

async function rawForward(res) {
  const text = await res.text();
  let body; try { body = JSON.parse(text); } catch { body = { raw: text }; }
  return resp(res.status, body);
}

function resp(statusCode, body) {
  return { statusCode, body: JSON.stringify(body) };
}