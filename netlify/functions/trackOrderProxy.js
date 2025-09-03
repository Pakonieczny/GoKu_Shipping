// trackOrderProxy.js  — proxy → ShipStation Mark-As-Shipped
// Requires two Netlify env-vars:
//   SS_API_KEY    = your ShipStation “API Key”
//   SS_API_SECRET = the matching “API Secret”
//
// NOTE: Do NOT use the frontend's baseURL here. This runs on Netlify.
const fetch = require("node-fetch");
const SS_BASE = "https://ssapi.shipstation.com";
const { SS_API_KEY, SS_API_SECRET } = process.env;
const SS_AUTH    = Buffer.from(`${SS_API_KEY || ""}:${SS_API_SECRET || ""}`).toString("base64");
const SS_HEADERS = {
  Authorization : `Basic ${SS_AUTH}`,
  "Content-Type": "application/json",
  Accept        : "application/json"
};

exports.handler = async event => {
  // Guard: missing credentials → explicit 500 with clear message
  if (!SS_API_KEY || !SS_API_SECRET) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Missing SS_API_KEY/SS_API_SECRET environment variables" })
    };
  }
  try {
    /* Front-end sends { receiptId, tracking, carrier } — keep names intact */
    const {
      receiptId:   orderNumber,
      tracking:    trackingNumber,
      carrier:     carrierCode,
      shipDate
    } = JSON.parse(event.body || "{}");

    /* Basic validation */
    if (!orderNumber || !trackingNumber || !carrierCode) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing orderNumber / trackingNumber / carrierCode" })
      };
    }

    /* 1️⃣  Mark the order as shipped by orderNumber (ShipStation can notify marketplace) */
    const markResp = await fetch(`${SS_BASE}/orders/markasshipped`, {
      method : "POST",
      headers: SS_HEADERS,
      body   : JSON.stringify((() => {
        const payload = {
          orderNumber,
          trackingNumber,
          shipDate: (shipDate || new Date().toISOString().slice(0, 10)),
          notifyCustomer: false,          // UI handles comms; marketplace still notified
          notifySalesChannel: true
        };
        // Map UI carriers → ShipStation form
        const cc = (carrierCode || "").toLowerCase();
        if (cc === "chitchats") {
          payload.carrierCode = "other";
          payload.carrierName = "Chit Chats";
        } else if (cc === "usps") {
          // Generic label if USPS isn’t integrated; avoids invalid codes
          payload.carrierCode = "other";
          payload.carrierName = "USPS";
        } else {
          payload.carrierCode = carrierCode;
        }
        return payload;
      })())
    });

    return { statusCode: markResp.status, body: await markResp.text() };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};