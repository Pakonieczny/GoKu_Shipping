/* netlify/functions/smartyVerify.js */
const fetch = require("node-fetch");

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
};

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  try {
    if (event.httpMethod !== "POST") {
      return { statusCode: 405, headers: CORS, body: "Method Not Allowed" };
    }

    const { to } = JSON.parse(event.body || "{}") || {};
    if (!to || (to.to_country_code || "").toUpperCase() !== "US") {
      return {
        statusCode: 200,
        headers: CORS,
        body: JSON.stringify({ suggested: null, note: "Non-US or empty address" }),
      };
    }

    // Auth: prefer secret key pair, fallback to embedded website key
    const AUTH_ID  = process.env.SMARTY_AUTH_ID || "";
    const AUTH_TOK = process.env.SMARTY_AUTH_TOKEN || "";
    const EMB_KEY  = process.env.SMARTY_EMBEDDED_KEY || "";

    const qs = new URLSearchParams();
    // Address inputs
    const street = [to.to_address_1 || "", to.to_address_2 || ""].filter(Boolean).join(" ");
    qs.set("street", street);
    if (to.to_city)            qs.set("city",   to.to_city);
    if (to.to_province_code)   qs.set("state",  to.to_province_code);
    if (to.to_postal_code)     qs.set("zipcode", to.to_postal_code);
    // Behavior
    qs.set("candidates", "5");        // give us options
    qs.set("match", "enhanced");      // include enhanced_match analysis (deliverability hints)
    // Auth
    if (AUTH_ID && AUTH_TOK) {
      qs.set("auth-id", AUTH_ID);
      qs.set("auth-token", AUTH_TOK);
    } else if (EMB_KEY) {
      qs.set("key", EMB_KEY);
    } else {
      return {
        statusCode: 500,
        headers: CORS,
        body: JSON.stringify({ error: "Smarty keys not configured" }),
      };
    }

    const url = `https://us-street.api.smarty.com/street-address?${qs.toString()}`;
    const resp = await fetch(url, { method: "GET" });
    const data = await resp.json();

    if (!Array.isArray(data) || data.length === 0) {
      return { statusCode: 200, headers: CORS, body: JSON.stringify({ suggested: null, raw: data }) };
    }

    // pick best candidate (prefer deliverable Y, else first)
    const pick =
      data.find(d => d.analysis?.dpv_match_code === "Y") ||
      data[0];

    const c  = pick.components || {};
    const zip = [c.zipcode || "", c.plus4_code ? `-${c.plus4_code}` : ""].join("");

    const suggested = {
      to_name          : to.to_name || "",
      to_address_1     : pick.delivery_line_1 || [c.primary_number, c.street_predirection, c.street_name, c.street_suffix, c.street_postdirection].filter(Boolean).join(" "),
      to_address_2     : [c.secondary_designator, c.secondary_number].filter(Boolean).join(" ") || "",
      to_city          : c.city_name || "",
      to_province_code : c.state_abbreviation || "",
      to_postal_code   : zip.trim(),
      to_country_code  : "US",
    };

    return {
      statusCode: 200,
      headers: CORS,
      body: JSON.stringify({ suggested, raw: pick }),
    };
  } catch (err) {
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: String(err && err.message || err) }) };
  }
};