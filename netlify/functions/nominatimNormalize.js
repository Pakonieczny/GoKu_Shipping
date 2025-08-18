/* /.netlify/functions/nominatimNormalize.js
 * OpenStreetMap Nominatim — free normalization (worldwide, best-effort)
 */
const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "OPTIONS,POST"
};

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 200, headers: CORS, body: "ok" };
  }

  try {
    const { q, country } = JSON.parse(event.body || "{}");
    const cc = (country || "").toLowerCase();
    const url = `https://nominatim.openstreetmap.org/search?format=json&limit=1&addressdetails=1${cc ? `&countrycodes=${encodeURIComponent(cc)}` : ""}&q=${encodeURIComponent(q || "")}`;

    const r = await fetch(url, { headers: { "User-Agent": "Brites-Addr-Normalizer/1.0 (contact@example.com)" } });
    const list = await r.json();
    const hit = Array.isArray(list) ? list[0] : null;
    const a = hit?.address || null;

    const suggested = a ? {
      address1: [a.house_number, a.road].filter(Boolean).join(" "),
      address2: [a.neighbourhood, a.suburb].filter(Boolean).join(", "),
      city    : a.city || a.town || a.village || a.hamlet || "",
      state   : a.state || a.region || a.province || "",
      postal  : a.postcode || "",
      country : (a.country_code || "").toUpperCase()
    } : null;

    return { statusCode: 200, headers: CORS, body: JSON.stringify({ suggested, raw: hit || null }) };
  } catch (err) {
    return { statusCode: 200, headers: CORS, body: JSON.stringify({ suggested: null, error: err.message }) };
  }
};