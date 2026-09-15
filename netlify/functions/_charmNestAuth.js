/*  netlify/functions/_charmNestAuth.js
 *  Shared door + response helpers for the Charm Nesting Station functions.
 *  Same contract as authGate.js: env EDIT_PASSCODE (unset ⇒ open), header
 *  X-Edit-Passcode, 401 {error:"unauthorized"} on mismatch. Constant-time
 *  compare through a hash so lengths leak nothing.                          */
"use strict";
const crypto = require("crypto");

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, X-Edit-Passcode, Authorization",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Max-Age": "86400",
  "Cache-Control": "no-store",
  "Content-Type": "application/json"
};
const json = (statusCode, body) => ({ statusCode, headers: CORS, body: JSON.stringify(body) });

function header(event, name) {
  const h = (event && event.headers) || {}; const want = name.toLowerCase();
  if (h[want] != null) return h[want];
  for (const k in h) if (k.toLowerCase() === want) return h[k];
  return "";
}
function sameSecret(a, b) {
  const ha = crypto.createHash("sha256").update(String(a == null ? "" : a), "utf8").digest();
  const hb = crypto.createHash("sha256").update(String(b == null ? "" : b), "utf8").digest();
  return crypto.timingSafeEqual(ha, hb);
}
/** Returns null when the request may proceed, or a 401 response object. */
function gate(event, body) {
  const configured = process.env.EDIT_PASSCODE || "";
  if (!configured) return null;
  const supplied = header(event, "x-edit-passcode") || (body && body.passcode) || "";
  if (supplied && sameSecret(supplied, configured)) return null;
  return json(401, { error: "unauthorized" });
}
function parseBody(event) {
  if (!event || !event.body) return {};
  try { return JSON.parse(event.isBase64Encoded ? Buffer.from(event.body, "base64").toString("utf8") : event.body) || {}; } catch (_) { return {}; }
}
const str = (v, n = 400) => (v == null ? "" : String(v)).slice(0, n);
const num = v => (Number.isFinite(Number(v)) ? Number(v) : 0);
/** Storage keys are confined to the charmnest/ prefix and plain characters. */
function safePath(p) {
  const s = str(p, 300).replace(/\\/g, "/").replace(/[^\w.\-\/]+/g, "_").replace(/\/+/g, "/").replace(/^\/+/, "").replace(/\.\.+/g, ".");
  if (!s.startsWith("charmnest/")) return "charmnest/" + s;
  return s;
}
module.exports = { CORS, json, gate, parseBody, header, str, num, safePath };
