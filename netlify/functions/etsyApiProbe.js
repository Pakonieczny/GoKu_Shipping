/*  netlify/functions/etsyApiProbe.js
 *
 *  GET /.netlify/functions/etsyApiProbe?app=<appId>
 *
 *  Spends EXACTLY ONE Etsy API call to refresh the authoritative whole-key
 *  rate-limit headers (x-limit-per-day / x-remaining-today), then records
 *  them so etsyApiUsage can serve them for free afterwards.
 *
 *  ═══ WHY A PROBE EXISTS AT ALL ══════════════════════════════════════════
 *
 *  Etsy only reports the whole-key meter in response headers. If the shop
 *  has been idle, the newest header reading may be hours old — and a stale
 *  "Key" row is worse than no row, because it looks live. One ping refreshes
 *  it. The endpoint Etsy provides for exactly this is openapi-ping: the
 *  cheapest call in the API, key-only (no OAuth, no shop scope), returning
 *  just the application id — but carrying the same rate-limit headers as
 *  every other response.
 *
 *  ═══ WHY IT IS CALLED SPARINGLY ════════════════════════════════════════
 *
 *  A probe is not free — it consumes one of the 5,000 daily calls. The
 *  clients therefore only probe when the stored header reading is actually
 *  stale (>5 min) and never more than once a minute even when it fails.
 *  This endpoint deliberately holds the same line server-side so a
 *  misbehaving or out-of-date client cannot burn quota on probes:
 *  a probe within PROBE_MIN_INTERVAL_MS of the last recorded header returns
 *  the existing reading and reports skipped:true instead of calling Etsy.
 *  Pass ?force=1 to override (the console's manual "Update" button does).
 *
 *  Response mirrors etsyApiUsage's contract and adds:
 *    probed  : true|false   whether an Etsy call was actually made
 *    skipped : "fresh"      why it declined to spend a call, when applicable
 */

"use strict";

const fetch = require("node-fetch");
const usage = require("./_etsyApiUsage");

const PING_URL = "https://openapi.etsy.com/v3/application/openapi-ping";
// Server-side floor. The clients throttle themselves too; this exists so
// quota can't be burned by a stale deploy or an over-eager tab.
const PROBE_MIN_INTERVAL_MS = 5 * 60 * 1000;

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Accept",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
};

const json = (statusCode, body) => ({
  statusCode,
  headers: {
    "Content-Type": "application/json",
    "Cache-Control": "no-store, max-age=0",
    ...CORS,
  },
  body: JSON.stringify(body),
});

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "GET") {
    return json(405, { ok: false, verified: false, error: "Method not allowed" });
  }

  const q = event.queryStringParameters || {};
  const appId = String(q.app || "unattributed").trim() || "unattributed";
  const force = q.force === "1" || q.force === "true";

  try {
    // Same key the working Etsy proxies on this site use.
    const key = process.env.CLIENT_ID;
    if (!key) {
      return json(500, {
        ok: false,
        verified: false,
        error: "Missing CLIENT_ID — cannot reach Etsy to verify rate limits.",
        server_time: Date.now(),
      });
    }

    // Don't spend a call if the stored reading is already fresh.
    const before = await usage.readUsage(appId);
    const reportedAt = before && before.etsy_reported_at ? Number(before.etsy_reported_at) : 0;
    const age = reportedAt ? Date.now() - reportedAt : Infinity;
    if (!force && Number.isFinite(age) && age >= 0 && age < PROBE_MIN_INTERVAL_MS) {
      return json(200, { ...before, probed: false, skipped: "fresh" });
    }

    const res = await fetch(PING_URL, {
      method: "GET",
      headers: { "x-api-key": key, Accept: "application/json" },
    });

    // Headers are the entire point of the call — capture them whatever the
    // status was. Etsy returns rate-limit headers on errors too, and a 429
    // in particular carries the most important reading of all.
    usage.captureHeaders(res);
    // The probe is itself an Etsy call against our budget; count it honestly
    // rather than letting monitoring traffic go untracked.
    usage.recordCall(appId, res);
    await usage.flushNow();

    if (!res.ok) {
      const text = await res.text().catch(() => "");
      const after = await usage.readUsage(appId);
      return json(res.status === 429 ? 200 : 502, {
        ...after,
        // A 429 still refreshed the headers, so the reading is valid.
        verified: res.status === 429 ? after.verified : false,
        probed: true,
        error: "Etsy ping returned HTTP " + res.status + (text ? ": " + text.slice(0, 200) : ""),
      });
    }

    const after = await usage.readUsage(appId);
    return json(200, { ...after, probed: true });
  } catch (e) {
    return json(503, {
      ok: false,
      verified: false,
      probed: false,
      error: "Rate-limit verification failed: " + (e && e.message ? e.message : e),
      server_time: Date.now(),
    });
  }
};
