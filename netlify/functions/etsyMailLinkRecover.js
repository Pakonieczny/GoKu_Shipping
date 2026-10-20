/*  netlify/functions/etsyMailLinkRecover.js
 *
 *  The inbox's "Reconnect & retry all" (the Charm Sorter has the same through etsyMailOrderLink ops link_recover /
 *  link_recover_status). Requires the inbox's shared secret.
 *    POST { op: "start", by }   claims the one recovery run and starts it in the background (a press while one runs joins it)
 *    POST { op: "status" }      the run's progress and result (EtsyMail_Config/linkRecover)
 *  The work and its rules: _etsyMailLinkRecover.js.
 */
"use strict";

const admin = require("./firebaseAdmin");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");
const Recover = require("./_etsyMailLinkRecover");

const json = (statusCode, body) => ({ statusCode, headers: CORS, body: JSON.stringify(body) });

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event.httpMethod !== "POST") return json(405, { error: "Method Not Allowed" });
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch (e) { return json(400, { error: "Invalid JSON" }); }
  const db = admin.firestore();
  try {
    if (body.op === "start") {
      const r = await Recover.request(db, { by: typeof body.by === "string" && body.by.trim() ? body.by.trim().slice(0, 60) : "the inbox", auto: false });
      return json(200, { started: r.started, joined: !r.started, status: await Recover.status(db) });
    }
    if (body.op === "status") return json(200, { status: await Recover.status(db) });
    return json(400, { error: "Unknown op" });
  } catch (e) {
    console.error("etsyMailLinkRecover:", e);
    return json(500, { error: e && e.message || String(e) });
  }
};
