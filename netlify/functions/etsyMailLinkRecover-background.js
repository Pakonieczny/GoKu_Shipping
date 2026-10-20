/*  netlify/functions/etsyMailLinkRecover-background.js
 *
 *  The "Reconnect & retry all" run (see _etsyMailLinkRecover.js), as a background function: Netlify answers 202 at once and the
 *  run has up to 15 minutes (it takes 2-3: the last step watches the Etsy helper for up to two minutes). Started only by
 *  _etsyMailLinkRecover.request(), which first claims the one run on EtsyMail_Config/linkRecover and passes its runId; a call for
 *  any other run does nothing. Requires the inbox's shared secret, like every other function the server calls.
 */
"use strict";

const admin = require("./firebaseAdmin");
const { requireExtensionAuth } = require("./_etsyMailAuth");
const Recover = require("./_etsyMailLinkRecover");

exports.handler = async (event) => {
  const auth = requireExtensionAuth(event || { headers: {} });
  if (!auth.ok) return auth.response;
  let body = {};
  try { body = JSON.parse((event && event.body) || "{}"); } catch (e) { body = {}; }
  const runId = typeof body.runId === "string" ? body.runId.slice(0, 40) : "";
  if (!runId) return { statusCode: 400, body: JSON.stringify({ error: "Missing runId" }) };
  const out = await Recover.run(admin.firestore(), { runId });
  console.log("etsyMailLinkRecover:", JSON.stringify({ runId, ok: out.ok, skipped: out.skipped || false, after: out.after ? out.after.level : null }));
  return { statusCode: 200, body: JSON.stringify(out) };
};
