/*  netlify/functions/charmNestReview.js — synchronous wrapper around _charmNestAgent.
 *  Kept for small inputs and tests; the page uses charmNestAgent-background via
 *  charmNestLibrary startAgent/getAgent because a high-effort review of a whole
 *  sheet exceeds Netlify's ~10 s synchronous limit (504).                     */
"use strict";
const { json, gate, parseBody, CORS } = require("./_charmNestAuth");
const agent = require("./_charmNestAgent");
exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "POST") return json(405, { error: "method not allowed" });
  const body = parseBody(event);
  const denied = gate(event, body); if (denied) return denied;
  const out = await agent.run(body.mode === "layout" ? "layout" : "grouping", body);
  return json(out && out.error ? 400 : 200, out);
};
