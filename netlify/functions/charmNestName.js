/*  netlify/functions/charmNestName.js — synchronous wrapper around _charmNestAgent
 *  mode "name". The page uses the background route (see charmNestReview.js).   */
"use strict";
const { json, gate, parseBody, CORS } = require("./_charmNestAuth");
const agent = require("./_charmNestAgent");
exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "POST") return json(405, { error: "method not allowed" });
  const body = parseBody(event);
  const denied = gate(event, body); if (denied) return denied;
  const out = await agent.run("name", body);
  return json(out && out.error ? 400 : 200, out);
};
