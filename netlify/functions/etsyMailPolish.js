/*  netlify/functions/etsyMailPolish.js
 *
 *  The composer's Polish button. Takes what staff typed in the reply box
 *  and returns it reworded: proper, warm and human, in the shop's voice,
 *  as short as it can be without losing meaning (rules: POLISH in
 *  _etsyMailPrompts.js). One model call; no tools, no Firestore, no Etsy.
 *  The inbox keeps the original words for Undo, and marks a polished send
 *  so the learner never takes the model's wording for a staff correction.
 *
 *    POST { text, customerName?, customerMessages?: [string, newest last] }
 *      → 200 { ok: true, text }
 *      → 4xx / 502 { ok: false, error }   (the inbox shows error as a toast)
 *
 *  ETSYMAIL_AI_MODEL  optional; the AI Draft button's writer, same default
 */

"use strict";

const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { callClaudeRaw } = require("./_etsyMailAnthropic");
const { POLISH } = require("./_etsyMailPrompts");

const MODEL       = process.env.ETSYMAIL_AI_MODEL || "claude-sonnet-5-5";
const MAX_TEXT    = 4000;    // characters of staff text accepted
const MAX_CONTEXT = 900;     // characters of customer messages, for tone
const MAX_TOKENS  = 3000;    // a short message plus a little low-effort reasoning
const TIMEOUT_MS  = 20000;   // inside the 26 s synchronous limit

function json(statusCode, body) {
  return { statusCode, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) };
}
function fail(error, code = 400) { return json(code, { ok: false, error }); }

// The customer's latest messages, newest last, clipped to MAX_CONTEXT in all.
function customerContext(list) {
  const msgs = (Array.isArray(list) ? list : [])
    .map(m => String(m || "").replace(/\s+/g, " ").trim()).filter(Boolean).slice(-3);
  const out = [];
  let room = MAX_CONTEXT;
  for (let i = msgs.length - 1; i >= 0 && room > 40; i--) {
    const m = msgs[i].length > room ? msgs[i].slice(0, room - 1) + "…" : msgs[i];
    out.unshift(m);
    room -= m.length;
  }
  return out;
}

function buildUserMessage({ text, customerName, customerMessages }) {
  const ctx = customerContext(customerMessages);
  const name = String(customerName || "").trim().slice(0, 80);
  const parts = [];
  if (ctx.length) {
    parts.push("<customer_messages" + (name ? ` name="${name.replace(/"/g, "'")}"` : "") + ">\n" +
      ctx.map(m => "- " + m).join("\n") + "\n</customer_messages>");
  }
  parts.push("<staff_message>\n" + text + "\n</staff_message>");
  parts.push("Polish the staff message.");
  return parts.join("\n\n");
}

// The model's text, without wrapping quotes, fences or tags it may add.
function cleanOutput(res) {
  let s = ((res && res.content) || []).filter(b => b && b.type === "text").map(b => b.text).join("\n").trim();
  s = s.replace(/^```[a-z]*\s*\n?/i, "").replace(/\n?```\s*$/, "").trim();
  s = s.replace(/^<staff_message>\s*/i, "").replace(/\s*<\/staff_message>$/i, "").trim();
  if (s.length > 1 && /^["“]/.test(s) && /["”]$/.test(s) && !/["“”]/.test(s.slice(1, -1))) s = s.slice(1, -1).trim();
  return s;
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event.httpMethod !== "POST")     return fail("Method Not Allowed", 405);

  // Same gate as the other inbox AI endpoints (etsyMailDraftReply).
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return fail("Invalid JSON body"); }

  const text = String(body.text || "").replace(/\r\n/g, "\n").trim();
  if (!text) return fail("Type a reply first");
  if (text.length > MAX_TEXT) return fail(`Too long to polish (over ${MAX_TEXT} characters)`);

  let res;
  try {
    res = await callClaudeRaw({
      model      : MODEL,
      maxTokens  : MAX_TOKENS,
      effort     : "low",
      useThinking: false,
      timeoutMs  : TIMEOUT_MS,
      system     : POLISH,
      messages   : [{ role: "user", content: [{ type: "text", text: buildUserMessage({
        text, customerName: body.customerName, customerMessages: body.customerMessages }) }] }]
    });
  } catch (e) {
    console.error("[etsyMailPolish] model call failed:", e.message);
    return fail("The polish model didn't answer. Try again.", 502);
  }
  if (res && res.stop_reason === "max_tokens") return fail("The polish came back cut off. Try again.", 502);
  const out = cleanOutput(res);
  if (!out) return fail("The polish came back empty. Try again.", 502);
  return json(200, { ok: true, text: out });
};

// For tests.
exports._internals = { customerContext, buildUserMessage, cleanOutput };
