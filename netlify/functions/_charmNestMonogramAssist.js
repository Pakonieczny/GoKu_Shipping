/*  netlify/functions/_charmNestMonogramAssist.js
 *  MONOAI (Paul, 15 Oct 2026): "we do not sell many of these monograms and they need intelligent assembly: the AI should have freedom to resize:
 *  if a letter is too thin make it wider, if too small a bit bigger, a little nudge to connect better, just like a human would."
 *  A monogram that still breaks a rule after the engine's own autoFit is looked at by a vision model, like the person who assembles it by hand:
 *  it is shown the candidate (contacts drawn: upper blue, lower magenta) beside the ESG reference, with the numbers and the broken rules, and answers
 *  ONLY the numbers of the engine's adjust contract { L, M, R: { scale, sx, dx, dy } } and one sentence. It never returns geometry or SVG. The numbers
 *  are clamped here to the contract's bounds (and again inside Engine.build, which never trusts them); the page re-builds, re-measures and keeps a round
 *  only if it is better. This file is the model side; charmNestMonogramAssist-background.js runs it with the background budget (a vision call at effort
 *  medium can pass Netlify's 10 s synchronous limit), started and polled like every Claude job (charmNestLibrary startAgent / getAgent).
 *    - skipped, never an error: CHARM_NEST_MONOGRAM_AI=off (kill switch) or no ANTHROPIC_API_KEY
 *    - at most DAILY_MAX model calls per order per day (Charm_Nest_MonoAssist/{order}_{day}, reserved before the call; each call's usage is logged on it)
 *    - the request carries no secret and no order data beyond the three letters and the numbers; the system prompt is cached (cache_control)       */
"use strict";
const { str, num } = require("./_charmNestAuth");

const MODEL = process.env.CHARM_NEST_MONOGRAM_MODEL || "claude-sonnet-5";
const EFFORT = /^(low|medium|high)$/.test(process.env.CHARM_NEST_MONOGRAM_EFFORT || "") ? process.env.CHARM_NEST_MONOGRAM_EFFORT : "medium";
const COLL = "Charm_Nest_MonoAssist";
const DAILY_MAX = 3, MAX_ROUND = 3, MAX_IMAGE_B64 = 2200000, MAX_METRICS_CHARS = 6000;
/** The adjust contract's hard bounds (the engine clamps to the same ones: scale about the letter's body centre, sx extra horizontal-only stretch, dx / dy in mm, x right, y up). */
const BOUNDS = { scale: [0.85, 1.15], sx: [0.8, 1.25], dx: [-2, 2], dy: [-1.5, 1.5] };
const IDENTITY = { scale: 1, sx: 1, dx: 0, dy: 0 };
const SLOTS = ["L", "M", "R"];

const INSTRUCTIONS = `You assemble monogram charms for a small jewellery shop that laser-cuts them in gold filled, sterling silver and rose gold. A buyer's three letters (a Left letter, a bigger Middle letter and a Right letter, each from its own script alphabet) are laid out and joined into ONE flat charm piece by software. You do what a careful person does at the bench when the software's layout is not right: you look at the picture and make small adjustments to the size, width and position of a letter until the monogram reads like the model.

You receive: the three letters; the current adjustment of each letter; the measurements of the current layout; the rules it breaks, in words; the tries of earlier rounds that were refused; image 1, the MODEL (the word ESG, a perfect monogram: each letter easy to see and identify, the sides slightly smaller than the middle, joined at the top and at the bottom without being mixed up with the middle letter); image 2, the CURRENT candidate. On image 2 the solid contacts where two letters join are painted: upper contacts blue, lower contacts magenta.

What a good monogram is (the rules, in plain words):
1. Level and aligned: the Left and Right letters stand level with each other and centred on the Middle letter (their centres within 0.5 mm of the Middle's, the two sides within 0.15 mm of each other); a side letter never sticks out above or below the Middle letter by more than 0.5 mm.
2. Not squished, not tiny: a side letter's height is 0.74 to 0.84 of the Middle letter's (the model is about 0.80), its width 0.62 to 0.95 of the Middle's, each side letter shows at least 3.7 mm of itself beside the Middle letter; the whole piece is slightly taller than wide (width / height 0.82 to 1.00, the model 0.88). A letter that looks thin or squeezed must be made wider; one that looks too small, a little bigger.
3. Legible: a letter is not buried in another (at most 0.25 of a letter's ink lies inside the others), strokes of different letters that do not touch are not closer than 0.35 mm along more than 3 mm in all, and the open insides of letters (counters) stay at least 90 % open.
4. Balanced contacts: each neighbouring pair (Left-Middle and Middle-Right) is joined by at least TWO solid contacts (0.2 mm or wider), at least one in the upper 40 % and at least one in the lower 40 % of the height where the two letters share space, the two at least 0.45 of that height apart. Two contacts only at the top (all blue, nothing magenta) is a failure: the joint is unstable and breaks off. Fix it by moving a side letter a little toward the middle one, up or down (dx, dy) or by changing its size so a lower (or upper) stroke reaches.
5. One piece, level, the counters open.
Anything that cannot meet all of them is left to a person; say so in the sentence instead of ruining a letter.

Your answer: ONLY the JSON { "adjust": { "L": {scale, sx, dx, dy}, "M": {...}, "R": {...} }, "why": "one sentence" }. The adjust you return REPLACES the current one: give the complete new numbers for all three letters (the current number where you change nothing), never a difference.
- scale: size multiplier of the whole letter about its centre (1 = as laid out); allowed 0.85 to 1.15.
- sx: extra stretch across only (a thin or narrow letter wider: more than 1; a wide one narrower: less than 1); allowed 0.8 to 1.25.
- dx, dy: move of the letter in millimetres, x to the right, y UP, relative to where the software put it; allowed -2.0 to 2.0 for dx and -1.5 to 1.5 for dy.
Whatever you give is clamped to these bounds by the software, so there is no use going past them.
How to work: change only what the broken rules point at, one or two things per round, in small steps (a few percent of scale or sx, a few tenths of a millimetre of move): a refused earlier try tells you the step was wrong or too big, so do something different. Keep the two side letters level with each other. A letter must NEVER be altered to the point where it stops reading as that letter: never squeeze or stretch a letter so far that its shape changes, never push it so far that it is buried in or detached from its neighbour. If a rule cannot be met without doing that, return the current adjust unchanged and say in "why" which rule cannot be met and why.
"why" is one plain sentence (at most 25 words) saying what you changed and for which rule, e.g. "Widened the right G and moved it down 0.4 mm so a lower contact joins it to the S."`;

const ADJ_SCHEMA = { type: "object", additionalProperties: false, properties: { scale: { type: "number" }, sx: { type: "number" }, dx: { type: "number" }, dy: { type: "number" } }, required: ["scale", "sx", "dx", "dy"] };
const SCHEMA = {
  type: "object", additionalProperties: false,
  properties: { adjust: { type: "object", additionalProperties: false, properties: { L: ADJ_SCHEMA, M: ADJ_SCHEMA, R: ADJ_SCHEMA }, required: ["L", "M", "R"] }, why: { type: "string" } },
  required: ["adjust", "why"]
};

const cleanKey = k => String(k || "").replace(/[^\w\-]/g, "").slice(0, 80);
const clamp = (v, [lo, hi]) => Math.min(hi, Math.max(lo, v));
const r3 = v => Math.round(v * 1000) / 1000;
/** One letter's adjust, whatever came in: missing or non-numeric fields keep `base`'s (else the identity), every field clamped to the bounds. */
function clampOne(a, base) {
  const out = {}; let clamped = false; base = base || IDENTITY;
  for (const k of Object.keys(BOUNDS)) {
    const raw = a && typeof a === "object" && a[k] != null && a[k] !== "" && Number.isFinite(Number(a[k])) ? Number(a[k]) : base[k];
    const cl = clamp(raw, BOUNDS[k]); if (cl !== raw) clamped = true; out[k] = r3(cl);
  }
  return { a: out, clamped };
}
/** { L, M, R } clamped; `base` is the current adjust (what a missing letter or field keeps). */
function clampAdjust(adj, base) {
  const out = {}; let clamped = false;
  for (const s of SLOTS) { const r = clampOne(adj && adj[s], base && base[s]); out[s] = r.a; if (r.clamped) clamped = true; }
  return { adjust: out, clamped };
}
/** JSON-safe, bounded copy of the engine's report.metrics: numbers, booleans and short strings, a few levels deep. */
function plain(v, depth) {
  if (typeof v === "number") return Number.isFinite(v) ? r3(v) : null;
  if (typeof v === "boolean") return v;
  if (typeof v === "string") return str(v, 160);
  if (depth > 4 || v == null || typeof v !== "object") return null;
  if (Array.isArray(v)) return v.slice(0, 24).map(x => plain(x, depth + 1));
  const o = {}; for (const k of Object.keys(v).slice(0, 48)) { const x = plain(v[k], depth + 1); if (x !== null) o[str(k, 40)] = x; } return o;
}
const pngOk = u => typeof u === "string" && /^data:image\/png;base64,[A-Za-z0-9+/=]+$/.test(u) && u.length <= MAX_IMAGE_B64 && u.slice(22, 38).startsWith("iVBORw0KGgo");

/** The page's request, bounded. → { req } or { error }. */
function cleanRequest(b) {
  b = b || {};
  const orderKey = cleanKey(b.orderKey); if (!orderKey) return { error: "which order?" };
  const letters = (Array.isArray(b.letters) ? b.letters : []).map(x => String(x || "").toUpperCase());
  if (letters.length !== 3 || !letters.every(x => /^[A-Z]$/.test(x))) return { error: "three letters needed" };
  if (!pngOk(b.image)) return { error: "the candidate picture is missing or is not a PNG" };
  const round = Math.min(MAX_ROUND, Math.max(1, Math.round(num(b.round)) || 1));
  const adjust = clampAdjust(b.adjust, null).adjust;
  let metrics = JSON.stringify(plain(b.metrics, 0) || {}); if (metrics.length > MAX_METRICS_CHARS) metrics = metrics.slice(0, MAX_METRICS_CHARS) + "…";
  const violations = (Array.isArray(b.violations) ? b.violations : []).slice(0, 12).map(x => str(x && typeof x === "object" ? x.text || x.rule : x, 220)).filter(Boolean);
  const history = (Array.isArray(b.history) ? b.history : []).slice(-2).map(h => ({ adjust: clampAdjust(h && h.adjust, null).adjust, why: str(h && h.why, 200), violations: Math.max(0, Math.round(num(h && h.violations))) }));
  return { req: { orderKey, letters, image: b.image, round, adjust, metrics, violations, history } };
}

const adjText = a => SLOTS.map(s => `${s}: scale ${a[s].scale}, sx ${a[s].sx}, dx ${a[s].dx} mm, dy ${a[s].dy} mm`).join(" | ");
/** The request content: the numbers in words, then the model picture, then the candidate picture. */
function content(req, ref) {
  const [l, m, r] = req.letters;
  const text = [
    `Round ${req.round} of ${MAX_ROUND}. The buyer's letters: Left ${l}, Middle ${m}, Right ${r} (the monogram reads ${l} ${m} ${r}).`,
    `Current adjustment: ${adjText(req.adjust)}`,
    `Rules it breaks now: ${req.violations.length ? req.violations.map(v => "\n- " + v).join("") : "none reported (look at the picture)"}`,
    `Measurements of the current layout (JSON from the layout engine): ${req.metrics}`,
    req.history.length ? `Earlier tries of this word that were refused (they did not improve it): ${req.history.map(h => `[${adjText(h.adjust)}${h.why ? " · " + h.why : ""} · ${h.violations} rules broken]`).join(" ")}` : "Earlier tries: none.",
    "Image 1 is the MODEL (the word ESG). Image 2 is the CURRENT candidate (upper contacts blue, lower contacts magenta). Answer with the JSON only."
  ].join("\n");
  const png = d => ({ type: "image", source: { type: "base64", media_type: "image/png", data: String(d).replace(/^data:image\/png;base64,/, "") } });
  const out = [{ type: "text", text }];
  if (ref) out.push({ type: "text", text: "Image 1: the MODEL, ESG" }, png(ref));
  out.push({ type: "text", text: "Image 2: the CURRENT candidate " + l + m + r }, png(req.image));
  return out;
}

/** The model's answer → { adjust, why, clamped } (numbers clamped to the bounds; a missing letter or field keeps `base`'s). Throws on anything that is not the asked JSON. */
function normalize(parsed, base) {
  if (!parsed || typeof parsed !== "object" || !parsed.adjust || typeof parsed.adjust !== "object") throw new Error("unreadable answer");
  const c = clampAdjust(parsed.adjust, base);
  const why = String(parsed.why == null ? "" : parsed.why).replace(/\s+/g, " ").trim().slice(0, 240) || "Adjusted the letters.";
  return { adjust: c.adjust, why, clamped: c.clamped };
}

const dayOf = ms => new Date(ms).toISOString().slice(0, 10);
const off = () => /^(off|0|false|no|disabled)$/i.test(String(process.env.CHARM_NEST_MONOGRAM_AI || "").trim());

/** One round: the model looks at the candidate and answers the numbers. Never throws for a normal failure: { skipped }, { error }, or { adjust, why, usage, ... }. */
async function run(body, opts = {}) {
  if (off()) return { skipped: "monogram AI assist is switched off (CHARM_NEST_MONOGRAM_AI=off)" };
  if (!process.env.ANTHROPIC_API_KEY && !opts.anthropic) return { skipped: "ANTHROPIC_API_KEY is not set" };
  const c = cleanRequest(body); if (c.error) return { error: c.error };
  const req = c.req, now = opts.now || Date.now(), dailyMax = opts.dailyMax || DAILY_MAX, day = dayOf(now);
  let admin = opts.admin; const need = () => admin || (admin = require("./firebaseAdmin"));
  const db = opts.db || need().firestore(), FV = opts.FV || need().firestore.FieldValue;
  const ref = db.collection((opts.sandbox ? "Sandbox_" : "") + COLL).doc(`${req.orderKey}_${day}`);
  // the day's allowance for this order: reserved before the call (a call that fails after it started is still counted: it may have been paid for)
  const left = await db.runTransaction(async tx => {
    const s = await tx.get(ref), n = (s.exists && s.data().calls) || 0;
    if (n >= dailyMax) return -1;
    tx.set(ref, { order: req.orderKey, day, calls: n + 1, updatedAt: now }, { merge: true }); return dailyMax - n - 1;
  });
  if (left < 0) return { skipped: `the daily limit of ${dailyMax} AI calls for this order is reached`, limited: true };
  const anthropic = opts.anthropic || require("./_etsyMailAnthropic");
  let refPng = opts.refPng; if (refPng === undefined) { try { refPng = require("./_charmNestMonogramRef").ESG_PNG_DATA_URL; } catch (_) { refPng = null; } }
  const usage = { input_tokens: 0, output_tokens: 0, cache_read_input_tokens: 0, cache_creation_input_tokens: 0 };
  let res;
  try {
    res = await anthropic.callClaudeRaw({ model: opts.model || MODEL, maxTokens: 8000, effort: opts.effort || EFFORT, system: [{ type: "text", text: INSTRUCTIONS, cache_control: { type: "ephemeral" } }], messages: [{ role: "user", content: content(req, refPng) }], outputFormat: { type: "json_schema", schema: SCHEMA }, thinkingDisplay: "summarized" });
  } catch (e) { return { error: "the model call failed: " + str(e && e.message || e, 160), left }; }
  for (const k of Object.keys(usage)) usage[k] += (res && res.usage && res.usage[k]) || 0;
  const log = { at: now, round: req.round, model: opts.model || MODEL, stop: str(res && res.stop_reason, 30), usage };
  let out;
  if (res && res.stop_reason === "refusal") out = { declined: true, error: "the model declined" };
  else {
    const text = ((res && res.content) || []).filter(b => b && b.type === "text").map(b => b.text).join("");
    try { out = normalize(JSON.parse(text), req.adjust); }
    catch (_) { out = { error: res && res.stop_reason === "max_tokens" ? "the answer was cut off" : "the answer could not be read" }; }
  }
  try { await ref.set({ log: FV.arrayUnion(Object.assign({ ok: !out.error }, log)) }, { merge: true }); } catch (e) { console.warn("[monoAssist] usage not logged", e && e.message || e); }
  console.log(`[monoAssist] ${req.orderKey} ${req.letters.join("")} round ${req.round}: ${out.error || "ok"} · in=${usage.input_tokens} out=${usage.output_tokens} cached=${usage.cache_read_input_tokens} · ${left} call(s) left today`);
  return Object.assign(out, { model: log.model, usage, left, round: req.round });
}

module.exports = { run, cleanRequest, clampAdjust, clampOne, normalize, content, INSTRUCTIONS, SCHEMA, BOUNDS, DAILY_MAX, MAX_ROUND, MODEL, EFFORT, COLL, dayOf };
