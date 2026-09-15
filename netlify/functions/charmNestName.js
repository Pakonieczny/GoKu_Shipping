/*  netlify/functions/charmNestName.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  One Claude call per sheet drop: name the charms so Illustrator layers
 *  read `compass-rose` instead of `charm-04`. Optionally suggest a metal
 *  for files the filename router could not place.
 *
 *    model        claude-opus-5
 *    effort       low (recognition, not reasoning)
 *    thinking     default (adaptive) — disabling it can leak tags / misroute
 *    format       output_config.format json_schema → no prose parsing
 *    caching      the instruction block is byte-identical every call and
 *                 carries cache_control so 21 thumbnails cost one call
 *    transport    _etsyMailAnthropic.callClaudeRaw (endpoint, version header,
 *                 5-attempt overload backoff) — extended here, not duplicated
 *
 *  NEVER ON THE CRITICAL PATH. The page nests in parallel; if this fails,
 *  layers fall back to <file>-<index> and a toast says naming was skipped.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const { json, gate, parseBody, str, num, CORS } = require("./_charmNestAuth");
const { callClaudeRaw } = require("./_etsyMailAnthropic");

const MODEL = process.env.CHARM_NEST_NAME_MODEL || "claude-opus-5";
const MAX_CHARMS = 40;

const INSTRUCTIONS = `You name jewelry charm artwork for a laser-cutting operator's Illustrator layers.
Each image is one charm's vector artwork rendered on white: its cut outline plus engraving details. Charms are small (usually 0.3 to 1.5 inches) and are jewelry pendants: animals, symbols, letters, badges, tools, flowers, celestial shapes, vehicles, hearts, initials, dates, names.

For every charm, in the order given, return:
- slug: 2-4 lowercase words joined by hyphens that a person would use to find this exact charm again (e.g. "compass-rose", "police-vest", "axolotl", "celtic-knot", "initial-m", "date-9-26-25"). If the charm carries engraved text, prefer that text in the slug. Never use generic slugs like "charm" or "pendant" alone.
- label: one short plain-English phrase describing it (max 8 words).
- confidence: 0 to 1, how sure you are of the identification.
- metal: only when asked (wantMetal), your best guess of "gold", "silver" or "rose" from any hint in the artwork or file name; otherwise null.

Return one entry per input index, no more and no fewer. Do not invent charms.`;

const SCHEMA = {
  type: "object", additionalProperties: false,
  properties: {
    charms: {
      type: "array",
      items: {
        type: "object", additionalProperties: false,
        properties: {
          index: { type: "integer" },
          slug: { type: "string" },
          label: { type: "string" },
          confidence: { type: "number" },
          metal: { type: ["string", "null"], enum: ["gold", "silver", "rose", null] }
        },
        required: ["index", "slug", "label", "confidence", "metal"]
      }
    }
  },
  required: ["charms"]
};

function parseDataUrl(u) {
  const m = /^data:(image\/(?:png|jpeg|webp));base64,([A-Za-z0-9+/=]+)$/.exec(String(u || ""));
  return m ? { media_type: m[1], data: m[2] } : null;
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "POST") return json(405, { error: "method not allowed" });
  const body = parseBody(event);
  const denied = gate(event, body); if (denied) return denied;
  if (!process.env.ANTHROPIC_API_KEY) return json(200, { charms: [], skipped: "ANTHROPIC_API_KEY is not set" });

  const charms = (body.charms || []).slice(0, MAX_CHARMS).map(c => ({ index: num(c.index), img: parseDataUrl(c.thumb), widthPt: num(c.widthPt), heightPt: num(c.heightPt), qty: num(c.qty) || 1 })).filter(c => c.img);
  if (!charms.length) return json(400, { error: "no charm thumbnails" });
  const wantMetal = !!body.wantMetal;
  const sourceName = str(body.sourceName, 120);

  const content = [];
  content.push({ type: "text", text: `Source file: ${sourceName || "(unknown)"}. wantMetal: ${wantMetal ? "yes" : "no"}. ${charms.length} charms follow, each preceded by its index and size.` });
  for (const c of charms) {
    content.push({ type: "text", text: `index ${c.index} · ${(c.widthPt / 72).toFixed(2)} × ${(c.heightPt / 72).toFixed(2)} in${c.qty > 1 ? ` · appears ×${c.qty}` : ""}` });
    content.push({ type: "image", source: { type: "base64", media_type: c.img.media_type, data: c.img.data } });
  }

  try {
    const res = await callClaudeRaw({
      model: MODEL,
      maxTokens: 4000,
      effort: "low",
      system: [{ type: "text", text: INSTRUCTIONS, cache_control: { type: "ephemeral" } }],
      messages: [{ role: "user", content }],
      outputFormat: { type: "json_schema", schema: SCHEMA }
    });
    if (res.stop_reason === "refusal") return json(200, { charms: [], skipped: "model declined" });
    const text = (res.content || []).filter(b => b.type === "text").map(b => b.text).join("");
    let parsed; try { parsed = JSON.parse(text); } catch (_) { return json(200, { charms: [], skipped: "unparseable model output" }); }
    const out = (parsed.charms || []).map(x => ({
      index: num(x.index),
      slug: str(x.slug, 60).toLowerCase().replace(/[^a-z0-9\-]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 48) || null,
      label: str(x.label, 120),
      confidence: Math.max(0, Math.min(1, num(x.confidence))),
      metal: wantMetal && /^(gold|silver|rose)$/.test(x.metal || "") ? x.metal : null
    })).filter(x => x.slug);
    return json(200, { charms: out, model: MODEL, usage: res.usage || null });
  } catch (e) {
    console.error("[charmNestName]", e.status || "", e.message);
    return json(200, { charms: [], skipped: `naming unavailable (${e.status || e.message})` });
  }
};
