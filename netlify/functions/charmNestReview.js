/*  netlify/functions/charmNestReview.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  The agent half of the Charm Nesting Station. The geometry finds and
 *  groups the charms; Claude Opus 5 then LOOKS at the result the way an
 *  operator would and says what is wrong, and the page acts on it:
 *
 *    mode "grouping"  — before nesting. Gets a rendering of the whole source
 *                       page with every detected charm boxed and numbered,
 *                       plus each charm's own thumbnail. Returns a verdict
 *                       per charm: complete, fragment (and which charm it
 *                       belongs to), multiple (two charms in one box), or
 *                       not_a_charm. The page merges fragments, excludes
 *                       junk, re-traces silhouettes and reports.
 *    mode "layout"    — after nesting. Gets the finished sheet preview and
 *                       the placement list. Returns issues (dislodged part,
 *                       overlap, outside sheet, unreadable text, …). The
 *                       page shows them and re-runs once if it can fix them.
 *
 *  model claude-opus-5 · effort high · adaptive thinking · structured output
 *  Transport: _etsyMailAnthropic.callClaudeRaw (shared endpoint + backoff).
 *  Never on the critical path: any failure returns {skipped} and the sheet
 *  proceeds on geometry alone.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const { json, gate, parseBody, str, num, CORS } = require("./_charmNestAuth");
const { callClaudeRaw } = require("./_etsyMailAnthropic");

const MODEL = process.env.CHARM_NEST_NAME_MODEL || "claude-opus-5";
const EFFORT = /^(low|medium|high|xhigh|max)$/.test(process.env.CHARM_NEST_REVIEW_EFFORT || "") ? process.env.CHARM_NEST_REVIEW_EFFORT : "high";
const MAX_CHARMS = 60;

const GROUPING_INSTRUCTIONS = `You are the quality reviewer for a laser-cutting operator's charm-nesting tool.

The tool reads an Illustrator sheet of loose jewelry charm artwork and splits it into individual charms by geometry. Geometry makes mistakes an operator spots instantly: a jump ring, a badge, an engraved rectangle or a hole outline gets split off as its own "charm"; two charms drawn close together get merged into one; a stray guide or frame is treated as a charm.

You receive:
1. An overview image of the whole source sheet. Every detected "charm" is boxed and labelled with its number.
2. One cropped image per detected charm, in number order, with its size in inches.

For every detected charm give a verdict:
- "complete": this is one whole charm exactly as a customer would receive it (its cut outline plus its engraving, holes and jump ring).
- "fragment": this is only a PIECE of a charm (a ring, hole, badge, panel, text, inner detail) that belongs to another detected charm. Set mergeInto to that charm's number — the charm it visibly sits on or touches in the overview. If its parent is not among the detected charms, use null.
- "multiple": this box contains two or more separate charms.
- "not_a_charm": a frame, guide, page border, stray mark or empty box.

Be decisive and specific. Jewelry charms are typically 0.3–1.6 inches; a "charm" under 0.25 inch that is a plain ring or rectangle is almost always a fragment. Use the overview to see what touches what. In summary, say in one sentence how many real charms the sheet holds and what you corrected.`;

const GROUPING_SCHEMA = {
  type: "object", additionalProperties: false,
  properties: {
    verdicts: {
      type: "array",
      items: {
        type: "object", additionalProperties: false,
        properties: {
          index: { type: "integer" },
          verdict: { type: "string", enum: ["complete", "fragment", "multiple", "not_a_charm"] },
          mergeInto: { type: ["integer", "null"] },
          note: { type: "string" }
        },
        required: ["index", "verdict", "mergeInto", "note"]
      }
    },
    realCharmCount: { type: "integer" },
    summary: { type: "string" }
  },
  required: ["verdicts", "realCharmCount", "summary"]
};

const LAYOUT_INSTRUCTIONS = `You are the final inspector of a nested charm sheet before it goes to the laser.

You receive the rendered sheet (the red rectangle is the stock edge) and the list of placed charms with their positions. The nesting algorithm guarantees no two silhouettes overlap beyond the allowed stroke tolerance, but it cannot judge what a charm IS. Look for what an operator would reject:
- a piece that is clearly a fragment of another charm placed on its own (a lone ring, a lone badge or panel, text without its charm)
- a charm that looks incomplete (missing its ring or a panel that appears elsewhere)
- anything visibly overlapping, cut off by the sheet edge, or mirrored/unreadable engraved text
- anything that is not a charm at all

Report only real problems. If the sheet is good, say so with ok=true and no issues. For each issue name the charms involved by their listed names and say what to do (merge X into Y, exclude X, re-nest).`;

const LAYOUT_SCHEMA = {
  type: "object", additionalProperties: false,
  properties: {
    ok: { type: "boolean" },
    issues: {
      type: "array",
      items: {
        type: "object", additionalProperties: false,
        properties: {
          kind: { type: "string", enum: ["fragment", "incomplete", "overlap", "outside", "text", "not_a_charm", "other"] },
          charms: { type: "array", items: { type: "string" } },
          mergeInto: { type: ["string", "null"] },
          action: { type: "string", enum: ["merge", "exclude", "renest", "review"] },
          note: { type: "string" }
        },
        required: ["kind", "charms", "mergeInto", "action", "note"]
      }
    },
    summary: { type: "string" }
  },
  required: ["ok", "issues", "summary"]
};

function parseDataUrl(u) {
  const m = /^data:(image\/(?:png|jpeg|webp));base64,([A-Za-z0-9+/=]+)$/.exec(String(u || ""));
  return m ? { type: "image", source: { type: "base64", media_type: m[1], data: m[2] } } : null;
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "POST") return json(405, { error: "method not allowed" });
  const body = parseBody(event);
  const denied = gate(event, body); if (denied) return denied;
  if (!process.env.ANTHROPIC_API_KEY) return json(200, { skipped: "ANTHROPIC_API_KEY is not set" });
  const mode = body.mode === "layout" ? "layout" : "grouping";

  const content = [];
  let system, schema;
  if (mode === "grouping") {
    const overview = parseDataUrl(body.overview);
    const charms = (body.charms || []).slice(0, MAX_CHARMS).map(c => ({ index: num(c.index), img: parseDataUrl(c.thumb), widthPt: num(c.widthPt), heightPt: num(c.heightPt), members: num(c.members) })).filter(c => c.img);
    if (!overview || !charms.length) return json(400, { error: "overview and charm thumbnails are required" });
    system = GROUPING_INSTRUCTIONS; schema = GROUPING_SCHEMA;
    content.push({ type: "text", text: `Source file: ${str(body.sourceName, 120) || "(unknown)"}. The geometry detected ${charms.length} charms. Overview of the whole sheet with numbered boxes:` });
    content.push(overview);
    for (const c of charms) {
      content.push({ type: "text", text: `Charm ${c.index} · ${(c.widthPt / 72).toFixed(2)} × ${(c.heightPt / 72).toFixed(2)} in · ${c.members} drawing elements` });
      content.push(c.img);
    }
  } else {
    const preview = parseDataUrl(body.preview);
    if (!preview) return json(400, { error: "preview image is required" });
    system = LAYOUT_INSTRUCTIONS; schema = LAYOUT_SCHEMA;
    const list = (body.placements || []).slice(0, 200).map(p => `${str(p.n, 4)}. ${str(p.name, 60)} — ${(num(p.wPt) / 72).toFixed(2)} × ${(num(p.hPt) / 72).toFixed(2)} in at ${(num(p.cxPt) / 72).toFixed(2)}, ${(num(p.cyPt) / 72).toFixed(2)} in from the top-left, ${num(p.angle)}°`).join("\n");
    content.push({ type: "text", text: `Sheet ${str(body.sheetName, 80)} · ${(body.placements || []).length} charms placed${body.rejects ? ` · ${str(body.rejects, 300)} did not fit` : ""}.\n\nPlaced charms:\n${list}\n\nRendered sheet:` });
    content.push(preview);
  }

  try {
    const res = await callClaudeRaw({
      model: MODEL, maxTokens: 8000, effort: EFFORT,
      system: [{ type: "text", text: system, cache_control: { type: "ephemeral" } }],
      messages: [{ role: "user", content }],
      outputFormat: { type: "json_schema", schema }
    });
    if (res.stop_reason === "refusal") return json(200, { skipped: "model declined" });
    const text = (res.content || []).filter(b => b.type === "text").map(b => b.text).join("");
    let parsed; try { parsed = JSON.parse(text); } catch (_) { return json(200, { skipped: "unparseable model output" }); }
    return json(200, Object.assign({ mode, model: MODEL, effort: EFFORT, usage: res.usage || null }, parsed));
  } catch (e) {
    console.error("[charmNestReview]", mode, e.status || "", e.message);
    return json(200, { skipped: `review unavailable (${e.status || e.message})` });
  }
};
