/*  netlify/functions/_charmNestAgent.js
 *  The model side of the Charm Nesting Station (Claude Opus 5, high effort,
 *  adaptive thinking, structured output): grouping review, sheet inspection
 *  and charm naming. Shared by the synchronous endpoints (charmNestReview,
 *  charmNestName — fine for tiny inputs) and by charmNestAgent-background,
 *  which the page actually uses: a high-effort vision call over a whole sheet
 *  takes far longer than Netlify's ~10 s synchronous limit (it 504'd on the
 *  first live sheet), so the page starts a job and polls Charm_Nest_Agent.  */
"use strict";
const { str, num } = require("./_charmNestAuth");
const anthropic = require("./_etsyMailAnthropic");   // referenced through the module so tests can stub callClaudeRaw

const MODEL = process.env.CHARM_NEST_NAME_MODEL || "claude-opus-5";
const EFFORT = /^(low|medium|high|xhigh|max)$/.test(process.env.CHARM_NEST_REVIEW_EFFORT || "") ? process.env.CHARM_NEST_REVIEW_EFFORT : "high";
const NAME_EFFORT = /^(low|medium|high|xhigh|max)$/.test(process.env.CHARM_NEST_NAME_EFFORT || "") ? process.env.CHARM_NEST_NAME_EFFORT : "high";
const MAX_CHARMS = 60;

const GROUPING_INSTRUCTIONS = `You are the quality reviewer for a laser-cutting operator's charm-nesting tool.

The tool reads an Illustrator sheet of loose jewelry charm artwork and splits it into individual charms by geometry. Geometry makes mistakes an operator spots instantly: a jump ring, a badge, an engraved rectangle or a hole outline gets split off as its own "charm"; two charms drawn close together get merged into one; a stray guide or frame is treated as a charm.

You receive:
1. An overview image of the whole source sheet on a light grey background. Every detected "charm" is boxed and labelled with its number, and each detected CUT OUTLINE is traced in red (the artwork's own outline may be black, grey or WHITE, so trust the red trace to see where the cut line is).
2. One cropped image per detected charm, in number order, with its size in inches, with the same red cut-outline trace.

A charm whose red outline encloses engraved text (a date, a name) is complete: the text is engraving on it, not a separate piece. Only text with NO red cut outline around it is a fragment.

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

You receive the rendered sheet on a light grey background — the outer red rectangle is the stock edge, every charm's CUT OUTLINE is traced in red (some artwork strokes its outline in white, so trust the red trace), and each placed charm carries its number — plus the list of placed charms with their positions. Two independent pixel-level verifiers have ALREADY proven that no silhouette overlaps another beyond the allowed stroke tolerance and that nothing crosses the stock edge; do not re-litigate bounds or overlap from the picture, your pixel estimates are less precise than the verifier. Your job is identity and completeness — what the geometry cannot judge. Look for what an operator would reject:
- a piece that is clearly a fragment of another charm placed on its own (a lone ring, a lone badge or panel, text with NO red cut outline around it — text inside a red outline is engraving on a complete charm)
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

const NAME_INSTRUCTIONS = `You name jewelry charm artwork for a laser-cutting operator's Illustrator layers.
Each image is one charm's vector artwork rendered on white: its cut outline plus engraving details. Charms are small (usually 0.3 to 1.5 inches) and are jewelry pendants: animals, symbols, letters, badges, tools, flowers, celestial shapes, vehicles, hearts, initials, dates, names.

For every charm, in the order given, return:
- slug: 2-4 lowercase words joined by hyphens that a person would use to find this exact charm again (e.g. "compass-rose", "police-vest", "axolotl", "celtic-knot", "initial-m", "date-9-26-25"). If the charm carries engraved text, prefer that text in the slug. Never use generic slugs like "charm" or "pendant" alone.
- label: one short plain-English phrase describing it (max 8 words).
- confidence: 0 to 1, how sure you are of the identification.
- metal: only when asked (wantMetal), your best guess of "gold", "silver" or "rose" from any hint in the artwork or file name; otherwise "unknown".

Return one entry per input index, no more and no fewer. Do not invent charms.`;

const NAME_SCHEMA = {
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
          metal: { type: "string", enum: ["gold", "silver", "rose", "unknown"] }
        },
        required: ["index", "slug", "label", "confidence", "metal"]
      }
    }
  },
  required: ["charms"]
};


const PLACE_INSTRUCTIONS = `You are nesting jewelry charms onto a metal sheet for laser cutting. YOU decide where every charm goes; a measuring tool executes your moves exactly and tells you when something did not fit.

Coordinates: inches, x from the LEFT edge, y from the TOP edge (y grows downward), giving the CENTRE of the charm. Angle in degrees, clockwise as seen on the sheet; 0 = as drawn. Each charm is listed with its width × height at 0° and its solid area; rotating swaps/changes its footprint. Charms may touch each other — their cut lines may even share — but their bodies may not overlap, and nothing may cross the stock edge inset.

You receive the rendered sheet (light grey; the outer red rectangle is the stock edge; already-placed charms are drawn with red cut outlines and a number), the list of what is placed, the remaining charms with crops, the largest open pockets the tool measured, and the tool's feedback on your previous moves (placed as asked / slid up to a small distance to clear a neighbour / failed and why).

Think like a nesting expert: biggest pieces first into corners and along edges, long pieces along the sides, rotate pieces so concave shapes interlock (a crescent hugs a circle, a tail tucks into a notch), keep small pieces for the gaps last. Use the pockets list for real free space. Give as many confident moves as you can this round (aim for everything remaining when the sheet is empty), ordered so that each move leaves room for the next; positions must keep the whole charm inside the stock. Keep each "why" under 12 words; keep the whole answer compact — the moves are what matter. If a piece cannot fit anywhere, say so in the summary and set it aside rather than forcing it. Set done=true only when every remaining charm has a move in this plan or you are certain nothing more fits.`;

const PLACE_SCHEMA = {
  type: "object", additionalProperties: false,
  properties: {
    moves: { type: "array", items: { type: "object", additionalProperties: false, properties: { id: { type: "string" }, angle: { type: "number" }, xIn: { type: "number" }, yIn: { type: "number" }, why: { type: "string" } }, required: ["id", "angle", "xIn", "yIn", "why"] } },
    setAside: { type: "array", items: { type: "string" } },
    done: { type: "boolean" },
    summary: { type: "string" }
  },
  required: ["moves", "setAside", "done", "summary"]
};


// Packing advice supplies search heuristics, never unchecked coordinates or permission to alter artwork.
const PACKING_INSTRUCTIONS = `You advise a geometric nesting solver for laser-cut jewelry. You see a numbered contact sheet of exact charm artwork, the current layout, and dimensions in points. Recommend up to 24 difficult shapes to place early and up to three promising rotation angles for each. Favor long or concave shapes and complementary silhouettes; reserve small simple pieces for gaps. Use the supplied integer indices only. Dates and whole-order sheet membership are enforced separately: never change membership, size, clearance, outline, or the fill ceiling. Your advice is tried alongside unguided searches and accepted only through geometric collision checks. Return a short, specific explanation of your packing strategy. Do not claim an achieved density or that a sheet has been verified.`;
const PACKING_SCHEMA = { type: "object", additionalProperties: false, properties: {
  suggestions: { type: "array", maxItems: 24, items: { type: "object", additionalProperties: false, properties: { index: { type: "integer" }, angles: { type: "array", maxItems: 3, items: { type: "number" } } }, required: ["index", "angles"] } },
  summary: { type: "string" }
}, required: ["suggestions", "summary"] };

/* ═══ bridge modes (design §7.1, §7.4, §6.3 item 8) ═══════════════════════ */
const ENGRAVE_INTENT_INSTRUCTIONS = `You read one Etsy order line for a jewelry engraver and decide, from the customer's own words only, whether the charm is to be engraved and with exactly what text.

You receive: the personalisation field (what the customer typed in the listing's engraving box), the buyer's message on the order, a staff note written by the shop (which OVERRIDES anything the customer wrote when they conflict), and the last internal staff messages about the order. You also receive whether the charm's design can take engraving at all.

Rules, without exception:
- engrave=true when the personalisation is non-empty, or when the message/note plainly asks for engraving. Otherwise engrave=false.
- text is the customer's words VERBATIM: same spelling, same capitalisation, same punctuation, same symbols. Never invent, translate, correct, expand, abbreviate or "improve" anything. A staff note that gives the text wins over the Etsy field.
- Separate production instructions from the inscription in any language. VERBATIM applies to the requested inscription, not the surrounding request. Example: "Bitte folgendes wenn es geht hochkant (von unten nach oben des Otters zu lesen) auf die Rückseite gravieren: Always." requests the inscription "Always."; do not engrave the German instructions. Put an explicit orientation/layout request in questions for the operator to confirm, rather than adding it to text. If the boundary is ambiguous, ask instead of guessing.
- Split lines ONLY at the customer's own line breaks, or between a name and a date. Nothing else. Use "\\n" between lines.
- Engraving is always on the BACK in the shop's standard font. If the customer asks for the front, a particular font, their own handwriting, an image or symbol drawn, say so in requests (side "front", font name, handwriting true, image true) — the shop decides, you never do.
- Anything ambiguous — two candidate texts, a request you cannot resolve, a date whose format is unclear, "same as last time", a note that contradicts the field — goes into questions, one short question each, and lowers confidence.
- source names where the text came from: personalization | buyerMessage | staffNote | messages | none. sourceQuote is the exact fragment you took it from.
- confidence is your certainty that text is exactly what the customer wants engraved, 0 to 1.`;
const ENGRAVE_INTENT_SCHEMA = { type: "object", additionalProperties: false, properties: {
  engrave: { type: "boolean" }, text: { type: "string" }, source: { type: "string", enum: ["personalization", "buyerMessage", "staffNote", "messages", "none"] }, sourceQuote: { type: "string" },
  requests: { type: "object", additionalProperties: false, properties: { side: { type: "string", enum: ["back", "front", "both", "unspecified"] }, font: { type: ["string", "null"] }, handwriting: { type: "boolean" }, image: { type: "boolean" } }, required: ["side", "font", "handwriting", "image"] },
  questions: { type: "array", items: { type: "string" } }, confidence: { type: "number" } }, required: ["engrave", "text", "source", "sourceQuote", "requests", "questions", "confidence"] };

const ENGRAVE_REVIEW_INSTRUCTIONS = `You look at the rendered BACK of one jewelry charm with engraving text already placed by a measuring tool. The image shows a millimetre grid, the charm's cut outline and cut-outs (holes), and the text as it will be engraved. The tool has already PROVEN that no ink touches a cut edge or a hole and that the text is the largest size that fits; do not re-judge geometry. Judge readability and taste only: is the text legible at this size, does the placement look right on the piece, is anything awkward (a line breaking oddly, text crowding a hole, an orientation that reads wrong for a pendant). Answer legible true/false and one or two plain sentences of notes for the person who approves it.`;
const ENGRAVE_REVIEW_SCHEMA = { type: "object", additionalProperties: false, properties: { legible: { type: "boolean" }, notes: { type: "string" }, concerns: { type: "array", items: { type: "string", enum: ["small", "crowded", "odd-break", "orientation", "placement", "other"] } } }, required: ["legible", "notes", "concerns"] };

const LABEL_READ_INSTRUCTIONS = `Each image is the strip directly BELOW one charm in a jewelry master Illustrator file, where the artist wrote the charm's SKU as text (sometimes converted to outlines). Read the SKU exactly. A SKU looks like two to four capital letters, a hyphen, two to six letters or digits, optionally another hyphen and one to four more (for example BR-CMP-01 or BRT-HRT-12-A); it may be followed by a size after a middle dot or a space (BR-CMP-01 · S). Return the SKU in capitals with no spaces, the size if one is written, and your confidence 0 to 1. If the strip holds no SKU or you cannot read it with certainty, return sku "" and a low confidence — never guess a character.`;
const LABEL_READ_SCHEMA = { type: "object", additionalProperties: false, properties: { reads: { type: "array", items: { type: "object", additionalProperties: false, properties: { index: { type: "integer" }, sku: { type: "string" }, size: { type: ["string", "null"] }, confidence: { type: "number" } }, required: ["index", "sku", "size", "confidence"] } } }, required: ["reads"] };

function parseDataUrl(u) {
  const m = /^data:(image\/(?:png|jpeg|webp));base64,([A-Za-z0-9+/=]+)$/.exec(String(u || ""));
  return m ? { type: "image", source: { type: "base64", media_type: m[1], data: m[2] } } : null;
}

/** Build the request for a mode; returns {error} for bad input. */
function buildRequest(mode, body) {
  const content = [];
  if (mode === "grouping") {
    const overview = parseDataUrl(body.overview);
    const charms = (body.charms || []).slice(0, MAX_CHARMS).map(c => ({ index: num(c.index), img: parseDataUrl(c.thumb), widthPt: num(c.widthPt), heightPt: num(c.heightPt), members: num(c.members) })).filter(c => c.img);
    if (!overview || !charms.length) return { error: "overview and charm thumbnails are required" };
    content.push({ type: "text", text: `Source file: ${str(body.sourceName, 120) || "(unknown)"}. The geometry detected ${charms.length} charms. Overview of the whole sheet with numbered boxes:` });
    content.push(overview);
    for (const c of charms) { content.push({ type: "text", text: `Charm ${c.index} · ${(c.widthPt / 72).toFixed(2)} × ${(c.heightPt / 72).toFixed(2)} in · ${c.members} drawing elements` }); content.push(c.img); }
    return { system: GROUPING_INSTRUCTIONS, schema: GROUPING_SCHEMA, content, effort: EFFORT };
  }
  if (mode === "layout") {
    const preview = parseDataUrl(body.preview);
    if (!preview) return { error: "preview image is required" };
    const list = (body.placements || []).slice(0, 200).map(p => `${str(p.n, 4)}. ${str(p.name, 60)} — ${(num(p.wPt) / 72).toFixed(2)} × ${(num(p.hPt) / 72).toFixed(2)} in at ${(num(p.cxPt) / 72).toFixed(2)}, ${(num(p.cyPt) / 72).toFixed(2)} in from the top-left, ${num(p.angle)}°`).join("\n");
    content.push({ type: "text", text: `Sheet ${str(body.sheetName, 80)} · ${(body.placements || []).length} charms placed${body.rejects ? ` · ${str(body.rejects, 300)} did not fit` : ""}.\n\nPlaced charms:\n${list}\n\nRendered sheet:` });
    content.push(preview);
    return { system: LAYOUT_INSTRUCTIONS, schema: LAYOUT_SCHEMA, content, effort: EFFORT };
  }
  if (mode === "packing") {
    const image = parseDataUrl(body.contactSheet), preview = parseDataUrl(body.preview);
    const pieces = (body.pieces || []).slice(0, 120).map((p,index) => ({ index, widthPt: num(p.widthPt), heightPt: num(p.heightPt), areaPt2: num(p.areaPt2) }));
    if (!image || !pieces.length) return { error: "numbered contact sheet and pieces required" };
    content.push({ type: "text", text: `Stock ${num(body.wPt)} × ${num(body.hPt)} pt. Fill ceiling ${num(body.maxFill) * 100}%. Current fill ${num(body.density) * 100}%. Geometry remains authoritative. Numbered shapes:\n${JSON.stringify(pieces)}` }, image);
    if (preview) content.push({ type: "text", text: "Current best layout (its numbers label placements, not the contact-sheet indices):" }, preview);
    return { system: PACKING_INSTRUCTIONS, schema: PACKING_SCHEMA, content, effort: EFFORT, pieceCount: pieces.length };
  }
  if (mode === "place") {
    const sheet = parseDataUrl(body.sheet);
    if (!sheet) return { error: "sheet image is required" };
    const remaining = (body.remaining || []).slice(0, MAX_CHARMS).map(c => ({ id: str(c.id, 80), img: parseDataUrl(c.thumb), wIn: num(c.wIn), hIn: num(c.hIn), areaIn2: num(c.areaIn2), name: str(c.name, 60) })).filter(c => c.img);
    if (!remaining.length) return { error: "no remaining charms" };
    const placed = (body.placed || []).slice(0, 200).map(p => `#${str(p.n, 4)} ${str(p.name, 60)} (id ${str(p.id, 80)}) — ${num(p.wIn).toFixed(2)}×${num(p.hIn).toFixed(2)} in footprint, centre ${num(p.xIn).toFixed(2)}, ${num(p.yIn).toFixed(2)} in, ${num(p.angle)}°`).join("\n") || "(nothing placed yet)";
    const pockets = (body.pockets || []).slice(0, 8).map(k => `${num(k.wIn).toFixed(2)} × ${num(k.hIn).toFixed(2)} in open rectangle with its top-left at ${num(k.xIn).toFixed(2)}, ${num(k.yIn).toFixed(2)} in`).join("\n") || "(none measured)";
    const feedback = str(body.feedback, 4000) || "(first round)";
    content.push({ type: "text", text: `Sheet: ${num(body.wIn).toFixed(3)} × ${num(body.hIn).toFixed(3)} in, stock edge inset ${num(body.insetIn).toFixed(3)} in, clearance ${num(body.clearancePt).toFixed(2)} pt (negative = cut lines may share). Round ${num(body.round)} of ${num(body.maxRounds)}.\n\nPlaced so far:\n${placed}\n\nLargest open pockets now:\n${pockets}\n\nFeedback on your last moves:\n${feedback}\n\nRendered sheet:` });
    content.push(sheet);
    content.push({ type: "text", text: `Remaining charms (${remaining.length}), each with its crop:` });
    for (const c of remaining) { content.push({ type: "text", text: `id ${c.id} · ${c.name} · ${c.wIn.toFixed(2)} × ${c.hIn.toFixed(2)} in at 0° · solid ${c.areaIn2.toFixed(2)} in²` }); content.push(c.img); }
    return { system: PLACE_INSTRUCTIONS, schema: PLACE_SCHEMA, content, effort: EFFORT };
  }
  if (mode === "engraveIntent") {
    const lines = [];
    lines.push(`Order ${str(body.order, 40)} · SKU ${str(body.sku, 40) || "(none)"} · ${str(body.title, 160)} · form: ${str(body.form, 40) || "unknown"} · quantity ${num(body.quantity) || 1}`);
    lines.push(`Design can take engraving: ${body.engravable === false ? "NO — the shop will decide what to do" : "yes"}`);
    lines.push(`Personalisation field: ${JSON.stringify((body.personalization || []).map(x => str(x, 400)))}`);
    lines.push(`Buyer message on the order: ${JSON.stringify(str(body.buyerMessage, 2000))}`);
    lines.push(`Staff note (overrides the customer's text): ${JSON.stringify(str(body.staffNote, 2000))}`);
    lines.push(`Internal staff messages, oldest first: ${JSON.stringify((body.messages || []).slice(-5).map(m => ({ who: str(m.senderName, 60), text: str(m.text, 400) })))}`);
    content.push({ type: "text", text: lines.join("\n") });
    return { system: ENGRAVE_INTENT_INSTRUCTIONS, schema: ENGRAVE_INTENT_SCHEMA, content, effort: EFFORT };
  }
  if (mode === "engraveReview") {
    const img = parseDataUrl(body.image); if (!img) return { error: "back image is required" };
    content.push({ type: "text", text: `Order ${str(body.order, 40)} · SKU ${str(body.sku, 40)} · text ${JSON.stringify(str(body.text, 400))} · cap height ${num(body.capMm).toFixed(2)} mm · font ${str(body.font, 40)} ${str(body.weight, 20)} · angle ${num(body.angle)}°${body.small ? " · flagged SMALL by the tool" : ""}. Rendered back with a 1 mm grid:` });
    content.push(img);
    return { system: ENGRAVE_REVIEW_INSTRUCTIONS, schema: ENGRAVE_REVIEW_SCHEMA, content, effort: EFFORT };
  }
  if (mode === "labelRead") {
    const strips = (body.strips || []).slice(0, MAX_CHARMS).map(c => ({ index: num(c.index), img: parseDataUrl(c.image) })).filter(c => c.img);
    if (!strips.length) return { error: "no strips" };
    content.push({ type: "text", text: `Master file: ${str(body.sourceName, 120) || "(unknown)"}. ${strips.length} strips follow, each preceded by its charm index.` });
    for (const c of strips) { content.push({ type: "text", text: `charm index ${c.index}` }); content.push(c.img); }
    return { system: LABEL_READ_INSTRUCTIONS, schema: LABEL_READ_SCHEMA, content, effort: EFFORT };
  }
  if (mode === "name") {
    const charms = (body.charms || []).slice(0, MAX_CHARMS).map(c => ({ index: num(c.index), img: parseDataUrl(c.thumb), widthPt: num(c.widthPt), heightPt: num(c.heightPt), qty: num(c.qty) || 1 })).filter(c => c.img);
    if (!charms.length) return { error: "no charm thumbnails" };
    const wantMetal = !!body.wantMetal;
    content.push({ type: "text", text: `Source file: ${str(body.sourceName, 120) || "(unknown)"}. wantMetal: ${wantMetal ? "yes" : "no"}. ${charms.length} charms follow, each preceded by its index and size.` });
    for (const c of charms) { content.push({ type: "text", text: `index ${c.index} · ${(c.widthPt / 72).toFixed(2)} × ${(c.heightPt / 72).toFixed(2)} in${c.qty > 1 ? ` · appears ×${c.qty}` : ""}` }); content.push(c.img); }
    return { system: NAME_INSTRUCTIONS, schema: NAME_SCHEMA, content, effort: NAME_EFFORT, wantMetal };
  }
  return { error: "unknown mode" };
}

/** Run one mode. Never throws for model trouble: returns {skipped} instead. */
async function run(mode, body) {
  if (!process.env.ANTHROPIC_API_KEY) return { skipped: "ANTHROPIC_API_KEY is not set" };
  const req = buildRequest(mode, body);
  if (req.error) return { error: req.error };
  try {
    // Thinking tokens count against max_tokens; a placement round reasons at length, so give it room.
    const res = await anthropic.callClaudeRaw({ model: MODEL, maxTokens: mode === "packing" ? 8000 : mode === "place" ? 32000 : mode === "engraveIntent" ? 8000 : 16000, effort: req.effort, system: [{ type: "text", text: req.system, cache_control: { type: "ephemeral" } }], messages: [{ role: "user", content: req.content }], outputFormat: { type: "json_schema", schema: req.schema }, thinkingDisplay: "summarized" });
    const reasoning = (res.content || []).filter(b => b.type === "thinking" && b.thinking).map(b => b.thinking).join("\n").slice(0, 6000);
    if (res.stop_reason === "refusal") return { skipped: "model declined" };
    const text = (res.content || []).filter(b => b.type === "text").map(b => b.text).join("");
    let parsed; try { parsed = JSON.parse(text); } catch (_) {
      const why = res.stop_reason === "max_tokens" ? "output cut off at the token limit" : `stop_reason ${res.stop_reason}`;
      console.error("[charmNestAgent] unparseable", mode, why, String(text).slice(0, 300));
      return { skipped: `unparseable model output (${why}; ${(res.usage && res.usage.output_tokens) || "?"} output tokens; starts: ${JSON.stringify(String(text).slice(0, 120))})`, reasoning: reasoning || null };
    }
  if (mode === "labelRead") {
      const pat = /^[A-Z]{2,4}-[A-Z0-9]{2,6}(-[A-Z0-9]{1,4})?$/;
      parsed = { reads: (parsed.reads || []).map(x => { const sku = str(x.sku, 40).toUpperCase().replace(/\s+/g, ""); return { index: num(x.index), sku: pat.test(sku) ? sku : "", size: x.size ? str(x.size, 4).toUpperCase() : null, confidence: pat.test(sku) ? Math.max(0, Math.min(1, num(x.confidence))) : 0 }; }) };
    }
    if (mode === "engraveIntent") {
      parsed.text = str(parsed.text, 400); parsed.confidence = Math.max(0, Math.min(1, num(parsed.confidence))); parsed.questions = (parsed.questions || []).map(q => str(q, 300)).filter(Boolean);
    }
    if (mode === "packing") {
      const seen = new Set();
      parsed = { summary: str(parsed.summary, 700), suggestions: (Array.isArray(parsed.suggestions) ? parsed.suggestions : []).filter(p => Number.isInteger(p.index) && p.index >= 0 && p.index < req.pieceCount && !seen.has(p.index) && seen.add(p.index)).slice(0,24).map(p => ({ index: p.index, angles: (Array.isArray(p.angles) ? p.angles : []).filter(Number.isFinite).slice(0,3).map(a => ((Math.round(a) % 360) + 360) % 360) })) };
    }
    if (mode === "name") {
      parsed = { charms: (parsed.charms || []).map(x => ({ index: num(x.index), slug: str(x.slug, 60).toLowerCase().replace(/[^a-z0-9\-]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 48) || null, label: str(x.label, 120), confidence: Math.max(0, Math.min(1, num(x.confidence))), metal: req.wantMetal && /^(gold|silver|rose)$/.test(x.metal || "") ? x.metal : null })).filter(x => x.slug) };
    }
    return Object.assign({ mode, model: MODEL, effort: req.effort, usage: res.usage || null, reasoning: reasoning || null }, parsed);
  } catch (e) {
    console.error("[charmNestAgent]", mode, e.status || "", e.message);
    const detail = (e.data && e.data.error && e.data.error.message) || e.message || "";
    return { skipped: `${mode === "name" ? "naming" : /^engrave/.test(mode) ? "engraving classifier" : "review"} unavailable (${e.status ? e.status + " " : ""}${String(detail).slice(0, 200)})` };
  }
}
module.exports = { run, buildRequest, MODEL, EFFORT, NAME_EFFORT };
