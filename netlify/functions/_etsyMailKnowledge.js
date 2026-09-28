/*  netlify/functions/_etsyMailKnowledge.js
 *
 *  What the inbox AIs may state as fact, and what they learned from the
 *  team's corrections. Both the support drafter and the sales agent add
 *  the block this module renders to their system prompt.
 *
 *  - Product facts: one sheet grouped by product family. The default is
 *    _etsyMailProductFacts.js (built from staff answers and the listings);
 *    the owner's edits and the facts added by the owner or learned from
 *    corrections live in EtsyMail_Config/productFacts (see effectiveFacts).
 *  - Learned rules: EtsyMail_Config/learnedRules, written by
 *    etsyMailLearn-background.js from staff corrections. Only rules marked
 *    active are shown to the AI.
 *
 *  Cost: the block sits at the end of the cached system prompt and only
 *  changes when a fact or rule changes (at most once a day from the
 *  learner), so each draft reads it from the prompt cache at a tenth of
 *  the input price.
 */

"use strict";

const DEFAULT_FACTS = require("./_etsyMailProductFacts");

const CONFIG_COLL = "EtsyMail_Config";
const FACTS_DOC   = "productFacts";
const RULES_DOC   = "learnedRules";
const CACHE_MS    = 10 * 60 * 1000;
const MAX_RULES   = 30;

// Families in the order the sheet shows them.
const FAMILY_ORDER = [
  "What we make", "Necklaces and charms", "Chains", "Huggie earrings", "Stud earrings",
  "Bracelets and anklets", "Rings", "Metals", "Stones and pearls", "Engraving",
  "Custom designs", "Production and rush", "Shipping", "Tracking and lost packages",
  "Returns, exchanges and cancellations", "Add-ons and fees"
];

let _cache = { at: 0, facts: null, rules: null };

function _db() { return require("./firebaseAdmin").firestore(); }

function familyRank(f) {
  const i = FAMILY_ORDER.indexOf(f);
  return i < 0 ? FAMILY_ORDER.length : i;
}

function cleanFacts(list) {
  return (Array.isArray(list) ? list : [])
    .filter(f => f && typeof f.text === "string" && f.text.trim())
    .map(f => ({
      id    : String(f.id || ""),
      family: String(f.family || "Other"),
      text  : f.text.trim(),
      status: f.status || "active",
      source: f.source || "staff"
    }));
}

/** The sheet the AI sees: the default facts with the owner's edits
 *  (doc.overrides: id -> {text, status, family}) plus facts added by the
 *  owner or learned from corrections (doc.added). Kept this way so better
 *  default facts in the code still reach the AI after the owner edits one. */
function effectiveFacts(doc) {
  const d = doc || {};
  const ov = (d.overrides && typeof d.overrides === "object") ? d.overrides : {};
  const base = cleanFacts(DEFAULT_FACTS).map(f => ov[f.id] ? { ...f, ...cleanPatch(ov[f.id]) } : f);
  const seen = new Set(base.map(f => f.id));
  const added = cleanFacts(d.added).filter(f => f.id && !seen.has(f.id));
  return base.concat(added);
}
function cleanPatch(p) {
  const out = {};
  if (p && typeof p.text === "string" && p.text.trim()) out.text = p.text.trim();
  if (p && typeof p.status === "string") out.status = p.status;
  if (p && typeof p.family === "string" && p.family) out.family = p.family;
  return out;
}

async function loadKnowledge({ fresh = false } = {}) {
  if (!fresh && _cache.facts && Date.now() - _cache.at < CACHE_MS) return _cache;
  let facts = cleanFacts(DEFAULT_FACTS), rules = [];
  try {
    const db = _db();
    const [fSnap, rSnap] = await db.getAll(
      db.collection(CONFIG_COLL).doc(FACTS_DOC),
      db.collection(CONFIG_COLL).doc(RULES_DOC));
    facts = effectiveFacts(fSnap.exists ? fSnap.data() : null);
    if (rSnap.exists && Array.isArray((rSnap.data() || {}).rules)) rules = rSnap.data().rules.filter(r => r && r.text);
  } catch (e) {
    console.warn("[knowledge] load failed, using the default facts:", e.message);
  }
  _cache = { at: Date.now(), facts, rules };
  return _cache;
}

/** The text block both AIs get. Deterministic for the same facts and
 *  rules, so the prompt cache keeps hitting. audience: "support" | "sales". */
function renderKnowledgeBlock({ facts, rules }, audience, opts = {}) {
  const active = facts.filter(f => f.status === "active");
  const byFamily = new Map();
  for (const f of active) {
    if (!byFamily.has(f.family)) byFamily.set(f.family, []);
    byFamily.get(f.family).push(f.text);
  }
  const families = Array.from(byFamily.keys())
    .sort((a, b) => (familyRank(a) - familyRank(b)) || a.localeCompare(b));

  // opts.bare: the short instructions (_etsyMailPrompts.js) already say how
  // to use the facts, so the sheet comes without that preamble.
  const out = opts.bare ? [
    "═══ PRODUCT AND SHOP FACTS ═══",
    "",
    "Listing links in these facts are the shop's own and may be sent.",
    ""
  ] : [
    "═══ PRODUCT AND SHOP FACTS (the owner's fact sheet) ═══",
    "",
    "State product, shipping and policy details only from these facts, the listing data you",
    "looked up, the order data, or what staff already told this customer in the thread. These",
    "facts win over a listing's own fields, over any tool's data and over your assumptions (a",
    "listing field saying 3 days processing does not change production time).",
    "",
    "When the customer asks for a detail that none of those sources gives (a back type, a stone",
    "size, a transit time to a country, whether something can be made or engraved somewhere):",
    "never guess or infer it from similar products. Answer everything else, name the detail in one",
    "short clause such as \"we'll confirm the stone size here\" (never \"the team is checking\"), hold",
    "the draft for a person, and name the missing detail in missing_facts. A person then answers it",
    "and it is added to this sheet. Never say how a future step will reach the customer (an Etsy or",
    "USPS email, a tracking number to follow) unless a fact says so.",
    "Listing links in these facts are the shop's own and may be sent like the add-on block's.",
    ""
  ];
  for (const fam of families) {
    out.push(fam + ":");
    for (const t of byFamily.get(fam)) out.push("- " + t);
    out.push("");
  }

  const scoped = (rules || [])
    .filter(r => r.status === "active" && (!r.scope || r.scope === "all" || r.scope === audience))
    .sort((a, b) => String(a.id).localeCompare(String(b.id)))
    .slice(0, MAX_RULES);
  if (scoped.length) {
    out.push("═══ LEARNED FROM THE TEAM'S CORRECTIONS ═══", "");
    out.push("Staff corrected earlier drafts in these ways. Follow them. The owner's rules above");
    out.push("(delivery dates, origin, refunds needing approval, codes) always win over these.");
    out.push("");
    for (const r of scoped) out.push("- " + String(r.text).trim());
    out.push("");
  }
  return out.join("\n").trim();
}

async function getKnowledgeBlock(audience = "support", opts = {}) {
  const k = await loadKnowledge();
  return renderKnowledgeBlock(k, audience, opts);
}

function invalidateKnowledgeCache() { _cache = { at: 0, facts: null, rules: null }; }

// The shop's automatic away reply ("We are away from the shop at the
// moment...") reads like a staff message but answers nothing.
// The business hours alone are not the away reply: the fact sheet states
// them, so a real answer quotes them. Nor is "we were away last week".
const AWAY_RX = /(?<!\b(?:were|was|been)\s+)\b(?:away from the (?:shop|studio|office)|out of (?:the )?office)\b/i;
function isAwayMessage(text) { return AWAY_RX.test(String(text || "")); }
const AWAY_NOTE = "[Automatic away reply, not written by staff. It answers nothing: every customer question before it is still open.]";

/** The stored option sheets were seeded with a guessed earring back
 *  ("post + butterfly back"). The back type comes from the fact sheet or
 *  the listing, so it is taken out of what the AI tools return. */
function scrubStyleFacts(charmStyles) {
  if (!charmStyles || typeof charmStyles !== "object") return charmStyles || null;
  const drop = new Set(["postType", "backType", "backing"]);
  const text = JSON.stringify(charmStyles, (k, v) => drop.has(k) ? undefined : v)
    .replace(/,?\s*(?:mounted )?on a (?:sterling silver )?post\s*\+\s*(?:a )?butterfly back/gi, ", mounted on a post (earring backs: see the fact sheet)")
    .replace(/(?:sterling silver )?post\s*\+\s*(?:a )?butterfly back/gi, "post (earring backs: see the fact sheet)");
  try { return JSON.parse(text); } catch { return null; }
}

module.exports = {
  CONFIG_COLL, FACTS_DOC, RULES_DOC, FAMILY_ORDER, DEFAULT_FACTS,
  loadKnowledge, renderKnowledgeBlock, getKnowledgeBlock, invalidateKnowledgeCache, cleanFacts, effectiveFacts,
  scrubStyleFacts, isAwayMessage, AWAY_NOTE
};
