/*  netlify/functions/etsyMailLearn-background.js
 *
 *  Turns staff corrections into rules and facts the inbox AIs follow.
 *
 *  Input: EtsyMail_DraftOutcomes marked learnStatus "pending" (a person
 *  rewrote the AI's draft before sending; see _etsyMailLearning.js).
 *  One Haiku call reads up to 30 of them next to the current rules and
 *  facts and says which general lesson or product fact each one teaches.
 *
 *  Safeguards:
 *  - a lesson or fact becomes active once 2 different conversations
 *    taught it (owner, 2026-09-28: staff know what they write, so two is
 *    confirmation enough), or after 1 when the learner marks it direct
 *    (staff stated it outright and nothing else explains the edit); until
 *    then it waits as "suggested" in Settings > Learning, where the owner
 *    can switch any of them on or off (an owner's "off" is never overridden);
 *  - learned rules sit below the owner's rules, which always win;
 *  - a learned rule nobody reconfirms for 120 days expires.
 *
 *  Started by etsyMailReapers.js at most once a day when corrections are
 *  waiting, or by the owner's "Learn now" button (op "run"). Cost: one
 *  Haiku call of about 15-20K input tokens per run.
 */

"use strict";

const crypto = require("crypto");
const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { callClaudeRaw } = require("./_etsyMailAnthropic");
const K = require("./_etsyMailKnowledge");
const { OUTCOMES_COLL, lessonHoldReason } = require("./_etsyMailLearning");

const db = admin.firestore();
const MODEL = process.env.ETSYMAIL_LEARN_MODEL || "claude-haiku-4-5-20251001";
const STATE_DOC = "learnState";
const BATCH = 30;
const RULE_THRESHOLD = 2;
const FACT_THRESHOLD = 2;
// A lesson the learner marks direct needs only the one conversation.
const needed = (item, n) => (item.direct ? 1 : n);
// A lesson that names a calendar date or promises a delivery time never goes
// live by itself: it waits as "suggested" (with heldReason) for the owner,
// and a learned one already live goes back to waiting. The owner's own
// switch (ownerSet) is never overridden.
function holdForPerson(item) {
  if (item.source !== "learned") return false;
  const why = lessonHoldReason(item.text);
  if (why) item.heldReason = why; else delete item.heldReason;
  if (why && item.status === "active" && item.source === "learned" && !item.ownerSet) item.status = "suggested";
  return !!why;
}
const RULE_EXPIRY_MS = 120 * 86400000;
const OUTCOME_KEEP_MS = 180 * 86400000;

const HARD_RULES = [
  "Never give a delivery date; business-day ranges only, with the no-guarantee sentence.",
  "Never say pieces are made in the US; origin questions follow the owner's origin rule.",
  "Any refund, remake, reship, replacement or discount waits for a person's approval.",
  "Only discount codes the issue tool returned may appear.",
  "Production is 4-6 business days."
];

const idFor = (prefix, text) => prefix + crypto.createHash("sha1").update(String(text).toLowerCase()).digest("hex").slice(0, 10);
const clip = (s, n) => { s = String(s || "").replace(/\s+/g, " ").trim(); return s.length > n ? s.slice(0, n) + "…" : s; };

function buildPrompt({ outcomes, rules, facts }) {
  const system = [
    "You improve the inbox AI of CustomBrites, a handmade jewelry shop on Etsy, from its staff's corrections.",
    "Each correction shows what the customer asked, what the AI drafted, and what staff actually sent instead.",
    "Find what each correction teaches that will hold for FUTURE customers:",
    "- a RULE: a general instruction about how to answer (what to say, offer, avoid, or check), written as a short imperative, at most 30 words, with no names, order numbers, tracking numbers or dates of one customer;",
    "- a FACT: a product, shipping or policy fact that staff stated in what they sent (never infer a fact staff did not write), one plain sentence.",
    "Reuse an existing rule or fact when the correction teaches the same thing: give its id in \"match\".",
    "Ignore corrections that only fix one customer's details, a typo, the greeting or the sign-off, or that you cannot explain.",
    "Set \"direct\": true only when this one correction settles it beyond doubt: staff stated the fact or instruction outright in what they sent, it holds for every customer, and nothing else explains the edit. Otherwise false; it then waits for a second conversation.",
    "Never write a rule that conflicts with these owner rules: " + HARD_RULES.join(" "),
    "Reply with JSON only, no prose, in exactly this shape:",
    '{"rules":[{"match":"<existing rule id or empty>","text":"...","scope":"all|support|sales","direct":false,"outcomes":["<outcome id>"]}],',
    ' "facts":[{"match":"<existing fact id or empty>","family":"<family>","text":"...","direct":false,"outcomes":["<outcome id>"]}],',
    ' "ignored":[{"outcome":"<outcome id>","why":"<a few words>"}]}',
    "Families: " + K.FAMILY_ORDER.join(", ") + "."
  ].join("\n");
  const user = [
    "CURRENT RULES:",
    ...(rules.length ? rules.map(r => `${r.id} [${r.status}] ${clip(r.text, 200)}`) : ["(none)"]),
    "",
    "CURRENT FACTS:",
    ...facts.map(f => `${f.id} (${f.family}) ${clip(f.text, 200)}`),
    "",
    "CORRECTIONS:",
    ...outcomes.map(o => [
      `### ${o.id} (${o.route}, ${o.kind})`,
      `Customer asked: ${clip(o.customerAsked, 500)}`,
      `AI drafted: ${clip(o.aiText, 700)}`,
      `Staff sent: ${clip(o.sentText, 700)}`
    ].join("\n"))
  ].join("\n");
  return { system, user };
}

function parseJson(text) {
  const t = String(text || "").trim().replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/, "");
  const i = t.indexOf("{"), j = t.lastIndexOf("}");
  if (i < 0 || j < i) throw new Error("no JSON in the learner's answer");
  return JSON.parse(t.slice(i, j + 1));
}

async function runLearning({ force = false } = {}) {
  const now = Date.now();
  const stateRef = db.collection("EtsyMail_Config").doc(STATE_DOC);
  const st = await stateRef.get();
  const prev = st.exists ? (st.data() || {}) : {};
  if (!force && prev.runningSinceMs && now - prev.runningSinceMs < 10 * 60000) return { skipped: "already running" };
  await stateRef.set({ runningSinceMs: now }, { merge: true });

  try {
    const pendSnap = await db.collection(OUTCOMES_COLL).where("learnStatus", "==", "pending").limit(BATCH).get();
    // A polished reply's wording is the model's, not a staff correction
    // (recordOutcome already skips these; this catches any that slip by).
    const polished = pendSnap.docs.filter(d => (d.data() || {}).polished === true);
    const outcomes = pendSnap.docs.filter(d => (d.data() || {}).polished !== true).map(d => ({ id: d.id, ...d.data() }));

    const cfg = db.collection(K.CONFIG_COLL);
    const [rSnap, fSnap] = await db.getAll(cfg.doc(K.RULES_DOC), cfg.doc(K.FACTS_DOC));
    const rules = rSnap.exists && Array.isArray((rSnap.data() || {}).rules) ? rSnap.data().rules : [];
    const factsDoc = fSnap.exists ? (fSnap.data() || {}) : {};
    const facts = K.effectiveFacts(factsDoc);

    let result = { rules: [], facts: [], ignored: [] }, usage = null;
    if (outcomes.length) {
      const { system, user } = buildPrompt({ outcomes, rules: rules.filter(r => r.status !== "expired"), facts });
      const res = await callClaudeRaw({
        model: MODEL, maxTokens: 4000, useThinking: false,
        system, messages: [{ role: "user", content: [{ type: "text", text: user }] }]
      });
      usage = res.usage || null;
      const text = (res.content || []).filter(b => b.type === "text").map(b => b.text).join("\n");
      result = parseJson(text);
    }

    const byOutcome = new Map(outcomes.map(o => [o.id, o]));
    const threadsOf = ids => Array.from(new Set((ids || []).map(id => byOutcome.get(id)).filter(Boolean).map(o => o.threadId)));
    const learnedFrom = new Map();   // outcome id -> [rule/fact ids]
    const note = (ids, id) => (ids || []).forEach(o => { if (byOutcome.has(o)) learnedFrom.set(o, (learnedFrom.get(o) || []).concat(id)); });

    // Rules
    const ruleById = new Map(rules.map(r => [r.id, r]));
    for (const r of Array.isArray(result.rules) ? result.rules : []) {
      const threads = threadsOf(r.outcomes);
      if (!threads.length || !r.text) continue;
      let rule = r.match && ruleById.get(r.match);
      if (!rule) {
        const id = idFor("r", r.text);
        rule = ruleById.get(id) || { id, text: clip(r.text, 300), scope: ["all", "support", "sales"].includes(r.scope) ? r.scope : "all",
          status: "suggested", source: "learned", threads: [], createdAtMs: now };
        ruleById.set(rule.id, rule);
      }
      rule.threads = Array.from(new Set([...(rule.threads || []), ...threads])).slice(-50);
      rule.support = rule.threads.length;
      rule.lastSeenAtMs = now;
      if (r.direct === true) rule.direct = true;
      if (rule.status === "expired") rule.status = "suggested";
      note(r.outcomes, rule.id);
    }
    for (const rule of ruleById.values()) {
      // Suggestions that already meet today's bar (it was 3 before 2026-09-28).
      const held = holdForPerson(rule);
      if (!held && rule.status === "suggested" && (rule.support || 0) >= needed(rule, RULE_THRESHOLD)) rule.status = "active";
      if (rule.source === "learned" && !rule.ownerSet && rule.status === "active" && now - (rule.lastSeenAtMs || rule.createdAtMs || now) > RULE_EXPIRY_MS) {
        rule.status = "expired";
      }
    }

    // Facts (learned ones are added next to the default sheet)
    const added = Array.isArray(factsDoc.added) ? factsDoc.added.slice() : [];
    const addedById = new Map(added.map(f => [f.id, f]));
    const knownIds = new Set(facts.map(f => f.id));
    for (const f of Array.isArray(result.facts) ? result.facts : []) {
      const threads = threadsOf(f.outcomes);
      if (!threads.length || !f.text) continue;
      if (f.match && knownIds.has(f.match) && !addedById.has(f.match)) { note(f.outcomes, f.match); continue; }  // already a fact
      let fact = (f.match && addedById.get(f.match)) || addedById.get(idFor("f", f.text));
      if (!fact) {
        fact = { id: idFor("f", f.text), family: K.FAMILY_ORDER.includes(f.family) ? f.family : "What we make",
          text: clip(f.text, 300), status: "suggested", source: "learned", threads: [], createdAtMs: now };
        added.push(fact); addedById.set(fact.id, fact);
      }
      fact.threads = Array.from(new Set([...(fact.threads || []), ...threads])).slice(-50);
      fact.support = fact.threads.length;
      fact.lastSeenAtMs = now;
      if (f.direct === true) fact.direct = true;
      note(f.outcomes, fact.id);
    }
    for (const fact of added) {
      const held = holdForPerson(fact);
      if (!held && fact.status === "suggested" && (fact.support || 0) >= needed(fact, FACT_THRESHOLD)) fact.status = "active";
    }

    const batch = db.batch();
    batch.set(cfg.doc(K.RULES_DOC), { rules: Array.from(ruleById.values()), updatedAtMs: now }, { merge: true });
    batch.set(cfg.doc(K.FACTS_DOC), { added, updatedAtMs: now }, { merge: true });
    const ignoredWhy = new Map((Array.isArray(result.ignored) ? result.ignored : []).map(x => [x.outcome, clip(x.why, 120)]));
    for (const o of outcomes) {
      batch.update(db.collection(OUTCOMES_COLL).doc(o.id), {
        learnStatus: "done", learnedAtMs: now,
        learnedInto: learnedFrom.get(o.id) || [], learnIgnored: ignoredWhy.get(o.id) || null
      });
    }
    for (const d of polished) batch.update(d.ref, { learnStatus: "skip", learnedAtMs: now });
    await batch.commit();

    // Keep 180 days of outcomes.
    let deleted = 0;
    try {
      const old = await db.collection(OUTCOMES_COLL).where("atMs", "<", now - OUTCOME_KEEP_MS).limit(300).get();
      if (!old.empty) { const b = db.batch(); old.docs.forEach(d => b.delete(d.ref)); await b.commit(); deleted = old.size; }
    } catch (e) { console.warn("[learn] cleanup skipped:", e.message); }

    const summary = {
      outcomes: outcomes.length, rulesTouched: (result.rules || []).length, factsTouched: (result.facts || []).length,
      activeRules: Array.from(ruleById.values()).filter(r => r.status === "active").length, deleted,
      inputTokens: usage ? usage.input_tokens : 0, outputTokens: usage ? usage.output_tokens : 0
    };
    await stateRef.set({ runningSinceMs: null, lastRunAtMs: now, lastResult: summary, lastError: null }, { merge: true });
    K.invalidateKnowledgeCache();
    return summary;
  } catch (e) {
    await stateRef.set({ runningSinceMs: null, lastRunAtMs: now, lastError: e.message }, { merge: true });
    throw e;
  }
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch {}
  try {
    const r = await runLearning({ force: body.force === true });
    console.log("[learn]", JSON.stringify(r));
    return { statusCode: 200, headers: CORS, body: JSON.stringify({ success: true, ...r }) };
  } catch (e) {
    console.error("[learn] failed:", e);
    return { statusCode: 500, headers: CORS, body: JSON.stringify({ error: e.message }) };
  }
};

exports.runLearning = runLearning;
exports.buildPrompt = buildPrompt;
