/*  netlify/functions/_etsyMailLearning.js
 *
 *  Records what the AI drafted next to what staff actually sent, and the
 *  questions the AI could not answer from the fact sheet. No model calls:
 *  the comparison is word by word. etsyMailLearn-background.js later turns
 *  the rewrites into rules and facts (_etsyMailKnowledge.js).
 *
 *    EtsyMail_DraftOutcomes/{id}   one per sent reply
 *    EtsyMail_KnowledgeGaps/{id}   one per missing fact, counted
 *    EtsyMail_Config/learningStats weekly counts (unchanged/light/rewrite)
 */

"use strict";

const crypto = require("crypto");

const OUTCOMES_COLL = "EtsyMail_DraftOutcomes";
const GAPS_COLL     = "EtsyMail_KnowledgeGaps";
const STATS_DOC     = "learningStats";
const SENT_STATUSES = new Set(["sent", "sent_unverified", "sent_text_only", "queued", "sending"]);

// ── comparison (pure) ────────────────────────────────────────────────

// The sign-off differs by who sends; it says nothing about the answer.
function stripSignoff(s) {
  return String(s || "")
    .replace(/\n\s*(?:kind regards|many thanks|best regards|best|thanks|thank you|warmly|cheers)[,!.]?\s*\n[\s\S]{0,60}$/i, "")
    .trim();
}
function words(s) {
  return stripSignoff(s).toLowerCase().replace(/[‘’]/g, "'").replace(/[“”]/g, '"')
    .match(/[a-z0-9$£€%'.:/-]+/g) || [];
}
function sentences(s) {
  return stripSignoff(s).split(/(?<=[.!?])\s+|\n+/).map(t => t.trim()).filter(t => t.length > 2);
}
function lcsLen(a, b) {
  if (!a.length || !b.length) return 0;
  const prev = new Uint16Array(b.length + 1), cur = new Uint16Array(b.length + 1);
  for (let i = 1; i <= a.length; i++) {
    for (let j = 1; j <= b.length; j++) {
      cur[j] = a[i - 1] === b[j - 1] ? prev[j - 1] + 1 : Math.max(prev[j], cur[j - 1]);
    }
    prev.set(cur);
  }
  return prev[b.length];
}
const numbersOf = s => (String(s || "").match(/[$£€]?\d+(?:[.,]\d+)?\s*(?:%|mm|cm|in(?:ch(?:es)?)?|"|days?|business days?|weeks?)?/gi) || [])
  .map(x => x.toLowerCase().replace(/\s+/g, " ").trim()).sort();
const linksOf = s => (String(s || "").match(/(?:https?:\/\/)?(?:www\.)?etsy\.com\/[^\s)]+|https?:\/\/[^\s)]+/gi) || [])
  .map(x => x.toLowerCase().replace(/^https?:\/\/(www\.)?/, "")).sort();

/** How much staff changed the AI's draft.
 *  kind: unchanged | light | rewrite | replaced */
function compareTexts(aiText, sentText) {
  const a = words(aiText).slice(0, 600), b = words(sentText).slice(0, 600);
  const l = lcsLen(a, b);
  const similarity = a.length + b.length ? (2 * l) / (a.length + b.length) : 1;
  const sa = sentences(aiText), sb = sentences(sentText);
  const norm = t => t.toLowerCase().replace(/\s+/g, " ");
  const setA = new Set(sa.map(norm)), setB = new Set(sb.map(norm));
  const added = sb.filter(t => !setA.has(norm(t))).slice(0, 8);
  const removed = sa.filter(t => !setB.has(norm(t))).slice(0, 8);
  const numbersChanged = numbersOf(aiText).join("|") !== numbersOf(sentText).join("|");
  const linksChanged = linksOf(aiText).join("|") !== linksOf(sentText).join("|");
  const kind = similarity >= 0.97 ? "unchanged"
    : similarity >= 0.8 ? "light"
    : similarity >= 0.35 ? "rewrite" : "replaced";
  return { similarity: Math.round(similarity * 1000) / 1000, kind, added, removed, numbersChanged, linksChanged };
}

function isoWeek(ms) {
  const d = new Date(ms);
  const t = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const day = t.getUTCDay() || 7;
  t.setUTCDate(t.getUTCDate() + 4 - day);
  const y = t.getUTCFullYear();
  const w = Math.ceil(((t - Date.UTC(y, 0, 1)) / 86400000 + 1) / 7);
  return `${y}-W${String(w).padStart(2, "0")}`;
}

// ── writes ───────────────────────────────────────────────────────────

/** One record per sent reply. prev is the draft doc as it stood before
 *  this send (the AI's text, if the AI wrote it). Never throws. */
async function recordOutcome({ db, admin, draftId, threadId, prev, sentText, sendOrigin, employeeName, polished = false }) {
  try {
    const FV = admin.firestore.FieldValue;
    const now = Date.now();
    const aiText = prev && prev.generatedByAI && !SENT_STATUSES.has(prev.status) ? String(prev.text || "") : "";
    let thread = {};
    try { const t = await db.collection("EtsyMail_Threads").doc(threadId).get(); thread = t.exists ? (t.data() || {}) : {}; } catch {}
    const cmp = aiText ? compareTexts(aiText, sentText) : null;
    const kind = cmp ? cmp.kind : "staff_only";
    const manual = sendOrigin !== "auto";
    // Worth learning from: a person changed what the AI wrote, or wrote the
    // reply themselves when the AI could not answer. Never a reply the
    // Polish button reworded: that wording is the model's, not a correction.
    const learnable = manual && !polished && cmp && (cmp.kind === "rewrite" || cmp.kind === "replaced"
      || (cmp.kind === "light" && (cmp.numbersChanged || cmp.linksChanged)));
    const route = prev && prev.generatedBySalesAgent ? "sales" : "support";
    const doc = {
      threadId, draftId, atMs: now, at: FV.serverTimestamp(), week: isoWeek(now),
      sendOrigin: sendOrigin || null, employeeName: employeeName || null, kind, route,
      customerName: thread.customerName || null,
      customerAsked: String((prev && prev.aiActiveQuestion) || thread.lastInboundPreview || "").slice(0, 800),
      aiText: aiText.slice(0, 4000), sentText: String(sentText || "").slice(0, 4000),
      aiConfidence: prev && typeof prev.aiConfidence === "number" ? prev.aiConfidence : null,
      aiModel: (prev && prev.aiModel) || null,
      aiMissingFacts: (prev && Array.isArray(prev.aiMissingFacts)) ? prev.aiMissingFacts.slice(0, 5) : [],
      learnStatus: learnable ? "pending" : "skip"
    };
    if (polished) doc.polished = true;
    if (cmp) Object.assign(doc, {
      similarity: cmp.similarity, added: cmp.added, removed: cmp.removed,
      numbersChanged: cmp.numbersChanged, linksChanged: cmp.linksChanged
    });
    const batch = db.batch();
    batch.set(db.collection(OUTCOMES_COLL).doc(`${draftId}_${now}`), doc);
    const inc = { [`weeks.${doc.week}.${kind}`]: FV.increment(1), [`weeks.${doc.week}.${route}_${kind}`]: FV.increment(1), updatedAtMs: now };
    batch.set(db.collection("EtsyMail_Config").doc(STATS_DOC), inc, { merge: true });
    await batch.commit();
    return doc;
  } catch (e) {
    console.warn("[learning] outcome not recorded:", e.message);
    return null;
  }
}

function gapId(q) {
  const norm = String(q || "").toLowerCase().replace(/[^a-z0-9 ]+/g, " ").replace(/\s+/g, " ").trim();
  return crypto.createHash("sha1").update(norm).digest("hex").slice(0, 20);
}

/** Facts the AI said it could not find. Counted per question. Never throws. */
async function recordMissingFacts({ db, admin, threadId, facts, route }) {
  try {
    const list = (Array.isArray(facts) ? facts : []).map(f => String(f || "").trim()).filter(f => f.length > 3).slice(0, 5);
    if (!list.length) return 0;
    const FV = admin.firestore.FieldValue;
    const batch = db.batch();
    for (const q of list) {
      batch.set(db.collection(GAPS_COLL).doc(gapId(q)), {
        question: q.slice(0, 300), count: FV.increment(1), threads: FV.arrayUnion(threadId),
        lastAtMs: Date.now(), route: route || "support"
      }, { merge: true });
    }
    await batch.commit();
    return list.length;
  } catch (e) {
    console.warn("[learning] missing facts not recorded:", e.message);
    return 0;
  }
}

module.exports = { OUTCOMES_COLL, GAPS_COLL, STATS_DOC, compareTexts, isoWeek, recordOutcome, recordMissingFacts, gapId };
