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
 *    EtsyMail_ReplyDaily/{day}     one small entry per sent reply, for the Employee portal's
 *                                  inbox figures (who, auto or manual, order, customer); see
 *                                  replyEntry() and _employeeInbox.js (Paul, 6 Oct 2026)
 */

"use strict";

const crypto = require("crypto");

const OUTCOMES_COLL = "EtsyMail_DraftOutcomes";
const GAPS_COLL     = "EtsyMail_KnowledgeGaps";
const STATS_DOC     = "learningStats";
const REPLY_DAILY_COLL = "EtsyMail_ReplyDaily";
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

// ── the sent-reply entry (for the portal's inbox figures) ────────────

// The shop's day, daylight saving included. America/Toronto is the same Eastern clock (same days, same changes), so this is
// the portal's own rule (employeeEfficiency.js nyDay) and the portal reads the document by the same day.
const REPLY_DAY_FMT = new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit" });
function replyDay(ms) { return REPLY_DAY_FMT.format(new Date(ms)); }          // "2026-10-06"

const foldText = s => String(s == null ? "" : s).normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
const plainText = (s, n) => String(s == null ? "" : s).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, n);

/** One small entry per sent reply: who (the inbox username the page sent, "" when none), manual or auto, the conversation's order
 *  number, the customer (a short key, so "messages per customer" can be counted, and the name for a top list) and the
 *  conversation number. Pure; JSON-safe; about 120 bytes. Auto = the auto-pipeline (sendOrigin "auto" or a system: name):
 *  never a person. No text of the reply, no address, no PIN is ever in it. */
function replyEntry({ atMs, employeeName, sendOrigin, thread, threadId }) {
  const t = thread && typeof thread === "object" ? thread : {};
  const by = plainText(employeeName, 60);
  const th = String(threadId || "").replace(/\D/g, "").slice(0, 24);
  const oid = String(t.etsyOrderId || t.linkedOrderId || "").replace(/\D/g, "");
  const ident = foldText(t.etsyUsername) || foldText(t.customerName) || th;
  return {
    t: Math.floor(Number(atMs) || Date.now()),
    by,
    o: sendOrigin === "auto" || /^system:/i.test(by) ? "a" : "m",
    r: /^\d{9,14}$/.test(oid) ? oid : "",
    c: ident ? crypto.createHash("sha1").update(ident).digest("hex").slice(0, 12) : "",
    n: plainText(t.customerName, 40),
    th
  };
}

/** Append the entry to the day's document. Its own try/catch: never throws, and the send never waits on a failure. */
async function recordReply({ db, admin, entry }) {
  try {
    const FV = admin.firestore.FieldValue, day = replyDay(entry.t);
    await db.collection(REPLY_DAILY_COLL).doc(day).set({ day, updatedAtMs: Date.now(), replies: FV.arrayUnion(entry) }, { merge: true });
    return true;
  } catch (e) {
    console.warn("[learning] reply entry not recorded:", e.message);
    return false;
  }
}

// ── writes ───────────────────────────────────────────────────────────

/** One record per sent reply. prev is the draft doc as it stood before
 *  this send (the AI's text, if the AI wrote it). Never throws. */
async function recordOutcome({ db, admin, draftId, threadId, prev, sentText, sendOrigin, employeeName, polished = false }) {
  let replyP = null;
  try {
    const FV = admin.firestore.FieldValue;
    const now = Date.now();
    const aiText = prev && prev.generatedByAI && !SENT_STATUSES.has(prev.status) ? String(prev.text || "") : "";
    let thread = {};
    try { const t = await db.collection("EtsyMail_Threads").doc(threadId).get(); thread = t.exists ? (t.data() || {}) : {}; } catch {}
    // the portal's inbox figures: who sent it, which order, which customer (independent of the learning record below)
    try { replyP = recordReply({ db, admin, entry: replyEntry({ atMs: now, employeeName, sendOrigin, thread, threadId }) }); } catch (e) { console.warn("[learning] reply entry skipped:", e.message); }
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
  } finally {
    if (replyP) await replyP;                         // (recordReply never rejects; awaited so the function does not end before the write)
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

// ── lessons that wait for a person (pure) ────────────────────────────

// A learned rule or fact goes live with nobody reading it, so one that names
// a calendar date or promises when an order arrives waits for the owner
// (owner's rule: no delivery dates in any draft). Business-day ranges and
// "we can't guarantee delivery dates" are fine.
const L_MON = "(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\\.?";
const LESSON_DATE_RX = new RegExp("\\b(?:" + L_MON + "\\s+\\d{1,2}(?:st|nd|rd|th)?\\b|\\d{1,2}(?:st|nd|rd|th)?\\s+(?:of\\s+)?" + L_MON + "(?![a-z])" +
  "|\\d{1,2}/\\d{1,2}/\\d{2,4}\\b|\\d{4}-\\d{2}-\\d{2}\\b)", "i");
const LESSON_PROMISE_RX = /\b(?:arriv\w*|deliver(?:s|ed|y|ies)?|get\s+(?:it|there|to\s+(?:you|them))|reach\w*|be\s+there|in\s+(?:your|their)\s+hands)\b[^.!?\n]{0,40}?\b(?:by|before|no\s+later\s+than|in\s+time\s+for|(?:with)?in\s+\d+(?:\s*(?:-|to)\s*\d+)?\s+(?!business)(?:\w+\s+)?(?:days?|weeks?|hours?))\b|\bguarantee[sd]?\s+(?:delivery|arrival)\b/i;
const LESSON_NEG_RX = /\b(?:never|not|no|cannot|avoid)\b|n['’]t\b/i;
function lessonHoldReason(text) {
  const s = String(text || "");
  if (LESSON_DATE_RX.test(s)) return "names a calendar date";
  const { deliveryDateSentence } = require("./_etsyMailVetoes");
  for (const t of s.split(/(?<=[.!?;])\s+|\n+/)) {
    if (LESSON_NEG_RX.test(t)) continue;   // "never give a delivery date"
    if (LESSON_PROMISE_RX.test(t) || deliveryDateSentence(t)) return "promises a delivery time";
  }
  return null;
}

module.exports = { OUTCOMES_COLL, GAPS_COLL, STATS_DOC, REPLY_DAILY_COLL, compareTexts, isoWeek, replyDay, replyEntry, recordReply, recordOutcome, recordMissingFacts, gapId, lessonHoldReason };
