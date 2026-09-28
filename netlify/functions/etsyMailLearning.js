/*  netlify/functions/etsyMailLearning.js
 *
 *  Settings > Learning (owner only): what the AI learned from the team.
 *
 *  ops
 *    summary                      weekly edit rates, this week's biggest
 *                                 rewrites, rules, facts, open questions
 *    setRule    {id, status}      "active" | "off" (the owner's choice sticks)
 *    editRule   {id, text}
 *    saveFact   {id?, family, text, status?}
 *    answerGap  {gapId, family, answer}   the answer becomes a fact
 *    dismissGap {gapId}
 *    learnNow                     run the learner now
 *
 *  No model calls here and no Etsy calls; learnNow starts
 *  etsyMailLearn-background.js (one Haiku call).
 */

"use strict";

const crypto = require("crypto");
const fetch = require("node-fetch");
const admin = require("./firebaseAdmin");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const { requireOwnerSession } = require("./_etsyMailRoles");
const K = require("./_etsyMailKnowledge");
const { OUTCOMES_COLL, GAPS_COLL, STATS_DOC } = require("./_etsyMailLearning");

const db = admin.firestore();
const cfg = db.collection(K.CONFIG_COLL);

const json = (code, body) => ({ statusCode: code, headers: { ...CORS, "Content-Type": "application/json" }, body: JSON.stringify(body) });
const idFor = (prefix, text) => prefix + crypto.createHash("sha1").update(String(text).toLowerCase()).digest("hex").slice(0, 10);
const clean = (s, n) => String(s || "").replace(/\s+/g, " ").trim().slice(0, n);

async function summary() {
  const now = Date.now();
  const [statsSnap, rSnap, fSnap, stSnap] = await db.getAll(
    cfg.doc(STATS_DOC), cfg.doc(K.RULES_DOC), cfg.doc(K.FACTS_DOC), cfg.doc("learnState"));
  const weeks = statsSnap.exists ? ((statsSnap.data() || {}).weeks || {}) : {};
  const weekKeys = Object.keys(weeks).sort().slice(-8);
  const recent = await db.collection(OUTCOMES_COLL).where("atMs", ">=", now - 7 * 86400000).limit(400).get();
  const rewrites = recent.docs.map(d => ({ id: d.id, ...d.data() }))
    .filter(o => o.kind === "rewrite" || o.kind === "replaced" || (o.kind === "light" && (o.numbersChanged || o.linksChanged)))
    .sort((a, b) => (a.similarity || 0) - (b.similarity || 0) || b.atMs - a.atMs)
    .slice(0, 15)
    .map(o => ({ id: o.id, threadId: o.threadId, atMs: o.atMs, kind: o.kind, similarity: o.similarity, route: o.route,
      customerName: o.customerName, customerAsked: o.customerAsked, aiText: o.aiText, sentText: o.sentText,
      employeeName: o.employeeName, learnedInto: o.learnedInto || [], learnStatus: o.learnStatus }));
  const gapsSnap = await db.collection(GAPS_COLL).limit(300).get();
  const gaps = gapsSnap.docs.map(d => ({ id: d.id, ...d.data() }))
    .filter(g => !g.status || g.status === "open")
    .sort((a, b) => (b.count || 0) - (a.count || 0) || (b.lastAtMs || 0) - (a.lastAtMs || 0))
    .slice(0, 30)
    .map(g => ({ id: g.id, question: g.question, count: g.count || 1, threads: (g.threads || []).length, lastAtMs: g.lastAtMs || null }));
  const rules = (rSnap.exists && Array.isArray((rSnap.data() || {}).rules) ? rSnap.data().rules : [])
    .filter(r => r.status !== "expired")
    .map(r => ({ id: r.id, text: r.text, scope: r.scope || "all", status: r.status, support: r.support || 0, source: r.source || "learned" }));
  const facts = K.effectiveFacts(fSnap.exists ? fSnap.data() : null)
    .map(f => ({ id: f.id, family: f.family, text: f.text, status: f.status, source: f.source }));
  return { weeks: weekKeys.map(k => ({ week: k, ...weeks[k] })), rewrites, rules, facts, gaps,
           families: K.FAMILY_ORDER, learnState: stSnap.exists ? stSnap.data() : null };
}

async function setRule(id, patch) {
  await db.runTransaction(async tx => {
    const ref = cfg.doc(K.RULES_DOC);
    const snap = await tx.get(ref);
    const rules = snap.exists && Array.isArray((snap.data() || {}).rules) ? snap.data().rules : [];
    const r = rules.find(x => x.id === id);
    if (!r) throw new Error("rule not found");
    Object.assign(r, patch, { ownerSet: true, lastSeenAtMs: Date.now() });
    tx.set(ref, { rules, updatedAtMs: Date.now() }, { merge: true });
  });
}

async function saveFact({ id, family, text, status }) {
  const t = clean(text, 400);
  if (!t && !id) throw new Error("fact text required");
  const fam = K.FAMILY_ORDER.includes(family) ? family : (family ? clean(family, 60) : "What we make");
  let savedId = id;
  await db.runTransaction(async tx => {
    const ref = cfg.doc(K.FACTS_DOC);
    const snap = await tx.get(ref);
    const d = snap.exists ? (snap.data() || {}) : {};
    const overrides = { ...(d.overrides || {}) };
    const added = Array.isArray(d.added) ? d.added.slice() : [];
    const isDefault = id && K.cleanFacts(K.DEFAULT_FACTS).some(f => f.id === id);
    const a = id && added.find(f => f.id === id);
    const patch = { status: status || "active" };
    if (t) patch.text = t;
    if (family) patch.family = fam;
    if (isDefault) overrides[id] = { ...(overrides[id] || {}), ...patch };
    else if (a) Object.assign(a, patch, { ownerSet: true });
    else {
      savedId = idFor("f", t);
      added.push({ id: savedId, family: fam, text: t, status: "active", source: "owner", createdAtMs: Date.now() });
    }
    tx.set(ref, { overrides, added, updatedAtMs: Date.now() }, { merge: true });
  });
  K.invalidateKnowledgeCache();
  return savedId;
}

function functionsBase() {
  return process.env.URL || process.env.DEPLOY_PRIME_URL || "https://goldenspike.app";
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event.httpMethod !== "POST") return json(405, { error: "Method Not Allowed" });
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { return json(400, { error: "Invalid JSON" }); }
  // Customer messages are shown here, so a signed-in owner session is
  // required (a typed actor name is not enough).
  const owner = await requireOwnerSession(event);
  if (!owner.ok) return owner.reason === "OWNER_REQUIRED"
    ? json(403, { error: "Owner only", errorCode: "OWNER_REQUIRED" })
    : json(401, { error: "Sign in again", reason: owner.reason || "NO_SESSION" });
  body.actor = owner.username || owner.displayName || body.actor || null;

  try {
    switch (body.op) {
      case "summary":
        return json(200, { success: true, ...(await summary()) });
      case "setRule":
        if (!["active", "off"].includes(body.status)) return json(400, { error: "status must be active or off" });
        await setRule(String(body.id || ""), { status: body.status });
        return json(200, { success: true });
      case "editRule":
        if (!clean(body.text, 300)) return json(400, { error: "text required" });
        await setRule(String(body.id || ""), { text: clean(body.text, 300) });
        return json(200, { success: true });
      case "saveFact":
        return json(200, { success: true, id: await saveFact(body) });
      case "answerGap": {
        const gapId = String(body.gapId || "");
        const factId = await saveFact({ family: body.family, text: body.answer, status: "active" });
        await db.collection(GAPS_COLL).doc(gapId).set({ status: "answered", answer: clean(body.answer, 400),
          factId, answeredBy: body.actor || null, answeredAtMs: Date.now() }, { merge: true });
        return json(200, { success: true, factId });
      }
      case "dismissGap":
        await db.collection(GAPS_COLL).doc(String(body.gapId || "")).set({ status: "dismissed", answeredBy: body.actor || null,
          answeredAtMs: Date.now() }, { merge: true });
        return json(200, { success: true });
      case "learnNow": {
        const headers = { "Content-Type": "application/json" };
        if (process.env.ETSYMAIL_EXTENSION_SECRET) headers["X-EtsyMail-Secret"] = process.env.ETSYMAIL_EXTENSION_SECRET;
        const res = await fetch(`${functionsBase()}/.netlify/functions/etsyMailLearn-background`,
          { method: "POST", headers, body: JSON.stringify({ force: true }) });
        return json(200, { success: res.status === 202 || res.ok, status: res.status });
      }
      default:
        return json(400, { error: "unknown op" });
    }
  } catch (e) {
    console.error("[learning]", body.op, e);
    return json(500, { error: e.message });
  }
};
