/*  netlify/functions/_investorEventRouter.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — evidence-event routing (blueprint §5.5, §7.4, Appendix B).
 *
 *  Code attaches OBJECTIVE event classes and high-recall safety flags; it
 *  never decides investment materiality. "Material" means plausibly capable
 *  of changing the thesis, valuation, duration, invalidation or portfolio
 *  risk — that is Sol's call. What code may do:
 *
 *    · resolve which issuers an item concerns (entity resolution, fan-out);
 *    · deduplicate by a canonical event hash;
 *    · classify by form, item, source class and a high-recall keyword set;
 *    · record every verified delta so it reaches the next Sol packet;
 *    · on a high-impact class, safety-pause the affected UNFILLED entry and
 *      enqueue one deduplicated event revision;
 *    · on a routine delta for a name with an active mandate, prioritise it
 *      in the next manager packet.
 *
 *  Held-share protection is never touched here. Routing false negatives and
 *  review latency are measured through a deterministic sample.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");

const ROUTER_VERSION = "event-router.v1";

/* ── objective event classes ───────────────────────────────────────────── */
const EVENT_CLASSES = Object.freeze([
  "EARNINGS", "NEW_8K", "PERIODIC_FILING", "PROXY", "MERGER", "ACTIVIST", "MATERIAL_AGREEMENT", "ACQUISITION",
  "CAPITAL_RAISE", "AUDITOR_CHANGE", "RESTATEMENT", "CEO_OR_CFO_CHANGE", "GUIDANCE_CHANGE",
  "MATERIAL_REGULATORY_EVENT", "RECALL", "LITIGATION", "CYBER_EVENT", "CONTRACT_AWARD", "LABOR_EVENT",
  "WEATHER_HAZARD", "ROUTINE_NEWS", "DISCOVERY_LEAD", "PRICE_VOLUME_ANOMALY", "UNKNOWN",
]);
/* High-recall: an item in one of these classes pauses an unfilled entry
   and goes to Sol at once. Recall is deliberately high; Sol filters. */
const HIGH_IMPACT_CLASSES = Object.freeze(new Set([
  "EARNINGS", "MERGER", "ACQUISITION", "CAPITAL_RAISE", "AUDITOR_CHANGE", "RESTATEMENT", "CEO_OR_CFO_CHANGE",
  "GUIDANCE_CHANGE", "MATERIAL_REGULATORY_EVENT", "RECALL", "LITIGATION", "CYBER_EVENT", "ACTIVIST", "MATERIAL_AGREEMENT",
]));
const SAFETY_CLASSES = Object.freeze(["high_impact", "routine", "attention_only"]);

const ITEM_CLASSES = Object.freeze({
  "1.01": "MATERIAL_AGREEMENT", "1.02": "MATERIAL_AGREEMENT", "1.03": "LITIGATION",
  "2.01": "ACQUISITION", "2.02": "EARNINGS", "2.03": "CAPITAL_RAISE", "2.04": "CAPITAL_RAISE", "2.05": "RESTATEMENT",
  "2.06": "RESTATEMENT", "3.01": "MATERIAL_REGULATORY_EVENT", "3.02": "CAPITAL_RAISE", "4.01": "AUDITOR_CHANGE",
  "4.02": "RESTATEMENT", "5.01": "MERGER", "5.02": "CEO_OR_CFO_CHANGE", "5.03": "PROXY", "7.01": "ROUTINE_NEWS",
  "8.01": "ROUTINE_NEWS", "9.01": "ROUTINE_NEWS",
});
const KEYWORDS = Object.freeze([
  { re: /\b(recall|recalled|recalls)\b/i, cls: "RECALL" },
  { re: /\b(consent order|enforcement action|civil penalty|cease[- ]and[- ]desist|indict|indictment|settlement agreement|false claims act)\b/i, cls: "MATERIAL_REGULATORY_EVENT" },
  { re: /\b(lawsuit|class action|litigation|complaint filed|jury verdict|injunction)\b/i, cls: "LITIGATION" },
  { re: /\b(data breach|ransomware|cyberattack|cyber attack|unauthorized access|vulnerability|CVE-\d{4}-\d+)\b/i, cls: "CYBER_EVENT" },
  { re: /\b(guidance|outlook)\b.*\b(raise|raised|lower|lowered|withdraw|withdrawn|reaffirm|reaffirmed|cut|update|updated)\b/i, cls: "GUIDANCE_CHANGE" },
  { re: /\b(chief executive officer|chief financial officer|CEO|CFO)\b.*\b(resign|resigned|resignation|appoint|appointed|steps down|departure|terminated)\b/i, cls: "CEO_OR_CFO_CHANGE" },
  { re: /\b(restate|restated|restatement|non-reliance)\b/i, cls: "RESTATEMENT" },
  { re: /\b(merger|to acquire|acquisition of|definitive agreement|tender offer|take-private)\b/i, cls: "MERGER" },
  { re: /\b(contract award|awarded a contract|task order|IDIQ)\b/i, cls: "CONTRACT_AWARD" },
  { re: /\b(strike|walkout|unionize|collective bargaining|NLRB)\b/i, cls: "LABOR_EVENT" },
  { re: /\b(hurricane|tornado|wildfire|flood warning|winter storm)\b/i, cls: "WEATHER_HAZARD" },
]);
const REGULATOR_CLASSES = new Set(["regulator_primary", "government_primary", "investigator_primary"]);

function sha256(v) { return crypto.createHash("sha256").update(String(v)).digest("hex"); }
function items8k(text) {
  const out = new Set();
  for (const m of String(text || "").matchAll(/\bItem\s+(\d\.\d{2})\b/gi)) out.add(m[1]);
  return [...out].sort();
}

/** PURE. Objective class + safety flag for one document. */
function classifyDocument(doc = {}) {
  const form = String(doc.form || "").toUpperCase().trim();
  const text = `${doc.title || ""} ${doc.summary || ""} ${(doc.canonicalText || "").slice(0, 20000)}`;
  const reasons = [];
  let eventClass = "UNKNOWN";
  const discovery = doc.discoveryOnly === true || doc.sourceClass === "discovery_index";
  if (/^8-K/.test(form)) {
    const items = items8k(text);
    const classes = items.map((i) => ITEM_CLASSES[i]).filter(Boolean);
    const strongest = classes.find((c) => HIGH_IMPACT_CLASSES.has(c)) || classes[0] || null;
    eventClass = strongest || "NEW_8K";
    reasons.push(`form 8-K${items.length ? ` items ${items.join(",")}` : ""}`);
    if (strongest) reasons.push(`item class ${strongest}`);
    return finish({ eventClass, items, reasons, discovery, verified: true, doc, text });
  }
  if (/^10-K|^10-Q|^20-F|^40-F/.test(form)) { eventClass = "PERIODIC_FILING"; reasons.push(`form ${form}`); }
  else if (/^DEF 14A|^DEFA14A/.test(form)) { eventClass = "PROXY"; reasons.push("proxy"); }
  else if (/^S-4|^425|^DEFM14A|^SC TO/.test(form)) { eventClass = "MERGER"; reasons.push(`form ${form}`); }
  else if (/^SC 13D/.test(form)) { eventClass = "ACTIVIST"; reasons.push("13D"); }
  else if (/^S-1|^S-3|^424B/.test(form)) { eventClass = "CAPITAL_RAISE"; reasons.push(`form ${form}`); }
  for (const k of KEYWORDS) {
    if (k.re.test(text)) {
      if (eventClass === "UNKNOWN" || eventClass === "PERIODIC_FILING" || eventClass === "ROUTINE_NEWS") eventClass = k.cls;
      reasons.push(`keyword ${k.cls}`);
      break;
    }
  }
  if (eventClass === "UNKNOWN") {
    if (discovery) eventClass = "DISCOVERY_LEAD";
    else if (REGULATOR_CLASSES.has(doc.sourceClass) && /\b(fine|penalt|violation|order|investigat)/i.test(text)) eventClass = "MATERIAL_REGULATORY_EVENT";
    else eventClass = "ROUTINE_NEWS";
  }
  return finish({ eventClass, items: [], reasons, discovery, verified: !discovery, doc, text });
}
function finish({ eventClass, items, reasons, discovery, verified }) {
  const safetyClass = discovery ? "attention_only" : (HIGH_IMPACT_CLASSES.has(eventClass) ? "high_impact" : "routine");
  return { routerVersion: ROUTER_VERSION, eventClass, items, safetyClass, verified, discoveryOnly: discovery, reasons };
}

/** PURE. One hash per (symbol, source, accession-or-link, class) so a
 *  syndicated copy or a re-poll never becomes a second event. */
function canonicalEventHash({ symbol, doc, eventClass }) {
  const key = doc.accession || doc.link || doc.documentId || doc.title || "";
  return sha256(`${String(symbol).toUpperCase()}|${doc.sourceId || ""}|${String(key).toLowerCase()}|${eventClass}`);
}

/** PURE. Which issuers does an item concern? Alias regex per profile, the
 *  same resolution the source adapters use. Fan-out, never fan-in. */
function resolveEntities(item, profiles = [], matcher = null) {
  const match = matcher || (() => { try { return require("./_investorIntelligenceSources").matchesProfile; } catch { return null; } })();
  if (!match) return [];
  return profiles.filter((p) => { try { return match(item, p, { source_id: item.sourceId || "router" }); } catch { return false; } })
    .map((p) => p.symbol);
}

/** PURE. The routing decision of Appendix B. Never an investment judgment. */
function decideRoute(match) {
  if (!match || match.isNewVerifiedDelta !== true) return { action: "ignore", reason: match && match.reason || "not_a_new_verified_delta" };
  const actions = ["record_delta"];
  if (match.safetyClass === "high_impact") {
    actions.push("pause_unfilled_entry", "enqueue_event_revision");
    return { action: "high_impact", actions, dedupeId: `${match.symbol}:${match.canonicalEventHash}`, reason: `high-recall class ${match.eventClass}` };
  }
  if (match.hasActiveMandate) {
    actions.push("prioritize_next_manager_packet");
    return { action: "routine_with_mandate", actions, reason: "routine delta on a name with an active mandate" };
  }
  return { action: "routine", actions, reason: "routine evidence waits for the next Manager Meeting" };
}

/** PURE. A deterministic sample of routine deltas for the false-negative
 *  review of §7.4 (did a routine class hide something material?). */
function sampleForReview(deltas, { ppm = 50000 } = {}) {
  return (deltas || []).filter((d) => d && d.canonicalEventHash && parseInt(String(d.canonicalEventHash).slice(0, 6), 16) % 1000000 < ppm);
}

/** Resolve, deduplicate and classify one event against stored deltas. */
async function resolveDeduplicateAndClassifyObjective(event, { deps = {} } = {}) {
  const doc = event.document || event;
  const symbol = String(event.symbol || doc.symbol || "").toUpperCase();
  if (!symbol) return { isNewVerifiedDelta: false, reason: "no_symbol" };
  const classified = classifyDocument(doc);
  const canonical = canonicalEventHash({ symbol, doc, eventClass: classified.eventClass });
  const id = `delta_${symbol}_${canonical.slice(0, 32)}`;
  const admin = deps.admin || A;
  const ref = admin.col(admin.COL.evidenceDeltas).doc(id);
  const firstSeenAtMs = Number(doc.firstSeenAtMs) || A.now();
  const publishedAtMs = Date.parse(doc.source_published_at || doc.sourcePublishedAt || "");
  const inclusionMs = Math.max(firstSeenAtMs, Number(doc.fetchedAtMs) || 0);
  const created = await admin.runTransaction(async (tx) => {
    const cur = await tx.get(ref);
    if (cur.exists) return false;
    tx.set(ref, { schemaVersion: "evidence-delta.v1", deltaId: id, symbol, eventId: id, canonicalEventHash: canonical,
      eventClass: classified.eventClass, safetyClass: classified.safetyClass, items: classified.items, verified: classified.verified,
      discoveryOnly: classified.discoveryOnly, reasons: classified.reasons, documentId: doc.documentId || null,
      documentVersionId: doc.versionId || doc.documentVersionId || null, sourceId: doc.sourceId || null,
      summary: String(doc.summary || doc.canonicalText || "").slice(0,800),
      sourceClass: doc.sourceClass || null, title: String(doc.title || "").slice(0, 300),
      publishedAtMs: Number.isFinite(publishedAtMs) ? publishedAtMs : null, firstSeenAtMs, inclusionAtMs: inclusionMs,
      managerMateriality: "pending", reviewedAtMs: null, createdAtMs: A.now(),
      ...admin.envelope({ created_by: "eventRouter" }) });
    return true;
  });
  return { isNewVerifiedDelta: created && classified.verified, duplicate: !created, symbol, deltaId: id, eventId: id,
    canonicalEventHash: canonical, eventClass: classified.eventClass, safetyClass: classified.safetyClass,
    highRecallSafetyClass: classified.safetyClass === "high_impact", verified: classified.verified,
    firstSeenAt: inclusionMs, reason: created ? (classified.verified ? null : "discovery_lead_not_verified") : "duplicate" };
}

/** Appendix B — routeEvidenceEvent. deps allow the fixture set to run it
 *  in memory; production uses the real modules. */
async function routeEvidenceEvent(event, { deps = {} } = {}) {
  const match = await resolveDeduplicateAndClassifyObjective(event, { deps });
  if (!match.isNewVerifiedDelta) return { ...match, route: decideRoute(match) };
  const DOSSIER = deps.dossier || lazy("./_investorDossier");
  const MANDATE = deps.mandate || lazy("./_investorMandate");
  const JOBS = deps.jobs || lazy("./_investorJobs");
  let hasActiveMandate = false;
  try { hasActiveMandate = MANDATE && typeof MANDATE.hasActiveMandate === "function" ? await MANDATE.hasActiveMandate(match.symbol) : false; } catch {}
  const route = decideRoute({ ...match, hasActiveMandate });
  const effects = [];
  try { if (DOSSIER && typeof DOSSIER.recordRoutineDelta === "function") { await DOSSIER.recordRoutineDelta(match); effects.push("record_delta"); } } catch (e) { effects.push(`record_delta_failed:${String(e.message).slice(0, 60)}`); }
  if (route.action === "high_impact") {
    try { if (MANDATE && typeof MANDATE.pauseUnfilledEntry === "function") { await MANDATE.pauseUnfilledEntry(match.symbol, match.eventId); effects.push("pause_unfilled_entry"); } }
    catch (e) { effects.push(`pause_failed:${String(e.message).slice(0, 60)}`); }
    if (JOBS && typeof JOBS.enqueueOnce === "function") {
      const q = await JOBS.enqueueOnce({ task: "event_revision", dedupeId: route.dedupeId, accountId: event.accountId || null,
        priority: 50, payload: { accountId:event.accountId || "paper-1", symbol: match.symbol, eventId: match.eventId, cutoff: match.firstSeenAt, eventClass: match.eventClass } });
      effects.push(q.enqueued ? "enqueued_event_revision" : "event_revision_already_queued");
    }
  } else if (route.action === "routine_with_mandate" && JOBS && typeof JOBS.prioritizeNextManagerPacket === "function") {
    await JOBS.prioritizeNextManagerPacket({ symbol: match.symbol, deltaId: match.deltaId });
    effects.push("prioritized_next_packet");
  }
  return { ...match, hasActiveMandate, route, effects };
}
function lazy(mod) { try { return require(mod); } catch { return null; } }

module.exports = {
  ROUTER_VERSION, EVENT_CLASSES, HIGH_IMPACT_CLASSES, SAFETY_CLASSES, ITEM_CLASSES,
  items8k, classifyDocument, canonicalEventHash, resolveEntities, decideRoute, sampleForReview,
  resolveDeduplicateAndClassifyObjective, routeEvidenceEvent,
};
