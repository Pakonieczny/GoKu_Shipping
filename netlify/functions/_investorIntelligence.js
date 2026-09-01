/* Investor_AI — point-in-time public company intelligence.
 *
 * This module deliberately separates five questions that are often collapsed
 * into a single, misleading "sentiment" number:
 *   1. Is the reported event probably true?
 *   2. Is it probably material to this company?
 *   3. If material, how large and in which direction could the effect be?
 *   4. When might it matter, and for how long?
 *   5. How much of it appears to have reached the price already?
 *
 * Source collection and language-model extraction can propose evidence. Only
 * the deterministic policy below can block, resize, or request an exit. A
 * discovery index never counts as confirmation, syndicated copies count once,
 * positive intelligence never increases risk above the base position, and
 * missing/stale coverage blocks new risk.
 */
"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const T = require("./_investorTemporal");

const DAY_MS = 864e5;
const LOOKBACK_DAYS = 180;
const DEFAULT_MAX_AGE_HOURS = 6;
/* The only preregistered floor challenger is 25.  Letting an arbitrary policy
   clamp this just below the hard block (69.999) would erase nearly every
   non-blocking warning while still claiming the hard block was preserved. */
const MAX_NON_BLOCKING_RISK_FLOOR = 25;

const EVENT_RULES = [
  { type: "labor_operations", direction: -1, severity: 72,
    re: /\b(strike|walkout|work stoppage|labor dispute|union vote|picket|lockout|layoff|furlough|collective bargain|contract negotiation)\b/i },
  { type: "regulatory_safety", direction: -1, severity: 82,
    re: /\b(ground(?:ed|ing)?|airworthiness directive|safety investigation|regulatory action|enforcement|inspection|certification delay|consent order|recall|defect|accident|incident)\b/i },
  { type: "production_supply", direction: -1, severity: 66,
    re: /\b(production (?:halt|delay|cut|slow)|supply chain|supplier disruption|shortage|delivery delay|quality escape|factory closure)\b/i },
  { type: "commercial_order", direction: 1, severity: 55,
    re: /\b(firm order|purchase agreement|ordered? \d+|customer order|air show|delivery agreement|contract award|booked order)\b/i },
  { type: "commercial_cancellation", direction: -1, severity: 68,
    re: /\b(order cancellation|cancelled? order|contract termination|customer defection|lost bid|delivery refusal)\b/i },
  { type: "government_contract", direction: 1, severity: 58,
    re: /\b(contract award|task order|defense contract|procurement|obligat(?:ed|ion)|award amount)\b/i },
  { type: "financial_guidance", direction: -1, severity: 76,
    re: /\b(profit warning|guidance cut|lowered guidance|cash burn|liquidity concern|going concern|restatement|missed estimates|covenant breach)\b/i },
  { type: "financial_positive", direction: 1, severity: 64,
    re: /\b(raised guidance|guidance increase|record revenue|beat estimates|free cash flow improvement|debt reduction)\b/i },
  { type: "legal_enforcement", direction: -1, severity: 70,
    re: /\b(indictment|criminal investigation|civil penalty|lawsuit|subpoena|fraud|antitrust|settlement|fine|debarment)\b/i },
  { type: "leadership_governance", direction: -1, severity: 48,
    re: /\b(chief executive|chief financial officer|ceo|cfo|director).{0,55}\b(resign|depart|removed|terminated|succession)\b/i },
  { type: "product_technology", direction: 1, severity: 42,
    re: /\b(launch|certified|approval|first flight|new product|technology milestone|production milestone)\b/i },
  { type: "cyber_operational", direction: -1, severity: 62,
    re: /\b(cyberattack|ransomware|data breach|systems outage|operational outage|unauthorized access)\b/i },
];

const CONTRADICTION_RE = /\b(den(?:y|ies|ied)|dispute[sd]?|false|unfounded|inaccurate|no plans? to|not true|retracted?)\b/i;
const IMMINENT_RE = /\b(today|tomorrow|this week|within \d+ days?|deadline|vote|scheduled|effective immediately|imminent)\b/i;
const MATERIAL_RE = /\b(material|significant|major|all employees|plant-wide|fleet-wide|billion|\$\s?\d+(?:\.\d+)?\s?b|\d{4,}\s+(?:employees|workers|aircraft|orders))\b/i;

function clamp(value, lo = 0, hi = 1) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.max(lo, Math.min(hi, n)) : lo;
}
function round(value, places = 3) {
  const p = 10 ** places;
  return Number((Math.round(Number(value) * p) / p).toFixed(places));
}
function norm(value) { return String(value || "").replace(/\s+/g, " ").trim(); }
function sha(value) { return crypto.createHash("sha256").update(String(value)).digest("hex"); }
function eventTime(doc) {
  const published = Date.parse(doc && (doc.source_published_at || doc.sourcePublishedAt) || "");
  if (Number.isFinite(published)) return published;
  const seen = Number(doc && (doc.firstSeenAtMs || doc.decisionKnownAtMs));
  return Number.isFinite(seen) ? seen : null;
}
function docRef(doc) { return String(doc.documentId || doc.versionId || sha(doc.link || doc.title).slice(0, 24)); }
function publisherGroup(doc) {
  return norm(doc.publisherGroup || doc.independenceGroup || doc.publisherDomain || doc.sourceId || "unknown").toLowerCase();
}
function isDiscovery(doc) { return doc.discoveryOnly === true || doc.sourceClass === "discovery_index"; }
function reliability(doc) {
  const explicit = Number(doc.sourceReliability);
  if (Number.isFinite(explicit)) return clamp(explicit, 0.1, 0.99);
  if (doc.sourceTier === "A") return 0.88;
  if (doc.sourceTier === "B") return 0.72;
  return isDiscovery(doc) ? 0.45 : 0.55;
}
function sourceIsGovernment(doc) {
  return /government_primary|regulator_primary|investigator_primary/.test(String(doc.sourceClass || ""));
}
function documentPreference(doc) {
  const authority = isDiscovery(doc) ? 0 : (doc.sourceTier === "A" ? 3 : doc.sourceTier === "B" ? 2 : 1);
  return authority * 10 + reliability(doc);
}

/** Keep the newest point-in-time version, collapse identical bytes/URLs, and
 * retain one publisher group only once for corroboration. */
function deduplicateDocuments(documents, asOfMs = Date.now()) {
  const byContent = new Map();
  for (const doc of documents || []) {
    const known = Number(doc.decisionKnownAtMs || doc.firstSeenAtMs);
    if (Number.isFinite(known) && known > asOfMs) continue;
    const key = String(doc.canonicalContentSha256 || "")
      || sha(norm(doc.link).replace(/[?#].*$/, "") || `${norm(doc.title).toLowerCase()}|${norm(doc.canonicalText).slice(0, 500)}`);
    const prior = byContent.get(key);
    const docKnown = Number(doc.decisionKnownAtMs || doc.firstSeenAtMs || Infinity);
    const priorKnown = Number(prior && (prior.decisionKnownAtMs || prior.firstSeenAtMs) || Infinity);
    if (!prior || documentPreference(doc) > documentPreference(prior)
        || (documentPreference(doc) === documentPreference(prior) && docKnown < priorKnown)) {
      byContent.set(key, doc);
    }
  }
  return [...byContent.values()].sort((a, b) => (eventTime(b) || 0) - (eventTime(a) || 0));
}

function exactSnippet(text, re, max = 360) {
  const clean = norm(text);
  const m = clean.match(re);
  if (!m) return clean.slice(0, max);
  const at = Math.max(0, m.index - 90);
  return clean.slice(at, at + max);
}
function documentText(doc) {
  return norm(`${doc && doc.title || ""} ${doc && (doc.canonicalText || doc.summary) || ""}`);
}

const CLUSTER_STOP = new Set(["about", "after", "again", "against", "agency", "announced",
  "announcement", "company", "could", "developing", "effective", "enforcement", "event",
  "federal", "fleet", "from", "government", "immediately", "incident", "investigation",
  "major", "material", "more",
  "news", "official", "public", "recall", "regulator", "regulatory", "release", "report",
  "reported", "safety", "says", "scheduled", "state", "that", "their", "this", "update",
  "wide", "with", "would"]);
function eventTokens(value) {
  return new Set(norm(value).toLowerCase().split(/[^a-z0-9]+/)
    .filter((x) => (x.length >= 4 || /^\d{2,}$/.test(x))
      && !/^(?:19|20)\d{2}$/.test(x) && !CLUSTER_STOP.has(x)));
}
function tokenOverlap(a, b) {
  const aa = eventTokens(a), bb = eventTokens(b);
  if (!aa.size || !bb.size) return 0;
  const common = [...aa].filter((x) => bb.has(x)).length;
  return common / Math.min(aa.size, bb.size);
}
function explicitIdentifiers(value) {
  const ids = norm(value).toUpperCase().match(/\b(?=[A-Z0-9-]{4,}\b)(?=[A-Z0-9-]*\d)[A-Z0-9]+(?:-[A-Z0-9]+)+\b|\b\d{4,}\b/g) || [];
  return new Set(ids.filter((x) => !/^(?:19|20)\d{2}$/.test(x)));
}
/* Tokens carried by most documents in the batch under consideration.
 *
 * Every document in a symbol's batch names the same issuer, the same sector
 * vocabulary and often the same fiscal period, so those tokens are constants
 * and carry no power to tell two matters apart. Measuring that from the batch
 * itself replaces the hand-maintained CLUSTER_STOP blocklist, which could
 * only ever exclude words somebody had already been burned by. */
function batchCommonTokens(subjects, threshold = 0.5) {
  const sets = (subjects || []).map(eventTokens).filter((s) => s.size);
  const out = new Set();
  if (sets.length < 3) return out;         // too few documents to estimate
  const df = new Map();
  for (const s of sets) for (const t of s) df.set(t, (df.get(t) || 0) + 1);
  for (const [t, c] of df) if (c / sets.length >= threshold) out.add(t);
  return out;
}

/* Two subjects describe the same matter when they share an explicit
 * identifier, or when what they share is BOTH substantial and
 * discriminating.
 *
 * The v8.4 rule merged on two shared tokens at a 0.28 ratio, which the issuer
 * name plus one filler word clears — that is how three unrelated regulator
 * matters became one event with manufactured corroboration. The threshold is
 * back at 0.34, batch-common tokens are excluded from the numerator, and two
 * or more disjoint distinguishing tokens on BOTH sides now block the merge:
 * documents that agree on vocabulary but disagree on subject stay apart. */
function sameEventSubject(a, b, common = null) {
  const idsA = explicitIdentifiers(a), idsB = explicitIdentifiers(b);
  if ([...idsA].some((x) => idsB.has(x))) return true;

  /* Substance is measured on the full token sets. Batch-common vocabulary is
     NOT removed here: when a batch happens to be homogeneous — three
     publishers covering one matter — every shared token is batch-common and
     discounting them all would split an event that is genuinely one. */
  const aa = eventTokens(a), bb = eventTokens(b);
  if (!aa.size || !bb.size) return false;
  const shared = [...aa].filter((x) => bb.has(x));
  if (shared.length < 2) return false;
  if (shared.length / Math.min(aa.size, bb.size) < 0.34) return false;

  /* Refusal is measured on the discounted sets. Two or more distinguishing
     tokens on BOTH sides, after the vocabulary this batch shares by
     construction is set aside, means the documents agree on wording and
     disagree on subject. An asymmetric remainder (one side elaborating on
     the other) is how follow-up coverage of a single matter reads. */
  const drop = common instanceof Set ? common : new Set();
  const aOnly = [...aa].filter((x) => !bb.has(x) && !drop.has(x)).length;
  const bOnly = [...bb].filter((x) => !aa.has(x) && !drop.has(x)).length;
  const sharedSpecific = shared.filter((x) => !drop.has(x)).length;
  if (aOnly >= 2 && bOnly >= 2) return false;
  /* Nothing specific in common, and one side naming two or more things the
     other never mentions: the agreement is entirely batch vocabulary. */
  if (sharedSpecific <= 1 && Math.max(aOnly, bOnly) >= 2) return false;
  return true;
}

/** Deterministic fallback extraction. It is intentionally conservative: it
 * emits event hypotheses, not facts, and every claim is an exact source span. */
function extractEvents(documents, asOfMs = Date.now()) {
  const docs = deduplicateDocuments(documents, asOfMs);
  /* First pass collects every candidate subject so the second pass can
     discount the vocabulary this batch shares by construction. */
  const allSubjects = [];
  for (const doc of docs) {
    const text = documentText(doc);
    if (!text) continue;
    for (const rule of EVENT_RULES) {
      if (rule.re.test(text)) allSubjects.push(exactSnippet(text, rule.re));
    }
  }
  const commonTokens = batchCommonTokens(allSubjects);
  const buckets = [];
  for (const doc of docs) {
    const text = documentText(doc);
    if (!text) continue;
    for (const rule of EVENT_RULES) {
      if (!rule.re.test(text)) continue;
      const at = eventTime(doc);
      const subject = exactSnippet(text, rule.re);
      let bucket = buckets.find((x) => x.type === rule.type
        && Number.isFinite(at) && Number.isFinite(x.eventAtMs)
        && Math.abs(at - x.eventAtMs) <= 4 * DAY_MS
        && sameEventSubject(subject, x.anchorText, commonTokens));
      if (!bucket) {
        bucket = { ...rule, eventAtMs: at, anchorText: subject, documents: [], claims: [] };
        buckets.push(bucket);
      }
      bucket.documents.push(doc);
      bucket.claims.push({ documentRef: docRef(doc), quote: subject,
        claim: norm(doc.title || `${rule.type} event`) });
    }
  }
  return buckets.map((b) => ({
    eventType: b.type,
    title: norm((b.documents[0] || {}).title || b.type.replace(/_/g, " ")).slice(0, 240),
    direction: b.direction,
    proposedSeverity: b.severity,
    eventAtMs: b.eventAtMs,
    timing: b.documents.some((d) => IMMINENT_RE.test(`${d.title || ""} ${d.canonicalText || d.summary || ""}`))
      ? "imminent" : "developing",
    claims: b.claims.slice(0, 8),
    documentRefs: [...new Set(b.documents.map(docRef))],
  }));
}

/** Prefer quote-grounded model groupings while retaining distinct fallback
 * events of the same category. Category-wide replacement can hide a second
 * order, recall, strike or investigation. */
function mergeEventHypotheses(preferred = [], fallback = []) {
  const out = [...preferred];
  for (const event of fallback) {
    const refs = new Set([...(event.documentRefs || []), ...(event.claims || []).map((x) => x.documentRef)].map(String));
    const duplicate = out.some((x) => {
      if (x.eventType !== event.eventType) return false;
      const xrefs = new Set([...(x.documentRefs || []), ...(x.claims || []).map((c) => c.documentRef)].map(String));
      if (refs.size && xrefs.size) return [...refs].some((r) => xrefs.has(r));
      return tokenOverlap(x.title || "", event.title || "") >= 0.5;
    });
    if (!duplicate) out.push(event);
  }
  return out.slice(0, 16);
}

function referencedEventDocuments(rawEvent, documents) {
  const refs = new Set([...(rawEvent.documentRefs || []), ...(rawEvent.claims || []).map((x) => x.documentRef)]
    .map(String));
  return (documents || []).filter((d) => refs.has(docRef(d)));
}

function eventSubjectForDocument(rawEvent, doc) {
  const ref = docRef(doc);
  const quoted = (rawEvent.claims || []).filter((x) => String(x.documentRef || "") === ref)
    .map((x) => norm(`${x.claim || ""} ${x.quote || ""}`)).filter(Boolean).join(" ");
  if (quoted) return quoted;
  const rule = EVENT_RULES.find((x) => x.type === rawEvent.eventType);
  return rule ? exactSnippet(documentText(doc), rule.re) : documentText(doc);
}

function clusterEventDocuments(rawEvent, documents) {
  const referenced = referencedEventDocuments(rawEvent, documents);
  const rows = referenced.map((doc) => ({ doc, subject: eventSubjectForDocument(rawEvent, doc) }));
  const commonTokens = batchCommonTokens(rows.map((x) => x.subject));
  const groups = [];
  for (const row of rows) {
    const group = groups.find((g) => g.every((x) =>
      sameEventSubject(row.subject, x.subject, commonTokens)));
    if (group) group.push(row); else groups.push([row]);
  }
  return groups;
}

function splitEventHypothesis(rawEvent, documents) {
  const groups = clusterEventDocuments(rawEvent, documents);
  if (groups.length <= 1) return [rawEvent];
  return groups.map((group) => {
    const refs = new Set(group.map((x) => docRef(x.doc)));
    const claims = (rawEvent.claims || []).filter((x) => refs.has(String(x.documentRef || "")));
    const first = group[0].doc;
    return { ...rawEvent, title: norm(first.title || rawEvent.title).slice(0, 240),
      claims, documentRefs: [...refs], subjectSplit: true,
      subjectSplitCount: groups.length };
  });
}

function verifiedClaims(rawEvent, documents) {
  const map = new Map((documents || []).map((d) => [docRef(d), d]));
  const selectedRefs = new Set(eventDocuments(rawEvent, documents).map(docRef));
  const claims = [];
  for (const claim of rawEvent.claims || []) {
    const doc = map.get(String(claim.documentRef || ""));
    const quote = norm(claim.quote);
    const haystack = documentText(doc);
    if (!doc || !selectedRefs.has(docRef(doc)) || quote.length < 4 || !haystack.includes(quote)) continue;
    claims.push({ claim: norm(claim.claim).slice(0, 300), quote: quote.slice(0, 500),
      documentRef: docRef(doc) });
  }
  return claims.slice(0, 10);
}

function eventDocuments(rawEvent, documents) {
  const rule = EVENT_RULES.find((x) => x.type === rawEvent.eventType);
  const groups = clusterEventDocuments(rawEvent, documents);
  if (!groups.length) return [];
  const title = rawEvent.title || "";
  const score = (group) => {
    const overlap = Math.max(0, ...group.map((x) => tokenOverlap(title, x.subject)));
    const authority = Math.max(0, ...group.map((x) => documentPreference(x.doc)));
    return overlap * 100 + authority;
  };
  groups.sort((a, b) => score(b) - score(a));
  const selected = groups[0].map((x) => x.doc), subjects = groups[0].map((x) => x.subject);
  if (!rule) return selected;
  for (const doc of documents || []) {
    if (selected.some((x) => docRef(x) === docRef(doc))) continue;
    const text = documentText(doc);
    if (!CONTRADICTION_RE.test(text) || !rule.re.test(text)) continue;
    const subject = exactSnippet(text, rule.re);
    if (subjects.some((x) => sameEventSubject(x, subject))) selected.push(doc);
  }
  return selected;
}

/** Corroboration is computed over publisher groups, not URLs or feed rows.
 * Discovery rows are visible but excluded from the independent confirmation
 * count. Contradictions remain first-class and lower confidence. */
function corroborateEvent(rawEvent, documents) {
  const docs = eventDocuments(rawEvent, documents);
  const bestByGroup = new Map();
  let discoveryLeads = 0, governmentPrimary = false, tierAPrimary = false;
  const contradictionDocs = docs.filter((d) => CONTRADICTION_RE.test(documentText(d)));
  const contradictionIds = new Set(contradictionDocs.map(docRef));
  for (const doc of docs) {
    if (isDiscovery(doc)) { discoveryLeads += 1; continue; }
    /* A denial/retraction is adverse evidence about the claim, never another
       supporting vote merely because it came from a new publisher. */
    if (contradictionIds.has(docRef(doc))) continue;
    const group = publisherGroup(doc);
    const prior = bestByGroup.get(group);
    if (!prior || reliability(doc) > reliability(prior)) bestByGroup.set(group, doc);
    if (sourceIsGovernment(doc)) governmentPrimary = true;
    if (doc.sourceTier === "A") tierAPrimary = true;
  }
  const independent = [...bestByGroup.values()];
  let miss = 1;
  for (const doc of independent) miss *= (1 - 0.72 * reliability(doc));
  let probabilityTrue = independent.length ? 1 - miss : Math.min(0.54, discoveryLeads * 0.12 + 0.30);
  const contradictionGroups = new Set(contradictionDocs.map(publisherGroup));
  const unresolvedContradiction = contradictionGroups.size > 0;
  if (unresolvedContradiction) probabilityTrue *= independent.length >= 3 ? 0.72 : 0.55;
  return {
    probabilityTrue: round(clamp(probabilityTrue, 0, 0.99)),
    independentGroups: independent.length,
    publisherGroups: independent.map(publisherGroup).sort(),
    discoveryLeads,
    governmentPrimary,
    tierAPrimary,
    unresolvedContradiction,
    contradictionGroups: [...contradictionGroups].sort(),
    sourceRefs: docs.map(docRef),
  };
}

function materiality(rawEvent, docs) {
  const text = docs.map((d) => `${d.title || ""} ${d.canonicalText || d.summary || ""}`).join(" ");
  const base = {
    regulatory_safety: 0.82, financial_guidance: 0.84, commercial_cancellation: 0.75,
    labor_operations: 0.70, production_supply: 0.69, legal_enforcement: 0.66,
    government_contract: 0.58, commercial_order: 0.56, financial_positive: 0.67,
    cyber_operational: 0.66, leadership_governance: 0.45, product_technology: 0.40,
  }[rawEvent.eventType] || 0.40;
  return clamp(base + (MATERIAL_RE.test(text) ? 0.12 : 0) + (docs.length >= 2 ? 0.04 : 0), 0, 0.95);
}

function calibratedSeverity(rawEvent, docs) {
  const rule = EVENT_RULES.find((x) => x.type === rawEvent.eventType);
  const base = rule ? rule.severity : 45;
  const text = docs.map(documentText).join(" ");
  /* Model-proposed magnitude is intentionally excluded. Only explicit text
     cues may move the preregistered event-class baseline. */
  return clamp(base + (MATERIAL_RE.test(text) ? 20 : 0), 0, 100);
}

/* Immediacy is the MINIMUM of what the language claims and what the clock
 * allows.
 *
 * Previously "imminent" and "near_term" returned a constant and never looked
 * at eventAtMs at all, so a six-month-old document containing the phrase
 * "effective immediately" scored the same immediacy as this morning's
 * filing — and since IMMINENT_RE fires on words as ordinary as "vote",
 * "deadline" and "scheduled", that unreachable clock branch applied to
 * exactly the events that scored highest. A lexical cue can now only ever
 * lower immediacy relative to elapsed time, never raise it. Future-dated
 * scheduled events keep full immediacy: their elapsed age is zero. */
function timingWeight(timing, eventAtMs, asOfMs) {
  const claimed = timing === "imminent" ? 1
    : timing === "near_term" ? 0.85
      : timing === "long_term" ? 0.35 : 0.62;
  const ageDays = Number.isFinite(eventAtMs)
    ? Math.max(0, (asOfMs - Number(eventAtMs)) / DAY_MS) : 30;
  const elapsed = ageDays <= 2 ? 1
    : ageDays <= 7 ? 0.82
      : ageDays <= 30 ? 0.62
        : ageDays <= 90 ? 0.40 : 0.24;
  return Math.min(claimed, elapsed);
}

function priceContext(dailyBars, asOfMs = Date.now(), eventAtMs = null) {
  const cutoffDay = new Date(asOfMs).toISOString().slice(0, 10);
  const rows = (dailyBars || []).filter((b) => b && b.date < cutoffDay && Number(b.c) > 0)
    .sort((a, b) => String(a.date).localeCompare(String(b.date))).slice(-132);
  if (rows.length < 20) return { ok: false, reason: "fewer than 20 completed daily bars", days: rows.length };
  const returns = [];
  for (let i = 1; i < rows.length; i += 1) returns.push(rows[i].c / rows[i - 1].c - 1);
  const mean = returns.reduce((s, x) => s + x, 0) / Math.max(1, returns.length);
  const variance = returns.reduce((s, x) => s + (x - mean) ** 2, 0) / Math.max(1, returns.length - 1);
  const dailyVol = Math.sqrt(Math.max(0, variance));
  const first = rows[0].c, last = rows[rows.length - 1].c;
  const out = {
    ok: true, days: rows.length, startDate: rows[0].date, endDate: rows[rows.length - 1].date,
    return6mPct: round((last / first - 1) * 100, 2),
    dailyVolPct: round(dailyVol * 100, 3),
    drawdownFromHighPct: round((last / Math.max(...rows.map((b) => b.c)) - 1) * 100, 2),
    eventResponse: null,
  };
  if (!Number.isFinite(Number(eventAtMs))) return out;
  const eventDay = new Date(Number(eventAtMs)).toISOString().slice(0, 10);
  const idx = rows.findIndex((b) => b.date >= eventDay);
  if (idx < 0) return out;
  const base = idx > 0 ? rows[idx - 1].c : rows[idx].o || rows[idx].c;
  const horizon = {};
  for (const days of [1, 5, 20]) {
    const j = Math.min(rows.length - 1, idx + days - 1);
    if (j >= idx && base > 0) horizon[`${days}dPct`] = round((rows[j].c / base - 1) * 100, 2);
  }
  const move = Math.abs(Number(horizon["5dPct"] ?? horizon["1dPct"] ?? 0) / 100);
  const volScale = dailyVol * Math.sqrt(horizon["5dPct"] != null ? 5 : 1);
  out.eventResponse = { eventDay, ...horizon,
    absorptionEstimate: round(clamp(volScale > 0 ? move / (3 * volScale) : 0, 0, 0.75)),
    note: "Heuristic price-response context; never proof that an event is fully priced." };
  return out;
}

function calibrateEvent(rawEvent, documents, price = null, asOfMs = Date.now()) {
  const docs = eventDocuments(rawEvent, documents);
  const claims = verifiedClaims(rawEvent, documents);
  const corr = corroborateEvent(rawEvent, documents);
  const eventAtMs = docs.map(eventTime).filter(Number.isFinite).sort((a, b) => b - a)[0] || null;
  const pMaterial = materiality(rawEvent, docs);
  const rule = EVENT_RULES.find((x) => x.type === rawEvent.eventType);
  const severity = calibratedSeverity(rawEvent, docs);
  const direction = rule ? rule.direction : clamp(Number(rawEvent.direction), -1, 1);
  const eventPrice = price && price.eventResponse ? price : null;
  const absorption = clamp(eventPrice && eventPrice.eventResponse.absorptionEstimate, 0, 0.75);
  const allText = docs.map(documentText).join(" ");
  const timing = IMMINENT_RE.test(allText) ? "imminent"
    : (rawEvent.timing === "near_term" ? "near_term" : (rawEvent.timing || "developing"));
  const immediacy = timingWeight(timing, eventAtMs, asOfMs);
  const impact = corr.probabilityTrue * pMaterial * (severity / 100) * immediacy * (1 - absorption);
  const sourceEligible = claims.length > 0 && corr.independentGroups > 0;
  return {
    eventId: sha(`${rawEvent.eventType}|${rawEvent.title}|${eventAtMs || "unknown"}`).slice(0, 24),
    eventType: rawEvent.eventType || "other",
    title: norm(rawEvent.title || rawEvent.eventType || "event hypothesis").slice(0, 240),
    direction: round(direction, 2),
    probabilityTrue: corr.probabilityTrue,
    probabilityMaterial: round(pMaterial),
    severity: round(severity, 1),
    timing, eventAtMs,
    duration: rawEvent.duration || "uncertain",
    marketAbsorption: round(absorption),
    impactScore: round(impact * 100, 1),
    adverseRiskScore: round(Math.max(0, -direction) * impact * 100, 1),
    opportunityScore: round(Math.max(0, direction) * impact * 100, 1),
    claims, corroboration: corr,
    evidenceEligible: sourceEligible,
    modelOrigin: rawEvent.modelOrigin || "deterministic",
  };
}

function sourceCoverage(profile, pollResults, { secHealthy = false, asOfMs = Date.now(), sourceRegistry = {} } = {}) {
  const byId = new Map((pollResults || []).map((r) => [r.sourceId, r]));
  const roles = {};
  for (const sourceId of profile.sourceIds || []) {
    const source = (profile.sourceRegistry || sourceRegistry)[sourceId];
    const result = byId.get(sourceId);
    const role = source && source.requiredRole;
    if (!role) continue;
    const row = { sourceId, healthy: result && result.healthy === true,
      deferred: result && result.deferred === true, error: result && result.error || null };
    if (!roles[role] || row.healthy) roles[role] = row;
  }
  if (secHealthy) roles.sec_filings = { sourceId: "sec.latest", healthy: true, deferred: false, error: null };
  const requiredRoles = [...new Set([...(profile.requiredRoles || []), "sec_filings"] )];
  const missingRoles = requiredRoles.filter((r) => !(roles[r] && roles[r].healthy));
  const deferredSources = (pollResults || []).filter((r) => r.deferred).map((r) => r.sourceId);
  return {
    monitored: profile.curationStatus !== "identifier_incomplete",
    curationStatus: profile.curationStatus,
    complete: missingRoles.length === 0,
    asOfMs, requiredRoles, missingRoles, roles,
    healthySources: (pollResults || []).filter((r) => r.healthy).map((r) => r.sourceId),
    failedSources: (pollResults || []).filter((r) => !r.healthy && !r.deferred).map((r) => r.sourceId),
    deferredSources,
    limitation: "Public-source coverage is observable and bounded; it is not a claim that every relevant fact on the internet was found.",
  };
}

function intelligenceSizeMultiplier(value) {
  const risk = clamp(value, 0, 100);
  const anchors = [[0, 1], [10, 0.85], [25, 0.60], [45, 0.25], [70, 0]];
  if (risk >= 70) return 0;
  for (let i = 1; i < anchors.length; i += 1) {
    const [x1, y1] = anchors[i - 1], [x2, y2] = anchors[i];
    if (risk <= x2) return y1 + ((risk - x1) / (x2 - x1)) * (y2 - y1);
  }
  return 0;
}

function decisionPolicy({ coverage, events, temporalContext = null, requireTemporalContext = false,
  asOfMs = Date.now(), maxAgeHours = DEFAULT_MAX_AGE_HOURS,
  temporalMaxAgeHours = maxAgeHours, decisionMatrixPolicy = null } = {}) {
  const matrix = decisionMatrixPolicy && typeof decisionMatrixPolicy === "object"
    ? decisionMatrixPolicy : {};
  const nonBlockingRiskFloor = clamp(matrix.nonBlockingRiskFloor,
    0, MAX_NON_BLOCKING_RISK_FLOOR);
  const intelligenceRiskScale = clamp(matrix.intelligenceRiskScale == null
    ? 1 : matrix.intelligenceRiskScale, 0.5, 2);
  const temporalRiskScale = clamp(matrix.temporalRiskScale == null
    ? 1 : matrix.temporalRiskScale, 0.5, 2);
  const signedAgeHours = coverage && Number.isFinite(Number(coverage.asOfMs))
    ? (asOfMs - Number(coverage.asOfMs)) / 3600e3 : Infinity;
  const futureDated = signedAgeHours < -(5 / 60);
  const ageHours = Math.max(0, signedAgeHours);
  const monitored = !!(coverage && coverage.monitored);
  const fresh = !futureDated && ageHours <= maxAgeHours;
  const complete = !!(coverage && coverage.complete);
  const adverse = (events || []).filter((e) => e.direction < 0 && e.evidenceEligible)
    .sort((a, b) => b.adverseRiskScore - a.adverseRiskScore);
  const pendingAdverse = (events || []).filter((e) => e.direction < 0 && !e.evidenceEligible
    && e.corroboration && e.corroboration.discoveryLeads > 0
    && Number(e.probabilityMaterial) >= 0.65 && Number(e.severity) >= 60)
    .sort((a, b) => b.impactScore - a.impactScore);
  const top = adverse[0] || null;
  const pendingLead = pendingAdverse[0] || null;
  const riskScore = top ? Number(top.adverseRiskScore) : 0;
  const decisionRiskScore = riskScore > 0 && riskScore < 70 && riskScore >= nonBlockingRiskFloor
    ? Math.min(69.999, riskScore * intelligenceRiskScale) : 0;
  let entryAllowed = monitored && fresh && complete;
  let sizeMultiplier = 1;
  const reasons = [];
  if (!monitored) reasons.push("company is not in the monitored intelligence set");
  if (futureDated) reasons.push("company intelligence timestamp is in the future");
  else if (!fresh) reasons.push("company intelligence is stale");
  if (!complete) reasons.push(`required public-source lanes incomplete${coverage && coverage.missingRoles && coverage.missingRoles.length ? `: ${coverage.missingRoles.join(", ")}` : ""}`);
  if (!entryAllowed) sizeMultiplier = 0;
  else if (pendingLead) {
    entryAllowed = false; sizeMultiplier = 0;
    reasons.push(`unresolved adverse discovery lead requires a direct source: ${pendingLead.title}`);
  }
  else if (riskScore >= 70) { entryAllowed = false; sizeMultiplier = 0; reasons.push(`adverse event risk ${riskScore}/100`); }
  else if (riskScore > 0) {
    sizeMultiplier = intelligenceSizeMultiplier(decisionRiskScore);
    reasons.push(decisionRiskScore > 0
      ? `adverse event risk ${riskScore}/100 (decision weight ${round(decisionRiskScore, 1)})`
      : `adverse event risk ${riskScore}/100 is below the frozen non-blocking materiality threshold`);
  }

  const rawTemporalPolicy = temporalContext
    ? T.temporalPolicy(temporalContext, { maxAgeHours: temporalMaxAgeHours, asOfMs })
    : { monitored: false, fresh: false, complete: false,
      entryAllowed: !requireTemporalContext, sizeMultiplier: requireTemporalContext ? 0 : 1,
      riskScore: 0, reasons: ["temporal context unavailable"], positiveRiskIncreaseAllowed: false };
  const temporalRawRisk = Number(rawTemporalPolicy.riskScore) || 0;
  const temporalDecisionRisk = temporalRawRisk > 0 && temporalRawRisk < 70
    && temporalRawRisk >= nonBlockingRiskFloor
    ? Math.min(69.999, temporalRawRisk * temporalRiskScale) : 0;
  const temporalPolicy = {
    ...rawTemporalPolicy,
    rawRiskScore: round(temporalRawRisk, 1),
    decisionRiskScore: round(temporalDecisionRisk, 1),
    sizeMultiplier: rawTemporalPolicy.entryAllowed
      ? round(T.sizeMultiplierForRisk(temporalDecisionRisk))
      : 0,
  };
  if (requireTemporalContext && !temporalPolicy.entryAllowed) {
    entryAllowed = false; sizeMultiplier = 0;
  } else sizeMultiplier = Math.min(sizeMultiplier, Number(temporalPolicy.sizeMultiplier));
  if (temporalPolicy.reasons && (temporalPolicy.riskScore > 0 || !temporalPolicy.entryAllowed))
    reasons.push(...temporalPolicy.reasons.map((x) => `temporal: ${x}`));

  const critical = monitored && fresh && complete ? adverse.find((e) => e.adverseRiskScore >= 75
    && e.probabilityTrue >= 0.85 && e.probabilityMaterial >= 0.70
    && !e.corroboration.unresolvedContradiction
    && e.corroboration.independentGroups >= 2
    && (e.corroboration.governmentPrimary
      || (e.corroboration.tierAPrimary && e.corroboration.independentGroups >= 3))) || null : null;
  return {
    monitored, fresh, complete, ageHours: Number.isFinite(ageHours) ? round(ageHours, 2) : null,
    entryAllowed, sizeMultiplier: round(clamp(sizeMultiplier)),
    adverseRiskScore: round(riskScore, 1),
    decisionRiskScore: round(decisionRiskScore, 1),
    decisionMatrixPolicy: {
      intelligenceRiskScale: round(intelligenceRiskScale, 3),
      temporalRiskScale: round(temporalRiskScale, 3),
      nonBlockingRiskFloor: round(nonBlockingRiskFloor, 3),
      hardBlocksInvariant: true,
    },
    reasons: reasons.length ? reasons : ["no material adverse public-source event detected in covered lanes"],
    exitReview: !!critical,
    criticalExit: !!critical,
    exitEventId: critical && critical.eventId || null,
    exitReason: critical ? `corroborated material downside: ${critical.title}` : null,
    pendingAdverseLead: !!pendingLead,
    pendingLeadEventId: pendingLead && pendingLead.eventId || null,
    temporalPolicy,
    positiveRiskIncreaseAllowed: false,
  };
}

function buildSnapshot({ profile, documents = [], dailyBars = [], rawEvents = null,
  coverage, temporalInputs = {}, requireTemporalContext = true,
  asOfMs = Date.now(), maxAgeHours = DEFAULT_MAX_AGE_HOURS,
  temporalMaxAgeHours = maxAgeHours, minSeasonalityDays = T.MIN_SEASONAL_DAYS } = {}) {
  if (!profile || !profile.symbol) throw new Error("company profile required");
  const docs = deduplicateDocuments(documents, asOfMs);
  const initialHypotheses = Array.isArray(rawEvents) && rawEvents.length ? rawEvents : extractEvents(docs, asOfMs);
  const hypotheses = initialHypotheses.flatMap((raw) => splitEventHypothesis(raw, docs));
  const events = hypotheses.slice(0, 12).map((raw) => {
    const ds = eventDocuments(raw, docs);
    const at = ds.map(eventTime).filter(Number.isFinite).sort((a, b) => b - a)[0] || null;
    return calibrateEvent(raw, docs, priceContext(dailyBars, asOfMs, at), asOfMs);
  }).filter((e) => e.claims.length > 0 || e.corroboration.discoveryLeads > 0)
    .sort((a, b) => b.impactScore - a.impactScore);
  const temporalContext = T.buildTemporalContext({ profile, documents: docs, dailyBars,
    ...temporalInputs, asOfMs, maxAgeHours: temporalMaxAgeHours, minSeasonalityDays });
  const policy = decisionPolicy({ coverage, events, temporalContext,
    requireTemporalContext, asOfMs, maxAgeHours, temporalMaxAgeHours });
  const dossierHash = sha(JSON.stringify({ symbol: profile.symbol, asOfMs,
    docs: docs.map((d) => [docRef(d), d.canonicalContentSha256 || null, d.decisionKnownAtMs || null]),
    coverage, events, temporalHash: temporalContext.contextHash }));
  return {
    schemaVersion: "company-intelligence-v2", symbol: profile.symbol,
    companyName: profile.companyName, sector: profile.sector || null,
    sectorPack: profile.sectorPack || "general", curationStatus: profile.curationStatus,
    asOf: new Date(asOfMs).toISOString(), asOfMs, lookbackDays: LOOKBACK_DAYS,
    documentCount: docs.length, dossierHash,
    coverage, events, temporalContext, policy,
    priceContext: priceContext(dailyBars, asOfMs),
    limitations: [
      "Public sources can be delayed, incomplete, wrong, or unavailable.",
      "Probabilities are conservative decision heuristics until externally calibrated; they are not statistical guarantees.",
      "Price response is context, not proof that information is fully reflected.",
      "Temporal factors can only block or reduce new risk until forward calibration proves incremental net value.",
    ],
  };
}

async function storeSnapshot(snapshot) {
  if (!snapshot || !/^[A-Z][A-Z0-9.-]{0,9}$/.test(String(snapshot.symbol || ""))) {
    throw new Error("valid snapshot symbol required");
  }
  await A.col(A.COL.intelligence).doc(snapshot.symbol).set({ ...snapshot,
    updated_at: A.FV.serverTimestamp(), ...A.envelope({ created_by: "intelligence.storeSnapshot" }) });
  for (const event of (snapshot.events || []).slice(0, 12)) {
    await A.col(A.COL.events).doc(`${snapshot.symbol}_${event.eventId}`).set({
      symbol: snapshot.symbol, dossierHash: snapshot.dossierHash, asOfMs: snapshot.asOfMs,
      ...event, updated_at: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "intelligence.storeSnapshot" }),
    }, { merge: true });
  }
  return snapshot;
}

async function readSnapshot(symbol) {
  const snap = await A.col(A.COL.intelligence).doc(String(symbol || "").toUpperCase()).get();
  return snap.exists ? snap.data() : null;
}

module.exports = {
  DAY_MS, LOOKBACK_DAYS, DEFAULT_MAX_AGE_HOURS, EVENT_RULES,
  clamp, eventTime, docRef, publisherGroup, reliability, isDiscovery,
  deduplicateDocuments, extractEvents, verifiedClaims, corroborateEvent,
  eventTokens, sameEventSubject, batchCommonTokens, clusterEventDocuments, timingWeight,
  eventDocuments, splitEventHypothesis,
  mergeEventHypotheses, priceContext, calibrateEvent, sourceCoverage,
  intelligenceSizeMultiplier, decisionPolicy, buildSnapshot,
  storeSnapshot, readSnapshot,
};
