/* Investor_AI — deterministic point-in-time temporal context.
 *
 * Language-model extraction may identify a quote-grounded exposure, but it
 * cannot set a trading weight. This module owns calendar distance, seasonal
 * estimates, active-hazard matching, rolling driver correlation and the final
 * risk-only policy. Positive temporal evidence never increases base size.
 */
"use strict";

const crypto = require("crypto");

const DAY_MS = 864e5;
const MIN_SEASONAL_DAYS = 756; // three trading years; shorter samples abstain
/* Same-calendar-month observations available from retained history. Daily
   retention is KEEP_DAYS trading days (~252/year), and the current month is
   never complete, so the ceiling is floor(KEEP_DAYS/252) - 1. Seasonality is
   intentionally context-only at the default retention: four annual points
   are useful as a warning to investigate, but not enough evidence to alter a
   position. A deployment may retain a longer point-in-time history, but the
   decision gate still requires eight completed years and a one-sided upper
   confidence bound below zero. */
function maxSeasonalSamples(keepDays) {
  return Math.max(0, Math.floor(Number(keepDays) / 252) - 1);
}
const MIN_SEASONAL_DECISION_SAMPLES = 8;
const MAX_SERIES_AGE_DAYS = 8; // weekends/holidays allowed; old histories are not "current"
const MIN_DRIVER_OBSERVATIONS = 126;
const PRIOR_EXPOSURE_TTL_DAYS = 45;
const MAX_HAZARD_SOURCE_AGE_MS = 2 * 3600e3;
const STATE_GEOGRAPHIC_WEIGHT = 0.7;
const DRIVER_BY_SECTOR = Object.freeze({
  semi: "SMH", hw: "XLK", sw: "XLK", plat: "XLK",
  health: "XLV", pharma: "XLV", energy: "XLE", power: "XLU",
  mat: "XLB", indus: "XLI", fin: "XLF", cons: "XLY", other: "SPY",
});
const DRIVER_KEYWORDS = Object.freeze([
  { re: /\b(crude|oil|petroleum|gasoline|diesel)\b/i, symbol: "USO", name: "crude oil" },
  { re: /\b(natural gas|lng|henry hub)\b/i, symbol: "UNG", name: "natural gas" },
  { re: /\b(gold|precious metal)\b/i, symbol: "GLD", name: "gold" },
  { re: /\b(interest rate|treasury|bond yield|duration)\b/i, symbol: "TLT", name: "long rates" },
  { re: /\b(dollar|foreign exchange|currency|fx)\b/i, symbol: "UUP", name: "U.S. dollar" },
]);
const MONTHS = Object.freeze({ jan:0, january:0, feb:1, february:1, mar:2, march:2,
  apr:3, april:3, may:4, jun:5, june:5, jul:6, july:6, aug:7, august:7,
  sep:8, sept:8, september:8, oct:9, october:9, nov:10, november:10,
  dec:11, december:11 });
const MONTH_PATTERNS = Object.freeze([
  /\bjan(?:uary)?\b/i, /\bfeb(?:ruary)?\b/i, /\bmar(?:ch)?\b/i,
  /\bapr(?:il)?\b/i, /\bmay\b/i, /\bjun(?:e)?\b/i,
  /\bjul(?:y)?\b/i, /\baug(?:ust)?\b/i, /\bsep(?:t(?:ember)?)?\b/i,
  /\boct(?:ober)?\b/i, /\bnov(?:ember)?\b/i, /\bdec(?:ember)?\b/i,
]);
const US_STATE_NAMES = Object.freeze({
  AL:"Alabama", AK:"Alaska", AZ:"Arizona", AR:"Arkansas", CA:"California", CO:"Colorado",
  CT:"Connecticut", DE:"Delaware", FL:"Florida", GA:"Georgia", HI:"Hawaii", ID:"Idaho",
  IL:"Illinois", IN:"Indiana", IA:"Iowa", KS:"Kansas", KY:"Kentucky", LA:"Louisiana",
  ME:"Maine", MD:"Maryland", MA:"Massachusetts", MI:"Michigan", MN:"Minnesota",
  MS:"Mississippi", MO:"Missouri", MT:"Montana", NE:"Nebraska", NV:"Nevada",
  NH:"New Hampshire", NJ:"New Jersey", NM:"New Mexico", NY:"New York",
  NC:"North Carolina", ND:"North Dakota", OH:"Ohio", OK:"Oklahoma", OR:"Oregon",
  PA:"Pennsylvania", RI:"Rhode Island", SC:"South Carolina", SD:"South Dakota",
  TN:"Tennessee", TX:"Texas", UT:"Utah", VT:"Vermont", VA:"Virginia",
  WA:"Washington", WV:"West Virginia", WI:"Wisconsin", WY:"Wyoming",
  DC:"District of Columbia", PR:"Puerto Rico", VI:"U.S. Virgin Islands", GU:"Guam",
});
const EVENT_RE = /\b(earnings|quarterly results?|financial results?|investor day|analyst day|trial readout|data readout|union vote|strike vote|hearing|meeting|deadline|expiration|conference|product launch|regulatory decision|pdufa)\b/i;
const TENTATIVE_RE = /\b(may|might|could|expects?|expected|anticipates?|anticipated|projects?|projected|approximately|tentative|preliminar(?:y|ily)|provisional(?:ly)?|indicative|targets?|aims?|intends?|seeks?|proposes?|planned|plans? to|subject to)\b/i;
const CONFIRMED_DATE_RE = /\b(?:will|shall)\b|\bscheduled\b|\bconfirmed\b|\b(?:set|fixed)\s+for\b|\bannounced\s+(?:the\s+)?(?:date\s+)?(?:for|on)\b|\bdeadline\s+(?:is|falls on)\b|\bexpires?\s+on\b|\bdue\s+on\b/i;
const DATE_RE = /\b(?:20\d{2}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}\/20\d{2}|(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s+\d{1,2}(?:st|nd|rd|th)?(?:,?\s+20\d{2})?)\b/gi;
const HURRICANE_STATES = new Set(["AL","FL","GA","LA","MS","NC","SC","TX","VA"]);
const WINTER_STATES = new Set(["AK","CO","CT","IA","ID","IL","IN","MA","ME","MI","MN","MT","ND","NE","NH","NJ","NY","OH","PA","RI","SD","UT","VT","WI","WY"]);
const TEMPORAL_WEIGHTS = Object.freeze({
  schedule: Object.freeze({ earnings: 75, regulatory_deadline: 65, scheduled_event: 55,
    customer_budget_cycle: 34, supply_cycle: 38, seasonal_demand: 28, weather: 38, geography: 24 }),
  recurring: Object.freeze({ weather: 14, geography: 8, seasonal_demand: 24,
    customer_budget_cycle: 22, supply_cycle: 26, commodity: 12, regulatory_deadline: 18,
    scheduled_event: 16 }),
  combination: Object.freeze([1, 0.35, 0.20, 0.10, 0.05]),
});
const RISK_SIZE_ANCHORS = Object.freeze([
  Object.freeze([0, 1]), Object.freeze([10, 0.9]), Object.freeze([20, 0.75]),
  Object.freeze([35, 0.5]), Object.freeze([50, 0.25]), Object.freeze([70, 0]),
]);

function clamp(v, lo = 0, hi = 1) { const n = Number(v); return Number.isFinite(n) ? Math.max(lo, Math.min(hi, n)) : lo; }
function round(v, p = 3) { const x = 10 ** p; return Number((Math.round(Number(v) * x) / x).toFixed(p)); }
function mean(a) { return a.length ? a.reduce((s, x) => s + x, 0) / a.length : 0; }
function stdev(a) { if (a.length < 2) return 0; const m = mean(a); return Math.sqrt(a.reduce((s, x) => s + (x - m) ** 2, 0) / (a.length - 1)); }
function median(a) { if (!a.length) return 0; const s = [...a].sort((x, y) => x - y), m = Math.floor(s.length / 2); return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; }
function sha(v) { return crypto.createHash("sha256").update(String(v)).digest("hex"); }
function norm(v) { return String(v || "").replace(/\s+/g, " ").trim(); }
function docRef(d) { return String(d && (d.documentId || d.versionId) || ""); }
function docText(d) { return norm(`${d && d.title || ""} ${d && (d.canonicalText || d.summary) || ""}`); }
function sourceReliability(d) {
  const n = Number(d && d.sourceReliability);
  if (Number.isFinite(n)) return clamp(n, 0.1, 0.99);
  return d && d.sourceTier === "A" ? 0.88 : d && d.sourceTier === "B" ? 0.72 : 0.52;
}
function asOfDay(asOfMs) { return new Date(asOfMs).toISOString().slice(0, 10); }
function wordRe(value) { return new RegExp(`\\b${String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i"); }
function knownAt(doc) {
  const n = Number(doc && (doc.decisionKnownAtMs || doc.firstSeenAtMs));
  return Number.isFinite(n) ? n : null;
}
function dateAgeDays(date, asOfMs) {
  const t = Date.parse(`${date}T00:00:00Z`);
  return Number.isFinite(t) ? Math.max(0, (Date.parse(`${asOfDay(asOfMs)}T00:00:00Z`) - t) / DAY_MS) : Infinity;
}

function monthMentionSupported(quote, month, dates = []) {
  const n = Number(month);
  if (!Number.isInteger(n) || n < 1 || n > 12) return false;
  if ((dates || []).some((d) => Number(String(d).slice(5, 7)) === n)) return true;
  const raw = String(quote || ""), re = new RegExp(MONTH_PATTERNS[n - 1].source, "gi");
  const matches = [...raw.matchAll(re)];
  if (![3, 5].includes(n)) return matches.length > 0;
  /* March and May are also ordinary words. Require a nearby calendar marker
     rather than accepting a modal ("may reduce") or a verb ("march toward"). */
  return matches.some((m) => {
    const before = raw.slice(Math.max(0, m.index - 60), m.index).toLowerCase();
    const after = raw.slice(m.index + m[0].length, m.index + m[0].length + 70).toLowerCase();
    return /(?:\b(?:in|during|throughout|from|by|until|since|for|beginning|ending|through)\s+(?:the\s+)?|\bmonth\s+of\s+)$/.test(before)
      || /^\s*(?:20\d{2}\b|quarter\b|month\b|period\b|season\b|sales\b|demand\b|production\b|deliver(?:y|ies)\b|revenue\b|fiscal\b|calendar\b|weather\b|operations?\b)/.test(after);
  });
}

function stateMentionSupported(quote, code) {
  const name = US_STATE_NAMES[code];
  if (!name) return false;
  let raw = String(quote || "");
  if (code === "WA") raw = raw.replace(/\bWashington\s*,?\s*D\.?\s*C\.?\b/gi, " ");
  if (code === "DC" && /\b(?:District of Columbia|Washington\s*,?\s*D\.?\s*C\.?)\b/i.test(raw)) return true;
  if (wordRe(name).test(raw)) return true;
  for (const m of raw.matchAll(new RegExp("\\b" + code + "\\b", "g"))) {
    const before = raw.slice(Math.max(0, m.index - 70), m.index).toLowerCase();
    const after = raw.slice(m.index + code.length, m.index + code.length + 28).toLowerCase();
    if (/(?:\b(?:in|within|across|throughout|near|from|to|of)\s+(?:the\s+state\s+of\s+)?|\bstate\s+of\s+|,\s*)$/.test(before)
        || /^\s*(?:-based\b|\d{5}(?:-\d{4})?\b)/.test(after)) return true;
  }
  return false;
}

function parseDatePhrase(value, asOfMs) {
  const raw = norm(value);
  if (/^20\d{2}-\d{2}-\d{2}$/.test(raw)) {
    const t = Date.parse(`${raw}T12:00:00Z`), d = new Date(t);
    return Number.isFinite(t) && d.toISOString().slice(0, 10) === raw ? raw : null;
  }
  const numeric = raw.match(/^(\d{1,2})\/(\d{1,2})\/(20\d{2})$/);
  if (numeric) {
    const t = Date.UTC(Number(numeric[3]), Number(numeric[1]) - 1, Number(numeric[2])), d = new Date(t);
    return d.getUTCFullYear() === Number(numeric[3]) && d.getUTCMonth() === Number(numeric[1]) - 1
      && d.getUTCDate() === Number(numeric[2]) ? d.toISOString().slice(0, 10) : null;
  }
  const m = raw.match(/^([A-Za-z]+)\.?\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s+(20\d{2}))?$/i);
  if (!m || MONTHS[m[1].toLowerCase()] == null) return null;
  let year = Number(m[3]) || new Date(asOfMs).getUTCFullYear();
  let t = Date.UTC(year, MONTHS[m[1].toLowerCase()], Number(m[2]));
  if (!m[3] && t < asOfMs - 30 * DAY_MS) { year += 1; t = Date.UTC(year, MONTHS[m[1].toLowerCase()], Number(m[2])); }
  const d = new Date(t);
  return d.getUTCMonth() === MONTHS[m[1].toLowerCase()] && d.getUTCDate() === Number(m[2])
    ? d.toISOString().slice(0, 10) : null;
}

function normalizeEventType(value) {
  const v = norm(value).toLowerCase().replace(/\s+/g, "_");
  if (/earnings|quarterly_results?|financial_results?/.test(v)) return "earnings";
  if (/pdufa|regulatory_decision|deadline|expiration/.test(v)) return "regulatory_deadline";
  return v || "scheduled_event";
}

function isDiscoveryDocument(d) {
  return d && (d.discoveryOnly === true || d.sourceClass === "discovery_index");
}

function hasTentativeLanguage(value) {
  // “May” is both a modal and a month. Remove explicit date spans before the
  // case-insensitive modal test so a confirmed May 20 date is not haircutted.
  return TENTATIVE_RE.test(String(value || "").replace(new RegExp(DATE_RE.source, "gi"), " "));
}

function clauseAround(text, index, length = 0) {
  const raw = String(text || ""), at = Math.max(0, Number(index) || 0);
  const before = raw.slice(0, at);
  const prior = Math.max(before.lastIndexOf("."), before.lastIndexOf("!"), before.lastIndexOf("?"),
    before.lastIndexOf(";"));
  const after = raw.slice(at + length), nextRel = after.search(/[.!?;]/);
  const end = nextRel >= 0 ? at + length + nextRel + 1 : Math.min(raw.length, at + length + 220);
  return norm(raw.slice(Math.max(0, prior + 1), end));
}

function contextForDate(text, targetDate, asOfMs) {
  for (const match of String(text || "").matchAll(new RegExp(DATE_RE.source, "gi"))) {
    if (parseDatePhrase(match[0], asOfMs) === targetDate)
      return clauseAround(text, match.index, match[0].length);
  }
  return "";
}

function validSeriesProvenance(value) {
  return !!(value && value.provider && value.adjustment
    && value.homogeneous === true
    && /^[a-f0-9]{64}$/.test(String(value.sourceSha256 || "")));
}

function validNwsResponse(value, asOfMs = Date.now(), requiredState = null) {
  if (!value || !/^[a-f0-9]{64}$/.test(String(value.responseSha256 || ""))) return false;
  if (requiredState && String(value.state || "").toUpperCase() !== String(requiredState).toUpperCase()) return false;
  const fetchedAtMs = Date.parse(value.fetchedAt || "");
  if (!Number.isFinite(fetchedAtMs) || fetchedAtMs > asOfMs + 5 * 60e3) return false;
  return asOfMs - fetchedAtMs <= MAX_HAZARD_SOURCE_AGE_MS;
}

function validNwsHazard(value, responses = [], asOfMs = Date.now()) {
  if (!validNwsResponse(value, asOfMs)) return false;
  const effectiveMs = Date.parse(value.effective || ""), expiresMs = Date.parse(value.expires || "");
  if (!Number.isFinite(effectiveMs) || !Number.isFinite(expiresMs)) return false;
  if (effectiveMs > asOfMs + 3600e3 || expiresMs < asOfMs - 5 * 60e3) return false;
  const hash = String(value.responseSha256 || ""), fetchedAt = Date.parse(value.fetchedAt || "");
  return (value.states || []).some((state) => (responses || []).some((row) =>
    validNwsResponse(row, asOfMs, state)
    && String(row.responseSha256 || "") === hash
    && Date.parse(row.fetchedAt || "") === fetchedAt));
}

function extractScheduledEvents(documents, asOfMs = Date.now()) {
  const out = [];
  for (const d of documents || []) {
    const decisionKnownAtMs = knownAt(d);
    if (decisionKnownAtMs != null && decisionKnownAtMs > asOfMs) continue;
    const text = docText(d);
    if (!EVENT_RE.test(text)) continue;
    for (const match of text.matchAll(DATE_RE)) {
      const date = parseDatePhrase(match[0], asOfMs);
      if (!date) continue;
      const at = Date.parse(`${date}T12:00:00Z`);
      if (at < asOfMs - 14 * DAY_MS || at > asOfMs + 550 * DAY_MS) continue;
      const dateClause = clauseAround(text, match.index, match[0].length);
      if (!EVENT_RE.test(dateClause)) continue;
      const start = Math.max(0, match.index - 150), quote = text.slice(start, start + 360);
      const eventTerm = dateClause.match(EVENT_RE);
      const discoveryLead = isDiscoveryDocument(d);
      const tentativeLanguage = hasTentativeLanguage(dateClause);
      const explicitlyConfirmed = CONFIRMED_DATE_RE.test(dateClause) && !tentativeLanguage;
      const estimated = discoveryLead || !explicitlyConfirmed;
      out.push({ eventId: sha(`${docRef(d)}|${date}|${eventTerm && eventTerm[0]}`).slice(0, 20),
        type: normalizeEventType(eventTerm && eventTerm[0]), date,
        estimated, explicitlyConfirmed,
        confirmation: discoveryLead ? "discovery_lead" : (estimated ? "estimated" : "confirmed"),
        decisionEligible: decisionKnownAtMs != null && !discoveryLead && sourceReliability(d) >= 0.65,
        title: norm(d.title || quote).slice(0, 220), quote,
        documentRef: docRef(d), sourceTier: d.sourceTier || null,
        sourceReliability: sourceReliability(d), decisionKnownAtMs });
    }
  }
  const byKey = new Map();
  for (const e of out) {
    const key = `${e.type}|${e.date}`;
    const prior = byKey.get(key);
    const quality = (x) => (x.decisionEligible === false ? 0 : 100)
      + 10 * Number(x.sourceReliability || 0) + (x.explicitlyConfirmed ? 1 : 0);
    if (!prior || quality(e) > quality(prior)) byKey.set(key, e);
  }
  return [...byKey.values()].sort((a, b) => a.date.localeCompare(b.date)).slice(0, 24);
}

function supportedStates(rawStates, quote) {
  return [...new Set((rawStates || []).map((s) => String(s).trim().toUpperCase()))]
    .filter((code) => stateMentionSupported(quote, code))
    .slice(0, 20);
}

function quoteDates(quote, asOfMs) {
  return [...String(quote || "").matchAll(DATE_RE)].map((m) => parseDatePhrase(m[0], asOfMs)).filter(Boolean);
}

function supportedMonths(rawMonths, quote, dates) {
  return [...new Set((rawMonths || []).map(Number))].filter((n) => Number.isInteger(n) && n >= 1 && n <= 12
    && monthMentionSupported(quote, n, dates)).sort((a, b) => a - b);
}

function groundedDirection(requested, quote) {
  const negative = /\b(adverse(?:ly)?|negative|risk|exposed|disrupt(?:ion|ed)?|reduce[sd]?|lower[sd]?|declin(?:e|ed)|increase[sd]? (?:our )?(?:costs?|expenses?)|pressure (?:on )?(?:margin|profit))\b/i.test(quote);
  const positive = /\b(benefit(?:s|ed)?|positive|favourable|favorable|increase[sd]? (?:our )?(?:revenue|profit|margin|demand)|improve[sd]? (?:margin|profit))\b/i.test(quote);
  if (requested === "mixed" && negative && positive) return "mixed";
  if (requested === "negative" && negative) return "negative";
  if (requested === "positive" && positive) return "positive";
  return "unknown";
}

function groundedName(value, exposureType, quote) {
  const name = norm(value).slice(0, 160);
  const terms = name.toLowerCase().split(/[^a-z0-9]+/).filter((x) => x.length >= 4);
  const q = quote.toLowerCase();
  const overlap = terms.length ? terms.filter((x) => q.includes(x)).length / terms.length : 0;
  return overlap >= 0.5 ? name : `${String(exposureType || "temporal").replace(/_/g, " ")} exposure`;
}

function operationalMateriality(quote) {
  if (/\b(sole|only|principal|primary|largest|critical|material)\b/i.test(quote)
      && /\b(factory|plant|facility|distribution cent(?:er|re)|warehouse|headquarters|operations?)\b/i.test(quote)) return 1;
  if (/\b(factory|plant|manufactur|distribution cent(?:er|re)|warehouse|refinery|mine|port)\b/i.test(quote)) return 0.78;
  if (/\b(office|sales office|representative office)\b/i.test(quote)) return 0.35;
  return 0.55;
}

function exposureTypeSupported(type, quote) {
  const rules = {
    geography: /\b(operations?|facility|plant|factory|office|distribution|located|based|headquarters|geograph|region|state)\b/i,
    weather: /\b(weather|hurricanes?|storms?|floods?|wildfires?|droughts?|heat|freez(?:e|ing)|tornado(?:es)?|cyclones?|typhoons?)\b/i,
    commodity: /\b(crude|oil|petroleum|gasoline|diesel|natural gas|lng|gold|precious metal|commodity|raw material|interest rate|treasury|bond yield|currency|foreign exchange|fx)\b/i,
    seasonal_demand: /\b(seasonal|seasonality|winter|spring|summer|fall|autumn|holiday|quarter|month|annual cycle)\b/i,
    scheduled_event: EVENT_RE,
    regulatory_deadline: /\b(regulatory|regulator|deadline|expiration|pdufa|approval date|decision date)\b/i,
    customer_budget_cycle: /\b(customer|budget|fiscal year|procurement|appropriation|spending cycle)\b/i,
    supply_cycle: /\b(supply|inventory|production cycle|harvest|capacity cycle|lead times?)\b/i,
  };
  return !!(rules[type] && rules[type].test(quote));
}

function normalizeExposures(raw, documents, asOfMs = Date.now()) {
  const docs = new Map((documents || []).filter((d) => knownAt(d) != null && knownAt(d) <= asOfMs)
    .map((d) => [docRef(d), d]));
  const allowed = new Set(["geography", "weather", "commodity", "seasonal_demand",
    "scheduled_event", "regulatory_deadline", "customer_budget_cycle", "supply_cycle"]);
  const out = [];
  for (const x of raw || []) {
    const proposedPriorAt = x && /^temporal-grounding-v[23]$/.test(String(x.groundingVersion || ""))
      ? Number(x.lastVerifiedAtMs) : null;
    const trustedPriorAt = Number.isFinite(proposedPriorAt) && proposedPriorAt <= asOfMs + 5 * 60e3
      ? proposedPriorAt : null;
    if (Number.isFinite(trustedPriorAt) && asOfMs - trustedPriorAt > PRIOR_EXPOSURE_TTL_DAYS * DAY_MS) continue;
    const support = x && x.support || {}, d = docs.get(String(support.documentRef || ""));
    const quote = norm(support.quote), text = docText(d);
    if (!d || !allowed.has(x.exposureType) || quote.length < 12
        || !text.toLowerCase().includes(quote.toLowerCase())
        || !exposureTypeSupported(x.exposureType, quote)) continue;
    const dates = quoteDates(quote, asOfMs);
    const states = supportedStates(x.states, quote);
    const months = supportedMonths(x.months, quote, dates);
    const proposedDate = parseDatePhrase(x.scheduledDate, asOfMs);
    let scheduledDate = proposedDate && dates.includes(proposedDate) ? proposedDate : null;
    let scheduledContext = scheduledDate ? contextForDate(quote, scheduledDate, asOfMs) : "";
    if (scheduledDate && !exposureTypeSupported(x.exposureType, scheduledContext)) {
      scheduledDate = null; scheduledContext = "";
    }
    const driver = DRIVER_KEYWORDS.find((k) => k.re.test(quote));
    const direction = groundedDirection(x.directionWhenDriverRises, quote);
    const name = groundedName(x.name, x.exposureType, quote);
    const discoveryLead = isDiscoveryDocument(d);
    const tentativeLanguage = hasTentativeLanguage(scheduledDate ? scheduledContext : quote);
    const explicitlyConfirmed = !!scheduledDate && CONFIRMED_DATE_RE.test(scheduledContext) && !tentativeLanguage;
    const estimated = scheduledDate ? !explicitlyConfirmed : tentativeLanguage;
    const decisionEligible = !discoveryLead && sourceReliability(d) >= 0.65;
    out.push({ exposureId: sha(`${x.exposureType}|${support.documentRef}|${quote}|${states.join(",")}|${scheduledDate || ""}`).slice(0, 20),
      exposureType: x.exposureType, name,
      directionWhenDriverRises: direction,
      states, months, scheduledDate,
      estimated, explicitlyConfirmed,
      confirmation: discoveryLead ? "discovery_lead" : (estimated ? "estimated" : "confirmed"),
      decisionEligible,
      support: { claim: norm(support.claim).slice(0, 300), quote: quote.slice(0, 500), documentRef: docRef(d) },
      sourceTier: d.sourceTier || null, sourceClass: d.sourceClass || null,
      evidenceReliability: round(sourceReliability(d)), driverSymbol: driver && driver.symbol || null,
      driverName: driver && driver.name || null,
      operationalMateriality: operationalMateriality(quote),
      groundingVersion: "temporal-grounding-v3",
      lastVerifiedAtMs: Number.isFinite(trustedPriorAt) ? trustedPriorAt : asOfMs,
      inventoryOrigin: Number.isFinite(trustedPriorAt) ? "verified_prior_within_ttl" : "current_refresh",
      grounding: { rejectedStates: (x.states || []).map((s) => String(s).toUpperCase()).filter((s) => !states.includes(s)),
        rejectedMonths: (x.months || []).map(Number).filter((m) => !months.includes(m)),
        rejectedScheduledDate: proposedDate && !scheduledDate ? proposedDate : null,
        directionGrounded: direction !== "unknown" } });
  }
  const deduped = new Map();
  for (const x of out) {
    const key = `${x.exposureType}|${x.driverSymbol || ""}|${x.states.join(",")}|${x.scheduledDate || ""}|${x.support.documentRef}`;
    const prior = deduped.get(key);
    if (!prior || x.evidenceReliability > prior.evidenceReliability) deduped.set(key, x);
  }
  return [...deduped.values()].sort((a, b) => {
    const priority = (x) => (x.scheduledDate ? 30 : 0) + (x.states.length ? 20 : 0)
      + (x.driverSymbol ? 15 : 0) + ((x.months || []).includes(new Date(asOfMs).getUTCMonth() + 1) ? 10 : 0)
      + Number(x.evidenceReliability || 0);
    return priority(b) - priority(a) || b.lastVerifiedAtMs - a.lastVerifiedAtMs;
  }).slice(0, 32);
}

function extractDeterministicExposures(documents, asOfMs = Date.now()) {
  const out = [];
  for (const d of documents || []) {
    if (knownAt(d) == null || knownAt(d) > asOfMs) continue;
    const text = docText(d).slice(0, 200000);
    const sentences = text.match(/[^.!?]{20,700}[.!?]?/g) || [];
    for (const quote of sentences) {
      const states = Object.keys(US_STATE_NAMES).filter((code) => stateMentionSupported(quote, code));
      const dates = quoteDates(quote, asOfMs);
      const months = MONTH_PATTERNS.map((_, i) => monthMentionSupported(quote, i + 1, dates) ? i + 1 : null).filter(Boolean);
      const support = { claim: norm(quote).slice(0, 300), quote: norm(quote).slice(0, 500), documentRef: docRef(d) };
      const directionWhenDriverRises = groundedDirection("negative", quote) === "negative" ? "negative"
        : groundedDirection("positive", quote) === "positive" ? "positive" : "unknown";
      if (states.length && /\b(operations?|facility|plant|factory|office|distribution|located|based|headquarters|warehouse|refinery|mine|port)\b/i.test(quote)) {
        out.push({ exposureType: "geography", name: `${states.join("/")} operating geography`,
          directionWhenDriverRises: "unknown", states, months: [], scheduledDate: "", support });
      }
      if (/\b(weather|hurricanes?|storms?|floods?|wildfires?|droughts?|heat|freez(?:e|ing)|tornado(?:es)?|cyclones?|typhoons?)\b/i.test(quote)
          && (states.length || /\b(operations?|facility|plant|factory|distribution|warehouse|supply chain)\b/i.test(quote))) {
        out.push({ exposureType: "weather", name: states.length ? `${states.join("/")} weather exposure` : "unresolved weather exposure",
          directionWhenDriverRises, states, months, scheduledDate: "", support });
      }
      const driver = DRIVER_KEYWORDS.find((x) => x.re.test(quote));
      if (driver && /\b(cost|price|input|exposure|risk|revenue|margin|demand)\b/i.test(quote)) {
        out.push({ exposureType: "commodity", name: driver.name,
          directionWhenDriverRises, states: [], months, scheduledDate: "", support });
      }
      if (months.length && /\b(seasonal|seasonality|demand|sales|holiday|annual cycle)\b/i.test(quote)) {
        out.push({ exposureType: "seasonal_demand", name: "documented seasonal demand cycle",
          directionWhenDriverRises, states, months, scheduledDate: "", support });
      }
      if (months.length && /\b(customer|budget|fiscal year|procurement|appropriation|spending cycle)\b/i.test(quote)) {
        out.push({ exposureType: "customer_budget_cycle", name: "customer budget cycle",
          directionWhenDriverRises, states: [], months, scheduledDate: "", support });
      }
      if (months.length && /\b(supply|inventory|production cycle|harvest|capacity cycle|lead times?)\b/i.test(quote)) {
        out.push({ exposureType: "supply_cycle", name: "supply cycle",
          directionWhenDriverRises, states, months, scheduledDate: "", support });
      }
      if (out.length >= 64) return out;
    }
  }
  return out;
}

function monthlyReturns(rows) {
  const groups = new Map();
  for (let i = 0; i < rows.length; i += 1) {
    const b = rows[i];
    const key = String(b.date).slice(0, 7);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push({ ...b, priorClose: i > 0 ? rows[i - 1].c : null });
  }
  const out = [];
  for (const [key, bars] of groups) {
    bars.sort((a, b) => String(a.date).localeCompare(String(b.date)));
    if (bars.length < 10 || !(bars[0].priorClose > 0) || !(bars.at(-1).c > 0)) continue;
    out.push({ key, year: Number(key.slice(0, 4)), month: Number(key.slice(5, 7)),
      value: bars.at(-1).c / bars[0].priorClose - 1, observations: bars.length });
  }
  return out;
}

/* Deterministic bootstrap.
 *
 * Each observation is one calendar year's return for the month in question,
 * so the year IS the resampling block and an ordinary bootstrap over the
 * annual observations is the correct block bootstrap. The generator is
 * seeded from the data so the same series always yields the same bound —
 * the temporal context is hashed, and a random bound would make that hash
 * meaningless. Returns the 5th percentile of the shrunk mean. */
function bootstrapBounds(values, shrink, draws = 2000, alpha = 0.05) {
  const n = values.length;
  if (n < 3) return { lower: 0, upper: 0 };
  let seed = 0;
  for (const v of values) {
    seed = (seed * 1664525 + Math.round(v * 1e9) + 1013904223) >>> 0;
  }
  const next = () => {
    seed = (seed * 1664525 + 1013904223) >>> 0;
    return seed / 4294967296;
  };
  const means = new Array(draws);
  for (let d = 0; d < draws; d += 1) {
    let sum = 0;
    for (let i = 0; i < n; i += 1) sum += values[Math.floor(next() * n)];
    means[d] = (sum / n) * shrink;
  }
  means.sort((a, b) => a - b);
  const lowerIndex = Math.min(draws - 1, Math.max(0, Math.floor(alpha * draws)));
  const upperIndex = Math.min(draws - 1, Math.max(0, Math.ceil((1 - alpha) * draws) - 1));
  return { lower: means[lowerIndex], upper: means[upperIndex] };
}

function bootstrapLowerBound(values, shrink, draws = 2000, alpha = 0.05) {
  return bootstrapBounds(values, shrink, draws, alpha).lower;
}

function seasonalityContext(series, asOfMs = Date.now(), { minDays = MIN_SEASONAL_DAYS, exposures = [] } = {}) {
  const cutoff = asOfDay(asOfMs);
  const rows = (series || []).filter((b) => b && b.date < cutoff && Number(b.c) > 0)
    .sort((a, b) => String(a.date).localeCompare(String(b.date)));
  const lastDate = rows.length ? rows.at(-1).date : null, ageDays = lastDate ? dateAgeDays(lastDate, asOfMs) : null;
  if (lastDate && ageDays > MAX_SERIES_AGE_DAYS) return { status: "stale", days: rows.length,
    lastDate, ageDays: round(ageDays, 1), maxAgeDays: MAX_SERIES_AGE_DAYS,
    reason: "company daily history is too old for a current temporal decision" };
  if (rows.length < minDays) return { status: "warming", days: rows.length,
    lastDate, ageDays, requiredDays: minDays, reason: "annual seasonality requires at least three point-in-time trading years" };
  const month = new Date(asOfMs).getUTCMonth() + 1;
  const completed = monthlyReturns(rows).filter((x) => x.month === month && `${x.key}-31` < cutoff);
  const values = completed.map((x) => x.value);
  if (values.length < 3) return { status: "warming", days: rows.length, samples: values.length,
    requiredSamples: 3, reason: "fewer than three completed same-calendar-month observations" };
  const avg = mean(values), sd = stdev(values), med = median(values), hit = values.filter((x) => x > 0).length / values.length;
  const shrink = values.length / (values.length + 8); // severe small-sample shrinkage
  const shrunk = avg * shrink, t = sd > 0 ? avg / (sd / Math.sqrt(values.length)) : 0;
  const negatives = values.filter((x) => x < 0).length;
  const bounds = bootstrapBounds(values, shrink);
  const bootLower = bounds.lower, bootUpper = bounds.upper;
  const diagnosticCredibleAdverse = shrunk < 0 && t <= -1.25 && hit <= 0.4;
  const exposureConditioned = (exposures || []).some((x) => x.decisionEligible !== false
    && (x.months || []).includes(month));

  /* DECISION GATE. The binding constraint is inferential, not a raw count.
     A same-month effect earns decision weight only when
       - enough completed observations exist to bootstrap at all,
       - the one-sided 95% UPPER bound is adverse (below zero),
       - the sign agrees in at least ceil(0.8n) years AND at least n-1 of
         them, which at the four-sample floor means every observed year,
       - the studentised mean clears -1.75, and
       - a quote-grounded company exposure names this calendar month.
     The last condition is the one that matters most: the residual model
     already removes market and sector, so a residual month-of-year effect
     with no company-specific cause is a data-mining artefact by
     construction. Measured false-positive rate on no-effect series is
     asserted by test. */
  const sampleEligible = values.length >= MIN_SEASONAL_DECISION_SAMPLES && sd > 0;
  const requiredAdverseYears = Math.max(values.length - 1,
    Math.ceil(values.length * 0.8));
  const inferentiallyAdverse = shrunk < 0 && bootUpper < 0
    && negatives >= requiredAdverseYears && t <= -1.75;
  const decisionEligible = sampleEligible;
  const credibleAdverse = sampleEligible && inferentiallyAdverse && exposureConditioned;
  /* For an adverse (negative-return) effect the confidence endpoint nearest
     zero is the UPPER bound. Using the lower tail both reversed the inference
     and exaggerated the haircut. The risk score is therefore bounded by the
     magnitude that survives at the conservative upper endpoint. */
  const riskScore = credibleAdverse ? Math.min(25, Math.abs(bootUpper) * 100) : 0;
  const rawRisk = diagnosticCredibleAdverse ? Math.min(25, Math.abs(t) * 6 + Math.abs(shrunk) * 100) : 0;
  const diagnosticRiskScore = rawRisk * (exposureConditioned ? 1 : 0.6);
  return { status: "ready", days: rows.length, lastDate, ageDays: round(ageDays, 1),
    calendarMonth: month, samples: values.length,
    meanReturnPct: round(avg * 100, 2), medianReturnPct: round(med * 100, 2),
    shrunkReturnPct: round(shrunk * 100, 2), positiveHitRate: round(hit), tStatistic: round(t, 2),
    bootstrapLowerPct: round(bootLower * 100, 2), bootstrapUpperPct: round(bootUpper * 100, 2), adverseYears: negatives,
    requiredAdverseYears,
    diagnosticCredibleAdverse, credibleAdverse, exposureConditioned,
    inferentiallyAdverse, decisionEligible,
    requiredDecisionSamples: MIN_SEASONAL_DECISION_SAMPLES,
    diagnosticRiskScore: round(diagnosticRiskScore, 1), riskScore: round(riskScore, 1),
    decisionReason: !sampleEligible ? "insufficient_completed_same_month_observations"
      : !inferentiallyAdverse ? "no_adverse_effect_survives_block_bootstrap"
      : !exposureConditioned ? "no_quote_grounded_exposure_names_this_month"
      : "decision_weighted",
    note: credibleAdverse
      ? "Adverse at the one-sided 95% upper bootstrap bound with a named company exposure; reduces size only."
      : "Descriptive only: this month's history does not survive the decision gate." };
}

function returnsByDate(series, asOfMs) {
  const cutoff = asOfDay(asOfMs), rows = (series || []).filter((b) => b && b.date < cutoff && Number(b.c) > 0)
    .sort((a, b) => String(a.date).localeCompare(String(b.date)));
  const out = new Map();
  for (let i = 1; i < rows.length; i += 1) if (rows[i - 1].c > 0) out.set(rows[i].date, rows[i].c / rows[i - 1].c - 1);
  const lastDate = rows.length ? rows.at(-1).date : null;
  return { rows, returns: out, lastDate,
    ageDays: lastDate ? dateAgeDays(lastDate, asOfMs) : Infinity,
    fresh: !!lastDate && dateAgeDays(lastDate, asOfMs) <= MAX_SERIES_AGE_DAYS };
}
function regression(x, y) {
  if (x.length < 40 || x.length !== y.length) return null;
  const mx = mean(x), my = mean(y), vx = mean(x.map((v) => (v - mx) ** 2));
  if (!(vx > 0)) return null;
  const cov = mean(x.map((v, i) => (v - mx) * (y[i] - my)));
  const sy = stdev(y), sx = stdev(x), corr = sx > 0 && sy > 0 ? cov / (sx * sy) : 0;
  return { beta: cov / vx, correlation: clamp(corr, -1, 1), observations: x.length };
}

function pairwiseCorrelation(seriesA, seriesB, asOfMs = Date.now(), {
  provenanceA = null, provenanceB = null, requireProvenance = false,
} = {}) {
  if (requireProvenance && (!validSeriesProvenance(provenanceA) || !validSeriesProvenance(provenanceB))) {
    return { status: "unavailable", reason: "untrusted_provenance",
      provenanceAValid: validSeriesProvenance(provenanceA),
      provenanceBValid: validSeriesProvenance(provenanceB) };
  }
  const a = returnsByDate(seriesA, asOfMs), b = returnsByDate(seriesB, asOfMs);
  if (!a.fresh || !b.fresh) return { status: "unavailable", reason: "stale_series",
    lastDateA: a.lastDate, lastDateB: b.lastDate, ageDaysA: round(a.ageDays, 1), ageDaysB: round(b.ageDays, 1) };
  const dates = [...a.returns.keys()].filter((d) => b.returns.has(d)).sort().slice(-252);
  const xs = dates.map((d) => a.returns.get(d)), ys = dates.map((d) => b.returns.get(d));
  const long = regression(xs, ys), recent = regression(xs.slice(-63), ys.slice(-63));
  if (dates.length < MIN_DRIVER_OBSERVATIONS || !long || !recent) return { status: "unavailable",
    reason: "insufficient_aligned_history", observations: dates.length,
    lastDateA: a.lastDate, lastDateB: b.lastDate };
  const shrink = dates.length / (dates.length + 30);
  const corr252 = long.correlation * shrink, corr63 = recent.correlation * (63 / 93);
  return { status: "ready", observations: dates.length, lastDateA: a.lastDate, lastDateB: b.lastDate,
    ageDaysA: round(a.ageDays, 1), ageDaysB: round(b.ageDays, 1), correlation252: round(corr252, 3),
    correlation63: round(corr63, 3), stressedCorrelation: round(clamp(Math.max(corr252, corr63) + 0.1, -1, 1), 3),
    unstable: Math.abs(corr63 - corr252) >= 0.35,
    provenanceValidated: requireProvenance };
}

function driverContext(companySeries, driverSeries = {}, exposures = [], sector = "other", asOfMs = Date.now(),
  { companyProvenance = null, driverProvenance = {} } = {}) {
  const required = new Set([DRIVER_BY_SECTOR[sector] || "SPY"]);
  const supportedDriverExposure = (x) => x && x.exposureType === "commodity"
    && x.decisionEligible !== false && !!x.driverSymbol
    && !!(x.support && x.support.documentRef && x.support.quote)
    && (x.sourceTier === "A" || Number(x.evidenceReliability) >= 0.85)
    && Number(x.operationalMateriality) >= 0.55
    && (x.directionWhenDriverRises === "negative" || x.directionWhenDriverRises === "positive");
  for (const x of exposures) if (supportedDriverExposure(x)) required.add(x.driverSymbol);
  const company = returnsByDate(companySeries, asOfMs), results = [];
  for (const symbol of required) {
    const raw = driverSeries[symbol] || [], driver = returnsByDate(raw, asOfMs);
    const dates = [...company.returns.keys()].filter((d) => driver.returns.has(d)).sort().slice(-252);
    const xs = dates.map((d) => driver.returns.get(d)), ys = dates.map((d) => company.returns.get(d));
    const full = regression(xs, ys), recent = regression(xs.slice(-63), ys.slice(-63));
    if (!company.fresh || !driver.fresh) { results.push({ symbol, status: "unavailable", reason: "stale_series",
      observations: dates.length, companyLastDate: company.lastDate, driverLastDate: driver.lastDate,
      companyAgeDays: round(company.ageDays, 1), driverAgeDays: round(driver.ageDays, 1) }); continue; }
    if (dates.length < MIN_DRIVER_OBSERVATIONS || !full || !recent) { results.push({ symbol,
      status: "unavailable", reason: "insufficient_aligned_history", observations: dates.length,
      companyLastDate: company.lastDate, driverLastDate: driver.lastDate }); continue; }
    const prior = regression(xs.slice(-126, -63), ys.slice(-126, -63));
    const dRows = driver.rows.slice(-21), ret5 = dRows.length >= 6 ? dRows.at(-1).c / dRows.at(-6).c - 1 : 0;
    const ret20 = dRows.length >= 21 ? dRows.at(-1).c / dRows[0].c - 1 : 0;
    const contribution = recent.beta * ret5;
    const unstable = !!prior && (Math.sign(prior.correlation) !== Math.sign(recent.correlation)
      || Math.abs(prior.correlation - recent.correlation) >= 0.35);
    const supported = exposures.filter((x) => supportedDriverExposure(x) && x.driverSymbol === symbol)
      .filter((x) => Math.abs(recent.correlation) >= 0.25
        && Math.sign(recent.correlation) === (x.directionWhenDriverRises === "negative" ? -1 : 1));
    const directed = supported.map((x) => ({ exposureId: x.exposureId,
      contribution: x.directionWhenDriverRises === "negative" ? -ret5 : ret5,
      weight: Number(x.evidenceReliability || 0.5) * Number(x.operationalMateriality || 0.55) }))
      .sort((a, b) => a.contribution - b.contribution)[0] || null;
    const isSectorDriver = symbol === (DRIVER_BY_SECTOR[sector] || "SPY");
    /* The sector/market driver earns NO decision weight.
     *
     * The signal is built on residuals from a leave-one-out, sector-balanced
     * factor model: market and sector moves are already removed before a
     * z-score exists. Charging temporal risk for the same sector ETF move is
     * a second deduction for an exposure the residual has neutralised, and
     * the previous `isSectorDriver ||` clause made that the DEFAULT path —
     * every symbol had an always-eligible driver requiring no evidence at
     * all, which is exactly the named-driver requirement it was meant to
     * enforce. Its beta and correlation stay in the record as diagnostics;
     * only company-specific, quote-grounded drivers can reduce size. */
    const namedDriverEligible = supported.length > 0;
    const statisticalRisk = !unstable && namedDriverEligible
      && contribution < -0.01 && Math.abs(recent.correlation) >= 0.25
      ? Math.min(35, Math.abs(contribution) * 900) : 0;
    const directedRisk = !unstable && directed && directed.contribution < -0.01
      ? Math.min(35, Math.abs(directed.contribution) * 800 * directed.weight) : 0;
    results.push({ symbol, status: "ready", observations: dates.length,
      companyLastDate: company.lastDate, driverLastDate: driver.lastDate,
      companyAgeDays: round(company.ageDays, 1), driverAgeDays: round(driver.ageDays, 1),
      companyProvenance, driverProvenance: driverProvenance[symbol] || null,
      beta252: round(full.beta, 3), correlation252: round(full.correlation, 3),
      beta63: round(recent.beta, 3), correlation63: round(recent.correlation, 3),
      correlationShift: prior ? round(recent.correlation - prior.correlation, 3) : null,
      driverReturn5dPct: round(ret5 * 100, 2), driverReturn20dPct: round(ret20 * 100, 2),
      impliedContribution5dPct: round(contribution * 100, 2), unstable,
      directedContribution5dPct: directed ? round(directed.contribution * 100, 2) : null,
      directedExposureId: directed && directed.exposureId || null,
      directedEvidenceEligible: !!directed && !unstable,
      directedEvidenceCount: supported.length,
      isSectorDriver, namedDriverEligible,
      decisionReason: unstable ? "correlation_regime_unstable"
        : (isSectorDriver && !supported.length
          ? "sector_factor_already_neutralised_by_residual_model"
          : (!supported.length ? "insufficient_company_specific_exposure_evidence" : "eligible")),
      statisticalRiskScore: round(statisticalRisk, 1), directedRiskScore: round(directedRisk, 1),
      riskScore: round(Math.max(statisticalRisk, directedRisk), 1) });
  }
  return { required: [...required], drivers: results,
    missing: results.filter((x) => x.status !== "ready").map((x) => x.symbol) };
}

function hazardSeasonPrior(exposures, asOfMs) {
  const month = new Date(asOfMs).getUTCMonth() + 1;
  const relevant = exposures.filter((x) => x.decisionEligible !== false
    && (x.exposureType === "weather" || x.exposureType === "geography"));
  const states = new Set(relevant.flatMap((x) => x.states || []));
  const weightFor = (subset) => Math.max(0, ...relevant.filter((x) => (x.states || []).some((s) => subset.has(s)))
    .map((x) => Number(x.evidenceReliability || 0.5) * Number(x.operationalMateriality || 0.55)));
  const rows = [];
  if (month >= 6 && month <= 11 && [...states].some((s) => HURRICANE_STATES.has(s))) {
    const relevance = weightFor(HURRICANE_STATES);
    rows.push({ hazard: "Atlantic hurricane season", states: [...states].filter((s) => HURRICANE_STATES.has(s)),
      relevanceWeight: round(relevance), riskScore: round(14 * relevance, 1) });
  }
  if ((month >= 11 || month <= 3) && [...states].some((s) => WINTER_STATES.has(s))) {
    const relevance = weightFor(WINTER_STATES);
    rows.push({ hazard: "winter storm season", states: [...states].filter((s) => WINTER_STATES.has(s)),
      relevanceWeight: round(relevance), riskScore: round(10 * relevance, 1) });
  }
  return rows;
}

function hazardContext(exposures, activeHazards = [], asOfMs = Date.now(), {
  requireProvenance = false, nwsResponses = [],
} = {}) {
  const eligible = (exposures || []).filter((x) => x.decisionEligible !== false);
  const states = new Set(eligible.flatMap((x) => x.states || []));
  const certainty = { Observed: 0.98, Likely: 0.82, Possible: 0.58, Unlikely: 0.25, Unknown: 0.4 };
  const severity = { Extreme: 85, Severe: 68, Moderate: 44, Minor: 24, Unknown: 32 };
  const matched = [];
  let provenanceFailures = 0;
  for (const h of activeHazards || []) {
    const overlap = (h.states || []).filter((s) => states.has(s));
    if (!overlap.length) continue;
    const effectiveMs = Date.parse(h.effective || ""), expiresMs = Date.parse(h.expires || "");
    if (Number.isFinite(effectiveMs) && effectiveMs > asOfMs + 3600e3) continue;
    if (Number.isFinite(expiresMs) && expiresMs < asOfMs - 5 * 60e3) continue;
    if (requireProvenance && !validNwsHazard(h, nwsResponses, asOfMs)) {
      provenanceFailures += 1;
      continue;
    }
    const p = certainty[h.certainty] || certainty.Unknown, base = severity[h.severity] || severity.Unknown;
    const relevantExposures = eligible.filter((x) => (x.states || []).some((s) => overlap.includes(s))
      && (x.exposureType === "weather" || x.exposureType === "geography"));
    const exposureWeight = Math.max(0, ...relevantExposures.map((x) => Number(x.evidenceReliability || 0.5)
      * (0.5 + 0.5 * Number(x.operationalMateriality || 0.55)))) * STATE_GEOGRAPHIC_WEIGHT;
    matched.push({ id: h.id || sha(`${h.event}|${h.effective}`).slice(0, 20), event: h.event || "weather alert",
      headline: norm(h.headline).slice(0, 240), states: overlap, severity: h.severity || "Unknown",
      certainty: h.certainty || "Unknown", urgency: h.urgency || "Unknown",
      effective: h.effective || null, expires: h.expires || null,
      probability: round(p), exposureWeight: round(exposureWeight),
      riskScore: round(base * p * exposureWeight, 1), source: "NWS",
      responseSha256: h.responseSha256 || null, fetchedAt: h.fetchedAt || null,
      matchingGranularity: "state", geographicPrecisionWeight: STATE_GEOGRAPHIC_WEIGHT });
  }
  matched.sort((a, b) => b.riskScore - a.riskScore);
  const seasonalPriors = hazardSeasonPrior(eligible, asOfMs);
  return { exposureStates: [...states].sort(), active: matched.slice(0, 20), seasonalPriors,
    riskScore: Math.max(matched.length ? matched[0].riskScore : 0,
      ...seasonalPriors.map((x) => Number(x.riskScore || 0))),
    discoveryLeadCount: (exposures || []).filter((x) => x.decisionEligible === false
      && (x.exposureType === "weather" || x.exposureType === "geography")).length,
    provenanceFailures,
    limitation: "Public alert matching is state-level and receives a 0.70 geographic-precision weight; operational materiality and evidence reliability further reduce overbreadth." };
}

function recurringContext(exposures, asOfMs = Date.now()) {
  const month = new Date(asOfMs).getUTCMonth() + 1, active = [];
  for (const x of exposures || []) {
    if (!(x.months || []).includes(month)) continue;
    const base = TEMPORAL_WEIGHTS.recurring[x.exposureType] || 10;
    const directionFactor = x.directionWhenDriverRises === "negative" ? 1
      : x.directionWhenDriverRises === "mixed" ? 0.65
      : x.directionWhenDriverRises === "unknown" ? 0.5 : 0;
    const evidenceWeight = Number(x.evidenceReliability || 0.5)
      * (0.6 + 0.4 * Number(x.operationalMateriality || 0.55));
    const decisionEligible = x.decisionEligible !== false;
    active.push({ exposureId: x.exposureId, exposureType: x.exposureType, name: x.name,
      calendarMonth: month, direction: x.directionWhenDriverRises,
      decisionEligible,
      evidenceWeight: round(evidenceWeight), baseWeight: base,
      riskScore: decisionEligible ? round(base * directionFactor * evidenceWeight, 1) : 0,
      support: x.support });
  }
  active.sort((a, b) => b.riskScore - a.riskScore);
  return { calendarMonth: month, active, riskScore: active.length ? active[0].riskScore : 0 };
}

function scheduleRisk(event) {
  if (event.decisionEligible === false) return 0;
  if (event.type === "earnings") return 0; // enforced once by the signal blackout
  const base = TEMPORAL_WEIGHTS.schedule[event.type] || TEMPORAL_WEIGHTS.schedule.scheduled_event;
  const reliability = clamp(event.sourceReliability == null ? 0.75 : event.sourceReliability, 0.1, 0.99);
  const uncertainty = event.estimated ? 0.78 : 1;
  return round(base * (0.65 + 0.35 * reliability) * uncertainty, 1);
}

function scheduleContext(documents, earningsWindow, rawExposures, asOfMs) {
  const rows = extractScheduledEvents(documents, asOfMs);
  for (const date of earningsWindow && earningsWindow.dates || []) rows.push({
    eventId: sha(`earnings|${date}`).slice(0, 20), type: "earnings", date,
    estimated: earningsWindow.estimated !== false, title: "Earnings/results window",
    explicitlyConfirmed: earningsWindow.estimated === false,
    confirmation: earningsWindow.estimated === false ? "confirmed" : "estimated",
    decisionEligible: true, basis: earningsWindow.basis || null,
    basisKind: earningsWindow.basisKind || null,
    uncertaintyDays: Number(earningsWindow.uncertaintyDays) || null,
    sourceReliability: earningsWindow.estimated === false ? 0.9
      : earningsWindow.basisKind === "issuer_8k_item_2_02" ? 0.72 : 0.5 });
  for (const x of rawExposures || []) if (x.scheduledDate)
    rows.push({ eventId: x.exposureId, type: normalizeEventType(x.exposureType), date: x.scheduledDate,
      estimated: !!x.estimated, title: x.name, documentRef: x.support.documentRef,
      explicitlyConfirmed: x.explicitlyConfirmed === true,
      confirmation: x.confirmation || (x.estimated ? "estimated" : "confirmed"),
      decisionEligible: x.decisionEligible !== false,
      quote: x.support.quote, sourceReliability: x.evidenceReliability });
  const byEvent = new Map();
  for (const row of rows.filter((x) => x.date)) {
    const key = `${row.type}|${row.date}`, prior = byEvent.get(key);
    const quality = (x) => (x.decisionEligible === false ? 0 : 100)
      + 10 * Number(x.sourceReliability || 0) + (x.explicitlyConfirmed ? 1 : 0);
    if (!prior || quality(row) > quality(prior)) byEvent.set(key, row);
  }
  const unique = [...byEvent.values()].sort((a, b) => a.date.localeCompare(b.date));
  const upcoming = unique.map((x) => {
    const uncertainty = Number(x.uncertaintyDays) || null;
    const preDays = uncertainty || (x.estimated ? 5 : 2);
    const postDays = Math.max(x.estimated ? 6 : 4, uncertainty || 0);
    const event = { ...x, preDays, postDays,
      daysAway: round((Date.parse(`${x.date}T12:00:00Z`) - asOfMs) / DAY_MS, 1) };
    return { ...event, riskScore: scheduleRisk(event),
      decisionRole: event.type === "earnings" ? "signal_blackout_only" : "temporal_matrix" };
  })
    .filter((x) => x.daysAway >= -45 && x.daysAway <= 550);
  const events = upcoming.slice(0, 30);
  const nearest = [...events].sort((a, b) => Math.abs(a.daysAway) - Math.abs(b.daysAway))[0] || null;
  return { events, nearest };
}

function combineAdverseRisks(rows) {
  const byGroup = new Map();
  for (const row of rows || []) {
    const score = clamp(row && row.score, 0, 100), group = String(row && row.group || row && row.id || "other");
    if (!byGroup.has(group) || score > byGroup.get(group).score) byGroup.set(group, { ...row, score });
  }
  const independent = [...byGroup.values()].sort((a, b) => b.score - a.score);
  const combined = independent.reduce((sum, row, i) => sum + row.score
    * (TEMPORAL_WEIGHTS.combination[i] == null ? 0.03 : TEMPORAL_WEIGHTS.combination[i]), 0);
  return { riskScore: round(Math.min(100, combined), 1), components: independent };
}

function sizeMultiplierForRisk(value) {
  const risk = clamp(value, 0, 100);
  if (risk >= 70) return 0;
  for (let i = 1; i < RISK_SIZE_ANCHORS.length; i += 1) {
    const [x1, y1] = RISK_SIZE_ANCHORS[i - 1], [x2, y2] = RISK_SIZE_ANCHORS[i];
    if (risk <= x2) return y1 + ((risk - x1) / (x2 - x1)) * (y2 - y1);
  }
  return 0;
}

function temporalPolicy(context, { maxAgeHours = 6, asOfMs = Date.now() } = {}) {
  if (!context) return { monitored: false, fresh: false, complete: false, entryAllowed: false,
    sizeMultiplier: 0, riskScore: 0, reasons: ["temporal context unavailable"], positiveRiskIncreaseAllowed: false };
  const signedAgeHours = (asOfMs - Number(context.asOfMs || 0)) / 3600e3;
  const futureDated = signedAgeHours < -(5 / 60);
  const ageHours = Math.max(0, signedAgeHours);
  const fresh = !futureDated && ageHours <= maxAgeHours;
  const reasons = [], requiredMissing = [...(context.coverage && context.coverage.requiredMissing || [])];
  const currentMonth = new Date(asOfMs).getUTCMonth() + 1;
  const calendarBoundaryStale = (context.recurring && Number(context.recurring.calendarMonth) !== currentMonth)
    || (context.seasonality && context.seasonality.calendarMonth != null
      && Number(context.seasonality.calendarMonth) !== currentMonth);
  if (calendarBoundaryStale) requiredMissing.push("calendar_month_boundary_refresh");
  const sourceHealth = context.coverage && context.coverage.sourceHealth || {};
  const hazardStates = context.hazards && context.hazards.exposureStates || [];
  const nwsResponses = sourceHealth.nwsResponses || [];
  for (const state of hazardStates) if (!nwsResponses.some((row) => validNwsResponse(row, asOfMs, state)))
    requiredMissing.push(`nws_response_provenance_${state}`);
  const persistedHazards = context.hazards && context.hazards.active || [];
  const activeHazards = persistedHazards.filter((row) => validNwsHazard(row, nwsResponses, asOfMs));
  const potentiallyCurrentHazards = persistedHazards.filter((row) => {
    const effectiveMs = Date.parse(row.effective || ""), expiresMs = Date.parse(row.expires || "");
    return (!Number.isFinite(effectiveMs) || effectiveMs <= asOfMs + 3600e3)
      && (!Number.isFinite(expiresMs) || expiresMs >= asOfMs - 5 * 60e3);
  });
  if (potentiallyCurrentHazards.some((row) => !validNwsHazard(row, nwsResponses, asOfMs)))
    requiredMissing.push("nws_alert_provenance");
  const seasonalHazardRisk = calendarBoundaryStale ? 0 : Math.max(0,
    ...(context.hazards && context.hazards.seasonalPriors || []).map((x) => Number(x.riskScore || 0)));
  const currentHazardRisk = Math.max(seasonalHazardRisk,
    ...activeHazards.map((x) => Number(x.riskScore || 0)));
  const riskRows = [
    { id: "seasonality", group: "seasonality", score: calendarBoundaryStale ? 0
      : Number(context.seasonality && context.seasonality.riskScore || 0) },
    { id: "hazards", group: "hazards", score: currentHazardRisk },
    { id: "recurring", group: "recurring", score: calendarBoundaryStale ? 0
      : Number(context.recurring && context.recurring.riskScore || 0) },
    ...(context.drivers && context.drivers.drivers || []).map((x) => ({ id: `driver_${x.symbol}`,
      group: "drivers", score: Number(x.riskScore || 0) })),
  ];
  const decisionEvents = (context.schedule && context.schedule.events || []).map((event) => {
    const eventAtMs = Date.parse(`${event.date}T12:00:00Z`);
    return Number.isFinite(eventAtMs) ? { ...event, daysAway: round((eventAtMs - asOfMs) / DAY_MS, 1) } : event;
  });
  const eventWindows = decisionEvents.filter((event) => {
    const pre = Number(event.preDays) || (event.estimated ? 5 : 2);
    const post = Number(event.postDays) || (event.estimated ? 6 : 4);
    return event.daysAway >= -post && event.daysAway <= pre;
  }).sort((a, b) => (Number.isFinite(Number(b.riskScore)) ? Number(b.riskScore) : (b.type === "earnings" ? 75 : 55))
    - (Number.isFinite(Number(a.riskScore)) ? Number(a.riskScore) : (a.type === "earnings" ? 75 : 55))
    || Math.abs(a.daysAway) - Math.abs(b.daysAway));
  for (const event of eventWindows.slice(0, 3)) {
    const eventRisk = event.type === "earnings" ? 0
      : (Number.isFinite(Number(event.riskScore)) ? Number(event.riskScore) : 55);
    if (eventRisk > 0) riskRows.push({ id: event.eventId || event.type, group: "schedule", score: eventRisk });
    if (event.type === "earnings") reasons.push("earnings window is visible here but enforced only by the existing signal blackout");
    else if (event.decisionEligible === false) reasons.push(`unconfirmed ${event.type} lead is visible but not decision-weighted`);
    else reasons.push(`${event.estimated ? "estimated" : "confirmed"} ${event.type} ${event.daysAway >= 0 ? "in" : "was"} ${Math.abs(event.daysAway)} day(s)`);
  }
  if (activeHazards.length) reasons.push(`active public weather hazard: ${activeHazards[0].event}`);
  if (!calendarBoundaryStale && context.seasonality && context.seasonality.credibleAdverse)
    reasons.push(`adverse same-month history after shrinkage (${context.seasonality.samples} samples)`);
  const stressedDriver = [...(context.drivers && context.drivers.drivers || [])]
    .sort((a, b) => Number(b.riskScore) - Number(a.riskScore))[0];
  if (stressedDriver && stressedDriver.riskScore > 0) reasons.push(`adverse driver move/correlation: ${stressedDriver.symbol}`);
  if (!calendarBoundaryStale && context.recurring && context.recurring.riskScore > 0)
    reasons.push(`active recurring company cycle: ${context.recurring.active[0].name}`);
  const combined = combineAdverseRisks(riskRows), risk = combined.riskScore;
  const uniqueMissing = [...new Set(requiredMissing)];
  const complete = uniqueMissing.length === 0;
  let entryAllowed = fresh && complete, sizeMultiplier = 1;
  if (futureDated) reasons.push("temporal context timestamp is in the future");
  else if (!fresh) reasons.push("temporal context is stale");
  if (!complete) reasons.push(`required temporal inputs missing: ${uniqueMissing.join(", ")}`);
  if (!entryAllowed || risk >= 70) { entryAllowed = false; sizeMultiplier = 0; }
  else sizeMultiplier = sizeMultiplierForRisk(risk);
  if (!reasons.length) reasons.push("no material adverse temporal condition detected in covered inputs");
  return { monitored: true, fresh, complete, ageHours: round(ageHours, 2), entryAllowed,
    sizeMultiplier: round(sizeMultiplier), riskScore: round(risk, 1), reasons,
    requiredMissing: uniqueMissing, positiveRiskIncreaseAllowed: false,
    riskComponents: combined.components,
    sizingCurve: RISK_SIZE_ANCHORS,
    combinationRule: "max within dependent groups; then 1.00/0.35/0.20/0.10/0.05 capped additive weights",
    exitReview: risk >= 70, criticalExit: false,
    limitation: "Temporal context can block or reduce new risk; it cannot independently liquidate a position or increase size." };
}

function shadowCalibrationObservation(context, policy = context && context.policy) {
  const drivers = context && context.drivers && context.drivers.drivers || [];
  const schedule = context && context.schedule && context.schedule.events || [];
  return {
    schemaVersion: "context-feature-observation-v1",
    mode: "measurement_only", affectsDecision: false, outcomeKnown: false,
    observedAtMs: Number(context && context.asOfMs) || null,
    features: {
      temporalRiskScore: Number(policy && policy.riskScore) || 0,
      nonEarningsScheduleRisk: Math.max(0, ...schedule.filter((x) => x.type !== "earnings")
        .map((x) => Number(x.riskScore) || 0)),
      hazardRisk: Number(context && context.hazards && context.hazards.riskScore) || 0,
      recurringRisk: Number(context && context.recurring && context.recurring.riskScore) || 0,
      seasonalityDiagnosticRisk: Number(context && context.seasonality
        && context.seasonality.diagnosticRiskScore) || 0,
      seasonalityDecisionRisk: Number(context && context.seasonality && context.seasonality.riskScore) || 0,
      driverRisk: Math.max(0, ...drivers.map((x) => Number(x.riskScore) || 0)),
      verifiedAdverseDriverCount: drivers.filter((x) =>
        x.directedEvidenceEligible && Number(x.riskScore) > 0).length,
    },
    exclusions: { earnings: "signal_blackout_only",
      seasonalityWithoutNamedExposure: "display_only",
      positiveContext: "cannot_increase_size" },
  };
}

function buildTemporalContext({ profile, documents = [], dailyBars = [], driverSeries = {},
  earningsWindow = null, rawExposures = [], activeHazards = [], temporalSourceHealth = {},
  companyProvenance = null, driverProvenance = {}, asOfMs = Date.now(), maxAgeHours = 6,
  minSeasonalityDays = MIN_SEASONAL_DAYS } = {}) {
  const deterministicExposures = extractDeterministicExposures(documents, asOfMs);
  const exposures = normalizeExposures([...(rawExposures || []), ...deterministicExposures], documents, asOfMs);
  const effectiveMinSeasonalityDays = Math.max(MIN_SEASONAL_DAYS,
    Number(minSeasonalityDays) || MIN_SEASONAL_DAYS);
  const seasonality = seasonalityContext(dailyBars, asOfMs,
    { minDays: effectiveMinSeasonalityDays, exposures });
  const drivers = driverContext(dailyBars, driverSeries, exposures, profile && profile.sector || "other", asOfMs,
    { companyProvenance, driverProvenance });
  const requireNwsProvenance = temporalSourceHealth.requireNwsProvenance !== false;
  const hazards = hazardContext(exposures, activeHazards, asOfMs, { requireProvenance: requireNwsProvenance,
    nwsResponses: temporalSourceHealth.nwsResponses || [] });
  const schedule = scheduleContext(documents, earningsWindow, exposures, asOfMs);
  const recurring = recurringContext(exposures, asOfMs);
  const requiredMissing = [];
  const weatherRelevant = exposures.some((x) => x.decisionEligible !== false
    && (x.exposureType === "weather" || x.exposureType === "geography"));
  if (temporalSourceHealth.exposureInventory !== true) requiredMissing.push("temporal_exposure_inventory");
  if (weatherRelevant && !hazards.exposureStates.length) requiredMissing.push("weather_geography_unresolved");
  if (weatherRelevant && hazards.exposureStates.length && temporalSourceHealth.nws !== true) requiredMissing.push("nws_active_alerts");
  if (weatherRelevant && hazards.exposureStates.length && requireNwsProvenance) {
    const responses = temporalSourceHealth.nwsResponses || [];
    const missingNwsStates = hazards.exposureStates.filter((state) =>
      !responses.some((row) => validNwsResponse(row, asOfMs, state)));
    if (missingNwsStates.length) requiredMissing.push(`nws_response_provenance_${missingNwsStates.join("_")}`);
    if (hazards.provenanceFailures > 0) requiredMissing.push("nws_alert_provenance");
  }
  for (const symbol of drivers.missing) requiredMissing.push(`driver_${symbol}`);
  for (const row of drivers.drivers.filter((x) => x.status === "ready")) {
    const p = row.driverProvenance;
    if (!validSeriesProvenance(p)) {
      requiredMissing.push(`driver_provenance_${row.symbol}`);
    }
  }
  if (seasonality.status !== "ready") requiredMissing.push(`seasonality_${seasonality.status}`);
  if (temporalSourceHealth.priceProvenance !== true || !validSeriesProvenance(companyProvenance))
    requiredMissing.push("daily_price_provenance");
  if (!earningsWindow || !(earningsWindow.dates || []).length) requiredMissing.push("earnings_window");
  const context = { schemaVersion: "temporal-context-v4", asOf: new Date(asOfMs).toISOString(), asOfMs,
    exposures, deterministicExposureCount: deterministicExposures.length,
    schedule, recurring, seasonality, hazards, drivers,
    coverage: { complete: requiredMissing.length === 0, requiredMissing,
      seasonalityStatus: seasonality.status, sourceHealth: temporalSourceHealth },
    weightModel: TEMPORAL_WEIGHTS,
    limitations: [
      "Seasonality is descriptive, heavily shrunk and never treated as causation.",
      "Weather alerts matter only where quote-grounded company exposure overlaps the alert geography.",
      "Rolling correlations can change abruptly and never authorize additional risk.",
    ] };
  context.policy = temporalPolicy(context, { maxAgeHours, asOfMs });
  context.contextHash = sha(JSON.stringify({ asOfMs, exposures, schedule, recurring, seasonality, hazards,
    drivers, coverage: context.coverage, weightModel: TEMPORAL_WEIGHTS }));
  context.shadowCalibration = shadowCalibrationObservation(context, context.policy);
  return context;
}

module.exports = { DAY_MS, MIN_SEASONAL_DAYS, MIN_SEASONAL_DECISION_SAMPLES, maxSeasonalSamples,
  bootstrapBounds, bootstrapLowerBound,
  MAX_SERIES_AGE_DAYS, MIN_DRIVER_OBSERVATIONS,
  PRIOR_EXPOSURE_TTL_DAYS, MAX_HAZARD_SOURCE_AGE_MS, STATE_GEOGRAPHIC_WEIGHT,
  DRIVER_BY_SECTOR, DRIVER_KEYWORDS, US_STATE_NAMES, TEMPORAL_WEIGHTS, RISK_SIZE_ANCHORS,
  parseDatePhrase, extractScheduledEvents, extractDeterministicExposures, normalizeExposures, seasonalityContext,
  driverContext, pairwiseCorrelation, hazardContext, recurringContext, scheduleContext,
  validSeriesProvenance, validNwsResponse, validNwsHazard,
  combineAdverseRisks, sizeMultiplierForRisk, shadowCalibrationObservation,
  temporalPolicy, buildTemporalContext };
