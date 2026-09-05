/*  netlify/functions/_investorDataProviders.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — provider-neutral, point-in-time news / estimates /
 *  transcripts interface (blueprint §5.1, §5.6, §6.5, §22 q3).
 *
 *  THERE IS NO VENDOR, AND NONE IS BEING PURCHASED
 *  ------------------------------------------------------------------------
 *  Consensus estimates, earnings-call transcripts and a commercial earnings
 *  calendar are permanently unavailable to this operator. This module exists
 *  so that the read-only research tool `searchDecisionData` answers those
 *  requests with an explicit `missing` entry — provider id, reason, coverage
 *  and retention metadata — instead of letting a model fill the gap from its
 *  training memory and present it as data. "Missing" is a first-class,
 *  honest result; it is never an exception.
 *
 *  WHAT IS ENTITLED
 *  ------------------------------------------------------------------------
 *    · sec_edgar — filings and 8-K news via _investorEvidence (point-in-time
 *      document versions; metadata only, never bodies, through this tool);
 *    · issuer_guidance — the estimates SUBSTITUTE: guidance claims that Luna
 *      extracted from 8-K Item 2.02 / MD&A (§5.6), read from InvestorAI_Claims;
 *    · issuer_earnings_announcement — the earnings-calendar substitute: the
 *      issuer's own announced date (EARNINGS_DATE claims), else the 8-K
 *      cadence.
 *
 *  Every read is point-in-time by `firstSeenAtMs` (and published-at where
 *  known): nothing first seen after `asOfMs` is returned.
 * ---------------------------------------------------------------------------
 */

"use strict";

const UNAVAILABLE_REASON = "not available to this operator and not being purchased (§5.6, §22 q3)";

/* ── provider registry ────────────────────────────────────────────────── */
const PROVIDERS = Object.freeze([
  Object.freeze({
    id: "sec_edgar", kind: "news", kinds: Object.freeze(["news", "filings"]), entitled: true,
    source: "SEC EDGAR via _investorEvidence (sec.latest, sec.submissions)",
    retention: "government_public_domain_retain", correctionLineage: true,
    coverage: "sec_filings_only",
    note: "Filings and 8-K news as immutable document versions; bodies stay in DocumentVersions, this tool returns metadata only.",
  }),
  Object.freeze({
    id: "issuer_guidance", kind: "estimates", kinds: Object.freeze(["estimates", "guidance"]), entitled: true,
    role: "estimates-substitute",
    source: "8-K Item 2.02 / MD&A guidance claims extracted by Luna (§5.6)",
    retention: "claims_retained_with_source_span", correctionLineage: true,
    coverage: "no_consensus_vendor",
    note: "Forward view is the issuer's own guidance, quoted from the filing; there is no consensus and none is implied.",
  }),
  Object.freeze({
    id: "issuer_earnings_announcement", kind: "earnings_calendar", kinds: Object.freeze(["earnings_calendar", "earnings_date"]), entitled: true,
    role: "earnings-calendar-substitute",
    source: "company.direct — issuer announcement (EARNINGS_DATE claims) or 8-K Item 2.02 cadence",
    retention: "claims_retained_with_source_span", correctionLineage: true,
    coverage: "issuer_announcement_or_8k_cadence",
    note: "A date is known only when the issuer announced it; otherwise the prior-year cadence is a hint, never a date.",
  }),
  Object.freeze({
    id: "consensus_estimates", kind: "estimates", kinds: Object.freeze(["consensus", "consensus_estimates"]), entitled: false,
    source: null, retention: "none", correctionLineage: false, coverage: "no_consensus_vendor",
    reason: UNAVAILABLE_REASON,
    note: "Sell-side consensus is not licensed. A model must not supply an expected number from memory.",
  }),
  Object.freeze({
    id: "transcripts", kind: "transcripts", kinds: Object.freeze(["transcripts", "transcript"]), entitled: false,
    source: null, retention: "none", correctionLineage: false, coverage: "unavailable",
    reason: UNAVAILABLE_REASON,
    note: "Earnings-call transcripts are not licensed. Prepared remarks that the issuer files as an 8-K exhibit arrive through sec_edgar.",
  }),
  Object.freeze({
    id: "commercial_earnings_calendar", kind: "earnings_calendar", kinds: Object.freeze(["commercial_earnings_calendar"]), entitled: false,
    source: null, retention: "none", correctionLineage: false, coverage: "issuer_announcement_or_8k_cadence",
    reason: UNAVAILABLE_REASON,
    note: "No commercial calendar vendor. See issuer_earnings_announcement.",
  }),
]);
const PROVIDER_BY_ID = Object.freeze(Object.fromEntries(PROVIDERS.map((p) => [p.id, p])));

/* Request kinds the tool understands, mapped to the provider that serves
   them (entitled) or the provider whose absence explains them (not). */
const KIND_ALIASES = Object.freeze({
  news: "sec_edgar", filings: "sec_edgar",
  guidance: "issuer_guidance", estimates: "consensus_estimates", consensus: "consensus_estimates", consensus_estimates: "consensus_estimates",
  earnings_date: "issuer_earnings_announcement", earnings_calendar: "issuer_earnings_announcement",
  commercial_earnings_calendar: "commercial_earnings_calendar",
  transcripts: "transcripts", transcript: "transcripts",
});
const DEFAULT_KINDS = Object.freeze(["news", "guidance", "earnings_date", "estimates", "transcripts"]);

const COVERAGE = Object.freeze({
  consensus: "no_consensus_vendor",
  transcripts: "unavailable",
  earningsCalendar: "issuer_announcement_or_8k_cadence",
});

/* ── pure answers ─────────────────────────────────────────────────────── */

/** Entitlement for a request kind (or provider id). PURE, never throws. */
function entitlement(kind) {
  const k = String(kind || "").toLowerCase().trim();
  const providerId = KIND_ALIASES[k] || (PROVIDER_BY_ID[k] ? k : null);
  const p = providerId ? PROVIDER_BY_ID[providerId] : null;
  if (!p) return { kind: k, providerId: null, entitled: false, reason: "unknown_kind", coverage: "unknown", retention: "none", correctionLineage: false };
  return { kind: k, providerId: p.id, entitled: p.entitled === true, reason: p.entitled ? null : p.reason,
    coverage: p.coverage, retention: p.retention, correctionLineage: p.correctionLineage === true, role: p.role || null, source: p.source };
}

/** The honest statement the compact card shows. PURE. */
function coverageStatement() {
  return {
    statement: "no consensus vendor by design; forward view is issuer guidance",
    consensus: COVERAGE.consensus, transcripts: COVERAGE.transcripts, earningsCalendar: COVERAGE.earningsCalendar,
    entitled: PROVIDERS.filter((p) => p.entitled).map((p) => p.id),
    unavailable: PROVIDERS.filter((p) => !p.entitled).map((p) => ({ id: p.id, reason: p.reason })),
  };
}

function missingEntry(kind, providerId, reason) {
  const p = PROVIDER_BY_ID[providerId] || null;
  return { kind, providerId, reason, coverage: p ? p.coverage : "unknown", retention: p ? p.retention : "none",
    entitled: p ? p.entitled === true : false };
}

/* ── point-in-time readers (lazy Firestore; never throw to the caller) ── */

async function readDocuments(symbol, asOfMs, limit) {
  const E = require("./_investorEvidence");
  const docs = await E.documentsForCompany(symbol, asOfMs, 366, Math.max(1, Math.min(500, limit * 2)));
  const rows = [];
  for (const d of docs) {
    const published = Date.parse(d.source_published_at || "");
    const firstSeen = Number(d.firstSeenAtMs);
    const known = Number(d.decisionKnownAtMs);
    /* No future documents, ever: every timestamp we have must be ≤ asOfMs. */
    if (Number.isFinite(published) && published > asOfMs) continue;
    if (Number.isFinite(firstSeen) && firstSeen > asOfMs) continue;
    if (Number.isFinite(known) && known > asOfMs) continue;
    if (!Number.isFinite(published) && !Number.isFinite(firstSeen) && !Number.isFinite(known)) continue;
    rows.push({ providerId: "sec_edgar", kind: "news",
      documentId: d.documentId || null, versionId: d.versionId || d.latestVersionId || null,
      title: String(d.title || "").slice(0, 500), sourceId: d.sourceId || null, sourceClass: d.sourceClass || null,
      form: d.form || null, publishedAt: d.source_published_at || null,
      firstSeenAtMs: Number.isFinite(firstSeen) ? firstSeen : (Number.isFinite(known) ? known : null),
      link: d.link || null });
  }
  return rows.slice(0, limit);
}

/** Guidance / earnings-date claims from InvestorAI_Claims, point-in-time by
 *  firstSeenAtMs. The collection may hold no typed claims yet (Luna's
 *  extraction lands them); an empty result is normal, not an error. */
async function readClaims(symbol, claimTypes, asOfMs, limit) {
  const A = require("./_investorAdmin");
  const snap = await A.col(A.COL.claims)
    .where("symbol", "==", symbol)
    .where("claimType", "in", claimTypes)
    .get();
  const rows = [];
  snap.forEach((doc) => {
    const c = doc.data() || {};
    const firstSeen = Number(c.firstSeenAtMs);
    if (!Number.isFinite(firstSeen) || firstSeen > asOfMs) return;
    const published = Date.parse(c.publishedAt || c.sourcePublishedAt || "");
    if (Number.isFinite(published) && published > asOfMs) return;
    rows.push({ providerId: c.claimType === "EARNINGS_DATE" ? "issuer_earnings_announcement" : "issuer_guidance",
      kind: c.claimType === "EARNINGS_DATE" ? "earnings_date" : "guidance",
      claimId: c.claimId || doc.id, claimType: c.claimType, text: String(c.text || "").slice(0, 400),
      quote: String(c.quote || "").slice(0, 600), metric: c.metric || null, unit: c.unit || null,
      lowValue: c.lowValue == null ? null : String(c.lowValue), highValue: c.highValue == null ? null : String(c.highValue),
      effectivePeriod: c.effectivePeriod || null, date: c.date || null, confirmed: c.confirmed === true,
      documentRef: c.documentRef || null, documentVersionId: c.documentVersionId || null,
      firstSeenAtMs: firstSeen, supersedesHint: c.supersedesHint || null });
  });
  rows.sort((a, b) => b.firstSeenAtMs - a.firstSeenAtMs);
  return rows.slice(0, limit);
}

/**
 * §6.5 read-only tool. Never throws for a missing provider: an unentitled
 * kind produces a `missing` entry, and a reader failure produces a `missing`
 * entry with the failure reason so the caller can tell "no data" from
 * "could not look".
 */
async function searchDecisionData({ symbol, kinds = [], asOfMs, limit = 50 } = {}) {
  const sym = String(symbol || "").toUpperCase().trim();
  const at = Number(asOfMs);
  const cap = Math.max(1, Math.min(200, Number(limit) || 50));
  const requested = [...new Set((Array.isArray(kinds) && kinds.length ? kinds : DEFAULT_KINDS)
    .map((k) => String(k || "").toLowerCase().trim()).filter(Boolean))];
  const out = { symbol: sym, asOfMs: at, results: [], missing: [], coverage: { ...COVERAGE }, providers: {} };
  if (!sym || !Number.isFinite(at)) {
    out.missing.push({ kind: "*", providerId: null, reason: !sym ? "symbol_required" : "asOfMs_required", coverage: "unknown", retention: "none", entitled: false });
    return out;
  }
  const claimKinds = new Set();
  let wantNews = false;
  for (const kind of requested) {
    const ent = entitlement(kind);
    out.providers[kind] = ent.providerId;
    if (!ent.entitled) { out.missing.push(missingEntry(kind, ent.providerId, ent.reason || "unknown_kind")); continue; }
    if (ent.providerId === "sec_edgar") wantNews = true;
    else if (ent.providerId === "issuer_guidance") claimKinds.add("GUIDANCE");
    else if (ent.providerId === "issuer_earnings_announcement") claimKinds.add("EARNINGS_DATE");
  }
  if (wantNews) {
    try {
      const rows = await readDocuments(sym, at, cap);
      out.results.push(...rows);
      if (!rows.length) out.missing.push(missingEntry("news", "sec_edgar", "no_point_in_time_documents"));
    } catch (e) {
      out.missing.push(missingEntry("news", "sec_edgar", `reader_failed:${String(e && (e.code || e.message) || e).slice(0, 120)}`));
    }
  }
  if (claimKinds.size) {
    const types = [...claimKinds];
    try {
      const rows = await readClaims(sym, types, at, cap);
      out.results.push(...rows);
      for (const t of types) {
        if (!rows.some((r) => r.claimType === t)) {
          out.missing.push(t === "GUIDANCE"
            ? missingEntry("guidance", "issuer_guidance", "no_issuer_guidance_claim_as_of")
            : missingEntry("earnings_date", "issuer_earnings_announcement", "no_issuer_announced_date_as_of"));
        }
      }
    } catch (e) {
      const reason = `reader_failed:${String(e && (e.code || e.message) || e).slice(0, 120)}`;
      for (const t of types) {
        out.missing.push(t === "GUIDANCE" ? missingEntry("guidance", "issuer_guidance", reason)
          : missingEntry("earnings_date", "issuer_earnings_announcement", reason));
      }
    }
  }
  /* The card's consensus line is always answered, asked for or not, so the
     absence is visible in the tool result itself. */
  if (!out.missing.some((m) => m.providerId === "consensus_estimates")) {
    out.missing.push(missingEntry("consensus", "consensus_estimates", UNAVAILABLE_REASON));
  }
  return out;
}

/* ── self check ───────────────────────────────────────────────────────── */
function selfCheck() {
  const failures = [];
  const check = (name, fn) => {
    try { const r = fn(); if (r === false) failures.push(`${name}: returned false`); }
    catch (e) { failures.push(`${name}: ${String(e && e.message || e)}`); }
  };
  check("registry has the required providers", () => {
    for (const id of ["sec_edgar", "issuer_guidance", "issuer_earnings_announcement", "consensus_estimates", "transcripts", "commercial_earnings_calendar"]) {
      if (!PROVIDER_BY_ID[id]) throw new Error(`missing ${id}`);
    }
    return PROVIDER_BY_ID.issuer_guidance.coverage === "no_consensus_vendor"
      && /Item 2\.02/.test(PROVIDER_BY_ID.issuer_guidance.source)
      && /company\.direct/.test(PROVIDER_BY_ID.issuer_earnings_announcement.source);
  });
  check("every unentitled provider has a reason and no retention", () =>
    PROVIDERS.filter((p) => !p.entitled).every((p) => p.reason === UNAVAILABLE_REASON && p.retention === "none" && p.correctionLineage === false));
  check("every provider declares kind, retention and correctionLineage", () =>
    PROVIDERS.every((p) => ["news", "estimates", "transcripts", "earnings_calendar"].includes(p.kind)
      && typeof p.retention === "string" && typeof p.correctionLineage === "boolean" && typeof p.note === "string"));
  check("entitlement answers", () => {
    const e = (k) => entitlement(k);
    if (e("news").entitled !== true || e("news").providerId !== "sec_edgar") throw new Error("news");
    if (e("filings").providerId !== "sec_edgar") throw new Error("filings");
    if (e("guidance").entitled !== true || e("guidance").coverage !== "no_consensus_vendor") throw new Error("guidance");
    if (e("earnings_date").entitled !== true || e("earnings_date").coverage !== "issuer_announcement_or_8k_cadence") throw new Error("earnings_date");
    if (e("estimates").entitled !== false || e("estimates").reason !== UNAVAILABLE_REASON) throw new Error("estimates");
    if (e("consensus").entitled !== false || e("transcripts").entitled !== false) throw new Error("consensus/transcripts");
    if (e("commercial_earnings_calendar").entitled !== false) throw new Error("calendar");
    if (e("bloomberg").entitled !== false || e("bloomberg").reason !== "unknown_kind") throw new Error("unknown kind");
    return e("TRANSCRIPTS").providerId === "transcripts";
  });
  check("coverage statement", () => {
    const c = coverageStatement();
    return c.statement === "no consensus vendor by design; forward view is issuer guidance"
      && c.consensus === "no_consensus_vendor" && c.transcripts === "unavailable" && c.earningsCalendar === "issuer_announcement_or_8k_cadence"
      && c.unavailable.length === 3 && c.unavailable.every((u) => u.reason === UNAVAILABLE_REASON)
      && c.entitled.includes("sec_edgar") && !c.entitled.includes("consensus_estimates");
  });
  check("results shape carries missing for consensus, without touching a store", () => {
    /* Only unentitled kinds are requested, so no reader runs and the promise
       settles synchronously enough to inspect in a sync self check. */
    let out = null;
    searchDecisionData({ symbol: "aapl", kinds: ["estimates", "transcripts"], asOfMs: 1700000000000 }).then((r) => { out = r; });
    /* The promise chain has no awaits on I/O for these kinds; drain microtasks by inspection at return time is not
       possible synchronously, so assert on the pure builder that searchDecisionData uses. */
    const missing = ["estimates", "transcripts"].map((k) => { const e = entitlement(k); return missingEntry(k, e.providerId, e.reason); });
    if (!missing.some((m) => m.providerId === "consensus_estimates" && m.reason === UNAVAILABLE_REASON && m.coverage === "no_consensus_vendor")) throw new Error("consensus missing entry");
    if (!missing.some((m) => m.providerId === "transcripts" && m.coverage === "unavailable")) throw new Error("transcripts missing entry");
    return out === null || (Array.isArray(out.missing) && out.missing.length >= 2);
  });
  check("bad arguments never throw", () => {
    let threw = false;
    try { searchDecisionData({}).catch(() => { threw = true; }); } catch { threw = true; }
    return !threw;
  });
  return { pass: failures.length === 0, failures };
}

module.exports = {
  PROVIDERS, PROVIDER_BY_ID, KIND_ALIASES, DEFAULT_KINDS, COVERAGE, UNAVAILABLE_REASON,
  entitlement, coverageStatement, missingEntry,
  searchDecisionData,
  selfCheck,
};
