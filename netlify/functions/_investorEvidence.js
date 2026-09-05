/*  netlify/functions/_investorEvidence.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — evidence lane. EDGAR ingestion, document versioning, and the
 *  three-state cause classification that decides fade-or-skip.
 *
 *  THE INVERTED TRIGGER — why this is cheap enough to run every 5 minutes
 *  ------------------------------------------------------------------------
 *  Polling every source and invoking a model every cycle creates avoidable cost
 *  and latency. This module inverts the trigger: the price loop detects an
 *  abnormal RESIDUAL move
 *  first; only then does the system go looking for a cause. Filings are polled
 *  cheaply (EDGAR's own API updates in under a second and we use 0.003 of its
 *  10 req/s ceiling), but the model is invoked only on a threshold breach or a
 *  genuinely new document version. Actual spend is metered rather than inferred
 *  from a hypothetical call count.
 *
 *  THE THREE STATES — and why they are the whole strategy
 *  ------------------------------------------------------------------------
 *  Ben-Rephael, Da, Easton & Israelsen studied 114,468 single-item 8-Ks and
 *  motivates attention as a research feature. This implementation does not
 *  possess order-flow data that can identify investor type, so it reports only
 *  abnormal same-clock activity and never calls it retail or institutional.
 *
 *    cause_detected_fundamental  a real repricing -> DO NOT FADE
 *    abnormal_activity_without_covered_fundamental_event -> eligible only with healthy coverage
 *    no_cause_detected           research-only until external recall is measured
 *
 *  The third label is deliberately worded as "not detected in covered
 *  sources". The system cannot bound what it does not cover, and the plan is
 *  explicit that "the stock fell for no reason" is a prohibited phrase. The
 *  recall benchmark in docs/ is what turns this from a guess into a measured
 *  quantity, and it is a blocking gate before this book is ever automated.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const crypto = require("crypto");
const { fetchPublic, normalizedHash } = require("./_investorFetch");
const S = require("./_investorSignal");
const { visibleText } = require("./_investorVisibleText");

/* ── source registry seeds. Nothing is fetched until a record is enabled. ─ */
const SOURCES = {
  "sec.latest": {
    source_id: "sec.latest",
    name: "SEC EDGAR latest filings (Atom)",
    canonical_domain: "www.sec.gov",
    source_class: "regulator_primary",
    tier: "A",
    credential_required: false,
    urlTemplate: "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK={cik}&type={form}&dateb=&owner=include&count=40&output=atom",
    accept: ["xml", "atom"],
    cadenceSeconds: 300,
    terms_url: "https://www.sec.gov/os/webmaster-faq",
    retention_rule: "government_public_domain_retain",
    full_text_allowed: true,
    notes: "10 req/s ceiling, declared User-Agent required. Form 4/13D/13G disseminate to 22:00 ET.",
    enabled: true,
  },
  "sec.submissions": {
    source_id: "sec.submissions",
    name: "SEC submissions JSON",
    canonical_domain: "data.sec.gov",
    source_class: "regulator_primary",
    tier: "A",
    credential_required: false,
    urlTemplate: "https://data.sec.gov/submissions/CIK{cik10}.json",
    accept: ["json"],
    cadenceSeconds: 900,
    terms_url: "https://www.sec.gov/os/webmaster-faq",
    retention_rule: "government_public_domain_retain",
    full_text_allowed: true,
    notes: "Updated in real time; sub-second processing delay per SEC.",
    enabled: true,
  },
  "fed.register": {
    source_id: "fed.register",
    name: "Federal Register (BIS export controls)",
    canonical_domain: "www.federalregister.gov",
    source_class: "regulator_primary",
    tier: "A",
    credential_required: false,
    urlTemplate: "https://www.federalregister.gov/api/v1/documents.json?per_page=20&order=newest&conditions[agencies][]=industry-and-security-bureau",
    accept: ["json"],
    cadenceSeconds: 1800,
    terms_url: "https://www.federalregister.gov/developers/documentation/api/v1",
    retention_rule: "government_public_domain_retain",
    full_text_allowed: true,
    notes: "Publishes 08:45 ET; the asymmetry lane — market often reacts hours later via secondary reporting.",
    enabled: true,
  },
};

/* ── EDGAR ─────────────────────────────────────────────────────────────── */
const FORM_WEIGHT = {
  "8-K": 1.0, "4": 0.6, "SC 13D": 0.9, "SC 13D/A": 0.8, "SC 13G": 0.4,
  "10-Q": 0.7, "10-K": 0.7, "425": 0.9, "DEFM14A": 0.9, "S-4": 0.7,
  "144": 0.2, "3": 0.2, "5": 0.2,
};

/** Minimal Atom parse — no XML dependency, entries are well-formed and small. */
function parseAtomEntries(xml) {
  const out = [];
  const blocks = String(xml || "").split(/<entry>/i).slice(1);
  for (const b of blocks) {
    const pick = (tag) => {
      const m = b.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)</${tag}>`, "i"));
      return m ? m[1].replace(/<[^>]+>/g, "").trim() : null;
    };
    const linkM = b.match(/<link[^>]*href="([^"]+)"/i);
    const title = pick("title") || "";
    const formM = title.match(/^([A-Z0-9][A-Z0-9\-\/ ]*?)\s+-\s+/);
    out.push({
      title,
      form: formM ? formM[1] : null,
      updated: pick("updated"),
      summary: pick("summary"),
      link: linkM ? linkM[1] : null,
      accession: (linkM && (linkM[1].match(/(\d{10}-\d{2}-\d{6})/) || [])[1]) || null,
    });
  }
  return out;
}

async function fetchFilingBody(entry) {
  if (!entry || !entry.link) return { text: "", sha256: null, fetchedAt: null };
  const res = await fetchPublic(entry.link, { sourceId: "sec.filing", accept: ["html","text","xml"],
    timeoutMs: 30000, maxBytes: 8 * 1024 * 1024 });
  return { text: filingPlainText(res.text).slice(0, 700000), sha256: res.sha256, fetchedAt: res.fetchedAt };
}

/** Convert inline-XBRL/HTML into bounded visible text before Firestore/model
 * storage. Raw byte identity remains in sha256; scripts, styles and hidden
 * XBRL payloads cannot crowd the actual filing language out of the excerpt. */
function filingPlainText(value) {
  return visibleText(value, { preserveBreaks: true });
}

/** Poll EDGAR for one CIK. Conditional request; returns only NEW versions. */
async function pollEdgar(cik, { forms = "", stateDoc = null } = {}) {
  const src = SOURCES["sec.latest"];
  const url = src.urlTemplate.replace("{cik}", encodeURIComponent(cik)).replace("{form}", encodeURIComponent(forms));
  const stRef = A.col(A.COL.sourceState).doc(`sec.latest_${cik}`);
  const st = stateDoc || (await stRef.get()).data() || {};

  let res;
  try {
    res = await fetchPublic(url, {
      sourceId: src.source_id, etag: st.etag, lastModified: st.lastModified,
      accept: ["xml", "atom", "text"], timeoutMs: 15000,
    });
  } catch (e) {
    await stRef.set({
      lastError: String(e.code || e.message).slice(0, 200),
      consecutiveFailures: (st.consecutiveFailures || 0) + 1,
      lastAttemptAt: A.FV.serverTimestamp(),
    }, { merge: true });
    return { cik, error: e.code || "fetch_failed", entries: [] };
  }

  if (res.notModified) {
    await stRef.set({ lastSuccessAt: A.FV.serverTimestamp(), consecutiveFailures: 0, notModified: true }, { merge: true });
    return { cik, notModified: true, entries: [] };
  }

  const entries = parseAtomEntries(res.text);
  const seen = new Set(st.seenAccessions || []);
  const fresh = entries.filter((e) => e.accession && !seen.has(e.accession));

  await stRef.set({
    etag: res.etag, lastModified: res.lastModified,
    lastSuccessAt: A.FV.serverTimestamp(), consecutiveFailures: 0, lastError: null,
    seenAccessions: [...entries.map((e) => e.accession).filter(Boolean), ...seen].slice(0, 200),
    lastRawSha256: res.sha256,
  }, { merge: true });

  return { cik, notModified: false, entries: fresh, total: entries.length, sha256: res.sha256 };
}

/** Backfill and incrementally maintain the six-month filing spine from the
 * SEC submissions API. This is issuer-agnostic and uses only CIK identity;
 * no ticker or company-specific endpoint is hardcoded. */
async function pollSubmissionsHistory(cik, { lookbackDays = 180, limit = 80 } = {}) {
  if (!cik) return { cik, error: "missing_cik", entries: [] };
  const cik10 = String(cik).padStart(10, "0");
  const stateRef = A.col(A.COL.sourceState).doc(`sec.submissions_${cik10}`);
  const stateSnap = await stateRef.get(), state = stateSnap.exists ? stateSnap.data() : {};
  try {
    const res = await fetchPublic(`https://data.sec.gov/submissions/CIK${cik10}.json`, {
      sourceId: "sec.submissions", accept: ["json"], timeoutMs: 20000,
      maxBytes: 6 * 1024 * 1024,
    });
    const recent = res.json && res.json.filings && res.json.filings.recent || {};
    const minMs = Date.now() - Math.max(1, Math.min(366, Number(lookbackDays) || 180)) * 864e5;
    const rows = [];
    const n = Math.max(...["accessionNumber", "filingDate", "form", "primaryDocument"]
      .map((k) => Array.isArray(recent[k]) ? recent[k].length : 0));
    for (let i = 0; i < n; i += 1) {
      const accession = recent.accessionNumber && recent.accessionNumber[i];
      const filingDate = recent.filingDate && recent.filingDate[i];
      const accepted = recent.acceptanceDateTime && recent.acceptanceDateTime[i];
      const published = accepted || filingDate;
      const at = Date.parse(published || "");
      if (!accession || !Number.isFinite(at) || at < minMs || at > Date.now()) continue;
      const primary = recent.primaryDocument && recent.primaryDocument[i];
      const accessionCompact = String(accession).replace(/-/g, "");
      const link = primary
        ? `https://www.sec.gov/Archives/edgar/data/${Number(cik)}/${accessionCompact}/${encodeURIComponent(primary)}`
        : `https://www.sec.gov/Archives/edgar/data/${Number(cik)}/${accessionCompact}/`;
      rows.push({ title: cleanSubmissionTitle(recent, i), sourceId: "sec.submissions",
        form: recent.form && recent.form[i] || null,
        updated: published, summary: recent.primaryDocDescription && recent.primaryDocDescription[i]
          || recent.items && recent.items[i] || null,
        link, accession, filingDate, reportDate: recent.reportDate && recent.reportDate[i] || null });
    }
    rows.sort((a, b) => Date.parse(b.updated) - Date.parse(a.updated));
    const seen = new Set(state.seenAccessions || []);
    const bounded = rows.slice(0, Math.max(1, Math.min(240, Number(limit) || 80)));
    const fresh = bounded.filter((x) => !seen.has(x.accession));
    await stateRef.set({ sourceId: "sec.submissions", cik: String(cik),
      lastSuccessAt: A.FV.serverTimestamp(), lastSuccessAtMs: Date.now(),
      consecutiveFailures: 0, lastError: null, responseSha256: res.sha256,
      seenAccessions: uniqAccessions([...bounded.map((x) => x.accession), ...seen]).slice(0, 500),
      lookbackDays, matched: bounded.length }, { merge: true });
    return { cik, entries: fresh, total: bounded.length, sha256: res.sha256,
      notModified: fresh.length === 0, identity: { name: res.json.name || null,
        sic: res.json.sic || null, sicDescription: res.json.sicDescription || null,
        formerNames: res.json.formerNames || [] } };
  } catch (error) {
    await stateRef.set({ sourceId: "sec.submissions", cik: String(cik),
      lastAttemptAt: A.FV.serverTimestamp(),
      consecutiveFailures: Number(state.consecutiveFailures || 0) + 1,
      lastError: String(error.code || error.message).slice(0, 200) }, { merge: true });
    return { cik, error: error.code || "fetch_failed", entries: [] };
  }
}
function cleanSubmissionTitle(recent, i) {
  const form = recent.form && recent.form[i] || "SEC filing";
  const description = recent.primaryDocDescription && recent.primaryDocDescription[i];
  return `${form}${description ? ` — ${description}` : ""}`.slice(0, 500);
}
function uniqAccessions(values) { return [...new Set((values || []).filter(Boolean))]; }

/** Persist a document version. A version exists only when the bytes changed. */
async function recordVersion({ symbol, sourceId, entry, rawSha256, body = "", bodySha256 = null }) {
  const fetchedAtMs = Date.now();
  const sourceMeta = (entry && entry.sourceMeta) || SOURCES[sourceId] || {};
  /* One public article can concern several issuers. Symbol-bound identity
     prevents the first dossier to record it from stealing it from the rest. */
  const docId = `${String(symbol || "").toUpperCase()}_${sourceId}_${entry.accession || normalizedHash(entry.link || entry.title).hash.slice(0, 16)}`;
  const dref = A.col(A.COL.documents).doc(docId);
  const contentHash = bodySha256 || rawSha256 || normalizedHash(body || entry.summary || entry.title).hash;
  const versionId = `${docId}_${contentHash.slice(0, 24)}`, vref = A.col(A.COL.versions).doc(versionId);
  let created = false;
  await A.runTransaction(async (tx) => {
    const [d, v] = await Promise.all([tx.get(dref), tx.get(vref)]);
    created = !v.exists;
    if (!d.exists) tx.set(dref, {
      documentId: docId, symbol, sourceId, title: entry.title, form: entry.form,
      link: entry.link, accession: entry.accession, summary: entry.summary || null,
      source_published_at: entry.updated || null, first_seen_at: A.FV.serverTimestamp(),
      firstSeenAtMs: fetchedAtMs,
      publisherDomain: entry.publisherDomain || sourceMeta.publisher_domain || null,
      publisherGroup: entry.publisherGroup || sourceMeta.independence_group || sourceId,
      sourceClass: sourceMeta.source_class || entry.sourceClass || null,
      sourceTier: sourceMeta.tier || entry.sourceTier || null,
      sourceReliability: Number(sourceMeta.reliability ?? entry.sourceReliability) || null,
      discoveryOnly: sourceMeta.discovery_only === true || entry.discoveryOnly === true,
      latestVersionId: versionId, ...A.envelope({ created_by: "evidence.recordVersion" }),
    });
    else tx.set(dref, { latestVersionId: versionId, last_seen_at: A.FV.serverTimestamp() }, { merge: true });
    if (!v.exists) tx.set(vref, {
      versionId, documentId: docId, symbol, sourceId, raw_sha256: rawSha256 || null,
      canonical_content_sha256: contentHash, canonicalText: body || entry.summary || "",
      sourcePublishedAt: entry.updated || null,
      parserVersion: "sec-fulltext-v2", fetched_at: A.FV.serverTimestamp(), fetchedAtMs,
      permission_snapshot: { retention_rule: sourceMeta.retention_rule || "metadata_only",
        full_text_allowed: !!sourceMeta.full_text_allowed,
        terms_url: sourceMeta.terms_url || null },
      ...A.envelope({ created_by: "evidence.recordVersion" }),
    });
  });
  return { documentId: docId, versionId, contentHash, created };
}

/* ── CLAIMS (blueprint §5.3, §5.6) ───────────────────────────────────────
 * A claim is a source-bound fact extracted by Luna from a document version:
 * type, text, verbatim quote, and for guidance the metric, period and
 * bounds; for an earnings date the date and whether the issuer confirmed
 * it. Claims are versioned like any other fact: a later guidance release
 * SUPERSEDES an earlier one for the same metric and period, and a withdrawn
 * or reaffirmed guide is itself an event. Nothing here interprets a claim;
 * the compact card shows Sol the claim and its citation, or its absence. */
const CLAIM_TYPES = Object.freeze(["FACT", "GUIDANCE", "EARNINGS_DATE", "MANAGEMENT_CHANGE", "RISK_FACTOR", "CATALYST", "CONTRADICTION"]);
function claimKey(value) {
  return String(value || "").replace(/[‘’]/g, "'").replace(/[“”]/g, '"').replace(/\s+/g, " ").trim().toLowerCase();
}
function claimIdFor({ symbol, documentVersionId, claimType, quote }) {
  return "claim_" + crypto.createHash("sha256")
    .update(`${String(symbol).toUpperCase()}|${documentVersionId}|${claimType}|${claimKey(quote)}`).digest("hex").slice(0, 40);
}
function canonicalInt(v) { return typeof v === "string" && /^-?(0|[1-9][0-9]*)$/.test(v) ? v : null; }
/** Persist extracted claims for one document version. Idempotent by claim
 *  id; guidance supersession is linked within the symbol's stored claims. */
async function recordClaims({ symbol, documentId, documentVersionId, sourceId, claims = [], extractedBy = null,
  publishedAtMs = null, firstSeenAtMs = Date.now(), nowMs = Date.now(), admin = null }) {
  const DB = admin || A;
  const sym = String(symbol || "").toUpperCase();
  if (!sym || !documentVersionId) throw Object.assign(new Error("recordClaims: symbol and documentVersionId required"), { code: "CLAIM_IDENTITY_REQUIRED" });
  const written = [], skipped = [], superseded = [];
  const existingGuidance = claims.some((c) => c && c.claimType === "GUIDANCE")
    ? await claimsForCompany(sym, { asOfMs: Number.MAX_SAFE_INTEGER, claimTypes: ["GUIDANCE"], admin: DB }) : [];
  for (const c of claims) {
    if (!c || !CLAIM_TYPES.includes(c.claimType) || !c.quote || claimKey(c.quote).length < 12) { skipped.push({ reason: "invalid_claim", claimType: c && c.claimType }); continue; }
    const claimId = claimIdFor({ symbol: sym, documentVersionId, claimType: c.claimType, quote: c.quote });
    const ref = DB.col(DB.COL.claims).doc(claimId);
    const row = {
      kind: "claim", claimId, symbol: sym, documentId: documentId || null, documentVersionId, sourceId: sourceId || null,
      claimType: c.claimType, text: String(c.text || "").slice(0, 400), quote: String(c.quote).slice(0, 600),
      metric: c.metric ? String(c.metric).slice(0, 60) : null, periodLabel: c.periodLabel || c.effectivePeriod || null,
      normalizedLowMicros: require("./_investorDecisionContext").moneyMicros(c.lowValue, c.unit), normalizedHighMicros: require("./_investorDecisionContext").moneyMicros(c.highValue, c.unit),
      lowValue: canonicalInt(c.lowValue), highValue: canonicalInt(c.highValue), unit: c.unit ? String(c.unit).slice(0, 24) : null,
      date: c.date && /^\d{4}-\d{2}-\d{2}$/.test(c.date) ? c.date : null,
      confirmed: c.confirmed === true, sourceClass: c.sourceClass || null,
      publishedAtMs: Number.isFinite(Number(publishedAtMs)) ? Number(publishedAtMs) : null,
      firstSeenAtMs: Number(firstSeenAtMs) || Date.now(),
      knownAtMs:nowMs, extractedBy: extractedBy || null, supersedes: null, supersededBy: null,
      ...DB.envelope({ created_by: "evidence.recordClaims" }),
    };
    /* Guidance versioning: the newest statement for a metric and period
       supersedes the older one; both remain readable. */
    if (row.claimType === "GUIDANCE" && row.metric && row.periodLabel) {
      const prior = existingGuidance.filter((g) => g.claimId !== claimId && g.metric === row.metric
        && g.periodLabel === row.periodLabel && !g.supersededBy
        && (Number(g.publishedAtMs ?? g.firstSeenAtMs) || 0) <= (Number(row.publishedAtMs ?? row.firstSeenAtMs) || 0))
        .sort((a, b) => (Number(b.publishedAtMs ?? b.firstSeenAtMs) || 0) - (Number(a.publishedAtMs ?? a.firstSeenAtMs) || 0))[0];
      if (prior) {
        row.supersedes = prior.claimId;
        await DB.col(DB.COL.claims).doc(prior.claimId).set({ supersededBy: claimId, supersededAtMs: nowMs }, { merge: true });
        superseded.push({ claimId: prior.claimId, by: claimId });
      }
    }
    const outcome = await DB.runTransaction(async (tx) => {
      const cur = await tx.get(ref);
      if (cur.exists) return "exists";
      tx.set(ref, row);
      return "written";
    });
    if (outcome === "written") written.push(claimId); else skipped.push({ claimId, reason: "exists" });
  }
  return { symbol: sym, written, skipped, superseded };
}
/** Point-in-time claims: a claim is known from its publication time when
 *  stated, else from first-seen; never earlier. */
async function claimsForCompany(symbol, { asOfMs = Date.now(), claimTypes = null, limit = 400, admin = null } = {}) {
  const DB = admin || A;
  const sym = String(symbol || "").toUpperCase();
  const snap = await DB.col(DB.COL.claims).where("kind", "==", "claim").where("symbol", "==", sym).get();
  const rows = [];
  snap.forEach((d) => {
    const c = d.data();
    if (claimTypes && !claimTypes.includes(c.claimType)) return;
    const knownAt = Math.max(Number(c.publishedAtMs)||0, Number(c.firstSeenAtMs)||0, Number(c.knownAtMs)||0);
    if (!Number.isFinite(knownAt) || knownAt > Number(asOfMs)) return;
    rows.push(c.supersededAtMs > asOfMs ? { ...c, supersededBy:null, supersededAtMs:null } : c);
  });
  rows.sort((a, b) => (Number(b.publishedAtMs ?? b.firstSeenAtMs) || 0) - (Number(a.publishedAtMs ?? a.firstSeenAtMs) || 0));
  return rows.slice(0, limit);
}
/** PURE. The newest non-superseded guidance per metric and period. */
function latestGuidanceFrom(claims, { asOfMs = Date.now() } = {}) {
  const by = new Map();
  for (const c of claims || []) {
    if (c.claimType !== "GUIDANCE" || !c.metric) continue;
    const knownAt = Math.max(Number(c.publishedAtMs)||0, Number(c.firstSeenAtMs)||0, Number(c.knownAtMs)||0);
    if (!Number.isFinite(knownAt) || knownAt > asOfMs) continue;
    const key = `${c.metric}|${c.periodLabel || ""}`;
    const cur = by.get(key);
    if (!cur || Number(c.publishedAtMs ?? c.firstSeenAtMs) > Number(cur.publishedAtMs ?? cur.firstSeenAtMs)) by.set(key, c);
  }
  return [...by.values()].map((c) => ({ claimId: c.claimId, documentVersionId: c.documentVersionId, metric: c.metric,
    periodLabel: c.periodLabel, lowValue: c.lowValue, highValue: c.highValue, unit: c.unit,
    issuedAt: c.date || (c.publishedAtMs ? new Date(c.publishedAtMs).toISOString().slice(0, 10) : null),
    supersedes: c.supersedes || null, quote: c.quote }));
}
async function latestGuidance(symbol, { asOfMs = Date.now() } = {}) {
  return latestGuidanceFrom(await claimsForCompany(symbol, { asOfMs, claimTypes: ["GUIDANCE"] }), { asOfMs });
}
/** PURE. The issuer's own announced next earnings date, if any, else null;
 *  the caller falls back to the 8-K Item 2.02 cadence projection with
 *  confirmed:false and its wider window (§5.6). */
function nextEarningsFrom(claims, { asOfMs = Date.now() } = {}) {
  const today = new Date(asOfMs).toISOString().slice(0, 10);
  const candidates = (claims || []).filter((c) => c.claimType === "EARNINGS_DATE" && c.date && c.date >= today
    && Math.max(Number(c.publishedAtMs)||0, Number(c.firstSeenAtMs)||0, Number(c.knownAtMs)||0) <= asOfMs)
    .sort((a, b) => (Number(b.publishedAtMs ?? b.firstSeenAtMs) || 0) - (Number(a.publishedAtMs ?? a.firstSeenAtMs) || 0));
  const c = candidates[0];
  return c ? { date: c.date, confirmed: c.confirmed === true, claimId: c.claimId, documentVersionId: c.documentVersionId,
    sourceClass: c.sourceClass || "company_primary", quote: c.quote } : null;
}
async function nextEarningsClaim(symbol, { asOfMs = Date.now() } = {}) {
  return nextEarningsFrom(await claimsForCompany(symbol, { asOfMs, claimTypes: ["EARNINGS_DATE"] }), { asOfMs });
}
/** The stored spans a verifier needs: version id, canonical text, hashes. */
async function sourceSpans({ documentVersionIds = [] } = {}) {
  const out = {};
  for (const id of [...new Set(documentVersionIds)].slice(0, 64)) {
    const s = await A.col(A.COL.versions).doc(String(id)).get();
    if (!s.exists) { out[id] = null; continue; }
    const v = s.data();
    out[id] = { versionId: v.versionId, documentId: v.documentId, symbol: v.symbol, sourceId: v.sourceId,
      canonicalText: v.canonicalText || "", contentHash: v.canonical_content_sha256 || null,
      sourcePublishedAt: v.sourcePublishedAt || null, fetchedAtMs: v.fetchedAtMs || null };
  }
  return out;
}

/** Point-in-time company dossier. Unknown publication time never becomes
 *  backdated knowledge: such a document is eligible only from firstSeenAtMs. */
async function documentsForCompany(symbol, decisionAtMs, lookbackDays = 180, limit = 240, { admin = null } = {}) {
  const D=admin || A;
  const snap = await D.col(D.COL.documents).where("symbol", "==", symbol).get();
  const max = Number(decisionAtMs) || Date.now();
  const min = max - Math.max(1, Math.min(366, Number(lookbackDays) || 180)) * 864e5;
  const rows = [];
  for (const d of snap.docs) {
    const row = d.data();
    const published = Date.parse(row.source_published_at || "");
    const knownAt = Number(row.firstSeenAtMs);
    const eventAt = Number.isFinite(published) ? published : knownAt;
    if (!Number.isFinite(eventAt) || eventAt < min || eventAt > max || knownAt > max) continue;
    rows.push(row);
  }
  rows.sort((a, b) => {
    const at = Date.parse(a.source_published_at || "") || Number(a.firstSeenAtMs) || 0;
    const bt = Date.parse(b.source_published_at || "") || Number(b.firstSeenAtMs) || 0;
    return bt - at;
  });
  const boundedRows = rows.slice(0, Math.max(1, Math.min(500, Number(limit) || 240)));
  const wanted = new Set(boundedRows.map((x) => x.documentId));
  /* One symbol query replaces one version query per document while retaining
     the newest version that was actually known by the decision timestamp. */
  const versionSnap = await D.col(D.COL.versions).where("symbol", "==", symbol).get();
  const latest = new Map();
  versionSnap.forEach((v) => {
    const x = v.data(), seen = Number(x.fetchedAtMs);
    if (!wanted.has(x.documentId) || !Number.isFinite(seen) || seen > max) return;
    const prior = latest.get(x.documentId);
    if (!prior || seen > Number(prior.fetchedAtMs)) latest.set(x.documentId, x);
  });
  const docs = [];
  for (const row of boundedRows) {
    const version = latest.get(row.documentId);
    if (!version) continue;
    docs.push({ ...row, versionId: version.versionId,
      canonicalText: version.canonicalText || row.summary || "",
      canonicalContentSha256: version.canonical_content_sha256 || null,
      decisionKnownAtMs: Number(version.fetchedAtMs) });
  }
  return docs;
}

async function documentsForMove(symbol, moveStartedAtMs, decisionAtMs, lookbackHours = 72) {
  const snap = await A.col(A.COL.documents).where("symbol", "==", symbol).get();
  const min = Number(moveStartedAtMs) - lookbackHours * 3600e3, max = Number(decisionAtMs);
  const docs = [];
  for (const d of snap.docs) {
    const row = d.data(), published = Date.parse(row.source_published_at || "");
    if (Number.isFinite(published) && (published < min || published > max)) continue;
    const versions = await A.col(A.COL.versions).where("documentId", "==", row.documentId).get();
    let version = null;
    versions.forEach((v) => {
      const x = v.data(), seen = Number(x.fetchedAtMs);
      if (!Number.isFinite(seen) || seen > max) return;
      if (!version || seen > Number(version.fetchedAtMs)) version = x;
    });
    if (!version) continue; // not known to the system at decision time
    docs.push({ ...row, versionId: version.versionId,
      canonicalText: version ? version.canonicalText : row.summary || "",
      canonicalContentSha256: version ? version.canonical_content_sha256 : null });
  }
  return docs.sort((a,b)=>Date.parse(a.source_published_at||0)-Date.parse(b.source_published_at||0));
}

/* ── DETERMINISTIC PRE-CLASSIFIER ──────────────────────────────────────── */
/* Runs before any model call and resolves most cases outright. Only genuinely
   ambiguous ones reach the LLM, which is what keeps the bill at ~$40/mo. */
const FUNDAMENTAL_FORMS = new Set(["8-K", "10-Q", "10-K", "425", "DEFM14A", "S-4", "SC 13D", "SC 13D/A"]);
const FUNDAMENTAL_MARKERS = [
  /item\s*2\.02/i,           // results of operations
  /item\s*5\.02/i,           // officer / director departure
  /item\s*1\.01/i,           // material definitive agreement
  /item\s*4\.02/i,           // non-reliance / restatement
  /guidance|withdraw|restat|going concern|impairment|recall|investigation/i,
  /merger|acquisition|definitive agreement|tender offer/i,
  /strike|walkout|work stoppage|union vote|labor dispute|production halt|factory closure/i,
  /grounding|airworthiness directive|safety directive|certification delay|enforcement action/i,
  /firm order|order cancellation|contract award|contract termination|delivery delay|supply chain disruption/i,
];
const ROUTINE_MARKERS = [
  /item\s*7\.01/i,           // Reg FD
  /item\s*9\.01/i,           // exhibits only
  /rule\s*10b5-1/i,          // planned insider sale — mechanical, uninformative
];

/**
 * @returns {{cause:string, confidence:number, rationale:string, needsModel:boolean, drivers:object}}
 */
function preClassify({ symbol, freshDocs, residualZ, attentionScore, hoursSinceMove,
                       moveStartedAtMs, decisionAtMs, coverageComplete }) {
  const moveStart = Number(moveStartedAtMs), decision = Number(decisionAtMs);
  const drivers = { fundamentalDocs: [], routineDocs: [], attentionScore,
    moveStartedAtMs: moveStart || null, decisionAtMs: decision || null };

  if (coverageComplete !== true) return { cause: S.CAUSE.PENDING, confidence: 0,
    needsModel: false, drivers, rationale: "required cause-source coverage is incomplete" };

  for (const d of (freshDocs || [])) {
    const hay = `${d.title || ""} ${d.summary || ""} ${d.canonicalText || ""}`;
    const published = Date.parse(d.source_published_at || d.updated || "");
    const temporallyEligible = !Number.isFinite(published) || !Number.isFinite(decision)
      ? false : published <= decision && (!Number.isFinite(moveStart) || published >= moveStart - 72*3600e3);
    const routine = ROUTINE_MARKERS.some((r) => r.test(hay));
    const authoritativePublic = d.discoveryOnly !== true && ["A", "B"].includes(d.sourceTier);
    const fundamental = (FUNDAMENTAL_FORMS.has(d.form) || authoritativePublic)
      && FUNDAMENTAL_MARKERS.some((r) => r.test(hay));
    if (fundamental && !routine && temporallyEligible) drivers.fundamentalDocs.push({ form: d.form || d.sourceId, title: d.title,
      accession: d.accession, documentId: d.documentId, versionId: d.versionId,
      sourcePublishedAt: d.source_published_at || d.updated || null });
    else if (d.form) drivers.routineDocs.push({ form: d.form, title: d.title });
  }

  // A hard fundamental filing inside the move window: this is real repricing.
  if (drivers.fundamentalDocs.length) {
    return {
      cause: S.CAUSE.HARD_NEWS, confidence: 0.9, needsModel: false, drivers,
      rationale: `${drivers.fundamentalDocs.length} fundamental filing(s) within 48h: ` +
                 drivers.fundamentalDocs.map((d) => d.form).join(", "),
    };
  }

  // Nothing filed at all, and attention is elevated -> crowd flow.
  if (!drivers.fundamentalDocs.length && attentionScore >= 2.0) {
    return {
      cause: S.CAUSE.ABNORMAL_ACTIVITY, confidence: 0.7, needsModel: false, drivers,
      rationale: `no covered fundamental event in window; same-clock activity z ${attentionScore.toFixed(2)} elevated`,
    };
  }

  // Nothing filed, no attention spike -> genuinely uncaused within coverage.
  if (!drivers.fundamentalDocs.length && !drivers.routineDocs.length && attentionScore < 1.0) {
    return {
      cause: S.CAUSE.NONE, confidence: 0.6, needsModel: false, drivers,
      rationale: "no covered source published anything in the window and attention is normal",
    };
  }

  // Ambiguous: routine filings present, or attention in the middle band.
  return {
    cause: S.CAUSE.PENDING, confidence: 0, needsModel: true, drivers,
    rationale: "ambiguous — routine filings present or attention inconclusive; escalating to model",
  };
}

/** Source-coverage roster for the dashboard. "No cause" is only meaningful
 *  alongside an honest statement of what was actually watched and healthy. */
async function coverageRoster(symbol, cik) {
  const ids = ["sec.latest", "sec.submissions", "fed.register"];
  const snaps = await Promise.all(ids.map((id) =>
    A.col(A.COL.sourceState).doc(id === "sec.latest" ? `sec.latest_${cik}` : id).get()));
  const requiredOperational = new Set(["sec.latest"]);
  const lanes = ids.map((id, i) => {
    const d = snaps[i].exists ? snaps[i].data() : {};
    const fails = d.consecutiveFailures || 0;
    return {
      sourceId: id,
      name: (SOURCES[id] || {}).name || id,
      tier: (SOURCES[id] || {}).tier || "?",
      healthy: fails === 0 && !!d.lastSuccessAt,
      operationalForCause: requiredOperational.has(id),
      consecutiveFailures: fails,
      lastError: d.lastError || null,
      lastSuccessAt: d.lastSuccessAt ? d.lastSuccessAt.toDate().toISOString() : null,
    };
  });
  const operational = lanes.filter((l) => l.operationalForCause);
  const healthy = operational.filter((l) => l.healthy).length;
  return {
    symbol, lanes,
    healthyLanes: healthy, totalLanes: operational.length,
    complete: operational.length > 0 && healthy === operational.length,
    // Deliberately NOT a probability that the wider web is silent.
    statement: healthy === operational.length
      ? `All ${operational.length} operational cause lane(s) healthy. Absence is bounded to those lanes only.`
      : `${healthy}/${operational.length} operational cause lanes healthy — new risk is blocked.`,
  };
}

module.exports = {
  CLAIM_TYPES, claimIdFor, claimKey, recordClaims, claimsForCompany, latestGuidanceFrom, latestGuidance,
  nextEarningsFrom, nextEarningsClaim, sourceSpans,
  SOURCES, FORM_WEIGHT,
  parseAtomEntries, pollEdgar, pollSubmissionsHistory, filingPlainText, fetchFilingBody, recordVersion, documentsForMove,
  documentsForCompany, preClassify, coverageRoster,
};
