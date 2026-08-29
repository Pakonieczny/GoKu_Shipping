/*  netlify/functions/_investorEvidence.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — evidence lane. EDGAR ingestion, document versioning, and the
 *  three-state cause classification that decides fade-or-skip.
 *
 *  THE INVERTED TRIGGER — why this is cheap enough to run every 5 minutes
 *  ------------------------------------------------------------------------
 *  The naive design polls every source on every cycle and hands everything to
 *  a model: ~50 unconditional LLM calls per cycle around the clock costs about
 *  $285/month, roughly ten times the rest of the stack combined. This module
 *  inverts the trigger. The price loop detects an abnormal RESIDUAL move
 *  first; only then does the system go looking for a cause. Filings are polled
 *  cheaply (EDGAR's own API updates in under a second and we use 0.003 of its
 *  10 req/s ceiling), but the model is invoked only on a threshold breach or a
 *  genuinely new document version. Same answers, roughly a tenth the cost.
 *
 *  THE THREE STATES — and why they are the whole strategy
 *  ------------------------------------------------------------------------
 *  Ben-Rephael, Da, Easton & Israelsen studied 114,468 single-item 8-Ks and
 *  found the same filing produces opposite outcomes depending on WHO noticed:
 *  filings drawing institutional attention show no under- or over-reaction
 *  (the information is already in the price), while filings drawing retail
 *  attention reverse, peaking days t+7..t+8. So:
 *
 *    cause_detected_fundamental  a real repricing -> DO NOT FADE
 *    attention_driven            crowd flow, no new fundamentals -> FADE
 *    no_cause_detected           liquidity provision -> FADE, reduced size
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
const { fetchPublic, normalizedHash } = require("./_investorFetch");
const S = require("./_investorSignal");

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
    const formM = title.match(/^([A-Z0-9\-\/]+)\s+-\s+/);
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

/** Persist a document version. A version exists only when the bytes changed. */
async function recordVersion({ symbol, sourceId, entry, rawSha256 }) {
  const docId = `${sourceId}_${entry.accession || normalizedHash(entry.link || entry.title).hash.slice(0, 16)}`;
  const dref = A.col(A.COL.documents).doc(docId);
  await dref.set({
    documentId: docId, symbol, sourceId,
    title: entry.title, form: entry.form, link: entry.link,
    accession: entry.accession,
    source_published_at: entry.updated || null,
    first_seen_at: A.FV.serverTimestamp(),
    ...A.envelope({ created_by: "evidence.recordVersion" }),
  }, { merge: true });

  const vref = A.col(A.COL.versions).doc(`${docId}_${(rawSha256 || "0").slice(0, 16)}`);
  await vref.set({
    documentId: docId, symbol, sourceId,
    raw_sha256: rawSha256 || null,
    fetched_at: A.FV.serverTimestamp(),
    permission_snapshot: {
      retention_rule: (SOURCES[sourceId] || {}).retention_rule || "metadata_only",
      full_text_allowed: !!(SOURCES[sourceId] || {}).full_text_allowed,
      terms_url: (SOURCES[sourceId] || {}).terms_url || null,
    },
    ...A.envelope({ created_by: "evidence.recordVersion" }),
  }, { merge: true });

  return { documentId: docId, versionId: vref.id };
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
];
const ROUTINE_MARKERS = [
  /item\s*7\.01/i,           // Reg FD
  /item\s*9\.01/i,           // exhibits only
  /rule\s*10b5-1/i,          // planned insider sale — mechanical, uninformative
];

/**
 * @returns {{cause:string, confidence:number, rationale:string, needsModel:boolean, drivers:object}}
 */
function preClassify({ symbol, freshDocs, residualZ, attentionScore, hoursSinceMove }) {
  const drivers = { fundamentalDocs: [], routineDocs: [], attentionScore, hoursSinceMove };

  for (const d of (freshDocs || [])) {
    const hay = `${d.title || ""} ${d.summary || ""}`;
    const routine = ROUTINE_MARKERS.some((r) => r.test(hay));
    const fundamental = FUNDAMENTAL_FORMS.has(d.form) && FUNDAMENTAL_MARKERS.some((r) => r.test(hay));
    if (fundamental && !routine) drivers.fundamentalDocs.push({ form: d.form, title: d.title, accession: d.accession });
    else if (d.form) drivers.routineDocs.push({ form: d.form, title: d.title });
  }

  // A hard fundamental filing inside the move window: this is real repricing.
  if (drivers.fundamentalDocs.length && hoursSinceMove <= 48) {
    return {
      cause: S.CAUSE.HARD_NEWS, confidence: 0.9, needsModel: false, drivers,
      rationale: `${drivers.fundamentalDocs.length} fundamental filing(s) within 48h: ` +
                 drivers.fundamentalDocs.map((d) => d.form).join(", "),
    };
  }

  // Nothing filed at all, and attention is elevated -> crowd flow.
  if (!drivers.fundamentalDocs.length && attentionScore >= 2.0) {
    return {
      cause: S.CAUSE.ATTENTION, confidence: 0.7, needsModel: false, drivers,
      rationale: `no fundamental filing in window; attention z ${attentionScore.toFixed(2)} elevated`,
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
  const lanes = ids.map((id, i) => {
    const d = snaps[i].exists ? snaps[i].data() : {};
    const fails = d.consecutiveFailures || 0;
    return {
      sourceId: id,
      name: (SOURCES[id] || {}).name || id,
      tier: (SOURCES[id] || {}).tier || "?",
      healthy: fails === 0 && !!d.lastSuccessAt,
      consecutiveFailures: fails,
      lastError: d.lastError || null,
      lastSuccessAt: d.lastSuccessAt ? d.lastSuccessAt.toDate().toISOString() : null,
    };
  });
  const healthy = lanes.filter((l) => l.healthy).length;
  return {
    symbol, lanes,
    healthyLanes: healthy, totalLanes: lanes.length,
    complete: healthy === lanes.length,
    // Deliberately NOT a probability that the wider web is silent.
    statement: healthy === lanes.length
      ? `All ${lanes.length} covered lanes healthy. Absence of a cause here is absence within these lanes only.`
      : `${healthy}/${lanes.length} lanes healthy — coverage is degraded and "no cause" is correspondingly weaker.`,
  };
}

module.exports = {
  SOURCES, FORM_WEIGHT,
  parseAtomEntries, pollEdgar, recordVersion,
  preClassify, coverageRoster,
};
