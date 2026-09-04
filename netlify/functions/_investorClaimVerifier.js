/*  netlify/functions/_investorClaimVerifier.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — independent claim verification (blueprint §7.3, §17.2).
 *
 *  Every factual premise of an actionable mandate must resolve to a stored,
 *  verified source span. Two checks, kept deliberately apart:
 *
 *   1. SPAN IDENTITY (deterministic, here). The cited document version exists,
 *      the quote appears in it VERBATIM, and the span gets a stable id over
 *      (versionId, content hash, offset, normalised quote).
 *   2. INDEPENDENT SUPPORT (Luna, separate job). A structured verification
 *      request asks whether the immutable span SUPPORTS, CONTRADICTS or is
 *      INSUFFICIENT for the premise. The Sol generation that wrote the
 *      premise may NOT attest to itself; the verifier is a different model
 *      role with a different prompt and schema.
 *
 *  The verdict record is immutable and hashed. The activation transaction
 *  (_investorMandate.stagePortfolioPlan) verifies only those ids and hashes
 *  inside its retryable transaction — no network, no model. A missing
 *  verifier is INSUFFICIENT, never SUPPORTED: the absence of a verdict is not
 *  a verdict (invariant I-1). A CONTRADICTED material premise forces ABSTAIN
 *  or operator review; it never activates.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const POLICY = require("./_investorPolicy");

const VERIFIER_VERSION = "claim-verifier.v1";
const VERDICTS = POLICY.VERIFICATION_VERDICTS;   // SUPPORTED | CONTRADICTED | INSUFFICIENT
const MIN_QUOTE_CHARS = 12;
const HUMAN_SAMPLE_PPM = 20000;                  // 2% of verdicts sampled for human review

function sha256(v) { return crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(POLICY.canonical(v))).digest("hex"); }
function normalize(s) {
  return String(s || "").replace(/[‘’]/g, "'").replace(/[“”]/g, '"').replace(/\s+/g, " ").trim().toLowerCase();
}

/** PURE. Does the quote appear verbatim in the version's canonical text? */
function spanIdentity({ premise, version }) {
  if (!premise || !premise.quote) return { ok: false, reason: "no_quote" };
  if (!version || typeof version.canonicalText !== "string") return { ok: false, reason: "document_version_missing" };
  const needle = normalize(premise.quote), hay = normalize(version.canonicalText);
  if (needle.length < MIN_QUOTE_CHARS) return { ok: false, reason: "quote_too_short" };
  const offset = hay.indexOf(needle);
  if (offset < 0) return { ok: false, reason: "quote_not_found_verbatim" };
  const spanId = "span_" + sha256(`${version.versionId}|${version.contentHash || ""}|${offset}|${needle}`).slice(0, 40);
  return { ok: true, spanId, offset, length: needle.length, versionId: version.versionId, contentHash: version.contentHash || null };
}

/** PURE. Premises are the factual claims a proposal cites. Inferences are
 *  labelled INFERENCE and are never sent for factual verification. */
function premisesFromProposal(proposal, claimsById = {}) {
  const ids = [...new Set([...(proposal.thesis && proposal.thesis.evidenceFor || []),
    ...(proposal.thesis && proposal.thesis.evidenceAgainst || [])])];
  const manifest = new Map((proposal.sourceManifest || []).map((m) => [m.claimId, m]));
  return ids.map((claimId) => {
    const claim = claimsById[claimId] || null;
    const m = manifest.get(claimId) || {};
    return { premiseId: claimId, claimId, documentVersionId: (claim && claim.documentVersionId) || m.documentVersionId || null,
      text: claim ? claim.text : null, quote: claim ? claim.quote : null, material: true,
      side: (proposal.thesis.evidenceFor || []).includes(claimId) ? "for" : "against" };
  });
}

function verdictRecordId(claimId, premiseHash) { return `${claimId}_${VERIFIER_VERSION}_${premiseHash.slice(0, 16)}`; }
/** PURE. A deterministic 2% human-review sample by hash prefix. */
function sampledForHuman(verdictHash) { return parseInt(verdictHash.slice(0, 6), 16) % 1000000 < HUMAN_SAMPLE_PPM; }

/** Build immutable verdict records from span checks and verifier output. */
function composeVerdicts({ premises, spans, verifierOutput, model = null, usage = null, latencyMs = null, verifierAvailable = true }) {
  const byPremise = new Map((verifierOutput && verifierOutput.verdicts || []).map((v) => [v.claimId, v]));
  return premises.map((p) => {
    const span = spans[p.premiseId] || { ok: false, reason: "span_not_checked" };
    let verdict, reason = null, spanIds = [];
    if (!span.ok) { verdict = "INSUFFICIENT"; reason = `span_identity:${span.reason}`; }
    else if (!verifierAvailable) { verdict = "INSUFFICIENT"; reason = "verifier_unavailable"; }
    else {
      const v = byPremise.get(p.claimId);
      if (!v || !VERDICTS.includes(v.verdict)) { verdict = "INSUFFICIENT"; reason = "verifier_returned_no_verdict"; }
      else { verdict = v.verdict; spanIds = v.spanIds || [span.spanId]; reason = v.note || null; }
    }
    const premiseHash = sha256({ claimId: p.claimId, documentVersionId: p.documentVersionId, quote: normalize(p.quote), text: p.text });
    const core = { schemaVersion: "claim-verification.v1", verifierVersion: VERIFIER_VERSION, claimId: p.claimId,
      premiseId: p.premiseId, premiseHash, documentVersionId: p.documentVersionId, spanId: span.ok ? span.spanId : null,
      spanIdentityOk: span.ok === true, verdict, reason, spanIds, material: p.material !== false, side: p.side || null,
      model, verifierAvailable };
    const verdictHash = sha256(core);
    return { ...core, verdictHash, recordId: verdictRecordId(p.claimId, premiseHash),
      usage: usage || null, latencyMs, humanSample: sampledForHuman(verdictHash), humanVerdict: null, disagreement: null,
      asOfMs: Date.now() };
  });
}

/** PURE. The activation gate reads this: every MATERIAL premise supported,
 *  no contradicted premise, no missing verdict. */
function summarize(verdicts) {
  const material = verdicts.filter((v) => v.material !== false);
  const contradicted = material.filter((v) => v.verdict === "CONTRADICTED").map((v) => v.claimId);
  const insufficient = material.filter((v) => v.verdict === "INSUFFICIENT").map((v) => v.claimId);
  const supported = material.filter((v) => v.verdict === "SUPPORTED").map((v) => v.claimId);
  return { total: verdicts.length, material: material.length, supported: supported.length,
    contradictedClaimIds: contradicted, insufficientClaimIds: insufficient,
    allSupported: material.length > 0 && contradicted.length === 0 && insufficient.length === 0,
    blocking: contradicted.length > 0 || insufficient.length > 0,
    forceAbstain: contradicted.length > 0,
    reason: contradicted.length ? "CONTRADICTED_MATERIAL_PREMISE" : insufficient.length ? "UNSUPPORTED_MATERIAL_PREMISE" : (material.length ? null : "NO_MATERIAL_PREMISES") };
}

/* ── the default independent verifier: Luna through the gateway ────────── */
async function defaultVerifier({ premises, spans }) {
  let O;
  try { O = require("./_investorOpenai"); } catch { return { available: false, reason: "gateway_unavailable" }; }
  if (typeof O.verifyClaimsIndependently !== "function") return { available: false, reason: "verifier_role_unavailable" };
  const out = await O.verifyClaimsIndependently({ claimPremises: premises, immutableSourceSpans: spans, schemaVersion: "claim-verification.v1" });
  if (!out || out.ok !== true) return { available: false, reason: (out && out.error) || "verifier_failed", usage: out && out.usage || null };
  return { available: true, output: out.output, model: out.model || null, usage: out.usage || null, latencyMs: out.latencyMs || null };
}

/** Verify a set of premises against stored document versions, persist the
 *  immutable verdict records, and return the gate summary. */
async function verifyAndPersist({ premises, sourceManifest = [], verifier = null, spanReader = null, persist = true, admin = null }) {
  const DB = admin || A;
  const E = require("./_investorEvidence");
  const readSpans = spanReader || ((ids) => E.sourceSpans({ documentVersionIds: ids }));
  const versionIds = [...new Set(premises.map((p) => p.documentVersionId).filter(Boolean))];
  const versions = versionIds.length ? await readSpans(versionIds) : {};
  const spans = {};
  for (const p of premises) spans[p.premiseId] = spanIdentity({ premise: p, version: p.documentVersionId ? versions[p.documentVersionId] : null });
  const eligible = premises.filter((p) => spans[p.premiseId].ok);
  let verifierResult = { available: false, reason: "no_eligible_premises" };
  if (eligible.length) {
    const immutable = eligible.map((p) => ({ premiseId: p.premiseId, claimId: p.claimId, spanId: spans[p.premiseId].spanId,
      documentVersionId: p.documentVersionId, quote: p.quote, text: p.text,
      contextText: String((versions[p.documentVersionId] || {}).canonicalText || "").slice(0, 8000) }));
    try { verifierResult = await (verifier || defaultVerifier)({ premises: eligible, spans: immutable }); }
    catch (e) { verifierResult = { available: false, reason: `verifier_threw:${String(e.message).slice(0, 80)}` }; }
  }
  const verdicts = composeVerdicts({ premises, spans, verifierOutput: verifierResult.output || null,
    model: verifierResult.model || null, usage: verifierResult.usage || null, latencyMs: verifierResult.latencyMs || null,
    verifierAvailable: verifierResult.available === true });
  if (persist) {
    const SC = require("./_investorStorageCodec");
    for (const v of verdicts) {
      const ref = DB.col(DB.COL.claimVerifications).doc(v.recordId);
      await DB.runTransaction(async (tx) => {
        const cur = await tx.get(ref);
        if (cur.exists) return;      // immutable: the first verdict for this premise stands
        let doc = { ...v, ...DB.envelope({ created_by: "claimVerifier" }) };
        try { doc = SC.codecFor("claim-verification.v1") ? SC.encode(doc) : doc; } catch { /* unregistered shape: store plain */ }
        tx.set(ref, doc);
      });
    }
  }
  return { verdicts, summary: summarize(verdicts), verifier: { available: verifierResult.available === true, reason: verifierResult.reason || null },
    sourceManifest, spans };
}

/** Verify every actionable proposal's premises; returns per-proposal gates. */
async function verifyAndPersistBatch({ proposals = [], claimsById = null, verifier = null, spanReader = null, persist = true, admin = null }) {
  const DB = admin || A;
  const byProposal = {};
  let lookup = claimsById;
  if (!lookup) {
    lookup = {};
    const ids = [...new Set(proposals.flatMap((p) => [...(p.thesis && p.thesis.evidenceFor || []), ...(p.thesis && p.thesis.evidenceAgainst || [])]))];
    for (const id of ids.slice(0, 200)) {
      try { const s = await DB.col(DB.COL.claims).doc(id).get(); if (s.exists) lookup[id] = s.data(); } catch {}
    }
  }
  for (const proposal of proposals) {
    const premises = premisesFromProposal(proposal, lookup);
    const r = await verifyAndPersist({ premises, sourceManifest: proposal.sourceManifest || [], verifier, spanReader, persist, admin: DB });
    byProposal[proposal.symbol] = { symbol: proposal.symbol, decision: proposal.decision, ...r.summary,
      verdictIds: r.verdicts.map((v) => ({ recordId: v.recordId, verdictHash: v.verdictHash, verdict: v.verdict })) };
  }
  const symbols = Object.keys(byProposal);
  return { byProposal, allSupported: symbols.every((s) => byProposal[s].allSupported),
    blockingSymbols: symbols.filter((s) => byProposal[s].blocking), forceAbstainSymbols: symbols.filter((s) => byProposal[s].forceAbstain) };
}

/** Inside the activation transaction: only ids, hashes and current status.
 *  Throws when any required verdict is missing, altered, or not SUPPORTED. */
async function assertImmutableClaimVerdicts(tx, verified, { requireSupported = true, admin = null } = {}) {
  const DB = admin || A;
  const entries = Object.values(verified && verified.byProposal || {}).flatMap((p) => p.verdictIds || []);
  for (const e of entries) {
    const snap = await tx.get(DB.col(DB.COL.claimVerifications).doc(e.recordId));
    if (!snap.exists) throw Object.assign(new Error(`claim verdict ${e.recordId} missing`), { code: "CLAIM_VERDICT_MISSING" });
    const d = snap.data();
    const stored = d._codec ? require("./_investorStorageCodec").decode(d) : d;
    if (stored.verdictHash !== e.verdictHash) throw Object.assign(new Error(`claim verdict ${e.recordId} altered`), { code: "CLAIM_VERDICT_ALTERED" });
    if (requireSupported && stored.material !== false && stored.verdict !== "SUPPORTED") {
      throw Object.assign(new Error(`claim ${stored.claimId} is ${stored.verdict}`), { code: "CLAIM_NOT_SUPPORTED" });
    }
  }
  return { checked: entries.length };
}

module.exports = {
  VERIFIER_VERSION, VERDICTS, MIN_QUOTE_CHARS, HUMAN_SAMPLE_PPM,
  normalize, spanIdentity, premisesFromProposal, composeVerdicts, summarize, sampledForHuman,
  verifyAndPersist, verifyAndPersistBatch, assertImmutableClaimVerdicts, defaultVerifier,
};
