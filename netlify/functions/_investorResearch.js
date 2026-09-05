/*  netlify/functions/_investorResearch.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — focused research: packets, immutable memos and the bounded
 *  priority pool (blueprint §6.3, §6.5, §12.1, Appendix B).
 *
 *    latest(symbol)          — the newest immutable ResearchMemo for a symbol.
 *    buildPacket(...)        — the underwriting packet at the cutoff: the
 *                              dossier version and card, the typed delta since
 *                              the prior memo's dossier version, the pending
 *                              changes, the prior memo (interpretation layer,
 *                              by pointer and as read-only context), claims.
 *    persistImmutable(...)   — write one memo version; never overwritten.
 *    createPool().run(...)   — the ResearchPool: a bounded concurrent pool
 *                              (initial cap three) that always launches the
 *                              smallest unique priority first, checkpoints
 *                              each result, deprioritises only a SUFFIX of the
 *                              order under budget or deadline pressure after
 *                              every HOLDING_REQUIRED request, and waits at a
 *                              barrier before final synthesis. Response-array
 *                              or completion order has no authority.
 *
 *  A memo is an interpretation record (§5.3). It carries the model's factual
 *  premises by claim id and its inferences labelled INFERENCE; the claim
 *  verifier decides support independently. Nothing here is a decision.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const POLICY = require("./_investorPolicy");

const MEMO_SCHEMA = "research-memo.v1";
const PACKET_SCHEMA = "research-packet.v1";
const DEFAULT_CONCURRENCY = 3;

function sha(v) { return crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(POLICY.canonical(v))).digest("hex"); }
function db(admin) { return admin || A; }

/* ── memos ─────────────────────────────────────────────────────────────── */
async function latest(symbol, { admin = null, cutoffMs = Infinity } = {}) {
  const D = db(admin);
  const sym = String(symbol || "").toUpperCase();
  const snap = await D.col(D.COL.researchMemos).where("symbol", "==", sym).get();
  let best = null;
  snap.forEach((d) => { const m = d.data(); if (m && m.kind === "memo" && Number(m.createdAtMs || Date.parse(m.asOf)) <= cutoffMs && (!best || Number(m.researchVersion) > Number(best.researchVersion))) best = m; });
  return best;
}
async function history(symbol, { admin = null, cutoffMs = Infinity, openedAtMs = null } = {}) {
  const D = db(admin), list = [];
  const snap = await D.col(D.COL.researchMemos).where("symbol", "==", symbol).get();
  snap.forEach(d => { const m=d.data(); if (m.kind === "memo" && Number(m.createdAtMs || Date.parse(m.asOf)) <= cutoffMs) list.push(m); });
  list.sort((a,b)=>Number(a.researchVersion)-Number(b.researchVersion));
  const compact = m => m ? { memoId:m.memoId, researchVersion:m.researchVersion, directive:m.directive || null, asOf:m.asOf, memo:m.memo, verifiedValuation:m.verifiedValuation || null, label:"PRIOR_INTERPRETATION_NOT_EVIDENCE" } : null;
  const entries=list.filter(m=>m.proposedDecision === "BUY");
  const original=openedAtMs ? entries.filter(m=>Number(m.createdAtMs || Date.parse(m.asOf))<=openedAtMs).at(-1) : entries[0];
  return { original:compact(original || list[0]), latest:compact(list.at(-1)), revisions:list.slice(-6).map(compact), earlierRevisions:list.slice(0,-6).filter(m=>!openedAtMs || Number(m.createdAtMs || Date.parse(m.asOf))>=openedAtMs).map(m=>({memoId:m.memoId,asOf:m.asOf,decision:m.proposedDecision,thesisHealth:m.thesisHealth,thesis:m.memo && m.memo.mandate && m.memo.mandate.thesis || null,forecast:m.memo && m.memo.mandate && m.memo.mandate.forecast || null,mandateTerms:m.memo && m.memo.mandate && m.memo.mandate.action || null})), totalVersions:list.length };
}
function memoDocId(symbol, version, hash) { return `${symbol}_r${String(version).padStart(4, "0")}_${String(hash).slice(0, 16)}`; }

/** Persist one immutable memo version with its lineage and manifests. */
async function persistImmutable(result, { admin = null, managerRunId = null, directive = null, cutoffMs = null } = {}) {
  const D = db(admin);
  if (!result || result.ok !== true || !result.memo) {
    return { persisted: false, reason: result && result.error || "research_failed", symbol: result && result.symbol || null,
      decision: "ABSTAIN", reasonCode: (result && result.reasonCode) || "MODEL_FAILURE" };
  }
  const memo = result.memo;
  const symbol = String(memo.symbol).toUpperCase();
  const existing=[];
  (await D.col(D.COL.researchMemos).where("symbol","==",symbol).get()).forEach(d=>{const m=d.data();if(result.requestId && m.requestId===result.requestId && m.kind==="memo")existing.push(m);});
  if(existing.length){const m=existing[0];return {...m,persisted:true,duplicate:true,id:m.memoId,mandate:m.memo.mandate || null};}
  const prior = await latest(symbol, { admin });
  const researchVersion = (prior ? Number(prior.researchVersion) || 0 : 0) + 1;
  const outputHash = result.outputHash || sha(memo);
  const memoId = memoDocId(symbol, researchVersion, outputHash);
  const factualPremises = (memo.factualPremises || []).map((p) => ({ premiseId: p.premiseId, claimId: p.claimId, documentVersionId: p.documentVersionId, text: p.text, material: true }));
  const sourceManifest = [...new Map(factualPremises.map((p) => [p.claimId, { claimId: p.claimId, documentVersionId: p.documentVersionId }])).values()];
  const doc = {
    kind: "memo", schemaVersion: MEMO_SCHEMA, memoId, symbol, researchVersion, previousMemoId: prior ? prior.memoId : null,
    managerRunId, directive, cutoffMs, asOf: memo.asOf, proposedDecision: memo.proposedDecision, reasonCode: memo.reasonCode || null,
    thesisHealth: memo.thesisHealth, dossierVersionId: result.dossierVersionId || null, dossierHash: result.dossierHash || null,
    outputHash, requestId: result.requestId || null, responseId: result.responseId || null, model: result.model || null,
    verifiedValuation: result.verifiedValuation || null,
    usage: result.usage || null, costMinor: result.costMinor || null, toolCalls: (result.toolCalls || []).length,
    factualPremises, inferences: (memo.inferences || []).map((i) => ({ inferenceId: i.inferenceId, premiseIds: i.premiseIds, text: i.text, label: "INFERENCE" })),
    sourceManifest, sourceManifestHash: sha(sourceManifest), memo, createdAtMs: A.now(),
    ...D.envelope({ created_by: "research.persistImmutable" }),
  };
  const ref = D.col(D.COL.researchMemos).doc(memoId);
  const written = await D.runTransaction(async (tx) => { const cur = await tx.get(ref); if (cur.exists) return false; tx.set(ref, doc); return true; });
  /* the dossier pointer learns the interpretation layer by POINTER only */
  try { await D.col(D.COL.dossiers).doc(symbol).set({ researchMemoId: memoId, researchVersion, researchAtMs: A.now(), standingView: { status: memo.proposedDecision, researchVersion, decision: memo.proposedDecision, asOfMs: A.now() } }, { merge: true }); } catch {}
  return { persisted: true, duplicate: !written, memoId, id: memoId, symbol, researchVersion, outputHash, factualPremises, sourceManifest, sourceManifestHash: doc.sourceManifestHash,
    verifiedValuation: doc.verifiedValuation, memo, proposedDecision: memo.proposedDecision, reasonCode: memo.reasonCode || null, mandate: memo.mandate || null, dossierVersionId: doc.dossierVersionId };
}

/* ── packets ───────────────────────────────────────────────────────────── */
async function buildPacket({ symbol, cutoff, prior = null, directive = "RESEARCH_NOW", admin = null, deps = {}, portfolioBySymbol = {} } = {}) {
  const DOSSIER = deps.dossier || require("./_investorDossier");
  const E = deps.evidence || require("./_investorEvidence");
  const sym = String(symbol || "").toUpperCase();
  const cutoffMs = Number(cutoff && cutoff.cutoffMs != null ? cutoff.cutoffMs : cutoff) || A.now();
  const pointer = await DOSSIER.current(sym, { admin });
  const version = DOSSIER.versionAsOf ? await DOSSIER.versionAsOf(sym, { cutoffMs, admin, pointer }) : pointer && pointer.currentVersionId ? await DOSSIER.readVersion(pointer.currentVersionId, { admin }) : null;
  if (!version) return { ok: false, symbol: sym, reason: "no_dossier", cutoffMs };
  const priorMemo = deps.independentReview ? null : prior || await latest(sym, { admin, cutoffMs });
  const baseId = priorMemo && priorMemo.dossierVersionId;
  const base = baseId && baseId !== (version.versionId || DOSSIER.versionDocId(version)) ? await DOSSIER.readVersion(baseId, { admin }) : null;
  const delta = DOSSIER.buildDelta(base, version);
  const mk = await DOSSIER.marketInputs(sym, version.identity.sector, { cutoffMs, deps });
  const changes = await DOSSIER.pendingChanges(sym, { cutoffMs, admin }).catch(() => []);
  const card = DOSSIER.cardFromVersion(version, { cutoffMs, price: mk.price, sectorPrice: mk.sectorPrice, marketPrice: mk.marketPrice, changes,
    standingView: deps.independentReview ? null : pointer.standingView || null, portfolio: portfolioBySymbol[sym] || null });
  const claims = await E.claimsForCompany(sym, { asOfMs: cutoffMs, admin }).catch(() => []);
  const packet = {
    schemaVersion: PACKET_SCHEMA, symbol: sym, directive, cutoffMs, cutoff: new Date(cutoffMs).toISOString(),
    dossierVersionId: version.versionId || DOSSIER.versionDocId(version), dossierHash: version.contentHash, contentHash: version.contentHash,
    identity: version.identity, card, sectorBlock: version.sectorBlock, fundamentals: version.fundamentals, guidance: version.guidance, nextEarnings: version.nextEarnings,
    documents: (version.documents || []).slice(0, 40), claimIds: version.claimIds || [],
    claims: claims.slice(0, 120).map((c) => ({ claimId: c.claimId, claimType: c.claimType, text: c.text, quote: c.quote, documentVersionId: c.documentVersionId, metric: c.metric || null, periodLabel: c.periodLabel || null, lowValue: c.lowValue || null, highValue: c.highValue || null, unit: c.unit || null, date: c.date || null, confirmed: c.confirmed === true, publishedAtMs: c.publishedAtMs || null, supersedes: c.supersedes || null, supersededBy: c.supersededBy || null })),
    marketObservation: mk.observation || null, marketState: deps.frozenMarketState || await require("./_investorDecisionContext").marketState({ cards:[{symbol:sym,changes,marketObservation:mk.observation}], cutoffMs, deps }),
    learning: deps.independentReview ? null : await require("./_investorLearning").retrievalMemory({ symbol:sym, admin, cutoffMs }).catch(() => ({ unavailable:true })),
    pendingChanges: changes, delta: { ...delta, changes, claims: claims.slice(0,120) }, freshness: DOSSIER.freshnessMatrix(pointer, { nowMs: cutoffMs }),
    prior: priorMemo ? { memoId: priorMemo.memoId, researchVersion: priorMemo.researchVersion, asOf: priorMemo.asOf, proposedDecision: priorMemo.proposedDecision, thesisHealth: priorMemo.thesisHealth,
      dossierVersionId: priorMemo.dossierVersionId, memo: priorMemo.memo || null, label: "PRIOR_INTERPRETATION_NOT_EVIDENCE" } : null,
    dataQuality: version.dataQuality,
  };
  packet.packetHash = sha({ ...packet, cutoff: undefined });
  return { ok: true, ...packet };
}

/* ── the pool ──────────────────────────────────────────────────────────── */
/** PURE. Validate uniqueness and order requests by priority. */
function orderRequests(requests = []) {
  const seen = new Set();
  for (const r of requests) {
    const p = Number(r.researchPriority);
    if (!Number.isInteger(p) || p < 1) throw Object.assign(new Error(`research priority invalid for ${r.symbol}`), { code: "RESEARCH_PRIORITY_INVALID" });
    if (seen.has(p)) throw Object.assign(new Error(`research priority ${p} is not unique`), { code: "RESEARCH_PRIORITY_DUPLICATE" });
    seen.add(p);
  }
  const symbols = new Set();
  for (const r of requests) { if (symbols.has(r.symbol)) throw Object.assign(new Error(`duplicate research request for ${r.symbol}`), { code: "RESEARCH_REQUEST_DUPLICATE" }); symbols.add(r.symbol); }
  return [...requests].sort((a, b) => a.researchPriority - b.researchPriority);
}
/** PURE. Deferral may remove only a suffix, and only after every request in
 *  mustCompleteClasses. Returns { keep, deferred }. */
function deferSuffix(ordered, { maxJobs = Infinity, mustCompleteClasses = ["HOLDING_REQUIRED"] } = {}) {
  const lastMust = ordered.reduce((i, r, idx) => (mustCompleteClasses.includes(r.completionClass) ? idx : i), -1);
  const keepCount = Math.max(lastMust + 1, Math.min(ordered.length, Number.isFinite(maxJobs) ? maxJobs : ordered.length));
  return { keep: ordered.slice(0, keepCount), deferred: ordered.slice(keepCount).map((r) => ({ ...r, deferredReason: "budget_or_deadline_suffix" })) };
}
function createPool({ now = A.now } = {}) {
  async function run({ requests = [], concurrency = DEFAULT_CONCURRENCY, worker, maxJobs = Infinity, deadlineMs = null, mustCompleteClasses = ["HOLDING_REQUIRED"], onResult = null, budgetRemaining = null } = {}) {
    const ordered = orderRequests(requests);
    const { keep, deferred } = deferSuffix(ordered, { maxJobs, mustCompleteClasses });
    const completed = [], failed = [], deferredLate = [];
    const startedOrder = [];
    let next = 0;
    const cap = Math.max(1, Math.min(Number(POLICY.TOOL_POLICY.maxResearchConcurrency) || DEFAULT_CONCURRENCY, Number(concurrency) || DEFAULT_CONCURRENCY));
    const launch = async () => {
      while (next < keep.length) {
        const idx = next; next += 1;
        const request = keep[idx];
        /* pressure after all must-complete work: defer the remaining suffix, never the middle */
        const pastMust = keep.slice(idx).every((r) => !mustCompleteClasses.includes(r.completionClass));
        const overDeadline = deadlineMs != null && now() >= Number(deadlineMs);
        const overBudget = typeof budgetRemaining === "function" && budgetRemaining() === false;
        if (pastMust && (overDeadline || overBudget)) { deferredLate.push({ ...request, deferredReason: overDeadline ? "deadline" : "budget" }); continue; }
        startedOrder.push(request.researchPriority);
        try {
          const result = await worker(request);
          const row = { request, result, ok: !!(result && (result.ok === true || result.persisted === true)) };
          (row.ok ? completed : failed).push(row);
          if (typeof onResult === "function") { try { await onResult(row); } catch {} }
        } catch (e) { failed.push({ request, ok: false, error: String(e.code || e.message).slice(0, 160) }); }
      }
    };
    /* the barrier: every lane drains before this resolves */
    await Promise.all(Array.from({ length: cap }, () => launch()));
    const allDeferred = [...deferred, ...deferredLate];
    return {
      completed, failed, deferred: allDeferred, launchedOrder: startedOrder, concurrency: cap,
      ranges: { requested: range(ordered), completed: range(completed.map((r) => r.request)), deferred: range(allDeferred), failed: range(failed.map((r) => r.request)) },
      barrier: true,
    };
  }
  return { run, orderRequests, deferSuffix };
}
function range(list) {
  const ps = (list || []).map((r) => Number(r.researchPriority)).filter(Number.isFinite);
  return ps.length ? { from: Math.min(...ps), to: Math.max(...ps), count: ps.length } : { from: null, to: null, count: 0 };
}

module.exports = { MEMO_SCHEMA, PACKET_SCHEMA, DEFAULT_CONCURRENCY, latest, history, persistImmutable, buildPacket, createPool, orderRequests, deferSuffix, memoDocId };
