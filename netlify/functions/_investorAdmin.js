/*  netlify/functions/_investorAdmin.js  (v2.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — side-effect-free Firestore initializer + namespace guards.
 *
 *  WHY THIS FILE EXISTS INSTEAD OF `require("./firebaseAdmin")`
 *  ------------------------------------------------------------------------
 *  The legacy firebaseAdmin.js in this deployment calls
 *  bucket.setCorsConfiguration() on every cold start. setCorsConfiguration
 *  REPLACES the entire bucket CORS config — it does not merge. Importing it
 *  from Investor_AI would mean every investor cold start rewrites the CORS
 *  policy for the storefront, the design stations and the listing generator.
 *  It also cannot be extended safely: adding "investor.goldenspike.app" to
 *  its CORS_ORIGINS array changes behaviour for ~60 other functions.
 *
 *  This project uses only Firestore. The dedicated Google Cloud Firestore SDK
 *  avoids importing Firebase Auth and Cloud Storage (and their unused
 *  transitive dependency surface) while using the same service account.
 *
 *  REQUIRED ENVIRONMENT VARIABLES:
 *    FIREBASE_PROJECT_ID / FIREBASE_PRIVATE_KEY / FIREBASE_CLIENT_EMAIL
 *
 *  NAMESPACE ENFORCEMENT
 *  ------------------------------------------------------------------------
 *  A shared service credential can reach every collection in the project. That
 *  is a real, unavoidable blast radius under the no-new-credential rule. What
 *  we CAN do is make it impossible for investor code to touch legacy data by
 *  accident: every Firestore path must start with `InvestorAI_`, and no investor
 *  module is permitted to construct an unguarded client directly. This is
 *  collision containment, not a security
 *  boundary — it is documented as such in docs/threat-model.md.
 * ---------------------------------------------------------------------------
 */

"use strict";

const { Firestore, FieldValue, Timestamp } = require("@google-cloud/firestore");

const COL_PREFIX = "InvestorAI_";

/* ── credential (same three vars the rest of the deployment already uses) ── */
function serviceAccount() {
  if (!process.env.FIREBASE_PRIVATE_KEY) {
    throw new Error("_investorAdmin: FIREBASE_PRIVATE_KEY is not set in this environment");
  }
  if (!process.env.FIREBASE_PROJECT_ID || !process.env.FIREBASE_CLIENT_EMAIL) {
    throw new Error("_investorAdmin: FIREBASE_PROJECT_ID / FIREBASE_CLIENT_EMAIL are required");
  }
  return {
    type: "service_account",
    project_id: process.env.FIREBASE_PROJECT_ID,
    private_key: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, "\n"),
    client_email: process.env.FIREBASE_CLIENT_EMAIL,
  };
}

/* ── nested-array guard ────────────────────────────────────────────────────
 * Firestore refuses an array directly inside an array ("Property X contains
 * an invalid nested entity") and the whole write fails. One such value in a
 * 3,000-line run document reported every scan as incomplete for a day. Rather
 * than trust each of a few hundred write sites, every document write is
 * passed through this transform: an array found inside an array becomes an
 * object keyed by index ({"0": …, "1": …}), which Firestore accepts and which
 * loses no information. Plain objects and arrays are walked; FieldValue,
 * Timestamp, Date, Buffer and other class instances are left untouched. */
function isPlainObject(v) {
  if (v === null || typeof v !== "object") return false;
  const proto = Object.getPrototypeOf(v);
  return proto === Object.prototype || proto === null;
}
function firestoreSafe(value, insideArray = false, seen = new WeakSet(), depth = 0) {
  /* A cyclic or absurdly deep value is left for Firestore's own validation
     to reject with a normal error; this guard must never itself blow the
     stack, because the write sites run inside request handlers. */
  if (depth > 64) return value;
  if (Array.isArray(value)) {
    if (seen.has(value)) return value;
    seen.add(value);
    const items = value.map((x) => firestoreSafe(x, true, seen, depth + 1));
    if (!insideArray) return items;
    const out = {};
    items.forEach((x, i) => { out[String(i)] = x; });
    return out;
  }
  if (isPlainObject(value)) {
    if (seen.has(value)) return value;
    seen.add(value);
    const out = {};
    for (const [k, v] of Object.entries(value)) out[k] = firestoreSafe(v, false, seen, depth + 1);
    return out;
  }
  return value;
}
let _guarded = false;
function installNestedArrayGuard() {
  if (_guarded) return;
  _guarded = true;
  const { DocumentReference, Transaction, WriteBatch } = require("@google-cloud/firestore");
  const wrap = (proto, method, dataIndex) => {
    if (!proto || typeof proto[method] !== "function") return;
    const original = proto[method];
    proto[method] = function guarded(...args) {
      if (args.length > dataIndex && isPlainObject(args[dataIndex])) {
        try { args[dataIndex] = firestoreSafe(args[dataIndex]); }
        catch { /* fall through to the original write and its own validation */ }
      }
      return original.apply(this, args);
    };
  };
  wrap(DocumentReference.prototype, "set", 0);
  wrap(DocumentReference.prototype, "update", 0);
  wrap(DocumentReference.prototype, "create", 0);
  wrap(Transaction.prototype, "set", 1);
  wrap(Transaction.prototype, "update", 1);
  wrap(Transaction.prototype, "create", 1);
  wrap(WriteBatch.prototype, "set", 1);
  wrap(WriteBatch.prototype, "update", 1);
  wrap(WriteBatch.prototype, "create", 1);
  /* CollectionReference.add builds a DocumentReference and calls create on it,
     so it is covered by the wrap above. */
}

let _db = null;
function rawDb() {
  if (_db) return _db;
  const sa = serviceAccount();
  try { installNestedArrayGuard(); }
  catch (e) { console.error("_investorAdmin: nested-array guard not installed:", String(e && e.message || e)); }
  _db = new Firestore({
    projectId: sa.project_id,
    credentials: { client_email: sa.client_email, private_key: sa.private_key },
    ignoreUndefinedProperties: true,
  });
  return _db;
}

/* Keep the existing lazy callable contract: callers use A.FV.increment(x),
   A.FV.serverTimestamp(), and A.TS.fromDate(d). */
const FV = { get serverTimestamp() { return FieldValue.serverTimestamp; },
             get increment()       { return FieldValue.increment; },
             get arrayUnion()      { return FieldValue.arrayUnion; },
             get delete()          { return FieldValue.delete; } };
const TS = { get now()      { return Timestamp.now; },
             get fromDate() { return Timestamp.fromDate; } };

/* ── guarded accessors ─────────────────────────────────────────────────── */

/** Firestore collection, restricted to the InvestorAI_ namespace. */
function col(name) {
  const n = String(name || "");
  if (!n.startsWith(COL_PREFIX)) {
    throw new Error(`_investorAdmin.col: refused non-namespaced collection "${n}" (must start with ${COL_PREFIX})`);
  }
  if (n.includes("/")) {
    throw new Error(`_investorAdmin.col: refused path with separator "${n}" — use col(root).doc(id).collection(sub)`);
  }
  return rawDb().collection(n);
}

/** Firestore doc by "InvestorAI_Xxx/docId" path. */
function doc(path) {
  const p = String(path || "");
  if (!p.startsWith(COL_PREFIX)) {
    throw new Error(`_investorAdmin.doc: refused non-namespaced path "${p}"`);
  }
  const parts = p.split("/").filter(Boolean);
  if (parts.length % 2 !== 0) {
    throw new Error(`_investorAdmin.doc: "${p}" is a collection path, not a document path`);
  }
  return rawDb().doc(p);
}

/** Firestore transaction — callers still use col()/doc() for refs. */
function runTransaction(fn) { return rawDb().runTransaction(fn); }
function batch() { return rawDb().batch(); }

/* ── canonical collection names ────────────────────────────────────────── */
const COL = {
  control:    COL_PREFIX + "Control",       // mode, kill switch, budgets, active strategy
  universe:   COL_PREFIX + "Universe",      // frozen roster versions
  sources:    COL_PREFIX + "SourceRegistry",// permission + health per source
  sourceState:COL_PREFIX + "SourceState",   // cursors, ETags, last success
  jobs:       COL_PREFIX + "Jobs",          // leases, attempts, dead letters
  documents:  COL_PREFIX + "Documents",     // canonical document identity
  versions:   COL_PREFIX + "DocumentVersions",
  claims:     COL_PREFIX + "Claims",        // validated atomic claims + spans
  events:     COL_PREFIX + "Events",        // classified events
  intelligence:COL_PREFIX + "Intelligence", // latest point-in-time company dossiers
  marketLatest:COL_PREFIX+ "MarketLatest",  // one doc per symbol per day (bar array)
  marketFiles:COL_PREFIX + "MarketFiles",   // imported file manifests
  marketDaily:COL_PREFIX + "MarketDaily",   // long-horizon daily history + feed provenance
  candidates: COL_PREFIX + "Candidates",    // ranked, with visible factors
  decisions:  COL_PREFIX + "Decisions",     // immutable trade / no-trade
  accounts:   COL_PREFIX + "PaperAccounts",
  orders:     COL_PREFIX + "PaperOrders",
  fills:      COL_PREFIX + "PaperFills",
  positions:  COL_PREFIX + "Positions",
  trades:     COL_PREFIX + "Trades",
  ledger:     COL_PREFIX + "Ledger",        // immutable balanced journal
  strategies: COL_PREFIX + "StrategyVersions",
  runs:       COL_PREFIX + "Runs",          // cycle manifests
  costs:      COL_PREFIX + "Costs",         // OpenAI + friction meter
  audit:      COL_PREFIX + "Audit",
  sessions:   COL_PREFIX + "Sessions",      // operator sessions
  shadowDays: COL_PREFIX + "ShadowDays",
  shadowOpen: COL_PREFIX + "ShadowOpen",
  shadowClosed:COL_PREFIX + "ShadowClosed",
  shadowAccounts:COL_PREFIX + "ShadowAccounts", // self-financing policy NAV/cash/HWM
  shadowObservations:COL_PREFIX + "ShadowObservations", // trade + no-trade forward outcomes
  calibration:COL_PREFIX + "Calibration",
  invariants: COL_PREFIX + "Invariants",
  soakCycles: COL_PREFIX + "SoakCycles",    // idempotent production-paper evidence
  scanSnapshots:COL_PREFIX+ "ScanSnapshots", // per-scan signal panel + scoreboard, replayable
  archiveRuns: COL_PREFIX + "ArchiveRuns",   // nightly intraday archive manifests
  navMarks:   COL_PREFIX + "NavMarks",       // one doc per account-day: minute-level account value
  plans:      COL_PREFIX + "EntryPlans",     // armed entry levels written by the deep scan, struck by the guard

  /* ── AI Fund Manager collections (blueprint §10.2) ─────────────────────
     Small current pointers stay small; a version is appended only when its
     content hash changes; one logical decision per document (a run stores
     counters and hashes, never 304 rows). Large content lives in the
     private content bucket behind a ContentManifests pointer. */
  dossiers:            COL_PREFIX + "Dossiers",            // symbol: current pointer, freshness matrix, version/hash
  dossierVersions:     COL_PREFIX + "DossierVersions",     // symbol+version: immutable fact ids and memo/forecast pointers
  financialFacts:      COL_PREFIX + "FinancialFacts",      // issuer+concept+period+as-of: point-in-time XBRL fact
  evidenceDeltas:      COL_PREFIX + "EvidenceDeltas",      // symbol+from/to hashes: typed deltas, objective safety class
  managerRuns:         COL_PREFIX + "ManagerRuns",         // run id: immutable decision trace, counters, hashes
  managerDecisions:    COL_PREFIX + "ManagerDecisions",    // {managerRunId}_{symbol}: exactly one row per symbol
  claimVerifications:  COL_PREFIX + "ClaimVerifications",  // claim+verifier version: independent support verdict
  researchMemos:       COL_PREFIX + "ResearchMemos",       // symbol+version: immutable underwriting history
  portfolioPlans:      COL_PREFIX + "PortfolioPlans",      // plan id: RISK_MAINTENANCE | EXPANSION proposal and commit state
  activationSnapshots: COL_PREFIX + "ActivationSnapshots", // snapshot id: broker-reconciled truth used for CAS activation
  mandateProposals:    COL_PREFIX + "MandateProposals",    // proposal id/hash: strict Sol output only
  mandates:            COL_PREFIX + "Mandates",            // mandate version id: server binding and lineage
  activationEnvelopes: COL_PREFIX + "ActivationEnvelopes", // mandate version id: validator status and authorized quantity
  activeMandates:      COL_PREFIX + "ActiveMandates",      // account+symbol: desired/applied version pointers (CAS)
  mandateEvents:       COL_PREFIX + "MandateEvents",       // mandate+sequence: audit stream
  orderSets:           COL_PREFIX + "OrderSets",           // order-set id: desired/applied versions, broker group, state
  orderLegs:           COL_PREFIX + "OrderLegs",           // order-set+leg id: entry/target/stop/reduce/sell terms
  brokerEvents:        COL_PREFIX + "BrokerEvents",        // provider+account+event id: immutable normalized observations
  capitalReservations: COL_PREFIX + "CapitalReservations", // account+mandate version: reserved notional and risk
  reservationAccounts: COL_PREFIX + "ReservationAccounts", // account: CAS-protected aggregate ledger
  executionOutbox:     COL_PREFIX + "ExecutionOutbox",     // transition id: crash-resumable desired broker transition
  workerNonces:        COL_PREFIX + "WorkerNonces",        // job+attempt+nonce: atomic unused/consumed replay state
  emergencyPlans:      COL_PREFIX + "EmergencyPlans",      // account+symbol+version: protection-only authority
  emergencyRiskPolicies: COL_PREFIX + "EmergencyRiskPolicies", // account+policy version: owner-approved hard triggers
  alerts:              COL_PREFIX + "Alerts",              // condition id: typed alert, ack and resolution history
  mutations:           COL_PREFIX + "Mutations",           // actor+account+action+idempotency key: request hash and result
  calendarSnapshots:   COL_PREFIX + "CalendarSnapshots",   // venue+version/range: authoritative sessions
  corporateActions:    COL_PREFIX + "CorporateActions",    // provider/action id+version: quarantine and rebase lineage
  forecasts:           COL_PREFIX + "Forecasts",           // forecast id: horizon, distribution, later outcome
  counterfactuals:     COL_PREFIX + "Counterfactuals",     // decision+horizon: later return for selected and unselected
  postmortems:         COL_PREFIX + "Postmortems",         // decision/trade id: attribution and adjudication
  evals:               COL_PREFIX + "Evals",               // eval run id: challenger results and release gates
  kpiDaily:            COL_PREFIX + "KpiDaily",            // account+date: KPI aggregates
  contentManifests:    COL_PREFIX + "ContentManifests",    // content hash: pointer to object storage
  decisionInputs: COL_PREFIX + "DecisionInputs", // immutable chunked decision context
  modelRequests:       COL_PREFIX + "ModelRequests",       // request id: every model call's ids, status, hashes, tokens, cost (§12.2)
};

/* ── REQUIRED COMPOSITE INDEXES (blueprint §10.6) ────────────────────────
 * Declared here as data and surfaced by the health action; deployed through
 * whatever mechanism the operator already uses for Firestore. No writer
 * epoch flips while an index is building or missing (§17.5). */
const REQUIRED_INDEXES = Object.freeze([
  { collection: COL.modelRequests, fields: ["day", "role", "startedAtMs"] },
  { collection: COL.modelRequests, fields: ["status", "startedAtMs"] },
  { collection: COL.activeMandates, fields: ["accountId", "status", "expiresAtMs", "capitalRank"] },
  { collection: COL.managerRuns, fields: ["tradingDate", "status"] },
  { collection: COL.managerDecisions, fields: ["managerRunId", "decision", "symbol"] },
  { collection: COL.managerDecisions, fields: ["symbol", "asOfMs"] },
  { collection: COL.portfolioPlans, fields: ["accountId", "status", "asOfMs"] },
  { collection: COL.activationSnapshots, fields: ["accountId", "portfolioVersion", "asOfMs"] },
  { collection: COL.claimVerifications, fields: ["claimId", "verdict", "asOfMs"] },
  { collection: COL.dossierVersions, fields: ["symbol", "asOfMs"] },
  { collection: COL.evidenceDeltas, fields: ["symbol", "safetyClass", "createdAtMs"] },
  { collection: COL.evidenceDeltas, fields: ["managerMateriality", "reviewedAtMs"] },
  { collection: COL.mandateEvents, fields: ["mandateVersionId", "sequence"] },
  { collection: COL.mandateEvents, fields: ["accountId", "atMs"] },
  { collection: COL.forecasts, fields: ["horizonTradingDays", "resolutionAtMs"] },
  { collection: COL.counterfactuals, fields: ["horizonTradingDays", "resolutionAtMs"] },
  { collection: COL.versions, fields: ["symbol", "sourcePublishedAt", "fetchedAtMs"] },
  { collection: COL.orderSets, fields: ["accountId", "status"] },
  { collection: COL.orderSets, fields: ["accountId", "desiredMandateVersionId", "appliedMandateVersionId"] },
  { collection: COL.orderLegs, fields: ["orderSetId", "status", "role"] },
  { collection: COL.brokerEvents, fields: ["accountId", "providerCursor"] },
  { collection: COL.capitalReservations, fields: ["accountId", "status", "capitalRank"] },
  { collection: COL.reservationAccounts, fields: ["committedPortfolioPlanId", "version"] },
  { collection: COL.executionOutbox, fields: ["status", "nextAttemptAtMs"] },
  { collection: COL.alerts, fields: ["active", "severity", "updatedAtMs"] },
  { collection: COL.calendarSnapshots, fields: ["venue", "effectiveFrom", "version"] },
  { collection: COL.emergencyRiskPolicies, fields: ["accountId", "effectiveFromMs"] },
]);

/* ── ENGINE VERSIONS AND THE LEGACY FREEZE (blueprint §10.4, §13) ────────
 * Two engines share the InvestorAI_ namespace during the cutover: the
 * deterministic residual-reversal desk (legacy-v18) and the AI Fund Manager
 * (fund-manager-v1). Every record projected through the API is labelled
 * with the engine that produced it, and the legacy collections below are
 * frozen read-only after cutover. A legacy Candidate or EntryPlan is never
 * reinterpreted as an AI authorization. */
const ENGINE_VERSIONS = Object.freeze({ LEGACY: "legacy-v18", MANAGER: "fund-manager-v1" });
const LEGACY_COLLECTIONS = Object.freeze([
  COL.candidates, COL.decisions, COL.strategies,
  COL.shadowDays, COL.shadowOpen, COL.shadowClosed, COL.shadowAccounts, COL.shadowObservations,
  COL.calibration, COL.soakCycles, COL.scanSnapshots, COL.plans,
]);
function isLegacyCollection(name) { return LEGACY_COLLECTIONS.includes(String(name || "")); }
/** Label a record with the engine that produced it when it is projected
 *  through the API. A record that already declares an engine keeps it. */
function legacyProjection(record, { collection = null } = {}) {
  if (!record || typeof record !== "object") return record;
  if (record.engineVersion) return record;
  const legacy = collection ? isLegacyCollection(collection) : true;
  return { ...record, engineVersion: legacy ? ENGINE_VERSIONS.LEGACY : ENGINE_VERSIONS.MANAGER,
    decisionAuthority: record.decisionAuthority || (legacy ? "deterministic_v18" : "ai_manager") };
}

/* ── provenance envelope required on every generated record ────────────── */
function envelope(extra = {}) {
  return {
    created_at: FV.serverTimestamp(),
    created_by: extra.created_by || "investor-ai",
    app_commit: process.env.COMMIT_REF || process.env.DEPLOY_ID || "local",
    deploy_context: process.env.CONTEXT || "dev",
    ...extra,
  };
}

module.exports = {
  firestoreSafe, installNestedArrayGuard, rawDb,
  FV, TS, col, doc, runTransaction, batch,
  COL, COL_PREFIX,
  ENGINE_VERSIONS, LEGACY_COLLECTIONS, isLegacyCollection, legacyProjection,
  REQUIRED_INDEXES,
  envelope,
};
