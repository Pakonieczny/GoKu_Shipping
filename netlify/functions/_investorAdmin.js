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

let _db = null;
function rawDb() {
  if (_db) return _db;
  const sa = serviceAccount();
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
};

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
  FV, TS, col, doc, runTransaction, batch,
  COL, COL_PREFIX,
  envelope,
};
