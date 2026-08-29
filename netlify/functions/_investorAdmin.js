/*  netlify/functions/_investorAdmin.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — side-effect-free Firebase Admin initializer + namespace guards.
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
 *  So this module initializes its own NAMED firebase-admin app from the SAME
 *  three environment variables (no new credential) and never touches CORS,
 *  never mutates bucket policy, and never writes outside its own namespace.
 *
 *  ZERO NEW ENVIRONMENT VARIABLES:
 *    FIREBASE_PROJECT_ID / FIREBASE_PRIVATE_KEY / FIREBASE_CLIENT_EMAIL
 *    FIREBASE_STORAGE_BUCKET (optional, same default as legacy)
 *
 *  NAMESPACE ENFORCEMENT
 *  ------------------------------------------------------------------------
 *  A shared Admin credential can reach every collection in the project. That
 *  is a real, unavoidable blast radius under the no-new-credential rule. What
 *  we CAN do is make it impossible for investor code to touch legacy data by
 *  accident: every Firestore path must start with `InvestorAI_` and every
 *  Storage object key must start with `investor-ai/`. col() and obj() throw
 *  on anything else, and no investor module is permitted to call
 *  admin.firestore() directly. This is collision containment, not a security
 *  boundary — it is documented as such in docs/threat-model.md.
 * ---------------------------------------------------------------------------
 */

"use strict";

const admin = require("firebase-admin");

const APP_NAME       = "investor-ai";
const COL_PREFIX     = "InvestorAI_";
const STORAGE_PREFIX = "investor-ai/";

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

function normalizeBucket(v) {
  const s = String(v || "").trim();
  if (!s) return "";
  return s.replace(/^gs:\/\//i, "")
          .replace(/^https?:\/\/storage\.googleapis\.com\//i, "")
          .replace(/\/.+$/, "");
}

const BUCKET = normalizeBucket(process.env.FIREBASE_STORAGE_BUCKET) || "gokudatabase.firebasestorage.app";

/* A NAMED app. admin.apps is shared across the whole lambda, so if a legacy
   module has already initialized the default app we must not collide with it,
   and we must not inherit its configuration either. */
let _app = null;
function app() {
  if (_app) return _app;
  const existing = admin.apps.find((a) => a && a.name === APP_NAME);
  if (existing) { _app = existing; return _app; }
  _app = admin.initializeApp(
    { credential: admin.credential.cert(serviceAccount()), storageBucket: BUCKET },
    APP_NAME
  );
  return _app;
}
/* NOTE: no setCorsConfiguration call anywhere in this file, by design. */

/* firebase-admin exposes FieldValue/Timestamp on the firestore namespace only
   after that namespace is touched. Reading them at module load crashed on
   admin v13. They are lazy getters so importing this file stays side-effect
   free — which is the entire point of the module. */
const FV = { get serverTimestamp() { return admin.firestore.FieldValue.serverTimestamp; },
             get increment()       { return admin.firestore.FieldValue.increment; },
             get arrayUnion()      { return admin.firestore.FieldValue.arrayUnion; },
             get delete()          { return admin.firestore.FieldValue.delete; } };
const TS = { get now()      { return admin.firestore.Timestamp.now; },
             get fromDate() { return admin.firestore.Timestamp.fromDate; } };

function rawDb() { return app().firestore(); }

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

/** Storage object handle, restricted to the investor-ai/ prefix. */
function obj(key) {
  const k = String(key || "");
  if (!k.startsWith(STORAGE_PREFIX)) {
    throw new Error(`_investorAdmin.obj: refused key "${k}" outside ${STORAGE_PREFIX}`);
  }
  if (k.includes("..")) {
    throw new Error(`_investorAdmin.obj: refused traversal in key "${k}"`);
  }
  return app().storage().bucket(BUCKET).file(k);
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
  marketLatest:COL_PREFIX+ "MarketLatest",  // one doc per symbol per day (bar array)
  marketFiles:COL_PREFIX + "MarketFiles",   // imported file manifests
  candidates: COL_PREFIX + "Candidates",    // ranked, with visible factors
  decisions:  COL_PREFIX + "Decisions",     // immutable trade / no-trade
  accounts:   COL_PREFIX + "PaperAccounts",
  orders:     COL_PREFIX + "PaperOrders",
  fills:      COL_PREFIX + "PaperFills",
  positions:  COL_PREFIX + "Positions",
  ledger:     COL_PREFIX + "Ledger",        // immutable balanced journal
  strategies: COL_PREFIX + "StrategyVersions",
  runs:       COL_PREFIX + "Runs",          // cycle manifests
  costs:      COL_PREFIX + "Costs",         // OpenAI + friction meter
  audit:      COL_PREFIX + "Audit",
  sessions:   COL_PREFIX + "Sessions",      // operator sessions
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
  admin, app, FV, TS,
  col, doc, obj, runTransaction, batch,
  COL, COL_PREFIX, STORAGE_PREFIX, BUCKET,
  envelope,
};
