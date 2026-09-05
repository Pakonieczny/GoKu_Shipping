/*  netlify/functions/_investorApiSchemas.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — the API v2 wire contract (blueprint §11.4, §11.5, §14.10,
 *  §12.1). Pinned Ajv 2020-12, compiled once at cold start; fail closed.
 *
 *  Everything the browser and the server agree on lives here:
 *    · the request envelope {apiVersion, requestId, action, params} and the
 *      mutation envelope that adds {idempotencyKey, csrfToken, auditReason?}
 *      plus exactly one concurrency field;
 *    · ACTIONS_V2 — every read and mutation with its params schema, whether
 *      it needs reauthentication, an audit reason, a resource version, a
 *      one-use preview token, and which confirmation the UI must ask for;
 *    · the canonical UI primitives (Money, Price, Quantity, SessionRef,
 *      RFC-3339 instants), the enums of §14.8, and the minimum fields of
 *      every §14.10 view model;
 *    · the typed error shape and its HTTP mapping;
 *    · the JSON test vectors the server and the browser both run.
 *
 *  Schemas reject unknown fields, oversized page sizes, malformed cursors
 *  and unsafe numbers. Money never travels as a JSON number. The model
 *  output schemas of _investorPolicy are compiled through the same Ajv so a
 *  strict-subset violation fails the deploy instead of a request.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const Ajv2020 = require("ajv/dist/2020");
const addFormats = require("ajv-formats");
const POLICY = require("./_investorPolicy");

const API_VERSION = "investor.v2";
const PAYLOAD_VERSION = "investor.v2.1";
const PAGE_DEFAULT = 50, PAGE_MAX = 200, CURSOR_MAX = 1024, SEARCH_MAX = 64, REASON_MAX = 500;
const CANONICAL_INT = "^-?(0|[1-9][0-9]{0,29})$";
const SYMBOL_PATTERN = "^[A-Z][A-Z0-9.-]{0,9}$";
const ID_PATTERN = "^[A-Za-z0-9_.:@+-]{1,160}$";
const HASH_PATTERN = "^[a-f0-9]{64}$";
const DATE_PATTERN = "^[0-9]{4}-[0-9]{2}-[0-9]{2}$";

/* ── §14.8 canonical visible states ─────────────────────────────────────── */
const ENUMS = Object.freeze({
  Decision: ["BUY", "WATCH", "IGNORE", "HOLD", "REDUCE", "SELL", "ABSTAIN"],
  ReviewDirective: ["RESEARCH_NOW", "UPDATE_EXISTING", "REUSE_CURRENT", "NONE"],
  ResearchState: ["CURRENT", "UPDATE_AVAILABLE", "REVISION_RUNNING", "FULL_RESEARCH_REQUIRED", "STALE", "INCOMPLETE", "QUEUED", "DEFERRED_BUDGET", "FAILED"],
  RunState: ["QUEUED", "RUNNING", "PARTIAL", "COMPLETE", "FAILED", "CANCELLED"],
  MandateState: ["PROPOSED", "REJECTED", "VALIDATED", "RESERVED", "BROKER_SYNC_PENDING", "WAITING_FOR_PRICE", "WORKING", "PARTIALLY_FILLED", "PROTECTION_PENDING",
    "PARTIALLY_PROTECTED", "PROTECTED_RTH", "OVERNIGHT_GAP_EXPOSED", "FILLED", "CANCEL_PENDING", "ENTRY_EXPIRED", "CANCELLED", "PAUSED_OPERATIONAL", "PAUSED_EVIDENCE",
    "SUPERSEDED", "RECONCILING", "OVERSELL_INCIDENT", "ACTION_REQUIRED", "ERROR", "UNPROTECTED", "CLOSED"],
  SourceState: ["CURRENT", "STALE", "DEGRADED", "DOWN"],
  ThesisHealth: ["IMPROVING", "INTACT", "WEAKENED", "BROKEN", "UNKNOWN"],
  CorporateActionState: ["QUARANTINED", "AWAITING_CONFIRMATION", "REBASED"],
  AccountMode: ["OBSERVE", "PAPER_AI", "LIMITED_LIVE"],
  ManagerState: ["ENABLED", "PAUSED"],
  BuyState: ["OPEN", "FROZEN"],
  EmergencyState: ["CLEAR", "ENGAGED", "RECOVERING"],
  ExecutorState: ["MONITORING", "APPLYING", "RECONCILING", "PAUSED_SAFETY"],
  ComponentHealth: ["HEALTHY", "DEGRADED", "DOWN", "ACTION_REQUIRED"],
  CollectionState: ["LOADING", "READY", "EMPTY", "PARTIAL", "STALE", "FAILED"],
  Authority: ["AI_MANDATE", "OPERATOR", "EMERGENCY_RISK"],
  Severity: ["info", "warning", "critical"],
  ConfirmationKind: ["none", "confirm", "confirm_with_reason", "reauth", "reauth_with_preview"],
  CompanyBucket: ["eligible", "excluded", "managedOffRoster", "all"],
  MandateFilter: ["active", "history", "all"],
  EventFilter: ["unresolved", "resolved", "all"],
  AlertFilter: ["active", "resolved", "all"],
  PerformanceRange: ["1M", "3M", "6M", "1Y", "ALL"],
  CompanySort: ["symbol", "decision", "capitalRank", "changed", "freshness", "sector"],
  ExportScope: ["decisions", "mandates", "execution", "ledger", "all"],
  PauseKind: ["OPERATIONAL", "THESIS"],
  OrderType: ["MARKETABLE_LIMIT", "LIMIT"],
  TimeInForce: ["DAY", "GTC"],
  ExchangeSession: ["RTH"],
});

/* ── schema builders (compact; every object closes additionalProperties) ─ */
const T = {
  str: (o = {}) => ({ type: "string", ...o }),
  int: (o = {}) => ({ type: "integer", ...o }),
  bool: () => ({ type: "boolean" }),
  nul: () => ({ type: "null" }),
  or: (...xs) => ({ anyOf: xs }),
  nullable: (s) => ({ anyOf: [s, { type: "null" }] }),
  arr: (items, maxItems = 500) => ({ type: "array", items, maxItems }),
  enumOf: (name) => ({ type: "string", enum: ENUMS[name] }),
  ref: (name) => ({ $ref: `#/$defs/${name}` }),
  obj: (properties, required = null, extra = {}) => ({ type: "object", additionalProperties: false, properties, ...(required ? { required } : {}), ...extra }),
  open: (properties = {}, required = null) => ({ type: "object", additionalProperties: true, properties, ...(required ? { required } : {}) }),
};
const canonicalInt = () => T.str({ pattern: CANONICAL_INT, maxLength: 31 });

/* ── §14.10 canonical primitives and shared shapes ─────────────────────── */
const DEFS = {
  CanonicalInt: canonicalInt(),
  Money: T.obj({ currency: T.str({ enum: ["USD"] }), amountMinor: canonicalInt(), minorScale: T.int({ enum: [2] }) }, ["currency", "amountMinor", "minorScale"]),
  Price: T.obj({ currency: T.str({ enum: ["USD"] }), priceMicros: canonicalInt() }, ["currency", "priceMicros"]),
  Quantity: T.obj({ quantityUnits: canonicalInt(), quantityScale: T.int({ enum: [0] }) }, ["quantityUnits", "quantityScale"]),
  Instant: T.str({ format: "date-time", maxLength: 40 }),
  SessionRef: T.obj({ calendarId: T.str({ maxLength: 40 }), calendarVersion: T.str({ maxLength: 40 }), sessionDate: T.str({ pattern: DATE_PATTERN }) }, ["calendarId", "calendarVersion", "sessionDate"]),
  Symbol: T.str({ pattern: SYMBOL_PATTERN, maxLength: 10 }),
  Id: T.str({ pattern: ID_PATTERN, maxLength: 160 }),
  Hash: T.str({ pattern: HASH_PATTERN }),
  Date: T.str({ pattern: DATE_PATTERN }),
  Cursor: T.str({ pattern: "^[A-Za-z0-9_-]{8,}$", maxLength: CURSOR_MAX }),
  PageSize: T.int({ minimum: 1, maximum: PAGE_MAX }),
  Bps: canonicalInt(),
  Ppm: canonicalInt(),
  Capability: T.obj({ action: T.str({ maxLength: 64 }), enabled: T.bool(), disabledReason: T.nullable(T.str({ maxLength: 200 })), requiresReauth: T.bool(), requiresReason: T.bool(), confirmationKind: T.enumOf("ConfirmationKind") },
    ["action", "enabled", "disabledReason", "requiresReauth", "requiresReason", "confirmationKind"]),
  ResourceHeader: T.open({ resourceId: T.ref("Id"), resourceVersion: canonicalInt(), availableActions: T.arr(T.ref("Capability"), 40) }, ["resourceId", "resourceVersion", "availableActions"]),
  CollectionEnvelope: T.open({ collectionState: T.enumOf("CollectionState"), asOf: T.ref("Instant"), items: { type: "array", maxItems: PAGE_MAX }, completedCount: T.int({ minimum: 0 }), totalCount: T.nullable(T.int({ minimum: 0 })),
    nextCursor: T.nullable(T.ref("Cursor")), error: T.nullable(T.ref("ErrorShape")), buckets: T.open() }, ["collectionState", "asOf", "items", "completedCount", "totalCount", "nextCursor", "error"]),
  ErrorShape: T.obj({ code: T.str({ pattern: "^[A-Z][A-Z0-9_]{2,63}$" }), message: T.str({ maxLength: 500 }), severity: T.enumOf("Severity"), retryable: T.bool(),
    fieldIssues: T.arr(T.obj({ path: T.str({ maxLength: 200 }), message: T.str({ maxLength: 300 }) }, ["path", "message"]), 50), correlationId: T.str({ maxLength: 80 }) },
    ["code", "message", "severity", "retryable", "fieldIssues", "correlationId"]),
  HealthComponent: T.obj({ state: T.enumOf("ComponentHealth"), lastSuccessAt: T.nullable(T.ref("Instant")), expectedBy: T.nullable(T.ref("Instant")), ageSeconds: T.nullable(T.int({ minimum: 0 })), backlog: T.nullable(T.int({ minimum: 0 })),
    errorCode: T.nullable(T.str({ maxLength: 80 })), conditionId: T.nullable(T.str({ maxLength: 120 })), operatorAction: T.nullable(T.str({ maxLength: 300 })) },
    ["state", "lastSuccessAt", "expectedBy", "ageSeconds", "backlog", "errorCode", "conditionId", "operatorAction"]),
  ControlPair: T.obj({ requested: T.str({ maxLength: 40 }), applied: T.str({ maxLength: 40 }), requestedAt: T.nullable(T.ref("Instant")), appliedAt: T.nullable(T.ref("Instant")), reason: T.nullable(T.str({ maxLength: 300 })) },
    ["requested", "applied", "requestedAt", "appliedAt", "reason"]),
};

/* ── error vocabulary and HTTP mapping (§11.5) ──────────────────────────── */
const HTTP_STATUS = Object.freeze({
  SCHEMA_INVALID: 400, UNKNOWN_ACTION: 400, UNSUPPORTED_API_VERSION: 400, CURSOR_INVALID: 400, REQUEST_TOO_LARGE: 413,
  AUTH_MISSING: 401, SESSION_EXPIRED: 401, SESSION_REVOKED: 401, AUTH_NOT_CONFIGURED: 503,
  ORIGIN_REJECTED: 403, CSRF_INVALID: 403, REAUTH_REQUIRED: 403, ROLE_FORBIDDEN: 403, MUTATIONS_DISABLED: 403, WRITER_EPOCH_MISMATCH: 403,
  NOT_FOUND: 404, ACTION_RETIRED: 410,
  VERSION_CONFLICT: 409, STATE_CONFLICT: 409, IDEMPOTENCY_KEY_REUSED: 409, IN_PROGRESS: 409, PREVIEW_TOKEN_INVALID: 409,
  SEMANTIC_REJECTED: 422, TRANSITION_UNSUPPORTED: 422, RISK_BOUND_EXCEEDED: 422, PREFLIGHT_FAILED: 422,
  BUDGET_EXHAUSTED: 429, RATE_LIMITED: 429,
  DEPENDENCY_DEGRADED: 503, ATTESTATION_FAILED: 503, INTERNAL: 500,
});
const RETRYABLE = new Set(["RATE_LIMITED", "DEPENDENCY_DEGRADED", "IN_PROGRESS", "BUDGET_EXHAUSTED"]);
function errorShape(code, message, { fieldIssues = [], correlationId = null, severity = null } = {}) {
  const status = HTTP_STATUS[code] || 500;
  return { code, message: String(message || code).slice(0, 500), severity: severity || (status >= 500 ? "critical" : status === 409 || status === 422 ? "warning" : "info"),
    retryable: RETRYABLE.has(code), fieldIssues: fieldIssues.slice(0, 50), correlationId: correlationId || crypto.randomBytes(6).toString("hex") };
}

/* ── ACTIONS_V2 (§11.2 reads, §11.3 mutations) ──────────────────────────── */
const page = { cursor: T.ref("Cursor"), pageSize: T.ref("PageSize") };
const acct = { accountId: T.ref("Id") };
const read = (params, extra = {}) => ({ kind: "read", params: T.obj(params), ...extra });
const mutation = (params, { reauth = false, requiresReason = false, concurrency = "version", confirmationKind = null, required = null, immediate = false } = {}) =>
  ({ kind: "mutation", params: T.obj(params, required), reauth, requiresReason, concurrency, immediate, confirmationKind: confirmationKind || (reauth ? "reauth" : requiresReason ? "confirm_with_reason" : "confirm") });

const ACTIONS_V2 = Object.freeze({
  simulationOverview: read({batchId:T.ref("Id"),cursor:T.str({maxLength:30})}),
  simulationDetail: read({runId:T.ref("Id"),collection:T.str({enum:["curve","requests","fills","decisions","orders","events"]}),after:T.str({maxLength:160})}),
  simulationStart: mutation({count:T.int({minimum:1,maximum:500}),from:T.ref("Date"),to:T.ref("Date")},{concurrency:"none",required:["count","from","to"]}),
  simulationControl: mutation({batchId:T.ref("Id"),runId:T.ref("Id"),command:T.str({enum:["pause","resume"]})},{concurrency:"none",required:["command"]}),
  /* reads */
  managerDashboard: read({ ...acct }),
  controlState: read({ ...acct }),
  companies: read({ ...page, search: T.str({ maxLength: SEARCH_MAX }), bucket: T.enumOf("CompanyBucket"), decision: T.enumOf("Decision"), researchState: T.enumOf("ResearchState"), held: T.bool(), sort: T.enumOf("CompanySort"), snapshotId: T.ref("Id") }, { paged: true }),
  companyDossier: read({ symbol: T.ref("Symbol"), sections: T.arr(T.str({ enum: ["identity", "facts", "changes", "thesis", "valuation", "forecast", "decision", "plan", "catalysts", "risks", "sources", "history"] }), 12) }, { required: ["symbol"] }),
  portfolio: read({ ...acct }),
  mandates: read({ ...acct, ...page, status: T.enumOf("MandateFilter"), symbol: T.ref("Symbol") }, { paged: true }),
  orderSets: read({ ...acct, ...page, symbol: T.ref("Symbol"), status: T.str({ maxLength: 40 }) }, { paged: true }),
  executionEvents: read({ ...acct, ...page, symbol: T.ref("Symbol"), sinceMs: T.int({ minimum: 0 }) }, { paged: true }),
  managerRuns: read({ ...page, managerRunId: T.ref("Id"), tradingDate: T.ref("Date") }, { paged: true }),
  jobs: read({ ...page, jobId: T.ref("Id"), status: T.str({ maxLength: 40 }), task: T.str({ maxLength: 40 }) }, { paged: true }),
  decisionJournal: read({ ...page, symbol: T.ref("Symbol"), decision: T.enumOf("Decision"), tradingDate: T.ref("Date"), managerRunId: T.ref("Id") }, { paged: true }),
  decisionAnalytics: read({ horizon: T.int({ enum: [1, 5, 20, 60] }), managerRunId: T.ref("Id") }),
  performance: read({ ...acct, range: T.enumOf("PerformanceRange") }),
  materialEvents: read({ ...page, status: T.enumOf("EventFilter"), symbol: T.ref("Symbol") }, { paged: true }),
  corporateActions: read({ ...page, status: T.str({ maxLength: 40 }) }, { paged: true }),
  systemHealth: read({}),
  universe: read({ includeMembers: T.bool() }),
  sources: read({ ...page, symbol: T.ref("Symbol") }, { paged: true }),
  soakStatus: read({}),
  auditExports: read({ ...page, jobId: T.ref("Id") }, { paged: true }),
  alerts: read({ ...page, status: T.enumOf("AlertFilter") }, { paged: true }),
  account: read({ ...acct }),
  quotes: read({ symbols: T.arr(T.ref("Symbol"), 100) }, { required: ["symbols"] }),
  intraday: read({ symbol: T.ref("Symbol"), date: T.ref("Date") }, { required: ["symbol"] }),
  history: read({ symbol: T.ref("Symbol"), days: T.int({ minimum: 5, maximum: 400 }) }, { required: ["symbol"] }),
  navSeries: read({ ...acct, range: T.enumOf("PerformanceRange") }),
  ledger: read({ ...acct, ...page }, { paged: true }),
  /* mutations */
  pauseManager: mutation({ ...acct }, { requiresReason: true }),
  resumeManager: mutation({ ...acct }),
  runManagerReview: mutation({ ...acct, reason: T.str({ enum: ["OPERATOR"] }) }, { required: ["reason"], concurrency: "none" }),
  requestResearch: mutation({ symbol: T.ref("Symbol"), directive: T.str({ enum: ["RESEARCH_NOW", "UPDATE_EXISTING", "FULL_REUNDERWRITE"] }) }, { required: ["symbol"], concurrency: "none" }),
  runFocusedRevision: mutation({ symbol: T.ref("Symbol"), deltaId: T.ref("Id") }, { required: ["symbol", "deltaId"], concurrency: "none" }),
  pauseMandate: mutation({ mandateSeriesId: T.ref("Id"), symbol: T.ref("Symbol"), pauseKind: T.enumOf("PauseKind") }, { required: ["mandateSeriesId", "symbol", "pauseKind"], requiresReason: true }),
  resumeMandate: mutation({ mandateSeriesId: T.ref("Id"), symbol: T.ref("Symbol"), mandateHash: T.ref("Hash") }, { required: ["mandateSeriesId", "symbol", "mandateHash"] }),
  cancelMandate: mutation({ mandateSeriesId: T.ref("Id"), symbol: T.ref("Symbol") }, { required: ["mandateSeriesId", "symbol"], requiresReason: true }),
  requestSell: mutation({ positionId: T.ref("Id"), symbol: T.ref("Symbol"), quantityUnits: canonicalInt(), quantityScale: T.int({ enum: [0] }), orderType: T.enumOf("OrderType"), timeInForce: T.enumOf("TimeInForce"),
    limitPriceMicros: canonicalInt(), exchangeSession: T.enumOf("ExchangeSession"), collarBps: canonicalInt(), reason: T.str({ minLength: 3, maxLength: REASON_MAX }), expectedPositionVersion: canonicalInt(), expectedMandateVersion: T.nullable(canonicalInt()) },
    { required: ["positionId", "symbol", "quantityUnits", "quantityScale", "orderType", "timeInForce", "exchangeSession", "reason", "expectedPositionVersion", "expectedMandateVersion"], requiresReason: true, concurrency: "none" }),
  cancelSell: mutation({ overrideId: T.ref("Id"), orderSetId: T.ref("Id") }, { required: ["overrideId", "orderSetId"] }),
  freezeBuys: mutation({ ...acct }, { requiresReason: true, immediate: true }),
  resumeBuys: mutation({ ...acct }, { reauth: true }),
  emergencyStop: mutation({ ...acct }, { immediate: true, concurrency: "none", confirmationKind: "confirm_with_reason", requiresReason: true }),
  resumeSystem: mutation({ ...acct }, { reauth: true }),
  activateAccountMode: mutation({ ...acct, targetMode: T.str({ enum: ["PAPER_AI"] }), policyHash: T.ref("Hash"), universeHash: T.ref("Hash") }, { required: ["targetMode", "policyHash", "universeHash"], reauth: true }),
  deactivateAccountMode: mutation({ ...acct, targetMode: T.str({ enum: ["OBSERVE"] }) }, { required: ["targetMode"], reauth: true, requiresReason: true }),
  setBudget: mutation({ dailyReservationMinor: canonicalInt(), byRoleMinor: T.obj({ investment: canonicalInt(), extraction: canonicalInt() }), alertThresholdPpm: T.ref("Ppm") }, { required: ["dailyReservationMinor"], reauth: false, confirmationKind: "confirm" }),
  setRiskMandate: mutation({ overrides: T.obj(Object.fromEntries(Object.keys(POLICY.RISK_MANDATE_BOUNDS).map((k) => [k, canonicalInt()]))), note: T.str({ maxLength: REASON_MAX }) }, { required: ["overrides"], reauth: true }),
  setEmergencyRiskPolicy: mutation({ ...acct, policy: T.open({ schemaVersion: T.str({ enum: ["emergency-risk-policy.v1"] }) }, ["schemaVersion"]), dryRun: T.bool() }, { required: ["policy"], reauth: true, concurrency: "preview", confirmationKind: "reauth_with_preview" }),
  setMarketConfig: mutation({ provider: T.str({ maxLength: 40 }), feed: T.nullable(T.str({ maxLength: 40 })), alpacaKeyId: T.str({ maxLength: 200 }), alpacaSecretKey: T.str({ maxLength: 400 }) }, { required: ["provider"], reauth: true }),
  confirmSplit: mutation({ symbol: T.ref("Symbol"), shareRatio: T.str({ maxLength: 20 }), effectiveDate: T.ref("Date"), sourceRef: T.str({ maxLength: 300 }) }, { required: ["symbol", "shareRatio", "effectiveDate", "sourceRef"], reauth: true }),
  confirmCashDividend: mutation({ symbol: T.ref("Symbol"), corporateActionId: T.ref("Id"), positionLifecycleId: T.ref("Id"), eligibleQty: canonicalInt(), perShareMicros: canonicalInt(), recordDate: T.ref("Date"), payDate: T.ref("Date"), sourceRef: T.str({ maxLength: 300 }) },
    { required: ["symbol", "corporateActionId", "positionLifecycleId", "perShareMicros", "recordDate", "payDate", "sourceRef"], reauth: true }),
  requestAuditExport: mutation({ fromDate: T.ref("Date"), toDate: T.ref("Date"), scope: T.enumOf("ExportScope") }, { required: ["fromDate", "toDate"], concurrency: "none" }),
  createPaperAccount: mutation({ accountId: T.ref("Id"), startingNavMinor: canonicalInt() }, { required: ["startingNavMinor"], concurrency: "absent", reauth: true }),
  previewPaperAccountReset: mutation({ ...acct }, { concurrency: "none", confirmationKind: "none" }),
  resetPaperAccount: mutation({ ...acct, accountVersion: canonicalInt(), balanceHash: T.ref("Hash") }, { required: ["accountVersion", "balanceHash"], reauth: true, concurrency: "preview", confirmationKind: "reauth_with_preview" }),
  reconcile: mutation({ ...acct }, { concurrency: "none", confirmationKind: "none" }),
  freezeUniverse: mutation({}, { reauth: true, concurrency: "none" }),
  resolveCiks: mutation({ symbols: T.arr(T.ref("Symbol"), 50) }, { reauth: true, concurrency: "none" }),
  setIssuerDomains: mutation({ symbol: T.ref("Symbol"), domains: T.arr(T.str({ pattern: "^[a-z0-9.-]{3,120}$" }), 8), expectedSourceRegistryVersion: T.str({ maxLength: 40 }) }, { required: ["symbol", "domains", "expectedSourceRegistryVersion"], reauth: true, requiresReason: true, concurrency: "none" }),
  acknowledgeAlert: mutation({ alertId: T.ref("Id"), alertVersion: canonicalInt() }, { required: ["alertId", "alertVersion"], concurrency: "none", confirmationKind: "none" }),
});
const RETIRED_V1_MUTATIONS = Object.freeze(["approve", "reject", "kill", "resume", "activateSafety", "setControl", "recordRecallBenchmark", "setRegime", "setIntelligenceWatchlist", "setPaperLearning", "runCycleNow", "cancelPlan", "expireStaleOrders", "openAccount"]);
const V1_EMERGENCY_MAP = Object.freeze({ kill: "emergencyStop", resume: "resumeSystem" });

/* ── envelopes ──────────────────────────────────────────────────────────── */
const READ_ACTIONS = Object.keys(ACTIONS_V2).filter((a) => ACTIONS_V2[a].kind === "read");
const MUTATION_ACTIONS = Object.keys(ACTIONS_V2).filter((a) => ACTIONS_V2[a].kind === "mutation");
const REQUEST_SCHEMA = T.obj({
  apiVersion: T.str({ enum: [API_VERSION] }), requestId: T.str({ pattern: "^[A-Za-z0-9_-]{8,64}$" }), action: T.str({ enum: Object.keys(ACTIONS_V2) }), params: T.open(),
  idempotencyKey: T.str({ pattern: "^[A-Za-z0-9_-]{16,128}$" }), csrfToken: T.str({ minLength: 16, maxLength: 256 }), auditReason: T.str({ maxLength: REASON_MAX }),
  expectedResourceVersion: canonicalInt(), expectedAbsent: T.bool(), previewToken: T.str({ pattern: "^[A-Za-z0-9_-]{16,256}$" }), reauthToken: T.str({ maxLength: 512 }),
}, ["apiVersion", "requestId", "action"]);
const RESPONSE_SCHEMA = T.obj({
  ok: T.bool(), requestId: T.str({ maxLength: 64 }), payloadVersion: T.str({ enum: [PAYLOAD_VERSION] }), asOf: T.ref("Instant"), partial: T.bool(), partialReason: T.nullable(T.str({ maxLength: 300 })),
  nextCursor: T.nullable(T.ref("Cursor")), data: T.or(T.open(), T.nul()), error: T.nullable(T.ref("ErrorShape")),
  mutationId: T.ref("Id"), acceptedAt: T.ref("Instant"), jobId: T.nullable(T.ref("Id")), resourceVersion: canonicalInt(), requestedState: T.open(), appliedState: T.open(),
}, ["ok", "requestId", "payloadVersion", "asOf", "partial", "partialReason", "nextCursor", "data", "error"]);

/* ── §14.10 view models: minimum fields ─────────────────────────────────── */
const money = () => T.ref("Money"), price = () => T.ref("Price"), qty = () => T.ref("Quantity"), inst = () => T.ref("Instant"), nInst = () => T.nullable(T.ref("Instant"));
const VIEW_MODELS = Object.freeze({
  ManagerDashboardView: T.open({ account: T.open({ accountId: T.ref("Id"), accountMode: T.enumOf("AccountMode"), nav: money(), settledCash: money(), buyingPower: money(), reserved: money(), available: money() }, ["accountId", "accountMode", "nav", "settledCash", "buyingPower", "reserved", "available"]),
    models: T.open({ investment: T.open({ snapshot: T.str(), reasoningEffort: T.str() }, ["snapshot", "reasoningEffort"]), extraction: T.open({ snapshot: T.str(), reasoningEffort: T.str() }, ["snapshot", "reasoningEffort"]) }, ["investment", "extraction"]),
    identity: T.open({ policyHash: T.nullable(T.str()), schemaHashes: T.open(), promptHashes: T.open() }, ["policyHash", "schemaHashes", "promptHashes"]),
    latestRun: T.nullable(T.open()), coverage: T.nullable(T.open()), holdingReview: T.nullable(T.open()), committedPlan: T.open({ portfolioPlanId: T.nullable(T.ref("Id")), activationSnapshotId: T.nullable(T.ref("Id")) }, ["portfolioPlanId", "activationSnapshotId"]),
    counts: T.open({ decisions: T.open(), research: T.open(), events: T.open(), authorizations: T.open() }, ["decisions", "research", "events", "authorizations"]),
    spend: T.open(), health: T.open(), alerts: T.arr(T.open(), 50), evidenceDeltas: T.arr(T.open(), 50), newDecisions: T.arr(T.open(), 50), holdingRevisions: T.arr(T.open(), 50), workflow: T.arr(T.open(), 12) },
    ["account", "models", "identity", "latestRun", "coverage", "holdingReview", "committedPlan", "counts", "spend", "health", "alerts", "evidenceDeltas", "newDecisions", "holdingRevisions", "workflow"]),
  ControlStateView: T.open({ resourceId: T.ref("Id"), resourceVersion: canonicalInt(), availableActions: T.arr(T.ref("Capability"), 40), accountMode: T.ref("ControlPair"), managerState: T.ref("ControlPair"), buyState: T.ref("ControlPair"),
    emergencyState: T.ref("ControlPair"), executorState: T.ref("ControlPair"), writerEpoch: T.int({ minimum: 0 }), lastTransition: T.nullable(T.open()), blockingConditions: T.arr(T.open(), 40), mutationsEnabled: T.bool() },
    ["resourceId", "resourceVersion", "availableActions", "accountMode", "managerState", "buyState", "emergencyState", "executorState", "writerEpoch", "lastTransition", "blockingConditions", "mutationsEnabled"]),
  ManagerRunView: T.open({ managerRunId: T.ref("Id"), state: T.enumOf("RunState"), tradingDate: T.nullable(T.ref("Date")), universeVersion: T.nullable(T.str()), universeHash: T.nullable(T.str()), contextManifestHash: T.nullable(T.str()), policyHash: T.nullable(T.str()),
    startedAt: nInst(), cutoffAt: nInst(), deadlineAt: nInst(), completedAt: nInst(), checkpoints: T.arr(T.open(), 40), coverage: T.open({ eligibleCount: T.int(), completedCount: T.int(), missing: T.arr(T.ref("Symbol"), 400), duplicates: T.arr(T.ref("Symbol"), 100), unknown: T.arr(T.ref("Symbol"), 100) }, ["eligibleCount", "completedCount", "missing", "duplicates", "unknown"]),
    countsByDecision: T.open(), researchJobs: T.open(), finalPlan: T.nullable(T.open()), cost: T.open(), failure: T.nullable(T.open()) },
    ["managerRunId", "state", "tradingDate", "universeVersion", "universeHash", "contextManifestHash", "policyHash", "startedAt", "cutoffAt", "deadlineAt", "completedAt", "checkpoints", "coverage", "countsByDecision", "researchJobs", "finalPlan", "cost", "failure"]),
  CompanyRowView: T.open({ resourceId: T.ref("Id"), resourceVersion: canonicalInt(), availableActions: T.arr(T.ref("Capability"), 40), symbol: T.ref("Symbol"), name: T.nullable(T.str()), sector: T.nullable(T.str()), bucket: T.enumOf("CompanyBucket"), eligible: T.bool(), exclusionReason: T.nullable(T.str()),
    decision: T.nullable(T.enumOf("Decision")), reviewDirective: T.nullable(T.enumOf("ReviewDirective")), reason: T.nullable(T.str()), changed: T.bool(), freshness: T.open({ thesisAt: nInst(), dossierAt: nInst(), researchAt: nInst(), evidenceAt: nInst() }, ["thesisAt", "dossierAt", "researchAt", "evidenceAt"]),
    researchState: T.enumOf("ResearchState"), dataQuality: T.nullable(T.open()), held: T.bool(), pending: T.bool(), mandateState: T.nullable(T.enumOf("MandateState")), capitalRank: T.nullable(T.int()), opportunityRoute: T.nullable(T.str()) },
    ["resourceId", "resourceVersion", "availableActions", "symbol", "name", "sector", "bucket", "eligible", "exclusionReason", "decision", "reviewDirective", "reason", "changed", "freshness", "researchState", "dataQuality", "held", "pending", "mandateState", "capitalRank", "opportunityRoute"]),
  CompanyDetailView: T.open({ symbol: T.ref("Symbol"), identity: T.open(), identityHistory: T.arr(T.open(), 50), facts: T.open(), deltas: T.arr(T.open(), 100), premises: T.arr(T.open(), 200), thesis: T.nullable(T.open()), valuation: T.nullable(T.open()), forecast: T.nullable(T.open()),
    decision: T.nullable(T.open()), mandate: T.nullable(T.open()), hashes: T.open(), catalysts: T.arr(T.open(), 50), invalidators: T.arr(T.open(), 50), sources: T.arr(T.open(), 200), decisionHistory: T.arr(T.open(), 100) },
    ["symbol", "identity", "identityHistory", "facts", "deltas", "premises", "thesis", "valuation", "forecast", "decision", "mandate", "hashes", "catalysts", "invalidators", "sources", "decisionHistory"]),
  HoldingRowView: T.open({ resourceId: T.ref("Id"), resourceVersion: canonicalInt(), availableActions: T.arr(T.ref("Capability"), 40), ids: T.open(), symbol: T.ref("Symbol"), quantity: qty(), averageCost: T.nullable(price()), mark: T.nullable(price()), marketValue: money(), unrealizedPnl: money(),
    weightBps: T.ref("Bps"), forwardDownside: T.nullable(money()), stressedDownside: T.nullable(money()), thesisHealth: T.enumOf("ThesisHealth"), target: T.nullable(price()), lossBoundary: T.nullable(price()), timeExit: T.nullable(T.ref("SessionRef")),
    distanceToTargetBps: T.nullable(T.ref("Bps")), distanceToBoundaryBps: T.nullable(T.ref("Bps")), protection: T.open({ state: T.str(), coverage: T.str() }, ["state", "coverage"]), emergencyRank: T.nullable(T.int()), emergencyExpiry: nInst(), labels: T.arr(T.str(), 10) },
    ["resourceId", "resourceVersion", "availableActions", "ids", "symbol", "quantity", "averageCost", "mark", "marketValue", "unrealizedPnl", "weightBps", "forwardDownside", "stressedDownside", "thesisHealth", "target", "lossBoundary", "timeExit", "distanceToTargetBps", "distanceToBoundaryBps", "protection", "emergencyRank", "emergencyExpiry", "labels"]),
  AuthorizationExecutionView: T.open({ resourceId: T.ref("Id"), resourceVersion: canonicalInt(), availableActions: T.arr(T.ref("Capability"), 40), symbol: T.ref("Symbol"), ids: T.open(), hashes: T.open(), desiredVersion: T.nullable(T.str()), appliedVersion: T.nullable(T.str()), state: T.enumOf("MandateState"),
    proposedQuantity: T.nullable(qty()), authorizedQuantity: T.nullable(qty()), filledQuantity: qty(), remainingQuantity: qty(), reservations: T.open(), clamp: T.nullable(T.open()), entry: T.nullable(price()), target: T.nullable(price()), boundary: T.nullable(price()),
    validFrom: nInst(), timeInForce: T.nullable(T.str()), authorizedSessions: T.arr(T.ref("SessionRef"), 10), legs: T.arr(T.open(), 10), fills: T.arr(T.open(), 100), broker: T.open(), errors: T.arr(T.open(), 20), overrideLineage: T.arr(T.open(), 20) },
    ["resourceId", "resourceVersion", "availableActions", "symbol", "ids", "hashes", "desiredVersion", "appliedVersion", "state", "proposedQuantity", "authorizedQuantity", "filledQuantity", "remainingQuantity", "reservations", "clamp", "entry", "target", "boundary", "validFrom", "timeInForce", "authorizedSessions", "legs", "fills", "broker", "errors", "overrideLineage"]),
  ResearchJobView: T.open({ jobId: T.ref("Id"), managerRunId: T.nullable(T.ref("Id")), symbol: T.nullable(T.ref("Symbol")), kind: T.str(), directive: T.nullable(T.str()), state: T.enumOf("ResearchState"), versions: T.open(), requestedAt: nInst(), startedAt: nInst(), completedAt: nInst(), toolBudget: T.open(), claimVerification: T.nullable(T.open()), outputHash: T.nullable(T.str()), failure: T.nullable(T.open()) },
    ["jobId", "managerRunId", "symbol", "kind", "directive", "state", "versions", "requestedAt", "startedAt", "completedAt", "toolBudget", "claimVerification", "outputHash", "failure"]),
  OrderSetView: T.open({ resourceId: T.ref("Id"), resourceVersion: canonicalInt(), availableActions: T.arr(T.ref("Capability"), 40), ids: T.open(), symbol: T.ref("Symbol"), desiredState: T.str(), appliedState: T.nullable(T.str()), broker: T.open(), reservation: T.nullable(T.open()), legs: T.arr(T.open(), 10), fills: T.arr(T.open(), 100), coverageWindow: T.nullable(T.open()), reconciliationCursor: T.nullable(T.str()), error: T.nullable(T.str()) },
    ["resourceId", "resourceVersion", "availableActions", "ids", "symbol", "desiredState", "appliedState", "broker", "reservation", "legs", "fills", "coverageWindow", "reconciliationCursor", "error"]),
  ExecutionEventView: T.open({ eventId: T.ref("Id"), correlationId: T.nullable(T.str()), accountId: T.nullable(T.ref("Id")), symbol: T.nullable(T.ref("Symbol")), ids: T.open(), authority: T.enumOf("Authority"), type: T.str(), state: T.nullable(T.str()), requested: T.nullable(T.open()), observed: T.nullable(T.open()), eventAt: nInst(), receivedAt: nInst(), brokerSource: T.nullable(T.str()), dedupeId: T.nullable(T.str()), ledgerIds: T.arr(T.str(), 20) },
    ["eventId", "correlationId", "accountId", "symbol", "ids", "authority", "type", "state", "requested", "observed", "eventAt", "receivedAt", "brokerSource", "dedupeId", "ledgerIds"]),
  JobView: T.open({ jobId: T.ref("Id"), task: T.str(), handler: T.nullable(T.str()), attempt: T.int({ minimum: 0 }), state: T.str(), priority: T.int(), lease: T.nullable(T.open()), checkpoint: T.nullable(T.open()), createdAt: nInst(), dueAt: nInst(), heartbeatAt: nInst(), completedAt: nInst(), pollingSlaSeconds: T.nullable(T.int()), progress: T.open({ numerator: T.nullable(T.int()), denominator: T.nullable(T.int()) }, ["numerator", "denominator"]), cost: T.nullable(money()), retryLineage: T.arr(T.str(), 20), error: T.nullable(T.open()) },
    ["jobId", "task", "handler", "attempt", "state", "priority", "lease", "checkpoint", "createdAt", "dueAt", "heartbeatAt", "completedAt", "pollingSlaSeconds", "progress", "cost", "retryLineage", "error"]),
  MaterialEventView: T.open({ resourceId: T.ref("Id"), resourceVersion: canonicalInt(), availableActions: T.arr(T.ref("Capability"), 40), symbol: T.ref("Symbol"), eventClass: T.str(), headline: T.nullable(T.str()), sourceUrl: T.nullable(T.str()), sourceManifest: T.nullable(T.open()), publishedAt: nInst(), firstSeenAt: nInst(), verification: T.nullable(T.open()), affectedMandate: T.nullable(T.open()), safetyPause: T.bool(), review: T.nullable(T.open()) },
    ["resourceId", "resourceVersion", "availableActions", "symbol", "eventClass", "headline", "sourceUrl", "sourceManifest", "publishedAt", "firstSeenAt", "verification", "affectedMandate", "safetyPause", "review"]),
  CorporateActionView: T.open({ resourceId: T.ref("Id"), resourceVersion: canonicalInt(), availableActions: T.arr(T.ref("Capability"), 40), type: T.str(), state: T.enumOf("CorporateActionState"), symbol: T.ref("Symbol"), source: T.nullable(T.str()), effectiveDate: T.nullable(T.ref("Date")), terms: T.open(), affected: T.open(), preview: T.nullable(T.open()), history: T.arr(T.open(), 20) },
    ["resourceId", "resourceVersion", "availableActions", "type", "state", "symbol", "source", "effectiveDate", "terms", "affected", "preview", "history"]),
  AlertView: T.open({ resourceId: T.ref("Id"), resourceVersion: canonicalInt(), availableActions: T.arr(T.ref("Capability"), 40), conditionId: T.str(), severity: T.enumOf("Severity"), active: T.bool(), title: T.str(), detail: T.nullable(T.str()), action: T.nullable(T.str()), firstSeenAt: nInst(), lastSeenAt: nInst(), acknowledgedBy: T.nullable(T.str()), acknowledgedAt: nInst(), resolvedAt: nInst(), resolutionEvidence: T.nullable(T.str()), correlationIds: T.arr(T.str(), 10) },
    ["resourceId", "resourceVersion", "availableActions", "conditionId", "severity", "active", "title", "detail", "action", "firstSeenAt", "lastSeenAt", "acknowledgedBy", "acknowledgedAt", "resolvedAt", "resolutionEvidence", "correlationIds"]),
  AuditExportView: T.open({ jobId: T.ref("Id"), resourceVersion: canonicalInt(), snapshot: T.open(), filter: T.open(), state: T.str(), requestedAt: nInst(), completedAt: nInst(), expiresAt: nInst(), manifest: T.nullable(T.open({ manifestId: T.str(), sha256: T.str(), bytes: T.int(), downloadUrl: T.str(), downloadExpiresAt: inst() }, ["manifestId", "sha256", "bytes", "downloadUrl", "downloadExpiresAt"])) },
    ["jobId", "resourceVersion", "snapshot", "filter", "state", "requestedAt", "completedAt", "expiresAt", "manifest"]),
  DecisionJournalRowView: T.open({ decisionId: T.ref("Id"), managerRunId: T.ref("Id"), tradingDate: T.nullable(T.ref("Date")), symbol: T.ref("Symbol"), decision: T.enumOf("Decision"), reviewDirective: T.nullable(T.str()), reasonCode: T.nullable(T.str()), rationale: T.nullable(T.str()), versions: T.open(), hashes: T.open(), forecast: T.nullable(T.open()), alternatives: T.arr(T.open(), 20), activationState: T.nullable(T.str()), resultState: T.nullable(T.str()), outcomes: T.nullable(T.open()), attributionAuthority: T.enumOf("Authority") },
    ["decisionId", "managerRunId", "tradingDate", "symbol", "decision", "reviewDirective", "reasonCode", "rationale", "versions", "hashes", "forecast", "alternatives", "activationState", "resultState", "outcomes", "attributionAuthority"]),
  DecisionAnalyticsView: T.open({ cohort: T.open(), benchmark: T.open(), horizon: T.int(), method: T.open(), calibration: T.nullable(T.open()), selection: T.nullable(T.open()), counterfactuals: T.arr(T.open(), 100), errors: T.nullable(T.open()), exclusions: T.arr(T.open(), 50), versions: T.open() },
    ["cohort", "benchmark", "horizon", "method", "calibration", "selection", "counterfactuals", "errors", "exclusions", "versions"]),
  PerformanceView: T.open({ seriesIds: T.open(), basis: T.open(), deposits: money(), withdrawals: money(), realizedPnl: money(), unrealizedPnl: money(), costs: T.open({ trading: money(), ai: money(), data: money() }, ["trading", "ai", "data"]), returns: T.open(), drawdown: T.open(), volatility: T.open(), exposure: T.open(), stress: T.nullable(T.open()), confidence: T.nullable(T.open()), nav: T.open() },
    ["seriesIds", "basis", "deposits", "withdrawals", "realizedPnl", "unrealizedPnl", "costs", "returns", "drawdown", "volatility", "exposure", "stress", "confidence", "nav"]),
  SystemView: T.open({ health: T.open(), backlog: T.open(), identities: T.open(), configuration: T.open(), audit: T.open(), availableActions: T.arr(T.ref("Capability"), 40) }, ["health", "backlog", "identities", "configuration", "audit", "availableActions"]),
  ChartSeriesView: T.open({ seriesId: T.str(), version: T.str(), asOf: inst(), currency: T.str(), calendarId: T.str(), adjustment: T.nullable(T.str()), provenance: T.nullable(T.open()), resolution: T.str(), points: T.arr(T.open(), 5000), gaps: T.arr(T.open(), 100), scorable: T.bool(), markers: T.arr(T.open(), 200) },
    ["seriesId", "version", "asOf", "currency", "calendarId", "adjustment", "provenance", "resolution", "points", "gaps", "scorable", "markers"]),
  SpendView: T.open({ byTask: T.arr(T.open(), 40), reserved: money(), actual: money(), tool: money(), data: money(), retries: T.int(), dailyLimit: money(), remaining: money(), deferredWork: T.int(), latency: T.open(), denominators: T.open() },
    ["byTask", "reserved", "actual", "tool", "data", "retries", "dailyLimit", "remaining", "deferredWork", "latency", "denominators"]),
  HealthView: T.open({}, []),
});

/* ── shared JSON test vectors (the browser runs the same table) ─────────── */
const TEST_VECTORS = Object.freeze({
  version: "ui-vectors.v1",
  money: [
    { amountMinor: "123456", display: "$1,234.56" }, { amountMinor: "-50", display: "-$0.50" }, { amountMinor: "0", display: "$0.00" }, { amountMinor: "100000000000", display: "$1,000,000,000.00" },
  ],
  price: [{ priceMicros: "1234567890", display: "$1,234.57" }, { priceMicros: "500000", display: "$0.50" }, { priceMicros: "99999999", display: "$100.00" }],
  quantity: [{ quantityUnits: "10", display: "10" }, { quantityUnits: "1250", display: "1,250" }],
  bps: [{ bps: "-250", display: "-2.50%" }, { bps: "10000", display: "100.00%" }, { bps: "7", display: "0.07%" }],
  ppm: [{ ppm: "996711", display: "99.67%" }, { ppm: "500000", display: "50.00%" }],
  labels: [{ enum: "WAITING_FOR_PRICE", display: "Waiting for price" }, { enum: "PROTECTED_RTH", display: "Protected RTH" }, { enum: "PAUSED_SAFETY", display: "Paused safety" }, { enum: "ABSTAIN", display: "Abstain" }],
  unsafe: [{ value: 1e21, rejected: true }, { value: "12.5", rejected: true }, { value: "0123", rejected: true }, { value: "1".repeat(31), rejected: true }],
});

/* ── compile (cold start, fail closed) ──────────────────────────────────── */
let COMPILED = null;
function walk(schema, path, problems, { strictModel = false } = {}) {
  if (!schema || typeof schema !== "object") return;
  if (Array.isArray(schema)) { schema.forEach((s, i) => walk(s, `${path}[${i}]`, problems, { strictModel })); return; }
  if (schema.type === "object" || schema.properties) {
    if (schema.additionalProperties === undefined) problems.push(`${path}: object without additionalProperties`);
    if (strictModel) {
      if (schema.additionalProperties !== false) problems.push(`${path}: strict model schema must close additionalProperties`);
      const props = Object.keys(schema.properties || {});
      const req = schema.required || [];
      if (props.some((p) => !req.includes(p))) problems.push(`${path}: strict model schema must require every property`);
    }
  }
  if (schema.type === "number") problems.push(`${path}: floating "number" is not an allowed wire type (use canonical integer strings)`);
  if (schema.type === "integer" && schema.maximum === undefined && !schema.enum && !/PageSize|minimum/.test(path) && strictModel) problems.push(`${path}: unbounded integer`);
  for (const [k, v] of Object.entries(schema)) {
    if (["properties", "$defs"].includes(k)) for (const [pk, pv] of Object.entries(v)) walk(pv, `${path}.${k}.${pk}`, problems, { strictModel });
    else if (["items", "anyOf", "oneOf", "allOf", "additionalProperties"].includes(k) && typeof v === "object") walk(v, `${path}.${k}`, problems, { strictModel });
  }
}
function compileAll() {
  if (COMPILED) return COMPILED;
  const ajv = new Ajv2020({ strict: true, allErrors: true, coerceTypes: false, useDefaults: false, removeAdditional: false, allowUnionTypes: true, validateFormats: true });
  addFormats(ajv, ["date-time", "uri"]);
  const problems = [];
  const withDefs = (s) => ({ $defs: DEFS, ...s });
  const validators = { request: null, response: null, params: {}, views: {} };
  try {
    walk({ $defs: DEFS }, "$defs", problems);
    walk(REQUEST_SCHEMA, "request", problems); walk(RESPONSE_SCHEMA, "response", problems);
    validators.request = ajv.compile(withDefs(REQUEST_SCHEMA));
    validators.response = ajv.compile(withDefs(RESPONSE_SCHEMA));
    for (const [name, spec] of Object.entries(ACTIONS_V2)) { walk(spec.params, `params.${name}`, problems); validators.params[name] = ajv.compile(withDefs(spec.params)); }
    for (const [name, schema] of Object.entries(VIEW_MODELS)) { walk(schema, `view.${name}`, problems); validators.views[name] = ajv.compile(withDefs(schema)); }
    /* the model output schemas share the compiler: a strict-subset violation is a deploy failure */
    for (const [name, schema] of Object.entries(POLICY.SCHEMAS)) { walk(schema, `model.${name}`, problems, { strictModel: true }); validators[`model:${name}`] = ajv.compile(schema); }
  } catch (e) { problems.push(`compile: ${e.message}`); }
  if (problems.length) { const err = new Error(`API schemas failed to compile: ${problems.slice(0, 8).join("; ")}`); err.code = "SCHEMA_COMPILE_FAILED"; err.problems = problems; throw err; }
  COMPILED = { ajv, validators, compiledAtMs: Date.now(), count: Object.keys(validators.params).length + Object.keys(validators.views).length + 2 + Object.keys(POLICY.SCHEMAS).length };
  return COMPILED;
}
function issuesOf(validator) { return (validator.errors || []).map((e) => ({ path: e.instancePath || "$", message: `${e.message}${e.params && e.params.additionalProperty ? ` (${e.params.additionalProperty})` : ""}`.slice(0, 300) })).slice(0, 50); }

/** Validate one v2 request body. Returns {ok, action, spec, params, kind} or {ok:false, error}. */
function validateRequest(body) {
  const c = compileAll();
  if (!body || typeof body !== "object") return { ok: false, error: errorShape("SCHEMA_INVALID", "request body must be a JSON object") };
  if (body.apiVersion !== API_VERSION) return { ok: false, error: errorShape("UNSUPPORTED_API_VERSION", `apiVersion must be exactly ${API_VERSION}`) };
  if (!c.validators.request(body)) return { ok: false, error: errorShape("SCHEMA_INVALID", "request envelope rejected", { fieldIssues: issuesOf(c.validators.request) }) };
  const spec = ACTIONS_V2[body.action];
  if (!spec) return { ok: false, error: errorShape("UNKNOWN_ACTION", `unknown v2 action ${body.action}`) };
  const params = body.params === undefined ? {} : body.params;
  if (!c.validators.params[body.action](params)) return { ok: false, error: errorShape("SCHEMA_INVALID", `params rejected for ${body.action}`, { fieldIssues: issuesOf(c.validators.params[body.action]) }) };
  if (spec.kind === "mutation") {
    const issues = [];
    if (!body.idempotencyKey) issues.push({ path: "$.idempotencyKey", message: "required for a mutation" });
    if (!body.csrfToken) issues.push({ path: "$.csrfToken", message: "required for a mutation" });
    if (spec.requiresReason && !(body.auditReason && body.auditReason.trim().length >= 3)) issues.push({ path: "$.auditReason", message: "an audit reason is required for this action" });
    const conc = [body.expectedResourceVersion !== undefined, body.expectedAbsent === true, body.previewToken !== undefined].filter(Boolean).length;
    if (spec.concurrency === "version" && body.expectedResourceVersion === undefined) issues.push({ path: "$.expectedResourceVersion", message: "required: the resource version the operator saw" });
    if (spec.concurrency === "absent" && body.expectedAbsent !== true) issues.push({ path: "$.expectedAbsent", message: "required: true" });
    if (spec.concurrency === "preview" && body.previewToken === undefined && !(body.params && body.params.dryRun === true)) issues.push({ path: "$.previewToken", message: "required: the one-use preview token" });
    if (conc > 1) issues.push({ path: "$", message: "exactly one concurrency field is allowed" });
    if (issues.length) return { ok: false, error: errorShape("SCHEMA_INVALID", `mutation envelope rejected for ${body.action}`, { fieldIssues: issues }) };
  } else if (body.idempotencyKey !== undefined || body.expectedResourceVersion !== undefined || body.previewToken !== undefined || body.expectedAbsent !== undefined) {
    return { ok: false, error: errorShape("SCHEMA_INVALID", "a read carries no mutation fields") };
  }
  return { ok: true, action: body.action, spec, params, kind: spec.kind };
}
function validateView(name, value) {
  const c = compileAll();
  const v = c.validators.views[name];
  if (!v) throw Object.assign(new Error(`unknown view model ${name}`), { code: "VIEW_MODEL_UNKNOWN" });
  return v(value) ? { ok: true, issues: [] } : { ok: false, issues: issuesOf(v) };
}
function validateResponse(value) { const c = compileAll(); return c.validators.response(value) ? { ok: true, issues: [] } : { ok: false, issues: issuesOf(c.validators.response) }; }
function validateModelOutput(schemaVersion, value) {
  const c = compileAll();
  const v = c.validators[`model:${schemaVersion}`];
  if (!v) throw Object.assign(new Error(`unknown model schema ${schemaVersion}`), { code: "SCHEMA_UNKNOWN" });
  return v(value) ? { ok: true, issues: [] } : { ok: false, issues: issuesOf(v) };
}
function capability(action, { enabled = true, disabledReason = null } = {}) {
  const spec = ACTIONS_V2[action] || { reauth: false, requiresReason: false, confirmationKind: "none" };
  return { action, enabled: enabled === true, disabledReason: enabled ? null : String(disabledReason || "not available").slice(0, 200), requiresReauth: spec.reauth === true, requiresReason: spec.requiresReason === true, confirmationKind: spec.confirmationKind || "none" };
}
function contractHash() { return crypto.createHash("sha256").update(JSON.stringify(POLICY.canonical({ API_VERSION, PAYLOAD_VERSION, ACTIONS_V2, VIEW_MODELS, DEFS, ENUMS }))).digest("hex"); }

module.exports = {
  API_VERSION, PAYLOAD_VERSION, PAGE_DEFAULT, PAGE_MAX, CURSOR_MAX, CANONICAL_INT, SYMBOL_PATTERN, ID_PATTERN,
  ENUMS, DEFS, ACTIONS_V2, READ_ACTIONS, MUTATION_ACTIONS, RETIRED_V1_MUTATIONS, V1_EMERGENCY_MAP,
  REQUEST_SCHEMA, RESPONSE_SCHEMA, VIEW_MODELS, TEST_VECTORS, HTTP_STATUS, RETRYABLE,
  errorShape, compileAll, validateRequest, validateView, validateResponse, validateModelOutput, capability, contractHash, issuesOf,
};
