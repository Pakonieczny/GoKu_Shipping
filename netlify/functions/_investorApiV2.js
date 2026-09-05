/*  netlify/functions/_investorApiV2.js  (fund-manager-v1)
 *  ---------------------------------------------------------------------------
 *  Investor AI — ACTIONS_V2: the bounded manager/dossier/mandate/journal/KPI
 *  API and its typed view-model presenters (blueprint §11.2–§11.5, §14.10).
 *
 *  Rules this module keeps:
 *    · every read answers {ok, requestId, payloadVersion, asOf, partial,
 *      partialReason, nextCursor, data, error}; every collection carries
 *      {collectionState, asOf, items, completedCount, totalCount, nextCursor,
 *      error, buckets}; cursors are opaque and bound to the snapshot, the
 *      filter hash and the sort so a mid-run write can neither skip nor
 *      duplicate a row;
 *    · managerDashboard is summaries only; details are paginated/on demand;
 *    · every mutable resource carries {resourceId, resourceVersion,
 *      availableActions[]} and the browser never infers eligibility;
 *    · before any side effect a mutation claims actorId+accountId+action+
 *      idempotencyKey in Mutations with the canonical request hash: same key
 *      and hash replays the stored result, same key with other content is
 *      409 IDEMPOTENCY_KEY_REUSED; previews are one-use and consumed in the
 *      same transaction that accepts the mutation;
 *    · during observation the investment mutations are disabled; the
 *      account-mode release itself, safety freezes and administration stay
 *      available; no v1 fallthrough exists here.
 *  Money is canonical integer strings throughout; nothing is preformatted.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const A = require("./_investorAdmin");
const S = require("./_investorApiSchemas");
const POLICY = require("./_investorPolicy");
const M = require("./_investorMoney");

const API_BUILD = "api-v2.1";
const CONTROL_DOC = "control";
const MEMO_TTL_MS = 20000;
const MUTATION_RESULT_TTL_MS = 7 * 24 * 3600 * 1000;
const PREVIEW_TTL_MS = 10 * 60 * 1000;
const INVESTMENT_MUTATIONS = new Set(["runManagerReview", "requestResearch", "runFocusedRevision", "pauseMandate", "resumeMandate", "cancelMandate", "requestSell", "cancelSell",
  "resumeBuys", "resumeSystem", "confirmSplit", "confirmCashDividend", "resetPaperAccount"]);
const HORIZONS = [1, 5, 20, 60];

function lazy(m) { try { return require(m); } catch { return null; } }
function db(admin) { return admin || A; }
function rows(snap) { const out = []; if (snap && typeof snap.forEach === "function") snap.forEach((d) => out.push(d.data())); return out; }
function decode(d) { const SC = lazy("./_investorStorageCodec"); if (!SC || !d || !d._codec) return d; try { return SC.decode(d); } catch { return d; } }
function sha(v) { return crypto.createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(POLICY.canonical(v))).digest("hex"); }
function typed(code, message, extra = {}) { return Object.assign(new Error(message || code), { code, ...extra }); }
function iso(ms) { const n = Number(ms); return Number.isFinite(n) && n > 0 ? new Date(n).toISOString() : null; }
function isoOf(v) { if (v == null) return null; if (typeof v === "number") return iso(v); if (typeof v === "string") { const t = Date.parse(v); return Number.isFinite(t) ? new Date(t).toISOString() : null; } if (v.toDate) return v.toDate().toISOString(); return null; }
function big(v) { try { return BigInt(String(v == null || v === "" ? 0 : v)); } catch { return 0n; } }
function canon(v) { try { return M.toCanonical(big(v)); } catch { return "0"; } }
function money(minor) { return { currency: "USD", amountMinor: canon(minor), minorScale: 2 }; }
function moneyUsd(usd) { const n = Number(usd); return money(Number.isFinite(n) ? Math.round(n * 100) : 0); }
function price(micros) { return micros == null || micros === "" ? null : { currency: "USD", priceMicros: canon(micros) }; }
function priceUsd(usd) { const n = Number(usd); return Number.isFinite(n) && n > 0 ? price(Math.round(n * 1e6)) : null; }
function qty(units) { return { quantityUnits: canon(units), quantityScale: 0 }; }
function bpsOf(fromMicros, toMicros) { const f = big(fromMicros), t = big(toMicros); if (f <= 0n || t <= 0n) return null; return M.toCanonical(M.returnBps({ fromMicros: f, toMicros: t, mode: M.ROUNDING.HALF_EVEN })); }
function weightBps(partMinor, wholeMinor) { const w = big(wholeMinor); if (w <= 0n) return "0"; return M.toCanonical(M.divRound(big(partMinor) * 10000n, w, M.ROUNDING.HALF_EVEN)); }
function sessionRef(date) { return date ? { calendarId: POLICY.CALENDAR_ID || "XNYS", calendarVersion: String((lazy("./_investorMarket") || {}).CALENDAR_VERSION || "1"), sessionDate: String(date).slice(0, 10) } : null; }
function clip(s, n = 300) { return s == null ? null : String(s).slice(0, n); }
function count(list, key) { const out = {}; for (const x of list || []) { const k = x && x[key]; if (k == null) continue; out[k] = (out[k] || 0) + 1; } return out; }

/* ── a tiny per-container memo so 30-second polling does not re-read whole collections ── */
const MEMO = new Map();
const ADMIN_IDS = new WeakMap();
function adminKey(D) { if (!ADMIN_IDS.has(D)) ADMIN_IDS.set(D, D === A ? "real" : crypto.randomBytes(4).toString("hex")); return ADMIN_IDS.get(D); }
async function memo(D, rawKey, ttlMs, fn) {
  const key = `${adminKey(D)}:${rawKey}`;
  const hit = MEMO.get(key);
  if (hit && Date.now() - hit.atMs < ttlMs) return hit.value;
  const value = await fn();
  MEMO.set(key, { atMs: Date.now(), value });
  if (MEMO.size > 64) MEMO.delete(MEMO.keys().next().value);
  return value;
}
function forgetMemo(prefix = "") { for (const k of [...MEMO.keys()]) if (k.startsWith(prefix)) MEMO.delete(k); }

/* ── opaque cursors and bounded pages (§11.4) ─────────────────────────── */
function cursorEncode(obj) { const payload = Buffer.from(JSON.stringify(obj)).toString("base64url"); return `${payload}${sha(payload).slice(0, 16)}`; }
function cursorDecode(cursor) {
  if (!cursor) return null;
  const s = String(cursor);
  if (s.length < 24) throw typed("CURSOR_INVALID", "malformed cursor");
  const payload = s.slice(0, -16), mac = s.slice(-16);
  if (sha(payload).slice(0, 16) !== mac) throw typed("CURSOR_INVALID", "cursor integrity check failed");
  try { return JSON.parse(Buffer.from(payload, "base64url").toString("utf8")); } catch { throw typed("CURSOR_INVALID", "cursor payload unreadable"); }
}
/** Page a fully-known, deterministically sorted list. The cursor binds snapshot + filter + sort + last key. */
function page(items, { cursor = null, pageSize = S.PAGE_DEFAULT, keyOf = (x) => x.resourceId || x.id, snapshotId = "none", filterHash = "none", sort = "default", asOf = new Date().toISOString(), state = null, partialReason = null, buckets = null, error = null } = {}) {
  const size = Math.max(1, Math.min(S.PAGE_MAX, Number(pageSize) || S.PAGE_DEFAULT));
  const c = cursorDecode(cursor);
  if (c && (c.s !== snapshotId || c.f !== filterHash || c.o !== sort)) throw typed("CURSOR_INVALID", "cursor belongs to another snapshot, filter or sort");
  let start = 0;
  if (c && c.k != null) { const idx = items.findIndex((x) => String(keyOf(x)) === String(c.k)); if (idx < 0) throw typed("CURSOR_INVALID", "cursor key no longer present in the snapshot"); start = idx + 1; }
  const slice = items.slice(start, start + size);
  const last = slice[slice.length - 1];
  const nextCursor = start + size < items.length && last ? cursorEncode({ s: snapshotId, f: filterHash, o: sort, k: String(keyOf(last)) }) : null;
  const collectionState = state || (error ? "FAILED" : items.length === 0 ? "EMPTY" : partialReason ? "PARTIAL" : "READY");
  return { collectionState, asOf, items: slice, completedCount: start + slice.length, totalCount: items.length, nextCursor, error, buckets: buckets || null, partialReason: partialReason || null, snapshotId };
}
function emptyCollection(asOf, error = null, state = null) { return { collectionState: state || (error ? "FAILED" : "EMPTY"), asOf, items: [], completedCount: 0, totalCount: 0, nextCursor: null, error, buckets: null, partialReason: null, snapshotId: "none" }; }
function filterHashOf(params, keys) { return sha(Object.fromEntries(keys.filter((k) => params[k] !== undefined).map((k) => [k, params[k]]))).slice(0, 16); }

/* ── control state (§14.8 orthogonal controls) ────────────────────────── */
async function controlDoc(D) { const s = await D.col(D.COL.control).doc(CONTROL_DOC).get(); return s.exists ? s.data() : {}; }
function accountModeOf(ctrl) { return ctrl.accountMode && S.ENUMS.AccountMode.includes(ctrl.accountMode) ? ctrl.accountMode : (ctrl.engineMode === "manager" && Number(ctrl.writerEpoch) > 0 ? "PAPER_AI" : "OBSERVE"); }
function managerStateOf(ctrl) { return ctrl.managerState === "PAUSED" || ctrl.managerPaused === true ? "PAUSED" : "ENABLED"; }
function buyStateOf(ctrl) { return ctrl.buyState === "FROZEN" || ctrl.freezeNewBuys === true ? "FROZEN" : "OPEN"; }
function emergencyStateOf(ctrl) { return ctrl.emergencyState && S.ENUMS.EmergencyState.includes(ctrl.emergencyState) ? ctrl.emergencyState : (ctrl.killSwitch === true ? "ENGAGED" : "CLEAR"); }
function executorStateOf(ctrl) { return ctrl.executorState && S.ENUMS.ExecutorState.includes(ctrl.executorState) ? ctrl.executorState : (ctrl.executorEnabled === false ? "PAUSED_SAFETY" : "MONITORING"); }
function controlVersionOf(ctrl) { return String(Number(ctrl.controlVersion) || 0); }
function pair(applied, req = null) {
  const r = req || {};
  return { requested: r.requested || applied, applied, requestedAt: iso(r.requestedAtMs) || null, appliedAt: iso(r.appliedAtMs) || null, reason: clip(r.reason, 300) };
}
function commitId() { return process.env.COMMIT_REF || process.env.DEPLOY_ID || "local"; }
function attestationOk(ctrl) { return ctrl.fixturesPass === true && ctrl.fixturesCommit === commitId(); }
function mutationsEnabled(ctrl) { return accountModeOf(ctrl) === "PAPER_AI" && ctrl.engineMode === "manager" && Number(ctrl.writerEpoch) > 0; }
/** Which of the global controls the server authorizes right now; the browser shows exactly these. */
function controlCapabilities(ctrl, { attested = attestationOk(ctrl), reconciled = true } = {}) {
  const mode = accountModeOf(ctrl), mgr = managerStateOf(ctrl), buy = buyStateOf(ctrl), em = emergencyStateOf(ctrl);
  const enabled = mutationsEnabled(ctrl);
  const off = (a, why) => S.capability(a, { enabled: false, disabledReason: why });
  const on = (a) => S.capability(a);
  const caps = [];
  caps.push(mgr === "ENABLED" ? on("pauseManager") : off("pauseManager", "manager already paused"));
  caps.push(mgr === "PAUSED" ? (enabled ? on("resumeManager") : off("resumeManager", "account is in OBSERVE mode")) : off("resumeManager", "manager is enabled"));
  caps.push(enabled ? (attested ? (mgr === "ENABLED" ? on("runManagerReview") : off("runManagerReview", "manager is paused")) : off("runManagerReview", "deployed build attestation failing")) : off("runManagerReview", "account is in OBSERVE mode"));
  caps.push(buy === "OPEN" ? on("freezeBuys") : off("freezeBuys", "buys already frozen"));
  caps.push(buy === "FROZEN" ? (em === "CLEAR" ? (reconciled ? on("resumeBuys") : off("resumeBuys", "reconciliation unresolved")) : off("resumeBuys", "emergency state must be CLEAR first")) : off("resumeBuys", "buys are open"));
  caps.push(em !== "ENGAGED" ? on("emergencyStop") : off("emergencyStop", "emergency already engaged"));
  caps.push(em === "ENGAGED" || em === "RECOVERING" ? on("resumeSystem") : off("resumeSystem", "emergency state is clear"));
  caps.push(mode === "OBSERVE" ? (attested ? on("activateAccountMode") : off("activateAccountMode", "deployed build attestation failing")) : off("activateAccountMode", `account mode is ${mode}`));
  caps.push(mode === "PAPER_AI" ? on("deactivateAccountMode") : off("deactivateAccountMode", `account mode is ${mode}`));
  for (const a of ["setBudget", "setRiskMandate", "setEmergencyRiskPolicy", "setMarketConfig", "freezeUniverse", "resolveCiks", "reconcile", "requestAuditExport", "createPaperAccount", "previewPaperAccountReset"]) caps.push(on(a));
  caps.push(enabled ? on("resetPaperAccount") : off("resetPaperAccount", "account is in OBSERVE mode"));
  return caps;
}
function blockingConditions(ctrl, { attested = attestationOk(ctrl) } = {}) {
  const out = [];
  if (!attested) out.push({ conditionId: "fixtures_failing", title: "Deployed build attestation failing", detail: `fixturesPass=${ctrl.fixturesPass === true} commit=${ctrl.fixturesCommit || "none"} vs ${commitId()}` });
  if (emergencyStateOf(ctrl) !== "CLEAR") out.push({ conditionId: "emergency_engaged", title: "Emergency risk state", detail: emergencyStateOf(ctrl) });
  if (executorStateOf(ctrl) === "PAUSED_SAFETY") out.push({ conditionId: "reconciliation_unresolved", title: "Executor paused for safety", detail: ctrl.executorPauseReason || null });
  if (buyStateOf(ctrl) === "FROZEN") out.push({ conditionId: "buys_frozen", title: "New buys frozen", detail: ctrl.freezeReason || (ctrl.buyRequested && ctrl.buyRequested.reason) || null });
  if (managerStateOf(ctrl) === "PAUSED") out.push({ conditionId: "manager_paused", title: "Manager paused", detail: ctrl.managerPauseReason || null });
  if (accountModeOf(ctrl) === "OBSERVE") out.push({ conditionId: "observe_mode", title: "Account in OBSERVE mode", detail: "investment mutations are disabled until PAPER_AI is activated" });
  return out;
}
function controlView(ctrl, opts = {}) {
  const attested = attestationOk(ctrl);
  return {
    resourceId: "control", resourceVersion: controlVersionOf(ctrl), availableActions: controlCapabilities(ctrl, { attested, reconciled: opts.reconciled !== false }),
    accountMode: pair(accountModeOf(ctrl), ctrl.accountModeRequested), managerState: pair(managerStateOf(ctrl), ctrl.managerRequested), buyState: pair(buyStateOf(ctrl), ctrl.buyRequested),
    emergencyState: pair(emergencyStateOf(ctrl), ctrl.emergencyRequested), executorState: pair(executorStateOf(ctrl), ctrl.executorRequested),
    writerEpoch: Number(ctrl.writerEpoch) || 0, engineMode: ctrl.engineMode || "legacy", lastTransition: ctrl.lastTransition || null, blockingConditions: blockingConditions(ctrl, { attested }), mutationsEnabled: mutationsEnabled(ctrl),
    attestation: { pass: attested, fixturesCommit: ctrl.fixturesCommit || null, commit: commitId(), fixturesCount: ctrl.fixturesCount || null, fixturesHash: ctrl.fixturesHash || null },
  };
}

/* ═══ READS ═══════════════════════════════════════════════════════════════ */

/* ── shared loaders (bounded, memoized per container) ─────────────────── */
async function latestRunDoc(D, ctrl) {
  const id = ctrl.lastManagerRunId || (ctrl.lastManagerRun && ctrl.lastManagerRun.managerRunId) || null;
  if (!id) return ctrl.lastManagerRun || null;
  const MG = lazy("./_investorManager");
  let doc = null;
  try { doc = MG ? await MG.readRun(id, { admin: D }) : null; } catch { doc = null; }
  return { ...(ctrl.lastManagerRun || {}), ...(doc || {}), managerRunId: id };
}
function runState(run) {
  const s = String((run && run.status) || "").toLowerCase();
  if (s === "complete") return "COMPLETE";
  if (s === "failed_closed" || s === "failed" || s === "dead") return "FAILED";
  if (s === "cancelled") return "CANCELLED";
  if (s === "partial") return "PARTIAL";
  if (s === "queued") return "QUEUED";
  return run ? "RUNNING" : "QUEUED";
}
function runView(run) {
  if (!run) return null;
  const cov = run.coverage || {};
  const research = run.research || {};
  const fail = run.failed || runState(run) === "FAILED" ? { reason: run.reason || run.failureReason || null, reasons: run.noBuyReasons || [], stage: run.stage || null } : null;
  return {
    managerRunId: run.managerRunId, state: runState(run), tradingDate: run.tradingDate || null, accountId: run.accountId || null, universeVersion: run.universeVersion || null, universeHash: run.universeHash || null,
    contextManifestHash: run.contextManifestHash || null, policyHash: run.policyHash || null, portfolioVersion: run.activation && run.activation.activationSnapshotId ? run.activation.activationSnapshotId : null,
    startedAt: iso(run.startedAtMs), cutoffAt: iso(run.cutoffMs), deadlineAt: iso(run.deadlineMs || run.hardDeadlineMs), completedAt: iso(run.completedAtMs),
    stage: run.stage || null, checkpoints: (run.checkpoints || run.lineage || []).slice(0, 40), segments: Number(run.segments) || null,
    coverage: { eligibleCount: Number(cov.eligibleCount) || 0, completedCount: Number(cov.completedCount) || 0, missing: (cov.missing || []).slice(0, 400), duplicates: (cov.duplicates || []).slice(0, 100), unknown: (cov.unknown || []).slice(0, 100), ok: cov.ok === true, repaired: Number(cov.repaired) || 0 },
    countsByDecision: run.byDecision || {}, decisionCount: Number(run.decisionCount) || 0, buys: (run.buys || []).slice(0, 40),
    researchJobs: { completed: Number(research.completed) || 0, failed: Number(research.failed) || 0, deferred: Number(research.deferred) || 0, ranges: research.ranges || null },
    maintenance: run.maintenance ? { actionable: Number(run.maintenance.actionable) || 0, actionRequired: (run.maintenance.actionRequired || []).slice(0, 40), deadlineMissed: run.maintenance.deadlineMissed === true, staged: run.maintenance.staged || null } : null,
    finalPlan: run.activation ? { status: run.activation.status || null, planId: run.activation.planId || null, activationSnapshotId: run.activation.activationSnapshotId || null, mandateVersionIds: (run.activation.mandateVersionIds || []).slice(0, 40), reason: run.activation.reason || null } : null,
    cost: { aiMinor: canon(run.costMinor || 0), ai: money(run.costMinor || 0), requests: Array.isArray(run.requestIds) ? run.requestIds.length : null },
    noBuyReasons: (run.noBuyReasons || []).slice(0, 12), elapsedMs: Number(run.elapsedMs) || null, failure: fail,
  };
}
async function rosterSnapshot(D, ctrl, { accountId, nowMs }) {
  const U = require("./_investorUniverse");
  const PF = require("./_investorPortfolio");
  const tradingDate = (ctrl.lastManagerRun && ctrl.lastManagerRun.tradingDate) || new Date(nowMs).toISOString().slice(0, 10);
  return memo(D, `roster:${accountId}:${tradingDate}`, MEMO_TTL_MS, async () => {
    const [positions, orders] = await Promise.all([PF.readPositions(accountId, { admin: D }), PF.readOpenOrders(accountId, { admin: D })]);
    return U.freezeEligibleSnapshot({ tradingDate, positions, pendingOrders: orders, nowMs, removed: ctrl.universeRemovals || [] });
  });
}
async function decisionsForRun(D, managerRunId) {
  if (!managerRunId) return [];
  return memo(D, `decisions:${managerRunId}`, MEMO_TTL_MS, async () => rows(await D.col(D.COL.managerDecisions).where("managerRunId", "==", managerRunId).get()).map(decode).filter((d) => d && d.symbol));
}
async function dossierPointers(D) {
  return memo(D, "dossiers", MEMO_TTL_MS, async () => { const out = {}; rows(await D.col(D.COL.dossiers).limit(2000).get()).forEach((p) => { if (p && p.symbol) out[String(p.symbol).toUpperCase()] = p; }); return out; });
}
async function pointersFor(D, accountId) { return rows(await D.col(D.COL.activeMandates).where("accountId", "==", accountId).get()); }
async function pendingDeltas(D, { symbol = null, limit = 200 } = {}) {
  const q = symbol ? D.col(D.COL.evidenceDeltas).where("symbol", "==", symbol).where("managerMateriality", "==", "pending") : D.col(D.COL.evidenceDeltas).where("managerMateriality", "==", "pending").limit(limit);
  return rows(await q.get()).sort((a, b) => Number(b.firstSeenAtMs || b.createdAtMs) - Number(a.firstSeenAtMs || a.createdAtMs)).slice(0, limit);
}
function deltaSummary(d) {
  return { deltaId: d.deltaId || d.eventId || null, symbol: d.symbol, safetyClass: d.safetyClass || null, eventClass: d.eventClass || null, summary: clip(d.title || (d.reasons || []).join("; ") || d.eventClass, 240),
    firstSeenAt: iso(d.firstSeenAtMs || d.createdAtMs), publishedAt: iso(d.publishedAtMs), managerMateriality: d.managerMateriality || null, verified: d.verified === true, sourceId: d.sourceId || null, documentVersionId: d.documentVersionId || null };
}
async function spendView(D, { nowMs, policy }) {
  const day = new Date(nowMs).toISOString().slice(0, 10);
  const s = await D.col(D.COL.costs).doc(`openai_${day}`).get();
  const d = s.exists ? s.data() : {};
  const ceiling = big((policy.budget && policy.budget.dailyReservationMinor) || 0);
  const spent = big(d.spentMinor || 0), reserved = big(d.reservedMinor || 0);
  const byRole = d.byRole || {};
  const byTask = Object.entries(byRole).map(([role, v]) => ({ task: role, model: (v && v.model) || null, calls: Number((v && v.calls) || 0), tokens: { input: String((v && v.inputTokens) || 0), cachedInput: String((v && v.cachedInputTokens) || 0), output: String((v && v.outputTokens) || 0), reasoning: String((v && v.reasoningTokens) || 0) }, actual: money((v && v.spentMinor) || 0) }));
  const t = d.tokens || {};
  return { day, byTask, tokens: { input: String(t.input || d.inputTokens || 0), cachedInput: String(t.cachedInput || 0), output: String(t.output || d.outputTokens || 0), reasoning: String(t.reasoning || 0) }, calls: Number(d.calls) || 0,
    reserved: money(reserved), actual: money(spent), tool: money(d.toolMinor || 0), data: money(d.dataMinor || 0), retries: Number(d.retries) || 0, dailyLimit: money(ceiling), remaining: money(ceiling - spent - reserved > 0n ? ceiling - spent - reserved : 0n),
    deferredWork: Number(d.deferred) || 0, latency: { p50Ms: d.p50Ms || null, p95Ms: d.p95Ms || null }, denominators: d.denominators || {}, blocked: ceiling > 0n && spent + reserved >= ceiling };
}
function alertView(a) {
  return { resourceId: a.conditionId, resourceVersion: String(Number(a.occurrences) || 1), availableActions: [S.capability("acknowledgeAlert", { enabled: a.active === true && !a.acknowledgedAtMs, disabledReason: a.active ? "already acknowledged" : "resolved" })],
    conditionId: a.conditionId, code: a.code || null, scope: a.scope || null, severity: a.severity || "info", active: a.active === true, title: a.title || a.conditionId, detail: a.detail ? clip(JSON.stringify(a.detail), 400) : null, action: a.action || null,
    firstSeenAt: iso(a.raisedAtMs), lastSeenAt: iso(a.lastSeenAtMs), acknowledgedBy: a.acknowledgedBy || null, acknowledgedAt: iso(a.acknowledgedAtMs), resolvedAt: iso(a.resolvedAtMs), resolutionEvidence: a.resolvedBy || null, correlationIds: [] };
}
function healthComponent({ state = "HEALTHY", lastSuccessAtMs = null, expectedByMs = null, backlog = null, errorCode = null, conditionId = null, operatorAction = null, nowMs = Date.now() } = {}) {
  return { state, lastSuccessAt: iso(lastSuccessAtMs), expectedBy: iso(expectedByMs), ageSeconds: lastSuccessAtMs ? Math.max(0, Math.round((nowMs - Number(lastSuccessAtMs)) / 1000)) : null, backlog, errorCode, conditionId, operatorAction };
}
async function healthMap(D, ctrl, { nowMs, accountId, pointers = null, spend = null }) {
  const MK = lazy("./_investorMarket");
  const ingest = ctrl.lastIngestPass || null, run = ctrl.lastManagerRun || null, tick = ctrl.lastExecutionTick || null;
  const day = 86400000;
  const pts = pointers || await pointersFor(D, accountId);
  const actionRequired = pts.filter((p) => p.status === "ACTION_REQUIRED" || p.status === "OVERSELL_INCIDENT").length;
  const res = await D.col(D.COL.reservationAccounts).doc(accountId).get();
  const reservation = res.exists ? res.data() : null;
  const sources = healthComponent({ state: !ingest ? "DEGRADED" : ingest.freshness && ingest.freshness.finishedBeforeFreeze === false ? "DEGRADED" : "HEALTHY", lastSuccessAtMs: ingest && (ingest.finishedAtMs || ingest.atMs), expectedByMs: ingest ? Number(ingest.finishedAtMs || ingest.atMs) + day : null,
    backlog: ingest ? Number(ingest.pending || 0) : null, errorCode: ingest && ingest.error ? "INGEST_ERROR" : null, conditionId: ingest && ingest.freshness && ingest.freshness.finishedBeforeFreeze === false ? "ingest_late" : null, operatorAction: ingest ? null : "no overnight ingest pass has completed", nowMs });
  const dossiers = healthComponent({ state: ingest && ingest.dossier && Number(ingest.dossier.failed) > 0 ? "DEGRADED" : ingest ? "HEALTHY" : "DEGRADED", lastSuccessAtMs: ingest && (ingest.finishedAtMs || ingest.atMs), backlog: ingest && ingest.dossier ? Number(ingest.dossier.pending || 0) : null, nowMs });
  const models = healthComponent({ state: spend && spend.blocked ? "ACTION_REQUIRED" : ctrl.lastModelFailureAtMs && nowMs - Number(ctrl.lastModelFailureAtMs) < 3600000 ? "DEGRADED" : "HEALTHY", lastSuccessAtMs: ctrl.lastModelSuccessAtMs || (run && run.completedAtMs) || null,
    errorCode: spend && spend.blocked ? "BUDGET_EXHAUSTED" : null, conditionId: spend && spend.blocked ? "budget_exhausted" : null, operatorAction: spend && spend.blocked ? "raise the daily reservation or wait for the reset" : null, nowMs });
  const manager = healthComponent({ state: !run ? "DEGRADED" : run.status === "failed_closed" ? "ACTION_REQUIRED" : run.coverage && run.coverage.ok === false ? "DEGRADED" : "HEALTHY", lastSuccessAtMs: run && run.status === "complete" ? run.completedAtMs : null, expectedByMs: run ? Number(run.completedAtMs || run.startedAtMs || nowMs) + day : null,
    errorCode: run && run.status === "failed_closed" ? "MANAGER_FAILED_CLOSED" : null, conditionId: run && run.status === "failed_closed" ? `manager_failed_closed:${run.managerRunId}` : null, operatorAction: run && run.status === "failed_closed" ? "read the run's typed reasons" : null, nowMs });
  const mandates = healthComponent({ state: actionRequired ? "ACTION_REQUIRED" : "HEALTHY", lastSuccessAtMs: run && run.completedAtMs, backlog: actionRequired, conditionId: actionRequired ? "action_required" : null, operatorAction: actionRequired ? "a holding needs a valid Sol mandate or an operator decision" : null, nowMs });
  const reservations = healthComponent({ state: reservation ? "HEALTHY" : "DEGRADED", lastSuccessAtMs: reservation && reservation.updatedAtMs, backlog: null, operatorAction: reservation ? null : "no reservation account yet (created by the first committed plan)", nowMs });
  const executor = healthComponent({ state: executorStateOf(ctrl) === "PAUSED_SAFETY" ? "ACTION_REQUIRED" : !tick ? "DEGRADED" : nowMs - Number(tick.atMs) > 15 * 60000 && MK && MK.sessionState(new Date(nowMs)).open ? "DEGRADED" : "HEALTHY", lastSuccessAtMs: tick && tick.atMs, expectedByMs: tick ? Number(tick.atMs) + 120000 : null,
    backlog: tick ? Number(tick.outboxApplied || 0) : null, errorCode: ctrl.executorPauseReason || null, conditionId: executorStateOf(ctrl) === "PAUSED_SAFETY" ? "reconciliation_unresolved" : null, operatorAction: executorStateOf(ctrl) === "PAUSED_SAFETY" ? "reconcile before any new order" : null, nowMs });
  const broker = healthComponent({ state: tick && tick.reasons && tick.reasons.includes("STALE_BROKER_TRUTH") ? "DEGRADED" : "HEALTHY", lastSuccessAtMs: tick && tick.atMs, errorCode: tick && tick.reasons && tick.reasons.includes("STALE_BROKER_TRUTH") ? "STALE_BROKER_TRUTH" : null, conditionId: tick && tick.reasons && tick.reasons.includes("STALE_BROKER_TRUTH") ? "stale_broker_truth" : null, nowMs });
  const ledger = healthComponent({ state: tick && tick.conservation && tick.conservation.pass === false ? "ACTION_REQUIRED" : "HEALTHY", lastSuccessAtMs: tick && tick.conservation && tick.conservation.pass !== false ? tick.atMs : null, errorCode: tick && tick.conservation && tick.conservation.pass === false ? "LEDGER_CONSERVATION_FAILED" : null, conditionId: tick && tick.conservation && tick.conservation.pass === false ? "ledger_conservation_failed" : null, nowMs });
  const cal = MK ? MK.sessionState(new Date(nowMs)) : null;
  const calendar = healthComponent({ state: cal ? "HEALTHY" : "DEGRADED", lastSuccessAtMs: nowMs, nowMs });
  return { sources, dossiers, models, manager, mandates, reservations, executor, broker, ledger, calendar };
}
function laneAsOf(ctrl) {
  const ingest = ctrl.lastIngestPass || {}, run = ctrl.lastManagerRun || {}, tick = ctrl.lastExecutionTick || {};
  return { sources: iso(ingest.finishedAtMs || ingest.atMs), dossiers: iso(ingest.finishedAtMs || ingest.atMs), managerReview: iso(run.completedAtMs), mandates: iso((run.activation && run.activation.committedAtMs) || run.completedAtMs), marks: iso(tick.atMs || (ctrl.lastPostclose && ctrl.lastPostclose.elapsedMs ? ctrl.lastPostcloseAtMs : null)) };
}
function workflowOf(ctrl, run) {
  const ingest = ctrl.lastIngestPass || null, tick = ctrl.lastExecutionTick || null;
  const stage = run ? String(run.stage || "") : "";
  const after = (s) => ["freeze", "review", "coverage", "maintenance", "research", "synthesis", "activation", "persist", "complete"].indexOf(stage) > ["freeze", "review", "coverage", "maintenance", "research", "synthesis", "activation", "persist", "complete"].indexOf(s);
  const st = (s, done, running) => (done ? "complete" : running ? "running" : "pending");
  const failed = run && run.status === "failed_closed";
  return [
    { step: "evidence", state: ingest ? (ingest.error ? "failed" : "complete") : "pending", detail: ingest ? `${ingest.companies || ingest.refreshed || 0} companies refreshed` : "overnight ingest pending", at: iso(ingest && (ingest.finishedAtMs || ingest.atMs)) },
    { step: "coverage", state: !run ? "pending" : failed && !after("coverage") ? "failed" : st("coverage", after("coverage") || run.status === "complete", ["freeze", "review", "coverage"].includes(stage)), detail: run && run.coverage ? `${run.coverage.completedCount}/${run.coverage.eligibleCount} reviewed` : "not started", at: iso(run && run.cutoffMs) },
    { step: "research", state: !run ? "pending" : failed && !after("research") ? "failed" : st("research", after("research") || run.status === "complete", stage === "research"), detail: run && run.research ? `${run.research.completed} completed, ${run.research.deferred} deferred, ${run.research.failed} failed` : "no research yet", at: null },
    { step: "revisions", state: !run ? "pending" : st("maintenance", after("maintenance") || run.status === "complete", stage === "maintenance"), detail: run && run.maintenance ? `${run.maintenance.actionable} actionable, ${(run.maintenance.actionRequired || []).length} action required` : "no holdings reviewed", at: null },
    { step: "mandates", state: !run ? "pending" : run.activation ? (run.activation.status === "COMMITTED" ? "complete" : run.activation.status === "EMPTY" ? "skipped" : "failed") : st("activation", after("activation"), stage === "activation"), detail: run && run.activation ? `${run.activation.status}${run.activation.planId ? ` plan ${run.activation.planId}` : ""}` : "no plan staged", at: iso(run && run.completedAtMs) },
    { step: "execution", state: tick ? (tick.conservation && tick.conservation.pass === false ? "failed" : "running") : "pending", detail: tick ? `${tick.outboxApplied || 0} transitions applied, ${(tick.fills && tick.fills.fills) || 0} fills` : "executor has not ticked", at: iso(tick && tick.atMs) },
  ];
}

/* ── managerDashboard (summaries only, §11.4) ─────────────────────────── */
async function readManagerDashboard({ params, ctx }) {
  const { admin: D, control: ctrl, accountId, nowMs, policy } = ctx;
  const PF = require("./_investorPortfolio");
  const AL = require("./_investorAlerts");
  const O = lazy("./_investorOpenai");
  const [snap, run, pointers, alerts, deltas, spend] = await Promise.all([
    PF.snapshot({ accountId, asOfMs: nowMs, admin: D }), latestRunDoc(D, ctrl), pointersFor(D, accountId), AL.listActive({ admin: D, accountId }), pendingDeltas(D, { limit: 50 }), spendView(D, { nowMs, policy }),
  ]);
  const decisions = await decisionsForRun(D, run && run.managerRunId);
  const avail = big(snap.settledCashMinor) - big(snap.reservedMinor);
  const health = await healthMap(D, ctrl, { nowMs, accountId, pointers, spend });
  const promptHashes = {};
  if (O && O.PROMPTS && O.promptHash) for (const k of Object.keys(O.PROMPTS)) { try { promptHashes[k] = O.promptHash(k); } catch {} }
  const byStatus = count(pointers, "status");
  const data = {
    account: { accountId, accountMode: accountModeOf(ctrl), nav: money(snap.navMinor), settledCash: money(snap.settledCashMinor), buyingPower: money(avail > 0n ? avail : 0n), reserved: money(snap.reservedMinor), available: money(avail > 0n ? avail : 0n), invested: money(snap.investedMinor), openPositions: snap.aggregates.openPositions, versions: snap.versions },
    models: { investment: { snapshot: POLICY.ROLE_MODELS.manager.model, reasoningEffort: POLICY.ROLE_MODELS.manager.reasoning.effort }, extraction: { snapshot: POLICY.ROLE_MODELS.facts.model, reasoningEffort: "none" } },
    identity: { policyHash: policy.policyHash || null, policyVersion: policy.policyVersion || POLICY.POLICY_VERSION, schemaHashes: POLICY.schemaHashes(), promptHashes, universeVersion: run ? run.universeVersion || null : null, universeHash: run ? run.universeHash || null : null, commit: commitId() },
    latestRun: runView(run), coverage: run && run.coverage ? { ...runView(run).coverage, universeVersion: run.universeVersion || null, universeHash: run.universeHash || null, tradingDate: run.tradingDate || null } : null,
    holdingReview: run && run.maintenance ? { total: decisions.filter((d) => d.held).length, revised: decisions.filter((d) => d.held && d.source === "holding_analysis").length, actionRequired: (run.maintenance.actionRequired || []).slice(0, 40), deadlineMissed: run.maintenance.deadlineMissed === true } : null,
    committedPlan: { portfolioPlanId: run && run.activation && run.activation.status === "COMMITTED" ? run.activation.planId : null, activationSnapshotId: run && run.activation ? run.activation.activationSnapshotId || null : null },
    counts: { decisions: count(decisions, "decision"), research: run && run.research ? { completed: run.research.completed || 0, failed: run.research.failed || 0, deferred: run.research.deferred || 0 } : { completed: 0, failed: 0, deferred: 0 },
      events: { unresolved: deltas.length, resolved: null }, authorizations: { active: pointers.filter((p) => !["CLOSED", "CANCELLED", "SUPERSEDED", "ENTRY_EXPIRED", "REJECTED"].includes(p.status)).length, working: (byStatus.WORKING || 0) + (byStatus.BROKER_SYNC_PENDING || 0) + (byStatus.DESIRED || 0), protected: byStatus.PROTECTED_RTH || 0, actionRequired: (byStatus.ACTION_REQUIRED || 0) + (byStatus.OVERSELL_INCIDENT || 0), byStatus } },
    spend, health, alerts: alerts.slice(0, 50).map(alertView), evidenceDeltas: deltas.slice(0, 50).map(deltaSummary),
    newDecisions: decisions.filter((d) => d.decision === "BUY" || d.changedSincePrior).sort((a, b) => (a.capitalRank || 999) - (b.capitalRank || 999)).slice(0, 50).map((d) => ({ symbol: d.symbol, decision: d.decision, reasonCode: d.reasonCode || null, reason: clip(d.reason, 240), capitalRank: d.capitalRank == null ? null : d.capitalRank, changedSincePrior: d.changedSincePrior === true, fundingState: d.fundingState || null })),
    holdingRevisions: decisions.filter((d) => d.held).slice(0, 50).map((d) => ({ symbol: d.symbol, decision: d.decision, reasonCode: d.reasonCode || null, reason: clip(d.reason, 240), revised: d.source === "holding_analysis" })),
    workflow: workflowOf(ctrl, run), control: controlView(ctrl, { reconciled: !(health.executor.state === "ACTION_REQUIRED") }), asOfByLane: laneAsOf(ctrl),
    capitalDeployment: { explanation: run && run.noBuyReasons && run.noBuyReasons.length ? run.noBuyReasons.map((r) => r.code).join(", ") : run && run.buys && run.buys.length ? `${run.buys.length} BUY mandate(s) activated` : "no Manager Meeting recorded", reserved: money(snap.reservedMinor), available: money(avail > 0n ? avail : 0n) },
  };
  return { data };
}
async function readControlState({ ctx }) { return { data: controlView(ctx.control) }; }

/* ── companies: the complete transparent coverage book ────────────────── */
function researchStateOf({ pointer, decision, jobsForSymbol = [], nowMs }) {
  if (jobsForSymbol.some((j) => j.status === "running" || j.status === "claimed" || j.status === "yielded_resumable")) return "REVISION_RUNNING";
  if (jobsForSymbol.some((j) => j.status === "queued")) return "QUEUED";
  if (decision && decision.reasonCode === "RESEARCH_DEFERRED") return "DEFERRED_BUDGET";
  if (decision && decision.reasonCode === "MODEL_FAILURE") return "FAILED";
  if (decision && decision.reviewDirective === "RESEARCH_NOW") return "FULL_RESEARCH_REQUIRED";
  if (!pointer) return "INCOMPLETE";
  if (pointer.pendingDeltaCount > 0 || decision && decision.reviewDirective === "UPDATE_EXISTING") return "UPDATE_AVAILABLE";
  const at = Number(pointer.lastManagerReviewAtMs || pointer.updatedAtMs || 0);
  if (at && nowMs - at > 30 * 86400000) return "STALE";
  if (!pointer.currentVersionId && !pointer.versionId) return "INCOMPLETE";
  return "CURRENT";
}
function companyRow({ symbol, member, bucket, exclusionReason, pointer, decision, pending, jobs, ctrl, nowMs, held, pendingOrder }) {
  const rs = researchStateOf({ pointer, decision, jobsForSymbol: jobs, nowMs });
  const enabled = mutationsEnabled(ctrl) && bucket !== "excluded";
  const caps = [S.capability("requestResearch", { enabled: enabled && rs !== "REVISION_RUNNING" && rs !== "QUEUED", disabledReason: !enabled ? (bucket === "excluded" ? "excluded from the roster" : "account is in OBSERVE mode") : "research already queued" })];
  const sv = pointer && pointer.standingView;
  return {
    resourceId: `company:${symbol}`, resourceVersion: String(pointer && pointer.versionCount ? pointer.versionCount : (pointer && pointer.currentVersionId ? 1 : 0)), availableActions: caps,
    symbol, name: (member && (member.company || member.name)) || null, sector: (member && member.sector) || null, bucket, eligible: bucket === "eligible", exclusionReason: exclusionReason || null,
    decision: decision ? decision.decision : (sv ? sv.decision || null : null), reviewDirective: decision ? decision.reviewDirective || null : null, reasonCode: decision ? decision.reasonCode || null : null, reason: decision ? clip(decision.reason, 200) : null,
    changed: decision ? decision.changedSincePrior === true : false, freshness: { thesisAt: iso(sv && sv.asOfMs), dossierAt: iso(pointer && (pointer.asOfMs || pointer.updatedAtMs)), researchAt: iso(pointer && pointer.researchAtMs), evidenceAt: iso(pointer && (pointer.lastSourceAtMs || pointer.sourceAtMs)) },
    researchState: rs, dataQuality: pointer && pointer.dataQuality ? pointer.dataQuality : null, held, pending: pendingOrder, mandateState: pending ? pending.status || null : null, capitalRank: decision && decision.capitalRank != null ? Number(decision.capitalRank) : null,
    opportunityRoute: decision && decision.mandate ? decision.mandate.opportunityRoute || null : (decision && decision.opportunityRoute) || null, managerRunId: decision ? decision.managerRunId : null, pendingDeltas: pointer ? Number(pointer.pendingDeltaCount) || 0 : 0,
  };
}
async function companyRows(D, ctrl, ctx) {
  const { accountId, nowMs } = ctx;
  const run = ctrl.lastManagerRun || null;
  const [snapshot, decisions, pointers, mandates, jobs, PF] = await Promise.all([rosterSnapshot(D, ctrl, { accountId, nowMs }), decisionsForRun(D, run && run.managerRunId), dossierPointers(D), pointersFor(D, accountId),
    memo(D, "jobs:research", MEMO_TTL_MS, async () => rows(await D.col(D.COL.jobs).where("task", "==", "focused_research").limit(300).get())), require("./_investorPortfolio")]);
  const decisionBy = new Map(decisions.map((d) => [d.symbol, d]));
  const mandateBy = new Map(mandates.filter((m) => !["CLOSED", "CANCELLED", "SUPERSEDED"].includes(m.status)).map((m) => [m.symbol, m]));
  const jobsBy = {};
  for (const j of jobs) { const s = j.payload && j.payload.symbol; if (s) (jobsBy[s] = jobsBy[s] || []).push(j); }
  const held = new Set((snapshot.managedPositionSymbols || []).concat([]));
  const pendingSyms = new Set(snapshot.pendingSymbols || []);
  const memberBy = new Map((snapshot.members || []).map((m) => [m.symbol, m]));
  const out = [];
  for (const m of snapshot.members || []) out.push(companyRow({ symbol: m.symbol, member: m, bucket: "eligible", exclusionReason: null, pointer: pointers[m.symbol], decision: decisionBy.get(m.symbol), pending: mandateBy.get(m.symbol), jobs: jobsBy[m.symbol] || [], ctrl, nowMs, held: held.has(m.symbol) || (mandateBy.get(m.symbol) || {}).status === "PROTECTED_RTH", pendingOrder: pendingSyms.has(m.symbol) }));
  for (const e of snapshot.excluded || []) { const sym = e.symbol || e; if (memberBy.has(sym)) continue; out.push(companyRow({ symbol: sym, member: e, bucket: "excluded", exclusionReason: e.exclusionReason || e.reason || "excluded", pointer: pointers[sym], decision: decisionBy.get(sym), pending: mandateBy.get(sym), jobs: jobsBy[sym] || [], ctrl, nowMs, held: held.has(sym), pendingOrder: pendingSyms.has(sym) })); }
  for (const o of snapshot.managedOffRoster || []) { const sym = o.symbol || o; if (out.some((r) => r.symbol === sym)) continue; out.push(companyRow({ symbol: sym, member: o, bucket: "managedOffRoster", exclusionReason: o.reason || "off roster, still managed", pointer: pointers[sym], decision: decisionBy.get(sym), pending: mandateBy.get(sym), jobs: jobsBy[sym] || [], ctrl, nowMs, held: true, pendingOrder: pendingSyms.has(sym) })); }
  return { rows: out, snapshotId: `${snapshot.universeHash || "u"}:${run ? run.managerRunId : "norun"}`, snapshot, run };
}
async function readCompanies({ params, ctx }) {
  const { admin: D, control: ctrl, nowMs } = ctx;
  const { rows: all, snapshotId } = await companyRows(D, ctrl, ctx);
  const search = params.search ? String(params.search).trim().toUpperCase() : "";
  let list = all;
  if (params.bucket && params.bucket !== "all") list = list.filter((r) => r.bucket === params.bucket);
  if (params.decision) list = list.filter((r) => r.decision === params.decision);
  if (params.researchState) list = list.filter((r) => r.researchState === params.researchState);
  if (params.held !== undefined) list = list.filter((r) => r.held === params.held);
  if (search) list = list.filter((r) => r.symbol.includes(search) || String(r.name || "").toUpperCase().includes(search) || String(r.sector || "").toUpperCase().includes(search));
  const sort = params.sort || "symbol";
  const cmp = { symbol: (a, b) => a.symbol.localeCompare(b.symbol), sector: (a, b) => String(a.sector || "").localeCompare(String(b.sector || "")) || a.symbol.localeCompare(b.symbol),
    decision: (a, b) => S.ENUMS.Decision.indexOf(a.decision || "ABSTAIN") - S.ENUMS.Decision.indexOf(b.decision || "ABSTAIN") || a.symbol.localeCompare(b.symbol),
    capitalRank: (a, b) => (a.capitalRank == null ? 9999 : a.capitalRank) - (b.capitalRank == null ? 9999 : b.capitalRank) || a.symbol.localeCompare(b.symbol),
    changed: (a, b) => Number(b.changed) - Number(a.changed) || a.symbol.localeCompare(b.symbol), freshness: (a, b) => String(b.freshness.dossierAt || "").localeCompare(String(a.freshness.dossierAt || "")) || a.symbol.localeCompare(b.symbol) }[sort];
  list = [...list].sort(cmp);
  if (params.snapshotId && params.snapshotId !== snapshotId) throw typed("CURSOR_INVALID", "the roster snapshot changed; reload the list");
  const buckets = { byBucket: count(all, "bucket"), byDecision: count(all, "decision"), byResearchState: count(all, "researchState"), held: all.filter((r) => r.held).length, total: all.length };
  const p = page(list, { cursor: params.cursor, pageSize: params.pageSize, keyOf: (r) => r.symbol, snapshotId, filterHash: filterHashOf(params, ["search", "bucket", "decision", "researchState", "held"]), sort, asOf: new Date(nowMs).toISOString(), buckets });
  return { data: p, nextCursor: p.nextCursor };
}

/* ── companyDossier: everything about one company, on demand ──────────── */
function chartSeries({ seriesId, version = "1", asOf, series = [], provenance = null, resolution = "1d", markers = [] }) {
  const points = (series || []).filter((b) => b && b.date && Number(b.c) > 0).map((b) => ({ t: `${b.date}T20:00:00.000Z`, o: priceUsd(b.o) ? priceUsd(b.o).priceMicros : null, h: priceUsd(b.h) ? priceUsd(b.h).priceMicros : null, l: priceUsd(b.l) ? priceUsd(b.l).priceMicros : null, c: priceUsd(b.c).priceMicros, v: String(Math.max(0, Math.round(Number(b.v) || 0))) }));
  const gaps = [];
  for (let i = 1; i < points.length; i += 1) { const d = (Date.parse(points[i].t) - Date.parse(points[i - 1].t)) / 86400000; if (d > 4) gaps.push({ from: points[i - 1].t, to: points[i].t, days: Math.round(d) }); }
  return { seriesId, version, asOf, currency: "USD", calendarId: POLICY.CALENDAR_ID || "XNYS", adjustment: provenance && provenance.adjustment || null, provenance, resolution, points: points.slice(-2000), gaps: gaps.slice(0, 100), scorable: points.length > 0 && !(provenance && provenance.homogeneous === false), markers: markers.slice(0, 200) };
}
function factRows(version, layer) {
  const facts = version && version.facts || {};
  const out = [];
  const push = (id, f, group) => { if (!f || typeof f !== "object") return; out.push({ factId: id, group, label: f.label || id, value: f.value != null ? f.value : (f.canonical != null ? f.canonical : (f.units != null ? f.units : null)), unit: f.unit || null, asOf: f.asOf || f.periodEnd || null, sourceId: f.sourceId || f.accession || null, filingFactId: f.factId || f.lineageKey || null }); };
  for (const [k, v] of Object.entries(facts[layer] || {})) push(k, v, layer);
  return out;
}
async function readCompanyDossier({ params, ctx }) {
  const { admin: D, control: ctrl, accountId, nowMs } = ctx;
  const symbol = String(params.symbol).toUpperCase();
  const DOS = require("./_investorDossier");
  const RS = require("./_investorResearch");
  const H = lazy("./_investorHistory");
  const U = require("./_investorUniverse");
  const run = ctrl.lastManagerRun || null;
  const [pointer, memo1, pending, mandatePointer, decisionSnap, historySnap, series] = await Promise.all([
    DOS.current(symbol, { admin: D }), RS.latest(symbol, { admin: D }), pendingDeltas(D, { symbol, limit: 50 }), D.col(D.COL.activeMandates).where("accountId", "==", accountId).where("symbol", "==", symbol).get().then(rows),
    run ? D.col(D.COL.managerDecisions).doc(`${run.managerRunId}_${symbol}`).get() : Promise.resolve({ exists: false }),
    D.col(D.COL.managerDecisions).where("symbol", "==", symbol).limit(100).get().then(rows).catch(() => []),
    H ? H.readDailyWithMetaFor(D, symbol).catch(() => ({ series: [], provenance: null })) : Promise.resolve({ series: [], provenance: null }),
  ]);
  const version = pointer && (pointer.currentVersionId || pointer.versionId) ? await DOS.readVersion(pointer.currentVersionId || pointer.versionId, { admin: D }).catch(() => null) : null;
  const decision = decisionSnap.exists ? decode(decisionSnap.data()) : null;
  const member = [...(U.tradeTier || []), ...(U.researchTier || []), ...(U.excludedTier || [])].find((r) => r.symbol === symbol) || null;
  const mp = mandatePointer.find((m) => !["CLOSED", "CANCELLED", "SUPERSEDED"].includes(m.status)) || mandatePointer[0] || null;
  let envelope = null, orderSet = null;
  if (mp && mp.desiredVersionId) {
    const [es, os] = await Promise.all([D.col(D.COL.activationEnvelopes).doc(mp.desiredVersionId).get(), D.col(D.COL.orderSets).doc(`os_${mp.desiredVersionId}`).get()]);
    envelope = es.exists ? decode(es.data()) : null; orderSet = os.exists ? os.data() : null;
  }
  const memoBody = memo1 && memo1.memo || {};
  const premises = [...((memo1 && memo1.factualPremises) || []).map((p) => ({ claimId: p.claimId || null, text: clip(p.text || p.claimText, 400), kind: "premise", verdict: p.verdict || p.verificationVerdict || null, sourceId: p.documentVersionId || p.sourceId || null, span: p.span || p.sourceSpan || null })),
    ...((memo1 && memo1.inferences) || []).map((i) => ({ claimId: i.inferenceId || null, text: clip(i.text, 400), kind: "inference", verdict: null, sourceId: null, span: null, premiseIds: i.premiseIds || [] }))];
  const entryLeg = orderSet && (orderSet.legs || []).find((l) => l.role === "ENTRY"), targetLeg = orderSet && (orderSet.legs || []).find((l) => l.role === "TARGET"), stopLeg = orderSet && (orderSet.legs || []).find((l) => l.role === "STOP");
  const markers = [];
  if (entryLeg && entryLeg.priceMicros) markers.push({ t: iso(mp.updatedAtMs || nowMs), kind: "entry", label: "AI limit (executable)", price: price(entryLeg.priceMicros) });
  if (targetLeg && targetLeg.priceMicros) markers.push({ t: iso(mp.updatedAtMs || nowMs), kind: "target", label: "Profit target", price: price(targetLeg.priceMicros) });
  if (stopLeg && stopLeg.stopMicros) markers.push({ t: iso(mp.updatedAtMs || nowMs), kind: "boundary", label: "Planned loss boundary", price: price(stopLeg.stopMicros) });
  if (memoBody.valuation && memoBody.valuation.preferredZone) markers.push({ t: iso(memo1.createdAtMs), kind: "valuation", label: "Preferred valuation zone (not executable)", price: price(memoBody.valuation.preferredZone.lowMicros), priceHigh: price(memoBody.valuation.preferredZone.highMicros) });
  for (const c of (memoBody.catalysts || []).slice(0, 20)) if (c && c.expectedDate) markers.push({ t: `${String(c.expectedDate).slice(0, 10)}T20:00:00.000Z`, kind: "catalyst", label: clip(c.text || c.kind, 80), price: null });
  const sourceSpans = version && version.pointers ? version.pointers : {};
  const sources = [...new Map([...((memo1 && memo1.sourceManifest) || []).map((s) => [s.documentVersionId || s.claimId, { sourceId: s.documentVersionId || null, claimId: s.claimId || null, title: null, url: null, publishedAt: null, contentHash: null }]),
    ...((version && version.documents) || []).map((d) => [d.versionId || d.documentVersionId || d.documentId, { sourceId: d.versionId || d.documentVersionId || d.documentId || null, title: clip(d.title || d.form, 160), url: typeof d.url === "string" && /^https:\/\//.test(d.url) ? d.url : null, publishedAt: isoOf(d.publishedAt || d.publishedAtMs), contentHash: d.contentHash || null }])]).values()].slice(0, 200);
  const data = {
    symbol, identity: { name: (member && (member.company || member.name)) || (version && version.identity && version.identity.name) || null, sector: (member && member.sector) || null, cik: (member && member.cik) || (version && version.identity && version.identity.cik) || null, exchange: null, bucket: member ? (member.exclusionReason ? "excluded" : member.tier === "research" ? "research" : "eligible") : "unknown" },
    identityHistory: (version && version.identity && version.identity.history) || [],
    facts: { common: factRows(version, "common"), sector: factRows(version, "sector"), asOf: version ? iso(version.asOfMs) : null, versionId: version ? version.versionId || null : null, contentHash: version ? version.contentHash || null : null, sectorBlock: version ? version.sectorBlock || null : null },
    deltas: pending.map(deltaSummary), premises,
    thesis: memo1 ? { summary: clip(memoBody.thesis && (memoBody.thesis.summary || memoBody.thesis), 2000), opposition: clip(memoBody.opposition && (memoBody.opposition.summary || memoBody.opposition) || memoBody.bearCase, 2000), uncertaintyLevel: memoBody.uncertaintyLevel || (memoBody.uncertainty && memoBody.uncertainty.level) || null, thesisHealth: memo1.thesisHealth || "UNKNOWN", researchVersion: memo1.researchVersion || null, memoId: memo1.memoId || null, asOf: memo1.asOf || iso(memo1.createdAtMs), directive: memo1.directive || null } : null,
    valuation: memoBody.valuation ? { method: memoBody.valuation.method || null, inputs: (memoBody.valuation.assumptions || memoBody.valuation.inputs || []).slice(0, 40), outputs: memoBody.valuation.outputs || memoBody.valuation.derived || null, preferredZone: memoBody.valuation.preferredZone || null, note: "derived outputs are calculated by the deterministic valuation module from the model's declared assumptions" } : null,
    forecast: memoBody.forecast ? { basis: memoBody.forecast.basis || null, outcomeBuckets: (memoBody.forecast.outcomeBuckets || []).slice(0, 12), fillProbabilityByExpiryPpm: memoBody.forecast.fillProbabilityByExpiryPpm || null, horizonTradingDays: memoBody.forecast.horizonTradingDays || null, uncertaintyLevel: memoBody.forecast.uncertaintyLevel || null } : null,
    decision: decision ? { decision: decision.decision, reasonCode: decision.reasonCode || null, reason: clip(decision.reason, 600), capitalRank: decision.capitalRank == null ? null : decision.capitalRank, reviewDirective: decision.reviewDirective || null, provisionalDisposition: decision.provisionalDisposition || null, fundingState: decision.fundingState || null, managerRunId: decision.managerRunId, asOf: iso(decision.asOfMs), changedSincePrior: decision.changedSincePrior === true, source: decision.source || null } : null,
    mandate: mp ? { state: mp.status || null, entryState: mp.entryState || null, entry: entryLeg ? price(entryLeg.priceMicros) : null, target: targetLeg ? price(targetLeg.priceMicros) : (mp.takeProfitPriceMicros ? price(mp.takeProfitPriceMicros) : null), boundary: stopLeg ? price(stopLeg.stopMicros) : (mp.lossBoundaryPriceMicros ? price(mp.lossBoundaryPriceMicros) : null),
      authorizedQuantity: envelope && envelope.authorizedQuantityUnits != null ? qty(envelope.authorizedQuantityUnits) : null, validFrom: iso(envelope && envelope.validatedAtMs), expiresAt: iso(mp.expiresAtMs), mandateVersionId: mp.desiredVersionId || null, appliedVersionId: mp.appliedVersionId || null, mandateSeriesId: mp.mandateSeriesId || null, planClass: mp.planClass || null, capitalRank: mp.capitalRank == null ? null : mp.capitalRank,
      plannedLoss: envelope ? money(envelope.plannedLossAtBoundaryMinor || 0) : null, reservedNotional: envelope ? money(envelope.reservedNotionalMinor || 0) : null, bindingConstraint: envelope ? envelope.bindingConstraint || null : null } : null,
    hashes: { decision: decision ? decision.contentHash || null : null, mandate: mp ? mp.desiredVersionId || null : null, binding: envelope ? envelope.bindingHash || null : null, envelope: envelope ? envelope.contentHash || envelope.envelopeHash || null : null, dossier: version ? version.contentHash || null : null, memo: memo1 ? memo1.outputHash || null : null },
    catalysts: (memoBody.catalysts || []).slice(0, 50), invalidators: (memoBody.invalidators || []).slice(0, 50).map((i) => ({ predicateType: i.predicateType || null, consequence: i.consequence || null, text: clip(i.text || i.description, 300) })), sources,
    decisionHistory: historySnap.map(decode).filter(Boolean).sort((a, b) => Number(b.asOfMs) - Number(a.asOfMs)).slice(0, 100).map((d) => ({ managerRunId: d.managerRunId, tradingDate: d.tradingDate || null, decision: d.decision, reasonCode: d.reasonCode || null, capitalRank: d.capitalRank == null ? null : d.capitalRank, asOf: iso(d.asOfMs) })),
    chart: chartSeries({ seriesId: `daily:${symbol}`, asOf: new Date(nowMs).toISOString(), series: series.series, provenance: series.provenance, markers }),
    freshness: { dossierAt: iso(pointer && (pointer.asOfMs || pointer.updatedAtMs)), researchAt: iso(memo1 && memo1.createdAtMs), managerReviewAt: iso(pointer && pointer.lastManagerReviewAtMs), pendingDeltas: pending.length },
  };
  return { data };
}

/* ── portfolio, mandates, order sets, execution events ────────────────── */
const TERMINAL_POINTER = new Set(["CLOSED", "CANCELLED", "SUPERSEDED", "ENTRY_EXPIRED", "REJECTED"]);
function mandateStateOf(p) { const s = p && p.status; return S.ENUMS.MandateState.includes(s) ? s : (s === "DESIRED" ? "VALIDATED" : s === "COMMITTED" ? "RESERVED" : "ERROR"); }
function pointerCaps(p, ctrl, { held = false } = {}) {
  const enabled = mutationsEnabled(ctrl);
  const st = mandateStateOf(p);
  const active = !TERMINAL_POINTER.has(p.status);
  const off = (a, why) => S.capability(a, { enabled: false, disabledReason: why });
  const on = (a) => S.capability(a);
  return [
    !enabled ? off("pauseMandate", "account is in OBSERVE mode") : active && !["PAUSED_OPERATIONAL", "PAUSED_EVIDENCE", "CANCEL_PENDING"].includes(st) && p.entryState !== "CANCELLED" ? on("pauseMandate") : off("pauseMandate", `mandate is ${st}`),
    !enabled ? off("resumeMandate", "account is in OBSERVE mode") : st === "PAUSED_OPERATIONAL" && !(p.pendingDeltaCount > 0) ? on("resumeMandate") : off("resumeMandate", st === "PAUSED_EVIDENCE" ? "an evidence pause needs a new Sol version" : st === "PAUSED_OPERATIONAL" ? "an unreviewed delta blocks same-hash resume" : `mandate is ${st}`),
    !enabled ? off("cancelMandate", "account is in OBSERVE mode") : active && st !== "CANCEL_PENDING" ? on("cancelMandate") : off("cancelMandate", `mandate is ${st}`),
    !enabled ? off("requestSell", "account is in OBSERVE mode") : held ? (emergencyStateOf(ctrl) === "ENGAGED" ? off("requestSell", "emergency policy engaged") : on("requestSell")) : off("requestSell", "no owned shares"),
  ];
}
function legView(l) {
  return { legId: l.legId, role: l.role, side: l.side, type: l.type, status: l.status || null, price: l.priceMicros ? price(l.priceMicros) : null, stop: l.stopMicros ? price(l.stopMicros) : null, collarBps: l.collarBps == null ? null : canon(l.collarBps), quantity: qty(l.quantityUnits || 0), remaining: qty(l.remainingUnits != null ? l.remainingUnits : (l.quantityUnits || 0)), filled: qty(l.filledUnits || 0),
    timeInForce: l.timeInForce || null, session: l.regularSessionOnly === false ? "EXTENDED" : "RTH", sessionDates: l.sessionDates || null, ocoGroup: l.ocoGroup || null, brokerOrderId: l.brokerOrderId || null, providerIds: l.providerIds || null, submitAt: l.submitAt || null, activatesOn: l.activatesOn || null, onHaltOrNonfill: l.onHaltOrNonfill || null };
}
function fillView(f) { return { fillId: f.fillId, legId: f.legId, role: f.role || null, side: f.side || null, quantity: qty(f.quantityUnits || 0), price: price(f.priceMicros), notional: money(f.notionalMinor || 0), fee: money(f.feeMinor || 0), basis: f.basis || null, source: f.source || null, eventAt: iso(f.eventAtMs), receivedAt: iso(f.receivedAtMs), bar: f.bar || null, provenance: f.provenance || null, brokerFillId: f.brokerFillId || null, ambiguous: f.ambiguous === true }; }
async function orderSetsFor(D, accountId) { return memo(D, `ordersets:${accountId}`, 5000, async () => rows(await D.col(D.COL.orderSets).where("accountId", "==", accountId).get())); }
async function fillsFor(D, accountId) { return memo(D, `fills:${accountId}`, 5000, async () => rows(await D.col(D.COL.fills).where("accountId", "==", accountId).get()).sort((a, b) => Number(b.receivedAtMs || b.atMs) - Number(a.receivedAtMs || a.atMs)).slice(0, 600)); }
function orderSetView(os, fills, ctrl) {
  const legs = (os.legs || []).map(legView);
  const fl = fills.filter((f) => f.orderSetId === os.orderSetId).map(fillView);
  const isOperatorSell = os.purpose === "OPERATOR_SELL";
  const cancelable = isOperatorSell && ["DESIRED", "WORKING", "PENDING"].includes(os.status) && fl.length === 0;
  return { resourceId: os.orderSetId, resourceVersion: String(Number(os.version) || 1), availableActions: isOperatorSell ? [S.capability("cancelSell", { enabled: mutationsEnabled(ctrl) && cancelable, disabledReason: !mutationsEnabled(ctrl) ? "account is in OBSERVE mode" : fl.length ? "execution has begun" : `order set is ${os.status}` })] : [],
    ids: { orderSetId: os.orderSetId, planId: os.planId || null, mandateVersionId: os.mandateVersionId || null, accountId: os.accountId, reservationId: os.reservationId || null, overrideId: os.overrideId || null }, symbol: os.symbol, purpose: os.purpose || "MANDATE", authority: os.authority || (isOperatorSell ? "OPERATOR" : os.purpose === "EMERGENCY_RISK" ? "EMERGENCY_RISK" : "AI_MANDATE"),
    desiredState: os.status || "DESIRED", appliedState: os.brokerGroupId ? "APPLIED" : null, broker: { groupId: os.brokerGroupId || null, capabilityVersion: os.capabilityVersion || null, acknowledgedAt: iso(os.acknowledgedAtMs), provider: os.provider || null }, reservation: os.reservationId ? { reservationId: os.reservationId } : null,
    legs, fills: fl, coverageWindow: os.coverageWindow || (legs.some((l) => l.role === "STOP") ? { session: "RTH", overnight: "gap_exposed" } : null), reconciliationCursor: os.reconciliationCursor || null, error: os.error || os.lastError || null, createdAt: iso(os.createdAtMs), appliedMandateVersionId: os.appliedMandateVersionId || null };
}
async function authorizationViews(D, ctrl, { accountId, pointers, orderSets, fills, envelopesById = null }) {
  const out = [];
  for (const p of pointers) {
    const os = orderSets.find((o) => o.mandateVersionId === p.desiredVersionId) || null;
    let env = envelopesById ? envelopesById[p.desiredVersionId] : null;
    if (!env && p.desiredVersionId) { const es = await D.col(D.COL.activationEnvelopes).doc(p.desiredVersionId).get(); env = es.exists ? decode(es.data()) : null; }
    const legs = os ? (os.legs || []) : [];
    const entry = legs.find((l) => l.role === "ENTRY"), target = legs.find((l) => l.role === "TARGET"), stop = legs.find((l) => l.role === "STOP");
    const fl = os ? fills.filter((f) => f.orderSetId === os.orderSetId) : [];
    const filled = fl.filter((f) => f.role === "ENTRY").reduce((n, f) => n + big(f.quantityUnits), 0n);
    const authorized = env && env.authorizedQuantityUnits != null ? big(env.authorizedQuantityUnits) : (entry ? big(entry.quantityUnits) : 0n);
    out.push({
      resourceId: p.mandateSeriesId || `${accountId}_${p.symbol}`, resourceVersion: String(Number(p.desiredVersion) || 0), availableActions: pointerCaps(p, ctrl, { held: p.status === "PROTECTED_RTH" || filled > 0n }), symbol: p.symbol,
      ids: { mandateSeriesId: p.mandateSeriesId || null, mandateVersionId: p.desiredVersionId || null, appliedVersionId: p.appliedVersionId || null, proposalId: env ? env.proposalId || null : null, bindingId: env ? env.bindingId || null : null, envelopeId: env ? env.envelopeId || p.desiredVersionId : null, portfolioPlanId: env ? env.portfolioPlanId || null : null, activationSnapshotId: p.activationSnapshotId || null, orderSetId: os ? os.orderSetId : null, reservationId: env ? env.reservationId || null : null },
      hashes: { proposal: env ? env.proposalHash || null : null, binding: env ? env.bindingHash || null : null, envelope: env ? env.contentHash || env.envelopeHash || null : null, plan: env ? env.planHash || null : null, desired: p.desiredVersionId || null },
      desiredVersion: p.desiredVersionId || null, appliedVersion: p.appliedVersionId || null, state: mandateStateOf(p), entryState: p.entryState || null, protectionState: p.protectionState || null, planClass: p.planClass || null, decision: p.decision || null, capitalRank: p.capitalRank == null ? null : p.capitalRank,
      proposedQuantity: env && env.proposedQuantityUnits != null ? qty(env.proposedQuantityUnits) : null, authorizedQuantity: env || entry ? qty(authorized) : null, filledQuantity: qty(filled), remainingQuantity: qty(authorized > filled ? authorized - filled : 0n),
      reservations: env ? { notional: money(env.reservedNotionalMinor || 0), plannedLoss: money(env.plannedLossAtBoundaryMinor || 0), stressLoss: money(env.bindingGapStressLossMinor || 0) } : {}, clamp: env ? { bindingConstraint: env.bindingConstraint || null, reasons: env.reasons || env.clampReasons || [], authorizedBelowProposed: env.proposedQuantityUnits != null && big(env.proposedQuantityUnits) > authorized } : null,
      entry: entry ? price(entry.priceMicros) : null, target: target ? price(target.priceMicros) : (p.takeProfitPriceMicros ? price(p.takeProfitPriceMicros) : null), boundary: stop ? price(stop.stopMicros) : (p.lossBoundaryPriceMicros ? price(p.lossBoundaryPriceMicros) : null),
      validFrom: iso(env && env.validatedAtMs), expiresAt: iso(p.expiresAtMs), timeInForce: entry ? entry.timeInForce || null : null, authorizedSessions: (entry && entry.sessionDates ? entry.sessionDates : []).slice(0, 10).map(sessionRef).filter(Boolean),
      legs: legs.map(legView), fills: fl.map(fillView), broker: { groupId: p.brokerGroupId || (os && os.brokerGroupId) || null, capability: os ? os.capabilityVersion || null : null, coverage: stop ? "RTH_PROTECTED" : "NONE", syncState: os ? os.status || null : "NO_ORDER_SET" }, errors: p.actionRequiredReason ? [{ code: "ACTION_REQUIRED", message: p.actionRequiredReason }] : [], overrideLineage: p.overrides || [],
      paused: p.paused === true, pausedReason: p.pausedReason || null, pendingDeltas: Number(p.pendingDeltaCount) || 0, updatedAt: iso(p.updatedAtMs),
    });
  }
  return out;
}
function holdingView(p, { snap, pointer, env, ctrl, nowMs }) {
  const mark = big(p.markMicros), boundary = p.lossBoundaryPriceMicros ? big(p.lossBoundaryPriceMicros) : null, target = p.takeProfitPriceMicros ? big(p.takeProfitPriceMicros) : null;
  const units = big(p.quantityUnits);
  const forward = boundary && mark > boundary ? (mark - boundary) * units / 10000n : 0n;
  const stressed = env && env.bindingGapStressLossMinor ? big(env.bindingGapStressLossMinor) : (boundary ? forward * 3n / 2n : null);
  const st = pointer ? pointerCaps(pointer, ctrl, { held: true }) : [S.capability("requestSell", { enabled: mutationsEnabled(ctrl) && emergencyStateOf(ctrl) !== "ENGAGED", disabledReason: mutationsEnabled(ctrl) ? "emergency policy engaged" : "account is in OBSERVE mode" })];
  return {
    resourceId: `position:${snap.accountId}_${p.symbol}`, resourceVersion: String(Number(snap.versions.portfolioVersion) || 0), availableActions: st,
    ids: { positionId: `${snap.accountId}_${p.symbol}`, positionLifecycleId: p.positionLifecycleId || p.lifecycleId || null, mandateSeriesId: pointer ? pointer.mandateSeriesId || null : null, mandateVersionId: p.mandateVersionId || (pointer && pointer.appliedVersionId) || null, envelopeId: env ? env.mandateVersionId || null : null, orderSetId: pointer && pointer.desiredVersionId ? `os_${pointer.desiredVersionId}` : null, decisionId: pointer ? pointer.decisionId || null : null },
    symbol: p.symbol, sector: p.sector || null, quantity: qty(units), averageCost: p.entryPriceMicros ? price(p.entryPriceMicros) : null, mark: mark > 0n ? price(mark) : null, markAt: isoOf(p.markAt), marketValue: money(p.marketValueMinor), unrealizedPnl: money(p.unrealisedMinor), costBasis: money(p.costBasisMinor || 0),
    weightBps: weightBps(p.marketValueMinor, snap.navMinor), forwardDownside: boundary ? money(forward) : null, stressedDownside: stressed != null ? money(stressed) : null, thesisHealth: (pointer && pointer.thesisHealth) || "UNKNOWN",
    target: target ? price(target) : null, lossBoundary: boundary ? price(boundary) : null, timeExit: pointer && pointer.expiresAtMs ? sessionRef(new Date(Number(pointer.expiresAtMs)).toISOString().slice(0, 10)) : null,
    distanceToTargetBps: target && mark > 0n ? bpsOf(mark, target) : null, distanceToBoundaryBps: boundary && mark > 0n ? bpsOf(mark, boundary) : null,
    protection: { state: p.protectionState || (boundary ? "PROTECTED_RTH" : "UNPROTECTED"), coverage: boundary ? "RTH" : "NONE", acknowledged: p.protectionAcknowledged === true, overnight: boundary ? "gap_exposed" : "unprotected" },
    emergencyRank: pointer && pointer.emergencyRank != null ? pointer.emergencyRank : null, emergencyExpiry: iso(pointer && pointer.emergencyExpiresAtMs), labels: [p.engine === "legacy" ? "legacy position" : null, snap.aggregates && snap.aggregates.unprotected && snap.aggregates.unprotected.includes(p.symbol) ? "unprotected" : null, pointer && pointer.status === "ACTION_REQUIRED" ? "action required" : null].filter(Boolean),
    nextReview: pointer ? { at: iso(pointer.nextReviewAtMs), reason: pointer.nextReviewReason || null } : null, planVersion: pointer ? pointer.desiredVersion || null : null, openedAt: isoOf(p.openedAt),
  };
}
async function readPortfolio({ params, ctx }) {
  const { admin: D, control: ctrl, accountId, nowMs } = ctx;
  const PF = require("./_investorPortfolio");
  const [snap, pointers, orderSets, fills, outbox, events] = await Promise.all([PF.snapshot({ accountId, asOfMs: nowMs, admin: D }), pointersFor(D, accountId), orderSetsFor(D, accountId), fillsFor(D, accountId),
    rows(await D.col(D.COL.executionOutbox).where("accountId", "==", accountId).get()).filter((t) => ["PENDING", "FAILED", "DEAD"].includes(t.status)).sort((a, b) => Number(b.createdAtMs) - Number(a.createdAtMs)).slice(0, 100),
    rows(await D.col(D.COL.mandateEvents).where("accountId", "==", accountId).get()).sort((a, b) => Number(b.atMs) - Number(a.atMs)).slice(0, 200)]);
  const pointerBy = new Map(pointers.filter((p) => !TERMINAL_POINTER.has(p.status)).map((p) => [p.symbol, p]));
  const envelopesById = {};
  for (const p of pointers) if (p.desiredVersionId && !envelopesById[p.desiredVersionId]) { const es = await D.col(D.COL.activationEnvelopes).doc(p.desiredVersionId).get(); envelopesById[p.desiredVersionId] = es.exists ? decode(es.data()) : null; }
  const asOf = new Date(nowMs).toISOString();
  const holdings = snap.positions.map((p) => holdingView(p, { snap, pointer: pointerBy.get(p.symbol) || null, env: pointerBy.get(p.symbol) ? envelopesById[pointerBy.get(p.symbol).desiredVersionId] : null, ctrl, nowMs })).sort((a, b) => a.symbol.localeCompare(b.symbol));
  const active = pointers.filter((p) => !TERMINAL_POINTER.has(p.status)).sort((a, b) => a.symbol.localeCompare(b.symbol));
  const auths = await authorizationViews(D, ctrl, { accountId, pointers: active, orderSets, fills, envelopesById });
  const working = orderSets.filter((o) => !["CLOSED", "CANCELLED", "FILLED", "COMPLETE"].includes(o.status)).sort((a, b) => Number(b.createdAtMs) - Number(a.createdAtMs)).map((o) => orderSetView(o, fills, ctrl));
  const queue = outbox.map((t) => ({ transitionId: t.transitionId, kind: t.kind, status: t.status, symbol: t.symbol || null, authority: t.authority || null, createdAt: iso(t.createdAtMs), attempts: Number(t.attempts) || 0, error: t.error || null, mandateVersionId: t.mandateVersionId || null, orderSetId: t.orderSetId || null }));
  const revisions = events.filter((e) => ["VALIDATED_AND_DESIRED", "SUPERSEDED", "ENTRY_REVOKED", "PAUSED", "RESUMED", "CANCELLED", "ACTION_REQUIRED", "PROTECTION_RETAINED"].includes(e.kind)).slice(0, 100).map((e) => ({ symbol: e.symbol, mandateVersionId: e.mandateVersionId || null, version: e.sequence == null ? null : e.sequence, kind: e.kind, at: iso(e.atMs), detail: clip(JSON.stringify({ ...e, accountId: undefined, symbol: undefined, kind: undefined, atMs: undefined }), 300) }));
  const collection = (items, key) => ({ collectionState: items.length ? "READY" : "EMPTY", asOf, items, completedCount: items.length, totalCount: items.length, nextCursor: null, error: null, buckets: null, partialReason: null, snapshotId: `${snap.contentHash}` });
  const realized = fills.filter((f) => String(f.side).toLowerCase() === "sell").length;
  const data = {
    summary: { accountId, nav: money(snap.navMinor), settledCash: money(snap.settledCashMinor), reserved: money(snap.reservedMinor), invested: money(snap.investedMinor), unrealizedPnl: money(snap.positions.reduce((n, p) => n + big(p.unrealisedMinor), 0n)), realizedPnl: money(await realizedMinor(D, accountId)), openPositions: snap.aggregates.openPositions, unprotected: snap.aggregates.unprotected || [], versions: snap.versions, contentHash: snap.contentHash, asOf, sellFills: realized },
    holdings: collection(holdings), standingAuthorizations: collection(auths), workingOrders: collection(working), executionQueue: collection(queue), planRevisions: collection(revisions),
  };
  return { data };
}
async function realizedMinor(D, accountId) { try { return rows(await D.col(D.COL.trades).where("accountId", "==", accountId).get()).filter((t) => t.engineVersion === "manager").reduce((n, t) => n + big(t.realizedMinor || 0), 0n); } catch { return 0n; } }
async function readMandates({ params, ctx }) {
  const { admin: D, control: ctrl, accountId, nowMs } = ctx;
  const [pointers, orderSets, fills] = await Promise.all([pointersFor(D, accountId), orderSetsFor(D, accountId), fillsFor(D, accountId)]);
  const status = params.status || "active";
  let list = pointers.filter((p) => (status === "all" ? true : status === "active" ? !TERMINAL_POINTER.has(p.status) : TERMINAL_POINTER.has(p.status)));
  if (params.symbol) list = list.filter((p) => p.symbol === params.symbol);
  list.sort((a, b) => Number(b.updatedAtMs || 0) - Number(a.updatedAtMs || 0) || a.symbol.localeCompare(b.symbol));
  const views = await authorizationViews(D, ctrl, { accountId, pointers: list, orderSets, fills });
  const p = page(views, { cursor: params.cursor, pageSize: params.pageSize, keyOf: (v) => v.resourceId, snapshotId: `mandates:${accountId}:${sha(list.map((x) => x.mandateSeriesId + ":" + x.updatedAtMs)).slice(0, 12)}`, filterHash: filterHashOf(params, ["status", "symbol"]), asOf: new Date(nowMs).toISOString(), buckets: { byState: count(pointers, "status") } });
  return { data: p, nextCursor: p.nextCursor };
}
async function readOrderSets({ params, ctx }) {
  const { admin: D, control: ctrl, accountId, nowMs } = ctx;
  const [orderSets, fills] = await Promise.all([orderSetsFor(D, accountId), fillsFor(D, accountId)]);
  let list = orderSets;
  if (params.symbol) list = list.filter((o) => o.symbol === params.symbol);
  if (params.status) list = list.filter((o) => o.status === params.status);
  list = [...list].sort((a, b) => Number(b.createdAtMs) - Number(a.createdAtMs)).map((o) => orderSetView(o, fills, ctrl));
  const p = page(list, { cursor: params.cursor, pageSize: params.pageSize, keyOf: (v) => v.resourceId, snapshotId: `ordersets:${accountId}:${sha(orderSets.map((o) => o.orderSetId + o.status)).slice(0, 12)}`, filterHash: filterHashOf(params, ["symbol", "status"]), asOf: new Date(nowMs).toISOString(), buckets: { byStatus: count(orderSets, "status") } });
  return { data: p, nextCursor: p.nextCursor };
}
function eventView(e, source) {
  const authority = e.authority === "EMERGENCY_RISK" || String(e.mandateVersionId || "").startsWith("EMERGENCY_RISK") ? "EMERGENCY_RISK" : e.authority === "OPERATOR" || e.purpose === "OPERATOR_SELL" ? "OPERATOR" : "AI_MANDATE";
  return { eventId: e.brokerEventId || e.eventId || `${e.mandateVersionId || e.orderSetId || "x"}_${e.sequence != null ? String(e.sequence).padStart(4, "0") : e.atMs}`, correlationId: e.mandateVersionId || e.orderSetId || e.transitionId || null, accountId: e.accountId || null, symbol: e.symbol || null,
    ids: { mandateVersionId: e.mandateVersionId || null, mandateSeriesId: e.mandateSeriesId || null, orderSetId: e.orderSetId || null, legId: e.legId || null, planId: e.planId || null, transitionId: e.transitionId || null, brokerGroupId: e.brokerGroupId || null }, authority, type: e.kind || e.type || "EVENT", state: e.status || null,
    requested: e.requested || (e.authorizedQuantityUnits ? { authorizedQuantity: qty(e.authorizedQuantityUnits) } : null), observed: e.observed || (e.priceMicros ? { price: price(e.priceMicros), quantity: e.quantityUnits ? qty(e.quantityUnits) : null } : null), eventAt: iso(e.eventAtMs || e.atMs), receivedAt: iso(e.receivedAtMs || e.atMs), brokerSource: source === "broker" ? e.provider || "paper" : null, dedupeId: e.dedupeKey || e.idempotencyKey || null, ledgerIds: e.ledgerIds || (e.txnId ? [e.txnId] : []) };
}
async function readExecutionEvents({ params, ctx }) {
  const { admin: D, accountId, nowMs } = ctx;
  const [me, be] = await Promise.all([rows(await D.col(D.COL.mandateEvents).where("accountId", "==", accountId).get()), rows(await D.col(D.COL.brokerEvents).where("accountId", "==", accountId).get())]);
  let list = [...me.map((e) => eventView(e, "mandate")), ...be.map((e) => eventView(e, "broker"))];
  if (params.symbol) list = list.filter((e) => e.symbol === params.symbol);
  if (params.sinceMs) list = list.filter((e) => Date.parse(e.eventAt || 0) >= Number(params.sinceMs));
  list.sort((a, b) => String(b.eventAt || "").localeCompare(String(a.eventAt || "")) || a.eventId.localeCompare(b.eventId));
  list = list.slice(0, 1000);
  const p = page(list, { cursor: params.cursor, pageSize: params.pageSize, keyOf: (v) => v.eventId, snapshotId: `events:${accountId}:${list.length}:${list[0] ? list[0].eventId : ""}`, filterHash: filterHashOf(params, ["symbol", "sinceMs"]), asOf: new Date(nowMs).toISOString(), buckets: { byAuthority: count(list, "authority"), byType: count(list, "type") } });
  return { data: p, nextCursor: p.nextCursor };
}

/* ── manager runs, jobs, journal, analytics, performance ──────────────── */
async function readManagerRuns({ params, ctx }) {
  const { admin: D, control: ctrl, nowMs } = ctx;
  if (params.managerRunId) { const MG = require("./_investorManager"); const run = await MG.readRun(params.managerRunId, { admin: D }); const v = run ? runView(run) : null; return { data: v ? page([v], { keyOf: (x) => x.managerRunId, snapshotId: params.managerRunId, asOf: new Date(nowMs).toISOString() }) : emptyCollection(new Date(nowMs).toISOString(), S.errorShape("NOT_FOUND", "unknown manager run")) }; }
  let list = rows(await (params.tradingDate ? D.col(D.COL.managerRuns).where("tradingDate", "==", params.tradingDate) : D.col(D.COL.managerRuns).limit(200)).get()).map(decode).filter((r) => r && r.managerRunId);
  if (ctrl.lastManagerRun && !list.some((r) => r.managerRunId === ctrl.lastManagerRun.managerRunId) && (!params.tradingDate || ctrl.lastManagerRun.tradingDate === params.tradingDate)) list.push(ctrl.lastManagerRun);
  list = list.sort((a, b) => Number(b.startedAtMs || b.updatedAtMs || 0) - Number(a.startedAtMs || a.updatedAtMs || 0)).map(runView);
  const p = page(list, { cursor: params.cursor, pageSize: params.pageSize, keyOf: (v) => v.managerRunId, snapshotId: `runs:${list.length}:${list[0] ? list[0].managerRunId : ""}`, filterHash: filterHashOf(params, ["tradingDate"]), asOf: new Date(nowMs).toISOString(), buckets: { byState: count(list, "state") } });
  return { data: p, nextCursor: p.nextCursor };
}
function jobView(j) {
  const cp = j.checkpoint || null;
  const progress = cp && cp.data && cp.data.coverage ? { numerator: Number(cp.data.coverage.completedCount) || 0, denominator: Number(cp.data.coverage.eligibleCount) || null } : cp && cp.progress ? cp.progress : { numerator: null, denominator: null };
  return { jobId: j.jobId, task: j.task, handler: j.targetFunction || null, attempt: Number(j.attempts) || 0, state: String(j.status || "queued").toUpperCase(), priority: Number(j.priority) || 100, lease: j.workerLeaseExpiresAt ? { expiresAt: iso(j.workerLeaseExpiresAt), owner: j.workerOwner || null } : null,
    checkpoint: cp ? { stage: cp.stage || null, at: iso(cp.atMs), segment: cp.segment == null ? null : cp.segment } : null, createdAt: iso(j.enqueuedAtMs), dueAt: iso(j.dueAtMs || j.resumeAtMs), heartbeatAt: iso(j.heartbeatAtMs), completedAt: iso(j.finishedAtMs), pollingSlaSeconds: j.heavy ? 900 : 60,
    progress, cost: j.summary && j.summary.costMinor != null ? money(j.summary.costMinor) : null, retryLineage: (j.lineage || []).map((l) => `${l.jobId}#${l.attempt}`).slice(0, 20), error: j.lastError ? { code: j.lastError.code || null, message: clip(j.lastError.message, 300), retryable: j.lastError.retryable === true } : null,
    accountId: j.accountId || null, runId: j.runId || null, sessionDate: j.sessionDate || null, symbol: j.payload && j.payload.symbol || null, segments: Number(j.segments) || 0, summary: j.summary ? clip(JSON.stringify(j.summary), 400) : null };
}
async function readJobs({ params, ctx }) {
  const { admin: D, nowMs } = ctx;
  if (params.jobId) { const s = await D.col(D.COL.jobs).doc(params.jobId).get(); return { data: s.exists ? page([jobView(s.data())], { keyOf: (v) => v.jobId, snapshotId: params.jobId, asOf: new Date(nowMs).toISOString() }) : emptyCollection(new Date(nowMs).toISOString(), S.errorShape("NOT_FOUND", "unknown job")) }; }
  let list = rows(await (params.status ? D.col(D.COL.jobs).where("status", "==", params.status).limit(300) : D.col(D.COL.jobs).limit(300)).get()).filter((j) => j && j.jobId && j.kind !== "run_lease");
  if (params.task) list = list.filter((j) => j.task === params.task);
  list = list.sort((a, b) => Number(b.enqueuedAtMs || 0) - Number(a.enqueuedAtMs || 0)).map(jobView);
  const p = page(list, { cursor: params.cursor, pageSize: params.pageSize, keyOf: (v) => v.jobId, snapshotId: `jobs:${list.length}:${list[0] ? list[0].jobId : ""}`, filterHash: filterHashOf(params, ["status", "task"]), asOf: new Date(nowMs).toISOString(), buckets: { byState: count(list, "state"), byTask: count(list, "task") } });
  return { data: p, nextCursor: p.nextCursor };
}
function journalRow(d, forecast) {
  return { decisionId: `${d.managerRunId}_${d.symbol}`, managerRunId: d.managerRunId, tradingDate: d.tradingDate || null, symbol: d.symbol, decision: d.decision, reviewDirective: d.reviewDirective || null, reasonCode: d.reasonCode || null, rationale: clip(d.reason, 600),
    versions: { contextManifestHash: d.contextManifestHash || null, researchMemoId: d.researchMemoId || null, maintenancePlanId: d.maintenancePortfolioPlanId || null, expansionPlanId: d.expansionPortfolioPlanId || null }, hashes: { decision: d.contentHash || null, mandateProposal: d.mandateProposalHash || null },
    forecast: forecast ? { referencePrice: price(forecast.referencePriceMicros), referenceType: forecast.referencePriceType || null, horizons: forecast.horizons || [], declaredHorizon: forecast.declaredHorizonTradingDays || null, outcomeBuckets: forecast.outcomeBuckets || null, status: forecast.status || null } : null,
    alternatives: [], activationState: d.fundingState || null, resultState: forecast ? forecast.status : null, outcomes: forecast && forecast.resolved ? { resolved: forecast.resolved, excursions: forecast.excursions || null, scoring: forecast.scoring || null } : null, attributionAuthority: "AI_MANDATE",
    capitalRank: d.capitalRank == null ? null : d.capitalRank, held: d.held === true, changedSincePrior: d.changedSincePrior === true, source: d.source || null, asOf: iso(d.asOfMs) };
}
async function readDecisionJournal({ params, ctx }) {
  const { admin: D, control: ctrl, nowMs } = ctx;
  const L = require("./_investorLearning");
  let list;
  if (params.symbol) list = rows(await D.col(D.COL.managerDecisions).where("symbol", "==", params.symbol).limit(400).get());
  else if (params.managerRunId) list = rows(await D.col(D.COL.managerDecisions).where("managerRunId", "==", params.managerRunId).get());
  else if (params.tradingDate) list = rows(await D.col(D.COL.managerDecisions).where("tradingDate", "==", params.tradingDate).limit(400).get());
  else list = ctrl.lastManagerRun ? await decisionsForRun(D, ctrl.lastManagerRun.managerRunId) : [];
  list = list.map(decode).filter((d) => d && d.symbol);
  if (params.decision) list = list.filter((d) => d.decision === params.decision);
  list.sort((a, b) => Number(b.asOfMs || 0) - Number(a.asOfMs || 0) || (a.capitalRank || 999) - (b.capitalRank || 999) || a.symbol.localeCompare(b.symbol));
  const snapshotId = `journal:${sha(list.map((d) => `${d.managerRunId}_${d.symbol}`)).slice(0, 12)}`;
  const p = page(list, { cursor: params.cursor, pageSize: params.pageSize, keyOf: (d) => `${d.managerRunId}_${d.symbol}`, snapshotId, filterHash: filterHashOf(params, ["symbol", "managerRunId", "tradingDate", "decision"]), asOf: new Date(nowMs).toISOString(), buckets: { byDecision: count(list, "decision") } });
  const items = [];
  for (const d of p.items) { const fs = await D.col(D.COL.forecasts).doc(L.forecastIdFor({ managerRunId: d.managerRunId, symbol: d.symbol })).get(); items.push(journalRow(d, fs.exists ? fs.data() : null)); }
  return { data: { ...p, items }, nextCursor: p.nextCursor };
}
async function readDecisionAnalytics({ params, ctx }) {
  const { admin: D, control: ctrl, nowMs } = ctx;
  const K = require("./_investorKpi");
  const horizon = params.horizon || 20;
  const [cfs, fcs] = await Promise.all([rows(await D.col(D.COL.counterfactuals).limit(400).get()), rows(await D.col(D.COL.forecasts).where("status", "==", "RESOLVED").limit(400).get())]);
  const rowsH = cfs.filter((c) => Number(c.horizonTradingDays) === Number(horizon) && (!params.managerRunId || c.managerRunId === params.managerRunId)).sort((a, b) => String(b.tradingDate || "").localeCompare(String(a.tradingDate || "")));
  const resolved = fcs.filter((f) => f.resolved && f.resolved[`h${horizon}`] && (!params.managerRunId || f.managerRunId === params.managerRunId));
  const events = resolved.filter((f) => f.outcomeBuckets && f.outcomeBuckets.length).map((f) => { const gain = f.outcomeBuckets.filter((b) => big(b.returnBps || 0) > 0n).reduce((n, b) => n + big(b.probabilityPpm || 0), 0n); return { probabilityPpm: gain.toString(), occurred: big(f.resolved[`h${horizon}`].returnBps) > 0n }; });
  const calibration = events.length ? K.calibrationTable({ events }) : null;
  const selected = resolved.filter((f) => f.decision === "BUY").map((f) => ({ symbol: f.symbol, returnBps: f.resolved[`h${horizon}`].returnBps, sector: null, sizeBucket: null }));
  const abstentions = resolved.filter((f) => f.decision === "ABSTAIN").map((f) => ({ symbol: f.symbol, laterReturnBps: f.resolved[`h${horizon}`].returnBps }));
  const lift = rowsH.length ? { rows: rowsH.length, meanSelectedMinusUnselectedBps: rowsH.some((r) => r.selectedMinusUnselectedBps != null) ? M.toCanonical(M.divRound(rowsH.filter((r) => r.selectedMinusUnselectedBps != null).reduce((n, r) => n + big(r.selectedMinusUnselectedBps), 0n), BigInt(Math.max(1, rowsH.filter((r) => r.selectedMinusUnselectedBps != null).length)), M.ROUNDING.HALF_EVEN)) : null, note: "point-in-time eligible controls; equal-weight means; identical cost assumptions" } : null;
  const data = {
    cohort: { definition: "every ManagerDecision row of the frozen eligible snapshot; selected = BUY; unselected = every other eligible decision", managerRunId: params.managerRunId || null, decisions: resolved.length, runs: new Set(resolved.map((f) => f.managerRunId)).size },
    benchmark: { symbol: "SPY", convention: "arithmetic price return from the cutoff close" }, horizon, method: { dependence: "overlapping-horizon returns are dependent; effective sample size is the number of distinct runs", effectiveSampleSize: new Set(resolved.map((f) => f.managerRunId)).size, confidenceInterval: null, note: "no interval is reported below 30 independent runs" },
    calibration, selection: lift, counterfactuals: rowsH.slice(0, 100), errors: abstentions.length ? K.abstentionQuality({ abstentions }) : null, exclusions: [{ reason: "unresolved horizon", count: fcs.length - resolved.length }], versions: { policyHash: ctrl.lastManagerRun ? ctrl.lastManagerRun.policyHash || null : null, kpiVersion: K.KPI_VERSION }, asOf: new Date(nowMs).toISOString(),
  };
  return { data };
}
async function readPerformance({ params, ctx }) {
  const { admin: D, accountId, nowMs } = ctx;
  const K = require("./_investorKpi");
  const H = lazy("./_investorHistory");
  const range = params.range || "3M";
  const days = { "1M": 31, "3M": 93, "6M": 186, "1Y": 366, ALL: 3650 }[range] || 93;
  const fromDate = new Date(nowMs - days * 86400000).toISOString().slice(0, 10);
  const navDocs = rows(await D.col(D.COL.navMarks).where("accountId", "==", accountId).get()).filter((d) => d.finalMark && d.finalMark.navMinor != null && String(d.date) >= fromDate).sort((a, b) => String(a.date).localeCompare(String(b.date)));
  const navSeries = navDocs.map((d) => ({ date: String(d.date), navMinor: String(d.finalMark.navMinor) }));
  let bench = null;
  try { const b = H ? await H.readDailyWithMetaFor(D, "SPY") : null; bench = b && b.series ? b.series.filter((x) => x.date >= fromDate) : null; } catch { bench = null; }
  const stats = navSeries.length ? K.portfolioStatistics({ navSeries, cashFlows: [], benchmarkSeries: bench && bench.length ? bench.map((b) => ({ date: b.date, closeMicros: priceUsd(b.c).priceMicros })) : null }) : null;
  const fills = await fillsFor(D, accountId);
  const feeMinor = fills.reduce((n, f) => n + big(f.feeMinor || 0), 0n);
  const dates = [...new Set([...navDocs.map((d) => String(d.date)), new Date(nowMs).toISOString().slice(0, 10)])].slice(-120);
  let aiMinor = 0n;
  for (const d of dates) { const s = await D.col(D.COL.costs).doc(`openai_${d}`).get(); if (s.exists) aiMinor += big(s.data().spentMinor || 0); }
  const PF = require("./_investorPortfolio");
  const snap = await PF.snapshot({ accountId, asOfMs: nowMs, admin: D });
  const data = {
    seriesIds: { nav: `nav:${accountId}:${range}`, benchmark: "daily:SPY" }, basis: { totalReturn: "time-weighted", cash: "settled", tax: "pre-tax", sampling: "one final mark per trading day", flows: "none recorded in Manager v1" },
    deposits: money(0), withdrawals: money(0), realizedPnl: money(await realizedMinor(D, accountId)), unrealizedPnl: money(snap.positions.reduce((n, p) => n + big(p.unrealisedMinor), 0n)), costs: { trading: money(feeMinor), ai: money(aiMinor), data: money(0) },
    returns: stats ? { twrBps: stats.timeWeightedReturnBps, annualizedBps: stats.statistics.annualizedReturnBps, benchmarkBps: stats.benchmarkReturnBps, excessBps: stats.benchmarkExcessBps, observations: stats.observations, smallSampleWarning: stats.smallSampleWarning === true, benchmarkReason: stats.benchmarkReason || null } : { twrBps: null, annualizedBps: null, benchmarkBps: null, excessBps: null, observations: 0, smallSampleWarning: true, benchmarkReason: "no NAV marks" },
    drawdown: stats ? { maxBps: stats.statistics.maxDrawdownBps, cvar95Bps: stats.statistics.cvar95Bps } : { maxBps: null, cvar95Bps: null }, volatility: stats ? { sharpeFloat: stats.statistics.sharpeFloat, sortinoFloat: stats.statistics.sortinoFloat, calmarFloat: stats.statistics.calmarFloat, deflatedSharpeFloat: stats.statistics.deflatedSharpeFloat, observations: stats.observations, precision: "float64_display_only", smallSampleWarning: stats.smallSampleWarning === true } : { sharpeFloat: null, sortinoFloat: null, calmarFloat: null, observations: 0, smallSampleWarning: true },
    exposure: (() => { try { const c = K.concentration({ averageNavMinor: snap.navMinor, grossExposureMinor: snap.investedMinor, positions: snap.positions.map((p) => ({ marketValueMinor: p.marketValueMinor })) }); return { grossBps: c.grossExposureBps, top1Bps: c.top1WeightBps, top5Bps: c.top5WeightBps, hhiPpm: c.hhiPpm }; } catch { return { grossBps: null, top1Bps: null, top5Bps: null, hhiPpm: null }; } })(),
    stress: null, confidence: null,
    nav: { series: { seriesId: `nav:${accountId}:${range}`, version: "1", asOf: new Date(nowMs).toISOString(), currency: "USD", calendarId: POLICY.CALENDAR_ID || "XNYS", adjustment: null, provenance: { source: "postclose_final_mark" }, resolution: "1d", points: navSeries.map((r) => ({ t: `${r.date}T20:00:00.000Z`, navMinor: r.navMinor })), gaps: [], scorable: navSeries.length > 1, markers: fills.filter((f) => f.receivedAtMs).slice(0, 200).map((f) => ({ t: iso(f.eventAtMs || f.receivedAtMs), kind: String(f.side).toLowerCase() === "buy" ? "buy" : (f.role === "TARGET" ? "fill" : f.role === "STOP" ? "fill" : "sell"), label: `${f.symbol} ${f.role || f.side}`, price: price(f.priceMicros) })) },
      benchmark: bench && bench.length ? chartSeries({ seriesId: "daily:SPY", asOf: new Date(nowMs).toISOString(), series: bench }) : null },
    period: { range, fromDate, toDate: new Date(nowMs).toISOString().slice(0, 10), navMarks: navSeries.length },
  };
  return { data };
}

/* ── material events, corporate actions, system, universe, sources ────── */
function materialEventView(d, ctrl, pointerBy) {
  const p = pointerBy.get(d.symbol) || null;
  const unresolved = d.managerMateriality === "pending";
  return { resourceId: d.deltaId || d.eventId, resourceVersion: String(Number(d.version) || 1), availableActions: [S.capability("runFocusedRevision", { enabled: mutationsEnabled(ctrl) && unresolved, disabledReason: !mutationsEnabled(ctrl) ? "account is in OBSERVE mode" : "already reviewed" })],
    symbol: d.symbol, eventClass: d.eventClass || d.safetyClass || "unknown", safetyClass: d.safetyClass || null, headline: clip(d.title, 240), sourceUrl: typeof d.sourceUrl === "string" && /^https:\/\//.test(d.sourceUrl) ? d.sourceUrl : null, sourceManifest: { sourceId: d.sourceId || null, documentId: d.documentId || null, documentVersionId: d.documentVersionId || null, canonicalEventHash: d.canonicalEventHash || null },
    publishedAt: iso(d.publishedAtMs), firstSeenAt: iso(d.firstSeenAtMs || d.createdAtMs), verification: { verified: d.verified === true, reasons: (d.reasons || []).slice(0, 8) }, affectedMandate: p ? { mandateSeriesId: p.mandateSeriesId || null, status: p.status || null, entryState: p.entryState || null } : null,
    safetyPause: !!(p && (p.status === "PAUSED_EVIDENCE" || p.pausedReason === "evidence")), review: { managerMateriality: d.managerMateriality || null, reviewedAt: iso(d.reviewedAtMs), reviewJobId: d.reviewJobId || null, resolution: d.resolution || null }, discoveryOnly: d.discoveryOnly === true };
}
async function readMaterialEvents({ params, ctx }) {
  const { admin: D, control: ctrl, accountId, nowMs } = ctx;
  const status = params.status || "unresolved";
  const pending = status === "resolved" ? [] : await pendingDeltas(D, { symbol: params.symbol || null, limit: 300 });
  const resolved = status === "unresolved" ? [] : rows(await D.col(D.COL.evidenceDeltas).where("managerMateriality", "!=", "pending").limit(300).get()).filter((d) => !params.symbol || d.symbol === params.symbol);
  const pointers = await pointersFor(D, accountId);
  const pointerBy = new Map(pointers.filter((p) => !TERMINAL_POINTER.has(p.status)).map((p) => [p.symbol, p]));
  const list = [...pending, ...resolved].sort((a, b) => Number(b.firstSeenAtMs || b.createdAtMs || 0) - Number(a.firstSeenAtMs || a.createdAtMs || 0)).map((d) => materialEventView(d, ctrl, pointerBy));
  const p = page(list, { cursor: params.cursor, pageSize: params.pageSize, keyOf: (v) => v.resourceId, snapshotId: `events:${status}:${list.length}:${list[0] ? list[0].resourceId : ""}`, filterHash: filterHashOf(params, ["status", "symbol"]), asOf: new Date(nowMs).toISOString(), buckets: { unresolved: pending.length, resolved: resolved.length, bySafetyClass: count(list, "safetyClass") } });
  return { data: p, nextCursor: p.nextCursor };
}
function corporateActionView(c, ctrl) {
  const state = c.state && S.ENUMS.CorporateActionState.includes(c.state) ? c.state : (c.confirmedAtMs || c.rebasedAtMs ? "REBASED" : c.quarantined ? "QUARANTINED" : "AWAITING_CONFIRMATION");
  const type = c.type || (c.shareRatio ? "SPLIT" : c.perShareUsd != null || c.perShareMicros != null ? "CASH_DIVIDEND" : "UNKNOWN");
  const enabled = mutationsEnabled(ctrl) && state !== "REBASED";
  return { resourceId: c.corporateActionId || c.id || `${c.symbol}_${c.effectiveDate || c.recordDate || "na"}`, resourceVersion: String(Number(c.version) || 1),
    availableActions: [S.capability("confirmSplit", { enabled: enabled && type === "SPLIT", disabledReason: type === "SPLIT" ? (state === "REBASED" ? "already rebased" : "account is in OBSERVE mode") : "not a split" }), S.capability("confirmCashDividend", { enabled: enabled && type === "CASH_DIVIDEND", disabledReason: type === "CASH_DIVIDEND" ? (state === "REBASED" ? "already rebased" : "account is in OBSERVE mode") : "not a cash dividend" })],
    type, state, symbol: c.symbol, source: c.source || c.sourceRef || null, effectiveDate: c.effectiveDate || c.exDate || c.recordDate || null, terms: { shareRatio: c.shareRatio || null, perShare: c.perShareMicros ? price(c.perShareMicros) : (c.perShareUsd != null ? priceUsd(c.perShareUsd) : null), recordDate: c.recordDate || null, payDate: c.payDate || null, oldSecurity: c.oldSymbol || null, newSecurity: c.newSymbol || null },
    affected: { positionLifecycleId: c.positionLifecycleId || null, eligibleQuantity: c.eligibleQty != null ? qty(c.eligibleQty) : null, mandateSeriesId: c.mandateSeriesId || null, orderSetIds: c.orderSetIds || [] }, preview: c.preview || (c.proposedBasis ? { proposedBasis: c.proposedBasis } : null), history: (c.history || []).slice(0, 20), detectedAt: iso(c.detectedAtMs || c.createdAtMs), confirmedAt: iso(c.confirmedAtMs) };
}
async function readCorporateActions({ params, ctx }) {
  const { admin: D, control: ctrl, accountId, nowMs } = ctx;
  const PF = require("./_investorPortfolio");
  const [docs, positions] = await Promise.all([rows(await D.col(D.COL.corporateActions).limit(200).get()), PF.readPositions(accountId, { admin: D })]);
  const list = docs.map((c) => corporateActionView(c, ctrl));
  for (const p of positions) if (p.corporateActionPending && !list.some((x) => x.symbol === p.symbol && x.state !== "REBASED")) list.push(corporateActionView({ symbol: p.symbol, quarantined: true, type: p.corporateActionPending.type || "SPLIT", shareRatio: p.corporateActionPending.shareRatio || null, positionLifecycleId: p.positionLifecycleId || null, eligibleQty: p.quantityUnits || p.qty, detectedAtMs: p.corporateActionPending.detectedAtMs || p.updatedAtMs, preview: p.corporateActionPending }, ctrl));
  const filtered = params.status ? list.filter((x) => x.state === params.status) : list;
  const p = page(filtered, { cursor: params.cursor, pageSize: params.pageSize, keyOf: (v) => v.resourceId, snapshotId: `corp:${list.length}`, filterHash: filterHashOf(params, ["status"]), asOf: new Date(nowMs).toISOString(), buckets: { byState: count(list, "state"), byType: count(list, "type") } });
  return { data: p, nextCursor: p.nextCursor };
}
async function readSystemHealth({ ctx }) {
  const { admin: D, control: ctrl, accountId, nowMs, policy } = ctx;
  const B = lazy("./_investorBroker");
  const O = lazy("./_investorOpenai");
  const MK = lazy("./_investorMarket");
  const [spend, pointers, queued, outboxPending, deltas, exportsJobs] = await Promise.all([spendView(D, { nowMs, policy }), pointersFor(D, accountId),
    rows(await D.col(D.COL.jobs).where("status", "==", "queued").limit(200).get()), rows(await D.col(D.COL.executionOutbox).where("status", "==", "PENDING").limit(200).get()), pendingDeltas(D, { limit: 200 }), rows(await D.col(D.COL.jobs).where("task", "==", "audit_export").limit(50).get())]);
  const health = await healthMap(D, ctrl, { nowMs, accountId, pointers, spend });
  const promptHashes = {};
  if (O && O.PROMPTS && O.promptHash) for (const k of Object.keys(O.PROMPTS)) { try { promptHashes[k] = O.promptHash(k); } catch {} }
  const adapter = B ? (() => { try { return B.adapterFor({ control: ctrl }); } catch { return null; } })() : null;
  const risk = policy.riskMandate || {};
  let market = { provider: null, feed: null, delayMinutes: null, credentialsPresent: false };
  try { if (MK) { const p = MK.activeProvider(); market = { provider: p.id || null, feed: p.feed || null, delayMinutes: p.delayMinutes == null ? null : p.delayMinutes, credentialsPresent: MK.providerCredentialed ? MK.providerCredentialed("alpaca") === true : false }; } } catch {}
  const U = require("./_investorUniverse");
  const data = {
    health, backlog: { jobs: queued.length, outbox: outboxPending.length, deltas: deltas.length, researchQueue: queued.filter((j) => j.task === "focused_research").length },
    identities: { policyHash: policy.policyHash || null, policyVersion: policy.policyVersion || POLICY.POLICY_VERSION, riskMandateHash: policy.riskMandateHash || (risk && risk.riskMandateHash) || null, emergencyPolicyHash: policy.emergencyRiskPolicyActive && policy.emergencyRiskPolicy ? policy.emergencyRiskPolicy.policyHash || null : null, schemaHashes: POLICY.schemaHashes(), promptHashes,
      roles: { investment: { model: POLICY.ROLE_MODELS.manager.model, reasoningEffort: POLICY.ROLE_MODELS.manager.reasoning.effort, authority: POLICY.ROLE_MODELS.manager.authority }, extraction: { model: POLICY.ROLE_MODELS.facts.model, reasoningEffort: "none", authority: POLICY.ROLE_MODELS.facts.authority }, verification: { model: POLICY.ROLE_MODELS.verification.model, reasoningEffort: "none", authority: POLICY.ROLE_MODELS.verification.authority }, forbiddenInvestmentModels: POLICY.FORBIDDEN_INVESTMENT_MODELS },
      universeVersion: U.version || null, universeHash: ctrl.lastManagerRun ? ctrl.lastManagerRun.universeHash || null : null, gatewayVersion: O ? O.GATEWAY_VERSION || null : null, apiBuild: API_BUILD, contractHash: S.contractHash(), commit: commitId(), fixtures: { pass: ctrl.fixturesPass === true, commit: ctrl.fixturesCommit || null, count: ctrl.fixturesCount || null, hash: ctrl.fixturesHash || null, matchesBuild: attestationOk(ctrl) } },
    configuration: { account: { accountId, accountMode: accountModeOf(ctrl), engineMode: ctrl.engineMode || "legacy", writerEpoch: Number(ctrl.writerEpoch) || 0 },
      riskMandate: { values: risk, hash: policy.riskMandateHash || null, version: risk.version || null, overrides: ctrl.riskMandateOverrides || null, overridesApplied: policy.riskOverridesApplied || [], overridesRefused: policy.riskOverridesRefused || [], bounds: POLICY.RISK_MANDATE_BOUNDS },
      emergencyRiskPolicy: { active: policy.emergencyRiskPolicyActive === true, reason: policy.emergencyRiskPolicyReason || null, policyHash: policy.emergencyRiskPolicy ? policy.emergencyRiskPolicy.policyHash || null : null, status: policy.emergencyRiskPolicy ? policy.emergencyRiskPolicy.status || null : null, template: POLICY.EMERGENCY_RISK_POLICY_TEMPLATE },
      budget: { dailyReservationMinor: canon((policy.budget && policy.budget.dailyReservationMinor) || 0), byRoleMinor: (ctrl.budget && ctrl.budget.byRoleMinor) || null, alertThresholdPpm: (ctrl.budget && ctrl.budget.alertThresholdPpm) || "800000", version: (ctrl.budget && ctrl.budget.version) || 0, source: ctrl.budget ? "control" : "policy_default" },
      universe: { version: U.version || null, eligibleCount: (U.tradeTier || []).length, excludedCount: (U.excludedTier || []).length, researchCount: (U.researchTier || []).length, removals: (ctrl.universeRemovals || []).slice(0, 40), lastFreeze: ctrl.lastUniverseFreeze || null },
      sources: { registryVersion: ctrl.sourceRegistryVersion || "1", issuerDomains: ctrl.issuerDomainOverrides || {}, scope: ctrl.sourceScope || null }, corporateActions: { pending: 0 }, market,
      executor: { cadenceSeconds: 60, dataResolution: "5-minute bars; daily closes for marks", adapter: adapter ? adapter.adapter : null, capabilityMatrix: B ? B.CAPABILITY_MATRIX : null, liveAdapter: adapter && adapter.adapter === "alpaca" ? "enabled" : "disabled" } },
    audit: { lastArchive: ctrl.lastArchiveSummary || null, lastArchiveDate: ctrl.lastArchiveDate || null, lastPostclose: ctrl.lastPostclose ? { ok: ctrl.lastPostclose.ok, tradingDate: ctrl.lastPostclose.tradingDate, errors: ctrl.lastPostclose.errors || [] } : null, lastPostcloseDate: ctrl.lastPostcloseDate || null, exports: exportsJobs.map(jobView).slice(0, 20), lastTransition: ctrl.lastTransition || null, v1Telemetry: ctrl.v1ActionTelemetry || {} },
    control: controlView(ctrl), availableActions: controlCapabilities(ctrl).concat([S.capability("setIssuerDomains"), S.capability("acknowledgeAlert")]), spend, asOf: new Date(nowMs).toISOString(),
  };
  return { data };
}
async function readUniverse({ params, ctx }) {
  const { admin: D, control: ctrl, accountId, nowMs } = ctx;
  const snapshot = await rosterSnapshot(D, ctrl, { accountId, nowMs });
  const data = { snapshot: { universeVersion: snapshot.universeVersion, universeHash: snapshot.universeHash, eligibleCount: snapshot.eligibleCount, frozenAt: iso(snapshot.frozenAtMs), tradingDate: snapshot.tradingDate || null, excluded: (snapshot.excluded || []).map((e) => ({ symbol: e.symbol || e, reason: e.exclusionReason || e.reason || null })).slice(0, 400),
    managedOffRoster: (snapshot.managedOffRoster || []).map((o) => ({ symbol: o.symbol || o, reason: o.reason || null })), researchOnly: (snapshot.researchOnly || []).map((r) => r.symbol || r).slice(0, 200), eligibility: snapshot.eligibility || null, identitySnapshot: snapshot.identitySnapshot ? { hash: snapshot.identitySnapshot.hash || null, unresolvedCiks: snapshot.identitySnapshot.unresolvedCiks || null } : null },
    members: params.includeMembers ? (snapshot.members || []).map((m) => ({ symbol: m.symbol, name: m.company || m.name || null, sector: m.sector || null, cik: m.cik || null })) : undefined };
  return { data };
}
function sourceStateView(id, x, nowMs) {
  const failures = Number(x.consecutiveFailures) || 0;
  const last = Number(x.lastSuccessAtMs || x.lastFetchedAtMs || x.updatedAtMs) || null;
  const age = last ? Math.round((nowMs - last) / 1000) : null;
  const state = failures >= 5 ? "DOWN" : failures > 0 ? "DEGRADED" : age != null && age > 3 * 86400 ? "STALE" : "CURRENT";
  return { sourceId: id, kind: x.kind || (id.startsWith("sec.") ? "sec" : id.split("_")[0]), symbol: x.symbol || null, state, lastSuccessAt: iso(last), ageSeconds: age, failures, error: clip(x.lastError, 200), domains: x.domains || [], etag: x.etag || null, lastStatus: x.lastStatus || null };
}
async function readSources({ params, ctx }) {
  const { admin: D, nowMs } = ctx;
  const snap = await D.col(D.COL.sourceState).limit(500).get();
  const list = [];
  snap.forEach((d) => list.push(sourceStateView(d.id || (d.data() || {}).sourceId || "source", d.data() || {}, nowMs)));
  const filtered = (params.symbol ? list.filter((s) => s.symbol === params.symbol || s.sourceId.includes(params.symbol)) : list).sort((a, b) => a.sourceId.localeCompare(b.sourceId));
  const p = page(filtered, { cursor: params.cursor, pageSize: params.pageSize, keyOf: (v) => v.sourceId, snapshotId: `sources:${list.length}`, filterHash: filterHashOf(params, ["symbol"]), asOf: new Date(nowMs).toISOString(), buckets: { byState: count(list, "state"), byKind: count(list, "kind") } });
  return { data: p, nextCursor: p.nextCursor };
}
async function readSoakStatus({ ctx }) {
  const { admin: D, control: ctrl, accountId, nowMs } = ctx;
  const runs = rows(await D.col(D.COL.managerRuns).limit(200).get()).map(decode).filter((r) => r && r.managerRunId);
  const complete = runs.filter((r) => r.status === "complete");
  const exact = complete.filter((r) => r.coverage && r.coverage.ok === true).length;
  const pointers = await pointersFor(D, accountId);
  let legacy = null;
  try { const V1 = require("./investorApi"); legacy = (await V1.ACTIONS.soakStatus({ accountId })).soak || null; } catch { legacy = null; }
  const data = { manager: { sessions: complete.length, coverageExact: exact, failures: runs.filter((r) => r.status === "failed_closed").length, lastRunId: ctrl.lastManagerRunId || null },
    executor: { ticks: ctrl.executorTicks || null, unprotectedIntervals: Number(ctrl.unprotectedIntervals) || 0, oversells: pointers.filter((p) => p.status === "OVERSELL_INCIDENT").length, lastTickAt: iso(ctrl.lastExecutionTick && ctrl.lastExecutionTick.atMs) },
    unscorableIntervals: (ctrl.unscorableIntervals || []).slice(0, 50), legacy, asOf: new Date(nowMs).toISOString() };
  return { data };
}
function exportView(j, ctx) {
  const AUTH = require("./_investorAuth");
  const done = j.status === "complete" && j.summary && j.summary.contentHash;
  let manifest = null;
  if (done) {
    const expiresAtMs = ctx.nowMs + 15 * 60 * 1000;
    const token = AUTH.signPayload("download", { h: j.summary.contentHash, sid: ctx.auth && ctx.auth.sessionId || null, exp: expiresAtMs });
    manifest = { manifestId: j.summary.contentHash, sha256: j.summary.contentHash, bytes: Number(j.summary.bytes) || 0, downloadUrl: `/.netlify/functions/investorApi?download=${encodeURIComponent(token || "")}`, downloadExpiresAt: iso(expiresAtMs) };
  }
  return { jobId: j.jobId, resourceVersion: String(Number(j.attempts) || 0), snapshot: { fromDate: j.payload && j.payload.fromDate || null, toDate: j.payload && j.payload.toDate || null, scope: j.payload && j.payload.scope || "all", requestedBy: j.payload && j.payload.requestedBy || null }, filter: j.payload || {}, state: String(j.status || "queued").toUpperCase(),
    requestedAt: iso(j.enqueuedAtMs), completedAt: iso(j.finishedAtMs), expiresAt: iso(Number(j.finishedAtMs || j.enqueuedAtMs) + 7 * 86400000), manifest, error: j.lastError ? { code: j.lastError.code || null, message: clip(j.lastError.message, 300) } : null };
}
async function readAuditExports({ params, ctx }) {
  const { admin: D, nowMs } = ctx;
  let list = rows(await D.col(D.COL.jobs).where("task", "==", "audit_export").limit(100).get());
  if (params.jobId) list = list.filter((j) => j.jobId === params.jobId);
  list = list.sort((a, b) => Number(b.enqueuedAtMs || 0) - Number(a.enqueuedAtMs || 0)).map((j) => exportView(j, ctx));
  const p = page(list, { cursor: params.cursor, pageSize: params.pageSize, keyOf: (v) => v.jobId, snapshotId: `exports:${list.length}`, filterHash: filterHashOf(params, ["jobId"]), asOf: new Date(nowMs).toISOString() });
  return { data: p, nextCursor: p.nextCursor };
}
async function readAlerts({ params, ctx }) {
  const { admin: D, accountId, nowMs } = ctx;
  const status = params.status || "active";
  const list = [];
  if (status !== "resolved") rows(await D.col(D.COL.alerts).where("active", "==", true).get()).forEach((a) => list.push(a));
  if (status !== "active") rows(await D.col(D.COL.alerts).where("active", "==", false).limit(200).get()).forEach((a) => list.push(a));
  const mine = list.filter((a) => !a.accountId || !accountId || a.accountId === accountId);
  const rank = { critical: 0, warning: 1, info: 2 };
  const views = mine.sort((a, b) => Number(b.active) - Number(a.active) || (rank[a.severity] ?? 3) - (rank[b.severity] ?? 3) || Number(b.raisedAtMs || 0) - Number(a.raisedAtMs || 0)).map(alertView);
  const p = page(views, { cursor: params.cursor, pageSize: params.pageSize, keyOf: (v) => v.resourceId, snapshotId: `alerts:${status}:${views.length}`, filterHash: filterHashOf(params, ["status"]), asOf: new Date(nowMs).toISOString(), buckets: { bySeverity: count(views.filter((v) => v.active), "severity"), active: views.filter((v) => v.active).length, resolved: views.filter((v) => !v.active).length } });
  return { data: p, nextCursor: p.nextCursor };
}
async function readAccount({ ctx }) {
  const { admin: D, control: ctrl, accountId, nowMs } = ctx;
  const PF = require("./_investorPortfolio");
  const snap = await PF.snapshot({ accountId, asOfMs: nowMs, admin: D });
  const acct = await D.col(D.COL.accounts).doc(accountId).get();
  const a = acct.exists ? acct.data() : {};
  return { data: { accountId, exists: acct.exists, accountMode: accountModeOf(ctrl), nav: money(snap.navMinor), settledCash: money(snap.settledCashMinor), reserved: money(snap.reservedMinor), invested: money(snap.investedMinor), startingNav: snap.startingNavMinor ? money(snap.startingNavMinor) : null, openedAt: isoOf(a.openedAt || a.createdAt || a.openedAtMs), versions: snap.versions, contentHash: snap.contentHash, openPositions: snap.aggregates.openPositions, resourceId: `account:${accountId}`, resourceVersion: String(Number(snap.versions.portfolioVersion) || 0), availableActions: [S.capability("previewPaperAccountReset", { enabled: acct.exists, disabledReason: "no account" }), S.capability("createPaperAccount", { enabled: !acct.exists, disabledReason: "Manager v1 has exactly one active paper account" })] } };
}

/* ── retained operational reads (quotes, intraday, history, navSeries, ledger) ── */
function v1() { const V = lazy("./investorApi"); if (!V || !V.ACTIONS) throw typed("DEPENDENCY_DEGRADED", "v1 operational reads unavailable"); return V.ACTIONS; }
async function readQuotes({ params, ctx }) {
  const out = await v1().quotes({ symbols: params.symbols });
  const asOf = new Date(ctx.nowMs).toISOString();
  const items = (params.symbols || []).map((s) => { const q = out.quotes && out.quotes[s] || null; return { symbol: s, price: q ? priceUsd(q.lastPrice != null ? q.lastPrice : q.price) : null, provider: q ? q.provider || out.provider || null : out.provider || null, feed: q ? q.feed || out.feed || null : out.feed || null, delayMinutes: q ? (q.priceDelayMinutes != null ? q.priceDelayMinutes : q.feedDelayMinutes != null ? q.feedDelayMinutes : null) : null, asOf: q ? isoOf(q.lastBarAt || q.asOf) : null, sessionDate: q ? q.quoteSessionDate || null : null, currentTradingDay: q ? q.currentTradingDay === true : false, displayOnly: true, note: "display quote; never the fill price" }; });
  return { data: { collectionState: out.ok === false ? "FAILED" : items.length ? "READY" : "EMPTY", asOf, items, completedCount: items.length, totalCount: items.length, nextCursor: null, error: out.ok === false ? S.errorShape("DEPENDENCY_DEGRADED", out.error || "quotes unavailable") : null, buckets: null, partialReason: null, snapshotId: "quotes" }, partial: out.ok === false, partialReason: out.ok === false ? out.error || "quotes unavailable" : null };
}
async function readHistory({ params, ctx }) {
  const H = require("./_investorHistory");
  const r = await H.readDailyWithMetaFor(ctx.admin, params.symbol);
  const series = (r.series || []).slice(-(params.days || 400));
  return { data: chartSeries({ seriesId: `daily:${params.symbol}`, asOf: new Date(ctx.nowMs).toISOString(), series, provenance: r.provenance, resolution: "1d" }) };
}
async function readIntraday({ params, ctx }) {
  const out = await v1().intraday({ symbol: params.symbol, sessions: 1 });
  if (out.error) return { data: { seriesId: `intraday:${params.symbol}`, version: "1", asOf: new Date(ctx.nowMs).toISOString(), currency: "USD", calendarId: POLICY.CALENDAR_ID || "XNYS", adjustment: null, provenance: null, resolution: "5m", points: [], gaps: [], scorable: false, markers: [] }, partial: true, partialReason: out.error };
  const session = (out.sessions || []).find((s) => !params.date || s.date === params.date) || (out.sessions || [])[0] || { bars: [] };
  const points = (session.bars || []).map((b) => ({ t: isoOf(b.t) || null, o: priceUsd(b.o) ? priceUsd(b.o).priceMicros : null, h: priceUsd(b.h) ? priceUsd(b.h).priceMicros : null, l: priceUsd(b.l) ? priceUsd(b.l).priceMicros : null, c: priceUsd(b.c) ? priceUsd(b.c).priceMicros : null, v: String(Math.max(0, Math.round(Number(b.v) || 0))) })).filter((p) => p.t && p.c);
  return { data: { seriesId: `intraday:${params.symbol}:${session.date || ""}`, version: "1", asOf: new Date(ctx.nowMs).toISOString(), currency: "USD", calendarId: POLICY.CALENDAR_ID || "XNYS", adjustment: null, provenance: { provider: session.provider || out.provider || null, feed: session.feed || out.feed || null, delayMinutes: out.delayMinutes == null ? null : out.delayMinutes }, resolution: "5m", points, gaps: [], scorable: points.length > 0, markers: [], sessionDate: session.date || null } };
}
async function readNavSeries({ params, ctx }) {
  const NAV = require("./_investorNav");
  const key = { "1M": "1m", "3M": "3m", "6M": "6m", "1Y": "1y", ALL: "all" }[params.range || "3M"];
  const rangeKey = Object.prototype.hasOwnProperty.call(NAV.RANGES, key) ? key : Object.keys(NAV.RANGES)[0];
  const out = await NAV.series(ctx.accountId, rangeKey, { nowMs: ctx.nowMs });
  const points = (out.points || []).map((p) => ({ t: iso(p.t), navMinor: String(Math.round(Number(p.nav || 0) * 100)), cashMinor: String(Math.round(Number(p.cash || 0) * 100)), investedMinor: String(Math.round(Number(p.inv || 0) * 100)), source: p.s || null })).filter((p) => p.t);
  return { data: { seriesId: `nav:${ctx.accountId}:${rangeKey}`, version: "1", asOf: new Date(ctx.nowMs).toISOString(), currency: "USD", calendarId: POLICY.CALENDAR_ID || "XNYS", adjustment: null, provenance: { source: "navMarks", thinned: out.thinned === true }, resolution: "intraday_marks", points: points.slice(-2000), gaps: [], scorable: points.length > 1, markers: [] } };
}
async function readLedger({ params, ctx }) {
  const { admin: D, accountId, nowMs } = ctx;
  const snap = await D.col(D.COL.ledger).where("accountId", "==", accountId).limit(600).get();
  const list = rows(snap).map((t) => ({ txnId: t.txnId || null, at: isoOf(t.postedAt || t.atMs || t.createdAtMs), kind: t.kind || null, memo: clip(t.memo || (t.meta && t.meta.memo), 200), legs: (t.legs || []).map((l) => ({ account: l.account, amount: moneyUsd(Number(l.amountCents || 0) / 100), memo: clip(l.memo, 120) })), ids: { fillId: t.meta && t.meta.fillId || t.fillId || null, orderSetId: t.meta && t.meta.orderSetId || null, mandateVersionId: t.meta && t.meta.mandateVersionId || null, legId: t.meta && t.meta.legId || null, symbol: t.meta && t.meta.symbol || t.symbol || null } }))
    .sort((a, b) => String(b.at || "").localeCompare(String(a.at || "")) || String(b.txnId).localeCompare(String(a.txnId)));
  const p = page(list, { cursor: params.cursor, pageSize: params.pageSize, keyOf: (v) => v.txnId, snapshotId: `ledger:${accountId}:${list.length}:${list[0] ? list[0].txnId : ""}`, filterHash: "none", asOf: new Date(nowMs).toISOString(), buckets: { byKind: count(list, "kind") } });
  return { data: p, nextCursor: p.nextCursor };
}

/* ═══ MUTATIONS ═══════════════════════════════════════════════════════════ */

function jobsFor(D) { const J = require("./_investorJobs"); return D === A ? J : J.withAdmin(D); }
function mutationDocId({ actorId, accountId, action, idempotencyKey }) { return `m_${sha(`${actorId}|${accountId}|${action}|${idempotencyKey}`).slice(0, 40)}`; }
/** Claim actorId+accountId+action+idempotencyKey with the canonical request hash BEFORE any side effect. */
async function claimMutation(D, { actorId, accountId, action, idempotencyKey, requestHash, nowMs, correlationId }) {
  const id = mutationDocId({ actorId, accountId, action, idempotencyKey });
  const ref = D.col(D.COL.mutations).doc(id);
  return D.runTransaction(async (tx) => {
    const s = await tx.get(ref);
    if (s.exists) {
      const m = s.data();
      if (m.requestHash !== requestHash) return { claimed: false, reused: true, mutationId: id };
      if (m.status === "IN_PROGRESS" && nowMs - Number(m.createdAtMs) < 15 * 60000) return { claimed: false, inProgress: true, mutationId: id };
      if (m.status === "COMPLETE" || m.status === "FAILED") return { claimed: false, replay: m, mutationId: id };
    }
    tx.set(ref, { mutationId: id, actorId, accountId, action, idempotencyKey, requestHash, status: "IN_PROGRESS", createdAtMs: nowMs, correlationId, expiresAtMs: nowMs + MUTATION_RESULT_TTL_MS, auditLinkRetained: true }, { merge: true });
    return { claimed: true, mutationId: id };
  });
}
async function finishMutation(D, mutationId, { status, result = null, error = null, nowMs = Date.now() }) {
  await D.col(D.COL.mutations).doc(mutationId).set({ status, result: result ? JSON.parse(JSON.stringify(result)) : null, error: error || null, finishedAtMs: nowMs }, { merge: true });
}
async function writeAudit(D, { action, actorId, accountId, mutationId, reason, before = null, after = null, correlationId, nowMs, extra = {} }) {
  const id = `audit_${sha(`${mutationId}|${action}|${nowMs}`).slice(0, 32)}`;
  await D.col(D.COL.audit).doc(id).set({ auditId: id, kind: "v2_mutation", action, actorId, accountId, mutationId, reason: reason || null, before, after, correlationId, atMs: nowMs, ...extra, ...D.envelope({ created_by: "apiV2" }) });
  return id;
}
/** A versioned control transition: optimistic version, requested/applied pair, audit. */
async function transitionControl(D, { expectedVersion = null, patch, requested = null, action, actorId, reason, nowMs, correlationId, mutationId }) {
  const ref = D.col(D.COL.control).doc(CONTROL_DOC);
  const out = await D.runTransaction(async (tx) => {
    const s = await tx.get(ref);
    const cur = s.exists ? s.data() : {};
    const version = Number(cur.controlVersion) || 0;
    if (expectedVersion != null && String(version) !== String(expectedVersion)) throw typed("VERSION_CONFLICT", `control version is ${version}, expected ${expectedVersion}`, { resourceVersion: String(version) });
    const next = { ...patch, ...(requested ? Object.fromEntries(Object.entries(requested).map(([k, v]) => [k, { requested: v.requested, applied: v.applied, requestedAtMs: nowMs, appliedAtMs: v.applied === v.requested ? nowMs : null, reason: reason || null }])) : {}),
      controlVersion: version + 1, lastTransition: { action, actorId, atMs: nowMs, reason: reason || null, correlationId, mutationId, fromVersion: version, toVersion: version + 1 } };
    tx.set(ref, next, { merge: true });
    return { before: cur, version: version + 1, next };
  });
  forgetMemo("");
  await writeAudit(D, { action, actorId, accountId: out.before.accountId || null, mutationId, reason, before: { controlVersion: out.version - 1, ...Object.fromEntries(Object.keys(patch).map((k) => [k, out.before[k] === undefined ? null : out.before[k]])) }, after: patch, correlationId, nowMs });
  return out;
}
async function issuePreview(D, { kind, accountId, payload, nowMs }) {
  const token = crypto.randomBytes(24).toString("base64url");
  const payloadHash = sha(payload);
  await D.col(D.COL.mutations).doc(`preview_${sha(token).slice(0, 40)}`).set({ kind: "preview", previewKind: kind, accountId, payloadHash, payload, status: "UNUSED", createdAtMs: nowMs, expiresAtMs: nowMs + PREVIEW_TTL_MS });
  return { previewToken: token, payloadHash, expiresAt: iso(nowMs + PREVIEW_TTL_MS) };
}
/** Consume a one-use preview inside a transaction: UNUSED → CONSUMED, or fail. `verify(stored)` may throw a typed error. */
async function consumePreview(D, { token, kind, accountId, nowMs, verify = null, andThen = null }) {
  const ref = D.col(D.COL.mutations).doc(`preview_${sha(String(token || "")).slice(0, 40)}`);
  return D.runTransaction(async (tx) => {
    const s = await tx.get(ref);
    if (!s.exists) throw typed("PREVIEW_TOKEN_INVALID", "unknown preview token");
    const p = s.data();
    if (p.previewKind !== kind || p.accountId !== accountId) throw typed("PREVIEW_TOKEN_INVALID", "preview token belongs to another operation");
    if (p.status !== "UNUSED") throw typed("PREVIEW_TOKEN_INVALID", "preview token already consumed");
    if (Number(p.expiresAtMs) < nowMs) throw typed("PREVIEW_TOKEN_INVALID", "preview token expired");
    if (verify) await verify(p, tx);
    tx.set(ref, { status: "CONSUMED", consumedAtMs: nowMs }, { merge: true });
    const extra = andThen ? await andThen(p, tx) : null;
    return { preview: p, extra };
  });
}
function requireEnabled(ctx, action) {
  if (INVESTMENT_MUTATIONS.has(action) && !mutationsEnabled(ctx.control)) throw typed("MUTATIONS_DISABLED", `${action} is disabled while the account is in ${accountModeOf(ctx.control)} mode`);
}
function tradingDateOf(nowMs) { const MK = lazy("./_investorMarket"); try { return MK.sessionState(new Date(nowMs)).date; } catch { return new Date(nowMs).toISOString().slice(0, 10); } }
async function pointerOrThrow(D, accountId, symbol, mandateSeriesId) {
  const MD = require("./_investorMandate");
  const p = await MD.readPointer(accountId, symbol, { admin: D });
  if (!p) throw typed("NOT_FOUND", `no mandate pointer for ${symbol}`);
  if (mandateSeriesId && p.mandateSeriesId && p.mandateSeriesId !== mandateSeriesId) throw typed("NOT_FOUND", "mandate series id does not match the live pointer");
  return p;
}
function checkVersion(expected, actual) { if (expected != null && String(expected) !== String(actual)) throw typed("VERSION_CONFLICT", `resource version is ${actual}, expected ${expected}`, { resourceVersion: String(actual) }); }
async function pointerEvent(D, p, kind, fields) { const MD = require("./_investorMandate"); try { await MD.appendEvent(D, p, kind, fields); } catch {} }

const MUTATIONS = {
  /* ── manager and global controls ─────────────────────────────────────── */
  async pauseManager(params, ctx, env) {
    const out = await transitionControl(ctx.admin, { expectedVersion: env.expectedResourceVersion, patch: { managerState: "PAUSED", managerPauseReason: env.auditReason || null, managerPausedAtMs: ctx.nowMs }, requested: { managerRequested: { requested: "PAUSED", applied: "PAUSED" } }, action: "pauseManager", actorId: ctx.actorId, reason: env.auditReason, nowMs: ctx.nowMs, correlationId: ctx.correlationId, mutationId: ctx.mutationId });
    return { data: { managerState: "PAUSED", note: "future Manager Meetings are skipped; the executor and every acknowledged protection keep working" }, resourceVersion: String(out.version), requestedState: { managerState: "PAUSED" }, appliedState: { managerState: "PAUSED" } };
  },
  async resumeManager(params, ctx, env) {
    requireEnabled(ctx, "resumeManager");
    const out = await transitionControl(ctx.admin, { expectedVersion: env.expectedResourceVersion, patch: { managerState: "ENABLED", managerPauseReason: null }, requested: { managerRequested: { requested: "ENABLED", applied: "ENABLED" } }, action: "resumeManager", actorId: ctx.actorId, reason: env.auditReason, nowMs: ctx.nowMs, correlationId: ctx.correlationId, mutationId: ctx.mutationId });
    return { data: { managerState: "ENABLED" }, resourceVersion: String(out.version), requestedState: { managerState: "ENABLED" }, appliedState: { managerState: "ENABLED" } };
  },
  async runManagerReview(params, ctx) {
    requireEnabled(ctx, "runManagerReview");
    const ctrl = ctx.control;
    if (!attestationOk(ctrl)) throw typed("ATTESTATION_FAILED", "the deployed build's fixtures are not attested; no Manager Meeting can be enqueued");
    if (managerStateOf(ctrl) === "PAUSED") throw typed("STATE_CONFLICT", "manager is paused; resume it first");
    const tradingDate = tradingDateOf(ctx.nowMs);
    const J = jobsFor(ctx.admin);
    const slot = Math.floor(ctx.nowMs / (30 * 60000));
    const runId = `run_premarket_manager_${ctx.accountId}_${tradingDate}_op${slot}`;
    const r = await J.enqueueOnce({ task: "premarket_manager", dedupeId: `${ctx.accountId}_${tradingDate}_operator_${slot}`, accountId: ctx.accountId, priority: 150, runId, sessionDate: tradingDate, createdBy: `apiV2:${ctx.actorId}`,
      payload: { accountId: ctx.accountId, tradingDate, reason: "OPERATOR", acceptedAtMs: ctx.nowMs, effectiveAsOfMs: ctx.nowMs, evidenceCutoffMs: ctx.nowMs, requestedBy: ctx.actorId, correlationId: ctx.correlationId } });
    await writeAudit(ctx.admin, { action: "runManagerReview", actorId: ctx.actorId, accountId: ctx.accountId, mutationId: ctx.mutationId, reason: "OPERATOR", after: { jobId: r.jobId, duplicate: r.duplicate === true, tradingDate }, correlationId: ctx.correlationId, nowMs: ctx.nowMs });
    return { data: { jobId: r.jobId, runId, tradingDate, acceptedAt: iso(ctx.nowMs), effectiveAsOf: iso(ctx.nowMs), evidenceCutoff: iso(ctx.nowMs), duplicate: r.duplicate === true, status: r.status }, jobId: r.jobId, requestedState: { run: "QUEUED" }, appliedState: { run: r.duplicate ? String(r.status).toUpperCase() : "QUEUED" } };
  },
  async requestResearch(params, ctx) {
    requireEnabled(ctx, "requestResearch");
    const tradingDate = tradingDateOf(ctx.nowMs);
    const J = jobsFor(ctx.admin);
    const r = await J.enqueueOnce({ task: "focused_research", dedupeId: `${params.symbol}_${tradingDate}_operator`, accountId: ctx.accountId, priority: 300, sessionDate: tradingDate, createdBy: `apiV2:${ctx.actorId}`,
      payload: { accountId: ctx.accountId, symbol: params.symbol, directive: params.directive || "RESEARCH_NOW", reason: "OPERATOR", cutoff: ctx.nowMs, requestedBy: ctx.actorId, correlationId: ctx.correlationId } });
    await writeAudit(ctx.admin, { action: "requestResearch", actorId: ctx.actorId, accountId: ctx.accountId, mutationId: ctx.mutationId, after: { jobId: r.jobId, symbol: params.symbol, duplicate: r.duplicate === true }, correlationId: ctx.correlationId, nowMs: ctx.nowMs });
    return { data: { jobId: r.jobId, symbol: params.symbol, duplicate: r.duplicate === true, note: "operator research persists a memo only; activation needs the normal synthesis and validation path" }, jobId: r.jobId };
  },
  async runFocusedRevision(params, ctx) {
    requireEnabled(ctx, "runFocusedRevision");
    const D = ctx.admin;
    const ds = await D.col(D.COL.evidenceDeltas).doc(params.deltaId).get();
    if (!ds.exists || ds.data().symbol !== params.symbol) throw typed("NOT_FOUND", "unknown evidence delta for that symbol");
    const J = jobsFor(D);
    const r = await J.enqueueOnce({ task: "event_revision", dedupeId: `${params.symbol}_${params.deltaId}`, accountId: ctx.accountId, priority: 120, createdBy: `apiV2:${ctx.actorId}`, payload: { accountId: ctx.accountId, symbol: params.symbol, deltaId: params.deltaId, eventId: params.deltaId, reason: "OPERATOR", requestedBy: ctx.actorId, correlationId: ctx.correlationId } });
    await D.col(D.COL.evidenceDeltas).doc(params.deltaId).set({ reviewJobId: r.jobId, reviewRequestedAtMs: ctx.nowMs, reviewRequestedBy: ctx.actorId }, { merge: true });
    return { data: { jobId: r.jobId, deltaId: params.deltaId, duplicate: r.duplicate === true }, jobId: r.jobId };
  },
  async freezeBuys(params, ctx, env) {
    const out = await transitionControl(ctx.admin, { expectedVersion: env.expectedResourceVersion, patch: { buyState: "FROZEN", freezeNewBuys: true, freezeReason: env.auditReason || "operator", freezeAtMs: ctx.nowMs }, requested: { buyRequested: { requested: "FROZEN", applied: "FROZEN" } }, action: "freezeBuys", actorId: ctx.actorId, reason: env.auditReason, nowMs: ctx.nowMs, correlationId: ctx.correlationId, mutationId: ctx.mutationId });
    return { data: { buyState: "FROZEN" }, resourceVersion: String(out.version), requestedState: { buyState: "FROZEN" }, appliedState: { buyState: "FROZEN" } };
  },
  async resumeBuys(params, ctx, env) {
    requireEnabled(ctx, "resumeBuys");
    const ctrl = ctx.control;
    if (emergencyStateOf(ctrl) !== "CLEAR") throw typed("STATE_CONFLICT", "emergency state must be CLEAR before buys resume");
    if (executorStateOf(ctrl) === "PAUSED_SAFETY") throw typed("STATE_CONFLICT", "reconciliation unresolved; run reconcile first");
    const tick = ctrl.lastExecutionTick || null;
    if (tick && tick.reasons && tick.reasons.includes("STALE_BROKER_TRUTH")) throw typed("STATE_CONFLICT", "broker truth is stale; reconcile first");
    const out = await transitionControl(ctx.admin, { expectedVersion: env.expectedResourceVersion, patch: { buyState: "OPEN", freezeNewBuys: false, freezeReason: null }, requested: { buyRequested: { requested: "OPEN", applied: "OPEN" } }, action: "resumeBuys", actorId: ctx.actorId, reason: env.auditReason, nowMs: ctx.nowMs, correlationId: ctx.correlationId, mutationId: ctx.mutationId });
    return { data: { buyState: "OPEN" }, resourceVersion: String(out.version), requestedState: { buyState: "OPEN" }, appliedState: { buyState: "OPEN" } };
  },
  async emergencyStop(params, ctx, env) {
    /* freeze expansion, cancel unfilled entries, stop non-protective transitions, reconcile; acknowledged protective exits keep working; NOT liquidate-all */
    const D = ctx.admin;
    const pointers = await pointersFor(D, ctx.accountId);
    const cancelled = [];
    for (const p of pointers) {
      if (TERMINAL_POINTER.has(p.status) || !p.desiredVersionId) continue;
      if (p.entryState === "FILLED" || p.entryState === "CANCELLED" || p.status === "PROTECTED_RTH") continue;
      const transitionId = `${p.desiredVersionId}:EMERGENCY_STOP:${ctx.mutationId}`;
      await D.col(D.COL.executionOutbox).doc(transitionId).set({ transitionId, accountId: ctx.accountId, symbol: p.symbol, mandateVersionId: p.desiredVersionId, orderSetId: `os_${p.desiredVersionId}`, kind: "CANCEL_UNFILLED_ENTRY", reason: "emergency_stop", status: "PENDING", idempotencyKey: transitionId, attempts: 0, createdAtMs: ctx.nowMs, authority: "OPERATOR", releaseReservationOnTerminal: true });
      await D.col(D.COL.activeMandates).doc(p.mandateSeriesId || `${ctx.accountId}_${p.symbol}`).set({ status: "CANCEL_PENDING", pausedReason: "emergency_stop", updatedAtMs: ctx.nowMs }, { merge: true });
      await pointerEvent(D, p, "EMERGENCY_STOP_CANCEL_REQUESTED", { mutationId: ctx.mutationId });
      cancelled.push(p.symbol);
    }
    const out = await transitionControl(D, { expectedVersion: null, patch: { emergencyState: "ENGAGED", emergencyEngagedAtMs: ctx.nowMs, emergencyReason: env.auditReason || "operator emergency stop", buyState: "FROZEN", freezeNewBuys: true, freezeReason: "emergency_stop", managerState: "PAUSED", managerPauseReason: "emergency_stop", executorNonProtectiveTransitions: "STOPPED" },
      requested: { emergencyRequested: { requested: "ENGAGED", applied: "ENGAGED" }, buyRequested: { requested: "FROZEN", applied: "FROZEN" }, managerRequested: { requested: "PAUSED", applied: "PAUSED" } }, action: "emergencyStop", actorId: ctx.actorId, reason: env.auditReason, nowMs: ctx.nowMs, correlationId: ctx.correlationId, mutationId: ctx.mutationId });
    const J = jobsFor(D);
    let job = null;
    try { job = await J.enqueueOnce({ task: "execute", dedupeId: `reconcile_${ctx.accountId}_${ctx.mutationId}`, accountId: ctx.accountId, priority: 10, createdBy: `apiV2:${ctx.actorId}`, payload: { accountId: ctx.accountId, reason: "emergency_stop_reconcile", correlationId: ctx.correlationId } }); } catch { job = null; }
    return { data: { emergencyState: "ENGAGED", buyState: "FROZEN", managerState: "PAUSED", entriesCancelled: cancelled, protectionRetained: true, note: "not a liquidation: acknowledged protective exits keep working; any liquidate-all is a separate, strongly confirmed operation" }, resourceVersion: String(out.version), jobId: job ? job.jobId : null, requestedState: { emergencyState: "ENGAGED" }, appliedState: { emergencyState: "ENGAGED" } };
  },
  async resumeSystem(params, ctx, env) {
    requireEnabled(ctx, "resumeSystem");
    const D = ctx.admin;
    const ctrl = ctx.control;
    if (emergencyStateOf(ctrl) === "CLEAR") throw typed("STATE_CONFLICT", "emergency state is already CLEAR");
    const tick = ctrl.lastExecutionTick || null;
    const unresolved = executorStateOf(ctrl) === "PAUSED_SAFETY" || (tick && tick.conservation && tick.conservation.pass === false) || (tick && tick.reasons && tick.reasons.includes("STALE_BROKER_TRUTH"));
    const J = jobsFor(D);
    let job = null;
    try { job = await J.enqueueOnce({ task: "execute", dedupeId: `reconcile_${ctx.accountId}_${ctx.mutationId}`, accountId: ctx.accountId, priority: 10, createdBy: `apiV2:${ctx.actorId}`, payload: { accountId: ctx.accountId, reason: "resume_system_reconcile", correlationId: ctx.correlationId } }); } catch { job = null; }
    const target = unresolved ? "RECOVERING" : "CLEAR";
    const out = await transitionControl(D, { expectedVersion: env.expectedResourceVersion, patch: { emergencyState: target, emergencyRecoveryAtMs: ctx.nowMs, executorNonProtectiveTransitions: "ALLOWED", ...(target === "CLEAR" ? { emergencyReason: null } : {}) }, requested: { emergencyRequested: { requested: "CLEAR", applied: target } }, action: "resumeSystem", actorId: ctx.actorId, reason: env.auditReason, nowMs: ctx.nowMs, correlationId: ctx.correlationId, mutationId: ctx.mutationId });
    return { data: { emergencyState: target, buyState: "FROZEN", managerState: managerStateOf(ctrl), note: "buys stay frozen and the manager stays paused until resumed explicitly; an evidence-paused or expired entry is never revived" }, resourceVersion: String(out.version), jobId: job ? job.jobId : null, requestedState: { emergencyState: "CLEAR" }, appliedState: { emergencyState: target } };
  },
  async activateAccountMode(params, ctx, env) {
    const D = ctx.admin, ctrl = ctx.control;
    if (accountModeOf(ctrl) !== "OBSERVE") throw typed("STATE_CONFLICT", `account mode is ${accountModeOf(ctrl)}`);
    const preflight = [];
    if (!attestationOk(ctrl)) preflight.push("fixtures_not_attested_for_this_build");
    if (params.policyHash !== ctx.policy.policyHash) preflight.push("policy_hash_mismatch");
    const snapshot = await rosterSnapshot(D, ctrl, { accountId: ctx.accountId, nowMs: ctx.nowMs });
    if (params.universeHash !== snapshot.universeHash) preflight.push("universe_hash_mismatch");
    const acct = await D.col(D.COL.accounts).doc(ctx.accountId).get();
    if (!acct.exists) preflight.push("paper_account_missing");
    const X = lazy("./_investorExecution");
    if (X && acct.exists) { try { const c = await X.assertConservation(ctx.accountId, { admin: D }); if (!c.pass) preflight.push("ledger_conservation_failed"); } catch (e) { preflight.push(`conservation_check_failed:${e.code || e.message}`); } }
    if (preflight.length) throw typed("PREFLIGHT_FAILED", `activation preflight failed: ${preflight.join(", ")}`, { preflight });
    const epoch = (Number(ctrl.writerEpoch) || 0) + 1;
    const out = await transitionControl(D, { expectedVersion: env.expectedResourceVersion, patch: { accountMode: "PAPER_AI", engineMode: "manager", writerEpoch: epoch, activationBinding: { policyHash: params.policyHash, universeHash: params.universeHash, universeVersion: snapshot.universeVersion, commit: commitId(), activatedBy: ctx.actorId, activatedAtMs: ctx.nowMs }, buyState: buyStateOf(ctrl), executorEnabled: true },
      requested: { accountModeRequested: { requested: "PAPER_AI", applied: "PAPER_AI" } }, action: "activateAccountMode", actorId: ctx.actorId, reason: env.auditReason, nowMs: ctx.nowMs, correlationId: ctx.correlationId, mutationId: ctx.mutationId });
    return { data: { accountMode: "PAPER_AI", writerEpoch: epoch, engineMode: "manager", binding: { policyHash: params.policyHash, universeHash: params.universeHash }, note: "v1 investment mutations now answer 410 ACTION_RETIRED; exactly one engine holds mutation authority" }, resourceVersion: String(out.version), requestedState: { accountMode: "PAPER_AI" }, appliedState: { accountMode: "PAPER_AI" } };
  },
  async deactivateAccountMode(params, ctx, env) {
    const D = ctx.admin, ctrl = ctx.control;
    if (accountModeOf(ctrl) !== "PAPER_AI") throw typed("STATE_CONFLICT", `account mode is ${accountModeOf(ctrl)}`);
    const pointers = await pointersFor(D, ctx.accountId);
    const cancelled = [];
    for (const p of pointers) {
      if (TERMINAL_POINTER.has(p.status) || !p.desiredVersionId || p.entryState === "FILLED" || p.entryState === "CANCELLED" || p.status === "PROTECTED_RTH") continue;
      const transitionId = `${p.desiredVersionId}:DEACTIVATE:${ctx.mutationId}`;
      await D.col(D.COL.executionOutbox).doc(transitionId).set({ transitionId, accountId: ctx.accountId, symbol: p.symbol, mandateVersionId: p.desiredVersionId, orderSetId: `os_${p.desiredVersionId}`, kind: "CANCEL_UNFILLED_ENTRY", reason: "account_mode_deactivated", status: "PENDING", idempotencyKey: transitionId, attempts: 0, createdAtMs: ctx.nowMs, authority: "OPERATOR", releaseReservationOnTerminal: true });
      await D.col(D.COL.activeMandates).doc(p.mandateSeriesId || `${ctx.accountId}_${p.symbol}`).set({ status: "CANCEL_PENDING", pausedReason: "account_mode_deactivated", updatedAtMs: ctx.nowMs }, { merge: true });
      cancelled.push(p.symbol);
    }
    const out = await transitionControl(D, { expectedVersion: env.expectedResourceVersion, patch: { accountMode: "OBSERVE", buyState: "FROZEN", freezeNewBuys: true, freezeReason: "account_mode_deactivated", managerState: "PAUSED", managerPauseReason: "account_mode_deactivated", deactivation: { by: ctx.actorId, atMs: ctx.nowMs, reason: env.auditReason || null, entriesCancelled: cancelled } },
      requested: { accountModeRequested: { requested: "OBSERVE", applied: "OBSERVE" }, buyRequested: { requested: "FROZEN", applied: "FROZEN" } }, action: "deactivateAccountMode", actorId: ctx.actorId, reason: env.auditReason, nowMs: ctx.nowMs, correlationId: ctx.correlationId, mutationId: ctx.mutationId });
    return { data: { accountMode: "OBSERVE", entriesCancelled: cancelled, protectionRetained: true, note: "held protection and executor reconciliation continue; buys never auto-unfreeze" }, resourceVersion: String(out.version), requestedState: { accountMode: "OBSERVE" }, appliedState: { accountMode: "OBSERVE" } };
  },
  /* ── mandates and manual overrides ───────────────────────────────────── */
  async pauseMandate(params, ctx, env) {
    requireEnabled(ctx, "pauseMandate");
    const D = ctx.admin;
    const p = await pointerOrThrow(D, ctx.accountId, params.symbol, params.mandateSeriesId);
    checkVersion(env.expectedResourceVersion, Number(p.desiredVersion) || 0);
    if (TERMINAL_POINTER.has(p.status)) throw typed("STATE_CONFLICT", `mandate is ${p.status}`);
    if (["PAUSED_OPERATIONAL", "PAUSED_EVIDENCE", "CANCEL_PENDING"].includes(p.status)) throw typed("STATE_CONFLICT", `mandate is already ${p.status}`);
    const owned = await D.col(D.COL.positions).doc(`${ctx.accountId}_${params.symbol}`).get();
    const ownedQty = owned.exists && owned.data().open ? big(owned.data().quantityUnits || owned.data().qty || 0) : 0n;
    const hasEntry = p.desiredVersionId && p.entryState !== "FILLED" && p.entryState !== "CANCELLED" && p.status !== "PROTECTED_RTH";
    let transitionId = null;
    if (hasEntry) {
      transitionId = `${p.desiredVersionId}:PAUSE_OPERATOR:${ctx.mutationId}`;
      await D.col(D.COL.executionOutbox).doc(transitionId).set({ transitionId, accountId: ctx.accountId, symbol: params.symbol, mandateVersionId: p.desiredVersionId, orderSetId: `os_${p.desiredVersionId}`, kind: "CANCEL_UNFILLED_ENTRY", reason: `operator_pause_${params.pauseKind.toLowerCase()}`, status: "PENDING", idempotencyKey: transitionId, attempts: 0, createdAtMs: ctx.nowMs, authority: "OPERATOR", releaseReservationOnTerminal: true });
    }
    const patch = { paused: true, pauseKind: params.pauseKind, pausedReason: params.pauseKind === "THESIS" ? "thesis" : "operational", pausedBy: ctx.actorId, pausedAtMs: ctx.nowMs, pausedHash: p.desiredVersionId || null, pauseAuditReason: env.auditReason || null, updatedAtMs: ctx.nowMs };
    if (ownedQty <= 0n) patch.status = "PAUSED_OPERATIONAL";
    await D.col(D.COL.activeMandates).doc(p.mandateSeriesId || `${ctx.accountId}_${params.symbol}`).set(patch, { merge: true });
    await pointerEvent(D, p, params.pauseKind === "THESIS" ? "PAUSED_THESIS" : "PAUSED_OPERATIONAL", { by: ctx.actorId, reason: env.auditReason || null, transitionId, protectionRetained: ownedQty > 0n });
    await writeAudit(D, { action: "pauseMandate", actorId: ctx.actorId, accountId: ctx.accountId, mutationId: ctx.mutationId, reason: env.auditReason, before: { status: p.status }, after: patch, correlationId: ctx.correlationId, nowMs: ctx.nowMs });
    forgetMemo("");
    return { data: { symbol: params.symbol, state: patch.status || p.status, pauseKind: params.pauseKind, entryCancelTransitionId: transitionId, protectionRetained: ownedQty > 0n, resumeRequires: params.pauseKind === "THESIS" ? "a new Sol mandate version" : "same-hash resume with no unreviewed delta" }, resourceVersion: String(Number(p.desiredVersion) || 0), requestedState: { paused: true }, appliedState: { paused: true, status: patch.status || p.status } };
  },
  async resumeMandate(params, ctx, env) {
    requireEnabled(ctx, "resumeMandate");
    const D = ctx.admin, ctrl = ctx.control;
    const p = await pointerOrThrow(D, ctx.accountId, params.symbol, params.mandateSeriesId);
    checkVersion(env.expectedResourceVersion, Number(p.desiredVersion) || 0);
    if (!p.paused) throw typed("STATE_CONFLICT", "mandate is not paused");
    if (p.pauseKind === "THESIS" || p.status === "PAUSED_EVIDENCE") throw typed("TRANSITION_UNSUPPORTED", "a thesis or evidence pause needs a new Sol mandate version; same-hash resume is refused");
    if (params.mandateHash !== sha(String(p.pausedHash || p.desiredVersionId || ""))) throw typed("VERSION_CONFLICT", "mandate hash does not match the paused version", { resourceVersion: String(Number(p.desiredVersion) || 0) });
    const deltas = await pendingDeltas(D, { symbol: params.symbol, limit: 5 });
    if (deltas.length) throw typed("STATE_CONFLICT", "an unreviewed evidence delta blocks same-hash resume");
    if (executorStateOf(ctrl) === "PAUSED_SAFETY" || emergencyStateOf(ctrl) !== "CLEAR") throw typed("STATE_CONFLICT", "reconcile and clear the emergency state before resuming");
    if (p.expiresAtMs && Number(p.expiresAtMs) < ctx.nowMs) throw typed("STATE_CONFLICT", "the authorized sessions have expired; a new Sol version is required");
    const owned = await D.col(D.COL.positions).doc(`${ctx.accountId}_${params.symbol}`).get();
    const ownedQty = owned.exists && owned.data().open ? big(owned.data().quantityUnits || owned.data().qty || 0) : 0n;
    let transitionId = null;
    if (p.desiredVersionId && ownedQty <= 0n) {
      /* re-reserve and re-apply the identical desired order set */
      const osRef = D.col(D.COL.orderSets).doc(`os_${p.desiredVersionId}`);
      const oss = await osRef.get();
      if (!oss.exists) throw typed("NOT_FOUND", "desired order set missing");
      const os = oss.data();
      const rref = os.reservationId ? D.col(D.COL.capitalReservations).doc(os.reservationId) : null;
      await D.runTransaction(async (tx) => {
        if (rref) { const r = await tx.get(rref); const aref = D.col(D.COL.reservationAccounts).doc(ctx.accountId); const a = await tx.get(aref); const res = r.exists ? r.data() : null; const acct = a.exists ? a.data() : {};
          if (res && res.status !== "ACTIVE") { const add = (k, v) => (big(acct[k] || 0) + big(v || 0)).toString(); tx.set(rref, { status: "ACTIVE", reactivatedAtMs: ctx.nowMs, releasedAtMs: null }, { merge: true }); tx.set(aref, { reservedNotionalMinor: add("reservedNotionalMinor", res.reservedNotionalMinor), reservedPlannedLossMinor: add("reservedPlannedLossMinor", res.plannedLossMinor), reservedStressLossMinor: add("reservedStressLossMinor", res.stressLossMinor), version: (Number(acct.version) || 0) + 1, updatedAtMs: ctx.nowMs }, { merge: true }); } }
        tx.set(osRef, { status: "DESIRED", resumedAtMs: ctx.nowMs }, { merge: true });
      });
      for (const l of rows(await D.col(D.COL.orderLegs).where("orderSetId", "==", os.orderSetId).get())) await D.col(D.COL.orderLegs).doc(l.legId).set({ status: "DESIRED", remainingUnits: l.quantityUnits, filledUnits: "0", cancelledAtMs: null, cancelReason: null }, { merge: true });
      transitionId = `${p.desiredVersionId}:RESUME:${ctx.mutationId}`;
      await D.col(D.COL.executionOutbox).doc(transitionId).set({ transitionId, accountId: ctx.accountId, symbol: params.symbol, mandateVersionId: p.desiredVersionId, orderSetId: os.orderSetId, planId: os.planId || null, kind: "APPLY_DESIRED_ORDER_SET", status: "PENDING", idempotencyKey: transitionId, attempts: 0, createdAtMs: ctx.nowMs, claimedBy: null, claimedAtMs: null, authority: "SOL_MANDATE", resumedBy: ctx.actorId });
    }
    const patch = { paused: false, pauseKind: null, pausedReason: null, resumedBy: ctx.actorId, resumedAtMs: ctx.nowMs, status: ownedQty > 0n ? p.status : "DESIRED", entryState: ownedQty > 0n ? p.entryState : "DESIRED", updatedAtMs: ctx.nowMs };
    await D.col(D.COL.activeMandates).doc(p.mandateSeriesId || `${ctx.accountId}_${params.symbol}`).set(patch, { merge: true });
    await pointerEvent(D, p, "RESUMED_SAME_HASH", { by: ctx.actorId, transitionId, mandateHash: params.mandateHash });
    await writeAudit(D, { action: "resumeMandate", actorId: ctx.actorId, accountId: ctx.accountId, mutationId: ctx.mutationId, reason: env.auditReason, before: { status: p.status }, after: patch, correlationId: ctx.correlationId, nowMs: ctx.nowMs });
    forgetMemo("");
    return { data: { symbol: params.symbol, state: patch.status, reapplyTransitionId: transitionId }, resourceVersion: String(Number(p.desiredVersion) || 0), requestedState: { paused: false }, appliedState: { paused: false, status: patch.status } };
  },
  async cancelMandate(params, ctx, env) {
    requireEnabled(ctx, "cancelMandate");
    const D = ctx.admin;
    const p = await pointerOrThrow(D, ctx.accountId, params.symbol, params.mandateSeriesId);
    checkVersion(env.expectedResourceVersion, Number(p.desiredVersion) || 0);
    if (TERMINAL_POINTER.has(p.status) || p.status === "CANCEL_PENDING") throw typed("STATE_CONFLICT", `mandate is ${p.status}`);
    const owned = await D.col(D.COL.positions).doc(`${ctx.accountId}_${params.symbol}`).get();
    const ownedQty = owned.exists && owned.data().open ? big(owned.data().quantityUnits || owned.data().qty || 0) : 0n;
    let transitionId = null;
    if (p.desiredVersionId && p.entryState !== "FILLED" && p.entryState !== "CANCELLED" && p.status !== "PROTECTED_RTH") {
      transitionId = `${p.desiredVersionId}:CANCEL_OPERATOR:${ctx.mutationId}`;
      await D.col(D.COL.executionOutbox).doc(transitionId).set({ transitionId, accountId: ctx.accountId, symbol: params.symbol, mandateVersionId: p.desiredVersionId, orderSetId: `os_${p.desiredVersionId}`, kind: "CANCEL_UNFILLED_ENTRY", reason: "operator_cancel", status: "PENDING", idempotencyKey: transitionId, attempts: 0, createdAtMs: ctx.nowMs, authority: "OPERATOR", releaseReservationOnTerminal: true });
    }
    const patch = ownedQty > 0n
      ? { entryAuthorizationRevoked: true, cancelRequestedBy: ctx.actorId, cancelRequestedAtMs: ctx.nowMs, replacementRequired: true, actionRequiredReason: "operator revoked the mandate; protection retained; a replacement mandate or a sell is required", status: p.status === "PROTECTED_RTH" ? "PROTECTED_RTH" : "ACTION_REQUIRED", updatedAtMs: ctx.nowMs }
      : { status: transitionId ? "CANCEL_PENDING" : "CANCELLED", cancelRequestedBy: ctx.actorId, cancelRequestedAtMs: ctx.nowMs, pausedReason: "operator_cancel", updatedAtMs: ctx.nowMs };
    await D.col(D.COL.activeMandates).doc(p.mandateSeriesId || `${ctx.accountId}_${params.symbol}`).set(patch, { merge: true });
    await pointerEvent(D, p, "OPERATOR_CANCEL_REQUESTED", { by: ctx.actorId, reason: env.auditReason || null, transitionId, protectionRetained: ownedQty > 0n });
    await writeAudit(D, { action: "cancelMandate", actorId: ctx.actorId, accountId: ctx.accountId, mutationId: ctx.mutationId, reason: env.auditReason, before: { status: p.status }, after: patch, correlationId: ctx.correlationId, nowMs: ctx.nowMs });
    forgetMemo("");
    return { data: { symbol: params.symbol, state: patch.status, cancelTransitionId: transitionId, protectionRetained: ownedQty > 0n, replacementRequired: ownedQty > 0n }, resourceVersion: String(Number(p.desiredVersion) || 0), requestedState: { status: "CANCELLED" }, appliedState: { status: patch.status } };
  },
  async requestSell(params, ctx, env) {
    requireEnabled(ctx, "requestSell");
    const D = ctx.admin, ctrl = ctx.control;
    const PF = require("./_investorPortfolio");
    if (emergencyStateOf(ctrl) === "ENGAGED") throw typed("STATE_CONFLICT", "EMERGENCY_ENGAGED: the emergency policy owns exposure changes right now");
    if (executorStateOf(ctrl) === "PAUSED_SAFETY") throw typed("STATE_CONFLICT", "RECONCILING: the executor is paused for safety");
    const snap = await PF.snapshot({ accountId: ctx.accountId, asOfMs: ctx.nowMs, admin: D });
    checkVersion(params.expectedPositionVersion, snap.versions.portfolioVersion);
    const pos = snap.positions.find((p) => p.symbol === params.symbol);
    if (!pos || params.positionId !== `${ctx.accountId}_${params.symbol}`) throw typed("NOT_FOUND", "NO_POSITION: no open position for that symbol");
    const q = big(params.quantityUnits), owned = big(pos.quantityUnits);
    if (q <= 0n || q > owned) throw typed("SEMANTIC_REJECTED", `quantity must be between 1 and the owned ${owned} whole shares`);
    const pointer = await require("./_investorMandate").readPointer(ctx.accountId, params.symbol, { admin: D });
    if (params.expectedMandateVersion != null && pointer && String(pointer.desiredVersion || 0) !== String(params.expectedMandateVersion)) throw typed("VERSION_CONFLICT", "mandate version changed", { resourceVersion: String(pointer.desiredVersion || 0) });
    const posDoc = await D.col(D.COL.positions).doc(params.positionId).get();
    const existing = posDoc.exists ? posDoc.data().operatorSell : null;
    if (existing && ["SUBMITTED", "PENDING", "PARTIALLY_FILLED"].includes(existing.state)) throw typed("STATE_CONFLICT", "PROTECTION_RESIZE_PENDING: a manual sell is already working; cancel it first");
    if (params.orderType === "LIMIT" && !params.limitPriceMicros) throw typed("SEMANTIC_REJECTED", "a LIMIT sell needs limitPriceMicros");
    const overrideId = `ov_${sha(`${ctx.mutationId}|${params.symbol}`).slice(0, 24)}`;
    const orderSetId = `os_op_${overrideId}`;
    const leg = { legId: `${orderSetId}_SELL`, orderSetId, accountId: ctx.accountId, symbol: params.symbol, role: "SELL", side: "sell", type: params.orderType, priceMicros: params.orderType === "LIMIT" ? canon(params.limitPriceMicros) : null, collarBps: params.orderType === "MARKETABLE_LIMIT" ? canon(params.collarBps || "100") : null,
      quantityUnits: q.toString(), remainingUnits: q.toString(), filledUnits: "0", timeInForce: params.timeInForce, regularSessionOnly: true, status: "DESIRED", createdAtMs: ctx.nowMs, authority: "OPERATOR" };
    const os = { schemaVersion: "order-set.v1", orderSetId, accountId: ctx.accountId, symbol: params.symbol, mandateVersionId: pointer ? pointer.appliedVersionId || pointer.desiredVersionId || null : null, purpose: "OPERATOR_SELL", authority: "OPERATOR", overrideId, positionLifecycleId: pos.positionLifecycleId || null,
      status: "DESIRED", legs: [leg], legIds: [leg.legId], reservationId: null, createdAtMs: ctx.nowMs, requestedBy: ctx.actorId, reason: params.reason, expectedPositionVersion: params.expectedPositionVersion, expectedMandateVersion: params.expectedMandateVersion, exchangeSession: params.exchangeSession, version: 1 };
    await D.col(D.COL.orderSets).doc(orderSetId).set(os);
    await D.col(D.COL.orderLegs).doc(leg.legId).set(leg);
    const transitionId = `${orderSetId}:OPERATOR_SELL`;
    await D.col(D.COL.executionOutbox).doc(transitionId).set({ transitionId, accountId: ctx.accountId, symbol: params.symbol, mandateVersionId: os.mandateVersionId, orderSetId, kind: "OPERATOR_SELL", status: "PENDING", idempotencyKey: transitionId, attempts: 0, createdAtMs: ctx.nowMs, claimedBy: null, claimedAtMs: null, authority: "OPERATOR", overrideId, reason: params.reason });
    await D.col(D.COL.positions).doc(params.positionId).set({ operatorSell: { overrideId, orderSetId, quantityUnits: q.toString(), state: "PENDING", atMs: ctx.nowMs, transitionId }, updatedAtMs: ctx.nowMs }, { merge: true });
    await writeAudit(D, { action: "requestSell", actorId: ctx.actorId, accountId: ctx.accountId, mutationId: ctx.mutationId, reason: params.reason, after: { overrideId, orderSetId, quantityUnits: q.toString(), orderType: params.orderType }, correlationId: ctx.correlationId, nowMs: ctx.nowMs, extra: { thesisUnchanged: true } });
    forgetMemo("");
    return { data: { overrideId, orderSetId, symbol: params.symbol, quantity: qty(q), orderType: params.orderType, requestedState: "PENDING_EXECUTOR", appliedState: "NOT_YET_APPLIED", cancelableUntil: "execution begins (first fill)", protectionResize: "resized to the remaining owned quantity when the executor applies the sale; restored if cancelled", thesisUnchanged: true }, resourceVersion: String(snap.versions.portfolioVersion), requestedState: { sell: "PENDING" }, appliedState: { sell: "NOT_APPLIED" } };
  },
  async cancelSell(params, ctx, env) {
    requireEnabled(ctx, "cancelSell");
    const D = ctx.admin;
    const oss = await D.col(D.COL.orderSets).doc(params.orderSetId).get();
    if (!oss.exists || oss.data().purpose !== "OPERATOR_SELL" || oss.data().overrideId !== params.overrideId) throw typed("NOT_FOUND", "unknown operator sell");
    const os = oss.data();
    checkVersion(env.expectedResourceVersion, Number(os.version) || 1);
    if (["CANCELLED", "FILLED", "COMPLETE", "REJECTED"].includes(os.status)) throw typed("STATE_CONFLICT", `sell is ${os.status}`);
    const legs = rows(await D.col(D.COL.orderLegs).where("orderSetId", "==", os.orderSetId).get());
    if (legs.some((l) => big(l.filledUnits || 0) > 0n)) throw typed("STATE_CONFLICT", "EXECUTION_BEGUN: the sell has started filling and can no longer be cancelled");
    const pending = await D.col(D.COL.executionOutbox).doc(`${os.orderSetId}:OPERATOR_SELL`).get();
    if (pending.exists && pending.data().status === "PENDING") {
      await D.col(D.COL.executionOutbox).doc(`${os.orderSetId}:OPERATOR_SELL`).set({ status: "CANCELLED", cancelledAtMs: ctx.nowMs, cancelledBy: ctx.actorId }, { merge: true });
      await D.col(D.COL.orderSets).doc(os.orderSetId).set({ status: "CANCELLED", cancelledAtMs: ctx.nowMs, version: (Number(os.version) || 1) + 1 }, { merge: true });
      await D.col(D.COL.orderLegs).doc(legs[0] ? legs[0].legId : `${os.orderSetId}_SELL`).set({ status: "CANCELLED", cancelledAtMs: ctx.nowMs }, { merge: true });
      await D.col(D.COL.positions).doc(`${ctx.accountId}_${os.symbol}`).set({ operatorSell: { overrideId: os.overrideId, orderSetId: os.orderSetId, state: "CANCELLED", atMs: ctx.nowMs } }, { merge: true });
      forgetMemo("");
      return { data: { overrideId: os.overrideId, orderSetId: os.orderSetId, state: "CANCELLED", note: "cancelled before the executor applied it; no protection was resized" }, resourceVersion: String((Number(os.version) || 1) + 1), requestedState: { sell: "CANCELLED" }, appliedState: { sell: "CANCELLED" } };
    }
    const transitionId = `${os.orderSetId}:CANCEL_OPERATOR_SELL:${ctx.mutationId}`;
    await D.col(D.COL.executionOutbox).doc(transitionId).set({ transitionId, accountId: ctx.accountId, symbol: os.symbol, mandateVersionId: os.mandateVersionId || null, orderSetId: os.orderSetId, kind: "CANCEL_OPERATOR_SELL", status: "PENDING", idempotencyKey: transitionId, attempts: 0, createdAtMs: ctx.nowMs, claimedBy: null, claimedAtMs: null, authority: "OPERATOR", overrideId: os.overrideId });
    await D.col(D.COL.orderSets).doc(os.orderSetId).set({ status: "CANCEL_PENDING", version: (Number(os.version) || 1) + 1 }, { merge: true });
    forgetMemo("");
    return { data: { overrideId: os.overrideId, orderSetId: os.orderSetId, state: "CANCEL_PENDING", transitionId, note: "the executor cancels the working sell and restores protection to the whole owned quantity" }, resourceVersion: String((Number(os.version) || 1) + 1), requestedState: { sell: "CANCELLED" }, appliedState: { sell: "CANCEL_PENDING" } };
  },
  /* ── administration ─────────────────────────────────────────────────── */
  async setBudget(params, ctx, env) {
    const D = ctx.admin, ctrl = ctx.control;
    const currentMinor = big((ctrl.budget && ctrl.budget.dailyReservationMinor) || ctx.policy.budget.dailyReservationMinor || 0);
    const next = big(params.dailyReservationMinor);
    if (next > currentMinor && !ctx.auth.reauthed) throw typed("REAUTH_REQUIRED", "raising the daily reservation requires reauthentication");
    if (next < 0n || next > 100000000n) throw typed("SEMANTIC_REJECTED", "daily reservation must be between 0 and $1,000,000");
    const budget = { dailyReservationMinor: next.toString(), byRoleMinor: params.byRoleMinor ? { investment: params.byRoleMinor.investment ? canon(params.byRoleMinor.investment) : null, extraction: params.byRoleMinor.extraction ? canon(params.byRoleMinor.extraction) : null } : null, alertThresholdPpm: params.alertThresholdPpm ? canon(params.alertThresholdPpm) : "800000", version: (Number(ctrl.budget && ctrl.budget.version) || 0) + 1, setBy: ctx.actorId, setAtMs: ctx.nowMs };
    const out = await transitionControl(D, { expectedVersion: env.expectedResourceVersion, patch: { budget }, action: "setBudget", actorId: ctx.actorId, reason: env.auditReason, nowMs: ctx.nowMs, correlationId: ctx.correlationId, mutationId: ctx.mutationId });
    return { data: { budget, note: "the gateway reads the versioned daily reservation from Control at each call; per-role ceilings apply within it" }, resourceVersion: String(out.version) };
  },
  async setRiskMandate(params, ctx, env) {
    const D = ctx.admin, ctrl = ctx.control;
    const overrides = {};
    for (const [k, v] of Object.entries(params.overrides || {})) {
      const b = POLICY.RISK_MANDATE_BOUNDS[k];
      if (!b) throw typed("RISK_BOUND_EXCEEDED", `${k} is not an overridable risk field`);
      if (big(v) < big(b.min) || big(v) > big(b.max)) throw typed("RISK_BOUND_EXCEEDED", `${k}=${v} is outside the hard bound [${b.min}, ${b.max}]`);
      overrides[k] = canon(v);
    }
    const applied = POLICY.applyRiskMandateOverrides(overrides);
    if (applied.refused && applied.refused.length) throw typed("RISK_BOUND_EXCEEDED", `refused: ${applied.refused.map((r) => r.key || r).join(", ")}`);
    const version = (Number(ctrl.riskMandateVersion) || 0) + 1;
    const hash = sha(applied.riskMandate);
    /* a newly over-limit book freezes expansion rather than liquidating */
    const PF = require("./_investorPortfolio");
    const snap = await PF.snapshot({ accountId: ctx.accountId, asOfMs: ctx.nowMs, admin: D });
    const nav = big(snap.navMinor);
    const maxName = big(applied.riskMandate.weights.maxSingleNameWeightBps);
    const over = nav > 0n ? snap.positions.filter((p) => big(p.marketValueMinor) * 10000n / nav > maxName).map((p) => p.symbol) : [];
    const patch = { riskMandateOverrides: overrides, riskMandateVersion: version, riskMandateHash: hash, riskMandateNote: env.auditReason || params.note || null, riskMandateSetBy: ctx.actorId, riskMandateSetAtMs: ctx.nowMs };
    if (over.length) Object.assign(patch, { buyState: "FROZEN", freezeNewBuys: true, freezeReason: `risk mandate v${version}: over-limit names ${over.join(",")} queued for Sol/operator resolution` });
    const out = await transitionControl(D, { expectedVersion: env.expectedResourceVersion, patch, requested: over.length ? { buyRequested: { requested: "FROZEN", applied: "FROZEN" } } : null, action: "setRiskMandate", actorId: ctx.actorId, reason: env.auditReason, nowMs: ctx.nowMs, correlationId: ctx.correlationId, mutationId: ctx.mutationId });
    if (over.length) { const id = `risk_mandate_over_limit:v${version}`; try { await D.col(D.COL.alerts).doc(id).set({ conditionId: id, code: "buys_frozen", scope: `v${version}`, severity: "warning", kind: "RISK_MANDATE_OVER_LIMIT", title: "Book over the new risk mandate", action: "Sol or the operator must resolve the over-limit names; expansion is frozen; nothing is liquidated", detail: { over }, accountId: ctx.accountId, active: true, raisedAtMs: ctx.nowMs, lastSeenAtMs: ctx.nowMs, acknowledgedAtMs: null, acknowledgedBy: null, resolvedAtMs: null, occurrences: 1 }, { merge: true }); } catch {} }
    return { data: { version, hash, overrides, applied: applied.applied || [], overLimit: over, buyState: over.length ? "FROZEN" : buyStateOf(ctrl) }, resourceVersion: String(out.version) };
  },
  async setEmergencyRiskPolicy(params, ctx, env) {
    const D = ctx.admin, ctrl = ctx.control;
    const ER = require("./_investorEmergencyRisk");
    const PF = require("./_investorPortfolio");
    const template = POLICY.EMERGENCY_RISK_POLICY_TEMPLATE;
    const candidate = { ...template, ...params.policy, schemaVersion: template.schemaVersion, label: "EMERGENCY_RISK", forbiddenOperations: template.forbiddenOperations };
    const permitted = Array.isArray(candidate.permittedOperations) ? candidate.permittedOperations : [];
    if (permitted.some((op) => !template.permittedOperations.includes(op)) || template.forbiddenOperations.some((op) => permitted.includes(op))) throw typed("SEMANTIC_REJECTED", "the emergency policy may only narrow the template; it can never open long exposure");
    const { policyHash: _h, status: _s, approvedBy: _b, approvedAtMs: _a, ...content } = candidate;
    const policyHash = sha(content);
    const snap = await PF.snapshot({ accountId: ctx.accountId, asOfMs: ctx.nowMs, admin: D });
    let preview = null;
    try { preview = ER.planActions({ fired: (candidate.triggers || []).map((t) => ({ ...t, fired: true })), portfolio: snap, policy: ctx.policy, riskMandate: ctx.policy.riskMandate, ranks: {}, marks: Object.fromEntries(snap.positions.map((p) => [p.symbol, p.markMicros])), advBySymbol: {}, nowMs: ctx.nowMs, policyActive: true }); } catch (e) { preview = { error: String(e.code || e.message).slice(0, 120) }; }
    const impact = { policyHash, triggers: (candidate.triggers || []).length, permittedOperations: permitted, dryRunActions: preview && preview.actions ? preview.actions.slice(0, 20) : [], opensLongExposure: false, note: preview && preview.error ? `dry run could not plan: ${preview.error}` : "actions a full trigger set would produce against the current book" };
    if (params.dryRun === true || !env.previewToken) {
      const pv = await issuePreview(D, { kind: "emergencyRiskPolicy", accountId: ctx.accountId, payload: { policyHash, controlVersion: controlVersionOf(ctrl) }, nowMs: ctx.nowMs });
      return { data: { dryRun: true, impact, previewToken: pv.previewToken, previewExpiresAt: pv.expiresAt }, resourceVersion: controlVersionOf(ctrl) };
    }
    if (!ctx.auth.reauthed) throw typed("REAUTH_REQUIRED", "activating an emergency policy requires reauthentication");
    const stored = { ...candidate, policyHash, status: "APPROVED", approvedBy: ctx.actorId, approvedAtMs: ctx.nowMs, effectiveFrom: iso(ctx.nowMs), version: (Number(ctrl.emergencyRiskPolicyVersion) || 0) + 1 };
    await consumePreview(D, { token: env.previewToken, kind: "emergencyRiskPolicy", accountId: ctx.accountId, nowMs: ctx.nowMs, verify: (p) => { if (p.payload.policyHash !== policyHash) throw typed("PREVIEW_TOKEN_INVALID", "the policy changed after the preview"); } });
    const check = POLICY.activeEmergencyPolicy(stored);
    if (!check.active) throw typed("SEMANTIC_REJECTED", `policy rejected: ${check.reason}`);
    await D.col(D.COL.emergencyRiskPolicies).doc(policyHash).set({ ...stored, ...D.envelope({ created_by: "apiV2.setEmergencyRiskPolicy" }) });
    const out = await transitionControl(D, { expectedVersion: null, patch: { emergencyRiskPolicy: stored, emergencyRiskPolicyVersion: stored.version, activeEmergencyPolicyHash: policyHash }, action: "setEmergencyRiskPolicy", actorId: ctx.actorId, reason: env.auditReason, nowMs: ctx.nowMs, correlationId: ctx.correlationId, mutationId: ctx.mutationId, requested: null });
    return { data: { policyHash, version: stored.version, active: true, impact }, resourceVersion: String(out.version) };
  },
  async setMarketConfig(params, ctx, env) {
    const r = await v1().setMarketConfig({ operator: ctx.actorId, provider: params.provider, feed: params.feed, alpacaKeyId: params.alpacaKeyId, alpacaSecretKey: params.alpacaSecretKey });
    if (r && r.error) throw typed("SEMANTIC_REJECTED", r.error);
    await writeAudit(ctx.admin, { action: "setMarketConfig", actorId: ctx.actorId, accountId: ctx.accountId, mutationId: ctx.mutationId, reason: env.auditReason, after: { provider: params.provider, feed: params.feed || null, credentialsSupplied: !!(params.alpacaKeyId && params.alpacaSecretKey) }, correlationId: ctx.correlationId, nowMs: ctx.nowMs });
    return { data: { provider: params.provider, feed: params.feed || null, secretsStored: !!(params.alpacaKeyId && params.alpacaSecretKey), note: "secrets are write-only and never returned", result: r && r.ok !== undefined ? { ok: r.ok } : null }, resourceVersion: controlVersionOf(ctx.control) };
  },
  async confirmSplit(params, ctx, env) {
    requireEnabled(ctx, "confirmSplit");
    const r = await v1().confirmSplit({ symbol: params.symbol, shareRatio: params.shareRatio, effectiveDate: params.effectiveDate, sourceRef: params.sourceRef, operator: ctx.actorId });
    if (r && r.error) throw typed("SEMANTIC_REJECTED", r.error);
    await writeAudit(ctx.admin, { action: "confirmSplit", actorId: ctx.actorId, accountId: ctx.accountId, mutationId: ctx.mutationId, reason: env.auditReason, after: { symbol: params.symbol, shareRatio: params.shareRatio, effectiveDate: params.effectiveDate, sourceRef: params.sourceRef }, correlationId: ctx.correlationId, nowMs: ctx.nowMs });
    forgetMemo("");
    return { data: { symbol: params.symbol, state: "REBASED", result: r }, resourceVersion: controlVersionOf(ctx.control) };
  },
  async confirmCashDividend(params, ctx, env) {
    requireEnabled(ctx, "confirmCashDividend");
    const r = await v1().confirmCashDividend({ symbol: params.symbol, corporateActionId: params.corporateActionId, positionLifecycleId: params.positionLifecycleId, eligibleQty: params.eligibleQty != null ? Number(params.eligibleQty) : undefined, perShareUsd: Number(BigInt(params.perShareMicros)) / 1e6, recordDate: params.recordDate, payDate: params.payDate, sourceRef: params.sourceRef, operator: ctx.actorId });
    if (r && r.error) throw typed("SEMANTIC_REJECTED", r.error);
    await writeAudit(ctx.admin, { action: "confirmCashDividend", actorId: ctx.actorId, accountId: ctx.accountId, mutationId: ctx.mutationId, reason: env.auditReason, after: { symbol: params.symbol, corporateActionId: params.corporateActionId, perShareMicros: params.perShareMicros }, correlationId: ctx.correlationId, nowMs: ctx.nowMs });
    forgetMemo("");
    return { data: { symbol: params.symbol, state: "REBASED", result: r }, resourceVersion: controlVersionOf(ctx.control) };
  },
  async requestAuditExport(params, ctx) {
    if (params.toDate < params.fromDate) throw typed("SEMANTIC_REJECTED", "toDate precedes fromDate");
    const J = jobsFor(ctx.admin);
    const scope = params.scope || "all";
    const r = await J.enqueueOnce({ task: "audit_export", dedupeId: `${ctx.accountId}_${params.fromDate}_${params.toDate}_${scope}_${ctx.mutationId.slice(-8)}`, accountId: ctx.accountId, priority: 600, createdBy: `apiV2:${ctx.actorId}`, payload: { accountId: ctx.accountId, fromDate: params.fromDate, toDate: params.toDate, scope, requestedBy: ctx.actorId, requestedAtMs: ctx.nowMs, correlationId: ctx.correlationId } });
    return { data: { jobId: r.jobId, state: "QUEUED", note: "bounded point-in-time export; the manifest and a short-lived download link appear on auditExports when complete" }, jobId: r.jobId };
  },
  async createPaperAccount(params, ctx, env) {
    const D = ctx.admin, ctrl = ctx.control;
    const accountId = params.accountId || ctx.accountId;
    const exists = await D.col(D.COL.accounts).doc(accountId).get();
    if (exists.exists) throw typed("STATE_CONFLICT", "Manager v1 has exactly one active paper account; reset it instead", { resourceVersion: String(exists.data().balanceRevision || 0) });
    if (ctrl.accountId && ctrl.accountId !== accountId) { const other = await D.col(D.COL.accounts).doc(ctrl.accountId).get(); if (other.exists) throw typed("STATE_CONFLICT", `account ${ctrl.accountId} is already the active paper account`); }
    const L = require("./_investorLedger");
    const starting = big(params.startingNavMinor);
    if (starting <= 0n || starting > 100000000000n) throw typed("SEMANTIC_REJECTED", "starting NAV must be between $0.01 and $1,000,000,000");
    const r = await L.openAccount(accountId, Number(starting) / 100);
    const out = await transitionControl(D, { expectedVersion: null, patch: { accountId }, action: "createPaperAccount", actorId: ctx.actorId, reason: env.auditReason, nowMs: ctx.nowMs, correlationId: ctx.correlationId, mutationId: ctx.mutationId });
    forgetMemo("");
    return { data: { accountId, startingNav: money(starting), opened: true, result: r && r.ok !== undefined ? { ok: r.ok } : null }, resourceVersion: String(out.version) };
  },
  async previewPaperAccountReset(params, ctx) {
    const D = ctx.admin;
    const PF = require("./_investorPortfolio");
    const X = lazy("./_investorExecution");
    const snap = await PF.snapshot({ accountId: ctx.accountId, asOfMs: ctx.nowMs, admin: D });
    const pointers = await pointersFor(D, ctx.accountId);
    const activeMandates = pointers.filter((p) => !TERMINAL_POINTER.has(p.status)).length;
    const conservation = X ? await X.assertConservation(ctx.accountId, { admin: D }).catch((e) => ({ pass: false, discrepancies: [String(e.code || e.message)] })) : { pass: null, discrepancies: [] };
    const [ledgerN, fillsN, tradesN] = await Promise.all([rows(await D.col(D.COL.ledger).where("accountId", "==", ctx.accountId).get()).length, rows(await D.col(D.COL.fills).where("accountId", "==", ctx.accountId).get()).length, rows(await D.col(D.COL.trades).where("accountId", "==", ctx.accountId).get()).length]);
    const blockers = [];
    if (snap.positions.length) blockers.push(`open_positions:${snap.positions.length}`);
    if (snap.workingOrders.length) blockers.push(`working_orders:${snap.workingOrders.length}`);
    if (activeMandates) blockers.push(`active_mandates:${activeMandates}`);
    if (conservation.pass === false) blockers.push("ledger_conservation_failed");
    const balanceHash = sha({ cash: snap.settledCashMinor, reserved: snap.reservedMinor, invested: snap.investedMinor, version: snap.versions.portfolioVersion });
    const pv = await issuePreview(D, { kind: "paperAccountReset", accountId: ctx.accountId, payload: { accountVersion: String(snap.versions.portfolioVersion), balanceHash, blockers }, nowMs: ctx.nowMs });
    return { data: { previewToken: pv.previewToken, accountVersion: String(snap.versions.portfolioVersion), balanceHash, recordsAffected: { ledger: ledgerN, fills: fillsN, trades: tradesN, navMarks: null }, expiresAt: pv.expiresAt, blockers, conservation, canReset: blockers.length === 0 }, resourceVersion: String(snap.versions.portfolioVersion) };
  },
  async resetPaperAccount(params, ctx, env) {
    requireEnabled(ctx, "resetPaperAccount");
    const D = ctx.admin;
    const PF = require("./_investorPortfolio");
    const X = lazy("./_investorExecution");
    const snap = await PF.snapshot({ accountId: ctx.accountId, asOfMs: ctx.nowMs, admin: D });
    if (String(snap.versions.portfolioVersion) !== String(params.accountVersion)) throw typed("VERSION_CONFLICT", "account version changed since the preview", { resourceVersion: String(snap.versions.portfolioVersion) });
    const balanceHash = sha({ cash: snap.settledCashMinor, reserved: snap.reservedMinor, invested: snap.investedMinor, version: snap.versions.portfolioVersion });
    if (balanceHash !== params.balanceHash) throw typed("VERSION_CONFLICT", "balances changed since the preview", { resourceVersion: String(snap.versions.portfolioVersion) });
    const pointers = await pointersFor(D, ctx.accountId);
    if (snap.positions.length || snap.workingOrders.length || pointers.some((p) => !TERMINAL_POINTER.has(p.status))) throw typed("STATE_CONFLICT", "reset needs no open position, working order or active mandate");
    const conservation = X ? await X.assertConservation(ctx.accountId, { admin: D }) : { pass: true };
    if (!conservation.pass) throw typed("STATE_CONFLICT", "ledger conservation must pass before a reset");
    const starting = big(snap.startingNavMinor || snap.navMinor);
    await consumePreview(D, { token: env.previewToken, kind: "paperAccountReset", accountId: ctx.accountId, nowMs: ctx.nowMs, verify: (p) => { if (p.payload.balanceHash !== params.balanceHash || String(p.payload.accountVersion) !== String(params.accountVersion)) throw typed("PREVIEW_TOKEN_INVALID", "preview does not match the reset request"); },
      andThen: async (p, tx) => {
        const aref = D.col(D.COL.accounts).doc(ctx.accountId);
        const cur = await tx.get(aref);
        const a = cur.exists ? cur.data() : {};
        const resetId = `reset_${ctx.mutationId.slice(-12)}`;
        tx.set(D.col(D.COL.ledger).doc(`${resetId}_close`), { txnId: `${resetId}_close`, accountId: ctx.accountId, kind: "account_reset_close", legs: [{ account: "cash", amountCents: -Number(big(snap.settledCashMinor)) }, { account: "contributed_capital", amountCents: Number(big(snap.settledCashMinor)) }], postedAt: iso(ctx.nowMs), atMs: ctx.nowMs, meta: { mutationId: ctx.mutationId, by: ctx.actorId } });
        tx.set(D.col(D.COL.ledger).doc(`${resetId}_open`), { txnId: `${resetId}_open`, accountId: ctx.accountId, kind: "capital_contribution", legs: [{ account: "cash", amountCents: Number(starting) }, { account: "contributed_capital", amountCents: -Number(starting) }], postedAt: iso(ctx.nowMs), atMs: ctx.nowMs, meta: { mutationId: ctx.mutationId, by: ctx.actorId, reset: true } });
        tx.set(aref, { accountId: ctx.accountId, balanceCents: { cash: Number(starting), reserved: 0, positions: 0, contributed_capital: -Number(starting) }, balanceRevision: (Number(a.balanceRevision) || 0) + 2, portfolioVersion: (Number(a.portfolioVersion || a.balanceRevision) || 0) + 2, writerEpoch: (Number(a.writerEpoch) || 0) + 1, startingNavCents: Number(starting), resetAtMs: ctx.nowMs, resetBy: ctx.actorId, resetMutationId: ctx.mutationId, updatedAtMs: ctx.nowMs }, { merge: true });
        tx.set(D.col(D.COL.reservationAccounts).doc(ctx.accountId), { accountId: ctx.accountId, reservedNotionalMinor: "0", reservedPlannedLossMinor: "0", reservedStressLossMinor: "0", committedPortfolioPlanId: null, resetAtMs: ctx.nowMs, updatedAtMs: ctx.nowMs }, { merge: true });
        return { resetId };
      } });
    await writeAudit(D, { action: "resetPaperAccount", actorId: ctx.actorId, accountId: ctx.accountId, mutationId: ctx.mutationId, reason: env.auditReason, before: { navMinor: snap.navMinor, version: snap.versions.portfolioVersion }, after: { startingNavMinor: starting.toString() }, correlationId: ctx.correlationId, nowMs: ctx.nowMs, extra: { conservation } });
    forgetMemo("");
    return { data: { accountId: ctx.accountId, startingNav: money(starting), conservation, note: "history is retained in the ledger; balances restart from the contributed capital" }, resourceVersion: String(Number(snap.versions.portfolioVersion) + 2) };
  },
  async reconcile(params, ctx) {
    const J = jobsFor(ctx.admin);
    const r = await J.enqueueOnce({ task: "execute", dedupeId: `reconcile_${ctx.accountId}_${Math.floor(ctx.nowMs / 60000)}`, accountId: ctx.accountId, priority: 20, createdBy: `apiV2:${ctx.actorId}`, payload: { accountId: ctx.accountId, reason: "operator_reconcile", correlationId: ctx.correlationId } });
    return { data: { jobId: r.jobId, duplicate: r.duplicate === true, note: "reconciliation runs in the executor, never inline" }, jobId: r.jobId };
  },
  async freezeUniverse(params, ctx, env) {
    const r = await v1().freezeUniverse({ operator: ctx.actorId });
    if (r && r.error) throw typed("SEMANTIC_REJECTED", r.error);
    await writeAudit(ctx.admin, { action: "freezeUniverse", actorId: ctx.actorId, accountId: ctx.accountId, mutationId: ctx.mutationId, reason: env.auditReason, after: { universeVersion: r && r.universeVersion || null, universeHash: r && r.universeHash || null }, correlationId: ctx.correlationId, nowMs: ctx.nowMs });
    forgetMemo("");
    return { data: { result: r, note: "a frozen roster snapshot never alters an investment decision" } };
  },
  async resolveCiks(params, ctx, env) {
    const r = await v1().resolveCiks({ operator: ctx.actorId, symbols: params.symbols });
    if (r && r.error) throw typed("SEMANTIC_REJECTED", r.error);
    await writeAudit(ctx.admin, { action: "resolveCiks", actorId: ctx.actorId, accountId: ctx.accountId, mutationId: ctx.mutationId, reason: env.auditReason, after: { symbols: params.symbols || null, result: r ? { ok: r.ok, resolved: r.resolved, unresolved: r.unresolved } : null }, correlationId: ctx.correlationId, nowMs: ctx.nowMs });
    return { data: { result: r } };
  },
  async setIssuerDomains(params, ctx, env) {
    const D = ctx.admin, ctrl = ctx.control;
    const current = String(ctrl.sourceRegistryVersion || "1");
    if (params.expectedSourceRegistryVersion !== current) throw typed("VERSION_CONFLICT", `source registry version is ${current}`, { resourceVersion: current });
    const domains = [...new Set(params.domains.map((d) => String(d).toLowerCase()))];
    const next = { ...(ctrl.issuerDomainOverrides || {}), [params.symbol]: { domains, setBy: ctx.actorId, setAtMs: ctx.nowMs, reason: env.auditReason || null } };
    const out = await transitionControl(D, { expectedVersion: null, patch: { issuerDomainOverrides: next, sourceRegistryVersion: String(Number(current) + 1) }, action: "setIssuerDomains", actorId: ctx.actorId, reason: env.auditReason, nowMs: ctx.nowMs, correlationId: ctx.correlationId, mutationId: ctx.mutationId });
    return { data: { symbol: params.symbol, domains, sourceRegistryVersion: String(Number(current) + 1), note: "a domain correction is audited and cannot authorize a trade" }, resourceVersion: String(out.version) };
  },
  async acknowledgeAlert(params, ctx) {
    const AL = require("./_investorAlerts");
    const D = ctx.admin;
    const s = await D.col(D.COL.alerts).doc(params.alertId).get();
    if (!s.exists) throw typed("NOT_FOUND", "unknown alert");
    if (String(Number(s.data().occurrences) || 1) !== String(params.alertVersion)) throw typed("VERSION_CONFLICT", "alert version changed", { resourceVersion: String(Number(s.data().occurrences) || 1) });
    const r = await AL.acknowledge({ admin: D, conditionId: params.alertId, by: ctx.actorId, nowMs: ctx.nowMs });
    return { data: { alertId: params.alertId, acknowledged: r.acknowledged, stillActive: r.stillActive, note: "acknowledgement is recorded; the condition resolves only when it clears" }, resourceVersion: String(params.alertVersion) };
  },
};

/* ═══ DISPATCH ════════════════════════════════════════════════════════════ */
const READS = { managerDashboard: readManagerDashboard, controlState: readControlState, companies: readCompanies, companyDossier: readCompanyDossier, portfolio: readPortfolio, mandates: readMandates, orderSets: readOrderSets, executionEvents: readExecutionEvents,
  managerRuns: readManagerRuns, jobs: readJobs, decisionJournal: readDecisionJournal, decisionAnalytics: readDecisionAnalytics, performance: readPerformance, materialEvents: readMaterialEvents, corporateActions: readCorporateActions, systemHealth: readSystemHealth,
  universe: readUniverse, sources: readSources, soakStatus: readSoakStatus, auditExports: readAuditExports, alerts: readAlerts, account: readAccount, quotes: readQuotes, intraday: readIntraday, history: readHistory, navSeries: readNavSeries, ledger: readLedger };
for (const a of S.READ_ACTIONS) if (!READS[a]) throw new Error(`v2 read ${a} declared in the contract has no handler`);
for (const a of S.MUTATION_ACTIONS) if (!MUTATIONS[a]) throw new Error(`v2 mutation ${a} declared in the contract has no handler`);

function envelope({ ok, requestId, nowMs, data = null, error = null, partial = false, partialReason = null, nextCursor = null, extra = {} }) {
  return { ok, requestId: requestId || null, payloadVersion: S.PAYLOAD_VERSION, asOf: new Date(nowMs).toISOString(), partial: partial === true, partialReason: partialReason || null, nextCursor: nextCursor || null, data, error, ...extra };
}
function statusOf(err) { return S.HTTP_STATUS[err && err.code] || 500; }
function errorOf(err, correlationId) {
  const code = err && S.HTTP_STATUS[err.code] ? err.code : "INTERNAL";
  return S.errorShape(code, err && err.message ? err.message : "internal error", { correlationId, fieldIssues: err && err.fieldIssues ? err.fieldIssues : [] });
}
/** The v2 entry point. Returns {statusCode, body, headers?}. `authOverride` lets the attestation run without an HTTP session. */
async function dispatch({ body, event = {}, admin = null, nowMs = Date.now(), authOverride = null }) {
  const D = db(admin);
  const correlationId = `c_${crypto.randomBytes(6).toString("hex")}`;
  const v = S.validateRequest(body);
  if (!v.ok) return { statusCode: statusOf({ code: v.error.code }), body: envelope({ ok: false, requestId: body && body.requestId || null, nowMs, error: { ...v.error, correlationId } }) };
  const { action, spec, params, kind } = v;
  const AUTH = require("./_investorAuth");
  const auth = authOverride || await AUTH.requireOperatorV2(event, body, { mutation: kind === "mutation", reauth: spec.reauth === true, admin: D, nowMs });
  if (!auth.ok) return { statusCode: statusOf({ code: auth.code }), body: envelope({ ok: false, requestId: body.requestId, nowMs, error: S.errorShape(auth.code, auth.message, { correlationId }) }) };
  const ctrl = await controlDoc(D);
  const accountId = String(params.accountId || ctrl.accountId || "paper-1");
  const policy = POLICY.loadActiveSync(ctrl);
  const ctx = { admin: D, control: ctrl, accountId, nowMs, policy, auth, actorId: auth.subject || "operator", correlationId, mutationId: null };
  if (kind === "read") {
    try {
      const out = await READS[action]({ params, ctx });
      return { statusCode: 200, body: envelope({ ok: true, requestId: body.requestId, nowMs, data: out.data, partial: out.partial === true, partialReason: out.partialReason || null, nextCursor: out.nextCursor || null }) };
    } catch (e) {
      if (!S.HTTP_STATUS[e.code]) console.error("investorApiV2 read failed", AUTH.redact({ action, correlationId, error: e.message, stack: (e.stack || "").slice(0, 300) }));
      return { statusCode: statusOf(e), body: envelope({ ok: false, requestId: body.requestId, nowMs, error: errorOf(e, correlationId) }) };
    }
  }
  /* mutation: attestation first, then the idempotency claim before any side effect */
  if (!attestationOk(ctrl) && action !== "freezeBuys" && action !== "emergencyStop" && action !== "acknowledgeAlert" && action !== "pauseManager") {
    return { statusCode: 503, body: envelope({ ok: false, requestId: body.requestId, nowMs, error: S.errorShape("ATTESTATION_FAILED", "the deployed build's fixtures are not attested; only freezeBuys, emergencyStop, pauseManager and acknowledgeAlert are accepted", { correlationId }) }) };
  }
  const env = { idempotencyKey: body.idempotencyKey, auditReason: body.auditReason || null, expectedResourceVersion: body.expectedResourceVersion, expectedAbsent: body.expectedAbsent, previewToken: body.previewToken };
  const requestHash = sha({ action, params, auditReason: env.auditReason, expectedResourceVersion: env.expectedResourceVersion === undefined ? null : env.expectedResourceVersion, previewToken: env.previewToken || null, expectedAbsent: env.expectedAbsent === true });
  const claim = await claimMutation(D, { actorId: ctx.actorId, accountId, action, idempotencyKey: body.idempotencyKey, requestHash, nowMs, correlationId });
  if (claim.reused) return { statusCode: 409, body: envelope({ ok: false, requestId: body.requestId, nowMs, error: S.errorShape("IDEMPOTENCY_KEY_REUSED", "this idempotency key was used with different content", { correlationId }) }) };
  if (claim.inProgress) return { statusCode: 409, body: envelope({ ok: false, requestId: body.requestId, nowMs, error: S.errorShape("IN_PROGRESS", "the same mutation is still in progress", { correlationId }) }) };
  if (claim.replay) {
    const m = claim.replay;
    if (m.status === "FAILED") return { statusCode: statusOf({ code: m.error && m.error.code }), body: envelope({ ok: false, requestId: body.requestId, nowMs, error: { ...(m.error || S.errorShape("INTERNAL", "stored failure")), correlationId }, extra: { mutationId: m.mutationId, replayed: true } }) };
    const r = m.result || {};
    return { statusCode: 200, body: envelope({ ok: true, requestId: body.requestId, nowMs, data: r.data || null, extra: { mutationId: m.mutationId, acceptedAt: iso(m.createdAtMs), jobId: r.jobId || null, resourceVersion: r.resourceVersion || null, requestedState: r.requestedState || null, appliedState: r.appliedState || null, replayed: true } }) };
  }
  ctx.mutationId = claim.mutationId;
  try {
    const out = await MUTATIONS[action](params, ctx, env);
    const result = { data: out.data || null, jobId: out.jobId || null, resourceVersion: out.resourceVersion || null, requestedState: out.requestedState || null, appliedState: out.appliedState || null };
    await finishMutation(D, claim.mutationId, { status: "COMPLETE", result, nowMs });
    return { statusCode: 200, body: envelope({ ok: true, requestId: body.requestId, nowMs, data: result.data, extra: { mutationId: claim.mutationId, acceptedAt: iso(nowMs), jobId: result.jobId, resourceVersion: result.resourceVersion, requestedState: result.requestedState, appliedState: result.appliedState } }) };
  } catch (e) {
    const err = errorOf(e, correlationId);
    if (!S.HTTP_STATUS[e.code]) console.error("investorApiV2 mutation failed", AUTH.redact({ action, correlationId, error: e.message, stack: (e.stack || "").slice(0, 300) }));
    await finishMutation(D, claim.mutationId, { status: "FAILED", error: err, nowMs }).catch(() => ({}));
    return { statusCode: statusOf(e), body: envelope({ ok: false, requestId: body.requestId, nowMs, error: err, extra: { mutationId: claim.mutationId, resourceVersion: e.resourceVersion || null } }) };
  }
}

module.exports = {
  API_BUILD, READS, MUTATIONS, INVESTMENT_MUTATIONS, TERMINAL_POINTER, dispatch, envelope,
  controlView, controlCapabilities, blockingConditions, accountModeOf, managerStateOf, buyStateOf, emergencyStateOf, executorStateOf, mutationsEnabled, attestationOk,
  page, cursorEncode, cursorDecode, money, price, qty, runView, alertView, healthComponent, claimMutation, transitionControl, issuePreview, consumePreview, forgetMemo, mandateStateOf,
};
