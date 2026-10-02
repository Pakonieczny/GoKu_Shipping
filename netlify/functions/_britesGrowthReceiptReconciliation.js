'use strict';

// Internal sandbox helper. Supply raw diagnostics captured by a trusted
// server-side receipt GET, never browser-supplied success flags. A plan is an
// in-memory capability belonging to this instance; JSON cannot recreate it.
// There is no provider client, event construction, ingestion or reupload here.
const crypto = require('node:crypto');
const SANDBOX_NAMESPACE = 'Brites_Growth_Sandbox';
const DEFAULT_QUEUE = SANDBOX_NAMESPACE + '_ReceiptQueue';
const MAX_BATCH = 25;
const MAX_EVIDENCE_AGE_MS = 5 * 60000;
const MAX_PLAN_TTL_MS = 2 * 60000;
const CLAIM_FIELDS = ['individualOrderConfirmed', 'dmIndividualOrderConfirmed', 'attributionConfirmed', 'orderAttributionConfirmed'];
const EVIDENCE_FIELDS = new Set(['rowId', 'requestId', 'observedAt', 'destination', 'diagnostics', 'individualOrderConfirmed']);
const INPUT_FIELDS = new Set(['rows', 'evidence']);
const APPLY_FIELDS = new Set(['dryRun']);
const STATUS_FIELDS = Object.freeze(['uploaded', 'failed', 'dmState', 'dmChecks', 'dmCheckedAt', 'dmLastStatus',
  'dmCheckError', 'dmWarnings', 'dmProviderReceiptConfirmed', 'dmReceiptEvidenceAt',
  'dmReceiptEvidenceKind', 'dmReceiptReconciliationId']);
const hash = value => crypto.createHash('sha256').update(String(value)).digest('hex');
const validId = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(value);
const validRequest = value => typeof value === 'string' && value.length <= 1024 && !!value.trim() && !/[\x00-\x1f]/.test(value);
const object = value => !!value && typeof value === 'object' && !Array.isArray(value);

function millis(value) {
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (value instanceof Date) return Number.isFinite(value.getTime()) ? value.getTime() : null;
  if (object(value) && typeof value.toMillis === 'function') {
    try {const result = value.toMillis(); return Number.isFinite(result) ? result : null;} catch {return null;}
  }
  return null;
}
function stable(value, seen = new Set(), depth = 0) {
  if (depth > 25) throw Error('Row exceeds fingerprint depth.');
  if (value === null) return ['null'];
  if (value === undefined) return ['undefined'];
  if (['string', 'boolean'].includes(typeof value)) return [typeof value, value];
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw Error('Non-finite row number.');
    return ['number', Object.is(value, -0) ? '-0' : value];
  }
  if (value instanceof Date) {
    const at = millis(value); if (at === null) throw Error('Invalid row date.'); return ['date', at];
  }
  if (Buffer.isBuffer(value)) return ['bytes', value.toString('base64')];
  if (!object(value) && !Array.isArray(value)) throw Error('Unsupported row value.');
  if (seen.has(value)) throw Error('Cyclic row.');
  if (typeof value.toMillis === 'function') {
    const at = millis(value); if (at === null) throw Error('Invalid row timestamp.');
    if (Number.isInteger(value.seconds) && Number.isInteger(value.nanoseconds)) return ['timestamp', value.seconds, value.nanoseconds];
    return ['timestamp', at];
  }
  if (typeof value.path === 'string' && typeof value.get === 'function') return ['reference', value.path];
  seen.add(value);
  const result = Array.isArray(value) ? ['array', value.map(item => stable(item, seen, depth + 1))]
    : ['object', Object.keys(value).sort().map(key => [key, stable(value[key], seen, depth + 1)])];
  seen.delete(value); return result;
}
function rowFingerprint(value) {
  const encoded = JSON.stringify(stable(value));
  if (encoded.length > 128000) throw Error('Row exceeds fingerprint size.');
  return hash(encoded);
}
function canonicalDestination(value) {
  if (!object(value) || !object(value.operatingAccount)) return null;
  const account = value.operatingAccount, type = account.accountType || account.product;
  if (type !== 'GOOGLE_ADS' || (account.accountType && account.product && account.accountType !== account.product)) return null;
  const accountId = account.accountId, actionId = value.productDestinationId;
  return typeof accountId === 'string' && typeof actionId === 'string' && /^\d+$/.test(accountId) && /^\d+$/.test(actionId)
    ? {accountId, actionId, accountType: 'GOOGLE_ADS'} : null;
}
function sameDestination(a, b) {
  return !!a && !!b && a.accountId === b.accountId && a.actionId === b.actionId && a.accountType === b.accountType;
}
function claimedAttribution(value) {return CLAIM_FIELDS.some(key => value?.[key] !== undefined && value[key] !== false);}
function expiryProblem(row, at) {
  if (row.expired === true || row.deleted === true) return 'ROW_EXPIRED';
  for (const field of ['expiresAt', 'expireAt', 'expiresAtMs', 'dmReconciliationExpiresAt']) {
    if (row[field] === undefined || row[field] === null) continue;
    const expiry = millis(row[field]);
    if (expiry === null) return 'ROW_EXPIRY_UNCONFIRMED';
    if (expiry <= at) return 'ROW_EXPIRED';
  }
  return null;
}
function rowProblem(row, at, namespace) {
  if (!object(row)) return 'SAVED_ROW_MISSING';
  if (row.namespace !== undefined && row.namespace !== namespace) return 'ROW_NAMESPACE_UNSAFE';
  if (claimedAttribution(row)) return 'ATTRIBUTION_CLAIM_UNSUPPORTED';
  const expiry = expiryProblem(row, at); if (expiry) return expiry;
  if (row.uploaded !== false || row.failed !== undefined && row.failed !== false || row.dmState !== 'processing') return 'ROW_NOT_PENDING_RECEIPT';
  if (!validRequest(row.dmRequestId)) return 'SAVED_REQUEST_ID_INVALID';
  if (!canonicalDestination(row.dmDestination)) return 'SAVED_DESTINATION_UNSUPPORTED';
  const submitted = millis(row.dmSubmittedAt);
  if (!(submitted > 0) || submitted > at) return 'SAVED_SUBMISSION_UNCONFIRMED';
  if (row.dmChecks !== undefined && (!Number.isSafeInteger(row.dmChecks) || row.dmChecks < 0 || row.dmChecks >= Number.MAX_SAFE_INTEGER)) return 'SAVED_CHECK_COUNT_INVALID';
  if (row.dmReconcileLeaseUntil !== undefined && row.dmReconcileLeaseUntil !== null) {
    const until = millis(row.dmReconcileLeaseUntil);
    if (until === null) return 'LEASE_UNCONFIRMED';
    // This helper does not own or renew any lease, so every active lease is foreign.
    if (until > at) return 'ACTIVE_FOREIGN_LEASE';
  }
  if (row.leaseUntil !== undefined && row.leaseUntil !== null) {
    const until = millis(row.leaseUntil);
    if (until === null) return 'LEASE_UNCONFIRMED';
    if (until > at) return 'ACTIVE_FOREIGN_LEASE';
  }
  return null;
}
function evidenceProblem(evidence, row, at, maximumAge) {
  if (claimedAttribution(evidence) || claimedAttribution(evidence?.diagnostics)) return {code: 'ATTRIBUTION_CLAIM_UNSUPPORTED'};
  if (!object(evidence) || Object.keys(evidence).some(key => !EVIDENCE_FIELDS.has(key))) return {code: 'EVIDENCE_INVALID'};
  if (evidence.requestId !== row.dmRequestId) return {code: 'REQUEST_ID_CHANGED'};
  if (!Number.isFinite(evidence.observedAt) || evidence.observedAt <= 0 || evidence.observedAt > at) return {code: 'EVIDENCE_TIME_UNCONFIRMED'};
  if (at - evidence.observedAt >= maximumAge) return {code: 'EVIDENCE_EXPIRED'};
  if (evidence.observedAt < millis(row.dmSubmittedAt)) return {code: 'EVIDENCE_PRECEDES_SUBMISSION'};
  if (row.dmCheckedAt !== undefined && row.dmCheckedAt !== null) {
    const checked = millis(row.dmCheckedAt);
    if (checked === null || checked > evidence.observedAt) return {code: 'NEWER_ROW_CHECK'};
  }
  try {
    if (rowFingerprint(evidence.destination) !== rowFingerprint(row.dmDestination)) return {code: 'SAVED_DESTINATION_CHANGED'};
  } catch {return {code: 'SAVED_DESTINATION_UNSUPPORTED'};}
  const target = canonicalDestination(row.dmDestination), statuses = evidence.diagnostics?.requestStatusPerDestination;
  if (!Array.isArray(statuses) || statuses.length !== 1) return {code: 'SINGLE_DESTINATION_REQUIRED'};
  const status = statuses[0];
  if (!sameDestination(canonicalDestination(status?.destination), target)) return {code: 'PROVIDER_DESTINATION_UNCONFIRMED'};
  if (['audienceMembersIngestionStatus', 'audienceMembersRemovalStatus', 'removeAllAudienceMembersStatus'].some(field => status[field] !== undefined && status[field] !== null)) return {code: 'UNSUPPORTED_RECEIPT_STATUS_KIND'};
  if (status.requestStatus !== 'SUCCESS') return {code: 'PROVIDER_SUCCESS_REQUIRED'};
  if (!['number', 'string'].includes(typeof status.eventsIngestionStatus?.recordCount) || String(status.eventsIngestionStatus.recordCount) !== '1') return {code: 'ONE_RECORD_REQUIRED'};
  if (status.errorInfo !== undefined && status.errorInfo !== null) {
    if (!object(status.errorInfo) || Object.keys(status.errorInfo).some(key => key !== 'errorCounts') || status.errorInfo.errorCounts !== undefined && !Array.isArray(status.errorInfo.errorCounts)) return {code: 'ERROR_DIAGNOSTICS_UNCONFIRMED'};
    if (status.errorInfo.errorCounts?.length) return {code: 'PROVIDER_ERRORS_PRESENT'};
  }
  const warnings = status.warningInfo?.warningCounts;
  if (status.warningInfo !== undefined && status.warningInfo !== null && (!object(status.warningInfo) || Object.keys(status.warningInfo).some(key => key !== 'warningCounts') || warnings !== undefined && !Array.isArray(warnings))) return {code: 'WARNING_DIAGNOSTICS_UNCONFIRMED'};
  if (warnings?.length > 20 || warnings?.some(warning => !object(warning) || !/^[A-Z][A-Z0-9_]{0,179}$/.test(String(warning.reason || '')))) return {code: 'WARNING_DIAGNOSTICS_UNCONFIRMED'};
  return {code: null, warnings: [...new Set((warnings || []).map(warning => warning.reason))]};
}

function createReceiptReconciliation({env = {}, db, service, queueName = DEFAULT_QUEUE, now = Date.now,
  evidenceMaxAgeMs = MAX_EVIDENCE_AGE_MS, planTtlMs = MAX_PLAN_TTL_MS} = {}) {
  const plans = new WeakMap();
  const validBudget = value => typeof value === 'number' && Number.isFinite(value) && value > 0;
  const evidenceAge = validBudget(evidenceMaxAgeMs) ? Math.min(MAX_EVIDENCE_AGE_MS, evidenceMaxAgeMs) : null;
  const ttl = validBudget(planTtlMs) ? Math.min(MAX_PLAN_TTL_MS, planTtlMs) : null;
  function scopeProblem() {
    if (env.BRITES_GROWTH_SANDBOX !== '1' || env.BRITES_GROWTH_NAMESPACE !== SANDBOX_NAMESPACE || service && service.namespace !== SANDBOX_NAMESPACE) return 'SANDBOX_NAMESPACE_REQUIRED';
    if (typeof queueName !== 'string' || !new RegExp('^' + SANDBOX_NAMESPACE + '_ReceiptQueue[A-Za-z0-9]{0,64}$').test(queueName)) return 'SANDBOX_QUEUE_REQUIRED';
    if (!evidenceAge || !ttl || typeof now !== 'function') return 'RECONCILIATION_BUDGET_INVALID';
    return null;
  }
  function base(at) {return {schemaVersion: 1, at, sandboxOnly: true, receiptOnly: true,
    evidenceKind: 'receipt_status_reconciliation', providerAggregateUsedForConfirmation: false,
    individualOrderAttributionConfirmed: false, eventsIngested: 0, queueUpdated: false};}
  function failure(at, code, entries = []) {return Object.freeze({...base(at), ok: false, blocked: true, code, eligible: 0, entries: Object.freeze(entries)});}
  function plan(input = {}) {
    const at = now(), scope = scopeProblem(); if (scope) return failure(at, scope);
    if (!object(input) || Object.keys(input).some(key => !INPUT_FIELDS.has(key)) || !Array.isArray(input.rows) || !Array.isArray(input.evidence) || !input.rows.length || input.rows.length > MAX_BATCH || input.evidence.length > MAX_BATCH) return failure(at, 'PLAN_INPUT_INVALID');
    if (input.rows.length !== input.evidence.length) return failure(at, 'EXACT_ROW_EVIDENCE_REQUIRED');
    const rowIds = new Set(), requestIds = new Set(), evidenceIds = new Set(), evidenceRequests = new Set();
    for (const entry of input.rows) {
      if (!object(entry) || Object.keys(entry).some(key => !['id', 'row'].includes(key)) || !validId(entry.id)) return failure(at, 'SAVED_ROW_ID_INVALID');
      if (rowIds.has(entry.id)) return failure(at, 'DUPLICATE_ROW_ID'); rowIds.add(entry.id);
      if (validRequest(entry.row?.dmRequestId)) {
        if (requestIds.has(entry.row.dmRequestId)) return failure(at, 'DUPLICATE_REQUEST_ID'); requestIds.add(entry.row.dmRequestId);
      }
    }
    for (const evidence of input.evidence) {
      if (!object(evidence) || !validId(evidence.rowId)) return failure(at, 'EVIDENCE_ROW_ID_INVALID');
      if (evidenceIds.has(evidence.rowId)) return failure(at, 'DUPLICATE_EVIDENCE_ROW_ID'); evidenceIds.add(evidence.rowId);
      if (evidenceRequests.has(evidence.requestId)) return failure(at, 'DUPLICATE_EVIDENCE_REQUEST_ID'); evidenceRequests.add(evidence.requestId);
      if (!rowIds.has(evidence.rowId)) return failure(at, 'EXACT_ROW_EVIDENCE_REQUIRED');
    }
    const byRow = new Map(input.evidence.map(evidence => [evidence.rowId, evidence]));
    const id = crypto.randomUUID(), entries = [], bindings = [];
    let expiresAt = at + ttl;
    for (const entry of input.rows) {
      const row = entry.row, evidence = byRow.get(entry.id), receiptKey = validRequest(row?.dmRequestId) ? hash(row.dmRequestId).slice(0, 20) : null;
      const publicEntry = {rowKey: hash(queueName + '/' + entry.id).slice(0, 20), receiptKey, outcome: 'refused', code: null};
      let problem = rowProblem(row, at, SANDBOX_NAMESPACE), proof;
      if (!problem) {proof = evidenceProblem(evidence, row, at, evidenceAge); problem = proof.code;}
      if (!problem) {
        try {
          const before = rowFingerprint(row), patch = {uploaded: true, failed: false, dmState: 'success', dmChecks: (row.dmChecks || 0) + 1,
            dmCheckedAt: evidence.observedAt, dmLastStatus: 'SUCCESS', dmCheckError: null, dmWarnings: proof.warnings,
            dmProviderReceiptConfirmed: true, dmReceiptEvidenceAt: evidence.observedAt,
            dmReceiptEvidenceKind: 'exact_destination_one_record_success', dmReceiptReconciliationId: id};
          const after = rowFingerprint({...row, ...patch});
          bindings.push({id: entry.id, requestId: row.dmRequestId, before, after, patch,
            observedAt: evidence.observedAt, publicEntry});
          expiresAt = Math.min(expiresAt, evidence.observedAt + evidenceAge);
          for (const field of ['expiresAt', 'expireAt', 'expiresAtMs', 'dmReconciliationExpiresAt']) {
            if (row[field] !== undefined && row[field] !== null) expiresAt = Math.min(expiresAt, millis(row[field]));
          }
          Object.assign(publicEntry, {outcome: 'eligible', code: 'EXACT_SAVED_ONE_RECORD_SUCCESS'});
        } catch {problem = 'ROW_FINGERPRINT_UNAVAILABLE';}
      }
      if (problem) publicEntry.code = problem;
      entries.push(Object.freeze(publicEntry));
    }
    if (entries.some(entry => entry.outcome !== 'eligible')) return failure(at, 'PLAN_REFUSED', entries);
    const publicPlan = Object.freeze({...base(at), ok: true, blocked: false, planId: id, queueName,
      expiresAt, eligible: bindings.length, dryRun: true, fields: STATUS_FIELDS, entries: Object.freeze(entries)});
    plans.set(publicPlan, {bindings, expiresAt, id}); return publicPlan;
  }
  function queue() {
    const collection = service ? service.col(queueName.slice(SANDBOX_NAMESPACE.length + 1)) : db.collection(queueName);
    if (!collection || collection.path !== queueName || typeof collection.doc !== 'function' || typeof collection.where !== 'function') throw Error('SANDBOX_QUEUE_REFERENCE_UNCONFIRMED');
    return collection;
  }
  async function apply(publicPlan, options = {}) {
    const at = now(), scope = scopeProblem(), out = {...base(at), ok: false, blocked: true, dryRun: true,
      applied: 0, alreadyApplied: 0, wouldApply: 0, individualOrdersUpdated: 0, entries: []};
    const denied = code => ({...out, code});
    if (scope) return denied(scope);
    if (!object(options) || Object.keys(options).some(key => !APPLY_FIELDS.has(key)) || options.dryRun !== undefined && typeof options.dryRun !== 'boolean') return denied('APPLY_OPTIONS_INVALID');
    out.dryRun = options.dryRun !== false;
    const binding = object(publicPlan) ? plans.get(publicPlan) : null;
    if (!binding) return denied('PLAN_NOT_ISSUED_BY_THIS_INSTANCE');
    if (at >= binding.expiresAt) return denied('PLAN_EXPIRED');
    if (!db || typeof db.runTransaction !== 'function' || !service && typeof db.collection !== 'function') return denied('SANDBOX_TRANSACTION_UNAVAILABLE');
    let collection;
    try {collection = queue();} catch {return denied('SANDBOX_QUEUE_REFERENCE_UNCONFIRMED');}
    try {
      const result = await db.runTransaction(async tx => {
        const decisions = [];
        if (scopeProblem()) return {code: 'SANDBOX_NAMESPACE_REQUIRED'};
        for (const entry of binding.bindings) {
          if (now() >= binding.expiresAt) return {code: 'PLAN_EXPIRED'};
          const ref = collection.doc(entry.id);
          if (!ref || ref.path !== queueName + '/' + entry.id) return {code: 'SANDBOX_ROW_REFERENCE_UNCONFIRMED'};
          const saved = await tx.get(ref), row = saved?.exists ? saved.data() : null;
          if (!object(row)) return {code: 'SAVED_ROW_MISSING'};
          const expired = expiryProblem(row, now()); if (expired) return {code: expired};
          if (claimedAttribution(row)) return {code: 'ATTRIBUTION_CLAIM_UNSUPPORTED'};
          let current;
          try {current = rowFingerprint(row);} catch {return {code: 'ROW_FINGERPRINT_UNAVAILABLE'};}
          const alreadyApplied = row.dmReceiptReconciliationId === binding.id && current === entry.after;
          if (!alreadyApplied) {
            const problem = rowProblem(row, now(), SANDBOX_NAMESPACE); if (problem) return {code: problem};
            if (row.dmRequestId !== entry.requestId || current !== entry.before) return {code: 'ROW_CHANGED'};
          }
          // Include terminal rows: a second saved row sharing this receipt is
          // ambiguous even if another worker has already marked it successful.
          const duplicates = await tx.get(collection.where('dmRequestId', '==', entry.requestId).limit(2));
          if (!Array.isArray(duplicates?.docs) || duplicates.docs.length !== 1 || duplicates.docs[0].id !== entry.id) return {code: 'DUPLICATE_OR_UNCONFIRMED_SAVED_REQUEST'};
          if (now() >= binding.expiresAt) return {code: 'PLAN_EXPIRED'};
          decisions.push({entry, ref, alreadyApplied});
        }
        if (scopeProblem()) return {code: 'SANDBOX_NAMESPACE_REQUIRED'};
        if (now() >= binding.expiresAt) return {code: 'PLAN_EXPIRED'};
        if (!out.dryRun) for (const decision of decisions) if (!decision.alreadyApplied) tx.update(decision.ref, decision.entry.patch);
        return {decisions};
      });
      if (result.code) return denied(result.code);
      for (const decision of result.decisions) {
        if (decision.alreadyApplied) out.alreadyApplied++;
        else if (out.dryRun) out.wouldApply++;
        else out.applied++;
        out.entries.push({...decision.entry.publicEntry, outcome: decision.alreadyApplied ? 'already_applied' : out.dryRun ? 'would_apply' : 'applied'});
      }
      return {...out, ok: true, blocked: false, queueUpdated: out.applied > 0};
    } catch {return denied('SANDBOX_TRANSACTION_FAILED');}
  }
  return {plan, apply};
}

module.exports = {createReceiptReconciliation, rowFingerprint, canonicalDestination,
  SANDBOX_NAMESPACE, DEFAULT_QUEUE, MAX_BATCH, MAX_EVIDENCE_AGE_MS, MAX_PLAN_TTL_MS, STATUS_FIELDS};
