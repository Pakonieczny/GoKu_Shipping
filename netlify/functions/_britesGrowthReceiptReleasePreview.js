'use strict';

// Read-only chain: pin server queue rows -> existing bounded observer -> raw
// server GET evidence -> read-only current-row/uniqueness transaction -> preview.
// Raw diagnostics are captured privately at the fetch boundary, never accepted
// from a caller. The sandbox reconciliation planner validates them; apply is
// never called. A preview is evidence, not a reservation or an apply capability.
const crypto = require('node:crypto');
const {createReceiptObserver, QUEUE, MAX_SCAN, MAX_BATCH, MAX_MS} = require('./_britesGrowthReceiptObserver');
const {createReceiptReconciliation, rowFingerprint, canonicalDestination, DEFAULT_QUEUE, SANDBOX_NAMESPACE,
  MAX_EVIDENCE_AGE_MS, MAX_PLAN_TTL_MS} = require('./_britesGrowthReceiptReconciliation');
const TOKEN_URL = 'https://oauth2.googleapis.com/token';
const STATUS_URL = 'https://datamanager.googleapis.com/v1/requestStatus:retrieve';
const CREDENTIAL_DOC = 'config/googleAdsDataManager';
const OPTIONS = new Set(['limit', 'maxMs']);
const STATUS_FIELDS = Object.freeze(['uploaded', 'failed', 'dmState', 'dmChecks', 'dmCheckedAt', 'dmLastStatus', 'dmCheckError', 'dmWarnings']);
const STATUSES = new Set(['SUCCESS', 'PROCESSING', 'FAILED', 'FAILURE', 'PARTIAL_SUCCESS', 'REQUEST_STATUS_UNKNOWN', 'REQUEST_STATUS_UNSPECIFIED']);
const object = value => !!value && typeof value === 'object' && !Array.isArray(value);
const digest = value => crypto.createHash('sha256').update(String(value)).digest('hex');
const validRequest = value => typeof value === 'string' && !!value.trim() && value.length <= 1024 && !/[\x00-\x1f]/.test(value);
const codeOf = (value, fallback) => /^[A-Z][A-Z0-9_]{0,99}$/.test(String(value || '')) ? value : fallback;
function copy(value, seen = new Set()) {
  if (value === null || typeof value !== 'object') return value;
  if (value instanceof Date) return new Date(value.getTime());
  if (Buffer.isBuffer(value)) return Buffer.from(value);
  // Firestore timestamps and references are immutable value objects; retaining
  // their types is necessary for an exact typed row fingerprint.
  if (typeof value.toMillis === 'function' || typeof value.path === 'string' && typeof value.get === 'function') return value;
  if (seen.has(value)) throw Error('Unfingerprintable saved row');
  seen.add(value);
  const result = Array.isArray(value) ? value.map(item => copy(item, seen))
    : Object.fromEntries(Object.entries(value).map(([key, item]) => [key, copy(item, seen)]));
  seen.delete(value); return result;
}
function statusPatch(row, evidence) {
  const warnings = evidence.diagnostics.requestStatusPerDestination[0].warningInfo?.warningCounts || [];
  return {uploaded: true, failed: false, dmState: 'success', dmChecks: (row.dmChecks || 0) + 1,
    dmCheckedAt: evidence.observedAt, dmLastStatus: 'SUCCESS', dmCheckError: null,
    dmWarnings: [...new Set(warnings.map(warning => warning.reason))]};
}
function statusChanges(row, patch) {
  return {uploaded: {from: false, to: true}, failed: {from: row.failed === true, to: false},
    dmState: {from: 'processing', to: 'success'}, dmChecks: {from: row.dmChecks || 0, to: patch.dmChecks},
    dmCheckedAt: {from: typeof row.dmCheckedAt === 'number' ? row.dmCheckedAt : null, to: patch.dmCheckedAt},
    dmLastStatus: {from: STATUSES.has(row.dmLastStatus) ? row.dmLastStatus : null, to: 'SUCCESS'},
    dmCheckError: {fromHasValue: !!row.dmCheckError, to: null},
    dmWarnings: {fromCount: Array.isArray(row.dmWarnings) ? row.dmWarnings.length : 0, to: patch.dmWarnings}};
}
function providerProven(raw, saved, at) {
  if (!raw || raw.requestId !== saved.dmRequestId || !Number.isFinite(raw.observedAt) || raw.observedAt > at || at - raw.observedAt >= MAX_EVIDENCE_AGE_MS) return false;
  const rows = raw.diagnostics?.requestStatusPerDestination;
  if (!Array.isArray(rows) || rows.length !== 1) return false;
  const row = rows[0], original = canonicalDestination(saved.dmDestination), received = canonicalDestination(row?.destination);
  if (!original || !received || original.accountId !== received.accountId || original.actionId !== received.actionId) return false;
  if (row.requestStatus !== 'SUCCESS' || !['number', 'string'].includes(typeof row.eventsIngestionStatus?.recordCount) || String(row.eventsIngestionStatus.recordCount) !== '1') return false;
  if (['audienceMembersIngestionStatus', 'audienceMembersRemovalStatus', 'removeAllAudienceMembersStatus'].some(field => row[field] !== undefined && row[field] !== null)) return false;
  if (row.errorInfo !== undefined && row.errorInfo !== null && (!object(row.errorInfo) || Object.keys(row.errorInfo).some(key => key !== 'errorCounts') || row.errorInfo.errorCounts !== undefined && (!Array.isArray(row.errorInfo.errorCounts) || row.errorInfo.errorCounts.length))) return false;
  const warnings = row.warningInfo?.warningCounts;
  if (row.warningInfo !== undefined && row.warningInfo !== null && (!object(row.warningInfo) || Object.keys(row.warningInfo).some(key => key !== 'warningCounts') || warnings !== undefined && !Array.isArray(warnings))) return false;
  if (warnings?.length > 20 || warnings?.some(warning => !object(warning) || !/^[A-Z][A-Z0-9_]{0,179}$/.test(String(warning.reason || '')))) return false;
  return !['individualOrderConfirmed', 'dmIndividualOrderConfirmed', 'attributionConfirmed', 'orderAttributionConfirmed'].some(field => raw.diagnostics[field] !== undefined && raw.diagnostics[field] !== false);
}
function createReceiptReleasePreview({env = {}, db, fetch: fetcher = globalThis.fetch, now = Date.now} = {}) {
  const scopeSafe = () => env.BRITES_GROWTH_SANDBOX === '1' && env.BRITES_GROWTH_NAMESPACE === SANDBOX_NAMESPACE;
  async function bounded(task, deadline) {
    const remaining = deadline - now();
    if (!(remaining > 0)) throw Object.assign(Error('Preview budget expired'), {code: 'TIME_LIMIT'});
    let timer;
    try {return await Promise.race([Promise.resolve().then(task), new Promise((_, reject) => {
      timer = setTimeout(() => reject(Object.assign(Error('Read-only preview timed out'), {code: 'TIME_LIMIT'})), remaining);
    })]);} finally {clearTimeout(timer);}
  }
  async function read(options = {}) {
    const started = now(), out = {schemaVersion: 1, at: started, readOnly: true, receiptOnly: true, dryRun: true,
      evidenceKind: 'trusted_receipt_reconciliation_preview', queueUpdated: false, individualOrdersUpdated: 0,
      individualOrderAttributionConfirmed: false, providerAggregateUsedForConfirmation: false, eventsIngested: 0,
      productionApplyAvailable: false, recheckRequiredBeforeAnyApply: true, rowsReserved: false,
      scannedRows: 0, selectedReceipts: 0, providerReceiptsConfirmed: 0, proposedRepairs: 0, blockedRows: 0,
      omittedReceiptEntries: 0, blocked: false, rows: [], proposedStatusFields: STATUS_FIELDS,
      maximumEvidenceAgeMs: MAX_EVIDENCE_AGE_MS, maximumPlanTtlMs: MAX_PLAN_TTL_MS,
      verificationLimit: 'Provider processing success does not prove purchase attribution or browser/server duplicate counting. Re-read every row and receipt before any later status repair.'};
    const denied = code => ({...out, proposedRepairs: 0, blockedRows: Math.max(out.blockedRows, out.selectedReceipts),
      blocked: true, code, elapsedMs: Math.max(0, now() - started)});
    if (!scopeSafe()) return denied('SANDBOX_NAMESPACE_REQUIRED');
    if (!object(options) || Object.keys(options).some(key => !OPTIONS.has(key))) return denied('INVALID_PREVIEW_OPTIONS');
    const rawLimit = options.limit === undefined ? 15 : Number(options.limit), rawMs = options.maxMs === undefined ? 20000 : Number(options.maxMs);
    if (!Number.isFinite(rawLimit) || !Number.isFinite(rawMs)) return denied('INVALID_PREVIEW_OPTIONS');
    const limit = Math.min(MAX_BATCH, Math.max(0, Math.floor(rawLimit))), maxMs = Math.min(MAX_MS, Math.max(0, rawMs)), deadline = started + maxMs;
    out.limit = limit; out.maxMs = maxMs;
    if (!limit || !maxMs) return {...out, stopped: !limit ? 'limit' : 'time_limit'};
    if (!db || typeof db.collection !== 'function' || typeof db.doc !== 'function' || typeof db.runTransaction !== 'function') return denied('READ_ONLY_STORAGE_UNAVAILABLE');
    if (typeof fetcher !== 'function') return denied('PROVIDER_UNAVAILABLE');
    try {
      const collection = db.collection(QUEUE);
      if (collection?.path !== QUEUE || typeof collection.doc !== 'function') return denied('SAVED_QUEUE_REFERENCE_UNCONFIRMED');
      const snapshot = await bounded(() => collection.where('uploaded', '==', false).limit(MAX_SCAN).get(), deadline);
      if (!Array.isArray(snapshot?.docs)) return denied('SAVED_ROWS_UNAVAILABLE');
      if (snapshot.docs.length > MAX_SCAN) return denied('SAVED_SCAN_LIMIT_EXCEEDED');
      const pinned = snapshot.docs.map(doc => {
        const row = copy(doc.data()), ref = collection.doc(doc.id);
        if (typeof doc.id !== 'string' || !doc.id || doc.id.includes('/') || ref.path !== QUEUE + '/' + doc.id) throw Error('Unsafe saved row reference');
        return {id: doc.id, ref, row, fingerprint: rowFingerprint(row), rowKey: digest(QUEUE + '/' + doc.id).slice(0, 20)};
      });
      out.scannedRows = pinned.length; out.scanComplete = pinned.length < MAX_SCAN;
      const byKey = new Map(), allowedRequests = new Set(), captured = new Map();
      for (const entry of pinned) {
        if (!entry.row || entry.row.uploaded !== false || ['success', 'failed'].includes(entry.row.dmState) || !entry.row.dmRequestId) continue;
        const request = entry.row.dmRequestId, key = validRequest(request) ? digest(request).slice(0, 20) : digest('invalid:' + entry.id).slice(0, 20);
        if (!byKey.has(key)) byKey.set(key, []); byKey.get(key).push(entry);
        if (validRequest(request)) allowedRequests.add(request);
      }
      const pinnedQuery = (filtered = false, maximum = MAX_SCAN) => ({path: QUEUE,
        where(field, operator, value) {
          if (field !== 'uploaded' || operator !== '==' || value !== false || filtered) throw Error('Unsupported observer storage read');
          return pinnedQuery(true, maximum);
        },
        limit(value) {if (value !== MAX_SCAN) throw Error('Unsupported observer scan'); return pinnedQuery(filtered, value);},
        async get() {
          if (!filtered || !scopeSafe()) throw Error('Unsafe observer snapshot');
          return {docs: pinned.slice(0, maximum).map(entry => ({id: entry.id, data: () => copy(entry.row)}))};
        }});
      const observerDb = {collection(name) {if (name !== QUEUE) throw Error('Unsupported observer collection'); return pinnedQuery();},
        doc(path) {if (path !== CREDENTIAL_DOC) throw Error('Unsupported credential read'); return {get: () => db.doc(CREDENTIAL_DOC).get()};}};
      const attempted = new Set();
      const observedFetch = async (url, init) => {
        if (!scopeSafe()) throw Error('Sandbox namespace changed');
        const endpoint = new URL(String(url)), requestId = endpoint.searchParams.get('requestId');
        const oauth = endpoint.href === TOKEN_URL && init.method === 'POST' && !new URLSearchParams(init.body).has('scope');
        const status = endpoint.origin + endpoint.pathname === STATUS_URL && init.method === 'GET' && init.body === undefined
          && [...endpoint.searchParams.keys()].length === 1 && allowedRequests.has(requestId) && !attempted.has(requestId);
        if (!oauth && !status) throw Error('Unsupported provider request');
        if (status) attempted.add(requestId);
        const response = await fetcher(url, init);
        return {ok: response.ok, status: response.status, json: async () => {
          const data = await response.json();
          if (status && response.ok && object(data)) {
            const diagnostics = copy(data), observedAt = now();
            captured.set(requestId, {requestId, observedAt, diagnostics});
          }
          return data;
        }};
      };
      const remaining = deadline - now(); if (!(remaining > 0)) return denied('TIME_LIMIT');
      const observation = await createReceiptObserver({env, db: observerDb, fetch: observedFetch, now}).read({limit, maxMs: remaining});
      out.selectedReceipts = Number(observation.selected) || 0;
      out.observation = Object.fromEntries(['requested', 'observed', 'confirmed', 'rejected', 'processing', 'unconfirmed', 'unavailable', 'selectionComplete', 'stopped'].filter(key => observation[key] !== undefined).map(key => [key, observation[key]]));
      if (!scopeSafe()) return denied('SANDBOX_NAMESPACE_REQUIRED');
      const validator = createReceiptReconciliation({env, queueName: DEFAULT_QUEUE, now});
      const entries = [], candidates = [];
      const receipts = Array.isArray(observation.receipts) ? observation.receipts : [];
      out.omittedReceiptEntries = Math.max(0, receipts.length - MAX_BATCH);
      for (const receipt of receipts.slice(0, MAX_BATCH)) {
        const saved = byKey.get(receipt.receiptKey) || [], raw = saved.length === 1 ? captured.get(saved[0].row.dmRequestId) : null;
        const publicEntry = {rowKey: saved.length === 1 ? saved[0].rowKey : null, receiptKey: receipt.receiptKey,
          savedRows: saved.length || 1, outcome: 'blocked', code: null,
          providerReceiptConfirmed: saved.length === 1 && providerProven(raw, saved[0].row, now()), individualOrderConfirmed: false,
          requestStatus: STATUSES.has(receipt.requestStatus) ? receipt.requestStatus : null,
          recordCount: Number.isSafeInteger(receipt.recordCount) ? receipt.recordCount : null,
          matchesConfiguredDestination: receipt.matchesConfiguredDestination === true ? true : receipt.matchesConfiguredDestination === false ? false : null};
        let problem;
        if (saved.length !== 1) problem = saved.length > 1 ? 'DUPLICATE_SAVED_REQUEST' : 'SAVED_ROW_MAPPING_UNCONFIRMED';
        else if (!raw) problem = codeOf(receipt.code, 'TRUSTED_RAW_EVIDENCE_UNAVAILABLE');
        else {
          const entry = saved[0], evidence = {rowId: entry.id, requestId: raw.requestId, observedAt: raw.observedAt,
            destination: copy(entry.row.dmDestination), diagnostics: raw.diagnostics, individualOrderConfirmed: false};
          const plan = validator.plan({rows: [{id: entry.id, row: entry.row}], evidence: [evidence]});
          if (!plan.ok) problem = codeOf(plan.entries[0]?.code || plan.code, 'RECONCILIATION_PLAN_REFUSED');
          else {
            const evidenceFingerprint = rowFingerprint({requestId: raw.requestId, destination: evidence.destination,
              diagnostics: evidence.diagnostics, observedAt: evidence.observedAt, savedRowFingerprint: entry.fingerprint});
            Object.assign(publicEntry, {rowFingerprintBefore: entry.fingerprint, destinationFingerprint: rowFingerprint(evidence.destination),
              evidenceFingerprint, observedAt: evidence.observedAt, plannedAt: plan.at, expiresAt: plan.expiresAt});
            candidates.push({entry, evidence, plan, publicEntry});
          }
        }
        if (problem) publicEntry.code = problem;
        entries.push(publicEntry);
      }
      out.providerReceiptsConfirmed = entries.filter(entry => entry.providerReceiptConfirmed).length;
      if (observation.blocked) return denied(codeOf(observation.code, 'PROVIDER_OBSERVATION_BLOCKED'));
      if (candidates.length) {
        if (now() >= deadline) return denied('TIME_LIMIT');
        const current = await bounded(() => db.runTransaction(async tx => {
          if (!scopeSafe() || typeof tx.get !== 'function') throw Error('Unsafe read-only transaction');
          const results = new Map(); let cursor = 0;
          // Independent reads share a consistent read-only transaction snapshot.
          await Promise.all(Array.from({length: Math.min(4, candidates.length)}, async () => {
            while (cursor < candidates.length) {
              if (now() >= deadline || !scopeSafe()) throw Object.assign(Error('Preview snapshot expired'), {code: 'TIME_LIMIT'});
              const candidate = candidates[cursor++], saved = await tx.get(candidate.entry.ref);
              const duplicate = await tx.get(collection.where('dmRequestId', '==', candidate.entry.row.dmRequestId).limit(2));
              const row = saved?.exists ? copy(saved.data()) : null;
              const unique = Array.isArray(duplicate?.docs) && duplicate.docs.length === 1 && duplicate.docs[0].id === candidate.entry.id;
              results.set(candidate.entry.id, {row, unique});
            }
          }));
          if (!scopeSafe()) throw Error('Sandbox namespace changed');
          return results;
        }, {readOnly: true}), deadline);
        out.currentRowsRechecked = current.size; out.currentRowsCheckedAt = now();
        for (const candidate of candidates) {
          const {entry, evidence, plan, publicEntry} = candidate, fresh = current.get(entry.id);
          let problem;
          if (!fresh?.row) problem = 'SAVED_ROW_MISSING';
          else if (fresh.row.dmRequestId !== entry.row.dmRequestId) problem = 'REQUEST_ID_CHANGED';
          else if (!fresh.unique) problem = 'DUPLICATE_OR_UNCONFIRMED_SAVED_REQUEST';
          else if (now() >= plan.expiresAt) problem = 'PLAN_EXPIRED';
          else {
            const currentFingerprint = rowFingerprint(fresh.row);
            publicEntry.rowFingerprintCurrent = currentFingerprint;
            if (currentFingerprint !== entry.fingerprint) {
              const changed = validator.plan({rows: [{id: entry.id, row: fresh.row}], evidence: [evidence]});
              problem = changed.ok ? 'ROW_CHANGED' : codeOf(changed.entries[0]?.code || changed.code, 'ROW_CHANGED');
            }
          }
          if (problem) {publicEntry.code = problem; continue;}
          const patch = statusPatch(entry.row, evidence), proposed = {...entry.row, ...patch};
          const preserved = Object.fromEntries(Object.entries(entry.row).filter(([key]) => !STATUS_FIELDS.includes(key)));
          Object.assign(publicEntry, {outcome: 'proposed', code: 'EXACT_SAVED_ONE_RECORD_STATUS_REPAIR',
            statusChanges: statusChanges(entry.row, patch), statusProposalFingerprint: rowFingerprint(patch),
            proposedRowFingerprint: rowFingerprint(proposed), preservedFieldsFingerprint: rowFingerprint(preserved),
            monetaryClickRefundFieldsPreserved: true});
        }
      }
      if (!scopeSafe()) return denied('SANDBOX_NAMESPACE_REQUIRED');
      out.rows = entries;
      out.proposedRepairs = entries.filter(entry => entry.outcome === 'proposed').length;
      out.blockedRows = entries.filter(entry => entry.outcome !== 'proposed').reduce((sum, entry) => sum + entry.savedRows, 0) + out.omittedReceiptEntries;
      out.plannedAt = now();
      out.planExpiresAt = out.proposedRepairs ? Math.min(...entries.filter(entry => entry.outcome === 'proposed').map(entry => entry.expiresAt)) : null;
      out.elapsedMs = Math.max(0, now() - started);
      out.blocked = entries.length > 0 && out.proposedRepairs === 0;
      if (!entries.length) out.stopped = observation.stopped || 'no_saved_receipts';
      return out;
    } catch (error) {return denied(error?.code === 'TIME_LIMIT' ? 'TIME_LIMIT' : 'READ_ONLY_PREVIEW_UNAVAILABLE');}
  }
  return {read};
}

module.exports = {createReceiptReleasePreview, STATUS_FIELDS};
