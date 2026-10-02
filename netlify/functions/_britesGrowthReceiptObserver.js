'use strict';

// Observes existing server-saved receipts only. No event construction, ingestion,
// upload, queue/lease/cache/audit write, or caller-supplied request ID is reachable.
// OAuth refresh preserves the existing grant; request diagnostics use GET only.
const crypto = require('node:crypto');
const {openCredentials} = require('./googleAdsDataManager.js');
const QUEUE = 'Brites_GAds_ConvQueue';
const CREDENTIAL_DOC = 'config/googleAdsDataManager';
const STATUS_URL = 'https://datamanager.googleapis.com/v1/requestStatus:retrieve';
const TOKEN_URL = 'https://oauth2.googleapis.com/token';
const DM_SCOPE = 'https://www.googleapis.com/auth/datamanager';
const CLOUD_SCOPE = 'https://www.googleapis.com/auth/cloud-platform';
const MAX_SCAN = 500, MAX_BATCH = 25, MAX_MS = 30000, MAX_CONCURRENCY = 5;
const ALLOWED_OPTIONS = new Set(['limit', 'maxMs']);
const STATES = new Set(['SUCCESS', 'FAILED', 'FAILURE', 'PARTIAL_SUCCESS', 'PROCESSING', 'REQUEST_STATUS_UNSPECIFIED']);

function observerError(code, message, httpStatus = null) {return Object.assign(Error(message), {code, httpStatus});}
function numericCount(value) {
  if (!(typeof value === 'number' || typeof value === 'string') || value === '' || !/^\d+$/.test(String(value))) return null;
  const number = Number(value); return Number.isSafeInteger(number) && number >= 0 ? number : null;
}
function canonicalDestination(value) {
  const account = value?.operatingAccount, type = account?.accountType || account?.product;
  const accountId = String(account?.accountId || ''), actionId = String(value?.productDestinationId || '');
  return type === 'GOOGLE_ADS' && /^\d+$/.test(accountId) && /^\d+$/.test(actionId) ? {type, accountId, actionId} : null;
}
function configuredDestination(env) {
  const match = /^customers\/(\d+)\/conversionActions\/(\d+)$/.exec(String(env.GADS_CONVERSION_ACTION || ''));
  return match ? {type: 'GOOGLE_ADS', accountId: match[1], actionId: match[2]} : null;
}
function sameDestination(a, b) {return !!a && !!b && a.type === b.type && a.accountId === b.accountId && a.actionId === b.actionId;}
function reasonCounts(info) {
  const counts = Array.isArray(info?.errorCounts) ? info.errorCounts : Array.isArray(info?.warningCounts) ? info.warningCounts : [];
  return counts.slice(0, 20).map(value => ({
    reason: /^[A-Z][A-Z0-9_]{0,179}$/.test(String(value?.reason || '')) ? value.reason : 'UNSPECIFIED_PROCESSING_REASON',
    recordCount: numericCount(value?.recordCount ?? value?.count)
  }));
}
function receiptEvidence(data, original, storedRows) {
  const target = canonicalDestination(original), rows = Array.isArray(data?.requestStatusPerDestination) ? data.requestStatusPerDestination : [];
  const matches = target ? rows.filter(row => sameDestination(canonicalDestination(row?.destination), target)) : [];
  const base = {providerDestinationVerified: false, providerReceiptConfirmed: false, individualOrderConfirmed: false,
    destinationRows: rows.length, matchingDestinations: matches.length, requestStatus: null, recordCount: null, errors: [], warnings: []};
  if (!target) return {...base, outcome: 'unconfirmed', code: 'ORIGINAL_DESTINATION_MISSING'};
  if (matches.length !== 1) return {...base, outcome: 'unconfirmed', code: !rows.length ? 'NO_STATUS' : matches.length > 1 ? 'AMBIGUOUS_DESTINATION' : 'DESTINATION_UNCONFIRMED'};
  const row = matches[0], raw = String(row.requestStatus || ''), status = STATES.has(raw) ? raw : raw ? 'UNRECOGNIZED_STATUS' : null;
  const recordCount = numericCount(row.eventsIngestionStatus?.recordCount), errors = reasonCounts(row.errorInfo), warnings = reasonCounts(row.warningInfo);
  const evidence = {...base, providerDestinationVerified: true, requestStatus: status, recordCount, errors, warnings};
  if (status === 'SUCCESS' && !errors.length && recordCount === 1 && storedRows === 1) return {...evidence,
    outcome: 'confirmed', code: 'EXACT_DESTINATION_ONE_RECORD_SUCCESS', providerReceiptConfirmed: true};
  if (status === 'SUCCESS') return {...evidence, outcome: 'unconfirmed', code: errors.length ? 'SUCCESS_WITH_ERRORS' : storedRows > 1 ? 'SHARED_RECEIPT_REQUIRES_REVIEW' : 'RECORD_COUNT_UNCONFIRMED'};
  if (status === 'FAILED' || status === 'FAILURE') return {...evidence, outcome: 'rejected', code: 'PROVIDER_RECEIPT_REJECTED'};
  if (status === 'PARTIAL_SUCCESS') return {...evidence, outcome: 'unconfirmed', code: 'PARTIAL_SUCCESS_REQUIRES_RECORD_REVIEW'};
  if (status === 'PROCESSING') return {...evidence, outcome: 'processing', code: 'PROVIDER_PROCESSING'};
  return {...evidence, outcome: 'unconfirmed', code: status ? 'STATUS_UNCONFIRMED' : 'NO_STATUS'};
}

function createReceiptObserver({env = {}, db, fetch: fetcher = globalThis.fetch, now = Date.now} = {}) {
  async function bounded(task, deadline, operation, maximum = MAX_MS) {
    const remaining = Math.min(maximum, deadline - now());
    if (!(remaining > 0)) throw observerError('TIME_LIMIT', 'The read-only observation budget expired.');
    const controller = new AbortController(); let timer;
    const timeout = new Promise((_, reject) => {timer = setTimeout(() => {
      controller.abort(); reject(observerError('TIME_LIMIT', 'The ' + operation + ' observation did not finish within its read budget.'));
    }, remaining);});
    try {return await Promise.race([Promise.resolve().then(() => task(controller.signal)), timeout]);}
    finally {clearTimeout(timer);}
  }
  async function jsonRequest(url, init, deadline, operation, maximum) {
    return bounded(async signal => {
      let response;
      try {response = await fetcher(url, {...init, signal, redirect: 'error'});}
      catch (error) {throw observerError(error?.name === 'AbortError' ? 'TIME_LIMIT' : 'PROVIDER_UNAVAILABLE', 'The provider observation request could not finish.');}
      let data;
      try {data = await response.json();} catch {throw observerError('INVALID_PROVIDER_RESPONSE', 'The provider did not return readable diagnostics.', response.status || null);}
      if (!response.ok) {
        const code = response.status === 401 ? 'PROVIDER_AUTHORIZATION_REQUIRED' : response.status === 403 ? 'PROVIDER_ACCESS_DENIED' : response.status === 429 ? 'PROVIDER_RATE_LIMIT' : response.status === 404 ? 'PROVIDER_RECEIPT_NOT_FOUND' : 'PROVIDER_HTTP_ERROR';
        throw observerError(code, 'The provider observation returned HTTP ' + response.status + '.', response.status);
      }
      if (!data || typeof data !== 'object' || Array.isArray(data)) throw observerError('INVALID_PROVIDER_RESPONSE', 'The provider did not return a diagnostics object.');
      return data;
    }, deadline, operation, maximum);
  }
  async function credentials(deadline) {
    const value = {client: env.GADS_DATAMANAGER_CLIENT_ID || env.GADS_CLIENT_ID,
      secret: env.GADS_DATAMANAGER_CLIENT_SECRET || env.GADS_CLIENT_SECRET, refresh: env.GADS_DATAMANAGER_REFRESH_TOKEN};
    if (value.client && value.secret && value.refresh) return value;
    const saved = await bounded(() => db.doc(CREDENTIAL_DOC).get(), deadline, 'credential read');
    if (!saved.exists) throw observerError('CREDENTIALS_MISSING', 'The existing Data Manager connection is not available.');
    try {
      const decoded = openCredentials(saved.data(), env);
      if (!decoded?.client || !decoded.secret || !decoded.refresh) throw Error('Incomplete connection');
      return decoded;
    } catch {throw observerError('CREDENTIALS_UNREADABLE', 'The saved Data Manager connection could not be opened.');}
  }
  async function read(options = {}) {
    const started = now(), out = {schemaVersion: 1, at: started, readOnly: true, receiptOnly: true,
      evidenceKind: 'saved_receipt_status_observation', queueUpdated: false, individualOrdersUpdated: 0,
      providerAggregateUsedForConfirmation: false, scannedRows: 0, scanComplete: null, savedReceiptRows: 0,
      uniqueReceipts: 0, selected: 0, requested: 0, observed: 0, confirmed: 0, rejected: 0,
      processing: 0, unconfirmed: 0, unavailable: 0, blocked: false, receipts: []};
    if (!options || typeof options !== 'object' || Array.isArray(options) || Object.keys(options).some(key => !ALLOWED_OPTIONS.has(key))) {
      return {...out, blocked: true, code: 'INVALID_OBSERVER_OPTIONS', message: 'Only bounded batch size and read budget can be supplied.'};
    }
    const rawLimit = options.limit === undefined ? 15 : Number(options.limit), rawMs = options.maxMs === undefined ? 20000 : Number(options.maxMs);
    if (!Number.isFinite(rawLimit) || !Number.isFinite(rawMs)) return {...out, blocked: true, code: 'INVALID_OBSERVER_OPTIONS', message: 'Use finite observation limits.'};
    const limit = Math.min(MAX_BATCH, Math.max(0, Math.floor(rawLimit))), maxMs = Math.min(MAX_MS, Math.max(0, rawMs)), deadline = started + maxMs;
    out.limit = limit; out.maxMs = maxMs;
    if (!limit || !maxMs) return {...out, stopped: !limit ? 'limit' : 'time_limit'};
    if (!db || typeof db.collection !== 'function' || typeof db.doc !== 'function') return {...out, blocked: true, code: 'SAVED_RECEIPTS_UNAVAILABLE', message: 'Saved receipt storage is unavailable.'};
    if (typeof fetcher !== 'function') return {...out, blocked: true, code: 'PROVIDER_UNAVAILABLE', message: 'Provider diagnostics are unavailable.'};
    const configured = configuredDestination(env), groups = new Map();
    try {
      const snapshot = await bounded(() => db.collection(QUEUE).where('uploaded', '==', false).limit(MAX_SCAN).get(), deadline, 'saved receipt read');
      if (!Array.isArray(snapshot?.docs)) throw observerError('SAVED_RECEIPTS_UNAVAILABLE', 'Saved receipt storage returned an invalid observation.');
      out.scannedRows = snapshot.docs.length; out.scanComplete = snapshot.docs.length < MAX_SCAN;
      for (const doc of snapshot.docs) {
        const row = doc.data();
        if (!row || row.uploaded !== false || ['success', 'failed'].includes(row.dmState) || !row.dmRequestId) continue;
        const id = row.dmRequestId;
        if (typeof id !== 'string' || !id.trim() || id.length > 1024 || /[\x00-\x1f]/.test(id)) {
          out.receipts.push({receiptKey: crypto.createHash('sha256').update('invalid:' + String(doc.id || out.receipts.length)).digest('hex').slice(0, 20),
            savedRows: 1, outcome: 'unconfirmed', code: 'SAVED_REQUEST_ID_INVALID', providerReceiptConfirmed: false, individualOrderConfirmed: false});
          out.unconfirmed++; continue;
        }
        out.savedReceiptRows++;
        if (!groups.has(id)) groups.set(id, []);
        groups.get(id).push({destination: row.dmDestination, submittedAt: Number(row.dmSubmittedAt) || null,
          neverChecked: !(row.dmCheckedAt || Number(row.dmChecks)), state: row.dmState || null});
      }
      out.uniqueReceipts = groups.size;
      const candidates = [...groups].sort((a, b) => Math.min(...a[1].map(row => row.submittedAt || 0)) - Math.min(...b[1].map(row => row.submittedAt || 0))).slice(0, limit);
      out.selected = candidates.length;
      if (!candidates.length) return {...out, stopped: 'no_saved_receipts'};
      const connection = await credentials(deadline);
      const auth = await jsonRequest(TOKEN_URL, {method: 'POST', headers: {'Content-Type': 'application/x-www-form-urlencoded'}, body: new URLSearchParams({
        client_id: connection.client, client_secret: connection.secret, refresh_token: connection.refresh, grant_type: 'refresh_token'
      })}, deadline, 'authorization', 10000);
      if (typeof auth.access_token !== 'string' || !auth.access_token) throw observerError('CREDENTIALS_UNAVAILABLE', 'The existing Data Manager connection did not authorize observation.');
      const scopes = typeof auth.scope === 'string' ? auth.scope.split(/\s+/).filter(Boolean) : null;
      out.authorization = {scopeReported: scopes !== null, dataManagerScope: scopes ? scopes.includes(DM_SCOPE) : null, cloudPlatformScope: scopes ? scopes.includes(CLOUD_SCOPE) : null};
      if (!scopes) throw observerError('SCOPE_UNCONFIRMED', 'The provider did not confirm the Data Manager observation scope.');
      if (!scopes.includes(DM_SCOPE)) throw observerError('SCOPE_MISSING', 'The existing connection does not include the Data Manager observation scope.');
      // Status reads are performed by a small cursor-driven worker pool. Fifteen sequential provider
      // reads could consume the whole synchronous-function allowance even when
      // every individual request stayed inside its own timeout. The pool remains
      // bounded, preserves candidate order in the returned evidence, and stops
      // launching work as soon as a global blocker is known.
      //
      // Small batches remain sequential so an authorization/rate-limit response
      // cannot unnecessarily fan out. Larger batches use at most five GETs at a
      // time; no cursor, receipt ID, or mutable checkpoint is accepted from the
      // caller or written to storage.
      const concurrency = candidates.length >= MAX_CONCURRENCY ? MAX_CONCURRENCY : 1;
      const batchReceipts = new Array(candidates.length); let completedSelected = 0, incompleteBatch = false;
      const observe = async (candidate, index) => {
        const [requestId, rows] = candidate;
        if (now() >= deadline) {incompleteBatch = true; return;}
        const first = rows[0], original = canonicalDestination(first.destination), conflictingDestinations = rows.some(row => !sameDestination(canonicalDestination(row.destination), original));
        const receipt = {receiptKey: crypto.createHash('sha256').update(requestId).digest('hex').slice(0, 20), savedRows: rows.length,
          submittedAt: Math.min(...rows.map(row => row.submittedAt || started)), neverChecked: rows.every(row => row.neverChecked),
          originalDestinationAvailable: !!original, matchesConfiguredDestination: configured && original ? sameDestination(original, configured) : null,
          overdue: rows.some(row => row.submittedAt && started - row.submittedAt > 86400000),
          providerReceiptConfirmed: false, individualOrderConfirmed: false};
        if (!original || conflictingDestinations) {
          Object.assign(receipt, {outcome: 'unconfirmed', code: !original ? 'ORIGINAL_DESTINATION_MISSING' : 'CONFLICTING_SAVED_DESTINATIONS'});
          batchReceipts[index] = receipt; completedSelected++; out.unconfirmed++; return;
        }
        out.requested++;
        try {
          const data = await jsonRequest(STATUS_URL + '?requestId=' + encodeURIComponent(requestId), {method: 'GET', headers: {Authorization: 'Bearer ' + auth.access_token, Accept: 'application/json'}}, deadline, 'receipt diagnostics', 10000);
          Object.assign(receipt, receiptEvidence(data, first.destination, rows.length)); out.observed++;
          if (receipt.outcome === 'confirmed') out.confirmed++;
          else if (receipt.outcome === 'rejected') out.rejected++;
          else if (receipt.outcome === 'processing') out.processing++;
          else out.unconfirmed++;
        } catch (error) {
          Object.assign(receipt, {outcome: 'unavailable', code: error.code || 'PROVIDER_UNAVAILABLE', httpStatus: error.httpStatus || null}); out.unavailable++;
          if (error.code === 'TIME_LIMIT') {out.stopped = 'time_limit'; incompleteBatch = true;}
          if ([401, 403, 429].includes(error.httpStatus)) {out.blocked = true; out.stopped = error.httpStatus === 429 ? 'rate_limit' : 'authorization';}
        }
        batchReceipts[index] = receipt; completedSelected++;
      };
      let cursor = 0;
      await Promise.all(Array.from({length: Math.min(concurrency, candidates.length)}, async () => {
        while (cursor < candidates.length) {
          if (out.stopped || now() >= deadline) {incompleteBatch = true; return;}
          const index = cursor++;
          await observe(candidates[index], index);
        }
      }));
      out.receipts.push(...batchReceipts.filter(Boolean));
      out.batchProgress = {completed: completedSelected, selected: candidates.length, concurrency};
      out.batchComplete = !incompleteBatch && completedSelected === candidates.length;
      // A partial provider batch may contain individually successful reads, but
      // it is not an all-or-nothing reconciliation input. The trusted preview
      // observes `blocked` and therefore emits zero repair proposals. A later
      // invocation safely re-reads the same saved receipts; nothing is claimed,
      // cached, advanced, or replayed here.
      if (!out.batchComplete) {
        out.blocked = true; out.code = out.code || 'INCOMPLETE_RECEIPT_BATCH'; out.reconciliationEligible = false;
        out.stopped = out.stopped || 'time_limit';
      } else out.reconciliationEligible = !out.blocked;
      if (!out.stopped && groups.size > limit) out.stopped = 'limit';
      out.selectionComplete = out.selected === out.uniqueReceipts && !out.stopped;
      out.elapsedMs = Math.max(0, now() - started);
      return out;
    } catch (error) {
      return {...out, blocked: true, code: error.code || 'SAVED_RECEIPTS_UNAVAILABLE',
        message: error.code ? error.message : 'Saved receipt observation could not finish.',
        ...(error.httpStatus ? {httpStatus: error.httpStatus} : {}),
        ...(error.code === 'TIME_LIMIT' ? {stopped: 'time_limit'} : {}), elapsedMs: Math.max(0, now() - started)};
    }
  }
  return {read};
}

module.exports = {createReceiptObserver, receiptEvidence, canonicalDestination, QUEUE, DM_SCOPE, MAX_SCAN, MAX_BATCH, MAX_MS};
