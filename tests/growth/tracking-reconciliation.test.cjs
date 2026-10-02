'use strict';

// All accounts, receipts and orders in this suite are synthetic. No real
// credentials, order data, account requests or conversion uploads are used.
const assert = require('node:assert/strict');
const test = require('node:test');
const { createDataManager, destination } = require('../../netlify/functions/googleAdsDataManager');
const clone = value => value == null ? value : JSON.parse(JSON.stringify(value));
const target = destination('customers/123/conversionActions/456', '999');
const epoch = Date.parse('2026-09-30T12:00:00Z');
const receipt = over => ({ orderId: 'synthetic-order', uploaded: false, failed: false, dmState: 'processing',
  dmRequestId: 'synthetic-receipt', dmDestination: clone(target), dmSubmittedAt: epoch - 3 * 86400000,
  dmNextCheckAt: 0, dmChecks: 0, value: 82, refundedTotal: 7, currency: 'USD',
  conversionDateTime: '2026-09-26 10:30:00-04:00', gclid: 'synthetic-click', ...over });
const response = (data, status = 200) => ({ ok: status >= 200 && status < 300, status, json: async () => clone(data) });
const diagnostics = (state = 'SUCCESS', over = {}) => ({ requestStatusPerDestination: [
  { destination: clone(target), requestStatus: state, eventsIngestionStatus: { recordCount: '1' }, ...over }
] });

function fixture(initial = [receipt()], options = {}) {
  const rows = new Map(initial.map((row, i) => ['row-' + i, clone(row)]));
  const calls = [], logs = [], transactionCalls = [], reads = [];
  let clock = epoch, serial = Promise.resolve();
  const snapshot = id => {
    const value = clone(rows.get(id));
    return { id, ref: ref(id), exists: value !== undefined, data: () => clone(value) };
  };
  const ref = id => ({ id, update: async patch => rows.set(id, { ...rows.get(id), ...clone(patch) }) });
  const query = (conditions = [], maximum = Infinity) => ({
    where: (field, op, value) => { assert.equal(op, '=='); return query([...conditions, [field, value]], maximum); },
    limit: n => query(conditions, n),
    get: async () => {
      reads.push(clone(conditions));
      const docs = [...rows.keys()].filter(id => conditions.every(([field, value]) => rows.get(id)[field] === value))
        .slice(0, maximum).map(snapshot);
      return { docs, size: docs.length, empty: !docs.length, forEach: fn => docs.forEach(fn) };
    }
  });
  const db = { collection: () => query(), runTransaction: fn => {
    const current = serial.then(async () => {
      const writes = [], sequence = transactionCalls.length + 1;
      transactionCalls.push(sequence);
      if (options.transactionError?.(sequence)) throw Error('Synthetic transaction failure');
      if (options.transactionAdvance) clock += options.transactionAdvance(sequence);
      const value = await fn({ get: async r => snapshot(r.id), update: (r, patch) => writes.push([r.id, clone(patch)]) });
      writes.forEach(([id, patch]) => rows.set(id, { ...rows.get(id), ...patch }));
      return value;
    });
    serial = current.catch(() => {}); return current;
  } };
  const f = { FV: { serverTimestamp: () => clock }, db };
  const env = { GADS_DATAMANAGER_REFRESH_TOKEN: 'fixture-refresh', GADS_CLIENT_ID: 'fixture-client',
    GADS_CLIENT_SECRET: 'fixture-secret', GADS_CONVERSION_ACTION: 'customers/123/conversionActions/456',
    GADS_LOGIN_CUSTOMER_ID: '999', ...options.env };
  const fetch = async (url, init) => {
    calls.push({ url, init: clone(init) });
    if (url.includes('oauth2.googleapis.com')) return options.oauth?.() || response({
      access_token: 'fixture-token', scope: 'https://www.googleapis.com/auth/datamanager', expires_in: 3600
    });
    assert.match(url, /\/requestStatus:retrieve\?requestId=/, 'receipt refresh must never upload an event');
    assert.equal(init.method, 'GET'); assert.equal(init.body, undefined);
    return options.diagnostic ? options.diagnostic({ url, init, rows, advance: ms => { clock += ms; } }) : response(diagnostics());
  };
  const api = createDataManager({ env, fetch, fb: () => options.noStorage ? null : f, COL: { convQueue: 'synthetic-queue' },
    ledger: async row => { if (options.ledgerError) throw Error('Synthetic audit failure'); logs.push(clone(row)); }, now: () => clock });
  return { api, rows, calls, logs, reads, transactionCalls, advance: ms => { clock += ms; },
    statusCalls: () => calls.filter(c => c.url.includes('requestStatus:retrieve')) };
}

test('receipt refresh confirms one exact original destination and preserves the order', async () => {
  const original = receipt();
  // Configuration can change after an upload: its receipt belongs to the saved
  // original account/action, not whichever action is configured today.
  const f = fixture([original], { env: { GADS_CONVERSION_ACTION: 'customers/888/conversionActions/777' } });
  const result = await f.api.reconcile();
  assert.equal(result.receiptOnly, true); assert.equal(result.checked, 1); assert.equal(result.confirmed, 1);
  assert.equal(f.statusCalls().length, 1); assert.match(f.statusCalls()[0].url, /synthetic-receipt$/);
  const saved = f.rows.get('row-0');
  assert.equal(saved.uploaded, true); assert.equal(saved.dmState, 'success'); assert.equal(saved.failed, false);
  for (const field of ['orderId', 'value', 'currency', 'refundedTotal', 'conversionDateTime', 'gclid', 'dmRequestId', 'dmDestination'])
    assert.deepEqual(saved[field], original[field], field + ' is preserved');
  assert.equal(saved.dmChecks, 1); assert.equal(saved.dmReconcileLease, null); assert.equal(saved.dmReconcileLeaseUntil, 0);
  assert.equal(f.logs.length, 1); assert.equal(f.logs[0].processingVerified, true); assert.equal(f.logs[0].accepted, 1);
  assert.equal((await f.api.health()).confirmed, 0, 'another configured action cannot inherit this confirmation');
});

test('unsent, unknown-submission and already terminal orders are left untouched', async () => {
  const unsent = receipt({ dmRequestId: undefined, dmState: undefined, dmDestination: undefined });
  const unknown = receipt({ dmRequestId: undefined, dmState: 'submission_unknown', failed: true });
  const failed = receipt({ dmState: 'failed', failed: true, uploadError: 'PROCESSING_ERROR_REASON_INVALID_GCLID' });
  const confirmed = receipt({ dmState: 'success', uploaded: true });
  const f = fixture([unsent, unknown, failed, confirmed]);
  const before = clone([...f.rows]);
  const result = await f.api.reconcile({ force: true });
  assert.equal(result.checked, 0); assert.deepEqual([...f.rows], before); assert.equal(f.statusCalls().length, 0);
  assert.equal(f.logs.length, 0); assert.equal(f.transactionCalls.length, 0);
});

for (const [name, answer] of [
  ['wrong account', diagnostics('SUCCESS', { destination: destination('customers/999/conversionActions/456') })],
  ['wrong action', diagnostics('SUCCESS', { destination: destination('customers/123/conversionActions/999') })],
  ['wrong account type', diagnostics('SUCCESS', { destination: { operatingAccount: { accountType: 'DISPLAY_VIDEO', accountId: '123' }, productDestinationId: '456' } })],
  ['missing destination', diagnostics('SUCCESS', { destination: undefined })],
  ['missing account type', diagnostics('SUCCESS', { destination: { operatingAccount: { accountId: '123' }, productDestinationId: '456' } })],
  ['missing account', diagnostics('SUCCESS', { destination: { productDestinationId: '456' } })],
  ['missing action', diagnostics('SUCCESS', { destination: { operatingAccount: { accountType: 'GOOGLE_ADS', accountId: '123' } } })],
  ['no status rows', {}],
  ['malformed status list', { requestStatusPerDestination: {} }],
  ['null row', { requestStatusPerDestination: [null] }],
  ['duplicate matching destination', { requestStatusPerDestination: [...diagnostics().requestStatusPerDestination, ...diagnostics().requestStatusPerDestination] }],
  ['zero events', diagnostics('SUCCESS', { eventsIngestionStatus: { recordCount: '0' } })],
  ['missing event count', diagnostics('SUCCESS', { eventsIngestionStatus: undefined })],
  ['aggregate fifteen events', diagnostics('SUCCESS', { eventsIngestionStatus: { recordCount: '15' } })],
  ['unknown status', diagnostics('REQUEST_STATUS_UNKNOWN')],
  ['still processing', diagnostics('PROCESSING')],
  ['contradictory success with errors', diagnostics('SUCCESS', { errorInfo: { errorCounts: [{ reason: 'PROCESSING_ERROR_REASON_INVALID_GCLID', recordCount: '1' }] } })]
]) test(name + ' cannot confirm a particular order', async () => {
  const f = fixture([receipt()], { diagnostic: async () => response(answer) });
  const result = await f.api.reconcile();
  assert.equal(result.confirmed, 0); assert.equal(result.failed, 0); assert.equal(result.processing, 1);
  assert.equal(f.rows.get('row-0').uploaded, false); assert.equal(f.rows.get('row-0').dmState, 'processing');
  assert.equal(f.rows.get('row-0').dmChecks, 1); assert.equal(f.logs.length, 0);
  const health = await f.api.health(); assert.equal(health.confirmed, 0); assert.equal(health.staleProcessing, 1);
  await f.api.reconcile(); assert.equal(f.statusCalls().length, 1, 'normal backoff applies to unresolved receipts');
});

test('deprecated account product field and processing warnings are retained', async () => {
  const d = clone(target); d.operatingAccount.product = 'GOOGLE_ADS'; delete d.operatingAccount.accountType;
  const f = fixture([receipt()], { diagnostic: async () => response(diagnostics('SUCCESS', {
    destination: d, warningInfo: { warningCounts: [{ reason: 'PROCESSING_WARNING_REASON_USER_IDENTIFIER_DECRYPTION_ERROR', recordCount: '1' }] }
  })) });
  assert.equal((await f.api.reconcile()).confirmed, 1);
  assert.deepEqual(f.rows.get('row-0').dmWarnings, ['PROCESSING_WARNING_REASON_USER_IDENTIFIER_DECRYPTION_ERROR']);
});

for (const state of ['FAILED', 'FAILURE', 'PARTIAL_SUCCESS']) test(state + ' keeps the rejected receipt and cannot reupload it', async () => {
  const f = fixture([receipt()], { diagnostic: async () => response(diagnostics(state, {
    errorInfo: { errorCounts: [{ reason: 'PROCESSING_ERROR_REASON_DUPLICATE_TRANSACTION_ID', recordCount: '1' }] },
    warningInfo: { warningCounts: [{ reason: 'PROCESSING_WARNING_REASON_UNSPECIFIED', recordCount: '1' }] }
  })) });
  const result = await f.api.reconcile(); assert.equal(result.failed, 1); assert.equal(result.confirmed, 0);
  const saved = clone(f.rows.get('row-0')); assert.equal(saved.dmState, 'failed'); assert.equal(saved.failed, true);
  assert.equal(saved.uploaded, false); assert.equal(saved.dmRequestId, 'synthetic-receipt');
  assert.match(saved.uploadError, /DUPLICATE_TRANSACTION_ID/); assert.equal(f.logs.length, 0);
  f.advance(2 * 3600000); await f.api.reconcile({ force: true }); await f.api.run({ retryRejected: true });
  assert.equal(f.statusCalls().length, 1); assert.deepEqual(f.rows.get('row-0'), saved);
});

test('missing saved destination is recorded without making a status or ingest request', async () => {
  const f = fixture([receipt({ dmDestination: undefined })]);
  const result = await f.api.reconcile(); assert.equal(result.confirmed, 0); assert.equal(result.processing, 1);
  assert.match(result.errors[0].error, /original conversion destination/); assert.equal(f.statusCalls().length, 0);
  assert.equal(f.rows.get('row-0').uploaded, false); assert.match(f.rows.get('row-0').dmCheckError, /original conversion destination/);
});

for (const [name, diagnostic] of [
  ['network error', async () => { throw Error('Synthetic connection timeout'); }],
  ['server error', async () => response({ error: { status: 'UNAVAILABLE', message: 'Synthetic status service unavailable' } }, 503)]
]) test(name + ' preserves the receipt and previous failure details', async () => {
  const f = fixture([receipt({ uploadError: 'Original unresolved diagnostic', dmLastStatus: 'PROCESSING' })], { diagnostic });
  const result = await f.api.reconcile(); const row = f.rows.get('row-0');
  assert.equal(result.confirmed, 0); assert.equal(result.processing, 1); assert.equal(row.dmRequestId, 'synthetic-receipt');
  assert.equal(row.uploaded, false); assert.equal(row.failed, false); assert.equal(row.uploadError, 'Original unresolved diagnostic');
  assert.equal(row.dmLastStatus, 'PROCESSING'); assert.ok(row.dmCheckError); assert.equal(row.dmReconcileLeaseUntil, 0);
  assert.equal(f.logs.length, 0); assert.equal((await f.api.health()).staleProcessing, 1);
});

test('missing or invalid OAuth authorization never claims or mutates an order', async () => {
  const missing = fixture([receipt()], { env: { GADS_DATAMANAGER_REFRESH_TOKEN: '' } });
  const beforeMissing = clone([...missing.rows]); assert.equal((await missing.api.reconcile()).blocked, true);
  assert.equal(missing.calls.length, 0); assert.deepEqual([...missing.rows], beforeMissing);
  const invalid = fixture([receipt()], { oauth: () => response({ error: 'invalid_grant' }, 400) });
  const beforeInvalid = clone([...invalid.rows]); await assert.rejects(invalid.api.reconcile(), /authorization failed/);
  assert.equal(invalid.statusCalls().length, 0); assert.equal(invalid.transactionCalls.length, 0);
  assert.deepEqual([...invalid.rows], beforeInvalid);
  const scope = fixture([receipt()], { oauth: () => response({ access_token: 'fixture-token', scope: 'unrelated-fixture-scope' }) });
  await assert.rejects(scope.api.reconcile(), /Data Manager scope/); assert.equal(scope.transactionCalls.length, 0);
});

for (const status of [401, 403, 429]) test('HTTP ' + status + ' stops the run without touching further orders', async () => {
  const f = fixture([receipt(), receipt({ orderId: 'other-order', dmRequestId: 'other-receipt' })], {
    diagnostic: async () => response({ error: { message: 'Synthetic access/quota error' } }, status)
  });
  const untouched = clone(f.rows.get('row-1'));
  const result = await f.api.reconcile(); assert.equal(result.blocked, true); assert.equal(result.checked, 1);
  assert.equal(result.stopped, status === 429 ? 'rate_limit' : 'authorization'); assert.equal(f.statusCalls().length, 1);
  assert.deepEqual(f.rows.get('row-1'), untouched); assert.equal(f.rows.get('row-0').failed, false);
  assert.equal(f.rows.get('row-0').dmRequestId, 'synthetic-receipt'); assert.equal(f.logs.length, 0);
});

test('concurrent refreshes retrieve and audit one receipt once', { timeout: 3000 }, async () => {
  const f = fixture(); const results = await Promise.all(Array.from({ length: 12 }, () => f.api.reconcile({ force: true })));
  assert.equal(f.statusCalls().length, 1); assert.equal(results.reduce((sum, r) => sum + r.confirmed, 0), 1);
  assert.equal(f.rows.get('row-0').dmChecks, 1); assert.equal(f.logs.length, 1);
});

test('a forced refresh bypasses backoff but not a fresh check or an active lease', async () => {
  const f = fixture([receipt({ dmNextCheckAt: epoch + 4 * 3600000 })], { diagnostic: async () => response(diagnostics('PROCESSING')) });
  await f.api.reconcile(); assert.equal(f.statusCalls().length, 0);
  await f.api.reconcile({ force: true }); assert.equal(f.statusCalls().length, 1);
  await f.api.reconcile({ force: true }); assert.equal(f.statusCalls().length, 1);
  f.advance(61000); await f.api.reconcile({ force: true }); assert.equal(f.statusCalls().length, 2);
  const leased = fixture([receipt({ dmReconcileLease: 'another-worker', dmReconcileLeaseUntil: epoch + 90000 })]);
  const before = clone([...leased.rows]); await leased.api.reconcile({ force: true });
  assert.equal(leased.statusCalls().length, 0); assert.deepEqual([...leased.rows], before);
});

for (const [name, mutate] of [
  ['replacement receipt', row => ({ ...row, dmRequestId: 'replacement-receipt' })],
  ['changed destination', row => ({ ...row, dmDestination: destination('customers/777/conversionActions/456') })],
  ['another worker already confirmed it', row => ({ ...row, uploaded: true, dmState: 'success' })],
  ['a terminal failure was recorded', row => ({ ...row, failed: true, dmState: 'failed', uploadError: 'Independent failure' })]
]) test('an in-flight answer cannot overwrite a ' + name, async () => {
  let changed;
  const f = fixture([receipt()], { diagnostic: async ({ rows }) => {
    changed = mutate(rows.get('row-0')); rows.set('row-0', clone(changed)); return response(diagnostics());
  } });
  const result = await f.api.reconcile(); assert.equal(result.confirmed, 0); assert.equal(f.logs.length, 0);
  assert.deepEqual(f.rows.get('row-0'), changed);
});

test('a lost lease cannot let a late refresh overwrite the newer answer', { timeout: 3000 }, async () => {
  let release, started;
  const firstStarted = new Promise(resolve => { started = resolve; });
  const held = new Promise(resolve => { release = resolve; });
  let requests = 0;
  const f = fixture([receipt()], { diagnostic: async ({ advance }) => {
    requests++;
    if (requests === 1) { started(); await held; return response(diagnostics()); }
    return response(diagnostics('FAILED', { errorInfo: { errorCounts: [{ reason: 'PROCESSING_ERROR_REASON_INVALID_GCLID', recordCount: '1' }] } }));
  } });
  const first = f.api.reconcile(); await firstStarted; f.advance(91000);
  const second = await f.api.reconcile(); release(); const late = await first;
  assert.equal(second.failed, 1); assert.equal(late.confirmed, 0); assert.equal(f.rows.get('row-0').dmState, 'failed');
  assert.equal(f.rows.get('row-0').uploaded, false); assert.equal(f.logs.length, 0);
});

test('audit failure cannot undo a durable confirmation or cause another upload', async () => {
  const f = fixture([receipt()], { ledgerError: true });
  const result = await f.api.reconcile(); assert.equal(result.confirmed, 1); assert.match(result.errors[0].error, /audit write failed/);
  assert.equal(f.rows.get('row-0').uploaded, true); assert.equal(f.rows.get('row-0').dmState, 'success');
  f.advance(3600000); await f.api.reconcile({ force: true }); await f.api.run({ retryRejected: true });
  assert.equal(f.statusCalls().length, 1);
});

test('a failed save preserves uncertainty and a later lease can recover it', async () => {
  let failSave = true;
  const f = fixture([receipt()], { transactionError: sequence => sequence === 2 && failSave });
  const failed = await f.api.reconcile(); assert.equal(failed.confirmed, 0); assert.equal(failed.errors.length, 1);
  assert.equal(f.rows.get('row-0').uploaded, false); assert.equal(f.logs.length, 0);
  failSave = false; f.advance(91000); assert.equal((await f.api.reconcile()).confirmed, 1);
  assert.equal(f.logs.length, 1); assert.equal(f.statusCalls().length, 2);
});

test('limits bound successful checks and failed transaction attempts', async () => {
  const input = Array.from({ length: 6 }, (_, i) => receipt({ orderId: 'order-' + i, dmRequestId: 'receipt-' + i }));
  const limited = fixture(input); const result = await limited.api.reconcile({ limit: 2 });
  assert.equal(result.confirmed, 2); assert.equal(result.stopped, 'limit'); assert.equal(limited.statusCalls().length, 2);
  assert.deepEqual(limited.rows.get('row-2'), input[2]);
  const failing = fixture(input, { transactionError: () => true }); const before = clone([...failing.rows]);
  const stopped = await failing.api.reconcile({ limit: 2 }); assert.equal(stopped.attempted, 2); assert.equal(stopped.checked, 0);
  assert.equal(stopped.errors.length, 2); assert.equal(failing.transactionCalls.length, 2); assert.equal(failing.statusCalls().length, 0);
  assert.deepEqual([...failing.rows], before);
});

test('zero work budgets do not mint a token, read a queue or claim orders', async () => {
  for (const options of [{ limit: 0 }, { maxMs: 0 }]) {
    const f = fixture(); const before = clone([...f.rows]); await f.api.reconcile(options);
    assert.equal(f.calls.length, 0); assert.equal(f.reads.length, 0); assert.equal(f.transactionCalls.length, 0);
    assert.deepEqual([...f.rows], before);
  }
});

test('elapsed time stops further checks and each request uses the remaining budget', async () => {
  const f = fixture(Array.from({ length: 4 }, (_, i) => receipt({ dmRequestId: 'receipt-' + i })), {
    diagnostic: async ({ advance }) => { advance(100); return response(diagnostics()); }
  });
  const result = await f.api.reconcile({ maxMs: 200 }); assert.equal(result.checked, 2); assert.equal(result.stopped, 'time_limit');
  assert.equal(f.statusCalls().length, 2); assert.equal(f.statusCalls()[0].init.timeout, 200); assert.equal(f.statusCalls()[1].init.timeout, 100);
});

test('a slow receipt claim consumes the time budget and releases its lease without a request', async () => {
  const f = fixture([receipt()], { transactionAdvance: sequence => sequence === 1 ? 250 : 0 });
  const before = clone(f.rows.get('row-0')); const result = await f.api.reconcile({ maxMs: 200 });
  assert.equal(result.checked, 0); assert.equal(result.confirmed, 0); assert.equal(f.statusCalls().length, 0);
  const saved = f.rows.get('row-0'); assert.equal(saved.dmReconcileLease, null); assert.equal(saved.dmReconcileLeaseUntil, 0);
  assert.equal(saved.dmChecks, 0); assert.equal(saved.dmCheckedAt, undefined); assert.equal(saved.dmNextCheckAt, before.dmNextCheckAt);
  assert.equal(saved.uploaded, false); assert.equal(saved.dmRequestId, before.dmRequestId); assert.equal(f.logs.length, 0);
});

test('due receipts are checked before freshly checked receipts', async () => {
  const f = fixture([receipt({ dmCheckedAt: epoch - 70000, dmRequestId: 'later' }), receipt({ dmRequestId: 'oldest' })]);
  await f.api.reconcile({ limit: 1 }); assert.match(f.statusCalls()[0].url, /oldest$/);
});
