'use strict';

// Synthetic saved receipts and credentials only. Provider calls are injected;
// this suite cannot upload conversions or contact live accounts.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {pathToFileURL} = require('node:url');
const {createReceiptObserver, QUEUE, DM_SCOPE, MAX_BATCH, MAX_SCAN, MAX_MS} = require('../../netlify/functions/_britesGrowthReceiptObserver');
const {sealCredentials} = require('../../netlify/functions/googleAdsDataManager');
const clone = value => JSON.parse(JSON.stringify(value));
const EPOCH = Date.parse('2026-10-02T00:00:00Z');
const DESTINATION = {operatingAccount: {accountType: 'GOOGLE_ADS', accountId: '123'}, productDestinationId: '456'};
const receipt = over => ({uploaded: false, failed: false, dmState: 'processing', dmRequestId: 'synthetic-receipt', dmDestination: clone(DESTINATION), dmSubmittedAt: EPOCH - 3 * 86400000, dmChecks: 0,
  orderId: 'private-synthetic-order', gclid: 'private-click', value: 54, currency: 'USD', customerEmail: 'private@example.test', ...over});
const reply = (status = 'SUCCESS', over = {}) => ({requestStatusPerDestination: [{destination: clone(DESTINATION), requestStatus: status, eventsIngestionStatus: {recordCount: '1'}, ...over}]});
const response = (value, status = 200) => ({ok: status >= 200 && status < 300, status, json: async () => clone(value)});
function fixture(rows = [receipt()], options = {}) {
  let clock = EPOCH;
  const env = {GADS_DATAMANAGER_REFRESH_TOKEN: 'private-refresh', GADS_CLIENT_ID: 'private-client', GADS_CLIENT_SECRET: 'private-secret', GADS_CONVERSION_ACTION: 'customers/123/conversionActions/456', ...options.env};
  const calls = [], reads = [], writes = [];
  const forbidden = method => {writes.push(method); throw Error('Read-only observer reached a write.');};
  const db = {
    collection(name) {
      assert.equal(name, QUEUE); let filters = [], maximum;
      return {where(field, op, value) {filters.push([field, op, value]); return this;}, limit(value) {maximum = value; return this;},
        async get() {
          reads.push({kind: 'queue', filters, maximum}); assert.deepEqual(filters, [['uploaded', '==', false]]); assert.equal(maximum, MAX_SCAN);
          if (options.queueRead) return options.queueRead({advance: ms => {clock += ms;}});
          return {docs: rows.filter(row => row.uploaded === false).slice(0, maximum).map((row, index) => ({id: 'row-' + index, data: () => clone(row), ref: {update: () => forbidden('ref.update'), set: () => forbidden('ref.set')}}))};
        }, add: () => forbidden('collection.add')};
    },
    doc(name) {assert.equal(name, 'config/googleAdsDataManager'); return {get: async () => {
      reads.push({kind: 'credential'}); if (options.credentialRead) return options.credentialRead();
      return {exists: !!options.savedCredentials, data: () => options.savedCredentials};
    }, set: () => forbidden('credential.set'), update: () => forbidden('credential.update')};},
    runTransaction: () => forbidden('runTransaction'), batch: () => forbidden('batch'), update: () => forbidden('update'), set: () => forbidden('set')
  };
  const fetch = async (raw, init) => {
    const url = new URL(raw); calls.push({url, init}); assert.equal(init.redirect, 'error'); assert.ok(init.signal instanceof AbortSignal);
    if (url.hostname === 'oauth2.googleapis.com') {
      assert.equal(url.pathname, '/token'); assert.equal(init.method, 'POST');
      assert.ok(init.body instanceof URLSearchParams); assert.equal(init.body.get('grant_type'), 'refresh_token');
      return options.token ? options.token(init) : response({access_token: 'private-access-token', scope: DM_SCOPE + ' https://www.googleapis.com/auth/cloud-platform'});
    }
    assert.equal(url.hostname, 'datamanager.googleapis.com'); assert.equal(url.pathname, '/v1/requestStatus:retrieve');
    assert.equal(init.method, 'GET'); assert.equal(init.body, undefined);
    assert.equal(init.headers.Authorization, 'Bearer private-access-token');
    return options.status ? options.status(url.searchParams.get('requestId'), {advance: ms => {clock += ms;}, init}) : response(reply());
  };
  const api = createReceiptObserver({env, db, fetch, now: () => clock});
  return {api, env, db, calls, reads, writes, rows, read: opts => api.read(opts)};
}
const statusCalls = f => f.calls.filter(call => call.url.hostname === 'datamanager.googleapis.com');

test('exact destination one-record SUCCESS confirms provider receipt without changing orders', async () => {
  const f = fixture(), before = clone(f.rows), out = await f.read();
  assert.equal(out.confirmed, 1); assert.equal(out.observed, 1); assert.equal(out.receipts[0].providerReceiptConfirmed, true);
  assert.equal(out.receipts[0].individualOrderConfirmed, false); assert.equal(out.queueUpdated, false); assert.equal(out.individualOrdersUpdated, 0);
  assert.deepEqual(f.rows, before); assert.deepEqual(f.writes, []); assert.equal(out.providerAggregateUsedForConfirmation, false);
});

test('output excludes order, request, account, click, customer, and credential values', async () => {
  const f = fixture(); const text = JSON.stringify(await f.read());
  for (const forbidden of ['private-synthetic-order', 'synthetic-receipt', 'private-click', 'private@example.test', 'private-refresh', 'private-secret', 'private-access-token', 'customers/123/conversionActions/456', 'accountId', 'productDestinationId']) assert.equal(text.includes(forbidden), false, forbidden);
  assert.match(JSON.parse(text).receipts[0].receiptKey, /^[a-f0-9]{20}$/);
});

test('unsent, terminal, and uploaded rows are never queried or changed', async () => {
  const f = fixture([receipt({dmRequestId: null, dmState: null}), receipt({dmState: 'success'}), receipt({dmState: 'failed', failed: true}), receipt({uploaded: true}), receipt({dmRequestId: 'safe-receipt'})]);
  const out = await f.read(); assert.equal(out.requested, 1); assert.equal(statusCalls(f)[0].url.searchParams.get('requestId'), 'safe-receipt'); assert.deepEqual(f.writes, []);
});

test('receipt identity is encoded and only comes from saved rows', async () => {
  const id = 'saved-receipt?x=1&requestId=other'; const f = fixture([receipt({dmRequestId: id})]);
  await f.read(); const url = statusCalls(f)[0].url; assert.equal(url.searchParams.size, 1); assert.equal(url.searchParams.get('requestId'), id);
});

test('arbitrary caller request IDs are rejected before storage or provider access', async () => {
  const f = fixture(); const out = await f.read({requestId: 'caller-request'});
  assert.equal(out.blocked, true); assert.equal(out.code, 'INVALID_OBSERVER_OPTIONS'); assert.equal(f.reads.length, 0); assert.equal(f.calls.length, 0);
});

for (const invalid of ['', 'bad\nrequest', 123, 'x'.repeat(1025)]) test('invalid saved request ID is retained as unconfirmed without a status GET: ' + typeof invalid + '/' + String(invalid).length, async () => {
  const f = fixture([receipt({dmRequestId: invalid})]), out = await f.read();
  assert.equal(statusCalls(f).length, 0); assert.equal(out.confirmed, 0);
  if (invalid) assert.equal(out.receipts[0].code, 'SAVED_REQUEST_ID_INVALID');
});

for (const override of [{operatingAccount: {accountType: 'GOOGLE_ADS', accountId: '124'}, productDestinationId: '456'}, {operatingAccount: {accountType: 'GOOGLE_ADS', accountId: '123'}, productDestinationId: '457'}, {operatingAccount: {accountType: 'GOOGLE_ANALYTICS', accountId: '123'}, productDestinationId: '456'}, {}]) test('a mismatched or sparse destination never confirms: ' + JSON.stringify(override), async () => {
  const f = fixture(undefined, {status: async () => response(reply('SUCCESS', {destination: override}))}), out = await f.read();
  assert.equal(out.confirmed, 0); assert.equal(out.unconfirmed, 1); assert.equal(out.receipts[0].code, 'DESTINATION_UNCONFIRMED');
});

test('deprecated Google Ads product type remains a verifiable original destination', async () => {
  const legacy = {operatingAccount: {product: 'GOOGLE_ADS', accountId: '123'}, productDestinationId: '456'};
  const f = fixture([receipt({dmDestination: legacy})], {status: async () => response(reply('SUCCESS', {destination: legacy}))});
  assert.equal((await f.read()).confirmed, 1);
});

test('missing original destination stays unresolved without querying or inventing one', async () => {
  const f = fixture([receipt({dmDestination: null})]), out = await f.read();
  assert.equal(out.confirmed, 0); assert.equal(out.receipts[0].code, 'ORIGINAL_DESTINATION_MISSING'); assert.equal(statusCalls(f).length, 0);
});

test('multiple exact provider destinations are ambiguous', async () => {
  const f = fixture(undefined, {status: async () => response({requestStatusPerDestination: [reply().requestStatusPerDestination[0], reply().requestStatusPerDestination[0]]})});
  const out = await f.read(); assert.equal(out.confirmed, 0); assert.equal(out.receipts[0].code, 'AMBIGUOUS_DESTINATION');
});

test('exact original destination remains independent from current configured action', async () => {
  const f = fixture(undefined, {env: {GADS_CONVERSION_ACTION: 'customers/123/conversionActions/999'}}), out = await f.read();
  assert.equal(out.confirmed, 1); assert.equal(out.receipts[0].matchesConfiguredDestination, false);
});

for (const count of [0, 15, undefined, null, 'one', -1]) test('SUCCESS with missing/wrong record count never confirms: ' + String(count), async () => {
  const f = fixture(undefined, {status: async () => response(reply('SUCCESS', {eventsIngestionStatus: {recordCount: count}}))}), out = await f.read();
  assert.equal(out.confirmed, 0); assert.equal(out.receipts[0].code, 'RECORD_COUNT_UNCONFIRMED');
});

test('aggregate-like fifteen-record SUCCESS is not fifteen individual confirmations', async () => {
  const rows = Array.from({length: 15}, () => receipt()), f = fixture(rows, {status: async () => response(reply('SUCCESS', {eventsIngestionStatus: {recordCount: 15}}))}), out = await f.read();
  assert.equal(out.savedReceiptRows, 15); assert.equal(out.uniqueReceipts, 1); assert.equal(out.requested, 1); assert.equal(out.confirmed, 0);
  assert.equal(out.receipts[0].savedRows, 15); assert.equal(out.receipts[0].code, 'SHARED_RECEIPT_REQUIRES_REVIEW'); assert.deepEqual(f.writes, []);
});

test('shared request ID cannot confirm one stored order even when provider says one record', async () => {
  const f = fixture([receipt(), receipt()]), out = await f.read();
  assert.equal(out.confirmed, 0); assert.equal(out.requested, 1); assert.equal(out.receipts[0].individualOrderConfirmed, false);
});

test('a shared receipt with conflicting saved destinations is not queried', async () => {
  const f = fixture([receipt(), receipt({dmDestination: {...DESTINATION, productDestinationId: '999'}})]), out = await f.read();
  assert.equal(out.confirmed, 0); assert.equal(out.receipts[0].code, 'CONFLICTING_SAVED_DESTINATIONS'); assert.equal(statusCalls(f).length, 0);
});

test('provider processing remains pending even when the local receipt is overdue', async () => {
  const f = fixture(undefined, {status: async () => response(reply('PROCESSING'))}), out = await f.read();
  assert.equal(out.processing, 1); assert.equal(out.rejected, 0); assert.equal(out.receipts[0].overdue, true); assert.equal(out.receipts[0].neverChecked, true);
});

test('empty and unknown status responses remain unconfirmed', async () => {
  for (const data of [{}, {requestStatusPerDestination: []}, reply(null), reply('UNRECOGNIZED_PROVIDER_VALUE')]) {
    const f = fixture(undefined, {status: async () => response(data)}), out = await f.read(); assert.equal(out.confirmed, 0); assert.equal(out.unconfirmed, 1);
  }
});

test('provider rejection is recorded as receipt evidence, never written to queue', async () => {
  const f = fixture(undefined, {status: async () => response(reply('FAILED', {errorInfo: {errorCounts: [{reason: 'CLICK_NOT_FOUND', recordCount: 1}]}}))});
  const out = await f.read(); assert.equal(out.rejected, 1); assert.equal(out.receipts[0].requestStatus, 'FAILED'); assert.deepEqual(out.receipts[0].errors, [{reason: 'CLICK_NOT_FOUND', recordCount: 1}]); assert.deepEqual(f.writes, []);
});

test('partial success requires per-record review rather than guessing the order outcome', async () => {
  const f = fixture(undefined, {status: async () => response(reply('PARTIAL_SUCCESS', {eventsIngestionStatus: {recordCount: 2}}))}), out = await f.read();
  assert.equal(out.unconfirmed, 1); assert.equal(out.confirmed, 0); assert.equal(out.rejected, 0); assert.equal(out.receipts[0].code, 'PARTIAL_SUCCESS_REQUIRES_RECORD_REVIEW');
});

test('SUCCESS with processing errors remains unresolved; warnings remain visible', async () => {
  const f = fixture(undefined, {status: async () => response(reply('SUCCESS', {errorInfo: {errorCounts: [{reason: 'CLICK_NOT_FOUND', recordCount: 1}]}, warningInfo: {warningCounts: [{reason: 'PROCESSING_WARNING_REASON_CLICK_ID_NOT_FOUND', recordCount: 1}]}}))});
  const out = await f.read(); assert.equal(out.confirmed, 0); assert.equal(out.receipts[0].code, 'SUCCESS_WITH_ERRORS'); assert.equal(out.receipts[0].warnings.length, 1);
});

test('warnings do not falsely reject otherwise exact one-record success', async () => {
  const f = fixture(undefined, {status: async () => response(reply('SUCCESS', {warningInfo: {warningCounts: [{reason: 'PROCESSING_WARNING_REASON_USER_IDENTIFIER_DECRYPTION_ERROR', recordCount: 1}]}}))});
  const out = await f.read(); assert.equal(out.confirmed, 1); assert.equal(out.receipts[0].warnings.length, 1);
});

test('provider processing reasons cannot echo private data into evidence', async () => {
  const f = fixture(undefined, {status: async () => response(reply('FAILED', {errorInfo: {errorCounts: [{reason: 'private-synthetic-order private@example.test', recordCount: 1}]}}))});
  const out = await f.read(); assert.equal(JSON.stringify(out).includes('private-synthetic-order'), false); assert.equal(out.receipts[0].errors[0].reason, 'UNSPECIFIED_PROCESSING_REASON');
});

test('missing credentials block before any provider request', async () => {
  const f = fixture(undefined, {env: {GADS_DATAMANAGER_REFRESH_TOKEN: undefined}}), out = await f.read();
  assert.equal(out.blocked, true); assert.equal(out.code, 'CREDENTIALS_MISSING'); assert.equal(f.calls.length, 0);
});

test('saved encrypted connection is read without saving or expanding it', async () => {
  const key = {FIREBASE_PRIVATE_KEY: 'synthetic-encryption-key', FIREBASE_PROJECT_ID: 'synthetic-project'};
  const saved = sealCredentials({client: 'private-client', secret: 'private-secret', refresh: 'private-refresh'}, key);
  const f = fixture(undefined, {env: {...key, GADS_DATAMANAGER_REFRESH_TOKEN: undefined}, savedCredentials: saved}), out = await f.read();
  assert.equal(out.confirmed, 1); assert.equal(f.reads.filter(r => r.kind === 'credential').length, 1); assert.deepEqual(f.writes, []);
});

test('unreadable encrypted credentials are a precise access blocker', async () => {
  const f = fixture(undefined, {env: {GADS_DATAMANAGER_REFRESH_TOKEN: undefined}, savedCredentials: {version: 999}}), out = await f.read();
  assert.equal(out.blocked, true); assert.equal(out.code, 'CREDENTIALS_UNREADABLE'); assert.equal(f.calls.length, 0);
});

test('wrong OAuth scope blocks all receipt GETs', async () => {
  const f = fixture(undefined, {token: async () => response({access_token: 'private-access-token', scope: 'https://www.googleapis.com/auth/adwords'})}), out = await f.read();
  assert.equal(out.blocked, true); assert.equal(out.code, 'SCOPE_MISSING'); assert.equal(statusCalls(f).length, 0);
});

test('unreported OAuth scope stays unconfirmed rather than inventing access', async () => {
  const f = fixture(undefined, {token: async () => response({access_token: 'private-access-token'})}), out = await f.read();
  assert.equal(out.blocked, true); assert.equal(out.code, 'SCOPE_UNCONFIRMED'); assert.equal(statusCalls(f).length, 0);
});

test('read-only diagnostics require DM scope but do not expand the grant for ingestion', async () => {
  const f = fixture(undefined, {token: async () => response({access_token: 'private-access-token', scope: DM_SCOPE})}), out = await f.read();
  assert.equal(out.confirmed, 1); assert.equal(out.authorization.cloudPlatformScope, false);
  assert.equal(f.calls[0].init.body.has('scope'), false, 'refresh requests no new scopes');
});

test('OAuth refusal returns a generic credential blocker without exposing provider body', async () => {
  const f = fixture(undefined, {token: async () => response({error: 'private-refresh'}, 400)}), out = await f.read();
  assert.equal(out.blocked, true); assert.equal(out.httpStatus, 400); assert.equal(statusCalls(f).length, 0); assert.equal(JSON.stringify(out).includes('private-refresh'), false);
});

for (const status of [401, 403, 429]) test('HTTP ' + status + ' stops remaining observations without changing prior receipts', async () => {
  const f = fixture([receipt(), receipt({dmRequestId: 'second-receipt'})], {status: async () => response({error: 'private-refresh'}, status)}), out = await f.read();
  assert.equal(out.blocked, true); assert.equal(out.unavailable, 1); assert.equal(out.requested, 1); assert.equal(out.stopped, status === 429 ? 'rate_limit' : 'authorization');
  assert.deepEqual(f.writes, []); assert.equal(JSON.stringify(out).includes('private-refresh'), false);
});

test('receipt not found is not called an event rejection and other receipts can continue', async () => {
  const f = fixture([receipt(), receipt({dmRequestId: 'second-receipt'})], {status: async id => id === 'synthetic-receipt' ? response({}, 404) : response(reply())}), out = await f.read();
  assert.equal(out.unavailable, 1); assert.equal(out.rejected, 0); assert.equal(out.confirmed, 1); assert.equal(out.receipts[0].code, 'PROVIDER_RECEIPT_NOT_FOUND');
});

test('unavailable provider JSON does not falsely confirm a receipt', async () => {
  const f = fixture(undefined, {status: async () => ({ok: true, status: 200, json: async () => {throw Error('private-synthetic-order');}})}), out = await f.read();
  assert.equal(out.unavailable, 1); assert.equal(out.confirmed, 0); assert.equal(out.receipts[0].code, 'INVALID_PROVIDER_RESPONSE');
});

test('batch limit and scan cap remain bounded for caller-supplied large limits', async () => {
  const f = fixture(Array.from({length: 600}, (_, i) => receipt({dmRequestId: 'receipt-' + i}))), out = await f.read({limit: 999, maxMs: 999999});
  assert.equal(out.limit, MAX_BATCH); assert.equal(out.maxMs, MAX_MS); assert.equal(out.scannedRows, MAX_SCAN); assert.equal(out.scanComplete, false);
  assert.equal(out.requested, MAX_BATCH); assert.equal(out.stopped, 'limit'); assert.equal(out.selectionComplete, false);
});

test('zero limits perform no storage, credential, or provider calls', async () => {
  for (const options of [{limit: 0}, {maxMs: 0}]) {const f = fixture(), out = await f.read(options); assert.equal(f.reads.length, 0); assert.equal(f.calls.length, 0); assert.equal(out.requested, 0);}
});

test('malformed limits fail closed before provider access', async () => {
  for (const options of [{limit: 'bad'}, {maxMs: NaN}, {limit: Infinity}]) {const f = fixture(), out = await f.read(options); assert.equal(out.code, 'INVALID_OBSERVER_OPTIONS'); assert.equal(f.calls.length, 0);}
});

test('elapsed storage time consumes the total observation budget', async () => {
  const f = fixture(undefined, {queueRead: async ({advance}) => {advance(30); return {docs: [{data: () => receipt()}]};}}), out = await f.read({maxMs: 20});
  assert.equal(out.stopped, 'time_limit'); assert.equal(f.calls.length, 0); assert.deepEqual(f.writes, []);
});

test('elapsed request time prevents starting further receipt GETs', async () => {
  const f = fixture([receipt(), receipt({dmRequestId: 'second-receipt'})], {status: async (_id, {advance}) => {advance(30); return response(reply());}}), out = await f.read({maxMs: 20});
  assert.equal(out.requested, 1); assert.equal(out.stopped, 'time_limit'); assert.equal(out.confirmed, 1);
});

test('a delayed provider ignoring abort still cannot keep the observer running', async () => {
  const f = fixture(undefined, {status: async () => new Promise(() => {})}), started = Date.now(), out = await f.read({maxMs: 25});
  assert.equal(out.stopped, 'time_limit'); assert.equal(out.unavailable, 1); assert.ok(Date.now() - started < 1000); assert.equal(statusCalls(f)[0].init.signal.aborted, true); assert.deepEqual(f.writes, []);
});

test('a stalled credential token request is bounded too', async () => {
  const f = fixture(undefined, {token: async () => new Promise(() => {})}), out = await f.read({maxMs: 25});
  assert.equal(out.stopped, 'time_limit'); assert.equal(statusCalls(f).length, 0); assert.equal(f.calls[0].init.signal.aborted, true);
});

test('concurrent observers do not claim, overwrite, or upload the same saved receipt', async () => {
  const f = fixture(), before = clone(f.rows); const [a, b] = await Promise.all([f.read(), f.read()]);
  assert.equal(a.confirmed, 1); assert.equal(b.confirmed, 1); assert.deepEqual(f.rows, before); assert.deepEqual(f.writes, []); assert.equal(statusCalls(f).length, 2);
});

test('empty queue requires no credentials or provider calls', async () => {
  const f = fixture([], {env: {GADS_DATAMANAGER_REFRESH_TOKEN: undefined}}), out = await f.read();
  assert.equal(out.stopped, 'no_saved_receipts'); assert.equal(out.blocked, false); assert.equal(f.calls.length, 0); assert.equal(f.reads.length, 1);
});

async function endpoint() {
  const root = path.resolve(__dirname, '../..'), file = path.join(root, 'netlify/functions/britesGrowthAds.js'); let source = fs.readFileSync(file, 'utf8');
  for (const name of ['_britesGrowth.js', '_britesGrowthDemand.js', '_britesGrowthReceiptObserver.js']) source = source.replace("'./" + name + "'", JSON.stringify(pathToFileURL(path.join(root, 'netlify/functions', name)).href));
  return import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
}
test('protected endpoint only passes bounded observation options without loading Ads engine', async () => {
  const {createHandler, READ_ACTIONS} = await endpoint(); let captured, observations = 0, engines = 0;
  const env = {BRITES_GROWTH_SANDBOX: '1', BRITES_GROWTH_NAMESPACE: 'Brites_Growth_Sandbox', BRITES_GROWTH_ADMIN_KEY: 'synthetic-operator'};
  const handler = createHandler({environment: () => env, readReceipts: async (_env, options) => {observations++; captured = options; return {readOnly: true, receiptOnly: true, confirmed: 1};}, savedPasscode: async () => null, loadEngine: async () => {engines++; throw Error('Observer must not load engine.');}});
  const call = (body, headers = {}, method = 'POST') => handler(new Request('https://sandbox.example/api/growth-ads', {method, headers: {'Content-Type': 'application/json', 'X-Growth-Key': 'synthetic-operator', ...headers}, ...(method === 'POST' ? {body: JSON.stringify(body)} : {})}));
  assert.ok(READ_ACTIONS.includes('receiptDiagnostics'));
  const good = await call({action: 'receiptDiagnostics', limit: 15, maxMs: 20000}); assert.equal(good.status, 200); assert.deepEqual(captured, {limit: 15, maxMs: 20000});
  assert.equal((await good.json()).sandboxReadOnly, true); assert.equal(engines, 0);
  for (const extra of [{requestId: 'caller'}, {requestIds: ['caller']}, {orderId: 'order'}, {destination: DESTINATION}, {force: true}, {upload: true}]) assert.equal((await call({action: 'receiptDiagnostics', ...extra})).status, 400);
  assert.equal(observations, 1); assert.equal(engines, 0);
  assert.equal((await call({action: 'receiptDiagnostics'}, {'X-Growth-Key': 'wrong'})).status, 401);
  assert.equal((await call({action: 'receiptDiagnostics'}, {Origin: 'https://other.example'})).status, 403);
  assert.equal((await call({}, {}, 'GET')).status, 405);
  env.BRITES_GROWTH_NAMESPACE = 'Brites_Growth_Live'; assert.equal((await call({action: 'receiptDiagnostics'})).status, 503);
  env.BRITES_GROWTH_NAMESPACE = 'Brites_Growth_Sandbox'; env.BRITES_GROWTH_SANDBOX = '0'; assert.equal((await call({action: 'receiptDiagnostics'})).status, 503);
  assert.equal(observations, 1); assert.equal(engines, 0);
});
