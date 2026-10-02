'use strict';

// Synthetic queue rows and injected provider reads only. These tests exercise
// the read-only timeout/resume boundary and cannot upload or repair a receipt.
const test = require('node:test');
const assert = require('node:assert/strict');
const {createReceiptObserver, QUEUE, DM_SCOPE} = require('../../netlify/functions/_britesGrowthReceiptObserver');
const {createReceiptReleasePreview} = require('../../netlify/functions/_britesGrowthReceiptReleasePreview');

const DESTINATION = {operatingAccount: {accountType: 'GOOGLE_ADS', accountId: '123'}, productDestinationId: '456'};
const row = index => ({uploaded: false, failed: false, dmState: 'processing', dmRequestId: 'synthetic-receipt-' + index,
  dmDestination: structuredClone(DESTINATION), dmSubmittedAt: Date.now() - (index + 1) * 1000, dmChecks: 0});
const diagnostics = () => ({requestStatusPerDestination: [{destination: structuredClone(DESTINATION), requestStatus: 'SUCCESS',
  eventsIngestionStatus: {recordCount: '1'}}]});
const response = value => ({ok: true, status: 200, json: async () => structuredClone(value)});
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

function storage(rows, writes) {
  const docs = rows.map((value, index) => ({id: 'row-' + index, data: () => structuredClone(value)}));
  const forbidden = () => {writes.push('write'); throw Error('Read-only receipt observation attempted a write');};
  const collection = {
    path: QUEUE,
    doc(id) {return {id, path: QUEUE + '/' + id, kind: 'document'};},
    where(field, operator, value) {
      assert.equal(field, 'uploaded'); assert.equal(operator, '=='); assert.equal(value, false);
      return {limit(maximum) {assert.equal(maximum, 500); return {get: async () => ({docs: docs.slice(0, maximum)})};}};
    },
    add: forbidden
  };
  return {collection(name) {assert.equal(name, QUEUE); return collection;},
    doc(name) {assert.equal(name, 'config/googleAdsDataManager'); return {get: async () => ({exists: false}), set: forbidden, update: forbidden};},
    runTransaction: forbidden, batch: forbidden};
}

function provider({hangIndex = null, statusDelayMs = 0, calls}) {
  return async (raw, init) => {
    const url = new URL(raw); calls.push({url, init});
    if (url.hostname === 'oauth2.googleapis.com') return response({access_token: 'synthetic-access', scope: DM_SCOPE});
    const index = Number(url.searchParams.get('requestId').split('-').at(-1));
    if (index === hangIndex) return new Promise(() => {});
    if (statusDelayMs) await delay(statusDelayMs);
    return response(diagnostics());
  };
}

function observerFixture(options = {}) {
  const rows = Array.from({length: 15}, (_, index) => row(index)), calls = [], writes = [];
  const env = {GADS_DATAMANAGER_REFRESH_TOKEN: 'synthetic-refresh', GADS_CLIENT_ID: 'synthetic-client', GADS_CLIENT_SECRET: 'synthetic-secret'};
  return {api: createReceiptObserver({env, db: storage(rows, writes), fetch: provider({...options, calls})}), calls, writes};
}

test('fifteen bounded receipt reads finish through five ordered cursor workers', async () => {
  const fixture = observerFixture({statusDelayMs: 25}), started = Date.now();
  const result = await fixture.api.read({limit: 15, maxMs: 180});
  assert.equal(result.requested, 15); assert.equal(result.observed, 15); assert.equal(result.confirmed, 15);
  assert.equal(result.batchComplete, true); assert.equal(result.reconciliationEligible, true);
  assert.deepEqual(result.batchProgress, {completed: 15, selected: 15, concurrency: 5});
  assert.equal(result.receipts.length, 15); assert.equal(result.blocked, false); assert.deepEqual(fixture.writes, []);
  assert.ok(Date.now() - started < 450, 'the bounded wave should not regress to fifteen serial waits');
});

test('a fresh fourteen-of-fifteen timeout is explicitly incomplete and cannot feed reconciliation', async () => {
  const fixture = observerFixture({hangIndex: 14, statusDelayMs: 5});
  const result = await fixture.api.read({limit: 15, maxMs: 90});
  assert.equal(result.confirmed, 14); assert.equal(result.unavailable, 1); assert.equal(result.requested, 15);
  assert.equal(result.batchComplete, false); assert.equal(result.reconciliationEligible, false);
  assert.equal(result.blocked, true); assert.equal(result.code, 'INCOMPLETE_RECEIPT_BATCH'); assert.equal(result.stopped, 'time_limit');
  assert.deepEqual(result.batchProgress, {completed: 15, selected: 15, concurrency: 5});
  const hanging = fixture.calls.find(call => call.url.searchParams.get('requestId') === 'synthetic-receipt-14');
  assert.equal(hanging.init.signal.aborted, true); assert.deepEqual(fixture.writes, []);
});

test('trusted release preview emits zero proposals when one read in the selected batch times out', async () => {
  const rows = Array.from({length: 15}, (_, index) => row(index)), calls = [], writes = [], db = storage(rows, writes);
  // A preview needs an exact collection reference, while its observer is still
  // constrained to the same synthetic read-only snapshot.
  const baseCollection = db.collection(QUEUE);
  db.collection = name => {assert.equal(name, QUEUE); return baseCollection;};
  db.runTransaction = () => {throw Error('An incomplete provider batch must not reach the repair recheck');};
  const env = {BRITES_GROWTH_SANDBOX: '1', BRITES_GROWTH_NAMESPACE: 'Brites_Growth_Sandbox',
    GADS_DATAMANAGER_REFRESH_TOKEN: 'synthetic-refresh', GADS_CLIENT_ID: 'synthetic-client', GADS_CLIENT_SECRET: 'synthetic-secret'};
  const result = await createReceiptReleasePreview({env, db, fetch: provider({hangIndex: 14, statusDelayMs: 5, calls})}).read({limit: 15, maxMs: 90});
  assert.equal(result.blocked, true); assert.equal(result.code, 'INCOMPLETE_RECEIPT_BATCH');
  assert.equal(result.proposedRepairs, 0); assert.equal(result.blockedRows, 15);
  assert.equal(result.queueUpdated, false); assert.equal(result.individualOrdersUpdated, 0); assert.deepEqual(writes, []);
});
