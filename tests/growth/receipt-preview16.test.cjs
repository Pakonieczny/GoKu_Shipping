'use strict';

// Synthetic diagnostics only. No live account, order, receipt, connection,
// storage mutation, conversion submission, or external fetch is used.
const assert = require('node:assert/strict');
const test = require('node:test');
const {createReceiptReleasePreview} = require('../../netlify/functions/_britesGrowthReceiptReleasePreview');
const {summarizeDiagnostics} = require('../../netlify/functions/googleAdsDataManager');
const epoch = Date.parse('2026-10-02T19:00:00Z');
const queue = 'Brites_GAds_ConvQueue';
const target = {operatingAccount: {accountType: 'GOOGLE_ADS', accountId: '123'}, productDestinationId: '456'};
const clone = value => structuredClone(value);
const answer = patch => ({requestStatusPerDestination: [{destination: clone(target), requestStatus: 'SUCCESS',
  eventsIngestionStatus: {recordCount: '1'}, ...patch}]});

async function preview(diagnostics) {
  const row = {uploaded: false, failed: false, dmState: 'processing', dmRequestId: 'synthetic-receipt',
    dmDestination: clone(target), dmSubmittedAt: epoch - 1000, dmChecks: 0};
  let writes = 0, statusReads = 0;
  const document = {id: 'synthetic-row', path: queue + '/synthetic-row'};
  const saved = () => ({id: document.id, exists: true, data: () => clone(row)});
  const collection = {path: queue, doc: () => document, where: () => collection, limit: () => collection,
    get: async () => ({docs: [saved()]})};
  const forbidden = () => {writes++; throw Error('Read-only preview cannot write');};
  const db = {collection: name => {assert.equal(name, queue); return collection;}, doc: () => ({get: async () => ({exists: false})}),
    runTransaction: async (callback, config) => {
      assert.deepEqual(config, {readOnly: true});
      return callback({get: async ref => ref === document ? saved() : {docs: [saved()]},
        update: forbidden, set: forbidden, create: forbidden, delete: forbidden});
    }};
  const env = {BRITES_GROWTH_SANDBOX: '1', BRITES_GROWTH_NAMESPACE: 'Brites_Growth_Sandbox',
    GADS_DATAMANAGER_REFRESH_TOKEN: 'synthetic-refresh', GADS_CLIENT_ID: 'synthetic-client', GADS_CLIENT_SECRET: 'synthetic-secret'};
  const fetch = async (url, options) => {
    let body;
    if (url === 'https://oauth2.googleapis.com/token') {
      assert.equal(options.method, 'POST');
      body = {access_token: 'synthetic-token', scope: 'https://www.googleapis.com/auth/datamanager'};
    } else {
      assert.match(url, /^https:\/\/datamanager\.googleapis\.com\/v1\/requestStatus:retrieve\?requestId=/);
      assert.equal(options.method, 'GET'); assert.equal(options.body, undefined); statusReads++;
      body = diagnostics;
    }
    return {ok: true, status: 200, json: async () => clone(body)};
  };
  const result = await createReceiptReleasePreview({env, db, fetch, now: () => epoch}).read();
  assert.equal(writes, 0); assert.equal(statusReads, 1);
  assert.equal(result.queueUpdated, false); assert.equal(result.eventsIngested, 0);
  return result;
}

for (const [name, value] of [
  ['boolean', true], ['array', [1]], ['object', {}], ['leading-zero string', '01'],
  ['fractional string', '1.0'], ['exponent string', '1e0'], ['hex string', '0x1'], ['whitespace string', ' 1 ']
]) test('preview refuses a coercible but noncanonical event count: ' + name, async () => {
  const result = await preview(answer({eventsIngestionStatus: {recordCount: value}}));
  assert.equal(result.proposedRepairs, 0); assert.equal(result.providerReceiptsConfirmed, 0);
  assert.equal(result.rows[0].code, 'ONE_RECORD_REQUIRED');
});

for (const field of ['audienceMembersIngestionStatus', 'audienceMembersRemovalStatus', 'removeAllAudienceMembersStatus']) {
  test('preview refuses a mixed event/audience diagnostics kind: ' + field, async () => {
    const result = await preview(answer({[field]: {recordCount: '1'}}));
    assert.equal(result.proposedRepairs, 0); assert.equal(result.providerReceiptsConfirmed, 0);
    assert.equal(result.rows[0].code, 'UNSUPPORTED_RECEIPT_STATUS_KIND');
  });
}

test('preview refuses contradictory current and deprecated provider account types', async () => {
  const destination = clone(target); destination.operatingAccount.product = 'DISPLAY_VIDEO';
  const result = await preview(answer({destination}));
  assert.equal(result.proposedRepairs, 0); assert.equal(result.providerReceiptsConfirmed, 0);
  assert.equal(result.rows[0].code, 'PROVIDER_DESTINATION_UNCONFIRMED');
});

test('preview refuses a second different destination instead of isolating a convenient success', async () => {
  const diagnostics = answer();
  const other = answer().requestStatusPerDestination[0]; other.destination.productDestinationId = '789';
  diagnostics.requestStatusPerDestination.push(other);
  const result = await preview(diagnostics);
  assert.equal(result.proposedRepairs, 0); assert.equal(result.providerReceiptsConfirmed, 0);
  assert.equal(result.rows[0].code, 'SINGLE_DESTINATION_REQUIRED');
});

test('exact one-record number and string remain valid read-only provider receipts', async () => {
  for (const recordCount of [1, '1']) {
    const result = await preview(answer({eventsIngestionStatus: {recordCount}}));
    assert.equal(result.proposedRepairs, 1); assert.equal(result.providerReceiptsConfirmed, 1);
    assert.equal(result.rows[0].individualOrderConfirmed, false);
    assert.equal(result.individualOrderAttributionConfirmed, false);
  }
});

for (const [name, value] of [
  ['boolean', true], ['array', [1]], ['object', {}], ['leading-zero string', '01'],
  ['fractional string', '1.0'], ['exponent string', '1e0'], ['hex string', '0x1'], ['whitespace string', ' 1 ']
]) test('receipt parser cannot confirm a coercible event count: ' + name, () => {
  const result = summarizeDiagnostics(answer({eventsIngestionStatus: {recordCount: value}}), target);
  assert.equal(result.state, 'processing');
});

for (const field of ['audienceMembersIngestionStatus', 'audienceMembersRemovalStatus', 'removeAllAudienceMembersStatus']) {
  test('receipt parser cannot confirm an event mixed with ' + field, () => {
    const result = summarizeDiagnostics(answer({[field]: {recordCount: '1'}}), target);
    assert.equal(result.state, 'processing'); assert.equal(result.status, 'RECEIPT_STATUS_KIND_UNCONFIRMED');
  });
}

test('receipt parser rejects contradictory provider or original account types', () => {
  const conflicting = clone(target); conflicting.operatingAccount.product = 'DISPLAY_VIDEO';
  assert.equal(summarizeDiagnostics(answer({destination: conflicting}), target).state, 'processing');
  assert.equal(summarizeDiagnostics(answer(), conflicting).state, 'processing');
});

test('receipt parser rejects coerced provider IDs and preserves exact string identity', () => {
  for (const accountId of [123, ['123'], '0123']) {
    const destination = clone(target); destination.operatingAccount.accountId = accountId;
    assert.equal(summarizeDiagnostics(answer({destination}), target).state, 'processing');
  }
  for (const productDestinationId of [456, ['456'], '0456']) {
    const destination = {...clone(target), productDestinationId};
    assert.equal(summarizeDiagnostics(answer({destination}), target).state, 'processing');
  }
});

test('receipt parser preserves legitimate multi-destination responses and destination metadata', () => {
  const destination = clone(target);
  destination.reference = 'synthetic-reference';
  destination.loginAccount = {accountType: 'GOOGLE_ADS', accountId: '999'};
  destination.linkedAccount = {accountType: 'DATA_PARTNER', accountId: '888'};
  const diagnostics = answer({destination});
  const other = answer().requestStatusPerDestination[0]; other.destination.productDestinationId = '789';
  diagnostics.requestStatusPerDestination.push(other);
  assert.equal(summarizeDiagnostics(diagnostics, target).state, 'success');
});

test('receipt parser preserves deprecated type compatibility and exact numeric/string one', () => {
  for (const recordCount of [1, '1']) {
    const destination = clone(target); destination.operatingAccount.product = 'GOOGLE_ADS';
    delete destination.operatingAccount.accountType;
    assert.equal(summarizeDiagnostics(answer({destination, eventsIngestionStatus: {recordCount}}), target).state, 'success');
    destination.operatingAccount.accountType = 'GOOGLE_ADS';
    assert.equal(summarizeDiagnostics(answer({destination, eventsIngestionStatus: {recordCount}}), target).state, 'success');
  }
});
