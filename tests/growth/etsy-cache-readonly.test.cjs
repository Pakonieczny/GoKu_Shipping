'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { read } = require('../../netlify/functions/_britesGrowthEtsyCacheReadOnly');
const NOW = Date.parse('2026-10-02T09:00:00Z');
const SECRET = 'PRIVATE_VALUE_MUST_NEVER_BE_RETURNED';

function fixture({ states = {}, collections = {}, failures = {}, malformed = {} } = {}) {
  const calls = [];
  let mutations = 0;
  const db = {
    collection(coll) {
      calls.push({ op: 'collection', coll });
      return {
        doc(id) {
          calls.push({ op: 'doc', coll, id });
          return {
            async get() {
              calls.push({ op: 'getState', coll, id });
              if (failures[coll]) throw failures[coll];
              if (malformed[coll]) return malformed[coll];
              const key = coll + '/' + id;
              return { exists: Object.hasOwn(states, key), data: () => states[key] };
            },
            set() { mutations++; throw Error('Unexpected mutation'); },
            update() { mutations++; throw Error('Unexpected mutation'); },
            delete() { mutations++; throw Error('Unexpected mutation'); }
          };
        },
        select(...fields) {
          calls.push({ op: 'select', coll, fields });
          return {
            limit(n) {
              calls.push({ op: 'limit', coll, n });
              return {
                async get() {
                  calls.push({ op: 'getSample', coll });
                  if (failures[coll]) throw failures[coll];
                  if (malformed[coll]) return malformed[coll];
                  return { docs: (collections[coll] || []).slice(0, n).map(d => ({ id: SECRET, data: () => d })) };
                }
              };
            }
          };
        },
        get() { throw Error('Unbounded collection read'); },
        add() { mutations++; throw Error('Unexpected mutation'); }
      };
    },
    batch() { mutations++; throw Error('Unexpected mutation'); },
    runTransaction() { mutations++; throw Error('Unexpected mutation'); }
  };
  return { db, calls, mutations: () => mutations };
}

function completeFixture(overrides = {}) {
  return fixture({
    states: {
      'EtsyMail_ListingsSync/global': { inFlight: false, totalListings: 42, lastIncrementalAt: { toMillis: () => NOW }, lastFullSyncAt: { seconds: NOW / 1000 }, lastError: SECRET, token: SECRET },
      'EtsyMail_Config/receiptsMirrorState': {
        enabled: true, lastSyncTimestamp: NOW / 1000, lastSyncCompletedAt: { _seconds: NOW / 1000 }, lastSyncCallCount: 2,
        lastSyncReceiptsCount: 8, lastSyncOutcome: 'ok', lastSyncErrorMsg: SECRET, access_token: SECRET,
        backfillProgress: { status: 'complete', startedAt: new Date(NOW), completedAt: NOW, totalPagesEstimate: 3, pagesProcessed: 3,
          receiptsProcessed: 8, currentOffset: 8, windowMinCreated: NOW / 1000 - 100, windowMaxCreated: NOW / 1000, errorMsg: SECRET, buyer: SECRET }
      }
    },
    collections: {
      EtsyMail_Listings: [{ listingId: SECRET, title: SECRET, images: [{ url: SECRET }], variations: [], sku: SECRET, lastSyncedAt: NOW }],
      EtsyMail_Receipts: [{ receipt_id: SECRET, buyer_name: SECRET, email: SECRET, raw: { receipt_id: SECRET, name: SECRET, address: SECRET,
        transactions: [{ transaction_id: SECRET, listing_id: SECRET, product_id: SECRET, sku: SECRET, title: SECRET, personalization: SECRET,
          variations: [{ property_id: SECRET, value_id: SECRET, formatted_value: SECRET }] }] }, mirrorWrittenAt: NOW }],
      EtsyPricing_Listings: [{ original_saved: true, original_snapshot_hash: SECRET, chain_type: 'beady', chain_set: true,
        engraving: false, engrave_set: true, updated_at: NOW, original_inventory: { products: [{ product_id: SECRET, sku: SECRET,
          property_values: [{ property_id: SECRET, values: [SECRET] }], offerings: [{ offering_id: SECRET, price: { amount: 999 } }] }] } }]
    },
    ...overrides
  });
}

test('fixed read targets use two exact state reads and three one-document projected samples', async () => {
  const f = completeFixture();
  const result = await read({ db: f.db });
  assert.equal(result.mode, 'cache_only_read_only');
  assert.equal(result.status, 'observed');
  assert.deepEqual(f.calls.filter(c => c.op === 'doc'), [
    { op: 'doc', coll: 'EtsyMail_ListingsSync', id: 'global' },
    { op: 'doc', coll: 'EtsyMail_Config', id: 'receiptsMirrorState' }
  ]);
  assert.deepEqual(f.calls.filter(c => c.op === 'limit').map(c => [c.coll, c.n]), [
    ['EtsyMail_Listings', 1], ['EtsyMail_Receipts', 1], ['EtsyPricing_Listings', 1]
  ]);
  assert.equal(f.calls.filter(c => c.op === 'getState' || c.op === 'getSample').length, 5);
  assert.equal(f.mutations(), 0);
  assert.equal(result.identityBindingPerformed, false);
  assert.equal(result.bounds.readAll, false);
});

test('raw records, credentials, identity values, contact details and source document IDs never escape', async () => {
  const result = await read({ db: completeFixture().db });
  assert.ok(!JSON.stringify(result).includes(SECRET));
  const listing = result.sources.listingSample.fieldShape;
  const receipt = result.sources.receiptSample.fieldShape;
  const pricing = result.sources.pricingSample.fieldShape;
  assert.equal(listing.hasListingIdentifier, true);
  assert.equal(listing.variationsArrayLength, 0);
  assert.equal(receipt.hasNonemptySkuInInspectedTransactions, true);
  assert.equal(receipt.hasVariationPropertyIdInInspectedTransactions, true);
  assert.equal(receipt.hasVariationValueIdInInspectedTransactions, true);
  assert.equal(pricing.originalProducts.hasOfferingIdInInspectedProducts, true);
  assert.equal(result.sources.listingsSync.metadata.errorPresent, true);
});

test('receipt projection excludes raw receipt body, customer fields and authentication collections', async () => {
  const f = completeFixture();
  await read({ db: f.db, collection: 'config', id: 'etsyOauth' });
  const fields = f.calls.find(c => c.op === 'select' && c.coll === 'EtsyMail_Receipts').fields;
  assert.deepEqual(fields, ['receipt_id', 'receiptId', 'raw.receipt_id', 'raw.transactions', 'raw.receipt.transactions', 'transactions', 'mirrorWrittenAt']);
  assert.ok(!fields.includes('raw'));
  assert.ok(!f.calls.some(c => c.coll === 'config' || c.id === 'etsyOauth'));
});

test('missing state is explicitly absent and empty samples have only a verified sample zero', async () => {
  const result = await read({ db: fixture().db });
  assert.equal(result.sources.listingsSync.status, 'absent');
  assert.equal(result.sources.listingsSync.exists, false);
  assert.equal(result.sources.listingsSync.metadata, null);
  assert.equal(result.sources.receiptSample.status, 'empty');
  assert.equal(result.sources.receiptSample.sampleCount, 0);
  assert.equal(result.sources.receiptSample.totalCount, null);
  assert.equal(result.sources.receiptSample.fieldShape, null);
});

test('read failures preserve unknown counts and redact exception contents while independent sources continue', async () => {
  const error = Object.assign(new Error(SECRET), { code: 'permission-denied', details: SECRET });
  const f = completeFixture({ failures: { EtsyMail_Receipts: error } });
  const result = await read({ db: f.db });
  assert.equal(result.status, 'partial_or_unavailable');
  assert.equal(result.sources.receiptSample.status, 'error');
  assert.equal(result.sources.receiptSample.sampleCount, null);
  assert.equal(result.sources.receiptSample.totalCount, null);
  assert.deepEqual(result.sources.receiptSample.error, { code: 'permission-denied' });
  assert.equal(result.sources.listingSample.status, 'observed');
  assert.equal(result.sources.pricingSample.status, 'observed');
  assert.ok(!JSON.stringify(result).includes(SECRET));
});

test('unrecognized exception codes are not echoed and a missing db is explicitly unavailable', async () => {
  const result = await read({ db: fixture({ failures: { EtsyMail_Config: { code: SECRET, message: SECRET } } }).db });
  assert.deepEqual(result.sources.receiptsMirrorState.error, { code: 'cache_read_failed' });
  const missing = await read();
  assert.equal(missing.status, 'partial_or_unavailable');
  assert.ok(Object.values(missing.sources).every(s => s.status === 'error'));
  assert.ok(Object.values(missing.sources).every(s => s.exists == null && s.sampleCount == null));
  assert.ok(!JSON.stringify(result).includes(SECRET));
});

test('prototype-key exception codes cannot escape the safe error-code whitelist', async () => {
  for (const code of ['__proto__', 'constructor', 'toString', { value: SECRET }]) {
    const r = await read({ db: fixture({ failures: { EtsyMail_Receipts: { code, message: SECRET } } }).db });
    assert.deepEqual(r.sources.receiptSample.error, { code: 'cache_read_failed' });
    assert.ok(!JSON.stringify(r).includes(SECRET));
  }
  const numeric = await read({ db: fixture({ failures: { EtsyMail_Receipts: { code: 7, message: SECRET } } }).db });
  assert.deepEqual(numeric.sources.receiptSample.error, { code: 'permission-denied' });
});

test('missing error metadata remains unknown while explicit null or empty errors are false', async () => {
  for (const [state, expected] of [[{}, null], [{ lastError: null }, false], [{ lastError: '' }, false], [{ lastError: { message: SECRET } }, true]]) {
    const r = await read({ db: fixture({ states: { 'EtsyMail_ListingsSync/global': state } }).db });
    assert.equal(r.sources.listingsSync.metadata.errorPresent, expected);
    assert.ok(!JSON.stringify(r).includes(SECRET));
  }
});

test('stored zero counts remain zero but missing, negative or string counts remain unknown', async () => {
  const f = fixture({ states: {
    'EtsyMail_ListingsSync/global': { totalListings: '42', inFlight: 'false' },
    'EtsyMail_Config/receiptsMirrorState': { lastSyncCallCount: 0, lastSyncReceiptsCount: -1,
      backfillProgress: { pagesProcessed: 0, receiptsProcessed: '8', currentOffset: NaN } }
  } });
  const r = await read({ db: f.db });
  assert.equal(r.sources.listingsSync.metadata.storedTotalListings, null);
  assert.equal(r.sources.listingsSync.metadata.inFlight, null);
  assert.equal(r.sources.receiptsMirrorState.metadata.lastSyncCallCount, 0);
  assert.equal(r.sources.receiptsMirrorState.metadata.lastSyncReceiptsCount, null);
  assert.equal(r.sources.receiptsMirrorState.metadata.backfill.pagesProcessed, 0);
  assert.equal(r.sources.receiptsMirrorState.metadata.backfill.receiptsProcessed, null);
  assert.equal(r.sources.receiptsMirrorState.metadata.backfill.currentOffset, null);
});

test('only whitelisted state enums are returned; arbitrary strings are replaced', async () => {
  const f = fixture({ states: { 'EtsyMail_Config/receiptsMirrorState': { lastSyncOutcome: SECRET, backfillProgress: { status: SECRET } } },
    collections: { EtsyPricing_Listings: [{ chain_type: SECRET, original_snapshot_hash: SECRET }] } });
  const r = await read({ db: f.db });
  assert.equal(r.sources.receiptsMirrorState.metadata.lastSyncOutcome, 'unrecognized');
  assert.equal(r.sources.receiptsMirrorState.metadata.backfill.status, 'unrecognized');
  assert.equal(r.sources.pricingSample.fieldShape.chainType, 'unrecognized');
  assert.ok(!JSON.stringify(r).includes(SECRET));
});

test('verified state timestamps are normalized and malformed or secret timestamps are null', async () => {
  const r = await read({ db: completeFixture().db });
  assert.equal(r.sources.listingsSync.metadata.lastIncrementalAt, NOW);
  assert.equal(r.sources.listingsSync.metadata.lastFullSyncAt, NOW);
  assert.equal(r.sources.receiptsMirrorState.metadata.lastSyncCompletedAt, NOW);
  assert.equal(r.sources.receiptsMirrorState.metadata.backfill.startedAt, NOW);
  const f = fixture({ states: {
    'EtsyMail_ListingsSync/global': { lastIncrementalAt: { toMillis: () => SECRET }, lastFullSyncAt: Infinity },
    'EtsyMail_Config/receiptsMirrorState': { lastSyncTimestamp: SECRET, lastSyncCompletedAt: { toMillis: () => { throw Error(SECRET); } },
      backfillProgress: { startedAt: { seconds: 1, nanoseconds: -2 }, completedAt: -1, windowMinCreated: Infinity } }
  } });
  const bad = await read({ db: f.db });
  assert.equal(bad.sources.listingsSync.metadata.lastIncrementalAt, null);
  assert.equal(bad.sources.listingsSync.metadata.lastFullSyncAt, null);
  assert.equal(bad.sources.receiptsMirrorState.metadata.lastSyncCompletedAt, null);
  assert.equal(bad.sources.receiptsMirrorState.metadata.lastSyncTimestamp, null);
  assert.equal(bad.sources.receiptsMirrorState.metadata.backfill.startedAt, null);
  assert.ok(!JSON.stringify(bad).includes(SECRET));
});

test('raw, top-level and wrapped receipt transaction arrays are recognized without returning their values', async () => {
  for (const d of [
    { raw: { transactions: [{ sku: SECRET, listing_id: SECRET }] } },
    { transactions: [{ sku: SECRET, listing_id: SECRET }] },
    { raw: { receipt: { transactions: [{ sku: SECRET, listing_id: SECRET }] } } }
  ]) {
    const r = await read({ db: fixture({ collections: { EtsyMail_Receipts: [d] } }).db });
    assert.equal(r.sources.receiptSample.fieldShape.hasTransactionsArray, true);
    assert.equal(r.sources.receiptSample.fieldShape.hasNonemptySkuInInspectedTransactions, true);
    assert.ok(!JSON.stringify(r).includes(SECRET));
  }
});

test('an empty older transaction path does not hide a populated supported path', async () => {
  const r = await read({ db: fixture({ collections: { EtsyMail_Receipts: [{ raw: { transactions: [] }, transactions: [{ sku: SECRET, listing_id: SECRET }] }] } }).db });
  assert.equal(r.sources.receiptSample.fieldShape.transactionsArrayLength, 1);
  assert.equal(r.sources.receiptSample.fieldShape.hasNonemptySkuInInspectedTransactions, true);
});

test('field presence is separate from empty and malformed transaction arrays', async () => {
  for (const [value, isArray, length] of [[[], true, 0], [SECRET, false, null], [null, false, null]]) {
    const r = await read({ db: fixture({ collections: { EtsyMail_Receipts: [{ raw: { transactions: value } }] } }).db });
    assert.equal(r.sources.receiptSample.fieldShape.hasTransactionsField, true);
    assert.equal(r.sources.receiptSample.fieldShape.hasTransactionsArray, isArray);
    assert.equal(r.sources.receiptSample.fieldShape.transactionsArrayLength, length);
    assert.equal(r.sources.receiptSample.fieldShape.hasNonemptySkuInInspectedTransactions, false);
  }
});

test('array inspection is bounded and absence is labelled for the inspected records only', async () => {
  const transactions = Array.from({ length: 10 }, () => ({})).concat([{ sku: SECRET, variations: [{ property_id: SECRET }] }]);
  const products = Array.from({ length: 10 }, () => ({})).concat([{ sku: SECRET, product_id: SECRET }]);
  const r = await read({ db: fixture({ collections: {
    EtsyMail_Receipts: [{ raw: { transactions } }], EtsyPricing_Listings: [{ original_inventory: { products } }]
  } }).db });
  assert.equal(r.sources.receiptSample.fieldShape.transactionsArrayLength, 11);
  assert.equal(r.sources.receiptSample.fieldShape.inspectedTransactions, 10);
  assert.equal(r.sources.receiptSample.fieldShape.hasNonemptySkuInInspectedTransactions, false);
  assert.equal(r.sources.pricingSample.fieldShape.originalProducts.inspectedObjects, 10);
  assert.equal(r.sources.pricingSample.fieldShape.originalProducts.hasNonemptySkuInInspectedProducts, false);
});

test('additional documents are ignored even if a malformed adapter returns more than the requested limit', async () => {
  const r = await read({ db: fixture({ malformed: { EtsyMail_Receipts: { docs: [
    { data: () => ({ raw: { transactions: [] } }) },
    { data: () => { throw Error('Second document must not be inspected'); } }
  ] } } }).db });
  assert.equal(r.sources.receiptSample.sampleCount, 1);
  assert.equal(r.sources.receiptSample.fieldShape.transactionsArrayLength, 0);
});

test('malformed SDK snapshots become explicit errors rather than misleading empty counts', async () => {
  const f = fixture({ malformed: { EtsyMail_Receipts: { docs: null }, EtsyMail_ListingsSync: { exists: true, data: () => SECRET } } });
  const r = await read({ db: f.db });
  assert.equal(r.sources.receiptSample.sampleCount, null);
  assert.deepEqual(r.sources.receiptSample.error, { code: 'cache_response_invalid' });
  assert.equal(r.sources.listingsSync.exists, null);
  assert.deepEqual(r.sources.listingsSync.error, { code: 'cache_response_invalid' });
});

test('no provider fetch, database write or process credential read is required', async () => {
  const original = global.fetch;
  let fetches = 0;
  global.fetch = async () => { fetches++; throw Error('Provider fetch forbidden'); };
  try {
    const f = completeFixture();
    const r = await read({ db: f.db });
    assert.equal(r.status, 'observed');
    assert.equal(fetches, 0);
    assert.equal(f.mutations(), 0);
  } finally { global.fetch = original; }
});
