'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { Firestore, FieldPath } = require('@google-cloud/firestore');
const { createHistoricalLookup, LIMITS } = require('../../netlify/functions/_britesGrowthHistoricalLookup');
const NOW = Date.parse('2026-10-02T09:00:00Z');
const KEY = 'fixture-cursor-key-long-enough';
const PRIVATE = 'PRIVATE_NAME_CONTACT_ADDRESS_ENGRAVING';
const QUEUE = 'Brites_Growth_Sandbox_Queue';
const receiptOptions = extra => ({ method: 'receiptIdentityPage', fromSec: 100, untilSec: 1000, ...extra });
const path = value => value instanceof FieldPath ? value.segments : String(value).split('.');
function at(data, field, docId) {
  const parts = path(field);
  return parts.length === 1 && parts[0] === '__name__' ? docId : parts.reduce((v, k) => v?.[k], data);
}
function projected(data, fields) {
  const out = {};
  for (const field of fields) {
    const parts = path(field), value = at(data, field);
    if (value === undefined) continue;
    let current = out;
    for (const part of parts.slice(0, -1)) current = current[part] ||= {};
    current[parts.at(-1)] = structuredClone(value);
  }
  return out;
}
function fixture({ seeds = [{ rank: 1, sku: 'TEST', status: 'pending_match' }], collections = {}, failures = {}, delayMs = {}, malformed = {} } = {}) {
  const data = { [QUEUE]: Object.fromEntries(seeds.map((s, i) => ['rank-' + i, s])), ...collections };
  const calls = [];
  let writes = 0;
  const forbidden = () => { writes++; throw Error('Unexpected write'); };
  const pause = async coll => {
    if (delayMs[coll]) await new Promise(resolve => setTimeout(resolve, delayMs[coll]));
    if (failures[coll]) throw failures[coll];
  };
  class Query {
    constructor(coll, state = { filters: [], orders: [], fields: null, limit: null, start: null }) { this.coll = coll; this.state = state; }
    next(change) { return new Query(this.coll, { ...this.state, ...change }); }
    where(field, op, value) { return this.next({ filters: [...this.state.filters, { field, op, value }] }); }
    orderBy(field, direction = 'asc') { return this.next({ orders: [...this.state.orders, { field, direction }] }); }
    select(...fields) { return this.next({ fields }); }
    limit(limit) { return this.next({ limit }); }
    startAfter(...start) { return this.next({ start }); }
    doc(id) {
      return { coll: this.coll, id, set: forbidden, update: forbidden, delete: forbidden,
        get() { throw Error('Unprojected document get'); } };
    }
    async get() {
      assert.ok(this.state.fields, 'Every query has an explicit projection');
      assert.ok(Number.isInteger(this.state.limit) && this.state.limit <= 100, 'Every query is bounded');
      calls.push({ op: 'query', coll: this.coll, ...this.state });
      await pause(this.coll);
      if (malformed[this.coll]) return malformed[this.coll];
      const compare = (a, b) => {
        for (const order of this.state.orders) {
          const av = at(a[1], order.field, a[0]), bv = at(b[1], order.field, b[0]);
          if (av !== bv) return (av > bv ? 1 : -1) * (order.direction === 'desc' ? -1 : 1);
        }
        return a[0].localeCompare(b[0]);
      };
      let rows = Object.entries(data[this.coll] || {}).filter(([docId, d]) => this.state.filters.every(f => {
        const v = at(d, f.field, docId);
        if (v === undefined) return false;
        return f.op === 'in' ? f.value.includes(v) : f.op === '>' ? v > f.value : f.op === '>=' ? v >= f.value : f.op === '<=' ? v <= f.value : f.op === '==' ? v === f.value : false;
      })).filter(([docId, d]) => this.state.orders.every(o => at(d, o.field, docId) !== undefined));
      rows.sort(compare);
      if (this.state.start) {
        const cursorRow = [{}, {}];
        for (let i = 0; i < this.state.orders.length; i++) {
          const parts = path(this.state.orders[i].field);
          if (parts[0] === '__name__') cursorRow[0] = this.state.start[i];
          else cursorRow[1][parts[0]] = this.state.start[i];
        }
        rows = rows.filter(r => compare(r, cursorRow) > 0);
      }
      return { docs: rows.slice(0, this.state.limit).map(([id, d]) => ({ id, data: () => projected(d, this.state.fields) })) };
    }
    add = forbidden;
    stream() { throw Error('Unbounded stream'); }
    offset() { throw Error('Offset pagination'); }
  }
  const db = {
    collection(coll) { return new Query(coll); },
    async getAll(...args) {
      const { fieldMask } = args.pop();
      assert.ok(Array.isArray(fieldMask) && fieldMask.length, 'getAll must project fields');
      assert.ok(args.length <= LIMITS.listingIds, 'Exact document reads are bounded');
      assert.ok(args.every(ref => ref.coll === args[0].coll));
      calls.push({ op: 'getAll', coll: args[0].coll, ids: args.map(r => r.id), fields: fieldMask });
      await pause(args[0].coll);
      if (malformed[args[0].coll]) return malformed[args[0].coll];
      return args.map(ref => ({ id: ref.id, exists: Object.hasOwn(data[ref.coll] || {}, ref.id),
        data: () => projected(data[ref.coll]?.[ref.id], fieldMask) }));
    },
    batch: forbidden, runTransaction: forbidden, recursiveDelete: forbidden
  };
  return { db, calls, data, writes: () => writes,
    helper: options => createHistoricalLookup({ db, cursorSecret: KEY, now: () => NOW, ...options }) };
}
function tx(extra = {}) { return { transaction_id: 810000001, listing_id: 10000001, product_id: 20000001, sku: 'TEST', title: 'Fixture Charm', variations: [], ...extra }; }
function receipt(transactions = [tx()], timestamp = 500) {
  return { created_timestamp: timestamp, receipt_id: 910000001, buyer_name: PRIVATE, email: PRIVATE,
    raw: { address: PRIVATE, buyer_user_id: PRIVATE, transactions } };
}
function receiptFixture(transactions, extra = {}) {
  return fixture({ collections: { EtsyMail_Receipts: { '910000001': receipt(transactions) } }, ...extra });
}
function assertPrivate(result) {
  const json = JSON.stringify(result);
  for (const value of [PRIVATE, KEY, '910000001', '810000001']) assert.ok(!json.includes(value), 'Private value must be omitted or encrypted');
  assert.equal(result.customerDataReturned, false);
  assert.equal(result.identityBindingPerformed, false);
  assert.equal(result.originalRanksCountsChanged, false);
  assert.equal(result.providerCalls, 0);
  assert.equal(result.storeWrites, 0);
}

test('caller cannot supply a collection, SKU, provider, identity mutation or arbitrary method', async () => {
  for (const options of [{ collection: 'config' }, { sku: PRIVATE }, { method: PRIVATE }, { method: { token: PRIVATE } }, { method: 'match' }, { listingIds: ['10000001'] }, null, []]) {
    const f = fixture(), result = await f.helper().read(options);
    assert.equal(result.status, 'unavailable');
    assert.equal(result.code, 'INVALID_OPTIONS');
    assert.equal(f.calls.length, 0);
    assert.ok(!JSON.stringify(result).includes(PRIVATE));
  }
});

test('sandbox namespace is mandatory and never opens a caller-selected collection', async () => {
  const f = fixture(), result = await f.helper({ namespace: 'Brites_Growth_Live' }).read();
  assert.equal(result.code, 'SANDBOX_REQUIRED');
  assert.equal(f.calls.length, 0);
});

test('seed projection excludes titles, orders, holds and leases, and retains original unresolved ranks', async () => {
  const f = fixture({ seeds: [{ rank: 1, sku: 'Test Sku', title: PRIVATE, orders: 9, leaseToken: PRIVATE },
    { rank: 2, sku: 'SECOND; SECOND-ENG', status: 'pending_match' }, { rank: 3, sku: 'DONE', productId: 'gid://shopify/Product/999' },
    { rank: 4, sku: 'ALSO_DONE', status: 'complete' }] });
  const result = await f.helper().read();
  assert.deepEqual(result.unresolvedRanks, [1, 2]);
  assert.deepEqual(f.calls[0].fields, ['rank', 'sku', 'productId', 'status']);
  assert.equal(f.calls[0].limit, 100);
  assert.deepEqual(f.calls[0].filters, [{ field: 'rank', op: '<=', value: 100 }]);
  assert.ok(!JSON.stringify(result).includes(PRIVATE));
  assert.equal(f.writes(), 0);
});

test('a rank subset may include only stored unresolved rows', async () => {
  const f = fixture({ seeds: [{ rank: 1, sku: 'TEST' }, { rank: 2, sku: 'OTHER' }] });
  const result = await f.helper().read({ ranks: [2] });
  assert.deepEqual(result.unresolvedRanks, [2]);
  assert.deepEqual(result.queries.filter(q => q.token).map(q => q.token), ['OTHER']);
  for (const ranks of [[], [3], ['1'], [0]]) assert.equal((await f.helper().read({ ranks })).code, 'RANK_NOT_UNRESOLVED');
});

test('empty unresolved seed set stops without cache reads or making historical absence claims', async () => {
  const f = fixture({ seeds: [{ rank: 1, sku: 'DONE', status: 'complete' }] });
  const result = await f.helper().read(receiptOptions());
  assert.equal(result.status, 'no_unresolved_seeds');
  assert.equal(result.windowExhausted, null);
  assert.equal(f.calls.length, 1);
});

test('invalid, duplicate or overlarge stored seeds fail closed without printing their contents', async () => {
  const cases = [
    [{ rank: 1, sku: 'bad@value' }], [{ rank: 1, sku: 'TEST' }, { rank: 1, sku: 'OTHER' }],
    [{ rank: 1, sku: null }], [{ rank: 1, sku: 'A'.repeat(501) }],
    Array.from({ length: 13 }, (_, i) => ({ rank: i + 1, sku: 'T' + i })),
    [{ rank: 1, sku: Array.from({ length: 13 }, (_, i) => 'T' + i).join(',') }]
  ];
  for (const seeds of cases) {
    const f = fixture({ seeds }), result = await f.helper().read();
    assert.ok(['SEEDS_INVALID', 'SEEDS_TOO_MANY'].includes(result.code));
    assert.equal(f.calls.length, 1);
    assert.equal(result.transactionsInspected, null);
  }
});

test('alias lookups are bounded candidate evidence with SKU and huggie mapping kinds preserved', async () => {
  const f = fixture({ seeds: [{ rank: 1, sku: 'CHARM' }, { rank: 2, sku: 'HUGGIE' }], collections: {
    Charm_Sku_Aliases: { '10000001': { listingId: '10000001', sku: 'CHARM', huggie: 'HUGGIE', v: 2, by: PRIVATE } }
  } });
  const result = await f.helper().read();
  assert.deepEqual(result.candidates.map(c => [c.lookupKind, c.ranks]), [['alias_sku', [1]], ['alias_huggie', [2]]]);
  assert.ok(result.candidates.every(c => c.aliasVersion2 && !c.originalCaseVerified && !c.historicalPurchaseVerified));
  assert.ok(result.queries.every(q => q.sampleCount !== null && q.status === 'observed'));
  assert.equal(result.candidateEvidenceOnly, true);
  assertPrivate(result);
});

test('literal bySku keys use real SDK FieldPath in filter, ordering and projection', async () => {
  const f = fixture({ seeds: [{ rank: 1, sku: 'TEST.1' }], collections: {
    Charm_Sku_Aliases: { '10000001': { listingId: '10000001', bySku: { 'TEST.1': 'NEW_TEST', TEST: { 1: 'WRONG' }, SECRET: PRIVATE } } }
  } });
  const result = await f.helper().read(), query = f.calls.find(c => c.filters.some(f => f.field instanceof FieldPath));
  assert.deepEqual(query.filters[0].field.segments, ['bySku', 'TEST.1']);
  assert.equal(query.filters[0].op, '>');
  assert.equal(query.filters[0].value, '');
  assert.equal(query.orders[0].field, query.filters[0].field);
  assert.equal(query.fields[1], query.filters[0].field);
  assert.equal(result.candidates[0].listingId, '10000001');
  assert.ok(!JSON.stringify(result).includes('NEW_TEST'));
  assert.ok(!JSON.stringify(result).includes(PRIVATE));
});

test('actual SDK compiles literal field paths and descending document-ID cursor without network reads', () => {
  const firestore = new Firestore({ projectId: 'offline-fixture' }), literal = new FieldPath('bySku', 'TEST.1');
  const alias = firestore.collection('Charm_Sku_Aliases').where(literal, '>', '').orderBy(literal).select('listingId', literal).limit(10).toStructuredQuery();
  assert.equal(alias.where.fieldFilter.field.fieldPath, 'bySku.`TEST.1`');
  assert.equal(alias.where.fieldFilter.op, 'GREATER_THAN');
  assert.equal(alias.select.fields[1].fieldPath, 'bySku.`TEST.1`');
  const receiptQuery = firestore.collection('EtsyMail_Receipts').where('created_timestamp', '>=', 100).where('created_timestamp', '<=', 1000)
    .orderBy('created_timestamp', 'desc').orderBy(FieldPath.documentId(), 'desc').select('created_timestamp', 'raw.transactions').limit(100).startAfter(500, '910000001').toStructuredQuery();
  assert.equal(receiptQuery.orderBy[1].field.fieldPath, '__name__');
  assert.equal(receiptQuery.startAt.values[1].referenceValue, 'projects/offline-fixture/databases/(default)/documents/EtsyMail_Receipts/910000001');
  assert.equal(receiptQuery.offset, undefined);
});

test('alias ID mismatch and invalid leaves are reported without binding or returning unrelated aliases', async () => {
  const f = fixture({ collections: { Charm_Sku_Aliases: {
    '10000001': { listingId: '10000002', sku: 'TEST' },
    '10000003': { listingId: '10000003', bySku: { TEST: PRIVATE + '@' } }
  } } });
  const result = await f.helper().read();
  assert.deepEqual(result.candidates, []);
  assert.ok(result.queries.some(q => q.inconsistentRows === 1));
  assert.ok(!JSON.stringify(result).includes(PRIVATE));
});

test('alias result caps are explicit, and independent lookups continue after an error', async () => {
  const f = fixture({ collections: { Charm_Sku_Aliases: Object.fromEntries(Array.from({ length: 12 }, (_, i) => [String(10000001 + i), { listingId: String(10000001 + i), sku: 'TEST' }])) } });
  const result = await f.helper().read();
  assert.equal(result.queries[0].sampleCount, 10);
  assert.equal(result.queries[0].possiblyCapped, true);
  const e = fixture({ failures: { Charm_Sku_Aliases: { code: 7, message: PRIVATE } } });
  const failed = await e.helper().read();
  assert.equal(failed.status, 'partial_or_unavailable');
  assert.ok(failed.queries.every(q => q.sampleCount === null && q.error === 'CACHE_ACCESS_DENIED'));
  assert.ok(!JSON.stringify(failed).includes(PRIVATE));
});

test('many alias candidates respect distinct listing-ID limit and do not choose a winner', async () => {
  const seeds = Array.from({ length: 4 }, (_, i) => ({ rank: i + 1, sku: 'T' + i }));
  const aliases = Object.fromEntries(Array.from({ length: 40 }, (_, i) => [String(10000001 + i), { listingId: String(10000001 + i), bySku: { ['T' + Math.floor(i / 10)]: 'NEW' } }]));
  const result = await fixture({ seeds, collections: { Charm_Sku_Aliases: aliases } }).helper().read();
  assert.equal(new Set(result.candidates.map(c => c.listingId)).size, 24);
  assert.equal(result.candidateIdsCapped, true);
  assert.ok(result.candidates.every(c => !c.historicalPurchaseVerified));
});

test('exact listing verification projects only catalogue/image/pricing fields and never returns raw titles', async () => {
  const f = fixture({ collections: {
    EtsyMail_Listings: { '10000001': { listingId: 10000001, title: PRIVATE + ' Charm Necklace', description: PRIVATE, active: true, state: 'active', listingUrl: 'https://private.invalid/' + PRIVATE,
      images: [{ url_fullxfull: 'https://i.etsystatic.com/333/il_fullxfull.7389130580_5zix.jpg', alt_text: PRIVATE }], lastSyncedAt: NOW } },
    Etsy_Listing_Image_Cache: { '10000001': { images: [{ url: 'https://i.etsystatic.com/333/il_fullxfull.7389130581_5zin.webp' }], token: PRIVATE } },
    EtsyPricing_Listings: { '10000001': { original_saved: true, chain_type: 'beady', updated_at: NOW, original_inventory: { products: [
      { product_id: 20000001, sku: 'TEST', property_values: [{ property_id: 1, values: [PRIVATE] }], offerings: [{ price: 123 }] },
      { product_id: 20000002, sku: 'test' }, { product_id: 20000003, sku: PRIVATE }
    ] } } }
  } });
  const result = await f.helper().read({ method: 'listingVerification', listingIds: ['10000001'] }), row = result.listings[0];
  assert.equal(row.canonicalUrl, 'https://www.etsy.com/listing/10000001');
  assert.equal(row.catalogueIdentifierAgrees, true);
  assert.deepEqual(row.titleWords, ['necklace', 'charm']);
  assert.equal(row.titleFingerprint.length, 64);
  assert.deepEqual(row.photoAssetTokens, ['7389130580_5zix', '7389130581_5zin']);
  assert.deepEqual(row.originalInventoryMatches.map(m => m.caseRelation), ['case_sensitive_exact', 'case_variant_unconfirmed']);
  assert.equal(row.currentChainType, 'beady');
  assert.equal(row.currentChainIsHistoricalProof, false);
  assert.deepEqual(f.calls.filter(c => c.op === 'getAll').map(c => c.fields), [
    ['listingId', 'title', 'state', 'active', 'listingUrl', 'images', 'lastSyncedAt'], ['images'],
    ['original_saved', 'original_inventory.products', 'chain_type', 'updated_at']
  ]);
  assertPrivate(result);
});

test('absent listing cache is not disproof; unavailable pricing preserves unknown values', async () => {
  const f = fixture({ failures: { EtsyPricing_Listings: { code: 9, message: PRIVATE } } });
  const result = await f.helper().read({ method: 'listingVerification', listingIds: ['10000001'] }), row = result.listings[0];
  assert.equal(row.catalogueStatus, 'absent');
  assert.equal(row.catalogueIdentifierAgrees, null);
  assert.equal(row.pricingCacheStatus, 'error');
  assert.equal(row.pricingCacheError, 'CACHE_INDEX_UNAVAILABLE');
  assert.equal(row.originalSaved, null);
  assert.equal(row.originalInventoryPresent, null);
  assert.equal(row.originalInventoryInspectionTruncated, null);
  assert.equal(result.status, 'partial_or_unavailable');
});

test('listing cache mismatches, unknown state/type and unsafe image hosts cannot establish identity', async () => {
  const f = fixture({ collections: { EtsyMail_Listings: { '10000001': { listingId: 10000002, title: PRIVATE, state: PRIVATE, active: 'true',
    images: ['https://evil.invalid/il_fullxfull.7389130580_5zix.jpg', 'http://i.etsystatic.com/il_fullxfull.7389130580_5zix.jpg',
      'https://user:password@i.etsystatic.com/il_fullxfull.7389130580_5zix.jpg', 'https://i.etsystatic.com:444/il_fullxfull.7389130580_5zix.jpg'] } } } });
  const row = (await f.helper().read({ method: 'listingVerification', listingIds: ['10000001'] })).listings[0];
  assert.equal(row.catalogueIdentifierAgrees, false);
  assert.equal(row.catalogueState, null);
  assert.equal(row.catalogueActive, null);
  assert.deepEqual(row.photoAssetTokens, []);
  assert.ok(!JSON.stringify(row).includes(PRIVATE));
});

test('listing inventory cap is explicit and exact ID inputs are strictly bounded', async () => {
  const products = Array.from({ length: 101 }, (_, i) => ({ sku: i === 100 ? 'TEST' : 'OTHER', product_id: 20000001 + i }));
  const f = fixture({ collections: { EtsyPricing_Listings: { '10000001': { original_inventory: { products } } } } });
  const row = (await f.helper().read({ method: 'listingVerification', listingIds: ['10000001'] })).listings[0];
  assert.equal(row.originalInventoryInspectionTruncated, true);
  assert.deepEqual(row.originalInventoryMatches, []);
  for (const listingIds of [[], [10000001], ['../config'], ['001'], Array.from({ length: 25 }, (_, i) => String(10000001 + i))]) {
    const invalid = fixture(), result = await invalid.helper().read({ method: 'listingVerification', listingIds });
    assert.equal(result.code, 'INVALID_LISTING_IDS');
    assert.equal(invalid.calls.length, 0);
  }
});

test('historical receipt uses time filters, actual document-ID ordering and transaction projection only', async () => {
  const f = receiptFixture(), result = await f.helper().read(receiptOptions()), q = f.calls.find(c => c.coll === 'EtsyMail_Receipts');
  assert.deepEqual(q.filters, [{ field: 'created_timestamp', op: '>=', value: 100 }, { field: 'created_timestamp', op: '<=', value: 1000 }]);
  assert.deepEqual(q.fields, ['created_timestamp', 'raw.transactions']);
  assert.equal(q.orders[0].direction, 'desc');
  assert.ok(q.orders[1].field instanceof FieldPath);
  assert.deepEqual(q.orders[1].field.segments, ['__name__']);
  assert.equal(q.limit, 100);
  assert.equal(result.transactionsInspected, 1);
  assert.equal(result.witnesses[0].etsyListingId, '10000001');
  assert.equal(result.exactHistoricalEvidenceIsCurrentShopifyMatch, false);
  assertPrivate(result);
});

test('raw customer, receipt, transaction and free-text option values never escape', async () => {
  const f = receiptFixture([tx({ title: PRIVATE + ' Custom Order Charm', personalization: PRIVATE, buyer_user_id: PRIVATE,
    variations: [{ property_id: 1, value_id: 11, formatted_name: 'Custom message', formatted_value: PRIVATE },
      { property_id: 2, value_id: 12, formatted_name: 'Engraving text', formatted_value: PRIVATE },
      { property_id: 3, value_id: 13, formatted_name: PRIVATE, formatted_value: PRIVATE }] })]);
  const result = await f.helper().read(receiptOptions());
  assertPrivate(result);
  assert.ok(result.witnesses[0].selectedOptions.every(v => v.value === null && v.unreturnedValuePresent));
  assert.deepEqual(result.witnesses[0].observedTitleWords, ['charm']);
});

test('only strictly generic selected option values are returned, with property/value IDs', async () => {
  const values = [
    ['Metal Choice', 'Sterling Silver', 'metal', 'sterling silver'], ['Length', '18 inches', 'length', '18 inches'],
    ['Chain Type', 'Beady Chain', 'chain', 'beady chain'], ['Engraving', 'With Engraving', 'engraving_toggle', 'with engraving'],
    ['Metal', 'Silver for ' + PRIVATE, 'metal', null], ['Length', '18 inches ' + PRIVATE, 'length', null],
    ['Chain', 'Beady Chain ' + PRIVATE, 'chain', null], ['Engraving', PRIVATE, 'engraving_toggle', null]
  ];
  const result = await receiptFixture([tx({ variations: values.map(([formatted_name, formatted_value], i) => ({ property_id: i + 1, value_id: i + 11, formatted_name, formatted_value })) })]).helper().read(receiptOptions());
  assert.deepEqual(result.witnesses[0].selectedOptions.map(v => [v.kind, v.value]), values.map(v => v.slice(2)));
  assert.deepEqual(result.witnesses[0].selectedOptions.map(v => v.propertyId), values.map((_, i) => String(i + 1)));
  assertPrivate(result);
});

test('variation array cap is explicit and malformed variation identifiers are omitted', async () => {
  const result = await receiptFixture([tx({ variations: Array.from({ length: 21 }, () => ({ property_id: PRIVATE, value_id: -1, formatted_name: PRIVATE, formatted_value: PRIVATE })) })]).helper().read(receiptOptions());
  const w = result.witnesses[0];
  assert.equal(w.selectedOptions.length, 20);
  assert.equal(w.variationsInspectionTruncated, true);
  assert.ok(w.selectedOptions.every(v => v.propertyId === null && v.valueId === null));
  assertPrivate(result);
});

test('exact and case-variant historical SKUs remain separate product evidence', async () => {
  const transactions = ['TEST SKU', 'Test Sku', 'test sku'].map((sku, i) => tx({ sku, transaction_id: 810000001 + i, listing_id: 10000001 + i, title: ['Fixture Charm', 'Fixture Necklace', 'Fixture Earrings'][i] }));
  const result = await receiptFixture(transactions, { seeds: [{ rank: 1, sku: 'TEST SKU' }] }).helper().read(receiptOptions());
  assert.deepEqual(result.witnesses.map(w => w.observedSku), ['TEST SKU', 'Test Sku', 'test sku']);
  assert.deepEqual(result.witnesses.map(w => w.caseRelation), ['case_sensitive_exact', 'case_variant_unconfirmed', 'case_variant_unconfirmed']);
  assert.ok(result.witnesses.every(w => !w.rankedIdentityConfirmed));
  assert.equal(new Set(result.witnesses.map(w => w.etsyListingId)).size, 3);
});

test('multiple stored tokens preserve exact spelling without duplicating weaker case witnesses', async () => {
  const f = receiptFixture([tx({ sku: 'Test' })], { seeds: [{ rank: 1, sku: 'TEST, Test' }] });
  const result = await f.helper().read(receiptOptions());
  assert.equal(result.witnesses.length, 1);
  assert.equal(result.witnesses[0].originalSkuToken, 'Test');
  assert.equal(result.witnesses[0].caseRelation, 'case_sensitive_exact');
  const ambiguous = await receiptFixture([tx({ sku: 'test' })], { seeds: [{ rank: 1, sku: 'TEST, Test' }] }).helper().read(receiptOptions());
  assert.equal(ambiguous.witnesses.length, 1);
  assert.equal(ambiguous.witnesses[0].caseRelation, 'case_variant_unconfirmed');
});

test('one shared SKU retains separate unresolved ranks and distinct Etsy product identities', async () => {
  const f = receiptFixture([tx(), tx({ transaction_id: 810000002, product_id: 20000002 })], { seeds: [{ rank: 1, sku: 'TEST' }, { rank: 2, sku: 'TEST' }] });
  const result = await f.helper().read(receiptOptions());
  assert.deepEqual(result.witnesses.map(w => [w.rank, w.etsyProductId]), [[1, '20000001'], [2, '20000001'], [1, '20000002'], [2, '20000002']]);
  assert.ok(result.witnesses.every(w => !w.rankedIdentityConfirmed));
});

test('unrelated SKU and invalid listing ID do not return title, variants or identity values', async () => {
  const result = await receiptFixture([tx({ sku: PRIVATE, title: PRIVATE }), tx({ listing_id: PRIVATE }), tx({ listing_id: 'gid://shopify/Product/10000001' })]).helper().read(receiptOptions());
  assert.deepEqual(result.witnesses, []);
  assert.equal(result.transactionsInspected, 3);
  assertPrivate(result);
});

test('witness samples and scanned transaction rows never become order or quantity counts', async () => {
  const transactions = Array.from({ length: 6 }, (_, i) => tx({ transaction_id: 810000001 + i, quantity: 999 }));
  const result = await receiptFixture(transactions).helper().read(receiptOptions());
  assert.equal(result.transactionsInspected, 6);
  assert.equal(result.witnesses.length, 3);
  assert.equal(result.witnessCountsAreOrderCounts, false);
  assert.equal(result.scannedEntireHistory, false);
  assert.ok(!JSON.stringify(result).includes('quantity'));
});

test('duplicate transaction witnesses are deduplicated, but missing IDs use distinct inspected positions', async () => {
  const result = await receiptFixture([tx(), tx(), tx({ transaction_id: null }), tx({ transaction_id: null })]).helper().read(receiptOptions());
  assert.equal(result.witnesses.length, 3);
  assert.equal(new Set(result.witnesses.map(w => w.witnessFingerprint)).size, 3);
  assert.equal(result.transactionsInspected, 4);
});

test('reused transaction identifier cannot suppress conflicting product or case evidence', async () => {
  const result = await receiptFixture([tx(), tx({ product_id: 20000002 }), tx({ sku: 'test' })]).helper().read(receiptOptions());
  assert.equal(result.witnesses.length, 3);
  assert.deepEqual(result.witnesses.map(w => w.etsyProductId), ['20000001', '20000002', '20000001']);
  assert.deepEqual(result.witnesses.map(w => w.caseRelation), ['case_sensitive_exact', 'case_sensitive_exact', 'case_variant_unconfirmed']);
  assertPrivate(result);
});

test('fallback offset cannot collide with a numeric transaction identifier', async () => {
  const result = await receiptFixture([tx({ transaction_id: 1 }), tx({ transaction_id: null })]).helper().read(receiptOptions());
  assert.equal(result.witnesses.length, 2);
  assert.notEqual(result.witnesses[0].witnessFingerprint, result.witnesses[1].witnessFingerprint);
});

test('invalid time windows, limits and budgets are rejected before any read', async () => {
  for (const options of [receiptOptions({ fromSec: -1 }), receiptOptions({ fromSec: 1001 }), receiptOptions({ untilSec: NOW / 1000 + 1 }),
    receiptOptions({ limit: 101 }), receiptOptions({ limit: 0 }), receiptOptions({ limit: '1' }), receiptOptions({ budgetMs: 9 }), receiptOptions({ budgetMs: 15001 })]) {
    const f = fixture(), result = await f.helper().read(options);
    assert.equal(result.status, 'unavailable');
    assert.equal(f.calls.length, 0);
  }
});

test('receipt reads require a server cursor key; missing key never opens receipts', async () => {
  const f = fixture(), result = await f.helper({ cursorSecret: '' }).read(receiptOptions());
  assert.equal(result.code, 'CURSOR_KEY_REQUIRED');
  assert.ok(!f.calls.some(c => c.coll === 'EtsyMail_Receipts'));
});

test('stable timestamp tie pagination uses the encrypted timestamp-plus-ID cursor exactly once', async () => {
  const f = fixture({ collections: { EtsyMail_Receipts: Object.fromEntries([1, 2, 3].map(i => [String(910000000 + i), receipt([tx({ transaction_id: 810000000 + i, listing_id: 10000000 + i })], 500)])) } });
  const helper = f.helper(), first = await helper.read(receiptOptions({ limit: 2 })), second = await helper.read(receiptOptions({ limit: 2, cursor: first.nextCursor }));
  assert.deepEqual(first.witnesses.map(w => w.etsyListingId), ['10000003', '10000002']);
  assert.deepEqual(second.witnesses.map(w => w.etsyListingId), ['10000001']);
  assert.equal(first.windowExhausted, false);
  assert.equal(second.windowExhausted, true);
  assert.equal(second.nextCursor, null);
  const q = f.calls.filter(c => c.coll === 'EtsyMail_Receipts')[1];
  assert.deepEqual(q.start, [500, '910000002']);
  assert.ok(first.nextCursor.startsWith('h1.'));
  assert.ok(!Buffer.from(first.nextCursor.slice(3), 'base64url').toString('utf8').includes('910000002'));
  assertPrivate(first);
});

test('tampered, foreign-key, wrong-window and invalid-form cursors reject before receipt access', async () => {
  const f = receiptFixture(), first = await f.helper().read(receiptOptions({ limit: 1 }));
  for (const cursor of ['', null, false, 0, {}, PRIVATE, 'h1.AA', first.nextCursor.slice(0, -6) + 'AAAAAA', 'h1.' + 'A'.repeat(8192)]) {
    const g = receiptFixture(), result = await g.helper().read(receiptOptions({ cursor }));
    assert.equal(result.code, 'CURSOR_INVALID');
    assert.ok(!g.calls.some(c => c.coll === 'EtsyMail_Receipts'));
    assert.ok(!JSON.stringify(result).includes(PRIVATE));
  }
  const foreign = receiptFixture(), changed = await foreign.helper({ cursorSecret: 'another-fixture-key-long-enough' }).read(receiptOptions({ cursor: first.nextCursor }));
  assert.equal(changed.code, 'CURSOR_INVALID');
  assert.ok(!foreign.calls.some(c => c.coll === 'EtsyMail_Receipts'));
  const window = receiptFixture(), wrongWindow = await window.helper().read(receiptOptions({ fromSec: 101, cursor: first.nextCursor }));
  assert.equal(wrongWindow.code, 'CURSOR_FOREIGN');
  assert.ok(!window.calls.some(c => c.coll === 'EtsyMail_Receipts'));
});

test('cursor expires and original seed case changes invalidate authenticated continuation', async () => {
  const f = receiptFixture(), first = await f.helper().read(receiptOptions({ limit: 1 }));
  const expired = await f.helper({ now: () => NOW + LIMITS.cursorLifetimeMs }).read(receiptOptions({ cursor: first.nextCursor }));
  assert.equal(expired.code, 'CURSOR_EXPIRED');
  const g = receiptFixture(undefined, { seeds: [{ rank: 1, sku: 'Test' }] });
  const changed = await g.helper().read(receiptOptions({ cursor: first.nextCursor }));
  assert.equal(changed.code, 'CURSOR_INVALID');
  assert.ok(!g.calls.some(c => c.coll === 'EtsyMail_Receipts'));
});

test('1001-transaction receipt resumes the uninspected element instead of skipping it', async () => {
  const transactions = Array.from({ length: 1001 }, (_, i) => tx({ sku: i === 1000 ? 'TEST' : 'OTHER', transaction_id: 810000001 + i }));
  const f = receiptFixture(transactions), helper = f.helper(), first = await helper.read(receiptOptions());
  assert.equal(first.transactionsInspected, 1000);
  assert.equal(first.status, 'incomplete');
  assert.equal(first.inspectionTruncated, true);
  assert.deepEqual(first.witnesses, []);
  assert.equal(first.windowExhausted, false);
  assert.ok(first.nextCursor);
  const second = await helper.read(receiptOptions({ cursor: first.nextCursor }));
  assert.equal(second.transactionsInspected, 1);
  assert.equal(second.witnesses.length, 1);
  assert.equal(second.witnesses[0].etsyListingId, '10000001');
  assert.ok(f.calls.some(c => c.op === 'getAll' && c.coll === 'EtsyMail_Receipts' && c.ids[0] === '910000001'));
  const third = await helper.read(receiptOptions({ cursor: second.nextCursor }));
  assert.equal(third.windowExhausted, true);
  assert.equal(third.transactionsInspected, 0);
  assert.equal(third.nextCursor, null);
  assertPrivate(first);
});

test('within-document cursor detects missing or changed source before emitting stronger identity evidence', async () => {
  const transactions = Array.from({ length: 1001 }, (_, i) => tx({ sku: 'OTHER', transaction_id: 810000001 + i }));
  const f = receiptFixture(transactions), first = await f.helper().read(receiptOptions());
  f.data.EtsyMail_Receipts['910000001'].raw.transactions[1000].sku = 'TEST';
  const changed = await f.helper().read(receiptOptions({ cursor: first.nextCursor }));
  assert.equal(changed.code, 'CURSOR_SOURCE_CHANGED');
  assert.deepEqual(changed.witnesses, []);
  delete f.data.EtsyMail_Receipts['910000001'];
  const missing = await f.helper().read(receiptOptions({ cursor: first.nextCursor }));
  assert.equal(missing.code, 'CURSOR_SOURCE_MISSING');
});

test('within-document cache failures preserve safe explicit unavailable reasons', async () => {
  const f = receiptFixture(Array.from({ length: 1001 }, () => tx({ sku: 'OTHER' })));
  const first = await f.helper().read(receiptOptions());
  const error = { code: 7, message: PRIVATE };
  const g = receiptFixture([], { failures: { EtsyMail_Receipts: error } });
  const failed = await g.helper().read(receiptOptions({ cursor: first.nextCursor }));
  assert.equal(failed.code, 'CACHE_ACCESS_DENIED');
  assert.equal(failed.transactionsInspected, null);
  assertPrivate(failed);
});

test('ninth distinct historical listing/product pair resumes as additional evidence, never a chosen identity', async () => {
  const transactions = Array.from({ length: 9 }, (_, i) => tx({ transaction_id: 810000001 + i, listing_id: 10000001 + i, product_id: 20000001 + i }));
  const f = receiptFixture(transactions), helper = f.helper(), first = await helper.read(receiptOptions());
  assert.equal(first.witnesses.length, 8);
  assert.equal(first.transactionsInspected, 8);
  assert.equal(first.inspectionTruncated, true);
  const second = await helper.read(receiptOptions({ cursor: first.nextCursor }));
  assert.deepEqual(second.witnesses.map(w => w.etsyListingId), ['10000009']);
  assert.ok(first.witnesses.concat(second.witnesses).every(w => !w.rankedIdentityConfirmed));
});

test('global witness cap preserves the interrupted transaction atomically across shared target ranks', async () => {
  const seeds = Array.from({ length: 12 }, (_, i) => ({ rank: i + 1, sku: 'TEST' }));
  const transactions = Array.from({ length: 9 }, (_, i) => tx({ transaction_id: 810000001 + i, listing_id: 10000001 + Math.floor(i / 3), product_id: 20000001 + Math.floor(i / 3) }));
  const f = receiptFixture(transactions, { seeds }), helper = f.helper(), first = await helper.read(receiptOptions());
  assert.equal(first.witnesses.length, 96);
  assert.equal(first.transactionsInspected, 8);
  assert.equal(first.status, 'incomplete');
  const second = await helper.read(receiptOptions({ cursor: first.nextCursor }));
  assert.equal(second.witnesses.length, 12);
  assert.equal(second.transactionsInspected, 1);
  assert.ok(Buffer.byteLength(JSON.stringify(first)) < LIMITS.responseBytes);
});

test('response byte budget stops before an atomic transaction and continuation retains every rank witness', async () => {
  const seeds = Array.from({ length: 12 }, (_, i) => ({ rank: i + 1, sku: 'TEST' }));
  const variations = Array.from({ length: 20 }, (_, i) => ({ property_id: i + 1, value_id: 1000000 + i,
    formatted_name: 'Metal Choice', formatted_value: '14 karat rose gold filled' }));
  const transactions = Array.from({ length: 9 }, (_, i) => tx({ transaction_id: 810000001 + i,
    listing_id: 10000001 + Math.floor(i / 3), product_id: 20000001 + Math.floor(i / 3), variations }));
  const f = receiptFixture(transactions, { seeds }), helper = f.helper(), first = await helper.read(receiptOptions());
  assert.equal(first.status, 'incomplete');
  assert.equal(first.inspectionTruncated, true);
  assert.ok(first.witnesses.length > 0 && first.witnesses.length < LIMITS.witnesses);
  assert.equal(first.witnesses.length % seeds.length, 0);
  assert.ok(Buffer.byteLength(JSON.stringify(first)) < LIMITS.responseBytes);
  const second = await helper.read(receiptOptions({ cursor: first.nextCursor }));
  assert.equal(first.transactionsInspected + second.transactionsInspected, 9);
  assert.equal(first.witnesses.length + second.witnesses.length, 108);
  assert.equal(new Set(first.witnesses.concat(second.witnesses).map(w => w.witnessFingerprint)).size, 108);
});

test('the 10000 transaction-request bound resumes before the unread next document', async () => {
  const collections = { EtsyMail_Receipts: Object.fromEntries(Array.from({ length: 11 }, (_, i) => [String(910000001 + i), receipt(Array.from({ length: 1000 }, () => tx({ sku: 'OTHER' })), 500 - i)])) };
  const f = fixture({ collections }), helper = f.helper(), first = await helper.read(receiptOptions());
  assert.equal(first.transactionsInspected, 10000);
  assert.equal(first.status, 'incomplete');
  assert.equal(first.windowExhausted, false);
  const second = await helper.read(receiptOptions({ cursor: first.nextCursor }));
  assert.equal(second.transactionsInspected, 1000);
  assert.equal(second.witnesses.length, 0);
});

test('invalid transaction arrays preserve incomplete/unknown scope and return no skipping cursor', async () => {
  const f = receiptFixture(null), result = await f.helper().read(receiptOptions());
  assert.equal(result.status, 'incomplete');
  assert.equal(result.invalidTransactionArrays, 1);
  assert.equal(result.windowExhausted, null);
  assert.equal(result.nextCursor, null);
  assert.deepEqual(result.witnesses, []);
});

test('empty bounded page reports only the explicit requested window, never global absence', async () => {
  const result = await fixture().helper().read(receiptOptions());
  assert.equal(result.receiptDocumentsObserved, 0);
  assert.equal(result.transactionsInspected, 0);
  assert.equal(result.windowExhausted, true);
  assert.equal(result.scannedEntireHistory, false);
  assert.equal(result.nextCursor, null);
});

test('source structural errors and private SDK exception messages are never serialized', async () => {
  for (const error of [{ code: 'permission-denied', message: PRIVATE }, { code: 9, message: PRIVATE }, { code: '__proto__', message: PRIVATE }, { code: { secret: PRIVATE }, message: PRIVATE }]) {
    const f = receiptFixture(undefined, { failures: { EtsyMail_Receipts: error } }), result = await f.helper().read(receiptOptions());
    assert.equal(result.status, 'unavailable');
    assert.equal(result.transactionsInspected, null);
    assert.equal(result.windowExhausted, null);
    assert.ok(!JSON.stringify(result).includes(PRIVATE));
    assert.equal(f.writes(), 0);
  }
  const malformed = fixture({ malformed: { [QUEUE]: { docs: null } } });
  assert.equal((await malformed.helper().read()).code, 'SOURCE_INVALID');
});

test('oversized adapter pages, inconsistent exact document responses and malformed queue rows fail closed', async () => {
  const many = Array.from({ length: 101 }, (_, i) => ({ id: String(910000001 + i), data: () => receipt() }));
  const f = receiptFixture(undefined, { malformed: { EtsyMail_Receipts: { docs: many } } });
  const oversize = await f.helper().read(receiptOptions());
  assert.equal(oversize.code, 'SOURCE_INVALID');
  assert.equal(oversize.receiptDocumentsObserved, null);
  const listing = fixture({ malformed: { EtsyMail_Listings: [{ id: '10000002', exists: true, data: () => ({ listingId: '10000002' }) }] } });
  const result = await listing.helper().read({ method: 'listingVerification', listingIds: ['10000001'] });
  assert.equal(result.listings[0].catalogueError, 'SOURCE_INVALID');
  assert.equal(result.listings[0].catalogueIdentifierAgrees, null);
  const queue = fixture({ malformed: { [QUEUE]: { docs: [{ id: 'rank-1', data: () => ({ rank: '1', sku: PRIVATE }) }] } } });
  const bad = await queue.helper().read();
  assert.equal(bad.code, 'SEEDS_INVALID');
  assert.ok(!JSON.stringify(bad).includes(PRIVATE));
});

test('time limit returns unknown coverage and no provider or write fallback', async () => {
  const f = fixture({ delayMs: { [QUEUE]: 30 } }), result = await f.helper().read({ budgetMs: 10 });
  assert.equal(result.status, 'unavailable');
  assert.equal(result.code, 'TIME_LIMIT');
  assert.equal(result.transactionsInspected, null);
  assert.equal(result.windowExhausted, null);
  assert.equal(f.writes(), 0);
  assert.equal(f.calls.length, 1);
});

test('all methods stay projected, bounded and read-only with no external provider access', async () => {
  const f = receiptFixture(), originalFetch = global.fetch;
  let externalCalls = 0;
  global.fetch = () => { externalCalls++; throw Error('Unexpected provider call'); };
  try {
    const helper = f.helper();
    for (const options of [{}, { method: 'listingVerification', listingIds: ['10000001'] }, receiptOptions()]) assertPrivate(await helper.read(options));
  } finally { global.fetch = originalFetch; }
  assert.equal(externalCalls, 0);
  assert.equal(f.writes(), 0);
  assert.ok(f.calls.every(c => [QUEUE, 'Charm_Sku_Aliases', 'EtsyMail_Listings', 'Etsy_Listing_Image_Cache', 'EtsyPricing_Listings', 'EtsyMail_Receipts'].includes(c.coll)));
});
