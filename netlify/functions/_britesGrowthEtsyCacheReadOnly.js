'use strict';

// This helper is called only by a protected sandbox operator route. The caller
// supplies its trusted Firestore db; this module has no auth, provider, token,
// sync, write or environment-variable dependencies. All targets are fixed.
const SAMPLE_LIMIT = 1;
const ARRAY_INSPECTION_LIMIT = 10;
const MAX_EPOCH_MS = 4102444800000; // 2100-01-01, rejects invalid timestamps.
const own = (o, k) => !!o && Object.prototype.hasOwnProperty.call(o, k);
const object = o => !!o && typeof o === 'object' && !Array.isArray(o);
const integer = x => Number.isSafeInteger(x) && x >= 0 ? x : null;
const boolean = x => typeof x === 'boolean' ? x : null;
const present = x => (typeof x === 'string' && x.trim().length > 0) || (typeof x === 'number' && Number.isFinite(x));
const nonemptyString = x => typeof x === 'string' && x.trim().length > 0;
const array = x => Array.isArray(x) ? x : [];
const inspected = x => array(x).slice(0, ARRAY_INSPECTION_LIMIT).filter(object);
const enumValue = (x, values) => x == null ? null : values.includes(x) ? x : 'unrecognized';

function millis(value) {
  let n = null;
  try {
    if (typeof value === 'number') n = value;
    else if (value instanceof Date) n = value.getTime();
    else if (value && typeof value.toMillis === 'function') n = value.toMillis();
    else if (object(value)) {
      const seconds = own(value, 'seconds') ? value.seconds : value._seconds;
      const nanos = own(value, 'nanoseconds') ? value.nanoseconds : value._nanoseconds;
      if (Number.isSafeInteger(seconds) && seconds >= 0 &&
          (nanos == null || (Number.isInteger(nanos) && nanos >= 0 && nanos < 1000000000))) {
        n = seconds * 1000 + Math.floor((nanos || 0) / 1000000);
      }
    }
  } catch (_) { return null; }
  return typeof n === 'number' && Number.isFinite(n) && n >= 0 && n <= MAX_EPOCH_MS ? n : null;
}

function unixSeconds(value) {
  return Number.isSafeInteger(value) && value >= 0 && value <= MAX_EPOCH_MS / 1000 ? value : null;
}

function safeError(error) {
  const codes = ['permission-denied', 'unavailable', 'deadline-exceeded', 'not-found', 'resource-exhausted', 'cache_response_invalid'];
  const numeric = { 4: 'deadline-exceeded', 5: 'not-found', 7: 'permission-denied', 8: 'resource-exhausted', 14: 'unavailable' };
  const code = error && error.code;
  return { code: codes.includes(code) ? code : typeof code === 'number' && own(numeric, code) ? numeric[code] : 'cache_read_failed' };
}

function invalidResponse() {
  const e = new Error('Invalid cache response');
  e.code = 'cache_response_invalid';
  return e;
}

function errorPresence(d, field) {
  if (!own(d, field) || d[field] === undefined) return null;
  if (d[field] === null) return false;
  return typeof d[field] === 'string' ? nonemptyString(d[field]) : true;
}

function listingState(d) {
  return {
    inFlight: boolean(d.inFlight),
    storedTotalListings: integer(d.totalListings),
    lastIncrementalAt: millis(d.lastIncrementalAt),
    lastFullSyncAt: millis(d.lastFullSyncAt),
    errorPresent: errorPresence(d, 'lastError')
  };
}

function receiptState(d) {
  const p = object(d.backfillProgress) ? d.backfillProgress : null;
  return {
    enabled: boolean(d.enabled),
    lastSyncTimestamp: unixSeconds(d.lastSyncTimestamp),
    lastSyncCompletedAt: millis(d.lastSyncCompletedAt),
    lastSyncCallCount: integer(d.lastSyncCallCount),
    lastSyncReceiptsCount: integer(d.lastSyncReceiptsCount),
    lastSyncOutcome: enumValue(d.lastSyncOutcome, ['ok', 'error', 'skipped', 'rate_limited']),
    errorPresent: errorPresence(d, 'lastSyncErrorMsg'),
    backfill: p ? {
      status: enumValue(p.status, ['idle', 'running', 'complete', 'error', 'paused']),
      startedAt: millis(p.startedAt),
      completedAt: millis(p.completedAt),
      totalPagesEstimate: integer(p.totalPagesEstimate),
      pagesProcessed: integer(p.pagesProcessed),
      receiptsProcessed: integer(p.receiptsProcessed),
      currentOffset: integer(p.currentOffset),
      windowMinCreated: unixSeconds(p.windowMinCreated),
      windowMaxCreated: unixSeconds(p.windowMaxCreated),
      errorPresent: errorPresence(p, 'errorMsg')
    } : null
  };
}

function productShape(products) {
  const rows = inspected(products);
  return {
    isArray: Array.isArray(products),
    arrayLength: Array.isArray(products) ? products.length : null,
    inspectedObjects: rows.length,
    hasProductIdInInspectedProducts: rows.some(p => present(p.product_id)),
    hasSkuFieldInInspectedProducts: rows.some(p => own(p, 'sku')),
    hasNonemptySkuInInspectedProducts: rows.some(p => nonemptyString(p.sku)),
    hasPropertyValuesArrayInInspectedProducts: rows.some(p => Array.isArray(p.property_values)),
    hasPropertyIdInInspectedProducts: rows.some(p => inspected(p.property_values).some(v => present(v.property_id))),
    hasOfferingIdInInspectedProducts: rows.some(p => inspected(p.offerings).some(v => present(v.offering_id)))
  };
}

function listingShape(d) {
  const products = object(d.inventory) ? d.inventory.products : d.products;
  const variants = inspected(d.variants);
  return {
    hasListingIdentifier: present(d.listingId) || present(d.listing_id),
    hasTitle: nonemptyString(d.title),
    hasImageArray: Array.isArray(d.images),
    hasNonemptyImageArray: array(d.images).length > 0,
    hasSkuField: own(d, 'sku') || own(d, 'skus'),
    hasNonemptySku: nonemptyString(d.sku) || array(d.skus).slice(0, ARRAY_INSPECTION_LIMIT).some(nonemptyString),
    hasVariationsField: own(d, 'variations'),
    hasVariationsArray: Array.isArray(d.variations),
    variationsArrayLength: Array.isArray(d.variations) ? d.variations.length : null,
    hasVariantsArray: Array.isArray(d.variants),
    hasSkuInInspectedVariants: variants.some(v => nonemptyString(v.sku)),
    hasVariantProductIdInInspectedVariants: variants.some(v => present(v.product_id)),
    inventoryProducts: productShape(products),
    lastSyncedAt: millis(d.lastSyncedAt)
  };
}

function receiptShape(d) {
  const raw = object(d.raw) ? d.raw : {};
  const nested = object(raw.receipt) ? raw.receipt : {};
  const candidates = [raw.transactions, d.transactions, nested.transactions];
  const arrays = candidates.filter(Array.isArray);
  const transactions = arrays.find(a => a.length > 0) || arrays[0];
  const rows = inspected(transactions);
  return {
    hasReceiptIdentifier: present(d.receipt_id) || present(d.receiptId) || present(raw.receipt_id),
    hasRawObject: object(d.raw),
    hasTransactionsField: own(raw, 'transactions') || own(d, 'transactions') || own(nested, 'transactions'),
    hasTransactionsArray: Array.isArray(transactions),
    transactionsArrayLength: Array.isArray(transactions) ? transactions.length : null,
    inspectedTransactions: rows.length,
    hasTransactionIdInInspectedTransactions: rows.some(t => present(t.transaction_id)),
    hasListingIdInInspectedTransactions: rows.some(t => present(t.listing_id)),
    hasProductIdInInspectedTransactions: rows.some(t => present(t.product_id)),
    hasSkuFieldInInspectedTransactions: rows.some(t => own(t, 'sku')),
    hasNonemptySkuInInspectedTransactions: rows.some(t => nonemptyString(t.sku)),
    hasVariationsFieldInInspectedTransactions: rows.some(t => own(t, 'variations')),
    hasVariationsArrayInInspectedTransactions: rows.some(t => Array.isArray(t.variations)),
    hasNonemptyVariationsInInspectedTransactions: rows.some(t => array(t.variations).length > 0),
    hasVariationPropertyIdInInspectedTransactions: rows.some(t => inspected(t.variations).some(v => present(v.property_id))),
    hasVariationValueIdInInspectedTransactions: rows.some(t => inspected(t.variations).some(v => present(v.value_id))),
    mirrorWrittenAt: millis(d.mirrorWrittenAt)
  };
}

function pricingShape(d) {
  return {
    originalSaved: boolean(d.original_saved),
    hasOriginalInventoryObject: object(d.original_inventory),
    hasOriginalSnapshotHash: nonemptyString(d.original_snapshot_hash),
    originalProducts: productShape(object(d.original_inventory) ? d.original_inventory.products : undefined),
    chainType: enumValue(d.chain_type, ['regular', 'beady']),
    chainSet: boolean(d.chain_set),
    engraving: boolean(d.engraving),
    engravingSet: boolean(d.engrave_set),
    updatedAt: millis(d.updated_at)
  };
}

async function stateObservation(db, coll, id, project) {
  const source = coll + '/' + id;
  try {
    const snap = await db.collection(coll).doc(id).get();
    if (!snap || typeof snap.exists !== 'boolean') throw invalidResponse();
    if (!snap.exists) return { source, status: 'absent', exists: false, metadata: null, error: null };
    const d = snap.data();
    if (!object(d)) throw invalidResponse();
    return { source, status: 'observed', exists: true, metadata: project(d), error: null };
  } catch (error) {
    return { source, status: 'error', exists: null, metadata: null, error: safeError(error) };
  }
}

async function sampleObservation(db, coll, fields, project) {
  try {
    const snap = await db.collection(coll).select(...fields).limit(SAMPLE_LIMIT).get();
    if (!snap || !Array.isArray(snap.docs)) throw invalidResponse();
    const doc = snap.docs.slice(0, SAMPLE_LIMIT)[0];
    if (!doc) return { source: coll, status: 'empty', sampleLimit: SAMPLE_LIMIT, sampleCount: 0, totalCount: null, fieldShape: null, error: null };
    const d = doc.data();
    if (!object(d)) throw invalidResponse();
    return { source: coll, status: 'observed', sampleLimit: SAMPLE_LIMIT, sampleCount: 1, totalCount: null, fieldShape: project(d), error: null };
  } catch (error) {
    return { source: coll, status: 'error', sampleLimit: SAMPLE_LIMIT, sampleCount: null, totalCount: null, fieldShape: null, error: safeError(error) };
  }
}

async function read({ db } = {}) {
  const [listingsSync, receiptsMirrorState, listingSample, receiptSample, pricingSample] = await Promise.all([
    stateObservation(db, 'EtsyMail_ListingsSync', 'global', listingState),
    stateObservation(db, 'EtsyMail_Config', 'receiptsMirrorState', receiptState),
    sampleObservation(db, 'EtsyMail_Listings', ['listingId', 'listing_id', 'title', 'images', 'variations', 'sku', 'skus', 'variants', 'inventory.products', 'products', 'lastSyncedAt'], listingShape),
    sampleObservation(db, 'EtsyMail_Receipts', ['receipt_id', 'receiptId', 'raw.receipt_id', 'raw.transactions', 'raw.receipt.transactions', 'transactions', 'mirrorWrittenAt'], receiptShape),
    sampleObservation(db, 'EtsyPricing_Listings', ['original_saved', 'original_inventory.products', 'original_snapshot_hash', 'chain_type', 'chain_set', 'engraving', 'engrave_set', 'updated_at'], pricingShape)
  ]);
  const sources = { listingsSync, receiptsMirrorState, listingSample, receiptSample, pricingSample };
  return {
    schema: 1,
    mode: 'cache_only_read_only',
    checkedAt: Date.now(),
    status: Object.values(sources).some(s => s.status === 'error') ? 'partial_or_unavailable' : 'observed',
    bounds: { exactStateDocuments: 2, sampleCollections: 3, documentsPerSample: SAMPLE_LIMIT, arrayObjectsInspectedPerField: ARRAY_INSPECTION_LIMIT, readAll: false },
    sources,
    identityBindingPerformed: false,
    limitations: [
      'One sample per collection is not evidence about all cached listings or receipts.',
      'Sample totalCount is unknown; stored metadata counts are not live collection counts.',
      'Historical transaction field presence and backfill metadata cannot establish a particular SKU-to-Shopify match.',
      'No identity values, customer details, credentials, raw records, provider responses or exception messages are returned.'
    ]
  };
}

module.exports = { read };
