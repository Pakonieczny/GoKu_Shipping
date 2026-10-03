'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {assess, SHOPIFY_GID_RULE} = require('../../netlify/functions/_britesGrowthTransactionContinuity');

const NOW = Date.parse('2026-10-02T23:00:00Z');
const SALT = 'controlled-fixture-salt-2026';
const TRANSACTION = '7026412454051';
const DESTINATION = 'AW-123456789/Exact_label-1';
const fixture = over => {
  const value = {
    configuredDestination: DESTINATION,
    canonicalizationRule: SHOPIFY_GID_RULE,
    pixel: {sourceOwner:'GOOGLE_YOUTUBE_APP', eventName:'conversion', fired:true, hitCount:1, sendTo:DESTINATION,
      transactionId:TRANSACTION, conversionValue:61, currency:'USD', observedAt:NOW-3000, consentState:'GRANTED'},
    shopify: {eventName:'checkout_completed', eventId:'shopify-event-fixture', orderId:'gid://shopify/Order/'+TRANSACTION,
      conversionValue:61, currency:'USD', observedAt:NOW-2500},
    webhook: {topic:'orders/paid', payloadId:TRANSACTION, serverTransactionId:TRANSACTION,
      conversionValue:61, currency:'USD', observedAt:NOW-1000}
  };
  if (!over) return value;
  for (const [key, patch] of Object.entries(over)) value[key] = patch && typeof patch === 'object' && !Array.isArray(patch)
    ? {...value[key], ...patch} : patch;
  return value;
};
const run = (input, options = {}) => assess(input, {salt:SALT, now:NOW, ...options});

test('exact numeric browser/server identity with explicit Shopify GID mapping passes without exposing identifiers', () => {
  const result = run(fixture()), encoded = JSON.stringify(result);
  assert.equal(result.artifactContractPassed, true);
  assert.equal(result.transactionIdentityVerified, true);
  assert.equal(result.runtimeDispatchVerified, false);
  assert.equal(result.offlineAssessment, true);
  assert.equal(result.receiptOnly, true);
  assert.equal(result.providerReads, 0);
  assert.equal(result.providerReceiptStatusVerified, false);
  assert.equal(result.duplicateCountingVerified, false);
  assert.deepEqual(result.codes, []);
  assert.deepEqual(result.identity.canonicalization, {rule:SHOPIFY_GID_RULE, applied:true, equivalent:true});
  for (const raw of [TRANSACTION, DESTINATION, 'shopify-event-fixture', SALT]) assert.equal(encoded.includes(raw), false);
  for (const key of ['transactionFingerprint','shopifyEventFingerprint','destinationFingerprint']) assert.match(result.identity[key], /^[a-f0-9]{64}$/);
  for (const key of ['providerWrites','conversionUploads','conversionReplays','cartWrites','orderWrites']) assert.equal(result[key], key === 'providerWrites' ? false : 0);
});

test('exact plain Shopify order identity needs no canonicalization', () => {
  const result = run(fixture({canonicalizationRule:'NONE', shopify:{orderId:TRANSACTION}}));
  assert.equal(result.artifactContractPassed, true);
  assert.equal(result.identity.shopifyToWebhookExact, true);
  assert.equal(result.identity.canonicalization.applied, false);
});

for (const [name, transactionId, code] of [
  ['missing', '', 'PIXEL_ID_MISSING'],
  ['Google generated', 'GG_1234abcd', 'PIXEL_ID_GOOGLE_GENERATED'],
  ['placeholder', 'undefined', 'PIXEL_ID_PLACEHOLDER'],
  ['static short number', '1234', 'PIXEL_ID_PLACEHOLDER'],
  ['over 64 UTF-8 bytes', 'é'.repeat(33), 'PIXEL_ID_OVER_64']
]) test('pixel transaction ID ' + name + ' fails closed', () => {
  const result = run(fixture({pixel:{transactionId}}));
  assert.equal(result.artifactContractPassed, false); assert.ok(result.codes.includes(code));
  assert.equal(result.transactionIdentityVerified, false); assert.equal(result.identity.transactionFingerprint, undefined);
});

test('GID, display order and whitespace variants never become exact Google transaction matches', () => {
  for (const candidate of ['gid://shopify/Order/'+TRANSACTION, '#1042', TRANSACTION+' ']) {
    const result = run(fixture({pixel:{transactionId:candidate}}));
    assert.equal(result.artifactContractPassed, false); assert.ok(result.codes.includes('TRANSACTION_ID_MISMATCH'));
  }
});

test('Unicode normalization is not silently coerced', () => {
  const composed='Café-702641', decomposed='Cafe\u0301-702641';
  const result=run(fixture({pixel:{transactionId:composed}, webhook:{payloadId:decomposed,serverTransactionId:decomposed}, shopify:{orderId:decomposed}, canonicalizationRule:'NONE'}));
  assert.ok(result.codes.includes('TRANSACTION_ID_MISMATCH')); assert.equal(result.artifactContractPassed,false);
});

test('wrong or malformed destination and ambiguous purchase hits are refused', () => {
  const wrong=run(fixture({pixel:{sendTo:'AW-123456789/Other'}}));
  assert.ok(wrong.codes.includes('DESTINATION_MISMATCH'));
  const ambiguous=run(fixture({pixel:{hitCount:2}}));
  assert.ok(ambiguous.codes.includes('MULTIPLE_PURCHASE_HITS'));
  const absent=run(fixture({pixel:{hitCount:0}}));
  assert.ok(absent.codes.includes('PIXEL_HIT_COUNT_UNCONFIRMED'));
});

test('Shopify identity shape mismatch and undeclared canonicalization are refused', () => {
  const wrong=run(fixture({shopify:{orderId:'gid://shopify/Order/999'}}));
  assert.ok(wrong.codes.includes('SHOPIFY_ID_SHAPE_MISMATCH'));
  const undeclared=run(fixture({canonicalizationRule:'NONE'}));
  assert.ok(undeclared.codes.includes('SHOPIFY_ID_SHAPE_MISMATCH'));
  const unsupported=run(fixture({canonicalizationRule:'TRIM_AND_GUESS'}));
  assert.ok(unsupported.codes.includes('UNSUPPORTED_CANONICALIZATION'));
});

test('value, currency, time, owner and event failures stay explicit', () => {
  assert.ok(run(fixture({pixel:{conversionValue:62}})).codes.includes('VALUE_CURRENCY_MISMATCH'));
  assert.ok(run(fixture({webhook:{currency:'usd'}})).codes.includes('VALUE_CURRENCY_MISMATCH'));
  assert.ok(run(fixture({shopify:{observedAt:NOW-3600000}})).codes.includes('EVIDENCE_STALE'));
  assert.ok(run(fixture({pixel:{sourceOwner:'UNKNOWN_OWNER'}})).codes.includes('PIXEL_OWNER_UNCONFIRMED'));
  assert.ok(run(fixture({shopify:{eventName:'checkout_started'}})).codes.includes('SHOPIFY_EVENT_UNCONFIRMED'));
  assert.ok(run(fixture({webhook:{topic:'refunds/create'}})).codes.includes('WEBHOOK_TOPIC_UNCONFIRMED'));
  assert.ok(run(fixture({pixel:{consentState:'DENIED'}})).codes.includes('CONSENT_NOT_GRANTED'));
  assert.ok(run(fixture({pixel:{consentState:'MIXED'}})).codes.includes('CONSENT_NOT_GRANTED'));
  assert.ok(run(fixture({pixel:{consentState:'UNKNOWN'}})).codes.includes('CONSENT_NOT_GRANTED'));
  assert.ok(run(fixture({pixel:{conversionValue:-0},shopify:{conversionValue:-0},webhook:{conversionValue:-0}})).codes.includes('VALUE_CURRENCY_MISMATCH'));
});

test('future-ordered browser evidence cannot be explained by an earlier webhook receipt', () => {
  const result=run(fixture({webhook:{observedAt:NOW-4000}}));
  assert.equal(result.artifactContractPassed,false);
  assert.ok(result.codes.includes('EVIDENCE_SEQUENCE_UNCONFIRMED'));
});

test('identity values with surrounding whitespace are not silently normalized', () => {
  for (const patch of [
    {pixel:{transactionId:' '+TRANSACTION},webhook:{payloadId:' '+TRANSACTION,serverTransactionId:' '+TRANSACTION},shopify:{orderId:' '+TRANSACTION},canonicalizationRule:'NONE'},
    {shopify:{eventId:' shopify-event-fixture'}},
    {webhook:{serverTransactionId:TRANSACTION+' '}}
  ]) {
    const result=run(fixture(patch));
    assert.equal(result.artifactContractPassed,false);
    assert.equal(Object.prototype.hasOwnProperty.call(result.identity||{},'transactionFingerprint'),false);
  }
});

test('PII and unexpected fields are rejected before any identity is returned', () => {
  for (const input of [fixture({pixel:{email:'buyer@example.invalid'}}), fixture({webhook:{customer:{id:'1'}}}),
    fixture({shopify:{postalCode:'A1A 1A1'}}), fixture({pixel:{ipAddress:'192.0.2.1'}}), fixture({extra:'x'})]) {
    const result=run(input), encoded=JSON.stringify(result);
    assert.equal(result.artifactContractPassed,false); assert.equal(result.identity,undefined);
    assert.equal(encoded.includes('buyer@example.invalid'),false); assert.equal(encoded.includes(TRANSACTION),false);
  }
  assert.ok(run(fixture({pixel:{email:'buyer@example.invalid'}})).codes.includes('UNSAFE_PII_PRESENT'));
  assert.ok(run(fixture({extra:'x'})).codes.includes('UNEXPECTED_FIELD'));
});

test('every refused late-stage contract withholds all fingerprints', () => {
  for (const input of [fixture({pixel:{conversionValue:62}}), fixture({webhook:{topic:'refunds/create'}}),
    fixture({pixel:{consentState:'DENIED'}}), fixture({webhook:{observedAt:NOW-4000}})]) {
    const result=run(input), encoded=JSON.stringify(result);
    assert.equal(result.artifactContractPassed,false);
    assert.equal(encoded.includes('Fingerprint'),false);
  }
});

test('throwing artifact getters and proxies return a generic refusal instead of throwing or leaking', () => {
  const throwing=fixture();
  Object.defineProperty(throwing.pixel,'transactionId',{enumerable:true,get(){throw Error('PRIVATE_VALUE');}});
  assert.doesNotThrow(()=>run(throwing));
  assert.deepEqual(run(throwing),{
    schemaVersion:1,evidenceKind:'controlled_transaction_continuity_contract',offlineAssessment:true,receiptOnly:true,
    readOnly:true,providerReads:0,providerWrites:false,conversionUploads:0,conversionReplays:0,cartWrites:0,orderWrites:0,
    runtimeDispatchVerified:false,providerReceiptStatusVerified:false,transactionIdentityVerified:false,
    duplicateCountingVerified:false,artifactContractPassed:false,codes:['ARTIFACT_READ_FAILED']
  });
  const proxy=new Proxy(fixture(),{ownKeys(){throw Error('PRIVATE_PROXY_VALUE');}});
  const result=run(proxy);
  assert.deepEqual(result.codes,['ARTIFACT_READ_FAILED']);
  assert.equal(JSON.stringify(result).includes('PRIVATE'),false);
});

test('salt and validation window are mandatory and never appear in output', () => {
  const noSalt=assess(fixture(),{now:NOW}); assert.ok(noSalt.codes.includes('HASH_SALT_REQUIRED'));
  const badWindow=run(fixture(),{maxAgeMs:86400001}); assert.ok(badWindow.codes.includes('INVALID_VALIDATION_WINDOW'));
  assert.equal(JSON.stringify(run(fixture())).includes(SALT),false);
});

test('contract output cannot claim provider counting or cross-action deduplication', () => {
  const result=run(fixture());
  assert.equal(result.duplicateCountingVerified,false);
  assert.match(result.limitation,/Provider counted status.*remain unverified/);
});
