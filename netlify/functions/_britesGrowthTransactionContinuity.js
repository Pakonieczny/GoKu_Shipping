'use strict';

// Pure validation for evidence captured during a separately authorized,
// controlled Shopify checkout. This module does not fetch, persist, upload,
// reconcile or mutate anything. Inputs are treated as untrusted artifacts and
// raw order/event/transaction identifiers are never returned.
const crypto = require('node:crypto');

const SHOPIFY_GID_RULE = 'SHOPIFY_ORDER_GID_NUMERIC_SUFFIX_V1';
const NO_RULE = 'NONE';
const DESTINATION = /^AW-\d{5,20}\/[A-Za-z0-9_-]{1,100}$/;
const CURRENCY = /^[A-Z]{3}$/;
const SOURCE_OWNERS = new Set(['GOOGLE_YOUTUBE_APP', 'SHOPIFY_CUSTOM_PIXEL', 'GOOGLE_TAG_MANAGER']);
const CONSENT_STATES = new Set(['GRANTED', 'DENIED', 'MIXED', 'UNKNOWN']);
const TOP_FIELDS = new Set(['pixel', 'shopify', 'webhook', 'configuredDestination', 'canonicalizationRule']);
const PIXEL_FIELDS = new Set(['sourceOwner', 'eventName', 'fired', 'hitCount', 'sendTo', 'transactionId', 'conversionValue', 'currency', 'observedAt', 'consentState']);
const SHOPIFY_FIELDS = new Set(['eventName', 'eventId', 'orderId', 'conversionValue', 'currency', 'observedAt']);
const WEBHOOK_FIELDS = new Set(['topic', 'payloadId', 'serverTransactionId', 'conversionValue', 'currency', 'observedAt']);
const PII_KEYS = /^(?:email|e-mail|phone|telephone|customer|customer_?id|buyer|buyer_?id|contact|address|street|city|province|state|country|postal(?:_?code)?|zip(?:_?code)?|first_?name|last_?name|full_?name|shipping|billing|ip(?:_?address)?|user_?agent)$/i;
const PLACEHOLDER = /^(?:undefined|null|not set|unknown|button-confirm|congrats|thank_you|buy|page view|conversion tracking google ads|\{\{.*\}\}|\[object object\])$/i;

const plainObject = value => !!value && typeof value === 'object' && !Array.isArray(value)
  && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);

function base() {
  return {
    schemaVersion: 1,
    evidenceKind: 'controlled_transaction_continuity_contract',
    offlineAssessment: true,
    receiptOnly: true,
    readOnly: true,
    providerReads: 0,
    providerWrites: false,
    conversionUploads: 0,
    conversionReplays: 0,
    cartWrites: 0,
    orderWrites: 0,
    runtimeDispatchVerified: false,
    providerReceiptStatusVerified: false,
    transactionIdentityVerified: false,
    duplicateCountingVerified: false,
    artifactContractPassed: false,
    codes: []
  };
}

function piiKeyPresent(value, depth = 0, seen = new Set()) {
  if (!value || typeof value !== 'object' || depth > 12 || seen.has(value)) return false;
  seen.add(value);
  for (const [key, item] of Object.entries(value)) {
    if (PII_KEYS.test(key)) return true;
    if (piiKeyPresent(item, depth + 1, seen)) return true;
  }
  return false;
}

function exactFields(value, allowed) {
  return plainObject(value) && Object.keys(value).every(key => allowed.has(key));
}

function validTime(value, now, maxAgeMs) {
  return Number.isFinite(value) && value <= now && now - value <= maxAgeMs;
}

function validMoney(value) {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 && !Object.is(value, -0);
}

function validIdentity(value, maximum = 256) {
  return typeof value === 'string' && value.length > 0 && Buffer.byteLength(value, 'utf8') <= maximum
    && value === value.trim() && !/[\u0000-\u001f\u007f]/.test(value);
}

function invalidPixelIdentity(value) {
  if (!validIdentity(value, 64)) return value ? 'PIXEL_ID_OVER_64' : 'PIXEL_ID_MISSING';
  if (/^GG_/i.test(value)) return 'PIXEL_ID_GOOGLE_GENERATED';
  if (PLACEHOLDER.test(value) || /^\d{1,5}$/.test(value)) return 'PIXEL_ID_PLACEHOLDER';
  return null;
}

function hmac(salt, kind, value) {
  return crypto.createHmac('sha256', salt).update(kind).update('\0').update(value, 'utf8').digest('hex');
}

function assessUnsafe(input, {salt, now = Date.now(), maxAgeMs = 15 * 60 * 1000} = {}) {
  const out = base();
  const codes = new Set();
  const fail = code => codes.add(code);
  if (!plainObject(input)) fail('INVALID_INPUT');
  if (piiKeyPresent(input)) fail('UNSAFE_PII_PRESENT');
  if (!exactFields(input, TOP_FIELDS) || !exactFields(input?.pixel, PIXEL_FIELDS)
      || !exactFields(input?.shopify, SHOPIFY_FIELDS) || !exactFields(input?.webhook, WEBHOOK_FIELDS)) fail('UNEXPECTED_FIELD');
  if (typeof salt !== 'string' || Buffer.byteLength(salt, 'utf8') < 16) fail('HASH_SALT_REQUIRED');
  if (!Number.isFinite(now) || !Number.isFinite(maxAgeMs) || maxAgeMs <= 0 || maxAgeMs > 24 * 60 * 60 * 1000) fail('INVALID_VALIDATION_WINDOW');
  if (codes.size) return {...out, codes: [...codes].sort()};

  const pixel = input.pixel, shopify = input.shopify, webhook = input.webhook;
  const times = [pixel.observedAt, shopify.observedAt, webhook.observedAt];
  if (times.some(value => !validTime(value, now, maxAgeMs))) fail('EVIDENCE_STALE');
  if (times.every(Number.isFinite) && webhook.observedAt < Math.max(pixel.observedAt, shopify.observedAt)) fail('EVIDENCE_SEQUENCE_UNCONFIRMED');

  if (!SOURCE_OWNERS.has(pixel.sourceOwner) || !CONSENT_STATES.has(pixel.consentState)) fail('PIXEL_OWNER_UNCONFIRMED');
  if (pixel.consentState !== 'GRANTED') fail('CONSENT_NOT_GRANTED');
  if (pixel.eventName !== 'conversion' || pixel.fired !== true) fail('PIXEL_CONVERSION_NOT_FIRED');
  if (!Number.isSafeInteger(pixel.hitCount) || pixel.hitCount !== 1) fail(pixel.hitCount > 1 ? 'MULTIPLE_PURCHASE_HITS' : 'PIXEL_HIT_COUNT_UNCONFIRMED');
  const pixelIdentityProblem = invalidPixelIdentity(pixel.transactionId);
  if (pixelIdentityProblem) fail(pixelIdentityProblem);

  if (typeof input.configuredDestination !== 'string' || !DESTINATION.test(input.configuredDestination)
      || typeof pixel.sendTo !== 'string' || !DESTINATION.test(pixel.sendTo)
      || pixel.sendTo !== input.configuredDestination) fail('DESTINATION_MISMATCH');

  if (shopify.eventName !== 'checkout_completed' || !validIdentity(shopify.eventId, 256)) fail('SHOPIFY_EVENT_UNCONFIRMED');
  if (!validIdentity(shopify.orderId, 256)) fail('SHOPIFY_ORDER_ID_INVALID');
  if (!['orders/paid', 'orders/create'].includes(webhook.topic)) fail('WEBHOOK_TOPIC_UNCONFIRMED');
  if (!validIdentity(webhook.payloadId, 256) || !validIdentity(webhook.serverTransactionId, 64)) fail('SERVER_ORDER_ID_INVALID');

  const exactPixelServer = !pixelIdentityProblem && pixel.transactionId === webhook.serverTransactionId;
  const exactWebhookServer = webhook.payloadId === webhook.serverTransactionId;
  if (!exactPixelServer || !exactWebhookServer) fail('TRANSACTION_ID_MISMATCH');

  const rule = input.canonicalizationRule == null ? NO_RULE : input.canonicalizationRule;
  let shopifyWebhookExact = shopify.orderId === webhook.payloadId;
  let canonicalized = false, canonicalEquivalent = false;
  if (!shopifyWebhookExact) {
    if (rule === SHOPIFY_GID_RULE) {
      canonicalized = true;
      const match = /^gid:\/\/shopify\/Order\/(\d{1,20})$/.exec(shopify.orderId);
      canonicalEquivalent = !!match && /^\d{1,20}$/.test(webhook.payloadId) && match[1] === webhook.payloadId;
      if (!canonicalEquivalent) fail('SHOPIFY_ID_SHAPE_MISMATCH');
    } else if (rule === NO_RULE) fail('SHOPIFY_ID_SHAPE_MISMATCH');
    else fail('UNSUPPORTED_CANONICALIZATION');
  } else if (![NO_RULE, SHOPIFY_GID_RULE].includes(rule)) fail('UNSUPPORTED_CANONICALIZATION');

  const money = [pixel, shopify, webhook];
  if (money.some(item => !validMoney(item.conversionValue) || !CURRENCY.test(item.currency))
      || !money.every(item => item.conversionValue === pixel.conversionValue && item.currency === pixel.currency)) fail('VALUE_CURRENCY_MISMATCH');

  const identity = {
    pixelToServerExact: exactPixelServer,
    webhookPayloadToServerExact: exactWebhookServer,
    shopifyToWebhookExact: shopifyWebhookExact,
    shopifyToWebhookCanonical: canonicalEquivalent,
    canonicalization: {rule, applied: canonicalized, equivalent: shopifyWebhookExact || canonicalEquivalent}
  };
  const passed = codes.size === 0;
  // Fingerprints are success evidence, not a partial-validation oracle. A
  // refused artifact never emits even one independently valid fingerprint.
  if (passed) {
    identity.transactionFingerprint = hmac(salt, 'transaction', pixel.transactionId);
    identity.shopifyEventFingerprint = hmac(salt, 'shopify-event', shopify.eventId);
    identity.destinationFingerprint = hmac(salt, 'destination', pixel.sendTo);
  }
  return {
    ...out,
    artifactContractPassed: passed,
    runtimeDispatchVerified: false,
    transactionIdentityVerified: passed,
    duplicateCountingVerified: false,
    codes: [...codes].sort(),
    sourceOwner: SOURCE_OWNERS.has(pixel.sourceOwner) ? pixel.sourceOwner : 'UNCONFIRMED',
    consentState: CONSENT_STATES.has(pixel.consentState) ? pixel.consentState : 'UNKNOWN',
    identity,
    limitation: passed
      ? 'The sanitized controlled artifacts establish exact website-to-server transaction identity. Provider counted status and cross-action duplicate handling remain unverified.'
      : 'The supplied artifacts do not establish exact website-to-server transaction continuity.'
  };
}

function assess(input, options = {}) {
  try {
    return assessUnsafe(input, options);
  } catch (_) {
    // Artifacts are untrusted and may contain throwing getters/proxies. The
    // validator must fail closed without echoing a value or an exception.
    return {...base(), codes: ['ARTIFACT_READ_FAILED']};
  }
}

module.exports = {assess, SHOPIFY_GID_RULE, NO_RULE};
