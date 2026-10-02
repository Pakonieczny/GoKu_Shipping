'use strict';

const { createHash } = require('node:crypto');
const milestoneDiscovery = require('./_britesMilestoneDiscovery');

const MILESTONES = Object.freeze(['remembrance', 'graduation', 'wedding', 'birth',
  'achievement', 'recovery', 'season', 'christmas']);
const HOLD_KINDS = new Set(['identity', 'material', 'style', 'options', 'matching',
  'history', 'content']);
const INTERNAL_DIRECTION = /(?:system|developer|assistant|author|internal|hidden)\s+(?:prompt|message|instructions?)|(?:prompt|instruct(?:ion)?)\s+(?:the\s+)?(?:assistant|model)|\b(?:assistant|model|concierge)\s+(?:must|should|shall|needs? to)\b|\bignore (?:all |any )?(?:prior|previous|system|developer) instructions?\b/i;
const ACTIVATION_DIRECTION = /\b(?:apply|activate|publish|launch|upload|enable|pause|delete)\b|\b(?:set|raise|increase|change)\b[^.]{0,32}\bbudget\b|\bauto(?:matic(?:ally)?)?[- ]?(?:apply|activate|publish)\b/i;
const ABSOLUTE_PROMISE = /\b(?:guarantee[ds]?|heal(?:s|ed|ing)?|cure[sd]?|treats?|prevents?|protects?|wards? off|afterlife|heaven|divine intervention|manifest(?:s|ed|ing)?|bring(?:s|ing)? (?:good )?luck)\b/i;
const QUALIFIED_INTERPRETATION = /\b(?:may|might|could|can be|some|personal(?:ly)?|interpret(?:ed|ation)?|associated|suggest(?:s|ed)?|var(?:y|ies)|depends)\b/i;
const REVIEW_PROJECTION_HARDENED = Symbol.for('brites.growth.ads.reviewProjectionHardening19');

function compactText(value, limit) {
  if (typeof value !== 'string') return null;
  const text = value.replace(/\s+/g, ' ').trim();
  if (!text || text.length > limit || /[<>]|https?:\/\//i.test(text) || INTERNAL_DIRECTION.test(text) || ACTIVATION_DIRECTION.test(text)) return null;
  return text;
}
function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]));
}
function digest(value) {
  return createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
}
function httpsUrl(value) {
  try {
    const url = new URL(String(value || ''));
    return url.protocol === 'https:' && !url.username && !url.password && url.hostname.includes('.') ? url : null;
  } catch { return null; }
}
function normalizedHost(url) { return url.hostname.toLowerCase().replace(/^www\./, ''); }
function sameHostFamily(left, right) { return left === right || left.endsWith('.' + right) || right.endsWith('.' + left); }
function keywordTokens(value) {
  return String(value || '').normalize('NFKC').toLowerCase().match(/[\p{L}\p{N}]+/gu) || [];
}
function containsWholeTokenPhrase(haystack, needle) {
  if (!haystack.length || !needle.length || needle.length > haystack.length) return false;
  outer: for (let offset = 0; offset <= haystack.length - needle.length; offset++) {
    for (let index = 0; index < needle.length; index++) if (haystack[offset + index] !== needle[index]) continue outer;
    return true;
  }
  return false;
}
function suppressiveKeywordConflict(positive, negative) {
  const left = keywordTokens(positive), right = keywordTokens(negative);
  return left.length > 0 && right.length > 0 &&
    (containsWholeTokenPhrase(left, right) || containsWholeTokenPhrase(right, left));
}
function operatorPacketHasSuppressiveKeywordConflict(packet) {
  const positive = Array.isArray(packet?.positiveKeywords) ? packet.positiveKeywords : [];
  const negative = Array.isArray(packet?.negativeKeywords) ? packet.negativeKeywords : [];
  return positive.some(left => negative.some(right => suppressiveKeywordConflict(left?.term, right?.term)));
}
function hardenReviewProjection(projection) {
  if (!projection || typeof projection.projectRecommendations !== 'function' || projection[REVIEW_PROJECTION_HARDENED]) return projection;
  const original = projection.projectRecommendations;
  projection.projectRecommendations = function (...args) {
    // Keep the read adapter fail-closed even when it is paired with an older
    // projection module during a rolling deployment.
    if (args[0]?.evidenceHolds?.cartHold === true) return null;
    const result = original.apply(this, args);
    return operatorPacketHasSuppressiveKeywordConflict(result?.operatorReviewPacket) ? null : result;
  };
  Object.defineProperty(projection, REVIEW_PROJECTION_HARDENED, { value: true });
  return projection;
}
function clearlyRetailerEditorialUrl(url, own = false) {
  if (!url || own) return false;
  const host = normalizedHost(url), path = url.pathname.toLowerCase();
  const directCommercePath = /(?:^|\/)(?:products?|listing|shop|store|cart|checkout)(?:\/|$)/i.test(path);
  const knownMarketplace = /(?:^|\.)(?:etsy|amazon|ebay|walmart)\.(?:com|ca|co\.uk)$/i.test(host);
  const commerceHost = /(?:^|[._-])(?:retail(?:er)?|shop|store|boutique|marketplace|jewel(?:er|ers|ry)|jewell(?:er|ers|ery)|gifts?)(?:[._-]|$)/i.test(host);
  return directCommercePath || knownMarketplace || commerceHost;
}
function activeIssueHold(issueRecord, productId) {
  if (!issueRecord) return false;
  if (String(issueRecord.productId || '') !== productId || !Array.isArray(issueRecord.issues)) return true;
  return issueRecord.issues.some(issue => {
    if (!issue || ['resolved', 'closed', 'dismissed'].includes(String(issue.status || '').toLowerCase())) return false;
    return HOLD_KINDS.has(issue.kind) || (Array.isArray(issue.blocks) && issue.blocks.some(block => ['recommendation', 'meaning', 'cart'].includes(block)));
  });
}

// Convert approved, exact-product milestone interpretations into immutable
// creative hypotheses for a human review queue. This projection deliberately
// has no dispatch/apply method and carries explicit zero-write controls.
function projectApprovedMeaningHypotheses({ dossier, product, issueRecord = null, issueState = 'available', at = Date.now() } = {}) {
  const d = dossier, productId = String(d?.productId || ''), handle = String(d?.handle || ''), dossierVersion = String(d?.version || '');
  if (!d || d.status !== 'approved' || d.privateProposal === true || d.proposalOnly === true || d.reviewStatus === 'proposed' ||
    issueState !== 'available' || !/^gid:\/\/shopify\/Product\/\d+$/.test(productId) || String(product?.id || product?.productId || '') !== productId ||
    !/^[a-z0-9_-]{1,180}$/.test(handle) || product?.handle !== handle || !/^[a-f0-9]{64}$/i.test(dossierVersion) || d.currentDossierVersion !== dossierVersion ||
    !Number.isFinite(d.savedAt) || d.savedAt > at + 60000 || d.evidenceHolds?.recommendationHold === true || d.evidenceHolds?.meaningHold === true ||
    d.evidenceHolds?.cartHold === true || product?.recommendationHold === true || product?.meaningHold === true || product?.cartHold === true || activeIssueHold(issueRecord, productId)) return null;
  if (!Array.isArray(d.sources) || !d.sources.length || d.sources.length > 40 || !Array.isArray(d.meanings) || !d.meanings.length || d.meanings.length > 40) return null;

  const competitorHosts = new Set((Array.isArray(d.competitors) ? d.competitors : []).flatMap(value => {
    const url = httpsUrl(value?.url); return url ? [normalizedHost(url)] : [];
  }));
  const sources = new Map();
  for (const source of d.sources) {
    const id = String(source?.id || ''), url = httpsUrl(source?.url), title = compactText(source?.title, 300);
    const excerpt = typeof source?.excerpt === 'string' ? source.excerpt.replace(/\s+/g, ' ').trim() : '';
    if (!/^[a-zA-Z0-9:_-]{1,100}$/.test(id) || sources.has(id) || source?.reviewed !== true || source?.privateProposal === true || source?.proposalOnly === true ||
      source?.reviewStatus === 'proposed' || !url || !title || !excerpt || excerpt.length > 2000 || !Number.isFinite(source.checkedAt) || source.checkedAt > at + 60000 || at - source.checkedAt > 30 * 86400000) continue;
    const host = normalizedHost(url), own = host === 'britesjewelry.com';
    if ([...competitorHosts].some(other => sameHostFamily(host, other)) || clearlyRetailerEditorialUrl(url, own) || (!own && /(?:^|[./_-])(?:internal|private|admin|staging|author|prompt)(?:[./_-]|$)/i.test(host + url.pathname))) continue;
    const binding = { id, title, url: url.href, checkedAt: source.checkedAt, reviewed: true };
    sources.set(id, { ...binding, sourceVersion: digest({ ...binding, excerpt }) });
  }

  const candidates = [];
  for (const meaning of d.meanings) {
    if (!meaning || meaning.kind !== 'interpretation' || meaning.privateProposal === true || meaning.proposalOnly === true || meaning.reviewStatus === 'proposed' ||
      (meaning.status != null && meaning.status !== 'approved')) continue;
    const text = compactText(meaning.text, 1500), context = compactText(meaning.context, 300), sourceIds = meaning.sourceIds;
    if (!text || !context || ABSOLUTE_PROMISE.test(text) || ABSOLUTE_PROMISE.test(context) || !QUALIFIED_INTERPRETATION.test(text) ||
      !Array.isArray(sourceIds) || !sourceIds.length || sourceIds.length > 8 || new Set(sourceIds).size !== sourceIds.length || sourceIds.some(id => !sources.has(id))) continue;
    const citations = sourceIds.map(id => sources.get(id));
    const projectedMeaning = { kind: 'interpretation', text, context, sources: citations };
    for (const milestone of MILESTONES) {
      if (!milestoneDiscovery.matchingMeanings([projectedMeaning], milestone).length) continue;
      const presentation = milestoneDiscovery.presentation(milestone);
      const seed = { productId, handle, dossierVersion, milestone, text, context, sourceBindings: citations.map(({ id, sourceVersion }) => ({ id, sourceVersion })) };
      candidates.push({
        candidateId: 'meaning_' + digest(seed).slice(0, 24), candidateType: 'creative_meaning_hypothesis', productId, handle, dossierVersion,
        milestone, audienceContext: presentation.label, basis: 'approved_milestone_meaning', evidenceState: 'source_qualified_interpretation',
        hypothesis: `For operator review only: assess whether a ${presentation.label} concept using this exact product and the qualified interpretation below is suitable for a future creative test.`,
        interpretation: text, context, sourceIds: [...sourceIds], sourceBindings: citations.map(value => ({ ...value })),
        reviewState: 'pending_operator_review', operatorReviewRequired: true, measuredLift: false, automaticActivation: false
      });
    }
  }
  if (!candidates.length) return null;
  const candidateIds = candidates.map(value => value.candidateId);
  if (new Set(candidateIds).size !== candidateIds.length) return null;
  return {
    meaningReviewReadiness: { state: 'approved_milestone_hypotheses', dossierVersion, sourceVersionBound: true, candidateCount: candidates.length, measuredLift: false, automaticActivation: false, operatorReviewRequired: true },
    meaningOperatorReviewPacket: { schema: 1, productId, handle, dossierVersion, state: 'pending_operator_review', candidateIds, candidates, providerWrites: false, campaignWrites: false, budgetWrites: false, automaticActivation: false }
  };
}

// Both Ads read handlers load the ordinary review projection before this
// read-only adapter. Harden that shared in-memory module once so a conflicting
// packet cannot be returned through either existing read path.
try { hardenReviewProjection(require('./googleAdsAdDesignResearch')); } catch {}

// Legacy Ads reads are allowed to observe production Firestore, never to update
// it. Some reads try to save an optional cache; this adapter blocks that write
// while allowing the engine's existing cache-failure handling to keep the data.
const WRITE_METHODS = new Set(['set', 'update', 'delete', 'add', 'create', 'commit',
  'recursiveDelete', 'bulkWriter', 'batch', 'terminate', 'settings']);
function readOnlyFirestore(db) {
  const proxies = new WeakMap(), originals = new WeakMap();
  const original = value => value && typeof value === 'object' ? originals.get(value) || value : value;
  function wrap(value) {
    if (!value || typeof value !== 'object') return value;
    if (Array.isArray(value)) return value.map(wrap);
    // Query/reference/snapshot/transaction instances need their methods bound.
    // Data, timestamps and Dates stay ordinary Firestore return values.
    if (value instanceof Date || value.constructor === Object || value.constructor?.name === 'Timestamp' || Buffer.isBuffer(value)) return value;
    if (proxies.has(value)) return proxies.get(value);
    const proxy = new Proxy(value, {
      get(target, key) {
        if (WRITE_METHODS.has(key)) return () => { throw Error('Production Firestore writes are disabled in the growth Ads sandbox.'); };
        if (key === 'runTransaction') return async fn => target.runTransaction(tx => fn(wrap(tx)), { maxAttempts: 1 });
        if (key === 'onSnapshot') return () => { throw Error('Use bounded report reads in the growth Ads sandbox.'); };
        const found = Reflect.get(target, key, target);
        if (typeof found !== 'function') return wrap(found);
        if (key === 'data') return (...args) => found.apply(target, args.map(original));
        if (key === 'forEach') return fn => found.call(target, entry => fn(wrap(entry)));
        return (...args) => {
          const result = found.apply(target, args.map(original));
          return result && typeof result.then === 'function' ? result.then(wrap) : wrap(result);
        };
      }
    });
    proxies.set(value, proxy); originals.set(proxy, value); return proxy;
  }
  return wrap(db);
}
function adminFromEnvironment(env) {
  const admin = require('firebase-admin');
  if (!admin.apps.length) admin.initializeApp({ credential: admin.credential.cert({
    projectId: env.FIREBASE_PROJECT_ID, clientEmail: env.FIREBASE_CLIENT_EMAIL,
    privateKey: String(env.FIREBASE_PRIVATE_KEY || '').replace(/\\n/g, '\n')
  }), storageBucket: env.FIREBASE_STORAGE_BUCKET || 'gokudatabase.firebasestorage.app' });
  const db = readOnlyFirestore(admin.firestore());
  const firestore = new Proxy(admin.firestore, { apply() { return db; } });
  return new Proxy(admin, { get(target, key) { return key === 'firestore' ? firestore : Reflect.get(target, key, target); } });
}
module.exports = { readOnlyFirestore, adminFromEnvironment, projectApprovedMeaningHypotheses,
  suppressiveKeywordConflict, operatorPacketHasSuppressiveKeywordConflict, hardenReviewProjection,
  clearlyRetailerEditorialUrl };
