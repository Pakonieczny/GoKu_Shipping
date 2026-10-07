'use strict';

// A reviewed, version-bound addition to one existing private recommendation.
// No catalogue, provider, inference, campaign or budget writes are reachable.
const {STOP_AT, hash, productIssueHolds, validateDossier} = require('./_britesGrowth');
const SANDBOX = 'Brites_Growth_Sandbox';
const FIELDS = new Set(['productId', 'baseDossierVersion', 'recommendationIndex', 'sourceIds', 'keywords', 'reviewed']);
const REC_FIELDS = new Set(['channel', 'basis', 'action', 'measure', 'sourceIds', 'keywords', 'negativeKeywords']);
const VERSION = /^[a-f0-9]{64}$/;
const PRODUCT = /^gid:\/\/shopify\/Product\/[1-9]\d{0,19}$/;
const SOURCE = /^[a-zA-Z0-9:_-]{1,100}$/;
const LIVE_AGE = 5 * 60000, SOURCE_AGE = 30 * 86400000;
const NO_EXTERNAL_WRITES = {sandboxOnly:true, providerCalls:0, inferenceCalls:0, campaignWrites:0, budgetWrites:0};
const plain = value => !!value && typeof value === 'object' && !Array.isArray(value)
  && [Object.prototype, null].includes(Object.getPrototypeOf(value));
const exactFields = (value, fields) => plain(value) && Reflect.ownKeys(value).every(key => typeof key === 'string' && fields.has(key));
const keywordKey = value => value.normalize('NFKC').replace(/\s+/gu, ' ').trim().toLowerCase();
const unsafeText = /(?:system|developer|assistant|author|internal|hidden)\s+(?:prompt|message|instructions?)|(?:prompt|instruct(?:ion)?)\s+(?:the\s+)?(?:assistant|concierge|model)|\b(?:assistant|concierge|model)\s+(?:must|should|shall|needs? to)\b|\bignore (?:all |any )?(?:prior|previous|system|developer) instructions?\b|^(?:apply|activate|publish|launch|upload|enable|pause|delete|execute)\b|^(?:set|raise|increase|change)\b[^.]{0,32}\bbudget\b|\b(?:automatic|auto)\s*-?\s*(?:apply|activation|publish)/i;

function safePhrase(value) {
  if (typeof value !== 'string') return null;
  const text = value.trim(), normalized = text.normalize('NFKC');
  if (!text || text.length > 120 || !/^[\p{L}\p{M}\p{N} '&’\-]+$/u.test(text)
      || !/\p{L}/u.test(text) || !/^[\p{L}\p{M}\p{N} '&’\-]+$/u.test(normalized)
      || unsafeText.test(normalized)) return null;
  return text;
}
function fresh(value, at, age) {return Number.isFinite(value) && value <= at + 60000 && at - value <= age;}
function sameSet(left, right) {return left.length === right.length && new Set(left).size === left.length && left.every(value => right.includes(value));}
function productMatches(product, dossier, id, at) {
  if (!plain(product) || product.id !== id || typeof product.handle !== 'string' || !/^[a-z0-9_-]{1,180}$/.test(product.handle)
      || product.handle !== dossier?.handle || dossier?.productId !== id || !fresh(product.checkedAt, at, LIVE_AGE)) return false;
  return ['https://britesjewelry.com/products/', 'https://www.britesjewelry.com/products/'].some(base => product.url === base + product.handle);
}
function issueState(records, id) {
  if (!Array.isArray(records) || records.length > 1) return null;
  if (!records.length) return {recommendationHold:false};
  const record = records[0];
  if (!plain(record) || record.productId !== id || !Array.isArray(record.issues) || record.issues.length > 100) return null;
  if (record.issues.some(issue => !plain(issue) || !['open', 'resolved'].includes(issue.status)
      || !['identity', 'style', 'options', 'material', 'matching', 'history', 'content'].includes(issue.kind)
      || issue.blocks !== undefined && (!Array.isArray(issue.blocks) || issue.blocks.some(block => !['recommendation', 'cart', 'meaning'].includes(block))))) return null;
  return productIssueHolds(record);
}
function storyState(records, id, version) {
  if (!Array.isArray(records) || records.length > 1 || records.some(record => !plain(record) || record.productId !== id
      || !['approved', 'draft', 'rejected'].includes(record.status)
      || record.status === 'approved' && (typeof record.baseDossierVersion !== 'string' || !VERSION.test(record.baseDossierVersion)))) return null;
  return {bound:records.some(record => record.status === 'approved' && record.baseDossierVersion === version)};
}
function tokens(value) {return keywordKey(value).match(/[\p{L}\p{N}]+/gu) || [];}
function contains(haystack, needle) {return needle.length > 0 && haystack.some((_, index) => needle.every((word, offset) => haystack[index + offset] === word));}
function suppressed(term, negative) {const a = tokens(term), b = tokens(negative);return contains(a, b) || contains(b, a);}

function createKeywordRevision({service, now = Date.now} = {}) {
  const blocked = (code, extra = {}) => ({ok:false, blocked:true, changed:false, ...NO_EXTERNAL_WRITES, code, ...extra});
  const uncertain = code => blocked(code, {changed:null, researchWriteAttempted:true, recheckRequired:true});
  async function revise(body) {
    if (service?.namespace !== SANDBOX) return blocked('SANDBOX_REQUIRED');
    if (typeof now !== 'function' || ['getProduct', 'research', 'productIssues', 'storySupplements', 'saveDossier'].some(name => typeof service[name] !== 'function')) return blocked('REVISION_SERVICE_UNAVAILABLE');
    if (!exactFields(body, FIELDS) || typeof body.productId !== 'string' || !PRODUCT.test(body.productId)
        || typeof body.baseDossierVersion !== 'string' || !VERSION.test(body.baseDossierVersion)
        || !Number.isInteger(body.recommendationIndex) || body.recommendationIndex < 0 || body.recommendationIndex > 39
        || body.reviewed !== true || !Array.isArray(body.sourceIds) || !body.sourceIds.length || body.sourceIds.length > 12
        || body.sourceIds.some(id => typeof id !== 'string' || !SOURCE.test(id)) || new Set(body.sourceIds).size !== body.sourceIds.length
        || !Array.isArray(body.keywords) || !body.keywords.length || body.keywords.length > 8) return blocked('INVALID_KEYWORD_REVISION');
    const keywords = body.keywords.map(safePhrase);
    if (keywords.some(value => value === null) || new Set(keywords.map(value => value === null ? null : keywordKey(value))).size !== keywords.length) return blocked('INVALID_KEYWORDS');
    let at;
    try {at = now();} catch {return blocked('REVISION_CLOCK_UNAVAILABLE');}
    if (!Number.isFinite(at)) return blocked('REVISION_CLOCK_UNAVAILABLE');
    if (at >= STOP_AT) return blocked('GROWTH_STOPPED', {stopped:true});
    const id = body.productId, version = body.baseDossierVersion;
    const reads = await Promise.allSettled([
      Promise.resolve().then(() => service.getProduct(id)), Promise.resolve().then(() => service.research([id])),
      Promise.resolve().then(() => service.productIssues([id])), Promise.resolve().then(() => service.storySupplements([id]))
    ]);
    if (reads.some(result => result.status !== 'fulfilled')) return blocked('REVISION_READ_UNAVAILABLE');
    const [product, dossiers, issues, supplements] = reads.map(result => result.value);
    if (!Array.isArray(dossiers) || dossiers.length !== 1 || !plain(dossiers[0]) || dossiers[0].productId !== id
        || dossiers[0].status !== 'approved' || dossiers[0].version !== version) return blocked('RESEARCH_VERSION_CHANGED', {recheckRequired:true});
    const dossier = dossiers[0];
    try {at = now();} catch {return blocked('REVISION_CLOCK_UNAVAILABLE');}
    if (!Number.isFinite(at)) return blocked('REVISION_CLOCK_UNAVAILABLE');
    if (at >= STOP_AT) return blocked('GROWTH_STOPPED', {stopped:true});
    if (!productMatches(product, dossier, id, at)) return blocked('LIVE_PRODUCT_CHECK_REQUIRED');
    const holds = issueState(issues, id), story = storyState(supplements, id, version);
    if (!holds) return blocked('PRODUCT_ISSUES_UNAVAILABLE');
    if (holds.recommendationHold || holds.cartHold || holds.meaningHold || product.recommendationHold === true || dossier.evidenceHolds?.recommendationHold === true
        || dossier.privateProposal === true || dossier.proposalOnly === true || dossier.reviewStatus === 'proposed') return blocked('PRODUCT_RECOMMENDATION_HELD');
    if (!story) return blocked('STORY_STATE_UNAVAILABLE');
    if (story.bound) return blocked('STORY_REBIND_REQUIRED');
    const recommendations = dossier.recommendations;
    if (!Array.isArray(recommendations) || recommendations.length > 40 || body.recommendationIndex >= recommendations.length) return blocked('KEYWORD_RECOMMENDATION_REQUIRED');
    const selected = recommendations[body.recommendationIndex];
    if (!exactFields(selected, REC_FIELDS) || selected.channel !== 'keywords' || selected.basis !== 'hypothesis'
        || !Array.isArray(selected.sourceIds) || !sameSet(selected.sourceIds, body.sourceIds)) return blocked('KEYWORD_RECOMMENDATION_REQUIRED');
    if (!Array.isArray(dossier.sources) || !dossier.sources.length || dossier.sources.length > 40
        || dossier.sources.some(source => !plain(source) || typeof source.id !== 'string' || !SOURCE.test(source.id))
        || new Set(dossier.sources.map(source => source.id)).size !== dossier.sources.length) return blocked('SOURCE_REVIEW_REQUIRED');
    const sources = new Map(dossier.sources.map(source => [source.id, source]));
    if (body.sourceIds.some(sourceId => !sources.has(sourceId) || sources.get(sourceId).reviewed !== true
        || !fresh(sources.get(sourceId).checkedAt, at, SOURCE_AGE))) return blocked('SOURCE_REVIEW_REQUIRED');
    const existing = selected.keywords == null ? [] : selected.keywords;
    if (!Array.isArray(existing) || existing.length > 30 || existing.some(value => safePhrase(value) === null)
        || new Set(existing.map(keywordKey)).size !== existing.length) return blocked('EXISTING_KEYWORDS_INVALID');
    const existingKeys = new Set(existing.map(keywordKey));
    if (keywords.some(value => existingKeys.has(keywordKey(value)))) return blocked('KEYWORDS_ALREADY_PRESENT');
    if (existing.length + keywords.length > 30) return blocked('KEYWORD_LIMIT_REACHED');
    const negatives = [];
    for (const rec of recommendations) {
      if (rec?.negativeKeywords == null) continue;
      if (!Array.isArray(rec.negativeKeywords) || rec.negativeKeywords.length > 30
          || rec.negativeKeywords.some(term => safePhrase(term) === null)
          || new Set(rec.negativeKeywords.map(keywordKey)).size !== rec.negativeKeywords.length) return blocked('EXISTING_KEYWORDS_INVALID');
      negatives.push(...rec.negativeKeywords);
    }
    if (keywords.some(term => negatives.some(negative => suppressed(term, negative)))) return blocked('KEYWORD_NEGATIVE_CONFLICT');
    let next, validation;
    try {
      next = structuredClone(dossier);
      for (const field of ['version', 'status', 'savedAt', 'validation']) delete next[field];
      next.recommendations[body.recommendationIndex].keywords = [...existing, ...keywords];
      validation = validateDossier(next, product, at);
    } catch {return blocked('DOSSIER_VALIDATION_REQUIRED');}
    if (!validation.ok || validation.status !== 'approved') return blocked('DOSSIER_VALIDATION_REQUIRED');
    let saved;
    try {saved = await service.saveDossier(next, {expectedVersion:version});}
    catch (error) {
      if (['RESEARCH_VERSION_CHANGED', 'STORY_REBIND_REQUIRED', 'PRODUCT_HOLD'].includes(error?.code)) return blocked(error.code, {recheckRequired:true});
      return uncertain('REVISION_SAVE_UNCONFIRMED');
    }
    // Storage can complete the research transaction before a linked queue
    // update fails. An unconfirmed return requires a private read, never retry.
    if (!plain(saved) || saved.ok !== true || saved.productId !== id || typeof saved.version !== 'string'
        || !VERSION.test(saved.version) || saved.version !== hash(next) || saved.version === version || saved.validation?.ok !== true || saved.validation?.status !== 'approved') return uncertain('REVISION_SAVE_UNCONFIRMED');
    return {ok:true, changed:true, ...NO_EXTERNAL_WRITES, productId:id, baseDossierVersion:version,
      version:saved.version, recommendationIndex:body.recommendationIndex, keywordCount:next.recommendations[body.recommendationIndex].keywords.length};
  }
  return {revise};
}

module.exports = {createKeywordRevision};
