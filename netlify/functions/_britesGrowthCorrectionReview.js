'use strict';

// Review only. This module has no provider/DB transport or mutation builder.
const crypto = require('node:crypto');
const hash = value => crypto.createHash('sha256').update(typeof value === 'string' ? value : canonical(value), 'utf8').digest('hex');
function canonical(value) {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value && typeof value === 'object') return '{' + Object.keys(value).sort().filter(k => value[k] !== undefined).map(k => JSON.stringify(k) + ':' + canonical(value[k])).join(',') + '}';
  return JSON.stringify(value);
}
function normalizeExactProductId(value) {
  const raw = String(value || '').trim();
  return /^gid:\/\/shopify\/Product\/[1-9]\d*$/.test(raw) ? raw : /^[1-9]\d*$/.test(raw) ? 'gid://shopify/Product/' + raw : null;
}
function correctionPacketDocumentId(value) {
  const id = normalizeExactProductId(value);
  return id ? hash(id).slice(0,40) : null;
}
const object = v => !!v && typeof v === 'object' && !Array.isArray(v);
const digest = v => typeof v === 'string' && /^[a-f0-9]{64}$/.test(v);
function ownProductSource(source, handle) {
  try { const u = new URL(source.url); return source.reviewed === true && u.protocol === 'https:' && !u.username && !u.password && !u.port && ['britesjewelry.com', 'www.britesjewelry.com'].includes(u.hostname) && u.pathname === '/products/' + handle; } catch { return false; }
}
function validateCorrectionProposal(value) {
  if (!object(value)) throw Error('One typed correction proposal is required.');
  const keys = ['dossierVersion', 'expectedTitle', 'expectedDescriptionHtmlSha256', 'expectedProductType', 'sourceIds', 'reviewedIssueIds', 'issueRecordUpdatedAt', 'issueRecordHash', 'snippetPatches', 'draftProductType', 'expectedVariantIdSetSha256', 'expectedOptionLabels'];
  if (Object.keys(value).some(k => !keys.includes(k))) throw Error('Unsupported proposal field; raw queries and request drafts cannot be dispatched.');
  if (!digest(value.dossierVersion) || !digest(value.expectedDescriptionHtmlSha256)) throw Error('Exact approved version and retained body hash are required.');
  for (const [key, max] of [['expectedTitle', 500], ['expectedProductType', 300]]) if (typeof value[key] !== 'string' || value[key].length > max) throw Error('A bounded exact ' + key + ' is required.');
  for (const key of ['sourceIds', 'reviewedIssueIds']) if (!Array.isArray(value[key]) || !value[key].length || value[key].length > 20 || value[key].some(v => typeof v !== 'string' || !v || v.length > 120) || new Set(value[key]).size !== value[key].length) throw Error('Bounded exact ' + key + ' are required.');
  if (!Number.isFinite(value.issueRecordUpdatedAt) || value.issueRecordUpdatedAt < 0 || value.issueRecordHash != null && !digest(value.issueRecordHash)) throw Error('A retained issue-record timestamp and valid optional hash are required.');
  if (!digest(value.expectedVariantIdSetSha256)) throw Error('The retained complete variant ID-set hash is required.');
  if (!Array.isArray(value.expectedOptionLabels) || value.expectedOptionLabels.length > 10 || value.expectedOptionLabels.some(o => !object(o) || Object.keys(o).some(k => !['name', 'values'].includes(k)) || typeof o.name !== 'string' || !o.name || o.name.length > 150 || !Array.isArray(o.values) || o.values.length > 300 || o.values.some(v => typeof v !== 'string' || v.length > 250))) throw Error('Retained option labels and values are required.');
  if (!Array.isArray(value.snippetPatches) || value.snippetPatches.length > 10) throw Error('Provide at most ten exact description patches.');
  for (const patch of value.snippetPatches) {
    if (!object(patch) || Object.keys(patch).some(k => !['field', 'expectedExactSnippet', 'expectedOccurrences', 'replacementExactSnippet'].includes(k)) || patch.field !== 'descriptionHtml' || patch.expectedOccurrences !== 1 || typeof patch.expectedExactSnippet !== 'string' || !patch.expectedExactSnippet || patch.expectedExactSnippet.length > 6000 || typeof patch.replacementExactSnippet !== 'string' || patch.replacementExactSnippet.length > 6000 || /<\/?(?:script|style|iframe|form)\b|\bon\w+\s*=|javascript:/i.test(patch.replacementExactSnippet)) throw Error('Only bounded exact single-occurrence description patches are supported.');
  }
  if (value.draftProductType != null && (typeof value.draftProductType !== 'string' || !value.draftProductType.trim() || value.draftProductType.length > 150)) throw Error('A bounded proposed product type is required.');
  return JSON.parse(JSON.stringify(value));
}
function contentBaseline(product) {
  return { title: product.title, descriptionHtml: product.descriptionHtml, productType: product.productType };
}
function variantStructure(variants) {
  return (variants || []).map(v => ({ id: v.id, options: (v.selectedOptions || []).map(o => ({name:o.name, value:o.value})).sort((a,b) => a.name.localeCompare(b.name)) })).sort((a,b) => a.id.localeCompare(b.id));
}
function variantIdSetHash(variants) {
  // Retained packets use Python json.dumps(sorted(ids)): ASCII IDs, comma-space.
  return hash('[' + (variants || []).map(v => v.id).sort().map(id => JSON.stringify(id)).join(', ') + ']');
}
function optionLabels(variants) {
  const map = new Map();
  for (const v of variants || []) for (const o of v.selectedOptions || []) { if (!map.has(o.name)) map.set(o.name, new Set()); map.get(o.name).add(o.value); }
  return [...map].map(([name, values]) => ({name, values:[...values].sort()})).sort((a,b) => a.name.localeCompare(b.name));
}
function storedPacket({record, productId, handle, dossier, issueRecord, product, now=Date.now()}) {
  const id = normalizeExactProductId(productId), fail = reason => ({state:'unavailable',reason});
  if (!object(record) || Buffer.byteLength(JSON.stringify(record),'utf8') > 80000) return fail('No bounded private correction packet is available for this exact product.');
  const allowed = ['schemaVersion','mode','productId','handle','proposal','savedAt'];
  if (Object.keys(record).some(k => !allowed.includes(k)) || record.schemaVersion !== 1 || record.mode !== 'sandbox_correction_packet' || !Number.isFinite(record.savedAt) || record.savedAt > now + 60000) return fail('The private correction packet schema is unavailable.');
  if (normalizeExactProductId(record.productId) !== id || record.handle !== handle) return fail('The private correction packet identity differs from the selected product.');
  let proposal;
  try { proposal = validateCorrectionProposal(record.proposal); } catch { return fail('The private correction packet is not a valid typed proposal.'); }
  if (!dossier || dossier.productId !== id || dossier.handle !== handle || dossier.status !== 'approved' || dossier.version !== proposal.dossierVersion) return fail('The private correction packet differs from current approved research.');
  const sources = (dossier.sources || []).filter(s => proposal.sourceIds.includes(s.id));
  if (sources.length !== proposal.sourceIds.length || sources.some(s => !ownProductSource(s,handle))) return fail('The private correction packet sources are no longer current exact own-product evidence.');
  const activeIds = (issueRecord?.issues || []).filter(i => i?.status !== 'resolved').map(i => i.id).filter(Boolean).sort();
  if (issueRecord?.productId !== id || issueRecord.updatedAt !== proposal.issueRecordUpdatedAt || canonical(activeIds) !== canonical([...proposal.reviewedIssueIds].sort())) return fail('The private correction packet differs from current exact-product issues.');
  const variants = (product?.variants || []).map(v => ({id:v.id,selectedOptions:Array.isArray(v.selectedOptions)?v.selectedOptions:Array.isArray(v.options)?v.options:[]}));
  const expectedOptions = proposal.expectedOptionLabels.map(o => ({name:o.name,values:[...new Set(o.values)].sort()})).sort((a,b)=>a.name.localeCompare(b.name));
  if (product?.id !== id || product.handle !== handle || product.variantsComplete !== true || !Number.isFinite(product.checkedAt) || product.checkedAt > now + 60000 || now - product.checkedAt > 24*60*60*1000 || variantIdSetHash(variants) !== proposal.expectedVariantIdSetSha256 || hash(optionLabels(variants)) !== hash(expectedOptions)) return fail('The private correction packet differs from the recent complete shared catalogue variant structure.');
  proposal.issueRecordHash = hash(issueRecord);
  return {state:'available',packet:{schemaVersion:1,mode:'sandbox_correction_packet',readOnly:true,canApply:false,executionCompatibility:'not_established',productId:id,handle,proposal,packetVersion:hash({productId:id,handle,proposal})}};
}
function issueHolds(record) {
  const active = (record?.issues || []).filter(i => i.status !== 'resolved'), blocks = new Set(active.flatMap(i => i.blocks || []));
  if (active.some(i => ['identity', 'style', 'options', 'material', 'matching'].includes(i.kind))) { blocks.add('recommendation'); blocks.add('cart'); }
  if (active.some(i => ['history', 'content'].includes(i.kind))) blocks.add('meaning');
  return { recommendationHold: blocks.has('recommendation'), cartHold: blocks.has('cart'), meaningHold: blocks.has('meaning') };
}
function reviewCollectionImpact(read, before, after) {
  if (before.title === after.title && before.productType === after.productType) return {state:'not_required', knownEntries:[], knownExits:[], affectedRules:[], unknownSources:[], note:'Description-only review; no collection write is proposed. Downstream app/feed effects are not established.'};
  const unknown = [], affectedRules = [];
  if (read.coverage?.collections !== 'complete' || read.coverage?.memberships !== 'complete') unknown.push({reason:'Collection/membership pagination is incomplete or unavailable.'});
  for (const c of read.collections || []) {
    const rules = c.ruleSet?.rules || [];
    for (const rule of rules) {
      const column = String(rule.column || '').toUpperCase();
      if (['TYPE','TITLE'].includes(column)) affectedRules.push({collectionId:c.id, title:c.title, rule, beforeValue:column === 'TYPE' ? before.productType : before.title, afterValue:column === 'TYPE' ? after.productType : after.title, result:'unknown', reason:'Shopify relation/case semantics and provider membership consequences require verification.'});
    }
    // Source metadata alone cannot establish manual selections/exclusions or all conditions.
    if (c.sources?.length) for (const source of c.sources) unknown.push({collectionId:c.id, sourceId:source.id, sourceType:source.__typename, reason:'Source conditions/selections/exclusions are not established by metadata-only reads.'});
    else if (c.ruleSet) unknown.push({collectionId:c.id, reason:'Legacy rule membership semantics are not a verified provider prediction.'});
    else unknown.push({collectionId:c.id, reason:'Null legacy ruleSet does not prove manual or unaffected membership.'});
  }
  return {state:unknown.length || affectedRules.length ? 'unknown' : 'reviewed', knownEntries:[], knownExits:[], affectedRules, unknownSources:unknown, note:'No entry/exit is claimed from an unsupported evaluator. No collection mutation is available.'};
}
function compareCorrectionBaseline(binding, current) {
  if (!object(binding)) return ['A previous preview binding is required.'];
  const keys = ['productId','handle','dossierVersion','issueRecordHash','shopId','shopDomain','requestedApiVersion','servedApiVersion','contentHash','variantStructureHash','collectionContextHash'];
  return keys.filter(k => binding[k] !== current[k]).map(k => k + ' changed since the previous preview.');
}
function createCorrectionPreview({productId, handle, proposal, dossier, issueRecord, issueState='available', read, now=Date.now(), previousBinding}) {
  const id = normalizeExactProductId(productId), p = validateCorrectionProposal(proposal);
  const result = {schemaVersion:1, mode:'sandbox_review', readOnly:true, sandboxReadOnly:true, canApply:false, executionCompatibility:'not_established', productId:id, handle, state:'ready_for_review', conflicts:[], warnings:['Preflight comparisons are optimistic checks, not provider-atomic compare-and-swap. No apply operation is available.'], holds:issueHolds(issueRecord), productIssueState:issueState, proposal:p, coverage:read?.coverage || {}, sourceReceipts:[]};
  const conflict = (state, message) => { if (result.state === 'ready_for_review') result.state = state; result.conflicts.push(message); };
  if (!id || !/^[a-z0-9_-]{1,180}$/.test(handle || '')) conflict('identity_conflict','An exact Product ID and handle are required.');
  if (!dossier || dossier.productId !== id || dossier.handle !== handle || dossier.status !== 'approved' || dossier.version !== p.dossierVersion) conflict('dossier_conflict','The proposal does not match current approved exact-product research.');
  const sources = (dossier?.sources || []).filter(s => p.sourceIds.includes(s.id));
  if (sources.length !== p.sourceIds.length || sources.some(s => !ownProductSource(s,handle))) conflict('dossier_conflict','Every proposed source ID must be a reviewed exact own-product source in the current approved dossier.');
  result.sourceReceipts = sources.map(s => ({id:s.id, url:s.url, checkedAt:s.checkedAt, reviewed:s.reviewed}));
  const active = (issueRecord?.issues || []).filter(i => i.status !== 'resolved');
  if (issueState !== 'available' || issueRecord?.productId !== id || issueRecord.updatedAt !== p.issueRecordUpdatedAt || p.issueRecordHash && hash(issueRecord) !== p.issueRecordHash || p.reviewedIssueIds.some(i => !active.some(v => v.id === i))) conflict('issue_conflict','Current exact-product issue evidence is missing or changed since the proposal.');
  result.currentIssues = active.map(i => ({id:i.id, kind:i.kind, detail:i.detail, status:i.status, evidence:i.evidence || []}));
  if (!read?.product || read.state !== 'available') {
    if (!result.conflicts.length) result.state = 'runtime_unavailable';
    result.warnings.push(read?.reason || 'A current Admin baseline is unavailable; retained snapshots are not execution-compatible.');
    return result;
  }
  const product = read.product;
  if (product.id !== id || product.handle !== handle) conflict('identity_conflict','The Admin read returned a different Product ID or handle.');
  if (product.status !== 'ACTIVE' || !ownProductSource({url:product.onlineStoreUrl,reviewed:true},handle)) conflict('identity_conflict','The current Admin product is not the exact active own-store product page behind these sources.');
  if ([product.title, product.descriptionHtml, product.productType].some(v => typeof v !== 'string')) {conflict('baseline_conflict','Current raw content fields are incomplete.');return result;}
  const before = contentBaseline(product), after = {...before};
  if (product.title !== p.expectedTitle || hash(product.descriptionHtml || '') !== p.expectedDescriptionHtmlSha256 || product.productType !== p.expectedProductType) conflict('baseline_conflict','Current raw title, description or product type differs from the retained proposal baseline.');
  if (read.coverage?.variants !== 'complete' || variantIdSetHash(read.variants) !== p.expectedVariantIdSetSha256 || hash(optionLabels(read.variants)) !== hash(p.expectedOptionLabels.map(o => ({name:o.name,values:[...new Set(o.values)].sort()})).sort((a,b)=>a.name.localeCompare(b.name)))) conflict('baseline_conflict','Complete current variant IDs/options differ or could not be established.');
  for (const patch of p.snippetPatches) {
    const count = (after.descriptionHtml || '').split(patch.expectedExactSnippet).length - 1;
    if (count !== 1) conflict('baseline_conflict','Expected description snippet must occur exactly once: ' + patch.expectedExactSnippet);
    else after.descriptionHtml = after.descriptionHtml.replace(patch.expectedExactSnippet, patch.replacementExactSnippet);
  }
  if (p.draftProductType != null) after.productType = p.draftProductType;
  result.baseline = {...before, updatedAt:product.updatedAt}; result.after = after;
  result.variants = (read.variants || []).map(v => ({id:v.id, selectedOptions:v.selectedOptions || [], price:v.price, availableForSale:v.availableForSale}));
  result.collectionImpact = reviewCollectionImpact(read,before,after);
  result.binding = {productId:id,handle,dossierVersion:dossier?.version || null,issueRecordHash:hash(issueRecord || null),shopId:read.runtime?.shopId || null,shopDomain:read.runtime?.shopDomain || null,requestedApiVersion:read.runtime?.requestedApiVersion || null,servedApiVersion:read.runtime?.servedApiVersion || null,contentHash:hash(before),variantStructureHash:hash(variantStructure(read.variants)),collectionContextHash:hash({collections:read.collections || [],memberships:read.memberships || [],coverage:read.coverage?.collections}),readAt:now};
  if (read.runtime?.compatible !== true || !read.runtime?.shopId || !read.runtime?.shopDomain) conflict('runtime_unavailable','Actual served API version, installation scope or shop identity is not verified.');
  if (read.coverage?.consistent !== true) conflict('baseline_conflict','Product changed across baseline/pagination reads; a fresh consistent review is required.');
  if (previousBinding) for (const message of compareCorrectionBaseline(previousBinding,result.binding)) conflict('baseline_conflict',message);
  if (!p.snippetPatches.length && p.draftProductType == null && !result.conflicts.length) result.state = 'configuration_unverified';
  else if (result.collectionImpact.state === 'unknown' && !result.conflicts.length) result.state = 'collection_review_required';
  result.warnings.push('Review does not resolve issues, clear holds, prove historic identity, or refresh saved Demand versions.');
  return result;
}
module.exports = {canonical,hash,normalizeExactProductId,correctionPacketDocumentId,validateCorrectionProposal,contentBaseline,variantStructure,variantIdSetHash,optionLabels,storedPacket,issueHolds,reviewCollectionImpact,compareCorrectionBaseline,createCorrectionPreview};
