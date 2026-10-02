'use strict';

const review = require('./_britesGrowthCorrectionReview');
// Schema validated against 2026-07. No caller-supplied query or mutation exists.
const READ_VERSION = '2026-07';
const QUERIES = Object.freeze({
  content: 'query CorrectionContent($id: ID!) { product(id: $id) { id title descriptionHtml productType updatedAt } }',
  identity: 'query CorrectionIdentity($id: ID!) { product(id: $id) { id handle status onlineStoreUrl category { id } } }',
  ruleContext: 'query CorrectionRuleContext($id: ID!) { product(id: $id) { id tags vendor } }',
  variants: 'query CorrectionVariants($id: ID!, $after: String) { product(id: $id) { id variants(first: 100, after: $after) { nodes { id sku selectedOptions { name value } price availableForSale } pageInfo { hasNextPage endCursor } } } }',
  media: 'query CorrectionMedia($id: ID!, $after: String) { product(id: $id) { id media(first: 50, after: $after) { nodes { ... on MediaImage { id alt image { url } } } pageInfo { hasNextPage endCursor } } } }',
  collections: 'query CorrectionCollections($after: String) { collections(first: 250, after: $after) { nodes { id title handle ruleSet { appliedDisjunctively rules { column relation condition } } sources { __typename id title } } pageInfo { hasNextPage endCursor } } }',
  memberships: 'query CorrectionMembership($id: ID!, $after: String) { product(id: $id) { id collections(first: 250, after: $after) { nodes { id } pageInfo { hasNextPage endCursor } } } }',
  runtime: 'query CorrectionRuntime { currentAppInstallation { accessScopes { handle } } shop { id myshopifyDomain plan { partnerDevelopment } } }'
});
function createCorrectionReader({env={}, fetch=globalThis.fetch, now=Date.now, maxMs=40000, maxPages=25}={}) {
  const store = env.SHOPIFY_STORE, version = env.BRITES_GROWTH_CORRECTION_READ_VERSION || READ_VERSION;
  const configured = /^[a-z0-9-]+\.myshopify\.com$/.test(store || '') && typeof env.SHOPIFY_CLIENT_ID === 'string' && !!env.SHOPIFY_CLIENT_ID && typeof env.SHOPIFY_CLIENT_SECRET === 'string' && env.SHOPIFY_CLIENT_SECRET.length >= 16 && !/redact|\*{2,}|^[-x•●]+$/i.test(env.SHOPIFY_CLIENT_SECRET);
  let access = null, expires = 0;
  const capabilities = () => ({mode:'sandbox_review', canApply:false, readOnly:true, configuredReadVersion:version, schemaValidatedVersion:READ_VERSION, adminReadState:!configured ? 'unavailable' : version !== READ_VERSION ? 'unvalidated_version' : 'configured_not_verified', servedReadVersion:null, executionCompatibility:'not_established', limitations:['Actual Admin access/API version is verified only by an authenticated baseline read.','No provider mutation is available.','Source metadata is not a complete collection condition evaluator.']});
  async function read(productId, {collections=false}={}) {
    const id = review.normalizeExactProductId(productId), started = now(), deadline = started + Math.max(1000, Math.min(45000,maxMs)), headers = [];
    const coverage = {variants:'unavailable', media:'not_required', collections:collections ? 'unavailable' : 'not_required', memberships:collections ? 'unavailable' : 'not_required', consistent:false};
    const base = {state:'unavailable', product:null, variants:[], collections:[], memberships:[], coverage, runtime:{requestedApiVersion:version, servedApiVersion:null, compatible:false}, readAt:started};
    if (!id) return {...base, reason:'An exact Product ID is required.'};
    if (!configured) return {...base, reason:'Shopify Admin read credentials are unavailable. Retained evidence and fixture review can continue.'};
    if (version !== READ_VERSION) return {...base, reason:'The configured correction-reader API version has not been schema-validated.'};
    function signal() { const remaining = deadline - now(); if (remaining <= 0) throw Error('read_budget'); return AbortSignal.timeout(Math.max(1,Math.min(12000,remaining))); }
    async function token() {
      if (access && now() < expires - 60000) return access;
      const res = await fetch('https://' + store + '/admin/oauth/access_token', {method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'}, body:new URLSearchParams({grant_type:'client_credentials', client_id:env.SHOPIFY_CLIENT_ID, client_secret:env.SHOPIFY_CLIENT_SECRET}), signal:signal()});
      const data = await res.json();
      if (!res.ok || typeof data.access_token !== 'string' || !data.access_token) throw Error('authorization_unavailable');
      access = data.access_token; expires = now() + Number(data.expires_in || 86400)*1000; return access;
    }
    async function query(name, variables={}) {
      if (!Object.hasOwn(QUERIES,name)) throw Error('unsupported_read');
      const res = await fetch('https://' + store + '/admin/api/' + version + '/graphql.json', {method:'POST', headers:{'Content-Type':'application/json', 'X-Shopify-Access-Token':await token()}, body:JSON.stringify({query:QUERIES[name],variables}), signal:signal()});
      const served = res.headers?.get('X-Shopify-API-Version') || null;
      headers.push({operation:name, servedApiVersion:served});
      if (served !== version) throw Error('served_version_unverified');
      const data = await res.json();
      if (!res.ok || data.errors?.length || !data.data) throw Error('admin_operation_unavailable');
      return data.data;
    }
    async function pages(name, connection) {
      const all = [], cursors = new Set(), ids = new Set(); let after = null;
      for (let page=0;page<Math.max(1,Math.min(30,maxPages));page++) {
        const data = await query(name, {...(name === 'collections' ? {} : {id}), after});
        if (name !== 'collections' && data.product?.id !== id) throw Error('identity_changed');
        const conn = connection(data), info = conn?.pageInfo;
        if (!Array.isArray(conn?.nodes) || !info || typeof info.hasNextPage !== 'boolean') throw Error('pagination_incomplete');
        for (const node of conn.nodes) {
          if (!node || typeof node.id !== 'string' || ids.has(node.id)) throw Error('pagination_inconsistent');
          ids.add(node.id); all.push(node);
        }
        if (all.length > 6000) throw Error('read_budget');
        if (!info.hasNextPage) return all;
        if (typeof info.endCursor !== 'string' || !info.endCursor || cursors.has(info.endCursor)) throw Error('pagination_incomplete');
        cursors.add(info.endCursor); after = info.endCursor;
      }
      throw Error('pagination_incomplete');
    }
    try {
      const runtime = await query('runtime');
      const scopes = (runtime.currentAppInstallation?.accessScopes || []).map(s => s.handle);
      base.runtime = {...base.runtime, shopId:runtime.shop?.id || null, shopDomain:runtime.shop?.myshopifyDomain || null, partnerDevelopment:runtime.shop?.plan?.partnerDevelopment === true, servedApiVersion:headers.at(-1)?.servedApiVersion, readScopeVerified:scopes.includes('read_products') || scopes.includes('write_products'), compatible:false};
      if (!base.runtime.shopId || base.runtime.shopDomain !== store || !base.runtime.readScopeVerified) throw Error('shop_or_scope_unverified');
      const content = await query('content',{id}), identity = await query('identity',{id});
      if (!content.product || !identity.product) return {...base, reason:'Exact Admin product was not found.'};
      if (content.product.id !== id || identity.product.id !== id) throw Error('identity_changed');
      base.product = {...content.product,...identity.product};
      if (typeof base.product.descriptionHtml !== 'string' || base.product.descriptionHtml.length > 200000) throw Error('content_unavailable');
      base.variants = await pages('variants',d => d.product?.variants); coverage.variants = 'complete';
      if (!base.variants.length || base.variants.some(v => !/^gid:\/\/shopify\/ProductVariant\/\d+$/.test(v.id) || !Array.isArray(v.selectedOptions) || v.selectedOptions.some(o => typeof o.name !== 'string' || typeof o.value !== 'string'))) throw Error('variant_structure_unavailable');
      if (collections) {
        try {
          base.collections = await pages('collections',d => d.collections); coverage.collections = 'complete';
          base.memberships = await pages('memberships',d => d.product?.collections); coverage.memberships = 'complete';
          const all = new Set(base.collections.map(c => c.id)); if (base.memberships.some(c => !all.has(c.id))) coverage.memberships = 'incomplete';
          const context = await query('ruleContext',{id}); if (context.product?.id !== id) throw Error('identity_changed');
          base.ruleContext = context.product;
        } catch { coverage.collections = 'incomplete'; coverage.memberships = coverage.memberships === 'complete' ? 'complete' : 'incomplete'; base.collectionWarning = 'Complete collection/source evidence could not be established within the read budget.'; }
      }
      const end = await query('content',{id}), endIdentity = await query('identity',{id});
      coverage.consistent = end.product?.id === id && endIdentity.product?.id === id && review.hash(end.product) === review.hash(content.product) && review.hash(endIdentity.product) === review.hash(identity.product);
      base.runtime.compatible = headers.every(h => h.servedApiVersion === version) && base.runtime.readScopeVerified;
      return {...base, state:'available', readAt:now(), responseVersions:headers, ...(base.collectionWarning ? {reason:base.collectionWarning} : {})};
    } catch (error) {
      const reasons = {served_version_unverified:'Actual served Shopify API version differs or is unavailable; runtime compatibility is not established.', authorization_unavailable:'Shopify Admin authorization is unavailable.', shop_or_scope_unverified:'Exact shop identity or product read access could not be verified.', identity_changed:'Product identity changed across reads.', pagination_incomplete:'Current product pagination could not be completed.', pagination_inconsistent:'Current pagination repeated or changed records.', read_budget:'Current baseline read exceeded its bounded read budget.', variant_structure_unavailable:'Complete current variant structure could not be established.', content_unavailable:'Current raw description is unavailable or too large for a bounded review.'};
      base.runtime.servedApiVersion = headers.at(-1)?.servedApiVersion || null;
      return {...base, state:'unavailable', reason:reasons[error.message] || 'A required Shopify Admin read is unavailable. Credentials and provider exception text are not displayed.', responseVersions:headers, readAt:now()};
    }
  }
  return {capabilities,read};
}
module.exports = {READ_VERSION, QUERIES, createCorrectionReader};
