import core from './_britesGrowth.js';
import demandModule from './_britesGrowthDemand.js';

// A sandbox-only adapter to existing, read-only Ads operations. This is not a
// second autopilot API: campaign writes, AI jobs and conversion uploads have no
// dispatch path here, even when a caller supplies their action names directly.
export const READ_ACTIONS = Object.freeze([
  'dashboard', 'metricsRange', 'dailyStats', 'conversionHealth', 'countries',
  'collections', 'opportunities', 'diagnostics', 'remedyHistory', 'playbook',
  'playbookVersions', 'designStudioStatus', 'campaignOptions', 'servingCheck',
  'adGroups', 'adGroupDetail', 'adDesignSavedWorkspaces', 'adDesignStatus',
  'creativeStatus', 'campaignVersions', 'campaignVersionDetail', 'genStatus',
  'diagRunStatus', 'growthResearchStatus', 'growthResearchDossiers', 'campaignGoalEvidence', 'receiptDiagnostics', 'receiptReconciliationPreview', 'productDemandEvidence', 'growthProductDemand', 'conversionActionTagEvidence'
]);
const allowed = new Set(READ_ACTIONS);
const demandReaders = new WeakMap();
const ENV_NAMES = ['BRITES_GROWTH_ADMIN_KEY', 'BRITES_GROWTH_SANDBOX',
  'BRITES_GROWTH_NAMESPACE', 'FIREBASE_PROJECT_ID', 'FIREBASE_CLIENT_EMAIL',
  'FIREBASE_PRIVATE_KEY', 'GADS_CONVERSION_ACTION', 'GADS_DATAMANAGER_REFRESH_TOKEN',
  'GADS_DATAMANAGER_CLIENT_ID', 'GADS_DATAMANAGER_CLIENT_SECRET', 'GADS_CLIENT_ID', 'GADS_CLIENT_SECRET'];
const fields = (body, keys) => Object.fromEntries(keys.filter(k => body[k] !== undefined).map(k => [k, body[k]]));
export function productId(value) {
  const raw = String(value || '').trim();
  if (/^gid:\/\/shopify\/Product\/\d+$/.test(raw)) return raw;
  return /^\d+$/.test(raw) ? 'gid://shopify/Product/' + raw : null;
}
function environment() { return Object.fromEntries(ENV_NAMES.map(k => [k, Netlify.env.get(k)])); }
async function loadEngine() {
  // firebaseAdmin's legacy Storage CORS initialization must never run in this
  // sandbox. The staging builder also replaces it with a read-only DB adapter.
  process.env.CORS_SET = '1';
  process.env.BRITES_GROWTH_SANDBOX = '1';
  const module = await import('./googleAdsAutopilot.js');
  return module.default || module;
}
function researchService(env) {
  return core.createGrowthService({ db: core.makeDb(env), env });
}
async function savedPasscode(env) {
  const doc = await core.makeDb(env).collection('config').doc('editPasscode').get();
  return doc.exists ? doc.data().passcode : null;
}
export function createHandler(deps = {}) {
  const getEnv = deps.environment || environment;
  const engine = deps.loadEngine || loadEngine;
  const research = deps.researchService || researchService;
  const readPasscode = deps.savedPasscode || savedPasscode;
  const loadReviewProjection = deps.loadReviewProjection || (async () => { const module = await import('./googleAdsAdDesignResearch.js'); return module.default || module; });
  const readSavedDemand = deps.readSavedDemand || (async (env, ids) => {
    const module = await import('./_britesGrowthDemandStore.js');
    return (module.default || module).createDemandStore(research(env)).read(ids);
  });
  const readReceipts = deps.readReceipts || (async (env, options) => {
    const module = await import('./_britesGrowthReceiptObserver.js');
    return (module.default || module).createReceiptObserver({env, db: core.makeDb(env)}).read(options);
  });
  const readReceiptPreview = deps.readReceiptPreview || (async (env, options) => {
    const module = await import('./_britesGrowthReceiptReleasePreview.js');
    return (module.default || module).createReceiptReleasePreview({env, db: core.makeDb(env)}).read(options);
  });
  return async req => {
    const h = { 'Content-Type': 'application/json', 'Cache-Control': 'no-store',
      'X-Content-Type-Options': 'nosniff' };
    const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: h });
    if (req.method !== 'POST') return json({ error: 'Use POST to read the private Ads sandbox.' }, 405);
    const origin = req.headers.get('Origin');
    if (origin && origin !== new URL(req.url).origin) return json({ error: 'Use the sandbox website to open its private Ads data.' }, 403);
    const rawEnv = getEnv();
    if (rawEnv.BRITES_GROWTH_SANDBOX !== '1' || (rawEnv.BRITES_GROWTH_NAMESPACE && rawEnv.BRITES_GROWTH_NAMESPACE !== 'Brites_Growth_Sandbox')) {
      return json({ error: 'This read-only Ads bridge is available only in the isolated growth sandbox.' }, 503);
    }
    // Netlify build variables need not exist in the function runtime. Resolve
    // the same default as the research service, after rejecting any live scope.
    const env = {...rawEnv, BRITES_GROWTH_NAMESPACE: core.namespace(rawEnv)};
    const supplied = req.headers.get('X-Growth-Key') || req.headers.get('X-Edit-Passcode');
    let authenticated = core.sameSecret(supplied, env.BRITES_GROWTH_ADMIN_KEY);
    // The existing owner passcode remains a read-only sign-in option. No new
    // credential is generated and a missing/inaccessible document fails closed.
    if (!authenticated && supplied) try { authenticated = core.sameSecret(supplied, await readPasscode(env)); } catch {}
    if (!authenticated) return json({ error: 'Operator sign-in required.' }, 401);
    try {
      const raw = await req.text();
      if (raw.length > 30000) return json({ error: 'Request is too large.' }, 413);
      let body;
      try { body = JSON.parse(raw || '{}'); } catch { return json({ error: 'Send a valid JSON request.' }, 400); }
      if (!body || typeof body !== 'object' || Array.isArray(body)) return json({ error: 'Send one operation object.' }, 400);
      const action = body.action;
      if (!allowed.has(action) || (action === 'opportunities' && body.force)) {
        return json({ ok: false, code: 'SANDBOX_READ_ONLY', error: 'This sandbox reads saved evidence and Google Ads reports. This action cannot change campaigns, spend, conversions or run paid AI.' }, 403);
      }
      if (action === 'growthResearchStatus') return json(await research(env).status());
      if (action === 'receiptDiagnostics') {
        if (Object.keys(body).some(key => !['action', 'limit', 'maxMs'].includes(key))) return json({error: 'Receipt diagnostics accept only bounded batch size and read budget. Request IDs come from saved server receipts.'}, 400);
        return json({...await readReceipts(env, fields(body, ['limit', 'maxMs'])), sandboxReadOnly: true});
      }
      if (action === 'receiptReconciliationPreview') {
        if (Object.keys(body).some(key => !['action', 'limit', 'maxMs'].includes(key))) return json({error: 'Receipt repair previews accept only bounded read limits. Rows and diagnostics are loaded and verified by the server.'}, 400);
        return json({...await readReceiptPreview(env, fields(body, ['limit', 'maxMs'])), sandboxReadOnly: true});
      }
      if (action === 'growthProductDemand') {
        if (!Array.isArray(body.productIds) || body.productIds.length > 20) return json({ error: 'Request at most 20 exact product IDs.' }, 400);
        const requestedIds = body.productIds.map(productId);
        if (requestedIds.some(id => !id)) return json({ error: 'Use exact Shopify Product IDs.' }, 400);
        const ids = [...new Set(requestedIds)], products = await readSavedDemand(env, ids);
        if (!Array.isArray(products)) throw Error('Saved demand evidence returned an invalid response.');
        return json({ products: products.filter(product => ids.includes(product.productId)), sandboxReadOnly: true });
      }
      if (action === 'growthResearchDossiers') {
        if (!Array.isArray(body.productIds) || body.productIds.length > 20) return json({ error: 'Request at most 20 exact product IDs.' }, 400);
        const requestedIds = body.productIds.map(productId);
        if (requestedIds.some(id => !id)) return json({ error: 'Use exact Shopify Product IDs.' }, 400);
        const ids = [...new Set(requestedIds)];
        const service = research(env), dossiers = await service.research(ids);
        let productIssues = [], productIssueState = 'unavailable';
        if (typeof service.productIssues === 'function') try { productIssues = await service.productIssues(ids); productIssueState = 'available'; } catch {}
        const exactDossiers = (dossiers || []).filter(d => ids.includes(d.productId)), operatorReviews = [];
        // Research() reads the current private document, not a caller-supplied
        // historical version. Fresh live identity and issue evidence are still
        // required before deriving a bounded proposal-only review packet.
        let projection; try { projection = await loadReviewProjection(); } catch {}
        for (const id of ids) {
          const dossier = exactDossiers.find(d => d.productId === id), holds = core.productIssueHolds((productIssues || []).find(record => record.productId === id));
          const entry = {productId: id, handle: dossier?.handle || null, dossierVersion: dossier?.version || null, state: 'unavailable'};
          if (productIssueState !== 'available' || holds.recommendationHold || holds.meaningHold) entry.state = 'held';
          else if (projection && dossier && typeof service.getProduct === 'function') try {
            const product = await service.getProduct(id), current = {...dossier, currentDossierVersion: dossier.version, evidenceHolds: holds};
            if (projection.sharedDossierIsCurrent(current, product)) {
              const packet = projection.projectRecommendations(current, new Set(current.sources.map(source => source.id)))?.operatorReviewPacket;
              if (packet) { entry.state = 'pending_operator_review'; entry.operatorReviewPacket = packet; }
            }
          } catch { /* Identity/provider failures leave only an unavailable state. */ }
          operatorReviews.push(entry);
        }
        return json({ dossiers: exactDossiers, operatorReviews, productIssues: (productIssues || []).filter(record => ids.includes(record.productId)), productIssueState, sandboxReadOnly: true });
      }
      if (action === 'conversionActionTagEvidence') {
        if (Object.keys(body).some(key => key !== 'action')) return json({error:'Tag evidence accepts only its fixed read action.'},400);
        try {
          const E = await engine();
          const module = await import('./_britesGrowthConversionTagEvidence.js');
          return json({...await (module.default || module).read({gaql:query => E.gaql(query)}),sandboxReadOnly:true});
        } catch {
          return json({ok:false,error:'Conversion-action tag evidence is unavailable. Retry this read later.',sandboxReadOnly:true,providerWrites:false,conversionUploads:0},503);
        }
      }
      const E = await engine();
      if (action === 'productDemandEvidence') {
        const id = productId(body.productId);
        if (!id || body.marketKey != null && !/^[a-f0-9]{24}$/.test(body.marketKey)) return json({ error: 'Choose an exact Product ID and an observed market.' }, 400);
        const service = research(env), [product, dossiers] = await Promise.all([service.getProduct(id), service.research([id])]);
        const dossier = (dossiers || []).find(d => d.productId === id && d.status === 'approved' && d.handle === product?.handle);
        if (!dossier || !product) return json({ ok: false, code: 'PRODUCT_EVIDENCE_UNAVAILABLE', error: 'Approved research for this exact catalogue product is not yet available.', sandboxReadOnly: true }, 409);
        if (!demandReaders.has(E)) demandReaders.set(E, demandModule.createDemandEvidence({ gaql: E.gaql, keywordResearch: E.keywordResearch, readStorefrontLanguage: () => demandModule.readOwnStorefrontLanguage() }));
        let issueRecords = [], issueState = 'unavailable';
        if (typeof service.productIssues === 'function') try { issueRecords = await service.productIssues([id]); issueState = 'available'; } catch {}
        const holds = core.productIssueHolds((issueRecords || []).find(record => record.productId === id));
        return json({ ...await demandReaders.get(E).read(product, dossier, { planner: body.planner === true, marketKey: body.marketKey || null }), productHolds: holds, productIssueState: issueState, promotionAllowed: issueState === 'available' && !holds.recommendationHold, sandboxReadOnly: true });
      }
      let result;
      switch (action) {
        case 'dashboard': result = await E.dashboard(fields(body, ['activity', 'activityOnly'])); break;
        case 'metricsRange': result = await E.metricsRange(fields(body, ['start', 'end'])); break;
        case 'dailyStats': result = await E.dailyStats(fields(body, ['start', 'end', 'campaignId'])); break;
        case 'conversionHealth': result = await E.conversionHealth({ force: false }); break;
        case 'campaignGoalEvidence': result = await E.campaignGoalEvidence(); break;
        case 'countries': result = { ok: true, list: await E.listCountries({ force: false }) }; break;
        case 'collections': result = { collections: await E.getCollections({ force: false }) }; break;
        case 'opportunities': result = await E.opportunitiesWithStatus({ cacheOnly: true, force: false }); break;
        case 'diagnostics': result = (await E.getDiagnostics()) || { empty: true }; break;
        case 'remedyHistory': result = await E.remedyHistory({ limit: Math.max(1, Math.min(100, Number(body.limit) || 100)) }); break;
        case 'playbook': result = await E.learningOverview(); break;
        case 'playbookVersions': result = await E.playbookVersions(); break;
        case 'designStudioStatus': result = await E.designStudioOpportunityStatus({ refreshMetrics: false }); break;
        case 'campaignOptions': result = await E.campaignOptionsStatus(); break;
        case 'servingCheck': result = await E.servingCheck({ id: body.id }); break;
        case 'adGroups': result = await E.adGroups(fields(body, ['start', 'end', 'campaignId', 'reportingTree', 'listings'])); break;
        case 'adGroupDetail': result = await E.adGroupDetail(fields(body, ['campaignId', 'groupRef', 'start', 'end', 'reportingTree'])); break;
        case 'adDesignSavedWorkspaces': result = await E.adDesignSavedWorkspaces(fields(body, ['after'])); break;
        case 'adDesignStatus': result = await E.adDesignStatus(fields(body, ['workspaceId'])); break;
        case 'creativeStatus': result = await E.creativeApprovalStatus(body.id); break;
        case 'campaignVersions': result = await E.campaignVersions({ id: body.id || body.campaignId, beforeVersion: body.beforeVersion }); break;
        case 'campaignVersionDetail': result = await E.campaignVersionDetail({ id: body.id || body.campaignId, version: body.version }); break;
        case 'genStatus': result = (await E.getGenStatus(core.clean(body.genId, 150))) || { pending: true }; break;
        case 'diagRunStatus': result = (await E.getGenStatus('diag-' + core.clean(body.runId, 100))) || { pending: true }; break;
      }
      return json(result && typeof result === 'object' && !Array.isArray(result)
        ? { ...result, sandboxReadOnly: true } : result);
    } catch (error) {
      return json({ ok: false, error: core.clean(error.message, 350), sandboxReadOnly: true }, 503);
    }
  };
}
export default createHandler();
export const config = { path: '/api/growth-ads', method: ['POST'] };
