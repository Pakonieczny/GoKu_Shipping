// Read-only, product-bound demand research. No default country or inferred volume.
const crypto = require('crypto');
const unique = xs => [...new Set(xs.filter(Boolean))];
function cacheKey(seeds, geos, language) {
  return 'design_kw_' + crypto.createHash('sha256').update(JSON.stringify({seeds:[...seeds].sort(),geos:[...geos].sort(),language,network:'GOOGLE_SEARCH'})).digest('hex');
}
function createKeywordEvidence(D) {
  return async function collect({campaignId, product, keywords=[]}) {
    if (!/^\d+$/.test(String(campaignId || '')) || !product?.id || !product.title) return {available:false,reason:'A verified product and live campaign are required.'};
    const rows = await D.gaql(`SELECT campaign_criterion.type, campaign_criterion.negative, campaign_criterion.location.geo_target_constant, campaign_criterion.language.language_constant FROM campaign_criterion WHERE campaign.id = ${campaignId} AND campaign_criterion.status != 'REMOVED' AND campaign_criterion.type IN ('LOCATION','LANGUAGE','PROXIMITY')`);
    const criteria = rows.map(r=>r.campaignCriterion||{}), geos=unique(criteria.filter(c=>!c.negative).map(c=>c.location?.geoTargetConstant?.match(/^geoTargetConstants\/(\d+)$/)?.[1]));
    const languages=unique(criteria.filter(c=>!c.negative).map(c=>c.language?.languageConstant?.match(/^languageConstants\/(\d+)$/)?.[1]));
    // Negative/radius targets cannot be reproduced by Keyword Planner's request.
    if (!geos.length || geos.length>10 || languages.length!==1 || criteria.some(c=>c.negative||c.type==='PROXIMITY')) return {available:false,reason:'Campaign geography or language cannot be represented exactly by this bounded Keyword Planner request.',geos,languages};
    const title=String(product.title).trim().toLowerCase(), nouns=title.split(/[^a-z0-9]+/).filter(w=>w.length>3&&!['gold','silver','sterling','filled','jewelry','jewellery','pendant','necklace','charm'].includes(w));
    const seeds=unique([title,...keywords.map(k=>String(typeof k==='string'?k:k?.text||'').trim().toLowerCase()).filter(k=>nouns.length&&nouns.some(n=>k.split(/[^a-z0-9]+/).includes(n)))]).slice(0,12);
    const key=cacheKey(seeds,geos,languages[0]), cached=D.cacheGet?await D.cacheGet(key):null;
    let ideas=cached?Object.values(cached):null;
    if (!ideas) {
      const result=await D.keywordResearch(seeds,geos,{langId:languages[0]});
      if (!result?.ok) return {available:false,reason:'Google Keyword Planner did not return usable demand evidence.',status:result?.status||null,productId:String(product.id),seeds,geos,languages};
      ideas=(result.ideas||[]).filter(i=>i?.text);
      if(ideas.length&&D.cacheSet)await D.cacheSet(key,Object.fromEntries(ideas.map(i=>[i.text.toLowerCase(),i])));
    }
    // Broad Planner suggestions are research candidates, never automatic targeting.
    const relevant=ideas.filter(i=>i.text.toLowerCase()===title||nouns.some(n=>i.text.toLowerCase().split(/[^a-z0-9]+/).includes(n))).slice(0,60);
    return {available:relevant.length>0,productId:String(product.id),seeds,geos,language:languages[0],network:'GOOGLE_SEARCH',cached:!!cached,cacheMaxAgeDays:14,ideas:relevant,limitations:['Historical Google Search demand is not campaign performance or a sales forecast.','Suggestions require product relevance review; PMax search themes are signals, not keyword targeting.','Bid values use the querying Ads account currency; no currency conversion is inferred.']};
  };
}
module.exports={createKeywordEvidence,cacheKey};
