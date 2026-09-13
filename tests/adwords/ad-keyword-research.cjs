const assert=require('node:assert/strict');
const {createKeywordEvidence,cacheKey}=require('../../netlify/functions/googleAdsAdKeywordResearch');
const {createAdDesignResearch}=require('../../netlify/functions/googleAdsAdDesignResearch');
const input={campaignId:'42',product:{id:'peach',title:'Gold Peach Fruit Charm'},keywords:['peach pendant','seagull pendant','gold jewelry']};
const geo={type:'LOCATION',location:{geoTargetConstant:'geoTargetConstants/2840'}},language={type:'LANGUAGE',language:{languageConstant:'languageConstants/1000'}};
let checks=0;const ok=(v,m)=>{assert(v,m);checks++;};
(async()=>{
 let calls=[],saved,rows=[geo,language];
 const D={gaql:async q=>{ok(q.includes('campaign.id = 42'),'exact campaign targeting');return rows.map(c=>({campaignCriterion:c}));},keywordResearch:async(...args)=>{calls.push(args);return {ok:true,ideas:[{text:'peach pendant',searches:90},{text:'seagull necklace',searches:9999}]};},cacheSet:async(k,v)=>{saved={k,v};}};
 let result=await createKeywordEvidence(D)(input);
 ok(result.available&&result.productId==='peach','exact product evidence');
 ok(calls.length===1&&JSON.stringify(calls[0])===JSON.stringify([['gold peach fruit charm','peach pendant'],['2840'],{langId:'1000'}]),'only product-relevant seeds and actual campaign scope');
 ok(result.ideas.length===1&&result.ideas[0].searches===90,'no transfer of other product demand');
 ok(saved.k.startsWith('design_kw_'),'dedicated cache namespace');
 result=await createKeywordEvidence({...D,cacheGet:async()=>saved.v})(input);
 ok(result.cached&&calls.length===1,'cache prevents duplicate quota consumption');
 ok(cacheKey(['a'.repeat(150)+'one'],['2840'],'1000')!==cacheKey(['a'.repeat(150)+'two'],['2840'],'1000'),'long seed sets cannot collide by prefix truncation');
 ok(cacheKey(['peach'],['2840'],'1000')!==cacheKey(['peach'],['2840'],'1002'),'language isolated');
 for(const target of [[],[geo],[language],[geo,language,{...geo,negative:true}],[geo,language,{type:'PROXIMITY'}],[geo,language,{type:'LANGUAGE',language:{languageConstant:'languageConstants/1002'}}]]){
   rows=target;const before=calls.length;result=await createKeywordEvidence(D)(input);ok(!result.available&&calls.length===before,'unsupported scope never silently defaults to Canada or English');
 }
 rows=[geo,language];result=await createKeywordEvidence({...D,keywordResearch:async()=>({ok:false,status:403})})(input);
 ok(!result.available&&result.status===403,'permission denial remains unavailable');
 result=await createKeywordEvidence({...D,keywordResearch:async()=>({ok:true,ideas:[]})})(input);ok(!result.available,'empty response is not verified demand');
 const research=createAdDesignResearch({creativeFetch:async()=>'<p>'+('Gold Peach Fruit Charm product facts. '.repeat(6))+'</p>',keywordEvidence:async x=>{ok(x.campaignId==='42'&&x.product.id==='peach','research adapter exact scope');return {available:true,ideas:[{text:'peach pendant',searches:90}]};}});
 const evidence=await research.collect({campaignId:'42',group:{channel:'pmax',key:'g',ref:'customers/1/assetGroups/2',url:'https://britesjewelry.com/products/peach',keywords:['peach pendant']},selectedProducts:[{...input.product,url:'https://britesjewelry.com/products/peach',images:[{id:'p',url:'https://cdn.shopify.com/peach.jpg'}]}],settings:{sourceImageId:'p'}});
 ok(evidence.sources.some(s=>s.id==='keywordDemand'&&s.status==='available'&&s.data.ideas[0].searches===90),'demand reaches citation-bound AI evidence');
 console.log('PASS '+checks+' product keyword scope, demand, cache and unavailable-source checks');
})().catch(e=>{console.error(e);process.exitCode=1});
