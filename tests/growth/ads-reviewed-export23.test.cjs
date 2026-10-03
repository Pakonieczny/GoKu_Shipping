'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {JSDOM}=require('jsdom');
const projection=require('../../netlify/functions/googleAdsAdDesignResearch'),readOnly=require('../../netlify/functions/_britesGrowthAdsReadOnly');
const NOW=Date.now(),VERSION='b'.repeat(64),ID='gid://shopify/Product/2301';
function evidence({id=ID,handle='fixture-compass',version=VERSION}={}){
  const product={id:'synthetic-row-'+id,rank:1,title:'Fixture compass',productId:id,handle,dossierVersion:version,status:'complete'};
  const dossier={schema:1,productId:id,handle,status:'approved',version,currentDossierVersion:version,savedAt:NOW,evidenceHolds:{},sources:[{id:'own',url:'https://example.com/'+handle,title:'Reviewed fixture catalogue',excerpt:'Synthetic pendant evidence only.',checkedAt:NOW,reviewed:true}],facts:[],meanings:[],competitors:[],buyerIntents:['A gift hypothesis'],recommendations:[{channel:'keywords',basis:'hypothesis',action:'Review distinct option-related search intents.',measure:'Relevant queries and reconciled purchases.',sourceIds:['own'],keywords:['sterling silver compass pendant','gold filled compass pendant','engraved compass necklace']},{channel:'negatives',basis:'hypothesis',action:'Review unrelated download intent.',measure:'Query relevance after operator review.',sourceIds:['own'],negativeKeywords:['free compass template','compass drawing tutorial']}]};
  const packet=readOnly.bindOperatorPacketSources(projection.projectRecommendations(dossier,new Set(['own']),NOW),dossier).operatorReviewPacket;
  const result={dossiers:[dossier],productIssueState:'available',productIssues:[],operatorReviewBindingRequired:true,operatorReviews:[{productId:id,handle,dossierVersion:version,state:'pending_operator_review',operatorReviewPacket:packet}]};
  const entry={productId:id,state:'current',productIssueState:'available',promotionAllowed:true,productHolds:{},evidence:{schemaVersion:1,evidenceRevision:3,evidenceKind:'product_demand_and_ad_outcomes',readOnly:true,sandboxReadOnly:true,productId:id,handle,dossierVersion:version,at:NOW,range:'LAST_30_DAYS',currency:'CAD',sources:{shopping:{state:'available',truncated:false}},shopping:{totals:{impressions:100,clicks:4,cost:2.5,reportedConversions:1}},planner:{state:'available',bidCurrency:'CAD',market:{pooled:true,countries:[{code:'CA'},{code:'US'}],languageBasis:'research_hypothesis_from_verified_own_storefront',languageName:'English',languageSource:{finalUrl:'https://example.com/'}},keywords:[{text:'compass gift',averageMonthlySearches:120,competition:'HIGH',lowTopOfPageBid:.4,highTopOfPageBid:1.5},{text:'missing fixture volume',averageMonthlySearches:null,lowTopOfPageBid:null,highTopOfPageBid:null}]}}};
  return {product,dossier,packet,result,entry};
}
function ui(){const dom=new JSDOM('<div id="fixture-root"></div>',{url:'https://sandbox.invalid'});vm.runInContext(fs.readFileSync(path.resolve(__dirname,'../../brites-growth.js'),'utf8'),vm.createContext(dom.window));return {dom,api:dom.window.BritesGrowth,root:dom.window.document.querySelector('#fixture-root')};}
function corrective(text){assert.match(text,/CORRECTIVE RESEARCH ONLY/);assert.doesNotMatch(text,/Product advertising test brief/);assert.doesNotMatch(text,/Source-bound packet version:/);assert.doesNotMatch(text,/Candidate IDs:/);}
test('copyable review preserves complete separated terms, exact versions, IDs and reviewed citations',()=>{
  const e=evidence(),f=ui(),text=f.api.operatorBriefFor(e.result,e.product,e.dossier,e.product.title,NOW);
  assert.match(text,/pending operator review · hypotheses/);assert.match(text,/No automatic activation/);assert.match(text,/No provider, campaign or budget writes/);
  assert.ok(text.includes(VERSION));assert.ok(text.includes(e.packet.packetVersion));assert.ok(text.includes(e.packet.sourceBindings[0].sourceVersion));
  for(const id of e.packet.candidateIds)assert.ok(text.includes(id));
  const positive=text.split('Positive keyword hypotheses:')[1].split('Negative keyword hypotheses:')[0],negative=text.split('Negative keyword hypotheses:')[1].split('Keyword match types')[0];
  for(const term of e.dossier.recommendations[0].keywords){assert.ok(positive.includes(term));assert.ok(!negative.includes(term));}
  for(const term of e.dossier.recommendations[1].negativeKeywords){assert.ok(negative.includes(term));assert.ok(!positive.includes(term));}
  assert.ok(text.includes(e.dossier.sources[0].url));assert.ok(text.includes(new Date(NOW).toISOString()));assert.match(text,/match types and campaign scope require separate operator review/);f.dom.window.close();
});
test('source expiry and future review timestamps downgrade exports to context notes',()=>{
  const f=ui();for(const at of [NOW+31*86400000,NOW-60001]){const e=evidence();corrective(f.api.operatorBriefFor(e.result,e.product,e.dossier,'Fixture',at));}f.dom.window.close();
});
test('queue or packet version drift and foreign product identity cannot produce a test brief',()=>{
  const f=ui();for(const change of [e=>e.product.dossierVersion='c'.repeat(64),e=>e.packet.dossierVersion='c'.repeat(64),e=>e.product.productId='gid://shopify/Product/2399',e=>e.product.handle='foreign-fixture']){const e=evidence();change(e);corrective(f.api.operatorBriefFor(e.result,e.product,e.dossier,'Fixture',NOW));}f.dom.window.close();
});
test('recommendation, cart and meaning holds retain only corrective research',()=>{
  const f=ui();for(const block of ['recommendation','cart','meaning']){const e=evidence();e.result.productIssues=[{productId:ID,issues:[{kind:'review',status:'open',blocks:[block],detail:'Synthetic exact-product hold.'}]}];corrective(f.api.operatorBriefFor(e.result,e.product,e.dossier,'Fixture',NOW));}const e=evidence();e.result.operatorReviews[0].state='held';corrective(f.api.operatorBriefFor(e.result,e.product,e.dossier,'Fixture',NOW));f.dom.window.close();
});
test('missing packet, missing source bindings, source drift and unavailable issue evidence stay corrective',()=>{
  const f=ui();for(const change of [e=>e.result.operatorReviews=[],e=>delete e.packet.sourceBindings,e=>delete e.packet.packetVersion,e=>e.result.operatorReviewBindingRequired=false,e=>e.dossier.sources[0].excerpt='Later inspected fixture revision.',e=>e.dossier.sources[0].reviewed=false,e=>e.result.productIssueState='unavailable']){
    const e=evidence();change(e);if(e.result.operatorReviewBindingRequired===false)delete e.packet.sourceBindings;corrective(f.api.operatorBriefFor(e.result,e.product,e.dossier,'Fixture',NOW));
  }f.dom.window.close();
});
test('whole-token positive and negative conflicts cannot leak structured proposals into exports',()=>{
  const e=evidence(),f=ui();e.dossier.recommendations[1].negativeKeywords=['compass pendant'];e.packet.candidates[1].negativeKeywords=['compass pendant'];e.packet.negativeKeywords=[{term:'compass pendant',basis:'hypothesis',reviewState:'pending_operator_review',sourceIds:['own'],candidateIds:[e.packet.candidates[1].candidateId]}];
  corrective(f.api.operatorBriefFor(e.result,e.product,e.dossier,'Fixture',NOW));f.dom.window.close();
});
test('measured export preserves packet terms, native currencies and attribution limits',()=>{
  const e=evidence(),f=ui(),text=f.api.demandBriefFor(e.dossier,'Fixture',e.entry,null,'available',NOW,{result:e.result,product:e.product});
  assert.doesNotMatch(text,/CORRECTIVE RESEARCH ONLY/);assert.ok(text.includes(e.dossier.recommendations[0].keywords[2]));assert.ok(text.includes(e.dossier.recommendations[1].negativeKeywords[0]));assert.match(text,/reported spend 2.5 CAD/);assert.match(text,/0.4–1.5 CAD/);assert.match(text,/POOLED observed country targets: CA, US/);assert.match(text,/reported conversions \(unvalidated\) 1/);assert.match(text,/do not establish sales, revenue, profit or ROAS/);assert.match(text,/actual campaign language targeting is unknown/);assert.match(text,/missing fixture volume; pooled average monthly searches unavailable/);f.dom.window.close();
});
test('fresh demand never makes expired source reviews or later holds promotable',()=>{
  const f=ui();for(const change of [e=>{e.dossier.sources[0].checkedAt=NOW-31*86400000;e.packet.sourceBindings[0].checkedAt=e.dossier.sources[0].checkedAt;},e=>e.entry.productHolds.meaningHold=true,e=>e.entry.promotionAllowed=false]){const e=evidence();change(e);const text=f.api.demandBriefFor(e.dossier,'Fixture',e.entry,null,'available',NOW,{result:e.result,product:e.product});corrective(text);assert.match(text,/reported spend 2.5 CAD/);assert.match(text,/Promotion remains held/);}f.dom.window.close();
});
test('legacy context exports require a fresh packet and stale demand remains unexportable',()=>{
  const e=evidence(),f=ui();corrective(f.api.briefFor(e.dossier,'Fixture'));corrective(f.api.demandBriefFor(e.dossier,'Fixture',e.entry,null,'available',NOW));e.entry.evidence.at=NOW-86400001;assert.equal(f.api.demandBriefFor(e.dossier,'Fixture',e.entry,null,'available',NOW,{result:e.result,product:e.product}),null);e.entry.evidence.at=NOW;e.entry.evidence.dossierVersion='d'.repeat(64);assert.equal(f.api.demandBriefFor(e.dossier,'Fixture',e.entry,null,'available',NOW,{result:e.result,product:e.product}),null);f.dom.window.close();
});
async function until(predicate){for(let i=0;i<50;i++){if(predicate())return;await new Promise(setImmediate);}assert.fail('Fixture did not settle.');}
async function mounted({freshResearch=null,freshDemand=null,extraProduct=null}={}){
  const f=ui(),e=evidence(),copies=[],reads=[],counts=new Map();
  Object.defineProperty(f.dom.window.navigator,'clipboard',{value:{writeText:async text=>copies.push(text)},configurable:true});
  const request=async op=>{reads.push(op);if(op==='status')return {at:NOW,control:{},counts:{ranked:1,matched:1,complete:1,approvedDossiers:1},queue:[e.product,...(extraProduct?[extraProduct.product]:[])]};
    const id=decodeURIComponent(op.split('ids=')[1]||''),current=extraProduct&&extraProduct.product.productId===id?extraProduct:e;
    if(op.startsWith('research?')){const n=(counts.get(id)||0)+1;counts.set(id,n);if(current===e&&n>1&&freshResearch)return typeof freshResearch==='function'?freshResearch(e):freshResearch;return current.result;}
    if(op.startsWith('demand?')){if(current===e&&freshDemand&&counts.get(id)>1)return typeof freshDemand==='function'?freshDemand(e):freshDemand;return {products:[current.entry]};}
    throw Error('Unexpected fixture read.');
  };
  await f.api.mount(f.root,{request});await f.root.querySelector('.row').onclick();await until(()=>[...f.root.querySelectorAll('button')].some(b=>b.textContent==='Copy product test brief'));return {...f,e,copies,reads};
}
function button(f,label){const found=[...f.root.querySelectorAll('button')].find(b=>b.textContent===label);assert.ok(found,'Missing fixture button '+label);return found;}
test('copy click re-reads current research and retains the complete reviewed export',async()=>{
  const f=await mounted(),before=f.reads.filter(op=>op.startsWith('research?')).length;await button(f,'Copy product test brief').onclick();assert.equal(f.reads.filter(op=>op.startsWith('research?')).length,before+1);assert.equal(f.copies.length,1);assert.match(f.copies[0],/Positive keyword hypotheses/);assert.ok(f.copies[0].includes(f.e.dossier.recommendations[0].keywords[2]));assert.ok(f.copies[0].includes(f.e.packet.packetVersion));f.dom.window.close();
});
test('copy click observes a newly held product instead of exporting its earlier valid packet',async()=>{
  const later=evidence();later.result.productIssues=[{productId:ID,issues:[{kind:'material',status:'open',blocks:['recommendation'],detail:'New synthetic material review.'}]}];later.result.operatorReviews[0].state='held';
  const f=await mounted({freshResearch:later.result});await button(f,'Copy product test brief').onclick();assert.equal(f.copies.length,1);corrective(f.copies[0]);assert.match(f.copies[0],/New synthetic material review/);assert.equal(button(f,'Corrective notes copied').disabled,false);f.dom.window.close();
});
test('a changed dossier is copied only as current context notes, never the earlier test brief',async()=>{
  const later=evidence({version:'c'.repeat(64)});later.dossier.recommendations[0].action='New synthetic context action.';
  const f=await mounted({freshResearch:later.result});await button(f,'Copy product test brief').onclick();assert.equal(f.copies.length,1);corrective(f.copies[0]);assert.match(f.copies[0],/New synthetic context action/);assert.ok(f.copies[0].includes('c'.repeat(64)));assert.ok(!f.copies[0].includes('Review distinct option-related search intents.'));f.dom.window.close();
});
test('failed fresh reads cannot fall back to copying old research',async()=>{
  const f=await mounted({freshResearch:()=>Promise.reject(Error('Synthetic unavailable read.'))});await button(f,'Copy product test brief').onclick();assert.equal(f.copies.length,0);assert.ok(button(f,'Current evidence or clipboard unavailable · refresh first'));f.dom.window.close();
});
test('selection changes cancel a pending copy before clipboard writes',async()=>{
  let resolve;const extra=evidence({id:'gid://shopify/Product/2302',handle:'second-fixture'}),pending=new Promise(done=>{resolve=done;}),f=await mounted({freshResearch:()=>pending,extraProduct:extra});
  const copy=button(f,'Copy product test brief'),writing=copy.onclick();await f.root.querySelectorAll('.row')[1].onclick();resolve(f.e.result);await writing;assert.equal(f.copies.length,0);f.dom.window.close();
});
test('measured copy independently refreshes research and demand and refuses changed demand versions',async()=>{
  const later=evidence();later.entry.evidence.dossierVersion='d'.repeat(64);const f=await mounted({freshDemand:{products:[later.entry]}});await until(()=>[...f.root.querySelectorAll('button')].some(b=>b.textContent==='Copy research and measured demand'));
  const before=f.reads.length;await button(f,'Copy research and measured demand').onclick();const newReads=f.reads.slice(before);assert.equal(newReads.filter(op=>op.startsWith('research?')).length,1);assert.equal(newReads.filter(op=>op.startsWith('demand?')).length,1);assert.equal(f.copies.length,0);assert.ok(button(f,'Evidence expired or changed · refresh first'));f.dom.window.close();
});
