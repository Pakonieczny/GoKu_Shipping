'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {JSDOM}=require('jsdom');
const projection=require('../../netlify/functions/googleAdsAdDesignResearch');
const readOnly=require('../../netlify/functions/_britesGrowthAdsReadOnly');
const NOW=Date.now(),PRODUCT_ID='gid://shopify/Product/10',VERSION='a'.repeat(64);
function dossier(){return {productId:PRODUCT_ID,handle:'wolf-necklace',status:'approved',version:VERSION,currentDossierVersion:VERSION,savedAt:NOW,evidenceHolds:{recommendationHold:false,meaningHold:false,cartHold:false},sources:[{id:'shop',url:'https://example.com/wolf',title:'Wolf',excerpt:'Fixture.',reviewed:true,checkedAt:NOW}],competitors:[],facts:[],meanings:[],buyerIntents:['wolf gift'],recommendations:[{channel:'keywords',basis:'hypothesis',action:'Test wolf gift intent.',measure:'Verified purchases.',sourceIds:['shop'],keywords:['wolf necklace']},{channel:'negatives',basis:'hypothesis',action:'Review exclusions.',measure:'Relevant search traffic.',sourceIds:['shop'],negativeKeywords:['free template']}]};}
function ui(){const dom=new JSDOM('<div id="root"></div>',{url:'https://sandbox.invalid'}),context=vm.createContext(dom.window);vm.runInContext(fs.readFileSync(path.resolve(__dirname,'../../brites-growth.js'),'utf8'),context);return {dom,api:dom.window.BritesGrowth};}
function evidence(value){const packet=projection.projectRecommendations(value,new Set(['shop']),NOW)?.operatorReviewPacket;return {product:{productId:PRODUCT_ID,handle:value.handle,dossierVersion:value.version},result:{productIssueState:'available',productIssues:[],operatorReviews:[{productId:PRODUCT_ID,handle:value.handle,dossierVersion:value.version,state:'pending_operator_review',operatorReviewPacket:packet}]},packet};}
test('canonical projector rejects whole-token containment without the read-only adapter monkey patch',()=>{
  const direct=dossier();direct.recommendations[1].negativeKeywords=['necklace'];
  assert.equal(projection.projectRecommendations(direct,new Set(['shop']),NOW),null);
  const inverse=dossier();inverse.recommendations[0].keywords=['wolf'];inverse.recommendations[1].negativeKeywords=['wolf necklace'];
  assert.equal(projection.projectRecommendations(inverse,new Set(['shop']),NOW),null);
  assert.equal(projection.sharedDossierIsCurrent(direct,{id:PRODUCT_ID,handle:direct.handle},NOW),false);
});
test('canonical projector keeps unrelated whole-token terms',()=>{
  const value=dossier();value.recommendations[0].keywords=['cat necklace'];value.recommendations[1].negativeKeywords=['caterpillar necklace'];
  assert.ok(projection.projectRecommendations(value,new Set(['shop']),NOW)?.operatorReviewPacket);
});
test('cart holds block projection and current-dossier eligibility',()=>{
  const value=dossier();value.evidenceHolds.cartHold=true;
  assert.equal(projection.projectRecommendations(value,new Set(['shop']),NOW),null);
  assert.equal(projection.sharedDossierIsCurrent(value,{id:PRODUCT_ID,handle:value.handle},NOW),false);
});
test('read adapter keeps cart holds closed with an older projection implementation',()=>{
  const legacy={projectRecommendations:()=>({operatorReviewPacket:{positiveKeywords:[],negativeKeywords:[]}})};
  readOnly.hardenReviewProjection(legacy);
  assert.equal(legacy.projectRecommendations({evidenceHolds:{cartHold:true}}),null);
});
test('operator UI rejects phrase-containment conflicts even if a foreign packet bypasses server checks',()=>{
  const value=dossier(),e=evidence(value),{dom,api}=ui();
  value.recommendations[1].negativeKeywords=['necklace'];
  e.packet.candidates[1].negativeKeywords=['necklace'];e.packet.negativeKeywords[0].term='necklace';
  assert.equal(api.operatorReviewFor(e.result,e.product,value,NOW),null);dom.window.close();
});
test('operator UI withholds a packet for a cart-only issue hold',()=>{
  const value=dossier(),e=evidence(value),{dom,api}=ui();
  e.result.productIssues=[{productId:PRODUCT_ID,issues:[{kind:'fulfillment',status:'open',blocks:['cart']}]}];
  assert.equal(api.operatorReviewFor(e.result,e.product,value,NOW),null);dom.window.close();
});
