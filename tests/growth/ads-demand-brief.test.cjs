'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const window={};vm.runInNewContext(fs.readFileSync(path.resolve(__dirname,'../../brites-growth.js'),'utf8'),{window,document:{querySelector:()=>null},URL});
const {demandBriefFor}=window.BritesGrowth,at=Date.now();
const dossier={productId:'gid://shopify/Product/10',handle:'fixture-bunny',version:'fixture-version',status:'approved',recommendations:[{channel:'keywords',action:'Test a relevant gift query.',measure:'Validated purchases.',sourceIds:['own']}],sources:[{id:'own',url:'https://britesjewelry.com/products/fixture-bunny'}]};
function record(){return{productId:dossier.productId,state:'current',productIssueState:'available',promotionAllowed:true,productHolds:{recommendationHold:false},evidence:{schemaVersion:1,evidenceRevision:3,evidenceKind:'product_demand_and_ad_outcomes',readOnly:true,sandboxReadOnly:true,productId:dossier.productId,handle:dossier.handle,dossierVersion:dossier.version,at,range:'LAST_30_DAYS',currency:'CAD',sources:{shopping:{state:'available',truncated:false}},shopping:{totals:{impressions:100,clicks:4,cost:2.5,reportedConversions:1}},planner:{state:'available',bidCurrency:'CAD',market:{pooled:true,countries:[{code:'CA'},{code:'US'}],languageName:'English',languageBasis:'research_hypothesis_from_verified_own_storefront',languageSource:{sourceUrl:'https://britesjewelry.com/'}},keywords:[{text:'fixture necklace',averageMonthlySearches:120,competition:'HIGH',lowTopOfPageBid:.4,highTopOfPageBid:1.5},{text:'missing volume',averageMonthlySearches:null,lowTopOfPageBid:null,highTopOfPageBid:null}]}}};}
test('measured brief carries actual scope, native currency and uncertainty into the existing copy workflow',()=>{
  const text=demandBriefFor(dossier,'Fixture product',record(),null,'available',at);
  assert.match(text,/Test a relevant gift query/);assert.match(text,/POOLED observed country targets: CA, US/);assert.match(text,/pooled average monthly searches 120/);assert.match(text,/0.4–1.5 CAD/);assert.match(text,/actual campaign language targeting is unknown/);assert.match(text,/https:\/\/britesjewelry.com\//);assert.match(text,/reported conversions \(unvalidated\) 1/);assert.match(text,/missing volume; pooled average monthly searches unavailable/);assert.match(text,/not actual competitor spend or predicted returns/);assert.match(text,/PMax terms are campaign context only/);
});
test('stale, foreign, changed-version and unsafe source records never produce a measured brief',()=>{
  const mutations=[r=>r.state='stale',r=>r.productId='gid://shopify/Product/99',r=>r.evidence.productId='gid://shopify/Product/99',r=>r.evidence.handle='other-product',r=>r.evidence.dossierVersion='old-version',r=>r.evidence.at=at-86400001,r=>r.evidence.at=at+60001,r=>r.evidence.evidenceRevision=2,r=>r.evidence.readOnly=false,r=>r.evidence.sandboxReadOnly=false,r=>r.evidence.schemaVersion=2,r=>r.evidence.evidenceKind='other'];
  for(const change of mutations){const r=record();change(r);assert.equal(demandBriefFor(dossier,'Fixture product',r,null,'available',at),null);}
  assert.equal(demandBriefFor({...dossier,status:'draft'},'Fixture product',record(),null,'available',at),null);
  const unsafe=record();unsafe.evidence.planner.market.languageSource.sourceUrl='javascript:alert(1)';assert.doesNotMatch(demandBriefFor(dossier,'Fixture product',unsafe,null,'available',at),/javascript:/);
});
test('newly held or unavailable issue checks retain measurements only as corrective research',()=>{
  for(const change of [r=>r.promotionAllowed=false,r=>r.productHolds.recommendationHold=true,r=>r.productIssueState='unavailable']){const r=record();change(r);const text=demandBriefFor(dossier,'Fixture product',r,null,'available',at);assert.match(text,/CORRECTIVE RESEARCH ONLY/);assert.match(text,/Promotion remains held/);assert.match(text,/pooled average monthly searches 120/);}
});
test('cart and story holds keep the whole copied brief corrective until reviewed',()=>{
  for(const field of ['cartHold','meaningHold']){const r=record();r.productHolds[field]=true;assert.match(demandBriefFor(dossier,'Fixture product',r,null,'available',at),/CORRECTIVE RESEARCH ONLY/);}
  for(const kind of ['history','content']){const issues={issues:[{kind,status:'open',detail:'Review exact story evidence.'}]};assert.match(demandBriefFor(dossier,'Fixture product',record(),issues,'available',at),/CORRECTIVE RESEARCH ONLY/);}
});
test('missing reports, unknown currency and detail limits remain explicit',()=>{
  const r=record();r.evidence.currency=null;r.evidence.shopping.totals.cost=null;r.evidence.sources.shopping.truncated=true;r.evidence.payloadBounds={state:'detail_sampled'};let text=demandBriefFor(dossier,'Fixture product',r,null,'available',at);assert.match(text,/reported spend unavailable currency unavailable/);assert.match(text,/complete totals are unavailable/);assert.match(text,/Saved detail is sampled/);
  r.evidence.sources.shopping.state='unavailable';r.evidence.planner={state:'unavailable',reason:'Targeting unknown'};text=demandBriefFor(dossier,'Fixture product',r,null,'available',at);assert.match(text,/missing reports are not zero outcomes/);assert.match(text,/Targeting unknown/);assert.doesNotMatch(text,/impressions 100/i);
});
