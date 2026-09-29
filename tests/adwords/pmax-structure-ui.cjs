// Performance Max structure in the console. A product can join a Performance Max campaign that already runs (the
// opportunity card and the complete-ad chooser), defaulting to the one on the same feed and countries, with the
// combined budget in words; a finished draft settles out of the list instead of jumping. Approvals says plainly
// what each new draft changes: the combined budget against the daily ceiling (a shared budget once, an ended
// campaign not at all), each exclusion with why, and the brand exclusion beside the brand Search campaign.
// Offline: the page's code runs against stand-ins; nothing is sent anywhere.
'use strict';
const fs=require('fs'),path=require('path'),vm=require('vm'),assert=require('assert/strict'),{JSDOM}=require('jsdom');
const html=fs.readFileSync(path.resolve(__dirname,'../../brites-adwords.html'),'utf8');
const CS=require('../../brites-campaign-styles'),S=require('../../netlify/functions/_googleAdsPmaxStructure'),model=require('../../assets/pmax-recommendation.js');
let n=0;const check=(v,m)=>{assert.ok(v,m);n++;};
const fn=name=>{const m=html.match(new RegExp('^function '+name+'\\([\\s\\S]*?\\n(?=function |var |//)','m'));assert.ok(m,'the page defines '+name);return m[0];};
const esc=v=>String(v==null?'':v).replace(/[&<>"']/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
const tick=ms=>new Promise(r=>setTimeout(r,ms));
const B$='customers/123/campaignBudgets/';
// Spendable now: 101, 104, 105, 107, 108 and the shared Search budget once = 53 of a 62 ceiling.
// 103 has ended (50 not counted), 102 is paused (15 not counted).
const METRICS=[{id:'101',name:'BA · Necklaces PMax',status:'ENABLED',budget:20,budgetRes:B$+901},{id:'102',name:'BA · Rings PMax',status:'PAUSED',budget:15,budgetRes:B$+902},
  {id:'103',name:'BA · Old PMax',status:'ENABLED',primaryStatus:'ENDED',budget:50,budgetRes:B$+903},{id:'104',name:'BA · CA PMax',status:'ENABLED',budget:10,budgetRes:B$+904},
  {id:'105',name:'BA · Other store',status:'ENABLED',budget:5,budgetRes:B$+905},{id:'107',name:'BA · Charms',status:'ENABLED',budget:3,budgetRes:B$+907},
  {id:'108',name:'BA · Earrings',status:'ENABLED',budget:7,budgetRes:B$+908},{id:'201',name:'BA · brand-search',status:'ENABLED',budget:8,budgetRes:B$+950},
  {id:'202',name:'BA · rings-search',status:'ENABLED',budget:8,budgetRes:B$+950}];
// Destinations as the dashboard sends them (pmaxCampaignTargets): 104 is on the CA feed; 106 has no feed label,
// so it advertises every feed.
const target=(id,name,o={})=>({id,name,status:'ENABLED',merchantId:'555',feedLabel:'US',countries:['2840'],countryCodes:['US'],countryNames:['United States'],assetGroups:2,budget:20,sharedBudget:false,brandGuidelinesEnabled:false,bidding:'Maximize conversion value',...o});
const TARGETS=[target('101','BA · Necklaces PMax'),target('102','BA · Rings PMax',{status:'PAUSED',budget:15,assetGroups:1,brandGuidelinesEnabled:true}),
  target('104','BA · CA PMax',{feedLabel:'CA',countries:['2124'],countryCodes:['CA'],countryNames:['Canada'],budget:10,assetGroups:1}),target('106','BA · All products',{feedLabel:null,countries:[],countryCodes:[],countryNames:[],allCountries:true,budget:30,assetGroups:4})];

(async()=>{
  // ===== A. Approvals: what each draft changes =====
  const A={esc,BritesCampaignStyles:CS,Intl,DASH:{budgetCurrency:'CAD',control:{maxDailyBudgetTotal:62},lastMetrics:METRICS}};vm.createContext(A);
  for(const f of ['money','apCurrency','apMoney','apCampaignName','apMatch','apCeiling','apFacts','apTerms','apPmaxJoin','apPmaxNegatives','apBrandExclusion','apPmaxSearches','approvalAction','approvalTitle'])vm.runInContext(fn(f),A);
  const page=new JSDOM('<body></body>').window.document,view=h=>{const div=page.createElement('div');div.innerHTML=h;return div;};
  const facts=h=>{const out={};view(h).querySelectorAll('dl.apFacts dt').forEach(dt=>{out[dt.textContent]=dt.nextElementSibling;});return out;};
  const main=dd=>Array.from(dd.childNodes).filter(x=>x.nodeType===3).map(x=>x.textContent).join(''),small=dd=>(dd.querySelector('small')||{textContent:''}).textContent;
  const join=(meta={})=>({id:'j1',type:'pmax',payload:{mutateOperations:[],existingPmaxGuard:{campaignId:'101'},meta:{kind:'pmaxAddToCampaign',existingCampaignId:'101',existingCampaignName:'BA · Necklaces PMax',existingCampaignStatus:'ENABLED',brandGuidelinesEnabled:false,budgetBefore:20,addedDaily:5,dailyBudget:25,sharedBudget:false,existingAssetGroups:2,targetCountries:['United States'],bidding:'Maximize conversion value · target ROAS 350%',budgetCurrency:'CAD',collectionTitle:'Animal necklaces',itemIds:['shopify_US_11_101','shopify_US_22_201'],feedLabel:'US',groupNames:['AG · Corgi necklace · 2'],...meta}}});
  let card=A.apPmaxJoin(join()),F=facts(card.body);
  check(JSON.stringify(card.glance)==='["BA · Necklaces PMax","$25 CAD/day combined"]','the joining draft is glanced as its campaign and the combined budget');
  check(main(F['Adds to'])==='BA · Necklaces PMax'&&small(F['Adds to'])==='Existing Performance Max campaign · running','it names the running campaign the product joins');
  check(main(F['Product group'])==='AG · Corgi necklace · 2'&&small(F['Product group'])==='2 selected products · US Merchant Center feed','and the product group it adds');
  check(main(F['Daily budget'])==='CAD 20.00 + 5.00 = 25.00/day combined, shared by 3 product groups.'&&small(F['Daily budget'])==='Daily ceiling: $58 of $62 CAD with this campaign'&&!F['Daily budget'].querySelector('.apWarn'),'the combined budget in words, and the ceiling counting the shared budget once and no ended or paused campaign');
  check(F.Starts.textContent==='As soon as it is published: the campaign is running.'&&F.Bidding.textContent==='Maximize conversion value · target ROAS 350% · unchanged'&&F.Countries.textContent==='United States · unchanged','when it starts, and that bidding and countries stay the campaign\'s own');
  check(F['Stays the same'].textContent==='The campaign’s other product groups, language, excluded searches and schedule','what stays as it is');
  F=facts(A.apPmaxJoin(join({addedDaily:10,dailyBudget:30})).body);
  check(F['Daily budget'].querySelector('small.apWarn').textContent==='Over the daily ceiling: $63 of $62 CAD with this campaign. Publishing will be refused; lower the budget or raise the ceiling in Controls.','a raise over the ceiling is flagged before approval');
  card=A.apPmaxJoin(join({addedDaily:0,dailyBudget:20}));F=facts(card.body);
  check(card.glance[1]==='$20 CAD/day combined'&&main(F['Daily budget'])==='CAD 20.00/day combined, shared by 3 product groups.'&&!F['Daily budget'].querySelector('small')&&/and schedule, and its budget$/.test(F['Stays the same'].textContent),'adding nothing keeps the budget, and says so');
  F=facts(A.apPmaxJoin(join({existingCampaignId:'102',existingCampaignName:'BA · Rings PMax',existingCampaignStatus:'PAUSED',budgetBefore:15,dailyBudget:20,existingAssetGroups:1})).body);
  check(small(F['Adds to'])==='Existing Performance Max campaign · paused'&&main(F['Daily budget'])==='CAD 15.00 + 5.00 = 20.00/day combined, shared by 2 product groups. The campaign is paused; the product runs once you enable it.'&&!F['Daily budget'].querySelector('small')&&F.Starts.textContent==='When you enable the campaign in Overview.','a paused campaign: the raise waits for enabling (checked then), and the card says where to enable it');
  check(/shared by 3 product groups and every campaign using this shared budget\.$/.test(main(facts(A.apPmaxJoin(join({sharedBudget:true})).body)['Daily budget'])),'a shared budget is named as shared');

  const negs={service:'campaignCriteria',operations:[{create:{}},{create:{}},{create:{}}],meta:{kind:'pmaxNegatives',existingCampaignId:'101',campaignName:'BA · Necklaces PMax',terms:[{text:'how to make',matchType:'PHRASE',reason:'default'},{text:'wholesale',matchType:'BROAD',reason:'default'},{text:'corgi necklace pattern',matchType:'EXACT',reason:'waste',clicks:42,cost:12.5}],days:90,wasteCost:8,wasteClicks:30,budgetCurrency:'CAD'}};
  card=A.apPmaxNegatives(negs);F=facts(card.body);const nv=view(card.body);
  check(JSON.stringify(card.glance)==='["BA · Necklaces PMax","3 exclusions"]'&&F.Excludes.textContent==='3 searches · BA · Necklaces PMax stops showing ads for them on Google Search and Shopping','the exclusions draft says how many searches stop showing ads, and where');
  check(F.Why.textContent==='2 standard exclusions for searches a made-to-order jewellery shop cannot sell to: makers, resellers, jobs and digital files. 1 search with at least 30 clicks and $8 CAD spent without a sale in the last 90 days','and why, per kind');
  check(F['Never excluded'].textContent==='Searches for Brites (the brand exclusion covers them) and the campaign’s own search themes','and what is never excluded');
  check([...nv.querySelectorAll('.apTerms span')].map(x=>x.textContent).join('|')==='how to makephrase|wholesalebroad'&&[...nv.querySelectorAll('.apTable tbody tr')].map(r=>[...r.cells].map(c=>c.textContent).join('/')).join()==='corgi necklace pattern exact/42/$12.50 CAD','the standard exclusions with their match, and each wasted search with its clicks and cost');
  card=A.apPmaxNegatives({...negs,meta:{...negs.meta,terms:[negs.meta.terms[2]]}});F=facts(card.body);
  check(card.glance[1]==='1 exclusion'&&F.Excludes.textContent==='1 search · BA · Necklaces PMax stops showing ads for it on Google Search and Shopping'&&F.Why.textContent==='1 search with at least 30 clicks and $8 CAD spent without a sale in the last 90 days'&&!view(card.body).querySelector('.apTerms'),'one wasted search reads in the singular, with no empty list');

  const brandDraft=(meta={})=>({mutateOperations:[],meta:{kind:'pmaxBrandExclusion',brand:{entityId:'b-777',name:'Brites',urls:['https://britesjewelry.com']},list:{name:'Brites · brand exclusions',reused:false,addsBrand:true},campaigns:[{id:'101',name:'BA · Necklaces PMax',status:'ENABLED'},{id:'102',name:'BA · Rings PMax',status:'PAUSED'}],brandCost:7.56,days:90,brandSearch:{state:'live',name:'BA · brand-search'},budgetCurrency:'CAD',...meta}});
  card=A.apBrandExclusion(brandDraft());F=facts(card.body);
  check(JSON.stringify(card.glance)==='["Brites","2 campaigns"]'&&main(F.Excludes)==='Searches for the Brites brand (https://britesjewelry.com)'&&small(F.Excludes)==='Performance Max stops paying for people who already search for you by name','the brand exclusion names Google\'s brand and what it stops');
  check(F['Brand list'].textContent==='Creates the list “Brites · brand exclusions”'&&main(F.Campaigns)==='BA · Necklaces PMax, BA · Rings PMax'&&small(F.Campaigns)==='New Performance Max campaigns copy this exclusion when they are published','the list it creates, the campaigns it covers, and that new campaigns copy it');
  check(F['Last 90 days'].textContent==='$7.56 CAD spent on searches for Brites in these campaigns'&&F['Brand Search'].textContent==='“BA · brand-search” is running, so searches for Brites keep showing your brand ad.','what brand searches cost, and the brand Search campaign that keeps an ad on them');
  const pair=state=>facts(A.apBrandExclusion(brandDraft({brandSearch:{state,name:'BA · brand-search'}})).body)['Brand Search'].textContent;
  check(pair('paused')==='“BA · brand-search” is paused. Enable it with this exclusion, or searches for Brites show no ad.'&&pair('draft')==='The brand Search campaign is waiting in Approvals. Publish it with this exclusion, or searches for Brites show no ad.'&&pair('none')==='No brand Search campaign yet. Once this is published, searches for Brites show no ad until one runs; your site still appears in the free results.','a paused, waiting or missing brand Search campaign is said plainly');
  check(facts(A.apBrandExclusion(brandDraft({list:{name:'Brites · brand exclusions',reused:true,addsBrand:true}})).body)['Brand list'].textContent==='Uses your list “Brites · brand exclusions” and adds the brand to it'&&facts(A.apBrandExclusion(brandDraft({list:{name:'My brand',reused:true,addsBrand:false},brandCost:0})).body)['Brand list'].textContent==='Uses your list “My brand”'&&facts(A.apBrandExclusion(brandDraft({brandCost:0})).body)['Last 90 days'].textContent==='No spend on searches for Brites recorded in these campaigns','a reused or filled list, and no recorded brand spend, are stated');

  const camp='customers/123/campaigns/-2',fresh={mutateOperations:[{campaignCriterionOperation:{create:{campaign:camp,location:{geoTargetConstant:'geoTargetConstants/2840'}}}},{campaignCriterionOperation:{create:{campaign:camp,language:{languageConstant:'languageConstants/1000'}}}},...S.negativeOps(camp,S.PMAX_DEFAULT_NEGATIVES)]};
  check(A.apPmaxSearches(fresh)==='English searches only · 16 excluded searches (makers, resellers, jobs, digital files)'&&A.apPmaxSearches({mutateOperations:[]})==='','a new campaign says which searches it serves on (and nothing when there is nothing to say)');

  let act=A.approvalAction(join(),false);
  check(act.tag==='Product ads'&&act.from==='Opportunities'&&act.to==='Google Ads · joins BA · Necklaces PMax'&&act.publish==='Add to campaign'&&A.approvalAction(join(),true).publish==='Validate draft'&&A.approvalTitle(join())==='Add Animal necklaces to BA · Necklaces PMax','a joining draft is titled and labelled as joining its campaign (validated first in a dry run)');
  act=A.approvalAction({type:'negatives',payload:brandDraft()},false);
  check(act.tag==='Exclusions'&&act.to==='Google Ads · brand searches excluded'&&act.publish==='Exclude brand searches'&&A.approvalTitle({payload:brandDraft()})==='Exclude brand searches from Performance Max','the brand exclusion is titled and labelled as such');
  act=A.approvalAction({type:'negatives',payload:negs},false);
  check(act.to==='Google Ads · blocked searches'&&act.publish==='Add 3 exclusions'&&A.approvalTitle({payload:negs})==='Exclusions · BA · Necklaces PMax','the exclusions draft names its campaign');
  const render=html.slice(html.indexOf('function renderApprovals('),html.indexOf('\n}',html.indexOf('function renderApprovals('))),at=s=>{const i=render.indexOf(s);assert.ok(i>=0,'renderApprovals has '+s);return i;};
  check(at('pl.meta.kind==="pmaxAddToCampaign"){var jn=apPmaxJoin(a)')<at('else if(a.type==="pmax"&&pl.meta){var m=pl.meta;')&&at('pl.meta.kind==="pmaxBrandExclusion"){var bx=apBrandExclusion(pl)')<at('else if(pl.mutateOperations){var c=parseCreative')&&at('(pl.meta||{}).kind==="pmaxNegatives"){var pn=apPmaxNegatives(pl)')<at('else if(pl.service==="adGroupCriteria"||pl.service==="campaignCriteria")'),'each new draft gets its own card before the generic ones');
  check(at('["Searches",esc(apPmaxSearches(pl))]')>at('else if(a.type==="pmax"&&pl.meta){var m=pl.meta;'),'the new campaign card carries the searches it serves on');

  // ===== B. Opportunity card: join a campaign, or start one =====
  const src=html.slice(html.indexOf('function pmaxProductChoices('),html.indexOf('function renderOpportunities(){'));
  const offers=['shopify_US_11_101','shopify_US_22_201'],paid={available:true,monetaryComplete:true,currency:'USD',days:90,impressions:10000,clicks:200,conversions:10,cost:100,value:500};
  const candidate=()=>({tag:'pmax-animal-necklaces|US',handle:'animal-necklaces',recommendationSchema:1,collectionTitle:'Animal necklaces',feedLabel:'US',budgetCurrency:'CAD',itemIds:offers,productTitles:['Corgi necklace','Fox necklace'],offerDetails:[{itemId:offers[0],title:'Corgi necklace',paidPerformance:paid,evidenceIds:['corgi']},{itemId:offers[1],title:'Fox necklace',paidPerformance:paid,evidenceIds:['fox']}],demandEvidence:[{evidenceId:'corgi',orders:5,orders30d:2,revenue:250,revenue30d:100},{evidenceId:'fox',orders:50,orders30d:20,revenue:2500,revenue30d:1000}],paidPerformance:{available:true,monetaryComplete:true,currency:'USD',days:90},dailyBudget:12,days:30,evidenceDays:90,demandCoverage:{days30:true,days90:true,monetaryComplete:true},seasonalityCoverage:{complete:true}});
  const dom=new JSDOM('<body><main><div id="oppList"></div></main></body>'),d=dom.window.document,requests=[],toasts=[];let answer=null;
  const P={window:{BritesPmaxRecommendation:model},document:d,Intl,Date,Math,JSON,console,Promise,setTimeout,clearTimeout,setInterval,clearInterval,esc,apvEnc:encodeURIComponent,friendlyResearchError:String,researchNeedsRefresh:()=>false,
    toast:m=>toasts.push(String(m)),acctMoney:v=>'$'+v,wireOpportunityDeletion:()=>{},reload:async()=>{},setOppMeta:()=>{},PMAXAT:Date.now(),PMAXERR:null,BritesCampaignStyles:CS,DASH:{control:{defaultCountries:['2840']},pmaxTargets:TARGETS},
    api:async(action,payload)=>{requests.push([action,JSON.parse(JSON.stringify(payload))]);if(action==='generatePmax')return {queued:true,genId:payload.genId};if(action==='genStatus')return await new Promise(r=>{answer=r;});throw Error('Unexpected '+action);}};
  P.renderOpportunities=()=>P.renderPmaxSection(d.getElementById('oppList'));vm.createContext(P);vm.runInContext(src,P);
  const $=q=>d.querySelector('#pmaxSec '+q),sel=()=>$('.pmx-dest[data-i="0"]'),bud=()=>$('.pmx-bud[data-i="0"]'),gen=()=>$('.pmx-gen[data-i="0"]'),line=()=>$('[data-pmx-join="0"]'),label=()=>bud().closest('label').textContent;
  const design=()=>JSON.parse(decodeURIComponent($('[data-pmx-design="0"]').dataset.designOpportunity)),generated=()=>requests.filter(r=>r[0]==='generatePmax').map(r=>r[1]);
  const start=()=>{vm.runInContext('PMAX_UI={}',P);P.PMAXOPPS=[candidate()];P.renderOpportunities();};
  start();
  check(JSON.stringify([...sel().options].map(o=>o.value))==='["new","101","102","106"]'&&sel().value==='101','the card offers the campaigns on this feed (a campaign without a feed label takes every feed), defaulting to the running one on the same feed and countries');
  check([...sel().options].slice(1).map(o=>o.textContent).join('|')==='BA · Necklaces PMax · CAD 20.00/day · 2 product groups|BA · Rings PMax · CAD 15.00/day · 1 product group · paused|BA · All products · CAD 30.00/day · 4 product groups','each destination shows its budget, its product groups and whether it is paused');
  check(label()==='Add CAD $/day to its budget'&&bud().min==='0'&&bud().value==='12'&&line().textContent==='CAD 20.00 + 12.00 = 32.00/day combined, shared by 3 product groups.','joining, the budget field is what the product adds, with the combined budget beside it');
  bud().value='5';bud().oninput();
  check(line().textContent==='CAD 20.00 + 5.00 = 25.00/day combined, shared by 3 product groups.'&&P.pmaxUi(P.PMAXOPPS[0]).add==='5','the combined budget follows what is typed');
  check(design().dailyBudget===12,'"Design ad" keeps the new-campaign budget, not the amount added');
  P.renderOpportunities();check(bud().value==='5'&&line().textContent==='CAD 20.00 + 5.00 = 25.00/day combined, shared by 3 product groups.'&&design().dailyBudget===12,'a redraw keeps the amount typed and the new-campaign budget apart');
  gen().click();await tick(20);
  check(generated().length===1&&generated()[0].existingCampaignId==='101'&&generated()[0].addBudget===5&&sel().disabled&&gen().disabled,'the draft joins that campaign with what it adds; the destination is locked while it is written');
  sel().value='new';sel().onchange();check(!('dest' in P.pmaxUi(P.PMAXOPPS[0])),'the destination cannot change while the draft is written');
  answer({ok:true,approvalId:'a1',joined:{campaignId:'101',name:'BA · Necklaces PMax',dailyBudget:25,addedDaily:5},itemIds:[offers[0]]});await tick(20);
  check(toasts.includes('PMax draft ready — adds the products to “BA · Necklaces PMax”, in Approvals ✓')&&gen().disabled&&gen().textContent==='In Approvals ✓'&&$('.pmx-msg[data-i="0"]').textContent==='✓ in Approvals','the ready draft names the campaign it joins; the card pauses on its confirmation');
  bud().value='6';bud().oninput();check(gen().disabled,'typing during that pause does not offer the draft again');
  gen().onclick.call(gen());await tick(20);check(generated().length===1,'nothing more is sent during that pause');
  await tick(1600);check(!gen()&&$('.oppChannelEmpty'),'then the card folds away and the list is drawn again');
  // A new campaign instead: the card's budget is the campaign's own again.
  start();sel().value='new';sel().onchange();
  check(!line()&&label()==='CAD $/day · 30d'&&bud().min==='3'&&bud().value==='12'&&d.activeElement===sel(),'"New campaign" brings back its own budget and run, keeping focus on the choice');
  gen().click();await tick(20);check(!('existingCampaignId' in generated()[1])&&generated()[1].dailyBudget===12,'a new campaign draft names no existing campaign');
  answer({ok:false,error:'Merchant check failed'});await tick(20);check(!sel().disabled&&!gen().disabled,'a failed draft unlocks the destination again');
  // A paused campaign; adding nothing; an amount out of range.
  sel().value='102';sel().onchange();
  check(line().textContent==='CAD 15.00 + 12.00 = 27.00/day combined, shared by 2 product groups. The campaign is paused; the product runs once you enable it.','a paused destination says the product runs once it is enabled');
  bud().value='0';bud().oninput();check(line().textContent==='CAD 15.00/day combined, shared by 2 product groups. The campaign is paused; the product runs once you enable it.','adding 0 keeps its budget');
  gen().click();await tick(20);check(generated()[2].existingCampaignId==='102'&&generated()[2].addBudget===0,'a draft may add nothing to the budget');
  answer({ok:false,error:'x'});await tick(20);
  bud().value='31';bud().oninput();gen().click();await tick(20);
  check(generated().length===3&&toasts.includes('Add between $0 and $30 a day, or 0 to keep its budget.'),'an amount outside 0 to 30 a day is refused before anything is sent');
  P.DASH={control:{defaultCountries:['2840']},pmaxTargets:TARGETS.filter(t=>t.id==='104')};start();
  check(!sel()&&!line()&&label()==='CAD $/day · 30d','with no campaign on this feed there is no choice to make');
  P.DASH={control:{defaultCountries:['2840']},pmaxTargets:TARGETS};
  // The settle, step by step: the count rolls, the card folds, the list is drawn again.
  {const s=new JSDOM('<!doctype html><body><div id="pmaxSec"><section class="oppChannel"><header><span class="oppChannelCount">2 new</span></header><div class="pmxList"><article class="pmxrow" id="a"></article><article class="pmxrow" id="b"><button class="pmx-gen" data-working="false">Create review draft</button></article></div></section></div></body>',{runScripts:'outside-only'}),w=s.window,sd=w.document;
   const timers=[],flow=[],renders=[];w.setTimeout=(f,ms)=>{timers.push({f,ms});return timers.length;};const run=ms=>{const t=timers.shift();assert.equal(t.ms,ms);t.f();};
   Object.assign(w,{setOppMeta(){flow.push('meta');},renderOpportunities(){renders.push(1);},BritesFlow:{tick:(el,up)=>flow.push('tick '+el.textContent+' '+up),leave:el=>{flow.push('leave '+el.id);el.hidden=true;}}});
   w.eval(fn('pmaxSettle'));w.pmaxSettle(sd.getElementById('a'));check(flow.length===0&&!sd.getElementById('a').hidden,'the confirmation stays a moment');run(1200);
   check(JSON.stringify(flow)==='["meta","tick 1 new false","leave a"]'&&!renders.length,'then the count rolls down and the card folds away');run(300);check(renders.length===1,'and the list is drawn again once the fold is done');
   sd.querySelector('.oppChannelCount').textContent='2 new';sd.querySelector('#b .pmx-gen').dataset.working='true';flow.length=0;renders.length=0;
   w.pmaxSettle(sd.getElementById('a'));run(1200);run(300);check(!renders.length&&!sd.getElementById('a')&&sd.getElementById('b'),'while another draft is written only the settled card goes');
   sd.querySelector('.pmxList').insertAdjacentHTML('afterbegin','<article class="pmxrow" id="c"></article>');const c=sd.getElementById('c');flow.length=0;w.pmaxSettle(c);c.remove();run(1200);
   check(JSON.stringify(flow)==='["meta"]'&&!timers.length,'a list already drawn again needs nothing more');
   delete w.BritesFlow;sd.querySelector('#b .pmx-gen').dataset.working='false';sd.querySelector('.pmxList').insertAdjacentHTML('afterbegin','<article class="pmxrow" id="e"></article>');const e=sd.getElementById('e');
   w.pmaxSettle(e);run(1200);check(e.hidden===true&&sd.querySelector('.oppChannelCount').textContent==='0 new','without the motion module the card still goes');run(300);check(renders.length===1,'and the list is drawn again');s.window.close();}

  // ===== C. Complete-ad chooser: Performance Max joins a campaign =====
  const csrc=html.slice(html.indexOf('function campaignStyleChoices('),html.indexOf('function adApprovalDesignReviewHtml('));
  const cd=new JSDOM('<body></body>').window.document,asked=[];let plan=null;
  const C={document:cd,elFrom:h=>{const div=cd.createElement('div');div.innerHTML=h;return div.firstElementChild;},esc,adAttr:esc,adApprovalDesignReviewHtml:()=>'',wireApprovalAllSizes:()=>{},btnBusy:b=>{b.disabled=true;return()=>{b.disabled=false;};},
    api:async(action,payload)=>{asked.push([action,JSON.parse(JSON.stringify(payload))]);return await new Promise(r=>plan=r);},toast:()=>{},reload:async()=>{},openCountryEditor:()=>{},BritesCampaignStyles:CS,
    campaignBadgeHtml:(x,o)=>CS.badge(x,o),campaignStyleIcon:(k,s)=>CS.iconSvg(CS.describe(k).icon,s),COUNTRIES:[{id:'2840',name:'United States'}],countriesLoaded:true,ensureCountries:async()=>[],renderAll:()=>{},Intl,
    DASH:{budgetCurrency:'CAD',control:{maxDailyBudgetTotal:62},lastMetrics:METRICS,pmaxTargets:TARGETS}};
  vm.createContext(C);vm.runInContext(csrc,C);for(const f of ['money','apCurrency','apMoney','apCountries','ctyName','sgCountryText'])vm.runInContext(fn(f),C);vm.runInContext(html.match(/^var CTY_KNOWN=.*$/m)[0],C);
  const submission=()=>{const k=C.adDesignSubmissionCard({id:'s1',summary:'Peach charm'});cd.body.replaceChildren(k);C.wireAdDesignSubmission(k,{id:'s1',reviewHash:'h',designReview:{context:{countries:['2840'],feedLabel:'US'}}});return k;};
  let k=submission();const q=s=>k.querySelector(s),box=style=>q('[data-campaign-style="'+style+'"]'),money_=style=>q('[data-style-budget="'+style+'"]'),days=style=>q('[data-style-duration="'+style+'"]');
  const pick=(style,on)=>{box(style).checked=on;box(style).onchange();},type=(el,v)=>{el.value=String(v);el.oninput();},aim=v=>{q('[data-pmax-target]').value=v;q('[data-pmax-target]').onchange();};
  const total=()=>q('[data-style-total]').textContent,hint=()=>q('[data-pmax-join]'),limit=()=>q('[data-style-limit]'),prepare=()=>q('[data-prepare-styles]'),publish=()=>q('[data-publish-submission]');
  check(!q('[data-pmax-target-label]').hidden&&JSON.stringify([...q('[data-pmax-target]').options].map(o=>o.value))==='["new","101","102","106"]'&&q('[data-pmax-target]').value==='101'&&q('[data-pmax-target]').disabled,'the chooser offers the campaigns on this feed, defaulting to the one on the same feed and countries, until Performance Max is chosen');
  pick('pmax',true);
  check(!q('[data-pmax-target]').disabled&&!hint().hidden&&q('[data-style-budget-text="pmax"]').textContent==='Add to its daily budget (CAD)'&&money_('pmax').min==='0'&&days('pmax').disabled,'joining, the budget is what it adds and the campaign keeps its own schedule');
  check(prepare().disabled&&total()==='1 product group in “BA · Necklaces PMax” · adds $0 CAD/day'&&publish().textContent==='Approve & add the product group','an amount is required (0 keeps the budget); the button says what publishing does');
  type(money_('pmax'),0);check(!prepare().disabled&&hint().textContent==='CAD 20.00/day combined, shared by 3 product groups.','0 is an explicit choice: the campaign keeps its budget');
  type(money_('pmax'),5);check(hint().textContent==='CAD 20.00 + 5.00 = 25.00/day combined, shared by 3 product groups.'&&total()==='1 product group in “BA · Necklaces PMax” · adds $5 CAD/day'&&limit().hidden,'the combined budget follows what is added, within the ceiling');
  type(money_('pmax'),10);check(!limit().hidden&&limit().textContent==='Over your daily ceiling: these budgets total CAD 10.00, but only CAD 9.00 of your CAD 62.00 ceiling is free (enabled campaigns use CAD 53.00). Lower a budget, or raise the ceiling in Controls.','a running campaign\'s raise counts against the ceiling (shared budget once, ended and paused campaigns not at all)');
  aim('102');check(limit().hidden&&/The campaign is paused; the product runs once you enable it\.$/.test(hint().textContent),'a paused campaign\'s raise is checked when it is enabled, not now');
  aim('101');type(money_('pmax'),5);pick('responsive_display',true);type(money_('responsive_display'),5);type(days('responsive_display'),30);
  check(total()==='1 new campaign and 1 product group in “BA · Necklaces PMax” · adds $10 CAD/day'&&publish().textContent==='Approve, add the product group & create paused campaigns','with a Display campaign too, both are counted and named');
  pick('responsive_display',false);prepare().onclick();await tick(0);
  const ask=asked[asked.length-1][1];
  check(asked.length===1&&ask.prepareOnly===true&&JSON.stringify(ask.styles)==='["pmax"]'&&ask.budgets.pmax===5&&ask.durations.pmax===0&&ask.pmaxTarget==='101'&&JSON.stringify(ask.countries)==='["2840"]','preparing sends the chosen campaign and what it adds, with no run length of its own');
  plan({ok:true,planHash:'p1',plan:{currency:'CAD',totalDaily:5,campaigns:[{style:'pmax',joins:true,name:'BA · Necklaces PMax',existingCampaignId:'101',campaignStatus:'ENABLED',budgetBefore:20,addedDaily:5,dailyBudget:25,assetGroups:2,sharedBudget:false,countries:['United States'],bidding:'Maximize conversion value · unchanged',formats:['square','landscape']}],note:'Only “BA · Necklaces PMax” changes.'}});await tick(0);
  const shown=q('[data-style-plan]').textContent;
  check(shown.includes('Adds a product group to “BA · Necklaces PMax”')&&shown.includes('square, landscape · CAD 20.00 + 5.00 = 25.00/day combined, shared by 3 product groups. · Countries: United States · Maximize conversion value · unchanged')&&shown.includes('Adds CAD 5.00/day to “BA · Necklaces PMax”'),'the plan shows the joined campaign, its combined budget and what the change adds');
  check(C.campaignStylePlanTotal({currency:'CAD',totalDaily:15,campaigns:[{name:'A'}]})==='Total: CAD 15.00/day when enabled'&&C.campaignStylePlanTotal({currency:'CAD',totalDaily:5,campaigns:[{joins:true,name:'N',addedDaily:0},{name:'D'}]})==='“N” keeps its daily budget · new campaigns: CAD 5.00/day when enabled'&&C.campaignStylePlanTotal({currency:'CAD',totalDaily:10,campaigns:[{joins:true,name:'N',addedDaily:5},{name:'D'}]})==='Adds CAD 5.00/day to “N” · new campaigns: CAD 5.00/day when enabled','the plan total separates a raise applied at publication from new budgets that apply when enabled');
  k=submission();
  check(q('[data-pmax-target]').value==='101'&&box('pmax').checked&&money_('pmax').value==='5'&&q('[data-style-plan]').textContent.includes('Adds a product group to “BA · Necklaces PMax”'),'the ten-minute refresh keeps the chosen campaign, the amount and the prepared plan');
  aim('102');k=submission();
  check(q('[data-pmax-target]').value==='102'&&/The campaign is paused; the product runs once you enable it\.$/.test(hint().textContent),'a campaign other than the default is kept too');
  aim('new');
  check(hint().hidden&&q('[data-style-budget-text="pmax"]').textContent==='Average daily budget (CAD)'&&money_('pmax').min==='0.01'&&!days('pmax').disabled&&total()==='1 campaign · $5 CAD/day combined'&&publish().textContent==='Approve & create paused campaigns'&&prepare().disabled,'"New campaign" is its own campaign again: its own budget and a run length to choose');
  type(days('pmax'),30);check(!prepare().disabled,'with a run length it can be prepared');prepare().onclick();await tick(0);check(asked[asked.length-1][1].pmaxTarget===null&&asked[asked.length-1][1].durations.pmax===30,'and it joins nothing');plan({ok:false,error:'x'});await tick(0);
  C.DASH={...C.DASH,pmaxTargets:[]};vm.runInContext('CAMPAIGN_STYLE_DRAFTS={}',C);k=submission();
  check(q('[data-pmax-target-label]').hidden&&!q('[data-pmax-target]').options.length,'with no campaign to join there is no choice to make');
  console.log('PASS '+n+' Performance Max structure console checks: joining a campaign, settling a finished draft, and plain Approvals cards');
})().catch(e=>{console.error(e);process.exit(1);});
