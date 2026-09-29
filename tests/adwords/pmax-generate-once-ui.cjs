// A PMax product opportunity starts one paid draft generation at a time. The generation can take ten minutes;
// a redraw of the list meanwhile must not bring back a live "Create review draft" that would pay for a second
// draft. The working state lives with the opportunity, not with the button, and clears when polling ends.
'use strict';
const fs=require('fs'),path=require('path'),vm=require('vm'),assert=require('assert/strict'),{JSDOM}=require('jsdom');
const html=fs.readFileSync(path.resolve(__dirname,'../../brites-adwords.html'),'utf8');
const src=html.slice(html.indexOf('function pmaxProductChoices('),html.indexOf('function renderOpportunities(){'));
const model=require('../../assets/pmax-recommendation.js');
let n=0;const check=(v,m)=>{assert.ok(v,m);n++;console.log('PASS '+m);};
const offers=['shopify_US_11_101','shopify_US_22_201'],paid={available:true,monetaryComplete:true,currency:'USD',days:90,impressions:10000,clicks:200,conversions:10,cost:100,value:500};
const candidate={tag:'pmax-animal-necklaces|US',handle:'animal-necklaces',recommendationSchema:1,collectionTitle:'Animal necklaces',feedLabel:'US',itemIds:offers,productTitles:['Corgi necklace','Fox necklace'],offerDetails:[{itemId:offers[0],title:'Corgi necklace',paidPerformance:paid,evidenceIds:['corgi']},{itemId:offers[1],title:'Fox necklace',paidPerformance:paid,evidenceIds:['fox']}],demandEvidence:[{evidenceId:'corgi',orders:5,orders30d:2,revenue:250,revenue30d:100},{evidenceId:'fox',orders:50,orders30d:20,revenue:2500,revenue30d:1000}],paidPerformance:{available:true,monetaryComplete:true,currency:'USD',days:90},dailyBudget:12,days:30,evidenceDays:90,demandCoverage:{days30:true,days90:true,monetaryComplete:true},seasonalityCoverage:{complete:true}};
const dom=new JSDOM('<body><main><div id="oppList"></div></main></body>'),d=dom.window.document;
const requests=[],toasts=[];let answer=null;
const c={window:{BritesPmaxRecommendation:model},document:d,Intl,Date,Math,JSON,console,Promise,setTimeout,clearTimeout,setInterval,clearInterval,
  esc:v=>String(v==null?'':v).replace(/[&<>"']/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch])),apvEnc:encodeURIComponent,friendlyResearchError:String,researchNeedsRefresh:()=>false,
  toast:m=>toasts.push(String(m)),acctMoney:v=>'$'+v,wireOpportunityDeletion:()=>{},reload:async()=>{},setOppMeta:()=>{},PMAXAT:Date.now(),PMAXERR:null,
  api:async(action,payload)=>{requests.push(action);if(action==='generatePmax')return {queued:true,genId:payload.genId};if(action==='genStatus')return await new Promise(r=>{answer=r;});throw Error('Unexpected '+action);}};
c.renderOpportunities=()=>c.renderPmaxSection(d.getElementById('oppList'));
vm.createContext(c);vm.runInContext(src,c);
const button=()=>d.querySelector('#pmaxSec .pmx-gen[data-i="0"]'),tick=ms=>new Promise(r=>setTimeout(r,ms));
(async()=>{
  c.PMAXOPPS=[candidate];c.renderOpportunities();
  let b=button();check(b&&!b.disabled&&b.textContent==='Create review draft','the opportunity offers one draft');
  b.click();await tick(20);
  check(requests.join()==='generatePmax,genStatus'&&b.disabled&&b.dataset.working==='true','the click starts one paid generation and disables its button');
  // A redraw (research refresh, tab switch, product-ads refresh) while the draft is written.
  c.renderOpportunities();const redrawn=button();
  check(redrawn!==b&&!b.isConnected,'the list was drawn again');
  check(redrawn.disabled&&redrawn.getAttribute('aria-disabled')==='true'&&redrawn.dataset.working==='true'&&/^Generating/.test(redrawn.textContent),'the redrawn button stays disabled as "Generating…" with its spinner');
  redrawn.onclick.call(redrawn);redrawn.click();await tick(20);
  check(requests.filter(a=>a==='generatePmax').length===1,'a second click on the redrawn card sends nothing');
  const budget=d.querySelector('#pmaxSec .pmx-bud[data-i="0"]');budget.value='15';budget.oninput();
  check(button().disabled,'changing the budget meanwhile does not re-enable it');
  await tick(1100);
  check(/^Generating… \d+s$/.test(button().textContent),'the redrawn button shows the elapsed time');
  // Polling ends with a failure: the flag clears and the card offers the draft again.
  answer({ok:false,error:'Merchant check failed'});await tick(20);
  b=button();check(b&&!b.disabled&&b.dataset.working!=='true'&&b.textContent==='Create review draft','when polling ends the card offers the draft again');
  check(toasts.some(t=>/PMax draft failed: Merchant check failed/.test(t)),'the failure is reported even though the original button was gone');
  const ui=c.pmaxUi(c.PMAXOPPS[0]);check(!ui.working,'the working flag is cleared');
  // A later success marks this opportunity done, even after a redraw.
  b.click();await tick(20);c.renderOpportunities();answer({ok:true,approvalId:'a1',itemIds:[offers[0]]});await tick(20);
  b=button();check(b&&b.disabled&&b.textContent==='In Approvals ✓'&&c.PMAXOPPS[0].acted&&!c.pmaxUi(c.PMAXOPPS[0]).working,'the finished draft\'s card (redrawn meanwhile) pauses on its confirmation and offers nothing');
  b.onclick.call(b);await tick(20);check(requests.filter(a=>a==='generatePmax').length===2,'a click during that pause sends nothing');
  await tick(1600);check(!button()&&d.querySelector('#pmaxSec .oppChannelEmpty'),'then the card folds away and the list is drawn again');
  check(toasts.some(t=>/PMax draft ready/.test(t)),'the ready draft is announced');
  console.log('PASS '+n+' one-generation-per-PMax-opportunity checks');
})().catch(e=>{console.error(e);process.exit(1);});
