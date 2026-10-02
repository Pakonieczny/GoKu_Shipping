// Approvals "Complete ad" cards: every card loads its saved previews the same way, collapsed or open, and never sits on
// a loading text. Fake api only: no network, no paid calls.
const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict'),{JSDOM}=require('jsdom');
const html=fs.readFileSync('brites-adwords.html','utf8'),source=html.slice(html.indexOf('function campaignStyleChoices('),html.indexOf('function adApprovalDesignReviewHtml(')),component=fs.readFileSync('brites-approval-review.js','utf8');
const copy={headlines:['Saved Charm','A Silver Saved Charm','Jewellery Gifts'],longHeadlines:['Discover the saved silver charm'],descriptions:['Shop the charm at Brites Jewelry.','Find a charm for your collection.']};
const hex=n=>String(n).repeat(32).slice(0,32);
function approval(n,slug,title){return {id:'design-review-'+hex(n),reviewHash:'h-'+slug,summary:'Complete ad · '+title,designReview:{workspaceId:'w-'+slug,productId:slug,groupRef:'g-'+slug,productTitle:title,destination:'https://britesjewelry.com/products/'+slug,copy,context:{countries:['2840'],dailyBudget:12}}};}
function lanes(slug){const img=(format,w,h)=>({kind:'responsive',format,label:slug+' '+format,width:w,height:h,url:'https://storage.googleapis.com/brites/'+slug+'-'+format+'.jpg'}),set=[img('landscape',1200,628),img('square',1200,1200),img('portrait',1638,2048)],fixed={kind:'fixed',label:'300 × 250',width:300,height:250,url:'https://storage.googleapis.com/brites/'+slug+'-300x250.jpg'},film={included:true,key:'mobile_portrait',asset:{width:1080,height:1920},url:'https://storage.googleapis.com/brites/'+slug+'.mp4',inclusion:'Attaches after campaign creation'};
 return {ok:true,videoReview:{advisory:true},styles:{pmax:{images:set,videos:[film],savedVideos:[film],copy,videoNote:'Films attach after creation.'},responsive_display:{images:set.slice(0,2),videos:[],savedVideos:[{...film,included:false,inclusion:'Not on YouTube · not included'}],copy,videoNote:'Not on YouTube.'},fixed_display:{images:[fixed],videos:[],copy:null,videoNote:'Finished artwork only.'}}};}
const duck=approval(1,'duck','Duck Silhouette'),gecko=approval(2,'gecko','Gecko'),bunny=approval(3,'bunny','Bunny'),saturn=approval(4,'saturn','Planet Saturn');
const broken=approval(5,'moon','Moon'),withErrorObject=approval(6,'star','Star'),viaReload=approval(7,'sun','Sun'),viaPlan=approval(8,'leaf','Leaf');
const serverRefusal='HTTP 500 · This review belongs to a different product or ad group.';
let calls=[],gate=null,inFlight=0,maxInFlight=0;const failing=new Set([broken.id,withErrorObject.id,viaReload.id,viaPlan.id]),planFailing=new Set([broken.id,viaPlan.id]);
const dom=new JSDOM('<body></body>',{url:'https://brites.example'}),d=dom.window.document;dom.window.HTMLElement.prototype.scrollIntoView=function(){};
const c={document:d,localStorage:dom.window.localStorage,URL,console,setTimeout,clearTimeout,Intl,BritesCampaignStyles:require('../../brites-campaign-styles'),DASH:{pending:[],budgetCurrency:'CAD',pmaxTargets:[]},esc:s=>String(s),adAttr:s=>String(s),elFrom:s=>{const div=d.createElement('div');div.innerHTML=s;return div.firstElementChild;},adApprovalDesignReviewHtml:()=>'',wireApprovalAllSizes:()=>{},campaignStyleIcon:()=>'',apCountries:()=>'United States',apMoney:n=>'CAD '+Number(n).toFixed(2),toast:()=>{},reload:async()=>{},openAdDesign:()=>{},
 btnBusy:b=>{const was=b.innerHTML;b.disabled=true;b.textContent='Reloading…';return()=>{b.disabled=false;b.innerHTML=was;};},
 api:async(action,input)=>{calls.push({action,input});
  if(action==='adDesignSubmissionStatus'){inFlight++;maxInFlight=Math.max(maxInFlight,inFlight);try{if(gate)await gate.promise;const id=input.id;if(failing.has(id)){if(id===withErrorObject.id)return {error:'This review belongs to a different product or ad group.'};throw Error(serverRefusal);}return lanes(id===duck.id?'duck':id===gecko.id?'gecko':id===bunny.id?'bunny':id===saturn.id?'saturn':'other');}finally{inFlight--;}}
  if(action==='publishAdDesignSubmission'&&input.prepareOnly){if(planFailing.has(input.id))throw Error('This review belongs to a different product or ad group.');return {ok:true,planHash:'plan-'+input.id,plan:{currency:'CAD',totalDaily:12,campaigns:[{style:'pmax',name:'Saved Campaign',dailyBudget:12,formats:['square','landscape','portrait']}]}};}
  throw Error('Unexpected '+action+' in preview verification');}};
vm.createContext(c);vm.runInContext(component,c);vm.runInContext(source,c);vm.runInContext(html.split('\n').find(line=>line.startsWith('function openApprovalDesign(')),c);
const tick=()=>new Promise(r=>setImmediate(r)),settle=async()=>{for(let i=0;i<8;i++)await tick();};
function mount(list){const out=list.map(a=>{const card=c.adDesignSubmissionCard(a);d.body.appendChild(card);c.wireAdDesignSubmission(card,a);return card;});return out;}
const statusCalls=id=>calls.filter(x=>x.action==='adDesignSubmissionStatus'&&(!id||x.input.id===id)).length;
const loadingText=/Loading ad preview|Checking saved videos|Loading previews|Loading complete ad previews|Loading saved videos|Loading the included creative set/;
function assertFilled(card,label){
 const cover=card.querySelector('[data-review-cover]');assert.equal(cover.dataset.state,'ready',label+' cover ready');assert.equal(cover.querySelectorAll('img').length,3,label+' shows its three saved thumbnails collapsed');assert.equal(cover.querySelectorAll('.spin,[data-review-cover-retry]').length,0,label+' cover has no spinner or retry once loaded');
 for(const media of card.querySelectorAll('img,video'))assert.equal(media.getAttribute('crossorigin'),'anonymous',label+' media obeys cross-origin isolation');
 assert(card.querySelector('[data-style-thumb="pmax"] .arServed img'),label+' Performance Max tile shows the saved ad');assert(card.querySelector('[data-style-thumb="responsive_display"] .arServed img'),label+' Responsive Display tile shows the saved ad');assert(card.querySelector('[data-style-thumb="fixed_display"] img'),label+' Fixed Display tile shows the finished artwork');
 assert.match(card.querySelector('[data-style-media-count="pmax"]').textContent,/3 images · 1 saved videos/);assert.match(card.querySelector('[data-style-media-count="fixed_display"]').textContent,/1 sizes · image ads only/);
 assert.match(card.querySelector('[data-style-video-status="pmax"]').textContent,/1 videos · attach after creation/);assert.match(card.querySelector('[data-style-video-status="responsive_display"]').textContent,/0 videos included/);
 assert(!loadingText.test(card.textContent),label+' has no loading text left: '+(card.textContent.match(loadingText)||[])[0]);assert.equal(card.querySelector('[data-review-preview-error]').textContent,'');assert(card.querySelector('[data-review-retry]').hidden);
}
function assertFailed(card,label){
 const cover=card.querySelector('[data-review-cover]');assert.equal(cover.dataset.state,'error',label+' cover shows the failure');assert.match(cover.textContent,/Previews unavailable/);assert.match(cover.textContent,/belongs to a different product or ad group/);assert(!/HTTP 500/.test(cover.textContent),label+' cover keeps the reason short');assert(cover.querySelector('button[data-review-cover-retry]'),label+' cover offers Retry');assert.equal(cover.querySelectorAll('img').length,0);
 for(const key of ['pmax','responsive_display','fixed_display']){assert.equal(card.querySelector('[data-style-thumb="'+key+'"]').textContent,'Preview unavailable',label+' '+key+' tile says only unavailable');assert(!/different product or ad group/.test(card.querySelector('[data-style-media-count="'+key+'"]').closest('section').textContent),label+' '+key+' tile does not repeat the reason');}
 assert.equal(card.querySelector('[data-style-video-status="pmax"]').textContent,'Video status unavailable');assert.equal(card.querySelector('[data-style-video-status="pmax"]').title,'');assert.match(card.querySelector('[data-style-video-status="fixed_display"]').textContent,/Images only · no video/);
 assert.equal(card.querySelector('[data-review-style-note]').textContent,'Preview unavailable');const outsidePlan=card.cloneNode(true);outsidePlan.querySelectorAll('.arApproveRow,.arPlanDetails').forEach(x=>x.remove());assert.equal((outsidePlan.textContent.match(/different product or ad group/g)||[]).length,2,label+' the reason appears once on the strip and once beside Reload previews');assert.equal(card.querySelector('.arWorkspace').textContent.match(/different product or ad group/g).length,1,label+' review area states the reason once');assert.match(card.querySelector('[data-review-preview-error]').textContent,/different product or ad group/);assert(!card.querySelector('[data-review-retry]').hidden,label+' Reload previews is offered');
 assert(!loadingText.test(card.textContent),label+' never keeps a loading text after failure: '+(card.textContent.match(loadingText)||[])[0]);assert.equal(card.querySelectorAll('.spin').length,0,label+' no spinner after failure');
}
(async()=>{
 // 1. Four cards, the server answers for all: each shows a labelled spinner, then its own thumbnails, without being opened.
 let release;gate={promise:new Promise(r=>release=r)};
 const four=mount([duck,gecko,bunny,saturn]);await tick();
 for(const card of four){const cover=card.querySelector('[data-review-cover]');assert.equal(cover.dataset.state,'loading');assert.equal(cover.getAttribute('aria-busy'),'true');assert.match(cover.querySelector('[role=status]').textContent,/Loading previews…/);assert(cover.querySelector('.spin.sm[aria-hidden=true]'),'existing small spinner beside its label');
  assert(card.querySelector('[data-style-thumb="pmax"] .arThumbState .spin.sm'));assert.match(card.querySelector('[data-style-thumb="pmax"]').textContent,/Loading ad preview…/);assert(card.querySelector('[data-style-video-status="pmax"] .spin.sm'));assert.match(card.querySelector('[data-style-video-status="pmax"]').textContent,/Checking saved videos…/);assert(card.querySelector('.arLoading .spin.sm'));assert(!card.classList.contains('open'),'collapsed');}
 assert.equal(statusCalls(),3,'at most three preview requests start at once');
 release();gate=null;await settle();
 assert.equal(statusCalls(),4,'the fourth card loads as soon as a slot frees');assert(maxInFlight<=3,'never more than three preview requests in flight');
 four.forEach((card,i)=>assertFilled(card,['Duck','Gecko','Bunny','Planet Saturn'][i]));
 assert.match(four[1].querySelector('[data-review-cover] img').getAttribute('src'),/gecko-landscape/,'each card shows its own product');assert.match(four[3].querySelector('[data-review-cover] img').getAttribute('src'),/saturn-landscape/);
 // A saved film whose file cannot load (expired link, 403/404, unsupported codec) says so instead of "Loading video…" forever: browsers fire that error on its <source>.
 {const video=four[0].querySelector('video'),source=video&&video.querySelector('source');assert(source,'the card shows its saved film');source.dispatchEvent(new dom.window.Event('error'));const badge=video.parentElement.querySelector('.arImageLoading');assert(badge&&!badge.hidden&&badge.textContent==='Video unavailable','a film that cannot load says so instead of loading forever: '+(badge&&badge.textContent));}
 // A list refresh rebuilding the cards reuses the cache: no request storm.
 d.body.replaceChildren();const again=mount([duck,gecko,bunny,saturn]);await settle();assert.equal(statusCalls(),4,'rebuilt cards reuse cached previews');again.forEach((card,i)=>assertFilled(card,'rebuilt '+i));
 // A typed render does not rebuild a loaded cover (no flashing image badges).
 const coverImg=again[0].querySelector('[data-review-cover] img');again[0].querySelector('[data-review-tab="responsive_display"]').click();assert.equal(again[0].querySelector('[data-review-cover] img'),coverImg,'unchanged cover is kept');

 // 2. One card whose preview request throws: error and Retry on the collapsed cover and the tiles, then Retry recovers in place.
 d.body.replaceChildren();let [card]=mount([broken]);await settle();assertFailed(card,'Moon');
 card.classList.add('open');await card.arPreparePlan();assert.match(card.querySelector('[data-submission-error]').textContent,/different product or ad group/);assert(!card.querySelector('[data-submission-retry]').hidden,'Retry plan offered');
 card.querySelector('[data-review-tab="fixed_display"]').click();assert.match(card.querySelector('[data-review-style-title]').textContent,/Fixed Display/,'tabs still work while previews are unavailable');
 const before=statusCalls(broken.id);d.body.replaceChildren();[card]=mount([broken]);await settle();assert.equal(statusCalls(broken.id),before,'a rebuilt failing card reuses the recent failure instead of re-requesting');assertFailed(card,'Moon rebuilt');
 card.classList.add('open');await card.arPreparePlan();
 failing.delete(broken.id);planFailing.delete(broken.id);
 card.querySelector('[data-review-cover-retry]').click();assert.equal(card.querySelector('[data-review-cover]').dataset.state,'loading','Retry shows the spinner again');assert.match(card.querySelector('[data-review-cover]').textContent,/Loading previews…/);
 await settle();assert.equal(statusCalls(broken.id),before+2,'Retry asks the server again, then the recovered plan refreshes its prepared upload set once');assertFilled(card,'Moon after Retry');
 assert(card.querySelector('[data-submission-retry]').hidden,'the failed plan is retried once previews recover');assert(!card.querySelector('[data-publish-submission]').disabled,'plan recovered in place');assert.equal(card.querySelector('[data-submission-error]').textContent,'');

 // 3. An error object (not a throw) is handled the same way and never poisons later renders.
 [card]=mount([withErrorObject]);await settle();assertFailed(card,'Star');card.querySelector('[data-review-tab="responsive_display"]').click();card.querySelector('[data-review-layout]').value='wide';card.querySelector('[data-review-layout]').onchange();assertFailed(card,'Star after tab change');
 failing.delete(withErrorObject.id);card.querySelector('[data-review-cover-retry]').click();await settle();assertFilled(card,'Star after Retry');

 // 4. Reload previews (inside the card) recovers in place.
 [card]=mount([viaReload]);await settle();assertFailed(card,'Sun');failing.delete(viaReload.id);const reload=card.querySelector('[data-review-retry]');const reloading=reload.onclick.call(reload);assert(reload.disabled,'Reload previews shows it is busy');await reloading;await settle();assertFilled(card,'Sun after Reload previews');assert.equal(reload.textContent,'Reload previews');

 // 5. Retry plan recovers the plan and the previews together once the server answers.
 [card]=mount([viaPlan]);await settle();assertFailed(card,'Leaf');card.classList.add('open');await card.arPreparePlan();assert(!card.querySelector('[data-submission-retry]').hidden);
 failing.delete(viaPlan.id);planFailing.delete(viaPlan.id);card.querySelector('[data-submission-retry]').click();assert(card.querySelector('[data-submission-status] .spin.sm'),'plan check shows the small labelled spinner');await settle();assertFilled(card,'Leaf after Retry plan');assert(!card.querySelector('[data-publish-submission]').disabled);assert.match(card.querySelector('[data-submission-status]').textContent,/CAD 12.00\/day/);

 // 6. Cover-only wiring (the ad-type catalogue failed to load) still shows the saved images, or a reason and Retry.
 const bare=c.elFrom('<article class="draft arApproval"><div class="draft__quick"><div class="arCover" data-review-cover></div></div><div class="arLoading" role="status">Loading saved videos…</div></article>');d.body.replaceChildren(bare);const loaded=c.BritesApprovalReview.cover(bare,duck);assert.equal(bare.querySelector('[data-review-cover]').dataset.state,'loading');assert.equal(await loaded,true);assert.equal(bare.querySelectorAll('[data-review-cover] img[crossorigin=anonymous]').length,3);assert(!/Loading saved videos/.test(bare.textContent),'workspace placeholders never keep loading without their wiring');assert.match(bare.querySelector('.arLoading').textContent,/reload the page/);
 const lost=approval(9,'cloud','Cloud');failing.add(lost.id);const bare2=c.elFrom('<article><div class="arCover" data-review-cover></div></article>');d.body.replaceChildren(bare2);assert.equal(await c.BritesApprovalReview.cover(bare2,lost),false);assert.match(bare2.textContent,/Previews unavailable/);failing.delete(lost.id);bare2.querySelector('[data-review-cover-retry]').click();await settle();assert.equal(bare2.querySelectorAll('[data-review-cover] img').length,3);
 assert(calls.every(x=>x.action==='adDesignSubmissionStatus'||x.input.prepareOnly),'no publication during preview verification');
 dom.window.close();console.log('PASS every complete-ad card loads its own saved thumbnails collapsed with a labelled spinner, paced and cached requests, failure shows "Preview unavailable" on the tiles and the reason once on the strip (with Retry) and once beside Reload previews, Retry / Reload previews / Retry plan recover in place, CORS kept');require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exit(1);});
