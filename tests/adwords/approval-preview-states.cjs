// Approvals "Complete ad" cards: every card loads its saved previews the same way, collapsed or open, and never sits on
// a loading text; Approve ad polls the background publication to its end. Fake api / fetch only: no network, no paid calls.
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

 // 7. A card's saved reason says when Approve ad failed, in local time: the time today, the short date too otherwise.
 // A reason saved before lastErrorAt existed takes its attempt's start (applyStartedAt); with neither, today.
 vm.runInContext(html.split('\n').filter(line=>/^(var _MON=|function salesDay\()/.test(line)).join('\n'),c);
 const at=(daysAgo,h,m)=>{const t=new Date();t.setDate(t.getDate()-daysAgo);t.setHours(h,m,0,0);return t.getTime();},reason=a=>{const k=c.adDesignSubmissionCard(a);assert.equal(k.querySelector('.me').textContent,'Complete ad · not published · the reason is below');return k.querySelector('.arLastError').textContent;},old=at(1,9,5);
 assert.equal(reason({...approval(10,'comet','Comet'),lastError:'Over your daily ceiling.',lastErrorAt:at(0,12,17),applyStartedAt:at(0,12,16)}),'Approve ad at 12:17 pm failed: Over your daily ceiling.','a new reason shows the time it was saved');
 assert.match(c.salesDay(old),/^[A-Z][a-z]{2} \d{1,2}(, \d{4})?$/);assert.equal(reason({...approval(11,'comet','Comet'),lastError:'Over your daily ceiling.',applyStartedAt:old}),'Approve ad on '+c.salesDay(old)+' at 9:05 am failed: Over your daily ceiling.','an older reason shows its attempt\'s start with the short date');
 assert.match(reason({...approval(12,'comet','Comet'),lastError:'Over your daily ceiling.'}),/^Approve ad at \d{1,2}:\d{2} [ap]m failed: Over your daily ceiling\.$/,'with no time saved, it reads as today');
 // 8. Approve ad: the server answers at once ({queued:true}) and Google publishes in the background. The card keeps its busy button, reads the
 // approval's own saved state (approvalStatus) every 4 s for up to 10 min and shows how it ended; a 504 page / network error is never printed.
 // Real api() and btnBusy from the page against a scripted server on a virtual clock: no network, no waiting.
 {
 const apiSource=html.slice(html.indexOf('async function api(action, extra){'),html.indexOf('/* ---- centralized post-mutation refresh')),busySource=html.slice(html.indexOf('function btnBusy('),html.indexOf('/* --- step runner')),clockSource=html.split('\n').filter(line=>/^(var _MON=|function salesDay\()/.test(line)).join('\n');
 const gateway504='<HTML>\n<HEAD><TITLE>Inactivity Timeout</TITLE></HEAD>\n<BODY>Inactivity Timeout</BODY></HTML>';
 function rig(server,item){
  const dom2=new JSDOM('<body></body>',{url:'https://brites.example'}),d2=dom2.window.document;let clock=Date.now();const log={calls:[],toasts:[],flow:[],reloads:0,sleeps:[]};
  class FakeDate extends Date{constructor(...x){if(x.length)super(...x);else super(clock);}static now(){return clock;}}
  const ctx={document:d2,localStorage:dom2.window.localStorage,URL,console,Intl,Date:FakeDate,window:{},PASS:'p',gadsUrl:()=>'https://brites.example/gads',loadInfo:()=>({verb:'Working'}),API_MUTATING:new Set(),forgetDeletedCampaign:()=>{},scheduleReload:()=>{},
   setTimeout:(fn,ms)=>{log.sleeps.push(ms);clock+=ms;setImmediate(fn);return 1;},clearTimeout:()=>{},
   fetch:async(url,o)=>{const body=JSON.parse(o.body);log.calls.push(body);const r=await server(body,log,()=>clock,ctx);return {status:r.status||200,ok:(r.status||200)<400,text:async()=>r.text!==undefined?r.text:JSON.stringify(r.json),json:async()=>r.json};},
   BritesCampaignStyles:require('../../brites-campaign-styles'),DASH:{pending:[],budgetCurrency:'CAD',pmaxTargets:[]},esc:x=>String(x),adAttr:x=>String(x),elFrom:x=>{const div=d2.createElement('div');div.innerHTML=x;return div.firstElementChild;},adApprovalDesignReviewHtml:()=>'',wireApprovalAllSizes:()=>{},campaignStyleIcon:()=>'',apCountries:()=>'United States',apMoney:n=>'CAD '+Number(n).toFixed(2),
   toast:m=>log.toasts.push(m),reload:async()=>{log.reloads++;},openAdDesign:()=>{},BritesFlow:{published:(card,id,r)=>log.flow.push({id,status:r&&r.status,message:r&&r.message})}};
  vm.createContext(ctx);vm.runInContext(busySource+'\n'+apiSource+'\n'+clockSource,ctx);vm.runInContext(source,ctx);
  const a=item||approval(20,'owl','Owl'),card=ctx.adDesignSubmissionCard(a);d2.body.appendChild(card);ctx.wireAdDesignSubmission(card,a);
  const q=x=>card.querySelector(x),draftKey=a.id+'|'+a.reviewHash;
  return {ctx,log,card,a,q,button:()=>q('[data-publish-submission]'),now:()=>clock,error:()=>q('[data-submission-error]').textContent,text:()=>card.textContent,
   snap:()=>({label:q('[data-publish-submission]').textContent,spinner:!!q('[data-publish-submission] .spin'),disabled:q('[data-publish-submission]').disabled,locked:q('.campaignStyleChoices').disabled}),
   async open(){card.classList.add('open');await card.arPreparePlan();assert(!q('[data-publish-submission]').disabled,'the plan is ready, so Approve ad is enabled');ctx.CAMPAIGN_STYLE_DRAFTS[draftKey]={marker:true};},
   press:()=>q('[data-publish-submission]').onclick(),
   polls:()=>log.calls.filter(x=>x.action==='approvalStatus').length,presses:()=>log.calls.filter(x=>x.action==='publishAdDesignSubmission'&&x.confirmed).length,draft:()=>ctx.CAMPAIGN_STYLE_DRAFTS[draftKey]};
 }
 // The server script: prepare answers a plan, the confirmed press answers publish(), each poll answers the next step (the last one repeats).
 const plan={ok:true,planHash:'plan-owl',plan:{currency:'CAD',totalDaily:12,campaigns:[{style:'pmax',name:'Owl',dailyBudget:12,formats:['square']}]}};
 const script=(publish,steps)=>{let n=0;return async(body,log,now,ctx)=>{if(body.prepareOnly)return {json:plan};if(body.confirmed)return await publish(now,log);if(body.action==='approvalStatus'){const st=steps[Math.min(n++,steps.length-1)],out=typeof st==='function'?await st(now,log):st;if(out&&out.__throw)throw new TypeError('Failed to fetch');return {json:{ok:true,id:body.id,...out}};}throw Error('Unexpected '+body.action);};};
 const queued=now=>({json:{ok:true,id:'x',status:'APPROVED',queued:true,requestedAt:now(),message:'Publishing started.'}});
 const outcome=(status,message,now)=>({status,message,at:now()});
 const noRaw=(r,label)=>assert(!/<\/?(HTML|HEAD|TITLE|BODY)|Inactivity Timeout|HTTP 504/i.test(r.text()),label+' never prints the gateway page or its raw HTTP text: '+r.text().slice(0,200));
 const scenarios=[],scenario=(name,fn)=>scenarios.push([name,fn]);

 scenario('queued, then APPROVED, APPLYING, APPLIED: busy button with its labelled spinner, locked card, no double press, saved outcome toasted, draft cleared, flow told, list reloaded',async()=>{
  let during=null;const r=rig(script(queued,[{status:'APPROVED',publishRequestedAt:1},{status:'APPLYING'},async now=>{during=r.snap();r.button().onclick();r.button().onclick();return {status:'APPLIED',publishOutcome:outcome('APPLIED','Google accepted the update.',now)};}]));await r.open();
  const run=r.press();await tick();const first=r.snap();await run;
  assert.equal(first.label,'Publishing to Google…','the button says so as soon as the request is queued');assert(first.spinner,'with the existing small labelled spinner');assert(first.disabled&&first.locked,'and the card stays locked');assert.equal(during.label,'Publishing to Google…');assert(during.disabled&&during.locked&&during.spinner,'still busy on the third check');
  assert.equal(r.presses(),1,'a second press while busy does nothing');assert.equal(r.polls(),3);assert(r.log.sleeps.length>=2&&r.log.sleeps.every(ms=>ms===4000),'polls every 4 seconds: '+r.log.sleeps);assert.deepEqual(r.log.calls.filter(x=>x.action==='approvalStatus').map(x=>x.id),Array(3).fill(r.a.id),'reads the approval by id');
  assert.deepEqual(r.log.toasts,['Google accepted the update.'],'the saved outcome message is toasted');assert.equal(r.draft(),undefined,'the saved draft is cleared');assert.equal(r.log.flow.length,1);assert.equal(r.log.flow[0].status,'APPLIED');assert.equal(r.log.flow[0].id,r.a.id);assert.equal(r.log.reloads,1,'the list is reloaded');assert.equal(r.error(),'');
  const end=r.snap();assert.equal(end.label,'Approve ad');assert(!end.disabled&&!end.locked&&!end.spinner,'the card unlocks once it ends');
 });
 scenario('APPLIED before the outcome is saved: waits for it; with none it falls back to the page\'s own text',async()=>{
  let r=rig(script(queued,[{status:'APPLYING'},{status:'APPLIED',publishOutcome:null},async now=>({status:'APPLIED',publishOutcome:outcome('APPLIED','Google accepted it, with films.',now)})]));await r.open();await r.press();assert.deepEqual(r.log.toasts,['Google accepted it, with films.']);assert.equal(r.polls(),3);
  r=rig(script(queued,[{status:'APPLIED',publishOutcome:null}]));await r.open();await r.press();assert.deepEqual(r.log.toasts,['Published to Google Ads. New campaigns start paused.']);assert(r.polls()<=6,'a missing outcome does not wait for ten minutes: '+r.polls());assert.equal(r.log.flow.length,1);assert.equal(r.log.reloads,1);
 });
 scenario('dry run: back to PENDING with a VALIDATED outcome newer than the press ends as validated, not as an error; an older outcome is ignored',async()=>{
  const r=rig(script(queued,[async now=>({status:'PENDING',publishOutcome:{status:'VALIDATED',message:'Old dry run.',at:now()-60000}}),{status:'APPROVED'},async now=>({status:'PENDING',validatedAt:now(),publishOutcome:outcome('VALIDATED','Google validated the update in dry-run mode; it has not been published.',now)})]));await r.open();await r.press();
  assert.equal(r.polls(),3,'the earlier dry run\'s outcome is not mistaken for this press');assert.deepEqual(r.log.toasts,['Google validated the update in dry-run mode; it has not been published.']);assert.equal(r.log.flow[0].status,'VALIDATED');assert.equal(r.draft(),undefined);assert.equal(r.log.reloads,1);assert.equal(r.error(),'');
 });
 scenario('back to PENDING with a lastError newer than the press shows it in the error line, in the saved-error wording; an older saved error is ignored',async()=>{
  const old={...approval(21,'owl','Owl'),lastError:'Over your daily ceiling.',lastErrorAt:Date.now()-3600000};
  const r=rig(script(queued,[async now=>({status:'PENDING',error:'Over your daily ceiling.',lastErrorAt:now()-3600000}),{status:'APPLYING'},async now=>({status:'PENDING',error:'Google rejected the asset: image too small.',lastErrorAt:now()+50})]),old);await r.open();await r.press();
  assert.equal(r.polls(),3,'the hour-old error is not this press\'s failure');assert.match(r.error(),/^Approve ad at \d{1,2}:\d{2} [ap]m failed: Google rejected the asset: image too small\.$/);assert.deepEqual(r.log.toasts,[]);assert.equal(r.log.reloads,0);assert.deepEqual(r.draft(),{marker:true},'the saved draft stays so Approve can be pressed again');assert.equal(r.log.flow.length,0);
  const end=r.snap();assert.equal(end.label,'Approve ad');assert(!end.disabled&&!end.locked,'the card unlocks for another try');
 });
 scenario('APPLY_UNKNOWN shows the unconfirmed wording (its saved outcome, else its lastError) and ends',async()=>{
  let r=rig(script(queued,[{status:'APPLYING'},async now=>({status:'APPLY_UNKNOWN',error:'socket hang up',lastErrorAt:now(),publishOutcome:outcome('APPLY_UNKNOWN','Google\'s result could not be confirmed. Check this draft against Google Ads before another publication attempt.',now)})]));await r.open();await r.press();
  assert.match(r.error(),/^Approve ad at \d{1,2}:\d{2} [ap]m failed: Google's result could not be confirmed\. Check this draft against Google Ads/);assert.equal(r.polls(),2);assert.equal(r.log.toasts.length,0);assert.equal(r.log.reloads,0);assert(!r.snap().spinner);
  r=rig(script(queued,[{status:'APPLY_UNKNOWN',error:'socket hang up',startedAt:Date.now()}]));await r.open();await r.press();assert.match(r.error(),/failed: socket hang up$/);
 });
 scenario('polling that never ends stops after 10 minutes with a plain sentence, keeps Approve disabled and does not clear the draft',async()=>{
  const r=rig(script(queued,[{status:'APPROVED'}]));await r.open();const t0=r.now();assert.equal(r.ctx.APPROVE_POLL.every,4000);assert.equal(r.ctx.APPROVE_POLL.limit,600000);await r.press();
  const waited=r.now()-t0;assert(waited>=600000&&waited<=604000,'stops at 10 minutes, not before or long after: '+waited);assert(r.polls()>=150&&r.polls()<=152,'about one read every 4 seconds: '+r.polls());const polls=r.polls();
  assert.equal(r.error(),'Publishing is still running. This card updates when it finishes.');assert.equal(r.log.toasts.length,0);assert.equal(r.log.reloads,0);assert.deepEqual(r.draft(),{marker:true});
  assert(r.button().disabled&&r.button().textContent==='Approve ad'&&!r.snap().spinner,'it cannot be pressed again while Google may still be publishing');await tick();await tick();assert.equal(r.polls(),polls,'polling stopped');assert.equal(r.presses(),1);
 });
 scenario('a 504 HTML answer to Approve is never shown: the card polls, and APPLIED ends it as a success',async()=>{
  let during=null;const r=rig(script(()=>({status:504,text:gateway504}),[async()=>{during=r.snap();noRaw(r,'while waiting');return {status:'APPROVED'};},{status:'APPLYING'},async now=>({status:'APPLIED',publishOutcome:outcome('APPLIED','Google accepted the update.',now)})]));await r.open();await r.press();
  assert(during.disabled&&during.locked&&during.spinner,'the button stays busy while the card checks');assert.equal(r.polls(),3);assert.deepEqual(r.log.toasts,['Google accepted the update.']);assert.equal(r.draft(),undefined);assert.equal(r.log.flow.length,1);assert.equal(r.log.reloads,1);noRaw(r,'the end');assert.equal(r.error(),'');assert.equal(r.presses(),1);
 });
 scenario('a network error on Approve is polled the same way; a failed read in between is skipped',async()=>{
  const inner=script(null,[{__throw:1},{status:'APPLYING'},async now=>({status:'APPLIED',publishOutcome:outcome('APPLIED','Google accepted the update.',now)})]);
  const r=rig(async(body,log,now,ctx)=>{if(body.confirmed)throw new TypeError('Failed to fetch');return inner(body,log,now,ctx);});await r.open();await r.press();
  assert.equal(r.polls(),3);assert.deepEqual(r.log.toasts,['Google accepted the update.']);assert(!/Failed to fetch/.test(r.text()));assert.equal(r.log.reloads,1);
 });
 scenario('a 504 whose status never changes ends after 10 minutes with the plain sentence, nothing raw, and the card usable again',async()=>{
  const oldAt=Date.now()-7200000,r=rig(script(()=>({status:504,text:gateway504}),[{status:'PENDING',error:'Old reason.',lastErrorAt:oldAt}]),{...approval(22,'owl','Owl'),lastError:'Old reason.',lastErrorAt:oldAt});await r.open();const t0=r.now();await r.press();
  const waited=r.now()-t0;assert(waited>=600000&&waited<=604000,'polls for the full 10 minutes before deciding: '+waited);assert(r.polls()>=150);assert.equal(r.error(),'Google did not answer in time. Check the card again in a minute; nothing is lost.');noRaw(r,'the end');assert.equal(r.log.toasts.length,0);assert.equal(r.log.reloads,0);assert.deepEqual(r.draft(),{marker:true});
  const end=r.snap();assert.equal(end.label,'Approve ad');assert(!end.disabled&&!end.locked&&!end.spinner,'nothing happened on the server, so the card can be tried again');
 });
 scenario('a 504 followed by a new failure shows that failure; one followed by work that never finishes says it is still running and stays locked',async()=>{
  let r=rig(script(()=>({status:504,text:gateway504}),[{status:'APPROVED'},async now=>({status:'PENDING',error:'Google rejected the asset: image too small.',lastErrorAt:now()+50})]));await r.open();await r.press();assert.match(r.error(),/failed: Google rejected the asset: image too small\.$/);noRaw(r,'the failure');assert.equal(r.polls(),2);assert(!r.button().disabled);
  r=rig(script(()=>({status:504,text:gateway504}),[{status:'APPLYING'}]));await r.open();await r.press();assert.equal(r.error(),'Publishing is still running. This card updates when it finishes.');assert(r.button().disabled);noRaw(r,'the end');assert.equal(r.log.reloads,0);
 });
 scenario('refusals and the old synchronous answer behave exactly as before, with no polling',async()=>{
  let r=rig(script(()=>({json:{ok:false,error:'This review changed. Reload Approvals.'}}),[{status:'APPROVED'}]));await r.open();await r.press();assert.equal(r.error(),'This review changed. Reload Approvals.');assert.equal(r.polls(),0);assert(!r.button().disabled&&!r.snap().locked);assert.equal(r.log.reloads,0);
  r=rig(script(()=>({status:500,json:{error:'Over your daily ceiling.'}}),[{status:'APPROVED'}]));await r.open();await r.press();assert.equal(r.error(),'HTTP 500 · Over your daily ceiling.','a JSON refusal is shown, not polled');assert.equal(r.polls(),0);
  r=rig(script(()=>({json:{ok:true,status:'APPLIED',message:'Created paused'}}),[{status:'APPROVED'}]));await r.open();await r.press();assert.deepEqual(r.log.toasts,['Created paused']);assert.equal(r.polls(),0,'an answer without queued is the old synchronous result');assert.equal(r.log.flow.length,1);assert.equal(r.log.reloads,1);assert.equal(r.draft(),undefined);assert.equal(r.error(),'');
 });
 scenario('api() turns a gateway page or other markup into one short plain sentence and marks the answer as unanswered; JSON refusals keep their own text',async()=>{
  const r=rig(script(()=>({status:504,text:gateway504}),[{status:'APPROVED'}])),grab=text=>{return vm.runInContext('(async()=>{try{await api("x",{});return null}catch(e){return {message:e.message,unanswered:e.unanswered,status:e.status}}})()',r.ctx);},reply=(status,text)=>{r.ctx.fetch=async()=>({status,ok:false,text:async()=>text,json:async()=>JSON.parse(text)});};
  reply(504,gateway504);let e=await grab();assert.equal(e.message,'HTTP 504 · The server took too long to answer.');assert.equal(e.unanswered,true);
  reply(502,'');e=await grab();assert.equal(e.message,'HTTP 502 · The server took too long to answer.');assert.equal(e.unanswered,true);
  reply(500,'<html><body>Oops</body></html>');e=await grab();assert.equal(e.message,'HTTP 500 · The server sent an unexpected answer.');
  reply(500,'{"error":"Over your daily ceiling."}');e=await grab();assert.equal(e.message,'HTTP 500 · Over your daily ceiling.');assert.notEqual(e.unanswered,true);
 });
 const failed=[];for(const [name,fn] of scenarios){try{await fn();}catch(e){failed.push(name+'\n     '+String(e&&e.message||e).split('\n').slice(0,3).join(' | '));}}
 assert(!failed.length,'Approve ad polling: '+failed.length+' of '+scenarios.length+' scenarios failed:\n  - '+failed.join('\n  - '));
 }

 dom.window.close();console.log('PASS every complete-ad card loads its own saved thumbnails collapsed with a labelled spinner, paced and cached requests, failure shows "Preview unavailable" on the tiles and the reason once on the strip (with Retry) and once beside Reload previews, Retry / Reload previews / Retry plan recover in place, CORS kept, a saved reason says when Approve ad failed, Approve ad polls its approval until Google finishes and never prints a gateway page');require('./suite-guard.cjs').done();
})().catch(e=>{console.error(e);process.exit(1);});
