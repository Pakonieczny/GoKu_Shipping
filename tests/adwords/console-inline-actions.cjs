// Console actions stay in the page: campaign budget, schedule and pause edit in place under the
// campaign, yes/no questions (clear the activity log, delete a campaign, release an idea) open under
// the control that asked, both survive a refresh of their list, and nothing uses a browser pop-up.
// A launched draft settles out of Recommended instead of jumping, researched dates name their page,
// and the page keeps plain words: current tab names, no decorative emoji, readable date mismatches.
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {JSDOM}=require(process.env.BRITES_EDITOR_DOM_RUNTIME?path.join(process.env.BRITES_EDITOR_DOM_RUNTIME,'jsdom'):'jsdom');
const ROOT=path.resolve(__dirname,'../..'),read=f=>fs.readFileSync(path.join(ROOT,f),'utf8');
const html=read('brites-adwords.html'),server=read('netlify/functions/googleAdsAutopilot.js');
for(const m of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g))if(m[1].trim())new vm.Script(m[1]);
function pick(name){const m=new RegExp('^(?:async )?function '+name+'\\(','m').exec(html);assert(m,'the page defines '+name);const rest=html.slice(m.index),next=/\n(?:async )?function \w+\(/.exec(rest.slice(1));return next?rest.slice(0,next.index+1):rest;}
const tick=()=>new Promise(r=>setTimeout(r,0));
let passed=0;const test=async(name,fn)=>{await fn();passed++;console.log('PASS',name);};

(async()=>{
await test('no browser pop-ups for campaign edits, the activity log, deleting a campaign or releasing an idea',()=>{
  for(const name of ['wireCampRows','cmdEditOpen','cmdEditPaint','cmdEditBudget','cmdEditSchedule','cmdEditStatus','deleteCampaignCard','releaseOpp','bindControlsOnce','askInline'])
    assert.doesNotMatch(pick(name),/\b(?:prompt|confirm|alert)\(/,name+' asks in the page');
  assert.doesNotMatch(html,/function renderTimeline\(|function loadBlock\(|querySelectorAll\(["']\.ctl["']\)/,'unreachable timeline, .ctl handler and loadBlock are gone');
  assert.doesNotMatch(html,/function (?:renderPerf|rowHtml|detailHtml|analysisHtml|actIcon|optDot|fieldVal|wirePerf|doAnalyze|analysisWait|setBudget|campStatus|campStartNow|campSetCountries|schedHtml|schedHint)\(|#ptable|var P=\{sortKey|tr\.prow|tr\.pdetail|renderPerf\(\)/,
    'the unreachable old performance view (its slider, confirm pop-ups, emoji action icons, state and styles) is gone; Overview edits campaigns in place');
});

await test('plain words: current tab names, no decorative emoji in working UI, readable date mismatch',()=>{
  assert.doesNotMatch(html,/✨|thinking…|\\u2728/,'no sparkles');
  assert.doesNotMatch(html,/enable from Command Center|The Command Center and campaign window/);
  assert.match(html,/Overview and the campaign window use the same product report\./);
  assert.doesNotMatch(server,/Enable the campaign from Campaigns|Edit them from Campaigns|in Command Center/);
  assert.match(server,/Enable the campaign in Overview, where the daily ceiling and monthly stop are checked\./);
  assert.match(server,/Delete it from the campaign list in Opportunities first/);
  assert.doesNotMatch(html,/report dates do not match/i);
  assert.match(html,/Google sent figures for different dates than the ones you chose\./);assert.match(html,/Google sent daily figures for different dates than the ones you chose\./);
  const empty=pick('renderCommand');assert.match(empty,/researchNotice kpiNote" role="alert"/,'the mismatch reads across the whole row');
  assert.match(empty,/performanceValidRange\(performanceRange\(cmdRange\)\)\?'':' <button class="btn ghost sm" onclick="cmdApplyRange\(null,cmdRange\)">Try again<\/button>'/,'a retry is offered only when retrying can help');
  assert.match(pick('loadOccasions'),/<span class="spin sm" aria-hidden="true"><\/span> /,'occasions show the standard spinner');
  assert.match(html,/id="bOccWhy" class="muted" role="status"/);
});

// A small Overview: the real row wiring and inline editors around one campaign.
const dom=new JSDOM('<!doctype html><body><div id="snapshot"></div></body>',{runScripts:'outside-only',pretendToBeVisual:true}),w=dom.window,d=w.document;
const calls=[],toasts=[];let reply={};
Object.assign(w,{DASH:{budgetCurrency:'CAD',lastMetrics:[{id:'77',budget:35,status:'ENABLED',endDate:'2026-10-20'}]},cmdMetrics:[{id:'77',budget:35,status:'ENABLED',endDate:'2026-10-20'}],cmdReport:{budgetCurrency:'CAD'},
  BUDGET_OVERRIDES:{},END_DATE_OVERRIDES:{},REPORT_CAMPAIGN_OPEN:new Set(),_cmdRestoring:false,_MON:['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'],
  api:async(action,body)=>{calls.push([action,JSON.parse(JSON.stringify(body))]);const r=reply[action];return typeof r==='function'?r(body):r||{ok:true};},
  toast:m=>toasts.push(String(m)),renderDiag(){},loadCampaignTree(){}});
w.eval('var SERV_ASK={};\n'+['esc','cmdAttr','money','reportNumber','rpYmd','rpParse','fmtMon','cmdOpening','wireServing','wireCampRows','cmdEditRow','cmdEditMark','cmdEditOpen','cmdEditClose','cmdEditPaint','cmdEditFail','cmdEditSaved','cmdEditBudget','cmdEditSchedule','cmdEditStatus'].map(pick).join('\n'));
const camp=()=>w.cmdMetrics[0];
w.renderCommand=function(){const c=camp(),snap=d.getElementById('snapshot');
  snap.innerHTML='<table><tbody><tr class="crow" data-i="0" data-cid="77"><td>Autumn necklaces</td></tr><tr class="cdet" data-d="0" data-cid="77" style="display:none"><td><div class="campaignSettings"><button class="btn ghost sm bge" data-id="77" data-b="'+c.budget+'" data-res="customers/1/campaignBudgets/5">'+c.budget+'</button><button class="btn ghost sm sce" data-id="77" data-end="'+(c.endDate||'')+'" data-name="Autumn necklaces">schedule</button><button class="btn ghost sm cst" data-id="77" data-name="Autumn necklaces" data-to="'+(c.status==='ENABLED'?'PAUSED':'ENABLED')+'">'+(c.status==='ENABLED'?'Pause campaign':'Enable campaign')+'</button></div><div class="adlvl"></div></td></tr></tbody></table>';
  w.wireCampRows(snap);w._cmdRestoring=true;try{snap.querySelectorAll('.crow').forEach(function(r){if(w.REPORT_CAMPAIGN_OPEN.has(r.dataset.cid))r.click();});}finally{w._cmdRestoring=false;}};
w.renderCommand();
const $=q=>d.querySelector(q),box=()=>$('.cmdEdit'),msg=()=>(box().querySelector('.ieMsg')||{}).textContent,type=v=>{const i=$('#cmdEditIn');i.value=v;i.dispatchEvent(new w.Event('input',{bubbles:true}));},press=k=>box().querySelector('[data-k="'+k+'"]').click(),submit=()=>box().querySelector('form').dispatchEvent(new w.Event('submit',{cancelable:true}));
$('.crow').click();

await test('the daily budget edits in place, checks its input, asks before a large jump and saves through setBudget',async()=>{
  $('.bge').click();assert(box(),'the editor opens under the campaign settings');assert.equal(box().previousElementSibling.className,'campaignSettings');
  assert.match(box().textContent,/New daily budget for this campaign \(CAD\)/);assert.equal($('#cmdEditIn').value,'35');assert.equal($('.bge').getAttribute('aria-expanded'),'true');
  type('lots');submit();assert.equal(msg(),'Budget must be a positive number');assert.equal(calls.length,0);
  type('50');w.renderCommand();assert(box(),'a refresh keeps the open editor');assert.equal($('#cmdEditIn').value,'50','and what was typed');
  submit();assert.match(box().textContent,/That’s a 43% change \(\$35 CAD → \$50 CAD\)\. Large budget jumps can reset a Smart Bidding campaign’s learning\. Apply anyway\?/);assert.equal(calls.length,0);
  press('back');assert.equal($('#cmdEditIn').value,'50');submit();
  reply.setBudget={error:'Budget is above the daily ceiling.'};press('apply');await tick();await tick();
  assert.equal(msg(),'Budget change failed: Budget is above the daily ceiling.','a failed save keeps the editor with the reason');
  reply.setBudget={ok:true};press('apply');await tick();await tick();
  assert.deepEqual(calls.at(-1),['setBudget',{id:'77',budget:50,budgetRes:'customers/1/campaignBudgets/5'}]);assert.equal(box(),null,'saved: the editor closes');
  assert.equal(w.BUDGET_OVERRIDES['77'].budget,50);assert.equal($('.bge').textContent,'50','the row shows the new budget');assert.equal(toasts.at(-1),'Budget $35 CAD → $50 CAD','the budget names its currency');
  $('.bge').click();type('45');submit();await tick();await tick();assert.deepEqual(calls.at(-1),['setBudget',{id:'77',budget:45,budgetRes:'customers/1/campaignBudgets/5'}],'small changes save without the extra question');
});

await test('the schedule edits in place with the same checks and setEndDate requests',async()=>{
  const n=calls.length;$('.sce').click();assert.match(box().textContent,/How much longer should “Autumn necklaces” run\?/);assert.match(box().textContent,/Ends Oct 20/);
  assert.match(box().textContent,/Enter a number of days to add \(e\.g\. 30\), an exact end date \(e\.g\. \d{4}-12-24\), or the word open to remove the end date entirely\./);
  type('');submit();assert.equal(msg(),'Nothing entered');type('2001-01-01');submit();assert.equal(msg(),'That date is in the past');
  type('soon');submit();assert.equal(msg(),'Didn’t understand “soon” — use a number of days, a YYYY-MM-DD date, or ‘open’');assert.equal(calls.length,n);
  reply.setEndDate=b=>({ok:true,endDate:'2026-11-19',verified:true});type('30');submit();await tick();await tick();
  assert.deepEqual(calls.at(-1),['setEndDate',{id:'77',addDays:30}]);assert.match(toasts.at(-1),/Now runs to Nov 19 — Autumn necklaces · verified in Google Ads/);assert.equal(box(),null);
  reply.setEndDate={ok:true,endDate:null,cleared:true,verified:true};$('.sce').click();type('open');submit();await tick();await tick();assert.deepEqual(calls.at(-1),['setEndDate',{id:'77',endDate:null}]);
  $('.sce').click();assert(box());$('.sce').click();assert.equal(box(),null,'the same button closes its editor');
});

await test('pausing asks in place, Escape cancels, and the pause goes through setStatus',async()=>{
  $('.cst').click();assert.match(box().textContent,/Pause “Autumn necklaces”\?/);assert.equal(d.activeElement.dataset.k,'yes');
  box().dispatchEvent(new w.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));assert.equal(box(),null);assert(d.activeElement.classList.contains('cst'),'focus returns to the button');
  $('.cst').click();press('yes');assert.match(box().querySelector('[data-k="yes"]').textContent,/Pausing…/,'the wait shows on the button');await tick();await tick();
  assert.deepEqual(calls.at(-1),['setStatus',{id:'77',status:'PAUSED'}]);assert.equal($('.cst').textContent,'Enable campaign');assert.equal(toasts.at(-1),'Paused ✓ — Autumn necklaces');
  reply.setStatus={ok:true,endDate:'2026-11-19'};$('.cst').click();assert.match(box().textContent,/Enable “Autumn necklaces”\? It will start spending its daily budget\./);press('yes');await tick();await tick();
  assert.deepEqual(calls.at(-1),['setStatus',{id:'77',status:'ENABLED'}]);assert.equal(toasts.at(-1),'Enabled ✓ — Autumn necklaces · runs to 2026-11-19','enabling says how long it now runs');
});
dom.window.close();

// Yes/no questions under the control that asked.
const q=new JSDOM('<!doctype html><body><div class="card"><div class="card__h"><h3>Autopilot activity</h3><button id="clearFeed">Clear</button></div><div id="feed"></div></div><div id="list"></div></body>',{runScripts:'outside-only',pretendToBeVisual:true}),qw=q.window,qd=qw.document;
qw.eval([html.match(/^var ASK=null,ASK_SEQ=0;$/m)[0],pick('cmdOpening'),pick('askInline')].join('\n'));
const list=qd.getElementById('list'),drawList=()=>{list.innerHTML='<article class="existingCampaign"><h4>Autumn necklaces</h4><div class="existingCampaignActions"><button data-delete-campaign="9001">Delete</button></div></article><article class="existingCampaign"><h4>Rings</h4><div class="existingCampaignActions"><button data-delete-campaign="9002">Delete</button></div></article>';};
const del=id=>qd.querySelector('[data-delete-campaign="'+id+'"]'),ask=()=>qd.querySelector('.askInline');
const askDelete=id=>qw.askInline(del(id),{key:'deleteCampaign:'+id,q:'Delete “Autumn necklaces” and all its ad groups and ads?',note:'Google removal cannot be undone.',yes:'Delete campaign',danger:true,find:()=>del(id),place:(box,b)=>b.closest('.existingCampaign').appendChild(box)});
drawList();

await test('a question opens under its control and answers with the control to show the work on',async()=>{
  const answer=askDelete('9001');assert(ask(),'the question is in the page');assert.equal(ask().parentNode,del('9001').closest('.existingCampaign'));
  assert.equal(ask().querySelector('.ieQ').textContent,'Delete “Autumn necklaces” and all its ad groups and ads?');assert.equal(ask().querySelector('.ieNote').textContent,'Google removal cannot be undone.');
  assert.equal(qd.activeElement.dataset.ask,'no','a permanent act starts on Cancel');assert.equal(del('9001').getAttribute('aria-expanded'),'true');assert.equal(del('9001').getAttribute('aria-controls'),ask().id);
  drawList();await tick();assert(ask(),'a refresh keeps the question');assert.equal(ask().parentNode,del('9001').closest('.existingCampaign'),'under the new copy of its control');assert.equal(del('9001').getAttribute('aria-expanded'),'true');assert.equal(qd.activeElement.dataset.ask,'no','focus stays on the same answer');
  const fresh=del('9001');ask().querySelector('[data-ask="yes"]').click();assert.equal(await answer,fresh);assert.equal(ask(),null);assert.equal(fresh.hasAttribute('aria-expanded'),false);
});

await test('one question at a time: its control toggles it, Escape and Cancel say no, and a vanished control ends it',async()=>{
  let first=askDelete('9001');const again=await askDelete('9001');assert.equal(again,null);assert.equal(await first,null);assert.equal(ask(),null,'pressing the control again closes it');
  first=askDelete('9001');const second=askDelete('9002');assert.equal(await first,null,'opening another question answers the first with no');assert.equal(qd.querySelectorAll('.askInline').length,1);assert.equal(ask().parentNode,del('9002').closest('.existingCampaign'));
  ask().dispatchEvent(new qw.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));assert.equal(await second,null);assert.equal(qd.activeElement,del('9002'),'focus returns to the control');
  const cf=qd.getElementById('clearFeed'),clear=qw.askInline(cf,{key:'clearFeed',q:'Clear the autopilot activity log?',yes:'Clear log',place:box=>cf.closest('.card__h').after(box)});
  assert.equal(ask().previousElementSibling.className,'card__h');assert.equal(qd.activeElement.dataset.ask,'yes');ask().querySelector('[data-ask="no"]').click();assert.equal(await clear,null);
  const gone=askDelete('9002');list.innerHTML='';await tick();assert.equal(await gone,null);assert.equal(ask(),null,'the question ends with its campaign');
});
q.window.close();

await test('a launched draft settles: counts roll, the card folds away, then the list is drawn again',async()=>{
  const s=new JSDOM('<!doctype html><body><section class="oppChannel"><div class="oppTabs"><button class="oppTab active" data-view="unused">Recommended <span>3</span></button><button class="oppTab" data-view="inuse">Already in use <span>1</span></button></div><div id="oppCards"><div class="oppCard" id="a"><button class="opGen">Queued ✓</button></div><div class="oppCard" id="b"><button class="opGen">Create review draft</button></div></div></section></body>',{runScripts:'outside-only'}),sw=s.window,sd=sw.document;
  const timers=[],flow=[],renders=[];sw.setTimeout=(fn,ms)=>{timers.push({fn,ms});return timers.length;};const run=ms=>{const t=timers.shift();assert.equal(t.ms,ms);t.fn();};
  Object.assign(sw,{$:q=>sd.querySelector(q),setOppMeta(){flow.push('meta');},renderOpportunities(){renders.push(1);},BritesFlow:{tick:(el,up)=>flow.push('tick '+el.textContent+' '+up),pulse:el=>flow.push('pulse '+el.dataset.view),leave:el=>{flow.push('leave '+el.id);el.hidden=true;}}});
  sw.eval(pick('oppSettle'));const cards=sd.getElementById('oppCards'),reset=h=>{cards.innerHTML=h;flow.length=0;renders.length=0;};
  const card=sd.getElementById('a');sw.oppSettle(card);assert.equal(flow.length,0,'the confirmation stays a moment');run(1200);
  assert.deepEqual(flow,['meta','tick 2 false','tick 2 true','pulse inuse','leave a']);assert.equal(renders.length,0);run(300);assert.equal(renders.length,1,'drawn again once the fold is done');
  reset('<div class="oppCard" id="a2"><button class="opGen">Queued ✓</button></div><div class="oppCard" id="b2"><button class="opGen is-busy">Generating…</button></div>');
  sw.oppSettle(sd.getElementById('a2'));run(1200);run(300);
  assert.equal(renders.length,0,'a draft still being written keeps its card');assert.equal(sd.getElementById('a2'),null,'only the settled card goes');assert(sd.getElementById('b2'));
  reset('<div class="oppCard" id="a3"></div>');const a3=sd.getElementById('a3');sw.oppSettle(a3);a3.remove();run(1200);assert.deepEqual(flow,['meta'],'a list already drawn again needs nothing more');assert.equal(timers.length,0);
  delete sw.BritesFlow;reset('<div class="oppCard" id="c"></div>');const c=sd.getElementById('c');sw.oppSettle(c);run(1200);assert.equal(c.hidden,true,'without the motion module the card still goes');run(300);assert.equal(renders.length,1);
  const launch=pick('launchOpp');assert.match(launch,/oppSettle\(btn\.closest\("\.oppCard"\)\)/);assert.doesNotMatch(launch,/setTimeout\(function\(\)\{ setOppMeta\(\); renderOpportunities\(\); \}, 1500\)/);
  s.window.close();
});

await test('a researched occasion date names its source; research lists the pages it read under Research details',()=>{
  const r=new JSDOM('<!doctype html><body><div id="scanAudit"></div></body>',{runScripts:'outside-only'}),rw=r.window,rd=rw.document;let channel='search';
  Object.assign(rw,{$:q=>rd.querySelector(q),SCAN_AUDIT:null,OPP_RECONCILIATION:null,currentResearchChannel:()=>channel,researchState:()=>({status:'ready',checkedAt:Date.now()-3600000,message:'Research results are saved.'}),
    researchCheckChannel:x=>x.channel,auditStatus:()=>({color:'green',icon:'✓'}),auditTime:ms=>ms+' ms',friendlyResearchError:e=>e,timeago:()=>'1h',toast(){}});
  rw.eval(['esc','cmdAttr','oppSourceUrl','oppSourceName','renderScanAudit'].map(pick).join('\n'));
  assert.equal(rw.oppSourceName({source:'web-verified research',reference:'Holiday calendar lists "Halloween"',url:'https://www.timeanddate.com/holidays/us/halloween'}),'Holiday calendar lists "Halloween"');
  assert.equal(rw.oppSourceName({reference:'https://www.example.org/dates',url:'https://www.example.org/dates'}),'example.org','a bare link reads as its site');
  assert.equal(rw.oppSourceName({reference:'Almanac',url:'javascript:alert(1)'}),'Almanac');assert.equal(rw.oppSourceName({}),'');
  for(const url of ['javascript:alert(1)','https://evil.example@good.example/x','//cdn.example/x',' https://lead.example/x','ftp://files.example/x'])assert.equal(rw.oppSourceUrl({url}),null,'not linked: '+url);
  assert.match(pick('planBlock'),/confirmed by web research"\+\(oppSourceName\(dc\)\?": "\+esc\(oppSourceName\(dc\)\):""\)/,'the explanation names the source; the card itself links the page');
  assert.match(pick('oppCard'),/confirmed by web research'\+\(oppSourceName\(dc\)\?': '\+oppSourceName\(dc\):''\)/,'the date tooltip names the source');
  const audit={checks:[{id:'keyword_planner_pool',channel:'search',label:'Keyword Planner demand',status:'ok'}],sources:[{url:'https://www.timeanddate.com/holidays/us/halloween',title:'Halloween 2026',pageAge:'2 weeks ago'},{url:'javascript:alert(1)',title:'bad'},{url:'https://user@en.wikipedia.org/x',title:'sign-in part'},{url:'https://en.wikipedia.org/wiki/Thanksgiving',title:''}]};
  rw.renderScanAudit(audit);const pages=rd.querySelector('.researchTechnical .scanPages');assert(pages,'pages sit inside Research details');assert.equal(pages.open,false,'folded by default');
  assert.equal(pages.querySelector('summary').textContent,'Pages searched · 2');assert.deepEqual([...pages.querySelectorAll('a')].map(a=>[a.textContent,a.target,a.rel]),[['Halloween 2026','_blank','noopener noreferrer'],['en.wikipedia.org','_blank','noopener noreferrer']]);
  pages.open=true;rw.renderScanAudit();assert.equal(rd.querySelector('.scanPages').open,true,'an opened list stays open through a refresh');
  channel='pmax';rw.renderScanAudit();assert.equal(rd.querySelector('.scanPages'),null,'product ads research does not claim the Search pages');r.window.close();
});

await test('asking Google for the serving check asks for servingCheck alone and shows the standard spinner, through a redraw; a failure gives the button back',async()=>{
  const v=new JSDOM('<!doctype html><body><div id="host"></div></body>',{runScripts:'outside-only'}),vw=v.window,vd=vw.document,host=vd.getElementById('host');let answer;const asked=[];
  Object.assign(vw,{DASH:{},api:(action,body)=>{asked.push([action,JSON.parse(JSON.stringify(body))]);return new Promise((res,rej)=>{answer={res,rej};});},servingOf:()=>null,servingClass:()=>'',servingBadgeAttrs:()=>({cls:'',text:'',title:''}),renderServing:s=>'<p class="answer">'+s.headline+'</p>',
    servingStoredHtml:()=>'<div class="servHead"><b>Google serving check</b><button type="button" class="btn ghost sm servRun">Check with Google</button></div>'});
  vw.eval([html.match(/^var SERV_ASK=\{\};$/m)[0]].concat(['esc','btnBusy','loadServing','wireServing'].map(pick)).join('\n'));
  const draw=()=>{host.innerHTML='<div class="servbox" data-sc="5">'+vw.servingStoredHtml()+'</div>';vw.wireServing(host);},btn=()=>host.querySelector('.servRun'),spin='<span class="spin bspin"></span>Asking Google…';
  draw();let b=btn(),run=vw.loadServing(host.querySelector('.servbox'));
  assert.equal(b.disabled,true);assert.equal(b.innerHTML,spin);assert.equal(await vw.loadServing(host.querySelector('.servbox')),undefined,'one ask at a time');
  assert.deepEqual(asked,[['servingCheck',{id:'5'}]],'the check asks the server for the serving check alone');assert.doesNotMatch(html,/campaignTimeline/,'nothing on the page asks for the old timeline');
  answer.rej(new Error('Google did not answer.'));await run;
  assert.equal(b.disabled,false);assert.equal(b.textContent,'Check with Google','the button keeps its own words');
  assert.equal(host.querySelector('.servErr').textContent,'Google serving check failed: Google did not answer.');
  run=vw.loadServing(host.querySelector('.servbox'));draw();assert.notEqual(btn(),b);assert.equal(btn().disabled,true);assert.equal(btn().innerHTML,spin,'a redraw keeps the wait on the new button');
  answer.res({ok:true,serving:{headline:'Ready to serve'}});await run;assert.equal(host.querySelector('.answer').textContent,'Ready to serve','the answer lands in the box on screen');
  assert.equal(btn().textContent,'Check again with Google');assert.equal(btn().disabled,false);
  draw();run=vw.loadServing(host.querySelector('.servbox'));draw();answer.rej(new Error('Timed out'));await run;
  assert.equal(btn().disabled,false);assert.equal(btn().textContent,'Check with Google');assert.equal(host.querySelector('.servErr').textContent,'Google serving check failed: Timed out');
  draw();assert.equal(btn().disabled,false,'nothing is left waiting');v.window.close();
});

await test('a date range popover stays on screen: presets above the calendar on phones, leftwards at the right edge',()=>{
  assert.match(html,/@media \(max-width:520px\)\{\s*\.rpPop\{max-height:84vh;overflow-y:auto\}\.rpPop \.rpRow\{flex-direction:column\}/);
  assert.match(pick('rpDraw'),/<div class="rpRow" style="display:flex"><div>'\+presets\+'<\/div><div class="rpMain" /);assert.match(pick('rpDraw'),/var presets='<div class="rpPresets" /);
  assert.match(pick('rpToggle'),/p\.style\.display="block";rpFit\(p\);/);
  const f=new JSDOM('<!doctype html><body><div class="rp"><div class="rpPop" style="position:absolute;left:0"></div></div></body>',{runScripts:'outside-only'}),fw=f.window,fd=fw.document,p=fd.querySelector('.rpPop');
  Object.defineProperty(fd.documentElement,'clientWidth',{value:1440});fw.eval(pick('rpFit'));let right=1701;p.getBoundingClientRect=()=>({right});
  fw.rpFit(p);assert.equal(p.style.left,'auto');assert.equal(p.style.right,'0px','past the right edge it opens leftwards');
  right=893;fw.rpFit(p);assert.equal(p.style.left,'0px');assert.equal(p.style.right,'auto','with room it opens as before');f.window.close();
});

await test('one indicator per wait, shell top bar, phone channel tabs and a shadow-free closed drawer',()=>{
  assert.match(html,/BritesProgress\.own\(function\(host\)\{var scope=host===document\.body\?document\.querySelector\("\.view:not\(\.hidden\)"\):host;/);
  assert.match(html,/<script src="\/brites-progress\.js\?v=20260929-owner"/);assert.match(html,/href="\/brites-groups\.css\?v=20260929-shell"/);
  assert.doesNotMatch(read('brites-groups.css'),/\.topbar|\.brand\b/,'the top bar is styled only by the shell');
  assert.match(html,/\.rail\{[^}]*box-shadow:none[^}]*\}\s*body\.navOpen \.rail\{transform:none;box-shadow:14px 0 40px rgba\(0,0,0,\.25\)\}/);
  assert.match(html,/@media\(max-width:420px\)\{\.growthLaneTabs button\{flex:1 1 auto;/);assert.match(html,/\.spin\.sm\{width:12px;height:12px;flex:0 0 12px\}/);
  assert.match(html,/\.ieRow input\{flex:1 1 110px;max-width:170px;min-width:0;/,'on a phone the amount, Save and Cancel share one row');});
console.log(passed+' console inline action checks passed.');
})().catch(e=>{console.error(e);process.exit(1);});
