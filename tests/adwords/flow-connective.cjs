// Connective cues between console tabs (brites-flow.js): a draft is followed from
// its opportunity to Approvals, a published campaign from Approvals to Overview,
// each is marked where it lands and keeps that mark through a refresh, and the
// Opportunities journey header shows real state and opens each step. Nothing
// flies over an open dialog or under reduced motion. The console's own nav,
// badge and tab functions run unmodified around the module.
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const {JSDOM}=require(process.env.BRITES_EDITOR_DOM_RUNTIME?path.join(process.env.BRITES_EDITOR_DOM_RUNTIME,'jsdom'):'jsdom');
const ROOT=path.resolve(__dirname,'../..'),read=f=>fs.readFileSync(path.join(ROOT,f),'utf8');
const html=read('brites-adwords.html'),flow=read('brites-flow.js'),css=read('brites-flow.css');
const source=re=>{const m=html.match(re);assert(m,'console source has '+re);return m[0];};
const wait=ms=>new Promise(r=>setTimeout(r,ms)),settle=()=>wait(60);

// 1. Wiring: one cache version for both files, every hook guarded and in its place.
const head=html.slice(0,html.indexOf('</head>'));
const cssTag=head.match(/<link rel="stylesheet" href="\/brites-flow\.css\?v=([\w.-]+)">/),jsTag=head.match(/<script src="\/brites-flow\.js\?v=([\w.-]+)" defer><\/script>/);
assert(cssTag&&jsTag&&cssTag[1]===jsTag[1],'the page loads the flow stylesheet and deferred script with one cache version');
const hooks=[...html.matchAll(/BritesFlow\.\w+\(/g)].length;
assert.equal(hooks,11);assert.equal([...html.matchAll(/typeof BritesFlow!=="undefined"/g)].length,hooks,'every hook is guarded, so the console works without the module');
const within=(start,end,needle)=>{const a=html.indexOf(start),b=html.indexOf(end,a+start.length);assert(a>=0&&b>a,'found '+start);assert(html.slice(a,b).includes(needle),needle+' is called from '+start);};
[['function buildNav(){','\n','BritesFlow.nav(n)'],['function updateBadges(){','\n','BritesFlow.badge(b,c)'],['function go(k){','\n','BritesFlow.go(k)'],
 ['async function generateAndWait(','\n}','BritesFlow.expect("approvals",{id:st.approvalId,from:"Opportunities"})'],
 ['async function launchOpp(','\n}','BritesFlow.send(btn,"approvals",{id:r.approvalId,from:"Opportunities"})'],
 ['function renderPmaxSection(','\n}',"BritesFlow.send(btn,'approvals',{id:st.approvalId,from:'Opportunities'})"],
 ['async function openAdDesignApproval(','\n',"BritesFlow.arrive(card,{from:'Design studio'})"],['async function publishDraft(','\n','BritesFlow.published(btn,id)'],
 ['function wireAdDesignSubmission(','\n}','BritesFlow.published(card,a.id,r)'],['function renderApprovals(','\n}',"BritesFlow.leave(b.closest('.draft'))"]].forEach(h=>within(...h));
for(const m of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g))if(m[1].trim())new vm.Script(m[1]);
new vm.Script(flow);assert(css.includes('@media (prefers-reduced-motion:reduce)'),'every motion has a still equivalent');

// 2. A small console: the real nav, badge and tab functions, with stubbed data loaders.
const page=[/^const \$=.*$/m,/^const elFrom=.*$/m,/^var NAV=.*$/m,/^function buildNav\(\)\{.*$/m,/^function pendingInScope\(\)\{.*\n.*$/m,/^function updateBadges\(\)\{.*$/m,/^function navDrawer\(open\)\{.*$/m,/^function go\(k\)\{.*$/m].map(source).join('\n');
const inner={command:'<div class="card pad"><div class="card__h"><h3>Your campaigns</h3></div><div id="snapshot"><table class="ptable campaignTree"><tbody><tr class="crow" data-cid="111"><td>Existing campaign</td><td>Enabled</td></tr></tbody></table></div></div>',
 approvals:'<div class="appr" id="apprList"></div>',
 bench:source(/<div class="oppJourney"[^\n]*?<\/div>/)+'<section id="growthStore"><p class="oppStoreStatus">Research is current</p><button id="oppScan">Refresh research</button><div id="oppCards"><article class="oppCard"><h4>Autumn necklaces — gift searches</h4><button class="opGen">Create review draft</button><span class="opMsg"></span></article></div></section><section id="growthLearning"><h3>What worked</h3></section>'};
const skeleton='<!doctype html><html><head></head><body><nav class="nav" id="nav"></nav><header class="topbar"><button class="navburger" id="navBurger" aria-label="Menu"></button><h1 id="vtitle"></h1><p id="vsub"></p></header>'+
 ['groups','command','sales','approvals','bench','controls'].map(v=>'<section class="view hidden" id="v-'+v+'"'+(v==='bench'?' data-lane="search"':'')+'>'+(inner[v]||'')+'</section>').join('')+
 '<script>let DASH={pending:[],stuck:[],lastMetrics:[{status:"ENABLED"},{status:"PAUSED"}]};var OPPS=[{},{},{acted:{where:"approval"}}],PMAXOPPS=[],PB={lessons:[{},{},{}]},RS="ready";'+
 'function researchState(){return {status:RS};}function researchNeedsRefresh(){return RS!=="ready";}function growthLane(l){document.getElementById("v-bench").dataset.lane=l;}'+
 'function ensureOpportunities(){}function loadPlaybook(){}function loadDesignStudioGrowth(){}function loadGrowthPerformance(){}window.scrollTo=function(){};\n'+page+'\nbuildNav();go("groups");</script></body></html>';
const card=(id,title)=>'<div class="draft"><div class="draft__top" role="button" tabindex="0"><span class="tagk">search</span><div><div class="ti">'+title+'</div><div class="me">Review campaign changes</div></div></div><div class="draft__quick"><button data-ap="'+id+'">Publish</button><button data-rj="'+id+'">Delete</button></div></div>';
async function open(){const dom=new JSDOM(skeleton,{runScripts:'dangerously',pretendToBeVisual:true,url:'https://console.test/'});await new Promise(r=>dom.window.addEventListener('load',r));return dom;}

(async()=>{
 {// Without the module the console behaves exactly as before.
  const dom=await open(),w=dom.window;w.go('approvals');w.eval('DASH.pending.push({id:"x"})');w.updateBadges();w.buildNav();
  assert.equal(w.document.querySelector('#nav button[data-v="approvals"] .badge').textContent,'1');assert(w.document.querySelector('#v-bench .oppJourney > span'),'the journey keeps its static labels');dom.window.close();}

 const dom=await open(),w=dom.window,d=w.document;let now=1.9e12,reduce=false,phone=false;const anims=[],scrolls=[];
 w.Date.now=()=>now;w.matchMedia=q=>({matches:reduce&&q.includes('reduce'),media:q,addEventListener(){},removeEventListener(){}});
 w.Element.prototype.animate=function(frames,opts){const a={el:this,frames,opts,onfinish:null,oncancel:null,cancel(){}};anims.push(a);w.setTimeout(()=>a.onfinish&&a.onfinish(),5);return a;};
 w.Element.prototype.scrollIntoView=function(){scrolls.push(this);};
 const R=(left,top,width,height)=>({left,top,width,height,right:left+width,bottom:top+height,x:left,y:top});
 // Desktop: the rail is on screen. Phone: the rail is an off-canvas drawer and the menu button stands in.
 w.Element.prototype.getBoundingClientRect=function(){if(this.id==='navBurger')return phone?R(330,12,40,40):R(0,0,0,0);const b=this.closest('#nav button');if(b)return phone?R(0,0,0,0):R(12,80+[...b.parentNode.children].indexOf(b)*44,196,36);return R(260,300,400,120);};
 const script=d.createElement('script');script.textContent=flow;d.body.appendChild(script);const F=w.BritesFlow;assert(F,'the module exposes BritesFlow');
 const $=q=>d.querySelector(q),nav=v=>$('#nav button[data-v="'+v+'"]'),run=c=>w.eval(c),cardOf=id=>$('#apprList [data-ap="'+id+'"]').closest('.draft'),ghostOf=()=>anims.find(a=>a.el.classList.contains('bf-ghost'));

 // 3. The journey header reads the same state its tabs show, and each step opens its place.
 const J=$('#v-bench .oppJourney'),step=k=>J.querySelector('[data-journey="'+k+'"]'),said=k=>step(k).querySelector('strong').textContent+'|'+step(k).querySelector('em').textContent,flag=(k,a)=>step(k).hasAttribute('data-'+a);
 assert(J.classList.contains('bf-journey')&&J.getAttribute('role')==='navigation');
 assert.deepEqual([...J.querySelectorAll('[data-journey]')].map(b=>b.firstChild.textContent),['Research','Create','Review','Measure','Learn']);
 assert.deepEqual(['research','create','review','measure','learn'].map(said),['|Up to date','2|ideas','|Clear','1|live','3|lessons']);
 assert(flag('research','here')&&flag('create','here')&&!flag('learn','here'));assert.equal(step('create').getAttribute('aria-current'),'step');assert.equal(step('create').title,'Create · 2 ideas');
 run('window.loaded=DASH;DASH=null');w.updateBadges();run('DASH=window.loaded');// the badge is drawn before the dashboard arrives
 run('RS="stale";DASH.pending.push({id:"ap0",payload:{}})');w.updateBadges();await settle();
 assert.equal(said('research'),'|Refresh due');assert(flag('research','attn'));assert.equal(said('review'),'1|waiting');assert(flag('review','attn'));
 assert.equal(F._state.arrivals.length,0,'drafts already waiting at start are not announced as new');
 assert(!nav('approvals').querySelector('.badge').classList.contains('bf-pulse')&&!$('#navBurger').classList.contains('bf-has-dot'));
 const gen=$('#oppCards .opGen'),msg=$('#oppCards .opMsg');gen.classList.add('is-busy');msg.textContent='writing copy… 3s';await settle();
 assert.equal(said('create'),'|Drafting');assert(flag('create','busy'),'a draft being written shows a spinner in the header');gen.classList.remove('is-busy');msg.textContent='';await settle();
 w.go('bench');await settle();
 step('learn').click();await settle();assert.equal($('#v-bench').dataset.lane,'learning');assert.equal(scrolls.at(-1).id,'growthLearning');assert($('#growthLearning h3').classList.contains('bf-pulse'));assert(flag('learn','here')&&!flag('create','here'));
 step('create').click();await settle();assert.equal($('#v-bench').dataset.lane,'search');assert(scrolls.at(-1).classList.contains('oppCard'));assert(gen.classList.contains('bf-pulse'),'Create points at the next draft to create');
 step('research').click();assert.equal(scrolls.at(-1).id,'growthStore');assert($('#oppScan').classList.contains('bf-pulse'),'stale research points at its refresh button');
 step('review').click();assert(!$('#v-approvals').classList.contains('hidden')&&nav('approvals').classList.contains('active'));
 w.go('bench');step('measure').click();assert(!$('#v-command').classList.contains('hidden'));assert($('#v-command .card__h').classList.contains('bf-pulse'),'Measure opens the live campaign list');

 // 4. A draft travels from its opportunity to the Approvals tab.
 run('DASH.pending=[]');w.updateBadges();w.go('bench');await settle();anims.length=0;
 F.send(gen,'approvals',{id:'ap1',from:'Opportunities'});const ghost=$('.bf-ghost'),toApprovals=ghostOf();
 assert(ghost&&ghost.getAttribute('aria-hidden')==='true');assert.equal(ghost.textContent,'Autumn necklaces');
 assert.equal(toApprovals.frames[0].transform,'translate(460.0px,360.0px) scale(1.000)','the ghost starts on the opportunity');
 assert.equal(toApprovals.frames.at(-1).transform,'translate(110.0px,230.0px) scale(0.380)','and ends on the Approvals tab');
 assert(toApprovals.opts.duration>=150&&toApprovals.opts.duration<=400&&/^cubic-bezier/.test(toApprovals.opts.easing));
 assert(nav('approvals').querySelector('.bf-dot')&&$('#navBurger').classList.contains('bf-has-dot'),'the tab shows that something new is waiting');
 await settle();assert(!$('.bf-ghost'));assert(nav('approvals').classList.contains('bf-pulse'),'the tab acknowledges the hand-off');

 // 5. The draft lands while the operator is elsewhere, and is marked when they look.
 run('DASH.pending.push({id:"ap1",payload:{mutateOperations:[{campaignOperation:{create:{name:"BA · Autumn necklaces"}}}]}})');$('#apprList').innerHTML=card('ap1','Autumn necklaces — Search');w.updateBadges();await settle();
 const badge=nav('approvals').querySelector('.badge');assert.equal(badge.textContent,'1');assert(badge.classList.contains('bf-tick-up')&&badge.classList.contains('bf-pulse'),'the count rolls up');
 assert(!nav('approvals').querySelector('.bf-dot'),'the count replaces the dot');assert(!$('#apprList .bf-from'),'nothing is marked until the operator looks');
 w.go('approvals');await settle();let landed=cardOf('ap1');
 assert(landed.classList.contains('bf-arrived'));assert.equal(landed.querySelector('.draft__top .me .bf-from').textContent,'from Opportunities');assert(!$('#navBurger').classList.contains('bf-has-dot'),'visiting the tab clears its dot');
 $('#apprList').innerHTML=card('ap1','Autumn necklaces — Search');w.updateBadges();landed=cardOf('ap1');
 assert.equal(landed.querySelector('.bf-from').textContent,'from Opportunities','a refresh that redraws the list keeps the mark');assert(landed.classList.contains('bf-held')&&landed.querySelector('.bf-from').classList.contains('bf-still'),'held still, without replaying');
 now+=3000;w.updateBadges();assert(!landed.classList.contains('bf-held')&&landed.querySelector('.bf-from'));
 now+=6000;w.updateBadges();assert(landed.querySelector('.bf-from').classList.contains('bf-out'),'the mark fades after a few seconds');assert.equal(F._state.marks.length,0);
 run('DASH.pending.push({id:"ap2",payload:{}})');$('#apprList').insertAdjacentHTML('beforeend',card('ap2','Winter rings — Search'));w.updateBadges();await settle();
 assert.equal(cardOf('ap2').querySelector('.bf-from').textContent,'New','a draft from elsewhere is marked New');
 await wait(500);w.go('bench');w.BritesGroups={selection:()=>'rings',matchesApproval:a=>a.id==='ap2',chrome(){},show(){}};w.updateBadges();assert.equal(nav('approvals').querySelector('.badge').textContent,'1');
 delete w.BritesGroups;w.updateBadges();const regrouped=nav('approvals').querySelector('.badge');assert.equal(regrouped.textContent,'2');
 assert(regrouped.classList.contains('bf-tick-up')&&!regrouped.classList.contains('bf-pulse')&&!$('#navBurger').classList.contains('bf-has-dot'),'a wider product group rolls the count without announcing new drafts');
 w.go('approvals');await settle();

 // 6. Publishing follows the campaign to Overview and marks its own row; a new version marks the campaign it changed.
 anims.length=0;F.published(cardOf('ap1').querySelector('[data-ap]'),'ap1',{ok:true,status:'VALIDATED'});assert(!ghostOf()&&!nav('command').querySelector('.bf-dot'),'a validation-only run published nothing, so nothing moves');
 F.published(cardOf('ap1').querySelector('[data-ap]'),'ap1');
 assert(ghostOf()&&ghostOf().frames.at(-1).transform.startsWith('translate(110.0px,142.0px)'),'the published campaign flies to Overview');assert(nav('command').querySelector('.bf-dot'));await settle();
 run('DASH.pending=DASH.pending.filter(a=>a.id!=="ap1")');cardOf('ap1').remove();w.updateBadges();
 $('#snapshot tbody').insertAdjacentHTML('beforeend','<tr class="crow" data-cid="333"><td><span><b>Paused last month</b></span></td></tr><tr class="crow" data-cid="222"><td><span><b>BA · Autumn necklaces</b></span></td></tr>');await settle();assert(!$('#snapshot .bf-from'));
 w.go('command');await settle();const row=$('#snapshot tr[data-cid="222"]');
 assert(row.classList.contains('bf-arrived'));assert.equal(row.querySelector('td .bf-from').textContent,'Just published');assert(!nav('command').querySelector('.bf-dot'));
 assert(!$('#snapshot tr[data-cid="333"]').classList.contains('bf-arrived')&&!$('#snapshot tr[data-cid="111"]').classList.contains('bf-arrived'),'other campaigns that appear are left alone');
 w.go('approvals');run('DASH.pending.push({id:"ap3",payload:{versionChange:{campaignId:"111"}}})');$('#apprList').insertAdjacentHTML('beforeend',card('ap3','Existing campaign — new version'));w.updateBadges();await settle();
 F.published(cardOf('ap3').querySelector('[data-ap]'),'ap3');await settle();w.go('command');await settle();
 assert.equal($('#snapshot tr[data-cid="111"] td .bf-from').textContent,'Just updated');
 w.go('approvals');F.published(null,'design1',{ok:true,status:'APPLIED',publishedCampaignId:'444'});$('#snapshot tbody').insertAdjacentHTML('beforeend','<tr class="crow" data-cid="555"><td><span><b>Another new campaign</b></span></td></tr>');
 w.go('command');await settle();assert(!$('#snapshot tr[data-cid="555"] .bf-from'),'another new row is not mistaken for it');
 $('#snapshot tbody').insertAdjacentHTML('beforeend','<tr class="crow" data-cid="444"><td><span><b>Complete ad</b></span></td></tr>');await wait(600);
 assert.equal($('#snapshot tr[data-cid="444"] td .bf-from').textContent,'Just published','the campaign Google reports publishing is marked when its row appears');

 // 7. A deleted draft folds away instead of vanishing.
 w.go('approvals');await settle();anims.length=0;const doomed=cardOf('ap2');F.leave(doomed);const fold=anims.find(a=>a.el===doomed);
 assert(doomed.classList.contains('bf-leaving')&&fold&&fold.opts.fill==='forwards'&&fold.frames.at(-1).height==='0px');

 // 8. Nothing plays over an open dialog; the landing waits until it closes.
 const dlg=d.createElement('dialog');dlg.setAttribute('open','');d.body.appendChild(dlg);w.go('bench');anims.length=0;
 F.send(gen,'approvals',{id:'ap4',from:'Opportunities'});assert(!$('.bf-ghost')&&!anims.length,'no ghost over an open dialog');
 run('DASH.pending.push({id:"ap4",payload:{}})');$('#apprList').insertAdjacentHTML('beforeend',card('ap4','Spring earrings — Search'));w.updateBadges();w.go('approvals');await settle();
 assert(!cardOf('ap4').querySelector('.bf-from'),'arrivals wait while a dialog is open');
 dlg.removeAttribute('open');dlg.dispatchEvent(new w.Event('close'));await settle();assert.equal(cardOf('ap4').querySelector('.bf-from').textContent,'from Opportunities','and play once it closes');

 // 9. On a phone the ghost flies to the menu button, which carries the dot.
 phone=true;w.go('bench');await settle();anims.length=0;F.send(gen,'sales');
 assert.equal(ghostOf().frames.at(-1).transform,'translate(350.0px,32.0px) scale(0.380)');assert($('#navBurger').classList.contains('bf-has-dot'));
 await settle();assert($('#navBurger').classList.contains('bf-pulse'));w.go('sales');assert(!$('#navBurger').classList.contains('bf-has-dot'));phone=false;

 // 10. Reduced motion: no flight, fold or roll, but the same marks and dots.
 reduce=true;w.go('bench');await settle();anims.length=0;F.send(gen,'approvals',{id:'ap5',from:'Opportunities'});assert(!$('.bf-ghost')&&!anims.length);
 run('DASH.pending.push({id:"ap5",payload:{}})');$('#apprList').insertAdjacentHTML('beforeend',card('ap5','Summer anklets — Search'));w.updateBadges();w.go('approvals');await settle();
 assert.equal(cardOf('ap5').querySelector('.bf-from').textContent,'from Opportunities');
 const gone=cardOf('ap5');F.leave(gone);assert(gone.hidden&&!anims.length,'a deleted draft is removed at once');
 const still=d.createElement('b');still.textContent='3';F.count(still,7);assert.equal(still.textContent,'7');
 reduce=false;const rolling=d.createElement('b');rolling.textContent='2';F.count(rolling,9);assert.notEqual(rolling.textContent,'9');await wait(450);assert.equal(rolling.textContent,'9','a changed number counts up in under half a second');
 dom.window.close();

 const manifest=read('scripts/build-public.cjs');
 if(/brites-flow\.js/.test(manifest))assert.match(manifest,/brites-flow\.css/);else console.log('NOTE scripts/build-public.cjs does not list brites-flow.js and brites-flow.css yet; the public build will not ship them until it does.');
 console.log('PASS flow cues: guarded hooks, journey state and steps, hand-offs to Approvals and Overview, marks that survive refresh, fold-away, dialog and phone and reduced-motion behaviour');
})().catch(e=>{console.error(e);process.exit(1);});
