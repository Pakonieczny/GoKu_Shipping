/* Brites flow: the connective cues between console tabs.
   When work moves between tabs (a draft goes to Approvals, a published campaign
   goes to Overview), a small ghost travels from the item to the destination
   tab, the tab acknowledges it, and the item is marked where it lands with
   where it came from. Motion is short and eased out, never waits for or blocks
   the work, never plays over an open dialog, and is skipped entirely when the
   operator prefers reduced motion. Every entry point is optional: the console
   works unchanged when this file is absent. */
(function(root){
 'use strict';
 const doc=root.document;
 const VIEWS={groups:'Products & groups',command:'Overview',sales:'Sales',approvals:'Approvals',bench:'Opportunities',controls:'Controls'};
 const TTL=15*60000,FRESH=120000,CUE_MS=8000,HOLD_MS=2600,EARLY=10000;
 const st={badge:null,ids:null,arrivals:[],marks:[],dots:new Set(),rows:new Map(),rowsReady:false,poll:0,view:'',frame:0};
 const timers=new WeakMap();

 const reduced=()=>!root.matchMedia||root.matchMedia('(prefers-reduced-motion: reduce)').matches;
 const modalOpen=()=>!!doc.querySelector('dialog[open]');
 const canAnimate=el=>!!el&&typeof el.animate==='function'&&!reduced();
 const later=fn=>(root.requestAnimationFrame||(f=>setTimeout(f,16)))(fn);
 const esc=s=>root.CSS&&root.CSS.escape?root.CSS.escape(s):String(s).replace(/["\\\]]/g,'\\$&');
 // DASH is a top-level `let` in the console script: visible here by name, not on window.
 function dash(){try{return typeof DASH!=='undefined'&&DASH?DASH:null;}catch(e){return null;}}
 function call(name){const fn=root[name];if(typeof fn!=='function')return undefined;try{return fn.apply(root,[].slice.call(arguments,1));}catch(e){return undefined;}}

 function currentView(){const v=doc.querySelector('.view:not(.hidden)');return v&&v.id?v.id.replace(/^v-/,''):'';}
 function navButton(view){return doc.querySelector('#nav button[data-v="'+view+'"]');}
 function onScreen(el){if(!el||!el.isConnected)return false;const r=el.getBoundingClientRect();return r.width>0&&r.height>0&&r.right>0&&r.bottom>0&&r.left<root.innerWidth&&r.top<root.innerHeight;}
 // On narrow screens the rail is an off-canvas drawer; the menu button stands in for it.
 function navTarget(view){const b=navButton(view);if(onScreen(b))return b;const menu=doc.getElementById('navBurger');return onScreen(menu)?menu:null;}
 function labelOf(el){if(!el||!el.closest)return '';const card=el.closest('.draft,.oppCard,.pmxrow,[data-approval-id],article')||el;const t=card.querySelector('.ti,h4,.pmxTitle b,h3');return String((t||card).textContent||'').replace(/\s+/g,' ').trim().split(' — ')[0].slice(0,90);}

 /* ---- small motions ---------------------------------------------------- */
 function flash(el,cls,ms){
  if(!el)return;const t=timers.get(el)||{};clearTimeout(t[cls]);el.classList.remove(cls);void el.offsetWidth;el.classList.add(cls);
  t[cls]=setTimeout(()=>el.classList.remove(cls),ms);timers.set(el,t);
 }
 // A ring that widens and fades: "this is where it went". A still ring in reduced motion.
 function pulse(el){flash(el,'bf-pulse',reduced()?1200:450);}
 // A changed number rolls in from the direction it moved.
 function tick(el,up){if(!el||reduced())return;el.classList.remove(up?'bf-tick-down':'bf-tick-up');flash(el,up?'bf-tick-up':'bf-tick-down',420);}
 function count(el,to,opts){
  if(!el)return;opts=opts||{};to=Number(to)||0;const fmt=opts.format||(n=>String(Math.round(n)));
  const from=opts.from!=null?Number(opts.from):Number(el.dataset.bfValue!=null?el.dataset.bfValue:parseFloat(el.textContent))||0;
  el.dataset.bfValue=String(to);if(el._bfCount&&root.cancelAnimationFrame)root.cancelAnimationFrame(el._bfCount);el._bfCount=0;
  if(from===to||reduced()||typeof root.requestAnimationFrame!=='function'){el.textContent=fmt(to);return;}
  const t0=root.performance.now(),dur=opts.duration||360;
  const step=now=>{const p=Math.min(1,Math.max(0,now-t0)/dur),e=1-Math.pow(1-p,3);el.textContent=fmt(from+(to-from)*e);el._bfCount=p<1?root.requestAnimationFrame(step):0;};
  el._bfCount=root.requestAnimationFrame(step);
 }

 // A compact ghost of the item travels along a gentle curve to the destination tab.
 function fly(from,to,label){
  return new Promise(done=>{
   if(!from||!to||!from.isConnected||modalOpen()||!canAnimate(doc.body)){done(false);return;}
   const a=from.getBoundingClientRect(),b=(to.querySelector('.badge,.bf-dot')||to).getBoundingClientRect();
   if(!a.width||!b.width){done(false);return;}
   const g=doc.createElement('div');g.className='bf-ghost';g.setAttribute('aria-hidden','true');g.innerHTML='<i></i><span></span>';g.lastChild.textContent=label||'';
   doc.body.appendChild(g);
   const w=g.offsetWidth,h=g.offsetHeight,x0=a.left+a.width/2-w/2,y0=a.top+a.height/2-h/2,x1=b.left+b.width/2-w/2,y1=b.top+b.height/2-h/2;
   // Bow the path upward by a fraction of its length, like something handed across.
   const dx=x1-x0,dy=y1-y0,len=Math.hypot(dx,dy)||1,bow=Math.min(70,len*.16);let px=-dy/len,py=dx/len;if(py>0){px=-px;py=-py;}
   const cx=(x0+x1)/2+px*bow,cy=(y0+y1)/2+py*bow,at=t=>{const u=1-t;return [u*u*x0+2*u*t*cx+t*t*x1,u*u*y0+2*u*t*cy+t*t*y1];};
   const frames=[0,.2,.45,.7,1].map(t=>{const p=at(t);return {offset:t,transform:'translate('+p[0].toFixed(1)+'px,'+p[1].toFixed(1)+'px) scale('+(1-.62*t*t).toFixed(3)+')',opacity:t===0?0:t===1?.15:1};});
   let settled=false;const end=()=>{if(settled)return;settled=true;g.remove();done(true);};
   try{const anim=g.animate(frames,{duration:380,easing:'cubic-bezier(.25,.7,.25,1)'});anim.onfinish=end;anim.oncancel=end;}catch(e){end();return;}
   setTimeout(end,800);
  });
 }

 /* ---- nav dots: something new is waiting in that tab ---------------------- */
 function dot(view,on){if(!VIEWS[view])return;if(on)st.dots.add(view);else st.dots.delete(view);paintDots();}
 function paintDots(){
  doc.querySelectorAll('#nav button[data-v]').forEach(b=>{
   // Approvals carries a count badge; a dot beside it would say the same thing twice.
   const want=st.dots.has(b.dataset.v)&&!b.querySelector('.badge');let d=b.querySelector('.bf-dot');
   if(want&&!d){d=doc.createElement('span');d.className='bf-dot';d.innerHTML='<span class="bf-vh">new</span>';b.appendChild(d);}else if(!want&&d)d.remove();
  });
  const menu=doc.getElementById('navBurger');if(menu)menu.classList.toggle('bf-has-dot',st.dots.size>0);
 }

 /* ---- arrivals: mark the item where it lands ---------------------------- */
 function approvalCard(id){
  if(id==null)return null;const list=doc.getElementById('apprList');if(!list)return null;const q=esc(String(id));
  const direct=list.querySelector('[data-approval-id="'+q+'"]');if(direct)return direct;
  const button=list.querySelector('[data-ap="'+q+'"],[data-rj="'+q+'"],[data-retry="'+q+'"],[data-creative="'+q+'"]');return button&&button.closest('.draft');
 }
 function approvalIdOf(el){if(!el||!el.dataset)return '';if(el.dataset.approvalId)return el.dataset.approvalId;const b=el.querySelector&&el.querySelector('[data-ap],[data-rj],[data-retry]');return b?(b.dataset.ap||b.dataset.rj||b.dataset.retry||''):'';}
 function campaignRows(){return Array.from(doc.querySelectorAll('#snapshot .crow[data-cid]'));}
 // Remember when each campaign row first appeared, so a newly published campaign can be told apart.
 function scanRows(){const rows=campaignRows();if(!rows.length)return rows;const now=Date.now();rows.forEach(r=>{if(!st.rows.has(r.dataset.cid))st.rows.set(r.dataset.cid,st.rowsReady?now:0);});st.rowsReady=true;return rows;}
 function campaignRow(cid,since,name){
  const rows=scanRows();if(cid)return rows.find(x=>x.dataset.cid===String(cid))||null;
  // A new campaign's name is fixed in its draft: wait for that row rather than guess.
  if(name)return rows.find(x=>{const b=x.querySelector('td b');return !!b&&b.textContent.trim()===name;})||null;
  const fresh=rows.filter(x=>st.rows.get(x.dataset.cid)>=since);
  return fresh.length&&fresh.length<=3?fresh[0]:null; // more than a handful means a different report, not one arrival
 }
 function expect(view,opts){
  opts=opts||{};if(!VIEWS[view])return null;const id=opts.id!=null&&opts.id!==''?String(opts.id):null,key=id?view+':'+id:null;
  if(key){const old=st.arrivals.find(a=>a.key===key);if(old){if(opts.from&&!old.from)old.from=opts.from;return old;}}
  const find=opts.find||(id&&view==='approvals'?()=>approvalCard(id):null);if(!find)return null;
  const a={key:key,view:view,id:id,find:find,from:opts.from||'',text:opts.text||'',at:Date.now()};st.arrivals.push(a);watch();return a;
 }
 function watch(){if(st.poll||!st.arrivals.length&&!st.marks.length)return;st.poll=setInterval(()=>{tryArrivals(false);sweepMarks();if(!st.arrivals.length&&!st.marks.length){clearInterval(st.poll);st.poll=0;}},500);}
 function tryArrivals(scroll){
  const now=Date.now();st.arrivals=st.arrivals.filter(a=>now-a.at<TTL);
  if(!st.arrivals.length||modalOpen()||doc.hidden)return;const view=currentView();
  st.arrivals.slice().forEach(a=>{if(a.view!==view||st.arrivals.indexOf(a)<0)return;const el=a.find();if(!el)return;
   st.arrivals=st.arrivals.filter(x=>x!==a);arrive(el,{from:a.from,text:a.text,scroll:scroll&&now-a.at<FRESH});});
 }
 // Highlight the item and say where it came from. The mark is keyed to the item,
 // not the element, so a refresh that redraws the list keeps it until it fades.
 function arrive(el,opts){
  if(!el||!el.isConnected)return;opts=opts||{};
  const id=approvalIdOf(el),cid=el.matches('tr[data-cid]')?el.dataset.cid:'',now=Date.now();
  if(id)st.arrivals=st.arrivals.filter(a=>!(a.view==='approvals'&&a.id===id));
  const find=id?()=>approvalCard(id):cid?()=>doc.querySelector('#snapshot .crow[data-cid="'+esc(cid)+'"]'):()=>el.isConnected?el:null;
  st.marks=st.marks.filter(m=>m.find()!==el);
  st.marks.push({find:find,text:opts.text||(opts.from?'from '+opts.from:'New'),hold:now+HOLD_MS,until:now+CUE_MS});
  // Anything that scrolls this item into view should stop below the sticky top bar.
  const bar=doc.querySelector('.topbar');if(bar)doc.documentElement.style.setProperty('--bf-top',Math.round(bar.getBoundingClientRect().height)+'px');
  el.classList.remove('bf-held');flash(el,'bf-arrived',HOLD_MS);tagOn(el,st.marks[st.marks.length-1].text,true);watch();
  if(opts.scroll&&!onScreen(el))el.scrollIntoView({block:'center',behavior:reduced()?'auto':'smooth'});
 }
 function tagOn(el,text,fresh){
  const slot=el.matches('tr')?el.querySelector('td'):el.querySelector('.draft__top .me,.me')||el;if(!slot)return;
  let tag=slot.querySelector('.bf-from');if(tag&&fresh){tag.remove();tag=null;}
  if(!tag){tag=doc.createElement('span');tag.className='bf-from'+(fresh?'':' bf-still');slot.appendChild(tag);}
  if(tag.textContent!==text)tag.textContent=text;
 }
 // Re-apply marks to items a refresh redrew, and fade each one out when its time is up.
 function sweepMarks(){
  const now=Date.now();
  st.marks=st.marks.filter(m=>{const el=m.find();
   if(now>=m.until){if(el){el.classList.remove('bf-held');const tag=el.querySelector('.bf-from');if(tag){tag.classList.add('bf-out');setTimeout(()=>tag.remove(),300);}}return false;}
   if(el){if(!el.querySelector('.bf-from'))tagOn(el,m.text,false);el.classList.toggle('bf-held',now<m.hold&&!el.classList.contains('bf-arrived'));}
   return true;});
 }
 // The item leaves its list: fold it away rather than letting the refresh cut it out.
 function leave(el){
  if(!el||!el.isConnected||el.classList.contains('bf-leaving'))return;el.classList.add('bf-leaving');
  if(!canAnimate(el)){el.hidden=true;return;}
  const cs=root.getComputedStyle(el);el.style.overflow='hidden';
  el.animate([{opacity:1,height:el.offsetHeight+'px',marginTop:cs.marginTop,marginBottom:cs.marginBottom,paddingTop:cs.paddingTop,paddingBottom:cs.paddingBottom},{opacity:0,height:'0px',marginTop:'0px',marginBottom:'0px',paddingTop:'0px',paddingBottom:'0px',borderTopWidth:'0px',borderBottomWidth:'0px'}],{duration:260,easing:'cubic-bezier(.4,0,.2,1)',fill:'forwards'});
 }

 /* ---- hand-offs ---------------------------------------------------------- */
 // Send an item to another tab: ghost to the tab, acknowledge it there, and
 // mark the item when the operator arrives. `opts.id` or `opts.find` names it.
 function send(from,view,opts){
  opts=opts||{};if(!VIEWS[view])return;if(opts.id!=null||opts.find)expect(view,opts);
  const here=currentView()===view,target=here?null:navTarget(view);if(!here)dot(view,true);
  const src=from&&from.isConnected?from:null;
  fly(src,target,opts.label||labelOf(src)).then(()=>{if(target)pulse(target.querySelector('.badge')||target);});
  if(here)later(()=>tryArrivals(false));
 }
 function pendingItem(id){const d=dash();if(!d||id==null)return null;return (d.pending||[]).concat(d.stuck||[]).find(a=>String(a.id)===String(id))||null;}
 // The existing campaign a draft changes, read from the same fields the server matches on.
 function campaignOf(item){const p=item&&item.payload||{};return item&&((p.versionChange||{}).campaignId||(p.versionGuard||{}).campaignId||item.campaignId||p.campaignId||(p.meta||{}).existingCampaignId||(p.groupSplitGuard||{}).campaignId||(p.groupActivationGuard||{}).campaignId)||null;}
 function nameOf(item){const ops=item&&item.payload&&item.payload.mutateOperations||[];for(const op of ops){const c=op&&op.campaignOperation&&op.campaignOperation.create;if(c&&c.name)return String(c.name).trim();}return '';}
 // A reviewed draft reached Google: it now lives in Overview's campaign list.
 // `result` is the publish response when the caller has one; a validation-only run published nothing.
 function published(src,id,result){
  if(result&&result.status!=='APPLIED')return;
  const card=approvalCard(id)||(src&&src.isConnected&&src.closest?src.closest('.draft,[data-approval-id]')||src:null);
  const item=pendingItem(id),cid=result&&result.publishedCampaignId?String(result.publishedCampaignId):campaignOf(item),name=cid?'':nameOf(item),since=Date.now()-EARLY;
  const existed=scanRows().some(r=>!!cid&&r.dataset.cid===String(cid));
  send(card,'command',{label:labelOf(card)||'Campaign',text:existed?'Just updated':'Just published',find:()=>campaignRow(cid,since,name)});
 }

 /* ---- the journey header on Opportunities -------------------------------- */
 const STEPS=[['research','Research'],['create','Create'],['review','Review'],['measure','Measure'],['learn','Learn']];
 function journeyEl(){
  const j=doc.querySelector('#v-bench .oppJourney');if(!j||j.dataset.bfJourney)return j;
  j.dataset.bfJourney='1';j.classList.add('bf-journey');j.setAttribute('role','navigation');j.setAttribute('aria-label','Campaign journey');
  j.innerHTML=STEPS.map((s,i)=>(i?'<i aria-hidden="true"></i>':'')+'<button type="button" data-journey="'+s[0]+'"><span>'+s[1]+'</span><small><strong></strong><em></em></small></button>').join('');
  j.addEventListener('click',e=>{const b=e.target.closest&&e.target.closest('[data-journey]');if(b)journeyGo(b.dataset.journey);});
  return j;
 }
 function lane(){const b=doc.getElementById('v-bench');return b&&b.dataset.lane||'search';}
 // Each step reads the same state its own tab shows, so the header never disagrees with it.
 function journeyState(){
  const l=lane(),channel=l==='product'?'pmax':'search',d=dash(),s={};
  const r=call('researchState',channel)||{};
  s.research={text:{ready:'Up to date',running:'Scanning',stale:'Refresh due',missing:'Not run yet',partial:'Check sources',error:'Check sources'}[r.status]||'',attn:['stale','missing','partial','error'].includes(r.status),busy:r.status==='running'};
  const ideas=((channel==='pmax'?root.PMAXOPPS:root.OPPS)||[]).filter(o=>o&&!o.acted).length,drafting=doc.querySelectorAll('#v-bench .opGen.is-busy,#v-bench [data-working="true"]').length;
  s.create=drafting?{text:'Drafting',busy:true}:{n:ideas,text:ideas===1?'idea':'ideas'};
  const waiting=(call('pendingInScope')||[]).length;s.review=waiting?{n:waiting,text:'waiting',attn:true}:{text:'Clear'};
  const live=d?(d.lastMetrics||[]).filter(c=>c&&c.status==='ENABLED').length:null;s.measure=live==null?{text:''}:live?{n:live,text:'live'}:{text:'None live'};
  const pb=root.PB,lessons=pb&&Array.isArray(pb.lessons)?pb.lessons.length:null;s.learn=lessons==null?{text:''}:lessons?{n:lessons,text:lessons===1?'lesson':'lessons'}:{text:'None yet'};
  ({search:['research','create'],product:['research','create'],studio:['create'],learning:['learn']}[l]||[]).forEach(k=>{s[k].here=true;});
  return s;
 }
 function refreshJourney(){
  const j=journeyEl();if(!j)return;const s=journeyState();
  j.querySelectorAll('[data-journey]').forEach(b=>{
   const v=s[b.dataset.journey]||{},num=b.querySelector('strong'),text=b.querySelector('em'),n=v.n==null?'':String(v.n),old=num.dataset.bfValue;
   b.toggleAttribute('data-here',!!v.here);b.toggleAttribute('data-attn',!!v.attn);b.toggleAttribute('data-busy',!!v.busy);
   if(v.here)b.setAttribute('aria-current','step');else b.removeAttribute('aria-current');
   if(n===''){num.textContent='';delete num.dataset.bfValue;}
   else if(old!==n){if(old==null){num.textContent=n;num.dataset.bfValue=n;}else{tick(num,Number(n)>Number(old));count(num,Number(n),{from:Number(old)});}}
   if(text.textContent!==(v.text||''))text.textContent=v.text||'';
   b.title=b.firstChild.textContent+(n?' · '+n+' '+v.text:v.text?' · '+v.text:'');
  });
 }
 function scheduleJourney(){if(st.frame)return;st.frame=1;later(()=>{st.frame=0;refreshJourney();});}
 // Scroll the place into view and ring its heading or control, never a whole section.
 function reveal(el,mark){if(!el)return;el.scrollIntoView({block:'start',behavior:reduced()?'auto':'smooth'});pulse(mark||el.querySelector('h3,h4,.card__h')||el);}
 // Each step opens the place where that step happens.
 function journeyGo(key){
  if(key==='review'){call('go','approvals');return;}
  if(key==='measure'){call('go','command');const list=doc.getElementById('snapshot');const card=list&&list.closest('.card');reveal(card||list,card&&card.querySelector('.card__h'));return;}
  if(key==='learn'){if(lane()!=='learning')call('growthLane','learning');reveal(doc.getElementById('growthLearning'));return;}
  if(lane()!=='search'&&lane()!=='product')call('growthLane','search');
  const product=lane()==='product';
  if(key==='research'){reveal(doc.getElementById('growthStore')||doc.getElementById('oppList'),call('researchNeedsRefresh',product?'pmax':'search')?doc.getElementById('oppScan'):doc.querySelector('.oppStoreStatus'));return;}
  const create=doc.querySelector(product?'#pmaxSec .pmx-gen:not([disabled])':'#oppCards .opGen:not([disabled])');
  reveal(create?create.closest('.oppCard,.pmxrow')||create:doc.getElementById(product?'pmaxSec':'oppList'),create);
 }

 /* ---- hooks called by the console ----------------------------------------- */
 // After go(k): clear that tab's dot, ease the view in, and play what arrived there.
 function onGo(view){
  if(!VIEWS[view])return;dot(view,false);
  const v=doc.getElementById('v-'+view);if(v&&st.view&&st.view!==view&&!reduced())flash(v,'bf-enter',400);st.view=view;
  if(view==='bench')scheduleJourney();
  later(()=>tryArrivals(true));
 }
 // After updateBadges(): roll the Approvals count and notice drafts that just arrived.
 // Only a draft that was not listed before is news: not the first load, not a change of product group.
 function onBadges(button,n){
  const d=dash();if(!d){scheduleJourney();return;}
  const prev=st.badge,badge=button&&button.querySelector('.badge'),known=st.ids;st.badge=n;
  const ids=new Set((d.pending||[]).concat(d.stuck||[]).map(a=>String(a.id)));st.ids=ids;
  if(prev!=null&&n!==prev&&badge)tick(badge,n>prev);
  if(known){
   ids.forEach(id=>{if(!known.has(id))expect('approvals',{id:id});});
   const counted=(call('pendingInScope')||[]).some(a=>!known.has(String(a.id)));
   if(counted){if(badge)pulse(badge);if(currentView()!=='approvals')dot('approvals',true);}
   // A draft that was listed and is gone (published or deleted) has nothing left to mark.
   st.arrivals=st.arrivals.filter(a=>a.view!=='approvals'||!a.id||ids.has(a.id)||!known.has(a.id));
  }
  sweepMarks();setTimeout(()=>tryArrivals(false),0);scheduleJourney();
 }
 // After buildNav(): the rail was rebuilt, so repaint its dots.
 function onNav(){paintDots();scheduleJourney();}

 function init(){
  journeyEl();refreshJourney();
  const bench=doc.getElementById('v-bench'),snapshot=doc.getElementById('snapshot');
  if(root.MutationObserver){
   if(bench)new root.MutationObserver(list=>{if(list.some(m=>!(m.target.closest&&m.target.closest('.oppJourney'))))scheduleJourney();}).observe(bench,{childList:true,subtree:true,attributes:true,attributeFilter:['data-lane']});
   if(snapshot)new root.MutationObserver(()=>later(()=>{scanRows();sweepMarks();})).observe(snapshot,{childList:true,subtree:true});
  }
  // A dialog that closes may uncover an item that arrived underneath it.
  doc.addEventListener('close',()=>setTimeout(()=>tryArrivals(false),30),true);
 }
 root.BritesFlow={send:send,expect:expect,arrive:arrive,leave:leave,published:published,fly:fly,pulse:pulse,tick:tick,count:count,dot:dot,
  go:onGo,badge:onBadges,nav:onNav,journey:refreshJourney,_state:st};
 if(doc.readyState==='loading')doc.addEventListener('DOMContentLoaded',init);else init();
})(window);
