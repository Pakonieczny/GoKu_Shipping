// Every real timeline seal surface uses the shared face and the same delayed hover (500 ms): the seal itself grows where it stands (Seal.zoom), with no
// second seal, loupe or bubble; the step's explainer card is the only thing that opens beside it.
const assert=require('node:assert/strict'),fs=require('node:fs'),{JSDOM}=require('jsdom');
const dom=new JSDOM('<main id="timeline"></main><section id="overview"></section><section id="recent"></section><section id="cancelled"></section><p id="outside">Work area</p>',{url:'http://127.0.0.1',runScripts:'outside-only',pretendToBeVisual:true}),w=dom.window,d=w.document;
let now=0,id=0,hit=null,visibility='visible';const timers=new Map();
w.setTimeout=(fn,ms=0)=>{timers.set(++id,{fn,at:now+ms});return id;};w.clearTimeout=id=>timers.delete(id);w.setInterval=()=>++id;w.clearInterval=()=>{};
w.matchMedia=()=>({matches:false});w.Element.prototype.getAnimations=()=>[];w.Element.prototype.animate=()=>({finished:Promise.resolve(),cancel(){},playState:'finished'});
Object.defineProperty(d,'visibilityState',{get:()=>visibility});d.elementFromPoint=()=>hit;
Object.defineProperty(w.HTMLElement.prototype,'offsetWidth',{get(){return this.classList.contains('tlExp')?272:84;}});
Object.defineProperty(w.HTMLElement.prototype,'offsetHeight',{get(){return this.classList.contains('tlExp')?160:84;}});
w.eval(fs.readFileSync('charm-nest-motion.js','utf8'));w.eval(fs.readFileSync('charm-nest-timeline-ui.js','utf8'));
const DELAY=w.Seal.zoom.DELAY,T=w.OrderTimelineUI,orderId='4175254511',at=Date.UTC(2026,9,1),types=Object.keys(T.KIND).filter(type=>T.sealed({type}));
const events=types.map((type,index)=>({id:orderId+'-'+index,orderId,type,at:at+index*60000,by:'Paul Konieczny',source:'sorter',sheetId:'sheet1',sheet:'14K Sheet 1'}));
const cancellation={at:at+864e5,by:'Seth Signed',source:'sorter',why:'Customer requested cancellation'};
w.OrderTimeline={get:async id=>id==='555'?{events:events.slice(0,3),cancelled:cancellation,where:null}:{events,cancelled:null,where:null}};
w.localStorage.setItem('cn.employee','Current Viewer');
const outside=d.querySelector('#outside'),visible=e=>e&&w.getComputedStyle(e).display!=='none';
function rectFor(seal,x=410,y=560,size=84){const rect={left:x,right:x+size,top:y,bottom:y+size,width:size,height:size};seal.getBoundingClientRect=()=>rect;seal.getClientRects=()=>[rect];return rect;}
// (a pointerout carries where the pointer has gone to: the node it went to, else outside the page)
function point(type,node,relatedTarget=null){if(['pointerover','pointermove','pointerdown','click'].includes(type))hit=node;if(type==='pointerout')hit=relatedTarget;const r=(type==='pointerout'?relatedTarget&&relatedTarget.getBoundingClientRect():node.getBoundingClientRect())||{left:-9,width:0,top:-9,height:0};node.dispatchEvent(new w.MouseEvent(type,{bubbles:true,relatedTarget,clientX:r.left+r.width/2,clientY:r.top+r.height/2}));}
const tab=()=>d.dispatchEvent(new w.KeyboardEvent('keydown',{key:'Tab',bubbles:true})),focusTab=n=>{tab();n.focus();};
const zoomedIn=()=>d.querySelector('[data-seal-zoom]'),noCopy=label=>assert.equal(d.querySelectorAll('.tlLoupe,.tlNowZoom,.sealLens').length,0,label+' makes no second seal, loupe or bubble');
async function advance(ms){const end=now+ms;for(;;){const next=[...timers.entries()].filter(([,t])=>t.at<=end).sort((a,b)=>a[1].at-b[1].at)[0];if(!next)break;now=next[1].at;timers.delete(next[0]);next[1].fn();for(let i=0;i<10;i++)await Promise.resolve();}now=end;for(let i=0;i<10;i++)await Promise.resolve();}
function checkZoom(seal,label,signer){const z=zoomedIn();assert(z,label+' grows where it stands');assert(z===seal||seal.contains(z),label+' is the seal itself that grows (no copy)');assert.equal(d.querySelectorAll('[data-seal-zoom]').length,1,label+': one grown seal at a time');noCopy(label);
 const r=w.Seal.zoom.rectOf(z);assert(r&&r.left>=0&&r.top>=0&&r.right<=w.innerWidth&&r.bottom<=w.innerHeight,label+' stays in the view '+JSON.stringify(r));assert(!z.hasAttribute('title'),label+' has no tooltip');
 if(signer){const named=z.closest('[aria-label]')||seal.closest('[aria-label]'),said=named?named.getAttribute('aria-label'):((seal.closest('button')||seal).textContent||'');assert.match(said,new RegExp(signer.split(' ')[0]),label+' is named with its saved signer for assistive technology ('+said+')');}}
async function exercise(seal,shown,label,signer=null,grows=true){
 rectFor(seal);const history=seal.innerHTML;
 point('pointerover',seal);await advance(300);assert(!shown(),label+' cannot open during a 300 ms pass');point('pointerout',seal,outside);await advance(2000);assert(!shown(),label+' cancels a pending timer');
 point('pointerover',seal);await advance(DELAY);assert(shown(),label+' opens after resting');if(grows){checkZoom(seal,label,signer);assert.doesNotMatch(seal.outerHTML,/Current Viewer/,label+' ignores the current viewer name');}
 point('pointerout',seal,outside);await advance(0);assert(!shown(),label+' closes on departure');
 point('pointerover',seal);await advance(DELAY-100);point('pointermove',outside);await advance(2000);assert(!shown(),label+' cancels without pointerout');
 point('pointerover',seal);await advance(DELAY);assert(shown());point('pointermove',outside);await advance(0);assert(!shown(),label+' movement away puts it back');
 for(const event of ['blur','scroll','resize']){
  point('pointerover',seal);await advance(DELAY-100);w.dispatchEvent(new w.Event(event));await advance(2000);assert(!shown(),label+' cannot appear after '+event);
  point('pointerover',seal);await advance(DELAY);assert(shown());w.dispatchEvent(new w.Event(event));await advance(0);assert(!shown(),label+' open zoom closes after '+event);
 }
 point('pointerover',seal);await advance(DELAY-100);visibility='hidden';d.dispatchEvent(new w.Event('visibilitychange'));await advance(2000);assert(!shown(),label+' cannot open in a hidden tab');visibility='visible';
 assert.equal(seal.innerHTML,history,label+' keeps the historical face unchanged');
}
(async()=>{let timeline,cancelTimeline;try{
 timeline=T.mount(d.querySelector('#timeline'),{orderId,live:false});await advance(0);
 const box=d.querySelector('#timeline .tlUI'),grown=()=>!!box.querySelector('[data-seal-zoom]'),exp=()=>box.querySelector('.tlExp').classList.contains('on'),shown=()=>grown()||exp();
 const seals=[...box.querySelectorAll('.tlSt[data-key]')];assert(seals.length>15,'real chronology renders milestone, process, print and action seals');
 for(const seal of seals){assert.doesNotMatch(seal.querySelector('svg').textContent,/Paul Konieczny|Signed by/,'regular chronology seal reserves its signer for hover');await exercise(seal,shown,'timeline '+seal.dataset.key,'Paul Konieczny');}
 const families=new Set(seals.map(s=>s.querySelector('svg').dataset.sealFamily));assert.equal(families.size,8,'real chronology uses all eight selected seal families');
 const rail=box.querySelector('.tlStop.d .tlSeal');assert(rail);await exercise(rail,shown,'milestone rail','Paul Konieczny');
 point('pointerover',rail.parentElement.querySelector('span'));await advance(2000);assert(!shown(),'a rail label cannot activate its seal');
 point('pointerover',seals[0]);await advance(DELAY);assert(grown());point('pointerout',seals[0],seals[1]);point('pointerover',seals[1],seals[0]);await advance(DELAY-100);assert(!shown(),'adjacent timeline seals earn a new delay');await advance(100);assert(grown());point('pointerout',seals[1],outside);await advance(0);
 focusTab(seals[0]);assert(grown(),'chronology keyboard focus zooms at once');point('pointermove',outside);await advance(0);assert(!shown(),'focus cannot hold a mouse zoom open');
 // The lower detail pane of the Timeline tab (its big seal, "Around this step" seals, the pinned path, the Stamps legend) went on 5 Oct 2026:
 // nothing of it is drawn, and focus() rings a seal on the chart (there is no pane to pin a step in).
 const paneSel='.tlBig,.tlArw,.tlPath2,.tlLegend,.tlChip,.tlDetail';assert(!box.querySelector(paneSel),'the chart carries no detail pane, legend or chip');
 assert(timeline.focus({stage:'arrived'}));await advance(0);assert(!box.querySelector(paneSel),'a focus draws no pane');assert.equal(box.querySelectorAll('.tlSt.sel').length,1,'it rings one seal on the chart');
 assert(!shown(),'and grows nothing');
 // At a viewport edge or in a short view, the seal still grows where it stands, nudged just enough to stay wholly in the view.
 const edgeSeal=box.querySelector('.tlSt[data-key]');
 for(const [x,y,height]of [[8,20,768],[900,20,768],[410,150,300],[8,150,300],[900,150,300]]){
  w.innerHeight=height;rectFor(edgeSeal,x,y);point('pointerover',edgeSeal);await advance(DELAY);assert(grown(),'viewport edge still permits a timeline zoom');
  const z=w.Seal.zoom.rectOf(box.querySelector('[data-seal-zoom]')),r=edgeSeal.getBoundingClientRect();
  assert(z&&z.left>=0&&z.top>=0&&z.right<=w.innerWidth&&z.bottom<=height,'the grown timeline seal remains wholly inside a '+height+'px viewport '+JSON.stringify(z));
  assert(z.left<=(r.left+r.right)/2&&z.right>=(r.left+r.right)/2&&z.top<=(r.top+r.bottom)/2&&z.bottom>=(r.top+r.bottom)/2,'the grown seal still covers its own place');
  point('pointerout',edgeSeal,outside);await advance(0);assert(!shown(),'viewport-edge departure puts the timeline seal back');
 }
 w.innerHeight=768;
 const overview=d.querySelector('#overview'),story=events.filter(e=>['arrived','placed','engraveApproved','laserDone','sorted'].includes(e.type));overview.innerHTML=T.nowStamps(story,{}).seal;let opened=null;
 T.wireNow(overview,x=>opened=x);T.explainOn(overview,()=>({events:story}),x=>opened=x);
 let seal=overview.querySelector('.tlNowSeal'),ovShown=()=>!!overview.querySelector('[data-seal-zoom]')||overview._tlExp.classList.contains('on');
 await exercise(seal,ovShown,'overview current seal','Paul Konieczny');point('pointerover',seal);await advance(DELAY);assert(ovShown());point('pointerout',seal,overview._tlExp);await advance(0);assert(!ovShown(),'moving onto the step card cannot keep hover open');
 point('click',seal);assert(opened?.stage,'the original seal still opens its step on the Timeline');assert(!ovShown(),'clicking does not pin a floating overview hover');
 overview.innerHTML=T.nowStamps(story,{cancelled:cancellation}).seal;T.wireNow(overview,x=>opened=x);seal=overview.querySelector('.tlNowSeal.cx');await exercise(seal,ovShown,'overview cancelled-order seal','Seth Signed');
 const recent=d.querySelector('#recent'),e=story[0];recent.innerHTML='<button class="tlMini" data-tl-ev="'+e.id+'"><span class="s">'+T.stampSvg(e,false)+'</span><span class="f">'+T.stampSvg(e,true,{hover:true})+'</span></button>';T.wireNow(recent,x=>opened=x);
 const mini=recent.querySelector('.tlMini'),zoom=()=>!!recent.querySelector('[data-seal-zoom]');await exercise(mini,zoom,'legacy recent mini seal',null,false);
 focusTab(mini);assert(zoom(),'mini keyboard access remains');point('pointermove',outside);await advance(0);assert(!zoom(),'a focused mini does not stay open after departure');
 point('pointerover',mini);await advance(DELAY-100);mini.remove();await advance(2000);assert(!zoom(),'redrawing cannot resurrect a removed mini seal');
 cancelTimeline=T.mount(d.querySelector('#cancelled'),{orderId:'555',live:false});await advance(0);const cancelBox=d.querySelector('#cancelled .tlUI'),overlay=cancelBox.querySelector('.tlCxStamp');assert(overlay,'a cancelled order has its historical cancellation overlay');
 const cxShown=()=>!!cancelBox.querySelector('[data-seal-zoom]')||cancelBox.querySelector('.tlExp').classList.contains('on');await exercise(overlay,cxShown,'cancelled rail overlay','Seth Signed');
 point('pointerover',seals[0]);await advance(DELAY-100);timeline.destroy();timeline=null;await advance(2000);assert(!d.querySelector('#timeline .tlUI'),'destroy cancels every pending zoom');
 console.log('PASS: '+seals.length+' chronology variants in 8 families, rail, no detail pane or legend, current/cancelled overview, cancellation overlay and legacy mini; the seal itself grows in place (no loupe or copy), edges keep it in view, saved signer in assistive text, 500 ms rest, strict departure/dismissal, redraw safety and keyboard/navigation access');
}finally{timeline?.destroy();cancelTimeline?.destroy();w.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
