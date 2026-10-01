const assert=require('node:assert/strict'),fs=require('node:fs'),{JSDOM}=require('jsdom');
const dom=new JSDOM('<main id="timeline"></main><section id="overview"></section><section id="recent"></section><p id="outside">Work area</p>',{runScripts:'outside-only',pretendToBeVisual:true}),w=dom.window,d=w.document;
let now=0,id=0;const timers=new Map();
w.setTimeout=(fn,ms)=>{timers.set(++id,{fn,at:now+ms});return id;};w.clearTimeout=id=>timers.delete(id);w.setInterval=()=>++id;w.clearInterval=()=>{};
w.matchMedia=()=>({matches:false});w.Element.prototype.getAnimations=()=>[];w.Element.prototype.animate=()=>({finished:Promise.resolve(),cancel(){},playState:'finished'});
Object.defineProperty(w.HTMLElement.prototype,'offsetWidth',{get(){return this.classList.contains('tlExp')?272:this.classList.contains('tlLoupe')?122:34;}});
Object.defineProperty(w.HTMLElement.prototype,'offsetHeight',{get(){return this.classList.contains('tlExp')?160:this.classList.contains('tlLoupe')?122:34;}});
w.eval(fs.readFileSync('charm-nest-timeline-ui.js','utf8'));
const T=w.OrderTimelineUI,orderId='4175254511',at=Date.UTC(2026,9,1),types=Object.keys(T.KIND).filter(type=>T.sealed({type}));
const events=types.map((type,index)=>({id:orderId+'-'+index,orderId,type,at:at+index*60000,by:'Paul',source:'sorter',sheetId:'sheet1',sheet:'14K Sheet 1'}));
w.OrderTimeline={get:async()=>({events,cancelled:null,where:null})};
const outside=d.querySelector('#outside'),point=(type,node,relatedTarget=null)=>node.dispatchEvent(new w.MouseEvent(type,{bubbles:true,relatedTarget}));
async function advance(ms){const end=now+ms;for(;;){const next=[...timers.entries()].filter(([,t])=>t.at<=end).sort((a,b)=>a[1].at-b[1].at)[0];if(!next)break;now=next[1].at;timers.delete(next[0]);next[1].fn();await Promise.resolve();}now=end;for(let i=0;i<10;i++)await Promise.resolve();}
const visible=e=>e&&w.getComputedStyle(e).display!=='none';
async function exercise(seal,shown,label){
 point('pointerover',seal);await advance(1000);assert(!shown(),label+' cannot open on a quick pass');point('pointerout',seal,outside);await advance(2000);assert(!shown(),label+' cancels its timer');
 point('pointerover',seal);await advance(1100);assert(shown(),label+' opens after resting');point('pointerout',seal,outside);await advance(0);assert(!shown(),label+' closes on departure');
 point('pointerover',seal);await advance(900);point('pointermove',outside);await advance(2000);assert(!shown(),label+' cancels without pointerout');
 point('pointerover',seal);await advance(1100);assert(shown());point('pointermove',outside);await advance(0);assert(!shown(),label+' movement dismisses an open preview');
 point('pointerover',seal);await advance(900);w.dispatchEvent(new w.Event('blur'));await advance(2000);assert(!shown(),label+' cannot open after window blur');
}
(async()=>{let timeline;try{
 timeline=T.mount(d.querySelector('#timeline'),{orderId,live:false});await advance(0);
 const box=d.querySelector('.tlUI'),loupe=()=>visible(box.querySelector('.tlLoupe')),exp=()=>box.querySelector('.tlExp').classList.contains('on');
 const seals=[...box.querySelectorAll('.tlSt[data-key]')];assert(seals.length>15,'real chronology renders all milestone, process, print and action seal families');
 for(const seal of seals)await exercise(seal,()=>loupe()||exp(),'timeline '+seal.dataset.key);
 const rail=box.querySelector('.tlStop.d .tlSeal');assert(rail);await exercise(rail,()=>loupe()||exp(),'compact/milestone rail seal');
 point('pointerover',rail.parentElement.querySelector('span'));await advance(2000);assert(!loupe()&&!exp(),'a rail label cannot activate the seal');
 point('pointerover',seals[0]);await advance(1100);assert(loupe());point('pointerout',seals[0],seals[1]);point('pointerover',seals[1],seals[0]);await advance(1000);assert(!loupe()&&!exp(),'adjacent timeline seals wait again');await advance(100);assert(loupe());point('pointerout',seals[1],outside);await advance(0);
 seals[0].focus();assert(loupe(),'timeline keyboard focus remains readable');point('pointermove',outside);await advance(0);assert(!loupe()&&!exp(),'focus cannot hold a mouse preview open');
 const overview=d.querySelector('#overview'),story=events.filter(e=>['arrived','placed','engraveApproved','laserDone','sorted'].includes(e.type));overview.innerHTML=T.nowStamps(story,{}).seal;let opened=null;
 T.explainOn(overview,()=>({events:story}),x=>opened=x);let seal=overview.querySelector('.tlNowSeal'),shown=()=>overview._tlExp.classList.contains('on');
 await exercise(seal,shown,'overview current seal');point('pointerover',seal);await advance(1100);assert(shown());point('pointerout',seal,overview._tlExp);await advance(0);assert(!shown(),'moving onto the enlarged card cannot keep the seal hover open');
 point('click',seal);assert(opened?.stage,'the original seal still opens its step on the Timeline');assert(!shown(),'clicking does not pin a floating hover');
 overview.innerHTML=T.nowStamps(story,{cancelled:{at,by:'Seth'}}).seal;seal=overview.querySelector('.tlNowSeal.cx');await exercise(seal,shown,'cancelled order seal');
 const recent=d.querySelector('#recent'),e=story[0];recent.innerHTML='<button class="tlMini" data-tl-ev="'+e.id+'"><span class="s">'+T.stampSvg(e,false)+'</span><span class="f">'+T.stampSvg(e,true)+'</span></button>';T.wireNow(recent,x=>opened=x);
 const mini=recent.querySelector('.tlMini'),zoom=()=>visible(recent.querySelector('.tlNowZoom'));await exercise(mini,zoom,'recent mini seal');
 mini.focus();assert(zoom(),'mini keyboard access remains');point('pointermove',outside);await advance(0);assert(!zoom(),'a focused mini seal does not stay open after mouse departure');
 point('pointerover',mini);await advance(900);mini.remove();await advance(2000);assert(!zoom(),'redrawing cannot resurrect a removed seal');
 point('pointerover',seals[0]);await advance(900);timeline.destroy();timeline=null;await advance(2000);assert(!d.querySelector('.tlUI'),'destroy cancels pending seal previews');
 console.log('PASS: '+seals.length+' timeline seal variants, rail, current/cancelled overview and recent mini seals share 1.1-second rest, cancellation, neighbour delay and strict original-seal departure; keyboard and Timeline navigation remain');
}finally{timeline?.destroy();w.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
