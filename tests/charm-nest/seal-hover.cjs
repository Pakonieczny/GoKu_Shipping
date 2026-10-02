// The shared seal zoom (Seal.zoom, charm-nest-motion.js), with a clock and pointer hit testing. A seal that is rested on for 750 ms, reached with
// Tab or tapped grows where it stands; there is no second seal, no bubble, no tooltip. No production data is written.
const assert=require('node:assert/strict'),fs=require('node:fs'),{JSDOM}=require('jsdom');
const dom=new JSDOM('<span class="sealRow"></span><p id="outside">Work area</p><dialog id="dialog"><span class="sealRow"></span></dialog>',{url:'http://127.0.0.1',runScripts:'outside-only',pretendToBeVisual:true}),w=dom.window,d=w.document;
let now=0,id=0,hit=null,visibility='visible';const timers=new Map(),anims=[];
w.setTimeout=(fn,ms=0)=>{timers.set(++id,{fn,at:now+ms});return id;};w.clearTimeout=id=>timers.delete(id);
w.setInterval=(fn,ms)=>{timers.set(++id,{fn,at:now+ms,interval:ms});return id;};w.clearInterval=id=>timers.delete(id);
w.matchMedia=()=>({matches:false});w.Element.prototype.getAnimations=()=>[];
w.Element.prototype.animate=function(frames,o){anims.push({el:this,frames,o});return {finished:Promise.resolve(),cancel(){},playState:'finished'};};
Object.defineProperty(d,'visibilityState',{get:()=>visibility});d.elementFromPoint=()=>hit;
// (a seal is as wide as the size it was fitted to: --seal-fit, else the shared 84)
const sizeOf=el=>parseFloat(el.style.getPropertyValue('--seal-fit'))||84;
Object.defineProperty(w.HTMLElement.prototype,'offsetWidth',{get(){return sizeOf(this);}});
Object.defineProperty(w.HTMLElement.prototype,'offsetHeight',{get(){return sizeOf(this);}});
w.eval(fs.readFileSync('charm-nest-motion.js','utf8'));
const Seal=w.Seal,DELAY=Seal.zoom.DELAY;
const at=Date.UTC(2026,9,1,0,44),outside=d.querySelector('#outside'),zoomed=()=>d.querySelector('[data-seal-zoom]');
function rectFor(seal,x=410,y=560,size=sizeOf(seal)){const rect={left:x,right:x+size,top:y,bottom:y+size,width:size,height:size};seal.getBoundingClientRect=()=>rect;seal.getClientRects=()=>[rect];return rect;}
// (a pointerout carries where the pointer has gone to: the node it went to, else outside the page)
function point(type,node,relatedTarget=null){
  if(['pointerover','pointermove','pointerdown','click'].includes(type))hit=node;if(type==='pointerout')hit=relatedTarget;
  const r=(type==='pointerout'?relatedTarget&&relatedTarget.getBoundingClientRect():node.getBoundingClientRect())||{left:-9,width:0,top:-9,height:0};
  node.dispatchEvent(new w.MouseEvent(type,{bubbles:true,relatedTarget,clientX:r.left+r.width/2,clientY:r.top+r.height/2}));
}
async function advance(ms){const end=now+ms;for(;;){const next=[...timers.entries()].filter(([,t])=>t.at<=end).sort((a,b)=>a[1].at-b[1].at)[0];if(!next)break;now=next[1].at;if(next[1].interval)next[1].at+=next[1].interval;else timers.delete(next[0]);next[1].fn();for(let i=0;i<6;i++)await Promise.resolve();}now=end;for(let i=0;i<6;i++)await Promise.resolve();}
async function closed(label){await advance(0);assert.equal(zoomed(),null,label);}
const noSecondSeal=label=>assert.equal(d.querySelectorAll('.sealLens,.tlLoupe,.tlNowZoom,[data-seal-caption],.seal[title]').length,0,label+': no lens, loupe, caption or tooltip is ever made');
const inView=(r,label)=>assert(r&&r.left>=0&&r.top>=0&&r.right<=w.innerWidth&&r.bottom<=w.innerHeight,label+' stays in the view '+JSON.stringify(r));
async function exercise(seal,label){
 const history=seal.innerHTML,size=sizeOf(seal),k=Seal.zoomScale(size);
 point('pointerover',seal);await advance(DELAY-50);assert.equal(zoomed(),null,label+' waits through a pass shorter than '+DELAY+' ms');
 assert(!seal.hasAttribute('title'),label+' has no native tooltip');point('pointerout',seal,outside);await advance(2000);await closed(label+' cancels on departure');
 point('pointerover',seal);await advance(DELAY);assert.equal(zoomed(),seal,label+' grows itself once rested on for '+DELAY+' ms');
 assert.equal(seal.dataset.sealZoom,k.toFixed(2),label+' grows by the one curve for its '+size+'px size');
 const grow=anims.filter(a=>a.el===seal).at(-1);assert(grow&&new RegExp('^matrix\\('+k+',0,0,'+k+',').test(grow.frames[1].transform),label+' is moved by transform alone: '+(grow&&grow.frames[1].transform));
 assert.deepEqual(Object.keys(grow.frames[1]).sort(),['filter','opacity','transform'],label+' moves only transform, filter and opacity');
 assert.match(seal.getAttribute('aria-label')||'',/Paul/,label+' names its signer for assistive technology, not in a bubble');
 noSecondSeal(label);inView(Seal.zoom.rectOf(seal),label);
 point('pointerout',seal,outside);await closed(label+' goes back at once on departure');
 point('pointerover',seal);await advance(DELAY-100);point('pointermove',outside);await advance(2000);await closed(label+' cancels without pointerout');
 point('pointerover',seal);await advance(DELAY);point('pointermove',outside);await closed(label+' movement away puts it back');
 for(const event of ['blur','scroll','resize']){
  point('pointerover',seal);await advance(DELAY-100);w.dispatchEvent(new w.Event(event));await advance(2000);await closed(label+' a pending zoom cannot survive '+event);
  point('pointerover',seal);await advance(DELAY);assert(zoomed());w.dispatchEvent(new w.Event(event));await closed(label+' a zoomed seal goes back on '+event);
 }
 point('pointerover',seal);await advance(DELAY-100);visibility='hidden';d.dispatchEvent(new w.Event('visibilitychange'));await advance(2000);await closed(label+' cannot zoom in a hidden tab');visibility='visible';
 assert.equal(seal.innerHTML,history,label+' keeps the historical face unchanged');
}
(async()=>{try{
 assert.equal(DELAY,750,'one named hover delay of 750 ms');
 // A signed-in viewer is intentionally different from the employee stored on the old stamp.
 w.employee='Current Viewer';w.localStorage.setItem('cn.employee','Current Viewer');
 const row=d.querySelector('.sealRow');row.innerHTML=Seal.html({how:'engraveApproved',at,by:'Paul'},56)+Seal.html({how:'engraveApproved',at:at+1000,by:'Seth'},56);
 const [first,second]=row.querySelectorAll('.seal');rectFor(first);rectFor(second,506);
 point('pointerover',first);await advance(600);point('pointerout',first,second);point('pointerover',second,first);await advance(DELAY-100);assert.equal(zoomed(),null,'adjacent seals each get a full delay');
 await advance(100);assert.equal(zoomed(),second);assert.match(second.getAttribute('aria-label'),/Seth/);assert.doesNotMatch(second.getAttribute('aria-label'),/Current Viewer/,'an old seal never acquires the current viewer name');point('pointerout',second,outside);await closed('adjacent zoom closes');
 const child=first.querySelector('svg');child.getBoundingClientRect=first.getBoundingClientRect;
 point('pointerover',first);await advance(600);point('pointerout',first,child);point('pointerover',child,first);await advance(DELAY-600+50);assert.equal(zoomed(),first,'moving between seal descendants does not restart its rest');point('pointerout',child,outside);await closed('leaving an SVG descendant puts it back');
 point('pointerover',first);await advance(DELAY-100);hit=outside;await advance(1000);await closed('silent hit-test departure cannot start a pending zoom');
 // the keyboard: Tab (:focus-visible) zooms at once, Esc puts it back; a pointer leaving ends even a focused seal's zoom
 first.focus();assert.equal(zoomed(),first,'keyboard focus zooms a seal at once');point('pointerover',first);point('pointerout',first,outside);await closed('mouse departure puts back even a focused seal');
 first.blur();
 // a click away from any button zooms it at once; a press elsewhere puts it back
 point('click',first);assert.equal(zoomed(),first,'a direct click zooms it at once');point('pointerdown',outside);await closed('an outside press puts a clicked seal back');
 point('pointerover',first);await advance(DELAY);point('click',first);point('pointerout',first,outside);await closed('a click cannot keep a mouse zoom after departure');
 d.dispatchEvent(new w.KeyboardEvent('keydown',{key:'Tab',bubbles:true}));second.focus();assert.equal(zoomed(),second,'second seal stays keyboard accessible');w.dispatchEvent(new w.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));await closed('Escape puts a keyboard zoom back');
 second.blur();
 for(const [how,scope] of [['print','sheet'],['button','sheet'],['laserReady','sheet'],['laserReady','set'],['laserDone','sheet'],['laserDone','set'],['engraveApproved','sheet'],['engravePlain','sheet']])for(const legacySize of [34,56,84,112]){
  const host=d.createElement('span');host.className='sealRow';host.innerHTML=Seal.html({how,scope,at,by:'Paul'},legacySize);d.body.append(host);const seal=host.querySelector('.seal');rectFor(seal);
  assert.doesNotMatch(seal.querySelector('svg').textContent,/Paul|Signed by/,how+' reserves its signer for assistive text');
  await exercise(seal,how+' '+scope+' requested '+legacySize);host.remove();
 }
 // The shared rendering API also supplies timeline families beyond print/engraving/laser.
 const designs=[['received','ORDER RECEIVED'],['prepared','ON SHEET'],['engraving','BACK ENGRAVING'],['laser','LASER CUT'],['finishing','ASSEMBLED'],['fulfilment','SHIPPED'],['exceptions','ON HOLD'],['cancelled','CANCELLED']];
 const outlines=new Set();
 for(const [family,action]of designs){
  const model={family,action,date:'30 SEP 2026',time:'9:44 PM',by:'Paul',at},host=d.createElement('span');host.className='sealRow';host.innerHTML='<span class="seal" tabindex="0" role="img" aria-label="'+action+' by Paul">'+Seal.face(model)+'</span>';d.body.append(host);
  const seal=host.firstElementChild;rectFor(seal);const svg=seal.querySelector('svg'),texts=[...svg.querySelectorAll('text')].map(t=>t.textContent);
  assert.equal(svg.dataset.sealFamily,family);assert(!texts.some(t=>/Paul|Signed by/.test(t)),'regular '+family+' has no signer');assert(texts.includes('30 SEP 2026')&&texts.includes('9:44 PM'),'regular '+family+' displays its exact date and time');
  if(family==='engraving')assert.deepEqual(texts.slice(0,2),['BACK','ENGRAVING'],'engraving uses the approved two-line label');
  outlines.add(svg.querySelector('g>path').getAttribute('d'));await exercise(seal,'shared family '+family);host.remove();
 }
 assert.equal(outlines.size,8,'all eight families have distinguishable outlines');
 // The adaptive system: whatever the size a seal is fitted to, the smaller it is the more it grows, by one curve.
 const ladder=[];
 for(const size of [140,112,84,56,40,30,24]){
  const host=d.createElement('span');host.className='sealRow';host.innerHTML=Seal.html({how:'print',at,by:'Paul'});d.body.append(host);const seal=host.firstElementChild;seal.style.setProperty('--seal-fit',size+'px');rectFor(seal,300,300,size);
  point('pointerover',seal);await advance(DELAY);assert.equal(zoomed(),seal,size+'px seal zooms');ladder.push(+seal.dataset.sealZoom);
  assert.equal(seal.dataset.sealZoom,Seal.zoomScale(size).toFixed(2),size+'px takes its size\'s scale');point('pointerout',seal,outside);await closed(size+'px puts back');host.remove();
 }
 assert(ladder.every((k,i)=>i===0||k>ladder[i-1]),'smaller seals zoom more: '+ladder.join(' < '));
 assert(ladder[0]<=1.2&&ladder.at(-1)>=3.5,'a large seal grows a little, a tiny one a lot: '+ladder.join(', '));
 w.eval(fs.readFileSync('charm-nest-readiness.js','utf8'));const R=w.CharmNestReadiness;for(const ready of [true,false]){
  assert.equal(R.seal({ready}),'','readiness has no unearned zoom target');
 }
 const signedReadiness=d.createElement('div');signedReadiness.innerHTML=Seal.html({how:'laserReady',at,by:'Seth Signed'});d.body.append(signedReadiness);const readinessSeal=signedReadiness.querySelector('.seal');rectFor(readinessSeal);
 assert.doesNotMatch(readinessSeal.querySelector('svg').textContent,/Seth Signed|Signed by/,'recorded laser readiness keeps its signer off the regular face');assert.match(readinessSeal.getAttribute('aria-label'),/Seth Signed/,'recorded laser readiness names its saved employee for assistive technology');
 point('pointerover',readinessSeal);await advance(DELAY);assert.equal(zoomed(),readinessSeal);point('pointerout',readinessSeal,outside);await closed('signed readiness closes');signedReadiness.remove();
 const dialog=d.querySelector('dialog');dialog.querySelector('.sealRow').innerHTML=Seal.html({how:'laserReady',at,by:'Paul'},84);const dialogSeal=dialog.querySelector('.seal');rectFor(dialogSeal);
 point('pointerover',dialogSeal);await advance(DELAY-100);dialog.dispatchEvent(new w.Event('close'));await advance(2000);await closed('closing a dialog cancels its pending seal zoom');
 // A pending seal (waiting for its stamp) and anything in a ghost or inert box is not a target.
 const waiting=d.createElement('span');waiting.className='sealRow';waiting.innerHTML=Seal.html({how:'print',at,by:'Paul'},84,'pending');d.body.append(waiting);const pend=waiting.firstElementChild;rectFor(pend);
 point('pointerover',pend);await advance(DELAY+500);assert.equal(zoomed(),null,'a seal still waiting for its stamp does not zoom');waiting.remove();
 // A redraw may recreate identical historical markup, but it must earn a new hover rest.
 point('pointerover',first);await advance(DELAY);assert.equal(zoomed(),first);const redraw=d.createElement('span');redraw.innerHTML=Seal.html({how:'engraveApproved',at,by:'Paul'},56);const replacement=redraw.firstElementChild;rectFor(replacement);first.replaceWith(replacement);hit=replacement;await advance(250);await closed('redrawing a seal does not retain or resurrect its old zoom');
 point('pointerover',replacement);await advance(DELAY-100);assert.equal(zoomed(),null,'a replacement earns its own delay');point('pointerout',replacement,outside);await advance(2000);
 // Viewport edges: the seal grows where it stands, nudged just enough to stay wholly in the view (never off the screen).
 const edgeHost=d.createElement('span');edgeHost.className='sealRow';edgeHost.innerHTML=Seal.html({how:'engraveApproved',at,by:'Paul'},84);d.body.append(edgeHost);const edgeSeal=edgeHost.firstElementChild;
 for(const [x,y,height]of [[8,20,768],[900,20,768],[410,200,400],[8,200,400],[900,200,400],[8,300,400]]){
  w.innerHeight=height;rectFor(edgeSeal,x,y);point('pointerover',edgeSeal);await advance(DELAY);assert.equal(zoomed(),edgeSeal,'a seal at the edge still zooms');
  const z=Seal.zoom.rectOf(edgeSeal),r=edgeSeal.getBoundingClientRect(),cx=(r.left+r.right)/2,cy=(r.top+r.bottom)/2;
  inView(z,'in a '+height+'px view at '+x+','+y);assert(z.left<=cx&&z.right>=cx&&z.top<=cy&&z.bottom>=cy,'the grown seal still covers the place the pointer rests on, at '+x+','+y);
  noSecondSeal('edge '+x+','+y);point('pointerout',edgeSeal,outside);await closed('viewport-edge departure puts it back');
 }
 edgeHost.remove();
 console.log('PASS: 8 seal/scope variants across 4 legacy size requests, 8 shared designs, no unearned readiness seals and preserved saved readiness actor; one adaptive curve (smaller seals zoom more), 750 ms rest, no lens, caption or tooltip, signer for assistive text only, strict exit, descendant hit testing, scroll/resize/blur/tab/dialog/redraw dismissal, keyboard and click access, viewport edges');
}finally{w.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
