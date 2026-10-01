// Exercise actual shared seal handlers, with a clock and pointer hit testing. No production data is written.
const assert=require('node:assert/strict'),fs=require('node:fs'),{JSDOM}=require('jsdom');
const dom=new JSDOM('<span class="sealRow"></span><p id="outside">Work area</p><dialog id="dialog"><span class="sealRow"></span></dialog>',{url:'http://127.0.0.1',runScripts:'outside-only',pretendToBeVisual:true}),w=dom.window,d=w.document;
let now=0,id=0,hit=null,visibility='visible';const timers=new Map();
w.setTimeout=(fn,ms=0)=>{timers.set(++id,{fn,at:now+ms});return id;};w.clearTimeout=id=>timers.delete(id);
w.setInterval=(fn,ms)=>{timers.set(++id,{fn,at:now+ms,interval:ms});return id;};w.clearInterval=id=>timers.delete(id);
w.matchMedia=()=>({matches:false});w.Element.prototype.getAnimations=()=>[];
w.Element.prototype.animate=()=>({finished:Promise.resolve(),cancel(){},playState:'finished'});
Object.defineProperty(d,'visibilityState',{get:()=>visibility});d.elementFromPoint=()=>hit;
Object.defineProperty(w.HTMLElement.prototype,'offsetWidth',{get(){return this.classList.contains('sealLens')?parseFloat(this.style.width)||168:this.classList.contains('lc')?350:84;}});
Object.defineProperty(w.HTMLElement.prototype,'offsetHeight',{get(){return this.classList.contains('sealLens')?parseFloat(this.style.height)||168:this.classList.contains('lc')?28:84;}});
w.eval(fs.readFileSync('charm-nest-motion.js','utf8'));
const at=Date.UTC(2026,9,1,0,44),outside=d.querySelector('#outside'),lens=()=>d.querySelector('.sealLens');
function rectFor(seal,x=410,y=560,size=84){const rect={left:x,right:x+size,top:y,bottom:y+size,width:size,height:size};seal.getBoundingClientRect=()=>rect;seal.getClientRects=()=>[rect];return rect;}
function point(type,node,relatedTarget=null){if(['pointerover','pointermove','pointerdown','click'].includes(type))hit=node;if(type==='pointerout')hit=relatedTarget;const r=node.getBoundingClientRect();node.dispatchEvent(new w.MouseEvent(type,{bubbles:true,relatedTarget,clientX:r.left+r.width/2,clientY:r.top+r.height/2}));}
async function advance(ms){const end=now+ms;for(;;){const next=[...timers.entries()].filter(([,t])=>t.at<=end).sort((a,b)=>a[1].at-b[1].at)[0];if(!next)break;now=next[1].at;if(next[1].interval)next[1].at+=next[1].interval;else timers.delete(next[0]);next[1].fn();for(let i=0;i<6;i++)await Promise.resolve();}now=end;for(let i=0;i<6;i++)await Promise.resolve();}
async function closed(label){await advance(0);assert.equal(lens(),null,label);}
async function exercise(seal,label){
 const history=seal.innerHTML;
 point('pointerover',seal);await advance(1000);assert.equal(lens(),null,label+' waits through a one-second pass');
 assert(!seal.hasAttribute('title'),label+' has no native tooltip bypass');point('pointerout',seal,outside);await advance(2000);await closed(label+' cancels on departure');
 point('pointerover',seal);await advance(1100);assert(lens(),label+' opens after resting');assert.match(lens().textContent,/PAUL|Paul/,label+' shows the saved signer');
 assert.equal(lens().style.width,'168px',label+' uses the common preview size');
 const [,x,y]=/translate\(([-\d.]+)px,([-\d.]+)px\)/.exec(lens().style.transform),r=seal.getBoundingClientRect();
 assert(+y+168+36<=r.top,label+' enlargement and signer caption stay above the original');
 point('pointerout',seal,outside);await closed(label+' closes immediately on departure');
 point('pointerover',seal);await advance(900);point('pointermove',outside);await advance(2000);await closed(label+' cancels without pointerout');
 point('pointerover',seal);await advance(1100);point('pointermove',outside);await closed(label+' movement dismisses its open preview');
 for(const event of ['blur','scroll','resize']){
  point('pointerover',seal);await advance(900);w.dispatchEvent(new w.Event(event));await advance(2000);await closed(label+' pending preview cannot survive '+event);
  point('pointerover',seal);await advance(1100);assert(lens());w.dispatchEvent(new w.Event(event));await closed(label+' open preview closes on '+event);
 }
 point('pointerover',seal);await advance(900);visibility='hidden';d.dispatchEvent(new w.Event('visibilitychange'));await advance(2000);await closed(label+' cannot open in a hidden tab');visibility='visible';
 assert.equal(seal.innerHTML,history,label+' keeps the historical face unchanged');
}
(async()=>{try{
 // A signed-in viewer is intentionally different from the employee stored on the old stamp.
 w.employee='Current Viewer';w.localStorage.setItem('cn.employee','Current Viewer');
 const row=d.querySelector('.sealRow');row.innerHTML=w.Seal.html({how:'engraveApproved',at,by:'Paul'},56)+w.Seal.html({how:'engraveApproved',at:at+1000,by:'Seth'},56);
 const [first,second]=row.querySelectorAll('.seal');rectFor(first);rectFor(second,506);
 point('pointerover',first);await advance(600);point('pointerout',first,second);point('pointerover',second,first);await advance(1000);assert.equal(lens(),null,'adjacent seals each get a full delay');
 await advance(100);assert.match(lens().querySelector('.lc').textContent,/Seth/);assert.doesNotMatch(lens().textContent,/Current Viewer/,'an old seal never acquires the current viewer name');point('pointerout',second,outside);await closed('adjacent preview closes');
 const child=first.querySelector('svg');child.getBoundingClientRect=first.getBoundingClientRect;
 point('pointerover',first);await advance(600);point('pointerout',first,child);point('pointerover',child,first);await advance(500);assert(lens(),'moving between seal descendants does not restart its rest');point('pointerout',child,outside);await closed('leaving an SVG descendant closes the preview');
 point('pointerover',first);await advance(900);hit=outside;await advance(1000);await closed('silent hit-test departure cannot open a pending preview');
 first.focus();assert(lens(),'keyboard focus opens a readable preview');point('pointerover',first);point('pointerout',first,outside);await closed('mouse departure closes even a focused seal');
 point('click',first);assert(lens()?.classList.contains('pinned'),'a direct click remains accessible');point('pointerdown',outside);await closed('outside press dismisses a clicked preview');
 point('pointerover',first);await advance(1100);point('click',first);point('pointerout',first,outside);await closed('a click cannot pin a mouse preview after departure');
 second.focus();assert(lens(),'second seal stays keyboard accessible');w.dispatchEvent(new w.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));await closed('Escape dismisses keyboard preview');
 for(const [how,scope] of [['print','sheet'],['button','sheet'],['laserReady','sheet'],['laserReady','set'],['laserDone','sheet'],['laserDone','set'],['engraveApproved','sheet'],['engravePlain','sheet']])for(const legacySize of [34,56,84,112]){
  const host=d.createElement('span');host.className='sealRow';host.innerHTML=w.Seal.html({how,scope,at,by:'Paul'},legacySize);d.body.append(host);const seal=host.querySelector('.seal');rectFor(seal);
  assert.doesNotMatch(seal.querySelector('svg').textContent,/Paul|Signed by/,how+' reserves its signer for hover');
  await exercise(seal,how+' '+scope+' requested '+legacySize);host.remove();
 }
 // The shared rendering API also supplies timeline families beyond print/engraving/laser.
 const designs=[['received','ORDER RECEIVED'],['prepared','ON SHEET'],['engraving','BACK ENGRAVING'],['laser','LASER CUT'],['finishing','ASSEMBLED'],['fulfilment','SHIPPED'],['exceptions','ON HOLD'],['cancelled','CANCELLED']];
 const outlines=new Set();
 for(const [family,action]of designs){
  const model={family,action,date:'30 SEP 2026',time:'9:44 PM',by:'Paul',at},host=d.createElement('span');host.className='sealRow';host.innerHTML='<span class="seal" tabindex="0" data-seal-caption="'+action+' by Paul">'+w.Seal.face(model)+'</span>';d.body.append(host);
  const seal=host.firstElementChild;rectFor(seal);const svg=seal.querySelector('svg'),texts=[...svg.querySelectorAll('text')].map(t=>t.textContent);
  assert.equal(svg.dataset.sealFamily,family);assert(!texts.some(t=>/Paul|Signed by/.test(t)),'regular '+family+' has no signer');assert(texts.includes('30 SEP 2026')&&texts.includes('9:44 PM'),'regular '+family+' displays its exact date and time');
  if(family==='engraving')assert.deepEqual(texts.slice(0,2),['BACK','ENGRAVING'],'engraving uses the approved two-line label');
  outlines.add(svg.querySelector('g>path').getAttribute('d'));await exercise(seal,'shared family '+family);host.remove();
 }
 assert.equal(outlines.size,8,'all eight families have distinguishable outlines');
 w.eval(fs.readFileSync('charm-nest-readiness.js','utf8'));const R=w.CharmNestReadiness;for(const ready of [true,false]){
  const host=d.createElement('div');host.innerHTML=R.seal({ready});d.body.append(host);const seal=host.querySelector('.laserSeal');rectFor(seal);
  assert.equal(seal.querySelector('svg').dataset.sealFamily,'laser','readiness uses the shared laser family');
  point('pointerover',seal);await advance(1000);assert.equal(lens(),null,'readiness seal uses the same delay');point('pointerout',seal,outside);await advance(2000);await closed('readiness pass cancels');
  point('pointerover',seal);await advance(1100);assert(lens());assert.equal(lens().style.width,'168px');assert.match(lens().querySelector('.lc').textContent,ready?/Sheet ready for laser cutting/:/Sheet not ready for laser cutting/);point('pointerout',seal,outside);await closed('readiness departure closes');host.remove();
 }
 const signedReadiness=d.createElement('div');signedReadiness.innerHTML=R.seal({ready:true},'Sheet',{processSeals:[{how:'laserReady',at,by:'Seth Signed'}]});d.body.append(signedReadiness);const readinessSeal=signedReadiness.querySelector('.laserSeal');rectFor(readinessSeal);
 assert.doesNotMatch(readinessSeal.querySelector('svg').textContent,/Seth Signed|Signed by/,'signed readiness keeps its signer off the regular face');point('pointerover',readinessSeal);await advance(1100);assert.match(lens().querySelector('.lf').textContent,/Seth Signed/,'readiness hover uses its saved employee');point('pointerout',readinessSeal,outside);await closed('signed readiness closes');signedReadiness.remove();
 const dialog=d.querySelector('dialog');dialog.querySelector('.sealRow').innerHTML=w.Seal.html({how:'laserReady',at,by:'Paul'},84);const dialogSeal=dialog.querySelector('.seal');rectFor(dialogSeal);
 point('pointerover',dialogSeal);await advance(900);dialog.dispatchEvent(new w.Event('close'));await advance(2000);await closed('closing a dialog cancels its pending seal preview');
 // A redraw may recreate identical historical markup, but it must earn a new hover rest.
 point('pointerover',first);await advance(1100);assert(lens());const replacement=first.cloneNode(true);rectFor(replacement);first.replaceWith(replacement);hit=replacement;await advance(250);await closed('redrawing a seal does not retain or resurrect its old hover');
 point('pointerover',replacement);await advance(1000);assert.equal(lens(),null,'a replacement earns its own delay');point('pointerout',replacement,outside);await advance(2000);
 // Short views and viewport edges still leave the original seal unobstructed, including the signer caption.
 const edgeHost=d.createElement('span');edgeHost.className='sealRow';edgeHost.innerHTML=w.Seal.html({how:'engraveApproved',at,by:'Paul'},84);d.body.append(edgeHost);const edgeSeal=edgeHost.firstElementChild;
 for(const [x,y,height]of [[8,20,768],[900,20,768],[410,200,400],[8,200,400],[900,200,400]]){
  w.innerHeight=height;rectFor(edgeSeal,x,y);point('pointerover',edgeSeal);await advance(1100);assert(lens(),'viewport edge still permits a preview');
  const [px,py]=lens().style.transform.match(/-?\d+(?:\.\d+)?(?=px)/g).map(Number),preview={left:px-91,right:px+259,top:py,bottom:py+204},r=edgeSeal.getBoundingClientRect();
  assert(preview.right<=r.left || preview.left>=r.right || preview.bottom<=r.top || preview.top>=r.bottom,'preview and caption do not cover the original in a '+height+'px view at '+x+','+y);
  assert(preview.left>=8 && preview.right<=w.innerWidth-8 && preview.top>=8 && preview.bottom<=height-8,'the full preview remains inside the viewport');
  point('pointerout',edgeSeal,outside);await closed('viewport-edge departure closes');
 }
 edgeHost.remove();
 console.log('PASS: 8 seal/scope variants across 4 legacy size requests, 8 shared designs, both readiness states and saved readiness actor; common 168 px preview, hover-only signer, unobstructed edge placement, 1.1-second rest, strict exit, descendant hit testing, scroll/resize/blur/tab/dialog/redraw dismissal and keyboard access');
}finally{w.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
