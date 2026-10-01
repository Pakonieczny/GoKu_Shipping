const assert=require('node:assert/strict'),fs=require('node:fs'),{JSDOM}=require('jsdom');
const dom=new JSDOM('<span class="sealRow"></span><p id="outside">Work area</p>',{runScripts:'outside-only',pretendToBeVisual:true}),w=dom.window,d=w.document;
let now=0,id=0;const timers=new Map();
w.setTimeout=(fn,ms)=>{timers.set(++id,{fn,at:now+ms});return id;};w.clearTimeout=id=>timers.delete(id);
w.setInterval=()=>++id;w.clearInterval=()=>{};w.matchMedia=()=>({matches:false});
w.Element.prototype.getAnimations=()=>[];w.Element.prototype.animate=()=>({finished:Promise.resolve(),cancel(){},playState:'finished'});
Object.defineProperty(w.HTMLElement.prototype,'offsetWidth',{get(){return this.classList.contains('sealLens')?parseFloat(this.style.width)||200:this.classList.contains('lc')?350:56;}});
Object.defineProperty(w.HTMLElement.prototype,'offsetHeight',{get(){return this.classList.contains('sealLens')?parseFloat(this.style.height)||200:this.classList.contains('lc')?28:56;}});
w.eval(fs.readFileSync('charm-nest-motion.js','utf8'));
const at=Date.UTC(2026,9,1,0,44);d.querySelector('.sealRow').innerHTML=w.Seal.html({how:'engraveApproved',at,by:'Paul'},56)+w.Seal.html({how:'engraveApproved',at:at+1000,by:'Seth'},56);
const [first,second]=d.querySelectorAll('.seal'),outside=d.querySelector('#outside');
for(const [index,seal] of [first,second].entries()){const rect={left:410+index*65,right:466+index*65,top:560,bottom:616,width:56,height:56};seal.getBoundingClientRect=()=>rect;seal.getClientRects=()=>[rect];}
const original=[first.innerHTML,second.innerHTML],lens=()=>d.querySelector('.sealLens');
const point=(type,node,relatedTarget=null)=>node.dispatchEvent(new w.MouseEvent(type,{bubbles:true,relatedTarget}));
async function advance(ms){const end=now+ms;for(;;){const next=[...timers.entries()].filter(([,t])=>t.at<=end).sort((a,b)=>a[1].at-b[1].at)[0];if(!next)break;now=next[1].at;timers.delete(next[0]);next[1].fn();await Promise.resolve();}now=end;await Promise.resolve();}
(async()=>{try{
 point('pointerover',first);await advance(1000);assert.equal(lens(),null,'a pass lasting up to one second cannot open a seal preview');
 assert(!first.hasAttribute('title'),'the native tooltip cannot bypass the hover delay');
 point('pointerout',first,outside);await advance(2000);assert.equal(lens(),null,'leaving cancels a pending preview');
 assert(first.hasAttribute('title'),'cancelling a preview restores its native description');
 point('pointerover',first);await advance(600);point('pointerout',first,second);point('pointerover',second,first);await advance(1000);assert.equal(lens(),null,'each adjacent seal gets its own full delay');
 await advance(100);assert(lens());assert.match(lens().querySelector('.lc').textContent,/Seth/);
 point('pointerout',second,outside);await advance(0);assert.equal(lens(),null,'an open hover preview closes when the pointer leaves');
 first.focus();assert(lens(),'keyboard focus opens a readable preview');point('pointerover',first);point('pointerout',first,outside);await advance(0);assert.equal(lens(),null,'mouse departure closes the preview even when the seal retains focus');
 point('pointerover',first);await advance(1100);assert(lens());point('pointerout',first,second);point('pointerover',second,first);await advance(500);assert.equal(lens(),null,'an open preview cannot jump immediately to another seal');point('pointerout',second,outside);await advance(2000);assert.equal(lens(),null);
 point('pointerover',first);await advance(900);point('pointermove',outside);await advance(2000);assert.equal(lens(),null,'pointer movement away cancels the timer even without pointerout');
 point('pointerover',first);await advance(1100);assert(lens());point('pointermove',outside);await advance(0);assert.equal(lens(),null,'pointer movement away also dismisses an open preview');
 point('pointerover',first);await advance(900);w.dispatchEvent(new w.Event('blur'));await advance(2000);assert.equal(lens(),null,'a pending preview cannot appear after leaving the window');
 point('click',first);assert(lens()?.classList.contains('pinned'),'explicit clicks remain available');point('pointerdown',outside);await advance(0);assert.equal(lens(),null);
 point('pointerover',first);await advance(1100);point('click',first);assert(lens());point('pointerout',first,outside);await advance(0);assert.equal(lens(),null,'clicking cannot keep a mouse preview open off the original seal');
 second.focus();assert(lens(),'keyboard access remains available');w.dispatchEvent(new w.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));await advance(0);assert.equal(lens(),null);
 assert.deepEqual([first.innerHTML,second.innerHTML],original,'hover never changes historical seals');
 for(const [how,scope] of [['print','sheet'],['button','sheet'],['laserReady','sheet'],['laserReady','set'],['laserDone','sheet'],['laserDone','set'],['engraveApproved','sheet'],['engravePlain','sheet']])for(const size of [34,56,112]){
  const row=d.createElement('span');row.className='sealRow';row.innerHTML=w.Seal.html({how,scope,at,by:'Paul'},size);d.body.append(row);const seal=row.querySelector('.seal');
  const rect={left:410,right:410+size,top:560,bottom:560+size,width:size,height:size};seal.getBoundingClientRect=()=>rect;seal.getClientRects=()=>[rect];const history=seal.innerHTML;
  point('pointerover',seal);await advance(1000);assert.equal(lens(),null,how+' '+scope+' '+size+' waits');point('pointerout',seal,outside);await advance(2000);assert.equal(lens(),null);
  point('pointerover',seal);await advance(1100);assert(lens(),how+' '+scope+' '+size+' opens');assert.match(lens().textContent,/PAUL/);point('pointerout',seal,outside);await advance(0);assert.equal(lens(),null,how+' '+scope+' '+size+' closes');assert.equal(seal.innerHTML,history);row.remove();
 }
 const R=require('../../charm-nest-readiness.js');for(const ready of [true,false]){
  const row=d.createElement('div');row.innerHTML=R.seal({ready});d.body.append(row);const seal=row.querySelector('.laserSeal'),rect={left:410,right:430,top:560,bottom:584,width:20,height:24};seal.getBoundingClientRect=()=>rect;seal.getClientRects=()=>[rect];
  point('pointerover',seal);await advance(1000);assert.equal(lens(),null,'readiness badges use the same delay');point('pointerout',seal,outside);await advance(2000);assert.equal(lens(),null);
  point('pointerover',seal);await advance(1100);assert(lens());assert.match(lens().querySelector('.lc').textContent,ready?/Sheet ready for laser cutting/:/Sheet not ready for laser cutting/);point('pointerout',seal,outside);await advance(0);assert.equal(lens(),null);row.remove();
 }
 console.log('PASS: all 8 seal/scope variants at 34, 56 and 112 px; 1.1-second delay per seal, cancelled fast crossings, immediate departure even after focus/click, no native tooltip bypass or history changes; keyboard access remains');
}finally{w.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
