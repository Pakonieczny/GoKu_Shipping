// The actual Seal module: wooden press, ink timing, delayed lens and independent historical controls.
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{JSDOM}=require('jsdom');
const dom=new JSDOM('<button data-seal-btn>Complete</button><span class="sealRow processSealRow"></span>',{runScripts:'outside-only',pretendToBeVisual:true});
const w=dom.window,d=w.document,wait=ms=>new Promise(r=>setTimeout(r,ms)),moves=[],pending=[];
w.matchMedia=()=>({matches:false});
w.Element.prototype.getAnimations=()=>[];
w.Element.prototype.animate=function(frames,opts){
  const move={el:this,frames,opts};moves.push(move);let finish;
  const finished=this.classList.contains('sealTool')?new Promise(r=>{finish=r;pending.push(r);}):Promise.resolve();
  return {finished,cancel:()=>finish?.(),playState:'finished'};
};
Object.defineProperty(w.HTMLElement.prototype,'offsetWidth',{get(){return this.classList.contains('sealLens')?parseFloat(this.style.width)||200:this.classList.contains('lc')?350:56;}});
Object.defineProperty(w.HTMLElement.prototype,'offsetHeight',{get(){return this.classList.contains('sealLens')?parseFloat(this.style.height)||200:this.classList.contains('lc')?28:56;}});
w.eval(fs.readFileSync(path.join(__dirname,'../../charm-nest-motion.js'),'utf8'));
const stamp={id:'laserReady-123',how:'laserReady',at:Date.UTC(2026,8,30,18,35),by:'Paul Konieczny'};
d.querySelector('.sealRow').innerHTML=w.Seal.html(stamp,56,'sheetProcessSeal ready pending');
const seal=d.querySelector('.seal');seal.dataset.sealOwner='sheet:test';
const rect={left:410,right:466,top:560,bottom:616,width:56,height:56};
seal.getBoundingClientRect=()=>rect;seal.getClientRects=()=>[rect];
(async()=>{
  try{
    const pressing=w.Seal.press(seal);w.Seal.press(seal);await wait(1);
    assert.equal(d.querySelectorAll('.sealTool').length,1,'a pending stamp is pressed only once');
    assert.equal(d.querySelectorAll('.sealTool radialGradient').length,2,'the existing wooden tool is used');
    assert(seal.classList.contains('pending'),'ink waits for the tool to reach the paper');
    let redrawn=0,clicks=0;assert(w.Seal.busy());w.Seal.defer('redraw',()=>redrawn++);d.querySelector('button').onclick=()=>clicks++;d.querySelector('button').click();assert.equal(clicks,0,'navigation is blocked during stamping');assert.equal(redrawn,0);
    assert.equal(moves[0].opts.duration,560);pending.shift()();await wait(1);
    assert(!seal.classList.contains('pending'));assert(seal.classList.contains('wet'));assert(moves.some(m=>m.el.classList.contains('sealRing')));
    pending.shift()();await pressing;await wait(30);assert.equal(d.querySelector('.sealTool'),null);assert.equal(redrawn,1);assert(!w.Seal.busy());
    const saved=seal.innerHTML;
    seal.dispatchEvent(new w.MouseEvent('pointerover',{bubbles:true}));
    await wait(150);assert.equal(d.querySelector('.sealLens'),null,'a passing pointer does not open the lens');
    await wait(1000);let lens=d.querySelector('.sealLens');assert(lens,'resting over a seal opens the enlargement');
    assert.match(lens.querySelector('.lc').textContent,/Laser ready by Paul Konieczny/);
    assert.match(lens.querySelector('.lf').textContent,/30 SEP 2026/);
    const [,x,y]=/translate\(([-\d.]+)px,([-\d.]+)px\)/.exec(lens.style.transform);
    assert(+y+200+36<rect.top,'the enlarged face and caption both stay above the original');
    assert.equal(seal.innerHTML,saved,'zoom does not change the original seal');
    const ids=[...d.querySelectorAll('[id]')].map(e=>e.id);assert.equal(new Set(ids).size,ids.length,'cloned SVG ids remain unique');
    let completes=0;d.querySelector('button').onclick=()=>completes++;
    seal.dispatchEvent(new w.MouseEvent('click',{bubbles:true,clientX:430,clientY:580}));assert.equal(completes,0,'historical seals cannot complete or undo anything');
    assert(d.querySelector('.sealLens').classList.contains('pinned'));
    w.dispatchEvent(new w.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));await wait(1);assert.equal(d.querySelector('.sealLens'),null);
    d.querySelector('.sealRow').innerHTML+=w.Seal.html({...stamp,id:'done-124',how:'laserDone',scope:'set',by:'Seth'},56);
    assert.match(d.querySelector('.seal-laserDone').textContent,/SET COMPLETED/);assert.match(d.querySelector('.seal-laserDone').getAttribute('aria-label'),/Seth/);
    d.querySelector('.sealRow').innerHTML+=w.Seal.html({how:'engraveApproved',at:stamp.at,by:'Seth'},56);assert.match(d.querySelector('.seal-engraveApproved').textContent,/ENGRAVING APPROVED/);assert.match(d.querySelector('.seal-engraveApproved').getAttribute('aria-label'),/Engraving placement approved by Seth/);
    assert.match(w.Seal.titleOf({how:'print',at:stamp.at,by:'Paul'}),/^QR label printed by Paul/);
    assert.match(w.Seal.titleOf({how:'button',at:stamp.at,by:'Paul'}),/^Completed with the Complete Order button by Paul/);
    console.log('PASS: existing wooden tool, ink on contact, single press, delayed hover, enlargement above, full provenance and safe historical controls');
  }finally{w.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
