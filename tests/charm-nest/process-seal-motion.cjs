// Actual Seal module with controlled Web Animations. Each hold represents a real unfinished animation,
// so the test does not guess when a stamp may reveal ink or release its card from elapsed wall time.
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),{JSDOM}=require('jsdom');
const SOURCE=fs.readFileSync(path.join(__dirname,'../../charm-nest-motion.js'),'utf8');
const wait=ms=>new Promise(r=>setTimeout(r,ms)),tick=()=>wait(1);
const STAMP={id:'laserReady-123',how:'laserReady',at:Date.UTC(2026,8,30,18,35),by:'Paul Konieczny'};

function harness({reduced=false}={}){
  const dom=new JSDOM('<section class="setCard"><button data-seal-btn>Complete</button><span class="sealRow processSealRow"></span></section><section class="stampHost"><button>Print</button></section>',{runScripts:'outside-only',pretendToBeVisual:true});
  const w=dom.window,d=w.document,moves=[],holds=[],cancelled=[];
  w.matchMedia=()=>({matches:reduced});
  w.HTMLDialogElement.prototype.showModal=function(){this.open=true;};
  w.HTMLDialogElement.prototype.close=function(value){this.open=false;this.returnValue=value || '';this.dispatchEvent(new w.Event('close'));};
  const hold=(el,label,frames=[],opts={})=>{
    let resolve,reject;
    const a={el,label,frames,opts,playState:'running',effect:{getComputedTiming:()=>({endTime:opts.duration || 100})}};
    a.finished=new Promise((r,j)=>{resolve=r;reject=j;});a.finish=()=>{if(a.playState==='finished')return;a.playState='finished';resolve();};
    a.cancel=()=>{if(a.playState!=='running')return;cancelled.push(a.label);a.playState='idle';const error=new Error('Animation cancelled');error.name='AbortError';reject(error);};holds.push(a);return a;
  };
  w.Element.prototype.getAnimations=function(opts={}){return holds.filter(a=>a.playState==='running' && (a.el===this || opts.subtree && this.contains(a.el)));};
  w.Element.prototype.animate=function(frames,opts){
    const label=this.classList.contains('sealTool')?'wooden-tool':this.classList.contains('sealRing')?'ink-ripple':'other';
    const a=label==='other'?{el:this,label,frames,opts,playState:'finished',finished:Promise.resolve(),cancel(){cancelled.push(label);}}:hold(this,label,frames,opts);
    moves.push(a);return a;
  };
  Object.defineProperty(w.HTMLElement.prototype,'offsetWidth',{get(){return parseFloat(this.style.width) || parseFloat(this.style.getPropertyValue('--sz')) || 84;}});
  Object.defineProperty(w.HTMLElement.prototype,'offsetHeight',{get(){return parseFloat(this.style.height) || parseFloat(this.style.getPropertyValue('--sz')) || 84;}});
  w.eval(SOURCE);
  const rect=(el,left=410,top=440,width=84,height=width)=>{
    const r={left,top,right:left+width,bottom:top+height,width,height,x:left,y:top};el.getBoundingClientRect=()=>r;el.getClientRects=()=>[r];return r;
  };
  const card=d.querySelector('.setCard'),host=d.querySelector('.stampHost'),button=card.querySelector('button');
  rect(card,390,410,270,180);rect(button,410,440,140,36);rect(host,60,220,270,180);rect(host.querySelector('button'),80,250,140,36);
  const addSeal=(stamp=STAMP,left=410,size=84)=>{
    const wrapper=d.createElement('span');wrapper.innerHTML=w.Seal.html(stamp,size,'sheetProcessSeal pending');
    const seal=wrapper.firstChild;seal.dataset.sealOwner='sheet:test';card.querySelector('.sealRow').appendChild(seal);rect(seal,left,440,size);return seal;
  };
  const active=label=>holds.filter(a=>a.label===label && a.playState==='running');
  return{w,d,moves,holds,cancelled,hold,rect,card,host,button,addSeal,active,close:()=>w.close()};
}

async function contactAndLift(h){
  const tool=h.active('wooden-tool');assert.equal(tool.length,1,'one wooden head is moving');
  assert(tool[0].opts.duration>=500,'the descent remains long enough to see');tool[0].finish();await tick();
  const lift=h.active('wooden-tool');assert.equal(lift.length,1,'the head lifts after contact');assert(lift[0].opts.duration>=600,'the lift remains long enough to see');
  lift[0].finish();for(const a of h.active('ink-ripple'))a.finish();await wait(280);
}

async function fullPress(){
  const h=harness();
  try{
    const list=h.d.createElement('div');h.d.body.insertBefore(list,h.card);list.appendChild(h.card);h.card.dataset.mkey='order:test';
    const seal=h.addSeal(),saved=seal.innerHTML,incoming=h.hold(h.card,'incoming-card'),ancestor=h.hold(list,'incoming-order-panel');
    const ghost=h.d.createElement('span');ghost.className='mGhost';h.d.body.appendChild(ghost);const transition=h.hold(ghost,'incoming-transfer');
    let resolved=false,redraws=0,clicks=0;
    const pressing=h.w.Seal.press(seal).then(()=>{resolved=true;});const duplicate=h.w.Seal.press(seal);
    await tick();assert(h.w.Seal.busy());assert.equal(h.d.querySelector('.sealTool'),null,'card and transfer animations finish before the stamp starts');
    incoming.finish();await tick();assert.equal(h.d.querySelector('.sealTool'),null,'an unfinished transfer cannot be interrupted');transition.finish();await tick();assert.equal(h.d.querySelector('.sealTool'),null,'the containing work area must also finish moving');ancestor.finish();await tick();
    assert.equal(h.d.querySelectorAll('.sealTool').length,1,'a double press does not add another wooden head');assert(seal.classList.contains('pending'),'ink stays hidden throughout descent');assert(!resolved);
    assert(h.w.Seal.defer('orders',()=>{redraws=1;}));assert(h.w.Seal.defer('orders',()=>{redraws=2;}));h.button.onclick=()=>clicks++;h.button.click();assert.equal(clicks,0,'navigation is blocked while the stamp is in motion');assert.equal(redraws,0);
    h.w.Motion.reconcile(list,[],{animate:false});assert(h.card.isConnected,'a list update cannot remove the card beneath a moving stamp');
    const ink=h.hold(seal,'ink-settle');h.active('wooden-tool')[0].finish();await tick();
    assert(!seal.classList.contains('pending'),'ink appears at contact');assert(seal.classList.contains('wet'));assert.equal(h.active('ink-ripple').length,1);assert(!resolved);
    h.active('wooden-tool')[0].finish();await wait(280);assert(!resolved,'lifting the head alone cannot release the card');assert(h.d.querySelector('.sealTool'),'the head remains until the full contact sequence finishes');
    h.active('ink-ripple')[0].finish();await tick();assert(!resolved,'unfinished ink reveal also holds the card');ink.finish();await Promise.all([pressing,duplicate]);await wait(25);
    assert.equal(h.d.querySelector('.sealTool'),null);assert.equal(h.d.querySelector('.sealRing'),null);assert(!seal.classList.contains('wet'));assert(!h.w.Seal.busy());
    assert.equal(redraws,2,'the latest redraw runs once after the complete sequence');assert(!h.card.isConnected,'the requested list removal runs after the full stamp');assert.equal(seal.innerHTML,saved,'stamping never modifies historical face data');assert.deepEqual(h.cancelled,[],'incoming, stamp and ink animations all finish naturally');
    h.button.click();assert.equal(clicks,1,'navigation resumes after the stamp is complete');assert.match(h.w.Seal.titleOf({...STAMP,how:'print'}),/^QR label printed by Paul/);assert.match(h.w.Seal.titleOf({...STAMP,how:'button'}),/^Completed with the Complete Order button by Paul/);
  }finally{h.close();}
}

async function oneQueue(){
  const h=harness();
  try{
    const first=h.addSeal(),second=h.addSeal({...STAMP,id:'laserDone-124',how:'laserDone',by:'Seth'},510),completed=[];let redraws=0;
    const a=h.w.Seal.press(first).then(()=>completed.push('first')),b=h.w.Seal.press(second).then(()=>completed.push('second'));
    const c=h.w.Seal.stampOn(h.host,{btn:h.host.querySelector('button'),stamp:{...STAMP,id:'print-125',how:'print'}}).then(()=>completed.push('button'));
    await tick();assert.equal(h.d.querySelectorAll('.sealTool').length,1,'press and button stamps share a single queue');assert(second.classList.contains('pending'),'the second seal stays pending while the first stamps');h.w.Seal.defer('render',()=>redraws++);
    await contactAndLift(h);assert.deepEqual(completed,['first']);assert.equal(redraws,0);assert(h.w.Seal.busy());assert.equal(h.d.querySelectorAll('.sealTool').length,1,'the second head starts after the first is gone');assert(!first.classList.contains('pending'));assert(second.classList.contains('pending'));
    await contactAndLift(h);assert.deepEqual(completed,['first','second']);assert.equal(redraws,0);assert(h.w.Seal.busy());
    await contactAndLift(h);await Promise.all([a,b,c]);await wait(25);assert.deepEqual(completed,['first','second','button']);assert.equal(redraws,1);assert(!h.w.Seal.busy());assert.equal(h.d.querySelectorAll('.sealTool,.sealRing').length,0);assert.equal(h.host.querySelectorAll('.seal').length,1,'the queued button stamps once');
  }finally{h.close();}
}

async function stillAndReduced(){
  const h=harness({reduced:true});
  try{
    const seal=h.addSeal();await h.w.Seal.press(seal);assert(!seal.classList.contains('pending'));assert.equal(h.moves.length,0,'reduced motion places ink without the moving tool');
    await h.w.Seal.stampOn(h.host,{btn:h.host.querySelector('button'),stamp:{...STAMP,how:'print'}});assert.equal(h.d.querySelectorAll('.sealTool').length,0);assert.equal(h.host.querySelectorAll('.seal').length,1);assert(!h.w.Seal.busy());
  }finally{h.close();}
  const carried=harness();
  try{await carried.w.Seal.stampOn(carried.host,{btn:carried.host.querySelector('button'),stamp:{...STAMP,how:'print'},still:true});assert.equal(carried.moves.length,0,'carrying a historical seal does not stamp again');assert.equal(carried.host.querySelectorAll('.seal').length,1);assert.equal(carried.d.querySelectorAll('.sealTool').length,0);}finally{carried.close();}
}

async function redrawFrameRace(){
  const h=harness();
  try{
    const first=h.addSeal();let redraws=0,value=0;
    const a=h.w.Seal.press(first);await tick();h.w.Seal.defer('redraw',()=>{redraws++;value=1;});
    h.active('wooden-tool')[0].finish();await wait(280);
    h.active('wooden-tool')[0].finish();h.active('ink-ripple')[0].finish();await a;
    // The previous queue has completed, but its render callback has not reached the next frame yet.
    const second=h.addSeal({...STAMP,id:'new-press',at:STAMP.at+1},510),b=h.w.Seal.press(second);
    h.w.Seal.defer('redraw',()=>{redraws++;value=2;});await wait(25);
    assert.equal(redraws,0,'a new approval between queue completion and the redraw frame holds the old redraw');
    await contactAndLift(h);await b;await wait(25);assert.equal(redraws,1,'the pending redraw still coalesces once across the frame gap');assert.equal(value,2,'the newest callback survives the old frame');
  }finally{h.close();}
}

async function cancelledPhase(){
  const h=harness();
  try{
    const seal=h.addSeal();let resolved=false;
    const pressing=h.w.Seal.press(seal).then(()=>{resolved=true;});await tick();
    const down=h.active('wooden-tool')[0];down.cancel();await tick();
    assert(seal.classList.contains('pending'),'cancelled descent cannot show ink at once');assert.equal(h.active('ink-ripple').length,0);assert(!resolved);
    const resumedDown=h.active('wooden-tool')[0];assert(resumedDown,'a cancelled descent resumes its visible remainder');assert(resumedDown.opts.duration>0 && resumedDown.opts.duration<=560);
    resumedDown.finish();await tick();assert(!seal.classList.contains('pending'));
    const up=h.active('wooden-tool')[0];up.cancel();await tick();
    assert(!resolved,'cancelled lift cannot advance the order');const resumedUp=h.active('wooden-tool')[0];assert(resumedUp,'a cancelled lift resumes its visible remainder');assert(resumedUp.opts.duration>0 && resumedUp.opts.duration<=700);
    resumedUp.finish();for(const a of h.active('ink-ripple'))a.finish();await pressing;assert.equal(h.d.querySelector('.sealTool'),null);assert(!h.w.Seal.busy());
  }finally{h.close();}
}

async function dialogCloseWaits(){
  for(const viaMotion of [false,true]){
    const h=harness();
    try{
      const dialog=h.d.createElement('dialog');dialog.className='sheetWin';dialog.open=true;h.d.body.appendChild(dialog);dialog.appendChild(h.card);
      const seal=h.addSeal(),pressing=h.w.Seal.press(seal);await tick();let closeFinished=false,closing;
      if(viaMotion)closing=h.w.Motion.dialogClose(dialog,{returnValue:'after-stamp'}).then(()=>{closeFinished=true;});else dialog.close('after-stamp');
      assert(dialog.open,'a programmatic close cannot hide the stamp surface');assert(!closeFinished);
      await contactAndLift(h);await pressing;await wait(25);if(closing)await closing;
      assert(!dialog.open,'the deferred close happens after the whole stamp');assert.equal(dialog.returnValue,'after-stamp');if(viaMotion)assert(closeFinished);
    }finally{h.close();}
  }
}

async function orderModalWaits(){
  const bridge=fs.readFileSync(path.join(__dirname,'../../charm-nest-bridge.js'),'utf8'),base=bridge.indexOf('const OrderWin =');
  for(const operation of ['close','open']){
    const h=harness();
    try{
      const dialog=h.d.createElement('dialog');dialog.id='orderWin';dialog.open=true;h.d.body.appendChild(dialog);dialog.appendChild(h.card);
      const first=h.addSeal(),second=h.addSeal({...STAMP,id:'later-stamp',how:'laserDone'},510),a=h.w.Seal.press(first),b=h.w.Seal.press(second);await tick();
      let wired=0,stopped=0,shown=0,resolved=false;
      const context={window:{Seal:h.w.Seal},W:{dlg:dialog,closing:false,anims:[]},Promise,setTimeout,still:()=>true,origin:()=>null,rectOf:()=>null,stopMotion:()=>stopped++,giveBack(){},wire:()=>wired++,Orders:{rows:()=>[{key:'row',state:'written',order:{receiptId:'3701000'}}]},show:()=>shown++};vm.createContext(context);
      const start=bridge.indexOf(operation==='close'?'  async function shut()':'  async function openOrder(',base),end=bridge.indexOf(operation==='close'?'  /** The gold flash':'  // (a repaint asked from outside',start);assert(start>=0 && end>start);vm.runInContext(bridge.slice(start,end),context);
      const action=(operation==='close'?context.shut():context.openOrder('3701000')).then(()=>{resolved=true;});await tick();
      assert(!resolved,'an async order modal transition waits for the stamp queue');assert.equal(stopped,0);assert.equal(wired,0);assert.equal(shown,0);assert.equal(h.moves.filter(m=>m.el===dialog).length,0,'a queued close has not started its surface animation');
      await contactAndLift(h);assert(!resolved,'finishing the first head does not release a transition while the second is queued');assert.equal(stopped,0);assert.equal(wired,0);
      await contactAndLift(h);await Promise.all([a,b,action]);assert(resolved);
      if(operation==='close'){assert.equal(stopped,1);assert(!dialog.open);assert.equal(h.moves.filter(m=>m.el===dialog).length,1);}
      else{assert.equal(wired,1);assert.equal(shown,1);}
    }finally{h.close();}
  }
}

(async()=>{await fullPress();await oneQueue();await redrawFrameRace();await cancelledPhase();await dialogCloseWaits();await orderModalWaits();await stillAndReduced();console.log('PASS: shared queue; complete incoming/descent/contact/lift/ripple/ink before redraw/navigation; frame-gap and programmatic/async modal guards; cancelled phases resume; duplicate press idempotent; static history never re-stamps');})().catch(e=>{console.error(e);process.exitCode=1;});
