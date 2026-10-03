/* The manual "Approve for laser cutting" button on the Library's In progress cards (charm-nest-bridge.js, LaserReview).
   Fakes of LibraryFlow and LibraryApprovalUI: the button shows on a card that is not ready (sheet card, set card, a sheet
   inside a set) and never on a ready or completed one, is disabled with its plain reason while engravings are missing,
   a press calls approve once, a second press while it runs is ignored, the card list then updates by itself. */
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),{JSDOM}=require('jsdom');
const src=fs.readFileSync('charm-nest-bridge.js','utf8');
const dom=new JSDOM('<body><main id="libBody"></main></body>',{runScripts:'outside-only',pretendToBeVisual:true}),win=dom.window,document=win.document,frames=[],requests=[],calls=[],shows=[],opened=[];
win.matchMedia=()=>({matches:true});
win.eval(fs.readFileSync('charm-nest-motion.js','utf8'));
win.eval(fs.readFileSync('charm-nest-readiness.js','utf8'));
let world={};   // what the cloud answers to a laserStatus read: every record, as it is now
const esc=x=>String(x==null?'':x).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/"/g,'&quot;');
const c=vm.createContext({window:win,document,console,esc,cors:x=>x,S:{mode:'library',cloud:{ok:true}},allSheets:()=>[],Orders:{rows:()=>[]},Engrave:{items:()=>new Map()},O:{setLabel:n=>'Set '+n},CNListActivity:{compare:()=>0,state:()=>({direction:1})},innerHeight:win.innerHeight,requestAnimationFrame:fn=>(frames.push(fn),frames.length),setTimeout:win.setTimeout.bind(win),clearTimeout:win.clearTimeout.bind(win),setInterval(){},api:async(name,payload)=>{requests.push({name,payload});return {sheets:Object.values(world),sets:[]};}});
vm.runInContext(src.slice(src.indexOf('const LaserReview ='),src.indexOf('const Sets =')),c);
const L=win.LaserReview,body=document.querySelector('#libBody');
const flush=()=>{for(let i=0;i<8 && frames.length;i++)frames.splice(0).forEach(f=>f());};
const tick=(ms=15)=>new Promise(r=>setTimeout(r,ms));
const sheet=(id,extra={})=>({id,sheetIndex:1,poolIds:[id+'1'],placedCount:1,verification:{ok:true},preview:'p',outputs:{ai:'f'},label:{files:[]},backPool:[],engraving:{[id+'1']:{needed:false,state:'none',approved:true}},updatedAt:1,...extra});
const labelled=s=>({...s,label:{files:[{path:'qr',url:'u',payload:'o'}]},processReady:true,updatedAt:(s.updatedAt||0)+1});
const waiting=id=>sheet(id,{poolIds:[id+'1',id+'2'],placedCount:2,engraving:{[id+'1']:{needed:true,state:'words',approved:false},[id+'2']:{needed:true,state:'words',approved:false}}});
const rect=card=>{card.getBoundingClientRect=()=>({left:10,top:80,right:310,bottom:430,width:300,height:350});return card;};
const flat=id=>{const a=document.createElement('article');a.className='librarySheet';a.dataset.laserCard='sheet';a._laserSheets=[id];a.innerHTML=`<div class="libCard" data-id="${id}"><span data-sheet-status="${id}"></span></div>`;return rect(a);};
const setCard=(setId,ids)=>{const a=document.createElement('div');a.className='setCard';a.dataset.laserCard='set';a._laserSet={setId,seq:1,sheetIds:ids};a._laserSheets=ids;a.innerHTML=`<div class="sh"><span class="nm" data-set-title></span></div><div class="sheetsRow">${ids.map(id=>`<article class="librarySheet"><div class="libCard" data-id="${id}"><span data-sheet-status="${id}"></span></div></article>`).join('')}</div>`;return rect(a);};
const area=card=>card.closest('[data-laser-area]').dataset.laserArea;
const boxes=root=>[...root.querySelectorAll('.approveBox')];
const boxOf=(root,key)=>root.querySelector(`.approveBox[data-approve-for="${key}"]`);

// the records: F1 lacks only its QR label (an automatic step), R is ready, A is like F1, B waits on two back engravings, D is cut
const recs={F1:sheet('F1'),F2:sheet('F2'),F3:sheet('F3'),R:labelled(sheet('R')),A:sheet('A'),B:waiting('B'),D:labelled(sheet('D',{laserDoneAt:5}))};
Object.values(recs).forEach(L.record);world={...recs};L.sections(body);
const cards={F1:flat('F1'),F2:flat('F2'),F3:flat('F3'),R:flat('R'),set:setCard('set1',['A','B','D'])};
for(const k of ['F1','F2','F3','R'])L.place(cards[k],L.canCut(recs[k]),body);
L.place(cards.set,L.group(cards.set._laserSet,['A','B','D'].map(id=>recs[id])).ready,body);

(async()=>{try{
  // 1. no LibraryFlow on the page: no button at all, never a dead one
  L.changed();flush();
  assert.equal(boxes(body).length,0,'without LibraryFlow no card carries an Approve button');

  // 2. LibraryFlow present: which cards show the button
  const plans=[];let release=null;
  win.LibraryFlow={approve:o=>{calls.push(o);return new Promise(r=>{release=()=>r(plans.shift());});}};
  win.LibraryApprovalUI={show:(host,plan,opts)=>shows.push({host,plan,opts})};
  win.CNEmployee={name:()=>'Maria'};
  L.changed();flush();
  assert.equal(area(cards.R),'ready');assert.equal(area(cards.F1),'pending');
  assert.equal(boxes(cards.R).length,0,'a ready card has no Approve button');
  const f1=boxOf(cards.F1,'sheet:F1');assert(f1,'a sheet card that is not ready has one');
  const btn=f1.querySelector('[data-approve-btn]');
  assert.equal(btn.textContent,'Approve for laser cutting');assert.equal(btn.disabled,false);assert.equal(f1.querySelector('[data-approve-why]').textContent,'');
  assert.equal(f1.parentElement,cards.F1,'the button lives on its card, not at the bottom of the page');
  assert(boxOf(cards.set,'set:set1'),'a set card that is not ready has one');
  assert.equal(boxOf(cards.set,'set:set1').nextElementSibling,cards.set.querySelector('.sheetsRow'),'the set button sits above its sheets');
  const inSet=cards.set.querySelectorAll('.sheetsRow .approveBox');
  assert.deepEqual([...inSet].map(b=>b.dataset.approveFor),['sheet:A','sheet:B'],'a sheet inside a set gets its own button; the cut sheet D gets none');
  assert.equal(boxes(document).filter(b=>!b.closest('#libBody')).length,0);

  // 3. hard-blocked: visible, disabled, with the plain reason
  const setBox=boxOf(cards.set,'set:set1'),bBox=boxOf(cards.set,'sheet:B');
  assert.equal(bBox.querySelector('[data-approve-btn]').disabled,true);assert.equal(bBox.querySelector('[data-approve-why]').textContent,'Waiting on 2 back engravings');
  assert.equal(setBox.querySelector('[data-approve-btn]').disabled,true);assert.equal(setBox.querySelector('[data-approve-why]').textContent,'Waiting on 2 back engravings');
  assert.equal(boxOf(cards.set,'sheet:A').querySelector('[data-approve-btn]').disabled,false,'a sheet that only waits on automatic steps can be pressed');
  delete win.LaserReview.openChecklist;L.changed();flush();   // (the real one now ships in LaserReview: take it away to see the reason without it)
  assert.equal(bBox.querySelector('[data-approve-reason]'),null,'the reason is plain text until the checklist exists');
  bBox.querySelector('[data-approve-btn]').click();assert.equal(calls.length,0,'a disabled button presses nothing');
  win.LaserReview.openChecklist=(card,info)=>opened.push({card,...info});
  L.changed();flush();
  const why=bBox.querySelector('[data-approve-reason]');assert(why,'with the checklist shipped the reason opens it');
  why.click();assert.deepEqual(opened.map(o=>[o.kind,o.id]),[['sheet','B']]);assert.equal(opened[0].card,bBox.parentElement);assert.equal(calls.length,0);

  // 4. a press calls approve once, a second press while it runs is ignored, a labelled spinner shows
  btn.click();btn.click();f1.querySelector('[data-approve-btn]').click();
  assert.equal(calls.length,1,'one approve call for a double press');
  assert.deepEqual({...calls[0]},{kind:'sheet',id:'F1',by:'Maria'});
  assert.equal(btn.disabled,true);assert.match(btn.textContent,/Approving/);assert(btn.querySelector('.spin'),'small spinner on the button');
  L.changed();flush();btn.disabled=false;btn.click();assert.equal(calls.length,1,'still one call while it runs, even through a redraw');btn.disabled=true;

  // 5. the plan is shown on the card; the card moves to Laser cutting by itself
  world.F1=labelled(recs.F1);
  plans.push({ok:true,auto:[{key:'qr',label:'QR label remade'}],needs:[],confirm:[],notes:[]});
  release();await tick();
  assert.equal(shows.length,1);assert.equal(shows[0].host,cards.F1,'LibraryApprovalUI shows on the card that was pressed');
  assert.equal(shows[0].plan.auto[0].label,'QR label remade');assert.equal(typeof shows[0].opts.onConfirm,'function');assert.equal(typeof shows[0].opts.onCancel,'function');
  assert.equal(requests.length,1,'the card is read again at once');assert.equal(requests[0].payload.sheetIds.includes('F1'),true);
  await tick();flush();
  assert.equal(area(cards.F1),'ready','the card moved to Laser cutting');assert.equal(boxes(cards.F1).length,0,'and no longer has a button');
  assert.equal(btn.isConnected,false);

  // 6. without LibraryApprovalUI the plan is a plain list on the card, the press is explicit, an error is said in place
  delete win.LibraryApprovalUI;
  win.LibraryFlow.commit=async(plan,o)=>({ok:true,applied:[{key:'roseLine',label:'Green dash line added'}]});
  const f2=boxOf(cards.F2,'sheet:F2');f2.querySelector('[data-approve-btn]').click();
  plans.push({ok:false,auto:[{key:'layout',label:'Layout verified'}],needs:[{key:'backs',label:'2 back engravings not approved',items:[{label:'Order 123'}]}],confirm:[{key:'roseLine',label:'Add the green dash line?',detail:'This calculates the cut contour'}]});
  release();await tick();
  const plan=f2.querySelector('[data-approve-plan]');assert.equal(plan.hidden,false);
  assert.match(plan.textContent,/Layout verified/);assert.match(plan.textContent,/2 back engravings not approved/);assert.match(plan.textContent,/Order 123/);assert.match(plan.textContent,/Add the green dash line\?/);
  assert.equal(calls.length,2);assert.equal(f2.querySelector('[data-approve-btn]').disabled,false,'pressable again once the answer is in');
  assert.equal(calls[1].id,'F2');
  assert.equal(plan.querySelector('[data-approve-yes]').dataset.approveYes,'roseLine','a confirm is its own explicit button');
  plan.querySelector('[data-approve-yes]').click();await tick();
  assert.match(plan.textContent,/Green dash line added/,'the committed result is said on the card');
  plan.querySelector('[data-approve-hide]').click();assert.equal(plan.hidden,true);

  // 7. a failure is said in place, the button works again
  win.LibraryFlow.approve=async o=>{calls.push(o);throw new Error('Netlify said no');};
  const f3=boxOf(cards.F3,'sheet:F3'),b3=f3.querySelector('[data-approve-btn]'),n=calls.length;
  b3.click();await tick();
  assert.equal(calls.length,n+1);assert.match(f3.querySelector('[data-approve-plan]').textContent,/Netlify said no/);assert.equal(b3.disabled,false);

  console.log('Library approve button OK: shown on sheet, set and in-set sheet cards that are not ready, absent when ready or cut or without LibraryFlow, disabled with its reason when blocked, one call per press, plan on the card, card moves by itself');
}finally{win.close();}})().catch(e=>{console.error(e);process.exitCode=1;});
