// The Library says where a sheet stands on the way to Laser cutting: ONE small step rail directly under each sheet (a set of one
// sheet, a set of several, a loose group alike), a '!' button on the step that holds a sheet that is not ready, and nothing at
// all at set level: no set rail, no "what is left" line, no checklist (Paul, 5 Oct: "remove this UI completely from a multi
// sheets set ... each sheet has its own progress timeline"). The real LaserReview and set card code in a DOM, offline fixtures
// only (no cloud, no live set).
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{JSDOM}=require('jsdom');
const root=path.join(__dirname,'../..');
const tick=(n=40)=>new Promise(r=>setTimeout(r,n));
const back=(id,sheetId)=>({poolId:id,sheetId,approvedAt:10,approvedBy:'Paul',verified:{geometry:{ok:true},file:{ok:true}},outputs:{ai:{path:id+'.ai',url:'https://example.com/'+id+'.ai'}}});
// a gold sheet whose every back is saved and whose QR label covers every order (Paul's screenshot: back counter 28 / 28, QR label present)
function sheet(id,{n=28,metal='gold',setId='set1',index=1,base=1000}={}){
  const pool=[...Array(n)].map((_,i)=>`${base+i}_t${i}_1`),orders=pool.map(p=>p.split('_')[0]);
  return {id,metal,setId,runId:'run1',sheetIndex:index,status:'complete',poolIds:pool,placedCount:n,charmCount:n,density:.73,verification:{ok:true},preview:'https://example.com/p.png',outputs:{ai:'https://example.com/f.ai'},
    orders,label:{files:[{path:'qr.png',url:'https://example.com/qr.png',payload:'x',orders}]},backPool:pool.map(p=>back(p,id)),engraving:{},orderReadiness:Object.fromEntries(orders.map(o=>[o,{ready:true}])),updatedAt:1};
}
(async()=>{
  const dom=new JSDOM('<body><main id="libBody"></main></body>',{url:'https://example.test/',runScripts:'outside-only',pretendToBeVisual:true});
  const w=dom.window,d=w.document,jobs=new Map(),opened=[];
  w.matchMedia=()=>({matches:true});
  Object.assign(w,{S:{mode:'library',library:{rows:[],kind:'sets',metal:'all'},cloud:{ok:true}},api:async()=>({sheets:[],sets:[]}),allSheets:()=>[],Orders:{rows:()=>[{key:'2000_x',order:{receiptId:'2000',buyer:{name:'Grace Hopper'}},line:{title:'Star charm'},poolIds:['2000_x_1']}]},
    Engrave:{items:()=>jobs,backsMarkup:()=>''},esc:x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),el:(tag,cls)=>{const e=d.createElement(tag);e.className=cls;return e;},
    Gate:{projectLibraryRecords:x=>x},RoseStock:{},CODE:{gold:'GF',silver:'SS',rose:'RG'},toast:()=>null,CNEmployee:{name:()=> 'Tester'},cors:x=>x,sheetHead:r=>`<div class="h"><span class="nm">Sheet ${r.sheetIndex}</span></div>`,
    pvRatio:()=>'',openLibrarySheet:id=>opened.push(['sheet',id]),openOrderFrom:(btn,rid,o)=>{opened.push(['order',rid,o?.poolId || null]);return true;}});
  w.eval(fs.readFileSync(path.join(root,'charm-nest-orders.js'),'utf8'));w.O=w.CharmNestOrders;
  w.eval(fs.readFileSync(path.join(root,'charm-nest-readiness.js'),'utf8'));
  w.eval(fs.readFileSync(path.join(root,'charm-nest-activity.js'),'utf8'));
  w.eval(fs.readFileSync(path.join(root,'charm-nest-motion.js'),'utf8'));
  const bridge=fs.readFileSync(path.join(root,'charm-nest-bridge.js'),'utf8');
  w.eval(bridge.slice(bridge.indexOf('const LaserReview ='),bridge.indexOf('/* ═══ 22 · Sets — one run')));
  const a=bridge.indexOf('  function libraryGroups('),b=bridge.indexOf('  /** A set card whose completion',a),c=bridge.indexOf('  async function renderLibrary(body, opts)',b),e=bridge.indexOf('  return { releaseIssue',c);
  w.eval('window.Sets=(()=>{'+bridge.slice(a,b)+bridge.slice(c,e)+';return {libraryCard};})();');
  const L=w.LaserReview,R=w.CharmNestReadiness,body=d.getElementById('libBody');
  const addSet=(st,sheets)=>{sheets.forEach(L.record);const card=w.Sets.libraryCard(st,sheets,sheets);L.place(card,L.group(st,sheets).ready,body);return card;};
  const flows=card=>[...card.querySelectorAll('.flowBox')];
  const stepsOf=box=>[...box.querySelectorAll('.flowStep')].map(x=>[x.querySelector('span').textContent,x.classList.contains('done')?'done':x.classList.contains('blocked')?'blocked':'waiting',x.classList.contains('current')]);
  const bangs=box=>[...box.querySelectorAll('[data-issues-open]')];
  const pressed=[];d.addEventListener('click',e=>{const b=e.target.closest?.('[data-issues-open]');if(b)pressed.push([b.dataset.issuesId,b.dataset.issuesStep]);});
  const noSetLevel=card=>{
    assert.equal(card.querySelectorAll(':scope > .flowBox, :scope > .approveBox').length,0,'a set card carries no rail and no Approve button of its own');
    assert.equal(card.querySelectorAll('.flowLine, .flowList, [data-flow-toggle], .flowText, .flowItem').length,0,'no "what is left" line and no checklist anywhere on it');
    assert(!card.querySelector(':scope > .sheetsRow').previousElementSibling.matches('.flowBox, .approveBox'),'nothing sits between the set title and its sheets');
  };
  try{
    L.sections(body);
    // ── Paul's card: one GF sheet, 28 / 28 backs, QR label, but order 2000 has another piece on a Rose Gold sheet whose engraving is not approved
    const gf=sheet('gf1'),rg=sheet('rg1',{n:2,metal:'rose',index:1,setId:'set2',base:2000});
    gf.orders.push('2000');gf.poolIds.push('2000_x_1');gf.placedCount=29;gf.backPool.push(back('2000_x_1','gf1'));gf.label.files[0].orders=gf.orders.slice();
    rg.metalLabel='Rose Gold';rg.poolIds=['2000_y_1','3000_z_1'];rg.orders=['2000','3000'];rg.backPool=[];rg.engraving={'2000_y_1':{needed:true,state:'review',approved:false},'3000_z_1':{needed:false,state:'none',approved:true}};rg.label.files[0].orders=['2000','3000'];rg.placedCount=2;
    const lines=[...gf.poolIds.map(p=>({key:p.replace(/_1$/,''),orderId:p.split('_')[0],state:'written',poolIds:[p]})),{key:'2000_y',orderId:'2000',state:'written',poolIds:['2000_y_1']},{key:'3000_z',orderId:'3000',state:'written',poolIds:['3000_z_1']}];
    const rep=R.orderReports(lines,[gf,rg]);for(const s of [gf,rg])s.orderReadiness=Object.fromEntries(R.orderIds(s).map(o=>[o,rep[o] || {ready:true}]));
    assert.equal(R.sheet(gf).saved,29);assert.equal(R.sheet(gf).stages.qr,true);
    const set1={setId:'set1',seq:1,name:'Set 1',day:'2026-10-03',sheetIds:['gf1'],status:'open'};
    const blocked=addSet(set1,[gf]);
    L.changed();await tick();
    assert.equal(blocked.closest('[data-laser-area]').dataset.laserArea,'pending','the card is In progress');
    noSetLevel(blocked);
    assert.equal(flows(blocked).length,1,'a set of one sheet has exactly one rail: its sheet\'s, not a set\'s on top');
    const box=flows(blocked)[0];assert.equal(box.dataset.flowFor,'sheet:gf1');assert.equal(box.dataset.state,'blocked');
    assert.deepEqual(stepsOf(box),[['Nesting','done',false],['Engraving','done',false],['Back files','done',false],['QR label','done',false],['Order check','blocked',true],['Laser cutting','waiting',false],['Completed','waiting',false]]);
    assert.equal(box.querySelectorAll('.flowStep.done .flowDot svg').length,4,'done steps carry a check');
    assert.match(box.querySelector('.flowStep.blocked .flowDot').textContent,/!/,'the blocked step is marked');
    // the '!' is a real button with the stable hook the issues panel attaches to
    assert.equal(bangs(box).length,1,"one '!' on the sheet that is not ready");
    const bang=bangs(box)[0];assert.equal(bang.tagName,'BUTTON');assert.equal(bang.type,'button');assert.equal(bang.textContent,'!');
    assert.equal(bang.closest('.flowStep').classList.contains('blocked'),true,"on the blocking step");
    assert.deepEqual([bang.dataset.issuesKind,bang.dataset.issuesId,bang.dataset.issuesStep,bang.dataset.sheetId,bang.dataset.step],['sheet','gf1','orders','gf1','orders']);
    assert.equal(bang.getAttribute('aria-haspopup'),'dialog');assert.equal(bang.getAttribute('aria-expanded'),'false');assert.match(bang.getAttribute('aria-label'),/holds this sheet back/);
    assert.equal(bang.classList.contains('flowDot'),true,'it keeps the dot\'s look');
    assert.equal(box.querySelectorAll('.flowLine, .flowList, [data-flow-toggle]').length,0,'no red line and no checklist toggle under the rail');
    assert.match(box.querySelector('.flowNow').textContent,/Order check · step 5 of 7/);
    // where it sits: in the sheet's own article, outside the clickable preview card, under the QR label, above its one Approve button
    const art=box.parentElement;assert(art.classList.contains('librarySheet'),'the rail sits in its sheet\'s article');
    assert(!blocked.querySelector('.libCard').contains(box),'not inside the sheet card (its click opens the sheet)');
    assert.equal(box.previousElementSibling.className,'productionRow','directly under the sheet and its QR label');
    assert.equal(art.querySelectorAll('.flowBox').length,1,'one rail per sheet, never a second');
    // a redraw of the same card gives the same single rail
    const again=w.Sets.libraryCard(set1,[gf],[gf]);body.querySelector('[data-laser-area="pending"] .laserAreaItems').appendChild(again);blocked.remove();L.changed();await tick();
    assert.equal(flows(again).length,1);noSetLevel(again);
    // the Approve button hook: the plain reason opens the issues panel from the sheet's '!' (one press, not two)
    w.LibraryFlow={approve:async()=>({})};
    const steady=sheet('gf9',{n:1,setId:'set9',base:9000});steady.engraving={'9000_t0_1':{needed:true,state:'words',approved:false}};steady.backPool=[];
    const reason=addSet({setId:'set9',seq:9,name:'Set 9',sheetIds:['gf9'],status:'open'},[steady]);L.changed();await tick();
    const why=reason.querySelector('.approveBox [data-approve-reason]');assert(why,'the disabled button\'s reason is a link while the sheet carries a \'!\'');
    pressed.length=0;why.click();assert.deepEqual(pressed,[['gf9','engraving']],"the reason link presses the sheet's '!' exactly once");
    assert.equal(reason.querySelectorAll('.approveBox').length,1,'and the set of one sheet has one Approve button, under the sheet');
    assert.equal(reason.querySelector('.approveBox').previousElementSibling,reason.querySelector('.flowBox'),'directly under its rail');
    reason.remove();delete w.LibraryFlow;
    // real time: the other sheet's engraving is approved elsewhere and the Library learns it: the card moves and re-draws in the same frame
    L.record({...gf,orderReadiness:Object.fromEntries(R.orderIds(gf).map(o=>[o,{ready:true}])),updatedAt:2});L.changed();await tick();
    assert.equal(again.closest('[data-laser-area]').dataset.laserArea,'ready');
    const ready=flows(again)[0];assert.equal(ready.dataset.state,'ready');assert.equal(bangs(ready).length,0,"no '!' once ready");
    assert.deepEqual(stepsOf(ready).map(x=>x[1]),['done','done','done','done','done','waiting','waiting']);assert(ready.querySelector('.flowStep.current.ready span').textContent==='Laser cutting','Laser cutting is the highlighted step');
    assert.equal(ready.querySelectorAll('.flowDot').length,7);assert(![...ready.querySelectorAll('button')].length,'a ready sheet\'s rail has no buttons');
    // an engraving reopened on the page: back to In progress, within a frame; the '!' moves to the Engraving step
    jobs.set('j',{copies:['2000_x_1'],state:'words'});L.changed();await tick();
    assert.equal(again.closest('[data-laser-area]').dataset.laserArea,'pending');
    const wait=flows(again)[0];assert.equal(wait.dataset.state,'waiting');assert.deepEqual(stepsOf(wait).filter(x=>x[2]).map(x=>x[0]),['Engraving']);
    assert.match(wait.querySelector('.flowStep.current').title,/1 back engraving still needs approval/);
    assert.deepEqual(bangs(wait).map(b=>b.dataset.step),['engraving'],"the '!' is on the engraving that waits");
    jobs.clear();L.changed();await tick();assert.equal(again.closest('[data-laser-area]').dataset.laserArea,'ready');
    // the keyboard stays on the '!' when the rail is drawn again (the Library draws a card again after many changes)
    jobs.set('j',{copies:['2000_x_1'],state:'words'});L.changed();await tick();
    const kb=bangs(flows(again)[0])[0];kb.focus();assert.equal(d.activeElement,kb);
    jobs.set('j',{copies:['2000_x_1'],state:'review'});jobs.set('k',{copies:['2000_x_1'],state:'words'});L.record({...gf,updatedAt:3,placedCount:30});L.changed();await tick();
    assert.equal(kb.isConnected,false,'(the rail was drawn again)');assert(d.activeElement && d.activeElement.matches('[data-issues-open]'),'focus is still on the sheet\'s \'!\' after a redraw');
    jobs.clear();L.changed();await tick();
    // ── a set of two sheets: no set rail at all; each sheet carries its own rail, its own '!' and its own Approve button
    const s1=sheet('m1',{n:3,index:1,setId:'set3',base:5000}),s2=sheet('m2',{n:3,index:2,metal:'silver',setId:'set3',base:6000});
    s2.backPool=[s2.backPool[0]];s2.engraving={[s2.poolIds[1]]:{needed:true,state:'review',approved:false},[s2.poolIds[2]]:{needed:true,state:'review',approved:false}};
    const set3={setId:'set3',seq:3,name:'Set 3',day:'2026-10-03',sheetIds:['m1','m2'],status:'open'};
    w.LibraryFlow={approve:async()=>({})};
    const two=addSet(set3,[s1,s2]);L.changed();await tick();
    assert.equal(two.closest('[data-laser-area]').dataset.laserArea,'pending');noSetLevel(two);
    const bs=flows(two);assert.deepEqual(bs.map(x=>x.dataset.flowFor),['sheet:m1','sheet:m2'],'two rails, one per sheet, none for the set');
    assert(bs[0].closest('.librarySheet') && bs[1].closest('.librarySheet') && bs[0].closest('.librarySheet')!==bs[1].closest('.librarySheet') && !bs[0].closest('.libCard'),'each rail sits in its own sheet\'s article, outside the clickable preview card');
    assert.equal(bs[0].previousElementSibling.className,'productionRow','the QR label stays directly under the sheet; the rail follows it');
    assert.deepEqual(stepsOf(bs[1]).filter(x=>x[2]).map(x=>x[0]),['Engraving'],'the sheet that waits shows its own step');
    assert.match(bs[1].querySelector('.flowStep.current').title,/2 back engravings still need approval/);
    assert.deepEqual(bangs(bs[1]).map(b=>[b.dataset.issuesId,b.dataset.step]),[['m2','engraving']]);
    // a finished sheet whose set still waits for another sheet says so on its own rail: the '!' sits on Laser cutting
    assert.deepEqual(stepsOf(bs[0]).filter(x=>x[2]).map(x=>x[0]),['Laser cutting']);
    assert.deepEqual(bangs(bs[0]).map(b=>[b.dataset.issuesId,b.dataset.step]),[['m1','laser']],"the sheet that is done but waits for its set carries the '!' on Laser cutting");
    assert.match(bs[0].querySelector('.flowStep.current').title,/Set 3 is cut together/);
    assert.deepEqual([...two.querySelectorAll('.approveBox')].map(b=>b.dataset.approveFor),['sheet:m2'],'one Approve button, under the sheet that is not ready; none for the set, none for the finished sheet');
    assert.equal(two.querySelector('.approveBox').previousElementSibling,bs[1],'directly under that sheet\'s rail');
    // the hooks the Approve button uses
    pressed.length=0;two.dispatchEvent(new w.CustomEvent('library-checklist-open',{bubbles:true,detail:{kind:'set',id:'set3'}}));
    assert.equal(pressed.length,1,"library-checklist-open presses one '!' of the card");
    pressed.length=0;assert.equal(L.openChecklist(two,{kind:'sheet',id:'m1'}),true);assert.deepEqual(pressed,[['m1','laser']],"openChecklist presses that sheet's '!'");
    pressed.length=0;two.dispatchEvent(new w.CustomEvent('library-checklist-open',{bubbles:true,detail:{kind:'sheet',id:'m2',handled:true}}));assert.equal(pressed.length,0,'an event already handled is not pressed a second time');
    assert.equal(L.openChecklist(two,{kind:'sheet',id:'zzz'}),false,'a sheet that is not on the card has no \'!\'');
    // the rail of a sheet that belongs to the set stays out of the set's own area when it is drawn again
    L.changed();await tick();assert.deepEqual(flows(two).map(x=>x.dataset.flowFor),['sheet:m1','sheet:m2']);noSetLevel(two);
    delete w.LibraryFlow;
    // a set whose sheet is missing: the sheet that is there carries the '!' on Laser cutting (the set cannot be cut), nothing at set level
    const set4={setId:'set4',seq:4,name:'Set 4',sheetIds:['k1','k2']},k1=sheet('k1',{n:2,setId:'set4',base:7000});
    const lost=addSet(set4,[k1]);L.changed();await tick();
    noSetLevel(lost);assert.deepEqual(flows(lost).map(x=>x.dataset.flowFor),['sheet:k1']);
    assert.deepEqual(bangs(flows(lost)[0]).map(b=>b.dataset.step),['laser']);assert.match(flows(lost)[0].querySelector(".flowStep.current").title,/Set 4 is cut together/);
    // a loose group (sheets held for a later set) is not a set: each sheet carries its own rail, and no set box
    const loose=sheet('l1',{n:2,setId:null,base:8000});loose.draft=true;
    const group=addSet({standalone:false,working:true,sheetIds:undefined,status:'held for a later set'},[loose]);L.changed();await tick();
    assert.deepEqual(flows(group).map(x=>x.dataset.flowFor),['sheet:l1']);assert.match(flows(group)[0].querySelector('.flowStep.current').title,/not in a set yet/i);
    assert.equal(bangs(flows(group)[0]).length,0,"a sheet that only waits to join a set has nothing to act on: no '!'");
    // a sheet card of its own (the Library's sheet list): one rail and one button, in the card
    const lone=sheet('z1',{n:1,setId:null,base:9500});lone.engraving={'9500_t0_1':{needed:true,state:'words',approved:false}};lone.backPool=[];L.record(lone);
    const flat=d.createElement('article');flat.className='librarySheet';flat.dataset.laserCard='sheet';flat._laserSheets=['z1'];flat.innerHTML='<div class="libCard" data-id="z1"><span data-sheet-status="z1"></span></div>';
    L.place(flat,false,body);w.LibraryFlow={approve:async()=>({})};L.changed();await tick();
    assert.deepEqual(flows(flat).map(x=>x.dataset.flowFor),['sheet:z1']);assert.equal(bangs(flows(flat)[0]).length,1);assert.equal(flat.querySelectorAll('.approveBox').length,1);
    assert.equal(flat.querySelector('.approveBox').previousElementSibling,flows(flat)[0],'the sheet card: rail, then its one button');
    assert(!/undefined|\[object|NaN/.test(body.textContent),'no stray placeholders in any sentence');
    assert(!/\blines?\b/i.test(body.textContent.replace(/green dash line/gi,'')),'the cards say pieces, never lines');
    console.log("Library why OK: one small rail under each sheet (set of one, set of several, loose group, sheet card), none at set level, '!' button hook on the blocking step with the sheet and step in data attributes, one Approve button under each sheet that is not ready, live re-draw, keyboard focus kept, ready and blocked sheets, missing and loose sets");
  }finally{dom.window.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
