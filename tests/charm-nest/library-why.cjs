// The Library says why a sheet or set has not reached Laser cutting: a step rail on every card, one plain line on each card
// still In progress, and a checklist whose items open the order or the sheet. The real LaserReview and set card code in a DOM,
// offline fixtures only (no cloud, no live set).
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
    assert.equal(flows(blocked).length,1,'a set of one sheet has one box, not a second for its sheet');
    const box=flows(blocked)[0];assert.equal(box.dataset.flowFor,'set:set1');assert.equal(box.dataset.state,'blocked');
    assert.deepEqual(stepsOf(box),[['Nesting','done',false],['Engraving','done',false],['Back files','done',false],['QR label','done',false],['Order check','blocked',true],['Laser cutting','waiting',false],['Completed','waiting',false]]);
    assert.equal(box.querySelectorAll('.flowStep.done .flowDot svg').length,4,'done steps carry a check');
    assert.match(box.querySelector('.flowStep.blocked .flowDot').textContent,/!/,'the blocked step is marked');
    const line=box.querySelector('.flowLine');assert.match(line.textContent,/Order 2000 \(Grace Hopper\) waits for another piece/);assert.match(line.textContent,/RG Sheet 1/);
    assert.equal(line.getAttribute('aria-expanded'),'false');assert.equal(box.querySelector('.flowList').hidden,true,'the checklist starts closed');
    assert.match(box.querySelector('.flowNow').textContent,/Order check · step 5 of 7/);
    assert(!box.closest('.setCard').querySelector('.libCard').contains(box),'the box is not inside the sheet card (its click opens the sheet)');
    assert(box.nextElementSibling.classList.contains('sheetsRow'),'the set box sits under the title and above its sheets');
    // the checklist: tap the line
    line.click();assert.equal(line.getAttribute('aria-expanded'),'true');assert.equal(box.querySelector('.flowList').hidden,false);
    const rows=[...box.querySelectorAll('.flowRow')];assert.deepEqual(rows.map(r=>r.querySelector('b').textContent),['Nesting','Engraving','Back files','QR label','Order check']);
    assert.match(rows[4].querySelector('small').textContent,/1 of 29 orders waits for other pieces/);
    const items=[...box.querySelectorAll('.flowItem')];assert.deepEqual(items.map(i=>[i.dataset.fiKind,i.dataset.fiId]),[['order','2000'],['sheet','rg1']]);
    assert.match(items[0].textContent,/Order 2000 \(Grace Hopper\)/);assert.match(items[0].textContent,/engraving needs approval/);assert.match(items[1].textContent,/RG Sheet 1/);
    items[0].click();items[1].click();assert.deepEqual(opened,[['order','2000',null],['sheet','rg1']],'each item opens its order or sheet with the page\'s own helpers');
    // a redraw of the same card keeps the checklist open (the Library draws its cards again after many changes)
    const again=w.Sets.libraryCard(set1,[gf],[gf]);body.querySelector('[data-laser-area="pending"] .laserAreaItems').appendChild(again);blocked.remove();L.changed();await tick();
    assert.equal(flows(again)[0].querySelector('.flowLine').getAttribute('aria-expanded'),'true','an open checklist stays open when the card is drawn again');
    // real time: the other sheet's engraving is approved elsewhere and the Library learns it: the card moves and re-draws in the same frame
    L.record({...gf,orderReadiness:Object.fromEntries(R.orderIds(gf).map(o=>[o,{ready:true}])),updatedAt:2});L.changed();await tick();
    assert.equal(again.closest('[data-laser-area]').dataset.laserArea,'ready');
    const ready=flows(again)[0];assert.equal(ready.dataset.state,'ready');assert.equal(ready.querySelector('.flowLine'),null,'no "what is left" line once ready');
    assert.deepEqual(stepsOf(ready).map(x=>x[1]),['done','done','done','done','done','waiting','waiting']);assert(ready.querySelector('.flowStep.laser,.flowStep.ready') && ready.querySelector('.flowStep.current.ready span').textContent==='Laser cutting','Laser cutting is the highlighted step');
    // an engraving reopened on the page: back to In progress, the line names the engraving, within a frame
    jobs.set('j',{copies:['2000_x_1'],state:'words'});L.changed();await tick();
    assert.equal(again.closest('[data-laser-area]').dataset.laserArea,'pending');
    const lineAfter=flows(again)[0].querySelector('.flowLine');assert.match(lineAfter.textContent,/1 back engraving still needs approval/);assert.match(lineAfter.textContent,/then Back files/);
    assert.equal(flows(again)[0].dataset.state,'waiting');assert.deepEqual(stepsOf(flows(again)[0]).filter(x=>x[2]).map(x=>x[0]),['Engraving']);
    jobs.clear();L.changed();await tick();assert.equal(again.closest('[data-laser-area]').dataset.laserArea,'ready');
    // approve-button worker's hooks: the plain reason opens the checklist
    L.changed();await tick();
    assert.equal(L.openChecklist(again,{kind:'set',id:'set1'}),false,'a ready card has no checklist to open');
    // ── a set of two sheets: the set says which sheet holds it back, each sheet card carries its own rail
    const s1=sheet('m1',{n:3,index:1,setId:'set3',base:5000}),s2=sheet('m2',{n:3,index:2,metal:'silver',setId:'set3',base:6000});
    s2.backPool=[s2.backPool[0]];s2.engraving={[s2.poolIds[1]]:{needed:true,state:'review',approved:false},[s2.poolIds[2]]:{needed:true,state:'review',approved:false}};
    const set3={setId:'set3',seq:3,name:'Set 3',day:'2026-10-03',sheetIds:['m1','m2'],status:'open'};
    const two=addSet(set3,[s1,s2]);L.changed();await tick();
    assert.equal(two.closest('[data-laser-area]').dataset.laserArea,'pending');
    const bs=flows(two);assert.deepEqual(bs.map(x=>x.dataset.flowFor),['set:set3','sheet:m1','sheet:m2']);
    assert.match(bs[0].querySelector('.flowLine').textContent,/1 of 2 sheets is not ready: SS Sheet 2 \(2 back engravings still need approval\)/);
    assert.match(bs[1].querySelector('.flowLine').textContent,/This sheet is done; Set 3 is cut together and still waits for SS Sheet 2/,'a finished sheet says it waits for its set');
    assert.match(bs[2].querySelector('.flowLine').textContent,/2 back engravings still need approval/);
    assert(bs[1].closest('.librarySheet') && bs[2].closest('.librarySheet') && !bs[1].closest('.libCard'),'sheet rails sit in their sheet\'s article, outside the clickable preview card');
    assert.equal(bs[1].previousElementSibling.className,'productionRow','the QR label stays directly under the sheet; the rail follows it');
    // the hooks the Approve button uses
    bs[2].querySelector('.flowLine').click();w.document.dispatchEvent(new w.CustomEvent('library-checklist-open',{detail:{kind:'set',id:'set3'}}));
    two.dispatchEvent(new w.CustomEvent('library-checklist-open',{bubbles:true,detail:{kind:'set',id:'set3'}}));
    assert.equal(bs[0].querySelector('.flowLine').getAttribute('aria-expanded'),'true','library-checklist-open opens the checklist');
    assert.equal(L.openChecklist(two,{kind:'sheet',id:'m1'}),true);assert.equal(bs[1].querySelector('.flowLine').getAttribute('aria-expanded'),'true');
    // a set whose sheet is missing says so
    const set4={setId:'set4',seq:4,name:'Set 4',sheetIds:['k1','k2']},k1=sheet('k1',{n:2,setId:'set4',base:7000});
    const lost=addSet(set4,[k1]);L.changed();await tick();
    assert.match(flows(lost)[0].querySelector('.flowLine').textContent,/1 sheet of this set cannot be found/);
    // a loose group (sheets held for a later set) is not a set: each sheet carries its own box and no set box
    const loose=sheet('l1',{n:2,setId:null,base:8000});loose.draft=true;
    const group=addSet({standalone:false,working:true,sheetIds:undefined,status:'held for a later set'},[loose]);L.changed();await tick();
    assert.deepEqual(flows(group).map(x=>x.dataset.flowFor),['sheet:l1']);assert.match(flows(group)[0].querySelector('.flowLine').textContent,/not in a set yet/i);
    assert(!/undefined|\[object|NaN/.test(body.textContent),'no stray placeholders in any sentence');
    console.log('Library why OK: step rail on set and sheet cards, one plain line In progress, checklist items open the order or sheet, live re-draw, ready and blocked cards, missing and loose sets, hooks for the Approve button');
  }finally{dom.window.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
