// CharmNestReadiness.explain: why a sheet or set is not in Laser cutting yet, in plain words. Offline fixtures only.
const assert=require('node:assert/strict');
const R=require('../../charm-nest-readiness.js');
const clone=x=>JSON.parse(JSON.stringify(x));
const back=(id,sheetId)=>({poolId:id,sheetId,approvedAt:10,approvedBy:'Paul',verified:{geometry:{ok:true},file:{ok:true}},outputs:{ai:{path:id+'.ai',url:'https://example.com/'+id+'.ai'}}});
// a sheet of n pieces, one per order 1001.. (pool id "<order>_<line>_1"), every back approved and saved, QR label for all, orders checked
function sheet(id,{n=3,metal='gold',setId='set1',index=1,seq=1}={}){
  const pool=[...Array(n)].map((_,i)=>`${1000+i+(index-1)*100}_t${i}_1`),orders=pool.map(p=>p.split('_')[0]);
  return {id,metal,setId,runId:'run1',sheetIndex:index,status:'complete',poolIds:pool,placedCount:n,verification:{ok:true},preview:'https://example.com/p.png',outputs:{ai:'https://example.com/f.ai'},
    orders,label:{files:[{path:'qr.png',url:'https://example.com/qr.png',payload:'x',orders}]},backPool:pool.map(p=>back(p,id)),engraving:{},
    orderReadiness:Object.fromEntries(orders.map(o=>[o,{ready:true}]))};
}
const states=e=>Object.fromEntries(e.steps.map(s=>[s.key,s.state]));
const step=(e,k)=>e.steps.find(s=>s.key===k);
const rows=[{key:'1000_t0',order:{receiptId:'1000',buyer:{name:'Ada Lovelace'}},line:{title:'Heart charm, personalised'},poolIds:['1000_t0_1']}];

// 1. all ready
const ok=sheet('s1');
let e=R.explain(ok);
assert.equal(e.kind,'sheet');assert.equal(e.id,'s1');assert.equal(e.ready,true);assert.equal(e.done,false);assert.equal(e.step,'laser');
assert.deepEqual(e.steps.map(s=>s.key),['nesting','engraving','backFiles','qr','orders','laser','completed']);
assert.deepEqual(e.steps.map(s=>s.label),['Nesting','Engraving','Back files','QR label','Order check','Laser cutting','Completed']);
assert.deepEqual(states(e),{nesting:'done',engraving:'done',backFiles:'done',qr:'done',orders:'done',laser:'waiting',completed:'waiting'});
assert.equal(e.steps.filter(s=>s.current).length,1);assert.equal(step(e,'laser').current,true);
assert.match(e.nextText,/ready for laser cutting/i);
assert.equal(e.ready,R.laserSheet(ok).ready,'ready is the gate\'s own answer');
assert.equal(R.explain({...ok,laserDoneAt:5}).done,true);assert.equal(R.explain({...ok,laserDoneAt:5}).step,'completed');

// 2. engraving waiting: two of three backs not approved yet (one is plain, one is in words)
const eng=sheet('s2');eng.backPool=[eng.backPool[0]];eng.engraving={'1001_t1_1':{needed:true,state:'review',approved:false},'1002_t2_1':{needed:true,state:'words',approved:false}};
e=R.explain(eng,{rows:[...rows,{key:'1001_t1',order:{receiptId:'1001'},line:{title:'Moon charm'},poolIds:['1001_t1_1']}]});
assert.equal(e.ready,false);assert.equal(e.step,'engraving');assert.equal(states(e).engraving,'waiting');assert.equal(states(e).nesting,'done');
assert.equal(step(e,'engraving').items.length,2);assert(step(e,'engraving').items.every(i=>i.kind==='charm' && i.id && i.label && i.why));
assert.match(step(e,'engraving').items[0].label,/Order 1001/);assert.match(step(e,'engraving').items[0].label,/Moon charm/);
assert.match(e.nextText,/2 back engravings still need approval/);assert.match(e.nextText,/then Back files/);
assert.equal(R.sheet(eng).waiting,2,'the checklist counts what the gate counts');

// 3. back files unsaved: approved, no saved file yet (and a failed file check is a problem, not a wait)
const bf=sheet('s3');bf.backPool[1]={...bf.backPool[1],outputs:null};bf.backPool[2]={...bf.backPool[2],verified:{geometry:{ok:true},file:{ok:false}}};
e=R.explain(bf);
assert.equal(e.step,'backFiles');assert.equal(states(e).engraving,'done');assert.equal(states(e).backFiles,'blocked','a back whose saved file failed its check needs a person');
assert.equal(step(e,'backFiles').items.length,2);assert.match(step(e,'backFiles').items.map(i=>i.why).join(' '),/still being saved/);assert.match(step(e,'backFiles').items.map(i=>i.why).join(' '),/failed its check/);
assert.match(e.nextText,/2 approved back files still need saving/);
const bf2=sheet('s3b');bf2.backPool[1]={...bf2.backPool[1],outputs:null};
assert.equal(R.explain(bf2).steps.find(s=>s.key==='backFiles').state,'waiting','a file still being saved only waits');
assert.equal(R.sheet(bf2).saved,2);

// 4. QR missing, incomplete, and not covering every order
const qr=sheet('s4');qr.label={files:[]};
e=R.explain(qr);assert.equal(e.step,'qr');assert.equal(states(e).qr,'waiting');assert.match(step(e,'qr').detail,/No QR label/);assert.match(e.nextText,/QR label has not been made/);
const qr2=sheet('s4b');qr2.label.files[0].orders=['1000'];
e=R.explain(qr2);assert.equal(states(e).qr,'blocked');assert.deepEqual(step(e,'qr').items.map(i=>i.id),['1001','1002']);assert(step(e,'qr').items.every(i=>i.kind==='order' && /Order 100\d/.test(i.label)));
assert.match(e.nextText,/must also cover 2 orders/);
const qr3=sheet('s4c');delete qr3.label.files[0].payload;assert.equal(states(R.explain(qr3)).qr,'blocked');
assert.equal(R.sheet(qr2).stages.qr,false);

// 5. Paul's sheet: every back saved and a QR label, but an order has another line on another sheet that is not ready
const a=sheet('gf1',{n:3,index:1}),b=sheet('rg1',{n:2,metal:'rose',index:1,setId:'set2'});
a.orders.push('2000');a.poolIds.push('2000_x_1');a.placedCount=4;a.backPool.push(back('2000_x_1','gf1'));a.label.files[0].orders=a.orders.slice();delete a.orderReadiness;
b.metalLabel='Rose Gold';b.poolIds=['2000_y_1','3000_z_1'];b.orders=['2000','3000'];b.backPool=[];b.engraving={'2000_y_1':{needed:true,state:'review',approved:false},'3000_z_1':{needed:false,state:'none',approved:true}};b.label.files[0].orders=['2000','3000'];b.placedCount=2;delete b.orderReadiness;
const lines=[
  ...a.poolIds.map(p=>({key:p.replace(/_1$/,''),orderId:p.split('_')[0],state:'written',poolIds:[p]})),
  {key:'2000_y',orderId:'2000',state:'written',poolIds:['2000_y_1']},
  {key:'3000_z',orderId:'3000',state:'written',poolIds:['3000_z_1']}
];
const reports=R.orderReports(lines,[a,b]);
assert.equal(reports['2000'].ready,false);assert.equal(reports['2000'].sheetId,'rg1');assert.equal(reports['2000'].stage,'approval');assert.match(reports['2000'].why,/engraving needs approval/,'the words of the report are unchanged');
a.orderReadiness=Object.fromEntries(R.orderIds(a).map(o=>[o,reports[o] || {ready:true}]));
const names=[{key:'2000_x',order:{receiptId:'2000',buyer:{name:'Grace Hopper'}},line:{title:'Star'},poolIds:['2000_x_1']}];
e=R.explain(a,{rows:names});
assert.equal(R.sheet(a).stages.orders,false);assert.deepEqual([R.sheet(a).saved,R.sheet(a).required],[4,4],'4 / 4 backs saved');
assert.equal(e.ready,false);assert.equal(e.step,'orders');assert.deepEqual(states(e),{nesting:'done',engraving:'done',backFiles:'done',qr:'done',orders:'blocked',laser:'waiting',completed:'waiting'});
const oi=step(e,'orders').items;
assert.deepEqual(oi.map(i=>[i.kind,i.id]),[['order','2000'],['sheet','rg1']],'the order and the sheet that holds it');
assert.match(oi[0].label,/Order 2000 \(Grace Hopper\)/);assert.match(oi[0].why,/other piece is on RG Sheet 1/);assert.match(oi[0].why,/engraving needs approval/);
assert.match(oi[1].label,/RG Sheet 1/);assert.match(e.nextText,/Order 2000 \(Grace Hopper\) waits for another piece/);
assert.match(step(e,'orders').detail,/1 of 4 orders wait/);
// orders not read at all is a wait that says so, not 67 identical lines
const unread=clone(a);delete unread.orderReadiness;e=R.explain(unread);assert.equal(e.step,'orders');assert.equal(step(e,'orders').items.length,1);assert.match(step(e,'orders').detail,/not been checked yet/);
// a piece on a draft sheet says it is not in a set yet
const draft=clone(b);draft.draft=true;const rep2=R.orderReports(lines,[a,{...draft,backPool:[back('2000_y_1','rg1')],engraving:{'2000_y_1':{needed:true,state:'approved',approved:true},'3000_z_1':{needed:false,state:'none',approved:true}}}]);
assert.match(rep2['2000'].why,/not in a set yet/);

// 5b. (Paul, 5 Oct: "48 of 48 orders are awaiting on something, which is impossible") the Order check lists only an order that has ANOTHER piece holding it
// back. A sheet whose own engravings are unapproved does not wait for itself, and an order of one piece is never listed.
{
  const own=sheet('s9',{n:3,index:9});own.backPool=[];own.engraving=Object.fromEntries(own.poolIds.map(p=>[p,{needed:true,state:'review',approved:false}]));
  const ls=own.poolIds.map(p=>({key:p.replace(/_1$/,''),orderId:p.split('_')[0],state:'written',poolIds:[p]})),reps=R.orderReports(ls,[own]);
  assert(Object.values(reps).every(r=>!r.ready),'every piece sits on a sheet that is not ready');
  own.orderReadiness=Object.fromEntries(R.orderIds(own).map(o=>[o,R.forSheet(reps[o],'s9')]));
  const ex=R.explain(own);
  assert.equal(states(ex).orders,'done');assert.equal(step(ex,'orders').items.length,0);assert.match(step(ex,'orders').detail,/All 3 orders/);assert.equal(ex.step,'engraving');assert.match(ex.nextText,/3 back engravings still need approval/);
  assert.deepEqual(R.issues(own,{}).map(i=>[i.step,i.key]),[['engraving','approvalsNeeded']],'issues(): the sheet\'s own blocker, no order');
  // the same map handed over unread (the page's whole-order answer) reads the same from this sheet
  const raw=clone(own);raw.orderReadiness=reps;assert.equal(R.sheet(raw).stages.orders,true);assert.equal(states(R.explain(raw)).orders,'done');
}

// 5c. (Paul, round 7: "it's on both sheets and both sheets are in the same set") the very order of 5, with its two pieces on sheets of the SAME set: the order is fine.
// A set advances as one, so RG Sheet 1 not being ready is the SET's wait (said once, under the Approve button, in no sheet's list), never a wait of the order; the same order split between two sets still is one.
{
  const same=clone(b);same.setId='set1';
  const sets1={setId:'set1',seq:1,name:'Set 1',sheetIds:['gf1','rg1']},repsS=R.orderReports(lines,[a,same]);
  const ga=clone(a);ga.orderReadiness=Object.fromEntries(R.orderIds(ga).map(o=>[o,R.forSheet(repsS[o],'gf1',R.setOf(ga))]));
  assert.equal(R.sheet(ga).stages.orders,true,'the order spread over two sheets of its own set is no order wait');
  assert.equal(R.sheet(ga).ready,true,'GF Sheet 1 itself has nothing holding it');
  e=R.explain(ga,{rows:names,set:sets1,sheets:[ga,same]});
  assert.equal(states(e).orders,'done');assert.equal(step(e,'orders').items.length,0,'no order is listed as waiting');assert.match(step(e,'orders').detail,/All 4 orders/);
  assert.equal(e.step,'laser');assert.equal(e.ready,false,'the set is cut together, so the sheet still waits for its mate');assert.equal(step(e,'laser').items[0].id,'rg1');
  const same1=R.issues(ga,{rows:names,set:sets1,sheets:[ga,same]});
  assert.deepEqual(same1,[],'GF Sheet 1\'s list is empty (round 8: "only related items to that particular sheet"): no order issue, and the set\'s wait for RG Sheet 1 is not an entry of it');
  assert.match(R.setGate(sets1,[ga,same]).reason,/^RG Sheet 1 · back engravings \d+ of \d+$/,'the set\'s wait is said once, by the gate (the reason under the grey Approve button)');
  // the same shop with RG Sheet 1 in ANOTHER set: the order is split between two sets, a real issue worded as the split it is
  const apart=clone(b);apart.setId='set2';apart.setSeq=2;const repsX=R.orderReports(lines,[a,apart]);
  const gx=clone(a);gx.setSeq=1;gx.orderReadiness=Object.fromEntries(R.orderIds(gx).map(o=>[o,R.forSheet(repsX[o],'gf1',R.setOf(gx))]));
  assert.equal(R.sheet(gx).stages.orders,false);
  const x=R.issues(gx,{rows:names}).filter(i=>i.step==='orders');
  assert.deepEqual(x.map(i=>[i.orderId,i.key,i.split===true]),[['2000','otherSheetNotReady',true]]);assert.match(x[0].why,/^Split between Set 1 and Set 2/);assert.doesNotMatch(x[0].why,/\bwaits?\b/i);
}

// 6. a set with one blocked member (and its pieces named)
const m1=sheet('m1',{n:2,index:1}),m2=sheet('m2',{n:2,index:2,metal:'silver'});m2.backPool=[m2.backPool[0]];m2.engraving={[m2.poolIds[1]]:{needed:true,state:'review',approved:false}};
const set={setId:'set1',seq:1,name:'Set 1',sheetIds:['m1','m2']};
e=R.explain(set,{sheets:[m1,m2]});
assert.equal(e.kind,'set');assert.equal(e.id,'set1');assert.equal(e.ready,false);assert.equal(e.ready,R.laserGroup(set,[m1,m2]).ready);assert.equal(e.step,'engraving');
assert.equal(states(e).nesting,'done');assert.equal(states(e).engraving,'waiting');
assert.deepEqual(step(e,'engraving').items.map(i=>[i.kind,i.id,i.label]),[['sheet','m2','SS Sheet 2']],'the sheet that holds the set back, by name');
assert.match(step(e,'engraving').detail,/1 of 2 sheets/);
assert.match(e.nextText,/1 of 2 sheets is not ready: SS Sheet 2 \(1 back engraving still needs approval\)/);
assert.equal(R.explain({...set},{sheets:[m1,sheet('m2',{n:2,index:2,metal:'silver'})]}).ready,true,'both sheets ready: the set is ready');
assert.match(R.explain({...set},{sheets:[m1,sheet('m2',{n:2,index:2})]}).nextText,/ready for laser cutting/i);
// a sheet missing from the set, an archived one still listed, a 14K sheet not included
e=R.explain(set,{sheets:[m1]});assert.equal(e.ready,false);assert.equal(R.laserGroup(set,[m1]).ready,false);assert.equal(states(e).nesting,'blocked');
assert.match(step(e,'nesting').items[0].why,/cannot be found/);assert.match(e.nextText,/1 sheet of this set cannot be found/);
e=R.explain(set,{sheets:[m1,{...sheet('m2',{n:2,index:2}),archived:true}]});assert.equal(e.ready,false);assert.match(step(e,'nesting').items[0].why,/removed \(archived\)/);
const solid=sheet('k1',{n:2,index:3,metal:'gold14k'});solid.solidIncluded=false;
e=R.explain({...set,sheetIds:['m1','k1']},{sheets:[m1,solid]});assert.equal(e.ready,false);assert.equal(states(e).nesting,'blocked');assert.match(step(e,'nesting').items[0].why,/Include/);
// a single-sheet set shows that sheet's own checklist directly (the set card has no second box)
const lone=clone(eng);e=R.explain({setId:'set1',seq:1,name:'Set 1',sheetIds:['s2']},{sheets:[lone]});
assert.equal(e.step,'engraving');assert.equal(step(e,'engraving').items.length,2);assert(step(e,'engraving').items.every(i=>i.kind==='charm'));
// a set cut on the laser is done
assert.equal(R.explain({...set,laserDoneAt:9},{sheets:[m1,sheet('m2',{n:2,index:2})]}).done,true);

// a sheet in a set is cut with its set: ready alone, held by its mate
e=R.explain(m1,{set,sheets:[m1,m2]});
assert.equal(R.sheet(m1).ready,true);assert.equal(e.ready,false);assert.equal(e.step,'laser');assert.equal(states(e).orders,'done');
assert.match(step(e,'laser').detail,/Set 1 is cut together/);assert.equal(step(e,'laser').items[0].id,'m2');assert.match(e.nextText,/This sheet is done; Set 1 is cut together and still waits for SS Sheet 2/);
assert.equal(R.explain(m1,{setMissing:true}).ready,false);assert.equal(R.explain(m1,{set,sheets:[m1,sheet('m2',{n:2,index:2,metal:'silver'})]}).ready,true);
// a sheet cut before keeps that approval when reopened; one with its layout flagged does not
const cut=clone(eng);cut.laserDoneAt=0;cut.processSeals=[{how:'laserDone',at:50}];e=R.explain(cut);assert.equal(e.ready,true);assert.equal(R.laserSheet(cut).ready,true);
const flagged=sheet('f1');flagged.verification={ok:false};e=R.explain(flagged);assert.equal(e.step,'nesting');assert.equal(states(e).nesting,'blocked');
const unverified=sheet('f2');unverified.verification=null;assert.equal(states(R.explain(unverified)).nesting,'waiting');
const rose=sheet('f3',{metal:'rose'});rose.roseStockId='stock';e=R.explain(rose);assert.equal(states(e).nesting,'blocked');assert.match(step(e,'nesting').items[0].why,/green dash line/);
assert.equal(R.explain({id:'x',poolIds:[]}).ready,false,'an empty record never reads as ready');
const held=sheet('h1');held.laserHold={at:5,by:'Paul'};e=R.explain(held);assert.equal(R.laserSheet(held).ready,false);assert.equal(e.ready,false);assert.equal(e.step,'nesting');assert.equal(states(e).nesting,'blocked');assert.match(step(e,'nesting').items[0].why,/Held back from Laser cutting/);

// every item carries a label and a reason; no ids without a label
for(const x of [eng,bf,qr2,a,m1]){const r=R.explain(x,{rows:names});for(const s of r.steps)for(const i of s.items){assert(['order','charm','sheet'].includes(i.kind));assert(i.id && i.label && i.why,JSON.stringify(i));}}
console.log('explain OK: all ready, order check lists only other pieces, engraving waiting, back files unsaved, QR missing, order held by another sheet, set with a blocked member, missing/archived/excluded members, sheet held by its set');
