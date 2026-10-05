// Reopening returns already-cut sheets to Laser, even after their intake evidence has changed.
const assert=require('node:assert/strict');
const {start}=require('./bridge-server.cjs'),{seed}=require('./laser-workflow.cjs');
const R=require('../../charm-nest-readiness.js');
const S='Charm_Nest_Sheets',SET='Charm_Nest_Sets',RUN='Charm_Nest_Runs';
(async()=>{
  const srv=await start({receipts:[]});seed(srv);const {st}=srv;
  const ids=['cut-sheet','ready-sheet','pending-sheet'];
  const call=async b=>{const r=await fetch(srv.sorterOrigin+'/.netlify/functions/charmNestLibrary',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b)});return {status:r.status,...await r.json()};};
  const mark=(extra={})=>call({op:'laserDone',kind:'set',id:'set-fixture',stage:'laser',by:'Paul',...extra});
  const status=(extra={})=>call({op:'laserStatus',sheetIds:ids,...extra});
  try{
    st.put(S,'pending-sheet',{verification:{ok:true}});
    assert.equal((await mark()).status,200);
    const old=new Map(ids.map(id=>[id,R.processStamps(st.doc(S,id))]));
    const setSeals=structuredClone(st.doc(SET,'set-fixture').processSeals);
    // A current intake check must not turn a return from Completed into a new intake process.
    // (the pieces are on their sheets, so a stale problem on their own line is nothing; what a sheet waits for is ANOTHER piece of its order: a new line no master has)
    const lines=st.doc(RUN,'run-fixture').lines;
    for(const [k,l] of Object.entries(lines)){l.state='unmatched';l.problems=['unmatchedSku'];lines[l.orderId+'_9']={orderId:l.orderId,state:'unmatched',problems:['unmatchedSku'],sku:'NEW-SKU',poolIds:[]};}
    assert.equal((await mark({done:false})).status,200);
    let r=await status();assert(R.laserGroup(r.sets[0],r.sheets).ready,'the whole reopened set returns to Laser');
    assert(r.sheets.every(s=>s.laser.ready && !s.laserDoneAt));
    assert(r.sheets.every(s=>!R.sheet(s).ready),'the old approval, not new intake data, provides the return path');
    for(const s of r.sheets)assert.deepEqual(s.processSeals,old.get(s.id));
    assert.deepEqual(st.doc(SET,'set-fixture').processSeals,setSeals);
    assert.deepEqual((await call({op:'laserDoneList',countOnly:true})).counts,{sheets:0,sets:0});
    const listed=await call({op:'setList',includeSheets:true,excludeDone:true});assert(listed.sheets.every(s=>s.laser.ready),'a refresh preserves the Laser destination');
    assert.equal((await mark({stage:'progress'})).status,409,'reapproval still requires Laser cutting');

    st.put(SET,'set-fixture',{sheetIds:[...ids,'missing']});assert.equal((await mark()).status,409);
    st.put(SET,'set-fixture',{sheetIds:ids});
    for(const patch of [{archived:true},{draft:true},{solidIncluded:false}]){
      st.put(S,'ready-sheet',patch);r=await status();assert(!R.laserGroup(r.sets[0],r.sheets).ready);assert.equal((await mark()).status,409);
      st.put(S,'ready-sheet',{archived:false,draft:false,solidIncluded:true});
    }
    const unfinished={...st.doc(S,'ready-sheet'),id:'new-sheet',laserDoneAt:null,processSeals:[{id:'blue-only',how:'laserReady',at:10,by:'Paul'}]};
    st.put(S,'new-sheet',unfinished);st.put(SET,'set-fixture',{sheetIds:[...ids,'new-sheet']});
    r=await status();assert(!R.laserGroup(r.sets[0],r.sheets).ready,'new unfinished members cannot borrow the set history');
    assert.equal((await mark()).status,409,'a readiness seal alone cannot bypass the first completion checks');
    st.put(SET,'set-fixture',{sheetIds:ids});

    // Earlier versions erased only the flag; recover their existing timeline without inventing a completion.
    const legacy=st.doc(S,'ready-sheet'),completion=old.get('ready-sheet').find(s=>s.how==='laserDone');
    delete legacy.processSeals;
    st.put('Order_Timeline','legacy-return',{orderId:'3700000101',sheetId:'ready-sheet',setId:'set-fixture',type:'laserDone',at:completion.at,by:completion.by,data:{marked:'set'}});
    r=await status({recordSeals:true,by:'Paul'});assert(R.laserGroup(r.sets[0],r.sheets).ready);
    assert(r.sheets.find(s=>s.id==='ready-sheet').processSeals.some(s=>s.how==='laserDone' && s.at===completion.at && s.by===completion.by));
    const before=new Map(ids.map(id=>[id,structuredClone(st.doc(S,id).processSeals)]));
    r=await mark({by:'Seth'});assert.equal(r.status,200);assert(r.setDone);assert.deepEqual(r.counts,{sheets:3,sets:1});
    for(const id of ids){const seals=st.doc(S,id).processSeals;assert.deepEqual(seals.slice(0,-1),before.get(id));assert.equal(seals.at(-1).how,'laserDone');assert.equal(seals.at(-1).by,'Seth');}
    const final=JSON.stringify(ids.map(id=>st.doc(S,id).processSeals));
    assert.equal((await mark({by:'Seth'})).status,200);assert.equal(JSON.stringify(ids.map(id=>st.doc(S,id).processSeals)),final,'retry does not duplicate completion stamps');
    await mark({kind:'sheet',id:'ready-sheet',done:false});r=await status();assert(R.laserGroup(r.sets[0],r.sheets).ready,'reopening one sheet returns its entire set to Laser');
    assert.equal(r.sheets.filter(s=>s.laserDoneAt).length,2,'other sheets keep their current completion marks');
    console.log('PASS: Completed → Laser → Completed, changed intake, legacy recovery, history, repeat approval and unfinished-member guards');
  }finally{srv.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
