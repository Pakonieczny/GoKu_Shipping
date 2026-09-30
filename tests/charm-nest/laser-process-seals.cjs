// Real handlers: process history survives readiness changes, completion, undo, retries and stale saves.
const assert=require('node:assert/strict');
const {start}=require('./bridge-server.cjs'),{seed}=require('./laser-workflow.cjs');
const S='Charm_Nest_Sheets',SET='Charm_Nest_Sets';
(async()=>{
  const srv=await start({receipts:[]});seed(srv);const {st}=srv;
  const call=async b=>{const r=await fetch(srv.sorterOrigin+'/.netlify/functions/charmNestLibrary',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b)});return {status:r.status,...await r.json()};};
  const status=extra=>call({op:'laserStatus',sheetIds:['ready-sheet'],recordSeals:true,by:'Paul',...extra});
  const done=(id,extra={})=>call({op:'laserDone',kind:'sheet',id,done:true,stage:'laser',by:'Seth',...extra});
  const seals=(id,coll=S)=>st.doc(coll,id).processSeals;
  try{
    const oldAt=Date.now()-20000;
    for(const order of ['123','456'])st.put('Order_Timeline',order+'~laserDone~old',{orderId:order,sheetId:'cut-sheet',setId:'set-fixture',type:'laserDone',at:oldAt,by:'Earlier operator',data:{marked:'sheet'}});
    await status({recordSeals:false});assert.equal(seals('ready-sheet'),undefined,'ordinary status reads do not write');
    let r=await status();assert.equal(r.status,200);assert.equal(r.added.length,1);
    const blue=seals('ready-sheet')[0];assert.equal(blue.how,'laserReady');assert.equal(blue.by,'Paul');assert(blue.at>0);
    const legacy=seals('cut-sheet');assert.equal(legacy.length,3);assert.equal(legacy[0].at,0,'unknown legacy readiness time is not fabricated');assert.equal(legacy[1].by,'Earlier operator');assert.equal(legacy[1].at,oldAt,'an older undone completion is recovered once across order timelines');assert.equal(legacy[2].by,'Paul');
    r=await status();assert.equal(r.added.length,0);assert.deepEqual(seals('ready-sheet'),[blue],'reload does not restamp');
    st.put(S,'ready-sheet',{verification:{ok:false}});await status();assert.deepEqual(seals('ready-sheet'),[blue],'a readiness regression never removes the seal');
    st.put(S,'ready-sheet',{verification:{ok:true}});await status({by:'Seth'});assert.equal(seals('ready-sheet').length,2);assert.equal(seals('ready-sheet')[1].by,'Seth');
    st.put(S,'pending-sheet',{verification:{ok:true}});await status();assert.equal(seals('set-fixture',SET).length,1);
    const before=structuredClone(seals('ready-sheet')),setBlue=structuredClone(seals('set-fixture',SET));
    r=await done('ready-sheet');assert.equal(r.status,200);assert.equal(r.added.filter(x=>x.id==='ready-sheet').length,1);assert.equal(seals('ready-sheet').at(-1).how,'laserDone');
    assert.deepEqual(seals('ready-sheet').slice(0,-1),before);const completed=structuredClone(seals('ready-sheet'));
    await done('ready-sheet');assert.deepEqual(seals('ready-sheet'),completed,'a retried completion creates no duplicate');
    await done('pending-sheet');assert.deepEqual(seals('set-fixture',SET).slice(0,-1),setBlue);assert.equal(seals('set-fixture',SET).at(-1).how,'laserDone');
    const setCompleted=structuredClone(seals('set-fixture',SET));
    const listed=await call({op:'laserDoneList',kind:'sets'});assert.deepEqual(listed.rows[0].processSeals,setCompleted,'Completed includes the whole set history');
    await done('ready-sheet',{done:false});assert.deepEqual(seals('ready-sheet'),completed);assert.deepEqual(seals('set-fixture',SET),setCompleted,'undo keeps the set seals too');
    await status();await done('ready-sheet');assert.deepEqual(seals('ready-sheet').slice(0,completed.length),completed);assert.equal(seals('ready-sheet').length,completed.length+2,'new cycle appends readiness and completion');
    const all=structuredClone(seals('ready-sheet')),allSet=structuredClone(seals('set-fixture',SET));
    await call({op:'putSheet',sheet:{id:'ready-sheet',processSeals:[],processReady:false,laserDoneAt:null}});assert.deepEqual(seals('ready-sheet'),all,'stale sheet save cannot replace history');
    await call({op:'setUpdate',setId:'set-fixture',patch:{processSeals:[],processReady:false,laserDoneAt:null}});assert.deepEqual(seals('set-fixture',SET),allSet,'set patches cannot replace history');
    const fresh=await call({op:'getSheet',id:'ready-sheet'});assert.deepEqual(fresh.sheet.processSeals,all);
    console.log('PASS: immutable process seals, actual signers/times, legacy provenance, repeat cycles, idempotence and protected history');
  }finally{srv.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
