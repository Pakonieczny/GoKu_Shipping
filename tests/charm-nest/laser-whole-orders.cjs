// Real handlers: a ready piece cannot carry an unfinished order into Laser cutting.
const assert=require('node:assert/strict');
const {start}=require('./bridge-server.cjs'),{seed}=require('./laser-workflow.cjs');
const R=require('../../charm-nest-readiness.js');
const S='Charm_Nest_Sheets',SET='Charm_Nest_Sets',RUN='Charm_Nest_Runs';
(async()=>{
  const srv=await start({receipts:[]});seed(srv);const {st}=srv;
  const call=async b=>{const r=await fetch(srv.sorterOrigin+'/.netlify/functions/charmNestLibrary',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b)});return {status:r.status,...await r.json()};};
  const status=()=>call({op:'laserStatus',sheetIds:['ready-sheet'],recordSeals:true,by:'Paul'});
  const complete=()=>call({op:'laserDone',kind:'sheet',id:'ready-sheet',stage:'laser',by:'Seth'});
  const order='3700000101',key=order+'_2',pool=key+'_1',other='3700000199';
  const run=st.doc(RUN,'run-fixture'),ready=st.doc(S,'ready-sheet');
  st.put(S,'pending-sheet',{verification:{ok:true}});
  st.put(SET,'set-fixture',{orders:{[order]:{held:{why:'line is pooled'}}}});
  try{
    let r=await status();assert(R.laserGroup(r.sets[0],r.sheets).ready,'stale set notes do not override saved order evidence');
    const blue=structuredClone(st.doc(S,'ready-sheet').processSeals);
    run.lines[key]={orderId:order,state:'unmatched',problems:['unmatchedSku'],poolIds:[]};
    // A different held order in the same run is irrelevant to this set.
    run.lines['9999999999_1']={orderId:'9999999999',state:'pooled',poolIds:[]};
    r=await status();let sheet=r.sheets.find(s=>s.id==='ready-sheet');
    assert.equal(sheet.laser.ready,false);assert.match(sheet.orderReadiness[order].why,/SKU not in a master/);
    assert.equal(R.laserGroup(r.sets[0],r.sheets).ready,false);
    assert.equal((await complete()).status,409,'the API cannot complete an order with an unmatched item');
    assert.deepEqual(st.doc(S,'ready-sheet').processSeals,blue,'a newly discovered blocker never erases an earlier seal');
    run.lines[key]={orderId:order,state:'written',quantity:1,poolIds:[pool],engraveCandidate:false};
    r=await status();assert.match(r.sheets.find(s=>s.id==='ready-sheet').orderReadiness[order].why,/not on a saved sheet/);
    const second={...ready,id:'rose-other',setId:'other-set',metal:'rose',poolIds:[pool,other+'_1_1'],orders:[order,other],placedCount:2,draft:true,verification:{ok:true},label:{files:[{path:'rose-qr.png',url:'https://example.test/qr',payload:order,orders:[order,other]}]}};
    st.put(S,'rose-other',second);st.put(SET,'other-set',{setId:'other-set',sheetIds:['rose-other']});
    // Another order sharing that sheet has an archived plain piece; its evidence must also hydrate.
    const archived={[other+'_1']:{orderId:other,state:'committed',quantity:1,poolIds:[other+'_1_1'],engraveCandidate:false}};
    run.lineArchive={parts:1};st.put('Charm_Nest_Run_Lines','part1',{runId:'run-fixture',at:1,seq:0,orders:[other],keys:Object.keys(archived),json:JSON.stringify(archived)});
    r=await status();assert.equal(r.sheets.find(s=>s.id==='ready-sheet').laser.ready,false,'a draft on another metal blocks the order');
    st.put(S,'rose-other',{draft:false,label:{files:[]}});
    r=await status();assert.match(r.sheets.find(s=>s.id==='ready-sheet').orderReadiness[order].why,/QR labels/);
    assert.equal((await complete()).status,409);
    st.put(S,'rose-other',{label:second.label});
    r=await status();assert(R.laserGroup(r.sets[0],r.sheets).ready,'the complete order becomes ready, including the dependent sheet’s other plain piece');
    assert.equal(st.doc(S,'ready-sheet').processSeals.length,blue.length+1,'readiness returning appends a new seal');
    // (Paul, 5 Oct) where a piece sits is read from the sheets, not from its line's stored state: a line still 'pooled' whose piece is on a saved sheet holds nothing
    const plain=()=>call({op:'laserStatus',sheetIds:['ready-sheet']}),mine=r=>r.sheets.find(s=>s.id==='ready-sheet').orderReadiness[order];
    run.lines[order+'_1'].state='pooled';assert.equal(mine(await plain()).ready,true,'a piece on this sheet is not waited for because its line says pooled');
    run.lines[order+'_1'].state='written';
    run.lines[key].state='pooled';run.lines[key].problems=['unmatchedSku'];assert.equal(mine(await plain()).ready,true,'a stale problem on a piece that is on a ready sheet (rose-other) holds nothing');
    run.lines[key].state='written';delete run.lines[key].problems;
    // another line of the order that no master has: the order has two pieces, and the second holds it
    run.lines[order+'_9']={orderId:order,state:'unmatched',problems:['unmatchedSku'],sku:'ZZ',poolIds:[]};
    assert.equal(mine(await plain()).ready,false);assert.equal(mine(await plain()).key,'unmatched');
    delete run.lines[order+'_9'];assert.equal(mine(await plain()).ready,true);
    run.lines[key].hold='Check customer changes';r=await status();assert.match(r.sheets.find(s=>s.id==='ready-sheet').orderReadiness[order].why,/customer changes/);
    delete run.lines[key].hold;
    run.lines[order+'_3']={orderId:order,state:'committed',sku:'CHAIN-ONLY',poolIds:[]};
    st.put('Charm_Sku_NoDesign','chain',{sku:'CHAIN-ONLY'});
    run.lines[order+'_4']={orderId:order,state:'gone',poolIds:[],problems:['unmatchedSku']};
    r=await status();assert(R.laserGroup(r.sets[0],r.sheets).ready,'explicit no-design rules and cancelled lines need no cutting');
    run.lines[order+'_5']={orderId:order,state:'committed',sku:'LONGER_CHAIN_5682',poolIds:[]};
    st.put('Charm_Nest_CustomRead',order+'_5',{latest:'old-read',reads:{'old-read':{kind:'chainOnly',confidence:.95}}});
    run.lines[order+'_6']={orderId:order,state:'committed',sku:'CUSTOM-BY-HAND',poolIds:[]};
    st.put('Charm_Custom_Orders',order+'_6',{state:'complete',completedAt:100,completedBy:'Paul'});
    r=await status();assert(R.laserGroup(r.sets[0],r.sheets).ready,'older chain-only assessments and actual custom completion seals remain valid');
    st.put('Charm_Custom_Orders',order+'_6',{state:'open'});assert.equal((await complete()).status,409,'reopening custom work blocks the whole order');
    st.put('Charm_Custom_Orders',order+'_6',{state:'complete'});
    assert.equal((await complete()).status,200);
    console.log('PASS: whole-order laser gate, unmatched/missing items, cross-metal draft and QR gates, dependent archived decisions, current notices, no-design exemptions and retained seals');
  }finally{srv.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
