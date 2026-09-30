// Real Library API over the in-memory shop. No production records or services are used.
const assert = require('node:assert/strict');
const { start } = require('./bridge-server.cjs');
const R = require('../../charm-nest-readiness.js');
const S='Charm_Nest_Sheets', SET='Charm_Nest_Sets', RUN='Charm_Nest_Runs';
function seed(srv) {
  const {st}=srv, now=Date.now(), ts={toMillis:()=>now}, day='2026-09-30';
  const image='data:image/svg+xml,'+encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="300" height="220"><rect width="300" height="220" fill="#fffdf8"/><g fill="none" stroke="#333" stroke-width="1.2">'+Array.from({length:35},(_,i)=>`<circle cx="${22+(i%7)*42}" cy="${22+Math.floor(i/7)*42}" r="17"/><circle cx="${22+(i%7)*42}" cy="${9+Math.floor(i/7)*42}" r="2"/>`).join('')+'</g></svg>');
  const lines={};
  for(const [i,id] of ['cut-sheet','ready-sheet','pending-sheet'].entries()) {
    const order=String(3700000100+i),pool=order+'_1_1';
    lines[order+'_1']={orderId:order,state:'written',quantity:1,poolIds:[pool],engraveCandidate:false};
    st.put(S,id,{id,setId:'set-fixture',setSeq:1,sheetIndex:i+1,runId:'run-fixture',metal:['silver','gold','gold14k'][i],day,status:'complete',placedCount:1,charmCount:1,density:.72,stock:{wIn:6,hIn:4.5},poolIds:[pool],orders:[order],listings:[String(1718000+i)],verification:{ok:true},outputs:{ai:{path:id+'.ai',url:srv.sorterOrigin+'/'+id+'.ai'},preview:{path:id+'.png',url:image}},label:{files:[{path:id+'-qr.png',url:image,payload:order,orders:[order]}]},updatedAt:ts,createdAt:ts,...(i===0?{laserDoneAt:now-10000,laserDoneBy:'Paul'}:{}),...(i===2?{verification:{ok:false}}:{})});
  }
  st.put(RUN,'run-fixture',{runId:'run-fixture',lines});
  st.put(SET,'set-fixture',{setId:'set-fixture',seq:1,day,runId:'run-fixture',sheetIds:['cut-sheet','ready-sheet','pending-sheet'],materials:['silver','gold','gold14k'],orders:{},status:'labelled',updatedAt:ts,createdAt:ts});
}
async function main(){
  const srv=await start({receipts:[]});seed(srv);
  if(process.argv.includes('--serve')) {console.log(srv.sorterOrigin+'/charm-nest-1.html#library');return;}
  const {st}=srv;
  const call=async b=>{const r=await fetch(srv.sorterOrigin+'/.netlify/functions/charmNestLibrary',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b)});return {status:r.status,...await r.json()};};
  const complete=(kind,id,extra={})=>call({op:'laserDone',kind,id,by:'Test operator',stage:'laser',...extra});
  try {
    let current=await call({op:'listSheets',excludeDone:true});assert.equal(current.sheets.length,3);assert(current.sheets.find(s=>s.id==='cut-sheet').laserSetPending);
    assert.equal((await call({op:'laserDoneList'})).rows.length,0);
    assert.deepEqual((await call({op:'laserDoneList',countOnly:true})).counts,{sheets:0,sets:0});
    let found=await call({op:'findSheets',q:'3700000100'});assert.equal(found.sheets[0].id,'cut-sheet');assert.equal(found.rows.length,0);
    assert.equal((await complete('sheet','ready-sheet')).status,409,'no sheet skips its unfinished set');
    assert.equal((await call({op:'laserStatus',sheetIds:['ready-sheet']})).sheets.length,3,'single-sheet status includes its set for the same stage gate');
    assert.equal((await complete('set','set-fixture')).status,409,'set shortcut cannot bypass readiness');
    assert(!st.doc(S,'ready-sheet').laserDoneAt);
    st.put(S,'pending-sheet',{verification:{ok:true}});
    assert.equal((await complete('sheet','ready-sheet',{stage:'progress'})).status,409,'wrong section rejected');
    let r=await complete('sheet','ready-sheet');assert.equal(r.status,200);assert.equal(r.setDone,false);assert.equal(r.counts.sheets,0);
    const first=st.doc(S,'cut-sheet').laserDoneAt,second=st.doc(S,'ready-sheet').laserDoneAt;
    r=await complete('sheet','ready-sheet');assert.equal(r.status,200);assert.equal(st.doc(S,'ready-sheet').laserDoneAt,second,'retry preserves stamp');
    current=await call({op:'setList',includeSheets:true,excludeDone:true});assert.equal(current.sheets.length,3);
    assert.equal((await call({op:'laserDoneList',limit:1})).rows.length,0,'partial sheets never enter Completed');
    r=await complete('sheet','pending-sheet');assert(r.setDone);assert.deepEqual(r.counts,{sheets:3,sets:1});assert.equal(st.doc(S,'cut-sheet').laserDoneAt,first);
    assert.equal((await call({op:'listSheets',excludeDone:true})).sheets.length,0);
    assert.equal((await call({op:'laserDoneList',kind:'sets'})).rows[0].sheets.length,3);
    let seen=[],cursor;do{const p=await call({op:'laserDoneList',limit:1,cursor});seen.push(...p.rows.map(r=>r.id));cursor=p.next;}while(cursor);assert.equal(new Set(seen).size,3);
    r=await complete('sheet','ready-sheet',{done:false});assert.equal(r.setDone,false);assert.deepEqual(r.counts,{sheets:0,sets:0});
    current=await call({op:'listSheets',excludeDone:true});assert.equal(current.sheets.length,3,'undo returns every member');
    assert.equal((await call({op:'laserDoneList'})).rows.length,0);
    // Missing, empty and unverified members cannot be silently completed.
    st.put(SET,'set-fixture',{sheetIds:['cut-sheet','ready-sheet','pending-sheet','missing']});assert.equal((await complete('set','set-fixture')).status,409);
    st.put(SET,'empty-set',{setId:'empty-set',sheetIds:[]});assert.equal((await complete('set','empty-set')).status,409);
    st.put(SET,'set-fixture',{sheetIds:['cut-sheet','ready-sheet','pending-sheet']});
    st.put(S,'ready-sheet',{draft:true});assert.equal((await complete('set','set-fixture')).status,409);st.put(S,'ready-sheet',{draft:false});
    st.put(S,'ready-sheet',{setId:'missing-set'});assert.equal((await complete('sheet','ready-sheet')).status,404,'orphan member cannot complete as a standalone');st.put(S,'ready-sheet',{setId:'set-fixture'});
    const set=st.doc(SET,'set-fixture'),members=(await call({op:'laserStatus',sheetIds:set.sheetIds})).sheets;
    assert(R.laserGroup(set,members).ready);assert.equal(R.laserGroup(set,members.slice(0,2)).ready,false);
    assert.equal((await complete('set','set-fixture')).status,200);
    console.log('PASS: retained members, search/counts, laser-only completion, readiness gates, retries, whole-set filing, undo, pagination and missing-member guards');
  }finally{srv.close();}
}
module.exports={seed};
if(require.main===module)main().catch(e=>{console.error(e);process.exitCode=1;});
