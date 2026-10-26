const assert=require('node:assert/strict'),fs=require('fs'),vm=require('vm');
const html=fs.readFileSync('charm-nest-1.html','utf8'),bridge=fs.readFileSync('charm-nest-bridge.js','utf8');
const remote=html.slice(html.indexOf('async function agentCallRemote('),html.indexOf('function toBase64('));
(async()=>{
 for(const terminal of [{status:'done',result:{skipped:'400 maxItems not supported'}},{status:'done',result:{error:'bad schema'}},{status:'error',error:'API failed'},null]) {
  const calls=[],started=[];const c={performance:{now:()=>0},setTimeout:fn=>fn(),fmt:{s:()=>''},api:async(_,b)=>{calls.push(b);return b.op==='startAgent'?{id:'new'}:{job:b.id==='old'?terminal:{status:'done',result:{profiles:[{index:0}]}}};}};vm.createContext(c);vm.runInContext(remote,c);
  const result=await c.agentCallRemote('packing',{}, {existingId:'old',retryFailed:true,onStarted:id=>started.push(id)});
  assert.equal(result.profiles.length,1);assert.equal(calls.filter(x=>x.op==='startAgent').length,1,'replace failed job exactly once');assert.deepEqual(started,['old','new']);
 }
 for(const cached of [{status:'done',result:{profiles:[{index:0}]}},{status:'running'}]) {
  let polls=0,starts=0;const c={performance:{now:()=>0},setTimeout:fn=>fn(),fmt:{s:()=>''},api:async(_,b)=>{if(b.op==='startAgent'){starts++;return{id:'new'};}return{job:++polls===1?cached:{status:'done',result:{profiles:[{index:0}]}}};}};vm.createContext(c);vm.runInContext(remote,c);await c.agentCallRemote('packing',{}, {existingId:'old',retryFailed:true});assert.equal(starts,0,'successful/running analyses are reused without another paid request');
 }
 const c={performance:{now:()=>0},setTimeout:fn=>fn(),fmt:{s:()=>''},api:async()=>{throw Error('network unavailable');}};vm.createContext(c);vm.runInContext(remote,c);await assert.rejects(c.agentCallRemote('packing',{}, {existingId:'old',retryFailed:true}),e=>!e.agentTerminal&&/network/.test(e.message));
 const fail={...c,api:async(_,b)=>b.op==='startAgent'?{id:'new'}:{job:{status:'done',result:{skipped:'still unavailable'}}}};vm.createContext(fail);vm.runInContext(remote,fail);const result=await fail.agentCallRemote('packing',{}, {existingId:'old',retryFailed:true});assert(result.skipped,'a failed replacement returns, without an automatic paid retry loop');
 // A sheet whose shape analysis failed is held on its own card with its reason while the other sheets finish; the run is not stopped
 // for it (the single "shape analysis failed" stop was replaced by per-sheet holds, holdSheet in charm-nest-bridge.js; updated 26 Oct 2026)
 const stops=[],r={runId:'r',sheets:{},status:'nesting'};let saved=0;let pokes=0;const rc={B:{run:r},O:{},waiter:null,poke:()=>{pokes++;},window:{},CN:{renderCard(){}},sheetName:sh=>sh.metal,save:async()=>{saved++;},stop:(why,fix)=>{stops.push({why,fix});r.status='stopped';}};vm.createContext(rc);vm.runInContext(bridge.slice(bridge.indexOf('  function onSheetDone(sh, err)'),bridge.indexOf('  // a set that already waits for the same reason')),rc);
 for(const metal of ['gold','silver','rose']){const sh={metal,page:1,runId:'r',placements:[],rejects:[]};rc.onSheetDone(sh,Object.assign(Error('shape service rejected schema'),{stage:'shape-analysis'}));assert.equal(sh.problem,'shape service rejected schema');assert.equal(sh.runHold,'shape service rejected schema','the failed sheet is held with its reason');}
 assert.equal(stops.length,0,'a shape-analysis failure holds its sheet, it does not stop the run');assert.equal(r.status,'nesting');assert.deepEqual(Object.keys(r.sheetHolds).sort(),['gold-1','rose-1','silver-1']);assert.equal(saved,3,'every sheet failure still persists');assert.equal(pokes,3);
 console.log('Guidance retry OK: failed-cache replacement, running/successful job reuse, transient recovery, no paid retry loop, failed sheets held each with its reason');
})().catch(e=>{console.error(e);process.exitCode=1;});
