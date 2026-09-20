const assert=require('node:assert/strict'),fs=require('fs'),vm=require('vm'),Ops=require('../../charm-nest-operations.js'),O=require('../../charm-nest-orders.js');
const source=fs.readFileSync('charm-nest-bridge.js','utf8'),start=source.indexOf('const Gate ='),end=source.indexOf('/* ═══ 21',start);
const ops=Ops.create(),writes=[],toasts=[],sets=[];let fail=false,release;const hold=new Promise(r=>release=r);
const run={runId:'r',releasePolicy:2,status:'review',step:'engrave',solidIncluded:{gold14k:false},errors:[]};
const page={metal:'gold14k',runId:'r',sheetId:'s',draft:true,status:'nesting',outputs:{ai:'original'},persistedDone:true,verification:{ok:true},placements:[{id:'c'}],charms:[{id:'c',poolId:'p'}]};
const ctx={window:{CharmNestOrders:O,CharmNestOperations:ops},B:{run,sets:new Map()},S:{mode:'nest',settings:{},cloud:{ok:true},library:{rows:[]}},O,allSheets:()=>[page],pagesOf:()=>[page],refreshAllCards:()=>{},Session:{schedule(){}},document:{getElementById:()=>null},toast:m=>toasts.push(m),Pool:{update:async()=>{}},Orders:{rows:()=>[]},Engrave:{items:()=>new Map(),saveSheetBacks:async()=>{writes.push('back-metadata');}},RunCtl:{save:async()=>writes.push('run'),poke(){},onSheetDone(){},membershipUpdated(){}},CN:{sheetFileBase:()=> 'Set-1-Sheet-1',persistSheet:()=>{throw Error('must not re-upload artwork');},renderLibrary(){}},Sets:{ofRun:()=>sets,ensure:async()=>{const set={setId:'set',runId:'r',seq:1,day:'2026-09-20',group:'dispatch',sheetIds:[],labelFiles:[],materials:[],orders:{}};sets.push(set);return set;},save:async()=>{},labelsReady:()=>true,onSheetSaved:async(sh)=>{if(fail)throw Error('offline');writes.push('QR');const set=sets[0];if(!set.sheetIds.includes(sh.sheetId))set.sheetIds.push(sh.sheetId);}},api:async(name,body)=>writes.push(body),stockFor:()=>({wIn:1,hIn:1}),labelOf:x=>x,esc:x=>x};
vm.createContext(ctx);vm.runInContext(source.slice(start,end),ctx);const Gate=ctx.window.Gate;
(async()=>{
 const busy=ops.run({key:'sheet-save',resources:['production:r']},()=>hold);await Promise.resolve();await Promise.resolve();
 const one=Gate.changeMembership('gold14k',true),two=Gate.changeMembership('gold14k',false),three=Gate.changeMembership('gold14k',true);
 assert.equal(run.solidIncluded.gold14k,true,'selection is immediate despite ongoing nesting/save');assert.equal(run.status,'review','membership does not stop the run');assert.equal(writes.length,0,'conflicting saves wait');
 page.status='complete';release();await Promise.all([busy,one,two,three]);assert.equal(page.setId,'set');assert.equal(writes.filter(x=>x==='QR').length,1,'rapid toggles save only final queued membership');
 const preview=Gate.projectLibraryRecords([{id:'s',runId:'r',metal:'gold14k',draft:true}]);assert.equal(preview[0].setId,'set','stale server data cannot undo current selection');
 await Gate.changeMembership('gold14k',false);assert.equal(page.setId,null);assert.equal(page.draft,true);assert.equal(Gate.projectLibraryRecords([{id:'s',runId:'r',metal:'gold14k',setId:'set'}])[0].setId,null);
 fail=true;await assert.rejects(Gate.changeMembership('gold14k',true),/offline/);await assert.rejects(Gate.flush(run),/offline/);assert.equal(run.solidIncluded.gold14k,true,'failed save preserves visible intent for retry');assert.equal(page.draft,true,'failed membership attachment rolls back confirmed metadata');
 fail=false;await Gate.changeMembership('gold14k',true);await Gate.flush(run);assert.equal(page.setId,'set');assert.equal(run.status,'review');
 // A failed detach must retain confirmed membership so a retry repeats all writes.
 const originalApi=ctx.api;let detaches=0;ctx.api=async(name,body)=>{if(body.sheet?.draft){detaches++;throw Error('detach offline');}return originalApi(name,body);};
 await assert.rejects(Gate.changeMembership('gold14k',false),/detach offline/);assert.equal(page.setId,'set');assert.equal(run.solidIncluded.gold14k,false);assert.equal(detaches,1);
 ctx.api=originalApi;await Gate.changeMembership('gold14k',false);assert.equal(page.setId,null);await Gate.changeMembership('gold14k',true);
 let unlock;const commit=ops.run({key:'commit:r',resources:['production:r']},()=>new Promise(r=>unlock=r));await Promise.resolve();await Promise.resolve();await assert.rejects(Gate.changeMembership('gold14k',false),/committing/);unlock();await commit;
 // Historical/cached preview views render even when their live charm moved.
 const a=source.indexOf('  function renderBack('),b=source.indexOf('  function renderFront(',a);const noop=()=>{};const canvas=()=>({getContext:()=>new Proxy({},{get:(t,k)=>t[k]||noop}),width:0,height:0});
 const cut={},job={view:{members:[{bbox:[0,0,10,10],original:cut}],cutMembers:[cut]},fit:null};
 const paint={document:{createElement:canvas},PT:72/25.4,MM:25.4/72,P:{pathToCanvas:noop},charmFor:()=>null};vm.createContext(paint);vm.runInContext(source.slice(a,b),paint);assert(paint.renderBack(job,100,{hatch:false}).width>0);
 // Manual re-nesting waits for the same production resource before setup.
 const html=fs.readFileSync('charm-nest-1.html','utf8'),ns=html.indexOf('function startNestReady('),ne=html.indexOf('/* ═══ 9b',ns);
 const nq=Ops.create();let unblock;const saving=nq.run({key:'saving',resources:['production:n']},()=>new Promise(r=>unblock=r));await Promise.resolve();await Promise.resolve();
 const np={runId:'n',metal:'gold',page:1,status:'complete',placements:['saved'],charms:[{}]},running={status:'nesting',metal:'silver'};
 const nc={window:{CharmNestOperations:nq},S:{settings:{},nestQueue:[]},navigator:{hardwareConcurrency:2},allSheets:()=>[np,running],activeCharms:p=>p.charms,renderCard(){},labelOf:x=>x};vm.createContext(nc);vm.runInContext(html.slice(ns,ne),nc);
 const starting=nc.startNestReady(np);assert.equal(np.status,'queued');assert.equal(nc.S.nestQueue.length,0);assert.deepEqual(np.placements,['saved']);unblock();await Promise.all([saving,starting]);assert.equal(nc.S.nestQueue[0],np,'after writes finish the existing worker scheduler takes over');
 // Concurrent run-loop calls share one execution; stale labels cannot advance to commit.
 let finishLabels;const calls=[],lr={status:'running',step:'labels',membershipRevision:0,mode:'auto'};
 const lc={B:{run:lr},O,Gate:{flush:async()=>{}},renderBanner(){},save:async()=>{},PAUSE_AFTER:new Set(),onComplete(){},agent(){},queueMicrotask,stop(){},doStep:async(r,step)=>{calls.push(step);if(step==='labels')await new Promise(res=>finishLabels=res);else r.status='paused';}};
 vm.createContext(lc);vm.runInContext(source.slice(source.indexOf('  let loopTask='),source.indexOf('  async function next()',source.indexOf('  let loopTask='))),lc);
 const loopOne=lc.loop(),loopTwo=lc.loop();assert.equal(loopOne,loopTwo);assert.deepEqual(calls,['labels']);lr.membershipRevision++;lr.membershipNext='engrave';finishLabels();await loopOne;assert.deepEqual(calls,['labels','engrave']);
 // A click made while commit is queued sends the run back through verification.
 const cq=Ops.create();let releaseCommit;const ch=cq.run({key:'save',resources:['production:c']},()=>new Promise(r=>releaseCommit=r));await Promise.resolve();await Promise.resolve();
 const cr={runId:'c'},cc={window:{CharmNestOperations:cq},Gate:{flush:async()=>{},refreshMembership(){}},validateRelease(){throw Error('stale commit reached validation');}};
 vm.createContext(cc);const cs=source.indexOf('  async function commit(set, run, context)');vm.runInContext(source.slice(cs,source.indexOf('  async function ',cs+25)),cc);
 const committing=cc.commit({runId:'c'},cr);await Promise.resolve();cr.membershipNext='engrave';releaseCommit();await ch;assert.equal((await committing).membershipChanged,true);
 console.log('Membership OK: instant intent, queued latest toggle, no run stop/artwork upload, Library projection, retry/commit gate and detached preview');
})().catch(e=>{console.error(e);process.exitCode=1;});
