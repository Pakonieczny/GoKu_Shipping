// Node-only regression tests for the actual browser workspace/intake modules.
// No live Etsy, Firestore, orders or files are changed.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const code = fs.readFileSync(require('node:path').join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
const part = (name, end) => code.slice(code.indexOf(`const ${name} = window.${name} =`), code.indexOf(end, code.indexOf(`const ${name} = window.${name} =`)));
// Transaction-shaped IDB adapter uses structuredClone, as real IndexedDB does.
function indexedDB(realm = x => x, control = {}) {
  const records = new Map();
  const db = { createObjectStore() {}, transaction() {
    const tx = { objectStore: () => ({
      get(k) { const req = {}; setImmediate(() => { req.result = realm(structuredClone(records.get(k))); tx.oncomplete(); }); return req; },
      put(v, k) {
        const req = {}; const saved = structuredClone(v); control.puts = (control.puts || 0) + 1;
        const finish = () => setImmediate(() => { records.set(k, saved); tx.oncomplete(); });
        if (control.hold) (control.pending ||= []).push(finish); else finish();
        return req;
      }
    }) }; return tx;
  } };
  return { open() { const req = {}; setImmediate(() => { req.result = db; req.onupgradeneeded(); req.onsuccess(); }); return req; } };
}
function context() {
  const c = vm.createContext({ assert, console,  structuredClone, Blob, TextEncoder, performance,
    setTimeout, clearTimeout, setInterval: f => (c.tick = f, 1), clearInterval() {}, setImmediate });
  c.idbControl = {};
  c.indexedDB = indexedDB(x => c.rehome(x), c.idbControl);
  c.rehome = vm.runInContext(`(function rehome(x, seen = new Map()) {
    if (!x || typeof x !== 'object' || ArrayBuffer.isView(x) || x instanceof Blob) return x;
    if(seen.has(x)) return seen.get(x);
    const type=Object.prototype.toString.call(x);
    const out=type==='[object Map]'?new Map():type==='[object Set]'?new Set():Array.isArray(x)?[]:{};seen.set(x,out);
    if(type==='[object Map]') for(const [k,v] of x) out.set(k,rehome(v,seen));
    else if(type==='[object Set]') for(const v of x) out.add(rehome(v,seen));
    else for(const [k,v] of Object.entries(x)) out[k]=rehome(v,seen);
    return out;
  })`,c);
  vm.runInContext(`
    const nodes = new Map(), storage = new Map();
    const window = {addEventListener() {}};
    const document = { hidden:false, addEventListener() {}, getElementById:id=>nodes.get(id), querySelectorAll:()=>[], body:{appendChild:x=>nodes.set(x.id,x)} };
    const el = () => ({style:{},classList:{toggle(){}},setAttribute(){},removeAttribute(){}});
    const localStorage = {getItem:k=>storage.get(k),setItem:(k,v)=>storage.set(k,v)};
    const navigator = {};
    const S = {settings:{sandbox:'on',pollMinutes:10,runMode:'manual'},cloud:{ok:false},sources:[],poolSources:{},unassigned:[],sheets:{gold:{metal:'gold',active:0,charms:[],placements:[],status:'idle',cardEl:{node:true}}},mode:'nest'};
    S.sheets.gold.pages=[S.sheets.gold];
    // the page fixes its mode when it loads; the tests below flip it as a reload into the other mode would
    let WORKSPACE_SANDBOX = S.settings.sandbox === 'on';
    const METALS=[{key:'gold'}];
    const B={run:null,orders:{rows:[],byKey:new Map()},pool:{rows:new Map()},sets:new Map(),engrave:{items:new Map()},review:{items:[]}};
    const allSheets=()=>METALS.flatMap(m=>S.sheets[m.key].pages);
    const view={sort:'arrival'}, gate={}, recall={runId:null};
    let saved=0,parsed=0,polled=0;
    const Orders={view:()=>view,rows:()=>B.orders.rows,render(){},interpretAll(){},claim:async()=>{},applyPullRule:x=>x,loadMaps:async()=>{}};
    const Gate={state:()=>gate,modern:()=>false,nestable:()=>true,assemble:async()=>{}}; const Recall={state:()=>recall,on:()=>!!recall.runId};
    const Engrave={render(){},classifyAll:async()=>{},fitAll:async()=>{},background:()=>Promise.resolve()}; const Review={render(){}};
    const RunCtl={renderBanner(){},save:async()=>saved++,poke(){},stop(){},start:async()=>{},clearRunState(){recall.runId=null;B.orders={rows:[],byKey:new Map()};}};
    const CN={showPage(){}}; const LiveStrip={rows:[]};
    const P={parseSource:async()=>{parsed++;return {rebuilt:true};}};
    const O={lineKey:(o,l)=>o.receiptId+'/'+l.transactionId,stepIndex:s=>['pull','pool','nest','checkpoint','engrave'].indexOf(s)};
    const Sandbox={on:()=>S.settings.sandbox==='on'};
    const DesignLink={ensure:async()=>{},etsyBudgetOk:()=>true,meter(){},call:async()=>{polled++;return {total:0,hydrated:0,orders:[]};}};
    const Pool={repairRecoveredGeometry:async()=>0};const Master={load:async()=>{}}; const LiveNest={add:async()=>{}}; const ListMedia={prepare(){}};
    const apiCalls=[];const api=async(name,body)=>{apiCalls.push(body);return {run:null};};
    const toast=()=>{},notifyPerson=()=>{},refreshAllCards=()=>{},renderRail=()=>{},updateTopSub=()=>{};
    const setMode=m=>{S.mode=m;};
  `, c);
  vm.runInContext(part('Session', '/* Import cadence'), c);
  vm.runInContext(part('Arrivals', '/* A new batch'), c);
  return c;
}
(async () => {
  const c = context();
  await vm.runInContext(`(async()=>{
    const p=S.sheets.gold;
    Object.assign(p,{sheetId:'checkpoint-test',jobId:'search-1',bestKey:'geometry-1',status:'nesting',persistedDone:false,bestRevision:1,best:{placements:[{id:'a',angle:10}],rejects:['b'],density:.2},placements:[{id:'a',angle:10}]});
    Session.listen();await Session.flush();
    p.best={placements:[{id:'a',angle:20},{id:'b',angle:30}],rejects:[],density:.4};p.bestRevision=2;p.bestInfo={placed:2};
    let captured=false;
    Object.defineProperty(p,'fullWorkspaceTrap',{enumerable:true,configurable:true,get(){captured=true;throw new Error('full workspace copied');}});
    const writing=Session.checkpointBest(p);
    p.best.placements[0].angle=350;
    await writing;assert.equal(captured,false,'best updates save only the small checkpoint');
    delete p.fullWorkspaceTrap;
    await Session.restore();
    assert.equal(p.bestRevision,2);assert.equal(p.placements.length,2);assert.equal(p.placements[0].angle,20,'checkpoint is detached from subsequent mutations');
    assert.equal(p.status,'ready');assert.match(p.stage,/Recovered best/);
    Object.assign(p,{status:'nesting',jobId:'search-2',bestKey:'geometry-2',bestRevision:0,best:null,placements:[]});
    await Session.flush();await Session.restore();assert.equal(p.placements.length,0,'a stale checkpoint cannot enter a new job or changed geometry');
    Object.assign(p,{status:'idle',sheetId:null,jobId:null,bestKey:null,best:null,bestRevision:0});
  })()`,c);
  await vm.runInContext(`(async()=>{
    const bits=new Uint8Array(1024), view=new DataView(bits.buffer,4,8);
    const data=Session.copy({a:bits,b:bits,view});
    assert.equal(data.a,data.b,'shared source masks are copied once per checkpoint');
    assert.notEqual(data.a,bits,'checkpoint owns a stable copy');
    assert.equal(data.view.byteLength,8);
    const scratch={get masks(){throw new Error('checkpoint traversed solver internals');}};
    S.sheets.gold.best={placements:[{id:'c',angle:2}],rejects:[],density:.5,contactQuality:1.2,grids:scratch,rec:scratch};
    const compact=Session.capture().sheets[0].pages[0].best;
    assert.equal(compact.rec,undefined);assert.equal(compact.grids,undefined);
    assert.equal(compact.placements[0].angle,2);assert.equal(compact.contactQuality,1.2);
    delete S.sheets.gold.best;

    let captures=0,latest=0;
    Object.defineProperty(S.sheets.gold,'checkpointValue',{enumerable:true,configurable:true,get(){captures++;return latest;}});
    Session.listen();idbControl.hold=true;
    const first=Session.flush();await new Promise(setImmediate);await new Promise(setImmediate);
    assert.equal(idbControl.pending.length,1);
    for(let i=1;i<=100;i++){latest=i;assert.equal(Session.flush(),first);}
    assert.equal(captures,1,'slow storage does not queue full workspace snapshots');
    idbControl.pending.shift()();await new Promise(setImmediate);await new Promise(setImmediate);
    assert.equal(captures,2);assert.equal(idbControl.pending.length,1,'one follow-up checkpoint captures the latest state');
    idbControl.hold=false;idbControl.pending.shift()();await first;
    delete S.sheets.gold.checkpointValue;
    await Session.restore();assert.equal(S.sheets.gold.checkpointValue,100,'coalescing retains the latest edit');
    delete S.sheets.gold.checkpointValue;
    // Capture errors are reported through the same recoverable save path.
    Object.defineProperty(S.sheets.gold,'badCapture',{enumerable:true,configurable:true,get(){throw new Error('capture failed');}});
    await Session.flush();delete S.sheets.gold.badCapture;await Session.flush();
  })()`,c);
  await vm.runInContext(`(async()=>{
    const row={key:'100/1',order:{receiptId:'100',createTs:10},line:{transactionId:'1'},state:'written',poolIds:['p1'],arrivedAt:123,engrave:{approved:true}};
    B.orders.rows=[row];B.orders.byKey.set(row.key,row);
    B.run={runId:'run-test',status:'review',step:'engrave',orders:['100'],arrivalBusy:true};
    const charm={id:'c1',poolId:'p1',orderDate:10,bits:new Uint8Array([1,0,1]),pinned:{cxPt:4,cyPt:5,angle:30}};
    S.sources=[{id:'src',name:'a.ai',bytes:new Uint8Array([37,80,68,70]),parsed:{cannotPersist(){}}}];
    Object.assign(S.sheets.gold,{charms:[charm],placements:[{id:'c1',cxPt:4,cyPt:5,angle:30}],status:'complete',persistedDone:true,outputs:{ai:new Uint8Array([1,2]),previewPng:new Blob(['preview'])}});
    B.engrave.items.set(row.key,{key:row.key,row,state:'approved',fit:{capMm:2.1,bits:new Uint8Array([1])},copies:['p1']});
    B.review.items=[{row,job:B.engrave.items.get(row.key),state:'open'}];
    Session.listen();await Session.flush();
    B.orders.rows=[];B.engrave.items.clear();S.sheets.gold.charms=[];
    assert.equal(await Session.restore(),true);
    assert.equal(B.orders.rows[0].engrave.approved,true);
    const j=B.engrave.items.get('100/1');assert.equal(j.state,'approved');assert.equal(j.fit.capMm,2.1);assert.equal(j.row,B.orders.rows[0]);
    assert.equal(B.review.items[0].row,j.row);assert.equal(B.review.items[0].job,j);
    assert.equal(S.sheets.gold.charms[0].bits[2],1);assert.equal(S.sheets.gold.placements[0].angle,30);
    assert.equal(S.sheets.gold.outputs.previewPng.size,7);assert.equal(parsed,1);
    assert.equal(B.run.arrivalBusy,false);assert.equal(B.run.status,'stopped');assert.equal(B.run.step,'engrave');
    S.settings.sandbox='off';WORKSPACE_SANDBOX=false;assert.equal(await Session.restore(),false,'production must not restore sandbox work');S.settings.sandbox='on';WORKSPACE_SANDBOX=true;
  })()`, c);
  await vm.runInContext(`(async()=>{
    B.run.status='review';
    const existing=B.orders.rows[0];
    const incoming={receiptId:'200',createTs:20,lines:[{transactionId:'2'},{transactionId:'3'}]};
    await Arrivals.merge([incoming]);await Arrivals.merge([incoming]);
    assert.equal(B.orders.rows.length,3,'snapshot retries must not duplicate lines');
    assert.equal(B.orders.byKey.get('100/1'),existing,'prior human decision object stays intact');
    assert.equal(existing.engrave.approved,true);
    assert.equal(Object.keys(Arrivals.state().seen).length,1,'two lines count as one order');
    assert.equal(B.orders.byKey.get('200/2').order.createTs,20,'arrival time never replaces Etsy date');
    assert.equal(B.run.orders.filter(x=>x==='200').length,1);
    assert.equal(Arrivals.state().pending,true);
    assert.equal(JSON.parse(localStorage.getItem('cn.arrivals.sandbox')).pending,true,'pending intake survives reload');
    const before=Date.now();Arrivals.start();assert(Arrivals.state().nextCheck>=before+599000,'default interval is ten minutes');
    S.settings.pollOrders='off';await Arrivals.check();assert.equal(polled,0,'disabled polling makes no station request');
    S.settings.pollOrders='on';await Arrivals.check();assert.equal(polled,1);
    S.settings.pollMinutes=15;Arrivals.start();assert(Arrivals.state().nextCheck>=Date.now()+899000);
    Arrivals.state().seen={'old':Date.now()-25*3600000,'two-hours':Date.now()-2*3600000,'recent':Date.now()-1000};Arrivals.paint();
    assert.match(Arrivals.text(),/24h 2 · 1h 1/);
    Arrivals.state().seen.ancient=Date.now()-50*86400000;Arrivals.state().recorded={ancient:true};
    await Arrivals.record([{receiptId:'300',createTs:30}]);
    assert.equal(Arrivals.state().seen.ancient,undefined,'first-arrival times older than 45 days are dropped');assert.equal(Arrivals.state().recorded.ancient,undefined);
    assert(Arrivals.state().seen['300']&&Arrivals.state().seen.recent,'current ones are kept');
    await Session.flush();
  })()`, c);
  c.ordersLogic = require('../../charm-nest-orders.js');
  vm.runInContext(part('LiveNest', '/* ═══ 25').replace('const LiveNest = window.LiveNest =','const RealLiveNest = window.LiveNest ='),c);
  await vm.runInContext(`(async()=>{
    O.kinGroups=ordersLogic.kinGroups;O.FAST_MATERIALS=ordersLogic.FAST_MATERIALS;
    const force=Gate.state();force.forceFill={};
    B.run={runId:'run-live',status:'review',step:'engrave',sheets:{'sheet-first':{},'sheet-second':{}}};
    S.settings.maxFill=.74;S.settings.optimizeAt=85;
    const first=S.sheets.gold;
    Object.assign(first,{runId:'run-live',sheetId:'sheet-first',fileBase:'first',group:'gold',status:'complete',dirty:false,charms:[{id:'c1',poolId:'p1',orderDate:1,areaPt2:10}],placements:[{id:'c1',cxPt:10,cyPt:10,angle:30}],persisted:null});
    first.pages=[first];first.active=0;
    B.orders.rows=[{state:'written',spec:{material:'gold'},order:{receiptId:'1'}},{state:'pulled',spec:{material:'gold'},order:{receiptId:'2'}}];
    B.engrave.items=new Map([['approved',{state:'approved',copies:['p1']}]]);
  })()`,c);
  vm.runInContext(`
    let rewriteCount=0;Engrave.items=()=>B.engrave.items;Engrave.writeBacks=async()=>rewriteCount++;
    const set={setId:'set-live',runId:'run-live',group:'gold',materials:['gold'],sheetIds:['sheet-first','sheet-second'],labelFiles:[],orders:{}};
    const Sets={ofRun:()=>[set],save:async()=>{},ensure:async()=>set};
    const activePage=m=>S.sheets[m].pages[S.sheets[m].active], pagesOf=m=>S.sheets[m].pages;
    const inflatedArea=c=>c.areaPt2,usableArea=()=>1000;
    Pool.addAll=async()=>{activePage('gold').charms.push({id:'new',poolId:'pn',orderDate:3,areaPt2:10});activePage('gold').placements=[];};
    const sleep=async()=>{},agent=()=>{};
    const starts=[];
    const activeCharms=p=>p.charms;
    // the page's startNest prepares a run's sheet first, which is where earlier positions become pins
    const startNest=p=>{RealLiveNest.prepareSheet(p);starts.push(p.charms.map(c=>({...c})));p.status='complete';p.dirty=false;p.placements=p.charms.map(c=>({id:c.id,...(c.pinned||{})}));};
  `,c);
  await vm.runInContext(`(async()=>{
    await RealLiveNest.add(B.run);
    assert.equal(starts.length,1);assert.equal(starts[0][0].pinned.angle,30,'incremental intake retains previous placement before pooling dirties the sheet');
    assert.equal(S.sheets.gold.appendOnly,true,'arrivals fill the gaps around the saved layout');
    assert.equal(rewriteCount,0,'an approved back stays as written: appending moves no earlier piece');
    const first=S.sheets.gold;
    first.charms=[{id:'c1',poolId:'p1',orderDate:1,areaPt2:10,arrivalPin:true,pinned:{cxPt:1,cyPt:1,angle:0}}];
    first.placements=[{id:'c1',cxPt:1,cyPt:1,angle:0}];
    first.pages.push({metal:'gold',runId:'run-live',sheetId:'sheet-second',fileBase:'second',group:'gold',status:'complete',dirty:false,charms:[{id:'c2',poolId:'p2',orderDate:2,areaPt2:10}],placements:[{id:'c2',cxPt:5,cyPt:5,angle:90}]});
    S.cloud.ok=true;
    const second=first.pages[1];
    await RealLiveNest.add(B.run);
    assert.deepEqual(starts[1].map(c=>c.id),['c1','new'],'arrivals fill the earliest open sheet first, not the newest page');
    assert.equal(starts[1][0].pinned.angle,0,'that sheet keeps its saved positions');
    assert.deepEqual(second.charms.map(c=>c.id),['c2'],'the newer sheet is left as it was');
    assert(!apiCalls.some(x=>x.op==='archiveEmptySheet'),'no sheet is emptied or retired by an arrival');
    assert.deepEqual(Object.keys(B.run.sheets),['sheet-first','sheet-second']);
    // once the earlier sheet is full, arrivals go on to the next one
    first.releaseFull=true;Pool.addAll=async()=>{activePage('gold').charms.push({id:'later',poolId:'pl',orderDate:4,areaPt2:10});};
    await RealLiveNest.add(B.run);
    assert.deepEqual(starts[2].map(c=>c.id),['c2','later'],'a full sheet takes no more; the next open sheet does');
    first.releaseFull=false;
    await Session.flush();
  })()`,c);
  await vm.runInContext(`(async()=>{
    const first=S.sheets.gold;first.pages=[first];first.sheetId='original-gold';first.setId='set-gold';first.fileBase='old-gold';first.group='gold';
    first.charms=[{id:'gold-old',poolId:'gp',orderDate:1,areaPt2:10}];first.placements=[{id:'gold-old'}];
    const silver={metal:'silver',active:0,runId:'run-live',sheetId:'original-silver',setId:'set-silver',fileBase:'old-silver',group:'silver',status:'complete',dirty:false,charms:[{id:'silver-old',poolId:'sp',orderDate:2,areaPt2:10}],placements:[{id:'silver-old'}]};silver.pages=[silver];S.sheets.silver=silver;METALS.push({key:'silver'});
    const goldSet={setId:'set-gold',runId:'run-live',group:'gold',materials:['gold'],sheetIds:['original-gold'],labelFiles:[],orders:{}}, silverSet={...goldSet,setId:'set-silver',group:'silver',materials:['silver'],sheetIds:['original-silver']};
    B.sets=new Map([['gold',goldSet],['silver',silverSet]]);
    Sets.ofRun=()=>[...B.sets.values()];Sets.ensure=async(runId,group)=>{let x=B.sets.get(group);if(!x){x={...goldSet,setId:'combined',group,materials:group.split('+'),sheetIds:[],labelFiles:[],orders:{}};B.sets.set(group,x);}return x;};
    B.orders.rows=[{state:'written',spec:{material:'gold'},order:{receiptId:'1'}},{state:'written',spec:{material:'silver'},order:{receiptId:'2'}},{state:'pulled',spec:{material:'gold'},order:{receiptId:'3'}},{state:'pulled',spec:{material:'silver'},order:{receiptId:'3'}}];
    Pool.addAll=async()=>{B.run.groups={gold:'gold+silver',silver:'gold+silver'};first.charms.push({id:'new-gold',poolId:'ng',orderDate:3,areaPt2:10});silver.charms.push({id:'new-silver',poolId:'ns',orderDate:3,areaPt2:10});};
    await RealLiveNest.add(B.run);
    assert.equal(goldSet.status,'superseded');assert.equal(silverSet.status,'superseded');assert.equal(B.sets.size,1);
    assert.equal(first.group,'gold+silver');assert.equal(silver.group,'gold+silver');assert.equal(B.run.setIds[0],'combined');
    assert(apiCalls.some(x=>x.op==='archiveEmptySheet'&&x.id==='original-gold'));assert(apiCalls.some(x=>x.op==='archiveEmptySheet'&&x.id==='original-silver'));
    await Session.flush();
  })()`,c);
  await vm.runInContext(`(async()=>{
    B.run.status='review';B.run.step='engrave';B.run.intakeRecovery={retire:['interrupted-sheet'],backs:['p1']};B.run.sheets['interrupted-sheet']={};
    await Session.flush();await Session.restore();assert.equal(B.run.step,'checkpoint','refresh must finish repack bookkeeping before engraving/labels');
    await RealLiveNest.finish(B.run);await RealLiveNest.finish(B.run);
    assert.equal(B.run.intakeRecovery,undefined);assert.equal(B.run.sheets['interrupted-sheet'],undefined);
    assert.equal(apiCalls.filter(x=>x.op==='archiveEmptySheet'&&x.id==='interrupted-sheet').length,1);
    await Session.flush();
  })()`,c);
  console.log('workspace OK · layouts, typed geometry, approval links, pending intake, duplicate receipts, dates, cadence, rolling counts, live append to the earliest open sheet, mixed-material sets');
})().catch(e=>{console.error(e);process.exitCode=1;});
