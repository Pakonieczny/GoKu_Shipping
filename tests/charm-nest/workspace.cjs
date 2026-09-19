// Node-only regression tests for the actual browser workspace/intake modules.
// No live Etsy, Firestore, orders or files are changed.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const code = fs.readFileSync(require('node:path').join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
const part = (name, end) => code.slice(code.indexOf(`const ${name} = window.${name} =`), code.indexOf(end, code.indexOf(`const ${name} = window.${name} =`)));
// Transaction-shaped IDB adapter uses structuredClone, as real IndexedDB does.
function indexedDB(realm = x => x) {
  const records = new Map();
  const db = { createObjectStore() {}, transaction() {
    const tx = { objectStore: () => ({
      get(k) { const req = {}; setImmediate(() => { req.result = realm(structuredClone(records.get(k))); tx.oncomplete(); }); return req; },
      put(v, k) { const req = {}; const saved = structuredClone(v); setImmediate(() => { records.set(k, saved); tx.oncomplete(); }); return req; }
    }) }; return tx;
  } };
  return { open() { const req = {}; setImmediate(() => { req.result = db; req.onupgradeneeded(); req.onsuccess(); }); return req; } };
}
function context() {
  const c = vm.createContext({ assert, console,  structuredClone, Blob, TextEncoder, performance,
    setTimeout, clearTimeout, setInterval: f => (c.tick = f, 1), clearInterval() {}, setImmediate });
  c.indexedDB = indexedDB(x => c.rehome(x));
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
    const METALS=[{key:'gold'}];
    const B={run:null,orders:{rows:[],byKey:new Map()},pool:{rows:new Map()},sets:new Map(),engrave:{items:new Map()},review:{items:[]}};
    const allSheets=()=>METALS.flatMap(m=>S.sheets[m.key].pages);
    const view={sort:'arrival'}, gate={}, recall={runId:null};
    let saved=0,parsed=0,polled=0;
    const Orders={view:()=>view,rows:()=>B.orders.rows,render(){},interpretAll(){},claim:async()=>{},applyPullRule:x=>x,loadMaps:async()=>{}};
    const Gate={state:()=>gate}; const Recall={state:()=>recall,on:()=>!!recall.runId};
    const Engrave={render(){},classifyAll:async()=>{},fitAll:async()=>{}}; const Review={render(){}};
    const RunCtl={renderBanner(){},save:async()=>saved++,poke(){},stop(){},start:async()=>{},clearRunState(){recall.runId=null;B.orders={rows:[],byKey:new Map()};}};
    const CN={showPage(){}}; const LiveStrip={rows:[]};
    const P={parseSource:async()=>{parsed++;return {rebuilt:true};}};
    const O={lineKey:(o,l)=>o.receiptId+'/'+l.transactionId,stepIndex:s=>['pull','pool','nest','engrave'].indexOf(s)};
    const Sandbox={on:()=>S.settings.sandbox==='on'};
    const DesignLink={ensure:async()=>{},etsyBudgetOk:()=>true,meter(){},call:async()=>{polled++;return {total:0,hydrated:0,orders:[]};}};
    const Master={load:async()=>{}}; const LiveNest={add:async()=>{}};
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
    S.settings.sandbox='off';assert.equal(await Session.restore(),false,'production must not restore sandbox work');S.settings.sandbox='on';
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
    assert.match(document.getElementById('arrivalCounter').textContent,/24h 2 · 1h 1/);
    await Session.flush();
  })()`, c);
  c.ordersLogic = require('../../charm-nest-orders.js');
  vm.runInContext(part('LiveNest', '/* ═══ 25').replace('const LiveNest = window.LiveNest =','const RealLiveNest = window.LiveNest ='),c);
  await vm.runInContext(`(async()=>{
    O.kinGroups=ordersLogic.kinGroups;
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
    const Pool={addAll:async()=>{activePage('gold').charms.push({id:'new',poolId:'pn',orderDate:3,areaPt2:10});activePage('gold').placements=[];}};
    const sleep=async()=>{},agent=()=>{};
    const starts=[];
    const startNest=p=>{starts.push(p.charms.map(c=>({...c})));p.status='complete';p.dirty=false;p.placements=p.charms.map(c=>({id:c.id}));};
  `,c);
  await vm.runInContext(`(async()=>{
    await RealLiveNest.add(B.run);
    assert.equal(starts.length,1);assert.equal(starts[0][0].pinned.angle,30,'incremental intake retains previous placement before pooling dirties the sheet');
    assert.equal(rewriteCount,1,'approved back is regenerated without losing approval');
    const first=S.sheets.gold;
    first.charms=[{id:'c1',poolId:'p1',orderDate:1,areaPt2:10,arrivalPin:true,pinned:{cxPt:1,cyPt:1,angle:0}}];
    first.placements=[{id:'c1',cxPt:1,cyPt:1,angle:0}];
    first.pages.push({metal:'gold',runId:'run-live',sheetId:'sheet-second',fileBase:'second',group:'gold',status:'complete',dirty:false,charms:[{id:'c2',poolId:'p2',orderDate:2,areaPt2:10}],placements:[{id:'c2'}]});
    S.cloud.ok=true;
    await RealLiveNest.add(B.run);
    assert.equal(starts[1].length,3,'repack sees both earlier sheets and the new piece');
    assert.equal(starts[1][0].pinned,null,'automatic pins released before multi-sheet optimization');
    assert(apiCalls.some(x=>x.op==='archiveEmptySheet'&&x.id==='sheet-second'),'emptied overflow record retired');
    assert.equal(B.run.sheets['sheet-second'],undefined);
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
  console.log('workspace OK · layouts, typed geometry, approval links, pending intake, duplicate receipts, dates, cadence, rolling counts, live repack, mixed-material sets');
})().catch(e=>{console.error(e);process.exitCode=1;});
