const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const O=require('../../charm-nest-orders.js'),{JSDOM}=require('jsdom');
const bridge=fs.readFileSync('charm-nest-bridge.js','utf8'),html=fs.readFileSync('charm-nest-1.html','utf8'),station=fs.readFileSync('design-1.html','utf8');
(async()=>{
  assert.equal(O.intakePlan({count:84}).phase,'fill');assert.equal(O.intakePlan({count:85}).phase,'final');assert.equal(O.intakePlan({count:33}).phase,'fill');
  assert.equal(O.intakePlan({count:10,area:90,capacity:100}).phase,'final','capacity pressure can precede 85 pieces');
  assert.equal(O.intakePlan({count:10,force:true}).phase,'final','a draft that cannot fit gets full optimization before spillover');
  assert.equal(O.intakePlan({count:20,budgetS:180}).budgetMs,12000);assert.equal(O.intakePlan({count:85,budgetS:180}).budgetMs,180000);
  const day=(ts)=>O.orderDay({order:{createTs:Date.parse(ts)/1000},arrivedAt:Date.now()});
  assert.equal(day('2026-09-22T03:59:00Z').key,'2026-09-21');assert.equal(day('2026-09-22T04:00:00Z').key,'2026-09-22');
  assert.equal(day('2026-11-01T05:30:00Z').key,day('2026-11-01T06:30:00Z').key,'day grouping handles DST');
  assert.equal(O.evaluateOrder([{state:'written',changePending:true}]).committable,false);
  const dom=new JSDOM('<div id="ordBody"></div>'),document=dom.window.document;let observe;
  class IO{constructor(fn){observe=fn;}observe(){}disconnect(){}unobserve(){}}
  const rows=Array.from({length:130},(_,i)=>({key:'k'+i,order:{receiptId:String(i),createTs:Date.parse(i<70?'2026-09-22T12:00:00Z':'2026-09-21T12:00:00Z')/1000},line:{listingId:i,title:'Charm'},spec:{},problems:[],state:'pooled'}));
  const c={WORKSPACE_SANDBOX:true,document,window:{IntersectionObserver:IO,addEventListener(){}},IntersectionObserver:IO,O,OV:{sort:'arrival',limit:48},rowsOf:()=>rows,visibleRows:()=>rows,viewMode:()=> 'cards',el:(tag,cls)=>{const n=document.createElement(tag);n.className=cls;return n;},stateWords:()=>['info','processing'],dueOf:()=>({txt:'—'}),placeOf:()=>null,imageFor:()=>null,wordsOf:()=>'',Master:{entryFor:()=>null},Pool:{charmOf:()=>null},requestAnimationFrame:fn=>fn(),esc:String,labelOf:String,OrderWin:{open(){}},Review:{problemText:String}};
  vm.createContext(c);vm.runInContext(bridge.slice(bridge.indexOf('function purchaseMarkup('),bridge.indexOf('const fmtT =')),c);vm.runInContext(bridge.slice(bridge.indexOf('  let listKey='),bridge.indexOf('  /* The tab used to')),c);c.renderBody();
  assert.equal(document.querySelectorAll('[data-key]').length,48);assert.equal(document.querySelectorAll('.ordDay').length,1);assert.equal(document.querySelectorAll('[data-new-order]').length,0);
  observe([{isIntersecting:true}]);assert.equal(document.querySelectorAll('[data-key]').length,96);assert.equal(document.querySelectorAll('.ordDay').length,2);assert.match(document.querySelector('.orderTime').textContent,/EDT/);
  observe([{isIntersecting:true}]);assert.equal(document.querySelectorAll('[data-key]').length,130);assert.equal(document.querySelector('.listMore'),null);
  // The production bridge handles incremental receipts without dropping the authoritative open-id list.
  const receipts=[{receipt_id:'1',_stamp:10,_hydrated:true},{receipt_id:'2',_stamp:20,_hydrated:true},{receipt_id:'3',_stamp:30,_hydrated:false}];let reads=[],sweeps=0,minimum;
  const st={Date,SANDBOX:false,allOpenReceipts:receipts,completedOrders:new Set(),etsyMeter:{total:0},etsyBraked:()=>false,sweptRecently:n=>{minimum=n;return false;},SWEEP_MIN_MS:90000,Cursor:{act:async()=>{}},refreshOrders:async()=>{sweeps++;},receiptStamp:r=>r._stamp,detailCache:new Map(),makeQueue:()=>fn=>fn(),hydrateOrder:async rid=>{reads.push(rid);return[];},applyHydration:rid=>{receipts.find(r=>r.receipt_id===rid)._hydrated=true;},orderForBridge:async r=>({receiptId:r.receipt_id,hydrated:r._hydrated,updateTs:r._stamp}),etsyState:()=>({}),S:{}};
  const a=station.indexOf('    async "orders.snapshot"'),b=station.indexOf('    /** One paged list sweep',a);
  vm.createContext(st);vm.runInContext('var cmds={'+station.slice(a,b)+'};',st);
  const delta=await st.cmds['orders.snapshot']({refresh:true,intake:true,known:{'1':10,'2':19}},()=>{});
  assert.equal(minimum,600000);assert.equal(sweeps,1);assert.deepEqual(Array.from(delta.orders,o=>o.receiptId),['2','3']);assert.equal(delta.total,2);assert.equal(delta.hydrated,2);assert.deepEqual(Array.from(delta.openIds),['1','2','3']);assert.deepEqual(reads,['3']);
  const empty=await st.cmds['orders.snapshot']({intake:true,known:{'1':10,'2':20,'3':30}},()=>{});assert.equal(empty.total,0);assert.equal(empty.openIds.length,3,'empty delta is not an empty shop');
  const full=await st.cmds['orders.snapshot']({hydrate:true},()=>{});assert.equal(full.orders.length,3,'normal initial pull is still complete');
  // Explicit allowed origins must agree with the deployed station frame header.
  const toml=fs.readFileSync('netlify.toml','utf8');const frame=toml.slice(toml.indexOf('for = "/design-1.html"')).split('[[headers]]')[0];
  assert.match(frame,/frame-ancestors 'self' https:\/\/brites-charm-sorter.goldenspike.app https:\/\/goldenspike.app/);assert(!frame.includes('https://*'));
  // Setting migration keeps opt-outs but adopts the conservative live cadence.
  const settings={pollMinutes:10,pollOrders:'off',v:21};const cfg={localStorage:{getItem:()=>JSON.stringify(settings)},DEFAULTS:{stock:{},angleStep:10}};vm.createContext(cfg);vm.runInContext(html.slice(html.indexOf('function loadSettings()'),html.indexOf('function saveSettings()')),cfg);
  const migrated=cfg.loadSettings();assert.equal(migrated.pollMinutes,10);assert.equal(migrated.pollOrders,'off');assert.equal(migrated.finalOptimizeCount,85);assert.equal(migrated.maxFill,.80);
  console.log('Continuous intake OK: 85-piece threshold, capacity fallback, draft budgets, shop dates/DST, lazy loading, no New badges, delta reuse, complete snapshots and exact frame origins');
})().catch(e=>{console.error(e);process.exitCode=1;});

// Incremental nesting must not move a finished sheet; at 85 it frees automatic
// positioning pins for a full search, retaining explicit operator pins.
(async()=>{
 function setup(oldCount,newCount,{sealed=false}={}){
   const mk=i=>({id:'c'+i,poolId:'p'+i,areaPt2:1,orderDate:i});
   const pg={runId:'r',metal:'gold',page:sealed?2:1,charms:Array.from({length:oldCount},(_,i)=>mk(i)),placements:Array.from({length:oldCount},(_,i)=>({id:'c'+i,cxPt:i,cyPt:1,angle:0})),status:'complete',fileBase:'draft',sheetId:'draft',persistedDone:true};
   const done={runId:'r',metal:'gold',page:1,charms:[mk(999)],placements:[{id:'c999'}],status:'complete',fileBase:'sealed',sheetId:'sealed',intakeFinalized:true,persistedDone:true};
   const pages=sealed?[done,pg]:[pg],runs={runId:'r',status:'processed'},row={order:{receiptId:'incoming'},state:'pulled',spec:{material:'gold'},poolIds:[]};let starts=0;
   const c={window:{},O,S:{settings:{maxFill:.80,optimizeAt:85,finalOptimizeCount:85,budgetS:180},sheets:{gold:{pages,active:0}}},allSheets:()=>pages,pagesOf:()=>pages,activePage:()=>pages[c.S.sheets.gold.active],activeCharms:p=>p.charms,Orders:{rows:()=>[row]},Sets:{ofRun:()=>[]},Gate:{state:()=>({forceFill:{}}),modern:()=>true,nestable:()=>true},Pool:{addAll:async()=>{const p=pages[c.S.sheets.gold.active];for(let i=oldCount;i<oldCount+newCount;i++){const charm=mk(i);p.charms.push(charm);row.poolIds.push(charm.poolId);}row.state='pooled';},update:async()=>{}},usableArea:()=>10000,inflatedArea:x=>x.areaPt2,agent(){},startNest:p=>{starts++;p.status='complete';p.persistedDone=true;p.dirty=false;},sleep:async()=>{throw Error('unexpected unfinished worker');},RunCtl:{onSheetDone(){throw Error('unexpected hold');}}};
   const a=bridge.indexOf('const LiveNest ='),b=bridge.indexOf('\n/*',a+20);let code=bridge.slice(a,b).replace('    await finish(run);','    /* External storage verified separately. */');vm.createContext(c);vm.runInContext(code,c);
   return {c,pg,done,run:runs,starts:()=>starts,live:c.window.LiveNest};
 }
 const partial=setup(35,10,{sealed:true});await partial.live.add(partial.run);assert.equal(partial.pg.charms.length,45);assert.equal(partial.done.charms.length,1);assert.equal(partial.done.placements[0].id,'c999');assert.equal(partial.starts(),1);assert.equal(partial.live.prepareSheet(partial.pg).phase,'fill');
 const final=setup(80,5);await final.live.add(final.run);const fixed=final.pg.charms[0];fixed.pinned={cxPt:1,cyPt:2,angle:0};const auto=final.pg.charms[1];auto.pinned={cxPt:2,cyPt:2,angle:0};auto.arrivalPin=true;
 assert.equal(final.live.prepareSheet(final.pg).phase,'final');assert(fixed.pinned);assert.equal(auto.pinned,null);assert.equal(final.pg.placements.length,80,'previous layout seeds the next search');
 console.log('Live nesting OK: finalized sheets fixed, arrivals append to draft, full search at 85, operator pins preserved');
})().catch(e=>{console.error(e);process.exitCode=1;});

(async()=>{
 const committed={setId:'old',group:'dispatch',committedAt:1,sheetIds:['sealed']},page={runId:'r',sheetId:'sealed',setId:'old',outputs:{},persistedDone:true,metal:'gold'};
 const c={selected:()=>({}),policy:()=>({include:true}),modern:()=>true,allSheets:()=>[page],Sets:{ofRun:()=>[committed],ensure:async()=>{throw Error('must not reallocate a committed sheet');}}};vm.createContext(c);
 const a=bridge.indexOf('  async function assembleNow('),b=bridge.indexOf('  function editable(',a);vm.runInContext(bridge.slice(a,b),c);
 await c.assembleNow({runId:'r'});assert.equal(page.setId,'old');assert.deepEqual(committed.sheetIds,['sealed']);
 console.log('Set rollover OK: committed sheets stay in their original set');
})().catch(e=>{console.error(e);process.exitCode=1;});

(async()=>{
 const solver=require('../../charm-nest-solver');
 const pieces=[38,38,6].map((w,i)=>({id:String(i),order:String(i),orderDate:i+1,w,h:20,scale:1,bits:new Uint8Array(w*20).fill(1),areaPt2:w*20,centerPt:[w/2,10]}));
 const job={sheet:{wPt:100,hPt:20,insetPt:0},pieces,angles:[0],fineRes:1,coarseRes:1,clearancePt:0,timeBudgetMs:300,maxTrials:2};
 const old=await solver.solve({...job,maxFill:.74}),raised=await solver.solve({...job,maxFill:.80});
 assert.equal(old.placements.length,1);assert.equal(raised.placements.length,2);assert.equal(raised.density,.76);assert.deepEqual(raised.rejects,['2']);assert(solver.verify({...job,maxFill:.80},raised.placements,2).ok);
 console.log('80% fill OK: admits material above the old cap, holds excess and verifies the layout');
})().catch(e=>{console.error(e);process.exitCode=1;});
