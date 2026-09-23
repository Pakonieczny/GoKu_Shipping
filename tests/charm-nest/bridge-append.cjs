const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const bridge=fs.readFileSync('charm-nest-bridge.js','utf8');
const O=require('../../charm-nest-orders.js');

function liveCase({metal='rose',newRun=false,cut=false,full=false,committed=false,legacy=false,nothing=false}={}){
  const old={runId:'old-run',metal,page:1,sheetId:'physical-sheet',setId:'old-set',charms:[{id:'old',poolId:'old-pool'}],placements:[{id:'old',cxPt:20,cyPt:10,angle:0}],rosePlan:metal==='rose'?{profile:{axis:'x'},lines:[[[0,20],[40,20]]]}:null,roseProtected:metal==='rose'?{profile:{axis:'x'},placements:[{id:'old',cxPt:20,cyPt:10,angle:0}]}:null,roseStock:{id:'rose-stock',revision:0},roseCutAt:cut?123:null,releaseFull:full,status:'complete',fileBase:'saved',persistedDone:true};
  const prim={pages:[old],active:0},sheets={[metal]:prim};let starts=0,added=0,which;
  const run={runId:newRun?'new-run':'old-run',groups:{rose:'rose'},status:'processed'};
  const c={window:{},B:{sets:new Map()},O,S:{settings:{budgetS:180,maxFill:.8},sheets},
    METALS:[{key:metal}],activeCharms:p=>p.charms,allSheets:()=>prim.pages,activePage:()=>prim.pages[prim.active],pagesOf:()=>prim.pages,
    Orders:{rows:()=>[{state:'pulled',spec:{material:metal},order:{receiptId:'new'}},...(legacy?[{state:'pulled',spec:{material:'gold'},order:{receiptId:'new'}}]:[])]},
    Sets:{ofRun:()=>committed||legacy?[{setId:'old-set',group:metal,committedAt:committed?123:null,sheetIds:['physical-sheet'],materials:[metal]}]:[],ensure:async()=>{throw Error('unexpected group allocation');},save:async()=>{}},
    Gate:{state:()=>({forceFill:{}}),modern:()=>!legacy,nestable:()=>true},
    Pool:{addAll:async()=>{const page=prim.pages[prim.active];assert.equal(page.runId,run.runId,'target page must belong to the incoming run before Pool checks it');if(!nothing)page.charms.push({id:'new',poolId:'new-pool'});which=page;}},
    addPage:()=>{added++;const p={runId:prim.pages[0].runId,metal,page:prim.pages.length+1,sheetId:null,charms:[],placements:[],status:'idle'};prim.pages.push(p);return p;},
    removePage:p=>{const i=prim.pages.indexOf(p);if(i>0)prim.pages.splice(i,1);prim.active=prim.pages.length-1;},
    usableArea:()=>100,inflatedArea:()=>1,agent(){},startNest:p=>{starts++;p.status='complete';p.dirty=false;p.persistedDone=true;p.runHold=false;},sleep:async()=>{throw Error('unexpected worker wait');}};
  vm.createContext(c);
  const a=bridge.indexOf('const LiveNest ='),b=bridge.indexOf('\n/*',a+20);
  vm.runInContext(bridge.slice(a,b).replace('    await finish(run);','    /* Set persistence is tested elsewhere. */'),c);
  return {c,run,old,prim,added:()=>added,starts:()=>starts,which:()=>which};
}

(async()=>{
  const same=liveCase();same.old.runHold=true;same.old.intakeFinalized=true;
  const before=JSON.stringify({placements:same.old.placements,plan:same.old.rosePlan,stock:same.old.roseStock,id:same.old.sheetId});
  assert.equal(same.c.window.LiveNest.closed(same.old),false,'an uncut green line stays open for incoming orders');
  await same.c.window.LiveNest.add(same.run);
  assert.equal(same.added(),0);assert.equal(same.which(),same.old);assert.equal(same.starts(),1);
  assert.equal(JSON.stringify({placements:same.old.placements,plan:same.old.rosePlan,stock:same.old.roseStock,id:same.old.sheetId}),before);
  assert.equal(same.old.appendOnly,true);

  for(const variant of [{cut:true},{full:true},{committed:true}]){
    const t=liveCase(variant);
    assert.equal(t.c.window.LiveNest.closed(t.old),true,JSON.stringify(variant));
    await t.c.window.LiveNest.add(t.run);
    assert.equal(t.added(),1,JSON.stringify(variant));assert.equal(t.old.placements[0].id,'old');
  }
  // A material linked to an arriving order comes through even when nothing of it arrives. After its sheet was released,
  // the page opened for it stayed empty and queued, and every later arrival stopped the run.
  for(const metal of ['rose','gold']){
    const linked=liveCase({metal,full:true,nothing:true});await linked.c.window.LiveNest.add(linked.run);
    assert.equal(linked.added(),1,metal);assert.equal(linked.starts(),0,metal+': nothing is nested');
    assert.deepEqual(linked.prim.pages,[linked.old],metal+': the empty page is taken away again');
  }
  // A cancelled order's pieces taken off a filling sheet leave it waiting to be arranged again. With no order for its
  // metal coming in, nothing started it, and every later arrival waited on it for good.
  const dirtied=liveCase({metal:'silver',nothing:true});Object.assign(dirtied.old,{dirty:true,status:'ready',placements:[]});
  await dirtied.c.window.LiveNest.add(dirtied.run);
  assert.equal(dirtied.starts(),1,'the sheet is arranged again');assert.equal(dirtied.old.dirty,false);
  const cross=liveCase({newRun:true});await cross.c.window.LiveNest.add(cross.run);
  assert.equal(cross.added(),1);assert.notEqual(cross.which(),cross.old);
  assert.equal(cross.old.runId,'old-run');assert.equal(cross.old.sheetId,'physical-sheet');assert.equal(cross.old.roseStock.id,'rose-stock');
  const legacy=liveCase({legacy:true});
  await assert.rejects(legacy.c.window.LiveNest.add(legacy.run),/protected Rose Gold contour/);
  assert.equal(legacy.which(),undefined,'legacy regroup is blocked before pooling or altering the protected sheet');
  assert.equal(legacy.old.sheetId,'physical-sheet');assert.equal(legacy.old.placements[0].id,'old');

  const first={metal:'rose',runId:'previous',sheetId:'rose-live',setId:'rose-set',page:1,charms:[{id:'old',poolId:'old-pool'}],placements:[{id:'old',cxPt:20,cyPt:10,angle:0}],rosePlan:{profile:{axis:'x'},lines:[[[0,20],[40,20]]]},roseStock:{id:'physical-stock'},status:'complete'};
  const second={metal:'rose',runId:'previous',sheetId:'obsolete',page:2,charms:[{id:'obsolete',poolId:'obsolete-pool'}],placements:[],status:'complete'};
  const gold={metal:'gold',runId:'previous',sheetId:'gold-partial',page:1,charms:[{id:'g',poolId:'g-pool'}],placements:[{id:'g',cxPt:1,cyPt:1}],status:'partial'};
  const materials={rose:{...first,pages:[first,second],active:1,cardEl:{}},gold:{...gold,pages:[gold],active:0,cardEl:{}}};
  // In the actual page each first page is the primary card object.
  Object.assign(materials.rose,first);materials.rose.pages[0]=materials.rose;
  Object.assign(materials.gold,gold);materials.gold.pages[0]=materials.gold;
  const runState={window:{Gate:true},B:{run:{status:'complete'},orders:{rows:[]},engrave:{},review:{},pool:{}},S:{sheets:materials},METALS:[{key:'rose'},{key:'gold'}],Carry:{capture(){}},Orders:{rows:()=>[],render(){}},Gate:{state:()=>({plan:{},forceFill:{}})},Sets:{ofRun:()=>[]},Recall:{state:()=>({})},allSheets:()=>Object.values(materials).flatMap(p=>p.pages),showPage(m,n){materials[m].active=n;},sheetDirty(){},stopNest(){},toast(){},renderBanner(){},renderRail(){},updateTopSub(){},Engrave:{render(){}},Review:{render(){}}};
  vm.createContext(runState);
  const ca=bridge.indexOf('  function clearRunState('),cb=bridge.indexOf('  function setRunMode(',ca);
  vm.runInContext(bridge.slice(ca,cb),runState);
  runState.clearRunState();
  assert.equal(materials.rose.pages.length,1,'obsolete page cleared while green page survives');
  assert.equal(materials.rose.sheetId,'rose-live');assert.equal(materials.rose.roseStock.id,'physical-stock');
  assert.equal(materials.rose.placements[0].id,'old');assert.deepEqual(materials.rose.rosePlan.lines,[[[0,20],[40,20]]]);
  assert.equal(materials.gold.sheetId,'gold-partial');assert.equal(materials.gold.placements[0].id,'g');
  assert.equal(runState.B.run,null);
  const newer={metal:'rose',runId:'previous',sheetId:'later-green',page:2,charms:[{id:'later'}],placements:[{id:'later',cxPt:30,cyPt:15,angle:0}],roseProtected:{profile:{axis:'x'},lines:[[[0,25],[40,25]]]},roseStock:{id:'later-stock'},status:'complete'};
  const spent={metal:'rose',runId:'previous',sheetId:'earlier-spent',page:1,charms:[{id:'spent',poolId:'spent-pool'}],placements:[{id:'spent'}],releaseFull:true,status:'complete'};
  const multi={...spent,pages:[spent,newer],active:0,cardEl:{}};Object.assign(multi,spent);multi.pages[0]=multi;
  runState.S.sheets.rose=multi;runState.B.run={status:'complete'};
  runState.clearRunState();
  assert.equal(multi.pages.length,2,'an uncut protected second page survives clearing an older full first page');
  assert.equal(multi.pages[1],newer);assert.equal(newer.sheetId,'later-green');assert.equal(newer.roseStock.id,'later-stock');
  assert.equal(multi.active,1,'the retained partial page remains visible');
  newer.status='nesting';runState.B.run={status:'complete'};
  assert.equal(runState.clearRunState(),false,'a protected worker must settle before clearing its run');
  assert.equal(runState.B.run.status,'complete');assert.equal(newer.sheetId,'later-green');assert.equal(newer.roseStock.id,'later-stock');
  const savedCharm={id:'protected',poolId:'protected-pool',sourceId:'old-source',outline:{},ringGeometryVersion:1};
  const savedPage={metal:'rose',charms:[savedCharm],placements:[{id:'protected',cxPt:20,cyPt:15,angle:0}],rosePlan:{profile:{axis:'x'},lines:[[[0,30],[40,30]]]},roseStock:{id:'reserved'},verification:{ok:true},outputs:{ai:'saved'}};
  const recovery={sources:[],poolSources:{},unassigned:[],sheets:[{pages:[savedPage]}],jobs:[]};
  const recoverContext={G:{pathRole(){throw Error('protected geometry must not be rewritten');}},P:{integrateRings(){throw Error('protected geometry must not be rewritten');}},S:{settings:{}}};
  vm.createContext(recoverContext);
  const ra=bridge.indexOf('  async function repairRecoveredGeometry('),rb=bridge.indexOf('  function cloneCharm(',ra);
  vm.runInContext(bridge.slice(ra,rb),recoverContext);
  assert.equal(await recoverContext.repairRecoveredGeometry(recovery),0);
  assert.equal(savedPage.placements[0].id,'protected');assert.equal(savedPage.roseStock.id,'reserved');assert.equal(savedPage.verification.ok,true);
  const freshCharm={id:'new',poolId:'new-pool',sourceId:'fresh-source',outline:{},ringGeometryVersion:1};
  savedPage.charms.push(freshCharm);savedPage.backPool=[{poolId:'protected-pool',approved:true}];
  let repaired=0;recoverContext.G.pathRole=()=> 'cut';recoverContext.P.integrateRings=()=>({left:[],welded:true});recoverContext.P.buildSilhouettes=async()=>{repaired++;};
  assert.equal(await recoverContext.repairRecoveredGeometry(recovery),1);
  assert.equal(repaired,1,'only the newly arrived charm is upgraded');
  assert.equal(savedPage.placements[0].id,'protected');assert.equal(savedPage.roseStock.id,'reserved');
  assert.equal(savedPage.rosePlan.lines.length,1);assert.equal(savedPage.backPool[0].poolId,'protected-pool');
  assert.equal(savedPage.appendOnly,true);assert.equal(savedPage.dirty,true);
  const older={runId:'older',releasePolicy:1,intakeRecovery:null};
  const oldProtected={metal:'rose',runId:'older',sheetId:'same-sheet',roseProtected:{profile:{axis:'x'}},placements:[{id:'old'}]};
  const upgradeContext={Sets:{ofRun:()=>[]},allSheets:()=>[oldProtected],S:{cloud:{ok:true}}};
  vm.createContext(upgradeContext);
  const ua=bridge.indexOf('  async function upgrade(run)'),ub=bridge.indexOf('  let assemblyQueue',ua);
  vm.runInContext(bridge.slice(ua,ub),upgradeContext);
  await assert.rejects(upgradeContext.upgrade(older),/saved Rose Gold contour/);
  assert.equal(oldProtected.sheetId,'same-sheet');assert.equal(older.releasePolicy,1);
  // A sheet with nothing to nest leaves the queue, whatever started it: arrivals wait on a queued sheet of their material.
  const html=fs.readFileSync('charm-nest-1.html','utf8'),sa=html.indexOf('function startNestReady('),sb=html.indexOf('/* ═══ 9b',sa);
  const ops=[],nest={assert,window:{CharmNestOperations:{run:(o,work)=>{const p=Promise.resolve().then(()=>work({resources:o.resources,active:true}));ops.push(p);return p;}}},S:{settings:{},nestQueue:[]},
    labelOf:x=>x,renderCard(){},toast(){},activeCharms:sh=>sh.charms.filter(c=>!c.excluded)};
  const empty={metal:'rose',status:'ready',charms:[],placements:[]},excluded={metal:'gold',status:'ready',charms:[{excluded:true}],placements:[]};
  nest.allSheets=()=>[empty,excluded];vm.createContext(nest);vm.runInContext(html.slice(sa,sb),nest);
  nest.startNestReady(empty);nest.startNestReady(excluded);await Promise.all(ops);
  assert.deepEqual([empty.status,excluded.status,nest.S.nestQueue.length],['idle','ready',0],'a sheet with nothing to nest does not stay queued');
  console.log('Bridge append OK: same-run Rose green contour, cut/full/committed closure, cross-run isolation, retained sheet and stock on run clearing, no empty sheet left queued');
})().catch(error=>{console.error(error);process.exitCode=1;});
