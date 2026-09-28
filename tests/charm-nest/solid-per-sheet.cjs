// 10K and 14K join the set a sheet at a time, each by its own "Include in current set" (Paul, 28 Sep: ticking it
// took every sheet of the metal, the partial ones too).
const assert=require('node:assert/strict'),fs=require('fs'),vm=require('vm'),Ops=require('../../charm-nest-operations.js'),O=require('../../charm-nest-orders.js');
const source=fs.readFileSync('charm-nest-bridge.js','utf8'),start=source.indexOf('const Gate ='),end=source.indexOf('/* ═══ 21',start);
const ops=Ops.create(),sets=[],saved=new Map();
const run={runId:'r',releasePolicy:2,status:'review',step:'engrave',solidIncluded:{gold14k:true},errors:[]};
const sheet=(n,metal='gold14k')=>({metal,page:n,runId:'r',sheetId:metal+'-'+n,draft:true,status:'complete',outputs:{ai:'a'},persistedDone:true,verification:{ok:true},placements:[{id:'c'+n}],charms:[{id:'c'+n,poolId:'p'+n}]});
const pages=[sheet(1),sheet(2)];
const ctx={window:{CharmNestOrders:O,CharmNestOperations:ops},B:{run,sets:new Map()},S:{mode:'nest',settings:{},cloud:{ok:true},library:{rows:[]}},O,allSheets:()=>pages,pagesOf:m=>pages.filter(p=>p.metal===m),refreshAllCards:()=>{},Session:{schedule(){}},document:{getElementById:()=>null},toast:()=>{},Pool:{update:async()=>{}},Orders:{rows:()=>[]},Engrave:{items:()=>new Map(),saveSheetBacks:async()=>{}},RunCtl:{save:async()=>{},poke(){},onSheetDone(){},membershipUpdated(){}},CN:{sheetFileBase:sh=>'Set-1-Sheet-'+sh.page,persistSheet:()=>{throw Error('must not re-upload artwork');},renderLibrary(){}},Sets:{ofRun:()=>sets,ensure:async()=>{const set={setId:'set',runId:'r',seq:1,day:'2026-09-28',group:'dispatch',sheetIds:[],labelFiles:[],materials:[],orders:{}};sets.push(set);return set;},save:async()=>{},labelsReady:()=>true,onSheetSaved:async sh=>{const set=sets[0];if(!set.sheetIds.includes(sh.sheetId))set.sheetIds.push(sh.sheetId);}},api:async(name,body)=>{if(body.sheet)saved.set(body.sheet.id,body.sheet);return {};},stockFor:()=>({wIn:1,hIn:1}),labelOf:x=>x,esc:x=>x};
vm.createContext(ctx);vm.runInContext(source.slice(start,end),ctx);const Gate=ctx.window.Gate;
const inSet=()=>pages.filter(p=>!p.draft&&p.setId==='set').map(p=>p.page);
(async()=>{
  // a run from before: the metal's tick has both sheets in the set
  await Gate.assemble(run);assert.deepEqual(inSet(),[1,2]);
  // Sheet 2 unticked on its own card: Sheet 1 stays, Sheet 2 leaves, the metal's tick is handed over to the sheets
  await Gate.changeMembership('gold14k',false,pages[1]);
  assert.deepEqual(inSet(),[1],'unticking Sheet 2 leaves Sheet 1 in the set');
  assert(!pages[1].label);assert.equal(saved.get('gold14k-2').solidIncluded,false);assert.equal(run.solidIncluded.gold14k,false);
  assert.equal(Gate.solidSelected('gold14k',pages[0]),true);assert.equal(Gate.solidSelected('gold14k',pages[1]),false);
  // a new sheet starts out of the set; Sheet 1 is still nested and its metal still counts as in
  pages.push(sheet(3));await Gate.assemble(run);assert.deepEqual(inSet(),[1],'a new sheet waits for its own tick');
  assert.equal(Gate.nestable(pages[2],run),true,'the metal still nests while one of its sheets is in');
  // each sheet by its own tick, both ways
  await Gate.changeMembership('gold14k',true,pages[2]);assert.deepEqual(inSet(),[1,3]);
  await Gate.changeMembership('gold14k',false,pages[0]);assert.deepEqual(inSet(),[3],'unticking Sheet 1 keeps Sheet 3');
  // the library shows each record by its own sheet
  const rows=Gate.projectLibraryRecords(pages.map(p=>({id:p.sheetId,runId:'r',metal:'gold14k',draft:true})));
  assert.deepEqual(rows.map(r=>r.solidIncluded),[false,false,true]);
  // 10K untouched by 14K ticks; a fresh 10K sheet ticked alone joins alone
  pages.push(sheet(1,'gold10k'),sheet(2,'gold10k'));run.solidIncluded.gold10k=false;
  await Gate.changeMembership('gold10k',true,pages[3]);
  assert.deepEqual(pages.filter(p=>p.metal==='gold10k'&&!p.draft).map(p=>p.page),[1],'10K Sheet 2 stays out');
  assert.deepEqual(inSet(),[3,1]);
  // nothing ticked: the metal waits for you again
  await Gate.changeMembership('gold14k',false,pages[2]);assert(!Gate.nestable(pages[2],run),"nothing ticked: the metal waits");
  console.log('solid-per-sheet: ok');
})().catch(e=>{console.error(e);process.exit(1);});
