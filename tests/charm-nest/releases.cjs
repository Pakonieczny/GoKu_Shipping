const assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm');
const O = require('../../charm-nest-orders.js');
// The approved rehearsal origin must not open production or another preview to bridge messages.
{
 const html=fs.readFileSync(require('node:path').join(__dirname,'../../design-1.html'),'utf8');
 const start=html.indexOf('  const ALLOWED = new Set('), end=html.indexOf('  const S = { nonce:',start);
 const gate=html.slice(start,end);
 for (const [sandbox,page,sender,expected] of [
  [true,'https://deploy-preview-12.goldenspike.app','https://deploy-preview-12.goldenspike.app',true],
  [false,'https://deploy-preview-12.goldenspike.app','https://deploy-preview-12.goldenspike.app',false],
  [true,'https://design-1.goldenspike.app','https://deploy-preview-12.goldenspike.app',false],
  [true,'https://deploy-preview-12.goldenspike.app','https://deploy-preview-13.goldenspike.app',false],
  [true,'https://deploy-preview-12.goldenspike.app','https://example.com',false]])
   assert.equal(vm.runInNewContext(gate+'originOk(sender)',{SANDBOX:sandbox,location:{origin:page},sender}),expected);
}

const base = {verified:true, placed:3, full:false};
for (const material of ['gold','silver']) {
  assert.equal(O.sheetRelease({...base,material},{seq:2, selected:{[material]:true},urgent:true}).include,false);
  assert.equal(O.sheetRelease({...base,material,full:true},{seq:1}).include,true);
  assert.equal(O.sheetRelease({...base,material,full:true,stopped:true},{seq:1}).include,false);
  assert.equal(O.sheetRelease({...base,material,full:true,dirty:true},{seq:1}).include,false);
  assert.deepEqual(O.sheetRelease({...base,material,topup:true},{seq:1}),{include:false,reason:'Topping up · later orders fill its gaps first'},'a topping-up sheet waits and says why');
}
for (const material of ['gold10k','gold14k']) {
  assert.equal(O.sheetRelease({...base,material},{seq:2}).include,false);
  assert.equal(O.sheetRelease({...base,material},{seq:1,selected:{[material]:true}}).include,true);
  assert.equal(O.sheetRelease({...base,material,verified:false},{seq:1,selected:{[material]:true}}).include,false);
}
for (const seq of [1,2,3,4,5,6]) assert.equal(O.sheetRelease({...base,material:'rose'},{seq}).include,seq%2===0);
const code=fs.readFileSync(require('node:path').join(__dirname,'../../charm-nest-bridge.js'),'utf8');
const part=(name,end)=>code.slice(code.indexOf(`const ${name} = window.${name} =`),code.indexOf(end,code.indexOf(`const ${name} = window.${name} =`)));
const c=vm.createContext({assert,O,console});
vm.runInContext(`
const window={CharmNestOrders:O}, S={cloud:{ok:true},settings:{}}, METALS=['gold','silver','rose','gold10k','gold14k'].map(key=>({key}));
const B={run:{runId:'run-test',releasePolicy:2,solidIncluded:{}},sets:new Map(),engrave:{items:new Map()},pool:{rows:new Map()},orders:{rows:[],byKey:new Map()}};
const pages=[], writes=[]; let seq=1, allocations=0;
const allSheets=()=>pages, labelOf=m=>m, today=()=> '2026-09-19', refreshAllCards=()=>{};
const api=async(n,b)=>{writes.push(b);return {}};
const Orders={rows:()=>B.orders.rows}, Engrave={items:()=>B.engrave.items,writeBacks:async()=>{}};
const Pool={update:async(ids,p)=>writes.push({ids,p})};
const RunCtl={onSheetDone(){}};
const CN={sheetFileBase:sh=>'Set-'+sh.seq+'-'+sh.metal+'-'+sh.sheetIndex,persistSheet:async(sh)=>{writes.push(sh.fileBase);const s=[...B.sets.values()][0];s.sheetIds.push(sh.sheetId);s.materials.push(sh.metal)}};
const Sets={onSheetSaved:async(sh)=>{writes.push(sh.fileBase);const set=[...B.sets.values()][0];if(!set.sheetIds.includes(sh.sheetId))set.sheetIds.push(sh.sheetId);if(!set.materials.includes(sh.metal))set.materials.push(sh.metal);},labelsReady:()=>true,ofRun:()=>[...B.sets.values()],save:async()=>{},ensure:async(runId,group,o)=>{if(o.roseOnly&&seq%2) return null;allocations++;const s={runId,group,seq:seq++,day:today(),setId:'set-'+allocations,sheetIds:[],labelFiles:[],materials:[],orders:{}};B.sets.set(s.setId,s);return s}};
const page=(metal,full=false)=>({metal,runId:B.run.runId,sheetId:metal+'-id',draft:true,outputs:{},persistedDone:true,placements:[{}],verification:{ok:true},releaseFull:full,charms:[],status:'complete'});
`,c);
vm.runInContext(part('Gate','/* ═══ 21'),c);
(async()=>{
 // Exercise the actual cloud reconstruction path, including a run with no set yet.
 const recovery=vm.createContext({assert,console});
 vm.runInContext(`
 const rows=[{key:'o/a',state:'written',poolIds:['p1'],order:{receiptId:'o'},line:{}}];
 const page={charms:[],placements:[],metal:'gold'};page.pages=[page];
 const S={sheets:{gold:page}},B={pool:{rows:new Map()}};
 const record={id:'draft1',metal:'gold',draft:true,releaseFull:false,day:'2026-09-19',fileBase:'working',status:'complete',verification:{ok:true},poolIds:['p1'],placements:[{id:'c1',cxPt:1,cyPt:2}],charms:[{id:'c1',poolId:'p1',name:'one'}]};
 const api=async(n,b)=> b.op==='listSheets'?{sheets:[{id:record.id}]}:b.op==='getSheet'?{sheet:record}:b.op==='poolGet'?{pools:{p1:{poolId:'p1',state:'ready',sheetId:'draft1',setId:null,sku:'A',orderId:'o'}}}:{};
 const Orders={rows:()=>rows},Sets={byRun:()=>new Map(),keyOf:()=>''};
 const Master={entryFor:()=>({})},Pool={masterCharm:async()=>({id:'source',charms:[{sourceId:'source'}]}),cloneCharm:(c,id)=>({...c,id}),charmOf:id=>page.charms.find(c=>c.poolId===id)};
 const jobs=new Map(),Engrave={items:()=>jobs};
 const computeSaturation=()=>{},renderCard=()=>{},agent=()=>{};
 const sheetDirty=p=>{p.dirty=true;p.releaseFull=false;p.placements=[];p.status='ready';};
 `,recovery);
 const restore=code.slice(code.indexOf('  async function restoreRunSheets(rec)'),code.indexOf('  function onComplete(r)',code.indexOf('  async function restoreRunSheets(rec)')));
 vm.runInContext(restore,recovery);
 await vm.runInContext(`(async()=>{
 await restoreRunSheets({runId:'working',setIds:[]});
 assert.equal(page.charms.length,1,'a working run restores even before a set number exists');
 assert.equal(page.setId,null);assert.equal(page.seq,null);assert.equal(page.draft,true);
 assert.equal(rows[0].state,'pooled','saved draft placement is never treated as released');
 assert.equal(page.status,'ready','cloud-only draft gets rebuilt and verified before release');
 assert.equal(page.charms[0].pinned,null);
 })()`,recovery);
 const classifier=code.slice(code.indexOf('  let classifyPass = null'),code.indexOf('  /* ── 7.2',code.indexOf('  let classifyPass = null')));
 const eng=vm.createContext({assert,console});
 vm.runInContext(`
 const rows=[{state:'written',poolIds:['p1'],spec:{engraveCandidate:true},order:{receiptId:'1'},engrave:null}];
 let calls=0;const Orders={rows:()=>rows,render(){}},Gate={modern:()=>true},Pool={sheetOf:()=>({fileBase:'set-1'})};
 const jobs=new Map(),items=()=>jobs,window={},loadFonts=async()=>{},classify=async r=>{calls++;r.engrave={needed:true,state:'awaiting'}},render=()=>{};
 `+classifier,eng);
 await vm.runInContext(`(async()=>{await classifyAll();assert.equal(calls,1,'set assembly must not skip personalization after promoting a row to written')})()`,eng);
 const resume=vm.createContext({assert,console});
 vm.runInContext(`
 const rec={runId:'r1',step:'engrave',orders:['o'],lines:{'o/a':{updateTs:10,state:'pooled',poolIds:['p1']}}};
 const B={},O={stepIndex:s=>['pull','pool','nest','checkpoint','engrave'].indexOf(s)};
 const row={key:'o/a',order:{updateTs:10},poolIds:[],state:'pulled'};let restores=0;
 const api=async()=>({run:rec}),agent=()=>{},toast=()=>{},renderBanner=()=>{},save=async()=>{},loop=async()=>{};
 const Orders={rows:()=>[row],interpretAll(){},pull:async(run,opts)=>{assert.equal(run,null);assert.deepEqual(opts.receiptIds,['o']);}};
 const restoreRunSheets=async()=>restores++,Pool={addAll:async()=>{}},allSheets=()=>[],reviewStop=()=>false,preservePendingSheets=()=>{};
 `+code.slice(code.indexOf('  async function resumeRun(runId)'),code.indexOf('  async function restoreRunSheets(rec)')),resume);
 await vm.runInContext(`(async()=>{await resumeRun('r1');assert.equal(rec.step,'engrave');assert.equal(restores,1);assert.equal(row.poolIds[0],'p1');assert.equal(row.state,'pooled');})()`,resume);
 await vm.runInContext(`(async()=>{
 pages.push(page('gold'),page('silver'),page('rose'),page('gold10k'),page('gold14k'));
 await Gate.assemble(B.run);assert.equal(allocations,0,'partials/unselected solids/odd rose must not consume a set number');
 pages[0].releaseFull=true;await Gate.assemble(B.run);
 assert.equal(allocations,1);assert.equal(pages[0].setId,'set-1');assert.equal(pages[1].setId,undefined);assert.equal(pages[2].setId,undefined);
 B.run.solidIncluded.gold10k=true;await Gate.assemble(B.run);assert.equal(pages[3].setId,'set-1');assert.equal(pages[4].setId,undefined);
 const n=writes.length;await Gate.assemble(B.run);assert.equal(writes.length,n,'repeat checkpoint cannot duplicate outputs');
 B.run.solidIncluded.gold10k=false;await Gate.assemble(B.run);assert.equal(pages[3].setId,null);assert.equal(pages[3].draft,true);assert(![...B.sets.values()][0].sheetIds.includes('gold10k-id'));
 B.sets.clear();pages.splice(0,pages.length,page('rose'));await Gate.assemble(B.run);assert.equal(pages[0].seq,2,'even set accepts accumulated rose');
 B.sets.clear();pages.splice(0,pages.length,page('silver',true));
 const write=Sets.onSheetSaved; Sets.onSheetSaved=async()=>{throw new Error('test upload interrupted')};
 await assert.rejects(Gate.assemble(B.run),/interrupted/);assert.equal(pages[0].draft,true);assert.equal(pages[0].setId,undefined);
 Sets.onSheetSaved=write;await Promise.all([Gate.assemble(B.run),Gate.assemble(B.run)]);
 assert.equal([...B.sets.values()][0].sheetIds.length,1,'failed publication can retry; concurrent checkpoints serialize without duplicate membership');
 // a sheet that leaves the set does not hand its number to the next one while a later sheet still carries that number
 B.sets.clear();const g1={...page('gold',true),sheetId:'g1'},g2={...page('gold',true),sheetId:'g2'},g3={...page('gold',true),sheetId:'g3'};
 pages.splice(0,pages.length,g1,g2);await Gate.assemble(B.run);assert.deepEqual([g1.sheetIndex,g2.sheetIndex],[1,2]);
 g1.releaseFull=false;await Gate.assemble(B.run);assert.equal(g1.setId,null,'the first sheet left the set');
 pages.push(g3);await Gate.assemble(B.run);assert.equal(g3.sheetIndex,3,'the next sheet takes a new number, not the one sheet 2 still carries: '+g3.sheetIndex);
 })()`,c);
 vm.runInContext(`const Session={copy:x=>structuredClone(x)};`,c); c.structuredClone=structuredClone;
 vm.runInContext(part('Carry','/* ═══ 20b'),c);
 await vm.runInContext(`(async()=>{
 const written={key:'o/a',state:'written',poolIds:['p1'],order:{updateTs:10},line:{sku:'A'}};
 const partial={key:'o/b',state:'pooled',poolIds:['p2'],order:{updateTs:10},line:{sku:'B'}};
 B.run.status='complete';B.orders.rows=[written,partial];B.pool.rows.set('p1',{state:'written'});
 B.engrave.items.set('o/b',{key:'o/b',state:'written',approvedBy:'Operator',text:'EXACT WORDS',backs:[{}]});
 Carry.capture();B.orders.rows=[structuredClone(written),structuredClone(partial)];B.orders.byKey=new Map();B.engrave.items.clear();B.pool.rows.clear();Carry.adopt();
 assert.equal(B.orders.rows[0].state,'written','already cut line is not pooled twice');
 assert.equal(B.orders.rows[1].state,'pulled');assert.equal(B.orders.rows[1].poolIds.length,0);
 assert.equal(B.engrave.items.get('o/b').approvedBy,'Operator');assert.equal(B.engrave.items.get('o/b').state,'approved');assert.equal(B.engrave.items.get('o/b').text,'EXACT WORDS');
 Carry.capture();B.orders.rows=[{...partial,line:{sku:'CHANGED'},state:'pulled'}];B.orders.byKey=new Map();B.engrave.items.clear();Carry.adopt();assert.equal(B.orders.rows[0].line.sku,'CHANGED');assert.equal(B.engrave.items.size,0,'changed Etsy line cannot inherit an old approval');
 })()`,c);
 // giving up a run gives up its unfinished work only; what a committed set cut stays on record as cut
 const abandon=vm.createContext({assert,console});
 vm.runInContext(`
 const calls=[],unclaimed=[];const B={pool:{rows:new Map([['p1',{state:'committed',setId:'s1'}],['p2',{state:'written',setId:'s1'}],['p3',{state:'written',setId:null}]])}};
 const rows=[{state:'committed',poolIds:['p1'],order:{receiptId:'1'}},{state:'written',poolIds:['p2'],order:{receiptId:'2'}},{state:'pooled',poolIds:['p3'],order:{receiptId:'3'}}];
 const Orders={rows:()=>rows,unclaim:async ids=>{unclaimed.push(...ids);}},Sets={ofRun:()=>[{setId:'s1',committedAt:1}]},S={cloud:{ok:true}};
 const api=async(n,b)=>{calls.push(b);return {};},save=async()=>{};
 `+code.slice(code.indexOf('  function releaseRun(r)'),code.indexOf('  function stop(why, fix, at)')),abandon);
 vm.runInContext(`releaseRun({runId:'r'});assert.deepEqual(calls.map(c=>c.poolIds),[['p3']],'only unfinished pieces are abandoned');assert.deepEqual(unclaimed,['3']);`,abandon);
 // undoing a set reopens that set's own orders at the station and in the sorter, and nobody else's
 const undo=vm.createContext({assert,console});
 vm.runInContext(`
 const calls=[],saved=[],updates=[];const B={run:null,pool:{rows:new Map([['p1',{setId:'s1'}],['p2',{setId:'s2'}]])}};
 const rows=[{state:'committed',poolIds:['p1'],order:{receiptId:'1'}},{state:'committed',poolIds:['p2'],order:{receiptId:'2'}}];
 const Orders={rows:()=>rows,render(){}},DesignLink={ensure:async()=>{},call:async(cmd,a)=>{calls.push([cmd,a.receiptIds]);return {};}};
 const save=async s=>saved.push(s.setId),Pool={update:async(ids,p)=>updates.push([ids,p.state])},RunCtl={save:async()=>{},renderBanner(){}},agent=()=>{};
 `+code.slice(code.indexOf('  async function undo(set)'),code.indexOf('  /** The Library\'s Sets view')),undo);
 await vm.runInContext(`(async()=>{
 await undo({setId:'s1',runId:'r',name:'Set 1',committed:['1'],committedAt:1});
 assert.deepEqual(calls,[['complete.undo',['1']]],'the station is asked to reopen the orders of this set, by name');
 assert.deepEqual(rows.map(r=>r.state),['written','committed'],'only the order of this set reopens; the other set stays committed');
 assert.deepEqual(updates,[[['p1'],'written']]);
 await undo({setId:'s2',runId:'r',name:'Set 2',committed:[],committedAt:1});
 assert.equal(calls.length,1,'a set that committed nothing has nothing to reopen at the station');assert.equal(rows[1].state,'committed');
 })()`,undo);
 // a commit saves the run's lines before the station marks the orders done: the server checks the set against them
 // before it records the set as complete, and a set refused after the station commit stopped the run for good
 const commit=vm.createContext({assert,console});
 vm.runInContext(`
 const window={},order=[],B={pool:{rows:new Map([['p1',{orderId:'1'}]])}},rows=[{state:'written',poolIds:['p1'],order:{receiptId:'1'}}];
 const Orders={rows:()=>rows},Review={add(){}},Pool={update:async()=>{}},agent=()=>{},today=()=>'2026-09-23',employeeName=()=>'Tester';
 const validateRelease=()=>{},evaluate=()=>({committable:['1'],held:{},gone:[]});
 const DesignLink={ensure:async()=>{},call:async(cmd,a)=>{order.push(cmd);return cmd==='ui.select'?{selected:a.receiptIds,refused:[]}:cmd==='complete.preview'?{jobs:[]}:{completed:a.receiptIds,refused:[]};}};
 const RunCtl={save:async()=>{order.push('run saved');}},save=async s=>{order.push('set saved '+s.status);};
 `+code.slice(code.indexOf('  async function commit(set, run, context)'),code.indexOf('  async function undo(set)')),commit);
 await vm.runInContext(`(async()=>{
 await commit({setId:'s1',runId:'r',name:'Set 1',orders:{'1':{}},labels:{files:[{path:'l.png',url:'u',sheet:'S1'}]}},{runId:'r'});
 assert.deepEqual(order,['ui.select','complete.preview','run saved','complete.commit','set saved complete'],'the run is saved before the station commit');
 assert.equal(rows[0].state,'committed');
 })()`,commit);
 // the next set of a run is numbered only after the committed set it follows is saved as committed: a commit whose record
 // did not save is saved again first, and a refusal names its cause instead of refusing every later set
 const sa=code.indexOf('const Sets = window.Sets = (() => {'),sb=code.indexOf('\n})();',code.indexOf('  return { releaseIssue, ensure,',sa))+6;
 for(const refuse of [false,true]){
   const calls=[],sets=vm.createContext({window:{},B:{sets:new Map(),run:null},S:{cloud:{ok:true}},O:{setLabel:n=>'Set-'+n,setFolder:(d,n)=>d+'/Set-'+n},today:()=>'2026-09-23',agent(){},RunCtl:{renderBanner(){}},labelOf:m=>m,
     api:async(fn,b)=>{calls.push(b.op==='setUpdate'?'save '+b.setId+' '+b.patch.status:'number after '+b.after);if(b.op==='setUpdate'&&refuse)throw new Error('Set cannot be completed: every sheet needs approved engraving');return b.op==='setAllocate'?{setId:'set-3',seq:3,day:'2026-09-23'}:{ok:true};}});
   vm.runInContext(code.slice(sa,sb),sets);
   const committed={setId:'set-2',runId:'r',group:'dispatch',seq:2,day:'2026-09-23',name:'Set-2',status:'complete',committedAt:5,orders:{},labelFiles:[],sheetIds:['rose-1'],materials:['rose']};
   sets.B.sets.set('r|dispatch',committed);
   if(refuse){await assert.rejects(sets.window.Sets.ensure('r','dispatch'),/cannot be completed/);assert.deepEqual(calls,['save set-2 complete'],'no set is numbered after a set the server has not recorded');}
   else {const next=await sets.window.Sets.ensure('r','dispatch');assert.equal(next.setId,'set-3');assert.deepEqual(calls,['save set-2 complete','number after set-2']);}
 }
 console.log('releases OK · strict partial holds, verification, manual solids, rose parity, idempotent assembly, deselection, carry-forward approvals, duplicate-cut prevention, sheet numbering, undo, run lines saved before a commit and the committed set saved before the next is numbered');
})().catch(e=>{console.error(e);process.exitCode=1});
