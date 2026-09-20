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
 const classifier=code.slice(code.indexOf('  async function classifyAll(run)'),code.indexOf('  /* ── 7.2',code.indexOf('  async function classifyAll(run)')));
 const eng=vm.createContext({assert,console});
 vm.runInContext(`
 const rows=[{state:'written',poolIds:['p1'],spec:{engraveCandidate:true},order:{receiptId:'1'},engrave:null}];
 let calls=0;const Orders={rows:()=>rows,render(){}},Gate={modern:()=>true},Pool={sheetOf:()=>({fileBase:'set-1'})};
 const window={},loadFonts=async()=>{},classify=async()=>{calls++},render=()=>{};
 `+classifier,eng);
 await vm.runInContext(`(async()=>{await classifyAll();assert.equal(calls,1,'set assembly must not skip personalization after promoting a row to written')})()`,eng);
 const resume=vm.createContext({assert,console});
 vm.runInContext(`
 const rec={runId:'r1',step:'engrave',orders:['o'],lines:{'o/a':{updateTs:10,state:'pooled',poolIds:['p1']}}};
 const B={},O={stepIndex:s=>['pull','pool','nest','checkpoint','engrave'].indexOf(s)};
 const row={key:'o/a',order:{updateTs:10},poolIds:[],state:'pulled'};let restores=0;
 const api=async()=>({run:rec}),agent=()=>{},toast=()=>{},renderBanner=()=>{},save=async()=>{},loop=async()=>{};
 const Orders={rows:()=>[row],interpretAll(){},pull:async(run,opts)=>{assert.equal(run,null);assert.deepEqual(opts.receiptIds,['o']);}};
 const restoreRunSheets=async()=>restores++,Pool={addAll:async()=>{}},allSheets=()=>[];
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
 console.log('releases OK · strict partial holds, verification, manual solids, rose parity, idempotent assembly, deselection, carry-forward approvals and duplicate-cut prevention');
})().catch(e=>{console.error(e);process.exitCode=1});
