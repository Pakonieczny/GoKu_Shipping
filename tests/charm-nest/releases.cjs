const assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm');
const O = require('../../charm-nest-orders.js');
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
const Sets={ofRun:()=>[...B.sets.values()],save:async()=>{},ensure:async(runId,group,o)=>{if(o.roseOnly&&seq%2) return null;allocations++;const s={runId,group,seq:seq++,day:today(),setId:'set-'+allocations,sheetIds:[],labelFiles:[],materials:[],orders:{}};B.sets.set(s.setId,s);return s}};
const page=(metal,full=false)=>({metal,runId:B.run.runId,sheetId:metal+'-id',draft:true,outputs:{},persistedDone:true,placements:[{}],verification:{ok:true},releaseFull:full,charms:[],status:'complete'});
`,c);
vm.runInContext(part('Gate','/* ═══ 21'),c);
(async()=>{
 await vm.runInContext(`(async()=>{
 pages.push(page('gold'),page('silver'),page('rose'),page('gold10k'),page('gold14k'));
 await Gate.assemble(B.run);assert.equal(allocations,0,'partials/unselected solids/odd rose must not consume a set number');
 pages[0].releaseFull=true;await Gate.assemble(B.run);
 assert.equal(allocations,1);assert.equal(pages[0].setId,'set-1');assert.equal(pages[1].setId,undefined);assert.equal(pages[2].setId,undefined);
 B.run.solidIncluded.gold10k=true;await Gate.assemble(B.run);assert.equal(pages[3].setId,'set-1');assert.equal(pages[4].setId,undefined);
 const n=writes.length;await Gate.assemble(B.run);assert.equal(writes.length,n,'repeat checkpoint cannot duplicate outputs');
 B.run.solidIncluded.gold10k=false;await Gate.assemble(B.run);assert.equal(pages[3].setId,null);assert.equal(pages[3].draft,true);assert(![...B.sets.values()][0].sheetIds.includes('gold10k-id'));
 B.sets.clear();pages.splice(0,pages.length,page('rose'));await Gate.assemble(B.run);assert.equal(pages[0].seq,2,'even set accepts accumulated rose');
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
