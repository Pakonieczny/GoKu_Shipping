// Real Gate + Sets orchestration with only storage, canvas rendering and the run
// controller substituted. No nesting or production-order mutations are needed.
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const O=require('../../charm-nest-orders.js');
const source=fs.readFileSync('charm-nest-bridge.js','utf8');
const section=(name,end)=>source.slice(source.indexOf(`const ${name} = window.${name} =`),source.indexOf(end,source.indexOf(`const ${name} = window.${name} =`)));
function harness(){
 let sets=section('Sets','/* ═══ 23');
 {const a=sets.indexOf('  async function renderLabelPng('),b=sets.indexOf('  /** After a sheet',a);sets=sets.slice(0,a)+`  async function renderLabelPng(payload,label){return {blob:new Uint8Array([1]),dataUrl:'data:image/png;base64,AA==',ecc:'M'};}\n`+sets.slice(b);}
 return `
const windowRef=window, O=CharmNestOrders;
const S={cloud:{ok:true},mode:'library',settings:{stock:{gold10k:[1,1],gold14k:[1,1]}},library:{kind:'sets'}};
const B={run:{runId:'test-run',releasePolicy:2,solidIncluded:{},status:'stopped',sheets:{}},sets:new Map(),pool:{rows:new Map()}};
const uploads=[],savedSheets=new Map(),savedSets=new Map();let failUpload=false,holdUpload=null,refreshes=0;
const METAL_TAG={gold10k:'10K',gold14k:'14K'},METALS=Object.keys(METAL_TAG).map(key=>({key}));
const labelOf=m=>m==='gold10k'?'10K Gold':'14K Gold',today=()=> '2026-09-19',agent=()=>{},toast=()=>{},employeeName=()=> 'Test';
const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const stockFor=()=>({wIn:1,hIn:1}),Session={schedule(){}},pagesOf=m=>pages.filter(p=>p.metal===m);
const pages=METALS.map(({key},i)=>({metal:key,runId:B.run.runId,sheetId:key,draft:true,outputs:{},persistedDone:true,placements:[{id:key}],verification:{ok:true},status:'complete',rejects:[],page:1,
 charms:[{id:key,poolId:'4000000001_500000000'+i+'_1',order:'400000000'+i,orderInfo:{transactionId:'500000000'+i,copy:1}}]}));
const allSheets=()=>pages,Orders={rows:()=>[],render(){}},Engrave={items:()=>new Map(),backsMarkup:()=>''},Pool={update:async()=>{}};
const RunCtl={onSheetDone(){},save:async()=>{},renderBanner(){},stop(){B.run.status='stopped'}};
const api=async(name,b)=>{
 if(b.op==='setAllocate')return {setId:'test-set',seq:1,day:today()};
 if(b.op==='putSheet')savedSheets.set(b.sheet.id,{...savedSheets.get(b.sheet.id),...structuredClone(b.sheet)});
 if(b.op==='setUpdate')savedSets.set(b.setId,{setId:b.setId,...structuredClone(b.patch)});
 if(b.op==='setList')return {sets:[...savedSets.values()]};
 if(b.op==='listSheets')return {sheets:[...savedSheets.values()]};
 return {};
};
const uploadBytes=async(path,blob)=>{if(holdUpload)await holdUpload;if(failUpload){failUpload=false;throw Error('label upload interrupted')}uploads.push(path);return {path,url:typeof blob?.arrayBuffer==='function'?await new Promise(r=>{const f=new FileReader();f.onload=()=>r(f.result);f.readAsDataURL(blob)}):'https://example.com/'+path}};
const CN={sheetFileBase:p=>METAL_TAG[p.metal]+'_Set-1_Sheet-'+p.sheetIndex,
 persistSheet:async p=>{p.folderPath='charmnest/sets/test/'+p.fileBase;await api('',{op:'putSheet',sheet:{id:p.sheetId,metal:p.metal,runId:p.runId,day:today(),setId:p.setId,setSeq:p.seq,sheetIndex:p.sheetIndex,draft:p.draft,solidIncluded:Gate.solidSelected(p.metal),fileBase:p.fileBase,orders:p.charms.map(c=>c.order),placedCount:1,charmCount:1,status:'complete'}});await CN.loadLibrary();await Sets.onSheetSaved(p,p.charms);},
 loadLibrary:async()=>{refreshes++;}};
const refreshAllCards=()=>{};
${section('Gate','/* ═══ 21')}
${sets}

`;}
if(require.main===module){
 const c=vm.createContext({assert,CharmNestOrders:O,window:{CharmNestOrders:O},structuredClone,Uint8Array,console});vm.runInContext(harness(),c);
 vm.runInContext(`(async()=>{
 await Gate.assemble(B.run);assert.equal(uploads.length,0,'unchecked solids never get a release QR');
 for(const material of ['gold10k','gold14k']){
  B.run.solidIncluded[material]=true;await Gate.assemble(B.run);
  const p=pages.find(p=>p.metal===material),set=Sets.ofRun(B.run.runId)[0];
  assert(Sets.labelsReady(p,set));assert.equal(p.label.files[0].payload,O.encodeOrderList([p.charms[0].order],O.CARD_TO_METAL[material]));
  assert(savedSheets.get(p.sheetId).label.files[0].url);assert(savedSets.get(set.setId).labelFiles.some(f=>f.sheetId===p.sheetId));
 }
 const count=uploads.length;await Gate.assemble(B.run);assert.equal(uploads.length,count,'unchanged sheet cannot regenerate its label');
 const set=Sets.ofRun(B.run.runId)[0],p=pages[0];p.label=null;
 failUpload=true;await assert.rejects(Gate.assemble(B.run),/interrupted/);assert.equal(p.setId,set.setId,'failed QR retry keeps sheet membership');
 await Gate.assemble(B.run);assert(Sets.labelsReady(p,set),'existing sheet with missing QR is repaired');
 set.labels={pdf:'old'};B.run.solidIncluded.gold10k=false;await Gate.assemble(B.run);
 assert(!set.labelFiles.some(f=>f.sheetId===p.sheetId));assert.equal(savedSheets.get(p.sheetId).solidIncluded,false);assert.equal(set.labels,null,'old collected labels invalidated');
 B.run.solidIncluded.gold10k=true;await Promise.all([Gate.assemble(B.run),Gate.assemble(B.run)]);
 assert(Sets.labelsReady(p,set));assert.equal(set.labelFiles.filter(f=>f.sheetId===p.sheetId).length,1,'reselect creates one label');
 assert(refreshes>uploads.length,'Library refresh also occurs after labels finish');
})()`,c).then(()=>console.log('Set labels OK: both solid metals, exact QR payload, saved indexes, retries, deselection, reselect, serialized assembly and Library refresh')).catch(e=>{console.error(e);process.exitCode=1});
}
