// The real browser modules rendered in a DOM against the isolated Library API.
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {JSDOM}=require('jsdom'),{start}=require('./bridge-server.cjs'),{seed}=require('./laser-workflow.cjs');
const root=path.join(__dirname,'../..');
(async()=>{
 const srv=await start({receipts:[]});seed(srv);
 const dom=new JSDOM('<div id="stage"><div id="libView"><div id="libTab"><button data-t="current"></button><button data-t="done"></button></div><input id="libSearch"><span id="libDoneCount"></span><div id="libBody"></div><div id="libDone"></div></div></div>',{url:srv.sorterOrigin+'/#library',runScripts:'outside-only',pretendToBeVisual:true});
 const w=dom.window,d=w.document;w.matchMedia=()=>({matches:true});w.CSS={escape:x=>x};w.HTMLCanvasElement.prototype.getContext=()=>({measureText:t=>({width:t.length*8})});
 const api=async(_,b)=>{const r=await fetch(srv.sorterOrigin+'/.netlify/functions/charmNestLibrary',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(b)}),v=await r.json();if(!r.ok)throw Error(v.error);return v;};
 Object.assign(w,{S:{mode:'library',library:{rows:[],kind:'sets',metal:'all'},cloud:{ok:true}},api,allSheets:()=>[],Orders:{rows:()=>[]},Engrave:{items:()=>new Map(),backsMarkup:()=>''},esc:x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),el:(tag,cls)=>{const e=d.createElement(tag);e.className=cls;return e;},Gate:{projectLibraryRecords:x=>x},RoseStock:{},CODE:{gold:'GF',silver:'SS',gold14k:'14K'},toast:()=>null,CNEmployee:{name:()=> 'DOM tester'},dayShort:x=>x,metalOf:r=>({label:r.metal}),cors:x=>x,sheetHead:r=>`<div class="h"><span class="nm">Sheet ${r.sheetIndex}</span></div>`,openLibrarySheet:()=>{},showLibrary:()=>{},loadLibrary:()=>Promise.resolve()});
 w.eval(fs.readFileSync(path.join(root,'charm-nest-orders.js'),'utf8'));w.O=w.CharmNestOrders;
 w.eval(fs.readFileSync(path.join(root,'charm-nest-readiness.js'),'utf8'));
 const bridge=fs.readFileSync(path.join(root,'charm-nest-bridge.js'),'utf8');
 const laser=bridge.slice(bridge.indexOf('const LaserReview ='),bridge.indexOf('/* ═══ 22 · Sets — one run'));
 w.eval(laser);
 const a=bridge.indexOf('  function libraryGroups('),b=bridge.indexOf('  /** A set card whose completion',a),c=bridge.indexOf('  async function renderLibrary(body, opts)',b),e=bridge.indexOf('  return { releaseIssue',c);
 w.eval('window.Sets=(()=>{'+bridge.slice(a,b)+bridge.slice(c,e)+';return {renderLibrary,libraryCard};})();');
 w.eval(fs.readFileSync(path.join(root,'charm-nest-library.js'),'utf8'));
 const body=d.getElementById('libBody'),tick=()=>new Promise(r=>setTimeout(r,30));
 try{
  await w.Sets.renderLibrary(body);await tick();
  assert.equal(body.querySelectorAll('.libCard').length,3);
  assert.equal(body.querySelectorAll('.sheetProcessSeal.cut').length,1);assert.equal(body.querySelectorAll('.sheetProcessSeal.ready').length,1);
  assert.equal(body.querySelectorAll('[data-laser-area="pending"] .ldMark[data-done="0"]').length,0);
  assert.equal(body.querySelector('.ldPartial').textContent,'1 of 3 sheets completed');
  await assert.rejects(w.LibraryDone.mark('sheet','ready-sheet'),/Laser cutting/);
  srv.st.put('Charm_Nest_Sheets','pending-sheet',{verification:{ok:true}});
  await w.Sets.renderLibrary(body);await tick();
  assert.equal(body.querySelectorAll('[data-laser-area="ready"] .libCard').length,3);
  await w.LibraryDone.mark('sheet','ready-sheet');await tick();
  assert.equal(body.querySelectorAll('.libCard').length,3,'completed member stays in place');
  assert.equal(body.querySelectorAll('.sheetProcessSeal.cut').length,2);
  assert.equal(body.querySelector('.ldPartial').textContent,'2 of 3 sheets completed');
  await w.Sets.renderLibrary(body);await tick();assert.equal(body.querySelectorAll('.libCard').length,3,'refresh retains all members');
  w.S.library.metal='silver';await w.Sets.renderLibrary(body,{reuse:true});await tick();assert.equal(body.querySelectorAll('.libCard').length,3,'metal filter keeps the whole matching set');
  w.S.library.metal='all';await w.LibraryDone.mark('sheet','pending-sheet');await tick();assert.equal(body.querySelectorAll('.setCard').length,0,'whole set leaves together');
  await w.LibraryDone.mark('sheet','ready-sheet',false);await tick();await w.Sets.renderLibrary(body);await tick();assert.equal(body.querySelectorAll('.libCard').length,3,'undo restores all members');
  console.log('PASS: actual set cards, laser-only buttons, live seals, refresh, metal filter, whole-set removal and undo');
 }finally{w.close();srv.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
