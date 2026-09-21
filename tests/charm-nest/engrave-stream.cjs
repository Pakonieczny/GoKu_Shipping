const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const {JSDOM}=require('jsdom');
const source=fs.readFileSync('charm-nest-bridge.js','utf8');
const dom=new JSDOM('<body><div id="engraveView"></div></body>'),document=dom.window.document;
const jobs=new Map(),readers=new Map(),fits=[],errors=[];
const rows=Array.from({length:4},(_,i)=>({key:'order'+i,state:'pooled',poolIds:['copy'+i],order:{receiptId:String(100+i)},line:{sku:'CHARM'},spec:{designSku:'CHARM',engraveCandidate:true,personalization:['Name '+i]}}));
const charm={outline:{subpaths:[]},members:[],bbox:[0,0,40,60]};
dom.window.Worker=class {};
dom.window.HTMLCanvasElement.prototype.getContext=()=>({fillRect(){}});
dom.window.CharmNestEngraveFit={createClient:()=>({run:input=>new Promise((resolve,reject)=>fits.push({input,resolve,reject}))})};
const ctx={window:dom.window,document,console:{error:(...x)=>errors.push(x)},B:{engrave:{items:jobs,fonts:{ok:true,Regular:{},workerFonts:{Regular:new ArrayBuffer(8)}}}},S:{settings:{},mode:'engrave'},PT:72/25.4,MM:25.4/72,SOURCE_LABEL:{},
  Pool:{charmOf:()=>charm,sheetOf:()=>({fileBase:'sheet'}),update:async()=>{},sizeEntry:()=>({aiPath:'fixture.ai'}),masterCharm:async()=>({charms:[charm]})},
  P:{drawCharm(){}},Master:{entryFor:()=>({}),thumbOf:()=>'/cached-thumb.png'},G:{glyphCoverage:()=>({ok:true})},Gate:{modern:()=>false},
  Orders:{rows:()=>rows,render(){},lineRecord:r=>[r.key,{}]},Review:{items:()=>[],count:()=>0,remove(){},add(){}},LiveStrip:{render(){}},RunCtl:{poke(){},save:async()=>{}},
  agentCall:(_,payload)=>new Promise(resolve=>readers.set(payload.order,resolve)),agent(){},toast(){},cors:x=>x,setTimeout,clearTimeout,
  el:(tag,cls,html='')=>{const e=document.createElement(tag);e.className=cls;e.innerHTML=html;return e;},esc:v=>String(v??'').replace(/[&<>"']/g,x=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[x]))};
vm.createContext(ctx);vm.runInContext(source.slice(source.indexOf('const Engrave = window.Engrave ='),source.indexOf('/* ═══ 22 · Sets')),ctx);
const E=dom.window.Engrave;
const tick=()=>new Promise(r=>setTimeout(r,5));
async function until(fn){for(let i=0;i<100;i++){if(fn())return;await tick();}throw Error('timed out waiting for test event');}
const answer=text=>({engrave:true,text,source:'personalization',confidence:.99,questions:[]});
const fitted=lines=>({view:{},mask:{},lines,fit:{size:6,capMm:1.6,weight:'Regular',cmds:[]},check:{ok:true}});
(async()=>{
  E.restoreView({tab:'place',chosen:true,list:true});
  let finished=false;const pass=E.classifyAll(null).then(()=>{finished=true;});
  await until(()=>readers.size===3);
  const search=document.querySelector('#egQ'),list=document.querySelector('.egPlacementList');list.scrollTop=70;search.focus();
  const row0=document.querySelector('[data-open="order0"]');await until(()=>row0.querySelector('canvas'));const thumb=row0.querySelector('canvas');
  readers.get('100')(answer('First name'));
  await until(()=>fits.length===1);
  assert.equal(finished,false,'first preview starts before the remaining word-reading batch finishes');
  assert.equal(document.querySelector('[data-open="order0"]'),row0,'classification updates its existing row');
  assert.equal(row0.querySelector('canvas'),thumb,'decoded thumbnail is retained');
  assert.equal(row0.querySelector('.w').textContent,'First name','words appear immediately');
  assert.equal(document.activeElement,search,'background changes preserve focused search');
  assert.equal(list.scrollTop,70,'background changes preserve scroll');
  fits[0].resolve(fitted(['First name']));
  await until(()=>jobs.get('order0').state==='review');
  assert.equal(row0.getAttribute('aria-busy'),'false','first result becomes interactive while other requests continue');
  assert.equal(document.querySelector('#egQ'),search);
  // Navigation is unrestricted: another item finishes while Engrave is hidden.
  document.querySelector('#engraveView').classList.add('hidden');
  readers.get('101')(answer('Second name'));
  await until(()=>fits.length===2);
  fits[1].resolve(fitted(['Second name']));await until(()=>jobs.get('order1').state==='review');
  document.querySelector('#engraveView').classList.remove('hidden');E.render();
  assert.equal(document.querySelector('[data-open="order0"] canvas'),thumb,'returning to the tab reuses the loaded thumbnail');
  assert.equal(document.querySelector('[data-open="order1"] .w').textContent,'Second name');
  // A failed job leaves an actionable row and cannot stop the rest of the pass.
  readers.get('102')(answer('Third name'));await until(()=>fits.length===3);fits[2].reject(Error('worker failed'));
  await until(()=>jobs.get('order2').state==='blocked');
  await until(()=>readers.has('103'));readers.get('103')(answer('Fourth name'));await pass;
  await until(()=>fits.length===4);
  const last=jobs.get('order3');last.state='skipped';last.row.engrave={state:'skipped'};
  fits[3].resolve(fitted(['Stale result']));
  await until(()=>document.querySelector('[data-eg-working]').hidden);
  assert.equal(last.state,'skipped','late worker result cannot overwrite a newer decision');
  assert.notEqual(last.text,'Stale result');
  assert.equal(errors.length,0);
  assert.equal(readers.size,4,'one cloud request per order');
  console.log('Streaming UI OK: per-result words/previews, stable thumbnails/search/scroll, hidden-tab progress, failed job isolation, late-result safety and cleared working count');
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>dom.window.close());
