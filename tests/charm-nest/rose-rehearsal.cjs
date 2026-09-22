const assert=require('node:assert/strict'),Rose=require('../../charm-nest-rose'),create=require('../../netlify/functions/_charmNestRoseStock'),Readiness=require('../../charm-nest-readiness');
const store=new Map(),clone=x=>structuredClone(x);
function ref(path){return {path,id:path.split('/').at(-1),collection:n=>query(path+'/'+n),get:async()=>snap(path)};}
function snap(path){return {id:path.split('/').at(-1),ref:ref(path),exists:store.has(path),data:()=>clone(store.get(path))};}
function query(path,filters=[],order=null,limit=Infinity,after=null){return {doc:id=>ref(path+'/'+id),where:(...f)=>query(path,[...filters,f],order,limit,after),orderBy:(...o)=>query(path,filters,o,limit,after),limit:n=>query(path,filters,order,n,after),startAfter:n=>query(path,filters,order,limit,n),get:async()=>{let docs=[...store.keys()].filter(k=>k.startsWith(path+'/')&&!k.slice(path.length+1).includes('/')).map(snap);docs=docs.filter(d=>filters.every(([f,op,v])=>d.data()[f]===v));if(order)docs.sort((a,b)=>(a.data()[order[0]]-b.data()[order[0]])*(order[1]==='desc'?-1:1));if(after!==null)docs=docs.filter(d=>d.data()[order[0]]<after);docs=docs.slice(0,limit);return {docs,size:docs.length};}};}
let serial=Promise.resolve();const db={collection:query,runTransaction:fn=>{const promise=serial.then(async()=>{const writes=[];let wrote=false;const result=await fn({get:async r=>{assert(!wrote,'Firestore requires all reads before writes');return r.get();},set:(r,v)=>{wrote=true;writes.push(()=>store.set(r.path,clone(v)));},update:(r,v)=>{wrote=true;writes.push(()=>store.set(r.path,{...store.get(r.path),...clone(v)}));}});writes.forEach(f=>f());return result;});serial=promise.catch(()=>{});return promise;}};
const api=create({db,col:query,FV:{serverTimestamp:()=>123456},Readiness}),Solver=require('../../charm-nest-solver');
(async()=>{
 const base={sandbox:true,demoId:'rgdemo-fixture'},request=(action,extra={})=>api.roseDemo({...base,action,...extra});
 await assert.rejects(()=>api.roseDemo({...base,sandbox:false,action:'start'}),/only in the sandbox/);
 assert.equal(store.size,0);
 const layouts=[];
 let {state}=await request('start');
 const again=await request('start');assert.deepEqual(again.state,state);
 for(let batch=1;batch<=3;batch++){
  const before=state.profile,fixture=Rose.demoBatch(batch,before),result=await Solver.solve(fixture.job,{});
  layouts.push(result.placements);
  assert.equal(result.placements.length,7,'all sample charms fit batch '+batch);assert(Solver.verify(fixture.job,result.placements,6).ok);
  const args={revision:state.revision,requestId:'request-nest-'+batch,placements:result.placements};
  await assert.rejects(()=>request('cut',{revision:state.revision,requestId:'cut-too-early'}),/Include/);
  if(batch>1)await assert.rejects(()=>request('nest',{...args,placements:result.placements.map((p,i)=>i? p:{...p,cxPt:2,cyPt:2})}),/overlaps/);
  ({state}=await request('nest',args));assert.equal(state.phase,'nested');
  assert.deepEqual((await request('nest',args)).state,state,'network retry is idempotent');
  await assert.rejects(()=>request('include',{revision:0,requestId:'request-stale'}),/changed/);
  ({state}=await request('include',{revision:state.revision,requestId:'request-include-'+batch}));assert.equal(state.phase,'included');assert(state.plan.lines.length);
  if(before)assert(state.plan.profile.values.every((v,i)=>v>=before.values[i]));
  const cutArgs={revision:state.revision,requestId:'request-cut-'+batch};
  ({state}=await request('cut',cutArgs));assert.equal(state.cuts.length,batch);assert(state.cuts.at(-1).at>0);
  assert.deepEqual((await request('cut',cutArgs)).state,state);
  assert.deepEqual((await request('get')).state,state,'reload retrieves complete shapes and history');
  console.log('Batch',batch,'saved:',state.cuts.length,'cuts;',Math.round((state.wPt*state.hPt-Rose.area(state.profile))*Rose.MM*Rose.MM),'mm² left');
 }
 assert.equal(state.phase,'complete');
 assert.equal(store.size,1);assert([...store.keys()].every(k=>k.startsWith('Sandbox_Charm_Nest_Rose_Rehearsals/')),'only isolated rehearsal records are written');
 await assert.rejects(()=>request('nest',{revision:state.revision,requestId:'request-after-end',placements:[]}),/current batch/);
 // Exercise the actual client controls against the same transactional handler.
 const {JSDOM}=require('jsdom'),fs=require('fs');
 const dom=new JSDOM('<button id="sandboxPill">Sandbox</button>',{url:'https://example.test',runScripts:'outside-only'}),w=dom.window;
 w.CharmNestRose=Rose;w.confirm=()=>true;w.HTMLCanvasElement.prototype.getContext=()=>new Proxy({},{get:(t,k)=>t[k]||(()=>{}),set:(t,k,v)=>(t[k]=v,true)});
 const calls=[];let failCut=true;
 w.CN={S:{settings:{sandbox:'on'}},allSheets:()=>[],esc:s=>String(s).replace(/</g,'&lt;'),openDlg:d=>d.setAttribute('open',''),closeDlg:d=>d.removeAttribute('open'),api:async(name,b)=>{calls.push(b);const r=await api.roseDemo(b);if(b.action==='cut'&&failCut){failCut=false;throw new Error('Simulated lost response');}return r;}};
 w.Worker=class{postMessage(m){queueMicrotask(()=>this.onmessage({data:{type:'done',result:{placements:layouts[+m.jobId.split('-').at(-1)-1]}}}));}terminate(){}};
 w.eval(fs.readFileSync('charm-nest-rose-ui.js','utf8'));
 assert(w.document.querySelector('#roseDemoLaunch'));
 await w.RoseRehearsal.open();assert.match(w.document.body.textContent,/fresh 100/);
 const settle=async()=>{for(let n=0;n<100;n++){await new Promise(r=>setTimeout(r,5));if(!w.document.querySelector('[data-demo-reload]').disabled)return;}throw new Error('UI did not settle');};
 const click=async sel=>{assert(w.document.querySelector(sel),sel);w.document.querySelector(sel).click();await settle();};
 for(let b=1;b<=3;b++){
  await click('[data-demo-nest]');assert.match(w.document.body.textContent,/Options → Include/);
  await click('[data-demo-include]');assert(w.document.querySelector('[data-demo-include]').checked);assert(w.document.querySelector('[data-demo-cut]'));
  await click('[data-demo-cut]');
  if(b===1){assert.match(w.document.body.textContent,/lost response/);assert(w.document.querySelector('[data-demo-cut]').disabled);await click('[data-demo-retry]');}
  assert.equal(w.document.querySelectorAll('.roseTimeline time').length,b);
  await click('[data-demo-reload]');assert.equal(w.document.querySelectorAll('.roseTimeline time').length,b);
 }
 assert.match(w.document.body.textContent,/Rehearsal complete · remainder saved/);
 assert(calls.every(b=>b.sandbox===true&&b.op==='roseDemo'),'no live stock APIs or production writes from demo');
 w.close();
 console.log('Rose rehearsal OK: three real solver batches, vector contours, remnant exclusions, strict sandbox guard, idempotency, stale revisions, timestamps and reload recovery');
})().catch(e=>{console.error(e);process.exit(1)});
