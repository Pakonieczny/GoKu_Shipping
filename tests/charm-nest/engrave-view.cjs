const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const {JSDOM} = require('jsdom');
const source = fs.readFileSync('charm-nest-bridge.js', 'utf8');
const start = source.indexOf('const Engrave = window.Engrave =');
const end = source.indexOf('/* ═══ 22 · Sets', start);
const dom = new JSDOM('<body><div id="engraveView"></div></body>');
const document = dom.window.document, jobs = new Map(), frames = [], timers = [], errors = [], observers = [];
dom.window.ResizeObserver = class { constructor(){observers.push(this);} observe(){this.active=true;} disconnect(){this.active=false;} };
dom.window.HTMLCanvasElement.prototype.getContext = () => ({fillRect(){}});
let charm = null;
const c = vm.createContext({window:dom.window, document, console:{error:(...x)=>errors.push(x)}, ResizeObserver:dom.window.ResizeObserver, B:{engrave:{items:jobs,fonts:{ok:true}}},
  S:{settings:{},mode:'engrave'}, PT:72/25.4, MM:25.4/72, SOURCE_LABEL:{},
  Pool:{charmOf:()=>charm,sheetOf:()=>null}, P:{drawSegments(){throw Error('invalid saved path');}}, Review:{items:()=>[],count:()=>0,remove(){}}, Master:{entryFor:()=>null},
  LiveStrip:{render(){}}, RunCtl:{poke(){}}, Orders:{render(){}},
  setTimeout:fn=>(timers.push(fn),timers.length),clearTimeout(){},
  requestAnimationFrame:fn=>(frames.push(fn),frames.length),cancelAnimationFrame(){},
  el:(tag,cls,html='')=>{const e=document.createElement(tag);e.className=cls;e.innerHTML=html;return e;},
  esc:v=>String(v ?? '').replace(/[&<>"']/g,x=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[x])),
  toast(){}, agent(){}, getComputedStyle:()=>({flexGrow:'0'})});
vm.runInContext(source.slice(start,end),c);
const E=dom.window.Engrave;
const row={state:'pooled',poolIds:['copy'],order:{receiptId:'test-order'},line:{sku:'TEST'},spec:{designSku:'TEST'}};
const job={key:'test',row,copies:['copy'],state:'blocked',lines:['Test'],questions:[],reason:'Waiting for geometry',view:{}};
jobs.set(job.key,job);
assert.doesNotThrow(()=>E.render(),'legacy back views without detail must render');
assert(document.querySelector('[data-f="words"]'));
charm={outline:null,bbox:[0,0,20,30],members:[]};job.view=null;
assert.doesNotThrow(()=>E.render(),'a temporarily missing outline must not take down the tab');
assert(document.querySelector('[data-f="words"]'));
assert.equal(job.state,'blocked','rendering does not discard or approve a job');
assert.equal(jobs.get('test'),job,'saved job identity remains intact');
assert.equal(observers.filter(o=>o.active).length,1,'replaced editor observers are disconnected');
charm={outline:{subpaths:[]},bbox:[0,0,20,30],members:[]};
assert.doesNotThrow(()=>E.render(),'an unexpected drawing error remains inside the Engrave view');
assert(document.querySelector('[role="alert"]'));
assert.equal(jobs.get('test'),job,'preview failure never clears saved work');
assert.equal(observers.filter(o=>o.active).length,0);
charm=null;document.querySelector('[data-eg-retry]').click();
assert(document.querySelector('[data-f="words"]'),'retry restores the editor');
// Repainting a words job only queues preparation, so it cannot recursively fit
// an entire recovered set before the first frame is visible.
job.state='words';E.render();const queued=timers.length;E.render();
assert.equal(timers.length,queued+1,'only the card focus timer is added on the next render');
assert.equal(job.state,'words');
assert.equal(errors.length,1);
job.state='review';job.view={};job.fit={size:4,capMm:1,angle:0,fittedMax:6};
E.render();while(frames.length)frames.shift()();
assert(document.querySelector('.backHost').textContent.includes('could not be drawn'));
assert(document.querySelector('[data-a="approve"]').disabled,'a failed canvas cannot be approved with the button');
assert.equal(job.state,'review','draw failure does not revoke existing work');
job.state='written';job.fit=null;job.backs=[];
E.restoreView({tab:'done',chosen:true});E.render();
document.querySelector('.doneRow').click();
assert(document.querySelector('.doneDetail'),'legacy decided records expand without detail metadata');
console.log('Engrave view OK: legacy metadata, missing outlines, local error/retry, observer cleanup, deferred preparation');
assert(document.querySelector('.decidedRow .placementThumb'),'Decided shares the Placements thumbnail size');
assert(document.querySelector('.decisionActions .decisionStatus'));
assert.equal(document.querySelectorAll('.decidedRow [data-a="reopen"]').length,1,'one clear action even with details expanded');
(async()=>{
  job.state='review';job.view=null;
  const waiting={key:'waiting',row:{...row,engrave:{}},copies:['missing'],lines:['Pending'],state:'ready'};
  jobs.set(waiting.key,waiting);
  E.restoreView({tab:'place',focus:job.key,chosen:true});E.render();
  const card=document.querySelector('.rvItem');
  assert(document.querySelector('[data-eg-working]').hidden,'persisted ready state is not active work');
  let attempts=0;c.G={glyphCoverage(){attempts++;throw Error('fit failed');}};
  const task=E.fitJob(waiting);E.render();
  assert(!document.querySelector('[data-eg-working]').hidden);
  assert.equal(document.querySelector('[data-eg-working] b').textContent,'1');
  await assert.rejects(task,/fit failed/);
  assert(document.querySelector('[data-eg-working]').hidden,'failure clears spinner while another card stays open');
  assert.equal(document.querySelector('.rvItem'),card,'updating the spinner preserves the active editor');
  assert.equal(await E.fitAll(null),0,'missing sheet/outline is not retried in a busy loop');
  assert.equal(attempts,1);
  let finish;c.agentCall=()=>new Promise(resolve=>finish=resolve);
  const classified={...row,key:'classify',spec:{designSku:'TEST',engraveCandidate:true,personalization:[]}};
  const reading=E.classify(classified);
  assert(!document.querySelector('[data-eg-working]').hidden,'actual classifier request is counted');
  await Promise.resolve();finish({engrave:false,text:'',confidence:1,questions:[]});await reading;
  assert(document.querySelector('[data-eg-working]').hidden,'classifier completion clears spinner');
  console.log('Working indicator OK: active tasks only, completion/failure refresh in place, missing geometry waits without spinning');
})().catch(e=>{console.error(e);process.exitCode=1;}).finally(()=>dom.window.close());

// Boot must paint a direct Engrave URL before waiting for recovery, and must
// not save over the checkpoint or start Auto if that recovery fails.
const calls=[];
const boot=vm.createContext({window:{addEventListener(){}},document:{getElementById:()=>({}),addEventListener(){}},console:{error(){}},
  B:{run:null},S:{mode:'engrave',settings:{runMode:'auto'},cloud:{ok:true}},
  SetPicker:{mount(){}},RunCtl:{renderModeBtn(){},renderBanner(){}},LiveStrip:{render(){}},Sandbox:{render(){},afterReload(){},on:()=>false},
  Orders:{loadMaps:async()=>{}},Master:{load:async()=>{}},Engrave:{loadFonts:async()=>{}},Kin:{mount(){}},
  Views:{onShow:m=>calls.push('view:'+m)},Session:{restore:async()=>{calls.push('restore');throw Error('bad checkpoint');},listen:()=>calls.push('listen')},
  Arrivals:{start:()=>calls.push('arrivals')},DesignLink:{origin:()=>''},agent(){},
  api:()=>{throw Error('must not recall another run after failed recovery');},setTimeout:()=>{throw Error('must not start Auto after failed recovery');}});
vm.runInContext(source.slice(source.indexOf('async function bootBridge()'),source.indexOf('(function whenReady()')),boot);
boot.bootBridge().then(()=>{assert.deepEqual(calls,['view:engrave','restore','view:engrave']);console.log('Startup OK: immediate view and preserved checkpoint after recovery failure');}).catch(e=>{console.error(e);process.exitCode=1;});
