/* Exercise real async lifecycle functions with controlled fonts, workers and
 * reads. No production orders, credentials, or browser automation are used. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const {JSDOM} = require('jsdom');
const engravingSeals = require('../../charm-nest-engraving-seals.js');
const source = fs.readFileSync('charm-nest-bridge.js', 'utf8');
const section = (from, to) => {
  const a = source.indexOf(from), b = source.indexOf(to, a + from.length);
  assert(a >= 0 && b > a, 'actual function boundaries exist: ' + from);
  return source.slice(a, b);
};
const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return {promise, resolve, reject}; };
const turn = () => new Promise(setImmediate);
const within = async (promise, ms = 300) => {
  let timer;
  try { return await Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(Error('view waited for unrelated metadata')), ms); })]); }
  finally { clearTimeout(timer); }
};
const result = {view:{}, mask:{}, fit:{size:2, capMm:1, weight:'Regular'}, lines:['A'], check:{ok:true}};
function job(state = 'words') {
  return {key:'order-line', state, lines:['A'], text:'A', copies:['copy'], fit:{size:1}, row:{key:'order-line',state:'written',engrave:{needed:true,state,approved:false},spec:{designSku:'FROG'},order:{receiptId:'4000001'}}};
}
function engravingFixture() {
  let owner = new Map(), fonts = deferred(), worker = deferred(), calls = 0;
  const context = vm.createContext({lineOf:r=>r,timelineApproved(){},items:()=>owner, loadFonts:()=>fonts.promise, F_:{ok:true,Regular:{}}, G:{glyphCoverage:()=>({ok:true})}, Review:{add(){},remove(){}}, Pool:{update:async()=>{}}, RunCtl:{poke(){}}, EG:{cardKey:null}, render(){}, agent(){}, Master:{entryFor:()=>({})}, charmFor:()=>charm, fitClient:()=>({run:()=>{calls++;return worker.promise;}}), fitInput:(j,c)=>({charm:c,lines:j.lines.slice()}), fitStamp:x=>JSON.stringify(x.lines)});
  const charm = {outline:{},members:[]};
  vm.runInContext(section('  async function setReady(', '  /** A person\'s decision'), context);
  vm.runInContext(section('  const fitTasks =', '  async function claudeRead('), context);
  return {context, get owner(){return owner;}, replace(){owner = new Map();}, get fonts(){return fonts;}, get worker(){return worker;}, get calls(){return calls;}};
}
async function testEngravingCallbacks() {
  {
    const f = engravingFixture(), j = job(); f.owner.set(j.key,j);
    const pending = f.context.setReady(j); j.state = 'approved'; j.row.engrave = {needed:true,state:'approved',approved:true,approvedBy:'Paul',approvedAt:1000};
    f.fonts.resolve(); await pending;
    assert.equal(j.state,'approved','late font arrival cannot undo a newer approval'); assert.equal(j.row.engrave.approved,true);
  }
  {
    const f = engravingFixture(), j = job(); f.owner.set(j.key,j);
    const pending = f.context.setReady(j); f.replace(); const newer = job('approved'); newer.row.engrave.approved = true; f.owner.set(j.key,newer);
    f.fonts.resolve(); await pending;
    assert.equal(j.state,'words','a replaced workspace receives no old ready callback'); assert.equal(newer.state,'approved');
  }
  {
    const f = engravingFixture(), j = job(); f.owner.set(j.key,j);
    const pending = f.context.setReady(j); j.row.state = 'gone'; f.fonts.resolve(); await pending;
    assert.equal(j.state,'words','removed order receives no late preview transition');
  }
  {
    const f = engravingFixture(), j = job(); f.owner.set(j.key,j); f.fonts.resolve(); await f.context.setReady(j);
    assert.equal(j.state,'ready','active words still prepare normally');
    j.state = 'words'; j.row.engrave.approved = false; await f.context.setReady(j);
    assert.equal(j.state,'ready','explicit reopening can prepare the same order again');
  }
  {
    const f = engravingFixture(), j = job('ready'), oldFit = j.fit; f.owner.set(j.key,j);
    const pending = f.context.fitJob(j); await turn(); f.replace(); f.fonts.resolve(); await pending;
    assert.equal(f.calls,0,'closed/replaced edit starts no stale worker'); assert.equal(j.fit,oldFit,'pending font lookup leaves the saved fit intact');
  }
  {
    const f = engravingFixture(), j = job('approved'), oldFit = j.fit; j.row.engrave.approved = true; f.owner.set(j.key,j); f.fonts.resolve();
    await f.context.fitJob(j); assert.equal(f.calls,0); assert.equal(j.fit,oldFit); assert.equal(j.state,'approved','decided job cannot be reset by a queued fit');
  }
  {
    const f = engravingFixture(), j = job('ready'); f.owner.set(j.key,j); f.fonts.resolve();
    const a = f.context.fitJob(j), b = f.context.fitJob(j); assert.equal(a,b,'UI and background fitting share one actual task'); await turn(); assert.equal(f.calls,1);
    j.state = 'skipped'; j.row.engrave = {needed:false,state:'skipped',approved:true}; f.worker.resolve(result); await a;
    assert.equal(j.state,'skipped','late worker result cannot requeue a cut-plain decision'); assert.equal(j.row.engrave.approved,true);
  }
  {
    const f = engravingFixture(), j = job('ready'); f.owner.set(j.key,j); f.fonts.resolve();
    const pending = f.context.fitJob(j); await turn(); j.row.state = 'gone'; f.worker.reject(Error('worker failed')); await pending;
    assert.equal(j.state,'fitting','late failed worker cannot turn a removed order into blocked work');
  }
  {
    const f = engravingFixture(), j = job('ready'); f.owner.set(j.key,j); f.fonts.resolve();
    const pending = f.context.fitJob(j); await turn(); f.worker.resolve(result); await pending;
    assert.equal(j.state,'review'); assert.equal(j.fit.size,2,'an active preview still completes');
  }
  {
    const f = engravingFixture(), j = job('review'), oldFit = j.fit; f.owner.set(j.key,j);
    const pending = f.context.fitJob(j); await turn(); j.approvalPreparing = true; j.stamping = true; f.fonts.resolve(); await pending;
    assert.equal(f.calls,0); assert.equal(j.fit,oldFit,'a fit requested before approval cannot replace verified geometry during stamping');
  }
  {
    const f = engravingFixture(), j = job('review'); j.approvalPreparing = true; f.owner.set(j.key,j); f.fonts.resolve();
    const pending = f.context.fitJob(j); await turn(); f.worker.resolve(result); await pending;
    assert.equal(j.state,'review'); assert.equal(j.fit.size,2,'approval can deliberately fit unapplied words before verifying them');
  }
}
async function testRecoveryFailures() {
  for (const changed of [true,false]) {
    const j = job(), blocked = [], gate = deferred(), started = deferred(); let owner = new Map([[j.key,j]]);
    const context = vm.createContext({lineOf:r=>r,items:()=>owner,classifyTasks:new WeakMap(),fitTasks:new WeakMap(),charmFor:()=>({outline:{}}),sheetFor:()=>({fileBase:'saved'}),setTimeout,RunCtl:{poke(){}},render(){},setReady:()=>{started.resolve();return gate.promise;},fitJob:async()=>{},Review:{add:x=>blocked.push(x)}});
    vm.runInContext(section('  let previewRecovery =', '  let classifyPass ='),context);
    const pending = context.prepareWaitingPreviews(); await started.promise;
    if (changed) owner = new Map([[j.key,job('approved')]]);
    gate.reject(Error('font setup failed')); await pending;
    assert.equal(j.state,changed ? 'words' : 'blocked',changed ? 'failed preview recovery cannot block an old workspace' : 'a current recovery error remains actionable');
  }
  for (const changed of [true,false]) {
    const j = job('ready'), gate = deferred(), started = deferred(); let owner = new Map([[j.key,j]]), feedback = 0;
    const context = vm.createContext({lineOf:r=>r,items:()=>owner,canFit:()=>true,isWorking:()=>false,Pool:{sheetOf:()=>({fileBase:'saved'})},fitJob:()=>{started.resolve();return gate.promise;},Review:{add(){feedback++;}},agent(){},render(){}});
    vm.runInContext(section('  async function fitAll(', '  /* Reading the words'),context);
    const pending = context.fitAll(); await started.promise;
    if (changed) { j.state = 'approved'; j.row.engrave = {state:'approved',approved:true}; }
    gate.reject(Error('fit setup failed')); await pending;
    assert.equal(j.state,changed ? 'approved' : 'blocked',changed ? 'failed fitting cannot undo a newer approval' : 'a current fitting error still reaches Review');
    assert.equal(feedback,changed ? 0 : 1);
  }
}
function approvalFixture(f) {
  const verified = []; let stamped = 0, saved = 0;
  Object.assign(f.context, {Date,console,clearTimeout,employeeName:()=> 'Paul',askEmployee:()=> 'Paul',CNListActivity:{touch(){}},CNEngravingSeals:{...engravingSeals,press:async()=>{stamped++;}},Session:{schedule(){},flushNow:async()=>true},Review:{add(){},remove(){}},EG_TAB:()=>'',goes(){},toast(){},prepareApproval:async j=>{verified.push({fit:j.fit,view:j.view,text:j.text});return {fit:j.fit,view:j.view};},saveBacks:async j=>{saved++;j.state='written';j.row.engrave.state='written';}});
  vm.runInContext(section('  // A verified decision is checkpointed', '  /** An approval\'s back files'),f.context);
  return {verified,get stamped(){return stamped;},get saved(){return saved;}};
}
async function testApprovalDuringRefit() {
  {
    const f = engravingFixture(), a = approvalFixture(f), j = job('review'); j.fit = {size:1,capMm:1}; j.view = {}; j.verify = {geometry:{ok:true}}; j.lines = ['LATEST']; j.text = 'LATEST'; f.owner.set(j.key,j);
    const fitting = f.context.fitJob(j); await turn();
    const approving = f.context.approve(j,'Paul'); assert(j._approvalTask,'waiting on refit retains the duplicate approval task lock'); assert(!j.approvalPreparing,'existing refit can finish before preparation starts');
    await f.context.approve(j,'Paul'); assert.equal(a.verified.length,0,'double A while fonts load cannot verify old geometry'); assert.equal(a.stamped,0);
    f.fonts.resolve(); await turn(); assert.equal(f.calls,1); assert.equal(j.state,'fitting'); assert(!j.approvalPreparing);
    f.worker.resolve({...result,lines:['LATEST']}); await fitting; await approving;
    assert.equal(a.verified.length,1); assert.equal(a.verified[0].fit,result.fit,'export verification uses the completed current fit'); assert.equal(a.verified[0].view,result.view); assert.equal(a.verified[0].text,'LATEST');
    assert.equal(a.stamped,1); assert.equal(a.saved,1); assert.equal(j.state,'written'); assert.equal(j.approvedBy,'Paul'); assert(!j._approvalTask);
  }
  for (const replaceRow of [false,true]) {
    const f = engravingFixture(), a = approvalFixture(f), j = job('review'); j.fit = {size:1,capMm:1}; j.view = {}; j.verify = {geometry:{ok:true}}; f.owner.set(j.key,j);
    const fitting = f.context.fitJob(j); await turn(); const approving = f.context.approve(j,'Paul');
    if (replaceRow) j.row = {...j.row,engrave:{state:'written',approved:true}};
    else { f.replace(); f.owner.set(j.key,job('written')); }
    f.fonts.resolve(); await fitting; await approving;
    assert.equal(a.verified.length,0,replaceRow ? 'row replacement cancels the stale approval after fitting wait' : 'closing or replacing the workspace cancels its waiting approval'); assert.equal(a.stamped,0); assert.equal(a.saved,0); assert(!j._approvalTask);
  }
  {
    const f = engravingFixture(), a = approvalFixture(f), j = job('review'); j.view = {}; j.verify = {geometry:{ok:true}}; f.owner.set(j.key,j);
    const fitting = f.context.fitJob(j); fitting.catch(()=>{}); await turn(); const approving = f.context.approve(j,'Paul');
    f.fonts.reject(Error('font lookup failed')); await assert.rejects(fitting,/font lookup failed/); await approving;
    assert.equal(a.verified.length,0); assert.equal(a.stamped,0,'failed pending fitting is reviewable without a false stamp'); assert.equal(j.state,'review'); assert(!j.approvalPreparing); assert(!j._approvalTask);
  }
}
function cardFixture() {
  const dom = new JSDOM('<div id="egQueue"><div class="rvItem"><button data-a="close">Close</button><button data-a="skip">No engraving</button></div></div>', {pretendToBeVisual:true});
  const card = dom.window.document.querySelector('.rvItem'), j = job('review'); j.editingBack = true;
  const jobs = new Map([[j.key,j]]), task = deferred(); j.approvalPreparing = true; j._approvalTask = task.promise;
  task.promise.then(()=>{if(j._approvalTask===task.promise)delete j._approvalTask;if(j._backTask===task.promise)delete j._backTask;});
  let skipped = 0;
  const EG = {card,cardKey:j.key,list:false};
  const context = vm.createContext({window:dom.window,document:dom.window.document,card,job:j,EG,items:()=>jobs,setTimeout,Promise,Review:{remove(){}},render(){},goes(){},approve(){},centreText(){},rotateTo(){},queuedJobs:x=>x,matchesQ:()=>true,resplit(){},skip(){skipped++;},sendBack(){},nudge(){}});
  dom.window.Seal = {whenIdle:async()=>{}};
  vm.runInContext(section('    const approvalBusy =', '    const lineControl='),context);
  vm.runInContext(section('    card.addEventListener("keydown", e => { if (e.target.tagName', '    // the next card takes focus'),context);
  return {dom,card,j,jobs,task,EG,get skipped(){return skipped;}};
}
async function testCardClosing() {
  {
    const f = cardFixture(); f.card.querySelector('[data-a=close]').click(); f.card.querySelector('[data-a=skip]').click();
    assert(f.jobs.has(f.j.key),'close retains the editing job during export verification'); assert.equal(f.skipped,0,'a competing skip cannot replace in-flight approval');
    f.j.approvalPreparing = false; f.task.resolve(); await turn();
    assert(!f.jobs.has(f.j.key),'a failed/cancelled approval may be deliberately closed after it settles'); f.dom.window.close();
  }
  {
    const f = cardFixture(); f.card.dispatchEvent(new f.dom.window.KeyboardEvent('keydown',{key:'Escape',bubbles:true,cancelable:true}));
    assert(f.jobs.has(f.j.key),'Escape follows the same verified-operation wait');
    // A successful approval already moved to the next card before the queued close
    // wakes: its callback cannot clear that card or remove the decided job.
    f.j.state = 'written'; const next = f.dom.window.document.createElement('div'); f.card.replaceWith(next); f.EG.card = next; f.EG.cardKey = 'next';
    f.j.approvalPreparing = false; f.task.resolve(); await turn();
    assert(f.jobs.has(f.j.key),'late Escape cannot delete a completed approval'); assert.equal(f.EG.card,next,'late close cannot replace the next order'); f.dom.window.close();
  }
  {
    const f = cardFixture(); f.j.approvalPreparing = false; delete f.j._approvalTask; f.j.backSaving = true; f.j._backTask = f.task.promise;
    f.card.querySelector('[data-a=close]').click(); assert(f.jobs.has(f.j.key),'back-saving task also retains its edit');
    f.j.backSaving = false; f.task.resolve(); await turn(); assert(!f.jobs.has(f.j.key)); f.dom.window.close();
  }
  {
    const f = cardFixture(); f.card.querySelector('[data-a=close]').click();
    // The operator moved to another main tab, so render did not detach this card.
    // Closing after a successful save must still retain the Decided job.
    f.j.state = 'written'; f.j.approvalPreparing = false; f.task.resolve(); await turn();
    assert(f.jobs.has(f.j.key),'close after a completed edit keeps its Decided record even if the old card stayed connected'); assert(f.EG.list); f.dom.window.close();
  }
}
function sheetFixture({api, pages = {}, shortenedTimers = false, draw} = {}) {
  const dom = new JSDOM('<div id="owSheetPanel"></div><div id="owPlateWrap"><canvas id="owSheetCv"></canvas><span id="owPlateTip" hidden></span><div id="owPlateWait"><b></b></div></div><div id="owPlateFoot"></div><b id="owShCount"></b>',{pretendToBeVisual:true});
  const W = {key:'A',rid:'4000001',dlg:{open:true},closing:false,faceShown:'front',view:'sheet'}, rows = new Map([
    ['A',{key:'A',order:{receiptId:'4000001'},poolIds:['a']}],['B',{key:'B',order:{receiptId:'4000002'},poolIds:['b']}]
  ]);
  let drawings = 0, panelPaints = 0;
  const context = vm.createContext({window:dom.window,document:dom.window.document,W,byId:id=>dom.window.document.getElementById(id),rowOf:key=>rows.get(key),linesOf:r=>[r],Pool:{sheetOf:id=>pages[id]},B:{pool:{rows:new Map()}},api:api || (async()=>({pools:[]})),setTimeout:(fn,ms)=>setTimeout(fn,shortenedTimers && ms === 12000 ? 5 : ms),clearTimeout,Promise,console:{warn(){}},tryDo:f=>f(),paintPanel(){panelPaints++;},paintNow(){},paintFoot(){},unpick(){},landed:async()=>{},still:()=>true,sheetName:s=>'Sheet '+(s.n || 1),esc:s=>String(s || '').replaceAll('<','&lt;'),toast(){},noSheetHtml:()=> 'No sheet'});
  context.SheetWin = dom.window.SheetWin = {async drawOrder(cv,target,rid,opts){drawings++;return draw ? draw(cv,target,rid,opts) : {rec:{id:target},pieces:[],mine:[],focus(){},redraw(){}};}};
  dom.window.Pool = context.Pool;
  vm.runInContext(section('  const SV =', '  /** An order on no sheet:'),context); vm.runInContext('globalThis.sheetState = SV',context);
  return {dom,W,rows,context,SV:context.sheetState,get drawings(){return drawings;},get panelPaints(){return panelPaints;}};
}
async function testSheetViews() {
  {
    const read = deferred(), f = sheetFixture({api:()=>read.promise,pages:{a:{sheetId:'live-a',metal:'gold',page:1}}});
    await within(f.context.sheetShow()); assert.equal(f.drawings,1); assert.equal(f.SV.list[0].id,'live-a','known local sheet draws without waiting for pool metadata');
    read.resolve({pools:[]}); await turn(); assert.equal(f.SV.finding,null); f.dom.window.close();
  }
  {
    const A = deferred(), B = deferred(), f = sheetFixture({api:(fn,body)=>body.orderId==='4000001' ? A.promise : B.promise,pages:{a:{sheetId:'live-a',metal:'gold'},b:{sheetId:'live-b',metal:'silver'}}});
    await f.context.sheetShow(); f.W.key = 'B'; f.W.rid = '4000002'; await f.context.sheetShow();
    B.resolve({pools:[{poolId:'b',sheetId:'live-b',material:'silver'}]}); await turn();
    A.resolve({pools:[{poolId:'a',sheetId:'old-a',material:'gold'}]}); await turn();
    assert.equal(f.SV.rid,'4000002'); assert.deepEqual(Array.from(f.SV.pools,p=>p.poolId),['b'],'old discovery cannot overwrite the new order piece list');
    assert.equal(f.SV.list[0].id,'live-b'); f.dom.window.close();
  }
  {
    let failed = true;
    const f = sheetFixture({api:async(fn,body)=>{if(failed)throw Error('offline');return body.op==='poolList' ? {pools:[{poolId:'a',sheetId:'saved-a',material:'gold'}]} : {sheets:[]};}});
    await f.context.sheetShow(); assert.equal(f.SV.finding,null,'failed discovery releases its promise'); assert.equal(f.SV.list,null); assert(f.dom.window.document.querySelector('.owPlateNone button'),'failed record read is retryable');
    failed = false; await f.context.sheetShow(); assert.equal(f.SV.list[0].id,'saved-a','second visit retries the read rather than await a poisoned promise'); assert.equal(f.drawings,1); f.dom.window.close();
  }
  {
    const f = sheetFixture({api:()=>new Promise(()=>{}),shortenedTimers:true});
    await within(f.context.sheetShow()); assert.equal(f.SV.finding,null,'timeout works even without browser AbortSignal support'); assert(f.dom.window.document.querySelector('.owPlateNone button')); f.dom.window.close();
  }
  {
    const drawing = deferred(), f = sheetFixture({draw:()=>drawing.promise});
    f.SV.rid = '4000001'; f.SV.list = [{id:'saved-a',metal:'gold'}];
    const a = f.context.sheetDraw(), b = f.context.sheetDraw(); assert.equal(a,b,'concurrent callbacks share one actual sheet drawing'); assert.equal(f.drawings,1);
    drawing.resolve({failed:[]}); await a; assert.equal(f.SV.drawing,null,'completed drawing releases its task'); f.dom.window.close();
  }
  {
    const drawing = deferred(); let callback, disposed = 0;
    const f = sheetFixture({draw:(cv,target,rid,opts)=>{callback=opts;cv._order={dispose(){disposed++;}};return drawing.promise;}});
    f.SV.rid = '4000001'; f.SV.list = [{id:'saved-a',metal:'gold'}]; const pending = f.context.sheetDraw();
    f.context.sheetReset(); f.W.dlg.open = false; assert.equal(disposed,1,'close/reset releases canvas observers and animation frames');
    callback.onInfo({focus(){},rec:{id:'old'}}); callback.onProgress(1,2); callback.onWait('old request');
    drawing.resolve({failed:[]}); await pending; assert.equal(f.SV.info,null,'late draw cannot restore closed view state'); assert.equal(f.panelPaints,0,'late draw cannot repaint a closed panel'); f.dom.window.close();
  }
  {
    const drawing = deferred(); let callback, disposed = 0;
    const f = sheetFixture({draw:(cv,target,rid,opts)=>{callback=opts;cv._order={dispose(){disposed++;}};return drawing.promise;}});
    f.SV.rid = '4000001'; f.SV.list = [{id:'saved-a',metal:'gold'}]; const pending = f.context.sheetDraw();
    f.W.view = 'info'; f.context.sheetPause(); callback.onInfo({focus(){},rec:{id:'old'}}); drawing.resolve({failed:[]}); await pending;
    assert.equal(disposed,1,'leaving Sheet releases its observer even while the dialog stays open'); assert.equal(f.SV.info,null); assert.equal(f.SV.list[0].id,'saved-a','return navigation keeps discovered sheets');
    f.W.view = 'sheet'; f.context.SheetWin.drawOrder = async()=>({failed:[]}); await f.context.sheetShow(); await turn(); assert.equal(f.SV.drawing,null,'return to Sheet starts and settles a fresh draw'); f.dom.window.close();
  }
  {
    const pending = deferred(), SV = {epoch:3,at:0,list:[{id:'first'},{id:'second'}]}, W = {key:'A',rid:'4000001',view:'sheet',dlg:{open:true},closing:false}; let drawings = 0;
    const context = vm.createContext({W,SV,r:{loading:false},opts:{sheetAt:1},key:'A',rid:'4000001',sheetShow:()=>pending.promise,sheetDraw(){drawings++;}});
    vm.runInContext(section('    if (W.view === "sheet" && !r.loading)', '    if (opts.tab && W.view'),context);
    SV.epoch++; W.key = 'B'; W.rid = '4000002'; pending.resolve(); await turn();
    assert.equal(SV.at,0,'late requested sheet selection belongs only to its original order generation'); assert.equal(drawings,0);
  }
}
(async()=>{
  await testEngravingCallbacks(); await testRecoveryFailures(); await testApprovalDuringRefit(); await testCardClosing(); await testSheetViews();
  console.log('PASS engraving lifecycle resilience: stale fonts/workers, explicit reopen, Close/Escape verification/save wait, known-sheet immediate drawing, stale order isolation, discovery failure/timeout retry, drawing dedupe and disposal');
})().catch(error=>{console.error(error);process.exitCode=1;});
