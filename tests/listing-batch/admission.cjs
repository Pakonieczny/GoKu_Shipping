'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { admissionControl, quotaFailure, queuedName } = require('../../netlify/functions/lib/listingBatchAdmission.cjs');
const src = fs.readFileSync('netlify/functions/geminiImageProxy-background.js', 'utf8');
const clone = value => value == null ? value : structuredClone(value);
function database(active) {
  const data = new Map(Array.from({length:active}, (_, i) => ['batches/batch_live_'+i, {state:'JOB_STATE_RUNNING',collected:false}]));
  const snapshot = path => ({exists:data.has(path), id:path.split('/').pop(), data:()=>clone(data.get(path))});
  let serial = Promise.resolve();
  const db = {
    data,
    collection(name) {
      const query = (filters=[], limit=Infinity) => ({
        where:(key,op,val)=>query([...filters,[key,op,val]],limit),
        limit:n=>query(filters,n),
        get: async()=>({docs:[...data].filter(([k,v])=>k.startsWith(name+'/') && filters.every(([field,op,w])=>op==='in'?w.includes(v[field]):v[field]===w)).slice(0,limit).map(([path])=>snapshot(path))}),
      });
      return {...query(), doc(id) { const path=name+'/'+id; return {path,get:async()=>snapshot(path),set:async(value)=>data.set(path,{...data.get(path),...clone(value)})}; }};
    },
    runTransaction(fn) {
      const result=serial.then(()=>fn({get:ref=>ref.get(),set:(ref,val)=>{data.set(ref.path,{...data.get(ref.path),...clone(val)});}}));
      serial=result.catch(()=>{});return result;
    }
  };return db;
}
const sourceRecord = {state:'JOB_STATE_QUEUED',collected:false,locallyQueued:true};
async function unitScenarios() {
  let now=1000000;
  const db=database(29), gate=admissionControl(db,'batches',()=>now,()=>now);
  for(let i=0;i<20;i++) db.data.set('batches/source'+i,clone(sourceRecord));
  const claims=await Promise.all(Array.from({length:20},(_,i)=>gate.reserve('source'+i)));
  assert.equal(claims.filter(c=>c.token).length,1,'concurrent initial and retry submissions share one reservation');
  const claim=claims.find(c=>c.token);
  await gate.beforeCreate(claim,{inputFileName:'file-test',sets:[{tasks:['original']}]});
  await gate.complete(claim,'batch_new',{inputFileName:'file-test',sets:[{tasks:['original']}]},{id:'batch_new'});
  assert.equal((await gate.reserve('source1')).queued,true,'thirtieth active job closes admission');
  assert.equal((await gate.reserve('source0')).existing,'batch_new','same submission reuses its provider job');
  db.data.get('batches/batch_live_0').state='JOB_STATE_SUCCEEDED';
  assert.equal((await gate.reserve('source1')).queued,true,'unvalidated provider job blocks a refill');
  db.data.get('batches/batch_new').state='JOB_STATE_RUNNING';
  const next=await gate.reserve('source1');assert(next.token);
  await gate.release(next,new Error('upload failed'));
  await gate.rejected('batch_refused');
  assert.equal((await gate.reserve('source1')).queued,true,'token refusal waits for capacity to drain');
  db.data.get('batches/batch_live_1').state='JOB_STATE_SUCCEEDED';
  const retry=await gate.reserve('source1');assert(retry.token);
  await gate.beforeCreate(retry,{inputFileName:'file-unknown',sets:[{tasks:['preserve me']}]});
  await gate.release(retry,new Error('network timeout'));
  now+=20*60000;
  assert.equal((await gate.reserve('source2')).queued,true,'ambiguous create never releases admission for a duplicate');
  await gate.reconcile(async file=>({id:'batch_recovered',input_file_id:file}));
  assert.equal(db.data.get('batches/source1').retryBatchName,'batch_recovered');
  assert.deepEqual(db.data.get('batches/batch_recovered').sets,[{tasks:['preserve me']}]);
  assert.equal((await gate.reserve('source1')).existing,'batch_recovered');
  assert(quotaFailure('Enqueued token limit reached; limit 1,000,000'));
  assert(!quotaFailure('Invalid image format'));
}
async function staleProbeScenario() {
  // OpenAI validates a job in a minute or two. One stuck in validation
  // (2026-09-29: over half an hour) must not hold every queued set.
  let now=1000000;
  const db=database(10), gate=admissionControl(db,'batches',()=>now,()=>now);
  for(const id of ['source0','source1','source2']) db.data.set('batches/'+id,clone(sourceRecord));
  const claim=await gate.reserve('source0');
  await gate.beforeCreate(claim,{inputFileName:'file-a',sets:[]});
  await gate.complete(claim,'batch_probe',{inputFileName:'file-a',sets:[]},{id:'batch_probe'});
  now+=14*60000;
  assert.equal((await gate.reserve('source1')).reason,'Waiting for provider validation','a job validating for fourteen minutes still holds admission');
  now+=60000;
  const next=await gate.reserve('source1');
  assert(next.token,'a job validating for fifteen minutes no longer holds admission');
  await gate.beforeCreate(next,{inputFileName:'file-b',sets:[]});
  await gate.complete(next,'batch_after',{inputFileName:'file-b',sets:[]},{id:'batch_after'});
  assert.equal((await gate.reserve('source2')).reason,'Waiting for provider validation','the job sent next is waited for as usual');
  assert.equal(db.data.get('batches/batch_probe').state,'JOB_STATE_PENDING','the stuck job is left as it is');
  db.data.set('batches/batch_orphan_probe',{state:'JOB_STATE_PENDING',collected:false});
  await db.collection('LG1_Config').doc('batchAdmission').set({probeName:'batch_orphan_probe'});
  now+=60*60000;
  assert.equal((await gate.reserve('source2')).reason,'Waiting for provider validation','a probe with no send time is still waited for');
}
async function abandonedPreparationScenario() {
  let now=1000000;
  const db=database(0), gate=admissionControl(db,'batches',()=>now,()=>now);
  for(const id of ['source0','source1','source2']) db.data.set('batches/'+id,clone(sourceRecord));
  const abandoned=await gate.reserve('source0');
  now+=6*60000;
  const replacement=await gate.reserve('source1');
  assert(replacement.token,'a preparer that stopped cannot hold the queue indefinitely');
  await assert.rejects(gate.beforeCreate(abandoned,{inputFileName:'late-upload'}),/reservation expired/i,
    'a late abandoned preparer cannot create a duplicate paid job');
  await gate.release(abandoned,new Error('late failure'));
  assert.equal(db.data.get('LG1_Config/batchAdmission').owner,replacement.token);
  await gate.beforeCreate(replacement,{inputFileName:'file-unconfirmed'});
  now+=6*60000;
  assert.equal((await gate.reserve('source2')).queued,true,'an uncertain paid create is reconciled rather than replaced');
}
async function failedPreparationIsolationScenario() {
  let now=1000000;
  const db=database(0), gate=admissionControl(db,'batches',()=>now,()=>now);
  for(const id of ['broken','healthy']) db.data.set('batches/'+id,clone(sourceRecord));
  const stopped=await gate.reserve('broken');
  await gate.progress(stopped,'downloading reference images');
  now+=6*60000;
  const marker={batchName:'broken',stalledAt:now-6*60000};
  await gate.reconcile(async()=>{throw new Error('preparation has no paid provider job to reconcile');},marker);
  assert.equal(db.data.get('batches/broken').preparationFailures,1);
  assert.match(db.data.get('batches/broken').retryError,/downloading reference images/);
  await gate.reconcile(async()=>null,marker);
  assert.equal(db.data.get('batches/broken').preparationFailures,1,'same dead worker is recorded once');
  assert.equal((await gate.reserve('broken')).sourceError,true,'failed source is deferred rather than blocking healthy listings');
  const healthy=await gate.reserve('healthy'); assert(healthy.token);
  await assert.rejects(gate.beforeCreate(stopped,{inputFileName:'late-input'}),/reservation expired/);
  await gate.release(healthy,null);
  for(let attempt=2;attempt<=3;attempt++) {
    now+=11*60000;
    const claim=await gate.reserve('broken');assert(claim.token);
    await gate.failPreparation(claim,new Error('Reference download timed out'));
    await gate.release(claim,null);
  }
  const failed=db.data.get('batches/broken');
  assert.equal(failed.preparationFailures,3);
  assert.equal(failed.retryStatus,'preparation_failed');
  assert.equal(failed.locallyQueued,false);assert.equal(failed.retryRequested,false);
  assert.equal(failed.recoveryStatus,'blocked','repeated failure becomes a visible terminal issue');
  assert.equal((await gate.reserve('broken')).sourceError,true,'stopped preparation is not paid again');
  assert((await gate.reserve('healthy')).token,'terminal source failure does not hold the next listing');
}
async function originalSubmissionScenario() {
  const db=database(35); let uploads=0, creates=0, now=1000000;
  const body={kind:'batch_submit',sessionId:'sess_original',displayName:'lg1-Beady_Necklace-300sets-test-part282of300',
    sets:[{category:'Beady_Necklace',setN:282,outputBasePath:'listing-generator-1/Beady_Necklace/Ready_To_List/Set_282',
      tasks:[{type:'edits',slotIndex:0,prompt:'The original prompt',input_storage_path:'ref.png',input_charm_storage_path:'charm.png'}]}]};
  const start=src.indexOf('    if (kind === "batch_submit") {');
  const end=src.indexOf('    if (kind === "batch_retry_queue") {',start);
  const submit=async payload=>vm.runInNewContext(`(async()=>{ ${src.slice(start,end)} })()`,{
    kind:'batch_submit', body:clone(payload), modelConfig:{id:'gpt-image-2.5-sunburst',supportsBatch:true},
    apiKeyForImageModel:()=> 'test', batchDocIdFromName:x=>x, getDb:()=>db, BATCHES_COLL:'batches', admissionControl, queuedName,
    batchCharmPaths: require('../../netlify/functions/lib/listingBatchReservations.cjs').batchCharmPaths,
    normalizeCategory:x=>x,GENERATABLE_CATEGORIES:new Set(['Beady_Necklace']),assertAllowedOutputBase:()=>{},
    admin:{storage:()=>({bucket:()=>({})}),firestore:{FieldValue:{serverTimestamp:()=>now}}},
    process,Buffer,console:{log:()=>{}}, json:(statusCode,data)=>({statusCode,...data}), quotaFailure,
    setTimeout: fn => setTimeout(fn, 0), clearTimeout,
    runBoundedConcurrent:async(items,_n,fn)=>Promise.all(items.map(async(item,i)=>{try{return await fn(item,i);}catch(__error){return {__error};}})),
    storagePathToBuffer:async path=>path === 'unreadable' ? new Promise(()=>{}) : ({mime:'image/png',buffer:Buffer.from('test image')}),
    buildOpenAIBatchJsonlLine:()=>({request:'test'}),listingImageSize:()=> '2048x2048',
    withCurrentBeadyCharmSize:(_set,_slot,prompt)=>prompt,
    uploadOpenAIBatchFile:async()=>{uploads++;return 'file-input';},
    createOpenAIImageBatch:async()=>{creates++;return {batchName:'batch_one',raw:{id:'batch_one'}};},
  });
  const first=await submit(body), repeated=await submit(body);
  assert(first.queued);assert.equal(first.batchName,repeated.batchName);
  assert.equal(uploads,0);assert.equal(creates,0,'old browser cannot submit while over capacity');
  const record=db.data.get('batches/'+first.batchName);
  assert.deepEqual(record.sets,body.sets,'full tasks/charm/output paths saved before waiting');
  for(let i=0;i<7;i++)db.data.get('batches/batch_live_'+i).state='JOB_STATE_SUCCEEDED';
  const admitted=await submit({...body,retryOf:first.batchName});
  assert.equal(admitted.batchName,'batch_one');assert.equal(creates,1);
  assert.equal(db.data.get('batches/'+first.batchName).retryBatchName,'batch_one');
  assert.deepEqual(db.data.get('batches/batch_one').sets[0].tasks,body.sets[0].tasks);
  const after=await submit(body);assert.equal(after.batchName,'batch_one');assert.equal(creates,1,'repeated original request never creates another provider job');
  db.data.get('batches/batch_one').state='JOB_STATE_RUNNING';
  const broken=clone(body);broken.displayName='listing-with-unreadable-reference';
  broken.sets[0].outputBasePath += '-broken';broken.sets[0].tasks[0].input_storage_path='unreadable';
  const failed=await submit(broken);
  assert.equal(failed.sourceError,true,'reference timeout is a listing-specific preparation error');
  assert.equal(creates,1,'a timed-out reference never creates a paid provider job');
  assert.equal(db.data.get('LG1_Config/batchAdmission').owner,null,'failed preparation releases admission for other listings');
  assert.match(db.data.get('batches/'+failed.batchName).retryError,/reference download timed out/i);
}
async function quotaRecoveryScenario() {
  const db=database(35);
  db.data.set('batches/batch_refused',{batchName:'batch_refused',state:'JOB_STATE_RUNNING',collected:false,
    sets:[{category:'Beady_Necklace',setN:18,tasks:[{prompt:'preserved'}]}]});
  const start=src.indexOf('    if (kind === "batch_status") {', src.indexOf('    if (kind === "batch_retry_missing") {'));
  const end=src.indexOf('    if (kind === "batch_collect") {',start);
  const run=async message=>vm.runInNewContext(`(async()=>{ ${src.slice(start,end)} })()`,{
    kind:'batch_status',body:{batchName:'batch_refused'},batchApiKey:()=> 'test',
    getDb:()=>db,BATCHES_COLL:'batches',batchDocIdFromName:x=>x,
    getGeminiBatchJob:async()=>({state:'JOB_STATE_FAILED'}),batchFailureDetails:()=>message,
    firestoreRetry:fn=>fn(),quotaFailure,admissionControl,
    admin:{firestore:{FieldValue:{serverTimestamp:()=>123}}},json:(statusCode,data)=>({statusCode,...data}),
  });
  await run('Invalid image format');
  assert(!db.data.get('batches/batch_refused').retryRequested,'unrelated provider errors remain visible');
  await run('Enqueued token limit reached; limit 1,000,000');
  assert.equal(db.data.get('batches/batch_refused').retryRequested,true,'capacity rejection is automatically queued');
  assert.deepEqual(db.data.get('batches/batch_refused').sets[0].tasks,[{prompt:'preserved'}]);
  assert.equal(db.data.get('LG1_Config/batchAdmission').blockedAtActive,35);
}
async function cancellationFeedbackScenario() {
  const db=database(30);
  let status='cancelling';
  const normalized=()=>({state:status==='cancelling'?'JOB_STATE_RUNNING':'JOB_STATE_CANCELLED',providerStatus:status});
  const context={body:{batchName:'batch_live_0'},batchApiKey:()=> 'test',
    getDb:()=>db,BATCHES_COLL:'batches',batchDocIdFromName:x=>x,
    getGeminiBatchJob:async()=>normalized(),cancelGeminiBatchJob:async()=>normalized(),batchFailureDetails:()=>null,
    firestoreRetry:fn=>fn(),quotaFailure,admissionControl,
    admin:{firestore:{FieldValue:{serverTimestamp:()=>123}}},json:(statusCode,data)=>({statusCode,...data})};
  const cancelStart=src.indexOf('    if (kind === "batch_cancel") {',src.indexOf('    if (kind === "batch_list") {'));
  const cancelEnd=src.indexOf('    // ------------------------------------------------------------',cancelStart);
  const cancel=await vm.runInNewContext(`(async()=>{ ${src.slice(cancelStart,cancelEnd)} })()`,{...context,kind:'batch_cancel'});
  assert.equal(cancel.cancellationRequested,true);
  assert.equal(cancel.cancelled,false,'provider acknowledgement does not claim cancellation is complete');
  assert.equal(cancel.providerStatus,'cancelling');
  assert.equal(db.data.get('batches/batch_live_0').providerStatus,'cancelling');
  assert.equal((await admissionControl(db,'batches',()=>123).reserve('next')).queued,true,'stopping jobs still consume capacity');

  const statusStart=src.indexOf('    if (kind === "batch_status") {',src.indexOf('    if (kind === "batch_retry_missing") {'));
  const statusEnd=src.indexOf('    if (kind === "batch_collect") {',statusStart);
  const refresh=()=>vm.runInNewContext(`(async()=>{ ${src.slice(statusStart,statusEnd)} })()`,{...context,kind:'batch_status'});
  assert.equal((await refresh()).providerStatus,'cancelling','refresh preserves cancellation feedback');
  status='cancelled';
  const stopped=await refresh();
  assert.equal(stopped.done,true);
  assert.equal(db.data.get('batches/batch_live_0').providerStatus,'cancelled');
  assert((await admissionControl(db,'batches',()=>123).reserve('next')).token,'only confirmed cancellation frees capacity');
}
async function submitRefusalCountScenario() {
  // A token-limit refusal at submission adds one to the set's refusal count,
  // which stops it at the ceiling instead of letting it loop.
  const db=database(0);
  db.data.set('batches/batch_src',{batchName:'batch_src',state:'JOB_STATE_FAILED',collected:false,retryRequested:true});
  const start=src.indexOf('    if (kind === "batch_submit") {');
  const end=src.indexOf('    if (kind === "batch_retry_queue") {',start);
  const body={kind:'batch_submit',sessionId:'sess_original',displayName:'retry-x',retryOf:'batch_src',capacityRefusals:3,
    sets:[{category:'Beady_Necklace',setN:9,outputBasePath:'listing-generator-1/Beady_Necklace/Ready_To_List/Set_9',
      tasks:[{type:'edits',slotIndex:0,prompt:'p',input_storage_path:'ref.png',input_charm_storage_path:'charm.png'}]}]};
  const result=await vm.runInNewContext(`(async()=>{ ${src.slice(start,end)} })()`,{
    kind:'batch_submit', body, modelConfig:{id:'gpt-image-2.5-sunburst',supportsBatch:true},
    apiKeyForImageModel:()=> 'test', batchDocIdFromName:x=>x, getDb:()=>db, BATCHES_COLL:'batches', admissionControl, queuedName, quotaFailure,
    batchCharmPaths: require('../../netlify/functions/lib/listingBatchReservations.cjs').batchCharmPaths,
    normalizeCategory:x=>x,GENERATABLE_CATEGORIES:new Set(['Beady_Necklace']),assertAllowedOutputBase:()=>{},
    admin:{storage:()=>({bucket:()=>({})}),firestore:{FieldValue:{serverTimestamp:()=>1,increment:n=>({increment:n})}}},
    process,Buffer,console:{log:()=>{}}, json:(statusCode,data)=>({statusCode,...data}), setTimeout, clearTimeout,
    runBoundedConcurrent:async(items,_n,fn)=>Promise.all(items.map(fn)),
    storagePathToBuffer:async()=>({mime:'image/png',buffer:Buffer.from('test image')}),
    buildOpenAIBatchJsonlLine:()=>({request:'test'}),listingImageSize:()=> '2048x2048',
    withCurrentBeadyCharmSize:(_set,_slot,prompt)=>prompt,
    uploadOpenAIBatchFile:async()=>'file-input',
    createOpenAIImageBatch:async()=>{throw new Error('Enqueued token limit reached; limit 1,000,000');},
  });
  assert.equal(result.queued,true);
  assert.deepEqual(db.data.get('batches/batch_src').capacityRefusals,{increment:1});
}
(async()=>{await unitScenarios();await staleProbeScenario();await abandonedPreparationScenario();await failedPreparationIsolationScenario();await originalSubmissionScenario();await quotaRecoveryScenario();await cancellationFeedbackScenario();await submitRefusalCountScenario();console.log('Shared admission: concurrency, isolated bounded preparation failures, abandoned workers, capacity, validation, durable queue, idempotency and paid-create reconciliation passed');})().catch(e=>{console.error(e);process.exitCode=1;});
