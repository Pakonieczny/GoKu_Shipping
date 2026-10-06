'use strict';
// Actual handler with serial, atomic synthetic storage and provider doubles.
// These tests never authorize real spending or certify live microphone/audio.
const test=require('node:test'),assert=require('node:assert/strict');
const core=require('../../netlify/functions/_britesGrowth.js');
const voice=require('../../netlify/functions/_britesConciergeVoice.js');
const BASE_AT=Date.parse('2026-10-06T18:00:00Z'),WINDOW_MS=45*60000;
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n';
const TOKEN='a'.repeat(64),OWNER='synthetic-repair-root',ADMIN='synthetic-only-operator';
const clone=value=>value===undefined?undefined:structuredClone(value);
function deferred(){let resolve;const promise=new Promise(done=>{resolve=done;});return {promise,resolve};}

class Database{
  constructor(rows){this.rows=new Map(rows.map(([key,value])=>[key,clone(value)]));this.tail=Promise.resolve();this.counts={reads:0,writes:0};this.queries=[];}
  snapshot(key){const value=this.rows.get(key);return {exists:this.rows.has(key),data:()=>clone(value)};}
  write(key,value,options){this.counts.writes++;this.rows.set(key,options?.merge?{...this.rows.get(key),...clone(value)}:clone(value));}
  collection(name){
    const db=this;
    return {firestore:db,doc(id){const key=name+'/'+id;return {id,key,firestore:db,async get(){db.counts.reads++;return db.snapshot(key);},async set(value,options){db.write(key,value,options);}};},orderBy(field,direction){return {limit(limit){db.queries.push({name,field,direction,limit});return {async get(){db.counts.reads++;const docs=[...db.rows].filter(([key,value])=>key.startsWith(name+'/')&&value&&Object.hasOwn(value,field)).sort((a,b)=>direction==='desc'?b[1][field]-a[1][field]:a[1][field]-b[1][field]).slice(0,limit).map(([key,value])=>({id:key.slice(name.length+1),data:()=>clone(value)}));return {docs,size:docs.length};}};}};}};
  }
  runTransaction(task){
    const result=this.tail.then(async()=>{
      const writes=[];
      const answer=await task({async get(ref){assert.equal(writes.length,0,'Firestore requires all transaction reads before writes.');return ref.get();},set(ref,value,options){writes.push([ref.key,clone(value),options]);}});
      // Failed callbacks never commit; competing callbacks see the last commit.
      for(const [key,value,options]of writes)this.write(key,value,options);
      return answer;
    });
    this.tail=result.catch(()=>{});return result;
  }
}
function fixture(options={}){
  let at=options.at??BASE_AT,call=0;
  const env={BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',BRITES_CONCIERGE_REALTIME_ENABLED:'1',BRITES_CONCIERGE_REALTIME_PUBLIC_DEMO:'1',BRITES_CONCIERGE_REALTIME_DEMO_USD_CAP:'10',BRITES_CONCIERGE_REALTIME_RESERVE_USD:'1',OPENAI_API_KEY:'synthetic-only-provider-key',...options.env};
  const db=new Database([
    ['State/control',{enabled:true,aiEnabled:false,aiDailyUsdCap:1,stopAt:core.STOP_AT,...options.control}],
    ['State/controller',{owner:OWNER,tokenHash:core.hash(TOKEN),leaseUntil:at+WINDOW_MS,updatedAt:at}],
    ['State/checkpoint',{updatedAt:at-1000,phase:'synthetic-saved-work'}],
    ['VoiceUsage/preview-budget',{reservedCents:900,spentCents:100,calls:10,allocationCapCents:1000,at:at-10000}],
    ['VoiceUsage/session-'+('b'.repeat(64)),{allocatedCents:100,startedAt:at-10000,reconcile:'provider_evidence_required',originalHistory:'synthetic-history'}],
    ['VoiceDiagnostics/last-start',{at:at-10000,stage:'provider',providerStatus:429,code:'insufficient_quota'}]
  ]);
  const oldKeys=[...db.rows.keys()],provider=[],deadlines=[],counts={setup:0,providerStarts:0,hangups:0};
  const service={namespace:options.namespace??'Brites_Growth_Sandbox',col:name=>db.collection(name),async setup(){counts.setup++;return clone(db.rows.get('State/control'));},async rateLimit(){return true;}};
  const fetch=async(url,init)=>{
    provider.push({url,method:init?.method});
    if(url.endsWith('/hangup')){counts.hangups++;if(options.hangupFailure)throw Error('PRIVATE_HANGUP_UNCERTAINTY');return new Response(null,{status:200});}
    assert.equal(url,'https://api.openai.com/v1/realtime/calls');assert.equal(init.method,'POST');counts.providerStarts++;
    if(options.provider)return options.provider(url,init);
    const id='rtc_synthetic_'+(++call);return new Response(SDP,{status:201,headers:{Location:'/v1/realtime/calls/'+id,'x-request-id':'req_synthetic_'+call}});
  };
  const handler=voice.createHandler({env,service,now:()=>at,authorize:async req=>req.headers.get('X-Growth-Key')===ADMIN,fetch,scheduleHangup:async row=>{
    deadlines.push(clone(row));if(options.deadlineFailure)return false;
    await service.col('VoiceDeadlines').doc(row.callId).set({...row,state:'pending',at});return true;
  }});
  const request=(body,{operator=false,origin='https://preview.test'}={})=>new Request('https://preview.test/api/concierge-voice',{method:'POST',headers:{'Content-Type':'application/json',...(origin?{Origin:origin}:{}),...(operator?{'X-Growth-Key':ADMIN}:{})},body:JSON.stringify(body)});
  const authorization=changes=>({action:'authorize-test',owner:OWNER,token:TOKEN,expectedUpdatedAt:db.rows.get('State/checkpoint')?.updatedAt??0,...changes});
  const legacyBytes=()=>JSON.stringify([...db.rows].filter(([key])=>key.startsWith('VoiceUsage/')&&(key==='VoiceUsage/preview-budget'||key==='VoiceUsage/session-'+('b'.repeat(64)))));
  const savedBytes=()=>JSON.stringify(oldKeys.map(key=>[key,db.rows.get(key)]));
  const grant=()=>{const id=db.rows.get('VoiceContinuations/active')?.grantId;return id?db.rows.get('VoiceContinuations/'+id):undefined;};
  return {env,db,service,handler,request,authorization,counts,provider,deadlines,legacyBytes,savedBytes,grant,now:()=>at,advance:ms=>{at+=ms;},setAt:value=>{at=value;}};
}
async function authorize(f,changes={},requestOptions={}){const response=await f.handler(f.request(f.authorization(changes),{operator:true,...requestOptions}));return {response,value:await response.json()};}
async function capability(f){const response=await f.handler(f.request({action:'capabilities'}));return {response,value:await response.json()};}
async function token(f){const {response,value}=await capability(f);assert.equal(response.status,200);assert.equal(value.enabled,true);return value.demoToken;}
const nativeStart=(f,demoToken)=>f.handler(f.request({action:'start',sdp:SDP,demoToken}));
function assertDeniedUnchanged(f,before,writes){assert.equal(f.savedBytes(),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,0);assert.equal(f.grant(),undefined);}

test('authorization requires an operator, same origin, strict body and exact sandbox before storage writes',async()=>{
  const cases=[
    {name:'guest',request:{operator:false},status:401},
    {name:'foreign origin',request:{origin:'https://foreign.test'},status:403},
    {name:'extra requested money',body:{allocatedCents:1000},status:400},
    {name:'extra requested refunds',body:{refund:true},status:400},
    {name:'invalid owner',body:{owner:'invalid owner'},status:400},
    {name:'non-hex token',body:{token:'z'.repeat(64)},status:400},
    {name:'short token',body:{token:'a'.repeat(63)},status:400},
    {name:'string revision',body:{expectedUpdatedAt:String(BASE_AT-1000)},status:400},
    {name:'fractional revision',body:{expectedUpdatedAt:.5},status:400},
    {name:'negative revision',body:{expectedUpdatedAt:-1},status:400},
    {name:'live namespace',env:{BRITES_GROWTH_NAMESPACE:'Brites_Growth_Live'},status:403},
    {name:'missing explicit namespace',env:{BRITES_GROWTH_NAMESPACE:undefined},status:403},
    {name:'live service',namespace:'Brites_Growth_Live',status:403},
    {name:'sandbox disabled',env:{BRITES_GROWTH_SANDBOX:'0'},status:403}
  ];
  for(const item of cases){const f=fixture(item),before=f.savedBytes(),{response}=await authorize(f,item.body,item.request);assert.equal(response.status,item.status,item.name);assertDeniedUnchanged(f,before,0);assert.equal(f.counts.setup,0,item.name);assert.equal(f.db.counts.reads,0,item.name);}
});
test('authorization rejects absent, foreign, expired or malformed owned leases and stale checkpoint revisions without writes',async()=>{
  const cases=[
    {name:'missing lease',remove:true},
    {name:'foreign owner',lease:{owner:'another-repair-root'}},
    {name:'foreign token',body:{token:'c'.repeat(64)}},
    {name:'expired lease',lease:{leaseUntil:BASE_AT-1}},
    {name:'lease expires exactly now',lease:{leaseUntil:BASE_AT}},
    {name:'missing stored expiry',lease:{leaseUntil:undefined}},
    {name:'non-numeric stored expiry',lease:{leaseUntil:'private-invalid'}},
    {name:'string stored expiry',lease:{leaseUntil:String(BASE_AT+60000)}},
    {name:'stale revision',body:{expectedUpdatedAt:BASE_AT-1001}},
    {name:'coerced stored revision',checkpoint:{updatedAt:String(BASE_AT-1000)},body:{expectedUpdatedAt:BASE_AT-1000}}
  ];
  for(const item of cases){
    const f=fixture();if(item.remove)f.db.rows.delete('State/controller');if(item.lease)f.db.rows.set('State/controller',{...f.db.rows.get('State/controller'),...item.lease});if(item.checkpoint)f.db.rows.set('State/checkpoint',{...f.db.rows.get('State/checkpoint'),...item.checkpoint});
    const before=f.savedBytes(),{response}=await authorize(f,item.body);assert.ok([400,409,503].includes(response.status),item.name+' must fail closed, got '+response.status);assertDeniedUnchanged(f,before,0);
  }
});
for(const shape of ['activeWriter','leaseUntil','expiresAt'])test('active legacy '+shape+' checkpoint lease prevents a supplemental authorization without disturbing it',async()=>{
  const f=fixture(),checkpoint=f.db.rows.get('State/checkpoint');
  const legacy=shape==='activeWriter'?{activeWriter:'legacy-initial-root',leaseUntil:f.now()+60000}:{lease:{owner:'legacy-initial-root',[shape]:f.now()+60000}};
  f.db.rows.set('State/checkpoint',{...checkpoint,...legacy});const before=f.savedBytes(),{response,value}=await authorize(f);
  assert.equal(response.status,409);assert.match(value.error,/legacy writer/);assertDeniedUnchanged(f,before,0);
});
test('expired legacy checkpoint lease is preserved while the owned current controller authorizes a native slot',async()=>{
  const f=fixture();f.db.rows.set('State/checkpoint',{...f.db.rows.get('State/checkpoint'),activeWriter:'legacy-initial-root',leaseUntil:f.now()-1,lease:{owner:'legacy-initial-root',expiresAt:f.now()-1}});const before=f.savedBytes();
  const {response}=await authorize(f);assert.equal(response.status,200);assert.equal(f.savedBytes(),before);assert.equal(f.provider.length,0);
});
test('authorization records one bounded native-only slot without changing old budget, checkpoint, history or incurred holds',async()=>{
  const f=fixture(),before=f.savedBytes(),legacy=f.legacyBytes(),{response,value}=await authorize(f);
  assert.equal(response.status,200);assert.equal(value.ok,true);assert.equal(value.reused,false);assert.equal(response.headers.get('Cache-Control'),'no-store');
  assert.equal(value.grant.kind,undefined);assert.equal(value.grant.state,'available');assert.equal(value.grant.allocatedCents,100);assert.equal(value.grant.reservedCents,0);assert.equal(value.grant.spentCents,0);assert.equal(value.grant.expiresAt,f.now()+WINDOW_MS);
  assert.equal(f.grant().kind,'realtime_voice');assert.equal(f.grant().provider,'openai');assert.equal(f.grant().authorizedBy,OWNER);assert.equal(f.grant().checkpointUpdatedAt,BASE_AT-1000);
  assert.equal(value.providerCalls,0);assert.equal(value.refunds,0);assert.equal(value.legacyAllocationChanged,false);assert.equal(f.provider.length,0);assert.equal(f.counts.providerStarts,0);assert.equal(f.db.counts.writes,2);
  assert.equal(f.savedBytes(),before);assert.equal(f.legacyBytes(),legacy);assert.equal([...f.db.rows].filter(([key])=>key.startsWith('VoiceContinuations/continuation-')).length,1);
  assert.doesNotMatch(JSON.stringify(value),/synthetic-only-provider-key|synthetic-only-operator|tokenHash|originalHistory/);assert.equal(JSON.stringify(value).includes(TOKEN),false);
});
test('simultaneous same-lease authorizations are atomic and idempotent',async()=>{
  const f=fixture(),before=f.savedBytes(),results=await Promise.all(Array.from({length:6},()=>authorize(f)));
  assert.ok(results.every(({response,value})=>response.status===200&&value.ok));assert.equal(results.filter(({value})=>!value.reused).length,1);
  assert.equal(new Set(results.map(({value})=>value.grant.id)).size,1);assert.equal(f.db.counts.writes,2);assert.equal(f.savedBytes(),before);assert.equal(f.provider.length,0);
});
for(const state of ['available','consumed','expired'])test('same-lease '+state+' authorization cannot refresh or recreate its test slot',async()=>{
  const f=fixture();await authorize(f);if(state==='consumed')assert.equal((await nativeStart(f,await token(f))).status,200);
  if(state==='expired'){f.advance(WINDOW_MS);f.db.rows.set('State/controller',{...f.db.rows.get('State/controller'),leaseUntil:f.now()+60000});}else f.advance(1000);
  const before=JSON.stringify([...f.db.rows]),writes=f.db.counts.writes,calls=f.provider.length,{response,value}=await authorize(f);
  assert.equal(response.status,200);assert.equal(value.reused,true);assert.equal(value.grant.state,state==='consumed'?'consumed':'available');assert.equal(value.grant.issuedAt,BASE_AT);assert.equal(value.grant.expiresAt,BASE_AT+WINDOW_MS);
  assert.equal(JSON.stringify([...f.db.rows]),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,calls);
  if(state==='expired'){const paused=await capability(f);assert.equal(paused.response.status,429);assert.equal(paused.value.code,'VOICE_ALLOCATION_UNAVAILABLE');}
});
test('a replacement repair lease cannot overwrite another still-available test slot',async()=>{
  const f=fixture();await authorize(f);const replacement='c'.repeat(64);f.db.rows.set('State/controller',{owner:'synthetic-next-root',tokenHash:core.hash(replacement),leaseUntil:f.now()+WINDOW_MS,updatedAt:f.now()});
  const before=JSON.stringify([...f.db.rows]),writes=f.db.counts.writes,{response}=await authorize(f,{owner:'synthetic-next-root',token:replacement});
  assert.equal(response.status,409);assert.equal(JSON.stringify([...f.db.rows]),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,0);
});
for(const boundary of ['control','hard'])test('test slot expires at the earlier '+boundary+' stop rather than extending testing',async()=>{
  const at=boundary==='hard'?core.STOP_AT-60000:BASE_AT,stopAt=boundary==='hard'?core.STOP_AT+86400000:at+60000;
  const f=fixture({at,control:{stopAt}}),{response,value}=await authorize(f);assert.equal(response.status,200);assert.equal(value.grant.expiresAt,boundary==='hard'?core.STOP_AT:stopAt);assert.ok(value.grant.expiresAt-value.grant.issuedAt<=WINDOW_MS);
  f.setAt(value.grant.expiresAt);const stopped=await capability(f);assert.equal(stopped.response.status,503);assert.equal(stopped.value.code,'VOICE_RUNTIME_DISABLED');const writes=f.db.counts.writes;
  const denied=await authorize(f);assert.equal(denied.response.status,409);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,0);
});
test('disabled control and missing native configuration cannot authorize another allocation',async()=>{
  for(const options of [{control:{enabled:false}},{env:{OPENAI_API_KEY:''}},{env:{BRITES_CONCIERGE_REALTIME_ENABLED:'0'}},{env:{BRITES_CONCIERGE_REALTIME_PUBLIC_DEMO:'0'}}]){
    const f=fixture(options),before=f.savedBytes(),{response,value}=await authorize(f);assert.equal(response.status,503);assert.equal(value.code,'VOICE_GUARD_UNAVAILABLE');assertDeniedUnchanged(f,before,0);
  }
});
test('capabilities repeatedly uses an unused native slot without consuming it or starting a provider call',async()=>{
  const f=fixture();assert.equal((await capability(f)).response.status,429);await authorize(f);const before=JSON.stringify([...f.db.rows]),writes=f.db.counts.writes;
  const checks=await Promise.all(Array.from({length:4},()=>capability(f)));assert.ok(checks.every(({response,value})=>response.status===200&&value.enabled&&typeof value.demoToken==='string'));
  assert.equal(new Set(checks.map(({value})=>value.demoToken)).size,4);assert.equal(JSON.stringify([...f.db.rows]),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.grant().state,'available');assert.equal(f.provider.length,0);
});
test('concurrent native starts share one atomic supplemental slot and only one provider call',async()=>{
  const providerSeen=deferred(),answer=deferred(),f=fixture({provider:async()=>{providerSeen.resolve();await answer.promise;return new Response(SDP,{status:201,headers:{Location:'/v1/realtime/calls/rtc_concurrent','x-request-id':'req_concurrent'}});}});
  await authorize(f);const [firstToken,secondToken]=await Promise.all([token(f),token(f)]),legacy=f.legacyBytes();
  const calls=[nativeStart(f,firstToken),nativeStart(f,secondToken)];await providerSeen.promise;assert.equal(f.counts.providerStarts,1);assert.equal(f.grant().state,'consumed');answer.resolve();
  const responses=await Promise.all(calls);assert.deepEqual(responses.map(r=>r.status).sort(),[200,429]);const denied=responses.find(r=>r.status===429);assert.equal((await denied.json()).code,'VOICE_ALLOCATION_UNAVAILABLE');
  assert.equal(f.counts.providerStarts,1);assert.equal(f.deadlines.length,1);assert.equal(f.grant().reservedCents,100);assert.equal(f.grant().spentCents,0);assert.equal(f.legacyBytes(),legacy);
  const newAttempts=[...f.db.rows].filter(([key,value])=>key.startsWith('VoiceUsage/session-')&&value.fundingSource==='native_continuation');assert.equal(newAttempts.length,1);assert.equal(newAttempts[0][1].grantId,f.grant().id);assert.equal(newAttempts[0][1].stage,'call_verified');assert.equal(f.grant().sessionRecord,newAttempts[0][0].split('/')[1]);
  const paused=await capability(f);assert.equal(paused.response.status,429);assert.equal(paused.value.code,'VOICE_ALLOCATION_UNAVAILABLE');
});
test('reusing a consumed native start token cannot call again, refund its hold or reopen availability',async()=>{
  const f=fixture();await authorize(f);const demoToken=await token(f);assert.equal((await nativeStart(f,demoToken)).status,200);
  const before=JSON.stringify([...f.db.rows]),writes=f.db.counts.writes,response=await nativeStart(f,demoToken),value=await response.json();
  assert.equal(response.status,401);assert.equal(value.code,'VOICE_SESSION_REUSED');assert.equal(f.counts.providerStarts,1);assert.equal(f.db.counts.writes,writes);assert.equal(JSON.stringify([...f.db.rows]),before);assert.equal(f.grant().reservedCents,100);
});
test('typed legacy reservations cannot spend a native-only slot or change the exhausted original ledger',async()=>{
  const f=fixture();await authorize(f);const before=JSON.stringify([...f.db.rows]),writes=f.db.counts.writes;
  const reserve=voice.createDemoBudgetReservation(f.service,{capUsd:10,now:f.now});assert.equal(await reserve(.05,'synthetic-typed-session-id'),null);
  const result=voice.createDemoBudgetReservationResult(f.service,{capUsd:10,now:f.now,allowContinuation:true});assert.deepEqual(await result(.05,'synthetic-typed-next-session-id'),{granted:false,reason:'ALLOCATION_EXHAUSTED'});
  assert.equal(f.grant().state,'available');assert.equal(JSON.stringify([...f.db.rows]),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,0);
});
test('strict malformed ledgers fail closed before issuing or consuming a native slot',async()=>{
  for(const malformed of [{reservedCents:'900',spentCents:100,calls:10},{reservedCents:900,spentCents:null,calls:10},{reservedCents:900,spentCents:100,calls:'10'},{reservedCents:-1,spentCents:100,calls:10},{reservedCents:Number.MAX_SAFE_INTEGER,spentCents:1,calls:10}]){
    const denied=fixture();denied.db.rows.set('VoiceUsage/preview-budget',malformed);const before=denied.savedBytes(),authorization=await authorize(denied);assert.equal(authorization.response.status,503);assert.equal(authorization.value.code,'VOICE_GUARD_UNAVAILABLE');assertDeniedUnchanged(denied,before,0);
    const f=fixture();await authorize(f);const demoToken=await token(f);f.db.rows.set('VoiceUsage/preview-budget',malformed);const grantBefore=clone(f.grant()),writes=f.db.counts.writes;
    const capabilityResult=await capability(f),response=await nativeStart(f,demoToken);assert.equal(capabilityResult.response.status,503);assert.equal(capabilityResult.value.code,'VOICE_GUARD_UNAVAILABLE');assert.equal(response.status,503);assert.equal((await response.json()).code,'VOICE_GUARD_UNAVAILABLE');
    assert.deepEqual(f.grant(),grantBefore);assert.deepEqual(f.db.rows.get('VoiceUsage/preview-budget'),malformed);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,0);
  }
});
test('malformed grant, missing grant record and malformed pointer fail closed without resetting held history',async()=>{
  const changes=[{allocatedCents:'100'},{reservedCents:1},{spentCents:1},{state:'refunded'},{provider:'untrusted'},{issuedAt:'invalid'},{expiresAt:BASE_AT+WINDOW_MS+1},{remove:true},{pointer:'not-a-valid-grant'}];
  for(const patch of changes){
    const f=fixture();await authorize(f);const demoToken=await token(f),key='VoiceContinuations/'+f.grant().id;
    if(patch.remove)f.db.rows.delete(key);else if(patch.pointer)f.db.rows.set('VoiceContinuations/active',{grantId:patch.pointer});else f.db.rows.set(key,{...f.db.rows.get(key),...patch});
    const before=JSON.stringify([...f.db.rows]),writes=f.db.counts.writes,available=await capability(f),response=await nativeStart(f,demoToken);
    assert.equal(available.response.status,503);assert.equal(available.value.code,'VOICE_GUARD_UNAVAILABLE');assert.equal(response.status,503);assert.equal((await response.json()).code,'VOICE_GUARD_UNAVAILABLE');
    assert.equal(JSON.stringify([...f.db.rows]),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,0);
    const repair=await authorize(f);
    // A valid prior same-lease grant can be returned idempotently even if its
    // pointer is damaged. This must not heal the pointer or permit a start.
    if(patch.pointer){assert.equal(repair.response.status,200);assert.equal(repair.value.reused,true);}else assert.ok([409,503].includes(repair.response.status));
    assert.equal(f.db.counts.writes,writes);assert.equal(JSON.stringify([...f.db.rows]),before);
  }
});
for(const outcome of ['provider-rejection','transport-uncertainty','deadline-uncertainty','deadline-and-hangup-uncertainty'])test(outcome+' retains the full consumed supplemental hold and never retries a used token',async()=>{
  const options=outcome==='provider-rejection'?{provider:async()=>Response.json({error:{code:'insufficient_quota',message:'PRIVATE_PROVIDER_DETAIL'}},{status:429,headers:{'x-request-id':'req_rejected'}})}:outcome==='transport-uncertainty'?{provider:async()=>{throw Error('PRIVATE_TRANSPORT_DETAIL');}}:{deadlineFailure:true,hangupFailure:outcome==='deadline-and-hangup-uncertainty'};
  const f=fixture(options);await authorize(f);const demoToken=await token(f),legacy=f.legacyBytes(),response=await nativeStart(f,demoToken),publicValue=await response.json();
  assert.equal(response.status,503);assert.doesNotMatch(JSON.stringify(publicValue),/PRIVATE_|insufficient_quota|synthetic-only-provider-key/);assert.equal(f.counts.providerStarts,1);assert.equal(f.legacyBytes(),legacy);
  assert.equal(f.grant().state,'consumed');assert.equal(f.grant().reservedCents,100);assert.equal(f.grant().spentCents,0);
  const attempt=[...f.db.rows].find(([key,value])=>key.startsWith('VoiceUsage/session-')&&value.fundingSource==='native_continuation')[1];
  assert.equal(attempt.stage,outcome==='provider-rejection'?'provider_rejected':outcome==='transport-uncertainty'?'unknown':'deadline_failed');assert.equal(attempt.reconcile,'provider_evidence_required');assert.equal(attempt.allocatedCents,100);
  if(outcome.startsWith('deadline')){assert.equal(f.counts.hangups,1);assert.equal(attempt.hangupConfirmed,outcome!=='deadline-and-hangup-uncertainty');}
  const before=JSON.stringify([...f.db.rows]),writes=f.db.counts.writes,reused=await nativeStart(f,demoToken);assert.equal(reused.status,401);assert.equal((await reused.json()).code,'VOICE_SESSION_REUSED');assert.equal(f.db.counts.writes,writes);assert.equal(JSON.stringify([...f.db.rows]),before);assert.equal(f.counts.providerStarts,1);
  const paused=await capability(f);assert.equal(paused.response.status,429);assert.equal(paused.value.code,'VOICE_ALLOCATION_UNAVAILABLE');
});
test('an expired supplemental slot is paused even when its recently issued start token is still valid',async()=>{
  const f=fixture();await authorize(f);f.advance(WINDOW_MS-60000);const demoToken=await token(f),before=JSON.stringify([...f.db.rows]),writes=f.db.counts.writes;f.advance(60000);
  assert.ok(voice.readDemoToken(demoToken,f.env.OPENAI_API_KEY,f.now()));const paused=await capability(f),response=await nativeStart(f,demoToken);
  assert.equal(paused.response.status,429);assert.equal(paused.value.code,'VOICE_ALLOCATION_UNAVAILABLE');assert.equal(response.status,429);assert.equal((await response.json()).code,'VOICE_ALLOCATION_UNAVAILABLE');
  assert.equal(f.grant().state,'available');assert.equal(f.grant().reservedCents,0);assert.equal(f.db.counts.writes,writes);assert.equal(JSON.stringify([...f.db.rows]),before);assert.equal(f.provider.length,0);
});
