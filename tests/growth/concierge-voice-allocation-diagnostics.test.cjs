'use strict';
// Synthetic storage and provider responses only. These checks neither reconcile
// real allocations nor certify a live microphone or provider connection.
const test=require('node:test'),assert=require('node:assert/strict'),crypto=require('node:crypto');
const voice=require('../../netlify/functions/_britesConciergeVoice.js');
const SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n';
const env={BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',BRITES_CONCIERGE_REALTIME_ENABLED:'1',BRITES_CONCIERGE_REALTIME_PUBLIC_DEMO:'1',BRITES_CONCIERGE_REALTIME_DEMO_USD_CAP:'10',BRITES_CONCIERGE_REALTIME_RESERVE_USD:'1',OPENAI_API_KEY:'fixture-only-provider-key'};
const sessionId=i=>'session-'+crypto.createHash('sha256').update('synthetic-session-'+i).digest('hex');
const request=(body,{operator=false,origin='https://preview.test'}={})=>new Request('https://preview.test/api/concierge-voice',{method:'POST',headers:{'Content-Type':'application/json',...(origin?{Origin:origin}:{}),...(operator?{'X-Growth-Key':'synthetic-operator'}:{})},body:JSON.stringify(body)});
function fixture(changes={}){
  const rows=new Map(),queries=[],counts={reads:0,writes:0,provider:0,setup:0};let at=1000000,serial=Promise.resolve();
  const db={runTransaction(task){const result=serial.then(()=>task({get:ref=>ref.get(),set(ref,value){counts.writes++;rows.set(ref.key,structuredClone(value));}}));serial=result.catch(()=>{});return result;}};
  const service={namespace:'Brites_Growth_Sandbox',setup:async()=>{counts.setup++;return {enabled:true};},rateLimit:async()=>true,col(name){
    const collection={firestore:db,doc(id){const key=name+'/'+id;return {key,async get(){counts.reads++;return {exists:rows.has(key),data:()=>structuredClone(rows.get(key))};},async set(value){counts.writes++;rows.set(key,structuredClone(value));}};},orderBy(field,direction){return {limit(limit){queries.push({name,field,direction,limit});return {async get(){counts.reads++;const docs=[...rows].filter(([key,value])=>key.startsWith(name+'/')&&Object.prototype.hasOwnProperty.call(value,field)).sort((a,b)=>{const order=a[1][field]>b[1][field]?1:a[1][field]<b[1][field]?-1:0;return direction==='desc'?-order:order;}).slice(0,limit).map(([key,value])=>({id:key.slice(name.length+1),data:()=>structuredClone(value)}));return {docs,size:docs.length};}};}};}};
    return collection;
  }};
  const fetch=async(url,init)=>{counts.provider++;if(changes.fetch)return changes.fetch(url,init);if(url.endsWith('/hangup'))return new Response(null,{status:200});return new Response(SDP,{status:201,headers:{Location:'/v1/realtime/calls/rtc_fixture','x-request-id':'req_fixture'}});};
  const handler=voice.createHandler({env:{...env,...changes.env},service,authorize:async req=>req.headers.get('X-Growth-Key')==='synthetic-operator',fetch,now:()=>at,scheduleHangup:async row=>{if(changes.deadline===false)return false;await service.col('VoiceDeadlines').doc(row.callId).set({...row,state:'pending',at});return true;},...changes.handler});
  return {rows,queries,counts,service,handler,now:()=>at,advance(ms){at+=ms;}};
}
async function token(f){const response=await f.handler(request({action:'capabilities'}));assert.equal(response.status,200);return (await response.json()).demoToken;}
async function start(f,demoToken){return f.handler(request({action:'start',sdp:SDP,demoToken}));}
async function inspect(f,body={action:'allocation'}){const response=await f.handler(request(body,{operator:true}));return {response,value:await response.json()};}

test('legacy reservation consumers still receive only null or the original grant shape',async()=>{
  const f=fixture(),reserve=voice.createDemoBudgetReservation(f.service,{capUsd:1,now:f.now});
  assert.deepEqual(await reserve(1,'first-synthetic-session'),{allocatedUsd:1,reconcile:'provider_evidence_required'});
  assert.equal(await reserve(1,'first-synthetic-session'),null);assert.equal(await reserve(1,'another-synthetic-session'),null);
  assert.equal(f.rows.get('VoiceUsage/preview-budget').reservedCents,100);assert.equal(f.rows.get('VoiceUsage/preview-budget').calls,1);
});
test('voice distinguishes a consumed token from exhaustion without allocating or calling twice',async()=>{
  const f=fixture(),demoToken=await token(f);assert.equal((await start(f,demoToken)).status,200);const before=structuredClone(f.rows.get('VoiceUsage/preview-budget')),calls=f.counts.provider;
  const reused=await start(f,demoToken),value=await reused.json();assert.equal(reused.status,401);assert.equal(value.code,'VOICE_SESSION_REUSED');assert.match(value.message,/fresh voice session/);
  assert.deepEqual(f.rows.get('VoiceUsage/preview-budget'),before);assert.equal(f.counts.provider,calls);
  const capability=await f.handler(request({action:'capabilities'}));assert.equal(capability.status,200);assert.equal((await capability.json()).enabled,true);
});
test('a fresh token refused by actual capacity remains an allocation pause',async()=>{
  const f=fixture(),demoToken=await token(f);f.rows.set('VoiceUsage/preview-budget',{reservedCents:1000,spentCents:0,calls:10});const before=structuredClone(f.rows.get('VoiceUsage/preview-budget'));
  const denied=await start(f,demoToken);assert.equal(denied.status,429);assert.equal((await denied.json()).code,'VOICE_ALLOCATION_UNAVAILABLE');assert.equal(f.counts.provider,0);assert.deepEqual(f.rows.get('VoiceUsage/preview-budget'),before);
});
for(const malformed of [{reservedCents:'0',spentCents:0,calls:0},{reservedCents:0,spentCents:null,calls:0},{reservedCents:0,spentCents:0,calls:'0'},{reservedCents:-1,spentCents:0,calls:0},{reservedCents:Number.MAX_SAFE_INTEGER,spentCents:1,calls:0}])test('malformed stored integer ledger is a guard failure rather than a coerced grant: '+JSON.stringify(malformed),async()=>{
  const f=fixture(),demoToken=await token(f);f.rows.set('VoiceUsage/preview-budget',malformed);const writes=f.counts.writes;
  const capability=await f.handler(request({action:'capabilities'}));assert.equal(capability.status,503);assert.equal((await capability.json()).code,'VOICE_GUARD_UNAVAILABLE');
  const denied=await start(f,demoToken);assert.equal(denied.status,503);assert.equal((await denied.json()).code,'VOICE_GUARD_UNAVAILABLE');assert.equal(f.counts.provider,0);assert.equal(f.counts.writes,writes);assert.deepEqual(f.rows.get('VoiceUsage/preview-budget'),malformed);
});
test('typed result API distinguishes denial reasons while preserving concurrent capacity protection',async()=>{
  const f=fixture(),reserve=voice.createDemoBudgetReservationResult(f.service,{capUsd:1,now:f.now});
  const results=await Promise.all([reserve(1,'first-synthetic-session'),reserve(1,'other-synthetic-session')]);assert.equal(results.filter(result=>result.granted).length,1);assert.equal(results.find(result=>!result.granted).reason,'ALLOCATION_EXHAUSTED');
  assert.deepEqual(await reserve(1,'first-synthetic-session'),{granted:false,reason:'SESSION_ALREADY_USED'});
  f.rows.set('VoiceUsage/preview-budget',{reservedCents:'0',spentCents:0,calls:0});assert.deepEqual(await reserve(1,'third-synthetic-session'),{granted:false,reason:'INVALID_LEDGER'});
});
test('allocation inspection rejects guests, foreign origins and additional fields before storage',async()=>{
  const f=fixture();const guest=await f.handler(request({action:'allocation'}));assert.equal(guest.status,401);
  const foreign=await f.handler(request({action:'allocation'},{operator:true,origin:'https://foreign.test'}));assert.equal(foreign.status,403);
  for(const extra of [{limit:1000},{refund:true},{token:'private-token'},{records:[]}])assert.equal((await inspect(f,{action:'allocation',...extra})).response.status,400);
  assert.deepEqual(f.counts,{reads:0,writes:0,provider:0,setup:0});
});
test('allocation inspection requires the exact namespace even for authenticated operators',async()=>{
  for(const namespace of ['Brites_Growth_Live',undefined]){const f=fixture({env:{BRITES_GROWTH_NAMESPACE:namespace}});assert.equal((await inspect(f)).response.status,403);assert.equal(f.counts.reads,0);assert.equal(f.counts.provider,0);}
  const f=fixture();f.service.namespace='Brites_Growth_Live';assert.equal((await inspect(f)).response.status,403);assert.equal(f.counts.reads,0);
});
test('operator can inspect an exhausted allocation with voice disabled and no provider key, without writes or inference',async()=>{
  const f=fixture({env:{BRITES_CONCIERGE_REALTIME_ENABLED:'0',OPENAI_API_KEY:''}});f.rows.set('VoiceUsage/preview-budget',{reservedCents:1000,spentCents:0,calls:10,allocationCapCents:1000});f.rows.set('VoiceUsage/'+sessionId(1),{allocatedCents:100,startedAt:f.now(),reconcile:'provider_evidence_required'});
  const before=structuredClone([...f.rows]),{response,value}=await inspect(f);assert.equal(response.status,200);assert.equal(response.headers.get('Cache-Control'),'no-store');
  assert.equal(value.budget.nextReservationFits,false);assert.equal(value.budget.reservedCents,1000);assert.equal(value.recordWindow.unattributed,1);assert.equal(value.recordWindow.heldCents,100);assert.equal(value.evidenceComplete,false);assert.equal(value.financialWrites,0);assert.equal(value.refunds,0);assert.equal(value.providerCalls,0);
  assert.deepEqual([...f.rows],before);assert.equal(f.counts.writes,0);assert.equal(f.counts.provider,0);assert.equal(f.counts.setup,0);
});
test('private read sanitizes malformed ledger, reservation, deadline and diagnostic records instead of reflecting arbitrary fields',async()=>{
  const f=fixture();f.rows.set('VoiceUsage/preview-budget',{reservedCents:'PRIVATE_VALUE',spentCents:0,calls:0,secret:'PRIVATE_KEY'});
  f.rows.set('VoiceUsage/'+sessionId(1),{allocatedCents:'PRIVATE_VALUE',startedAt:f.now(),stopToken:'PRIVATE_TOKEN',providerRequestId:'PRIVATE_KEY',kind:'realtime_voice',provider:'openai',stage:'PRIVATE_STAGE',providerCode:'PRIVATE_CODE'});
  f.rows.set('VoiceUsage/PRIVATE_DOCUMENT',{allocatedCents:100,startedAt:f.now()});
  f.rows.set('VoiceDeadlines/rtc_fixture',{callId:'rtc_fixture',at:f.now(),expiresAt:'PRIVATE_VALUE',state:'PRIVATE_STAGE',token:'PRIVATE_TOKEN'});
  f.rows.set('VoiceDiagnostics/last-start',{at:'PRIVATE_VALUE',providerStatus:'PRIVATE_STATUS',code:'PRIVATE_CODE',stage:'PRIVATE_STAGE',secret:'PRIVATE_KEY'});
  const {response,value}=await inspect(f);assert.equal(response.status,200);assert.equal(value.budget.valid,false);assert.equal(value.budget.reservedCents,null);assert.equal(value.budget.nextReservationFits,null);assert.equal(value.recordWindow.invalid,2);assert.equal(value.records[0].allocatedCents,null);assert.equal(value.deadlineWindow.invalid,1);
  assert.deepEqual(value.lastStart,{at:null,providerStatus:null,code:null,stage:null});assert.doesNotMatch(JSON.stringify(value),/PRIVATE_/);assert.equal(f.counts.writes,0);assert.equal(f.counts.provider,0);
});
test('allocation read windows remain bounded and explicitly partial',async()=>{
  const f=fixture();f.rows.set('VoiceUsage/preview-budget',{reservedCents:65,spentCents:0,calls:65});for(let i=0;i<65;i++)f.rows.set('VoiceUsage/'+sessionId(i),{allocatedCents:1,startedAt:f.now()+i,reconcile:'provider_evidence_required'});
  for(let i=0;i<25;i++)f.rows.set('VoiceDeadlines/rtc_fixture'+i,{callId:'rtc_fixture'+i,at:f.now()+i,expiresAt:f.now()+1000,state:'closed',closedAt:f.now()+2000});
  const {value}=await inspect(f);assert.equal(value.records.length,50);assert.equal(value.recordWindow.truncated,true);assert.equal(value.recordWindow.heldCents,50);assert.equal(value.deadlineRecords.length,20);assert.equal(value.deadlineWindow.truncated,true);assert.equal(value.evidenceComplete,false);
  assert.deepEqual(f.queries,[{name:'VoiceUsage',field:'startedAt',direction:'desc',limit:50},{name:'VoiceDeadlines',field:'at',direction:'desc',limit:20}]);assert.equal(f.counts.writes,0);assert.equal(f.counts.provider,0);
});
test('successful future voice attempts retain exact private provider provenance and their full reservation',async()=>{
  const f=fixture(),demoToken=await token(f),response=await start(f,demoToken),publicAnswer=await response.json();assert.equal(response.status,200);assert.equal(typeof publicAnswer.stopToken,'string');
  const row=[...f.rows].find(([key])=>key.startsWith('VoiceUsage/session-'))[1];assert.equal(row.kind,'realtime_voice');assert.equal(row.provider,'openai');assert.equal(row.stage,'call_verified');assert.equal(row.callId,'rtc_fixture');assert.equal(row.providerRequestId,'req_fixture');assert.equal(row.reconcile,'provider_evidence_required');assert.equal(row.allocatedCents,100);
  const count=f.counts.provider,{value}=await inspect(f);assert.equal(value.records[0].callId,value.deadlineRecords[0].callId);assert.equal(value.budget.reservedCents,100);assert.equal(value.recordWindow.withProviderOutcome,1);assert.equal(f.counts.provider,count);assert.doesNotMatch(JSON.stringify(value),/fixture-only-provider-key|synthetic-operator/);assert.equal(JSON.stringify(value).includes(publicAnswer.stopToken),false);
});
test('provider rejection gets exact private provenance without a refund, leaked error body, or reused-token retry',async()=>{
  const f=fixture({fetch:async()=>Response.json({error:{code:'insufficient_quota',message:'PRIVATE_ACCOUNT'}},{status:429,headers:{'x-request-id':'req_rejected'}})}),demoToken=await token(f),response=await start(f,demoToken);assert.equal(response.status,503);assert.doesNotMatch(await response.text(),/PRIVATE_ACCOUNT|insufficient_quota/);
  const row=[...f.rows].find(([key])=>key.startsWith('VoiceUsage/session-'))[1];assert.equal(row.stage,'provider_rejected');assert.equal(row.providerStatus,429);assert.equal(row.providerCode,'insufficient_quota');assert.equal(row.providerRequestId,'req_rejected');assert.equal(row.callId,undefined);assert.equal(row.reconcile,'provider_evidence_required');assert.equal(f.rows.get('VoiceUsage/preview-budget').reservedCents,100);
  const count=f.counts.provider,reused=await start(f,demoToken);assert.equal((await reused.json()).code,'VOICE_SESSION_REUSED');assert.equal(f.counts.provider,count);const {value}=await inspect(f);assert.equal(value.lastStart.code,'insufficient_quota');assert.equal(value.records[0].providerCode,'insufficient_quota');assert.equal(value.evidenceComplete,false);
});
test('ambiguous transport failure and confirmed deadline-failure hangup keep their full held allocations',async()=>{
  const failed=fixture({fetch:async()=>{throw Error('PRIVATE_TRANSPORT');}}),demoToken=await token(failed);assert.equal((await start(failed,demoToken)).status,503);const row=[...failed.rows].find(([key])=>key.startsWith('VoiceUsage/session-'))[1];assert.equal(row.stage,'unknown');assert.equal(failed.rows.get('VoiceUsage/preview-budget').reservedCents,100);assert.doesNotMatch(JSON.stringify(row),/PRIVATE_TRANSPORT/);
  assert.equal((await inspect(failed)).value.recordWindow.withProviderOutcome,0);
  const deadline=fixture({deadline:false}),secondToken=await token(deadline);assert.equal((await start(deadline,secondToken)).status,503);const second=[...deadline.rows].find(([key])=>key.startsWith('VoiceUsage/session-'))[1];assert.equal(second.stage,'deadline_failed');assert.equal(second.callId,'rtc_fixture');assert.equal(second.hangupConfirmed,true);assert.equal(deadline.rows.get('VoiceUsage/preview-budget').reservedCents,100);
});
test('unreadable accepted answer preserves its known call reference and hangs up without assuming zero spend',async()=>{
  const urls=[],f=fixture({fetch:async url=>{urls.push(url);if(url.endsWith('/hangup'))return new Response(null,{status:200});const response=new Response(SDP,{status:201,headers:{Location:'/v1/realtime/calls/rtc_unreadable','x-request-id':'req_unreadable'}});response.text=async()=>{throw Error('PRIVATE_BODY_FAILURE');};return response;}}),demoToken=await token(f);
  const response=await start(f,demoToken);assert.equal(response.status,503);assert.doesNotMatch(await response.text(),/PRIVATE_BODY_FAILURE/);assert.equal(urls.at(-1),'https://api.openai.com/v1/realtime/calls/rtc_unreadable/hangup');
  const row=[...f.rows].find(([key])=>key.startsWith('VoiceUsage/session-'))[1];assert.equal(row.stage,'unknown');assert.equal(row.callId,'rtc_unreadable');assert.equal(row.providerRequestId,'req_unreadable');assert.equal(row.hangupConfirmed,true);assert.equal(row.providerStatus,201);assert.equal(f.rows.get('VoiceUsage/preview-budget').reservedCents,100);
  const {value}=await inspect(f);assert.equal(value.recordWindow.withProviderOutcome,0);assert.equal(value.evidenceComplete,false);assert.equal(value.lastStart.providerStatus,201);
});
