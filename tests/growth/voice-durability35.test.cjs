'use strict';
// Synthetic Firestore/provider fixtures only: no network, real usage or refunds.
const test=require('node:test'),assert=require('node:assert/strict');
const voice=require('../../netlify/functions/_britesConciergeVoice.js'),deadline=require('../../netlify/functions/_britesConciergeVoiceDeadline.js'),core=require('../../netlify/functions/_britesGrowth.js');
const AT=Date.parse('2026-10-07T23:00:00Z'),SDP='v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n',NATIVE='VoiceAllowances/native-sandbox',LEGACY='VoiceUsage/preview-budget';
const copy=value=>value===undefined?undefined:structuredClone(value);
const env={BRITES_GROWTH_SANDBOX:'1',BRITES_GROWTH_NAMESPACE:'Brites_Growth_Sandbox',BRITES_CONCIERGE_REALTIME_ENABLED:'1',BRITES_CONCIERGE_REALTIME_PUBLIC_DEMO:'1',BRITES_CONCIERGE_REALTIME_DEMO_USD_CAP:'10',BRITES_CONCIERGE_REALTIME_RESERVE_USD:'1',OPENAI_API_KEY:'synthetic-only-secret'};
function fixture({funding='native',failStages=[],missingDispatchRecord=false,hangupStatus=200,hangupThrows=false}={}){
  const rows=new Map([
    [LEGACY,{reservedCents:funding==='native'?950:100,spentCents:0,calls:funding==='native'?10:1,allocationCapCents:1000,at:AT-1000}],
    [NATIVE,{schema:2,id:'native-sandbox',kind:'realtime_voice',provider:'openai',allocatedCents:500,limitCents:1000,reservationCents:100,reservedCents:500,spentCents:0,calls:5,issuedAt:AT-600000,expiresAt:core.STOP_AT,lastStartedAt:AT-10000,continuationCents:500,continuationCount:1,continuationPolicy:'configured-preview-ceiling-once-v1',continuedAt:AT-5000,continuationAuthorizedBy:'synthetic-owner',continuationCheckpointUpdatedAt:AT-6000,accountingVersion:1,accountedCalls:0,accountedAllocationCents:0}],
    ['VoiceUsage/session-'+'a'.repeat(64),{allocatedCents:100,startedAt:AT-10000,reconcile:'provider_evidence_required',originalHistory:'synthetic-preserve'}],
    ['State/control',{enabled:true,stopAt:core.STOP_AT}]
  ]),operations=[],calls=[],writes=[],failureStages=new Set(failStages);let serial=Promise.resolve(),transactions=0;
  const db={runTransaction(task){const result=serial.then(async()=>{
    const ordinal=++transactions,pending=[];
    const result=await task({get:async ref=>{
      assert.equal(pending.length,0,'all transaction reads precede writes');
      if(missingDispatchRecord&&ordinal===2&&ref.key.startsWith('VoiceUsage/session-')&&!ref.key.endsWith('a'.repeat(64)))return {exists:false,data:()=>undefined};
      return ref.get();
    },set(ref,value){
      if(ref.key.startsWith('VoiceUsage/session-')&&failureStages.has(value.stage))throw Error('synthetic private storage detail');
      pending.push([ref,copy(value)]);
    }});
    for(const [ref,value]of pending){rows.set(ref.key,value);writes.push(ref.key);if(value.stage)operations.push('stored:'+value.stage);}
    return result;
  });serial=result.catch(()=>{});return result;}};
  const service={namespace:'Brites_Growth_Sandbox',setup:async()=>copy(rows.get('State/control')),rateLimit:async()=>true,col(name){return {firestore:db,doc(id){const key=name+'/'+id;return {id,key,get:async()=>({exists:rows.has(key),data:()=>copy(rows.get(key))}),set:async(value,options)=>{rows.set(key,options?.merge?{...rows.get(key),...copy(value)}:copy(value));writes.push(key);}};}};}};
  const fetch=async(url,init)=>{
    calls.push({url,method:init.method});
    if(url.endsWith('/hangup')){operations.push('provider:hangup');if(hangupThrows)throw Error('synthetic private hangup detail');return new Response(null,{status:hangupStatus});}
    assert.equal(url,'https://api.openai.com/v1/realtime/calls');operations.push('provider:start');
    return new Response(SDP,{status:201,headers:{Location:'/v1/realtime/calls/rtc_durable_synthetic','x-request-id':'req_durable_synthetic'}});
  };
  const handler=voice.createHandler({env,service,fetch,now:()=>AT,authorize:async()=>false,scheduleHangup:async row=>{operations.push('deadline');await service.col('VoiceDeadlines').doc(row.callId).set({...row,at:AT,state:'pending'});return true;}});
  const request=body=>new Request('https://preview.test/api/concierge-voice',{method:'POST',headers:{Origin:'https://preview.test','Content-Type':'application/json'},body:JSON.stringify(body)});
  const call=async body=>{const response=await handler(request(body));return {response,value:await response.json()};};
  const start=async()=>{const capability=await call({action:'capabilities'});assert.equal(capability.response.status,200);return {token:capability.value.demoToken,...await call({action:'start',sdp:SDP,demoToken:capability.value.demoToken})};};
  const attempt=()=>[...rows].find(([key])=>key.startsWith('VoiceUsage/session-')&&!key.endsWith('a'.repeat(64)))?.[1];
  return {rows,operations,calls,writes,call,start,attempt};
}
function assertHeld(f,funding){
  const native=f.rows.get(NATIVE),legacy=f.rows.get(LEGACY);
  assert.equal(native.reservedCents,funding==='native'?600:500);assert.equal(native.calls,funding==='native'?6:5);assert.equal(native.spentCents,0);
  assert.equal(native.allocatedCents,500);assert.equal(native.limitCents,1000);assert.equal(native.continuationCount,1);assert.equal(native.expiresAt,core.STOP_AT);assert.equal(native.accountedCalls,0);assert.equal(native.accountedAllocationCents,0);
  assert.equal(legacy.reservedCents,funding==='legacy'?200:950);assert.equal(legacy.calls,funding==='legacy'?2:10);assert.equal(legacy.spentCents,0);assert.equal(legacy.allocationCapCents,1000);
  assert.equal(f.rows.get('VoiceUsage/session-'+'a'.repeat(64)).originalHistory,'synthetic-preserve');
}
for(const funding of ['native','legacy']){
  test(funding+' voice persists dispatch and accepted linkage before releasing SDP',async()=>{
    const f=fixture({funding}),answer=await f.start();assert.equal(answer.response.status,200);assert.equal(answer.value.sdp,SDP);
    assert.ok(f.operations.indexOf('stored:provider_requested')<f.operations.indexOf('provider:start'));
    assert.ok(f.operations.indexOf('deadline')<f.operations.indexOf('stored:call_verified'));
    assert.equal(f.attempt().stage,'call_verified');assert.equal(f.attempt().callId,'rtc_durable_synthetic');assert.equal(f.attempt().providerRequestId,'req_durable_synthetic');assert.equal(f.attempt().reconcile,'provider_evidence_required');assert.equal(f.calls.length,1);assertHeld(f,funding);
  });
  test(funding+' dispatch-storage failure retains its reservation and cannot call the provider',async()=>{
    const f=fixture({funding,failStages:['provider_requested']}),answer=await f.start();assert.equal(answer.response.status,503);assert.equal(answer.value.code,'VOICE_GUARD_UNAVAILABLE');assert.equal(answer.value.sdp,undefined);assert.equal(answer.value.stopToken,undefined);assert.equal(f.calls.length,0);assert.equal(f.attempt().stage,'reserved');assertHeld(f,funding);
    assert.equal(f.rows.get('VoiceDiagnostics/last-start').code,'PROVENANCE_UNAVAILABLE');assert.doesNotMatch(JSON.stringify(answer.value),/storage|synthetic|secret|rtc_/);
    const retry=await f.call({action:'start',sdp:SDP,demoToken:answer.token});assert.equal(retry.response.status,401);assert.equal(retry.value.code,'VOICE_SESSION_REUSED');assert.equal(f.calls.length,0);assertHeld(f,funding);
  });
  for(const hangupStatus of [200,404,503])test(funding+' accepted-linkage storage failure with hangup'+hangupStatus+' withholds SDP and preserves its hold',async()=>{
    const f=fixture({funding,failStages:['call_verified'],hangupStatus}),answer=await f.start();assert.equal(answer.response.status,503);assert.equal(answer.value.code,'VOICE_GUARD_UNAVAILABLE');assert.equal(answer.value.sdp,undefined);assert.equal(answer.value.stopToken,undefined);
    assert.deepEqual(f.calls.map(x=>x.url),['https://api.openai.com/v1/realtime/calls','https://api.openai.com/v1/realtime/calls/rtc_durable_synthetic/hangup']);
    assert.equal(f.attempt().stage,'unknown');assert.equal(f.attempt().callId,'rtc_durable_synthetic');assert.equal(f.attempt().providerRequestId,'req_durable_synthetic');assert.equal(f.attempt().providerCode,'PROVENANCE_UNAVAILABLE');assert.equal(f.attempt().hangupConfirmed,hangupStatus!==503);
    assert.equal(f.rows.get('VoiceDeadlines/rtc_durable_synthetic').state,hangupStatus===503?'pending':'closed');assertHeld(f,funding);assert.equal(f.rows.get('VoiceDiagnostics/last-start').stage,'verification');
  });
}
test('a missing reserved record prevents dispatch while preserving the independently held allocation',async()=>{
  const f=fixture({missingDispatchRecord:true}),answer=await f.start();assert.equal(answer.response.status,503);assert.equal(f.calls.length,0);assert.equal(f.attempt().stage,'reserved');assertHeld(f,'native');
});
for(const hangupThrows of [false,true])test('persistent accepted-provenance failure keeps exact deadline backup with '+(hangupThrows?'unknown transport':'503 cleanup'),async()=>{
  const f=fixture({failStages:['call_verified','unknown'],hangupStatus:503,hangupThrows}),answer=await f.start();assert.equal(answer.response.status,503);assert.equal(answer.value.sdp,undefined);assert.equal(f.attempt().stage,'provider_requested');assert.equal(f.calls.length,2);
  const row=f.rows.get('VoiceDeadlines/rtc_durable_synthetic');assert.equal(row.callId,'rtc_durable_synthetic');assert.equal(row.state,'pending');assert.equal(row.expiresAt,AT+voice.MAX_DURATION_MS);assertHeld(f,'native');assert.doesNotMatch(JSON.stringify(answer.value),/synthetic|private|rtc_|req_/);
});

function reaperFixture(){
  let at=AT;const rows=new Map(),calls=[],queries=[],writes=[];
  const doc=id=>({get:async()=>({exists:rows.has(id),data:()=>copy(rows.get(id))}),set:async value=>{writes.push({id,value:copy(value)});rows.set(id,{...rows.get(id),...copy(value)});}});
  const query=limit=>({get:async()=>{queries.push(limit??null);return {docs:[...rows].sort(([a],[b])=>a.localeCompare(b)).slice(0,limit).map(([id,value])=>({id,data:()=>copy(value)}))};}});
  const service={col:name=>{assert.equal(name,'VoiceDeadlines','the reaper cannot write any funding ledger');return {doc,where:(field,op,value)=>{assert.deepEqual([field,op,value],['state','==','pending']);return {...query(),limit:n=>query(n)};}};}};
  const f={env,service,now:()=>at,fetch:async url=>{calls.push(url.split('/').at(-2));return {ok:false,status:503};}};
  return {f,rows,calls,queries,writes,advance:ms=>at+=ms};
}
test('all 25 expired pending calls get a turn despite persistent first-window failures and four-attempt bounds',async()=>{
  const f=reaperFixture();for(let n=0;n<25;n++){const id='rtc_due_'+String(n).padStart(2,'0');f.rows.set(id,{callId:id,state:'pending',expiresAt:AT-1000,lastAttemptAt:0});}
  for(let n=0;n<7;n++){const before=f.calls.length;assert.deepEqual(await deadline.reap(f.f),{closed:0});assert.equal(f.calls.length-before,4);f.advance(60000);}
  assert.equal(new Set(f.calls).size,25);assert.equal(f.calls.length,28);assert.ok(f.calls.includes('rtc_due_24'));assert.ok(f.queries.every(limit=>limit===null));assert.ok([...f.rows.values()].every(row=>row.state==='pending'&&row.lastStatus===503));assert.ok(f.writes.every(row=>Object.keys(row.value).sort().join(',')==='lastAttemptAt,lastStatus'));
});
test('malformed, stale and future rows cannot consume a reaper attempt slot',async()=>{
  const f=reaperFixture();
  for(const [id,value]of [
    ['bad_document',{callId:'bad_document',state:'pending',expiresAt:AT-1}],
    ['rtc_a_mismatch',{callId:'rtc_other',state:'pending',expiresAt:AT-1}],
    ['rtc_a_missing',{state:'pending',expiresAt:AT-1}],
    ['rtc_a_badtime',{callId:'rtc_a_badtime',state:'pending',expiresAt:String(AT-1)}],
    ['rtc_a_zero',{callId:'rtc_a_zero',state:'pending',expiresAt:0}],
    ['rtc_a_closed',{callId:'rtc_a_closed',state:'closed',expiresAt:AT-1}],
    ['rtc_a_future',{callId:'rtc_a_future',state:'pending',expiresAt:AT+1}],
    ['rtc_a_null',null],['rtc_a_array',[]]
  ])f.rows.set(id,value);
  for(let n=0;n<4;n++){const id='rtc_valid_'+n;f.rows.set(id,{callId:id,state:'pending',expiresAt:AT-1,lastAttemptAt:n===0?AT+60000:0});}
  const preserved=JSON.stringify([...f.rows].filter(([id])=>!id.startsWith('rtc_valid_')));
  assert.deepEqual(await deadline.reap(f.f),{closed:0});assert.deepEqual([...new Set(f.calls)].sort(),['rtc_valid_0','rtc_valid_1','rtc_valid_2','rtc_valid_3']);assert.equal(f.calls.length,4);assert.equal(JSON.stringify([...f.rows].filter(([id])=>!id.startsWith('rtc_valid_'))),preserved);
});
test('finish rechecks exact pending state and deadline after a candidate was scanned',async()=>{
  const f=reaperFixture();for(const value of [null,[],{callId:'rtc_other',state:'pending',expiresAt:AT-1},{callId:'rtc_checked',state:'unexpected',expiresAt:AT-1},{callId:'rtc_checked',state:'pending',expiresAt:0},{callId:'rtc_checked',state:'pending',expiresAt:AT+1}]){
    f.rows.set('rtc_checked',value);assert.equal(await deadline.finish({...f.f,callId:'rtc_checked'}),false);
  }
  assert.equal(f.calls.length,0);assert.equal(f.writes.length,0);
});
