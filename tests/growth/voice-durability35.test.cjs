'use strict';
// Synthetic Firestore/provider fixtures only: no network, real usage or refunds.
const test=require('node:test'),assert=require('node:assert/strict');
const voice=require('../../netlify/functions/_britesConciergeVoice.js'),deadline=require('../../netlify/functions/_britesConciergeVoiceDeadline.js');
const {fixture,AT,SDP,ENV:env,clone:copy}=require('./voice-fixture36.cjs');
function durable(options={}){return fixture({callId:'rtc_durable_synthetic',requestId:'req_durable_synthetic',...options});}
function attempt(f){return f.journal()[0];}
function assertHistorical(f,before){assert.equal(f.moneyBytes(),before);assert.ok(f.db.writes.every(key=>! /^(VoiceUsage|VoiceAllowances|VoiceContinuations|Usage)\//.test(key)));}
for(const reservedCents of [950,1000]){
  test('native dispatch and accepted linkage are durable with '+reservedCents+' historical cents held',async()=>{
    const f=durable({rows:[['VoiceUsage/preview-budget',{reservedCents,spentCents:0,calls:10,allocationCapCents:1000}]]}),before=f.moneyBytes(),answer=await f.start();assert.equal(answer.response.status,200);assert.equal(answer.value.sdp,SDP);
    assert.ok(f.db.operations.indexOf('stored:provider_requested')<f.db.operations.indexOf('provider:start'));
    assert.ok(f.db.operations.indexOf('deadline')<f.db.operations.indexOf('stored:call_verified'));
    assert.equal(attempt(f).stage,'call_verified');assert.equal(attempt(f).callId,'rtc_durable_synthetic');assert.equal(attempt(f).providerRequestId,'req_durable_synthetic');assert.equal(f.calls.length,1);assertHistorical(f,before);
    assert.doesNotMatch(JSON.stringify(attempt(f)),/allocated|reserved|spent|refund|funding|allowance|grant/i);
  });
  test('dispatch-journal failure cannot call the provider or change historical holds '+reservedCents,async()=>{
    const f=durable({failStages:['provider_requested'],rows:[['VoiceUsage/preview-budget',{reservedCents,spentCents:0,calls:10}]]}),before=f.moneyBytes(),token=await f.token(),answer=await f.start(token);assert.equal(answer.response.status,503);assert.equal(answer.value.code,'VOICE_GUARD_UNAVAILABLE');assert.equal(answer.value.sdp,undefined);assert.equal(answer.value.stopToken,undefined);assert.equal(f.calls.length,0);assert.equal(attempt(f),undefined);assertHistorical(f,before);
    assert.doesNotMatch(JSON.stringify(answer.value),/storage|PRIVATE_|secret|rtc_/);
    const retry=await f.start(token);assert.equal(retry.response.status,503);assert.equal(f.calls.length,0);assertHistorical(f,before);
  });
  for(const hangupStatus of [200,404,503])test('accepted-linkage storage failure with hangup'+hangupStatus+' withholds SDP and historical cents '+reservedCents,async()=>{
    const f=durable({failStages:['call_verified'],hangupStatus,rows:[['VoiceUsage/preview-budget',{reservedCents,spentCents:0,calls:10}]]}),before=f.moneyBytes(),token=await f.token(),answer=await f.start(token);assert.equal(answer.response.status,503);assert.equal(answer.value.code,'VOICE_GUARD_UNAVAILABLE');assert.equal(answer.value.sdp,undefined);assert.equal(answer.value.stopToken,undefined);
    assert.deepEqual(f.calls.map(x=>x.url),['https://api.openai.com/v1/realtime/calls','https://api.openai.com/v1/realtime/calls/rtc_durable_synthetic/hangup']);
    assert.equal(attempt(f).stage,'unknown');assert.equal(attempt(f).callId,'rtc_durable_synthetic');assert.equal(attempt(f).providerRequestId,'req_durable_synthetic');assert.equal(attempt(f).providerCode,'PROVENANCE_UNAVAILABLE');assert.equal(attempt(f).hangupConfirmed,hangupStatus!==503);
    assert.equal(f.rows.get('VoiceDeadlines/rtc_durable_synthetic').state,hangupStatus===503?'pending':'closed');assertHistorical(f,before);assert.equal(f.rows.get('VoiceDiagnostics/last-start').stage,'verification');
    const retry=await f.start(token);assert.equal(retry.response.status,401);assert.equal(retry.value.code,'VOICE_SESSION_REUSED');assert.equal(f.counts.starts,1);assertHistorical(f,before);
  });
}
test('missing journal outcome record closes an accepted call without releasing its SDP',async()=>{
  const f=durable({missingOutcomeRecord:true}),before=f.moneyBytes(),answer=await f.start();assert.equal(answer.response.status,503);assert.equal(answer.value.sdp,undefined);assert.equal(f.counts.starts,1);assert.equal(f.counts.hangups,1);assert.equal(attempt(f).stage,'provider_requested');assert.equal(f.rows.get('VoiceDeadlines/rtc_durable_synthetic').state,'closed');assertHistorical(f,before);
});
for(const hangupThrows of [false,true])test('persistent accepted-provenance failure keeps exact deadline backup with '+(hangupThrows?'unknown transport':'503 cleanup'),async()=>{
  const f=durable({failStages:['call_verified','unknown'],hangupStatus:503,hangupThrows}),before=f.moneyBytes(),answer=await f.start();assert.equal(answer.response.status,503);assert.equal(answer.value.sdp,undefined);assert.equal(attempt(f).stage,'provider_requested');assert.equal(f.calls.length,2);
  const row=f.rows.get('VoiceDeadlines/rtc_durable_synthetic');assert.equal(row.callId,'rtc_durable_synthetic');assert.equal(row.state,'pending');assert.equal(row.expiresAt,AT+voice.MAX_DURATION_MS);assertHistorical(f,before);assert.doesNotMatch(JSON.stringify(answer.value),/synthetic|private|rtc_|req_/i);
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
