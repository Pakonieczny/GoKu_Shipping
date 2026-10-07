'use strict';
// Retirement and historical-helper checks use atomic synthetic storage only.
const test=require('node:test'),assert=require('node:assert/strict');
const voice=require('../../netlify/functions/_britesConciergeVoice.js');
const {fixture,AT,clone}=require('./voice-fixture36.cjs');
const ACTION='authorize-test',ID='continuation-'+'c'.repeat(64),WINDOW=45*60000;
const retiredBody={action:ACTION,owner:'PRIVATE_CONTROLLER',token:'d'.repeat(64),expectedUpdatedAt:AT-1000};
const grant=()=>({schema:1,id:ID,kind:'realtime_voice',provider:'openai',allocatedCents:100,reservedCents:0,spentCents:0,state:'available',issuedAt:AT,expiresAt:AT+WINDOW,consumedAt:null,authorizedBy:'PRIVATE_CONTROLLER',checkpointUpdatedAt:AT-1000});
function legacy(options={}){const f=fixture(options);f.rows.delete('VoiceAllowances/native-sandbox');f.rows.set('VoiceContinuations/active',{grantId:ID,at:AT-1000});f.rows.set('VoiceContinuations/'+ID,grant());return f;}
function reserve(f,options={}){return voice.createDemoBudgetReservationResult(f.service,{capUsd:10,now:f.now,provenance:true,allowContinuation:true,...options});}
function unchanged(f,before,writes=0){assert.equal(f.protectedBytes(),before);assert.equal(f.db.counts.writes,writes);assert.equal(f.provider.length,0);}
for(const item of [
  {name:'operator',status:410,request:{operator:true}},
  {name:'guest',status:401},
  {name:'foreign origin',status:403,request:{operator:true,origin:'https://foreign.test'}},
  {name:'live environment',status:403,request:{operator:true},options:{env:{BRITES_GROWTH_NAMESPACE:'Live'}}},
  {name:'missing exact namespace',status:403,request:{operator:true},options:{env:{BRITES_GROWTH_NAMESPACE:undefined}}},
  {name:'live service',status:403,request:{operator:true},options:{namespace:'Live'}}
])test('retired one-test authorization '+item.name+' cannot read or change stored work',async()=>{
  const f=fixture(item.options),before=f.protectedBytes(),answer=await f.call(retiredBody,item.request);assert.equal(answer.response.status,item.status);unchanged(f,before);assert.equal(f.db.counts.reads,0);assert.equal(f.counts.setup,0);assert.doesNotMatch(JSON.stringify(answer.value),/PRIVATE_|tokenHash|allocatedCents|refund|d{64}/);
});
for(const patch of [{allocatedCents:1000},{refund:true},{expiresAt:AT+WINDOW},{owner:'invalid owner'},{token:'z'.repeat(64)},{expectedUpdatedAt:String(AT-1000)}])test('retired authorization never reflects or consumes obsolete authority '+Object.keys(patch)[0],async()=>{
  const f=fixture(),before=f.protectedBytes(),answer=await f.call({...retiredBody,...patch},{operator:true});assert.equal(answer.response.status,410);unchanged(f,before);assert.equal(f.db.counts.reads,0);assert.doesNotMatch(JSON.stringify(answer.value),/invalid owner|z{64}|PRIVATE_|allocatedCents/);
});
for(const lease of [null,{owner:'foreign',leaseUntil:AT+WINDOW},{owner:'PRIVATE_CONTROLLER',leaseUntil:AT},{owner:'PRIVATE_CONTROLLER',leaseUntil:String(AT+WINDOW)}])test('retirement preserves existing controller shape '+JSON.stringify(lease),async()=>{
  const f=fixture();if(lease===null)f.rows.delete('State/controller');else f.rows.set('State/controller',lease);const before=f.protectedBytes();assert.equal((await f.call(retiredBody,{operator:true})).response.status,410);unchanged(f,before);assert.equal(f.db.counts.reads,0);
});
test('parallel retired authorizations create no replacement grant, lease or checkpoint version',async()=>{
  const f=fixture(),before=f.protectedBytes(),answers=await Promise.all(Array.from({length:8},()=>f.call(retiredBody,{operator:true})));assert.ok(answers.every(row=>row.response.status===410));unchanged(f,before);assert.equal(f.db.counts.reads,0);assert.equal([...f.rows.keys()].some(key=>key.startsWith('VoiceContinuations/')),false);
});
test('historical continuation helper remains one-use, exact native-only, atomic and fully held',async()=>{
  const f=legacy(),before=clone(f.rows.get('VoiceUsage/preview-budget')),old=clone(f.rows.get('VoiceUsage/session-'+'a'.repeat(64))),results=await Promise.all([reserve(f)(1,'old-helper-first-session'),reserve(f)(1,'old-helper-second-session')]);assert.equal(results.filter(row=>row.granted).length,1);assert.equal(results.find(row=>!row.granted).reason,'ALLOCATION_EXHAUSTED');
  const granted=results.find(row=>row.granted);assert.deepEqual(granted.allocation,{allocatedUsd:1,reconcile:'provider_evidence_required',fundingSource:'native_continuation',grantId:ID});assert.equal(f.rows.get('VoiceContinuations/'+ID).state,'consumed');assert.equal(f.rows.get('VoiceContinuations/'+ID).reservedCents,100);assert.equal(f.rows.get('VoiceContinuations/'+ID).spentCents,0);assert.deepEqual(f.rows.get('VoiceUsage/preview-budget'),before);assert.deepEqual(f.rows.get('VoiceUsage/session-'+'a'.repeat(64)),old);assert.equal(f.provider.length,0);
  assert.deepEqual(await reserve(f)(1,'old-helper-first-session'),{granted:false,reason:'SESSION_ALREADY_USED'});
});
for(const config of [{provenance:false},{allowContinuation:false}])test('historical helper cannot spend native-only grant without '+Object.keys(config)[0],async()=>{
  const f=legacy(),before=f.protectedBytes();assert.deepEqual(await reserve(f,config)(1,'old-helper-unqualified-session'),{granted:false,reason:'ALLOCATION_EXHAUSTED'});unchanged(f,before);
});
for(const patch of [{allocatedCents:'100'},{reservedCents:1},{spentCents:1},{state:'refunded'},{provider:'untrusted'},{issuedAt:'invalid'},{expiresAt:AT+WINDOW+1001}])test('malformed historical grant helper fails closed '+JSON.stringify(patch),async()=>{
  const f=legacy();f.rows.set('VoiceContinuations/'+ID,{...grant(),...patch});const before=f.protectedBytes();assert.deepEqual(await reserve(f)(1,'old-helper-invalid-session'),{granted:false,reason:'INVALID_LEDGER'});unchanged(f,before);
});
for(const state of ['future','expired','missing','invalid-pointer'])test('historical '+state+' grant cannot fund a legacy helper',async()=>{
  const f=legacy();if(state==='future')f.rows.set('VoiceContinuations/'+ID,{...grant(),issuedAt:AT+1000,expiresAt:AT+WINDOW});if(state==='expired')f.setAt(AT+WINDOW);if(state==='missing')f.rows.delete('VoiceContinuations/'+ID);if(state==='invalid-pointer')f.rows.set('VoiceContinuations/active',{grantId:'PRIVATE_INVALID'});const before=f.protectedBytes();assert.deepEqual(await reserve(f)(1,'old-helper-unavailable-session'),{granted:false,reason:['missing','invalid-pointer'].includes(state)?'INVALID_LEDGER':'ALLOCATION_EXHAUSTED'});unchanged(f,before);
});
for(const stage of ['provider_rejected','unknown','deadline_failed'])test('historical helper provenance '+stage+' cannot invent a refund',async()=>{
  const f=legacy(),result=await reserve(f)(1,'old-helper-outcome-'+stage.replaceAll('_','-'));assert.equal(result.granted,true);assert.equal(await result.recordOutcome({stage,providerStatus:503,providerCode:'CALL_EXCEPTION',secret:'PRIVATE_SECRET',spentCents:0}),true);assert.equal(f.rows.get('VoiceContinuations/'+ID).reservedCents,100);assert.equal(f.rows.get('VoiceContinuations/'+ID).spentCents,0);const row=[...f.rows].find(([key,value])=>key.startsWith('VoiceUsage/session-')&&value.fundingSource==='native_continuation')[1];assert.equal(row.reconcile,'provider_evidence_required');assert.equal(row.allocatedCents,100);assert.equal(row.stage,stage);assert.equal(row.secret,undefined);assert.equal(row.spentCents,undefined);assert.equal(f.provider.length,0);
});
test('native API uses neither valid nor expired old one-test authority',async()=>{
  const f=legacy();f.rows.set('VoiceContinuations/'+ID,{...grant(),expiresAt:AT});const before=f.moneyBytes(),answer=await f.start();assert.equal(answer.response.status,200);assert.equal(f.moneyBytes(),before);assert.equal(f.journal()[0].stage,'call_verified');
});
